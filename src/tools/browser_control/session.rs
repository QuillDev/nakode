//! One private browser per owning session: its process, tabs, page events and observations.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, Instant},
};

use base64::Engine as _;
use serde_json::{Value, json};
use tokio::{
    process::Child,
    sync::{Notify, mpsc},
};

use super::{Outcome, cdp, keys};

/// Messages kept per tab for `diagnostics`, and the length each is cut to.
const MAX_DIAGNOSTICS: usize = 50;
const MAX_DIAGNOSTIC_CHARS: usize = 300;
/// Requests remembered per tab so a failure can name its URL.
const MAX_TRACKED_REQUESTS: usize = 512;
const MAX_OBSERVATIONS: usize = 64;
const MAX_TITLE_CHARS: usize = 200;
const DEFAULT_WAIT: Duration = Duration::from_secs(10);
const MAX_WAIT: Duration = Duration::from_secs(30);
const TAB_READY_TIMEOUT: Duration = Duration::from_secs(5);
/// How often a wait rereads the page when no event has said it changed.
const WAIT_POLL: Duration = Duration::from_millis(100);
/// How long a scroll may take to move the viewport before the result says it did not.
const SCROLL_SETTLE: Duration = Duration::from_millis(500);
pub(crate) const WINDOW_SIZE: (u32, u32) = (1280, 800);

/// Browsers by owning logical session.
static BROWSERS: LazyLock<Mutex<HashMap<String, Slot>>> = LazyLock::new(Mutex::default);

type Slot = Arc<tokio::sync::Mutex<Option<Browser>>>;

/// Closes a session's browser, e.g. when the session is deleted.
pub(crate) fn discard(owner: &str) {
    BROWSERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(owner);
}

fn slot(owner: &str) -> Slot {
    BROWSERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(owner.to_owned())
        .or_default()
        .clone()
}

/// The browser executable: `NAKODE_BROWSER_EXECUTABLE`, then Chromium or Chrome on `PATH`, then
/// the standard macOS application locations.
pub(crate) fn executable() -> Option<PathBuf> {
    if let Some(configured) = std::env::var_os("NAKODE_BROWSER_EXECUTABLE") {
        let configured = PathBuf::from(configured);
        return configured.is_file().then_some(configured);
    }
    let environment = crate::machine_path::environment();
    let path = environment
        .get("PATH")
        .cloned()
        .or_else(|| std::env::var("PATH").ok())
        .unwrap_or_default();
    let on_path = [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
    ]
    .into_iter()
    .find_map(|name| {
        std::env::split_paths(&path)
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
    });
    on_path.or_else(|| {
        [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]
        .into_iter()
        .map(PathBuf::from)
        .find(|candidate| cfg!(target_os = "macos") && candidate.is_file())
    })
}

/// A visible window only on the agent's own Linux desktop; elsewhere the browser is headless so
/// it never appears on an owner's screen.
fn headless() -> bool {
    !(cfg!(target_os = "linux")
        && std::env::var_os("DISPLAY").is_some_and(|value| !value.is_empty()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settle {
    None,
    Load,
    Idle,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Until {
    Load,
    Idle,
    Url(String),
    Title(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action {
    Open(Option<String>),
    Navigate(String, Settle),
    Tabs,
    SelectTab(u32),
    CloseTab(u32),
    History(i64, Settle),
    Reload(Settle),
    State,
    Screenshot,
    Wait(Until, Duration),
    Diagnostics,
    Click {
        observation: String,
        x: f64,
        y: f64,
        button: &'static str,
        clicks: u32,
    },
    Scroll {
        observation: String,
        x: f64,
        y: f64,
        delta_x: f64,
        delta_y: f64,
    },
    Type(String),
    Key(Vec<keys::Press>),
    Close,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Request {
    pub action: Action,
    pub screenshot: bool,
}

impl Request {
    pub(crate) fn parse(action: &str, arguments: &Value) -> Result<Self, String> {
        let text = |name: &str| arguments.get(name).and_then(Value::as_str);
        let number = |name: &str| arguments.get(name).and_then(Value::as_f64);
        let settle = || match text("wait") {
            None | Some("load") => Ok(Settle::Load),
            Some("idle") => Ok(Settle::Idle),
            Some("none") => Ok(Settle::None),
            Some(other) => Err(format!("unknown wait {other:?}")),
        };
        let tab = || {
            arguments
                .get("tab")
                .and_then(Value::as_u64)
                .and_then(|tab| u32::try_from(tab).ok())
                .ok_or("`tab` must be a tab number from tabs")
        };
        let point = || -> Result<(String, f64, f64), String> {
            let observation = text("observation")
                .ok_or("`observation` is required: use the id from the state or screenshot the coordinates came from")?;
            let (Some(x), Some(y)) = (number("x"), number("y")) else {
                return Err("`x` and `y` are required viewport CSS pixels".to_owned());
            };
            Ok((observation.to_owned(), x, y))
        };
        let action = match action {
            "open" => Action::Open(text("url").map(normalize_url).transpose()?),
            "navigate" => Action::Navigate(
                normalize_url(text("url").ok_or("`url` is required to navigate")?)?,
                settle()?,
            ),
            "tabs" => Action::Tabs,
            "select_tab" => Action::SelectTab(tab()?),
            "close_tab" => Action::CloseTab(tab()?),
            "back" => Action::History(-1, settle()?),
            "forward" => Action::History(1, settle()?),
            "reload" => Action::Reload(settle()?),
            "state" => Action::State,
            "screenshot" => Action::Screenshot,
            "wait" => Self::wait(arguments)?,
            "diagnostics" => Action::Diagnostics,
            "click" => Self::click(arguments, point()?)?,
            "scroll" => {
                let (observation, x, y) = point()?;
                let delta_x = number("delta_x").unwrap_or(0.0);
                let delta_y = number("delta_y").unwrap_or(0.0);
                if delta_x == 0.0 && delta_y == 0.0 {
                    return Err("give `delta_x` or `delta_y` in CSS pixels".to_owned());
                }
                Action::Scroll {
                    observation,
                    x,
                    y,
                    delta_x,
                    delta_y,
                }
            }
            "type" => {
                let typed = text("text")
                    .filter(|value| !value.is_empty())
                    .ok_or("`text` is required to type")?;
                if typed.chars().count() > 4_000 {
                    return Err("type at most 4000 characters at a time".to_owned());
                }
                Action::Type(typed.to_owned())
            }
            "key" => Action::Key(keys::parse(text("keys").unwrap_or_default())?),
            "close" => Action::Close,
            other => return Err(format!("unknown browser action {other:?}")),
        };
        Ok(Self {
            action,
            screenshot: arguments
                .get("screenshot")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    fn wait(arguments: &Value) -> Result<Action, String> {
        let text = |name: &str| arguments.get(name).and_then(Value::as_str);
        let timeout = arguments
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_WAIT, Duration::from_millis)
            .min(MAX_WAIT);
        let needle = || {
            text("text")
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or("`text` is required to wait for a URL or title")
        };
        let until = match text("until") {
            None | Some("load") => Until::Load,
            Some("idle") => Until::Idle,
            Some("url") => Until::Url(needle()?),
            Some("title") => Until::Title(needle()?),
            Some(other) => return Err(format!("unknown wait condition {other:?}")),
        };
        Ok(Action::Wait(until, timeout))
    }

    fn click(arguments: &Value, (observation, x, y): (String, f64, f64)) -> Result<Action, String> {
        let button = match arguments.get("button").and_then(Value::as_str) {
            None | Some("left") => "left",
            Some("right") => "right",
            Some("middle") => "middle",
            Some(other) => return Err(format!("unknown button {other:?}")),
        };
        let clicks = match arguments.get("clicks").and_then(Value::as_u64) {
            None | Some(1) => 1,
            Some(2) => 2,
            Some(_) => return Err("`clicks` must be 1 or 2".to_owned()),
        };
        Ok(Action::Click {
            observation,
            x,
            y,
            button,
            clicks,
        })
    }
}

/// Accepts http(s) and `about:blank`. Bare hosts get https, loopback hosts http.
pub(crate) fn normalize_url(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw == "about:blank" {
        return Ok(raw.to_owned());
    }
    let candidate = if raw.contains("://") {
        raw.to_owned()
    } else {
        let host = raw.split(['/', ':', '?', '#']).next().unwrap_or_default();
        let loopback = host == "localhost"
            || host.ends_with(".localhost")
            || host.starts_with("127.")
            || raw.starts_with("[::1]");
        format!("{}://{raw}", if loopback { "http" } else { "https" })
    };
    let url =
        reqwest::Url::parse(&candidate).map_err(|error| format!("invalid URL {raw:?}: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(format!("only http(s) URLs can be opened, not {raw:?}"));
    }
    Ok(url.into())
}

struct Tab {
    number: u32,
    target: String,
    session: Option<String>,
    ready: bool,
    url: String,
    title: String,
    /// The main frame's current document, the last one that fired load, and network idle.
    loader: String,
    loaded: String,
    idle: String,
    /// Committed main-frame navigations, including same-document ones.
    navigations: u64,
    console: VecDeque<String>,
    failures: VecDeque<String>,
    dropped: u64,
    requests: HashMap<String, String>,
}

impl Tab {
    fn loading(&self) -> bool {
        self.loaded != self.loader
    }

    fn record(list: &mut VecDeque<String>, dropped: &mut u64, message: &str) {
        if list.len() == MAX_DIAGNOSTICS {
            list.pop_front();
            *dropped += 1;
        }
        list.push_back(message.chars().take(MAX_DIAGNOSTIC_CHARS).collect());
    }
}

#[derive(Default)]
struct Tabs {
    list: Vec<Tab>,
    selected: Option<String>,
    next_number: u32,
}

impl Tabs {
    fn by_session(&mut self, session: &str) -> Option<&mut Tab> {
        self.list
            .iter_mut()
            .find(|tab| tab.session.as_deref() == Some(session))
    }

    fn by_target(&mut self, target: &str) -> Option<&mut Tab> {
        self.list.iter_mut().find(|tab| tab.target == target)
    }

    fn selected(&self) -> Option<&Tab> {
        let selected = self.selected.as_deref()?;
        self.list.iter().find(|tab| tab.target == selected)
    }

    fn add(&mut self, target: &str, url: &str, title: &str) -> &mut Tab {
        if let Some(index) = self.list.iter().position(|tab| tab.target == target) {
            return &mut self.list[index];
        }
        self.next_number += 1;
        self.list.push(Tab {
            number: self.next_number,
            target: target.to_owned(),
            session: None,
            ready: false,
            url: url.to_owned(),
            title: title.to_owned(),
            loader: String::new(),
            loaded: String::new(),
            idle: String::new(),
            navigations: 0,
            console: VecDeque::new(),
            failures: VecDeque::new(),
            dropped: 0,
            requests: HashMap::new(),
        });
        if self.selected.is_none() {
            self.selected = Some(target.to_owned());
        }
        self.list.last_mut().expect("just pushed")
    }

    fn remove(&mut self, target: &str) {
        self.list.retain(|tab| tab.target != target);
        if self.selected.as_deref() == Some(target) {
            self.selected = self.list.last().map(|tab| tab.target.clone());
        }
    }
}

#[derive(Default)]
struct Shared {
    tabs: Mutex<Tabs>,
    changed: Notify,
}

impl Shared {
    fn tabs(&self) -> std::sync::MutexGuard<'_, Tabs> {
        self.tabs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until `done` holds or the deadline passes; returns whether it held.
    async fn wait_for(&self, deadline: Instant, mut done: impl FnMut(&Tabs) -> bool) -> bool {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if done(&self.tabs()) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return done(&self.tabs());
            }
        }
    }
}

/// The URL without query or fragment, which can carry tokens.
fn without_query(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    url[..end].to_owned()
}

fn console_text(params: &Value) -> String {
    params["args"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|argument| match &argument["value"] {
            Value::String(text) => text.clone(),
            Value::Null => argument["description"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Folds one protocol event into the tab state.
fn observe_event(
    shared: &Shared,
    attach: &mpsc::UnboundedSender<(String, String, bool)>,
    method: &str,
    session: Option<&str>,
    params: &Value,
) {
    let mut tabs = shared.tabs();
    match (method, session) {
        ("Target.attachedToTarget", _) => {
            let info = &params["targetInfo"];
            let target = info["targetId"].as_str().unwrap_or_default();
            let attached = params["sessionId"].as_str().unwrap_or_default().to_owned();
            let page = info["type"].as_str() == Some("page");
            if page {
                let tab = tabs.add(
                    target,
                    info["url"].as_str().unwrap_or_default(),
                    info["title"].as_str().unwrap_or_default(),
                );
                tab.session = Some(attached.clone());
            }
            let _ = attach.send((attached, target.to_owned(), page));
        }
        ("Target.targetInfoChanged", None) => {
            let info = &params["targetInfo"];
            if let Some(tab) = tabs.by_target(info["targetId"].as_str().unwrap_or_default()) {
                info["url"]
                    .as_str()
                    .unwrap_or_default()
                    .clone_into(&mut tab.url);
                info["title"]
                    .as_str()
                    .unwrap_or_default()
                    .clone_into(&mut tab.title);
            }
        }
        ("Target.targetDestroyed", None) => {
            tabs.remove(params["targetId"].as_str().unwrap_or_default());
        }
        ("Target.detachedFromTarget", None) => {
            if let Some(target) = params["targetId"].as_str() {
                tabs.remove(target);
            }
        }
        (_, Some(session)) => {
            let Some(tab) = tabs.by_session(session) else {
                return;
            };
            observe_page_event(tab, method, params);
        }
        _ => return,
    }
    drop(tabs);
    shared.changed.notify_waiters();
}

fn observe_page_event(tab: &mut Tab, method: &str, params: &Value) {
    let main_frame = |frame: &Value| frame.as_str() == Some(tab.target.as_str());
    match method {
        "Page.lifecycleEvent" if main_frame(&params["frameId"]) => {
            let loader = params["loaderId"].as_str().unwrap_or_default().to_owned();
            match params["name"].as_str() {
                Some("init") => tab.loader = loader,
                Some("load") => tab.loaded = loader,
                Some("networkIdle") => tab.idle = loader,
                _ => {}
            }
        }
        "Page.frameNavigated" if params["frame"]["parentId"].is_null() => {
            let frame = &params["frame"];
            frame["url"]
                .as_str()
                .unwrap_or_default()
                .clone_into(&mut tab.url);
            frame["loaderId"]
                .as_str()
                .unwrap_or_default()
                .clone_into(&mut tab.loader);
            tab.navigations += 1;
            tab.requests.clear();
        }
        "Page.navigatedWithinDocument" if main_frame(&params["frameId"]) => {
            params["url"]
                .as_str()
                .unwrap_or_default()
                .clone_into(&mut tab.url);
            tab.navigations += 1;
        }
        "Runtime.consoleAPICalled" => {
            let level = params["type"].as_str().unwrap_or_default();
            if matches!(level, "error" | "warning" | "assert") {
                let message = format!("[{level}] {}", console_text(params));
                Tab::record(&mut tab.console, &mut tab.dropped, &message);
            }
        }
        "Runtime.exceptionThrown" => {
            let details = &params["exceptionDetails"];
            let description = details["exception"]["description"]
                .as_str()
                .and_then(|text| text.lines().next())
                .or_else(|| details["text"].as_str())
                .unwrap_or("uncaught exception");
            let message = format!("[exception] {description}");
            Tab::record(&mut tab.console, &mut tab.dropped, &message);
        }
        "Network.requestWillBeSent" => {
            if tab.requests.len() >= MAX_TRACKED_REQUESTS {
                tab.requests.clear();
            }
            let request = &params["request"];
            tab.requests.insert(
                params["requestId"].as_str().unwrap_or_default().to_owned(),
                format!(
                    "{} {}",
                    request["method"].as_str().unwrap_or("GET"),
                    without_query(request["url"].as_str().unwrap_or_default())
                ),
            );
        }
        "Network.responseReceived" => {
            let status = params["response"]["status"].as_u64().unwrap_or(0);
            if status >= 400 {
                let request = tab
                    .requests
                    .get(params["requestId"].as_str().unwrap_or_default())
                    .cloned()
                    .unwrap_or_else(|| {
                        without_query(params["response"]["url"].as_str().unwrap_or_default())
                    });
                Tab::record(
                    &mut tab.failures,
                    &mut tab.dropped,
                    &format!("{request} -> {status}"),
                );
            }
        }
        "Network.loadingFailed" if params["canceled"].as_bool() != Some(true) => {
            let request = tab
                .requests
                .get(params["requestId"].as_str().unwrap_or_default())
                .cloned()
                .unwrap_or_else(|| "request".to_owned());
            let error = params["errorText"].as_str().unwrap_or("failed");
            Tab::record(
                &mut tab.failures,
                &mut tab.dropped,
                &format!("{request} -> {error}"),
            );
        }
        _ => {}
    }
}

/// Enables page events on each newly attached tab, then lets it run.
async fn prepare_targets(
    connection: Arc<cdp::Connection>,
    shared: Arc<Shared>,
    mut attached: mpsc::UnboundedReceiver<(String, String, bool)>,
) {
    while let Some((session, target, page)) = attached.recv().await {
        let session = Some(session.as_str());
        if page {
            for (method, params) in [
                ("Page.enable", json!({})),
                ("Page.setLifecycleEventsEnabled", json!({"enabled": true})),
                ("Runtime.enable", json!({})),
                ("Network.enable", json!({})),
            ] {
                let _ = connection.call(session, method, params).await;
            }
        }
        let _ = connection
            .call(session, "Runtime.runIfWaitingForDebugger", json!({}))
            .await;
        if page {
            if let Some(tab) = shared.tabs().by_target(&target) {
                tab.ready = true;
            }
            shared.changed.notify_waiters();
        } else {
            let _ = connection
                .call(
                    None,
                    "Target.detachFromTarget",
                    json!({"sessionId": session}),
                )
                .await;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Geometry {
    width: f64,
    height: f64,
    /// Device pixels per CSS pixel: display scale times browser zoom.
    scale: f64,
    /// Pinch zoom of the visual viewport.
    zoom: f64,
    scroll_x: f64,
    scroll_y: f64,
}

impl Geometry {
    fn matches(&self, other: &Self) -> bool {
        let near = |a: f64, b: f64, tolerance: f64| (a - b).abs() <= tolerance;
        near(self.width, other.width, 0.5)
            && near(self.height, other.height, 0.5)
            && near(self.scale, other.scale, 0.001)
            && near(self.zoom, other.zoom, 0.001)
            && near(self.scroll_x, other.scroll_x, 0.5)
            && near(self.scroll_y, other.scroll_y, 0.5)
    }

    fn describe(&self) -> String {
        format!(
            "{}x{} CSS px at scale {} zoom {} scrolled ({}, {})",
            self.width, self.height, self.scale, self.zoom, self.scroll_x, self.scroll_y
        )
    }
}

struct Observation {
    id: String,
    target: String,
    navigations: u64,
    geometry: Geometry,
}

struct Browser {
    child: Child,
    connection: Arc<cdp::Connection>,
    shared: Arc<Shared>,
    _profile: tempfile::TempDir,
    observations: VecDeque<Observation>,
    next_observation: u64,
    headless: bool,
    product: String,
}

/// Runs one request against the owner's browser, starting it when the action needs one.
pub(crate) async fn execute(
    owner: &str,
    request: Request,
    screenshots: &Path,
) -> Result<Outcome, String> {
    let slot = slot(owner);
    let mut guard = slot.lock().await;
    if guard
        .as_mut()
        .is_some_and(|browser| !matches!(browser.child.try_wait(), Ok(None)))
    {
        *guard = None;
    }
    if request.action == Action::Close {
        let closed = guard.take().is_some();
        drop(guard);
        discard(owner);
        return Ok(Outcome {
            text: json!({"closed": closed}).to_string(),
            image: None,
        });
    }
    let launches = matches!(request.action, Action::Open(_) | Action::Navigate(..));
    let mut started = None;
    if guard.is_none() {
        if !launches {
            return Err(
                "no browser is running for this session; use open or navigate first".to_owned(),
            );
        }
        let began = Instant::now();
        *guard = Some(Browser::launch().await?);
        started = Some(began.elapsed());
    }
    let browser = guard.as_mut().expect("browser is running");
    let screenshot = request.screenshot || request.action == Action::Screenshot;
    let mut result = browser.perform(request.action, started.is_some()).await?;
    if let Some(started) = started {
        result["started"] = json!({
            "ms": u64::try_from(started.as_millis()).unwrap_or(u64::MAX),
            "headless": browser.headless,
            "browser": browser.product,
        });
    }
    let mut image = None;
    if screenshot {
        let observation = result["observation"].as_str().unwrap_or("o0").to_owned();
        let (path, png) = browser.screenshot(screenshots, &observation).await?;
        result["screenshot"] = Value::from(path.display().to_string());
        image = Some(png);
    }
    Ok(Outcome {
        text: result.to_string(),
        image,
    })
}

impl Browser {
    async fn launch() -> Result<Self, String> {
        let executable = executable().ok_or(
            "no Chromium or Google Chrome was found on this machine; set NAKODE_BROWSER_EXECUTABLE",
        )?;
        let root = nix::unistd::geteuid().is_root();
        // The waiver applies only to root, which cannot run Chromium's sandbox at all.
        let sandbox_waived =
            root && std::env::var("NAKODE_BROWSER_NO_SANDBOX").as_deref() == Ok("1");
        if root && !sandbox_waived {
            return Err("Chromium cannot keep its sandbox when run as root; run the agent as a normal user, or have the operator set NAKODE_BROWSER_NO_SANDBOX=1 where the machine itself is the isolation boundary".to_owned());
        }
        let profile = tempfile::Builder::new()
            .prefix("nakode-browser-")
            .tempdir()
            .map_err(|error| format!("could not create a browser profile: {error}"))?;
        let headless = headless();
        let mut arguments = vec![
            format!("--user-data-dir={}", profile.path().display()),
            format!("--window-size={},{}", WINDOW_SIZE.0, WINDOW_SIZE.1),
            "--no-first-run".to_owned(),
            "--no-default-browser-check".to_owned(),
            "--disable-sync".to_owned(),
            "--disable-background-networking".to_owned(),
            "--disable-component-update".to_owned(),
            "--disable-default-apps".to_owned(),
            "--disable-extensions".to_owned(),
            "--password-store=basic".to_owned(),
            "--use-mock-keychain".to_owned(),
            "--hide-crash-restore-bubble".to_owned(),
            // Back/forward cache restores skip load events that navigation waits observe.
            "--disable-features=Translate,BackForwardCache,MediaRouter".to_owned(),
        ];
        if headless {
            arguments.push("--headless=new".to_owned());
        }
        if sandbox_waived {
            arguments.push("--no-sandbox".to_owned());
        }
        arguments.push("about:blank".to_owned());
        let shared = Arc::new(Shared::default());
        let (attach, attached) = mpsc::unbounded_channel();
        let events = {
            let shared = shared.clone();
            Box::new(move |method: &str, session: Option<&str>, params: &Value| {
                observe_event(&shared, &attach, method, session, params);
            })
        };
        let (child, connection) = cdp::launch(&executable, &arguments, events)?;
        tokio::spawn(prepare_targets(
            connection.clone(),
            shared.clone(),
            attached,
        ));
        let version = connection
            .call(None, "Browser.getVersion", json!({}))
            .await
            .map_err(|error| format!("the browser did not start: {error}"))?;
        connection
            .call(None, "Target.setDiscoverTargets", json!({"discover": true}))
            .await?;
        connection
            .call(
                None,
                "Target.setAutoAttach",
                json!({"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true}),
            )
            .await?;
        connection
            .call(
                None,
                "Browser.setDownloadBehavior",
                json!({"behavior": "deny"}),
            )
            .await?;
        let mut browser = Self {
            child,
            connection,
            shared,
            _profile: profile,
            observations: VecDeque::new(),
            next_observation: 1,
            headless,
            product: version["product"].as_str().unwrap_or("Chromium").to_owned(),
        };
        browser.adopt_existing_pages().await?;
        Ok(browser)
    }

    /// Attaches to the first page when auto-attach did not report it, then waits until it is ready.
    async fn adopt_existing_pages(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + TAB_READY_TIMEOUT;
        let ready = |tabs: &Tabs| tabs.selected().is_some_and(|tab| tab.ready);
        if self
            .shared
            .wait_for(Instant::now() + Duration::from_millis(1_500), ready)
            .await
        {
            return Ok(());
        }
        let targets = self
            .connection
            .call(None, "Target.getTargets", json!({}))
            .await?;
        for info in targets["targetInfos"].as_array().into_iter().flatten() {
            let target = info["targetId"].as_str().unwrap_or_default();
            if info["type"].as_str() == Some("page") && !info["attached"].as_bool().unwrap_or(false)
            {
                self.connection
                    .call(
                        None,
                        "Target.attachToTarget",
                        json!({"targetId": target, "flatten": true}),
                    )
                    .await?;
            }
        }
        if self.shared.wait_for(deadline, ready).await {
            Ok(())
        } else {
            Err("the browser started but its first tab never became ready".to_owned())
        }
    }

    async fn perform(&mut self, action: Action, fresh: bool) -> Result<Value, String> {
        match action {
            Action::Open(url) => self.open(url, fresh).await,
            Action::Navigate(url, settle) => self.navigate(&url, settle).await,
            Action::Tabs => Ok(self.tab_list()),
            Action::SelectTab(number) => {
                let target = self.target_of(number)?;
                self.connection
                    .call(None, "Target.activateTarget", json!({"targetId": target}))
                    .await?;
                self.shared.tabs().selected = Some(target);
                self.state(json!({})).await
            }
            Action::CloseTab(number) => self.close_tab(number).await,
            Action::History(delta, settle) => self.history(delta, settle).await,
            Action::Reload(settle) => {
                let (session, before) = self.selected_session()?;
                self.connection
                    .call(Some(&session), "Page.reload", json!({}))
                    .await?;
                self.settle_after(before, None, settle).await
            }
            Action::State | Action::Screenshot => self.state(json!({})).await,
            Action::Wait(until, timeout) => self.wait(until, timeout).await,
            Action::Diagnostics => self.diagnostics(),
            Action::Click {
                observation,
                x,
                y,
                button,
                clicks,
            } => self.click(&observation, x, y, button, clicks).await,
            Action::Scroll {
                observation,
                x,
                y,
                delta_x,
                delta_y,
            } => self.scroll(&observation, x, y, delta_x, delta_y).await,
            Action::Type(text) => {
                let (session, _) = self.selected_session()?;
                self.connection
                    .call(Some(&session), "Input.insertText", json!({"text": text}))
                    .await?;
                self.state(json!({"typed": text.chars().count()})).await
            }
            Action::Key(presses) => {
                let (session, _) = self.selected_session()?;
                for press in &presses {
                    self.press(&session, press).await?;
                }
                self.state(json!({"pressed": presses.len()})).await
            }
            Action::Close => Err("close is handled before the browser is used".to_owned()),
        }
    }

    async fn open(&mut self, url: Option<String>, fresh: bool) -> Result<Value, String> {
        if !fresh && url.is_some() {
            let created = self
                .connection
                .call(None, "Target.createTarget", json!({"url": "about:blank"}))
                .await?;
            let target = created["targetId"]
                .as_str()
                .ok_or("the browser did not open a tab")?
                .to_owned();
            let ready = self
                .shared
                .wait_for(Instant::now() + TAB_READY_TIMEOUT, |tabs| {
                    tabs.list
                        .iter()
                        .any(|tab| tab.target == target && tab.ready)
                })
                .await;
            if !ready {
                return Err("the new tab never became ready".to_owned());
            }
            self.shared.tabs().selected = Some(target);
        }
        match url {
            Some(url) => self.navigate(&url, Settle::Load).await,
            None => self.state(json!({})).await,
        }
    }

    async fn navigate(&mut self, url: &str, settle: Settle) -> Result<Value, String> {
        let (session, before) = self.selected_session()?;
        let navigated = self
            .connection
            .call(Some(&session), "Page.navigate", json!({"url": url}))
            .await?;
        if let Some(error) = navigated["errorText"]
            .as_str()
            .filter(|error| !error.is_empty())
        {
            let state = self.state(json!({})).await.unwrap_or_default();
            return Err(format!("navigation to {url} failed: {error}; now {state}"));
        }
        let loader = navigated["loaderId"].as_str().map(str::to_owned);
        self.settle_after(before, loader, settle).await
    }

    async fn history(&mut self, delta: i64, settle: Settle) -> Result<Value, String> {
        let (session, before) = self.selected_session()?;
        let history = self
            .connection
            .call(Some(&session), "Page.getNavigationHistory", json!({}))
            .await?;
        let index = history["currentIndex"].as_i64().unwrap_or(0) + delta;
        let entry = usize::try_from(index)
            .ok()
            .and_then(|index| history["entries"].as_array()?.get(index))
            .ok_or(if delta < 0 {
                "there is no earlier page in this tab"
            } else {
                "there is no later page in this tab"
            })?;
        self.connection
            .call(
                Some(&session),
                "Page.navigateToHistoryEntry",
                json!({"entryId": entry["id"]}),
            )
            .await?;
        self.settle_after(before, None, settle).await
    }

    /// Waits for a started navigation to commit and then load or go idle, within the default bound.
    /// A navigation still in flight at the bound is reported as loading, not as a failure.
    async fn settle_after(
        &mut self,
        before: u64,
        loader: Option<String>,
        settle: Settle,
    ) -> Result<Value, String> {
        let target = self.selected_target()?;
        let deadline = Instant::now() + DEFAULT_WAIT;
        let settled = settle == Settle::None
            || self
                .shared
                .wait_for(deadline, |tabs| {
                    let Some(tab) = tabs.list.iter().find(|tab| tab.target == target) else {
                        return true;
                    };
                    let current = loader.as_deref().unwrap_or(&tab.loader);
                    let committed = match &loader {
                        Some(loader) => tab.loader == *loader,
                        None => tab.navigations > before,
                    };
                    committed
                        && match settle {
                            Settle::Load => tab.loaded == current,
                            Settle::Idle => tab.idle == current,
                            Settle::None => true,
                        }
                })
                .await;
        let note = if settled {
            json!({})
        } else {
            json!({"note": format!("still loading after {} ms; use wait", DEFAULT_WAIT.as_millis())})
        };
        self.state(note).await
    }

    /// Reads the tab's current URL and title from the browser rather than from events.
    async fn refresh_info(&self, target: &str) -> Result<(), String> {
        let info = self
            .connection
            .call(None, "Target.getTargetInfo", json!({"targetId": target}))
            .await?;
        let mut tabs = self.shared.tabs();
        let tab = tabs.by_target(target).ok_or("the tab closed")?;
        if let Some(url) = info["targetInfo"]["url"].as_str() {
            url.clone_into(&mut tab.url);
        }
        if let Some(title) = info["targetInfo"]["title"].as_str() {
            title.clone_into(&mut tab.title);
        }
        Ok(())
    }

    async fn wait(&mut self, until: Until, timeout: Duration) -> Result<Value, String> {
        let target = self.selected_target()?;
        let done = |tabs: &Tabs| {
            let Some(tab) = tabs.list.iter().find(|tab| tab.target == target) else {
                return true;
            };
            match &until {
                Until::Load => !tab.loading(),
                Until::Idle => tab.idle == tab.loader,
                Until::Url(text) => tab.url.contains(text.as_str()),
                Until::Title(text) => tab.title.contains(text.as_str()),
            }
        };
        let deadline = Instant::now() + timeout;
        let met = loop {
            let slice = (Instant::now() + WAIT_POLL).min(deadline);
            if self.shared.wait_for(slice, &done).await {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            // Title and URL change events can lag or never arrive; read them directly.
            self.refresh_info(&target).await?;
        };
        let state = self.state(json!({})).await?;
        if met {
            Ok(state)
        } else {
            Err(format!(
                "timed out after {} ms waiting for {until:?}; now {state}",
                timeout.as_millis()
            ))
        }
    }

    async fn close_tab(&mut self, number: u32) -> Result<Value, String> {
        let target = self.target_of(number)?;
        self.connection
            .call(None, "Target.closeTarget", json!({"targetId": target}))
            .await?;
        self.shared
            .wait_for(Instant::now() + TAB_READY_TIMEOUT, |tabs| {
                tabs.list.iter().all(|tab| tab.target != target)
            })
            .await;
        // The listener may not have heard yet; the closed tab is gone either way.
        self.shared.tabs().remove(&target);
        if self.shared.tabs().selected.is_none() {
            return Ok(json!({"closed_tab": number, "tabs": []}));
        }
        let mut state = self.state(json!({})).await?;
        state["closed_tab"] = Value::from(number);
        Ok(state)
    }

    fn diagnostics(&self) -> Result<Value, String> {
        let mut tabs = self.shared.tabs();
        let selected = tabs.selected.clone().ok_or("no tab is open")?;
        let tab = tabs.by_target(&selected).ok_or("no tab is open")?;
        let result = json!({
            "tab": tab.number,
            "url": tab.url,
            "console": tab.console.drain(..).collect::<Vec<_>>(),
            "failed_requests": tab.failures.drain(..).collect::<Vec<_>>(),
            "dropped": tab.dropped,
        });
        tab.dropped = 0;
        Ok(result)
    }

    async fn click(
        &mut self,
        observation: &str,
        x: f64,
        y: f64,
        button: &str,
        clicks: u32,
    ) -> Result<Value, String> {
        let session = self.aimed(observation, x, y).await?;
        let base = json!({"x": x, "y": y});
        self.mouse(&session, "mouseMoved", &base, "none", 0).await?;
        for count in 1..=clicks {
            self.mouse(&session, "mousePressed", &base, button, count)
                .await?;
            self.mouse(&session, "mouseReleased", &base, button, count)
                .await?;
        }
        self.state(json!({"clicked": [x, y]})).await
    }

    async fn scroll(
        &mut self,
        observation: &str,
        x: f64,
        y: f64,
        delta_x: f64,
        delta_y: f64,
    ) -> Result<Value, String> {
        let session = self.aimed(observation, x, y).await?;
        let before = self.measure(&session).await?;
        self.connection
            .call(
                Some(&session),
                "Input.dispatchMouseEvent",
                json!({"type": "mouseWheel", "x": x, "y": y, "deltaX": delta_x, "deltaY": delta_y}),
            )
            .await?;
        // Wheel scrolling lands asynchronously; report the viewport once it has moved.
        let deadline = Instant::now() + SCROLL_SETTLE;
        let mut moved = false;
        while Instant::now() < deadline {
            let now = self.measure(&session).await?;
            if (now.scroll_x - before.scroll_x).abs() > 0.5
                || (now.scroll_y - before.scroll_y).abs() > 0.5
            {
                moved = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(16)).await;
        }
        self.state(json!({"scrolled": moved})).await
    }

    async fn mouse(
        &self,
        session: &str,
        kind: &str,
        point: &Value,
        button: &str,
        clicks: u32,
    ) -> Result<(), String> {
        let mut event = point.clone();
        event["type"] = Value::from(kind);
        event["button"] = Value::from(button);
        event["clickCount"] = Value::from(clicks);
        self.connection
            .call(Some(session), "Input.dispatchMouseEvent", event)
            .await
            .map(drop)
    }

    async fn press(&self, session: &str, press: &keys::Press) -> Result<(), String> {
        let mut down = json!({
            "type": if press.text.is_some() { "keyDown" } else { "rawKeyDown" },
            "key": press.key,
            "code": press.code,
            "windowsVirtualKeyCode": press.key_code,
            "modifiers": press.modifiers,
        });
        if let Some(text) = &press.text {
            down["text"] = Value::from(text.as_str());
        }
        self.connection
            .call(Some(session), "Input.dispatchKeyEvent", down)
            .await?;
        self.connection
            .call(
                Some(session),
                "Input.dispatchKeyEvent",
                json!({
                    "type": "keyUp",
                    "key": press.key,
                    "code": press.code,
                    "windowsVirtualKeyCode": press.key_code,
                    "modifiers": press.modifiers,
                }),
            )
            .await
            .map(drop)
    }

    /// Confirms coordinates are aimed from a current observation of the selected tab: same page,
    /// same viewport size, scale, zoom and scroll, and inside the viewport. Returns its session.
    async fn aimed(&mut self, observation: &str, x: f64, y: f64) -> Result<String, String> {
        let (session, _) = self.selected_session()?;
        let target = self.selected_target()?;
        let seen = self
            .observations
            .iter()
            .find(|seen| seen.id == observation)
            .ok_or_else(|| {
                format!("unknown observation {observation:?}; take state or a screenshot first")
            })?;
        if seen.target != target {
            return Err(format!(
                "observation {observation} is of another tab; take state or a screenshot of this one"
            ));
        }
        let (expected, seen_navigations) = (seen.geometry, seen.navigations);
        let now = self.measure(&session).await?;
        // Checked after measuring, so a navigation that commits meanwhile is still caught.
        let (current, navigations) = self.selected_session()?;
        if current != session || seen_navigations != navigations {
            return Err(format!(
                "observation {observation} is stale: the page navigated since; observe again"
            ));
        }
        if !now.matches(&expected) {
            return Err(format!(
                "observation {observation} is stale: the viewport was {} and is now {}; observe again",
                expected.describe(),
                now.describe()
            ));
        }
        if x >= now.width || y >= now.height {
            return Err(format!(
                "({x}, {y}) is outside the {}x{} viewport",
                now.width, now.height
            ));
        }
        Ok(session)
    }

    async fn measure(&self, session: &str) -> Result<Geometry, String> {
        let metrics = self
            .connection
            .call(Some(session), "Page.getLayoutMetrics", json!({}))
            .await?;
        let css = &metrics["cssVisualViewport"];
        let width = css["clientWidth"].as_f64().unwrap_or(0.0);
        let device_width = metrics["visualViewport"]["clientWidth"]
            .as_f64()
            .unwrap_or(width);
        let scale = if width > 0.0 {
            (device_width / width * 1000.0).round() / 1000.0
        } else {
            1.0
        };
        Ok(Geometry {
            width,
            height: css["clientHeight"].as_f64().unwrap_or(0.0),
            scale,
            zoom: css["zoom"].as_f64().unwrap_or(1.0),
            scroll_x: css["pageX"].as_f64().unwrap_or(0.0),
            scroll_y: css["pageY"].as_f64().unwrap_or(0.0),
        })
    }

    /// Compact state of the selected tab with a fresh observation, merged into `extra`.
    async fn state(&mut self, mut extra: Value) -> Result<Value, String> {
        let (session, navigations) = self.selected_session()?;
        let target = self.selected_target()?;
        let geometry = self.measure(&session).await?;
        self.refresh_info(&target).await?;
        let (number, count, loading) = {
            let mut tabs = self.shared.tabs();
            let count = tabs.list.len();
            let tab = tabs.by_target(&target).ok_or("the tab closed")?;
            (tab.number, count, tab.loading())
        };
        let id = format!("o{}", self.next_observation);
        self.next_observation += 1;
        if self.observations.len() == MAX_OBSERVATIONS {
            self.observations.pop_front();
        }
        self.observations.push_back(Observation {
            id: id.clone(),
            target: target.clone(),
            navigations,
            geometry,
        });
        let tabs = self.shared.tabs();
        let tab = tabs
            .list
            .iter()
            .find(|tab| tab.target == target)
            .ok_or("the tab closed")?;
        let mut state = json!({
            "tab": number,
            "tabs": count,
            "url": tab.url,
            "title": tab.title.chars().take(MAX_TITLE_CHARS).collect::<String>(),
            "loading": loading,
            "viewport": {
                "width": geometry.width,
                "height": geometry.height,
                "scale": geometry.scale,
                "zoom": geometry.zoom,
                "scroll_x": geometry.scroll_x,
                "scroll_y": geometry.scroll_y,
            },
            "observation": id,
        });
        if !tab.console.is_empty() || !tab.failures.is_empty() {
            state["diagnostics"] =
                json!({"console": tab.console.len(), "failed_requests": tab.failures.len()});
        }
        drop(tabs);
        if let (Some(state), Some(extra)) = (state.as_object_mut(), extra.as_object_mut()) {
            state.append(extra);
        }
        Ok(state)
    }

    fn tab_list(&self) -> Value {
        let tabs = self.shared.tabs();
        let selected = tabs.selected.clone();
        Value::from(
            tabs.list
                .iter()
                .map(|tab| {
                    json!({
                        "tab": tab.number,
                        "url": tab.url,
                        "title": tab.title.chars().take(MAX_TITLE_CHARS).collect::<String>(),
                        "loading": tab.loading(),
                        "selected": selected.as_deref() == Some(tab.target.as_str()),
                    })
                })
                .collect::<Vec<_>>(),
        )
    }

    /// Captures the selected tab's viewport at one image pixel per CSS pixel and saves it under
    /// the name of the observation it belongs to.
    async fn screenshot(
        &self,
        directory: &Path,
        observation: &str,
    ) -> Result<(PathBuf, Vec<u8>), String> {
        let (session, _) = self.selected_session()?;
        let geometry = self.measure(&session).await?;
        // Clipped to the reported viewport (scrollbars excluded) at one image pixel per CSS pixel.
        let params = json!({
            "format": "png",
            "captureBeyondViewport": false,
            "clip": {
                "x": geometry.scroll_x,
                "y": geometry.scroll_y,
                "width": geometry.width,
                "height": geometry.height,
                "scale": 1.0 / geometry.scale,
            },
        });
        let captured = self
            .connection
            .call(Some(&session), "Page.captureScreenshot", params)
            .await?;
        let png = base64::engine::general_purpose::STANDARD
            .decode(captured["data"].as_str().unwrap_or_default())
            .map_err(|error| format!("the browser returned an unreadable screenshot: {error}"))?;
        std::fs::create_dir_all(directory)
            .map_err(|error| format!("could not keep the screenshot: {error}"))?;
        let path = directory.join(format!("{observation}.png"));
        std::fs::write(&path, &png)
            .map_err(|error| format!("could not keep the screenshot: {error}"))?;
        Ok((path, png))
    }

    fn selected_target(&self) -> Result<String, String> {
        self.shared
            .tabs()
            .selected
            .clone()
            .ok_or_else(|| "no tab is open; use open".to_owned())
    }

    /// The selected tab's protocol session and its navigation count.
    fn selected_session(&self) -> Result<(String, u64), String> {
        let tabs = self.shared.tabs();
        let tab = tabs.selected().ok_or("no tab is open; use open")?;
        let session = tab
            .session
            .clone()
            .filter(|_| tab.ready)
            .ok_or("the tab is still starting; try again")?;
        Ok((session, tab.navigations))
    }

    fn target_of(&self, number: u32) -> Result<String, String> {
        self.shared
            .tabs()
            .list
            .iter()
            .find(|tab| tab.number == number)
            .map(|tab| tab.target.clone())
            .ok_or_else(|| format!("there is no tab {number}; list them with tabs"))
    }
}

/// Benchmark oracle: where the selected tab's viewport origin is on the screen, for aiming the
/// desktop tools at the same page. Test-only; agents are never given script evaluation.
#[cfg(test)]
pub(crate) async fn viewport_screen_origin(owner: &str) -> Result<(f64, f64), String> {
    let slot = slot(owner);
    let guard = slot.lock().await;
    let browser = guard.as_ref().ok_or("no browser")?;
    let (session, _) = browser.selected_session()?;
    let result = browser
        .connection
        .call(
            Some(&session),
            "Runtime.evaluate",
            json!({
                // The window frame's side border is half the width difference; the toolbar sits
                // above the page and the same border below it.
                "expression": "[screenX + (outerWidth - innerWidth) / 2, screenY + (outerHeight - innerHeight) - (outerWidth - innerWidth) / 2]",
                "returnByValue": true,
            }),
        )
        .await?;
    let origin = &result["result"]["value"];
    Ok((
        origin[0].as_f64().unwrap_or(0.0),
        origin[1].as_f64().unwrap_or(0.0),
    ))
}
