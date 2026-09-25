//! The agent's private desktop: screenshots, mouse and keyboard input, and screen recordings
//! compressed into the Gallery. Native tools and the Claude bridge share these functions.
//!
//! The desktop exists where the machine provides one (`DISPLAY`, `xdotool` and `ffmpeg`), as
//! `FStack` environments do. Nothing here starts a display or touches a real screen.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{LazyLock, Mutex, PoisonError},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult};
use crate::runtime::ToolDefinition;

/// Longest recording kept; ffmpeg stops capturing by itself after this.
const MAX_RECORDING: Duration = Duration::from_secs(300);
/// A compressed recording must fit in this, or the attempt with the most compression is kept.
const RECORDING_BUDGET_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TYPED_CHARACTERS: usize = 4_000;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) const SCREENSHOT_TOOL: &str = "desktop_screenshot";
pub(crate) const ACTION_TOOL: &str = "desktop_action";
pub(crate) const RECORD_TOOL: &str = "screen_record";

/// One screenshot: PNG bytes, the file they were also written to, and the screen size.
pub(crate) struct Screenshot {
    pub png: Vec<u8>,
    pub path: PathBuf,
    pub width: u32,
    pub height: u32,
}

struct Recording {
    child: Child,
    raw: PathBuf,
    name: String,
    started: Instant,
}

/// Recordings in progress, one per session, across turns.
static RECORDINGS: LazyLock<Mutex<HashMap<String, Recording>>> = LazyLock::new(Mutex::default);

fn display() -> Option<String> {
    std::env::var("DISPLAY")
        .ok()
        .filter(|value| !value.is_empty())
}

fn on_path(program: &str) -> bool {
    let environment = crate::machine_path::environment();
    let path = environment
        .get("PATH")
        .cloned()
        .or_else(|| std::env::var("PATH").ok())
        .unwrap_or_default();
    std::env::split_paths(&path).any(|directory| directory.join(program).is_file())
}

/// Whether this machine gives agents a desktop.
pub(crate) fn available() -> bool {
    display().is_some() && on_path("xdotool") && on_path("ffmpeg")
}

fn command(program: &str) -> Command {
    let mut command = Command::new(program);
    command
        .envs(crate::machine_path::environment())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn output(mut command: Command, what: &str) -> Result<Vec<u8>, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let result = tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .map_err(|_| format!("{what} timed out"))?
        .map_err(|error| format!("{what} could not start: {error}"))?;
    if result.status.success() {
        Ok(result.stdout)
    } else {
        let detail = String::from_utf8_lossy(&result.stderr);
        Err(format!("{what} failed: {}", detail.trim()))
    }
}

async fn geometry() -> Result<(u32, u32), String> {
    let mut xdotool = command("xdotool");
    xdotool.arg("getdisplaygeometry");
    let text =
        String::from_utf8_lossy(&output(xdotool, "reading the screen size").await?).into_owned();
    let mut parts = text.split_whitespace().map(str::parse::<u32>);
    match (parts.next(), parts.next()) {
        (Some(Ok(width)), Some(Ok(height))) => Ok((width, height)),
        _ => Err(format!("unexpected screen size {text:?}")),
    }
}

/// Where desktop output goes: the stack's Gallery when there is one, else one in the workspace.
pub(crate) fn gallery(workspace: &Path) -> PathBuf {
    workspace
        .ancestors()
        .map(|directory| directory.join(".tmp-gallery"))
        .find(|gallery| gallery.is_dir())
        .unwrap_or_else(|| workspace.join(".tmp-gallery"))
}

/// Captures the whole screen and keeps it as `<workspace>/.fstack-desktop/screenshot.png`.
pub(crate) async fn screenshot(workspace: &Path) -> Result<Screenshot, String> {
    let display = display().ok_or("this machine has no desktop")?;
    let (width, height) = geometry().await?;
    let mut ffmpeg = command("ffmpeg");
    ffmpeg.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "x11grab",
        "-video_size",
    ]);
    ffmpeg.arg(format!("{width}x{height}"));
    ffmpeg.args([
        "-i",
        &display,
        "-frames:v",
        "1",
        "-f",
        "image2pipe",
        "-c:v",
        "png",
        "-",
    ]);
    let png = output(ffmpeg, "taking the screenshot").await?;
    let directory = workspace.join(".fstack-desktop");
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("could not keep the screenshot: {error}"))?;
    let path = directory.join("screenshot.png");
    std::fs::write(&path, &png)
        .map_err(|error| format!("could not keep the screenshot: {error}"))?;
    Ok(Screenshot {
        png,
        path,
        width,
        height,
    })
}

fn coordinate(arguments: &Value, field: &str, limit: u32) -> Result<String, String> {
    let value = arguments
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("`{field}` must be a whole number of pixels"))?;
    if value >= u64::from(limit) {
        return Err(format!("`{field}` {value} is off the {limit}-pixel screen"));
    }
    Ok(value.to_string())
}

fn point(arguments: &Value, (width, height): (u32, u32)) -> Result<[String; 2], String> {
    Ok([
        coordinate(arguments, "x", width)?,
        coordinate(arguments, "y", height)?,
    ])
}

/// Performs one input action and says what it did.
pub(crate) async fn act(arguments: &Value) -> Result<String, String> {
    display().ok_or("this machine has no desktop")?;
    let action = arguments
        .get("action")
        .and_then(Value::as_str)
        .ok_or("`action` is required")?;
    let size = geometry().await?;
    let mut xdotool = command("xdotool");
    let done = match action {
        "click" | "double_click" | "right_click" | "move" => {
            let [x, y] = point(arguments, size)?;
            xdotool.args(["mousemove", "--sync", &x, &y]);
            match action {
                "click" => xdotool.args(["click", "1"]),
                "double_click" => xdotool.args(["click", "--repeat", "2", "1"]),
                "right_click" => xdotool.args(["click", "3"]),
                _ => &mut xdotool,
            };
            format!("{} at ({x}, {y})", action.replace('_', " "))
        }
        "drag" => {
            let [x, y] = point(arguments, size)?;
            let to_x = coordinate(arguments, "to_x", size.0)?;
            let to_y = coordinate(arguments, "to_y", size.1)?;
            xdotool.args(["mousemove", "--sync", &x, &y, "mousedown", "1"]);
            xdotool.args(["mousemove", "--sync", &to_x, &to_y, "mouseup", "1"]);
            format!("dragged from ({x}, {y}) to ({to_x}, {to_y})")
        }
        "scroll" => {
            let button = match arguments.get("direction").and_then(Value::as_str) {
                Some("up") => "4",
                Some("down") | None => "5",
                Some("left") => "6",
                Some("right") => "7",
                Some(other) => return Err(format!("unknown scroll direction {other:?}")),
            };
            let amount = arguments
                .get("amount")
                .and_then(Value::as_u64)
                .unwrap_or(3)
                .clamp(1, 20)
                .to_string();
            if arguments.get("x").is_some() {
                let [x, y] = point(arguments, size)?;
                xdotool.args(["mousemove", "--sync", &x, &y]);
            }
            xdotool.args(["click", "--repeat", &amount, "--delay", "40", button]);
            format!("scrolled {amount} steps")
        }
        "type" => {
            let text = arguments
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .ok_or("`text` is required to type")?;
            if text.chars().count() > MAX_TYPED_CHARACTERS {
                return Err(format!(
                    "type at most {MAX_TYPED_CHARACTERS} characters at a time"
                ));
            }
            xdotool.args(["type", "--delay", "12", "--", text]);
            format!("typed {} characters", text.chars().count())
        }
        "key" => {
            let keys = arguments
                .get("keys")
                .and_then(Value::as_str)
                .filter(|keys| {
                    !keys.is_empty()
                        && keys.len() <= 200
                        && keys.chars().all(|c| {
                            c.is_ascii_alphanumeric() || matches!(c, '+' | '_' | ' ' | '-')
                        })
                })
                .ok_or("`keys` must be key names such as `Return` or `ctrl+l`")?;
            xdotool.arg("key").arg("--");
            xdotool.args(keys.split_whitespace());
            format!("pressed {keys}")
        }
        other => return Err(format!("unknown desktop action {other:?}")),
    };
    output(xdotool, "the desktop action").await?;
    Ok(done)
}

fn recording_name(requested: Option<&str>) -> String {
    let cleaned = requested
        .unwrap_or_default()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let cleaned = cleaned
        .trim_matches('-')
        .chars()
        .take(64)
        .collect::<String>();
    if cleaned.is_empty() {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        format!("recording-{seconds}")
    } else {
        cleaned
    }
}

/// Starts recording the whole screen for this session.
pub(crate) async fn record_start(session: &str, name: Option<&str>) -> Result<String, String> {
    let display = display().ok_or("this machine has no desktop")?;
    if RECORDINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(session)
    {
        return Err("a recording is already running; stop it first".to_owned());
    }
    let (width, height) = geometry().await?;
    let name = recording_name(name);
    let raw = std::env::temp_dir().join(format!("nakode-recording-{}.mp4", uuid::Uuid::now_v7()));
    let mut ffmpeg = command("ffmpeg");
    ffmpeg
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "x11grab",
            "-framerate",
            "15",
        ])
        .arg("-video_size")
        .arg(format!("{width}x{height}"))
        .args(["-i", &display, "-t"])
        .arg(MAX_RECORDING.as_secs().to_string())
        .args([
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
        ])
        .arg(&raw)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = ffmpeg
        .spawn()
        .map_err(|error| format!("could not start recording: {error}"))?;
    RECORDINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(
            session.to_owned(),
            Recording {
                child,
                raw,
                name: name.clone(),
                started: Instant::now(),
            },
        );
    Ok(format!(
        "Recording the screen as {name:?} (up to {} minutes). Call screen_record with action \"stop\" to finish and save it to the Gallery.",
        MAX_RECORDING.as_secs() / 60
    ))
}

/// What a finished recording became.
pub(crate) struct Saved {
    pub video: PathBuf,
    pub poster: PathBuf,
    pub bytes: u64,
    pub seconds: f64,
}

/// Stops this session's recording and saves it, compressed, to the Gallery.
pub(crate) async fn record_stop(session: &str, workspace: &Path) -> Result<Saved, String> {
    let mut recording = RECORDINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(session)
        .ok_or("no recording is running")?;
    if let Some(pid) = recording.child.id().and_then(|id| i32::try_from(id).ok()) {
        // SIGINT lets ffmpeg finish the file it is writing.
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGINT,
        );
    }
    if tokio::time::timeout(Duration::from_secs(20), recording.child.wait())
        .await
        .is_err()
    {
        let _ = recording.child.kill().await;
    }
    let seconds = recording
        .started
        .elapsed()
        .as_secs_f64()
        .min(MAX_RECORDING.as_secs_f64());
    let saved = save(&recording.raw, workspace, &recording.name).await;
    let _ = std::fs::remove_file(&recording.raw);
    saved.map(|(video, poster, bytes)| Saved {
        video,
        poster,
        bytes,
        seconds,
    })
}

/// Stops a recording without keeping it, e.g. when its session ends.
pub(crate) fn record_discard(session: &str) {
    if let Some(recording) = RECORDINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(session)
    {
        drop(recording.child);
        let _ = std::fs::remove_file(recording.raw);
    }
}

/// Compresses a raw capture: identical frames dropped, the last one held a second, H.264 at
/// the first quality that fits the budget.
async fn save(raw: &Path, workspace: &Path, name: &str) -> Result<(PathBuf, PathBuf, u64), String> {
    let size = std::fs::metadata(raw).map_err(|_| "the recording captured nothing".to_owned())?;
    if size.len() == 0 {
        return Err("the recording captured nothing".to_owned());
    }
    let directory = gallery(workspace).join("recordings");
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let mut stem = name.to_owned();
    let mut suffix = 2;
    while directory.join(format!("{stem}.mp4")).exists() {
        stem = format!("{name}-{suffix}");
        suffix += 1;
    }
    let video = directory.join(format!("{stem}.mp4"));
    let poster = directory.join(format!("{stem}.poster.png"));
    let mut bytes = 0;
    for (width, quality) in [(1280, "30"), (1280, "34"), (960, "36"), (720, "38")] {
        let mut ffmpeg = command("ffmpeg");
        ffmpeg
            .args(["-hide_banner", "-loglevel", "error", "-y", "-i"])
            .arg(raw)
            .arg("-vf")
            .arg(format!(
                "mpdecimate,scale='min({width},iw)':-2,tpad=stop_mode=clone:stop_duration=1"
            ))
            .args([
                "-fps_mode",
                "vfr",
                "-c:v",
                "libx264",
                "-preset",
                "slow",
                "-crf",
                quality,
            ])
            .args(["-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an"])
            .arg(&video);
        output_with(
            ffmpeg,
            "compressing the recording",
            Duration::from_secs(300),
        )
        .await?;
        bytes = std::fs::metadata(&video).map_or(0, |metadata| metadata.len());
        if bytes <= RECORDING_BUDGET_BYTES {
            break;
        }
    }
    let mut still = command("ffmpeg");
    still
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-sseof",
            "-0.5",
            "-i",
        ])
        .arg(&video)
        .args(["-frames:v", "1"])
        .arg(&poster);
    output(still, "taking the poster frame").await?;
    Ok((video, poster, bytes))
}

async fn output_with(command: Command, what: &str, limit: Duration) -> Result<(), String> {
    let mut command = command;
    command.stdout(Stdio::null()).stderr(Stdio::piped());
    let result = tokio::time::timeout(limit, command.output())
        .await
        .map_err(|_| format!("{what} timed out"))?
        .map_err(|error| format!("{what} could not start: {error}"))?;
    if result.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{what} failed: {}",
            String::from_utf8_lossy(&result.stderr).trim()
        ))
    }
}

/// How a saved recording is reported to the agent.
pub(crate) fn describe(saved: &Saved) -> String {
    format!(
        "Saved the recording to the Gallery: {} ({:.1} MB, {:.0} s), poster {}. The MP4 plays in the Gallery; to show a frame in chat, return_image the poster.",
        saved.video.display(),
        f64::from(u32::try_from(saved.bytes / 1_000).unwrap_or(u32::MAX)) / 1_000.0,
        saved.seconds,
        saved.poster.display()
    )
}

pub(crate) fn screenshot_definition() -> ToolDefinition {
    ToolDefinition {
        name: SCREENSHOT_TOOL,
        description: "Capture this agent's private desktop (a virtual screen with a browser, not the owner's screen). The image is saved to .fstack-desktop/screenshot.png in the workspace; coordinates in it are the ones desktop_action uses.",
        parameters: json!({"type": "object", "properties": {}, "additionalProperties": false}),
    }
}

pub(crate) fn action_definition() -> ToolDefinition {
    ToolDefinition {
        name: ACTION_TOOL,
        description: "Use the mouse and keyboard on this agent's private desktop. Start a browser with `fstack-browser <url> &` from the shell. Take a desktop_screenshot to find coordinates.",
        parameters: json!({
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["click", "double_click", "right_click", "move", "drag", "scroll", "type", "key"]},
                "x": {"type": "integer", "minimum": 0, "description": "Screen pixel, from the left"},
                "y": {"type": "integer", "minimum": 0, "description": "Screen pixel, from the top"},
                "to_x": {"type": "integer", "minimum": 0, "description": "drag: where to release"},
                "to_y": {"type": "integer", "minimum": 0, "description": "drag: where to release"},
                "direction": {"type": "string", "enum": ["up", "down", "left", "right"], "description": "scroll direction"},
                "amount": {"type": "integer", "minimum": 1, "maximum": 20, "description": "scroll steps"},
                "text": {"type": "string", "description": "type: the text to type"},
                "keys": {"type": "string", "description": "key: key names such as Return, Escape or ctrl+l"}
            },
            "required": ["action"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn record_definition() -> ToolDefinition {
    ToolDefinition {
        name: RECORD_TOOL,
        description: "Record this agent's private desktop as a small MP4 for the owner. `start` begins recording (across turns, up to 5 minutes); do the work you want to show; `stop` compresses it and saves it to the Gallery with a poster frame.",
        parameters: json!({
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["start", "stop"]},
                "name": {"type": "string", "description": "start: a short file name, e.g. checkout-flow"}
            },
            "required": ["action"],
            "additionalProperties": false
        }),
    }
}

fn session_key(context: &ToolContext<'_>) -> String {
    context
        .session
        .owner_session_id
        .clone()
        .unwrap_or_else(|| context.session.id.clone())
}

pub struct DesktopScreenshotTool;
pub struct DesktopActionTool;
pub struct ScreenRecordTool;

impl Tool for DesktopScreenshotTool {
    fn definition(&self) -> ToolDefinition {
        screenshot_definition()
    }

    fn summarize(&self, _arguments: &Value) -> String {
        "desktop screenshot".to_owned()
    }

    fn available(&self) -> bool {
        available()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        _arguments: Value,
        _cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            match screenshot(context.workspace).await {
                Ok(shot) => ToolResult::success(format!(
                    "Captured the {}x{} desktop to {}. Inspect it with the vision tool.",
                    shot.width,
                    shot.height,
                    shot.path.display()
                )),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

impl Tool for DesktopActionTool {
    fn definition(&self) -> ToolDefinition {
        action_definition()
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("desktop action")
            .replace('_', " ")
    }

    fn available(&self) -> bool {
        available()
    }

    fn execute<'a>(
        &'a self,
        _context: ToolContext<'a>,
        arguments: Value,
        _cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            act(&arguments)
                .await
                .map_or_else(ToolResult::failure, ToolResult::success)
        })
    }
}

impl Tool for ScreenRecordTool {
    fn definition(&self) -> ToolDefinition {
        record_definition()
    }

    fn summarize(&self, arguments: &Value) -> String {
        match arguments.get("action").and_then(Value::as_str) {
            Some("start") => "start screen recording".to_owned(),
            _ => "stop screen recording".to_owned(),
        }
    }

    fn available(&self) -> bool {
        available()
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::Exclusive
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        _cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let session = session_key(&context);
            match arguments.get("action").and_then(Value::as_str) {
                Some("start") => {
                    record_start(&session, arguments.get("name").and_then(Value::as_str))
                        .await
                        .map_or_else(ToolResult::failure, ToolResult::success)
                }
                Some("stop") => record_stop(&session, context.workspace)
                    .await
                    .map_or_else(ToolResult::failure, |saved| {
                        ToolResult::success(describe(&saved))
                    }),
                _ => ToolResult::failure("`action` must be \"start\" or \"stop\""),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_names_are_safe_file_stems() {
        assert_eq!(recording_name(Some("Checkout Flow!")), "checkout-flow");
        assert_eq!(recording_name(Some("../../etc")), "etc");
        assert!(recording_name(None).starts_with("recording-"));
        assert!(recording_name(Some("---")).starts_with("recording-"));
        assert_eq!(recording_name(Some(&"a".repeat(100))).len(), 64);
    }

    #[test]
    fn coordinates_must_be_on_screen() {
        let arguments = json!({"x": 10, "y": 900});
        assert_eq!(coordinate(&arguments, "x", 1440).as_deref(), Ok("10"));
        assert!(coordinate(&arguments, "y", 900).is_err());
        assert!(coordinate(&json!({"x": -1}), "x", 1440).is_err());
    }

    /// Drives a real desktop: `DISPLAY` with a browser window, `xdotool` and `ffmpeg` on PATH.
    #[tokio::test]
    #[ignore = "needs a live desktop"]
    async fn live_desktop_screenshot_input_and_recording() {
        assert!(available(), "no desktop on this machine");
        let root = tempfile::tempdir().expect("root");
        std::fs::create_dir_all(root.path().join(".tmp-gallery")).expect("gallery");
        let workspace = root.path().join("repo");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let shot = screenshot(&workspace).await.expect("screenshot");
        assert!(shot.png.starts_with(b"\x89PNG"));
        assert!(shot.width > 0 && shot.height > 0);
        record_start("live", Some("Live Test")).await.expect("start");
        assert!(record_start("live", None).await.is_err(), "one recording per session");
        for action in [
            json!({"action": "click", "x": 700, "y": 400}),
            json!({"action": "scroll", "direction": "down", "amount": 5, "x": 700, "y": 400}),
            json!({"action": "type", "text": "hello from a recorded desktop"}),
            json!({"action": "key", "keys": "ctrl+a"}),
        ] {
            act(&action).await.expect("action");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(act(&json!({"action": "click", "x": 99_999, "y": 1})).await.is_err());
        let saved = record_stop("live", &workspace).await.expect("stop");
        assert_eq!(saved.video, root.path().join(".tmp-gallery/recordings/live-test.mp4"));
        assert!(saved.bytes > 0 && saved.bytes <= RECORDING_BUDGET_BYTES);
        assert!(std::fs::read(&saved.poster).expect("poster").starts_with(b"\x89PNG"));
        eprintln!("LIVE {}", describe(&saved));
        assert!(record_stop("live", &workspace).await.is_err(), "already stopped");
    }

    #[test]
    fn the_gallery_is_the_nearest_stack_gallery_or_the_workspace() {
        let root = tempfile::tempdir().expect("root");
        let workspace = root.path().join("repo");
        std::fs::create_dir_all(&workspace).expect("workspace");
        assert_eq!(gallery(&workspace), workspace.join(".tmp-gallery"));
        std::fs::create_dir_all(root.path().join(".tmp-gallery")).expect("stack gallery");
        assert_eq!(gallery(&workspace), root.path().join(".tmp-gallery"));
    }
}
