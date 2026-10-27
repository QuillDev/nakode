//! Request contract tests, plus a live end-to-end run against a real browser.

use std::path::Path;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::session::{Action, Request, Settle, Until, normalize_url};

/// A loopback page with a button, a text field, a link, a tall body, a console error and a
/// missing request: everything the tool observes, acts on and diagnoses.
pub(super) mod fixture {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    pub(crate) const PAGE: &str = r#"<!doctype html><html><head><title>Fixture</title>
<style>body{margin:0;font:16px sans-serif}#go{position:absolute;left:100px;top:100px;width:200px;height:60px}
#field{position:absolute;left:100px;top:200px;width:300px;height:30px}
#next{position:absolute;left:100px;top:300px}#tall{height:5000px}</style></head>
<body><button id="go" onclick="window.n=(window.n||0)+1;document.title='Clicked '+window.n">Go</button>
<input id="field" oninput="document.title='Typed '+this.value" onkeydown="if(event.key==='Enter')document.title='Entered '+this.value">
<a id="next" href="/next">Next</a><div id="tall"></div>
<script>addEventListener('scroll',()=>{document.title='Scrolled '+Math.round(scrollY)});console.error('fixture error');fetch('/missing');</script>
</body></html>"#;

    const NEXT: &str = "<!doctype html><title>Next</title><p>next page</p>";

    /// Serves the fixture on an ephemeral loopback port; returns its origin.
    pub(crate) async fn serve() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let origin = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = vec![0; 4096];
                    let read = stream.read(&mut request).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&request[..read]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/");
                    let (status, body) = match path {
                        "/" => ("200 OK", PAGE),
                        "/next" => ("200 OK", NEXT),
                        _ => ("404 Not Found", "missing"),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        origin
    }
}

#[test]
fn urls_are_web_only_and_bare_hosts_get_a_scheme() {
    assert_eq!(
        normalize_url("example.com").as_deref(),
        Ok("https://example.com/")
    );
    assert_eq!(
        normalize_url("localhost:3000/a").as_deref(),
        Ok("http://localhost:3000/a")
    );
    assert_eq!(
        normalize_url("127.0.0.1:8080").as_deref(),
        Ok("http://127.0.0.1:8080/")
    );
    assert_eq!(normalize_url("about:blank").as_deref(), Ok("about:blank"));
    for refused in [
        "file:///etc/passwd",
        "javascript:alert(1)",
        "chrome://settings",
        "data:text/html,x",
    ] {
        assert!(normalize_url(refused).is_err(), "{refused} must be refused");
    }
}

#[test]
fn coordinate_input_requires_the_observation_it_was_aimed_from() {
    let parse = |arguments: Value| {
        Request::parse(arguments["action"].as_str().expect("action"), &arguments)
    };
    assert!(parse(json!({"action": "click", "x": 1, "y": 2})).is_err());
    assert!(parse(json!({"action": "click", "observation": "o1", "x": 1})).is_err());
    assert!(parse(json!({"action": "scroll", "observation": "o1", "x": 1, "y": 2})).is_err());
    assert!(
        parse(json!({"action": "click", "observation": "o1", "x": 1, "y": 2, "clicks": 3}))
            .is_err()
    );
    assert_eq!(
        parse(json!({"action": "click", "observation": "o1", "x": 1.5, "y": 2}))
            .map(|request| request.action),
        Ok(Action::Click {
            observation: "o1".to_owned(),
            x: 1.5,
            y: 2.0,
            button: "left",
            clicks: 1
        })
    );
}

#[test]
fn screenshots_are_opt_in_and_waits_are_bounded() {
    let navigate = Request::parse("navigate", &json!({"url": "example.com"})).expect("navigate");
    assert!(!navigate.screenshot);
    assert_eq!(
        navigate.action,
        Action::Navigate("https://example.com/".to_owned(), Settle::Load)
    );
    let wait = Request::parse(
        "wait",
        &json!({"until": "title", "text": "Done", "timeout_ms": 999_999}),
    )
    .expect("wait");
    assert_eq!(
        wait.action,
        Action::Wait(
            Until::Title("Done".to_owned()),
            std::time::Duration::from_secs(30)
        )
    );
    assert!(Request::parse("wait", &json!({"until": "url"})).is_err());
    assert!(
        Request::parse("evaluate", &json!({})).is_err(),
        "no script evaluation"
    );
}

#[tokio::test]
async fn actions_other_than_open_and_navigate_never_start_a_browser() {
    let error = super::run(
        "contract-no-browser",
        Path::new("/nonexistent"),
        &json!({"action": "click", "observation": "o1", "x": 1, "y": 1}),
        &CancellationToken::new(),
    )
    .await
    .err()
    .expect("refused");
    assert!(error.contains("no browser is running"), "{error}");
}

pub(super) async fn call(owner: &str, workspace: &Path, arguments: Value) -> Result<Value, String> {
    super::run(owner, workspace, &arguments, &CancellationToken::new())
        .await
        .map(|outcome| serde_json::from_str(&outcome.text).expect("JSON state"))
}

/// Drives a real Chromium through every action against the loopback fixture.
#[tokio::test]
#[ignore = "needs Chromium or Google Chrome"]
async fn live_browser_control_round_trip() {
    assert!(super::available(), "no browser on this machine");
    let origin = fixture::serve().await;
    let root = tempfile::tempdir().expect("root");
    let workspace = root.path();
    let owner = "live-browser-control";
    let call = |arguments: Value| call(owner, workspace, arguments);

    let state = call(json!({"action": "navigate", "url": format!("{origin}/")}))
        .await
        .expect("navigate");
    assert_eq!(state["title"], "Fixture");
    assert_eq!(state["loading"], false);
    assert!(
        state["started"]["ms"].as_u64().is_some(),
        "cold start is reported"
    );
    let observation = state["observation"]
        .as_str()
        .expect("observation")
        .to_owned();

    call(json!({"action": "click", "observation": observation, "x": 150, "y": 130}))
        .await
        .expect("click");
    let state =
        call(json!({"action": "wait", "until": "title", "text": "Clicked 1", "timeout_ms": 3000}))
            .await
            .expect("click landed");
    let fresh = state["observation"]
        .as_str()
        .expect("observation")
        .to_owned();

    let state = call(json!({"action": "click", "observation": fresh, "x": 200, "y": 215}))
        .await
        .expect("focus field");
    let fresh = state["observation"]
        .as_str()
        .expect("observation")
        .to_owned();
    call(json!({"action": "type", "text": "hello"}))
        .await
        .expect("type");
    call(json!({"action": "wait", "until": "title", "text": "Typed hello", "timeout_ms": 3000}))
        .await
        .expect("typed");
    call(json!({"action": "key", "keys": "Enter"}))
        .await
        .expect("key");
    call(json!({"action": "wait", "until": "title", "text": "Entered hello", "timeout_ms": 3000}))
        .await
        .expect("entered");

    let state =
        call(json!({"action": "scroll", "observation": fresh, "x": 600, "y": 400, "delta_y": 400}))
            .await
            .expect("scroll");
    assert_eq!(state["scrolled"], true, "{state}");
    assert!(state["viewport"]["scroll_y"].as_f64().expect("scroll") > 0.0);
    let refused_stale = call(json!({"action": "click", "observation": fresh, "x": 150, "y": 130}))
        .await
        .expect_err("stale observation refused");
    assert!(refused_stale.contains("stale"), "{refused_stale}");
    let offscreen =
        call(json!({"action": "click", "observation": state["observation"], "x": 99_999, "y": 1}))
            .await
            .expect_err("outside viewport refused");
    assert!(offscreen.contains("outside"), "{offscreen}");

    diagnose_capture_and_navigate(owner, workspace, &origin).await;
}

/// The second half of the live round trip: diagnostics, a screenshot, history, tabs and close.
async fn diagnose_capture_and_navigate(owner: &str, workspace: &Path, origin: &str) {
    let call = |arguments: Value| call(owner, workspace, arguments);

    let diagnostics = call(json!({"action": "diagnostics"}))
        .await
        .expect("diagnostics");
    assert!(
        diagnostics["console"].to_string().contains("fixture error"),
        "{diagnostics}"
    );
    assert!(
        diagnostics["failed_requests"]
            .to_string()
            .contains("/missing -> 404"),
        "{diagnostics}"
    );

    let outcome = super::run(
        owner,
        workspace,
        &json!({"action": "screenshot"}),
        &CancellationToken::new(),
    )
    .await
    .expect("screenshot");
    let shot: Value = serde_json::from_str(&outcome.text).expect("state");
    let png = outcome.image.expect("image");
    assert!(png.starts_with(b"\x89PNG"));
    let width = u32::from_be_bytes(png[16..20].try_into().expect("width"));
    let css_width = shot["viewport"]["width"].as_f64().expect("width");
    assert!(
        (f64::from(width) - css_width).abs() < 1.0,
        "image pixels are CSS pixels: {width} vs {css_width}"
    );
    assert!(
        Path::new(shot["screenshot"].as_str().expect("path"))
            .starts_with(workspace.join(".tmp-gallery/browser"))
    );

    let state = call(json!({"action": "navigate", "url": format!("{origin}/next")}))
        .await
        .expect("next");
    assert_eq!(state["title"], "Next");
    let state = call(json!({"action": "back"})).await.expect("back");
    assert!(
        state["url"].as_str().expect("url").ends_with('/'),
        "{state}"
    );
    let state = call(json!({"action": "forward"})).await.expect("forward");
    assert_eq!(state["title"], "Next");
    let state = call(json!({"action": "reload"})).await.expect("reload");
    assert_eq!(state["loading"], false);

    let state = call(json!({"action": "open", "url": format!("{origin}/")}))
        .await
        .expect("new tab");
    assert_eq!(state["tabs"], 2);
    assert_eq!(state["tab"], 2);
    let tabs = call(json!({"action": "tabs"})).await.expect("tabs");
    assert_eq!(tabs.as_array().map(Vec::len), Some(2));
    let state = call(json!({"action": "select_tab", "tab": 1}))
        .await
        .expect("select");
    assert_eq!(state["title"], "Next");
    let state = call(json!({"action": "close_tab", "tab": 2}))
        .await
        .expect("close tab");
    assert_eq!(state["tabs"], 1);

    let refused = call(json!({"action": "navigate", "url": "file:///etc/passwd"}))
        .await
        .expect_err("refused");
    assert!(refused.contains("only http(s)"), "{refused}");

    assert_eq!(
        call(json!({"action": "close"})).await.expect("close")["closed"],
        true
    );
    assert!(
        call(json!({"action": "state"})).await.is_err(),
        "closed browsers stay closed"
    );
}
