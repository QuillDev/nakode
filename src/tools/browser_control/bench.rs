//! Reproducible latency benchmark: `browser_control` against the desktop (CUA) tools on the same
//! loopback page, through their real tool paths. See `docs/browser-control.md` for how to run it
//! and what it does not measure (model inference between calls).
//!
//! ```text
//! cargo test --release --lib browser_control::bench -- --ignored --nocapture
//! ```
//!
//! `NAKODE_BROWSER_BENCH_ITERATIONS` sets warm iterations (default 30) and
//! `NAKODE_BROWSER_BENCH_OUT` a JSON results path. The desktop rows are measured only where
//! `desktop::available()` holds; elsewhere they are reported as unmeasured with the reason.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::{
    session,
    tests::{call, fixture},
};
use crate::tools::desktop;

/// One measured operation: its samples, and how many of them did what they were meant to.
struct Row {
    name: String,
    path: &'static str,
    round_trips: u32,
    images: u32,
    samples: Vec<Duration>,
    correct: usize,
    note: String,
}

impl Row {
    fn new(name: &str, path: &'static str, round_trips: u32, images: u32) -> Self {
        Self {
            name: name.to_owned(),
            path,
            round_trips,
            images,
            samples: Vec::new(),
            correct: 0,
            note: String::new(),
        }
    }

    fn percentile(&self, percent: usize) -> f64 {
        let mut sorted = self.samples.clone();
        sorted.sort();
        if sorted.is_empty() {
            return f64::NAN;
        }
        let rank = (percent * sorted.len())
            .div_ceil(100)
            .clamp(1, sorted.len());
        sorted[rank - 1].as_secs_f64() * 1000.0
    }

    fn json(&self) -> Value {
        json!({
            "name": self.name,
            "path": self.path,
            "samples": self.samples.len(),
            "p50_ms": self.percentile(50),
            "p95_ms": self.percentile(95),
            "correct": self.correct,
            "tool_round_trips": self.round_trips,
            "images_to_model": self.images,
            "note": self.note,
        })
    }
}

async fn timed<T>(future: impl Future<Output = T>) -> (Duration, T) {
    let began = Instant::now();
    let value = future.await;
    (began.elapsed(), value)
}

fn iterations() -> usize {
    std::env::var("NAKODE_BROWSER_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30)
}

async fn browser(owner: &str, workspace: &Path, arguments: Value) -> Value {
    call(owner, workspace, arguments)
        .await
        .unwrap_or_else(|error| panic!("browser_control failed: {error}"))
}

/// Polls the page title through the tool (untimed oracle) until it contains `text`.
async fn title_reaches(owner: &str, workspace: &Path, text: &str) -> bool {
    call(
        owner,
        workspace,
        json!({"action": "wait", "until": "title", "text": text, "timeout_ms": 2000}),
    )
    .await
    .is_ok()
}

async fn reset(owner: &str, workspace: &Path, origin: &str) -> Value {
    browser(
        owner,
        workspace,
        json!({"action": "navigate", "url": format!("{origin}/")}),
    )
    .await
}

fn observation(state: &Value) -> String {
    state["observation"]
        .as_str()
        .expect("observation")
        .to_owned()
}

async fn measure_cold_start(origin: &str, workspace: &Path, rows: &mut Vec<Row>) {
    let count = iterations();
    let mut cold = Row::new("cold start + first navigate", "browser_control", 1, 0);
    let mut launch = Row::new("cold browser launch only", "browser_control", 0, 0);
    for index in 0..count.min(10) {
        let owner = format!("bench-cold-{index}");
        let (elapsed, state) = timed(call(
            &owner,
            workspace,
            json!({"action": "navigate", "url": format!("{origin}/")}),
        ))
        .await;
        let state = state.expect("cold navigate");
        cold.samples.push(elapsed);
        launch.samples.push(Duration::from_millis(
            state["started"]["ms"].as_u64().unwrap_or_default(),
        ));
        if state["title"] == "Fixture" {
            cold.correct += 1;
            launch.correct += 1;
        }
        session::discard(&owner);
    }
    rows.extend([cold, launch]);
}

async fn measure_browser_control(origin: &str, workspace: &Path, rows: &mut Vec<Row>) {
    measure_cold_start(origin, workspace, rows).await;
    let count = iterations();
    let owner = "bench-warm";
    reset(owner, workspace, origin).await;
    let mut navigate = Row::new("warm navigate + wait load", "browser_control", 1, 0);
    let mut state = Row::new("observe: state", "browser_control", 1, 0);
    let mut screenshot = Row::new("observe: screenshot", "browser_control", 1, 1);
    for _ in 0..count {
        let (elapsed, result) = timed(call(
            owner,
            workspace,
            json!({"action": "navigate", "url": format!("{origin}/next")}),
        ))
        .await;
        navigate.samples.push(elapsed);
        navigate.correct += usize::from(result.is_ok_and(|state| state["title"] == "Next"));
        let (elapsed, result) = timed(call(owner, workspace, json!({"action": "state"}))).await;
        state.samples.push(elapsed);
        state.correct += usize::from(result.is_ok());
        let (elapsed, result) =
            timed(call(owner, workspace, json!({"action": "screenshot"}))).await;
        screenshot.samples.push(elapsed);
        screenshot.correct +=
            usize::from(result.is_ok_and(|state| state["screenshot"].is_string()));
    }
    rows.extend([navigate, state, screenshot]);

    let mut click = Row::new("act: click", "browser_control", 1, 0);
    let mut click_workflow = Row::new(
        "workflow: screenshot, click, verify title",
        "browser_control",
        2,
        1,
    );
    let mut type_text = Row::new("act: type 5 chars", "browser_control", 1, 0);
    let mut key = Row::new("act: key Enter", "browser_control", 1, 0);
    let mut scroll = Row::new("act: scroll 400px", "browser_control", 1, 0);
    let mut verified_in_result = 0;
    for _ in 0..count {
        reset(owner, workspace, origin).await;
        // Workflow: see the page, click, and read the effect from the click's own result.
        let began = Instant::now();
        let seen = browser(owner, workspace, json!({"action": "screenshot"})).await;
        let (elapsed, clicked) = timed(call(
            owner,
            workspace,
            json!({"action": "click", "observation": observation(&seen), "x": 150, "y": 130}),
        ))
        .await;
        let clicked = clicked.expect("click");
        let mut verified = clicked["title"] == "Clicked 1";
        if verified {
            verified_in_result += 1;
        } else {
            verified = title_reaches(owner, workspace, "Clicked 1").await;
        }
        click_workflow.samples.push(began.elapsed());
        click.samples.push(elapsed);
        click.correct += usize::from(verified);
        click_workflow.correct += usize::from(verified);

        browser(
            owner,
            workspace,
            json!({"action": "click", "observation": observation(&clicked), "x": 200, "y": 215}),
        )
        .await;
        let (elapsed, _) = timed(call(
            owner,
            workspace,
            json!({"action": "type", "text": "hello"}),
        ))
        .await;
        type_text.samples.push(elapsed);
        type_text.correct += usize::from(title_reaches(owner, workspace, "Typed hello").await);
        let (elapsed, _) = timed(call(
            owner,
            workspace,
            json!({"action": "key", "keys": "Enter"}),
        ))
        .await;
        key.samples.push(elapsed);
        key.correct += usize::from(title_reaches(owner, workspace, "Entered hello").await);

        let current = browser(owner, workspace, json!({"action": "state"})).await;
        let (elapsed, result) = timed(call(
            owner,
            workspace,
            json!({"action": "scroll", "observation": observation(&current), "x": 600, "y": 400, "delta_y": 400}),
        ))
        .await;
        scroll.samples.push(elapsed);
        scroll.correct += usize::from(result.is_ok_and(|state| state["scrolled"] == true));
    }
    click_workflow.note = format!(
        "{verified_in_result}/{count} verified from the click result itself; others needed one wait"
    );
    rows.extend([click, click_workflow, type_text, key, scroll]);
    session::discard(owner);
}

async fn desktop_bridge(workspace: &Path, tool: &str, arguments: Value) -> Value {
    crate::claude::desktop_request(
        &json!({"id": "bench", "turnId": "bench", "workspace": workspace.display().to_string(), "tool": tool, "arguments": arguments}),
        Some("bench-desktop"),
    )
    .await
}

/// Where the page's viewport sits on the screen, kept with a desktop frame as evidence.
async fn desktop_page_origin(owner: &str, workspace: &Path) -> (f64, f64) {
    let (left, top) = session::viewport_screen_origin(owner)
        .await
        .expect("viewport origin");
    println!("desktop: page viewport at screen ({left}, {top})");
    if let (Ok(shot), Some(out)) = (
        desktop::screenshot(workspace).await,
        std::env::var_os("NAKODE_BROWSER_BENCH_OUT"),
    ) {
        std::fs::write(Path::new(&out).with_extension("desktop.png"), shot.png)
            .expect("desktop frame");
    }
    (left, top)
}

async fn measure_desktop(origin: &str, workspace: &Path, rows: &mut Vec<Row>) {
    let count = iterations();
    // The desktop tools act on the same headed browser window, found through the oracle.
    let owner = "bench-desktop-page";
    reset(owner, workspace, origin).await;
    let (left, top) = desktop_page_origin(owner, workspace).await;
    // Whole screen pixels for xdotool; the window sits at integer positions.
    let at = |x: f64, y: f64| -> (u64, u64) {
        let pixel = |value: f64| format!("{value:.0}").parse().unwrap_or(0);
        (pixel(left + x), pixel(top + y))
    };

    let mut screenshot = Row::new("observe: desktop_screenshot", "desktop (CUA)", 1, 1);
    let mut click = Row::new("act: desktop_action click (xdotool)", "desktop (CUA)", 1, 0);
    let mut click_bridge = Row::new(
        "act: desktop_action click, Claude bridge (settle + screenshot)",
        "desktop (CUA)",
        1,
        1,
    );
    let mut click_workflow = Row::new(
        "workflow: screenshot, click, verify (Claude bridge)",
        "desktop (CUA)",
        2,
        2,
    );
    let mut type_text = Row::new("act: desktop_action type 5 chars", "desktop (CUA)", 1, 0);
    let mut key = Row::new("act: desktop_action key Return", "desktop (CUA)", 1, 0);
    let mut scroll = Row::new("act: desktop_action scroll 5 steps", "desktop (CUA)", 1, 0);
    for _ in 0..count {
        reset(owner, workspace, origin).await;
        let (elapsed, shot) = timed(desktop::screenshot(workspace)).await;
        screenshot.samples.push(elapsed);
        screenshot.correct += usize::from(shot.is_ok());

        let (x, y) = at(150.0, 130.0);
        let (elapsed, result) =
            timed(desktop::act(&json!({"action": "click", "x": x, "y": y}))).await;
        click.samples.push(elapsed);
        let landed = result.is_ok() && title_reaches(owner, workspace, "Clicked 1").await;
        click.correct += usize::from(landed);
        if !landed && click.note.is_empty() {
            let page = browser(owner, workspace, json!({"action": "state"})).await;
            click.note = format!("first miss: {result:?}; page title {}", page["title"]);
        }

        let began = Instant::now();
        let seen = desktop_bridge(workspace, desktop::SCREENSHOT_TOOL, json!({})).await;
        let (elapsed, reply) = timed(desktop_bridge(
            workspace,
            desktop::ACTION_TOOL,
            json!({"action": "click", "x": x, "y": y}),
        ))
        .await;
        click_workflow.samples.push(began.elapsed());
        click_bridge.samples.push(elapsed);
        let landed = seen["failed"] == false
            && reply["failed"] == false
            && reply["image"].is_object()
            && title_reaches(owner, workspace, "Clicked 2").await;
        click_bridge.correct += usize::from(landed);
        click_workflow.correct += usize::from(landed);

        let (fx, fy) = at(200.0, 215.0);
        desktop::act(&json!({"action": "click", "x": fx, "y": fy}))
            .await
            .expect("focus field");
        let (elapsed, result) =
            timed(desktop::act(&json!({"action": "type", "text": "hello"}))).await;
        type_text.samples.push(elapsed);
        type_text.correct +=
            usize::from(result.is_ok() && title_reaches(owner, workspace, "Typed hello").await);
        let (elapsed, result) =
            timed(desktop::act(&json!({"action": "key", "keys": "Return"}))).await;
        key.samples.push(elapsed);
        key.correct +=
            usize::from(result.is_ok() && title_reaches(owner, workspace, "Entered hello").await);

        let (sx, sy) = at(600.0, 400.0);
        let (elapsed, result) = timed(desktop::act(&json!({
            "action": "scroll", "direction": "down", "amount": 5, "x": sx, "y": sy
        })))
        .await;
        scroll.samples.push(elapsed);
        scroll.correct +=
            usize::from(result.is_ok() && title_reaches(owner, workspace, "Scrolled").await);
    }
    click_workflow.note = "the click's own screenshot is the verification".to_owned();
    rows.extend([
        screenshot,
        click,
        click_bridge,
        click_workflow,
        type_text,
        key,
        scroll,
    ]);
    session::discard(owner);
}

#[tokio::test]
#[ignore = "benchmark: needs Chromium; desktop rows also need DISPLAY, xdotool and ffmpeg"]
async fn browser_control_benchmark() {
    assert!(super::available(), "no browser on this machine");
    let origin = fixture::serve().await;
    let root = tempfile::tempdir().expect("root");
    let workspace = root.path();
    let mut rows = Vec::new();
    measure_browser_control(&origin, workspace, &mut rows).await;
    let desktop = if desktop::available() {
        measure_desktop(&origin, workspace, &mut rows).await;
        json!({"measured": true})
    } else {
        json!({"measured": false, "reason": "no DISPLAY with xdotool and ffmpeg on this machine"})
    };
    let report = json!({
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "headless": !(cfg!(target_os = "linux") && std::env::var_os("DISPLAY").is_some()),
        },
        "iterations": iterations(),
        "desktop": desktop,
        "rows": rows.iter().map(Row::json).collect::<Vec<_>>(),
    });
    println!("| path | operation | n | p50 ms | p95 ms | correct | tool calls | images |");
    println!("|---|---|---|---|---|---|---|---|");
    for row in &rows {
        println!(
            "| {} | {} | {} | {:.1} | {:.1} | {}/{} | {} | {} |{}",
            row.path,
            row.name,
            row.samples.len(),
            row.percentile(50),
            row.percentile(95),
            row.correct,
            row.samples.len(),
            row.round_trips,
            row.images,
            if row.note.is_empty() {
                String::new()
            } else {
                format!(" {}", row.note)
            }
        );
    }
    if let Some(path) = std::env::var_os("NAKODE_BROWSER_BENCH_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&report).expect("report"))
            .expect("write report");
    }
}
