//! `browser_control`: a private Chromium per logical session, driven over the `DevTools` protocol.
//!
//! The browser runs on the execution machine with a fresh temporary profile and Chromium's own
//! sandbox. It is never the owner's browser or the dashboard's browser pane, and it is never used
//! in place of one. Agents get navigation, tabs, compact page state, opt-in viewport screenshots,
//! bounded waits and diagnostics, and viewport-coordinate input that is refused when the
//! observation it was aimed from is stale. No script evaluation is exposed.
//!
//! Measured against the desktop tools in `docs/browser-control.md`.

mod cdp;
mod keys;
mod session;

#[cfg(test)]
mod bench;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult};
use crate::runtime::ToolDefinition;

pub(crate) use session::discard;

pub(crate) const TOOL_NAME: &str = "browser_control";

pub(crate) const DESCRIPTION: &str = "Drive this agent's own private browser on the execution machine: a headless Chromium (visible on the agent desktop where one exists) with a fresh profile. It is not the owner's browser or the dashboard browser pane, which cannot be controlled. `navigate` opens a URL (starting the browser when needed) and waits for load; `open` starts the browser or adds a tab; `tabs`, `select_tab`, `close_tab`, `back`, `forward`, `reload`, `close`. Every result is compact JSON page state: tab, URL, title, loading, CSS-pixel viewport, device scale and an `observation` id. `screenshot` saves a viewport PNG whose pixels are CSS pixels to the Gallery (attach it with return_image or inspect it with vision); any action takes one afterwards only with `screenshot: true`. `wait` blocks (bounded) for load, network idle, or URL/title text; `diagnostics` returns and clears recent console errors, page exceptions and failed requests. `click`/`scroll` take viewport CSS-pixel x/y plus the `observation` they were aimed from and are refused if the tab, page or viewport changed since; `type` inserts text into the focused element; `key` presses keys such as Enter, Tab, ctrl+a. Input is not retried: on an uncertain result, observe before acting again.";

/// What one call produced: text for the model and, for screenshots, the image.
pub(crate) struct Outcome {
    pub text: String,
    pub image: Option<Vec<u8>>,
}

/// Runs one call for the browser owned by `owner`, a logical Nakode session.
pub(crate) async fn run(
    owner: &str,
    workspace: &Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<Outcome, String> {
    let action = arguments
        .get("action")
        .and_then(Value::as_str)
        .ok_or("`action` is required")?;
    let request = session::Request::parse(action, arguments)?;
    let screenshots = screenshot_directory(workspace, owner);
    tokio::select! {
        () = cancellation.cancelled() => Err("browser action interrupted; its outcome is unknown, observe before acting again".to_owned()),
        outcome = session::execute(owner, request, &screenshots) => outcome,
    }
}

/// Screenshots go to the stack Gallery, one file per owning session.
fn screenshot_directory(workspace: &Path, owner: &str) -> PathBuf {
    let short: String = owner
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    super::desktop::gallery(workspace)
        .join("browser")
        .join(if short.is_empty() {
            "session".to_owned()
        } else {
            short
        })
}

/// Whether this machine can run the tool: Unix with a Chromium-family browser.
pub(crate) fn available() -> bool {
    session::executable().is_some()
}

pub(crate) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: TOOL_NAME,
        description: DESCRIPTION,
        parameters: json!({
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": [
                    "open", "navigate", "tabs", "select_tab", "close_tab", "back", "forward",
                    "reload", "state", "screenshot", "wait", "diagnostics", "click", "scroll",
                    "type", "key", "close"
                ]},
                "url": {"type": "string", "description": "navigate/open: http(s) URL; bare hosts get https, localhost gets http"},
                "tab": {"type": "integer", "minimum": 1, "description": "select_tab/close_tab: tab number from tabs"},
                "observation": {"type": "string", "description": "click/scroll: the observation id the coordinates were read from"},
                "x": {"type": "number", "minimum": 0, "description": "click/scroll: viewport CSS pixels from the left"},
                "y": {"type": "number", "minimum": 0, "description": "click/scroll: viewport CSS pixels from the top"},
                "button": {"type": "string", "enum": ["left", "right", "middle"], "description": "click: default left"},
                "clicks": {"type": "integer", "minimum": 1, "maximum": 2, "description": "click: 2 for double click"},
                "delta_x": {"type": "number", "description": "scroll: CSS pixels, positive is right"},
                "delta_y": {"type": "number", "description": "scroll: CSS pixels, positive is down"},
                "text": {"type": "string", "maxLength": 4000, "description": "type: text to insert; wait: URL or title text"},
                "keys": {"type": "string", "maxLength": 200, "description": "key: space-separated chords such as Enter, Tab, ctrl+a, shift+ArrowLeft"},
                "until": {"type": "string", "enum": ["load", "idle", "url", "title"], "description": "wait: condition; url/title wait until they contain `text`"},
                "wait": {"type": "string", "enum": ["none", "load", "idle"], "description": "navigate/back/forward/reload: default load"},
                "timeout_ms": {"type": "integer", "minimum": 0, "maximum": 30000, "description": "wait bound, default 10000"},
                "screenshot": {"type": "boolean", "description": "take a viewport screenshot after the action"}
            },
            "required": ["action"],
            "additionalProperties": false
        }),
    }
}

fn owner_key(context: &ToolContext<'_>) -> String {
    context
        .session
        .owner_session_id
        .clone()
        .unwrap_or_else(|| context.session.id.clone())
}

pub struct BrowserControlTool;

impl Tool for BrowserControlTool {
    fn definition(&self) -> ToolDefinition {
        definition()
    }

    fn summarize(&self, arguments: &Value) -> String {
        let action = arguments
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("browser");
        match arguments.get("url").and_then(Value::as_str) {
            Some(url) => format!("{action} {}", url.chars().take(100).collect::<String>()),
            None => action.replace('_', " "),
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
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let owner = owner_key(&context);
            match run(&owner, context.workspace, &arguments, cancellation).await {
                Ok(outcome) => ToolResult::success(outcome.text),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}
