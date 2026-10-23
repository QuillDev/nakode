//! The canonical `background_tasks` tool, shared by in-process providers and the Claude bridge.

use std::{collections::HashMap, path::Path, time::Duration};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    CallerScope, DEFAULT_LOG_TAIL_BYTES, MAX_LOG_TAIL_BYTES, MAX_STOP_GRACE, StartRequest,
};
use crate::{
    runtime::ToolDefinition,
    tools::{Tool, ToolContext, ToolFuture, ToolResult},
};

pub(crate) const BACKGROUND_TASKS_TOOL_NAME: &str = "background_tasks";

const DESCRIPTION: &str = "Manage long-running background processes such as dev servers, workers and watchers on this session's execution machine. The Nakode runtime supervises them: they keep running after this call and this turn, are stopped when this session is deleted, and write a bounded log. `start` launches a shell command; `list` shows this session's tasks; `inspect` returns status, a log tail and endpoint probes; `stop` terminates one task's whole process tree; `dismiss` acknowledges a confirmed terminal failure without stopping processes. Successful exits and intentional whole-tree stops are removed automatically. Running only means the processes are alive, not that the service is ready: check the log or endpoints. Endpoints are addresses on the execution machine, so localhost there is not the owner's localhost. Use bash for finite commands such as installs and builds.";

pub(crate) struct BackgroundTasksTool;

impl Tool for BackgroundTasksTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: BACKGROUND_TASKS_TOOL_NAME,
            description: DESCRIPTION,
            parameters: parameters(),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        let action = arguments
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target = arguments
            .get("name")
            .or_else(|| arguments.get("task_id"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        format!("{action} {target}")
            .trim()
            .chars()
            .take(100)
            .collect()
    }

    fn available(&self) -> bool {
        super::supported()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        _cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let scope = context
                .session
                .owner_session_id
                .clone()
                .map(|owner| CallerScope {
                    owner,
                    run_id: context.session.parent_run_id.clone(),
                    turn_id: Some(context.turn_id.to_owned()),
                    call_id: Some(context.call_id.to_owned()),
                });
            match execute_tool_call(scope, context.workspace, &arguments).await {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

/// Runs one tool call for an authenticated caller. `scope` is `None` when the runtime holds no
/// logical owner for the calling provider session; such calls are refused.
pub(crate) async fn execute_tool_call(
    scope: Option<CallerScope>,
    workspace: &Path,
    arguments: &Value,
) -> Result<String, String> {
    let scope = scope.ok_or_else(|| {
        "background tasks are unavailable because this provider session has no logical Nakode owner"
            .to_owned()
    })?;
    let service = super::service()
        .ok_or_else(|| "background tasks are unavailable in this runtime".to_owned())?;
    let action = arguments
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let output = match action {
        "start" => {
            let request = start_request(&scope, workspace, arguments)?;
            json!({"task": service.start(&scope, request).await?})
        }
        "list" => json!({"tasks": service.list_for(&scope)?}),
        "inspect" => {
            let log_bytes = arguments
                .get("log_bytes")
                .and_then(Value::as_u64)
                .map_or(DEFAULT_LOG_TAIL_BYTES, |bytes| {
                    u32::try_from(bytes).unwrap_or(MAX_LOG_TAIL_BYTES)
                });
            json!(
                service
                    .inspect_for(&scope, task_id(arguments)?, log_bytes)
                    .await?
            )
        }
        "stop" => {
            let grace = arguments
                .get("grace_seconds")
                .and_then(Value::as_f64)
                .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
                .map(|seconds| Duration::from_secs_f64(seconds).min(MAX_STOP_GRACE));
            json!({"task": service.stop_for(&scope, task_id(arguments)?, grace).await?})
        }
        "dismiss" => {
            service.dismiss(Some(&scope), task_id(arguments)?).await?;
            json!({"dismissed": task_id(arguments)?})
        }
        other => return Err(format!("unknown background_tasks action {other:?}")),
    };
    serde_json::to_string_pretty(&output).map_err(|error| error.to_string())
}

fn start_request(
    scope: &CallerScope,
    workspace: &Path,
    arguments: &Value,
) -> Result<StartRequest, String> {
    let command = arguments
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| "start requires `command`".to_owned())?
        .to_owned();
    let name = arguments
        .get("name")
        .and_then(Value::as_str)
        .map_or_else(|| default_name(&command), str::to_owned);
    let cwd = match arguments.get("cwd").and_then(Value::as_str) {
        None => workspace.to_path_buf(),
        // Absolute directories are allowed: a shell command could `cd` there anyway.
        Some(path) if Path::new(path).is_absolute() => Path::new(path).to_path_buf(),
        Some(path) => crate::tools::resolve_workspace_path(workspace, path)?,
    };
    let endpoints = arguments
        .get("endpoints")
        .and_then(Value::as_array)
        .map(|endpoints| {
            endpoints
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let environment: HashMap<String, String> = crate::session_environment::read(Some(&scope.owner));
    Ok(StartRequest {
        name,
        command,
        cwd,
        endpoints,
        environment,
    })
}

fn default_name(command: &str) -> String {
    command
        .split_whitespace()
        .take(4)
        .collect::<Vec<_>>()
        .join(" ")
}

fn task_id(arguments: &Value) -> Result<&str, String> {
    arguments
        .get("task_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| "this action requires `task_id`".to_owned())
}

pub(crate) fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["start", "list", "inspect", "stop", "dismiss"]},
            "command": {"type": "string", "description": "start: shell command to run"},
            "name": {"type": "string", "description": "start: short label, such as web dev server"},
            "cwd": {"type": "string", "description": "start: working directory; workspace-relative or absolute"},
            "endpoints": {"type": "array", "items": {"type": "string"}, "maxItems": 8, "description": "start: addresses the task will serve on the execution machine, such as http://127.0.0.1:3000"},
            "task_id": {"type": "string", "description": "inspect/stop/dismiss: exact task id"},
            "log_bytes": {"type": "integer", "minimum": 0, "maximum": MAX_LOG_TAIL_BYTES, "description": "inspect: log tail size; default 8192"},
            "grace_seconds": {"type": "number", "minimum": 0, "maximum": 60, "description": "stop: seconds between SIGTERM and SIGKILL; default 5"}
        },
        "required": ["action"],
        "additionalProperties": false
    })
}
