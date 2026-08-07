use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult};
use crate::{backend::NativeDelegationRequest, runtime::ToolDefinition};

pub(crate) const NAKODE_AGENT_TOOL_NAME: &str = "nakode_agent";

/// Server-routed native delegation. Unlike provider MCP bridges this never shells out to Nakode:
/// the workspace server remains the sole owner of run creation, policy, persistence, and
/// attribution.
pub struct NakodeAgentTool;

#[derive(Deserialize)]
struct Arguments {
    agent: String,
    task: String,
}

impl Tool for NakodeAgentTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: NAKODE_AGENT_TOOL_NAME,
            description: "Delegate one bounded task to a configured Nakode agent and wait for its attributed terminal result.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "agent": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Configured agent slug from the current Nakode catalogue."
                    },
                    "task": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Concrete bounded task for the delegated agent."
                    }
                },
                "required": ["agent", "task"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments.get("agent").and_then(Value::as_str).map_or_else(
            || "Delegate Nakode agent".to_owned(),
            |agent| format!("Delegate {agent}"),
        )
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::ReadOnly
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let arguments: Arguments = match serde_json::from_value(arguments) {
                Ok(arguments) => arguments,
                Err(error) => return ToolResult::failure(error.to_string()),
            };
            let Some(owner_session_id) = context.session.owner_session_id.clone() else {
                return ToolResult::failure(
                    "native delegation is unavailable because this provider session has no logical Nakode owner",
                );
            };
            let Some(requests) = context.delegation else {
                return ToolResult::failure(
                    "native delegation is unavailable because the workspace server route is closed",
                );
            };
            let (respond, response) = tokio::sync::oneshot::channel();
            let request = NativeDelegationRequest {
                owner_session_id,
                parent_run_id: context.session.parent_run_id.clone(),
                agent: arguments.agent,
                task: arguments.task,
                cancellation: cancellation.clone(),
                respond,
            };
            tokio::select! {
                sent = requests.send(request) => {
                    if sent.is_err() {
                        return ToolResult::failure("native delegation request channel closed");
                    }
                }
                () = cancellation.cancelled() => {
                    return ToolResult::failure("native delegation cancelled before dispatch");
                }
            }
            tokio::select! {
                result = response => match result {
                    Ok(Ok(output)) => ToolResult::success(output),
                    Ok(Err(error)) => ToolResult::failure(error),
                    Err(_) => ToolResult::failure("native delegation ended without a terminal result"),
                },
                () = cancellation.cancelled() => ToolResult::failure(
                    "native delegation cancelled; the workspace server will interrupt the attributed child run",
                ),
            }
        })
    }
}
