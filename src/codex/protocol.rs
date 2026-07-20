use std::fmt::Write;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::backend::{
    ApprovalKind, ApprovalRequest, BackendEvent, DeltaKind, ItemKind, ItemStatus, ModelInfo,
    NormalizedItem, SessionHistoryItem, TurnOutcome,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RpcMessage {
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
}

#[must_use]
pub fn request(id: u64, method: &str, params: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

#[must_use]
pub fn notification(method: &str, params: Option<Value>) -> Value {
    let mut value = json!({
        "jsonrpc": "2.0",
        "method": method,
    });
    if let Some(params) = params {
        value["params"] = params;
    }
    value
}

#[must_use]
pub fn response(id: &Value, result: Result<Value, RpcError>) -> Value {
    match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": error.code,
                "message": error.message,
                "data": error.data,
            }
        }),
    }
}

/// Parses one JSON-RPC message from a line of app-server output.
///
/// # Errors
///
/// Returns an error when `line` is not valid JSON for an RPC message.
pub fn parse_message(line: &str) -> Result<RpcMessage, serde_json::Error> {
    serde_json::from_str(line)
}

pub fn normalize_notification(method: &str, params: &Value) -> Option<BackendEvent> {
    match method {
        "thread/started" => Some(BackendEvent::SessionObserved {
            provider_session_id: nested_string(params, &["thread", "id"]),
        }),
        "turn/started" => Some(BackendEvent::TurnStarted {
            turn_id: nested_string(params, &["turn", "id"]),
        }),
        "turn/completed" => {
            let turn = params.get("turn")?;
            let status = string(turn, "status");
            let mut error = turn
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let outcome = match status.as_str() {
                "completed" => TurnOutcome::Completed,
                "interrupted" => TurnOutcome::Interrupted,
                "failed" => TurnOutcome::Failed,
                unknown => {
                    error.get_or_insert_with(|| {
                        format!("turn/completed carried unknown status {unknown:?}")
                    });
                    TurnOutcome::Failed
                }
            };
            Some(BackendEvent::TurnCompleted {
                turn_id: string(turn, "id"),
                outcome,
                error,
            })
        }
        "item/started" | "item/completed" => {
            let turn_id = string(params, "turnId");
            let raw_item = params.get("item")?;
            if is_internal_provider_item(raw_item) {
                return None;
            }
            let item = normalize_item(raw_item);
            if method == "item/started" {
                Some(BackendEvent::ItemStarted { turn_id, item })
            } else {
                Some(BackendEvent::ItemCompleted { turn_id, item })
            }
        }
        "item/agentMessage/delta" => Some(delta_event(params, DeltaKind::Assistant, "delta")),
        "item/plan/delta" => Some(delta_event(params, DeltaKind::Plan, "delta")),
        "item/reasoning/summaryTextDelta" => Some(reasoning_summary_delta(params)),
        "item/reasoning/textDelta" => Some(delta_event(params, DeltaKind::Reasoning, "delta")),
        "item/commandExecution/outputDelta" | "item/fileChange/outputDelta" => {
            Some(delta_event(params, DeltaKind::Tool, "delta"))
        }
        "item/mcpToolCall/progress" => Some(delta_event(params, DeltaKind::Tool, "message")),
        "turn/diff/updated" => Some(BackendEvent::TurnDiff {
            turn_id: string(params, "turnId"),
            diff: string(params, "diff"),
        }),
        "turn/plan/updated" => Some(BackendEvent::TurnPlan {
            turn_id: string(params, "turnId"),
            plan: format_plan(params),
        }),
        "item/fileChange/patchUpdated" => {
            let item_id = string(params, "itemId");
            let item = json!({
                "type": "fileChange",
                "id": item_id,
                "changes": params.get("changes").cloned().unwrap_or_else(|| json!([])),
                "status": "inProgress",
            });
            Some(BackendEvent::ItemStarted {
                turn_id: string(params, "turnId"),
                item: normalize_item(&item),
            })
        }
        "serverRequest/resolved" => Some(BackendEvent::ApprovalResolved {
            request_id: params.get("requestId").cloned().unwrap_or(Value::Null),
        }),
        "error" => Some(BackendEvent::TurnError {
            turn_id: string(params, "turnId"),
            message: nested_string(params, &["error", "message"]),
            will_retry: params
                .get("willRetry")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        "warning" | "guardianWarning" => Some(BackendEvent::Warning(string(params, "message"))),
        "configWarning" | "deprecationNotice" => {
            Some(BackendEvent::Warning(config_warning_text(params)))
        }
        "model/rerouted" => Some(BackendEvent::ModelRerouted {
            turn_id: string(params, "turnId"),
            from: string(params, "fromModel"),
            to: string(params, "toModel"),
        }),
        "thread/closed" => Some(BackendEvent::SessionClosed {
            provider_session_id: string(params, "threadId"),
        }),
        _ => None,
    }
}

pub fn normalize_server_request(id: Value, method: String, params: &Value) -> ApprovalRequest {
    let (kind, title, detail) = match method.as_str() {
        "item/commandExecution/requestApproval" | "execCommandApproval" => {
            let command = command_text(params.get("command"));
            let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("");
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("");
            let mut detail = command;
            if !cwd.is_empty() {
                write!(detail, "\n\nWorking directory: {cwd}")
                    .expect("writing to a String cannot fail");
            }
            if !reason.is_empty() {
                write!(detail, "\n\nReason: {reason}").expect("writing to a String cannot fail");
            }
            (ApprovalKind::Command, "Command approval".to_owned(), detail)
        }
        "item/fileChange/requestApproval" | "applyPatchApproval" => {
            let reason = params
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("Codex wants to modify workspace files.");
            (
                ApprovalKind::FileChange,
                "File change approval".to_owned(),
                reason.to_owned(),
            )
        }
        _ => (
            ApprovalKind::Other,
            "Unsupported server request".to_owned(),
            format!("{method}\n\nThis Nakode build cannot answer this request type."),
        ),
    };

    ApprovalRequest {
        id,
        method,
        kind,
        title,
        detail,
    }
}

fn config_warning_text(params: &Value) -> String {
    let summary = string(params, "summary");
    let details = params
        .get("details")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if details.is_empty() {
        summary
    } else {
        format!("{summary}\n{details}")
    }
}

fn command_text(command: Option<&Value>) -> String {
    match command {
        Some(Value::String(command)) => command.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
        _ => "command".to_owned(),
    }
}

pub fn parse_session_history(result: &Value) -> Vec<SessionHistoryItem> {
    result
        .pointer("/thread/turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            let turn_id = string(turn, "id");
            turn.get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|item| !is_internal_provider_item(item))
                .map(move |item| SessionHistoryItem {
                    turn_id: turn_id.clone(),
                    item: normalize_item(item),
                })
        })
        .collect()
}

fn is_internal_provider_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("subAgentActivity")
}

pub fn parse_models(result: &Value) -> Vec<ModelInfo> {
    result
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let id = model.get("model")?.as_str()?.to_owned();
            Some(ModelInfo {
                provider: crate::backend::CODEX_PROVIDER.to_owned(),
                is_default: model
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                id,
            })
        })
        .collect()
}

#[must_use]
pub fn normalize_item(item: &Value) -> NormalizedItem {
    let item_type = string(item, "type");
    let id = string(item, "id");
    match item_type.as_str() {
        "userMessage" => NormalizedItem {
            id,
            kind: ItemKind::User,
            title: "YOU".to_owned(),
            body: user_message_body(item),
            status: ItemStatus::Complete,
        },
        "agentMessage" => NormalizedItem {
            id,
            kind: ItemKind::Assistant,
            title: "ASSISTANT".to_owned(),
            body: string(item, "text"),
            status: ItemStatus::Complete,
        },
        "reasoning" => NormalizedItem {
            id,
            kind: ItemKind::Reasoning,
            title: "REASONING".to_owned(),
            body: string_array(item, "summary").join("\n"),
            status: ItemStatus::Complete,
        },
        "plan" => NormalizedItem {
            id,
            kind: ItemKind::Reasoning,
            title: "PLAN".to_owned(),
            body: string(item, "text"),
            status: ItemStatus::Complete,
        },
        "commandExecution"
        | "fileChange"
        | "mcpToolCall"
        | "dynamicToolCall"
        | "collabAgentToolCall" => normalize_tool_item(&item_type, id, item),
        "webSearch" => NormalizedItem {
            id,
            kind: ItemKind::Tool,
            title: "WEB SEARCH".to_owned(),
            body: pretty(item),
            status: ItemStatus::Complete,
        },
        "contextCompaction" => NormalizedItem {
            id,
            kind: ItemKind::System,
            title: "CONTEXT COMPACTED".to_owned(),
            body: String::new(),
            status: ItemStatus::Complete,
        },
        _ => NormalizedItem {
            id,
            kind: ItemKind::System,
            title: if item_type.is_empty() {
                "CODEX ITEM".to_owned()
            } else {
                item_type.to_uppercase()
            },
            body: pretty(item),
            status: item_status(item),
        },
    }
}

fn normalize_tool_item(item_type: &str, id: String, item: &Value) -> NormalizedItem {
    match item_type {
        "commandExecution" => {
            let output = item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .unwrap_or_default();
            NormalizedItem {
                id,
                kind: ItemKind::Tool,
                title: format!("$ {}", string(item, "command")),
                body: output.to_owned(),
                status: item_status(item),
            }
        }
        "fileChange" => NormalizedItem {
            id,
            kind: ItemKind::Diff,
            title: "FILE CHANGES".to_owned(),
            body: format_changes(item.get("changes")),
            status: item_status(item),
        },
        "mcpToolCall" => {
            let server = string(item, "server");
            let tool = string(item, "tool");
            NormalizedItem {
                id,
                kind: ItemKind::Tool,
                title: format!("MCP {server}/{tool}"),
                body: pretty_first(item, &["result", "error", "arguments"]),
                status: item_status(item),
            }
        }
        "dynamicToolCall" => {
            let tool = string(item, "tool");
            let namespace = item.get("namespace").and_then(Value::as_str);
            NormalizedItem {
                id,
                kind: ItemKind::Tool,
                title: match namespace {
                    Some(namespace) if !namespace.is_empty() => {
                        format!("TOOL {namespace}/{tool}")
                    }
                    _ => format!("TOOL {tool}"),
                },
                body: dynamic_tool_body(item),
                status: item_status(item),
            }
        }
        "collabAgentToolCall" => NormalizedItem {
            id,
            kind: ItemKind::Tool,
            title: format!("AGENT {}", value_label(item.get("tool"))),
            body: pretty(item),
            status: item_status(item),
        },
        _ => unreachable!("caller filters tool item types"),
    }
}

fn reasoning_summary_delta(params: &Value) -> BackendEvent {
    let index = params
        .get("summaryIndex")
        .and_then(Value::as_u64)
        .and_then(|index| usize::try_from(index).ok())
        .unwrap_or_default();
    delta_event(params, DeltaKind::ReasoningSummary { index }, "delta")
}

fn delta_event(params: &Value, kind: DeltaKind, field: &str) -> BackendEvent {
    BackendEvent::ItemDelta {
        turn_id: string(params, "turnId"),
        item_id: string(params, "itemId"),
        kind,
        delta: string(params, field),
    }
}

fn item_status(item: &Value) -> ItemStatus {
    match item.get("status").and_then(Value::as_str) {
        Some("inProgress") | None => ItemStatus::Running,
        Some("completed") => ItemStatus::Complete,
        Some("declined") => ItemStatus::Declined,
        Some("failed" | _) => ItemStatus::Failed,
    }
}

fn dynamic_tool_body(item: &Value) -> String {
    let text = item
        .get("contentItems")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|content| content.get("type").and_then(Value::as_str) == Some("inputText"))
        .filter_map(|content| content.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        pretty_first(item, &["arguments"])
    } else {
        text
    }
}

fn user_message_body(item: &Value) -> String {
    item.get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|content| content.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|content| content.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_changes(changes: Option<&Value>) -> String {
    changes
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|change| {
            let path = string(change, "path");
            let kind = value_label(change.get("kind"));
            let diff = string(change, "diff");
            if diff.is_empty() {
                format!("{kind}: {path}")
            } else {
                format!("{kind}: {path}\n{diff}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn format_plan(params: &Value) -> String {
    let explanation = params
        .get("explanation")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    let steps = params
        .get("plan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|step| {
            let marker = match step.get("status").and_then(Value::as_str) {
                Some("completed") => "✓",
                Some("inProgress") => "→",
                _ => "·",
            };
            format!("{marker} {}", string(step, "step"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    match explanation {
        Some(explanation) if !steps.is_empty() => format!("{explanation}\n\n{steps}"),
        Some(explanation) => explanation.to_owned(),
        None => steps,
    }
}

fn nested_string(value: &Value, path: &[&str]) -> String {
    let mut current = value;
    for component in path {
        let Some(next) = current.get(component) else {
            return String::new();
        };
        current = next;
    }
    current.as_str().unwrap_or_default().to_owned()
}

fn string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn pretty_first(value: &Value, fields: &[&str]) -> String {
    fields
        .iter()
        .find_map(|field| value.get(field).filter(|candidate| !candidate.is_null()))
        .map(pretty)
        .unwrap_or_default()
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn value_label(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BackendEvent, DeltaKind, ItemKind, ItemStatus, TurnOutcome, normalize_item,
        normalize_notification, parse_models, parse_session_history,
    };

    #[test]
    fn parses_installed_agent_delta_shape() {
        let event = normalize_notification(
            "item/agentMessage/delta",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "item-1",
                "delta": "hello",
            }),
        );

        assert_eq!(
            event,
            Some(BackendEvent::ItemDelta {
                turn_id: "turn-1".to_owned(),
                item_id: "item-1".to_owned(),
                kind: DeltaKind::Assistant,
                delta: "hello".to_owned(),
            })
        );
    }

    #[test]
    fn distinguishes_reasoning_summaries_from_reasoning_traces() {
        let params = json!({
            "turnId": "turn-1",
            "itemId": "reasoning-1",
            "summaryIndex": 3,
            "delta": "Planning the implementation",
        });
        let summary = normalize_notification("item/reasoning/summaryTextDelta", &params);
        let trace = normalize_notification("item/reasoning/textDelta", &params);

        assert!(matches!(
            summary,
            Some(BackendEvent::ItemDelta {
                kind: DeltaKind::ReasoningSummary { index: 3 },
                ..
            })
        ));
        assert!(matches!(
            trace,
            Some(BackendEvent::ItemDelta {
                kind: DeltaKind::Reasoning,
                ..
            })
        ));
    }

    #[test]
    fn config_warning_uses_summary_and_details_shape() {
        let event = normalize_notification(
            "deprecationNotice",
            &json!({
                "summary": "old option",
                "details": "use the replacement",
            }),
        );

        assert_eq!(
            event,
            Some(BackendEvent::Warning(
                "old option\nuse the replacement".to_owned()
            ))
        );
    }

    #[test]
    fn completed_item_is_authoritative() {
        let item = normalize_item(&json!({
            "type": "commandExecution",
            "id": "item-2",
            "command": "cargo test",
            "status": "completed",
            "aggregatedOutput": "ok",
        }));

        assert_eq!(item.kind, ItemKind::Tool);
        assert_eq!(item.status, ItemStatus::Complete);
        assert_eq!(item.body, "ok");
    }

    #[test]
    fn dynamic_tool_item_uses_text_result_instead_of_raw_json() {
        let item = normalize_item(&json!({
            "type": "dynamicToolCall",
            "id": "tool-1",
            "namespace": null,
            "tool": "bash",
            "arguments": {"command": "printf ok"},
            "status": "completed",
            "contentItems": [{"type": "inputText", "text": "ok"}],
            "success": true,
        }));
        assert_eq!(item.title, "TOOL bash");
        assert_eq!(item.body, "ok");
        assert_eq!(item.status, ItemStatus::Complete);
    }

    #[test]
    fn parses_failed_turn() {
        let event = normalize_notification(
            "turn/completed",
            &json!({
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "status": "failed",
                    "error": {"message": "boom"},
                }
            }),
        );

        assert_eq!(
            event,
            Some(BackendEvent::TurnCompleted {
                turn_id: "turn-1".to_owned(),
                outcome: TurnOutcome::Failed,
                error: Some("boom".to_owned()),
            })
        );
    }

    #[test]
    fn unknown_completed_status_is_not_reported_as_success() {
        let event = normalize_notification(
            "turn/completed",
            &json!({
                "threadId": "thread-1",
                "turn": {"id": "turn-1", "status": "futureStatus", "error": null}
            }),
        );

        assert!(matches!(
            event,
            Some(BackendEvent::TurnCompleted {
                outcome: TurnOutcome::Failed,
                error: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn parses_resumed_session_history() {
        let history = parse_session_history(&json!({
            "thread": {
                "turns": [{
                    "id": "turn-1",
                    "items": [
                        {"type": "userMessage", "id": "user-1", "content": [{"type": "text", "text": "hello"}]},
                        {"type": "subAgentActivity", "id": "activity-1", "kind": "started", "agentPath": "/root/explorer"},
                        {"type": "agentMessage", "id": "agent-1", "text": "hi"}
                    ]
                }]
            }
        }));

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].turn_id, "turn-1");
        assert_eq!(history[0].item.kind, ItemKind::User);
        assert_eq!(history[1].item.body, "hi");
    }

    #[test]
    fn internal_subagent_activity_does_not_become_a_transcript_item() {
        let event = normalize_notification(
            "item/started",
            &json!({
                "turnId": "turn-1",
                "item": {
                    "type": "subAgentActivity",
                    "id": "activity-1",
                    "kind": "started",
                    "agentPath": "/root/system_inventory",
                    "agentThreadId": "thread-child"
                }
            }),
        );

        assert_eq!(event, None);
    }

    #[test]
    fn parses_model_catalog() {
        let models = parse_models(&json!({
            "data": [{
                "model": "gpt-test",
                "displayName": "GPT Test",
                "description": "fixture",
                "isDefault": true,
            }]
        }));

        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "gpt-test");
        assert!(models[0].is_default);
    }
}
