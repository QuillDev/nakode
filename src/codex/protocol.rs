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
                    provider_id: None,
                    model_id: None,
                    attachments: Vec::new(),
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
                capabilities: super::model_capabilities(),
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
            tool_audit_json: None,
        },
        "agentMessage" => NormalizedItem {
            id,
            kind: ItemKind::Assistant,
            title: "ASSISTANT".to_owned(),
            body: string(item, "text"),
            status: ItemStatus::Complete,
            tool_audit_json: None,
        },
        "reasoning" => NormalizedItem {
            id,
            kind: ItemKind::Reasoning,
            title: "REASONING".to_owned(),
            body: string_array(item, "summary").join("\n"),
            status: ItemStatus::Complete,
            tool_audit_json: None,
        },
        "plan" => NormalizedItem {
            id,
            kind: ItemKind::Reasoning,
            title: "PLAN".to_owned(),
            body: string(item, "text"),
            status: ItemStatus::Complete,
            tool_audit_json: None,
        },
        "commandExecution"
        | "fileChange"
        | "mcpToolCall"
        | "dynamicToolCall"
        | "collabAgentToolCall"
        | "webSearch" => normalize_tool_item(&item_type, id, item),
        "contextCompaction" => NormalizedItem {
            id,
            kind: ItemKind::System,
            title: "CONTEXT COMPACTED".to_owned(),
            body: String::new(),
            status: ItemStatus::Complete,
            tool_audit_json: None,
        },
        _ if looks_like_tool(item) => normalize_tool_item(&item_type, id, item),
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
            tool_audit_json: None,
        },
    }
}

fn normalize_tool_item(item_type: &str, id: String, item: &Value) -> NormalizedItem {
    let status = item_status(item);
    let audit = tool_audit(item_type, &id, item, status);
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
                status,
                tool_audit_json: Some(audit.into_boxed_str()),
            }
        }
        "fileChange" => NormalizedItem {
            id,
            kind: ItemKind::Diff,
            title: "FILE CHANGES".to_owned(),
            body: format_changes(item.get("changes")),
            status,
            tool_audit_json: Some(audit.into_boxed_str()),
        },
        "mcpToolCall" => {
            let server = string(item, "server");
            let tool = string(item, "tool");
            NormalizedItem {
                id,
                kind: ItemKind::Tool,
                title: format!("MCP {server}/{tool}"),
                body: pretty_first(item, &["result", "error", "arguments"]),
                status,
                tool_audit_json: Some(audit.into_boxed_str()),
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
                status,
                tool_audit_json: Some(audit.into_boxed_str()),
            }
        }
        "collabAgentToolCall" => NormalizedItem {
            id,
            kind: ItemKind::Tool,
            title: format!("AGENT {}", value_label(item.get("tool"))),
            body: pretty(item),
            status,
            tool_audit_json: Some(audit.into_boxed_str()),
        },
        "webSearch" => NormalizedItem {
            id,
            kind: ItemKind::Tool,
            title: "WEB SEARCH".to_owned(),
            body: pretty(item),
            status,
            tool_audit_json: Some(audit.into_boxed_str()),
        },
        _ => NormalizedItem {
            id,
            kind: ItemKind::Tool,
            title: generic_tool_name(item_type, item),
            body: pretty_first(item, &["result", "error", "output"]),
            status,
            tool_audit_json: Some(audit.into_boxed_str()),
        },
    }
}

/// Each field is bounded independently and says when bytes were omitted. The provider's own session
/// history remains authoritative for data past these IPC-safe windows.
const MAX_AUDIT_FIELD_BYTES: usize = 64 * 1024;

fn tool_audit(item_type: &str, id: &str, item: &Value, status: ItemStatus) -> String {
    let kind = match item_type {
        "commandExecution" => "shell",
        "mcpToolCall" => "custom",
        "dynamicToolCall" if item.get("namespace").is_some_and(|value| !value.is_null()) => {
            "custom"
        }
        "fileChange" | "webSearch" | "dynamicToolCall" | "collabAgentToolCall" => "native",
        _ => "unknown",
    };
    let name = match item_type {
        "commandExecution" => "Shell".to_owned(),
        "fileChange" => "Edit".to_owned(),
        "webSearch" => "Search".to_owned(),
        _ => generic_tool_name(item_type, item),
    };
    let status = match status {
        ItemStatus::Running => "running",
        ItemStatus::Complete => "succeeded",
        ItemStatus::Failed => "failed",
        ItemStatus::Declined => "cancelled",
    };

    let mut audit = json!({
        "version": 1,
        "callId": id,
        "kind": kind,
        "name": name,
        "providerType": item_type,
        "status": status,
        "authoritative": "Nakode provider session history",
    });
    if item_type == "commandExecution" {
        let command = item.get("command").cloned().unwrap_or(Value::Null);
        audit["shell"] = json!({
            "command": bounded_payload(&command),
            "cwd": optional_payload(item.get("cwd")),
            "stdout": optional_payload(item.get("stdout")),
            "stderr": optional_payload(item.get("stderr")),
            "output": optional_payload(item.get("aggregatedOutput")),
            "exitCode": first_value(item, &["exitCode", "exit_code"]),
            "durationMs": first_value(item, &["durationMs", "duration_ms"]),
        });
    } else {
        let input = item
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| generic_input(item));
        audit["input"] = bounded_payload(&input);
        if let Some(output) = first_present(item, &["result", "error", "output", "contentItems"]) {
            audit["output"] = bounded_payload(output);
        }
    }
    if item.get("error").is_some_and(|value| !value.is_null()) {
        audit["error"] = bounded_payload(&item["error"]);
    }
    serde_json::to_string(&audit).unwrap_or_else(|_| {
        r#"{"version":1,"kind":"unknown","status":"failed","error":{"format":"text","value":"Nakode could not encode this tool audit.","bytes":41,"truncated":false,"redacted":false}}"#.to_owned()
    })
}

fn looks_like_tool(item: &Value) -> bool {
    let item_type = string(item, "type").to_ascii_lowercase();
    item_type.contains("tool")
        || item_type.contains("call")
        || item.get("arguments").is_some()
        || (item.get("tool").is_some() && item.get("status").is_some())
}

fn generic_tool_name(item_type: &str, item: &Value) -> String {
    let name = ["tool", "name"]
        .into_iter()
        .find_map(|field| item.get(field).and_then(Value::as_str))
        .unwrap_or(item_type);
    if name.is_empty() {
        "Unknown tool".to_owned()
    } else {
        name.to_owned()
    }
}

fn generic_input(item: &Value) -> Value {
    let mut input = item.as_object().cloned().unwrap_or_default();
    for field in [
        "id",
        "type",
        "status",
        "result",
        "error",
        "output",
        "contentItems",
        "aggregatedOutput",
        "stdout",
        "stderr",
        "exitCode",
        "durationMs",
    ] {
        input.remove(field);
    }
    Value::Object(input)
}

fn first_present<'a>(item: &'a Value, fields: &[&str]) -> Option<&'a Value> {
    fields
        .iter()
        .find_map(|field| item.get(field).filter(|value| !value.is_null()))
}

fn first_value(item: &Value, fields: &[&str]) -> Value {
    first_present(item, fields).cloned().unwrap_or(Value::Null)
}

fn optional_payload(value: Option<&Value>) -> Value {
    value
        .filter(|value| !value.is_null())
        .map_or(Value::Null, bounded_payload)
}

fn bounded_payload(value: &Value) -> Value {
    let (format, rendered) = match value {
        Value::String(value) => ("text", value.clone()),
        value => ("json", pretty(value)),
    };
    let bytes = rendered.len();
    let (rendered, truncated) = bounded_utf8(rendered, MAX_AUDIT_FIELD_BYTES);
    json!({
        "format": format,
        "value": rendered,
        "bytes": bytes,
        "truncated": truncated,
        "redacted": contains_redaction(value),
    })
}

fn bounded_utf8(mut value: String, maximum: usize) -> (String, bool) {
    if value.len() <= maximum {
        return (value, false);
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    (value, true)
}

fn contains_redaction(value: &Value) -> bool {
    match value {
        Value::String(value) => {
            let lower = value.to_ascii_lowercase();
            lower.contains("[redacted]") || lower.contains("<redacted>")
        }
        Value::Array(values) => values.iter().any(contains_redaction),
        Value::Object(values) => {
            values
                .get("redacted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || values.values().any(contains_redaction)
        }
        _ => false,
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
        let audit: serde_json::Value =
            serde_json::from_str(item.tool_audit_json.as_deref().expect("shell audit"))
                .expect("valid audit json");
        assert_eq!(audit["callId"], "item-2");
        assert_eq!(audit["kind"], "shell");
        assert_eq!(audit["shell"]["command"]["value"], "cargo test");
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
        let audit: serde_json::Value =
            serde_json::from_str(item.tool_audit_json.as_deref().expect("tool audit"))
                .expect("valid audit json");
        assert_eq!(audit["input"]["format"], "json");
        assert!(
            audit["input"]["value"]
                .as_str()
                .unwrap()
                .contains("command")
        );
    }

    #[test]
    fn unknown_future_tool_uses_generic_audit_with_explicit_limits() {
        let item = normalize_item(&json!({
            "type": "futureWidgetToolCall",
            "id": "future-1",
            "name": "InspectFuture",
            "arguments": {
                "hostile": "<script>never execute</script>",
                "secret": "[REDACTED]",
                "large": "x".repeat(70_000)
            },
            "status": "failed",
            "error": {"message": "nope"}
        }));
        assert_eq!(item.kind, ItemKind::Tool);
        let audit: serde_json::Value =
            serde_json::from_str(item.tool_audit_json.as_deref().expect("generic audit"))
                .expect("valid audit json");
        assert_eq!(audit["kind"], "unknown");
        assert_eq!(audit["name"], "InspectFuture");
        assert_eq!(audit["input"]["truncated"], true);
        assert_eq!(audit["input"]["redacted"], true);
        assert_eq!(audit["error"]["format"], "json");
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
