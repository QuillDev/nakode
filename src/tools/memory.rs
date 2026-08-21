use std::sync::Arc;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult};
use crate::{
    memory::{MemoryScope, SharedMemoryService},
    runtime::ToolDefinition,
};

pub struct MemorySearchTool {
    service: SharedMemoryService,
}

impl MemorySearchTool {
    #[must_use]
    pub fn new(service: SharedMemoryService) -> Self {
        Self { service }
    }
}

impl Tool for MemorySearchTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "memory_search",
            description: "Search durable project and global user memory for relevant facts, decisions, preferences, and prior context. Searches both scopes by default; search before storing to avoid duplicates.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural-language memory query."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5, "description": "Maximum results returned from each searched scope."},
                    "scope": {
                        "type": "string",
                        "enum": ["all", "project", "global"],
                        "default": "all",
                        "description": "Memory scopes to search. Defaults to both project and global memory."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments.get("query").and_then(Value::as_str).map_or_else(
            || "Search memory".into(),
            |query| format!("Search memory for {query}"),
        )
    }

    fn available(&self) -> bool {
        self.service.available()
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::ReadOnly
    }

    fn execute<'a>(
        &'a self,
        _context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let backend_arguments = json!({
                "query": arguments.get("query").cloned().unwrap_or(Value::Null),
                "limit": arguments.get("limit").cloned().unwrap_or(json!(5))
            });
            if backend_arguments["query"]
                .as_str()
                .is_some_and(str::is_empty)
            {
                return ToolResult::failure("memory query must not be empty");
            }
            match arguments
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("all")
            {
                "project" => {
                    single_search(
                        &self.service,
                        MemoryScope::Project,
                        backend_arguments,
                        cancellation,
                    )
                    .await
                }
                "global" => {
                    single_search(
                        &self.service,
                        MemoryScope::Global,
                        backend_arguments,
                        cancellation,
                    )
                    .await
                }
                _ => {
                    let (project, global) = tokio::join!(
                        self.service.call(
                            MemoryScope::Project,
                            "mnemosyne_recall",
                            backend_arguments.clone(),
                            cancellation,
                        ),
                        self.service.call(
                            MemoryScope::Global,
                            "mnemosyne_recall",
                            backend_arguments,
                            cancellation,
                        ),
                    );
                    match (project, global) {
                        (Ok(project), Ok(global)) => ToolResult::success(
                            json!({
                                "scope": "all",
                                "project": normalize_backend_output(&project, MemoryScope::Project),
                                "global": normalize_backend_output(&global, MemoryScope::Global)
                            })
                            .to_string(),
                        ),
                        (Err(project), Err(global)) => ToolResult::failure(format!(
                            "project memory search failed: {project}; global memory search failed: {global}"
                        )),
                        (Err(error), Ok(_)) => {
                            ToolResult::failure(format!("project memory search failed: {error}"))
                        }
                        (Ok(_), Err(error)) => {
                            ToolResult::failure(format!("global memory search failed: {error}"))
                        }
                    }
                }
            }
        })
    }
}

pub struct MemoryStoreTool {
    service: SharedMemoryService,
}

impl MemoryStoreTool {
    #[must_use]
    pub fn new(service: SharedMemoryService) -> Self {
        Self { service }
    }
}

impl Tool for MemoryStoreTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "memory_store",
            description: "Explicitly store a durable memory in either project or global user scope. Store stable facts, decisions, and preferences only; do not store routine logs or secrets, and search first for duplicates.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "Self-contained durable memory content."},
                    "scope": {
                        "type": "string",
                        "enum": ["project", "global"],
                        "description": "Required destination. Use project for workspace knowledge and global only for durable user-wide preferences."
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["fact", "decision", "preference", "insight", "identity", "task"],
                        "default": "fact",
                        "description": "Provider-neutral memory kind."
                    },
                    "importance": {"type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.5},
                    "confidence": {
                        "type": "string",
                        "enum": ["stated", "inferred", "tool", "unknown"],
                        "default": "unknown"
                    },
                    "sensitivity": {
                        "type": "string",
                        "enum": ["public", "internal", "sensitive"],
                        "default": "internal",
                        "description": "Classification for downstream policy and review; never store credentials or secrets."
                    },
                    "valid_until": {"type": "string", "description": "Optional expiry date in YYYY-MM-DD format."}
                },
                "required": ["content", "scope"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("content")
            .and_then(Value::as_str)
            .map_or_else(
                || "Store memory".into(),
                |content| {
                    let summary = content.chars().take(80).collect::<String>();
                    format!("Store memory: {summary}")
                },
            )
    }

    fn available(&self) -> bool {
        self.service.available()
    }

    fn execute<'a>(
        &'a self,
        tool_context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let content = arguments
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if content.trim().is_empty() {
                return ToolResult::failure("memory content must not be empty");
            }
            let scope = match arguments.get("scope").and_then(Value::as_str) {
                Some("project") => MemoryScope::Project,
                Some("global") => MemoryScope::Global,
                _ => return ToolResult::failure("memory scope must be project or global"),
            };
            let kind = arguments
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("fact");
            let sensitivity = arguments
                .get("sensitivity")
                .and_then(Value::as_str)
                .unwrap_or("internal");
            let mut backend_arguments = json!({
                "content": content,
                "source": kind,
                "scope": "global",
                "importance": arguments.get("importance").cloned().unwrap_or(json!(0.5)),
                "veracity": arguments.get("confidence").cloned().unwrap_or(json!("unknown")),
                "author_id": tool_context.session.model.as_str(),
                "author_type": "agent",
                "metadata": {
                    "nakode_scope": scope.label(),
                    "nakode_kind": kind,
                    "nakode_sensitivity": sensitivity,
                    "nakode_session_id": tool_context.session.id.as_str(),
                    "nakode_turn_id": tool_context.turn_id,
                    "nakode_model": tool_context.session.model.as_str()
                }
            });
            if let Some(valid_until) = arguments.get("valid_until") {
                backend_arguments["valid_until"] = valid_until.clone();
            }
            match self
                .service
                .call(scope, "mnemosyne_remember", backend_arguments, cancellation)
                .await
            {
                Ok(output) => {
                    ToolResult::success(normalize_backend_output(&output, scope).to_string())
                }
                Err(error) => ToolResult::failure(error.to_string()),
            }
        })
    }
}

async fn single_search(
    service: &SharedMemoryService,
    scope: MemoryScope,
    arguments: Value,
    cancellation: &CancellationToken,
) -> ToolResult {
    match service
        .call(scope, "mnemosyne_recall", arguments, cancellation)
        .await
    {
        Ok(output) => ToolResult::success(
            json!({
                "scope": scope.label(),
                "memory": normalize_backend_output(&output, scope)
            })
            .to_string(),
        ),
        Err(error) => ToolResult::failure(error.to_string()),
    }
}

fn normalize_backend_output(output: &str, scope: MemoryScope) -> Value {
    let mut value =
        serde_json::from_str(output).unwrap_or_else(|_| Value::String(output.to_owned()));
    if let Some(object) = value.as_object_mut() {
        object.remove("bank");
        object.insert("scope".to_owned(), Value::String(scope.label().to_owned()));
    }
    value
}

#[must_use]
pub fn tools(service: SharedMemoryService) -> [Arc<dyn Tool>; 2] {
    [
        Arc::new(MemorySearchTool::new(Arc::clone(&service))),
        Arc::new(MemoryStoreTool::new(service)),
    ]
}

#[cfg(test)]
mod tests {
    use super::{MemoryScope, normalize_backend_output};

    #[test]
    fn backend_bank_names_are_replaced_with_provider_neutral_scopes() {
        let output = normalize_backend_output(
            r#"{"status":"ok","bank":"nakode-project-secret","results":[]}"#,
            MemoryScope::Project,
        );
        assert!(output.get("bank").is_none());
        assert_eq!(output["scope"], "project");
    }
}
