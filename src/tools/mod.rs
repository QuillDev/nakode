mod ask;
mod bash;
mod browser;
mod edit;
mod eval;
mod find;
mod grep;
mod hypa;
mod ls;
mod memory;
mod nakode_agent;
pub(crate) use nakode_agent::NAKODE_AGENT_TOOL_NAME;
mod process;
mod read;
mod read_skill;
mod todo;
mod truncate;
mod vision;
mod write;

use std::{future::Future, path::Path, pin::Pin, sync::Arc};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    backend::{BackendEvent, NativeDelegationRequest},
    runtime::{QuestionBroker, RuntimeSession, ToolDefinition},
};

pub const MAX_TOOL_OUTPUT_BYTES: usize = 128 * 1024;
pub const MAX_MODEL_TOOL_OUTPUT_BYTES: usize = truncate::DEFAULT_MAX_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolConcurrency {
    ReadOnly,
    Exclusive,
}

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = ToolResult> + Send + 'a>>;

pub struct ToolContext<'a> {
    pub workspace: &'a Path,
    pub session: &'a mut RuntimeSession,
    pub backend_events: &'a mpsc::Sender<BackendEvent>,
    pub turn_id: &'a str,
    pub questions: &'a QuestionBroker,
    pub delegation: Option<&'a mpsc::Sender<NativeDelegationRequest>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolResult {
    pub output: String,
    pub failed: bool,
    /// Server-internal stable identity for successful capability invocation telemetry.
    pub invocation_identity: Option<String>,
}

impl ToolResult {
    #[must_use]
    pub fn success(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            failed: false,
            invocation_identity: None,
        }
    }

    #[must_use]
    pub fn failure(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            failed: true,
            invocation_identity: None,
        }
    }

    #[must_use]
    pub fn with_invocation_identity(mut self, identity: impl Into<String>) -> Self {
        self.invocation_identity = Some(identity.into());
        self
    }
}

pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    fn summarize(&self, arguments: &Value) -> String;
    fn prepare_arguments(&self, arguments: Value) -> Value {
        arguments
    }
    fn available(&self) -> bool {
        true
    }
    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::Exclusive
    }
    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a>;
}

#[derive(Clone)]
pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    #[must_use]
    pub fn base() -> Self {
        Self {
            tools: vec![
                Arc::new(read::ReadTool),
                Arc::new(read_skill::ReadSkillTool),
                Arc::new(write::WriteTool),
                Arc::new(edit::EditTool),
                Arc::new(bash::BashTool),
                Arc::new(grep::GrepTool),
                Arc::new(find::FindTool),
                Arc::new(ls::LsTool),
                Arc::new(eval::EvalTool::default()),
                Arc::new(ask::AskTool),
                Arc::new(todo::TodoTool),
            ],
        }
    }

    #[must_use]
    pub fn with_browser(mut self, config: Arc<std::sync::RwLock<crate::web::WebConfig>>) -> Self {
        self.tools.push(Arc::new(browser::BrowserTool::new(config)));
        self
    }

    #[must_use]
    pub fn with_vision(
        mut self,
        config: Arc<std::sync::RwLock<crate::vision::VisionConfig>>,
        service: Option<crate::vision::SharedVisionService>,
    ) -> Self {
        self.tools
            .push(Arc::new(vision::VisionTool::new(config, service)));
        self
    }

    #[must_use]
    pub fn with_memory(mut self, service: crate::memory::SharedMemoryService) -> Self {
        self.tools.extend(memory::tools(service));
        self
    }

    #[must_use]
    pub fn with_native_delegation(mut self) -> Self {
        self.tools.push(Arc::new(nakode_agent::NakodeAgentTool));
        self
    }

    #[must_use]
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .filter(|tool| tool.available())
            .map(|tool| tool.definition())
            .collect()
    }

    #[must_use]
    pub fn find(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools
            .iter()
            .find(|tool| tool.definition().name == name)
    }

    #[cfg(test)]
    pub(crate) fn testing(tools: Vec<Arc<dyn Tool>>) -> Self {
        Self { tools }
    }
}

pub(crate) fn prepare_and_validate(tool: &dyn Tool, arguments: Value) -> Result<Value, String> {
    let mut arguments = tool.prepare_arguments(arguments);
    let definition = tool.definition();
    coerce_schema(&definition.parameters, &mut arguments);
    validate_schema(&definition.parameters, &arguments, "arguments")
        .map_err(|error| format!("invalid {} arguments: {error}", definition.name))?;
    Ok(arguments)
}

fn coerce_schema(schema: &Value, value: &mut Value) {
    if let Value::String(string) = value {
        if schema_accepts(schema, "integer")
            && let Ok(integer) = string.parse::<i64>()
        {
            *value = Value::from(integer);
        } else if schema_accepts(schema, "number")
            && let Ok(number) = string.parse::<f64>()
            && let Some(number) = serde_json::Number::from_f64(number)
        {
            *value = Value::Number(number);
        } else if schema_accepts(schema, "boolean")
            && let Ok(boolean) = string.parse::<bool>()
        {
            *value = Value::Bool(boolean);
        }
    }
    match value {
        Value::Object(object) => {
            let properties = schema.get("properties").and_then(Value::as_object);
            for (name, value) in object {
                let child_schema = properties
                    .and_then(|properties| properties.get(name))
                    .or_else(|| {
                        schema
                            .get("additionalProperties")
                            .filter(|schema| schema.is_object())
                    });
                if let Some(child_schema) = child_schema {
                    coerce_schema(child_schema, value);
                }
            }
        }
        Value::Array(items) => {
            if let Some(item_schema) = schema.get("items") {
                for item in items {
                    coerce_schema(item_schema, item);
                }
            }
        }
        _ => {}
    }
}

fn schema_accepts(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(kind)) => kind == expected,
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind.as_str() == Some(expected)),
        _ => false,
    }
}

fn validate_schema(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(format!(
            "{path} must be one of {}",
            Value::Array(allowed.clone())
        ));
    }
    if let Some(types) = schema.get("type") {
        let matches = match types {
            Value::String(kind) => matches_type(kind, value),
            Value::Array(kinds) => kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| matches_type(kind, value)),
            _ => true,
        };
        if !matches {
            return Err(format!("{path} has the wrong type"));
        }
    }
    match value {
        Value::Object(object) => validate_object(schema, object, path)?,
        Value::Array(items) => validate_array(schema, items, path)?,
        Value::String(string) => validate_string(schema, string, path)?,
        Value::Number(number) => validate_number(schema, number, path)?,
        _ => {}
    }
    Ok(())
}

fn validate_string(schema: &Value, string: &str, path: &str) -> Result<(), String> {
    if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64)
        && string.chars().count() < usize::try_from(minimum).unwrap_or(usize::MAX)
    {
        return Err(format!(
            "{path} must contain at least {minimum} character(s)"
        ));
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64)
        && string.chars().count() > usize::try_from(maximum).unwrap_or(usize::MAX)
    {
        return Err(format!(
            "{path} must contain at most {maximum} character(s)"
        ));
    }
    Ok(())
}

fn matches_type(kind: &str, value: &Value) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

fn validate_object(
    schema: &Value,
    object: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    let properties = schema.get("properties").and_then(Value::as_object);
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!("{path}.{name} is required"));
            }
        }
    }
    for (name, value) in object {
        if let Some(property) = properties.and_then(|properties| properties.get(name)) {
            validate_schema(property, value, &format!("{path}.{name}"))?;
            continue;
        }
        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => {
                return Err(format!("{path}.{name} is not allowed"));
            }
            Some(additional @ Value::Object(_)) => {
                validate_schema(additional, value, &format!("{path}.{name}"))?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_array(schema: &Value, items: &[Value], path: &str) -> Result<(), String> {
    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64)
        && items.len() < usize::try_from(minimum).unwrap_or(usize::MAX)
    {
        return Err(format!("{path} must contain at least {minimum} item(s)"));
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64)
        && items.len() > usize::try_from(maximum).unwrap_or(usize::MAX)
    {
        return Err(format!("{path} must contain at most {maximum} item(s)"));
    }
    if let Some(item_schema) = schema.get("items") {
        for (index, value) in items.iter().enumerate() {
            validate_schema(item_schema, value, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_number(schema: &Value, number: &serde_json::Number, path: &str) -> Result<(), String> {
    let Some(number) = number.as_f64() else {
        return Err(format!("{path} is not a finite number"));
    };
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
        && number < minimum
    {
        return Err(format!("{path} must be at least {minimum}"));
    }
    if let Some(minimum) = schema.get("exclusiveMinimum").and_then(Value::as_f64)
        && number <= minimum
    {
        return Err(format!("{path} must be greater than {minimum}"));
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
        && number > maximum
    {
        return Err(format!("{path} must be at most {maximum}"));
    }
    Ok(())
}

/// Reads a required, non-empty string argument.
///
/// # Errors
///
/// Returns an error when the argument is absent, empty, or not a string.
pub fn required_string<'a>(arguments: &'a Value, name: &str) -> Result<&'a str, String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing non-empty string argument {name}"))
}

/// Reads an optional unsigned integer argument.
///
/// # Errors
///
/// Returns an error when the supplied value is not a non-negative integer.
pub fn optional_u64(arguments: &Value, name: &str, default: u64) -> Result<u64, String> {
    match arguments.get(name) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| format!("argument {name} must be a non-negative integer")),
    }
}

/// Resolves a local path using the workspace as the base for relative input.
///
/// # Errors
///
/// Returns an error when the path is empty, absolute, escapes through `..`, or resolves through
/// a symlink outside the workspace.
pub fn resolve_workspace_path(
    workspace: &Path,
    supplied: &str,
) -> Result<std::path::PathBuf, String> {
    if supplied.is_empty() {
        return Err("tool path must not be empty".to_owned());
    }
    let supplied = Path::new(supplied);
    if supplied.is_absolute() {
        return Err("tool paths must be relative to the workspace".to_owned());
    }
    let mut relative = std::path::PathBuf::new();
    for component in supplied.components() {
        match component {
            std::path::Component::Normal(component) => relative.push(component),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !relative.pop() {
                    return Err("tool path escapes the workspace through ..".to_owned());
                }
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err("tool paths must be relative to the workspace".to_owned());
            }
        }
    }
    let candidate = workspace.join(relative);
    ensure_existing_ancestor_is_confined(workspace, &candidate)?;
    Ok(candidate)
}

fn ensure_existing_ancestor_is_confined(workspace: &Path, candidate: &Path) -> Result<(), String> {
    let canonical_workspace = workspace.canonicalize().map_err(|error| {
        format!(
            "failed to resolve workspace {}: {error}",
            workspace.display()
        )
    })?;
    let mut ancestor = candidate;
    while std::fs::symlink_metadata(ancestor).is_err() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| "tool path has no existing ancestor".to_owned())?;
    }
    let canonical_ancestor = ancestor.canonicalize().map_err(|error| {
        format!(
            "failed to resolve path ancestor {}: {error}",
            ancestor.display()
        )
    })?;
    if !canonical_ancestor.starts_with(&canonical_workspace) {
        return Err(format!(
            "tool path resolves outside workspace {}",
            canonical_workspace.display()
        ));
    }
    Ok(())
}

#[must_use]
pub fn truncate_output(mut bytes: Vec<u8>) -> String {
    let truncated = bytes.len() > MAX_TOOL_OUTPUT_BYTES;
    bytes.truncate(MAX_TOOL_OUTPUT_BYTES);
    let mut output = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        output.push_str("\n[output truncated]");
    }
    output
}

#[must_use]
pub fn model_facing_output(output: &str) -> String {
    if output.len() <= MAX_MODEL_TOOL_OUTPUT_BYTES {
        return output.to_owned();
    }
    let notice = format!(
        "\n[model context truncated; full {}-byte output remains in the transcript]\n",
        output.len()
    );
    let content_budget = MAX_MODEL_TOOL_OUTPUT_BYTES.saturating_sub(notice.len());
    let tail_bytes = content_budget / 4;
    let head_end = floor_char_boundary(output, content_budget - tail_bytes);
    let tail_start = ceil_char_boundary(output, output.len() - tail_bytes);
    format!("{}{}{}", &output[..head_end], notice, &output[tail_start..])
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use serde_json::{Value, json};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{
        MAX_MODEL_TOOL_OUTPUT_BYTES, ToolContext, ToolRegistry, ToolResult, model_facing_output,
        prepare_and_validate, resolve_workspace_path,
    };
    use crate::{
        runtime::{QuestionBroker, RuntimeSession},
        web::{WebBackend, WebConfig},
    };

    #[test]
    fn browser_tool_tracks_optional_backend_enablement() {
        let config = Arc::new(RwLock::new(WebConfig::default()));
        let registry = ToolRegistry::base().with_browser(Arc::clone(&config));
        assert!(registry.find("browser").is_some());
        assert!(
            registry
                .definitions()
                .iter()
                .all(|tool| tool.name != "browser")
        );

        config.write().expect("web config").backend = WebBackend::AgentBrowser;
        assert!(
            registry
                .definitions()
                .iter()
                .any(|tool| tool.name == "browser")
        );
    }

    #[test]
    fn base_registry_contains_only_the_requested_tools() {
        let names = ToolRegistry::base()
            .definitions()
            .into_iter()
            .map(|definition| definition.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "read",
                "read_skill",
                "write",
                "edit",
                "bash",
                "grep",
                "find",
                "ls",
                "eval",
                "ask",
                "todo"
            ]
        );
    }

    #[test]
    fn base_tool_schemas_use_simple_typed_agent_facing_contracts() {
        let definitions = ToolRegistry::base().definitions();
        let properties = |name: &str| {
            definitions
                .iter()
                .find(|definition| definition.name == name)
                .and_then(|definition| definition.parameters["properties"].as_object())
                .map(|properties| properties.keys().map(String::as_str).collect::<Vec<_>>())
                .expect("tool properties")
        };

        assert_eq!(properties("read"), ["limit", "offset", "path"]);
        assert_eq!(properties("read_skill"), ["name"]);
        assert_eq!(properties("write"), ["content", "path"]);
        assert_eq!(properties("edit"), ["edits", "path"]);
        assert_eq!(
            properties("bash"),
            ["command", "cwd", "env", "pty", "timeout"]
        );
        assert_eq!(
            properties("grep"),
            [
                "context",
                "glob",
                "ignoreCase",
                "limit",
                "literal",
                "path",
                "pattern"
            ]
        );
        assert_eq!(properties("find"), ["limit", "path", "pattern"]);
        assert_eq!(properties("ls"), ["limit", "path"]);
        assert_eq!(
            properties("eval"),
            ["code", "language", "reset", "timeout", "title"]
        );
        assert_eq!(properties("ask"), ["questions"]);
        assert_eq!(properties("todo"), ["items", "list", "op", "phase", "task"]);
    }

    #[test]
    fn tool_arguments_are_prepared_coerced_and_validated_centrally() {
        let registry = ToolRegistry::base();
        let find = registry.find("find").expect("find tool");
        let prepared =
            prepare_and_validate(find.as_ref(), json!({"pattern": "*.rs", "limit": "3"}))
                .expect("coerced arguments");
        assert_eq!(prepared["limit"], 3);

        let error = prepare_and_validate(
            find.as_ref(),
            json!({"pattern": "*.rs", "unexpected": true}),
        )
        .expect_err("unknown argument");
        assert!(error.contains("arguments.unexpected is not allowed"));
    }

    #[test]
    fn tool_paths_are_confined_to_the_workspace() {
        let directory = tempfile::tempdir().expect("workspace");
        let root = directory.path();
        assert_eq!(
            resolve_workspace_path(root, "src/main.rs").expect("relative path"),
            root.join("src/main.rs")
        );
        assert!(resolve_workspace_path(root, "../secret").is_err());
        assert!(resolve_workspace_path(root, "/etc/passwd").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn tool_paths_reject_symlinks_that_leave_the_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        let outside = tempfile::tempdir().expect("outside");
        std::os::unix::fs::symlink(outside.path(), workspace.path().join("escape"))
            .expect("escape symlink");

        let error = resolve_workspace_path(workspace.path(), "escape/secret.txt")
            .expect_err("symlink escape must fail");
        assert!(error.contains("outside workspace"));
    }

    #[test]
    fn model_output_keeps_bounded_head_and_tail_while_the_transcript_stays_full() {
        let output = format!("HEAD{}TAIL", "x".repeat(MAX_MODEL_TOOL_OUTPUT_BYTES * 2));
        let model_output = model_facing_output(&output);

        assert!(model_output.len() <= MAX_MODEL_TOOL_OUTPUT_BYTES);
        assert!(model_output.len() < output.len());
        assert!(model_output.starts_with("HEAD"));
        assert!(model_output.ends_with("TAIL"));
        assert!(model_output.contains(&format!(
            "full {}-byte output remains in the transcript",
            output.len()
        )));
    }

    #[tokio::test]
    async fn installed_skills_load_by_catalogue_name_through_the_registry() {
        let directory = tempfile::tempdir().expect("workspace");
        let skill = directory.path().join(".agents/skills/review");
        std::fs::create_dir_all(&skill).expect("skill directory");
        std::fs::write(
            skill.join("SKILL.md"),
            "---\nid: fragile.review.v1\nname: review\ndescription: Review carefully\n---\n\nFULL REVIEW PROCEDURE\n",
        )
        .expect("skill definition");
        let mut harness = ToolHarness {
            registry: ToolRegistry::base(),
            workspace: directory.path(),
            session: RuntimeSession::new("test-model".to_owned(), String::new()),
            events: mpsc::channel(8).0,
            questions: QuestionBroker::default(),
            cancellation: CancellationToken::new(),
        };

        let result = harness
            .execute("read_skill", json!({"name": "review"}))
            .await;
        assert!(!result.failed, "{}", result.output);
        assert!(result.output.contains("FULL REVIEW PROCEDURE"));
        assert_eq!(
            result.invocation_identity.as_deref(),
            Some("fragile.review.v1")
        );

        let missing = harness
            .execute("read_skill", json!({"name": "missing"}))
            .await;
        assert!(missing.failed);
        assert!(missing.output.contains("not installed"));
        assert!(missing.invocation_identity.is_none());
    }

    #[tokio::test]
    async fn file_search_and_todo_tools_execute_through_the_registry() {
        let directory = tempfile::tempdir().expect("workspace");
        let (events, mut event_receiver) = mpsc::channel(8);
        let mut harness = ToolHarness {
            registry: ToolRegistry::base(),
            workspace: directory.path(),
            session: RuntimeSession::new("test-model".to_owned(), String::new()),
            events,
            questions: QuestionBroker::default(),
            cancellation: CancellationToken::new(),
        };

        let write = harness
            .execute(
                "write",
                json!({"path": "nested/file.txt", "content": "before\nsearch me\n"}),
            )
            .await;
        assert!(!write.failed, "{}", write.output);

        let edit = harness
            .execute(
                "edit",
                json!({"path": "nested/file.txt", "edits": [{"old_text": "before", "new_text": "after"}]}),
            )
            .await;
        assert!(!edit.failed, "{}", edit.output);

        for (name, arguments, expected) in [
            ("read", json!({"path": "nested/file.txt"}), "after"),
            ("find", json!({"pattern": "**/*.txt"}), "nested/file.txt"),
            (
                "grep",
                json!({"pattern": "search me"}),
                "nested/file.txt:2: search me",
            ),
        ] {
            let result = harness.execute(name, arguments).await;
            assert!(!result.failed, "{}", result.output);
            assert!(result.output.contains(expected), "{}", result.output);
        }

        let todo = harness
            .execute("todo", json!({"op": "init", "items": ["verify tools"]}))
            .await;
        assert!(!todo.failed, "{}", todo.output);
        assert_eq!(harness.session.todos.len(), 1);
        assert!(matches!(
            event_receiver.try_recv(),
            Ok(crate::backend::BackendEvent::TodoUpdated { phases }) if phases == harness.session.todos
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bash_honors_cwd_environment_and_pty_contracts() {
        let directory = tempfile::tempdir().expect("workspace");
        std::fs::create_dir(directory.path().join("nested")).expect("nested directory");
        let mut harness = ToolHarness {
            registry: ToolRegistry::base(),
            workspace: directory.path(),
            session: RuntimeSession::new("test-model".to_owned(), String::new()),
            events: mpsc::channel(8).0,
            questions: QuestionBroker::default(),
            cancellation: CancellationToken::new(),
        };

        let piped = harness
            .execute(
                "bash",
                json!({
                    "command": "printf '%s:%s' \"$NAKODE_VALUE\" \"$(basename \"$PWD\")\"",
                    "cwd": "nested",
                    "env": {"NAKODE_VALUE": "works"},
                    "timeout": 5
                }),
            )
            .await;
        assert_eq!(piped.output, "works:nested");

        let pty = harness
            .execute(
                "bash",
                json!({"command": "test -t 1 && printf pty-ok", "pty": true, "timeout": 5}),
            )
            .await;
        assert!(!pty.failed, "{}", pty.output);
        assert!(pty.output.contains("pty-ok"), "{}", pty.output);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bash_automatically_uses_hypa_when_it_is_on_path() {
        let directory = tempfile::tempdir().expect("workspace");
        let path = install_fake_hypa(
            directory.path(),
            "#!/bin/sh\nprintf '%s' '{\"outcome\":\"GenericWrapper\",\"command\":\"printf hypa-rewritten\"}'\n",
        );

        let mut harness = ToolHarness {
            registry: ToolRegistry::base(),
            workspace: directory.path(),
            session: RuntimeSession::new("test-model".to_owned(), String::new()),
            events: mpsc::channel(8).0,
            questions: QuestionBroker::default(),
            cancellation: CancellationToken::new(),
        };
        let result = harness
            .execute(
                "bash",
                json!({
                    "command": "printf original",
                    "env": {"PATH": path},
                    "timeout": 5
                }),
            )
            .await;

        assert!(!result.failed, "{}", result.output);
        assert_eq!(result.output, "hypa-rewritten");

        let pty = harness
            .execute(
                "bash",
                json!({
                    "command": "printf pty-original",
                    "env": {"PATH": path},
                    "pty": true,
                    "timeout": 5
                }),
            )
            .await;
        assert!(!pty.failed, "{}", pty.output);
        assert!(pty.output.contains("pty-original"), "{}", pty.output);
        assert!(!pty.output.contains("hypa-rewritten"), "{}", pty.output);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bash_falls_back_when_hypa_rewrite_fails() {
        let directory = tempfile::tempdir().expect("workspace");
        let path = install_fake_hypa(directory.path(), "#!/bin/sh\nexit 9\n");
        let mut harness = ToolHarness {
            registry: ToolRegistry::base(),
            workspace: directory.path(),
            session: RuntimeSession::new("test-model".to_owned(), String::new()),
            events: mpsc::channel(8).0,
            questions: QuestionBroker::default(),
            cancellation: CancellationToken::new(),
        };
        let result = harness
            .execute(
                "bash",
                json!({
                    "command": "printf original",
                    "env": {"PATH": path},
                    "timeout": 5
                }),
            )
            .await;

        assert!(!result.failed, "{}", result.output);
        assert_eq!(result.output, "original");
    }

    #[cfg(unix)]
    fn install_fake_hypa(workspace: &std::path::Path, contents: &str) -> String {
        use std::os::unix::fs::PermissionsExt;

        let bin_directory = workspace.join("bin");
        std::fs::create_dir(&bin_directory).expect("bin directory");
        let hypa = bin_directory.join("hypa");
        std::fs::write(&hypa, contents).expect("fake hypa");
        let mut permissions = std::fs::metadata(&hypa)
            .expect("fake hypa metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&hypa, permissions).expect("fake hypa permissions");
        format!("{}:/usr/bin:/bin", bin_directory.display())
    }

    #[test]
    fn native_delegation_schema_is_registered_only_with_a_server_route() {
        assert!(ToolRegistry::base().find("nakode_agent").is_none());
        let registry = ToolRegistry::base().with_native_delegation();
        let definition = registry
            .find("nakode_agent")
            .expect("native delegation tool")
            .definition();
        assert_eq!(definition.name, "nakode_agent");
        assert_eq!(definition.parameters["required"], json!(["agent", "task"]));
        assert_eq!(definition.parameters["additionalProperties"], false);
    }

    #[tokio::test]
    async fn native_delegation_binds_owner_parent_and_terminal_response() {
        let directory = tempfile::tempdir().expect("workspace");
        let registry = ToolRegistry::base().with_native_delegation();
        let tool = registry
            .find("nakode_agent")
            .expect("native delegation tool")
            .clone();
        let (requests, mut receiver) = mpsc::channel(1);
        let (events, _event_receiver) = mpsc::channel(1);
        let questions = QuestionBroker::default();
        let cancellation = CancellationToken::new();
        let mut session = RuntimeSession::new("test-model".to_owned(), String::new()).with_owner(
            Some("logical-session".to_owned()),
            Some("parent-run".to_owned()),
        );
        let invocation = tool.execute(
            ToolContext {
                workspace: directory.path(),
                session: &mut session,
                backend_events: &events,
                turn_id: "turn-native",
                questions: &questions,
                delegation: Some(&requests),
            },
            json!({"agent":"repo-explorer","task":"Inspect routing"}),
            &cancellation,
        );
        let server = async {
            let request = receiver.recv().await.expect("server request");
            assert_eq!(request.owner_session_id, "logical-session");
            assert_eq!(request.parent_run_id.as_deref(), Some("parent-run"));
            assert_eq!(request.agent, "repo-explorer");
            assert_eq!(request.task, "Inspect routing");
            request
                .respond
                .send(Ok(
                    "[Subagent Result] [run] [repo-explorer]\nDone".to_owned()
                ))
                .expect("tool waiter");
        };
        let (result, ()) = tokio::join!(invocation, server);
        assert!(!result.failed, "{}", result.output);
        assert!(result.output.contains("[Subagent Result]"));
    }

    struct ToolHarness<'a> {
        registry: ToolRegistry,
        workspace: &'a std::path::Path,
        session: RuntimeSession,
        events: mpsc::Sender<crate::backend::BackendEvent>,
        questions: QuestionBroker,
        cancellation: CancellationToken,
    }

    impl ToolHarness<'_> {
        async fn execute(&mut self, name: &str, arguments: Value) -> ToolResult {
            let tool = self.registry.find(name).expect("registered tool").clone();
            let arguments = match prepare_and_validate(tool.as_ref(), arguments) {
                Ok(arguments) => arguments,
                Err(error) => return ToolResult::failure(error),
            };
            tool.execute(
                ToolContext {
                    workspace: self.workspace,
                    session: &mut self.session,
                    backend_events: &self.events,
                    turn_id: "turn-1",
                    questions: &self.questions,
                    delegation: None,
                },
                arguments,
                &self.cancellation,
            )
            .await
        }
    }
}
