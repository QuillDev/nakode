mod ask;
mod bash;
mod browser;
mod edit;
mod eval;
mod glob;
mod grep;
mod hypa;
mod process;
mod read;
mod todo;
mod write;

use std::{future::Future, path::Path, pin::Pin, sync::Arc};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    backend::BackendEvent,
    runtime::{QuestionBroker, RuntimeSession, ToolDefinition},
};

pub const MAX_TOOL_OUTPUT_BYTES: usize = 128 * 1024;
pub const MAX_MODEL_TOOL_OUTPUT_BYTES: usize = 32 * 1024;

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolResult {
    pub output: String,
    pub failed: bool,
}

impl ToolResult {
    #[must_use]
    pub fn success(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            failed: false,
        }
    }

    #[must_use]
    pub fn failure(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            failed: true,
        }
    }
}

pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    fn summarize(&self, arguments: &Value) -> String;
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
                Arc::new(write::WriteTool),
                Arc::new(edit::EditTool),
                Arc::new(bash::BashTool),
                Arc::new(glob::GlobTool),
                Arc::new(grep::GrepTool),
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
        resolve_workspace_path,
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
                "read", "write", "edit", "bash", "glob", "grep", "eval", "ask", "todo"
            ]
        );
    }

    #[test]
    fn base_tool_schemas_follow_the_omp_agent_facing_contracts() {
        let definitions = ToolRegistry::base().definitions();
        let properties = |name: &str| {
            definitions
                .iter()
                .find(|definition| definition.name == name)
                .and_then(|definition| definition.parameters["properties"].as_object())
                .map(|properties| properties.keys().map(String::as_str).collect::<Vec<_>>())
                .expect("tool properties")
        };

        assert_eq!(properties("read"), ["path"]);
        assert_eq!(properties("write"), ["content", "path"]);
        assert_eq!(properties("edit"), ["edits", "path"]);
        assert_eq!(
            properties("bash"),
            ["command", "cwd", "env", "pty", "timeout"]
        );
        assert_eq!(properties("glob"), ["gitignore", "hidden", "limit", "path"]);
        assert_eq!(
            properties("grep"),
            ["case", "gitignore", "path", "pattern", "skip"]
        );
        assert_eq!(
            properties("eval"),
            ["code", "language", "reset", "timeout", "title"]
        );
        assert_eq!(properties("ask"), ["questions"]);
        assert_eq!(properties("todo"), ["items", "list", "op", "phase", "task"]);
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
        assert!(model_output.contains("full 65544-byte output remains in the transcript"));
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
            ("read", json!({"path": "nested/file.txt"}), "1|after"),
            ("glob", json!({"path": "**/*.txt"}), "nested/file.txt"),
            ("grep", json!({"pattern": "search me"}), "# nested/file.txt"),
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
            self.registry
                .find(name)
                .expect("registered tool")
                .execute(
                    ToolContext {
                        workspace: self.workspace,
                        session: &mut self.session,
                        backend_events: &self.events,
                        turn_id: "turn-1",
                        questions: &self.questions,
                    },
                    arguments,
                    &self.cancellation,
                )
                .await
        }
    }
}
