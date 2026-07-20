mod ask;
mod bash;
mod edit;
mod eval;
mod glob;
mod grep;
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

pub const MAX_TOOL_OUTPUT_BYTES: usize = 256 * 1024;

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
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.iter().map(|tool| tool.definition()).collect()
    }

    #[must_use]
    pub fn find(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools
            .iter()
            .find(|tool| tool.definition().name == name)
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
/// Returns an error when the path is empty or home-directory expansion is unavailable.
pub fn resolve_workspace_path(
    workspace: &Path,
    supplied: &str,
) -> Result<std::path::PathBuf, String> {
    if supplied.is_empty() {
        return Err("tool path must not be empty".to_owned());
    }
    let expanded = if supplied == "~" || supplied.starts_with("~/") {
        let home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .ok_or_else(|| {
                "cannot expand ~ because the home directory is unavailable".to_owned()
            })?;
        home.join(supplied.trim_start_matches('~').trim_start_matches('/'))
    } else {
        std::path::PathBuf::from(supplied)
    };
    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(workspace.join(expanded))
    }
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{ToolContext, ToolRegistry, ToolResult, resolve_workspace_path};
    use crate::runtime::{QuestionBroker, RuntimeSession};

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
    fn tool_paths_follow_shell_style_local_path_resolution() {
        let root = std::path::Path::new("/tmp/workspace");
        assert_eq!(
            resolve_workspace_path(root, "src/main.rs").expect("relative path"),
            root.join("src/main.rs")
        );
        assert_eq!(
            resolve_workspace_path(root, "../secret").expect("parent path"),
            root.join("../secret")
        );
        assert_eq!(
            resolve_workspace_path(root, "/etc/passwd").expect("absolute path"),
            std::path::PathBuf::from("/etc/passwd")
        );
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
