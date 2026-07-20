use std::{borrow::Cow, collections::HashMap, ffi::OsString, io::Read, time::Duration};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult,
    hypa::{RewriteDecision, rewrite_command},
    optional_u64,
    process::{ProcessRequest, ProcessResult, run_process},
    required_string, resolve_workspace_path,
};
use crate::runtime::ToolDefinition;

pub struct BashTool;

impl Tool for BashTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "bash",
            description: "Run a command or short pipeline with merged output. When Hypa is installed, eligible non-PTY commands are automatically rewritten for deterministic output compression. Use cwd instead of cd, env for values needing shell escaping, and pty only for real terminal semantics. Use grep/glob/read instead of shell search or directory listing.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "env": {"type": "object", "additionalProperties": {"type": "string"}},
                    "timeout": {"type": "number", "minimum": 0, "maximum": 3600, "description": "Seconds; 0 disables the deadline"},
                    "cwd": {"type": "string", "description": "Working directory"},
                    "pty": {"type": "boolean", "description": "Run with terminal semantics"}
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .chars()
            .take(100)
            .collect()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = run_shell(context.workspace, &arguments, cancellation).await;
            match result {
                Ok(output) => output,
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn run_shell(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, String> {
    let requested_command = required_string(arguments, "command")?;
    let requested_timeout = optional_u64(arguments, "timeout", 120)?.min(3_600);
    let timeout = (requested_timeout > 0).then(|| Duration::from_secs(requested_timeout));
    let run_directory = arguments.get("cwd").and_then(Value::as_str).map_or_else(
        || Ok(workspace.to_path_buf()),
        |path| resolve_workspace_path(workspace, path),
    )?;
    let environment = parse_environment(arguments)?;
    let use_pty = arguments
        .get("pty")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let command = if use_pty {
        Cow::Borrowed(requested_command)
    } else {
        match rewrite_command(
            &run_directory,
            requested_command,
            &environment,
            cancellation,
        )
        .await
        {
            RewriteDecision::Command(command) => Cow::Owned(command),
            RewriteDecision::Passthrough => Cow::Borrowed(requested_command),
            RewriteDecision::Blocked(reason) => return Ok(ToolResult::failure(reason)),
            RewriteDecision::Interrupted => {
                return Ok(ToolResult::failure("command interrupted"));
            }
        }
    };
    let (program, shell_arguments) = shell_command(&command);
    let result = if use_pty {
        run_pty_process(
            program,
            shell_arguments,
            run_directory,
            environment,
            timeout,
            cancellation.clone(),
        )
        .await?
    } else {
        run_process(
            &run_directory,
            ProcessRequest {
                program,
                arguments: &shell_arguments,
                input: None,
                environment: Some(&environment),
                timeout,
            },
            cancellation,
        )
        .await?
    };
    if result.success {
        return Ok(ToolResult::success(result.output));
    }
    let reason = if result.interrupted {
        "command interrupted".to_owned()
    } else if result.timed_out {
        format!("command timed out after {requested_timeout} seconds")
    } else {
        format!(
            "command exited with {}",
            result
                .exit_code
                .map_or_else(|| "unknown status".to_owned(), |code| code.to_string())
        )
    };
    let output = if result.output.is_empty() {
        reason
    } else {
        format!("{reason}\n{}", result.output)
    };
    Ok(ToolResult::failure(output))
}

async fn run_pty_process(
    program: &'static str,
    arguments: Vec<OsString>,
    cwd: std::path::PathBuf,
    environment: HashMap<String, String>,
    timeout: Option<Duration>,
    cancellation: CancellationToken,
) -> Result<ProcessResult, String> {
    tokio::task::spawn_blocking(move || {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|error| format!("failed to open PTY: {error}"))?;
        let mut command = CommandBuilder::new(program);
        command.args(arguments);
        command.cwd(cwd);
        for (name, value) in environment {
            command.env(name, value);
        }
        let mut child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| format!("failed to start PTY command: {error}"))?;
        drop(pair.slave);
        let mut reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| format!("failed to capture PTY output: {error}"))?;
        let output_reader = std::thread::spawn(move || {
            let mut output = Vec::new();
            reader.read_to_end(&mut output).map(|_| output)
        });
        let started = std::time::Instant::now();
        let mut interrupted = false;
        let mut timed_out = false;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("failed to inspect PTY command: {error}"))?
            {
                break status;
            }
            if cancellation.is_cancelled() {
                interrupted = true;
                child
                    .kill()
                    .map_err(|error| format!("failed to interrupt PTY command: {error}"))?;
                break child
                    .wait()
                    .map_err(|error| format!("failed to reap PTY command: {error}"))?;
            }
            if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                timed_out = true;
                child
                    .kill()
                    .map_err(|error| format!("failed to stop timed-out PTY command: {error}"))?;
                break child
                    .wait()
                    .map_err(|error| format!("failed to reap PTY command: {error}"))?;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        drop(pair.master);
        let output = output_reader
            .join()
            .map_err(|_| "PTY output reader panicked".to_owned())?
            .map_err(|error| format!("failed to read PTY output: {error}"))?;
        Ok(ProcessResult {
            output: super::truncate_output(output),
            success: status.success() && !timed_out && !interrupted,
            exit_code: i32::try_from(status.exit_code()).ok(),
            timed_out,
            interrupted,
        })
    })
    .await
    .map_err(|error| format!("PTY worker failed: {error}"))?
}

fn parse_environment(arguments: &Value) -> Result<HashMap<String, String>, String> {
    arguments.get("env").map_or_else(
        || Ok(HashMap::new()),
        |value| {
            value
                .as_object()
                .ok_or_else(|| "bash env must be an object".to_owned())?
                .iter()
                .map(|(name, value)| {
                    value
                        .as_str()
                        .map(|value| (name.clone(), value.to_owned()))
                        .ok_or_else(|| format!("bash env value for {name} must be a string"))
                })
                .collect()
        },
    )
}

#[cfg(unix)]
fn shell_command(command: &str) -> (&'static str, Vec<OsString>) {
    ("sh", vec!["-lc".into(), command.into()])
}

#[cfg(windows)]
fn shell_command(command: &str) -> (&'static str, Vec<OsString>) {
    (
        "cmd.exe",
        vec!["/D".into(), "/S".into(), "/C".into(), command.into()],
    )
}
