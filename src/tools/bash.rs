use std::{
    borrow::Cow, collections::HashMap, ffi::OsString, fmt::Write as _, io::Read, process::Command,
    time::Duration,
};

use sha2::{Digest, Sha256};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult,
    hypa::{RewriteDecision, rewrite_command},
    process::{ProcessRequest, ProcessResult, run_process},
    required_string, resolve_workspace_path,
};
use crate::{
    backend::{
        NativeAgentRequest, NativeValidationEvidenceOperation, NativeValidationEvidenceRequest,
    },
    runtime::ToolDefinition,
};

pub struct BashTool;

impl Tool for BashTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "bash",
            description: "Execute a shell command in the workspace and return stdout and stderr. Use read, grep, find, and ls instead of shell commands for file exploration. Commands have no deadline unless timeout is supplied.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                    "env": {"type": "object", "description": "Environment variables to set for the command", "additionalProperties": {"type": "string"}},
                    "timeout": {"type": "number", "exclusiveMinimum": 0, "maximum": 3600, "description": "Optional timeout in seconds"},
                    "cwd": {"type": "string", "description": "Workspace-relative working directory"},
                    "pty": {"type": "boolean", "description": "Use terminal semantics for commands that require a TTY; defaults to false"},
                    "reason": {"type": "string", "description": "Concrete reason required only to repeat unchanged successful validation"}
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
            let command = arguments
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let validation = validation_identity(context.workspace, &arguments, command);
            let reason_supplied = arguments
                .get("reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| !reason.trim().is_empty());
            if let Some(evidence) = validation.as_ref()
                && !reason_supplied
            {
                let sequence = if let (Some(requests), Some(owner_session_id)) =
                    (context.delegation, context.session.owner_session_id.clone())
                {
                    match request_validation_evidence(
                        requests,
                        owner_session_id,
                        context.session.parent_run_id.clone(),
                        evidence.relevant_state.clone(),
                        NativeValidationEvidenceOperation::Check,
                        cancellation,
                    )
                    .await
                    {
                        Ok(sequence) => sequence,
                        Err(error) => return ToolResult::failure(error),
                    }
                } else {
                    context
                        .session
                        .validation_evidence
                        .contains(evidence)
                        .then_some(0)
                };
                if let Some(sequence) = sequence {
                    let reference = if sequence == 0 {
                        "this provider session".to_owned()
                    } else {
                        format!("shared-context entry #{sequence}")
                    };
                    return ToolResult::success(format!(
                        "Skipped unchanged validation: reusing successful evidence from {reference}. Supply a concrete `reason` only when rerunning is necessary."
                    ));
                }
            }
            let result = run_shell(context.workspace, &arguments, cancellation).await;
            match result {
                Ok(mut output) => {
                    if !output.failed
                        && let Some(evidence) = validation
                    {
                        context.session.validation_evidence.retain(|existing| {
                            existing.command != evidence.command || existing.cwd != evidence.cwd
                        });
                        context.session.validation_evidence.push(evidence.clone());
                        if context.session.validation_evidence.len() > 32 {
                            context.session.validation_evidence.remove(0);
                        }
                        if let (Some(requests), Some(owner_session_id)) =
                            (context.delegation, context.session.owner_session_id.clone())
                        {
                            let body = validation_evidence_body(&evidence);
                            if let Err(error) = request_validation_evidence(
                                requests,
                                owner_session_id,
                                context.session.parent_run_id.clone(),
                                evidence.relevant_state,
                                NativeValidationEvidenceOperation::Record { body },
                                cancellation,
                            )
                            .await
                            {
                                let _ = write!(
                                    output.output,
                                    "\n\nWarning: validation succeeded but shared evidence could not be recorded: {error}"
                                );
                            }
                        }
                    }
                    output
                }
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn request_validation_evidence(
    requests: &tokio::sync::mpsc::Sender<NativeAgentRequest>,
    owner_session_id: String,
    requester_run_id: Option<String>,
    identity: String,
    operation: NativeValidationEvidenceOperation,
    cancellation: &CancellationToken,
) -> Result<Option<u64>, String> {
    let (respond, response) = tokio::sync::oneshot::channel();
    requests
        .send(NativeAgentRequest::ValidationEvidence(
            NativeValidationEvidenceRequest {
                owner_session_id,
                requester_run_id,
                identity,
                operation,
                respond,
            },
        ))
        .await
        .map_err(|_| "validation-evidence route closed before the request was sent".to_owned())?;
    tokio::select! {
        response = response => response.map_err(|_| "validation-evidence route closed before responding".to_owned())?,
        () = cancellation.cancelled() => Err("validation-evidence request interrupted".to_owned()),
    }
}

fn validation_evidence_body(evidence: &crate::runtime::ValidationEvidence) -> String {
    let mut body = format!(
        "Successful validation; reuse while repository state is unchanged.\ncommand: {}\ncwd: {}\nstate: {}",
        evidence.command, evidence.cwd, evidence.relevant_state
    );
    if body.len() > 4 * 1024 {
        let mut end = 4 * 1024;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
    }
    body
}

fn validation_identity(
    workspace: &std::path::Path,
    arguments: &Value,
    command: &str,
) -> Option<crate::runtime::ValidationEvidence> {
    let normalized = command.split_whitespace().collect::<Vec<_>>().join(" ");
    let validation = [
        "cargo fmt",
        "cargo check",
        "cargo clippy",
        "cargo test",
        "./dxp check",
        "./dxp build",
        "bun test",
        "tsc",
        "eslint",
        "prettier",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    if !validation {
        return None;
    }
    let cwd = arguments.get("cwd").and_then(Value::as_str).map_or_else(
        || workspace.to_path_buf(),
        |path| resolve_workspace_path(workspace, path).unwrap_or_else(|_| workspace.to_path_buf()),
    );
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    for args in [
        ["rev-parse", "HEAD"].as_slice(),
        ["status", "--porcelain=v1", "--untracked-files=all"].as_slice(),
        ["diff", "--binary", "HEAD", "--"].as_slice(),
    ] {
        if let Ok(output) = Command::new("git").args(args).current_dir(&cwd).output() {
            hasher.update(&output.stdout);
            hasher.update(&output.stderr);
        }
    }
    Some(crate::runtime::ValidationEvidence {
        command: normalized,
        cwd: cwd.to_string_lossy().into_owned(),
        relevant_state: format!("{:x}", hasher.finalize()),
    })
}

async fn run_shell(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, String> {
    let requested_command = required_string(arguments, "command")?;
    let requested_timeout = arguments
        .get("timeout")
        .map(|value| {
            value
                .as_f64()
                .filter(|seconds| seconds.is_finite() && *seconds > 0.0 && *seconds <= 3_600.0)
                .ok_or_else(|| "bash timeout must be between 0 and 3600 seconds".to_owned())
        })
        .transpose()?;
    let timeout = requested_timeout.map(Duration::from_secs_f64);
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
        return Ok(ToolResult::success(if result.output.is_empty() {
            "(no output)".to_owned()
        } else {
            result.output
        }));
    }
    let reason = if result.interrupted {
        "command interrupted".to_owned()
    } else if result.timed_out {
        format!(
            "Command timed out after {} seconds",
            requested_timeout.unwrap_or_default()
        )
    } else {
        format!(
            "Command exited with code {}",
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
        let output_reader =
            std::thread::spawn(move || read_bounded(&mut reader, super::MAX_TOOL_OUTPUT_BYTES));
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

fn read_bounded(reader: &mut impl Read, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut retained = Vec::with_capacity(limit.min(8 * 1024));
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(retained);
        }
        let remaining = limit.saturating_add(1).saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..read.min(remaining)]);
    }
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
