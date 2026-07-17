use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, Command},
    sync::mpsc,
    time::{Instant, interval, timeout},
};

use crate::backend::{
    ApprovalDecision, BackendCapabilities, BackendCommand, BackendError, BackendEvent,
    BackendHandle, BackendIdentity, BackendOperation, CODEX_PROVIDER,
};

use super::protocol::{
    RpcError, RpcMessage, normalize_notification, normalize_server_request, notification,
    parse_message, parse_models, parse_session_history, request, response,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub workspace: PathBuf,
}

impl BackendConfig {
    pub fn codex(program: PathBuf, workspace: PathBuf) -> Self {
        Self {
            program,
            args: vec!["app-server".into(), "--stdio".into()],
            workspace,
        }
    }
}

#[derive(Debug)]
struct PendingRequest {
    operation: BackendOperation,
    sent_at: Instant,
}

pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let mut child = Command::new(&config.program)
        .args(&config.args)
        .current_dir(&config.workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| BackendError::Spawn {
            backend: "Codex",
            program: config.program.clone(),
            source,
        })?;

    let mut stdin = child.stdin.take().ok_or(BackendError::MissingPipe {
        backend: "Codex",
        pipe: "stdin",
    })?;
    let stdout = child.stdout.take().ok_or(BackendError::MissingPipe {
        backend: "Codex",
        pipe: "stdout",
    })?;
    let stderr = child.stderr.take().ok_or(BackendError::MissingPipe {
        backend: "Codex",
        pipe: "stderr",
    })?;

    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let (stderr_tx, stderr_rx) = mpsc::channel(32);

    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stderr_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    let mut pending = HashMap::new();
    let initialize_id = 1;
    let initialize = request(
        initialize_id,
        "initialize",
        json!({
            "clientInfo": {
                "name": "flock",
                "title": "Flock",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "experimentalApi": true,
                "requestAttestation": false,
                "mcpServerOpenaiFormElicitation": false,
            },
        }),
    );
    write_json(&mut stdin, &initialize)
        .await
        .map_err(|source| BackendError::InitializeWrite {
            backend: "Codex",
            source,
        })?;
    pending.insert(
        initialize_id,
        PendingRequest {
            operation: BackendOperation::Initialize,
            sent_at: Instant::now(),
        },
    );

    let task = tokio::spawn(run_supervisor(
        child,
        stdin,
        BufReader::new(stdout).lines(),
        command_rx,
        event_tx,
        stderr_rx,
        pending,
        config.workspace,
        initialize_id + 1,
    ));

    Ok(BackendHandle::new(command_tx, event_rx, task))
}

#[allow(clippy::too_many_arguments)]
async fn run_supervisor(
    mut child: tokio::process::Child,
    mut stdin: ChildStdin,
    mut stdout: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
    mut stderr: mpsc::Receiver<String>,
    mut pending: HashMap<u64, PendingRequest>,
    workspace: PathBuf,
    mut next_id: u64,
) {
    let mut initialized = false;
    let mut deferred_commands = VecDeque::new();
    let mut pending_approvals = HashMap::new();
    let mut timeout_tick = interval(Duration::from_secs(1));
    let mut last_stderr = None;
    let mut requested_shutdown = false;

    'supervisor: loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    requested_shutdown = true;
                    break;
                };
                if matches!(command, BackendCommand::Shutdown) {
                    requested_shutdown = true;
                    break;
                }
                if !initialized && requires_initialization(&command) {
                    deferred_commands.push_back(command);
                    continue;
                }
                if let Err(error) = handle_command(
                    command,
                    &mut stdin,
                    &mut pending,
                    &mut next_id,
                    &workspace,
                    &mut pending_approvals,
                ).await {
                    let _ = events.send(BackendEvent::Disconnected {
                        reason: format!("failed to write to Codex: {error}"),
                    }).await;
                    break;
                }
            }
            line = stdout.next_line() => {
                match line {
                    Ok(Some(line)) if line.trim().is_empty() => {}
                    Ok(Some(line)) => {
                        match parse_message(&line) {
                            Ok(message) => {
                                if process_message(
                                    message,
                                    &mut stdin,
                                    &events,
                                    &mut pending,
                                    &mut next_id,
                                    &mut initialized,
                                    &mut pending_approvals,
                                ).await.is_err() {
                                    break;
                                }
                                if initialized {
                                    while let Some(command) = deferred_commands.pop_front() {
                                        if let Err(error) = handle_command(
                                            command,
                                            &mut stdin,
                                            &mut pending,
                                            &mut next_id,
                                            &workspace,
                                            &mut pending_approvals,
                                        ).await {
                                            let _ = events.send(BackendEvent::Disconnected {
                                                reason: format!("failed to write to Codex: {error}"),
                                            }).await;
                                            break 'supervisor;
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                let preview: String = line.chars().take(180).collect();
                                let _ = events.send(BackendEvent::ProtocolDiagnostic(format!(
                                    "malformed Codex JSON ({error}): {preview}"
                                ))).await;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        last_stderr = Some(format!("stdout read failed: {error}"));
                        break;
                    }
                }
            }
            line = stderr.recv() => {
                if let Some(line) = line {
                    last_stderr = Some(line);
                }
            }
            _ = timeout_tick.tick() => {
                let now = Instant::now();
                let timed_out = pending
                    .iter()
                    .filter(|(_, request)| now.duration_since(request.sent_at) >= REQUEST_TIMEOUT)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for id in timed_out {
                    if let Some(request) = pending.remove(&id) {
                        let _ = events.send(BackendEvent::RequestFailed {
                            operation: request.operation,
                            code: -32001,
                            message: format!("request {id} timed out after {}s", REQUEST_TIMEOUT.as_secs()),
                        }).await;
                    }
                }
            }
        }
    }

    drop(stdin);
    if requested_shutdown {
        if timeout(Duration::from_secs(2), child.wait()).await.is_err() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    } else {
        let status = child.wait().await.ok();
        let mut reason = match status {
            Some(status) => format!("Codex app-server exited with {status}"),
            None => "Codex app-server disconnected".to_owned(),
        };
        if let Some(stderr) = last_stderr {
            reason.push_str(": ");
            reason.push_str(&stderr);
        }
        let _ = events
            .send(BackendEvent::Disconnected {
                reason: reason.clone(),
            })
            .await;
        for (_, request) in pending {
            let _ = events
                .send(BackendEvent::RequestFailed {
                    operation: request.operation,
                    code: -32002,
                    message: reason.clone(),
                })
                .await;
        }
    }
}

fn requires_initialization(command: &BackendCommand) -> bool {
    matches!(
        command,
        BackendCommand::StartSession { .. }
            | BackendCommand::ResumeSession { .. }
            | BackendCommand::UnsubscribeSession { .. }
            | BackendCommand::StartTurn { .. }
            | BackendCommand::SteerTurn { .. }
            | BackendCommand::InterruptTurn { .. }
            | BackendCommand::Reload { .. }
            | BackendCommand::SetSessionModel { .. }
    )
}

async fn handle_command(
    command: BackendCommand,
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    workspace: &std::path::Path,
    pending_approvals: &mut HashMap<String, String>,
) -> std::io::Result<()> {
    match command {
        BackendCommand::ResolveApproval { id, decision } => {
            let method = pending_approvals
                .remove(&approval_key(&id))
                .unwrap_or_default();
            let legacy = matches!(
                method.as_str(),
                "execCommandApproval" | "applyPatchApproval"
            );
            let wire_decision = match (legacy, decision) {
                (true, ApprovalDecision::AcceptOnce) => "approved",
                (true, ApprovalDecision::AcceptForSession) => "approved_for_session",
                (true, ApprovalDecision::Decline) => "denied",
                (false, ApprovalDecision::AcceptOnce) => "accept",
                (false, ApprovalDecision::AcceptForSession) => "acceptForSession",
                (false, ApprovalDecision::Decline) => "decline",
            };
            return write_json(stdin, &response(id, Ok(json!({"decision": wire_decision})))).await;
        }
        BackendCommand::SetSessionModel { .. } => return Ok(()),
        BackendCommand::Shutdown => return Ok(()),
        _ => {}
    }

    let (operation, method, params) = match command {
        BackendCommand::Reload { .. } => (
            BackendOperation::Reload,
            "model/list",
            json!({"limit": 100}),
        ),
        BackendCommand::StartSession { model } => (
            BackendOperation::StartSession,
            "thread/start",
            with_optional_model(json!({"cwd": workspace.to_string_lossy()}), model),
        ),
        BackendCommand::ResumeSession {
            provider_session_id,
        } => (
            BackendOperation::ResumeSession,
            "thread/resume",
            json!({
                "threadId": provider_session_id,
                "cwd": workspace.to_string_lossy(),
            }),
        ),
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => (
            BackendOperation::UnsubscribeSession,
            "thread/unsubscribe",
            json!({"threadId": provider_session_id}),
        ),
        BackendCommand::StartTurn {
            session_id,
            client_id,
            prompt,
            model,
        } => (
            BackendOperation::StartTurn,
            "turn/start",
            with_optional_model(
                json!({
                    "threadId": session_id,
                    "clientUserMessageId": client_id,
                    "input": [{
                        "type": "text",
                        "text": prompt,
                        "text_elements": [],
                    }],
                }),
                model,
            ),
        ),
        BackendCommand::SteerTurn {
            session_id,
            turn_id,
            client_id,
            prompt,
        } => (
            BackendOperation::SteerTurn,
            "turn/steer",
            json!({
                "threadId": session_id,
                "expectedTurnId": turn_id,
                "clientUserMessageId": client_id,
                "input": [{
                    "type": "text",
                    "text": prompt,
                    "text_elements": [],
                }],
            }),
        ),
        BackendCommand::InterruptTurn {
            session_id,
            turn_id,
        } => (
            BackendOperation::InterruptTurn,
            "turn/interrupt",
            json!({"threadId": session_id, "turnId": turn_id}),
        ),
        BackendCommand::SetSessionModel { .. }
        | BackendCommand::ResolveApproval { .. }
        | BackendCommand::Shutdown => unreachable!(),
    };

    send_request(stdin, pending, next_id, operation, method, params).await
}

#[allow(clippy::too_many_arguments)]
async fn process_message(
    message: RpcMessage,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    initialized: &mut bool,
    pending_approvals: &mut HashMap<String, String>,
) -> Result<(), ()> {
    if let Some(method) = message.method {
        let params = message.params.unwrap_or(Value::Null);
        if let Some(id) = message.id {
            if is_approval_method(&method) {
                pending_approvals.insert(approval_key(&id), method.clone());
                let approval = normalize_server_request(id, method, params);
                events
                    .send(BackendEvent::ApprovalRequested(approval))
                    .await
                    .map_err(|_| ())?;
            } else {
                write_json(
                    stdin,
                    &response(
                        id,
                        Err(RpcError {
                            code: -32601,
                            message: format!("Flock does not support server request {method}"),
                            data: None,
                        }),
                    ),
                )
                .await
                .map_err(|_| ())?;
            }
        } else if let Some(event) = normalize_notification(&method, params) {
            events.send(event).await.map_err(|_| ())?;
        }
        return Ok(());
    }

    let Some(id) = message.id.and_then(|id| id.as_u64()) else {
        events
            .send(BackendEvent::ProtocolDiagnostic(
                "Codex response omitted a numeric request id".to_owned(),
            ))
            .await
            .map_err(|_| ())?;
        return Ok(());
    };
    let Some(request) = pending.remove(&id) else {
        events
            .send(BackendEvent::ProtocolDiagnostic(format!(
                "late or unknown Codex response id {id}"
            )))
            .await
            .map_err(|_| ())?;
        return Ok(());
    };

    if let Some(error) = message.error {
        events
            .send(BackendEvent::RequestFailed {
                operation: request.operation,
                code: error.code,
                message: error.message,
            })
            .await
            .map_err(|_| ())?;
        return Ok(());
    }

    let result = message.result.unwrap_or(Value::Null);
    match request.operation {
        BackendOperation::Initialize => {
            *initialized = true;
            let initialized_notification = notification("initialized", None);
            write_json(stdin, &initialized_notification)
                .await
                .map_err(|_| ())?;
            let user_agent = result
                .get("userAgent")
                .and_then(Value::as_str)
                .unwrap_or("Codex")
                .to_owned();
            events
                .send(BackendEvent::Ready(BackendIdentity {
                    provider: CODEX_PROVIDER.to_owned(),
                    display_name: user_agent,
                    version: None,
                    capabilities: BackendCapabilities {
                        resume: true,
                        steering: true,
                        interruption: true,
                        model_catalog: true,
                        models_require_session: false,
                        session_model_config: false,
                        approvals: true,
                        native_tools: true,
                        mcp: true,
                        close_session: true,
                    },
                }))
                .await
                .map_err(|_| ())?;
            send_request(
                stdin,
                pending,
                next_id,
                BackendOperation::ModelList,
                "model/list",
                json!({"limit": 100}),
            )
            .await
            .map_err(|_| ())?;
        }
        BackendOperation::ModelList | BackendOperation::Reload => {
            events
                .send(BackendEvent::Models(parse_models(&result)))
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::SetSessionModel => {}
        BackendOperation::StartSession => {
            events
                .send(BackendEvent::SessionCreated {
                    provider_session_id: nested_result_string(&result, &["thread", "id"]),
                    model: result
                        .get("model")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                })
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::ResumeSession => {
            events
                .send(BackendEvent::SessionResumed {
                    provider_session_id: nested_result_string(&result, &["thread", "id"]),
                    model: result
                        .get("model")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    history: parse_session_history(&result),
                })
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::UnsubscribeSession => {
            events
                .send(BackendEvent::SessionUnsubscribed)
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::StartTurn => {
            events
                .send(BackendEvent::TurnAccepted {
                    turn_id: nested_result_string(&result, &["turn", "id"]),
                })
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::SteerTurn => {
            events
                .send(BackendEvent::SteerAccepted {
                    turn_id: result
                        .get("turnId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                })
                .await
                .map_err(|_| ())?;
        }
        BackendOperation::InterruptTurn => {
            events
                .send(BackendEvent::InterruptAccepted)
                .await
                .map_err(|_| ())?;
        }
    }
    Ok(())
}

fn approval_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_default()
}

fn is_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
    )
}

async fn send_request(
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    operation: BackendOperation,
    method: &str,
    params: Value,
) -> std::io::Result<()> {
    let id = allocate_request_id(pending, next_id);
    write_json(stdin, &request(id, method, params)).await?;
    pending.insert(
        id,
        PendingRequest {
            operation,
            sent_at: Instant::now(),
        },
    );
    Ok(())
}

fn allocate_request_id(pending: &HashMap<u64, PendingRequest>, next_id: &mut u64) -> u64 {
    loop {
        let candidate = *next_id;
        *next_id = next_id.checked_add(1).unwrap_or(1);
        if !pending.contains_key(&candidate) {
            return candidate;
        }
    }
}

async fn write_json(stdin: &mut ChildStdin, value: &Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    stdin.write_all(&bytes).await?;
    stdin.flush().await
}

fn with_optional_model(mut params: Value, model: Option<String>) -> Value {
    if let Some(model) = model {
        params["model"] = Value::String(model);
    }
    params
}

fn nested_result_string(value: &Value, path: &[&str]) -> String {
    let mut current = value;
    for component in path {
        let Some(next) = current.get(component) else {
            return String::new();
        };
        current = next;
    }
    current.as_str().unwrap_or_default().to_owned()
}
