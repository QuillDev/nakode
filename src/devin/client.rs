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
    ApprovalDecision, ApprovalKind, ApprovalRequest, BackendCapabilities, BackendCommand,
    BackendError, BackendEvent, BackendHandle, BackendIdentity, BackendOperation, DEVIN_PROVIDER,
    DeltaKind, ItemKind, ItemStatus, ModelInfo, NormalizedItem, SessionHistoryItem, TurnOutcome,
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
    pub fn devin(program: PathBuf, workspace: PathBuf) -> Self {
        Self {
            program,
            args: vec!["acp".into()],
            workspace,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AcpCapabilities {
    load_session: bool,
    resume_session: bool,
    close_session: bool,
    mcp: bool,
}

impl AcpCapabilities {
    fn backend(&self) -> BackendCapabilities {
        BackendCapabilities {
            resume: self.load_session || self.resume_session,
            steering: false,
            interruption: true,
            model_catalog: true,
            models_require_session: true,
            session_model_config: true,
            approvals: true,
            native_tools: true,
            mcp: self.mcp,
            close_session: self.close_session,
        }
    }
}

#[derive(Clone, Debug)]
enum PendingKind {
    Initialize,
    StartSession {
        requested_model: Option<String>,
    },
    ResumeSession {
        session_id: String,
        replay: bool,
    },
    StartTurn {
        session_id: String,
        turn_id: String,
    },
    CloseSession,
    SetModel {
        session_id: String,
        announce_session: bool,
    },
    ReloadModels {
        session_id: String,
    },
}

#[derive(Clone, Debug)]
struct PendingRequest {
    kind: PendingKind,
    operation: BackendOperation,
    sent_at: Instant,
}

#[derive(Clone, Debug, Default)]
struct SessionModelOption {
    config_id: String,
    current_value: String,
    models: Vec<ModelInfo>,
}

#[derive(Clone, Debug, Default)]
struct PermissionOptions {
    accept_once: Option<String>,
    accept_always: Option<String>,
    decline: Option<String>,
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
            backend: "Devin ACP",
            program: config.program.clone(),
            source,
        })?;

    let mut stdin = child.stdin.take().ok_or(BackendError::MissingPipe {
        backend: "Devin ACP",
        pipe: "stdin",
    })?;
    let stdout = child.stdout.take().ok_or(BackendError::MissingPipe {
        backend: "Devin ACP",
        pipe: "stdout",
    })?;
    let stderr = child.stderr.take().ok_or(BackendError::MissingPipe {
        backend: "Devin ACP",
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

    let initialize = request(
        1,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {
                "name": "flock",
                "title": "Flock",
                "version": env!("CARGO_PKG_VERSION"),
            },
        }),
    );
    write_json(&mut stdin, &initialize)
        .await
        .map_err(|source| BackendError::InitializeWrite {
            backend: "Devin ACP",
            source,
        })?;
    let mut pending = HashMap::new();
    pending.insert(
        1,
        PendingRequest {
            kind: PendingKind::Initialize,
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
) {
    let mut next_id = 2_u64;
    let mut initialized = false;
    let mut deferred = VecDeque::new();
    let mut capabilities = AcpCapabilities::default();
    let mut active_turns = HashMap::new();
    let mut replay = HashMap::<String, Vec<SessionHistoryItem>>::new();
    let mut model_options = HashMap::<String, SessionModelOption>::new();
    let mut permissions = HashMap::<String, PermissionOptions>::new();
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
                if !initialized {
                    deferred.push_back(command);
                    continue;
                }
                if let Err(error) = handle_command(
                    command,
                    &mut stdin,
                    &events,
                    &mut pending,
                    &mut next_id,
                    &workspace,
                    &capabilities,
                    &mut active_turns,
                    &mut replay,
                    &mut model_options,
                    &mut permissions,
                ).await {
                    let _ = events.send(BackendEvent::Disconnected {
                        reason: format!("failed to write to Devin ACP: {error}"),
                    }).await;
                    break;
                }
            }
            line = stdout.next_line() => {
                match line {
                    Ok(Some(line)) if line.trim().is_empty() => {}
                    Ok(Some(line)) => match serde_json::from_str::<Value>(&line) {
                        Ok(message) => {
                            if process_message(
                                message,
                                &mut stdin,
                                &events,
                                &mut pending,
                                &mut next_id,
                                &mut capabilities,
                                &mut initialized,
                                &mut active_turns,
                                &mut replay,
                                &mut model_options,
                                &mut permissions,
                            ).await.is_err() {
                                break;
                            }
                            if initialized {
                                while let Some(command) = deferred.pop_front() {
                                    if let Err(error) = handle_command(
                                        command,
                                        &mut stdin,
                                        &events,
                                        &mut pending,
                                        &mut next_id,
                                        &workspace,
                                        &capabilities,
                                        &mut active_turns,
                                        &mut replay,
                                        &mut model_options,
                                        &mut permissions,
                                    ).await {
                                        let _ = events.send(BackendEvent::Disconnected {
                                            reason: format!("failed to write to Devin ACP: {error}"),
                                        }).await;
                                        break 'supervisor;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let preview: String = line.chars().take(180).collect();
                            let _ = events.send(BackendEvent::ProtocolDiagnostic(format!(
                                "malformed ACP JSON ({error}): {preview}"
                            ))).await;
                        }
                    },
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
                let timed_out = pending.iter()
                    .filter(|(_, request)| now.duration_since(request.sent_at) >= REQUEST_TIMEOUT)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for id in timed_out {
                    if let Some(request) = pending.remove(&id) {
                        let message = format!(
                            "request {id} timed out after {}s",
                            REQUEST_TIMEOUT.as_secs()
                        );
                        let _ = emit_request_failure(
                            request,
                            -32001,
                            message,
                            &events,
                            &mut active_turns,
                            &model_options,
                        )
                        .await;
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
        let mut reason = status
            .map(|status| format!("Devin ACP exited with {status}"))
            .unwrap_or_else(|| "Devin ACP disconnected".to_owned());
        if let Some(stderr) = last_stderr {
            reason.push_str(": ");
            reason.push_str(&stderr);
        }
        let _ = events
            .send(BackendEvent::Disconnected {
                reason: reason.clone(),
            })
            .await;
        for request in pending.into_values() {
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

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    command: BackendCommand,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    workspace: &std::path::Path,
    capabilities: &AcpCapabilities,
    active_turns: &mut HashMap<String, String>,
    replay: &mut HashMap<String, Vec<SessionHistoryItem>>,
    model_options: &mut HashMap<String, SessionModelOption>,
    permissions: &mut HashMap<String, PermissionOptions>,
) -> std::io::Result<()> {
    match command {
        BackendCommand::StartSession { model } => {
            send_request(
                stdin,
                pending,
                next_id,
                PendingKind::StartSession {
                    requested_model: model,
                },
                BackendOperation::StartSession,
                "session/new",
                json!({"cwd": workspace.to_string_lossy(), "mcpServers": []}),
            )
            .await
        }
        BackendCommand::Reload { session_id } => {
            if let Some(session_id) = session_id {
                let Some(option) = model_options.get(&session_id).cloned() else {
                    let _ = events
                        .send(BackendEvent::RequestFailed {
                            operation: BackendOperation::Reload,
                            code: -32601,
                            message: "Devin session did not expose a model config option"
                                .to_owned(),
                        })
                        .await;
                    return Ok(());
                };
                send_model_request(
                    stdin,
                    pending,
                    next_id,
                    PendingKind::ReloadModels {
                        session_id: session_id.clone(),
                    },
                    BackendOperation::Reload,
                    &session_id,
                    &option.config_id,
                    &option.current_value,
                )
                .await
            } else {
                send_request(
                    stdin,
                    pending,
                    next_id,
                    PendingKind::StartSession {
                        requested_model: None,
                    },
                    BackendOperation::Reload,
                    "session/new",
                    json!({"cwd": workspace.to_string_lossy(), "mcpServers": []}),
                )
                .await
            }
        }
        BackendCommand::SetSessionModel { session_id, model } => {
            let Some(option) = model_options.get(&session_id).cloned() else {
                let _ = events
                    .send(BackendEvent::RequestFailed {
                        operation: BackendOperation::SetSessionModel,
                        code: -32601,
                        message: "Devin session did not expose a model config option".to_owned(),
                    })
                    .await;
                return Ok(());
            };
            send_model_request(
                stdin,
                pending,
                next_id,
                PendingKind::SetModel {
                    session_id: session_id.clone(),
                    announce_session: false,
                },
                BackendOperation::SetSessionModel,
                &session_id,
                &option.config_id,
                &model,
            )
            .await
        }
        BackendCommand::ResumeSession {
            provider_session_id,
        } => {
            let (method, replay_history) = if capabilities.load_session {
                ("session/load", true)
            } else if capabilities.resume_session {
                ("session/resume", false)
            } else {
                let _ = events
                    .send(BackendEvent::RequestFailed {
                        operation: BackendOperation::ResumeSession,
                        code: -32601,
                        message: "Devin ACP did not advertise session resume".to_owned(),
                    })
                    .await;
                return Ok(());
            };
            if replay_history {
                replay.insert(provider_session_id.clone(), Vec::new());
            }
            send_request(
                stdin,
                pending,
                next_id,
                PendingKind::ResumeSession {
                    session_id: provider_session_id.clone(),
                    replay: replay_history,
                },
                BackendOperation::ResumeSession,
                method,
                json!({
                    "sessionId": provider_session_id,
                    "cwd": workspace.to_string_lossy(),
                    "mcpServers": [],
                }),
            )
            .await
        }
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => {
            if !capabilities.close_session {
                let _ = events.send(BackendEvent::SessionUnsubscribed).await;
                return Ok(());
            }
            send_request(
                stdin,
                pending,
                next_id,
                PendingKind::CloseSession,
                BackendOperation::UnsubscribeSession,
                "session/close",
                json!({"sessionId": provider_session_id}),
            )
            .await
        }
        BackendCommand::StartTurn {
            session_id,
            client_id,
            prompt,
            model: _,
        } => {
            let turn_id = client_id;
            let id = allocate_request_id(pending, next_id);
            write_json(
                stdin,
                &request(
                    id,
                    "session/prompt",
                    json!({
                        "sessionId": session_id,
                        "prompt": [{"type": "text", "text": prompt}],
                    }),
                ),
            )
            .await?;
            pending.insert(
                id,
                PendingRequest {
                    kind: PendingKind::StartTurn {
                        session_id: session_id.clone(),
                        turn_id: turn_id.clone(),
                    },
                    operation: BackendOperation::StartTurn,
                    sent_at: Instant::now(),
                },
            );
            active_turns.insert(session_id, turn_id.clone());
            let _ = events.send(BackendEvent::TurnAccepted { turn_id }).await;
            Ok(())
        }
        BackendCommand::SteerTurn { .. } => {
            let _ = events
                .send(BackendEvent::RequestFailed {
                    operation: BackendOperation::SteerTurn,
                    code: -32601,
                    message: "ACP does not define turn steering".to_owned(),
                })
                .await;
            Ok(())
        }
        BackendCommand::InterruptTurn { session_id, .. } => {
            write_json(
                stdin,
                &notification("session/cancel", json!({"sessionId": session_id})),
            )
            .await?;
            let _ = events.send(BackendEvent::InterruptAccepted).await;
            Ok(())
        }
        BackendCommand::ResolveApproval { id, decision } => {
            let options = permissions.remove(&approval_key(&id)).unwrap_or_default();
            let selected = match decision {
                ApprovalDecision::AcceptOnce => options.accept_once,
                ApprovalDecision::AcceptForSession => options.accept_always.or(options.accept_once),
                ApprovalDecision::Decline => options.decline,
            };
            let result = selected
                .map(|option_id| json!({"outcome": {"outcome": "selected", "optionId": option_id}}))
                .unwrap_or_else(|| json!({"outcome": {"outcome": "cancelled"}}));
            write_json(stdin, &response(id, result)).await
        }
        BackendCommand::Shutdown => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_message(
    message: Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    capabilities: &mut AcpCapabilities,
    initialized: &mut bool,
    active_turns: &mut HashMap<String, String>,
    replay: &mut HashMap<String, Vec<SessionHistoryItem>>,
    model_options: &mut HashMap<String, SessionModelOption>,
    permissions: &mut HashMap<String, PermissionOptions>,
) -> Result<(), ()> {
    if let Some(method) = message.get("method").and_then(Value::as_str) {
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        if let Some(id) = message.get("id").cloned() {
            if method == "session/request_permission" {
                let approval = normalize_permission(id.clone(), &params, permissions);
                events
                    .send(BackendEvent::ApprovalRequested(approval))
                    .await
                    .map_err(|_| ())?;
            } else {
                write_json(
                    stdin,
                    &error_response(
                        id,
                        -32601,
                        format!("Flock does not support ACP client method {method}"),
                    ),
                )
                .await
                .map_err(|_| ())?;
            }
        } else if method == "session/update" {
            normalize_update(&params, events, active_turns, replay).await?;
        }
        return Ok(());
    }

    let Some(id) = message.get("id").and_then(Value::as_u64) else {
        events
            .send(BackendEvent::ProtocolDiagnostic(
                "ACP response omitted a numeric request id".to_owned(),
            ))
            .await
            .map_err(|_| ())?;
        return Ok(());
    };
    let Some(request) = pending.remove(&id) else {
        events
            .send(BackendEvent::ProtocolDiagnostic(format!(
                "late or unknown ACP response id {id}"
            )))
            .await
            .map_err(|_| ())?;
        return Ok(());
    };
    if let Some(error) = message.get("error") {
        let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32000);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("ACP request failed")
            .to_owned();
        emit_request_failure(request, code, message, events, active_turns, model_options).await?;
        return Ok(());
    }
    let result = message.get("result").cloned().unwrap_or(Value::Null);
    match request.kind {
        PendingKind::Initialize => {
            let version = result
                .get("protocolVersion")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            if version != 1 {
                events
                    .send(BackendEvent::RequestFailed {
                        operation: BackendOperation::Initialize,
                        code: -32600,
                        message: format!("unsupported ACP protocol version {version}"),
                    })
                    .await
                    .map_err(|_| ())?;
                return Err(());
            }
            let caps = result.get("agentCapabilities").unwrap_or(&Value::Null);
            capabilities.load_session = caps
                .get("loadSession")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            capabilities.resume_session =
                capability_supported(caps.pointer("/sessionCapabilities/resume"));
            capabilities.close_session =
                capability_supported(caps.pointer("/sessionCapabilities/close"));
            capabilities.mcp = caps.get("mcpCapabilities").is_some();
            let info = result.get("agentInfo").unwrap_or(&Value::Null);
            let name = info
                .get("title")
                .or_else(|| info.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("Devin")
                .to_owned();
            let version = info
                .get("version")
                .and_then(Value::as_str)
                .map(str::to_owned);
            *initialized = true;
            events
                .send(BackendEvent::Ready(BackendIdentity {
                    provider: DEVIN_PROVIDER.to_owned(),
                    display_name: name,
                    version,
                    capabilities: capabilities.backend(),
                }))
                .await
                .map_err(|_| ())?;
        }
        PendingKind::StartSession { requested_model } => {
            let session_id = string(&result, "sessionId");
            let model_state = parse_model_options(&result);
            if let Some(model_state) = model_state.clone() {
                model_options.insert(session_id.clone(), model_state.clone());
                if let Some(requested_model) = requested_model
                    && requested_model != model_state.current_value
                    && model_state
                        .models
                        .iter()
                        .any(|model| model.id == requested_model)
                {
                    send_model_request(
                        stdin,
                        pending,
                        next_id,
                        PendingKind::SetModel {
                            session_id: session_id.clone(),
                            announce_session: true,
                        },
                        BackendOperation::SetSessionModel,
                        &session_id,
                        &model_state.config_id,
                        &requested_model,
                    )
                    .await
                    .map_err(|_| ())?;
                    return Ok(());
                }
            }
            announce_session_models(&session_id, model_state.as_ref(), events).await?;
        }
        PendingKind::ResumeSession {
            session_id,
            replay: replay_history,
        } => {
            let history = if replay_history {
                replay.remove(&session_id).unwrap_or_default()
            } else {
                Vec::new()
            };
            let model_state = parse_model_options(&result);
            if let Some(model_state) = model_state.clone() {
                model_options.insert(session_id.clone(), model_state);
            }
            events
                .send(BackendEvent::SessionResumed {
                    provider_session_id: session_id,
                    model: model_state
                        .as_ref()
                        .map(|state| state.current_value.clone())
                        .unwrap_or_default(),
                    history,
                })
                .await
                .map_err(|_| ())?;
            if let Some(model_state) = model_state {
                events
                    .send(BackendEvent::Models(model_state.models))
                    .await
                    .map_err(|_| ())?;
            }
        }
        PendingKind::SetModel {
            session_id,
            announce_session,
        } => {
            let Some(model_state) = parse_model_options(&result) else {
                events
                    .send(BackendEvent::RequestFailed {
                        operation: BackendOperation::SetSessionModel,
                        code: -32603,
                        message: "Devin returned no model options after selection".to_owned(),
                    })
                    .await
                    .map_err(|_| ())?;
                if announce_session {
                    announce_session_models(&session_id, model_options.get(&session_id), events)
                        .await?;
                } else if let Some(cached) = model_options.get(&session_id) {
                    events
                        .send(BackendEvent::Models(cached.models.clone()))
                        .await
                        .map_err(|_| ())?;
                }
                return Ok(());
            };
            model_options.insert(session_id.clone(), model_state.clone());
            if announce_session {
                announce_session_models(&session_id, Some(&model_state), events).await?;
            } else {
                events
                    .send(BackendEvent::Models(model_state.models))
                    .await
                    .map_err(|_| ())?;
            }
        }
        PendingKind::ReloadModels { session_id } => {
            let Some(model_state) = parse_model_options(&result) else {
                events
                    .send(BackendEvent::RequestFailed {
                        operation: BackendOperation::Reload,
                        code: -32603,
                        message: "Devin returned no model options during refresh".to_owned(),
                    })
                    .await
                    .map_err(|_| ())?;
                return Ok(());
            };
            model_options.insert(session_id, model_state.clone());
            events
                .send(BackendEvent::Models(model_state.models))
                .await
                .map_err(|_| ())?;
        }
        PendingKind::StartTurn {
            session_id,
            turn_id,
        } => {
            active_turns.remove(&session_id);
            let reason = string(&result, "stopReason");
            let outcome = match reason.as_str() {
                "end_turn" => TurnOutcome::Completed,
                "cancelled" => TurnOutcome::Interrupted,
                _ => TurnOutcome::Failed,
            };
            let error =
                (outcome == TurnOutcome::Failed).then(|| format!("Devin stopped: {reason}"));
            events
                .send(BackendEvent::TurnCompleted {
                    turn_id,
                    outcome,
                    error,
                })
                .await
                .map_err(|_| ())?;
        }
        PendingKind::CloseSession => {
            events
                .send(BackendEvent::SessionUnsubscribed)
                .await
                .map_err(|_| ())?;
        }
    }
    Ok(())
}

async fn emit_request_failure(
    request: PendingRequest,
    code: i64,
    message: String,
    events: &mpsc::Sender<BackendEvent>,
    active_turns: &mut HashMap<String, String>,
    model_options: &HashMap<String, SessionModelOption>,
) -> Result<(), ()> {
    events
        .send(BackendEvent::RequestFailed {
            operation: request.operation,
            code,
            message: message.clone(),
        })
        .await
        .map_err(|_| ())?;
    match request.kind {
        PendingKind::StartTurn {
            session_id,
            turn_id,
        } => {
            active_turns.remove(&session_id);
            events
                .send(BackendEvent::TurnCompleted {
                    turn_id,
                    outcome: TurnOutcome::Failed,
                    error: Some(message),
                })
                .await
                .map_err(|_| ())?;
        }
        PendingKind::SetModel {
            session_id,
            announce_session: true,
        } => {
            announce_session_models(&session_id, model_options.get(&session_id), events).await?;
        }
        PendingKind::SetModel {
            session_id,
            announce_session: false,
        } => {
            events
                .send(BackendEvent::Models(
                    model_options
                        .get(&session_id)
                        .map(|state| state.models.clone())
                        .unwrap_or_default(),
                ))
                .await
                .map_err(|_| ())?;
        }
        _ => {}
    }
    Ok(())
}

fn capability_supported(value: Option<&Value>) -> bool {
    value.is_some_and(|value| match value {
        Value::Bool(supported) => *supported,
        Value::Null => false,
        _ => true,
    })
}

async fn normalize_update(
    params: &Value,
    events: &mpsc::Sender<BackendEvent>,
    active_turns: &HashMap<String, String>,
    replay: &mut HashMap<String, Vec<SessionHistoryItem>>,
) -> Result<(), ()> {
    let session_id = string(params, "sessionId");
    let update = params.get("update").unwrap_or(&Value::Null);
    let kind = string(update, "sessionUpdate");
    let turn_id = active_turns
        .get(&session_id)
        .cloned()
        .unwrap_or_else(|| format!("history:{session_id}"));
    match kind.as_str() {
        "agent_message_chunk" | "user_message_chunk" => {
            let text = update
                .pointer("/content/text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if text.is_empty() {
                return Ok(());
            }
            let item_id = update
                .get("messageId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{kind}:{turn_id}"));
            if let Some(history) = replay.get_mut(&session_id) {
                let item_kind = if kind == "user_message_chunk" {
                    ItemKind::User
                } else {
                    ItemKind::Assistant
                };
                if let Some(existing) = history.iter_mut().find(|item| item.item.id == item_id) {
                    existing.item.body.push_str(&text);
                } else {
                    history.push(SessionHistoryItem {
                        turn_id,
                        item: NormalizedItem {
                            id: item_id,
                            kind: item_kind,
                            title: if item_kind == ItemKind::User {
                                "YOU"
                            } else {
                                "ASSISTANT"
                            }
                            .to_owned(),
                            body: text,
                            status: ItemStatus::Complete,
                        },
                    });
                }
            } else if kind == "agent_message_chunk" {
                events
                    .send(BackendEvent::ItemDelta {
                        turn_id,
                        item_id,
                        kind: DeltaKind::Assistant,
                        delta: text,
                    })
                    .await
                    .map_err(|_| ())?;
            }
        }
        "tool_call" => {
            let item = tool_item(update, false);
            events
                .send(BackendEvent::ItemStarted { turn_id, item })
                .await
                .map_err(|_| ())?;
        }
        "tool_call_update" => {
            let completed = matches!(string(update, "status").as_str(), "completed" | "failed");
            let item = tool_item(update, completed);
            let event = if completed {
                BackendEvent::ItemCompleted { turn_id, item }
            } else {
                BackendEvent::ItemStarted { turn_id, item }
            };
            events.send(event).await.map_err(|_| ())?;
        }
        "plan" => {
            let plan = update
                .get("entries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|entry| {
                    format!(
                        "- [{}] {}",
                        string(entry, "status"),
                        string(entry, "content")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            events
                .send(BackendEvent::TurnPlan { turn_id, plan })
                .await
                .map_err(|_| ())?;
        }
        _ => {}
    }
    Ok(())
}

fn normalize_permission(
    id: Value,
    params: &Value,
    permissions: &mut HashMap<String, PermissionOptions>,
) -> ApprovalRequest {
    let mut choices = PermissionOptions::default();
    for option in params
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let option_id = string(option, "optionId");
        match string(option, "kind").as_str() {
            "allow_once" => choices.accept_once = Some(option_id),
            "allow_always" => choices.accept_always = Some(option_id),
            "reject_once" | "reject_always" if choices.decline.is_none() => {
                choices.decline = Some(option_id)
            }
            _ => {}
        }
    }
    permissions.insert(approval_key(&id), choices);
    let tool = params.get("toolCall").unwrap_or(&Value::Null);
    let kind = match string(tool, "kind").as_str() {
        "execute" => ApprovalKind::Command,
        "edit" | "delete" | "move" => ApprovalKind::FileChange,
        _ => ApprovalKind::Other,
    };
    ApprovalRequest {
        id,
        method: "session/request_permission".to_owned(),
        kind,
        title: tool
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("Devin tool request")
            .to_owned(),
        detail: tool
            .get("rawInput")
            .map(pretty)
            .unwrap_or_else(|| string(tool, "toolCallId")),
    }
}

fn tool_item(update: &Value, completed: bool) -> NormalizedItem {
    let status = match string(update, "status").as_str() {
        "failed" => ItemStatus::Failed,
        "completed" => ItemStatus::Complete,
        _ if completed => ItemStatus::Complete,
        _ => ItemStatus::Running,
    };
    NormalizedItem {
        id: string(update, "toolCallId"),
        kind: ItemKind::Tool,
        title: update
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("TOOL")
            .to_owned(),
        body: tool_content(update),
        status,
    }
}

fn tool_content(update: &Value) -> String {
    update
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            if item.get("type").and_then(Value::as_str) == Some("diff") {
                Some(format!(
                    "{}\n{}",
                    string(item, "path"),
                    string(item, "newText")
                ))
            } else {
                item.pointer("/content/text")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_model_options(result: &Value) -> Option<SessionModelOption> {
    let option = result
        .get("configOptions")
        .and_then(Value::as_array)?
        .iter()
        .find(|option| {
            option.get("category").and_then(Value::as_str) == Some("model")
                && option.get("type").and_then(Value::as_str) == Some("select")
        })?;
    let config_id = string(option, "id");
    let current_value = string(option, "currentValue");
    if config_id.is_empty() {
        return None;
    }
    let models = option
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let id = string(model, "value");
            if id.is_empty() {
                return None;
            }
            Some(ModelInfo {
                display_name: model
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or(&id)
                    .to_owned(),
                description: string(model, "description"),
                is_default: id == current_value,
                id,
            })
        })
        .collect();
    Some(SessionModelOption {
        config_id,
        current_value,
        models,
    })
}

async fn announce_session_models(
    session_id: &str,
    model_state: Option<&SessionModelOption>,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), ()> {
    events
        .send(BackendEvent::SessionCreated {
            provider_session_id: session_id.to_owned(),
            model: model_state
                .map(|state| state.current_value.clone())
                .unwrap_or_default(),
        })
        .await
        .map_err(|_| ())?;
    events
        .send(BackendEvent::Models(
            model_state
                .map(|state| state.models.clone())
                .unwrap_or_default(),
        ))
        .await
        .map_err(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn send_model_request(
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    kind: PendingKind,
    operation: BackendOperation,
    session_id: &str,
    config_id: &str,
    model: &str,
) -> std::io::Result<()> {
    send_request(
        stdin,
        pending,
        next_id,
        kind,
        operation,
        "session/set_config_option",
        json!({
            "sessionId": session_id,
            "configId": config_id,
            "value": model,
        }),
    )
    .await
}

async fn send_request(
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    kind: PendingKind,
    operation: BackendOperation,
    method: &str,
    params: Value,
) -> std::io::Result<()> {
    let id = allocate_request_id(pending, next_id);
    write_json(stdin, &request(id, method, params)).await?;
    pending.insert(
        id,
        PendingRequest {
            kind,
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

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn response(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: Value, code: i64, message: String) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

async fn write_json(stdin: &mut ChildStdin, value: &Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    stdin.write_all(&bytes).await?;
    stdin.flush().await
}

fn string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn approval_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use serde_json::json;

    use super::{BackendConfig, capability_supported, normalize_permission};
    use crate::backend::ApprovalKind;

    #[test]
    fn default_launch_uses_the_acp_subcommand() {
        let config = BackendConfig::devin(PathBuf::from("devin"), PathBuf::from("/tmp"));
        assert_eq!(config.program, PathBuf::from("devin"));
        assert_eq!(config.args, vec!["acp"]);
    }

    #[test]
    fn false_capability_values_are_not_supported() {
        assert!(!capability_supported(Some(&json!(false))));
        assert!(!capability_supported(Some(&json!(null))));
        assert!(capability_supported(Some(&json!({}))));
        assert!(capability_supported(Some(&json!(true))));
    }

    #[test]
    fn permission_kind_is_normalized_from_the_tool() {
        let mut permissions = HashMap::new();
        let approval = normalize_permission(
            json!("permission-1"),
            &json!({
                "toolCall": {
                    "toolCallId": "tool-1",
                    "kind": "edit",
                    "title": "Edit file"
                },
                "options": [{
                    "optionId": "allow-once",
                    "name": "Allow",
                    "kind": "allow_once"
                }]
            }),
            &mut permissions,
        );
        assert_eq!(approval.kind, ApprovalKind::FileChange);
    }
}
