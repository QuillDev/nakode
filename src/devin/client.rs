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
    time::{Instant, interval},
};

use crate::backend::{
    BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
    BackendIdentity, BackendOperation, CapabilitySupport, DEVIN_PROVIDER, DeltaKind, ItemKind,
    ItemStatus, ModelInfo, NormalizedItem, SessionHistoryItem, TurnOutcome,
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
    #[must_use]
    pub fn devin(program: PathBuf, workspace: PathBuf) -> Self {
        Self {
            program,
            args: vec!["--permission-mode".into(), "dangerous".into(), "acp".into()],
            workspace,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AcpCapabilities {
    load_session: Option<()>,
    resume_session: Option<()>,
    close_session: Option<()>,
    mcp: Option<()>,
}

impl AcpCapabilities {
    fn backend(&self) -> BackendCapabilities {
        BackendCapabilities {
            resume: if self.load_session.is_some() || self.resume_session.is_some() {
                CapabilitySupport::Supported
            } else {
                CapabilitySupport::Unsupported
            },
            steering: CapabilitySupport::Unsupported,
            interruption: CapabilitySupport::Supported,
            model_catalog: CapabilitySupport::Supported,
            models_require_session: CapabilitySupport::Supported,
            session_model_config: CapabilitySupport::Supported,
            approvals: CapabilitySupport::Supported,
            native_tools: CapabilitySupport::Supported,
            mcp: self.mcp.map_or(CapabilitySupport::Unsupported, |()| {
                CapabilitySupport::Supported
            }),
            close_session: self
                .close_session
                .map_or(CapabilitySupport::Unsupported, |()| {
                    CapabilitySupport::Supported
                }),
        }
    }
}

struct AcpRuntime {
    pending: HashMap<u64, PendingRequest>,
    next_id: u64,
    capabilities: AcpCapabilities,
    initialized: bool,
    active_turns: HashMap<String, String>,
    replay: HashMap<String, Vec<SessionHistoryItem>>,
    model_options: HashMap<String, SessionModelOption>,
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

/// Starts a Devin ACP adapter.
///
/// # Errors
///
/// Returns an error when the child process cannot be spawned or its standard
/// streams are unavailable.
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
        &json!({
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {
                "name": "nako-agent",
                "title": "Nako Agent",
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

    let task = tokio::spawn(run_supervisor(SupervisorInput {
        child,
        stdin,
        stdout: BufReader::new(stdout).lines(),
        commands: command_rx,
        events: event_tx,
        stderr: stderr_rx,
        pending,
        workspace: config.workspace,
    }));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

struct SupervisorInput {
    child: tokio::process::Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
    stderr: mpsc::Receiver<String>,
    pending: HashMap<u64, PendingRequest>,
    workspace: PathBuf,
}

async fn run_supervisor(input: SupervisorInput) {
    let SupervisorInput {
        child,
        mut stdin,
        mut stdout,
        mut commands,
        events,
        mut stderr,
        pending,
        workspace,
    } = input;
    let mut deferred = VecDeque::new();
    let mut runtime = AcpRuntime {
        pending,
        next_id: 2,
        capabilities: AcpCapabilities::default(),
        initialized: false,
        active_turns: HashMap::new(),
        replay: HashMap::new(),
        model_options: HashMap::new(),
    };
    let mut timeout_tick = interval(Duration::from_secs(1));
    let mut last_error = None;
    let mut shutdown = false;

    'supervisor: loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    shutdown = true;
                    break;
                };
                if matches!(command, BackendCommand::Shutdown) {
                    shutdown = true;
                    break;
                }
                if !runtime.initialized {
                    deferred.push_back(command);
                    continue;
                }
                if let Err(error) = handle_command(
                    command,
                    &mut stdin,
                    &events,
                    &workspace,
                    &mut runtime,
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
                                &mut runtime,
                            ).await.is_err() {
                                break;
                            }
                            if runtime.initialized
                                && drain_deferred(
                                    &mut deferred, &mut stdin, &events, &workspace, &mut runtime,
                                ).await.is_err()
                            {
                                break 'supervisor;
                            }
                        }
                        Err(error) => {
                            report_malformed_json(&events, &line, &error).await;
                        }
                    },
                    Ok(None) => break,
                    Err(error) => {
                        last_error = Some(format!("stdout read failed: {error}"));
                        break;
                    }
                }
            }
            line = stderr.recv() => {
                record_stderr(line, &events, &mut last_error).await;
            }
            _ = timeout_tick.tick() => report_timeouts(&mut runtime, &events).await,
        }
    }

    finish(child, stdin, runtime.pending, &events, shutdown, last_error).await;
}

async fn drain_deferred(
    commands: &mut VecDeque<BackendCommand>,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    workspace: &std::path::Path,
    runtime: &mut AcpRuntime,
) -> Result<(), ()> {
    while let Some(command) = commands.pop_front() {
        if let Err(error) = handle_command(command, stdin, events, workspace, runtime).await {
            let _ = events
                .send(BackendEvent::Disconnected {
                    reason: format!("failed to write to Devin ACP: {error}"),
                })
                .await;
            return Err(());
        }
    }
    Ok(())
}

async fn report_timeouts(runtime: &mut AcpRuntime, events: &mpsc::Sender<BackendEvent>) {
    let now = Instant::now();
    let timed_out = runtime
        .pending
        .iter()
        .filter(|(_, request)| now.duration_since(request.sent_at) >= REQUEST_TIMEOUT)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for id in timed_out {
        if let Some(request) = runtime.pending.remove(&id) {
            let message = format!(
                "request {id} timed out after {}s",
                REQUEST_TIMEOUT.as_secs()
            );
            let _ = emit_request_failure(
                request,
                -32001,
                message,
                events,
                &mut runtime.active_turns,
                &runtime.model_options,
            )
            .await;
        }
    }
}

async fn report_malformed_json(
    events: &mpsc::Sender<BackendEvent>,
    line: &str,
    error: &serde_json::Error,
) {
    let preview: String = line.chars().take(180).collect();
    let _ = events
        .send(BackendEvent::ProtocolDiagnostic(format!(
            "malformed ACP JSON ({error}): {preview}"
        )))
        .await;
}

async fn record_stderr(
    line: Option<String>,
    events: &mpsc::Sender<BackendEvent>,
    last_error: &mut Option<String>,
) {
    if let Some(line) = line {
        if actionable_stderr_warning(&line) {
            let _ = events
                .send(BackendEvent::Warning(format!("Devin: {line}")))
                .await;
        }
        *last_error = Some(line);
    }
}

async fn finish(
    mut child: tokio::process::Child,
    stdin: ChildStdin,
    pending: HashMap<u64, PendingRequest>,
    events: &mpsc::Sender<BackendEvent>,
    shutdown: bool,
    last_error: Option<String>,
) {
    drop(stdin);
    if shutdown {
        let _ = child.start_kill();
        let _ = child.wait().await;
    } else {
        let status = child.wait().await.ok();
        let mut reason = status.map_or_else(
            || "Devin ACP disconnected".to_owned(),
            |status| format!("Devin ACP exited with {status}"),
        );
        if let Some(stderr) = last_error {
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

async fn handle_command(
    command: BackendCommand,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    workspace: &std::path::Path,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let AcpRuntime {
        pending,
        next_id,
        capabilities,
        ..
    } = runtime;
    match command {
        BackendCommand::StartSession {
            model,
            instructions: _,
        } => {
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
            reload_models(session_id, stdin, events, workspace, runtime).await
        }
        BackendCommand::SetSessionModel { session_id, model } => {
            set_session_model(session_id, model, stdin, events, runtime).await
        }
        BackendCommand::ResumeSession {
            provider_session_id,
        } => resume_session(provider_session_id, stdin, events, workspace, runtime).await,
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => {
            if capabilities.close_session.is_none() {
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
        } => start_turn(session_id, client_id, prompt, stdin, events, runtime).await,
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
                &notification("session/cancel", &json!({"sessionId": session_id})),
            )
            .await?;
            let _ = events.send(BackendEvent::InterruptAccepted).await;
            Ok(())
        }
        BackendCommand::ResolveApproval { .. } | BackendCommand::Shutdown => Ok(()),
    }
}

async fn set_session_model(
    session_id: String,
    model: String,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let Some(option) = runtime.model_options.get(&session_id).cloned() else {
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
        &mut runtime.pending,
        &mut runtime.next_id,
        ModelRequest {
            kind: PendingKind::SetModel {
                session_id: session_id.clone(),
                announce_session: false,
            },
            operation: BackendOperation::SetSessionModel,
            session_id: &session_id,
            config_id: &option.config_id,
            model: &model,
        },
    )
    .await
}

async fn start_turn(
    session_id: String,
    turn_id: String,
    prompt: String,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let id = allocate_request_id(&runtime.pending, &mut runtime.next_id);
    write_json(
        stdin,
        &request(
            id,
            "session/prompt",
            &json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": prompt}],
            }),
        ),
    )
    .await?;
    runtime.pending.insert(
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
    runtime.active_turns.insert(session_id, turn_id.clone());
    let _ = events.send(BackendEvent::TurnAccepted { turn_id }).await;
    Ok(())
}

async fn reload_models(
    session_id: Option<String>,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    workspace: &std::path::Path,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let Some(session_id) = session_id else {
        return send_request(
            stdin,
            &mut runtime.pending,
            &mut runtime.next_id,
            PendingKind::StartSession {
                requested_model: None,
            },
            BackendOperation::Reload,
            "session/new",
            json!({"cwd": workspace.to_string_lossy(), "mcpServers": []}),
        )
        .await;
    };
    let Some(option) = runtime.model_options.get(&session_id).cloned() else {
        let _ = events
            .send(BackendEvent::RequestFailed {
                operation: BackendOperation::Reload,
                code: -32601,
                message: "Devin session did not expose a model config option".to_owned(),
            })
            .await;
        return Ok(());
    };
    send_model_request(
        stdin,
        &mut runtime.pending,
        &mut runtime.next_id,
        ModelRequest {
            kind: PendingKind::ReloadModels {
                session_id: session_id.clone(),
            },
            operation: BackendOperation::Reload,
            session_id: &session_id,
            config_id: &option.config_id,
            model: &option.current_value,
        },
    )
    .await
}

async fn resume_session(
    session_id: String,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    workspace: &std::path::Path,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let (method, replay_history) = if runtime.capabilities.load_session.is_some() {
        ("session/load", true)
    } else if runtime.capabilities.resume_session.is_some() {
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
        runtime.replay.insert(session_id.clone(), Vec::new());
    }
    send_request(
        stdin,
        &mut runtime.pending,
        &mut runtime.next_id,
        PendingKind::ResumeSession {
            session_id: session_id.clone(),
            replay: replay_history,
        },
        BackendOperation::ResumeSession,
        method,
        json!({
            "sessionId": session_id,
            "cwd": workspace.to_string_lossy(),
            "mcpServers": [],
        }),
    )
    .await
}

async fn process_method(
    message: &Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    runtime: &mut AcpRuntime,
) -> Result<(), ()> {
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .expect("caller verified the method");
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    if let Some(id) = message.get("id").cloned() {
        if method == "session/request_permission" {
            write_json(stdin, &response(&id, &permission_acceptance(&params)))
                .await
                .map_err(|_| ())?;
        } else {
            let message = format!("Nako Agent does not support ACP client method {method}");
            write_json(stdin, &error_response(&id, -32601, &message))
                .await
                .map_err(|_| ())?;
        }
    } else if method == "session/update" {
        normalize_update(&params, events, &runtime.active_turns, &mut runtime.replay).await?;
    }
    Ok(())
}

async fn init(
    result: &Value,
    events: &mpsc::Sender<BackendEvent>,
    capabilities: &mut AcpCapabilities,
    initialized: &mut bool,
) -> Result<(), ()> {
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    if protocol_version != 1 {
        events
            .send(BackendEvent::RequestFailed {
                operation: BackendOperation::Initialize,
                code: -32600,
                message: format!("unsupported ACP protocol version {protocol_version}"),
            })
            .await
            .map_err(|_| ())?;
        return Err(());
    }
    let caps = result.get("agentCapabilities").unwrap_or(&Value::Null);
    capabilities.load_session = caps
        .get("loadSession")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        .then_some(());
    capabilities.resume_session =
        capability_supported(caps.pointer("/sessionCapabilities/resume")).then_some(());
    capabilities.close_session =
        capability_supported(caps.pointer("/sessionCapabilities/close")).then_some(());
    capabilities.mcp = caps.get("mcpCapabilities").map(|_| ());
    let info = result.get("agentInfo").unwrap_or(&Value::Null);
    let display_name = info
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
            display_name,
            version,
            capabilities: capabilities.backend(),
        }))
        .await
        .map_err(|_| ())
}

async fn handle_started_session(
    requested_model: Option<String>,
    result: &Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    model_options: &mut HashMap<String, SessionModelOption>,
) -> Result<(), ()> {
    let session_id = string(result, "sessionId");
    let model_state = parse_model_options(result);
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
                ModelRequest {
                    kind: PendingKind::SetModel {
                        session_id: session_id.clone(),
                        announce_session: true,
                    },
                    operation: BackendOperation::SetSessionModel,
                    session_id: &session_id,
                    config_id: &model_state.config_id,
                    model: &requested_model,
                },
            )
            .await
            .map_err(|_| ())?;
            return Ok(());
        }
    }
    announce_session_models(&session_id, model_state.as_ref(), events).await
}

async fn handle_resumed_session(
    session_id: String,
    replay_history: bool,
    result: &Value,
    events: &mpsc::Sender<BackendEvent>,
    replay: &mut HashMap<String, Vec<SessionHistoryItem>>,
    model_options: &mut HashMap<String, SessionModelOption>,
) -> Result<(), ()> {
    let history = if replay_history {
        replay.remove(&session_id).unwrap_or_default()
    } else {
        Vec::new()
    };
    let model_state = parse_model_options(result);
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
    Ok(())
}

async fn handle_model_selection(
    session_id: &str,
    announce_session: bool,
    result: &Value,
    events: &mpsc::Sender<BackendEvent>,
    model_options: &mut HashMap<String, SessionModelOption>,
) -> Result<(), ()> {
    let Some(model_state) = parse_model_options(result) else {
        events
            .send(BackendEvent::RequestFailed {
                operation: BackendOperation::SetSessionModel,
                code: -32603,
                message: "Devin returned no model options after selection".to_owned(),
            })
            .await
            .map_err(|_| ())?;
        if announce_session {
            announce_session_models(session_id, model_options.get(session_id), events).await?;
        } else if let Some(cached) = model_options.get(session_id) {
            events
                .send(BackendEvent::Models(cached.models.clone()))
                .await
                .map_err(|_| ())?;
        }
        return Ok(());
    };
    model_options.insert(session_id.to_owned(), model_state.clone());
    if announce_session {
        announce_session_models(session_id, Some(&model_state), events).await
    } else {
        events
            .send(BackendEvent::Models(model_state.models))
            .await
            .map_err(|_| ())
    }
}

async fn handle_reloaded_models(
    session_id: String,
    result: &Value,
    events: &mpsc::Sender<BackendEvent>,
    model_options: &mut HashMap<String, SessionModelOption>,
) -> Result<(), ()> {
    let Some(model_state) = parse_model_options(result) else {
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
        .map_err(|_| ())
}

async fn handle_completed_turn(
    session_id: String,
    turn_id: String,
    result: &Value,
    events: &mpsc::Sender<BackendEvent>,
    active_turns: &mut HashMap<String, String>,
) -> Result<(), ()> {
    active_turns.remove(&session_id);
    let reason = string(result, "stopReason");
    let outcome = match reason.as_str() {
        "end_turn" => TurnOutcome::Completed,
        "cancelled" => TurnOutcome::Interrupted,
        _ => TurnOutcome::Failed,
    };
    let error = (outcome == TurnOutcome::Failed).then(|| format!("Devin stopped: {reason}"));
    events
        .send(BackendEvent::TurnCompleted {
            turn_id,
            outcome,
            error,
        })
        .await
        .map_err(|_| ())
}

async fn process_message(
    message: Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    runtime: &mut AcpRuntime,
) -> Result<(), ()> {
    if message.get("method").and_then(Value::as_str).is_some() {
        return process_method(&message, stdin, events, runtime).await;
    }
    let AcpRuntime {
        pending,
        next_id,
        capabilities,
        initialized,
        active_turns,
        replay,
        model_options,
        ..
    } = runtime;
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
        PendingKind::Initialize => init(&result, events, capabilities, initialized).await?,
        PendingKind::StartSession { requested_model } => {
            handle_started_session(
                requested_model,
                &result,
                stdin,
                events,
                pending,
                next_id,
                model_options,
            )
            .await?;
        }
        PendingKind::ResumeSession {
            session_id,
            replay: replay_history,
        } => {
            handle_resumed_session(
                session_id,
                replay_history,
                &result,
                events,
                replay,
                model_options,
            )
            .await?;
        }
        PendingKind::SetModel {
            session_id,
            announce_session,
        } => {
            handle_model_selection(
                &session_id,
                announce_session,
                &result,
                events,
                model_options,
            )
            .await?;
        }
        PendingKind::ReloadModels { session_id } => {
            handle_reloaded_models(session_id, &result, events, model_options).await?;
        }
        PendingKind::StartTurn {
            session_id,
            turn_id,
        } => {
            handle_completed_turn(session_id, turn_id, &result, events, active_turns).await?;
        }
        PendingKind::CloseSession => announce_closed(events).await?,
    }
    Ok(())
}

async fn announce_closed(events: &mpsc::Sender<BackendEvent>) -> Result<(), ()> {
    events
        .send(BackendEvent::SessionUnsubscribed)
        .await
        .map_err(|_| ())
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
                .map_or_else(|| format!("{kind}:{turn_id}"), str::to_owned);
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

fn permission_acceptance(params: &Value) -> Value {
    let mut accept_once = None;
    let mut accept_always = None;
    for option in params
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let option_id = string(option, "optionId");
        match string(option, "kind").as_str() {
            "allow_once" => accept_once = Some(option_id),
            "allow_always" => accept_always = Some(option_id),
            _ => {}
        }
    }
    accept_always.or(accept_once).map_or_else(
        || json!({"outcome": {"outcome": "cancelled"}}),
        |option_id| json!({"outcome": {"outcome": "selected", "optionId": option_id}}),
    )
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
    let mut models = option
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
                provider: DEVIN_PROVIDER.to_owned(),
                is_default: id == current_value,
                id,
            })
        })
        .collect::<Vec<_>>();
    if !current_value.is_empty() && !models.iter().any(|model| model.id == current_value) {
        models.push(ModelInfo {
            provider: DEVIN_PROVIDER.to_owned(),
            id: current_value.clone(),
            is_default: true,
        });
    }
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

struct ModelRequest<'a> {
    kind: PendingKind,
    operation: BackendOperation,
    session_id: &'a str,
    config_id: &'a str,
    model: &'a str,
}

async fn send_model_request(
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    request: ModelRequest<'_>,
) -> std::io::Result<()> {
    send_request(
        stdin,
        pending,
        next_id,
        request.kind,
        request.operation,
        "session/set_config_option",
        json!({
            "sessionId": request.session_id,
            "configId": request.config_id,
            "value": request.model,
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
    write_json(stdin, &request(id, method, &params)).await?;
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

fn request(id: u64, method: &str, params: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn notification(method: &str, params: &Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn response(id: &Value, result: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: &Value, code: i64, message: &str) -> Value {
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

fn actionable_stderr_warning(line: &str) -> bool {
    let line = line.to_ascii_lowercase();
    line.contains("without credentials")
        || line.contains("not authenticated")
        || line.contains("authentication required")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::{
        BackendConfig, actionable_stderr_warning, capability_supported, parse_model_options,
        permission_acceptance,
    };

    #[test]
    fn default_launch_uses_dangerous_permission_mode_with_acp() {
        let config = BackendConfig::devin(PathBuf::from("devin"), PathBuf::from("/tmp"));
        assert_eq!(config.program, PathBuf::from("devin"));
        assert_eq!(config.args, vec!["--permission-mode", "dangerous", "acp"]);
    }

    #[test]
    fn false_capability_values_are_not_supported() {
        assert!(!capability_supported(Some(&json!(false))));
        assert!(!capability_supported(Some(&json!(null))));
        assert!(capability_supported(Some(&json!({}))));
        assert!(capability_supported(Some(&json!(true))));
    }

    #[test]
    fn current_model_is_kept_when_acp_omits_model_options() {
        let option = parse_model_options(&json!({
            "configOptions": [{
                "id": "model",
                "category": "model",
                "type": "select",
                "currentValue": "swe-1-6-fast",
                "options": []
            }]
        }))
        .expect("model option");
        assert_eq!(option.models.len(), 1);
        assert_eq!(option.models[0].qualified_id(), "devin-acp/swe-1-6-fast");
        assert!(option.models[0].is_default);
    }

    #[test]
    fn only_actionable_devin_stderr_warnings_reach_the_transcript() {
        assert!(actionable_stderr_warning(
            "WARN creating session without credentials"
        ));
        assert!(!actionable_stderr_warning(
            "WARN MessageChain tree duplication: system prefix changed"
        ));
    }

    #[test]
    fn permission_requests_prefer_permanent_acceptance() {
        let acceptance = permission_acceptance(&json!({
            "options": [{
                "optionId": "allow-once",
                "name": "Allow",
                "kind": "allow_once"
            }, {
                "optionId": "allow-always",
                "name": "Always allow",
                "kind": "allow_always"
            }]
        }));
        assert_eq!(
            acceptance,
            json!({"outcome": {"outcome": "selected", "optionId": "allow-always"}})
        );
    }
}
