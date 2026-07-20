use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{ChildStdin, Command},
    sync::mpsc,
    time::{Instant, interval, timeout},
};
use uuid::Uuid;

use crate::backend::{
    BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
    BackendIdentity, BackendOperation, CapabilitySupport, DEVIN_PROVIDER, DeltaKind, ItemKind,
    ItemStatus, ModelInfo, NormalizedItem, SessionHistoryItem, TurnOutcome,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DEVIN_CALLBACK_PORT: u16 = 59_653;
const DEVIN_CALLBACK_PATH: &str = "/callback";
const DEVIN_WEBAPP_URL: &str = "https://app.devin.ai";
const DEVIN_API_URL: &str = "https://api.devin.ai";

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub workspace: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
    oauth: OAuthConfig,
}

#[derive(Clone, Debug)]
struct OAuthConfig {
    webapp_url: String,
    api_url: String,
    callback_port: u16,
}

impl BackendConfig {
    #[must_use]
    pub fn devin(program: PathBuf, workspace: PathBuf) -> Self {
        Self::for_command(
            program,
            vec!["--permission-mode".into(), "dangerous".into(), "acp".into()],
            workspace,
        )
    }

    #[must_use]
    pub fn for_command(program: PathBuf, args: Vec<OsString>, workspace: PathBuf) -> Self {
        Self {
            program,
            args,
            workspace,
            environment: Vec::new(),
            oauth: OAuthConfig {
                webapp_url: DEVIN_WEBAPP_URL.to_owned(),
                api_url: DEVIN_API_URL.to_owned(),
                callback_port: DEVIN_CALLBACK_PORT,
            },
        }
    }

    #[must_use]
    pub fn with_api_key(mut self, api_key: Option<&str>) -> Self {
        if let Some(api_key) = api_key {
            self.environment
                .push(("DEVIN_API_KEY".into(), api_key.into()));
        }
        self
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
            context_compaction: CapabilitySupport::Unsupported,
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

/// Starts the native Devin agent through its ACP server.
///
/// # Errors
///
/// Returns an error when the child process cannot be spawned or its standard
/// streams are unavailable.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let mut child = Command::new(&config.program)
        .args(&config.args)
        .envs(config.environment.iter().cloned())
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
                "name": "nakode",
                "title": "Nakode",
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
        oauth: config.oauth,
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
    oauth: OAuthConfig,
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
        oauth,
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
                    &oauth,
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
                                    &mut deferred, &mut stdin, &events, &workspace, &oauth,
                                    &mut runtime,
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
    oauth: &OAuthConfig,
    runtime: &mut AcpRuntime,
) -> Result<(), ()> {
    while let Some(command) = commands.pop_front() {
        if let Err(error) = handle_command(command, stdin, events, workspace, oauth, runtime).await
        {
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
    oauth: &OAuthConfig,
    runtime: &mut AcpRuntime,
) -> std::io::Result<()> {
    let AcpRuntime {
        pending,
        next_id,
        capabilities,
        ..
    } = runtime;
    match command {
        BackendCommand::BeginAuthentication => {
            tokio::spawn(authenticate_devin(oauth.clone(), events.clone()));
            Ok(())
        }
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
        BackendCommand::CompactSession { .. } => {
            let _ = events
                .send(BackendEvent::RequestFailed {
                    operation: BackendOperation::CompactSession,
                    code: -32601,
                    message: "ACP does not define manual context compression".to_owned(),
                })
                .await;
            Ok(())
        }
        BackendCommand::ResolveApproval { .. }
        | BackendCommand::ResolveQuestion { .. }
        | BackendCommand::Shutdown => Ok(()),
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
    if !option.models.iter().any(|candidate| candidate.id == model) {
        let _ = events
            .send(BackendEvent::RequestFailed {
                operation: BackendOperation::SetSessionModel,
                code: -32602,
                message: format!("Devin did not advertise model {model}"),
            })
            .await;
        return Ok(());
    }
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
            let message = format!("Nakode does not support ACP client method {method}");
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
        .map(|model| {
            let id = string(model, "value");
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                is_default: id == current_value,
                id,
            }
        })
        .filter(|model| !model.id.is_empty())
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

async fn authenticate_devin(config: OAuthConfig, events: mpsc::Sender<BackendEvent>) {
    if let Err(message) = run_devin_oauth(&config, &events).await {
        let _ = events
            .send(BackendEvent::RequestFailed {
                operation: BackendOperation::Authenticate,
                code: -32003,
                message,
            })
            .await;
    }
}

pub(super) async fn authenticate_native(events: mpsc::Sender<BackendEvent>) {
    authenticate_devin(
        OAuthConfig {
            webapp_url: DEVIN_WEBAPP_URL.to_owned(),
            api_url: DEVIN_API_URL.to_owned(),
            callback_port: DEVIN_CALLBACK_PORT,
        },
        events,
    )
    .await;
}

async fn run_devin_oauth(
    config: &OAuthConfig,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), String> {
    let listener = bind_callback_listener(config.callback_port).await?;
    let callback_address = listener
        .local_addr()
        .map_err(|error| format!("failed to inspect Devin OAuth callback listener: {error}"))?;
    let redirect_uri = format!(
        "http://127.0.0.1:{}{DEVIN_CALLBACK_PATH}",
        callback_address.port()
    );
    let state = Uuid::now_v7().to_string();
    let verifier = pkce_verifier();
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let authentication_url =
        devin_authentication_url(&config.webapp_url, &redirect_uri, &state, &challenge)?;

    events
        .send(BackendEvent::AuthenticationChallenge {
            login_id: state.clone(),
            verification_url: authentication_url,
            user_code: String::new(),
        })
        .await
        .map_err(|_| "provider authentication channel closed".to_owned())?;

    let authorization_code = timeout(
        AUTHENTICATION_TIMEOUT,
        receive_authorization_code(&listener, &state),
    )
    .await
    .map_err(|_| "Devin authentication timed out after 10 minutes".to_owned())??;
    let token = exchange_devin_token(&config.api_url, &authorization_code, &verifier).await?;
    let expires_at = jwt_expiry(&token);
    events
        .send(BackendEvent::AuthenticationCompleted {
            kind: "oauth_pkce".to_owned(),
            metadata: json!({
                "token": token,
                "expires_at": expires_at,
                "api_endpoint": config.api_url,
            }),
        })
        .await
        .map_err(|_| "provider authentication channel closed".to_owned())
}

async fn bind_callback_listener(preferred_port: u16) -> Result<TcpListener, String> {
    let preferred = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), preferred_port);
    match TcpListener::bind(preferred).await {
        Ok(listener) => Ok(listener),
        Err(_) => TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .map_err(|error| format!("failed to bind Devin OAuth callback listener: {error}")),
    }
}

fn pkce_verifier() -> String {
    format!(
        "{}{}{}",
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple()
    )
}

fn devin_authentication_url(
    webapp_url: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(webapp_url)
        .map_err(|error| format!("invalid Devin web application URL: {error}"))?;
    url.set_path("/auth/cli/continue");
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("state", state)
        .append_pair("prompt", "select_account")
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256");
    Ok(url.into())
}

async fn receive_authorization_code(
    listener: &TcpListener,
    expected_state: &str,
) -> Result<String, String> {
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("failed to accept Devin OAuth callback: {error}"))?;
        match parse_callback(&mut stream, expected_state).await {
            Ok(code) => {
                respond_to_callback(
                    &mut stream,
                    "200 OK",
                    "Devin authentication complete. You can return to Nakode.",
                )
                .await;
                return Ok(code);
            }
            Err(error) => {
                respond_to_callback(&mut stream, "400 Bad Request", &error).await;
            }
        }
    }
}

async fn parse_callback(stream: &mut TcpStream, expected_state: &str) -> Result<String, String> {
    let mut request = vec![0_u8; 8_192];
    let bytes_read = stream
        .read(&mut request)
        .await
        .map_err(|error| format!("failed to read Devin OAuth callback: {error}"))?;
    let request = std::str::from_utf8(&request[..bytes_read])
        .map_err(|_| "Devin OAuth callback was not valid UTF-8".to_owned())?;
    let request_target = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| "Devin OAuth callback did not contain a request target".to_owned())?;
    let url = reqwest::Url::parse(&format!("http://localhost{request_target}"))
        .map_err(|error| format!("invalid Devin OAuth callback URL: {error}"))?;
    if url.path() != DEVIN_CALLBACK_PATH {
        return Err("Unexpected Devin OAuth callback path".to_owned());
    }
    let parameters = url.query_pairs().collect::<HashMap<_, _>>();
    let returned_state = parameters
        .get("state")
        .ok_or_else(|| "Devin OAuth callback did not contain state".to_owned())?;
    if returned_state != expected_state {
        return Err("Devin OAuth callback state did not match".to_owned());
    }
    parameters
        .get("code")
        .filter(|code| !code.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| "Devin OAuth callback did not contain an authorization code".to_owned())
}

async fn respond_to_callback(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!("<html><body><p>{message}</p></body></html>");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn exchange_devin_token(
    api_url: &str,
    authorization_code: &str,
    verifier: &str,
) -> Result<String, String> {
    let endpoint = format!("{}/auth/cli/token", api_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .post(endpoint)
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&json!({
            "code": authorization_code,
            "code_verifier": verifier,
        }))
        .send()
        .await
        .map_err(|error| format!("Devin CLI token exchange failed: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        let detail = response.text().await.unwrap_or_default();
        return Err(format!(
            "Devin CLI token exchange failed: {status} {}",
            detail.trim()
        ));
    }
    let payload = response
        .json::<Value>()
        .await
        .map_err(|error| format!("invalid Devin CLI token response: {error}"))?;
    payload
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| "Devin CLI token exchange returned an empty token".to_owned())
}

fn jwt_expiry(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice::<Value>(&decoded)
        .ok()?
        .get("exp")?
        .as_u64()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use base64::Engine as _;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };

    use super::{
        BackendConfig, OAuthConfig, actionable_stderr_warning, capability_supported,
        parse_model_options, permission_acceptance, run_devin_oauth,
    };
    use crate::backend::BackendEvent;

    #[test]
    fn default_launch_uses_devin_dangerous_permission_mode() {
        let config = BackendConfig::devin(PathBuf::from("devin"), PathBuf::from("/tmp"));
        assert_eq!(config.program, PathBuf::from("devin"));
        assert_eq!(config.args, vec!["--permission-mode", "dangerous", "acp"]);
    }

    #[test]
    fn api_key_is_only_added_to_the_devin_child_environment() {
        let config = BackendConfig::devin(PathBuf::from("devin"), PathBuf::from("/tmp"))
            .with_api_key(Some("secret-token"));
        assert_eq!(
            config.environment,
            vec![("DEVIN_API_KEY".into(), "secret-token".into())]
        );
    }

    #[tokio::test]
    async fn oauth_uses_pkce_callback_and_exchanges_the_authorization_code() {
        let token_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind token fixture");
        let token_address = token_listener.local_addr().expect("token fixture address");
        let token_server = tokio::spawn(async move {
            let (mut connection, _) = token_listener.accept().await.expect("accept token request");
            let mut request = vec![0_u8; 8_192];
            let bytes_read = connection
                .read(&mut request)
                .await
                .expect("read token request");
            let request = String::from_utf8_lossy(&request[..bytes_read]);
            assert!(request.starts_with("POST /auth/cli/token HTTP/1.1"));
            assert!(request.contains("\"code\":\"fixture-code\""));
            assert!(request.contains("\"code_verifier\":"));
            let payload =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"exp":2000000000}"#);
            let body = format!(r#"{{"token":"header.{payload}.signature"}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            connection
                .write_all(response.as_bytes())
                .await
                .expect("write token response");
        });
        let config = OAuthConfig {
            webapp_url: "https://app.example.test".to_owned(),
            api_url: format!("http://{token_address}"),
            callback_port: 0,
        };
        let (events, mut event_rx) = mpsc::channel(4);
        let oauth = tokio::spawn(async move { run_devin_oauth(&config, &events).await });

        let challenge = event_rx.recv().await.expect("authentication challenge");
        let (verification_url, state, redirect_uri) = match challenge {
            BackendEvent::AuthenticationChallenge {
                verification_url,
                login_id,
                user_code,
            } => {
                assert!(user_code.is_empty());
                let url = reqwest::Url::parse(&verification_url).expect("authentication URL");
                let parameters = url
                    .query_pairs()
                    .collect::<std::collections::HashMap<_, _>>();
                assert_eq!(
                    parameters.get("state").map(AsRef::as_ref),
                    Some(login_id.as_str())
                );
                assert_eq!(
                    parameters.get("code_challenge_method").map(AsRef::as_ref),
                    Some("S256")
                );
                let redirect_uri = parameters
                    .get("redirect_uri")
                    .expect("redirect URI")
                    .to_string();
                (verification_url, login_id, redirect_uri)
            }
            event => panic!("unexpected authentication event: {event:?}"),
        };
        assert!(verification_url.starts_with("https://app.example.test/auth/cli/continue?"));
        let redirect = reqwest::Url::parse(&redirect_uri).expect("redirect URL");
        let mut callback = TcpStream::connect((
            redirect.host_str().expect("redirect host"),
            redirect.port().expect("redirect port"),
        ))
        .await
        .expect("connect callback");
        callback
            .write_all(
                format!(
                    "GET /callback?code=fixture-code&state={state} HTTP/1.1\r\nHost: localhost\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("write callback");

        oauth.await.expect("OAuth task").expect("OAuth succeeds");
        token_server.await.expect("token server");
        assert!(matches!(
            event_rx.recv().await,
            Some(BackendEvent::AuthenticationCompleted { kind, metadata })
                if kind == "oauth_pkce"
                    && metadata["token"] == "header.eyJleHAiOjIwMDAwMDAwMDB9.signature"
                    && metadata["expires_at"] == 2_000_000_000_u64
        ));
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
