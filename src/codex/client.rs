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
    BackendIdentity, BackendOperation, CODEX_PROVIDER, CapabilitySupport,
};

use super::protocol::{
    RpcError, RpcMessage, normalize_notification, notification, parse_message, parse_models,
    parse_session_history, request, response,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub workspace: PathBuf,
    pub credential_home: Option<PathBuf>,
}

impl BackendConfig {
    #[must_use]
    pub fn codex(program: PathBuf, workspace: PathBuf) -> Self {
        Self {
            program,
            args: vec![
                "app-server".into(),
                "--stdio".into(),
                "--disable".into(),
                "multi_agent".into(),
            ],
            workspace,
            credential_home: None,
        }
    }

    #[must_use]
    pub fn with_credential_home(mut self, credential_home: PathBuf) -> Self {
        self.credential_home = Some(credential_home);
        self
    }
}

#[derive(Debug)]
struct PendingRequest {
    operation: BackendOperation,
    sent_at: Instant,
}

/// Starts a Codex app-server adapter.
///
/// # Errors
///
/// Returns an error when the child process cannot be spawned or its standard
/// streams are unavailable.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let mut command = Command::new(&config.program);
    command.args(&config.args).current_dir(&config.workspace);
    if let Some(credential_home) = &config.credential_home {
        command.env("CODEX_HOME", credential_home);
    }
    let mut child = command
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
        &json!({
            "clientInfo": {
                "name": "nako-agent",
                "title": "Nako Agent",
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

    let task = tokio::spawn(run_supervisor(SupervisorInput {
        child,
        stdin,
        stdout: BufReader::new(stdout).lines(),
        commands: command_rx,
        events: event_tx,
        stderr: stderr_rx,
        pending,
        workspace: config.workspace,
        next_id: initialize_id + 1,
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
    next_id: u64,
}

async fn run_supervisor(input: SupervisorInput) {
    let SupervisorInput {
        child,
        mut stdin,
        mut stdout,
        mut commands,
        events,
        mut stderr,
        mut pending,
        workspace,
        mut next_id,
    } = input;
    let mut initialized = false;
    let mut deferred_commands = VecDeque::new();
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
                                ).await.is_err() {
                                    break;
                                }
                                if initialized
                                    && drain_deferred_commands(
                                        &mut deferred_commands, &mut stdin, &mut pending,
                                        &mut next_id, &workspace, &events,
                                    ).await.is_err()
                                {
                                    break 'supervisor;
                                }
                            }
                            Err(error) => {
                                report_malformed_json(&events, &line, &error).await;
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
            line = stderr.recv() => last_stderr = line.or(last_stderr),
            _ = timeout_tick.tick() => report_timeouts(&mut pending, &events).await,
        }
    }

    finish(
        child,
        stdin,
        pending,
        &events,
        requested_shutdown,
        last_stderr,
    )
    .await;
}

async fn report_malformed_json(
    events: &mpsc::Sender<BackendEvent>,
    line: &str,
    error: &serde_json::Error,
) {
    let preview: String = line.chars().take(180).collect();
    let _ = events
        .send(BackendEvent::ProtocolDiagnostic(format!(
            "malformed Codex JSON ({error}): {preview}"
        )))
        .await;
}

async fn drain_deferred_commands(
    commands: &mut VecDeque<BackendCommand>,
    stdin: &mut ChildStdin,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    workspace: &std::path::Path,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), ()> {
    while let Some(command) = commands.pop_front() {
        if let Err(error) = handle_command(command, stdin, pending, next_id, workspace).await {
            let _ = events
                .send(BackendEvent::Disconnected {
                    reason: format!("failed to write to Codex: {error}"),
                })
                .await;
            return Err(());
        }
    }
    Ok(())
}

async fn report_timeouts(
    pending: &mut HashMap<u64, PendingRequest>,
    events: &mpsc::Sender<BackendEvent>,
) {
    let now = Instant::now();
    let timed_out = pending
        .iter()
        .filter(|(_, request)| now.duration_since(request.sent_at) >= REQUEST_TIMEOUT)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for id in timed_out {
        if let Some(request) = pending.remove(&id) {
            let _ = events
                .send(BackendEvent::RequestFailed {
                    operation: request.operation,
                    code: -32001,
                    message: format!(
                        "request {id} timed out after {}s",
                        REQUEST_TIMEOUT.as_secs()
                    ),
                })
                .await;
        }
    }
}

async fn finish(
    mut child: tokio::process::Child,
    stdin: ChildStdin,
    pending: HashMap<u64, PendingRequest>,
    events: &mpsc::Sender<BackendEvent>,
    requested_shutdown: bool,
    last_stderr: Option<String>,
) {
    drop(stdin);
    if requested_shutdown {
        let _ = child.start_kill();
        let _ = child.wait().await;
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
        BackendCommand::BeginAuthentication
            | BackendCommand::StartSession { .. }
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
) -> std::io::Result<()> {
    match command {
        BackendCommand::SetSessionModel { .. }
        | BackendCommand::ResolveApproval { .. }
        | BackendCommand::ResolveQuestion { .. }
        | BackendCommand::Shutdown => return Ok(()),
        _ => {}
    }

    let (operation, method, params) = command_request(command, workspace);
    send_request(stdin, pending, next_id, operation, method, params).await
}

fn command_request(
    command: BackendCommand,
    workspace: &std::path::Path,
) -> (BackendOperation, &'static str, Value) {
    match command {
        BackendCommand::Reload { .. } => (
            BackendOperation::Reload,
            "model/list",
            json!({"limit": 100}),
        ),
        BackendCommand::StartSession {
            model,
            instructions,
        } => (
            BackendOperation::StartSession,
            "thread/start",
            thread_start_params(workspace, model, instructions),
        ),
        BackendCommand::ResumeSession {
            provider_session_id,
        } => (
            BackendOperation::ResumeSession,
            "thread/resume",
            json!({
                "threadId": provider_session_id,
                "cwd": workspace.to_string_lossy(),
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
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
        BackendCommand::BeginAuthentication => (
            BackendOperation::Authenticate,
            "account/login/start",
            json!({"type": "chatgptDeviceCode"}),
        ),
        BackendCommand::SetSessionModel { .. }
        | BackendCommand::ResolveApproval { .. }
        | BackendCommand::ResolveQuestion { .. }
        | BackendCommand::Shutdown => unreachable!(),
    }
}

async fn process_message(
    message: RpcMessage,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    initialized: &mut bool,
) -> Result<(), ()> {
    if let Some(method) = message.method {
        process_incoming_method(
            &method,
            message.params.unwrap_or(Value::Null),
            message.id,
            stdin,
            events,
        )
        .await?;
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
    process_response(
        request.operation,
        result,
        stdin,
        events,
        pending,
        next_id,
        initialized,
    )
    .await
}

async fn process_incoming_method(
    method: &str,
    params: Value,
    id: Option<Value>,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), ()> {
    if let Some(id) = id {
        let response_value = if is_approval_method(method) {
            let decision = if matches!(method, "execCommandApproval" | "applyPatchApproval") {
                "approved"
            } else {
                "accept"
            };
            response(&id, Ok(json!({"decision": decision})))
        } else {
            response(
                &id,
                Err(RpcError {
                    code: -32601,
                    message: format!("Nako Agent does not support server request {method}"),
                    data: None,
                }),
            )
        };
        return write_json(stdin, &response_value).await.map_err(|_| ());
    }
    let event = if method == "account/login/completed" {
        Some(authentication_completed_event(&params))
    } else {
        normalize_notification(method, &params)
    };
    if let Some(event) = event {
        events.send(event).await.map_err(|_| ())?;
    }
    Ok(())
}

fn authentication_completed_event(params: &Value) -> BackendEvent {
    if params
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return BackendEvent::AuthenticationCompleted {
            kind: "chatgpt_device_code".to_owned(),
            metadata: json!({
                "credential_store": "codex_managed",
                "login_id": params.get("loginId").cloned().unwrap_or(Value::Null),
            }),
        };
    }
    BackendEvent::RequestFailed {
        operation: BackendOperation::Authenticate,
        code: -32000,
        message: params
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Codex device authentication failed")
            .to_owned(),
    }
}

async fn process_response(
    operation: BackendOperation,
    result: Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    initialized: &mut bool,
) -> Result<(), ()> {
    match operation {
        BackendOperation::Initialize => {
            initialize_response(&result, stdin, events, pending, next_id, initialized).await?;
        }
        BackendOperation::Authenticate => {
            events
                .send(BackendEvent::AuthenticationChallenge {
                    login_id: result
                        .get("loginId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    verification_url: result
                        .get("verificationUrl")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    user_code: result
                        .get("userCode")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                })
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

async fn initialize_response(
    result: &Value,
    stdin: &mut ChildStdin,
    events: &mpsc::Sender<BackendEvent>,
    pending: &mut HashMap<u64, PendingRequest>,
    next_id: &mut u64,
    initialized: &mut bool,
) -> Result<(), ()> {
    *initialized = true;
    write_json(stdin, &notification("initialized", None))
        .await
        .map_err(|_| ())?;
    let display_name = result
        .get("userAgent")
        .and_then(Value::as_str)
        .unwrap_or("Codex")
        .to_owned();
    events
        .send(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name,
            version: None,
            capabilities: BackendCapabilities {
                resume: CapabilitySupport::Supported,
                steering: CapabilitySupport::Supported,
                interruption: CapabilitySupport::Supported,
                model_catalog: CapabilitySupport::Supported,
                models_require_session: CapabilitySupport::Unsupported,
                session_model_config: CapabilitySupport::Unsupported,
                approvals: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                mcp: CapabilitySupport::Supported,
                close_session: CapabilitySupport::Supported,
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
    .map_err(|_| ())
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
    write_json(stdin, &request(id, method, &params)).await?;
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

fn thread_start_params(
    workspace: &std::path::Path,
    model: Option<String>,
    instructions: Option<String>,
) -> Value {
    let mut params = with_optional_model(json!({"cwd": workspace.to_string_lossy()}), model);
    if let Some(instructions) = instructions {
        params["developerInstructions"] = Value::String(instructions);
    }
    params["approvalPolicy"] = Value::String("never".to_owned());
    params["sandbox"] = Value::String("danger-full-access".to_owned());
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::BackendConfig;

    #[test]
    fn nako_codex_process_disables_native_multi_agent_tools() {
        let config =
            BackendConfig::codex(PathBuf::from("codex"), PathBuf::from("/tmp/nako-workspace"));
        let args = config
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(args, ["app-server", "--stdio", "--disable", "multi_agent"]);
    }
}
