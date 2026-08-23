use std::{path::PathBuf, process::Stdio};

use directories::ProjectDirs;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, Command},
    sync::mpsc,
};
use uuid::Uuid;

use crate::backend::{
    ApprovalDecision, ApprovalKind, ApprovalRequest, BackendCapabilities, BackendCommand,
    BackendError, BackendEvent, BackendHandle, BackendIdentity, BackendOperation,
    BackendTokenUsage, CLAUDE_PROVIDER, CapabilitySupport, DeltaKind, ExternalToolRequest,
    ItemKind, ItemStatus, ModelInfo, NormalizedItem, TurnOutcome, request_failed,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const SDK_VERSION: &str = "0.3.220";
const BRIDGE_SOURCE: &str = include_str!("bridge.mjs");
const TOOL_POLICY_SOURCE: &str = include_str!("tool_policy.mjs");

#[derive(Clone)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
    vision_config: Option<std::sync::Arc<std::sync::RwLock<crate::vision::VisionConfig>>>,
    vision_service: Option<crate::vision::SharedVisionService>,
}

impl BackendConfig {
    #[must_use]
    pub const fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
            vision_config: None,
            vision_service: None,
        }
    }

    #[must_use]
    pub fn with_credential(mut self, credential: Option<Value>) -> Self {
        self.credential = credential;
        self
    }

    #[must_use]
    pub fn with_vision(
        mut self,
        config: std::sync::Arc<std::sync::RwLock<crate::vision::VisionConfig>>,
        service: Option<crate::vision::SharedVisionService>,
    ) -> Self {
        self.vision_config = Some(config);
        self.vision_service = service;
        self
    }
}

struct Bridge {
    stdin: ChildStdin,
    messages: mpsc::Receiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

struct BridgeRequest {
    method: &'static str,
    payload: Value,
}

#[derive(Debug)]
struct UnsupportedCommand {
    operation: BackendOperation,
    message: &'static str,
}

/// Starts the Claude TypeScript SDK adapter.
///
/// # Errors
/// Returns an error when a stored credential is malformed or the Node SDK bridge cannot be prepared.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let authenticated = credential_is_external_login(config.credential.as_ref())?;
    let bridge = if authenticated {
        Some(spawn_bridge(&config.workspace).await?)
    } else {
        None
    };
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(run_supervisor(
        config,
        authenticated,
        bridge,
        command_rx,
        event_tx,
    ));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

fn credential_is_external_login(credential: Option<&Value>) -> Result<bool, BackendError> {
    let Some(credential) = credential else {
        return Ok(false);
    };
    if credential.get("external_login").and_then(Value::as_bool) == Some(true) {
        return Ok(true);
    }
    Err(BackendError::InvalidCredential {
        provider: CLAUDE_PROVIDER.to_owned(),
        detail: "missing external_login marker".to_owned(),
    })
}

async fn spawn_bridge(workspace: &std::path::Path) -> Result<Bridge, BackendError> {
    let directory = prepare_bridge_directory().await?;
    launch_bridge(&directory, workspace)
}

async fn prepare_bridge_directory() -> Result<PathBuf, BackendError> {
    ensure_node_version().await?;
    let project =
        ProjectDirs::from("dev", "nakode", "Nakode").ok_or_else(|| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: "platform does not expose an application data directory".to_owned(),
        })?;
    let directory = project.data_local_dir().join("claude-sdk");
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    let package = format!(
        r#"{{"private":true,"type":"module","dependencies":{{"@anthropic-ai/claude-agent-sdk":"{SDK_VERSION}"}}}}"#
    );
    tokio::fs::write(directory.join("package.json"), package)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    tokio::fs::write(directory.join("bridge.mjs"), BRIDGE_SOURCE)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    tokio::fs::write(directory.join("tool_policy.mjs"), TOOL_POLICY_SOURCE)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    if !claude_sdk_is_current(&directory).await {
        let output = Command::new("npm")
            .args([
                "install",
                "--omit=dev",
                "--no-audit",
                "--no-fund",
                "--package-lock=false",
            ])
            .current_dir(&directory)
            .output()
            .await
            .map_err(|error| BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: format!("Node.js 18+ and npm are required: {error}"),
            })?;
        if !output.status.success() {
            return Err(BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
    }
    Ok(directory)
}

async fn claude_sdk_is_current(directory: &std::path::Path) -> bool {
    let manifest = directory.join("node_modules/@anthropic-ai/claude-agent-sdk/package.json");
    if !manifest.is_file() {
        return false;
    }
    tokio::fs::read_to_string(manifest)
        .await
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|manifest| manifest["version"].as_str().map(str::to_owned))
        .is_some_and(|version| version == SDK_VERSION)
}

fn launch_bridge(
    directory: &std::path::Path,
    workspace: &std::path::Path,
) -> Result<Bridge, BackendError> {
    let executable = std::env::current_exe().map_err(|error| BackendError::BridgeSetup {
        provider: CLAUDE_PROVIDER.to_owned(),
        detail: error.to_string(),
    })?;
    let mut child = Command::new("node")
        .arg(directory.join("bridge.mjs"))
        .current_dir(directory)
        .env("NAKODE_WORKSPACE", workspace)
        .env("NAKODE_EXECUTABLE", executable)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| BackendError::Spawn {
            backend: "Claude SDK",
            program: PathBuf::from("node"),
            source,
        })?;
    let stdin = child.stdin.take().ok_or(BackendError::MissingPipe {
        backend: "Claude SDK",
        pipe: "stdin",
    })?;
    let stdout = child.stdout.take().ok_or(BackendError::MissingPipe {
        backend: "Claude SDK",
        pipe: "stdout",
    })?;
    let stderr = child.stderr.take().ok_or(BackendError::MissingPipe {
        backend: "Claude SDK",
        pipe: "stderr",
    })?;
    let (tx, messages) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let error_tx = tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let _ = error_tx
                    .send(json!({"event":"diagnostic", "message":line}))
                    .await;
            }
        });
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str(&line) {
                Ok(message) => {
                    if tx.send(message).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = tx.send(json!({"event":"diagnostic", "message":format!("invalid Claude SDK message: {error}: {line}")})).await;
                }
            }
        }
        let _ = child.wait().await;
    });
    Ok(Bridge {
        stdin,
        messages,
        task,
    })
}

async fn ensure_node_version() -> Result<(), BackendError> {
    let output = Command::new("node")
        .arg("--version")
        .output()
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: format!("Node.js 18+ is required: {error}"),
        })?;
    let version = String::from_utf8_lossy(&output.stdout);
    let mut parts = version.trim_start_matches('v').split('.');
    let major = parts.next().and_then(|part| part.parse::<u64>().ok());
    if !output.status.success() || major.is_none_or(|version| version < 18) {
        return Err(BackendError::BridgeSetup {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: format!("Node.js 18+ is required; found {}", version.trim()),
        });
    }
    Ok(())
}

async fn run_supervisor(
    config: BackendConfig,
    authenticated: bool,
    mut bridge: Option<Bridge>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let mut attachment = None;
    let mut session_options = None;
    let mut recovery_ready_event = None;
    let mut deferred_command = None;
    let _ = events.send(BackendEvent::Ready(claude_identity())).await;
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(bridge) = bridge.as_mut() { let _ = send(bridge, json!({"method":"shutdown"})).await; }
                    break;
                }
                remember_session_state(&command, &mut attachment, &mut session_options);
                if should_recover_bridge(authenticated, bridge.is_some(), &command) {
                    match spawn_bridge(&config.workspace).await {
                        Ok(restarted) => bridge = Some(restarted),
                        Err(error) => {
                            bridge_recovery_failed(&events, operation_for(&command), error).await;
                            continue;
                        }
                    }
                    if !is_attachment_command(&command) {
                        recovery_ready_event = reattach_session(
                            attachment.clone(),
                            &config,
                            authenticated,
                            &mut bridge,
                            &events,
                        )
                        .await;
                        if recovery_ready_event.is_some() {
                            deferred_command = Some(command);
                            continue;
                        }
                    }
                }
                handle_command(command, &config, authenticated, bridge.as_mut(), &events).await;
            }
            message = async { bridge.as_mut().expect("guarded").messages.recv().await }, if bridge.is_some() => {
                let Some(message) = message else {
                    let _ = events.send(BackendEvent::Disconnected { reason: "Claude SDK bridge exited".to_owned() }).await;
                    if let Some(stopped) = bridge.take() {
                        stopped.task.abort();
                    }
                    if authenticated {
                        match spawn_bridge(&config.workspace).await {
                            Ok(restarted) => {
                                bridge = Some(restarted);
                                recovery_ready_event = reattach_session(
                                    attachment.clone(),
                                    &config,
                                    authenticated,
                                    &mut bridge,
                                    &events,
                                )
                                .await;
                                if recovery_ready_event.is_none() {
                                    let _ = events.send(BackendEvent::Ready(claude_identity())).await;
                                }
                            }
                            Err(error) => {
                                bridge_recovery_failed(&events, BackendOperation::Reload, error)
                                    .await;
                            }
                        }
                    }
                    continue;
                };
                let event_name = message.get("event").and_then(Value::as_str);
                if event_name == Some("session_created")
                    && let Some(command) = attachment.take()
                {
                    attachment = Some(resume_after_create(command, &message));
                }
                handle_bridge_message(&message, &events).await;
                if recovery_ready_event == event_name {
                    replay_after_reattach(
                        session_options.clone(),
                        deferred_command.take(),
                        &config,
                        authenticated,
                        &mut bridge,
                        &events,
                    )
                    .await;
                    recovery_ready_event = None;
                    let _ = events.send(BackendEvent::Ready(claude_identity())).await;
                }
            }
        }
    }
    if let Some(bridge) = bridge {
        bridge.task.abort();
    }
}

async fn bridge_recovery_failed(
    events: &mpsc::Sender<BackendEvent>,
    operation: BackendOperation,
    error: BackendError,
) {
    request_failed(
        events,
        operation,
        format!("Claude SDK bridge recovery failed: {error}"),
    )
    .await;
}

async fn reattach_session(
    attachment: Option<BackendCommand>,
    config: &BackendConfig,
    authenticated: bool,
    bridge: &mut Option<Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) -> Option<&'static str> {
    let command = attachment?;
    let recovery_event = recovery_event_for(&command);
    handle_command(command, config, authenticated, bridge.as_mut(), events).await;
    recovery_event
}

async fn replay_after_reattach(
    session_options: Option<BackendCommand>,
    deferred_command: Option<BackendCommand>,
    config: &BackendConfig,
    authenticated: bool,
    bridge: &mut Option<Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if let Some(command) = session_options {
        handle_command(command, config, authenticated, bridge.as_mut(), events).await;
    }
    if let Some(command) = deferred_command {
        handle_command(command, config, authenticated, bridge.as_mut(), events).await;
    }
}

async fn handle_command(
    command: BackendCommand,
    config: &BackendConfig,
    authenticated: bool,
    bridge: Option<&mut Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if matches!(command, BackendCommand::BeginAuthentication) {
        authenticate(events).await;
        return;
    }
    if !authenticated {
        request_failed(
            events,
            operation_for(&command),
            "Claude is not authenticated; run `claude auth login`, then reconnect the provider",
        )
        .await;
        return;
    }
    let Some(bridge) = bridge else {
        request_failed(
            events,
            operation_for(&command),
            "Claude SDK bridge is not running",
        )
        .await;
        return;
    };
    let command = match augment_image_attachments(command, config).await {
        Ok(command) => command,
        Err(error) => {
            request_failed(events, BackendOperation::StartTurn, error).await;
            return;
        }
    };
    let BridgeRequest {
        method,
        mut payload,
    } = match bridge_request(command) {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(unsupported) => {
            request_failed(events, unsupported.operation, unsupported.message).await;
            return;
        }
    };
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    object.insert("method".to_owned(), Value::String(method.to_owned()));
    object.insert(
        "requestId".to_owned(),
        Value::String(Uuid::now_v7().to_string()),
    );
    object.insert(
        "operation".to_owned(),
        Value::String(operation_for_method(method).label().to_owned()),
    );
    object.insert(
        "workspace".to_owned(),
        Value::String(config.workspace.to_string_lossy().into_owned()),
    );
    if let Err(error) = send(bridge, payload).await {
        request_failed(events, operation_for_method(method), error).await;
    }
}

async fn authenticate(events: &mpsc::Sender<BackendEvent>) {
    let status = Command::new("claude")
        .args(["auth", "status"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    let event = match status {
        Ok(status) if status.success() => BackendEvent::AuthenticationCompleted {
            kind: "claude_code_login".to_owned(),
            metadata: json!({"external_login": true}),
        },
        _ => BackendEvent::AuthenticationChallenge {
            login_id: Uuid::now_v7().to_string(),
            verification_url: "https://claude.ai".to_owned(),
            user_code: "Run `claude auth login` in a terminal, then retry Connect".to_owned(),
        },
    };
    let _ = events.send(event).await;
}

fn bridge_request(command: BackendCommand) -> Result<Option<BridgeRequest>, UnsupportedCommand> {
    let (method, payload) = match command {
        BackendCommand::StartSession {
            model,
            instructions,
            owner_session_id,
            parent_run_id,
            enabled_skill_ids: _,
            external_tools,
            replace_builtin_tools,
            allowed_builtin_tools,
            max_turns,
            finalization_reserve_turns,
            timeout_seconds,
        } => (
            "create",
            json!({"model":model,"instructions":instructions,"ownerSessionId":owner_session_id,"parentRunId":parent_run_id,"maxTurns":max_turns,"finalizationReserveTurns":finalization_reserve_turns,"timeoutSeconds":timeout_seconds,"externalTools":external_tools,"replaceBuiltinTools":replace_builtin_tools,"allowedBuiltinTools":allowed_builtin_tools}),
        ),
        BackendCommand::ResumeSession {
            provider_session_id,
            owner_session_id,
            enabled_skill_ids: _,
            external_tools,
            replace_builtin_tools,
            allowed_builtin_tools,
            max_turns,
            timeout_seconds,
        } => (
            "resume",
            json!({"sessionId":provider_session_id,"ownerSessionId":owner_session_id,"externalTools":external_tools,"replaceBuiltinTools":replace_builtin_tools,"allowedBuiltinTools":allowed_builtin_tools,"maxTurns":max_turns,"timeoutSeconds":timeout_seconds}),
        ),
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => ("close", json!({"sessionId":provider_session_id})),
        BackendCommand::StartTurn {
            provider_session_id,
            client_id,
            prompt,
            attachments: _,
            model,
            skill_catalogue: _,
        } => (
            "send",
            json!({"sessionId":provider_session_id,"turnId":client_id,"prompt":prompt,"model":model}),
        ),
        BackendCommand::InterruptTurn {
            provider_session_id: _,
            turn_id,
        } => ("cancel", json!({"turnId":turn_id})),
        BackendCommand::Reload {
            provider_session_id,
        } => ("reload", json!({"sessionId":provider_session_id})),
        BackendCommand::SetSessionModel { .. } => {
            return Err(UnsupportedCommand {
                operation: BackendOperation::SetSessionModel,
                message: "Claude applies model changes on the next turn",
            });
        }
        BackendCommand::SetSessionOptions {
            provider_session_id,
            options,
        } => (
            "set_options",
            json!({"sessionId":provider_session_id,"fastMode":options.fast_mode,"reasoningEffort":options.reasoning_effort}),
        ),
        BackendCommand::CompactSession { .. } => {
            return Err(UnsupportedCommand {
                operation: BackendOperation::CompactSession,
                message: "Claude manages its own context",
            });
        }
        BackendCommand::SteerTurn { .. } => {
            return Err(UnsupportedCommand {
                operation: BackendOperation::SteerTurn,
                message: "Claude SDK does not expose steering",
            });
        }
        BackendCommand::ResolveApproval { id, decision } => (
            "resolve_approval",
            json!({
                "approvalId": id,
                "decision": match decision {
                    ApprovalDecision::AcceptOnce => "accept_once",
                    ApprovalDecision::AcceptForSession => "accept_session",
                    ApprovalDecision::Decline => "decline",
                }
            }),
        ),
        BackendCommand::ResolveExternalTool { id, output, failed } => (
            "resolve_external_tool",
            json!({"id":id,"output":output,"failed":failed}),
        ),
        BackendCommand::ResolveQuestion { .. }
        | BackendCommand::BeginAuthentication
        | BackendCommand::Shutdown => return Ok(None),
    };
    Ok(Some(BridgeRequest { method, payload }))
}

async fn send(bridge: &mut Bridge, value: Value) -> Result<(), String> {
    let mut encoded = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
    encoded.push(b'\n');
    bridge
        .stdin
        .write_all(&encoded)
        .await
        .map_err(|error| error.to_string())
}

async fn augment_image_attachments(
    command: BackendCommand,
    config: &BackendConfig,
) -> Result<BackendCommand, String> {
    let BackendCommand::StartTurn {
        provider_session_id,
        client_id,
        mut prompt,
        mut attachments,
        model,
        skill_catalogue,
    } = command
    else {
        return Ok(command);
    };
    let images = attachments
        .iter()
        .filter_map(|attachment| attachment.image.clone())
        .collect::<Vec<_>>();
    let vision_enabled = config
        .vision_config
        .as_ref()
        .is_some_and(|config| config.read().is_ok_and(|config| config.is_enabled()));
    if images.is_empty() || !vision_enabled {
        return Ok(BackendCommand::StartTurn {
            provider_session_id,
            client_id,
            prompt,
            attachments,
            model,
            skill_catalogue,
        });
    }
    let service = config.vision_service.as_ref().ok_or_else(|| {
        "Vision add-on model is configured but its provider is unavailable".to_owned()
    })?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let description = service
        .analyze(
            "Describe these attached images precisely for the coding agent. Focus on visible text, layout, state, errors, and implementation-relevant details.",
            images,
            &cancellation,
        )
        .await?;
    prompt.push_str("\n\nVision add-on analysis of the attached images:\n");
    prompt.push_str(&description);
    for attachment in &mut attachments {
        attachment.image = None;
    }
    Ok(BackendCommand::StartTurn {
        provider_session_id,
        client_id,
        prompt,
        attachments,
        model,
        skill_catalogue,
    })
}

#[allow(clippy::too_many_lines)]
async fn handle_bridge_message(message: &Value, events: &mpsc::Sender<BackendEvent>) {
    let event_name = message
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("diagnostic");
    let event = match event_name {
        "models" => models_event(message),
        "session_created" => BackendEvent::SessionCreated {
            provider_session_id: string(message, "sessionId"),
            model: string(message, "model"),
        },
        "session_resumed" => BackendEvent::SessionResumed {
            provider_session_id: string(message, "sessionId"),
            model: string(message, "model"),
            history: session_history(message),
        },
        "session_closed" => BackendEvent::SessionClosed {
            provider_session_id: string(message, "sessionId"),
        },
        "turn_started" => {
            let turn_id = string(message, "turnId");
            let _ = events
                .send(BackendEvent::TurnAccepted {
                    turn_id: turn_id.clone(),
                })
                .await;
            BackendEvent::TurnStarted { turn_id }
        }
        "delta" => delta_event(message),
        "tool_call" => tool_call_event(message),
        "external_tool_request" => external_tool_request_event(message),
        "plan" => BackendEvent::TurnPlan {
            turn_id: string(message, "turnId"),
            plan: string(message, "text"),
        },
        "approval_request" => approval_request_event(message),
        "approval_resolved" => BackendEvent::ApprovalResolved {
            request_id: message.get("approvalId").cloned().unwrap_or(Value::Null),
        },
        "interrupt_accepted" => BackendEvent::InterruptAccepted,
        "usage" => usage_event(message),
        "turn_completed" => turn_completed_event(message),
        "turn_start_failed" => BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -1,
            message: string(message, "message"),
        },
        "warning" => BackendEvent::Warning(string(message, "message")),
        "process_release_failed" => process_release_failed_event(message),
        "error" => {
            request_failed(
                events,
                BackendOperation::StartTurn,
                string(message, "message"),
            )
            .await;
            return;
        }
        _ => BackendEvent::ProtocolDiagnostic(string(message, "message")),
    };
    let _ = events.send(event).await;
}

fn external_tool_request_event(message: &Value) -> BackendEvent {
    BackendEvent::ExternalToolRequested(ExternalToolRequest {
        id: string(message, "id"),
        name: string(message, "name"),
        arguments_json: string(message, "argumentsJson"),
    })
}

fn claude_content_block_id(message: &Value) -> String {
    let message_id = string(message, "messageId");
    let block_index = message["blockIndex"].as_u64();
    if message_id.is_empty() || block_index.is_none() {
        return format!("{}:claude", string(message, "turnId"));
    }
    format!("claude:{message_id}:{}", block_index.unwrap_or_default())
}

fn delta_event(message: &Value) -> BackendEvent {
    let kind = if message["kind"] == "reasoning" {
        DeltaKind::Reasoning
    } else {
        DeltaKind::Assistant
    };
    BackendEvent::ItemDelta {
        item_id: claude_content_block_id(message),
        turn_id: string(message, "turnId"),
        kind,
        delta: string(message, "text"),
    }
}

fn approval_request_event(message: &Value) -> BackendEvent {
    BackendEvent::ApprovalRequested(ApprovalRequest {
        id: message.get("approvalId").cloned().unwrap_or(Value::Null),
        method: string(message, "toolName"),
        kind: match string(message, "toolName").as_str() {
            "Bash" => ApprovalKind::Command,
            "Edit" | "Write" | "NotebookEdit" => ApprovalKind::FileChange,
            _ => ApprovalKind::Other,
        },
        title: string(message, "title"),
        detail: message.get("input").map_or_else(String::new, display_value),
    })
}

fn usage_event(message: &Value) -> BackendEvent {
    let usage = &message["usage"];
    BackendEvent::TokenUsageUpdated {
        usage: BackendTokenUsage {
            input_tokens: usage["input_tokens"].as_u64().unwrap_or_default(),
            output_tokens: usage["output_tokens"].as_u64().unwrap_or_default(),
            cached_input_tokens: usage["cache_read_input_tokens"]
                .as_u64()
                .unwrap_or_default(),
            cache_write_tokens: usage["cache_creation_input_tokens"]
                .as_u64()
                .unwrap_or_default(),
        },
    }
}

fn process_release_failed_event(message: &Value) -> BackendEvent {
    BackendEvent::Disconnected {
        reason: string(message, "message"),
    }
}

fn turn_completed_event(message: &Value) -> BackendEvent {
    let outcome = match message["status"].as_str() {
        Some("finished") => TurnOutcome::Completed,
        Some("cancelled") => TurnOutcome::Interrupted,
        _ => TurnOutcome::Failed,
    };
    let error = message
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_owned);
    BackendEvent::TurnCompleted {
        turn_id: string(message, "turnId"),
        outcome,
        error,
    }
}

fn models_event(message: &Value) -> BackendEvent {
    let models = message["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|model| {
            Some(ModelInfo {
                provider: CLAUDE_PROVIDER.to_owned(),
                id: model.get("id")?.as_str()?.to_owned(),
                is_default: model
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                capabilities: crate::backend::ModelCapabilities {
                    reasoning_efforts: model
                        .get("supportedEffortLevels")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::trim)
                        .filter(|effort| !effort.is_empty())
                        .map(str::to_owned)
                        .collect(),
                },
            })
        })
        .collect();
    BackendEvent::Models(models)
}

fn session_history(message: &Value) -> Vec<crate::backend::SessionHistoryItem> {
    message["history"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|value| {
            let kind = match value["kind"].as_str() {
                Some("user") => ItemKind::User,
                Some("assistant") => ItemKind::Assistant,
                Some("reasoning") => ItemKind::Reasoning,
                _ => ItemKind::Tool,
            };
            let status = match value["status"].as_str() {
                Some("failed") => ItemStatus::Failed,
                _ => ItemStatus::Complete,
            };
            crate::backend::SessionHistoryItem {
                turn_id: string(value, "turnId"),
                provider_id: None,
                model_id: None,
                attachments: Vec::new(),
                item: NormalizedItem {
                    id: string(value, "id"),
                    kind,
                    title: string(value, "title"),
                    body: string(value, "body"),
                    status,
                    tool_audit_json: None,
                },
            }
        })
        .collect()
}

fn tool_call_event(message: &Value) -> BackendEvent {
    let status = match message["status"].as_str() {
        Some("running") => ItemStatus::Running,
        Some("error") => ItemStatus::Failed,
        _ => ItemStatus::Complete,
    };
    let body = message
        .get("result")
        .or_else(|| message.get("args"))
        .map_or_else(String::new, display_value);
    let name = string(message, "name");
    let arguments = message
        .get("args")
        .or_else(|| message.pointer("/result/input"))
        .cloned()
        .unwrap_or(Value::Null);
    let output = message.pointer("/result/output").cloned();
    let tool_audit_json = serde_json::to_string(&json!({
        "version": 1,
        "callId": string(message, "callId"),
        "name": name,
        "input": bounded_claude_audit_value(&arguments),
        "output": output.as_ref().map(bounded_claude_audit_value),
        "kind": if message.get("external").and_then(Value::as_bool) == Some(true) {
            "custom"
        } else {
            "native"
        },
        "providerType": "claudeAgentSdkTool",
        "authoritative": "Nakode Claude adapter authorization",
        "failed": status == ItemStatus::Failed,
        "status": match status {
            ItemStatus::Running => "running",
            ItemStatus::Failed => "failed",
            _ => "completed",
        },
        "denied": message.get("denied").and_then(Value::as_bool).unwrap_or(false),
        "denialReason": message.get("denialReason").and_then(Value::as_str),
    }))
    .ok()
    .map(String::into_boxed_str);
    let item = NormalizedItem {
        id: string(message, "callId"),
        kind: ItemKind::Tool,
        title: name,
        body,
        status,
        tool_audit_json,
    };
    if status == ItemStatus::Running {
        BackendEvent::ItemStarted {
            turn_id: string(message, "turnId"),
            item,
        }
    } else {
        BackendEvent::ItemCompleted {
            turn_id: string(message, "turnId"),
            item,
        }
    }
}

fn bounded_claude_audit_value(value: &Value) -> Value {
    let rendered = serde_json::to_string_pretty(value).unwrap_or_default();
    let mut end = rendered.len().min(64 * 1024);
    while !rendered.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    json!({
        "format": "json",
        "value": &rendered[..end],
        "bytes": rendered.len(),
        "truncated": end < rendered.len(),
        "redacted": rendered.to_ascii_lowercase().contains("[redacted]")
            || rendered.to_ascii_lowercase().contains("<redacted>"),
    })
}

fn string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn display_value(value: &Value) -> String {
    value.as_str().map_or_else(
        || serde_json::to_string_pretty(value).unwrap_or_default(),
        str::to_owned,
    )
}

fn claude_identity() -> BackendIdentity {
    BackendIdentity {
        provider: CLAUDE_PROVIDER.to_owned(),
        display_name: "Claude".to_owned(),
        version: Some(SDK_VERSION.to_owned()),
        capabilities: capabilities(),
    }
}

fn resume_after_create(command: BackendCommand, message: &Value) -> BackendCommand {
    match command {
        BackendCommand::StartSession {
            owner_session_id,
            enabled_skill_ids,
            external_tools,
            replace_builtin_tools,
            allowed_builtin_tools,
            max_turns,
            timeout_seconds,
            ..
        } => BackendCommand::ResumeSession {
            provider_session_id: string(message, "sessionId"),
            owner_session_id,
            enabled_skill_ids,
            external_tools,
            replace_builtin_tools,
            allowed_builtin_tools,
            max_turns,
            timeout_seconds,
        },
        command => command,
    }
}

fn remember_session_state(
    command: &BackendCommand,
    attachment: &mut Option<BackendCommand>,
    session_options: &mut Option<BackendCommand>,
) {
    if is_attachment_command(command) {
        *attachment = Some(command.clone());
    } else if matches!(command, BackendCommand::SetSessionOptions { .. }) {
        *session_options = Some(command.clone());
    } else if matches!(command, BackendCommand::UnsubscribeSession { .. }) {
        *attachment = None;
        *session_options = None;
    }
}

fn is_attachment_command(command: &BackendCommand) -> bool {
    matches!(
        command,
        BackendCommand::StartSession { .. } | BackendCommand::ResumeSession { .. }
    )
}

fn recovery_event_for(command: &BackendCommand) -> Option<&'static str> {
    match command {
        BackendCommand::StartSession { .. } => Some("session_created"),
        BackendCommand::ResumeSession { .. } => Some("session_resumed"),
        _ => None,
    }
}

fn should_recover_bridge(
    authenticated: bool,
    bridge_available: bool,
    command: &BackendCommand,
) -> bool {
    authenticated && !bridge_available && command_needs_bridge(command)
}

fn command_needs_bridge(command: &BackendCommand) -> bool {
    !matches!(
        command,
        BackendCommand::BeginAuthentication
            | BackendCommand::Shutdown
            | BackendCommand::SetSessionModel { .. }
            | BackendCommand::CompactSession { .. }
            | BackendCommand::SteerTurn { .. }
            | BackendCommand::ResolveQuestion { .. }
    )
}

fn operation_for(command: &BackendCommand) -> BackendOperation {
    match command {
        BackendCommand::StartSession { .. } => BackendOperation::StartSession,
        BackendCommand::ResumeSession { .. } => BackendOperation::ResumeSession,
        BackendCommand::UnsubscribeSession { .. } => BackendOperation::UnsubscribeSession,
        BackendCommand::SteerTurn { .. } => BackendOperation::SteerTurn,
        BackendCommand::InterruptTurn { .. } => BackendOperation::InterruptTurn,
        BackendCommand::CompactSession { .. } => BackendOperation::CompactSession,
        BackendCommand::SetSessionModel { .. } | BackendCommand::SetSessionOptions { .. } => {
            BackendOperation::SetSessionModel
        }
        BackendCommand::Reload { .. } => BackendOperation::Reload,
        BackendCommand::BeginAuthentication => BackendOperation::Authenticate,
        _ => BackendOperation::StartTurn,
    }
}

fn operation_for_method(method: &str) -> BackendOperation {
    match method {
        "create" => BackendOperation::StartSession,
        "resume" => BackendOperation::ResumeSession,
        "close" => BackendOperation::UnsubscribeSession,
        "reload" | "models" => BackendOperation::Reload,
        "set_options" => BackendOperation::SetSessionModel,
        "cancel" => BackendOperation::InterruptTurn,
        _ => BackendOperation::StartTurn,
    }
}

fn capabilities() -> BackendCapabilities {
    BackendCapabilities {
        resume: CapabilitySupport::Supported,
        interruption: CapabilitySupport::Supported,
        model_catalog: CapabilitySupport::Supported,
        approvals: CapabilitySupport::Supported,
        native_tools: CapabilitySupport::Supported,
        scoped_runtime_policy: CapabilitySupport::Supported,
        external_tools: CapabilitySupport::Supported,
        close_session: CapabilitySupport::Supported,
        ..BackendCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_uses_the_official_agent_sdk_and_external_claude_login() {
        assert!(BRIDGE_SOURCE.contains("@anthropic-ai/claude-agent-sdk"));
        assert!(BRIDGE_SOURCE.contains("pathToClaudeCodeExecutable"));
        assert!(BRIDGE_SOURCE.contains("canUseTool"));
        assert!(BRIDGE_SOURCE.contains("PreToolUse"));
        assert!(BRIDGE_SOURCE.contains("preToolUseHook(command.turnId"));
        assert!(TOOL_POLICY_SOURCE.contains("permissionDecision: \"deny\""));
        assert!(BRIDGE_SOURCE.contains("mode === \"bypassPermissions\""));
        assert!(BRIDGE_SOURCE.contains("event.message?.id || message.uuid"));
        assert!(BRIDGE_SOURCE.contains("message.message.id || message.uuid"));
        assert!(BRIDGE_SOURCE.contains("blockIndex: event.index"));
        assert!(BRIDGE_SOURCE.contains("`claude:${messageId}:${blockIndex}`"));
        assert!(BRIDGE_SOURCE.contains("allowedTools: securityValidator"));
        assert!(BRIDGE_SOURCE.contains("? []"));
        assert!(BRIDGE_SOURCE.contains("authoritativeAllowedTools(command.allowedBuiltinTools)"));
        assert!(TOOL_POLICY_SOURCE.contains("Archetype policy does not allow"));
        assert!(BRIDGE_SOURCE.contains("maxTurns: session.maxTurns"));
        assert!(BRIDGE_SOURCE.contains("session.finalizationReserveTurns"));
        assert!(BRIDGE_SOURCE.contains("session.finalizing"));
        assert!(BRIDGE_SOURCE.contains("Protected finalization reserve denies new tool use"));
        assert!(BRIDGE_SOURCE.contains("session.timeoutSeconds * 1000"));
    }

    #[test]
    fn claude_structured_tool_policy_is_exhaustive_and_fail_closed() {
        let projected = BRIDGE_SOURCE
            .split("const CLAUDE_POLICY_TOOLS = new Set([")
            .nth(1)
            .and_then(|source| source.split("]);\n\n/**").next())
            .expect("Claude projected identity set");
        for provider in [
            "Read",
            "Grep",
            "Glob",
            "Bash",
            "Write",
            "Edit",
            "AskUserQuestion",
            "mcp__nakode__delegate",
        ] {
            assert!(
                projected.contains(&format!("  \"{provider}\",")),
                "missing exact Claude provider identity {provider}"
            );
        }
        for unprojected in ["read", "bash", "BASH", "ls", "mcp__nakode_external__Bash"] {
            assert!(
                !projected.contains(&format!("  \"{unprojected}\",")),
                "undeclared identity {unprojected} must remain distinct"
            );
        }
        assert!(BRIDGE_SOURCE.contains("if (!Array.isArray(configured)) return null"));
        assert!(BRIDGE_SOURCE.contains("command.parentRunId || policy?.runId || null"));
        assert!(BRIDGE_SOURCE.contains("CLAUDE_POLICY_TOOLS.has(name)"));
        assert!(BRIDGE_SOURCE.contains("denied: true"));
        assert!(BRIDGE_SOURCE.contains("denialReason: message"));
        assert!(BRIDGE_SOURCE.contains("tools: builtinTools"));
        assert!(!BRIDGE_SOURCE.contains("tools: allowedTools"));
        assert!(BRIDGE_SOURCE.contains("disallowedTools: session.deniedTools"));
    }

    #[test]
    fn completed_claude_denial_audit_retains_input_output_and_reason() {
        let event = tool_call_event(&json!({
            "event": "tool_call",
            "turnId": "turn-1",
            "callId": "call-1",
            "name": "Bash",
            "status": "error",
            "result": {
                "input": {"command": "echo denied"},
                "output": "permission denied"
            },
            "denied": true,
            "denialReason": "Archetype policy does not allow Bash."
        }));
        let BackendEvent::ItemCompleted { item, .. } = event else {
            panic!("expected completed tool event");
        };
        let audit: Value = serde_json::from_str(
            item.tool_audit_json
                .as_deref()
                .expect("authoritative audit"),
        )
        .expect("audit json");
        assert_eq!(audit["denied"], true);
        assert_eq!(audit["name"], "Bash");
        assert!(
            audit["input"]["value"]
                .as_str()
                .is_some_and(|value| value.contains("echo denied"))
        );
        assert!(
            audit["output"]["value"]
                .as_str()
                .is_some_and(|value| value.contains("permission denied"))
        );
        assert_eq!(
            audit["denialReason"],
            "Archetype policy does not allow Bash."
        );
    }

    fn ticket_stage_tools() -> Vec<nakode_protocol::ExternalToolDefinition> {
        vec![
            nakode_protocol::ExternalToolDefinition {
                name: "ListAssociatedTicketStages".to_owned(),
                description: "List exact stage identities for the attached ticket".to_owned(),
                input_schema_json: r#"{"type":"object","properties":{},"additionalProperties":false}"#
                    .to_owned(),
            },
            nakode_protocol::ExternalToolDefinition {
                name: "MoveAssociatedTicketToStage".to_owned(),
                description: "Move the attached ticket by exact stage id".to_owned(),
                input_schema_json: r#"{"type":"object","properties":{"stageId":{"type":"string","format":"uuid"}},"required":["stageId"],"additionalProperties":false}"#.to_owned(),
            },
        ]
    }

    #[test]
    fn claude_external_tools_cross_the_mcp_bridge() {
        let tools = ticket_stage_tools();
        let create = bridge_request(BackendCommand::StartSession {
            model: Some("opus".to_owned()),
            instructions: None,
            owner_session_id: Some("owner".to_owned()),
            parent_run_id: Some("parent-run".to_owned()),
            enabled_skill_ids: Vec::new(),
            external_tools: tools.clone(),
            replace_builtin_tools: false,
            allowed_builtin_tools: None,
            max_turns: None,
            finalization_reserve_turns: 0,
            timeout_seconds: None,
        })
        .expect("Claude supports session tools")
        .expect("bridge request");
        assert_eq!(
            create.payload["externalTools"][0]["name"],
            "ListAssociatedTicketStages"
        );
        assert_eq!(
            create.payload["externalTools"][1]["name"],
            "MoveAssociatedTicketToStage"
        );
        assert_eq!(
            create.payload["externalTools"][1]["input_schema_json"],
            tools[1].input_schema_json
        );
        assert_eq!(create.payload["replaceBuiltinTools"], false);
        assert_eq!(create.payload["parentRunId"], "parent-run");
        assert!(create.payload["allowedBuiltinTools"].is_null());

        let resume = bridge_request(BackendCommand::ResumeSession {
            provider_session_id: "provider-session".to_owned(),
            owner_session_id: Some("owner".to_owned()),
            enabled_skill_ids: Vec::new(),
            external_tools: tools,
            replace_builtin_tools: false,
            allowed_builtin_tools: None,
            max_turns: None,
            timeout_seconds: None,
        })
        .expect("Claude supports resumed session tools")
        .expect("resume bridge request");
        assert_eq!(resume.method, "resume");
        assert_eq!(
            resume.payload["externalTools"][1]["name"],
            "MoveAssociatedTicketToStage"
        );

        let restricted = bridge_request(BackendCommand::StartSession {
            model: None,
            instructions: None,
            owner_session_id: Some("owner".to_owned()),
            parent_run_id: None,
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec!["Read".to_owned(), "Glob".to_owned()]),
            max_turns: None,
            finalization_reserve_turns: 0,
            timeout_seconds: None,
        })
        .expect("Claude supports restricted built-in tools")
        .expect("bridge request");
        assert_eq!(
            restricted.payload["allowedBuiltinTools"],
            json!(["Read", "Glob"])
        );

        let denied = bridge_request(BackendCommand::StartSession {
            model: None,
            instructions: None,
            owner_session_id: Some("owner".to_owned()),
            parent_run_id: None,
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(Vec::new()),
            max_turns: None,
            finalization_reserve_turns: 0,
            timeout_seconds: None,
        })
        .expect("Claude supports an empty built-in boundary")
        .expect("bridge request");
        assert_eq!(denied.payload["allowedBuiltinTools"], json!([]));

        let resolve = bridge_request(BackendCommand::ResolveExternalTool {
            id: "external-1".to_owned(),
            output: "ticket".to_owned(),
            failed: false,
        })
        .expect("Claude supports external tool results")
        .expect("bridge request");
        assert_eq!(resolve.method, "resolve_external_tool");
        assert_eq!(resolve.payload["id"], "external-1");
        assert!(capabilities().external_tools.is_supported());
        assert!(BRIDGE_SOURCE.contains("createSdkMcpServer"));
        assert!(BRIDGE_SOURCE.contains("event: \"external_tool_request\""));
        assert!(BRIDGE_SOURCE.contains("case \"resolve_external_tool\""));
        assert!(BRIDGE_SOURCE.contains("mcp__nakode_external__"));
    }

    #[test]
    fn claude_stage_tool_request_preserves_exact_identity_and_arguments() {
        assert!(matches!(
            external_tool_request_event(&json!({
                "id": "external-1",
                "name": "MoveAssociatedTicketToStage",
                "argumentsJson": r#"{"stageId":"33333333-3333-4333-8333-333333333333"}"#
            })),
            BackendEvent::ExternalToolRequested(ExternalToolRequest { id, name, arguments_json })
                if id == "external-1"
                    && name == "MoveAssociatedTicketToStage"
                    && arguments_json == r#"{"stageId":"33333333-3333-4333-8333-333333333333"}"#
        ));
    }

    #[test]
    fn claude_uses_auto_as_the_filtered_default_and_an_attributed_validator() {
        assert!(BRIDGE_SOURCE.contains("filterEscalatingDefaultMode"));
        assert!(BRIDGE_SOURCE.contains("defaultMode || \"auto\""));
        assert!(BRIDGE_SOURCE.contains("NAKODE_SECURITY_VALIDATOR_AGENT"));
        assert!(BRIDGE_SOURCE.contains("SecurityValidation"));
        assert!(BRIDGE_SOURCE.contains("validated: false"));
        assert!(BRIDGE_SOURCE.contains("Recursive security validation was prevented"));
    }

    #[test]
    fn claude_bridge_defers_turn_completion_until_the_provider_process_is_released() {
        let release = BRIDGE_SOURCE
            .find("await processLifecycle.released()")
            .expect("provider release barrier");
        let completion = BRIDGE_SOURCE[release..]
            .find("event: \"turn_completed\"")
            .map(|offset| release + offset)
            .expect("terminal turn event");
        assert!(completion > release);
        assert!(
            BRIDGE_SOURCE
                .contains("await processLifecycle.started();\n    write({ event: \"turn_started\"")
        );
        assert!(BRIDGE_SOURCE.contains("spawnClaudeCodeProcess: processLifecycle.spawn"));
        assert!(BRIDGE_SOURCE.contains("child.once(\"close\""));
        assert!(BRIDGE_SOURCE.contains("process_release_failed"));
    }

    #[test]
    fn claude_process_close_is_the_replacement_send_barrier() {
        let directory = tempfile::tempdir().expect("temporary lifecycle test directory");
        let bridge = directory.path().join("bridge.mjs");
        let test = directory.path().join("lifecycle-test.mjs");
        std::fs::write(&bridge, BRIDGE_SOURCE).expect("bridge fixture");
        std::fs::write(
            &test,
            r#"
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { PassThrough } from "node:stream";

const source = readFileSync(process.argv[2], "utf8");
const start = source.indexOf("const FORCE_RELEASE_AFTER_MS");
const end = source.indexOf("async function nativeAgentHistory", start);
assert.notEqual(start, -1);
assert.notEqual(end, -1);
const lifecycle = source.slice(start, end);

let child;
function spawnChild() {
  child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.exitCode = null;
  child.pid = 42;
  child.kill = () => true;
  queueMicrotask(() => child.emit("spawn"));
  return child;
}

await eval(`(async () => {
  ${lifecycle}
  const controller = new AbortController();
  const first = providerProcessLifecycle();
  first.spawn({
    command: "fake-claude",
    args: [],
    cwd: process.cwd(),
    env: process.env,
    signal: controller.signal,
  });
  await first.started();

  let replacementSent = false;
  const redirect = (async () => {
    await first.released();
    replacementSent = true;
  })();
  await new Promise(setImmediate);
  assert.equal(replacementSent, false, "replacement sent before child close");

  child.exitCode = 0;
  child.emit("close", 0, null);
  await redirect;
  assert.equal(replacementSent, true, "replacement did not send after child close");
})()`);
"#,
        )
        .expect("lifecycle test script");

        let output = std::process::Command::new("node")
            .arg(&test)
            .arg(&bridge)
            .output()
            .expect("run lifecycle test with Node");
        assert!(
            output.status.success(),
            "lifecycle test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn process_release_failure_disconnects_without_completing_the_turn() {
        let event = process_release_failed_event(&json!({
            "event": "process_release_failed",
            "message": "Claude Code did not exit; queued follow-up retained"
        }));
        assert!(matches!(
            event,
            BackendEvent::Disconnected { reason }
                if reason.contains("queued follow-up retained")
        ));
    }

    #[test]
    fn claude_session_options_are_forwarded_to_the_bridge() {
        let request = bridge_request(BackendCommand::SetSessionOptions {
            provider_session_id: "claude-session".to_owned(),
            options: crate::backend::ModelOptions {
                reasoning_effort: None,
                fast_mode: true,
            },
        })
        .expect("supported options")
        .expect("bridge request");

        assert_eq!(request.method, "set_options");
        assert_eq!(request.payload["sessionId"], "claude-session");
        assert_eq!(request.payload["fastMode"], true);
    }

    #[test]
    fn claude_models_keep_sdk_reasoning_effort_capabilities() {
        let event = models_event(&json!({
            "models": [{
                "id": "opus",
                "isDefault": true,
                "supportedEffortLevels": ["low", "medium", "high"]
            }]
        }));
        let BackendEvent::Models(models) = event else {
            panic!("expected models event");
        };

        assert_eq!(models[0].id, "opus");
        assert_eq!(
            models[0].capabilities.reasoning_efforts,
            ["low", "medium", "high"]
        );
    }

    #[test]
    fn claude_content_blocks_keep_stable_message_and_block_identity() {
        let first = delta_event(&json!({
            "turnId": "turn-1",
            "messageId": "message-a",
            "blockIndex": 0,
            "kind": "reasoning",
            "text": "Inspecting"
        }));
        let BackendEvent::ItemDelta { item_id, kind, .. } = first else {
            panic!("expected item delta");
        };
        assert_eq!(item_id, "claude:message-a:0");
        assert_eq!(kind, DeltaKind::Reasoning);

        let intro = delta_event(&json!({
            "turnId": "turn-1",
            "messageId": "message-a",
            "blockIndex": 1,
            "kind": "assistant",
            "text": "Before the tool."
        }));
        let final_text = delta_event(&json!({
            "turnId": "turn-1",
            "messageId": "message-b",
            "blockIndex": 0,
            "kind": "assistant",
            "text": "After the tool."
        }));
        let (
            BackendEvent::ItemDelta {
                item_id: intro_id, ..
            },
            BackendEvent::ItemDelta {
                item_id: final_id, ..
            },
        ) = (intro, final_text)
        else {
            panic!("expected item deltas");
        };
        assert_eq!(intro_id, "claude:message-a:1");
        assert_eq!(final_id, "claude:message-b:0");
        assert_ne!(intro_id, final_id);
    }

    #[test]
    fn claude_native_agent_history_ids_and_correlates_content_blocks() {
        let directory = tempfile::tempdir().expect("temporary native history test directory");
        let bridge = directory.path().join("bridge.mjs");
        let test = directory.path().join("native-history-test.mjs");
        std::fs::write(&bridge, BRIDGE_SOURCE).expect("bridge fixture");
        std::fs::write(
            &test,
            r#"
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync(process.argv[2], "utf8");
const start = source.indexOf("function nativeAgentHistoryId");
const end = source.indexOf("async function nativeAgentHistory", start);
assert.notEqual(start, -1);
assert.notEqual(end, -1);
const { nativeAgentHistoryId, nativeAgentBlockText } = new Function(
  `${source.slice(start, end)}\nreturn { nativeAgentHistoryId, nativeAgentBlockText };`,
)();

assert.equal(nativeAgentHistoryId("agent", { uuid: "message" }, 7, 0), "native:agent:message");
assert.equal(nativeAgentHistoryId("agent", { uuid: "message" }, 7, 1), "native:agent:message:1");
assert.equal(nativeAgentHistoryId("agent", {}, 7, 0), "native:agent:7:0");
const ids = [0, 1, 2].map((blockIndex) =>
  nativeAgentHistoryId("agent", { uuid: "message" }, 7, blockIndex),
);
assert.equal(new Set(ids).size, ids.length);

assert.deepEqual(
  JSON.parse(nativeAgentBlockText({ type: "tool_use", id: "call-1", name: "Read", input: { path: "src" } })),
  { toolUseId: "call-1", tool: "Read", input: { path: "src" } },
);
assert.deepEqual(
  JSON.parse(nativeAgentBlockText({ type: "tool_result", tool_use_id: "call-1", content: "result" })),
  { toolUseId: "call-1", output: "result" },
);
"#,
        )
        .expect("native history test script");

        let output = std::process::Command::new("node")
            .arg(&test)
            .arg(&bridge)
            .output()
            .expect("run native history test with Node");
        assert!(
            output.status.success(),
            "native history test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn claude_partial_wrapper_uuids_do_not_split_one_content_block() {
        let directory = tempfile::tempdir().expect("temporary stream identity test directory");
        let bridge = directory.path().join("bridge.mjs");
        let test = directory.path().join("stream-identity-test.mjs");
        std::fs::write(&bridge, BRIDGE_SOURCE).expect("bridge fixture");
        std::fs::write(
            &test,
            r#"
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync(process.argv[2], "utf8");
const start = source.indexOf("function emitStreamEvent");
const end = source.indexOf("async function sendTurn", start);
assert.notEqual(start, -1);
assert.notEqual(end, -1);
const handler = source.slice(start, end);
const streamMessageIds = new Map();
const emitted = [];
const write = (message) => emitted.push(message);

await eval(`(async () => {
  ${handler}
  emitStreamEvent("turn-1", {
    uuid: "wrapper-start",
    event: { type: "message_start", message: { id: "api-message-1" } },
  });
  emitStreamEvent("turn-1", {
    uuid: "wrapper-delta-a",
    event: {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "alpha " },
    },
  });
  emitStreamEvent("turn-1", {
    uuid: "wrapper-delta-b",
    event: {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "beta" },
    },
  });
  emitStreamEvent("turn-1", {
    uuid: "wrapper-stop",
    event: { type: "message_stop" },
  });
})()`);

assert.deepEqual(
  emitted.map(({ messageId, blockIndex, text }) => ({ messageId, blockIndex, text })),
  [
    { messageId: "api-message-1", blockIndex: 0, text: "alpha " },
    { messageId: "api-message-1", blockIndex: 0, text: "beta" },
  ],
);
assert.equal(streamMessageIds.size, 0);
"#,
        )
        .expect("stream identity test script");

        let output = std::process::Command::new("node")
            .arg(&test)
            .arg(&bridge)
            .output()
            .expect("run stream identity test with Node");
        assert!(
            output.status.success(),
            "stream identity test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn resumed_claude_history_preserves_reasoning_and_provider_order() {
        let history = session_history(&json!({
            "history": [
                {"turnId":"turn-1","id":"think","kind":"reasoning","title":"THINKING","body":"Inspect","status":"complete"},
                {"turnId":"turn-1","id":"intro","kind":"assistant","title":"CLAUDE","body":"Before","status":"complete"},
                {"turnId":"turn-1","id":"tool-a","kind":"tool","title":"Read","body":"result","status":"complete"},
                {"turnId":"turn-1","id":"final","kind":"assistant","title":"CLAUDE","body":"After","status":"complete"}
            ]
        }));

        assert_eq!(
            history
                .iter()
                .map(|entry| (entry.item.id.as_str(), entry.item.kind))
                .collect::<Vec<_>>(),
            [
                ("think", ItemKind::Reasoning),
                ("intro", ItemKind::Assistant),
                ("tool-a", ItemKind::Tool),
                ("final", ItemKind::Assistant),
            ]
        );
    }

    #[test]
    fn legacy_claude_delta_without_block_metadata_keeps_compatible_identity() {
        let event = delta_event(&json!({
            "turnId": "turn-1",
            "kind": "assistant",
            "text": "legacy"
        }));
        let BackendEvent::ItemDelta { item_id, .. } = event else {
            panic!("expected item delta");
        };
        assert_eq!(item_id, "turn-1:claude");
    }

    #[test]
    fn authenticated_retry_and_reload_commands_restart_a_lost_bridge() {
        let retry = BackendCommand::StartTurn {
            provider_session_id: "session".to_owned(),
            client_id: "turn".to_owned(),
            prompt: "retry".to_owned(),
            attachments: Vec::new(),
            model: None,
            skill_catalogue: crate::skill::SkillCatalog::default(),
        };
        let reload = BackendCommand::Reload {
            provider_session_id: Some("session".to_owned()),
        };

        assert!(should_recover_bridge(true, false, &retry));
        assert!(should_recover_bridge(true, false, &reload));
        assert!(!should_recover_bridge(false, false, &retry));
        assert!(!should_recover_bridge(true, true, &retry));
        assert!(!should_recover_bridge(
            true,
            false,
            &BackendCommand::BeginAuthentication
        ));
        assert!(!should_recover_bridge(
            true,
            false,
            &BackendCommand::Shutdown
        ));
    }

    #[test]
    fn created_sessions_cache_a_resumable_bridge_attachment() {
        let command = BackendCommand::StartSession {
            model: Some("opus".to_owned()),
            instructions: Some("briefing".to_owned()),
            owner_session_id: Some("owner".to_owned()),
            parent_run_id: None,
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: true,
            allowed_builtin_tools: Some(vec!["Read".to_owned()]),
            max_turns: Some(4),
            finalization_reserve_turns: 0,
            timeout_seconds: Some(30),
        };

        assert_eq!(recovery_event_for(&command), Some("session_created"));
        let resumed = resume_after_create(
            command,
            &json!({"event":"session_created","sessionId":"claude-session"}),
        );
        assert_eq!(recovery_event_for(&resumed), Some("session_resumed"));
        let BackendCommand::ResumeSession {
            provider_session_id,
            owner_session_id,
            allowed_builtin_tools,
            max_turns,
            timeout_seconds,
            ..
        } = resumed
        else {
            panic!("created session should become a resumable attachment");
        };
        assert_eq!(provider_session_id, "claude-session");
        assert_eq!(owner_session_id.as_deref(), Some("owner"));
        assert_eq!(allowed_builtin_tools, Some(vec!["Read".to_owned()]));
        assert_eq!(max_turns, Some(4));
        assert_eq!(timeout_seconds, Some(30));
    }

    #[test]
    fn bridge_recovery_failure_is_reported_for_the_original_operation() {
        assert_eq!(
            operation_for(&BackendCommand::Reload {
                provider_session_id: Some("session".to_owned()),
            }),
            BackendOperation::Reload
        );
        assert_eq!(
            operation_for(&BackendCommand::StartTurn {
                provider_session_id: "session".to_owned(),
                client_id: "turn".to_owned(),
                prompt: "retry".to_owned(),
                attachments: Vec::new(),
                model: None,
                skill_catalogue: crate::skill::SkillCatalog::default(),
            }),
            BackendOperation::StartTurn
        );
    }

    #[test]
    fn external_login_marker_is_validated() {
        assert!(credential_is_external_login(Some(&json!({"external_login":true}))).unwrap());
        assert!(!credential_is_external_login(None).unwrap());
        assert!(credential_is_external_login(Some(&json!({"api_key":"wrong"}))).is_err());
    }

    #[tokio::test]
    async fn installed_claude_sdk_must_match_the_bridge_version() {
        let directory = tempfile::tempdir().expect("temporary SDK directory");
        let sdk = directory
            .path()
            .join("node_modules/@anthropic-ai/claude-agent-sdk");
        std::fs::create_dir_all(&sdk).expect("SDK directory");
        std::fs::write(
            sdk.join("package.json"),
            format!(r#"{{"version":"{SDK_VERSION}"}}"#),
        )
        .expect("SDK manifest");
        assert!(claude_sdk_is_current(directory.path()).await);

        std::fs::write(sdk.join("package.json"), r#"{"version":"0.0.0"}"#)
            .expect("stale SDK manifest");
        assert!(!claude_sdk_is_current(directory.path()).await);
    }
}
