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
    BackendTokenUsage, CLAUDE_PROVIDER, CapabilitySupport, DeltaKind, ItemKind, ItemStatus,
    ModelInfo, NormalizedItem, TurnOutcome, request_failed,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const SDK_VERSION: &str = "0.3.220";
const BRIDGE_SOURCE: &str = include_str!("bridge.mjs");

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
    let _ = events
        .send(BackendEvent::Ready(BackendIdentity {
            provider: CLAUDE_PROVIDER.to_owned(),
            display_name: "Claude".to_owned(),
            version: Some(SDK_VERSION.to_owned()),
            capabilities: capabilities(),
        }))
        .await;
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(bridge) = bridge.as_mut() { let _ = send(bridge, json!({"method":"shutdown"})).await; }
                    break;
                }
                handle_command(command, &config, authenticated, bridge.as_mut(), &events).await;
            }
            message = async { bridge.as_mut().expect("guarded").messages.recv().await }, if bridge.is_some() => {
                let Some(message) = message else {
                    let _ = events.send(BackendEvent::Disconnected { reason: "Claude SDK bridge exited".to_owned() }).await;
                    bridge = None;
                    continue;
                };
                handle_bridge_message(&message, &events).await;
            }
        }
    }
    if let Some(bridge) = bridge {
        bridge.task.abort();
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
            external_tools: _,
            replace_builtin_tools: _,
        } => (
            "create",
            json!({"model":model,"instructions":instructions,"ownerSessionId":owner_session_id}),
        ),
        BackendCommand::ResumeSession {
            provider_session_id,
            owner_session_id,
        } => (
            "resume",
            json!({"sessionId":provider_session_id,"ownerSessionId":owner_session_id}),
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
        BackendCommand::ResolveQuestion { .. }
        | BackendCommand::ResolveExternalTool { .. }
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
                item: NormalizedItem {
                    id: string(value, "id"),
                    kind,
                    title: string(value, "title"),
                    body: string(value, "body"),
                    status,
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
    let item = NormalizedItem {
        id: string(message, "callId"),
        kind: ItemKind::Tool,
        title: string(message, "name"),
        body,
        status,
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
        assert!(BRIDGE_SOURCE.contains("messageId: message.uuid"));
        assert!(BRIDGE_SOURCE.contains("blockIndex: event.index"));
        assert!(BRIDGE_SOURCE.contains("`claude:${messageId}:${blockIndex}`"));
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
