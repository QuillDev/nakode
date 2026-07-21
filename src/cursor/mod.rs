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
    BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
    BackendIdentity, BackendOperation, CURSOR_PROVIDER, CapabilitySupport, DeltaKind, ItemKind,
    ItemStatus, ModelInfo, NormalizedItem, TurnOutcome,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const SDK_VERSION: &str = "1.0.23";
const BRIDGE_SOURCE: &str = include_str!("bridge.mjs");

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
}

impl BackendConfig {
    #[must_use]
    pub const fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
        }
    }

    #[must_use]
    pub fn with_credential(mut self, credential: Option<Value>) -> Self {
        self.credential = credential;
        self
    }
}

struct Bridge {
    stdin: ChildStdin,
    messages: mpsc::Receiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

/// Starts the Cursor TypeScript SDK adapter.
///
/// # Errors
/// Returns an error when a stored credential is malformed or the Node SDK bridge cannot be prepared.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let api_key = credential_api_key(config.credential.as_ref())?;
    let bridge = if api_key.is_some() {
        Some(spawn_bridge(&config.workspace).await?)
    } else {
        None
    };
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(run_supervisor(
        config, api_key, bridge, command_rx, event_tx,
    ));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

fn credential_api_key(credential: Option<&Value>) -> Result<Option<String>, BackendError> {
    let stored = credential
        .and_then(|value| value.get("api_key"))
        .and_then(Value::as_str);
    let environment = std::env::var("CURSOR_API_KEY").ok();
    let key = stored
        .map(str::to_owned)
        .or(environment)
        .filter(|key| !key.is_empty());
    if credential.is_some() && stored.is_none() {
        return Err(BackendError::InvalidCredential {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: "missing api_key".to_owned(),
        });
    }
    Ok(key)
}

#[allow(clippy::too_many_lines)]
async fn spawn_bridge(workspace: &std::path::Path) -> Result<Bridge, BackendError> {
    ensure_node_version().await?;
    let project =
        ProjectDirs::from("dev", "nakode", "Nakode").ok_or_else(|| BackendError::BridgeSetup {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: "platform does not expose an application data directory".to_owned(),
        })?;
    let directory = project.data_local_dir().join("cursor-sdk");
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    let package = format!(
        r#"{{"private":true,"type":"module","dependencies":{{"@cursor/sdk":"{SDK_VERSION}"}}}}"#
    );
    tokio::fs::write(directory.join("package.json"), package)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    tokio::fs::write(directory.join("bridge.mjs"), BRIDGE_SOURCE)
        .await
        .map_err(|error| BackendError::BridgeSetup {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: error.to_string(),
        })?;
    if !directory
        .join("node_modules/@cursor/sdk/dist/esm/index.js")
        .exists()
    {
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
                provider: CURSOR_PROVIDER.to_owned(),
                detail: format!("Node.js 22.13+ and npm are required: {error}"),
            })?;
        if !output.status.success() {
            return Err(BackendError::BridgeSetup {
                provider: CURSOR_PROVIDER.to_owned(),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
    }
    let executable = std::env::current_exe().map_err(|error| BackendError::BridgeSetup {
        provider: CURSOR_PROVIDER.to_owned(),
        detail: error.to_string(),
    })?;
    let mut child = Command::new("node")
        .arg(directory.join("bridge.mjs"))
        .current_dir(&directory)
        .env("NAKODE_WORKSPACE", workspace)
        .env("NAKODE_EXECUTABLE", executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| BackendError::Spawn {
            backend: "Cursor SDK",
            program: PathBuf::from("node"),
            source,
        })?;
    let stdin = child.stdin.take().ok_or(BackendError::MissingPipe {
        backend: "Cursor SDK",
        pipe: "stdin",
    })?;
    let stdout = child.stdout.take().ok_or(BackendError::MissingPipe {
        backend: "Cursor SDK",
        pipe: "stdout",
    })?;
    let stderr = child.stderr.take().ok_or(BackendError::MissingPipe {
        backend: "Cursor SDK",
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
                    let _ = tx.send(json!({"event":"diagnostic", "message":format!("invalid Cursor SDK message: {error}: {line}")})).await;
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
            provider: CURSOR_PROVIDER.to_owned(),
            detail: format!("Node.js 22.13+ is required: {error}"),
        })?;
    let version = String::from_utf8_lossy(&output.stdout);
    let mut parts = version.trim_start_matches('v').split('.');
    let major = parts.next().and_then(|part| part.parse::<u64>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u64>().ok());
    if !output.status.success() || major.zip(minor).is_none_or(|version| version < (22, 13)) {
        return Err(BackendError::BridgeSetup {
            provider: CURSOR_PROVIDER.to_owned(),
            detail: format!("Node.js 22.13+ is required; found {}", version.trim()),
        });
    }
    Ok(())
}

async fn run_supervisor(
    config: BackendConfig,
    api_key: Option<String>,
    mut bridge: Option<Bridge>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let _ = events
        .send(BackendEvent::Ready(BackendIdentity {
            provider: CURSOR_PROVIDER.to_owned(),
            display_name: "Cursor".to_owned(),
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
                handle_command(command, &config, api_key.as_deref(), bridge.as_mut(), &events).await;
            }
            message = async { bridge.as_mut().expect("guarded").messages.recv().await }, if bridge.is_some() => {
                let Some(message) = message else {
                    let _ = events.send(BackendEvent::Disconnected { reason: "Cursor SDK bridge exited".to_owned() }).await;
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

#[allow(clippy::too_many_lines)]
async fn handle_command(
    command: BackendCommand,
    config: &BackendConfig,
    api_key: Option<&str>,
    bridge: Option<&mut Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if matches!(command, BackendCommand::BeginAuthentication) {
        if let Some(key) = api_key {
            let _ = events
                .send(BackendEvent::AuthenticationCompleted {
                    kind: "api_key".to_owned(),
                    metadata: json!({"api_key": key}),
                })
                .await;
        } else {
            let _ = events
                .send(BackendEvent::AuthenticationChallenge {
                    login_id: Uuid::now_v7().to_string(),
                    verification_url: "https://cursor.com/dashboard/api".to_owned(),
                    user_code: "Set CURSOR_API_KEY, then retry".to_owned(),
                })
                .await;
        }
        return;
    }
    let Some(key) = api_key else {
        request_failed(
            events,
            operation_for(&command),
            "Cursor is not authenticated",
        )
        .await;
        return;
    };
    let Some(bridge) = bridge else {
        request_failed(
            events,
            operation_for(&command),
            "Cursor SDK bridge is not running",
        )
        .await;
        return;
    };
    let request_id = Uuid::now_v7().to_string();
    let (method, mut payload) = match command {
        BackendCommand::StartSession {
            model,
            instructions,
        } => ("create", json!({"model":model,"instructions":instructions})),
        BackendCommand::ResumeSession {
            provider_session_id,
        } => ("resume", json!({"sessionId":provider_session_id})),
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => ("close", json!({"sessionId":provider_session_id})),
        BackendCommand::StartTurn {
            session_id,
            client_id,
            prompt,
            model,
        } => (
            "send",
            json!({"sessionId":session_id,"turnId":client_id,"prompt":prompt,"model":model}),
        ),
        BackendCommand::InterruptTurn {
            session_id: _,
            turn_id,
        } => ("cancel", json!({"turnId":turn_id})),
        BackendCommand::Reload { session_id } => ("reload", json!({"sessionId":session_id})),
        BackendCommand::SetSessionModel { .. } => {
            request_failed(
                events,
                BackendOperation::SetSessionModel,
                "Cursor applies model changes on the next turn",
            )
            .await;
            return;
        }
        BackendCommand::CompactSession { .. } => {
            request_failed(
                events,
                BackendOperation::CompactSession,
                "Cursor manages its own context",
            )
            .await;
            return;
        }
        BackendCommand::SteerTurn { .. } => {
            request_failed(
                events,
                BackendOperation::SteerTurn,
                "Cursor SDK does not expose steering",
            )
            .await;
            return;
        }
        BackendCommand::ResolveApproval { .. }
        | BackendCommand::ResolveQuestion { .. }
        | BackendCommand::BeginAuthentication
        | BackendCommand::Shutdown => return,
    };
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    object.insert("method".to_owned(), Value::String(method.to_owned()));
    object.insert("requestId".to_owned(), Value::String(request_id));
    object.insert(
        "operation".to_owned(),
        Value::String(operation_for_method(method).label().to_owned()),
    );
    object.insert("apiKey".to_owned(), Value::String(key.to_owned()));
    object.insert(
        "workspace".to_owned(),
        Value::String(config.workspace.to_string_lossy().into_owned()),
    );
    if let Err(error) = send(bridge, payload).await {
        request_failed(events, operation_for_method(method), error).await;
    }
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

#[allow(clippy::too_many_lines)]
async fn handle_bridge_message(message: &Value, events: &mpsc::Sender<BackendEvent>) {
    let event = message
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("diagnostic");
    match event {
        "models" => {
            let models = message["models"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|model| {
                    Some(ModelInfo {
                        provider: CURSOR_PROVIDER.to_owned(),
                        id: model.get("id")?.as_str()?.to_owned(),
                        is_default: model
                            .get("isDefault")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect();
            let _ = events.send(BackendEvent::Models(models)).await;
        }
        "session_created" => {
            let _ = events
                .send(BackendEvent::SessionCreated {
                    provider_session_id: string(message, "sessionId"),
                    model: string(message, "model"),
                })
                .await;
        }
        "session_resumed" => {
            let _ = events
                .send(BackendEvent::SessionResumed {
                    provider_session_id: string(message, "sessionId"),
                    model: string(message, "model"),
                    history: Vec::new(),
                })
                .await;
        }
        "session_closed" => {
            let _ = events
                .send(BackendEvent::SessionClosed {
                    provider_session_id: string(message, "sessionId"),
                })
                .await;
        }
        "turn_started" => {
            let turn_id = string(message, "turnId");
            let _ = events
                .send(BackendEvent::TurnAccepted {
                    turn_id: turn_id.clone(),
                })
                .await;
            let _ = events.send(BackendEvent::TurnStarted { turn_id }).await;
        }
        "delta" => {
            let kind = if message["kind"] == "reasoning" {
                DeltaKind::Reasoning
            } else {
                DeltaKind::Assistant
            };
            let turn_id = string(message, "turnId");
            let _ = events
                .send(BackendEvent::ItemDelta {
                    item_id: format!("{turn_id}:cursor"),
                    turn_id,
                    kind,
                    delta: string(message, "text"),
                })
                .await;
        }
        "tool_call" => {
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
            let output = if status == ItemStatus::Running {
                BackendEvent::ItemStarted {
                    turn_id: string(message, "turnId"),
                    item,
                }
            } else {
                BackendEvent::ItemCompleted {
                    turn_id: string(message, "turnId"),
                    item,
                }
            };
            let _ = events.send(output).await;
        }
        "plan" => {
            let _ = events
                .send(BackendEvent::TurnPlan {
                    turn_id: string(message, "turnId"),
                    plan: string(message, "text"),
                })
                .await;
        }
        "interrupt_accepted" => {
            let _ = events.send(BackendEvent::InterruptAccepted).await;
        }
        "turn_completed" => {
            let outcome = match message["status"].as_str() {
                Some("finished") => TurnOutcome::Completed,
                Some("cancelled") => TurnOutcome::Interrupted,
                _ => TurnOutcome::Failed,
            };
            let error = message
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let _ = events
                .send(BackendEvent::TurnCompleted {
                    turn_id: string(message, "turnId"),
                    outcome,
                    error,
                })
                .await;
        }
        "error" => {
            request_failed(
                events,
                BackendOperation::StartTurn,
                string(message, "message"),
            )
            .await;
        }
        _ => {
            let _ = events
                .send(BackendEvent::ProtocolDiagnostic(string(message, "message")))
                .await;
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
        BackendCommand::SetSessionModel { .. } => BackendOperation::SetSessionModel,
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
        "cancel" => BackendOperation::InterruptTurn,
        _ => BackendOperation::StartTurn,
    }
}
async fn request_failed(
    events: &mpsc::Sender<BackendEvent>,
    operation: BackendOperation,
    message: impl Into<String>,
) {
    let _ = events
        .send(BackendEvent::RequestFailed {
            operation,
            code: -1,
            message: message.into(),
        })
        .await;
}
fn capabilities() -> BackendCapabilities {
    BackendCapabilities {
        resume: CapabilitySupport::Supported,
        interruption: CapabilitySupport::Supported,
        model_catalog: CapabilitySupport::Supported,
        native_tools: CapabilitySupport::Supported,
        close_session: CapabilitySupport::Supported,
        ..BackendCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bridge_injects_only_the_nakode_coordination_tool() {
        assert!(BRIDGE_SOURCE.contains("nakode_agent:"));
        assert!(!BRIDGE_SOURCE.contains("customTools: {\n      read:"));
        for name in [
            "read", "write", "edit", "bash", "glob", "grep", "eval", "ask", "todo",
        ] {
            assert!(!BRIDGE_SOURCE.contains(&format!("      {name}: {{")));
        }
    }
    #[test]
    fn credential_prefers_persisted_key() {
        let value = json!({"api_key":"stored"});
        assert_eq!(
            credential_api_key(Some(&value)).unwrap().as_deref(),
            Some("stored")
        );
    }
}
