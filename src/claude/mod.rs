use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    process::Stdio,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{ChildStdin, Command},
    sync::{Mutex, mpsc},
    time::timeout,
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
const PROCESS_LIFECYCLE_SOURCE: &str = include_str!("process_lifecycle.mjs");
const TOOL_POLICY_SOURCE: &str = include_str!("tool_policy.mjs");
// OAuth wire behavior is aligned with Oh My Pi's MIT-licensed Anthropic flow at
// can1357/oh-my-pi commit 530664c8f59abb8029c0138494494b5678457d4c
// (packages/ai/src/registry/oauth/anthropic.ts). Keep endpoint, client, scope, PKCE, and refresh
// changes source-verified rather than inferred from browser traffic.
const CLAUDE_AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
const CLAUDE_TOKEN_URL: &str = "https://api.anthropic.com/v1/oauth/token";
const CLAUDE_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const CLAUDE_OAUTH_SCOPES: &str = "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";
const CLAUDE_CALLBACK_PATH: &str = "/callback";
const CLAUDE_CALLBACK_PORT: u16 = 54_545;
const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const REFRESH_SKEW_MS: u64 = 5 * 60 * 1_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClaudeOAuthCredential {
    access_token: String,
    refresh_token: String,
    expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authorized_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    organization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    organization_name: Option<String>,
}

#[derive(Deserialize)]
struct ClaudeTokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
    #[serde(default)]
    account: Option<ClaudeAccount>,
    #[serde(default)]
    organization: Option<ClaudeOrganization>,
}

#[derive(Deserialize)]
struct ClaudeAccount {
    uuid: Option<String>,
    email_address: Option<String>,
}

#[derive(Deserialize)]
struct ClaudeOrganization {
    uuid: Option<String>,
    name: Option<String>,
}

struct ClaudeCallbackListeners {
    ipv4: TcpListener,
    ipv6: Option<TcpListener>,
}

impl ClaudeCallbackListeners {
    fn port(&self) -> Result<u16, String> {
        self.ipv4
            .local_addr()
            .map(|address| address.port())
            .map_err(|error| format!("Could not inspect the Claude sign-in callback: {error}"))
    }

    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        let Some(ipv6) = self.ipv6.as_ref() else {
            return self.ipv4.accept().await;
        };
        tokio::select! {
            accepted = self.ipv4.accept() => accepted,
            accepted = ipv6.accept() => accepted,
        }
    }
}

static REFRESHED_CREDENTIALS: LazyLock<Mutex<HashMap<String, ClaudeOAuthCredential>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
    vision_config: Option<std::sync::Arc<std::sync::RwLock<crate::vision::VisionConfig>>>,
    vision_service: Option<crate::vision::SharedVisionService>,
    publish_credential_updates: bool,
}

impl BackendConfig {
    #[must_use]
    pub const fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
            vision_config: None,
            vision_service: None,
            publish_credential_updates: false,
        }
    }

    #[must_use]
    pub fn with_credential(mut self, credential: Option<Value>) -> Self {
        self.credential = credential;
        self
    }

    #[must_use]
    pub const fn with_credential_updates(mut self) -> Self {
        self.publish_credential_updates = true;
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
    let credential = parse_credential(config.credential.as_ref())?;
    let (credential, credential_updated) =
        refresh_if_needed(credential)
            .await
            .map_err(|detail| BackendError::InvalidCredential {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail,
            })?;
    let bridge = match credential.as_ref() {
        Some(_) => Some(spawn_bridge(&config.workspace).await?),
        None => None,
    };
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(run_supervisor(
        config.publish_credential_updates,
        config,
        credential,
        credential_updated,
        bridge,
        command_rx,
        event_tx,
    ));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

fn parse_credential(
    credential: Option<&Value>,
) -> Result<Option<ClaudeOAuthCredential>, BackendError> {
    let Some(credential) = credential else {
        return Ok(None);
    };
    // The former marker represented credentials owned by an external Claude CLI. Treat it as
    // signed out so users can recover through Nakode's authoritative OAuth flow.
    if credential.get("external_login").and_then(Value::as_bool) == Some(true) {
        return Ok(None);
    }
    let parsed =
        serde_json::from_value::<ClaudeOAuthCredential>(credential.clone()).map_err(|error| {
            BackendError::InvalidCredential {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: format!("invalid OAuth credential: {error}"),
            }
        })?;
    if parsed.access_token.is_empty() || parsed.refresh_token.is_empty() {
        return Err(BackendError::InvalidCredential {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: "OAuth access or refresh token is empty".to_owned(),
        });
    }
    Ok(Some(parsed))
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
    tokio::fs::write(
        directory.join("process_lifecycle.mjs"),
        PROCESS_LIFECYCLE_SOURCE,
    )
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
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
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

#[allow(clippy::too_many_lines)]
async fn run_supervisor(
    publish_credential_updates: bool,
    config: BackendConfig,
    mut credential: Option<ClaudeOAuthCredential>,
    credential_updated: bool,
    mut bridge: Option<Bridge>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let mut attachment = None;
    let mut session_options = None;
    let mut recovery_ready_event = None;
    let mut deferred_command = None;
    let mut authentication_task: Option<tokio::task::JoinHandle<()>> = None;
    let _ = events.send(BackendEvent::Ready(claude_identity())).await;
    if publish_credential_updates
        && credential_updated
        && let Some(credential) = credential.as_ref()
    {
        emit_refreshed_credential(&events, credential).await;
    }
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(task) = authentication_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    if let Some(bridge) = bridge.as_mut() { let _ = send(bridge, json!({"method":"shutdown"})).await; }
                    break;
                }
                if matches!(command, BackendCommand::BeginAuthentication) {
                    if let Some(task) = authentication_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    let authentication_events = events.clone();
                    authentication_task = Some(tokio::spawn(async move {
                        authenticate(&authentication_events).await;
                    }));
                    continue;
                }
                if let Err(message) = refresh_supervisor_credential(
                    &mut credential,
                    publish_credential_updates,
                    &events,
                ).await {
                    request_failed(&events, operation_for(&command), message).await;
                    continue;
                }
                remember_session_state(&command, &mut attachment, &mut session_options);
                if should_recover_bridge(credential.is_some(), bridge.is_some(), &command) {
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
                            credential.as_ref(),
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
                handle_command(command, &config, credential.as_ref(), bridge.as_mut(), &events).await;
            }
            () = wait_until_credential_refresh(credential.as_ref()), if publish_credential_updates && credential.is_some() => {
                if let Err(message) = refresh_supervisor_credential(&mut credential, true, &events).await {
                    request_failed(&events, BackendOperation::Reload, message).await;
                }
            }
            message = async { bridge.as_mut().expect("guarded").messages.recv().await }, if bridge.is_some() => {
                let Some(message) = message else {
                    let _ = events.send(BackendEvent::Disconnected { reason: "Claude SDK bridge exited".to_owned() }).await;
                    if let Some(stopped) = bridge.take() {
                        stopped.task.abort();
                    }
                    if credential.is_some() {
                        match spawn_bridge(&config.workspace).await {
                            Ok(restarted) => {
                                bridge = Some(restarted);
                                recovery_ready_event = reattach_session(
                                    attachment.clone(),
                                    &config,
                                    credential.as_ref(),
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
                        credential.as_ref(),
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
    if let Some(task) = authentication_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(bridge) = bridge {
        bridge.task.abort();
    }
}

async fn wait_until_credential_refresh(credential: Option<&ClaudeOAuthCredential>) {
    let delay = credential.map_or(u64::MAX, |credential| {
        credential
            .expires_at_ms
            .saturating_sub(now_ms())
            .max(60_000)
    });
    tokio::time::sleep(Duration::from_millis(delay)).await;
}

async fn refresh_supervisor_credential(
    credential: &mut Option<ClaudeOAuthCredential>,
    publish_update: bool,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), String> {
    let Some(current) = credential.take() else {
        return Ok(());
    };
    let fallback = current.clone();
    match refresh_if_needed(Some(current)).await {
        Ok((updated, refreshed)) => {
            *credential = updated;
            if publish_update
                && refreshed
                && let Some(updated) = credential.as_ref()
            {
                emit_refreshed_credential(events, updated).await;
            }
            Ok(())
        }
        Err(message) => {
            *credential = Some(fallback);
            Err(message)
        }
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
    credential: Option<&ClaudeOAuthCredential>,
    bridge: &mut Option<Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) -> Option<&'static str> {
    let command = attachment?;
    let recovery_event = recovery_event_for(&command);
    handle_command(command, config, credential, bridge.as_mut(), events).await;
    recovery_event
}

async fn replay_after_reattach(
    session_options: Option<BackendCommand>,
    deferred_command: Option<BackendCommand>,
    config: &BackendConfig,
    credential: Option<&ClaudeOAuthCredential>,
    bridge: &mut Option<Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if let Some(command) = session_options {
        handle_command(command, config, credential, bridge.as_mut(), events).await;
    }
    if let Some(command) = deferred_command {
        handle_command(command, config, credential, bridge.as_mut(), events).await;
    }
}

async fn handle_command(
    command: BackendCommand,
    config: &BackendConfig,
    credential: Option<&ClaudeOAuthCredential>,
    bridge: Option<&mut Bridge>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if matches!(command, BackendCommand::BeginAuthentication) {
        authenticate(events).await;
        return;
    }
    if credential.is_none() {
        request_failed(
            events,
            operation_for(&command),
            "Claude is not authenticated; sign in from Provider Auth, then retry",
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
    object.insert(
        "oauthAccessToken".to_owned(),
        Value::String(credential.expect("checked above").access_token.clone()),
    );
    if let Err(error) = send(bridge, payload).await {
        request_failed(events, operation_for_method(method), error).await;
    }
}

async fn authenticate(events: &mpsc::Sender<BackendEvent>) {
    if let Err(message) = run_claude_oauth(CLAUDE_AUTHORIZE_URL, CLAUDE_TOKEN_URL, events).await {
        request_failed(events, BackendOperation::Authenticate, message).await;
    }
}

async fn run_claude_oauth(
    authorize_url: &str,
    token_url: &str,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), String> {
    let listener = bind_claude_callback().await?;
    let port = listener.port()?;
    let redirect_uri = format!("http://localhost:{port}{CLAUDE_CALLBACK_PATH}");
    let state = Uuid::now_v7().simple().to_string();
    let verifier = pkce_verifier();
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let verification_url =
        claude_authorization_url(authorize_url, &redirect_uri, &state, &challenge)?;
    events
        .send(BackendEvent::AuthenticationChallenge {
            login_id: state.clone(),
            verification_url,
            user_code: String::new(),
        })
        .await
        .map_err(|_| "Claude sign-in was cancelled".to_owned())?;
    let code = timeout(
        AUTHENTICATION_TIMEOUT,
        receive_claude_authorization_code(&listener, &state),
    )
    .await
    .map_err(|_| "Claude sign-in timed out. Retry sign-in from Provider Auth.".to_owned())??;
    let credential =
        exchange_claude_code(token_url, &code, &state, &redirect_uri, &verifier).await?;
    let metadata = serde_json::to_value(credential)
        .map_err(|error| format!("Could not store the Claude credential: {error}"))?;
    events
        .send(BackendEvent::AuthenticationCompleted {
            kind: "claude_oauth_pkce".to_owned(),
            metadata,
        })
        .await
        .map_err(|_| "Claude sign-in was cancelled".to_owned())
}

async fn bind_claude_callback() -> Result<ClaudeCallbackListeners, String> {
    let preferred = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), CLAUDE_CALLBACK_PORT);
    let ipv4 = match TcpListener::bind(preferred).await {
        Ok(listener) => listener,
        Err(_) => TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .map_err(|error| format!("Could not start the Claude sign-in callback: {error}"))?,
    };
    let port = ipv4
        .local_addr()
        .map_err(|error| format!("Could not inspect the Claude sign-in callback: {error}"))?
        .port();
    let ipv6 = TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port))
        .await
        .ok();
    Ok(ClaudeCallbackListeners { ipv4, ipv6 })
}

fn pkce_verifier() -> String {
    format!(
        "{}{}{}",
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple()
    )
}

fn claude_authorization_url(
    authorize_url: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(authorize_url)
        .map_err(|error| format!("Claude sign-in URL is invalid: {error}"))?;
    url.query_pairs_mut()
        .append_pair("code", "true")
        .append_pair("client_id", CLAUDE_OAUTH_CLIENT_ID)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", CLAUDE_OAUTH_SCOPES)
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state);
    Ok(url.into())
}

async fn receive_claude_authorization_code(
    listener: &ClaudeCallbackListeners,
    expected_state: &str,
) -> Result<String, String> {
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("Claude sign-in callback failed: {error}"))?;
        match parse_claude_callback(&mut stream, expected_state).await {
            Ok(code) => {
                respond_to_claude_callback(
                    &mut stream,
                    "200 OK",
                    "Claude authorization was received. Return to Nakode to finish sign-in.",
                )
                .await;
                return Ok(code);
            }
            Err(message) if message.starts_with("Claude sign-in was not completed") => {
                respond_to_claude_callback(
                    &mut stream,
                    "400 Bad Request",
                    "Claude sign-in was not completed. Return to Nakode and retry sign-in.",
                )
                .await;
                return Err(message);
            }
            Err(message) => {
                respond_to_claude_callback(&mut stream, "400 Bad Request", &message).await;
            }
        }
    }
}

async fn parse_claude_callback(
    stream: &mut TcpStream,
    expected_state: &str,
) -> Result<String, String> {
    let request = timeout(Duration::from_secs(5), async {
        let mut request = Vec::with_capacity(1_024);
        let mut buffer = [0_u8; 1_024];
        loop {
            let bytes = stream
                .read(&mut buffer)
                .await
                .map_err(|error| format!("Could not read the Claude sign-in callback: {error}"))?;
            if bytes == 0 {
                return Err("Claude sign-in callback ended before its headers".to_owned());
            }
            request.extend_from_slice(&buffer[..bytes]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(request);
            }
            if request.len() >= 8_192 {
                return Err("Claude sign-in callback headers were too large".to_owned());
            }
        }
    })
    .await
    .map_err(|_| "Claude sign-in callback connection timed out".to_owned())??;
    let request = std::str::from_utf8(&request)
        .map_err(|_| "Claude sign-in callback was not valid UTF-8".to_owned())?;
    let mut request_line = request
        .lines()
        .next()
        .into_iter()
        .flat_map(str::split_whitespace);
    if request_line.next() != Some("GET") {
        return Err("Claude sign-in callback must use GET".to_owned());
    }
    let target = request_line
        .next()
        .ok_or_else(|| "Claude sign-in callback was malformed".to_owned())?;
    let url = reqwest::Url::parse(&format!("http://localhost{target}"))
        .map_err(|_| "Claude sign-in callback URL was malformed".to_owned())?;
    if url.path() != CLAUDE_CALLBACK_PATH {
        return Err("Unexpected Claude sign-in callback path".to_owned());
    }
    let parameters = url.query_pairs().collect::<HashMap<_, _>>();
    if parameters.get("state").map(AsRef::as_ref) != Some(expected_state) {
        return Err("Claude sign-in callback state did not match".to_owned());
    }
    if parameters.contains_key("error") {
        return Err(
            "Claude sign-in was not completed. Retry sign-in from Provider Auth.".to_owned(),
        );
    }
    parameters
        .get("code")
        .filter(|code| !code.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| "Claude sign-in callback did not include a code".to_owned())
}

async fn respond_to_claude_callback(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!("<html><body><p>{message}</p></body></html>");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn exchange_claude_code(
    token_url: &str,
    code: &str,
    state: &str,
    redirect_uri: &str,
    verifier: &str,
) -> Result<ClaudeOAuthCredential, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not prepare the Claude sign-in request: {error}"))?;
    let response = client
        .post(token_url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header("anthropic-beta", "oauth-2025-04-20")
        .header(
            reqwest::header::USER_AGENT,
            "anthropic-sdk-typescript/0.112.1 userOAuthProvider",
        )
        .json(&json!({
            "grant_type": "authorization_code",
            "client_id": CLAUDE_OAUTH_CLIENT_ID,
            "code": code,
            "state": state,
            "redirect_uri": redirect_uri,
            "code_verifier": verifier,
        }))
        .send()
        .await
        .map_err(|error| format!("Could not exchange the Claude sign-in code: {error}"))?;
    parse_claude_token_response(response, Some(now_ms())).await
}

async fn parse_claude_token_response(
    response: reqwest::Response,
    authorized_at_ms: Option<u64>,
) -> Result<ClaudeOAuthCredential, String> {
    let status = response.status();
    if !status.is_success() {
        return Err(format!(
            "Claude rejected the sign-in token request ({status}). Retry sign-in."
        ));
    }
    let token = response
        .json::<ClaudeTokenResponse>()
        .await
        .map_err(|error| format!("Claude returned an invalid sign-in response: {error}"))?;
    if token.access_token.is_empty() || token.refresh_token.is_empty() || token.expires_in == 0 {
        return Err("Claude returned an incomplete sign-in response. Retry sign-in.".to_owned());
    }
    Ok(ClaudeOAuthCredential {
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        expires_at_ms: now_ms()
            .saturating_add(token.expires_in.saturating_mul(1_000))
            .saturating_sub(REFRESH_SKEW_MS),
        authorized_at_ms,
        account_id: token
            .account
            .as_ref()
            .and_then(|account| account.uuid.clone()),
        email: token.account.and_then(|account| account.email_address),
        organization_id: token
            .organization
            .as_ref()
            .and_then(|organization| organization.uuid.clone()),
        organization_name: token
            .organization
            .and_then(|organization| organization.name),
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

async fn refresh_if_needed(
    credential: Option<ClaudeOAuthCredential>,
) -> Result<(Option<ClaudeOAuthCredential>, bool), String> {
    refresh_if_needed_with_url(credential, CLAUDE_TOKEN_URL).await
}

async fn refresh_if_needed_with_url(
    credential: Option<ClaudeOAuthCredential>,
    token_url: &str,
) -> Result<(Option<ClaudeOAuthCredential>, bool), String> {
    let Some(credential) = credential else {
        return Ok((None, false));
    };
    if credential.expires_at_ms > now_ms() {
        return Ok((Some(credential), false));
    }

    let original_refresh_token = credential.refresh_token.clone();
    let mut credential = credential;
    let mut refreshed = REFRESHED_CREDENTIALS.lock().await;
    if let Some(cached) = refreshed.get(&credential.refresh_token).cloned() {
        if cached.expires_at_ms > now_ms() {
            return Ok((Some(cached), true));
        }
        credential = cached;
    }
    refreshed.retain(|_, cached| cached.expires_at_ms > now_ms());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not prepare the Claude refresh request: {error}"))?;
    let response = client
        .post(token_url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header("anthropic-beta", "oauth-2025-04-20")
        .header(
            reqwest::header::USER_AGENT,
            "anthropic-sdk-typescript/0.112.1 userOAuthProvider",
        )
        .json(&json!({
            "grant_type": "refresh_token",
            "client_id": CLAUDE_OAUTH_CLIENT_ID,
            "refresh_token": credential.refresh_token,
        }))
        .send()
        .await
        .map_err(|error| format!("Could not refresh Claude sign-in: {error}. Retry sign-in."))?;
    let mut updated = parse_claude_token_response(response, credential.authorized_at_ms).await?;
    updated.account_id = updated.account_id.or(credential.account_id);
    updated.email = updated.email.or(credential.email);
    updated.organization_id = credential.organization_id;
    updated.organization_name = credential.organization_name;
    refreshed.insert(original_refresh_token, updated.clone());
    refreshed.insert(updated.refresh_token.clone(), updated.clone());
    Ok((Some(updated), true))
}

async fn emit_refreshed_credential(
    events: &mpsc::Sender<BackendEvent>,
    credential: &ClaudeOAuthCredential,
) {
    if let Ok(metadata) = serde_json::to_value(credential) {
        let _ = events
            .send(BackendEvent::AuthenticationCompleted {
                kind: "claude_oauth_pkce".to_owned(),
                metadata,
            })
            .await;
    }
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
            code_mode: _,
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
            code_mode: _,
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
        | BackendCommand::SetSessionCodeMode { .. }
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
            detail: None,
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
            code_mode,
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
            code_mode,
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
        BackendCommand::SetSessionCodeMode { .. } => BackendOperation::SetSessionCodeMode,
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
    fn bridge_uses_the_official_agent_sdk_with_fd_scoped_oauth() {
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
        assert!(PROCESS_LIFECYCLE_SOURCE.contains("CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR"));
        assert!(PROCESS_LIFECYCLE_SOURCE.contains("child.stdio[3].end(oauthAccessToken)"));
        assert!(!PROCESS_LIFECYCLE_SOURCE.contains("env.CLAUDE_CODE_OAUTH_TOKEN ="));
        assert!(BRIDGE_SOURCE.contains("session.timeoutSeconds * 1000"));
    }

    #[tokio::test]
    async fn claude_process_receives_oauth_only_over_fd_three() {
        let directory = tempfile::tempdir().expect("process lifecycle directory");
        tokio::fs::write(
            directory.path().join("process_lifecycle.mjs"),
            PROCESS_LIFECYCLE_SOURCE,
        )
        .await
        .expect("process lifecycle module");
        let runner = r"
import { providerProcessLifecycle } from './process_lifecycle.mjs';
const lifecycle = providerProcessLifecycle('fd-secret-token');
const controller = new AbortController();
const script = `
  const fs = require('node:fs');
  const token = fs.readFileSync(3, 'utf8');
  console.log(JSON.stringify({
    token,
    descriptor: process.env.CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR,
    apiKey: process.env.ANTHROPIC_API_KEY || null,
    authToken: process.env.ANTHROPIC_AUTH_TOKEN || null,
    oauthEnvironment: process.env.CLAUDE_CODE_OAUTH_TOKEN || null,
  }));
`;
const child = lifecycle.spawn({
  command: process.execPath,
  args: ['-e', script],
  cwd: process.cwd(),
  env: {
    ...process.env,
    ANTHROPIC_API_KEY: 'leaked-api-key',
    ANTHROPIC_AUTH_TOKEN: 'leaked-auth-token',
    CLAUDE_CODE_OAUTH_TOKEN: 'leaked-oauth-token',
  },
  signal: controller.signal,
});
let output = '';
child.stdout.on('data', (chunk) => { output += chunk; });
await lifecycle.started();
await lifecycle.released();
process.stdout.write(output);
";
        tokio::fs::write(directory.path().join("runner.mjs"), runner)
            .await
            .expect("runner module");

        let output = Command::new("node")
            .arg("runner.mjs")
            .current_dir(directory.path())
            .output()
            .await
            .expect("run process lifecycle proof");
        assert!(
            output.status.success(),
            "runner failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let observed: Value =
            serde_json::from_slice(&output.stdout).expect("child observation JSON");
        assert_eq!(observed["token"], "fd-secret-token");
        assert_eq!(observed["descriptor"], "3");
        assert!(observed["apiKey"].is_null());
        assert!(observed["authToken"].is_null());
        assert!(observed["oauthEnvironment"].is_null());
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
    #[allow(clippy::too_many_lines)]
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
            code_mode: false,
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
            code_mode: false,
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
            code_mode: false,
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
            code_mode: false,
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
        let catalogue = BRIDGE_SOURCE
            .split("async function modelCatalogue(command)")
            .nth(1)
            .and_then(|source| source.split("async function handle(command)").next())
            .expect("model catalogue implementation");
        assert!(catalogue.contains("providerProcessLifecycle(command.oauthAccessToken)"));
        assert!(catalogue.contains("spawnClaudeCodeProcess: processLifecycle.spawn"));
        assert!(PROCESS_LIFECYCLE_SOURCE.contains("child.once(\"close\""));
        assert!(BRIDGE_SOURCE.contains("process_release_failed"));
    }

    #[test]
    fn claude_process_close_is_the_replacement_send_barrier() {
        let directory = tempfile::tempdir().expect("temporary lifecycle test directory");
        let lifecycle = directory.path().join("process_lifecycle.mjs");
        let test = directory.path().join("lifecycle-test.mjs");
        std::fs::write(&lifecycle, PROCESS_LIFECYCLE_SOURCE).expect("lifecycle fixture");
        std::fs::write(
            &test,
            r#"
import assert from "node:assert/strict";
import { providerProcessLifecycle } from "./process_lifecycle.mjs";

const controller = new AbortController();
const lifecycle = providerProcessLifecycle("token");
lifecycle.spawn({
  command: process.execPath,
  args: ["-e", "setTimeout(() => {}, 100)"],
  cwd: process.cwd(),
  env: process.env,
  signal: controller.signal,
});
await lifecycle.started();
let replacementSent = false;
const redirect = (async () => {
  await lifecycle.released();
  replacementSent = true;
})();
await new Promise(setImmediate);
assert.equal(replacementSent, false, "replacement sent before child close");
await redirect;
assert.equal(replacementSent, true, "replacement did not send after child close");
"#,
        )
        .expect("lifecycle test script");

        let output = std::process::Command::new("node")
            .arg(&test)
            .current_dir(directory.path())
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
            code_mode: false,
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
    fn oauth_credential_replaces_the_external_login_marker() {
        assert!(
            parse_credential(Some(&json!({"external_login":true})))
                .unwrap()
                .is_none()
        );
        assert!(parse_credential(None).unwrap().is_none());
        assert!(parse_credential(Some(&json!({"api_key":"wrong"}))).is_err());

        let credential = parse_credential(Some(&json!({
            "access_token": "access",
            "refresh_token": "refresh",
            "expires_at_ms": 42
        })))
        .expect("valid credential")
        .expect("configured");
        assert_eq!(credential.access_token, "access");
    }

    #[tokio::test]
    async fn claude_callback_accepts_headers_split_across_tcp_reads() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("callback listener");
        let address = listener.local_addr().expect("callback address");
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.expect("callback client");
            stream
                .write_all(b"GET /callback?code=authorization-code&state=expected")
                .await
                .expect("first callback fragment");
            tokio::task::yield_now().await;
            stream
                .write_all(b" HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .expect("second callback fragment");
        });
        let (mut stream, _) = listener.accept().await.expect("callback connection");

        let code = parse_claude_callback(&mut stream, "expected")
            .await
            .expect("split callback");
        client.await.expect("callback client task");
        assert_eq!(code, "authorization-code");
    }

    #[tokio::test]
    async fn shutting_down_closes_an_in_progress_claude_callback() {
        let handle = spawn(BackendConfig::native(PathBuf::from(".")))
            .await
            .expect("unauthenticated backend");
        let (commands, mut events, task) = handle.into_parts();
        assert!(matches!(events.recv().await, Some(BackendEvent::Ready(_))));
        commands
            .send(BackendCommand::BeginAuthentication)
            .await
            .expect("begin authentication");
        let verification_url = match events.recv().await {
            Some(BackendEvent::AuthenticationChallenge {
                verification_url, ..
            }) => verification_url,
            event => panic!("expected authentication challenge, got {event:?}"),
        };
        let verification_url = reqwest::Url::parse(&verification_url).expect("verification URL");
        let redirect_uri = verification_url
            .query_pairs()
            .find_map(|(name, value)| (name == "redirect_uri").then(|| value.into_owned()))
            .expect("redirect URI");
        let callback = reqwest::Url::parse(&redirect_uri).expect("callback URL");
        let callback_address = format!("127.0.0.1:{}", callback.port().expect("callback port"));

        commands
            .send(BackendCommand::Shutdown)
            .await
            .expect("shutdown authentication");
        timeout(Duration::from_secs(2), task)
            .await
            .expect("supervisor shutdown")
            .expect("supervisor task");
        timeout(Duration::from_secs(1), async {
            loop {
                if TcpStream::connect(&callback_address).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("callback listener closed after shutdown");
    }

    #[tokio::test]
    async fn claude_code_exchange_sends_pkce_and_returns_rotatable_credential() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("token listener");
        let endpoint = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("token request");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).await.expect("read token request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(headers_end) =
                    request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&request[..headers_end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::trim)
                                .and_then(|value| value.parse::<usize>().ok())
                        })
                        .unwrap_or_default();
                    if request.len() >= headers_end + 4 + length {
                        break;
                    }
                }
            }
            let text = String::from_utf8(request).expect("UTF-8 request");
            let _ = request_tx.send(text);
            let payload = r#"{"access_token":"access","refresh_token":"refresh","expires_in":3600,"account":{"uuid":"account","email_address":"user@example.com"},"organization":{"uuid":"org","name":"Team"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("token response");
        });

        let credential = exchange_claude_code(
            &endpoint,
            "authorization-code",
            "state",
            "http://localhost:54545/callback",
            "verifier",
        )
        .await
        .expect("token exchange");
        server.await.expect("token server");
        let request = request_rx.await.expect("token request");
        assert!(request.contains("anthropic-beta: oauth-2025-04-20"));
        let body: Value = serde_json::from_str(
            request
                .split("\r\n\r\n")
                .nth(1)
                .expect("token request body"),
        )
        .unwrap();
        assert_eq!(body["grant_type"], "authorization_code");
        assert_eq!(body["code_verifier"], "verifier");
        assert_eq!(credential.refresh_token, "refresh");
        assert_eq!(credential.organization_id.as_deref(), Some("org"));
        assert!(credential.expires_at_ms > now_ms());
    }

    #[tokio::test]
    async fn expired_claude_credential_refreshes_and_preserves_subscription_identity() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("refresh listener");
        let endpoint = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("refresh request");
            let mut request = vec![0_u8; 8192];
            let read = stream
                .read(&mut request)
                .await
                .expect("read refresh request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.contains("refresh-old"));
            assert!(request.contains("refresh_token"));
            assert!(request.contains("anthropic-beta: oauth-2025-04-20"));
            let payload =
                r#"{"access_token":"access-new","refresh_token":"refresh-new","expires_in":7200}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("refresh response");
        });
        let original = ClaudeOAuthCredential {
            access_token: "access-old".to_owned(),
            refresh_token: "refresh-old".to_owned(),
            expires_at_ms: 0,
            authorized_at_ms: Some(123),
            account_id: Some("account".to_owned()),
            email: Some("user@example.com".to_owned()),
            organization_id: Some("org".to_owned()),
            organization_name: Some("Team".to_owned()),
        };
        let (updated, refreshed) = refresh_if_needed_with_url(Some(original), &endpoint)
            .await
            .expect("refresh succeeds");
        server.await.expect("refresh server");
        let updated = updated.expect("credential remains configured");
        assert!(refreshed);
        assert_eq!(updated.access_token, "access-new");
        assert_eq!(updated.refresh_token, "refresh-new");
        assert_eq!(updated.authorized_at_ms, Some(123));
        assert_eq!(updated.account_id.as_deref(), Some("account"));
        assert_eq!(updated.organization_id.as_deref(), Some("org"));
    }

    #[tokio::test]
    async fn only_account_control_publishes_rotated_claude_credentials() {
        let updated = ClaudeOAuthCredential {
            access_token: "access-new".to_owned(),
            refresh_token: "refresh-new".to_owned(),
            expires_at_ms: now_ms() + 3_600_000,
            authorized_at_ms: Some(123),
            account_id: Some("account".to_owned()),
            email: None,
            organization_id: Some("org".to_owned()),
            organization_name: None,
        };
        let expired = |refresh_token: &str| ClaudeOAuthCredential {
            access_token: "access-old".to_owned(),
            refresh_token: refresh_token.to_owned(),
            expires_at_ms: 0,
            authorized_at_ms: Some(123),
            account_id: Some("account".to_owned()),
            email: None,
            organization_id: Some("org".to_owned()),
            organization_name: None,
        };
        REFRESHED_CREDENTIALS
            .lock()
            .await
            .insert("session-refresh".to_owned(), updated.clone());
        REFRESHED_CREDENTIALS
            .lock()
            .await
            .insert("account-refresh".to_owned(), updated);
        let (events, mut received) = mpsc::channel(2);

        let mut session_credential = Some(expired("session-refresh"));
        refresh_supervisor_credential(&mut session_credential, false, &events)
            .await
            .expect("session refresh");
        assert!(received.try_recv().is_err());

        let mut account_credential = Some(expired("account-refresh"));
        refresh_supervisor_credential(&mut account_credential, true, &events)
            .await
            .expect("account refresh");
        assert!(matches!(
            received.recv().await,
            Some(BackendEvent::AuthenticationCompleted { kind, metadata })
                if kind == "claude_oauth_pkce" && metadata["access_token"] == "access-new"
        ));
    }

    #[test]
    fn claude_oauth_url_uses_pkce_and_inference_scopes() {
        let url = claude_authorization_url(
            CLAUDE_AUTHORIZE_URL,
            "http://localhost:54545/callback",
            "state",
            "challenge",
        )
        .expect("authorization URL");
        let url = reqwest::Url::parse(&url).expect("parsed URL");
        assert_eq!(url.path(), "/oauth/authorize");
        assert_eq!(
            url.query_pairs().find(|(key, _)| key == "state").unwrap().1,
            "state"
        );
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "code_challenge_method")
                .unwrap()
                .1,
            "S256"
        );
        assert!(
            url.query_pairs()
                .find(|(key, _)| key == "scope")
                .is_some_and(|(_, value)| value.contains("user:inference"))
        );
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
