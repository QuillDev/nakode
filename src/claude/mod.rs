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
    BackendTokenUsage, CLAUDE_PROVIDER, CREDENTIAL_REFRESH_REQUIRED, CapabilitySupport, DeltaKind,
    ExternalToolRequest, ItemKind, ItemStatus, ModelInfo, NormalizedItem, TurnOutcome,
    request_failed,
};
use crate::credential::SecretValue;

pub(crate) mod delegation;

use delegation::{
    ClaudeDelegations, DELEGATION_CANCELLED_EVENT, DELEGATION_REQUEST_EVENT, DelegationScope,
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const SDK_VERSION: &str = "0.3.280";
static SDK_PREPARATION: Mutex<()> = Mutex::const_new(());
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
    /// Supplied by a broker that alone refreshes it; Nakode holds no refresh token for it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    brokered: bool,
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
    /// Where each session's latest history snapshot is kept, so it reads back while not loaded.
    session_database: Option<PathBuf>,
    /// The workspace server's attributed delegation route for `mcp__nakode__delegate`.
    native_delegation: Option<mpsc::Sender<crate::backend::NativeAgentRequest>>,
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
            session_database: None,
            native_delegation: None,
        }
    }

    #[must_use]
    pub fn with_credential(mut self, credential: Option<Value>) -> Self {
        self.credential = credential;
        self
    }

    #[must_use]
    pub fn with_session_database(mut self, path: PathBuf) -> Self {
        self.session_database = Some(path);
        self
    }

    #[must_use]
    pub fn with_native_delegation(
        mut self,
        requests: mpsc::Sender<crate::backend::NativeAgentRequest>,
    ) -> Self {
        self.native_delegation = Some(requests);
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
/// Returns an error when a stored credential is malformed or cannot be refreshed.
/// SDK preparation belongs to the provider supervisor, not the shared runtime actor.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let credential = parse_credential(config.credential.as_ref())?;
    let (credential, credential_updated) =
        refresh_if_needed(credential)
            .await
            .map_err(|detail| BackendError::InvalidCredential {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail,
            })?;
    // A cold SDK install can need network access. Never await it while the shared
    // runtime is enabling an account: status, other providers, and cancellation
    // must remain available. The supervisor's existing recovery path starts the
    // bridge for its first command and reports setup errors on that provider.
    let bridge = None;
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
    if parsed.access_token.is_empty() || (parsed.refresh_token.is_empty() && !parsed.brokered) {
        return Err(BackendError::InvalidCredential {
            provider: CLAUDE_PROVIDER.to_owned(),
            detail: "OAuth access or refresh token is empty".to_owned(),
        });
    }
    Ok(Some(parsed))
}

/// The broker's view of one Claude sign-in: a current access token and who it belongs to.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokeredClaudeCredential {
    access_token: String,
    expires_at_ms: u64,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    organization_id: Option<String>,
    #[serde(default)]
    organization_name: Option<String>,
}

/// Validates a brokered Claude credential (JSON) and returns the metadata this adapter stores.
///
/// # Errors
///
/// Returns a description when the credential is not the brokered shape or has no access token.
pub fn brokered_credential_metadata(credential: &str) -> Result<Value, String> {
    let brokered = serde_json::from_str::<BrokeredClaudeCredential>(credential)
        .map_err(|error| format!("invalid brokered Claude credential: {error}"))?;
    if brokered.access_token.is_empty() {
        return Err("brokered Claude credential has no access token".to_owned());
    }
    serde_json::to_value(ClaudeOAuthCredential {
        access_token: brokered.access_token,
        refresh_token: String::new(),
        expires_at_ms: brokered.expires_at_ms,
        authorized_at_ms: None,
        account_id: brokered.account_id,
        email: brokered.email,
        organization_id: brokered.organization_id,
        organization_name: brokered.organization_name,
        brokered: true,
    })
    .map_err(|error| error.to_string())
}

async fn spawn_bridge() -> Result<Bridge, BackendError> {
    let directory = prepare_bridge_directory().await?;
    launch_bridge(&directory)
}

async fn prepare_bridge_directory() -> Result<PathBuf, BackendError> {
    // Account and provider-control supervisors share this installation. Lazy
    // startup must not run concurrent npm installs into the same directory.
    let _preparation = SDK_PREPARATION.lock().await;
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
        invalidate_sdk_receipt(&directory)
            .await
            .map_err(|error| BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: error.to_string(),
            })?;
        let mut install = Command::new("npm");
        install
            .envs(crate::machine_path::environment())
            .args([
                "install",
                "--omit=dev",
                "--no-audit",
                "--no-fund",
                "--package-lock=false",
            ])
            .current_dir(&directory);
        let output = sdk_install_output(install, Duration::from_secs(120))
            .await
            .map_err(|error| BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: format!("Node.js 18+ and npm SDK setup failed: {error}"),
            })?;
        if !output.status.success() {
            return Err(BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        tokio::fs::write(directory.join(".nakode-sdk-ready"), SDK_VERSION)
            .await
            .map_err(|error| BackendError::BridgeSetup {
                provider: CLAUDE_PROVIDER.to_owned(),
                detail: error.to_string(),
            })?;
    }
    Ok(directory)
}

async fn sdk_install_output(
    mut command: Command,
    deadline: Duration,
) -> std::io::Result<std::process::Output> {
    // Dropping the timed-out/cancelled wait must not leave npm running behind a
    // failed provider setup. Tokio also reaps the terminated direct child.
    command.kill_on_drop(true);
    timeout(deadline, command.output()).await.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Claude SDK installation timed out; check registry connectivity and retry",
        )
    })?
}

async fn invalidate_sdk_receipt(directory: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::remove_file(directory.join(".nakode-sdk-ready")).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn claude_sdk_is_current(directory: &std::path::Path) -> bool {
    // npm can write a matching manifest before dependencies finish. A failed or
    // cancelled installation must not be accepted as a complete SDK on retry.
    if tokio::fs::read_to_string(directory.join(".nakode-sdk-ready"))
        .await
        .ok()
        .as_deref()
        != Some(SDK_VERSION)
        || !directory
            .join("node_modules/@anthropic-ai/claude-agent-sdk/sdk.mjs")
            .is_file()
    {
        return false;
    }
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

/// Starts the Node bridge. It never receives a Nakode executable: delegation crosses back to the
/// supervisor as a bridge event, so an embedding host binary (such as `FStack`) needs no Nakode CLI.
fn launch_bridge(directory: &std::path::Path) -> Result<Bridge, BackendError> {
    start_bridge_process(bridge_command(directory))
}

fn bridge_command(directory: &std::path::Path) -> Command {
    let mut command = Command::new("node");
    command
        .envs(crate::machine_path::environment())
        .arg(directory.join("bridge.mjs"))
        .current_dir(directory)
        // Not even an inherited value: delegation must never shell out to a Nakode CLI.
        .env_remove("NAKODE_EXECUTABLE")
        .env_remove("NAKODE_WORKSPACE")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The bridge offers browser_control only where this machine can run a browser for it.
    #[cfg(unix)]
    let browser = crate::tools::browser_control::available();
    #[cfg(not(unix))]
    let browser = false;
    if browser {
        command.env("NAKODE_BROWSER_CONTROL", "1");
    } else {
        command.env_remove("NAKODE_BROWSER_CONTROL");
    }
    command
}

fn start_bridge_process(mut command: Command) -> Result<Bridge, BackendError> {
    let mut child = command.spawn().map_err(|source| BackendError::Spawn {
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
        .envs(crate::machine_path::environment())
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
    let mut returned_images = ReturnedImages::default();
    let mut deferred_command = None;
    let mut delegation_scope = DelegationScope::default();
    let mut delegations = ClaudeDelegations::new(config.native_delegation.clone());
    let mut browser_calls = BrowserCalls::default();
    // Background task calls may wait on process termination, so they run off this loop.
    let (background_replies_tx, mut background_replies) = mpsc::channel::<Value>(16);
    let mut authentication_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut authentication_callback: Option<mpsc::Sender<String>> = None;
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
                // Interrupts, unsubscribes and shutdown cancel pending delegations even when the
                // command itself cannot reach the bridge.
                delegations.observe_command(&command);
                browser_calls.observe_command(&command);
                let mut command = command;
                if let BackendCommand::ResolveExternalTool { id, output, images, .. } = &mut command
                    && let Some(note) = retain_external_tool_images(id, images, &mut returned_images, &events).await
                {
                    output.push_str("\n\n");
                    output.push_str(&note);
                }
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(task) = authentication_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    if let Some(bridge) = bridge.as_mut() { let _ = send(bridge, json!({"method":"shutdown"})).await; }
                    break;
                }
                // A remote client starts the same browser flow: the challenge carries the localhost
                // redirect URI, and the client submits the callback URL its browser landed on.
                if matches!(command, BackendCommand::BeginAuthentication { .. }) {
                    if let Some(task) = authentication_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    let authentication_events = events.clone();
                    let (callback_tx, callback_rx) = mpsc::channel(1);
                    authentication_callback = Some(callback_tx);
                    authentication_task = Some(tokio::spawn(async move {
                        authenticate(&authentication_events, callback_rx).await;
                    }));
                    continue;
                }
                if let BackendCommand::SubmitAuthenticationCallback { callback_url } = command {
                    let Some(callback) = authentication_callback.as_ref() else {
                        request_failed(&events, BackendOperation::Authenticate, "no in-progress browser authentication challenge").await;
                        continue;
                    };
                    if callback.send(callback_url).await.is_err() {
                        request_failed(&events, BackendOperation::Authenticate, "browser authentication challenge is no longer active").await;
                    }
                    continue;
                }
                // A replaced credential applies from the next command; the native session continues.
                if let BackendCommand::UpdateCredential { credential: replacement } = command {
                    match parse_credential(replacement.as_ref().map(SecretValue::expose)) {
                        Ok(replacement) => credential = replacement,
                        Err(error) => {
                            request_failed(&events, BackendOperation::Authenticate, error.to_string()).await;
                        }
                    }
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
                delegation_scope.observe(&command);
                if should_recover_bridge(credential.is_some(), bridge.is_some(), &command) {
                    match spawn_bridge().await {
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
                let owner = session_owner(attachment.as_ref());
                handle_command(command, &config, credential.as_ref(), bridge.as_mut(), &events, owner).await;
            }
            Some(reply) = background_replies.recv() => {
                if reply["method"] == "resolve_browser" {
                    browser_calls.finished(&string(&reply, "id"));
                }
                if let Some(bridge) = bridge.as_mut() {
                    let _ = send(bridge, reply).await;
                }
            }
            completion = delegations.next_completion() => {
                if let Some(bridge) = bridge.as_mut() {
                    let _ = send(bridge, completion.bridge_reply()).await;
                }
            }
            () = wait_until_credential_refresh(credential.as_ref()), if publish_credential_updates && credential.as_ref().is_some_and(|credential| !credential.brokered) => {
                if let Err(message) = refresh_supervisor_credential(&mut credential, true, &events).await {
                    request_failed(&events, BackendOperation::Reload, message).await;
                }
            }
            message = async { bridge.as_mut().expect("guarded").messages.recv().await }, if bridge.is_some() => {
                let Some(mut message) = message else {
                    // The exited bridge's tool calls can no longer receive a result.
                    delegations.cancel_all();
                    browser_calls.cancel_all();
                    returned_images.external_calls.clear();
                    let _ = events.send(BackendEvent::Disconnected { reason: "Claude SDK bridge exited".to_owned() }).await;
                    if let Some(stopped) = bridge.take() {
                        stopped.task.abort();
                    }
                    if credential.is_some() {
                        match spawn_bridge().await {
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
                if matches!(message["event"].as_str(), Some("history_snapshot" | "session_resumed"))
                    && let Some(database) = config.session_database.clone()
                {
                    keep_history_snapshot(database, &mut message).await;
                }
                let event_name = message.get("event").and_then(Value::as_str);
                if event_name == Some("history_snapshot") {
                    continue;
                }
                if event_name == Some("desktop_request") {
                    let reply = desktop_request(&message, session_owner(attachment.as_ref())).await;
                    if let Some(bridge) = bridge.as_mut() {
                        let _ = send(bridge, reply).await;
                    }
                    continue;
                }
                if event_name == Some("background_task_request") {
                    // The owner comes from control-plane attachment, never from the payload.
                    let caller = delegation_scope
                        .background_task_caller(string(&message, "turnId"), string(&message, "callId"));
                    let replies = background_replies_tx.clone();
                    tokio::spawn(async move {
                        let workspace = PathBuf::from(string(&message, "workspace"));
                        let (output, failed) = match crate::background_tasks::execute_tool_call(
                            caller,
                            &workspace,
                            &message["arguments"],
                        )
                        .await
                        {
                            Ok(output) => (output, false),
                            Err(error) => (error, true),
                        };
                        let _ = replies
                            .send(json!({"method": "resolve_background_task", "id": string(&message, "id"), "output": output, "failed": failed}))
                            .await;
                    });
                    continue;
                }
                if event_name == Some("browser_request") {
                    // The owner comes from control-plane attachment, never from the payload.
                    let owner = session_owner(attachment.as_ref()).map(str::to_owned);
                    let replies = background_replies_tx.clone();
                    let cancellation = browser_calls.start(&message);
                    tokio::spawn(async move {
                        let reply = browser_request(&message, owner.as_deref(), &cancellation).await;
                        let _ = replies.send(reply).await;
                    });
                    continue;
                }
                if event_name == Some("browser_cancelled") {
                    browser_calls.cancel(&string(&message, "id"));
                    continue;
                }
                if event_name == Some(DELEGATION_REQUEST_EVENT) {
                    // Identity is bound from the control plane; any identity in the payload is ignored.
                    if let Some(refused) = delegations.start(&message, &delegation_scope)
                        && let Some(bridge) = bridge.as_mut()
                    {
                        let _ = send(bridge, refused.bridge_reply()).await;
                    }
                    continue;
                }
                if event_name == Some(DELEGATION_CANCELLED_EVENT) {
                    delegations.cancel(&string(&message, "id"));
                    continue;
                }
                if event_name == Some("image_return_request") {
                    let (output, failed) = match return_image(&message, &mut returned_images, &events).await {
                        Ok(output) => (output, false),
                        Err(error) => (error, true),
                    };
                    if let Some(bridge) = bridge.as_mut() {
                        let _ = send(bridge, json!({"method":"resolve_image_return","id":string(&message, "id"),"output":output,"failed":failed})).await;
                    }
                    continue;
                }
                // An ended turn's unresolved external calls (interrupted, say) never return images.
                if matches!(event_name, Some("turn_completed" | "turn_start_failed")) {
                    returned_images.forget_turn_calls(&string(&message, "turnId"));
                }
                if event_name == Some("external_tool_request") {
                    returned_images.external_calls.insert(
                        string(&message, "id"),
                        (string(&message, "turnId"), string(&message, "model")),
                    );
                }
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
                        session_owner(attachment.as_ref()),
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
    handle_command(command, config, credential, bridge.as_mut(), events, None).await;
    recovery_event
}

async fn replay_after_reattach(
    session_options: Option<BackendCommand>,
    deferred_command: Option<BackendCommand>,
    config: &BackendConfig,
    credential: Option<&ClaudeOAuthCredential>,
    bridge: &mut Option<Bridge>,
    events: &mpsc::Sender<BackendEvent>,
    owner: Option<&str>,
) {
    if let Some(command) = session_options {
        handle_command(command, config, credential, bridge.as_mut(), events, owner).await;
    }
    if let Some(command) = deferred_command {
        handle_command(command, config, credential, bridge.as_mut(), events, owner).await;
    }
}

/// The logical Nakode session a Claude backend serves; its environment is keyed by it.
fn session_owner(attachment: Option<&BackendCommand>) -> Option<&str> {
    match attachment? {
        BackendCommand::StartSession {
            owner_session_id, ..
        }
        | BackendCommand::ResumeSession {
            owner_session_id, ..
        } => owner_session_id.as_deref(),
        _ => None,
    }
}

async fn handle_command(
    command: BackendCommand,
    config: &BackendConfig,
    credential: Option<&ClaudeOAuthCredential>,
    bridge: Option<&mut Bridge>,
    events: &mpsc::Sender<BackendEvent>,
    owner: Option<&str>,
) {
    if matches!(
        command,
        BackendCommand::BeginAuthentication {
            client_context: crate::backend::ClientContext::Remote
        }
    ) {
        request_failed(
            events,
            BackendOperation::Authenticate,
            "Claude remote authentication is unsupported; use a client on the Nakode server machine",
        )
        .await;
        return;
    }
    if matches!(command, BackendCommand::BeginAuthentication { .. }) {
        let (_callback_tx, callback_rx) = mpsc::channel(1);
        authenticate(events, callback_rx).await;
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
    // Claude Code's own tools run in its process, so a turn carries the session's environment
    // (account variables such as GH_TOKEN) the way Nakode's own shell tools receive it.
    if method == "send" {
        object.insert(
            "environment".to_owned(),
            json!(crate::session_environment::read(owner)),
        );
    }
    if let Err(error) = send(bridge, payload).await {
        request_failed(events, operation_for_method(method), error).await;
    }
}

async fn authenticate(events: &mpsc::Sender<BackendEvent>, callback_rx: mpsc::Receiver<String>) {
    if let Err(message) =
        run_claude_oauth(CLAUDE_AUTHORIZE_URL, CLAUDE_TOKEN_URL, events, callback_rx).await
    {
        request_failed(events, BackendOperation::Authenticate, message).await;
    }
}

async fn run_claude_oauth(
    authorize_url: &str,
    token_url: &str,
    events: &mpsc::Sender<BackendEvent>,
    callback_rx: mpsc::Receiver<String>,
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
            callback_url: Some(redirect_uri.clone()),
        })
        .await
        .map_err(|_| "Claude sign-in was cancelled".to_owned())?;
    let code = timeout(
        AUTHENTICATION_TIMEOUT,
        receive_claude_authorization_code(&listener, &state, callback_rx),
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
    callback_rx: mpsc::Receiver<String>,
) -> Result<String, String> {
    let mut callback_rx = callback_rx;
    loop {
        let (mut stream, _) = tokio::select! {
            accepted = listener.accept() => accepted
                .map_err(|error| format!("Claude sign-in callback failed: {error}"))?,
            callback = callback_rx.recv() => {
                let callback = callback.ok_or_else(|| "Claude sign-in callback channel closed".to_owned())?;
                relay_claude_callback(listener, &callback).await?;
                listener.accept().await
                    .map_err(|error| format!("Claude sign-in callback relay failed: {error}"))?
            }
        };
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

async fn relay_claude_callback(
    listener: &ClaudeCallbackListeners,
    callback_url: &str,
) -> Result<(), String> {
    let url = reqwest::Url::parse(callback_url)
        .map_err(|_| "Claude sign-in callback URL was malformed".to_owned())?;
    let target = url.query().map_or_else(
        || url.path().to_owned(),
        |query| format!("{}?{query}", url.path()),
    );
    let address = listener
        .ipv4
        .local_addr()
        .map_err(|error| format!("Could not inspect Claude callback listener: {error}"))?;
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| format!("Could not relay Claude callback: {error}"))?;
    let request = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("Could not relay Claude callback: {error}"))
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
        brokered: false,
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
    if credential.brokered {
        return Err(format!(
            "{CREDENTIAL_REFRESH_REQUIRED}: the brokered Claude sign-in expired; its broker must supply a new access token"
        ));
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

fn bridge_image_blocks(attachments: &[crate::backend::PromptAttachment]) -> Vec<Value> {
    attachments
        .iter()
        .filter_map(|attachment| {
            attachment.image.as_ref().map(|image| {
                json!({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image.mime_type,
                        "data": base64::engine::general_purpose::STANDARD.encode(&image.data),
                    },
                })
            })
        })
        .collect()
}

fn bridge_request(command: BackendCommand) -> Result<Option<BridgeRequest>, UnsupportedCommand> {
    let (method, payload) = match command {
        BackendCommand::StartSession {
            model,
            instructions,
            owner_session_id,
            parent_run_id,
            external_tools,
            replace_builtin_tools,
            allowed_builtin_tools,
            max_turns,
            finalization_reserve_turns,
            timeout_seconds,
            ..
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
            attachments,
            model,
            skill_catalogue: _,
        } => (
            "send",
            json!({"sessionId":provider_session_id,"turnId":client_id,"prompt":prompt,"model":model,"images":bridge_image_blocks(&attachments)}),
        ),
        BackendCommand::InterruptTurn { turn_id, .. } => ("cancel", json!({"turnId":turn_id})),
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
        command @ BackendCommand::ResolveExternalTool { .. } => (
            "resolve_external_tool",
            external_tool_result_payload(command),
        ),
        BackendCommand::ResolveQuestion { .. }
        | BackendCommand::SetSessionCodeMode { .. }
        | BackendCommand::BeginAuthentication { .. }
        | BackendCommand::SubmitAuthenticationCallback { .. }
        | BackendCommand::UpdateCredential { .. }
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

/// How many images, and how many bytes, each turn has returned so far.
#[derive(Default)]
struct ReturnedImages {
    turns: HashMap<String, (usize, usize)>,
    /// Pending external tool calls by bridge id: their turn and model, for any images they return.
    external_calls: HashMap<String, (String, String)>,
}

impl ReturnedImages {
    fn forget_turn_calls(&mut self, turn_id: &str) {
        self.external_calls.retain(|_, (turn, _)| turn != turn_id);
    }
}

/// An external tool result for the bridge. Claude sees its images as MCP image content.
fn external_tool_result_payload(command: BackendCommand) -> Value {
    use base64::Engine as _;
    let BackendCommand::ResolveExternalTool {
        id,
        output,
        failed,
        images,
    } = command
    else {
        unreachable!("only external tool results have this payload");
    };
    let images: Vec<Value> = images
        .iter()
        .map(|image| {
            json!({
                "data": base64::engine::general_purpose::STANDARD.encode(&image.data),
                "mimeType": image.mime_type,
            })
        })
        .collect();
    json!({"id": id, "output": output, "failed": failed, "images": images})
}

/// Retains an external tool result's images for every client, as one reply, under the same
/// per-turn bounds as `return_image`. Claude always sees them in the tool result; past those
/// bounds the rest are only not kept. Returns the note to append to the result text then.
async fn retain_external_tool_images(
    call_id: &str,
    images: &[crate::backend::PromptImage],
    returned: &mut ReturnedImages,
    events: &mpsc::Sender<BackendEvent>,
) -> Option<String> {
    use crate::tools::return_image::{MAX_BYTES_PER_TURN, MAX_IMAGES_PER_TURN};
    let (turn_id, model_id) = returned.external_calls.remove(call_id)?;
    if images.is_empty() {
        return None;
    }
    let (count, bytes) = returned.turns.get(&turn_id).copied().unwrap_or_default();
    // Keep the images in order while they fit the turn's bounds, rather than none of them.
    let mut kept = 0;
    let mut added = 0usize;
    for image in images {
        let next = bytes.saturating_add(added).saturating_add(image.data.len());
        if count + kept >= MAX_IMAGES_PER_TURN || next > MAX_BYTES_PER_TURN {
            break;
        }
        kept += 1;
        added += image.data.len();
    }
    let note = (kept < images.len())
        .then(|| crate::tools::return_image::kept_images_note(kept, images.len()));
    if kept == 0 {
        return note;
    }
    returned
        .turns
        .insert(turn_id.clone(), (count + kept, bytes + added));
    let mut attachments = images[..kept]
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, image)| crate::backend::PromptAttachment {
            label: format!("Tool image {}", index + 1),
            path: None,
            image: Some(image),
        });
    let first = attachments.next()?;
    let image = crate::runtime::ReturnedImage {
        id: format!("{turn_id}:image:{call_id}"),
        turn_id,
        provider_id: CLAUDE_PROVIDER.to_owned(),
        model_id,
        history_index: 0,
        sequence: count,
        attachment: first,
        more_attachments: attachments.collect(),
    };
    let _ = events.send(BackendEvent::ImageReturned(image)).await;
    note
}

/// Claude's `return_image`: the bridge names a file, and Nakode reads, validates and attaches it
/// exactly as the native tool does, so the bytes never pass through the provider.
async fn return_image(
    message: &Value,
    returned: &mut ReturnedImages,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<String, String> {
    use crate::tools::return_image::{
        LoadedImage, MAX_BYTES_PER_TURN, MAX_IMAGES_PER_TURN, load_returnable_images,
        requested_image_paths, returned_images_output,
    };
    let turn_id = string(message, "turnId");
    let call_id = string(message, "id");
    let paths = requested_image_paths(message)?;
    let (count, bytes) = returned.turns.get(&turn_id).copied().unwrap_or_default();
    if count + paths.len() > MAX_IMAGES_PER_TURN {
        return Err("at most eight images may be returned per turn".to_owned());
    }
    let workspace = string(message, "workspace");
    let loaded = load_returnable_images(std::path::Path::new(&workspace), &paths).await?;
    let added: usize = loaded.iter().map(|image| image.data.len()).sum();
    if bytes.saturating_add(added) > MAX_BYTES_PER_TURN {
        return Err("returned images exceed 20 MiB per turn".to_owned());
    }
    returned
        .turns
        .insert(turn_id.clone(), (count + loaded.len(), bytes + added));
    let total = loaded.len();
    // One call is one message: every image it names travels in the same reply.
    let mut attachments = loaded.into_iter().map(
        |LoadedImage {
             label,
             mime_type,
             data,
         }| crate::backend::PromptAttachment {
            label,
            path: None,
            image: Some(crate::backend::PromptImage { mime_type, data }),
        },
    );
    let first = attachments.next().ok_or("no image to return")?;
    let image = crate::runtime::ReturnedImage {
        id: format!("{turn_id}:image:{call_id}"),
        turn_id,
        provider_id: CLAUDE_PROVIDER.to_owned(),
        model_id: string(message, "model"),
        history_index: 0,
        sequence: count,
        attachment: first,
        more_attachments: attachments.collect(),
    };
    events
        .send(BackendEvent::ImageReturned(image))
        .await
        .map_err(|_| "session event receiver closed".to_owned())?;
    Ok(returned_images_output(total))
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

/// The Agent SDK catalogue does not report input modalities. Normalize the documented
/// Opus 5.5 contract here, never from a display label or a client-side provider guess.
/// A resolved identity takes precedence over mutable aliases; unknown targets fail closed.
fn claude_native_image_input(id: &str, resolved_model: Option<&str>) -> bool {
    let identity = resolved_model.unwrap_or(id);
    matches!(identity, "claude-opus-5-5" | "claude-opus-5-5[1m]")
        || (resolved_model.is_none() && matches!(id, "opus" | "opus[1m]"))
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
                display_name: claude_model_display_name(
                    model
                        .get("displayName")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                    model
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                ),
                capabilities: crate::backend::ModelCapabilities {
                    accepts_image_input: claude_native_image_input(
                        model.get("id")?.as_str()?,
                        model.get("resolvedModel").and_then(Value::as_str),
                    ),
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
        .collect::<Vec<_>>();
    BackendEvent::Models(distinguish_context_windows(models))
}

/// Preserve a model-filter preference when discovery retires its explicit 1M picker row.
/// This is presentation compatibility only: session model IDs are never rewritten. When the
/// selected row still exists, its context-window distinction remains exact.
pub(crate) fn matches_retired_model_filter(
    selected: &str,
    candidate: &str,
    discovered: &[ModelInfo],
) -> bool {
    selected.strip_suffix("[1m]") == Some(candidate)
        && !discovered
            .iter()
            .any(|model| model.provider == CLAUDE_PROVIDER && model.id == selected)
}

/// Two rows of one model differ only in their context window (`opus` and `opus[1m]` both read
/// "Opus 5.5"), which the name leaves out; where names collide, the row whose identifier carries a
/// window says so.
fn distinguish_context_windows(mut models: Vec<ModelInfo>) -> Vec<ModelInfo> {
    let mut counts = std::collections::HashMap::<String, usize>::new();
    for model in &models {
        *counts.entry(model.display_name()).or_default() += 1;
    }
    for model in &mut models {
        if counts.get(&model.display_name()).copied().unwrap_or(0) < 2 {
            continue;
        }
        if let Some(window) = context_window_of(&model.id) {
            model.display_name = Some(format!("{} ({window} context)", model.display_name()));
        }
    }
    models
}

/// The context window a Claude Code model identifier asks for: `opus[1m]` → `1M`.
fn context_window_of(id: &str) -> Option<String> {
    let window = id.strip_suffix(']')?.rsplit_once('[')?.1;
    let valid = !window.is_empty()
        && window
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, 'k' | 'm' | '.'));
    valid.then(|| window.to_ascii_uppercase())
}

/// Claude Code's picker rows carry the family as the display name ("Fable", "Sonnet",
/// "Opus (1M context)") and the versioned name as the first segment of the description
/// ("Fable 5.1 · Most capable …", "Opus 5 with 1M context · Best for …"). People know the models
/// by the versioned name, so that segment wins whenever it clearly names the same family;
/// otherwise the display name stands, and an empty display name defers to the identifier. The
/// context window is a property of the row's identifier, not part of the name, so a trailing
/// "with 1M context" or "(1M context)" is dropped either way.
fn claude_model_display_name(display_name: &str, description: &str) -> Option<String> {
    let display_name = without_context_window(display_name.trim());
    if display_name.is_empty() {
        return None;
    }
    let family = display_name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .find(|part| !part.is_empty())
        .unwrap_or(display_name);
    let versioned = description
        .split(" · ")
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty() && segment.len() <= 48)
        .filter(|segment| {
            segment
                .split(|character: char| !character.is_ascii_alphanumeric())
                .find(|part| !part.is_empty())
                .is_some_and(|first| first.eq_ignore_ascii_case(family))
        });
    Some(without_context_window(versioned.unwrap_or(display_name)).to_owned())
}

/// "Opus 5 with 1M context" → "Opus 5"; "Opus (1M context)" → "Opus".
fn without_context_window(name: &str) -> &str {
    let lower = name.to_ascii_lowercase();
    let Some(position) = lower.rfind("context") else {
        return name;
    };
    let head = &lower[..position];
    let marker = [" with ", " ("]
        .iter()
        .filter_map(|marker| head.rfind(marker).map(|index| (index, *marker)))
        .max_by_key(|(index, _)| *index);
    let Some((index, marker)) = marker else {
        return name;
    };
    let window = head[index + marker.len()..].trim();
    let is_window = !window.is_empty()
        && window
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, 'k' | 'm' | '.'));
    if !is_window {
        return name;
    }
    name[..index].trim_end()
}

/// Runs one desktop tool call from the bridge. Screenshots, including the one taken after each
/// action, come back as an image the model sees.
pub(crate) async fn desktop_request(message: &Value, owner: Option<&str>) -> Value {
    use crate::tools::desktop;
    use base64::Engine as _;
    let workspace = PathBuf::from(string(message, "workspace"));
    let arguments = &message["arguments"];
    let session = owner.map_or_else(|| string(message, "turnId"), str::to_owned);
    let outcome = match string(message, "tool").as_str() {
        desktop::SCREENSHOT_TOOL => desktop::screenshot(&workspace).await.map(|shot| {
            (
                format!("The {}x{} desktop.", shot.width, shot.height),
                Some(shot),
            )
        }),
        desktop::ACTION_TOOL => match desktop::act(arguments).await {
            Ok(said) => {
                // Let the screen settle so the image shows the action's effect.
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok(match desktop::screenshot(&workspace).await {
                    Ok(shot) => (said, Some(shot)),
                    Err(error) => (format!("{said}; the screenshot failed: {error}"), None),
                })
            }
            Err(error) => Err(error),
        },
        desktop::RECORD_TOOL => match arguments["action"].as_str() {
            Some("start") => desktop::record_start(&session, arguments["name"].as_str())
                .await
                .map(|said| (said, None)),
            Some("stop") => desktop::record_stop(&session, &workspace)
                .await
                .map(|saved| (desktop::describe(&saved), None)),
            _ => Err("`action` must be \"start\" or \"stop\"".to_owned()),
        },
        other => Err(format!("unknown desktop tool {other:?}")),
    };
    let (output, failed, image) = match outcome {
        Ok((said, shot)) => (
            said,
            false,
            shot.map(|shot| {
                json!({
                    "data": base64::engine::general_purpose::STANDARD.encode(shot.png),
                    "mimeType": "image/png",
                })
            }),
        ),
        Err(error) => (error, true, None),
    };
    json!({"method": "resolve_desktop", "id": string(message, "id"), "output": output, "failed": failed, "image": image})
}

/// In-flight `browser_control` calls from the bridge, so an interrupted turn, a cancelled tool
/// call or an exited bridge stops them instead of letting queued input run afterwards.
#[derive(Default)]
struct BrowserCalls {
    pending: HashMap<String, (String, tokio_util::sync::CancellationToken)>,
}

impl BrowserCalls {
    fn start(&mut self, message: &Value) -> tokio_util::sync::CancellationToken {
        let cancellation = tokio_util::sync::CancellationToken::new();
        self.pending.insert(
            string(message, "id"),
            (string(message, "turnId"), cancellation.clone()),
        );
        cancellation
    }

    fn finished(&mut self, id: &str) {
        self.pending.remove(id);
    }

    fn cancel(&mut self, id: &str) {
        if let Some((_, cancellation)) = self.pending.remove(id) {
            cancellation.cancel();
        }
    }

    fn cancel_all(&mut self) {
        for (_, (_, cancellation)) in self.pending.drain() {
            cancellation.cancel();
        }
    }

    fn observe_command(&mut self, command: &BackendCommand) {
        match command {
            BackendCommand::InterruptTurn { turn_id, .. } => {
                self.pending.retain(|_, (turn, cancellation)| {
                    let interrupted = turn == turn_id;
                    if interrupted {
                        cancellation.cancel();
                    }
                    !interrupted
                });
            }
            BackendCommand::UnsubscribeSession { .. } | BackendCommand::Shutdown => {
                self.cancel_all();
            }
            _ => {}
        }
    }
}

/// Runs one `browser_control` call from the bridge. A screenshot comes back as an image the
/// model sees; other actions return only their compact page state.
async fn browser_request(
    message: &Value,
    owner: Option<&str>,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Value {
    use base64::Engine as _;
    let id = string(message, "id");
    let Some(owner) = owner else {
        return json!({"method": "resolve_browser", "id": id, "output": "browser_control needs an owning Nakode session", "failed": true});
    };
    let workspace = PathBuf::from(string(message, "workspace"));
    #[cfg(unix)]
    let outcome =
        crate::tools::browser_control::run(owner, &workspace, &message["arguments"], cancellation)
            .await
            .map(|outcome| (outcome.text, outcome.image));
    #[cfg(not(unix))]
    let outcome: Result<(String, Option<Vec<u8>>), String> = Err(format!(
        "browser_control is not supported on this machine (session {owner}, {})",
        workspace.display()
    ));
    match outcome {
        Ok((text, image)) => json!({
            "method": "resolve_browser",
            "id": id,
            "output": text,
            "failed": false,
            "image": image.map(|png| json!({
                "data": base64::engine::general_purpose::STANDARD.encode(png),
                "mimeType": "image/png",
            })),
        }),
        Err(error) => {
            json!({"method": "resolve_browser", "id": id, "output": error, "failed": true})
        }
    }
}

/// Keeps the bridge's latest history for a session. The Claude SDK owns the transcript itself;
/// this copy exists only so a session that is not loaded can still be read.
async fn keep_history_snapshot(database: PathBuf, message: &mut Value) {
    use rusqlite::OptionalExtension as _;
    let session_id = string(message, "sessionId");
    let Some(mut history) = message
        .get("history")
        .filter(|history| history.is_array())
        .cloned()
    else {
        return;
    };
    if session_id.is_empty() {
        return;
    }
    let saved = tokio::task::spawn_blocking(move || -> Result<Value, rusqlite::Error> {
        let mut connection = rusqlite::Connection::open(&database)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let transaction = connection.transaction()?;
        let previous: Option<String> = transaction.query_row(
            "SELECT history_json FROM provider_history_snapshots WHERE provider = ?1 AND session_id = ?2",
            rusqlite::params![CLAUDE_PROVIDER, session_id],
            |row| row.get(0),
        ).optional()?;
        if let Some(previous) = previous.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()) {
            retain_history_permissions(&mut history, &previous);
        }
        transaction.execute(
            "INSERT INTO provider_history_snapshots (provider, session_id, history_json, updated_at)
             VALUES (?1, ?2, ?3, unixepoch())
             ON CONFLICT(provider, session_id) DO UPDATE SET
               history_json = excluded.history_json,
               updated_at = excluded.updated_at",
            rusqlite::params![CLAUDE_PROVIDER, session_id, history.to_string()],
        )?;
        transaction.commit()?;
        Ok(history)
    })
    .await;
    match saved {
        Ok(Ok(history)) => message["history"] = history,
        Ok(Err(error)) => eprintln!("claude: could not keep the session history snapshot: {error}"),
        Err(error) => eprintln!("claude: history snapshot task failed: {error}"),
    }
}

// The SDK's history does not retain host callback provenance. Preserve only Nakode's prior
// evidence for the same provider session and exact call id, never infer it from model/tool text.
fn retain_history_permissions(history: &mut Value, previous: &Value) {
    let permissions: std::collections::HashMap<_, _> = previous
        .as_array()
        .into_iter()
        .flatten()
        .filter(|row| row["kind"] == "tool" && row["permission"].is_object())
        .filter_map(|row| row["id"].as_str().map(|id| (id, row)))
        .collect();
    for row in history.as_array_mut().into_iter().flatten() {
        if row["kind"] != "tool"
            || (row["permission"].is_object() && row["permission"]["source"] != "sdk_unknown")
        {
            continue;
        }
        let Some(saved) = row["id"].as_str().and_then(|id| permissions.get(id)) else {
            continue;
        };
        for field in ["permission", "denied", "denialReason"] {
            row[field] = saved[field].clone();
        }
    }
}

/// Keeps a history snapshot for each retained Claude session that has none yet, reading the
/// Claude SDK's transcript through one short-lived bridge without resuming any session.
pub async fn backfill_history_snapshots(database: PathBuf, sessions: Vec<(String, PathBuf)>) {
    let missing: Vec<_> = sessions
        .into_iter()
        .filter(|(session_id, _)| matches!(retained_history(&database, session_id), Ok(None)))
        .collect();
    if missing.is_empty() {
        return;
    }
    let mut bridge = match spawn_bridge().await {
        Ok(bridge) => bridge,
        Err(error) => {
            eprintln!("claude: history snapshot backfill skipped: {error}");
            return;
        }
    };
    'sessions: for (session_id, workspace) in missing {
        let request = json!({"method":"history","sessionId":session_id,"workspace":workspace});
        if send(&mut bridge, request).await.is_err() {
            break;
        }
        loop {
            match timeout(Duration::from_secs(30), bridge.messages.recv()).await {
                Ok(Some(mut message)) if message["event"] == "history_snapshot" => {
                    keep_history_snapshot(database.clone(), &mut message).await;
                    break;
                }
                Ok(Some(_)) => {}
                _ => break 'sessions,
            }
        }
    }
    bridge.task.abort();
}

/// The last history snapshot kept for a Claude session, when there is one.
///
/// # Errors
/// Returns an error when the session store cannot be read.
pub fn retained_history(
    database: &std::path::Path,
    provider_session_id: &str,
) -> Result<Option<Vec<crate::backend::SessionHistoryItem>>, String> {
    use rusqlite::OptionalExtension as _;
    let connection = rusqlite::Connection::open(database)
        .map_err(|error| format!("failed to open session store: {error}"))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| format!("failed to configure session store: {error}"))?;
    let history = connection
        .query_row(
            "SELECT history_json FROM provider_history_snapshots
             WHERE provider = ?1 AND session_id = ?2",
            rusqlite::params![CLAUDE_PROVIDER, provider_session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| format!("failed to read Claude history snapshot: {error}"))?;
    history
        .map(|history| {
            serde_json::from_str::<Value>(&history)
                .map(|history| session_history(&json!({ "history": history })))
                .map_err(|error| format!("invalid Claude history snapshot: {error}"))
        })
        .transpose()
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
            let body = string(value, "body");
            // Saved history keeps a tool call as `{input, output}` in its body; rebuild the audit a
            // live call carries so a restored transcript presents it the same way.
            let saved_tool = (kind == ItemKind::Tool)
                .then(|| serde_json::from_str::<Value>(&body).ok())
                .flatten()
                .filter(|saved| saved.get("input").is_some());
            let title = if kind == ItemKind::Tool {
                claude_tool_title(
                    &string(value, "title"),
                    saved_tool.as_ref().map(|saved| &saved["input"]),
                )
            } else {
                string(value, "title")
            };
            let tool_audit_json = saved_tool.as_ref().and_then(|saved| {
                tool_audit(
                    &json!({
                        "callId": string(value, "id"),
                        "name": string(value, "title"),
                        "args": saved["input"],
                        "result": saved,
                        "permission": value["permission"],
                        "denied": value["denied"],
                        "denialReason": value["denialReason"],
                    }),
                    status,
                )
            });
            crate::backend::SessionHistoryItem {
                turn_id: string(value, "turnId"),
                // Claude's own saved transcript: what Claude produced is Claude's, as it was live.
                // The model per turn is not recorded there, so it stays unknown.
                provider_id: (kind != ItemKind::User).then(|| CLAUDE_PROVIDER.to_owned()),
                model_id: None,
                attachments: Vec::new(),
                item: NormalizedItem {
                    id: string(value, "id"),
                    kind,
                    title,
                    body,
                    status,
                    tool_audit_json,
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
    let name = string(message, "name");
    // A delegation result is the attributed `[Subagent Result]` text, exactly as other providers
    // report it, so the transcript shows the child run card rather than a second copy.
    let delegation_result = if name == CLAUDE_DELEGATE_TOOL {
        message
            .pointer("/result/output/0/text")
            .and_then(Value::as_str)
    } else {
        None
    };
    let body = match delegation_result {
        Some(text) => text.to_owned(),
        None => message
            .get("result")
            .or_else(|| message.get("args"))
            .map_or_else(String::new, display_value),
    };
    let arguments = message
        .get("args")
        .or_else(|| message.pointer("/result/input"));
    let item = NormalizedItem {
        id: string(message, "callId"),
        kind: ItemKind::Tool,
        title: claude_tool_title(&name, arguments),
        body,
        status,
        tool_audit_json: tool_audit(message, status),
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

/// Claude's provider identity for Nakode's canonical `nakode_agent` tool.
const CLAUDE_DELEGATE_TOOL: &str = "mcp__nakode__delegate";

/// A Claude delegation row carries the canonical `nakode_agent · <agent>` title, so clients place
/// the attributed child run card on it by call id, as they do for every other provider.
fn claude_tool_title(name: &str, arguments: Option<&Value>) -> String {
    if name != CLAUDE_DELEGATE_TOOL {
        return name.to_owned();
    }
    let agent = arguments
        .and_then(|arguments| arguments.get("agent"))
        .and_then(Value::as_str)
        .unwrap_or("agent");
    format!("{} · {agent}", crate::tools::NAKODE_AGENT_TOOL_NAME)
}

/// The audit a Claude tool call carries, live or rebuilt from saved history, so transcripts
/// present both the same way.
fn tool_audit(message: &Value, status: ItemStatus) -> Option<Box<str>> {
    let name = string(message, "name");
    let arguments = message
        .get("args")
        .or_else(|| message.pointer("/result/input"))
        .cloned()
        .unwrap_or(Value::Null);
    let output = message.pointer("/result/output").cloned();
    serde_json::to_string(&json!({
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
        "permission": message.get("permission"),
        "error": message.get("denialReason").and_then(Value::as_str)
            .map(|reason| bounded_claude_audit_value(&Value::String(reason.to_owned()))),
    }))
    .ok()
    .map(String::into_boxed_str)
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
        BackendCommand::BeginAuthentication { .. }
            | BackendCommand::SubmitAuthenticationCallback { .. }
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
        BackendCommand::BeginAuthentication { .. } => BackendOperation::Authenticate,
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

    #[tokio::test]
    async fn partial_sdk_install_is_not_current() {
        let directory = tempfile::tempdir().unwrap();
        let sdk = directory
            .path()
            .join("node_modules/@anthropic-ai/claude-agent-sdk");
        tokio::fs::create_dir_all(&sdk).await.unwrap();
        tokio::fs::write(
            sdk.join("package.json"),
            json!({"version": SDK_VERSION}).to_string(),
        )
        .await
        .unwrap();
        tokio::fs::write(sdk.join("sdk.mjs"), "").await.unwrap();
        assert!(!claude_sdk_is_current(directory.path()).await);
        tokio::fs::write(directory.path().join(".nakode-sdk-ready"), SDK_VERSION)
            .await
            .unwrap();
        assert!(claude_sdk_is_current(directory.path()).await);
        invalidate_sdk_receipt(directory.path()).await.unwrap();
        invalidate_sdk_receipt(directory.path()).await.unwrap();
        assert!(!claude_sdk_is_current(directory.path()).await);
        tokio::fs::write(directory.path().join(".nakode-sdk-ready"), SDK_VERSION)
            .await
            .unwrap();
        tokio::fs::remove_file(sdk.join("sdk.mjs")).await.unwrap();
        assert!(!claude_sdk_is_current(directory.path()).await);
    }

    #[tokio::test]
    async fn authenticated_spawn_does_not_wait_for_sdk_installation() {
        let workspace = tempfile::tempdir().unwrap();
        let config =
            BackendConfig::native(workspace.path().to_path_buf()).with_credential(Some(json!({
                "access_token": "test-brokered-token",
                "refresh_token": "",
                "expires_at_ms": u64::MAX,
                "brokered": true
            })));
        let mut handle = timeout(Duration::from_secs(2), spawn(config))
            .await
            .expect("provider registration must not run the SDK installer")
            .unwrap();
        assert!(matches!(
            handle.events.recv().await,
            Some(BackendEvent::Ready(_))
        ));
        handle
            .commands
            .send(BackendCommand::Shutdown)
            .await
            .unwrap();
        timeout(Duration::from_secs(2), handle.join())
            .await
            .unwrap()
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sdk_install_timeout_kills_the_installer() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("installer.pid");
        let mut command = Command::new("sh");
        command
            .args(["-c", "echo $$ > \"$1\"; exec sleep 60", "installer"])
            .arg(&pid_file);
        let error = sdk_install_output(command, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        let pid: i32 = tokio::fs::read_to_string(pid_file)
            .await
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                    == Err(nix::errno::Errno::ESRCH)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed-out installer must be terminated and reaped");
    }

    #[test]
    fn claude_sdk_image_transport_isolated() {
        let directory = tempfile::tempdir().expect("isolated image transport directory");
        std::fs::write(directory.path().join("bridge.mjs"), BRIDGE_SOURCE).expect("bridge fixture");
        let test = directory.path().join("image_transport.test.mjs");
        std::fs::write(&test, include_str!("image_transport.test.mjs")).expect("transport test");
        let output = std::process::Command::new("node")
            .envs(crate::machine_path::environment())
            .arg("--test")
            .arg(test)
            .output()
            .expect("Node transport test");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn claude_tool_policy_isolated() {
        let directory = tempfile::tempdir().expect("isolated tool policy directory");
        std::fs::write(directory.path().join("tool_policy.mjs"), TOOL_POLICY_SOURCE)
            .expect("tool policy");
        let test = directory.path().join("tool_policy.test.mjs");
        std::fs::write(&test, include_str!("tool_policy.test.mjs")).expect("tool policy test");
        let output = std::process::Command::new("node")
            .envs(crate::machine_path::environment())
            .arg("--test")
            .arg(test)
            .output()
            .expect("Node tool policy test");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn claude_permission_provenance_isolated() {
        let directory = tempfile::tempdir().expect("isolated permission directory");
        std::fs::write(directory.path().join("bridge.mjs"), BRIDGE_SOURCE).expect("bridge");
        std::fs::write(directory.path().join("tool_policy.mjs"), TOOL_POLICY_SOURCE)
            .expect("tool policy");
        let test = directory.path().join("tool_provenance.test.mjs");
        std::fs::write(&test, include_str!("tool_provenance.test.mjs")).expect("provenance test");
        let output = std::process::Command::new("node")
            .envs(crate::machine_path::environment())
            .arg("--test")
            .arg(test)
            .output()
            .expect("Node provenance test");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Installs the production bridge beside a stub Agent SDK that calls `delegate` like Claude Code.
    fn stub_sdk_bridge_directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("isolated bridge directory");
        let root = directory.path();
        std::fs::write(root.join("bridge.mjs"), BRIDGE_SOURCE).expect("bridge");
        std::fs::write(root.join("process_lifecycle.mjs"), PROCESS_LIFECYCLE_SOURCE)
            .expect("process lifecycle");
        std::fs::write(root.join("tool_policy.mjs"), TOOL_POLICY_SOURCE).expect("tool policy");
        let sdk = root.join("node_modules/@anthropic-ai/claude-agent-sdk");
        std::fs::create_dir_all(&sdk).expect("sdk directory");
        std::fs::write(
            sdk.join("package.json"),
            r#"{"name":"@anthropic-ai/claude-agent-sdk","type":"module","exports":"./index.mjs"}"#,
        )
        .expect("sdk manifest");
        std::fs::write(
            sdk.join("index.mjs"),
            include_str!("delegation_sdk_stub.mjs"),
        )
        .expect("sdk stub");
        // Schemas are not exercised here; every zod builder call returns the same chainable value.
        let zod = root.join("node_modules/zod");
        std::fs::create_dir_all(&zod).expect("zod directory");
        std::fs::write(
            zod.join("package.json"),
            r#"{"name":"zod","type":"module","exports":{"./v4":"./v4.mjs"}}"#,
        )
        .expect("zod manifest");
        std::fs::write(
            zod.join("v4.mjs"),
            "const chain = new Proxy(function () {}, { get: () => chain, apply: () => chain });\nexport const z = chain;\n",
        )
        .expect("zod stub");
        directory
    }

    async fn next_matching<T>(
        events: &mut mpsc::Receiver<BackendEvent>,
        mut matcher: impl FnMut(BackendEvent) -> Option<T>,
    ) -> T {
        timeout(Duration::from_secs(20), async {
            loop {
                let event = events.recv().await.expect("Claude backend event");
                if let Some(found) = matcher(event) {
                    return found;
                }
            }
        })
        .await
        .expect("expected Claude backend event")
    }

    /// The real bridge and supervisor route Claude's `delegate` call to the workspace server with
    /// server-bound identity, without any Nakode executable (which an embedding host such as `FStack`
    /// cannot provide as `nakode agent`), and return the attributed result to the same tool call.
    /// Claude Code's native subagent routes are refused first, so they cannot stand in for it.
    #[tokio::test]
    async fn claude_delegate_tool_routes_through_the_server_without_a_nakode_executable() {
        delegate_through_stub_sdk("bypassPermissions").await;
    }

    /// Prompting modes decide ordinary tools through `canUseTool`, which settings `allow` rules can
    /// skip; native delegation is still refused before it, and Nakode delegation still works.
    #[tokio::test]
    async fn claude_native_subagents_are_refused_in_prompting_permission_modes() {
        delegate_through_stub_sdk("default").await;
    }

    fn denial_reason(item: &NormalizedItem) -> Option<String> {
        let audit: Value = serde_json::from_str(item.tool_audit_json.as_deref()?).ok()?;
        if audit["denied"] != true {
            return None;
        }
        audit["denialReason"].as_str().map(str::to_owned)
    }

    #[allow(clippy::too_many_lines)]
    async fn delegate_through_stub_sdk(permission_mode: &str) {
        let directory = stub_sdk_bridge_directory();
        let workspace = tempfile::tempdir().expect("workspace");
        let mut command = bridge_command(directory.path());
        command.env("NAKODE_CLAUDE_PERMISSION_MODE", permission_mode);
        let bridge = start_bridge_process(command).expect("bridge process");
        let (route, mut requests) = mpsc::channel(4);
        let config =
            BackendConfig::native(workspace.path().to_path_buf()).with_native_delegation(route);
        let credential = ClaudeOAuthCredential {
            access_token: "isolated-test-token".to_owned(),
            refresh_token: String::new(),
            expires_at_ms: now_ms() + 24 * 60 * 60 * 1_000,
            authorized_at_ms: None,
            account_id: None,
            email: None,
            organization_id: None,
            organization_name: None,
            brokered: true,
        };
        let (commands, command_rx) = mpsc::channel(8);
        let (event_tx, mut events) = mpsc::channel(64);
        let supervisor = tokio::spawn(run_supervisor(
            false,
            config,
            Some(credential),
            false,
            Some(bridge),
            command_rx,
            event_tx,
        ));

        commands
            .send(BackendCommand::StartSession {
                model: Some("opus".to_owned()),
                instructions: None,
                owner_session_id: Some("owner-session".to_owned()),
                parent_run_id: Some("run-parent".to_owned()),
                enabled_skill_ids: Vec::new(),
                external_tools: Vec::new(),
                replace_builtin_tools: false,
                code_mode: false,
                allowed_builtin_tools: None,
                max_turns: None,
                finalization_reserve_turns: 0,
                timeout_seconds: None,
            })
            .await
            .unwrap();
        let provider_session_id = next_matching(&mut events, |event| match event {
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            } => Some(provider_session_id),
            _ => None,
        })
        .await;
        commands
            .send(BackendCommand::StartTurn {
                provider_session_id,
                client_id: "turn-1".to_owned(),
                prompt: "Delegate the proof read.".to_owned(),
                attachments: Vec::new(),
                model: None,
                skill_catalogue: crate::skill::SkillCatalog::default(),
            })
            .await
            .unwrap();

        let Ok(request) = timeout(Duration::from_secs(20), requests.recv()).await else {
            let mut seen = Vec::new();
            while let Ok(event) = events.try_recv() {
                seen.push(format!("{event:?}"));
            }
            panic!("no delegation reached the server route; events: {seen:#?}");
        };
        let request = request.expect("delegation request");
        let crate::backend::NativeAgentRequest::Delegate(request) = request else {
            panic!("expected a delegation request");
        };
        assert_eq!(request.owner_session_id, "owner-session");
        assert_eq!(request.parent_run_id.as_deref(), Some("run-parent"));
        assert_eq!(request.invocation_turn_id, "turn-1");
        assert_eq!(request.invocation_call_id, "toolu_stub_delegate");
        assert_eq!(request.agent, "repo-explorer");
        assert_eq!(request.title, "Read delegation proof marker");
        assert_eq!(request.task, "Read README.md and report the marker.");
        request
            .respond
            .send(Ok(
                "[Subagent Result] [agent-7] [repo-explorer]\nNAKODE-CLAUDE-DELEGATION-7429"
                    .to_owned(),
            ))
            .unwrap();

        let mut delegation_row = None;
        let mut native_denials = std::collections::BTreeMap::new();
        let outcome = next_matching(&mut events, |event| match event {
            BackendEvent::ItemCompleted { item, .. } if item.id == "toolu_stub_delegate" => {
                delegation_row = Some(item);
                None
            }
            BackendEvent::ItemCompleted { item, .. } => {
                if let Some(reason) = denial_reason(&item) {
                    native_denials.insert(item.id, reason);
                }
                None
            }
            BackendEvent::TurnCompleted { outcome, error, .. } => Some((outcome, error)),
            _ => None,
        })
        .await;
        assert_eq!(outcome, (TurnOutcome::Completed, None));
        assert_eq!(
            native_denials.len(),
            4,
            "native denials: {native_denials:#?}"
        );
        assert!(
            native_denials["toolu_native_agent"]
                .contains("native Agent tool is disabled in Nakode"),
            "{native_denials:#?}"
        );
        assert!(
            native_denials["toolu_native_task"].contains("native Task tool is disabled in Nakode"),
            "{native_denials:#?}"
        );
        assert!(
            native_denials["toolu_future_native"]
                .contains("SpawnAgentTeam is outside Nakode's Claude tool boundary"),
            "{native_denials:#?}"
        );
        assert!(
            native_denials["toolu_native_subagent_read"]
                .contains("native subagents cannot use tools in Nakode"),
            "{native_denials:#?}"
        );
        let row = delegation_row.expect("completed delegation row");
        assert_eq!(row.status, ItemStatus::Complete);
        assert_eq!(row.title, "nakode_agent · repo-explorer");
        assert_eq!(
            row.body,
            "[Subagent Result] [agent-7] [repo-explorer]\nNAKODE-CLAUDE-DELEGATION-7429"
        );

        commands.send(BackendCommand::Shutdown).await.unwrap();
        timeout(Duration::from_secs(10), supervisor)
            .await
            .expect("supervisor stops")
            .expect("supervisor task");
    }

    #[test]
    fn image_handoff_reaches_claude_bridge_as_base64_content() {
        let command = |attachments| BackendCommand::StartTurn {
            provider_session_id: "session".to_owned(),
            client_id: "turn".to_owned(),
            prompt: "Inspect the selected image".to_owned(),
            attachments,
            model: None,
            skill_catalogue: crate::skill::SkillCatalog::default(),
        };
        let result = bridge_request(command(vec![crate::backend::PromptAttachment {
            label: "fixture.png".to_owned(),
            path: None,
            image: Some(crate::backend::PromptImage {
                mime_type: "image/png".to_owned(),
                data: crate::image_handoff::tests::png(16, 8),
            }),
        }]));
        let image = result
            .expect("image transport supported")
            .expect("send request");
        assert_eq!(image.payload["images"][0]["type"], "image");
        assert_eq!(
            image.payload["images"][0]["source"]["media_type"],
            "image/png"
        );
        assert_eq!(
            image.payload["images"][0]["source"]["data"],
            base64::engine::general_purpose::STANDARD
                .encode(crate::image_handoff::tests::png(16, 8)),
        );
        let text = bridge_request(command(Vec::new()))
            .expect("text-only remains supported")
            .expect("send request");
        assert_eq!(text.method, "send");
        assert_eq!(text.payload["prompt"], "Inspect the selected image");
    }

    #[test]
    fn restored_tool_calls_carry_the_audit_a_live_call_does() {
        let history = session_history(&json!({"history": [
            {"turnId":"t","id":"call-1","kind":"tool","title":"Bash","status":"complete",
             "body":"{\"input\":{\"command\":\"git status\"},\"output\":\"clean\"}"},
            {"turnId":"t","id":"text","kind":"assistant","title":"CLAUDE","status":"complete","body":"{\"input\":1}"}
        ]}));
        let audit: Value = serde_json::from_str(
            history[0]
                .item
                .tool_audit_json
                .as_deref()
                .expect("restored tool audit"),
        )
        .expect("audit json");
        assert_eq!(audit["name"], "Bash");
        assert_eq!(audit["callId"], "call-1");
        assert_eq!(audit["status"], "completed");
        assert!(audit["input"].to_string().contains("git status"));
        assert!(audit["output"].to_string().contains("clean"));
        assert!(history[1].item.tool_audit_json.is_none());
        assert!(
            history
                .iter()
                .all(|item| item.provider_id.as_deref() == Some(CLAUDE_PROVIDER))
        );
        let owner = session_history(&json!({"history": [
            {"turnId":"t","id":"u","kind":"user","title":"YOU","status":"complete","body":"hi"}
        ]}));
        assert_eq!(owner[0].provider_id, None);
    }

    #[test]
    fn turns_carry_the_owning_session_environment_to_claude_code() {
        let resume = BackendCommand::ResumeSession {
            provider_session_id: "claude-session".to_owned(),
            owner_session_id: Some("owner".to_owned()),
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            code_mode: false,
            allowed_builtin_tools: None,
            max_turns: None,
            timeout_seconds: None,
        };
        assert_eq!(session_owner(Some(&resume)), Some("owner"));
        assert_eq!(session_owner(None), None);
        assert!(BRIDGE_SOURCE.contains("...process.env,\n      ...(command.environment || {}),"));
    }

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
            .envs(crate::machine_path::environment())
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
        // Delegation identity is bound by the supervisor, never carried or parsed by the bridge.
        assert!(!BRIDGE_SOURCE.contains("attributedRunId"));
        assert!(!BRIDGE_SOURCE.contains("node:child_process"));
        assert!(!BRIDGE_SOURCE.contains("NAKODE_EXECUTABLE"));
        assert!(BRIDGE_SOURCE.contains("CLAUDE_POLICY_TOOLS.has(name)"));
        assert!(BRIDGE_SOURCE.contains("permission.decision === \"deny\""));
        assert!(BRIDGE_SOURCE.contains("denialReason: permission.reason"));
        assert!(BRIDGE_SOURCE.contains("    tools: builtinTools,"));
        assert!(BRIDGE_SOURCE.contains("? [...UNRESTRICTED_BUILTIN_TOOLS]"));
        assert!(!BRIDGE_SOURCE.contains("tools: allowedTools"));
        assert!(
            BRIDGE_SOURCE.contains("disallowedTools: disallowedProviderTools(session.deniedTools)")
        );
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
            "denialReason": "Archetype policy does not allow Bash.",
            "permission": {"decision": "deny", "source": "policy", "reason": "Archetype policy does not allow Bash.", "mode": "auto"}
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
        assert_eq!(audit["permission"]["source"], "policy");
        assert!(
            audit["error"]["value"]
                .as_str()
                .unwrap()
                .contains("Archetype policy")
        );
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

    #[tokio::test]
    async fn claude_snapshot_restoration_preserves_host_permission_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("history.sqlite");
        rusqlite::Connection::open(&database).unwrap().execute_batch(
            "CREATE TABLE provider_history_snapshots (provider TEXT, session_id TEXT, history_json TEXT, updated_at INTEGER, PRIMARY KEY(provider, session_id));"
        ).unwrap();
        let permission = json!({"decision": "deny", "source": "validator_error", "reason": "Validator unavailable"});
        let row = json!({"id": "call", "kind": "tool", "title": "Bash", "status": "failed", "body": r#"{"input":{"command":"git status"},"output":"denied"}"#});
        let mut original = row.clone();
        original["permission"] = permission.clone();
        original["denied"] = json!(true);
        original["denialReason"] = json!("Validator unavailable");
        let mut first = json!({"sessionId": "session", "history": [original]});
        keep_history_snapshot(database.clone(), &mut first).await;
        let mut resumed = json!({"sessionId": "session", "history": [row.clone()]});
        keep_history_snapshot(database.clone(), &mut resumed).await;
        assert_eq!(resumed["history"][0]["permission"], permission);
        let restored = retained_history(&database, "session").unwrap().unwrap();
        let audit: Value =
            serde_json::from_str(restored[0].item.tool_audit_json.as_deref().unwrap()).unwrap();
        assert_eq!(audit["permission"], permission);
        assert_eq!(audit["denied"], true);
        let mut other = json!({"sessionId": "other-session", "history": [row]});
        keep_history_snapshot(database, &mut other).await;
        assert!(other["history"][0]["permission"].is_null());
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

    #[tokio::test]
    async fn claude_external_tool_images_are_retained_once_per_live_call() {
        let (events, mut received) = mpsc::channel(4);
        let mut returned = ReturnedImages::default();
        let image = crate::backend::PromptImage {
            mime_type: "image/png".to_owned(),
            data: b"\x89PNG\r\n\x1a\n".to_vec(),
        };
        let track = |returned: &mut ReturnedImages, id: &str, turn: &str| {
            returned
                .external_calls
                .insert(id.to_owned(), (turn.to_owned(), "model".to_owned()));
        };

        track(&mut returned, "live", "turn-1");
        let images = vec![image.clone()];
        let note = retain_external_tool_images("live", &images, &mut returned, &events).await;
        assert!(note.is_none());
        let Some(BackendEvent::ImageReturned(retained)) = received.recv().await else {
            panic!("expected a retained image");
        };
        assert_eq!(retained.id, "turn-1:image:live");
        assert!(returned.external_calls.is_empty());

        track(&mut returned, "interrupted", "turn-2");
        returned.forget_turn_calls("turn-2");
        assert!(returned.external_calls.is_empty());
        let images = vec![image];
        let note =
            retain_external_tool_images("interrupted", &images, &mut returned, &events).await;
        assert!(note.is_none());
        assert!(received.try_recv().is_err());
    }

    #[tokio::test]
    async fn claude_external_tool_images_past_the_turn_bound_keep_what_fits() {
        let (events, mut received) = mpsc::channel(8);
        let mut returned = ReturnedImages::default();
        let image = crate::backend::PromptImage {
            mime_type: "image/jpeg".to_owned(),
            data: b"\xff\xd8\xff".to_vec(),
        };
        // Four screenshots and a failure screenshot, then four more in the same turn.
        let mut kept = Vec::new();
        let mut notes = Vec::new();
        for (call, count) in [("first", 5), ("second", 4), ("third", 1)] {
            returned
                .external_calls
                .insert(call.to_owned(), ("turn".to_owned(), "model".to_owned()));
            let images = vec![image.clone(); count];
            notes.push(retain_external_tool_images(call, &images, &mut returned, &events).await);
            kept.push(match received.try_recv() {
                Ok(BackendEvent::ImageReturned(retained)) => 1 + retained.more_attachments.len(),
                _ => 0,
            });
        }
        assert_eq!(kept, [5, 3, 0]);
        assert!(notes[0].is_none());
        let partial = notes[1].as_deref().expect("a partial note");
        assert!(
            partial.starts_with("Kept 3 of this result's 4 images"),
            "{partial}"
        );
        assert!(
            notes[2]
                .as_deref()
                .expect("a note")
                .starts_with("Kept 0 of this result's 1 images")
        );
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
            images: vec![crate::backend::PromptImage {
                mime_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            }],
        })
        .expect("Claude supports external tool results")
        .expect("bridge request");
        assert_eq!(resolve.method, "resolve_external_tool");
        assert_eq!(resolve.payload["id"], "external-1");
        assert_eq!(
            resolve.payload["images"],
            json!([{"data": "AQID", "mimeType": "image/png"}])
        );
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
    fn claude_returns_images_through_nakode_not_the_provider() {
        assert!(BRIDGE_SOURCE.contains("\"return_image\""));
        assert!(BRIDGE_SOURCE.contains("event: \"image_return_request\""));
        assert!(BRIDGE_SOURCE.contains("case \"resolve_image_return\""));
        assert!(BRIDGE_SOURCE.contains("\"mcp__nakode__return_image\""));
    }

    #[test]
    fn claude_bypasses_permissions_by_default_and_keeps_an_attributed_validator() {
        assert!(BRIDGE_SOURCE.contains("NAKODE_CLAUDE_PERMISSION_MODE"));
        assert!(BRIDGE_SOURCE.contains(": \"bypassPermissions\";"));
        assert!(BRIDGE_SOURCE.contains("allowDangerouslySkipPermissions: true"));
        assert!(!BRIDGE_SOURCE.contains("filterEscalatingDefaultMode"));
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
            .envs(crate::machine_path::environment())
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
    fn claude_image_capability_uses_sdk_identity_not_display_label() {
        let BackendEvent::Models(models) = models_event(&json!({"models": [
            {"id":"claude-opus-5-5"},
            {"id":"opus"},
            {"id":"opus[1m]", "resolvedModel":"claude-opus-5-5[1m]"},
            {"id":"default", "resolvedModel":"claude-opus-5-5[1m]"},
            {"id":"unknown", "displayName":"Opus 5.5"},
            {"id":"opus", "resolvedModel":"text-only"},
            {"id":"claude-opus-5-50"}
        ]})) else {
            panic!("models event");
        };
        for (index, model) in models.iter().enumerate() {
            assert_eq!(
                model.capabilities.accepts_image_input,
                index < 4,
                "{}",
                model.id
            );
            assert_eq!(
                crate::state::projection::model_configuration(model, false).accepts_image_input,
                index < 4
            );
        }
        let legacy: crate::backend::ModelCapabilities =
            serde_json::from_str("{\"reasoning_efforts\":[]}").expect("legacy cache");
        assert!(!legacy.accepts_image_input);
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
        assert_eq!(models[0].display_name, None);
    }

    #[test]
    fn claude_models_read_as_their_versioned_names() {
        let event = models_event(&json!({
            "models": [
                {"id": "default", "isDefault": true, "displayName": "Default (recommended)",
                 "description": "Use the default model (currently Opus 5 (1M context)) · $5/$25 per Mtok"},
                {"id": "opus[1m]", "displayName": "Opus (1M context)",
                 "description": "Opus 5 with 1M context · Best for everyday, complex tasks · $5/$25 per Mtok"},
                {"id": "claude-fable-5-1[1m]", "displayName": "Fable",
                 "description": "Fable 5.1 · Most capable for your hardest and longest-running tasks"},
                {"id": "sonnet", "displayName": "Sonnet", "description": "Sonnet 5 · Efficient for routine tasks"},
                {"id": "haiku", "displayName": "Haiku", "description": "Haiku 4.5 · Fastest for quick answers"},
                {"id": "mystery", "displayName": "", "description": "Whatever · text"}
            ]
        }));
        let BackendEvent::Models(models) = event else {
            panic!("expected models event");
        };
        let names: Vec<String> = models.iter().map(ModelInfo::display_name).collect();
        assert_eq!(
            names,
            [
                "Default (recommended)",
                "Opus 5",
                "Fable 5.1",
                "Sonnet 5",
                "Haiku 4.5",
                "Mystery"
            ]
        );
        assert_eq!(without_context_window("Opus (1M context)"), "Opus");
        let BackendEvent::Models(both) = models_event(&json!({
            "models": [
                {"id": "opus", "displayName": "Opus", "description": "Opus 5.5 · Best for everyday tasks"},
                {"id": "opus[1m]", "displayName": "Opus (1M context)",
                 "description": "Opus 5.5 with 1M context · Best for everyday tasks"},
                {"id": "claude-fable-5-1[1m]", "displayName": "Fable",
                 "description": "Fable 5.1 · Most capable"}
            ]
        })) else {
            panic!("expected models event");
        };
        // Only a name that would otherwise repeat says its window.
        assert_eq!(
            both.iter().map(ModelInfo::display_name).collect::<Vec<_>>(),
            ["Opus 5.5", "Opus 5.5 (1M context)", "Fable 5.1"]
        );
        assert_eq!(
            without_context_window("Sonnet 5 with 200k context"),
            "Sonnet 5"
        );
        assert_eq!(
            without_context_window("Context Engine 2"),
            "Context Engine 2"
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
            .envs(crate::machine_path::environment())
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
            .envs(crate::machine_path::environment())
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
            &BackendCommand::BeginAuthentication {
                client_context: crate::backend::ClientContext::Unspecified,
            }
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
            .send(BackendCommand::BeginAuthentication {
                client_context: crate::backend::ClientContext::Unspecified,
            })
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
            brokered: false,
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
            brokered: false,
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
            brokered: false,
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
        std::fs::write(sdk.join("sdk.mjs"), "").expect("SDK entrypoint");
        std::fs::write(directory.path().join(".nakode-sdk-ready"), SDK_VERSION)
            .expect("successful SDK installation receipt");
        assert!(claude_sdk_is_current(directory.path()).await);

        std::fs::write(sdk.join("package.json"), r#"{"version":"0.0.0"}"#)
            .expect("stale SDK manifest");
        assert!(!claude_sdk_is_current(directory.path()).await);
    }

    fn brokered(expires_at_ms: u64) -> ClaudeOAuthCredential {
        ClaudeOAuthCredential {
            access_token: "brokered-access".to_owned(),
            refresh_token: String::new(),
            expires_at_ms,
            authorized_at_ms: None,
            account_id: Some("account".to_owned()),
            email: None,
            organization_id: None,
            organization_name: None,
            brokered: true,
        }
    }

    #[test]
    fn brokered_credentials_carry_an_access_token_and_no_refresh_token() {
        let metadata = brokered_credential_metadata(
            r#"{"access_token":"access","expires_at_ms":42,"email":"owner@example.com"}"#,
        )
        .expect("brokered credential");
        assert_eq!(metadata["brokered"], true);
        assert_eq!(metadata["refresh_token"], "");
        let parsed = parse_credential(Some(&metadata))
            .expect("parses")
            .expect("present");
        assert!(parsed.brokered);
        assert_eq!(parsed.email.as_deref(), Some("owner@example.com"));
        assert!(brokered_credential_metadata(r#"{"access_token":"","expires_at_ms":1}"#).is_err());
        assert!(
            brokered_credential_metadata(
                r#"{"access_token":"a","expires_at_ms":1,"refresh_token":"r"}"#
            )
            .is_err(),
            "a broker never hands out its refresh token"
        );
        assert!(
            parse_credential(Some(&json!({
                "access_token": "a",
                "refresh_token": "",
                "expires_at_ms": 1
            })))
            .is_err(),
            "an ordinary credential still needs its refresh token"
        );
    }

    #[tokio::test]
    async fn brokered_credentials_are_never_refreshed() {
        // Nothing listens here: reaching the network would fail the test differently.
        let unreachable = "http://127.0.0.1:9/oauth/token";
        let (kept, refreshed) =
            refresh_if_needed_with_url(Some(brokered(now_ms() + 60_000)), unreachable)
                .await
                .expect("current brokered token");
        assert!(!refreshed);
        assert_eq!(
            kept.map(|credential| credential.access_token).as_deref(),
            Some("brokered-access")
        );
        let error = refresh_if_needed_with_url(Some(brokered(0)), unreachable)
            .await
            .expect_err("expired brokered token");
        assert!(error.starts_with(CREDENTIAL_REFRESH_REQUIRED), "{error}");
    }

    #[tokio::test]
    async fn a_replacement_credential_reaches_a_running_supervisor() {
        let (commands, command_rx) = mpsc::channel(4);
        let (events, mut received) = mpsc::channel(16);
        let supervisor = tokio::spawn(run_supervisor(
            false,
            BackendConfig::native(std::env::temp_dir()),
            None,
            false,
            None,
            command_rx,
            events,
        ));
        let expired = serde_json::to_value(brokered(0)).expect("credential");
        commands
            .send(BackendCommand::UpdateCredential {
                credential: Some(SecretValue::new(expired)),
            })
            .await
            .expect("update");
        commands
            .send(BackendCommand::Reload {
                provider_session_id: None,
            })
            .await
            .expect("reload");
        let failure = loop {
            if let BackendEvent::RequestFailed { message, .. } =
                received.recv().await.expect("event")
            {
                break message;
            }
        };
        assert!(
            failure.starts_with(CREDENTIAL_REFRESH_REQUIRED),
            "{failure}"
        );
        commands
            .send(BackendCommand::Shutdown)
            .await
            .expect("shutdown");
        supervisor.await.expect("supervisor exits");
    }
}
