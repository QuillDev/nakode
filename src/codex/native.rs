use std::{
    collections::HashMap,
    error::Error as _,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    backend::{
        BackendCapabilities, BackendCommand, BackendError, BackendEvent,
        BackendFailureClassification, BackendFailureDetail, BackendFailurePhase, BackendHandle,
        BackendIdentity, BackendOperation, CODEX_PROVIDER, CapabilitySupport, ModelInfo,
        ModelOptions, ProviderFailureClassification, TurnOutcome, bounded_failure_text,
        request_failed, request_failed_with_detail, sanitize_failure_endpoint,
        sanitize_failure_text as sanitize_backend_failure_text,
    },
    runtime::{
        AgentRuntime, ConversationItem, DEFAULT_COMPACTION_THRESHOLD_PERCENT, InferenceEvent,
        InferenceFailure, InferenceFuture, InferenceOutput, InferenceProvider, InferenceRequest,
        RuntimeSession, RuntimeSessionStore, ToolCall, TurnError, set_session_model,
    },
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api";
const CODEX_CLIENT_VERSION: &str = "0.144.6";
const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const AUTHORIZE_URL: &str = "https://auth.openai.com/oauth/authorize";
const DEVICE_USER_CODE_URL: &str = "https://auth.openai.com/api/accounts/deviceauth/usercode";
const DEVICE_TOKEN_URL: &str = "https://auth.openai.com/api/accounts/deviceauth/token";
const DEVICE_AUTH_URL: &str = "https://auth.openai.com/codex/device";
const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";
const BROWSER_CALLBACK_PATH: &str = "/auth/callback";
const BROWSER_CALLBACK_PORTS: [u16; 2] = [1455, 1457];
const BROWSER_AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const OAUTH_SCOPES: &str =
    "openid profile email offline_access api.connectors.read api.connectors.invoke";
const MAX_DEVICE_POLLS: usize = 120;
const MAX_INFERENCE_ATTEMPTS: usize = 4;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(8);

#[derive(Clone)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
    pub base_url: String,
    client: Client,
    auth_urls: AuthUrls,
    auth_flow: CodexAuthFlow,
    callback_ports: Vec<u16>,
    session_database: Option<PathBuf>,
    compaction_threshold_percent: usize,
    reasoning_effort: Option<String>,
    web_config: Option<Arc<std::sync::RwLock<crate::web::WebConfig>>>,
    vision_config: Option<Arc<std::sync::RwLock<crate::vision::VisionConfig>>>,
    vision_service: Option<crate::vision::SharedVisionService>,
    memory_service: Option<crate::memory::SharedMemoryService>,
    native_delegation: Option<mpsc::Sender<crate::backend::NativeAgentRequest>>,
}

#[derive(Clone, Debug)]
struct AuthUrls {
    authorize: String,
    user_code: String,
    device_token: String,
    verification: String,
    token: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum CodexAuthFlow {
    #[default]
    Browser,
    DeviceCode,
}

impl BackendConfig {
    #[must_use]
    pub fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
            base_url: CODEX_BASE_URL.to_owned(),
            client: Client::new(),
            auth_urls: AuthUrls {
                authorize: AUTHORIZE_URL.to_owned(),
                user_code: DEVICE_USER_CODE_URL.to_owned(),
                device_token: DEVICE_TOKEN_URL.to_owned(),
                verification: DEVICE_AUTH_URL.to_owned(),
                token: TOKEN_URL.to_owned(),
            },
            auth_flow: CodexAuthFlow::default(),
            callback_ports: BROWSER_CALLBACK_PORTS.to_vec(),
            session_database: None,
            compaction_threshold_percent: DEFAULT_COMPACTION_THRESHOLD_PERCENT,
            reasoning_effort: Some("medium".to_owned()),
            web_config: None,
            vision_config: None,
            vision_service: None,
            memory_service: None,
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
    pub fn with_memory(mut self, service: crate::memory::SharedMemoryService) -> Self {
        self.memory_service = Some(service);
        self
    }

    #[must_use]
    pub fn with_compaction_threshold_percent(mut self, threshold_percent: usize) -> Self {
        self.compaction_threshold_percent = threshold_percent;
        self
    }

    #[must_use]
    pub fn with_web_config(
        mut self,
        config: Arc<std::sync::RwLock<crate::web::WebConfig>>,
    ) -> Self {
        self.web_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_vision(
        mut self,
        config: Arc<std::sync::RwLock<crate::vision::VisionConfig>>,
        service: Option<crate::vision::SharedVisionService>,
    ) -> Self {
        self.vision_config = Some(config);
        self.vision_service = service;
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
    pub fn with_reasoning_effort(mut self, reasoning_effort: impl Into<String>) -> Self {
        self.reasoning_effort = Some(reasoning_effort.into());
        self
    }

    /// Explicitly opts this adapter into `OpenAI`'s device-code login flow.
    ///
    /// Ordinary Codex authentication uses browser OAuth with a localhost callback.
    #[must_use]
    pub fn with_device_code_authentication(mut self) -> Self {
        self.auth_flow = CodexAuthFlow::DeviceCode;
        self
    }

    #[cfg(test)]
    fn with_auth_urls(mut self, base_url: &str) -> Self {
        self.auth_urls = AuthUrls {
            authorize: format!("{base_url}/oauth/authorize"),
            user_code: format!("{base_url}/device/usercode"),
            device_token: format!("{base_url}/device/token"),
            verification: format!("{base_url}/codex/device"),
            token: format!("{base_url}/oauth/token"),
        };
        self
    }

    #[cfg(test)]
    fn with_callback_port(mut self, port: u16) -> Self {
        self.callback_ports = vec![port];
        self
    }

    #[cfg(test)]
    fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self.client = Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client");
        self
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CodexCredential {
    access_token: String,
    refresh_token: String,
    expires_at_ms: u64,
    account_id: String,
    #[serde(default)]
    email: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexFailureClassification {
    Authentication,
    Quota,
    RateLimit,
    Transient,
    Provider,
    Model,
}

impl CodexFailureClassification {
    fn normalized(self) -> ProviderFailureClassification {
        match self {
            Self::Authentication => ProviderFailureClassification::Authentication,
            Self::Quota => ProviderFailureClassification::Quota,
            Self::RateLimit => ProviderFailureClassification::RateLimit,
            Self::Transient => ProviderFailureClassification::Transient,
            Self::Provider => ProviderFailureClassification::Provider,
            Self::Model => ProviderFailureClassification::Model,
        }
    }
}

#[derive(Debug)]
struct InferenceAttemptError {
    message: String,
    classification: CodexFailureClassification,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl InferenceAttemptError {
    fn terminal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            classification: CodexFailureClassification::Provider,
            retryable: false,
            retry_after: None,
        }
    }

    fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            classification: CodexFailureClassification::Transient,
            retryable: true,
            retry_after: None,
        }
    }
}

#[derive(Clone)]
struct CodexProvider {
    client: Client,
    base_url: String,
    credential: CodexCredential,
}

#[derive(Clone)]
struct CodexVisionService {
    provider: CodexProvider,
    config: Arc<std::sync::RwLock<crate::vision::VisionConfig>>,
}

impl crate::vision::VisionService for CodexVisionService {
    fn analyze<'a>(
        &'a self,
        prompt: &'a str,
        images: Vec<crate::backend::PromptImage>,
        cancellation: &'a CancellationToken,
    ) -> crate::vision::VisionFuture<'a> {
        Box::pin(async move {
            let model = self
                .config
                .read()
                .map_err(|_| "vision settings lock is unavailable".to_owned())?
                .model_id()
                .map(str::to_owned)
                .ok_or_else(|| "vision add-on has no selected model".to_owned())?;
            let attachments = images
                .into_iter()
                .enumerate()
                .map(|(index, image)| crate::backend::PromptAttachment {
                    label: format!("Image {}", index + 1),
                    path: None,
                    image: Some(image),
                })
                .collect();
            let request = InferenceRequest {
                session_id: Uuid::now_v7().to_string(),
                model,
                instructions: "Analyze images accurately. Return only the requested visual analysis; do not use tools or continue the caller's coding task.".to_owned(),
                history: vec![ConversationItem::User {
                    text: prompt.to_owned(),
                    attachments,
                }],
                tools: Vec::new(),
                reasoning_effort: Some("low".to_owned()),
                fast_mode: false,
            };
            let (events, mut event_rx) = mpsc::channel(32);
            let drain = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });
            let result = self
                .provider
                .infer_response(request, events, cancellation.clone())
                .await
                .map(|output| output.text)
                .map_err(|failure| failure.message);
            let _ = drain.await;
            result
        })
    }
}

/// Creates the shared OpenAI-backed vision service when its credential exists.
///
/// # Errors
/// Returns an error when the stored `OpenAI` credential has an invalid shape.
pub fn vision_service(
    credential: Option<Value>,
    config: Arc<std::sync::RwLock<crate::vision::VisionConfig>>,
) -> Result<Option<crate::vision::SharedVisionService>, BackendError> {
    let credential = credential
        .map(serde_json::from_value::<CodexCredential>)
        .transpose()
        .map_err(|source| BackendError::InvalidCredential {
            provider: CODEX_PROVIDER.to_owned(),
            detail: source.to_string(),
        })?;
    Ok(credential.map(|credential| {
        Arc::new(CodexVisionService {
            provider: CodexProvider {
                client: Client::new(),
                base_url: CODEX_BASE_URL.to_owned(),
                credential,
            },
            config,
        }) as crate::vision::SharedVisionService
    }))
}

#[derive(Clone, Debug)]
struct DiscoveredModel {
    info: ModelInfo,
    context_window: Option<usize>,
}

impl InferenceProvider for CodexProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move { self.infer_response(request, events, cancellation).await })
    }
}

impl CodexProvider {
    async fn infer_response(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> Result<InferenceOutput, InferenceFailure> {
        let body = codex_request_body(&request);
        let url = format!("{}/codex/responses", self.base_url.trim_end_matches('/'));
        for attempt in 0..MAX_INFERENCE_ATTEMPTS {
            if cancellation.is_cancelled() {
                return Err("turn interrupted".into());
            }
            match self
                .infer_attempt(
                    &url,
                    &body,
                    &request.session_id,
                    events.clone(),
                    cancellation.clone(),
                )
                .await
            {
                Ok(mut output) => {
                    output.retry_count = attempt;
                    return Ok(output);
                }
                Err(error) if error.retryable && attempt + 1 < MAX_INFERENCE_ATTEMPTS => {
                    let delay = error
                        .retry_after
                        .unwrap_or_else(|| retry_delay(attempt))
                        .min(MAX_RETRY_DELAY);
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancellation.cancelled() => return Err("turn interrupted".into()),
                    }
                }
                Err(error) => {
                    let classification = error.classification.normalized();
                    return Err(InferenceFailure::new(error.message, attempt)
                        .with_provider_failure(classification, error.retry_after));
                }
            }
        }
        unreachable!("the bounded inference retry loop returns on its final attempt")
    }

    async fn infer_attempt(
        &self,
        url: &str,
        body: &Value,
        session_id: &str,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> Result<InferenceOutput, InferenceAttemptError> {
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.credential.access_token)
            .header("chatgpt-account-id", &self.credential.account_id)
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "nakode")
            .header("version", CODEX_CLIENT_VERSION)
            .header("conversation_id", session_id)
            .header("session_id", session_id)
            .header("x-client-request-id", Uuid::now_v7().to_string())
            .header("accept", "text/event-stream")
            .json(body)
            .send()
            .await
            .map_err(|error| {
                let safe = sanitize_backend_failure_text(&error.to_string(), 256);
                let message = if safe.is_empty() {
                    "Codex request transport failed.".to_owned()
                } else {
                    format!("Codex request transport failed: {safe}")
                };
                if error.is_connect() || error.is_timeout() || error.is_request() {
                    InferenceAttemptError::transient(message)
                } else {
                    InferenceAttemptError::terminal(message)
                }
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let retry_after = retry_after(response.headers());
            let detail = read_bounded_provider_error_body(response).await;
            let classification = codex_failure_classification(status, &detail);
            let message = format!("Codex returned {status}.");
            return Err(InferenceAttemptError {
                message,
                classification,
                retryable: retryable_status(status),
                retry_after,
            });
        }
        parse_codex_sse(response, events, cancellation).await
    }
}

/// Starts the in-process `OpenAI` Codex adapter.
///
/// # Errors
///
/// Returns an error when the stored credential has an invalid shape.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let credential = config
        .credential
        .as_ref()
        .map(|value| serde_json::from_value::<CodexCredential>(value.clone()))
        .transpose()
        .map_err(|source| BackendError::InvalidCredential {
            provider: CODEX_PROVIDER.to_owned(),
            detail: source.to_string(),
        })?;
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(run_supervisor(config, credential, command_rx, event_tx));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

async fn run_supervisor(
    config: BackendConfig,
    credential: Option<CodexCredential>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let capabilities = native_capabilities();
    let _ = events
        .send(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "OpenAI Codex".to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            capabilities,
        }))
        .await;
    let credential = refresh_if_needed(&config, credential, &events).await;
    let provider = credential.clone().map(|credential| {
        Arc::new(CodexProvider {
            client: config.client.clone(),
            base_url: config.base_url.clone(),
            credential,
        }) as Arc<dyn InferenceProvider>
    });
    let runtime = provider.map(|provider| {
        let mut runtime = AgentRuntime::new(config.workspace.clone(), provider)
            .with_compaction_threshold_percent(config.compaction_threshold_percent);
        if let Some(requests) = &config.native_delegation {
            runtime = runtime.with_native_delegation(requests.clone());
        }
        if let Some(web_config) = &config.web_config {
            runtime = runtime.with_web_config(Arc::clone(web_config));
        }
        if let Some(memory_service) = &config.memory_service {
            runtime = runtime.with_memory(Arc::clone(memory_service));
        }
        if let Some(vision_config) = &config.vision_config {
            runtime = runtime.with_vision(
                Arc::clone(vision_config),
                config.vision_service.clone(),
                true,
            );
        }
        runtime
    });
    let session_store = config
        .session_database
        .clone()
        .map(|database| RuntimeSessionStore::new(database, CODEX_PROVIDER));
    let mut sessions = HashMap::<String, RuntimeSession>::new();
    let mut pending_options = HashMap::<String, ModelOptions>::new();
    let mut active: Option<ActiveTurn> = None;
    let mut authentication_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut authentication_callback: Option<mpsc::Sender<String>> = None;
    let (completed_tx, mut completed_rx) = mpsc::channel::<CompletedTurn>(8);

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(active) = active.take() { active.cancellation.cancel(); }
                    if let Some(task) = authentication_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    break;
                }
                let mut context = CommandContext {
                    config: &config,
                    credential: credential.as_ref(),
                    runtime: runtime.as_ref(),
                    sessions: &mut sessions,
                    pending_options: &mut pending_options,
                    active: &mut active,
                    authentication_task: &mut authentication_task,
                    authentication_callback: &mut authentication_callback,
                    completed: &completed_tx,
                    events: &events,
                    session_store: session_store.as_ref(),
                };
                handle_command(command, &mut context).await;
            }
            completed = completed_rx.recv() => {
                let Some(completed) = completed else { break };
                handle_completed_turn(
                    completed,
                    &mut pending_options,
                    session_store.as_ref(),
                    &mut sessions,
                    &mut active,
                    &events,
                ).await;
            }
        }
    }
    if let Some(task) = authentication_task {
        task.abort();
        let _ = task.await;
    }
}

async fn handle_completed_turn(
    mut completed: CompletedTurn,
    pending_options: &mut HashMap<String, ModelOptions>,
    session_store: Option<&RuntimeSessionStore>,
    sessions: &mut HashMap<String, RuntimeSession>,
    active: &mut Option<ActiveTurn>,
    events: &mpsc::Sender<BackendEvent>,
) {
    if let Some(options) = pending_options.remove(&completed.session.id) {
        completed.session.reasoning_effort = options.reasoning_effort;
        completed.session.fast_mode = options.fast_mode;
    }
    if let Some(store) = session_store
        && let Err(error) = store.save(&completed.session)
    {
        let operation = match completed.kind {
            CompletedWorkKind::Turn => BackendOperation::StartTurn,
            CompletedWorkKind::Compaction => BackendOperation::CompactSession,
        };
        request_failed(events, operation, error).await;
    }
    sessions.insert(completed.session.id.clone(), completed.session);
    if active
        .as_ref()
        .is_some_and(|turn| turn.turn_id == completed.turn_id)
    {
        *active = None;
    }
    if completed.kind == CompletedWorkKind::Turn {
        let (outcome, error) = match completed.result {
            Ok(()) => (TurnOutcome::Completed, None),
            Err(TurnError::Interrupted) => (TurnOutcome::Interrupted, None),
            Err(error) => (TurnOutcome::Failed, Some(error.to_string())),
        };
        let _ = events
            .send(BackendEvent::TurnCompleted {
                turn_id: completed.turn_id,
                outcome,
                error,
            })
            .await;
    }
}

struct ActiveTurn {
    session_id: String,
    turn_id: String,
    cancellation: CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletedWorkKind {
    Turn,
    Compaction,
}

struct CompletedTurn {
    turn_id: String,
    session: RuntimeSession,
    result: Result<(), TurnError>,
    kind: CompletedWorkKind,
}

struct CommandContext<'a> {
    config: &'a BackendConfig,
    credential: Option<&'a CodexCredential>,
    runtime: Option<&'a AgentRuntime>,
    sessions: &'a mut HashMap<String, RuntimeSession>,
    pending_options: &'a mut HashMap<String, ModelOptions>,
    active: &'a mut Option<ActiveTurn>,
    authentication_task: &'a mut Option<tokio::task::JoinHandle<()>>,
    authentication_callback: &'a mut Option<mpsc::Sender<String>>,
    completed: &'a mpsc::Sender<CompletedTurn>,
    events: &'a mpsc::Sender<BackendEvent>,
    session_store: Option<&'a RuntimeSessionStore>,
}

#[allow(clippy::too_many_lines)]
async fn handle_command(command: BackendCommand, context: &mut CommandContext<'_>) {
    match command {
        BackendCommand::BeginAuthentication { client_context } => {
            if let Some(task) = context.authentication_task.take() {
                task.abort();
                let _ = task.await;
            }
            let (callback_tx, callback_rx) = mpsc::channel(1);
            *context.authentication_callback = Some(callback_tx);
            *context.authentication_task = Some(tokio::spawn(authenticate(
                client_context,
                context.config.clone(),
                context.events.clone(),
                callback_rx,
            )));
        }
        BackendCommand::SubmitAuthenticationCallback { callback_url } => {
            let Some(callback) = context.authentication_callback.as_ref() else {
                request_failed(
                    context.events,
                    BackendOperation::Authenticate,
                    "no in-progress browser authentication challenge",
                )
                .await;
                return;
            };
            if callback.send(callback_url).await.is_err() {
                request_failed(
                    context.events,
                    BackendOperation::Authenticate,
                    "browser authentication challenge is no longer active",
                )
                .await;
            }
        }
        BackendCommand::Reload { .. } => match context.credential {
            Some(credential) => match discover_models(context.config, credential).await {
                Ok(models) => {
                    let _ = context
                        .events
                        .send(BackendEvent::Models(model_infos(models)))
                        .await;
                }
                Err(error) => {
                    request_failed_with_detail(
                        context.events,
                        BackendOperation::Reload,
                        error.message,
                        Some(error.detail),
                    )
                    .await;
                }
            },
            None => {
                request_failed(
                    context.events,
                    BackendOperation::Reload,
                    "OpenAI is not authenticated",
                )
                .await;
            }
        },
        BackendCommand::StartSession {
            model,
            instructions,
            external_tools,
            replace_builtin_tools,
            code_mode,
            allowed_builtin_tools,
            max_turns,
            finalization_reserve_turns,
            timeout_seconds,
            owner_session_id,
            parent_run_id,
            enabled_skill_ids,
        } => {
            start_session(
                model,
                instructions,
                owner_session_id,
                parent_run_id,
                enabled_skill_ids,
                external_tools,
                replace_builtin_tools,
                code_mode,
                allowed_builtin_tools,
                max_turns,
                finalization_reserve_turns,
                timeout_seconds,
                context,
            )
            .await;
        }
        BackendCommand::ResumeSession {
            provider_session_id,
            owner_session_id,
            enabled_skill_ids,
            external_tools,
            replace_builtin_tools,
            code_mode,
            allowed_builtin_tools,
            max_turns,
            timeout_seconds,
        } => {
            if let Some(runtime) = context.runtime
                && let Err(error) = runtime
                    .configure_external_tools(
                        &provider_session_id,
                        external_tools,
                        replace_builtin_tools,
                        code_mode,
                        allowed_builtin_tools,
                        max_turns,
                        0,
                        timeout_seconds,
                    )
                    .await
            {
                request_failed(context.events, BackendOperation::ResumeSession, error).await;
                return;
            }
            resume_session(
                provider_session_id,
                owner_session_id,
                enabled_skill_ids,
                context,
            )
            .await;
        }
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => {
            context.sessions.remove(&provider_session_id);
            let _ = context.events.send(BackendEvent::SessionUnsubscribed).await;
        }
        BackendCommand::CompactSession {
            provider_session_id,
            compaction_id,
        } => compact_session(provider_session_id, compaction_id, context).await,
        BackendCommand::SetSessionModel {
            provider_session_id,
            model,
        } => {
            if let Err(error) = set_session_model(
                context.sessions,
                context.session_store,
                &provider_session_id,
                model,
            ) {
                request_failed(context.events, BackendOperation::SetSessionModel, error).await;
            }
        }
        BackendCommand::SetSessionCodeMode {
            provider_session_id,
            enabled,
        } => {
            if let Some(runtime) = context.runtime
                && let Err(error) = runtime.set_code_mode(&provider_session_id, enabled).await
            {
                request_failed(context.events, BackendOperation::SetSessionCodeMode, error).await;
            }
        }
        BackendCommand::SetSessionOptions {
            provider_session_id,
            options,
        } => {
            if let Err(error) = set_session_options(
                context.sessions,
                context.pending_options,
                context.active.as_ref(),
                context.session_store,
                &provider_session_id,
                options,
            ) {
                request_failed(context.events, BackendOperation::SetSessionModel, error).await;
            }
        }
        BackendCommand::StartTurn {
            provider_session_id,
            client_id,
            prompt,
            attachments,
            model,
            skill_catalogue,
        } => {
            start_turn(
                provider_session_id,
                client_id,
                prompt,
                attachments,
                model,
                skill_catalogue,
                context,
            )
            .await;
        }
        BackendCommand::InterruptTurn { turn_id, .. } => {
            if let Some(active) = context
                .active
                .as_ref()
                .filter(|active| active.turn_id == turn_id)
            {
                active.cancellation.cancel();
                let _ = context.events.send(BackendEvent::InterruptAccepted).await;
            }
        }
        BackendCommand::SteerTurn { .. } => {
            request_failed(
                context.events,
                BackendOperation::SteerTurn,
                "native turn steering is not implemented",
            )
            .await;
        }
        BackendCommand::ResolveQuestion { id, answer } => {
            if let Some(runtime) = context.runtime {
                runtime.resolve_question(&id, answer).await;
            }
        }
        BackendCommand::ResolveExternalTool { id, output, failed } => {
            if let Some(runtime) = context.runtime {
                runtime
                    .resolve_external_tool(
                        &id,
                        crate::tools::ToolResult {
                            output,
                            failed,
                            invocation_identity: None,
                        },
                    )
                    .await;
            }
        }
        BackendCommand::ResolveApproval { .. } | BackendCommand::Shutdown => {}
    }
}

fn set_session_options(
    sessions: &mut HashMap<String, RuntimeSession>,
    pending_options: &mut HashMap<String, ModelOptions>,
    active: Option<&ActiveTurn>,
    store: Option<&RuntimeSessionStore>,
    session_id: &str,
    options: ModelOptions,
) -> Result<(), String> {
    let Some(session) = sessions.get_mut(session_id) else {
        if active.is_some_and(|active| active.session_id == session_id) {
            pending_options.insert(session_id.to_owned(), options);
            return Ok(());
        }
        return Err("unknown native session".to_owned());
    };
    session.reasoning_effort = options.reasoning_effort;
    session.fast_mode = options.fast_mode;
    if let Some(store) = store {
        store.save(session)?;
    }
    Ok(())
}

async fn compact_session(
    session_id: String,
    compaction_id: String,
    context: &mut CommandContext<'_>,
) {
    let Some(runtime) = context.runtime else {
        request_failed(
            context.events,
            BackendOperation::CompactSession,
            "OpenAI is not authenticated",
        )
        .await;
        return;
    };
    if context.active.is_some() {
        request_failed(
            context.events,
            BackendOperation::CompactSession,
            "another turn is active",
        )
        .await;
        return;
    }
    let Some(mut session) = context.sessions.remove(&session_id) else {
        request_failed(
            context.events,
            BackendOperation::CompactSession,
            "unknown native session",
        )
        .await;
        return;
    };
    let cancellation = CancellationToken::new();
    *context.active = Some(ActiveTurn {
        session_id: session.id.clone(),
        turn_id: compaction_id.clone(),
        cancellation: cancellation.clone(),
    });
    let completed = context.completed.clone();
    let events = context.events.clone();
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let result = runtime
            .force_compact(&mut session, &compaction_id, &events, cancellation)
            .await
            .map_err(TurnError::from);
        let _ = completed
            .send(CompletedTurn {
                turn_id: compaction_id,
                session,
                result,
                kind: CompletedWorkKind::Compaction,
            })
            .await;
    });
}

async fn start_turn(
    session_id: String,
    client_id: String,
    prompt: String,
    attachments: Vec<crate::backend::PromptAttachment>,
    model: Option<String>,
    skill_catalogue: crate::skill::SkillCatalog,
    context: &mut CommandContext<'_>,
) {
    let Some(runtime) = context.runtime else {
        request_failed(
            context.events,
            BackendOperation::StartTurn,
            "OpenAI is not authenticated",
        )
        .await;
        return;
    };
    if context.active.is_some() {
        request_failed(
            context.events,
            BackendOperation::StartTurn,
            "another turn is active",
        )
        .await;
        return;
    }
    let Some(mut session) = context.sessions.remove(&session_id) else {
        request_failed(
            context.events,
            BackendOperation::StartTurn,
            "unknown native session",
        )
        .await;
        return;
    };
    session.enabled_skill_ids = Some(skill_catalogue.stable_ids());
    session.skill_catalogue = skill_catalogue;
    if let Some(model) = model {
        if session.model != model {
            session.context_window = None;
        }
        session.model = model;
    }
    if session.context_window.is_none() {
        session.context_window = discover_context_window(
            context.config,
            context
                .credential
                .expect("authenticated runtime has a credential"),
            &session.model,
        )
        .await;
    }
    let cancellation = CancellationToken::new();
    *context.active = Some(ActiveTurn {
        session_id: session.id.clone(),
        turn_id: client_id.clone(),
        cancellation: cancellation.clone(),
    });
    let _ = context
        .events
        .send(BackendEvent::TurnAccepted {
            turn_id: client_id.clone(),
        })
        .await;
    let completed = context.completed.clone();
    let events = context.events.clone();
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let result = runtime
            .run_turn(
                &mut session,
                &client_id,
                prompt,
                attachments,
                &events,
                cancellation,
            )
            .await;
        let _ = completed
            .send(CompletedTurn {
                turn_id: client_id,
                session,
                result,
                kind: CompletedWorkKind::Turn,
            })
            .await;
    });
}

#[allow(clippy::too_many_arguments)]
// Session startup mirrors the complete backend command contract; grouping only policy fields here
// would create a provider-local shape that the other native backends must duplicate.
async fn start_session(
    model: Option<String>,
    instructions: Option<String>,
    owner_session_id: Option<String>,
    parent_run_id: Option<String>,
    enabled_skill_ids: Vec<String>,
    external_tools: Vec<nakode_protocol::ExternalToolDefinition>,
    replace_builtin_tools: bool,
    code_mode: bool,
    allowed_builtin_tools: Option<Vec<String>>,
    max_turns: Option<u32>,
    finalization_reserve_turns: u32,
    timeout_seconds: Option<u32>,
    context: &mut CommandContext<'_>,
) {
    let Some(credential) = context.credential else {
        request_failed(
            context.events,
            BackendOperation::StartSession,
            "OpenAI is not authenticated",
        )
        .await;
        return;
    };
    let models = match discover_models(context.config, credential).await {
        Ok(models) => models,
        Err(error) => {
            request_failed_with_detail(
                context.events,
                BackendOperation::StartSession,
                error.message,
                Some(error.detail),
            )
            .await;
            return;
        }
    };
    let selected = model
        .and_then(|requested| {
            models
                .iter()
                .find(|candidate| candidate.info.id == requested)
        })
        .or_else(|| models.iter().find(|model| model.info.is_default))
        .or_else(|| models.first());
    let Some(selected) = selected else {
        request_failed(
            context.events,
            BackendOperation::StartSession,
            "OpenAI returned no usable models",
        )
        .await;
        return;
    };
    let selected_id = selected.info.id.clone();
    let session = RuntimeSession::new(selected_id.clone(), instructions.unwrap_or_default())
        .with_enabled_skill_ids(enabled_skill_ids)
        .with_provider(CODEX_PROVIDER)
        .with_owner(owner_session_id, parent_run_id)
        .with_context_window(selected.context_window)
        .with_reasoning_effort(context.config.reasoning_effort.clone());
    let session_id = session.id.clone();
    if let Some(runtime) = context.runtime
        && let Err(error) = runtime
            .configure_external_tools(
                &session_id,
                external_tools,
                replace_builtin_tools,
                code_mode,
                allowed_builtin_tools,
                max_turns,
                finalization_reserve_turns,
                timeout_seconds,
            )
            .await
    {
        request_failed(context.events, BackendOperation::StartSession, error).await;
        return;
    }
    let estimated_tokens = session.estimated_context_tokens();
    let context_window = session.context_window;
    if let Some(store) = context.session_store
        && let Err(error) = store.save(&session)
    {
        request_failed(context.events, BackendOperation::StartSession, error).await;
        return;
    }
    context.sessions.insert(session_id.clone(), session);
    let _ = context
        .events
        .send(BackendEvent::SessionCreated {
            provider_session_id: session_id,
            model: selected_id,
        })
        .await;
    let _ = context
        .events
        .send(BackendEvent::ContextUsageUpdated {
            estimated_tokens,
            context_window,
        })
        .await;
    let _ = context
        .events
        .send(BackendEvent::Models(model_infos(models)))
        .await;
}

async fn resume_session(
    provider_session_id: String,
    owner_session_id: Option<String>,
    enabled_skill_ids: Vec<String>,
    context: &mut CommandContext<'_>,
) {
    let persisted = context
        .session_store
        .map(|store| store.load(&provider_session_id))
        .transpose();
    let persisted = match persisted {
        Ok(session) => session.flatten(),
        Err(error) => {
            request_failed(context.events, BackendOperation::ResumeSession, error).await;
            return;
        }
    };
    if let Some(mut session) = context
        .sessions
        .get(&provider_session_id)
        .cloned()
        .or(persisted)
    {
        session.owner_session_id = owner_session_id;
        session.parent_run_id = None;
        session.enabled_skill_ids = Some(enabled_skill_ids);
        if session.context_window.is_none()
            && let Some(credential) = context.credential
        {
            session.context_window =
                discover_context_window(context.config, credential, &session.model).await;
        }
        if let Some(store) = context.session_store
            && let Err(error) = store.save(&session)
        {
            request_failed(context.events, BackendOperation::ResumeSession, error).await;
            return;
        }
        context
            .sessions
            .insert(provider_session_id.clone(), session.clone());
        let _ = context
            .events
            .send(BackendEvent::SessionResumed {
                provider_session_id,
                model: session.model.clone(),
                history: session.normalized_history(),
            })
            .await;
        let _ = context
            .events
            .send(BackendEvent::ContextUsageUpdated {
                estimated_tokens: session.estimated_context_tokens(),
                context_window: session.context_window,
            })
            .await;
        let _ = context
            .events
            .send(BackendEvent::TodoUpdated {
                phases: session.todos,
            })
            .await;
    } else {
        request_failed(
            context.events,
            BackendOperation::ResumeSession,
            "native session is not loaded",
        )
        .await;
    }
}

fn native_capabilities() -> BackendCapabilities {
    BackendCapabilities {
        resume: CapabilitySupport::Supported,
        steering: CapabilitySupport::Unsupported,
        interruption: CapabilitySupport::Supported,
        model_catalog: CapabilitySupport::Supported,
        models_require_session: CapabilitySupport::Unsupported,
        session_model_config: CapabilitySupport::Supported,
        context_compaction: CapabilitySupport::Supported,
        approvals: CapabilitySupport::Unsupported,
        native_tools: CapabilitySupport::Supported,
        external_tools: CapabilitySupport::Supported,
        scoped_runtime_policy: CapabilitySupport::Supported,
        mcp: CapabilitySupport::Unsupported,
        close_session: CapabilitySupport::Supported,
    }
}

#[derive(Debug)]
struct ModelDiscoveryFailure {
    message: String,
    detail: BackendFailureDetail,
}

impl ModelDiscoveryFailure {
    fn new(
        classification: BackendFailureClassification,
        summary: impl Into<String>,
        endpoint: &str,
        http_status: Option<u16>,
        source_chain: Vec<String>,
    ) -> Self {
        let summary = bounded_text(&summary.into(), 512);
        Self {
            message: summary.clone(),
            detail: BackendFailureDetail {
                phase: BackendFailurePhase::ModelDiscovery,
                classification,
                summary,
                operation: "discover provider models".to_owned(),
                safe_endpoint: Some(safe_endpoint(endpoint)),
                http_status,
                source_chain,
                correlation_id: None,
            },
        }
    }
}

const MAX_MODEL_CATALOGUE_BYTES: usize = 2 * 1024 * 1024;
const MAX_MODEL_CATALOGUE_ENTRIES: usize = 1_024;
const MAX_MODEL_ID_CHARS: usize = 256;

#[allow(clippy::result_large_err, clippy::too_many_lines)]
async fn discover_models(
    config: &BackendConfig,
    credential: &CodexCredential,
) -> Result<Vec<DiscoveredModel>, ModelDiscoveryFailure> {
    let mut last_failure = None;
    for path in ["codex/models", "models"] {
        let url = format!("{}/{path}", config.base_url.trim_end_matches('/'));
        let response = match config
            .client
            .get(&url)
            .query(&[("client_version", CODEX_CLIENT_VERSION)])
            .bearer_auth(&credential.access_token)
            .header("chatgpt-account-id", &credential.account_id)
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "nakode")
            .header("version", CODEX_CLIENT_VERSION)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let classification = if error.is_timeout() {
                    BackendFailureClassification::Timeout
                } else if error.is_connect() {
                    BackendFailureClassification::Connectivity
                } else {
                    BackendFailureClassification::Transport
                };
                return Err(ModelDiscoveryFailure::new(
                    classification,
                    "Provider model discovery could not reach the provider.",
                    &url,
                    None,
                    safe_source_chain(&error),
                ));
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let (classification, summary) = match status {
                StatusCode::UNAUTHORIZED => (
                    BackendFailureClassification::Authentication,
                    "Provider model discovery was not authenticated.",
                ),
                StatusCode::FORBIDDEN => (
                    BackendFailureClassification::Authorization,
                    "Provider model discovery was not authorized.",
                ),
                StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => (
                    BackendFailureClassification::Timeout,
                    "Provider model discovery timed out at the provider.",
                ),
                status if status.is_server_error() => (
                    BackendFailureClassification::ProviderUnavailable,
                    "The provider model catalogue is unavailable.",
                ),
                _ => (
                    BackendFailureClassification::HttpStatus,
                    "Provider model discovery returned an unexpected HTTP status.",
                ),
            };
            let failure = ModelDiscoveryFailure::new(
                classification,
                summary,
                &url,
                Some(status.as_u16()),
                Vec::new(),
            );
            if matches!(
                status,
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ) {
                last_failure = Some(failure);
                continue;
            }
            return Err(failure);
        }
        let payload = read_model_catalogue(response, &url).await?;
        let entries = payload
            .get("models")
            .or_else(|| payload.get("data"))
            .and_then(Value::as_array);
        let Some(entries) = entries else {
            last_failure = Some(ModelDiscoveryFailure::new(
                BackendFailureClassification::MalformedResponse,
                "The provider response did not contain a model catalogue.",
                &url,
                None,
                Vec::new(),
            ));
            continue;
        };
        if entries.len() > MAX_MODEL_CATALOGUE_ENTRIES {
            return Err(ModelDiscoveryFailure::new(
                BackendFailureClassification::MalformedResponse,
                "The provider model catalogue exceeded its safe entry limit.",
                &url,
                None,
                Vec::new(),
            ));
        }
        let models = entries
            .iter()
            .filter_map(|entry| {
                if entry.get("supported_in_api").and_then(Value::as_bool) == Some(false) {
                    return None;
                }
                let id = entry
                    .get("slug")
                    .or_else(|| entry.get("id"))
                    .and_then(Value::as_str)?
                    .trim();
                if id.is_empty() || id.chars().count() > MAX_MODEL_ID_CHARS {
                    return None;
                }
                let context_window = entry
                    .get("context_window")
                    .and_then(Value::as_u64)
                    .and_then(|window| usize::try_from(window).ok())
                    .map(|window| {
                        let percent = entry
                            .get("effective_context_window_percent")
                            .and_then(Value::as_u64)
                            .and_then(|percent| usize::try_from(percent).ok())
                            .unwrap_or(100)
                            .min(100);
                        window.saturating_mul(percent) / 100
                    });
                Some(DiscoveredModel {
                    info: ModelInfo {
                        provider: CODEX_PROVIDER.to_owned(),
                        id: id.to_owned(),
                        is_default: entry
                            .get("is_default")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        capabilities: super::model_capabilities(),
                    },
                    context_window,
                })
            })
            .collect::<Vec<_>>();
        if models.is_empty() {
            last_failure = Some(ModelDiscoveryFailure::new(
                BackendFailureClassification::MalformedResponse,
                "The provider returned no usable models.",
                &url,
                None,
                Vec::new(),
            ));
            continue;
        }
        return Ok(models);
    }
    Err(last_failure.unwrap_or_else(|| {
        ModelDiscoveryFailure::new(
            BackendFailureClassification::Unknown,
            "The provider did not expose a usable model catalogue.",
            config.base_url.as_str(),
            None,
            Vec::new(),
        )
    }))
}

#[allow(clippy::result_large_err)]
async fn read_model_catalogue(
    response: reqwest::Response,
    url: &str,
) -> Result<Value, ModelDiscoveryFailure> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MODEL_CATALOGUE_BYTES as u64)
    {
        return Err(ModelDiscoveryFailure::new(
            BackendFailureClassification::MalformedResponse,
            "The provider model catalogue exceeded its safe response limit.",
            url,
            None,
            Vec::new(),
        ));
    }

    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|error| {
            let (classification, summary) = if error.is_timeout() {
                (
                    BackendFailureClassification::Timeout,
                    "Provider model discovery timed out while reading the response.",
                )
            } else if error.is_connect() {
                (
                    BackendFailureClassification::Connectivity,
                    "Provider model discovery lost its connection while reading the response.",
                )
            } else {
                (
                    BackendFailureClassification::Transport,
                    "Provider model discovery could not read the provider response.",
                )
            };
            ModelDiscoveryFailure::new(
                classification,
                summary,
                url,
                None,
                safe_source_chain(&error),
            )
        })?;
        if body.len().saturating_add(chunk.len()) > MAX_MODEL_CATALOGUE_BYTES {
            return Err(ModelDiscoveryFailure::new(
                BackendFailureClassification::MalformedResponse,
                "The provider model catalogue exceeded its safe response limit.",
                url,
                None,
                Vec::new(),
            ));
        }
        body.extend_from_slice(&chunk);
    }

    serde_json::from_slice(&body).map_err(|_| {
        ModelDiscoveryFailure::new(
            BackendFailureClassification::MalformedResponse,
            "The provider returned a malformed model catalogue.",
            url,
            None,
            Vec::new(),
        )
    })
}

fn safe_endpoint(value: &str) -> String {
    sanitize_failure_endpoint(value, 512)
}

fn safe_source_chain(error: &reqwest::Error) -> Vec<String> {
    let mut result = Vec::new();
    let mut source = error.source();
    while let Some(cause) = source {
        if result.len() == 4 {
            break;
        }
        let safe = sanitize_failure_text(&cause.to_string());
        if !safe.is_empty() && result.last() != Some(&safe) {
            result.push(safe);
        }
        source = cause.source();
    }
    result
}

fn sanitize_failure_text(value: &str) -> String {
    sanitize_backend_failure_text(value, 256)
}

fn bounded_text(value: &str, maximum: usize) -> String {
    bounded_failure_text(value, maximum)
}

fn model_infos(models: Vec<DiscoveredModel>) -> Vec<ModelInfo> {
    models.into_iter().map(|model| model.info).collect()
}

async fn discover_context_window(
    config: &BackendConfig,
    credential: &CodexCredential,
    model: &str,
) -> Option<usize> {
    discover_models(config, credential)
        .await
        .ok()?
        .into_iter()
        .find(|candidate| candidate.info.id == model)
        .and_then(|candidate| candidate.context_window)
}

fn codex_request_body(request: &InferenceRequest) -> Value {
    let input = request
        .history
        .iter()
        .flat_map(conversation_input)
        .collect::<Vec<_>>();
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
                "strict": false
            })
        })
        .collect::<Vec<_>>();
    let mut body = json!({
        "model": request.model,
        "input": input,
        "instructions": request.instructions,
        "tools": tools,
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "reasoning": {
            "effort": request.reasoning_effort.as_deref().unwrap_or("medium"),
            "summary": "detailed"
        },
        "include": ["reasoning.encrypted_content"],
        "stream": true,
        "store": false,
        "prompt_cache_key": request.session_id
    });
    if request.fast_mode {
        body["service_tier"] = Value::String("priority".to_owned());
    }
    body
}

fn conversation_input(item: &ConversationItem) -> Vec<Value> {
    match item {
        ConversationItem::User { text, attachments } => {
            let mut content = vec![json!({"type": "input_text", "text": text})];
            content.extend(attachments.iter().filter_map(|attachment| {
                let image = attachment.image.as_ref()?;
                Some(json!({
                    "type": "input_image",
                    "image_url": format!(
                        "data:{};base64,{}",
                        image.mime_type,
                        base64::engine::general_purpose::STANDARD.encode(&image.data)
                    )
                }))
            }));
            vec![json!({"role": "user", "content": content})]
        }
        ConversationItem::Assistant {
            text,
            tool_calls,
            provider_state,
            ..
        } => {
            let mut items = provider_state.clone();
            if !text.is_empty() {
                items.push(json!({"role": "assistant", "content": [{"type": "output_text", "text": text, "annotations": []}]}));
            }
            items.extend(tool_calls.iter().map(|call| json!({
                "type": "function_call", "call_id": call.id, "name": call.name,
                "arguments": serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_owned())
            })));
            items
        }
        ConversationItem::ToolResult {
            call_id,
            output,
            model_output,
            ..
        } => vec![json!({
            "type": "function_call_output", "call_id": call_id,
            "output": model_output.as_ref().unwrap_or(output)
        })],
        ConversationItem::Compaction { summary } => {
            vec![
                json!({"role": "user", "content": [{"type": "input_text", "text": format!("Context checkpoint from earlier work:\n\n{summary}")}]}),
            ]
        }
        ConversationItem::CompactionEvent { .. } => Vec::new(),
    }
}

const MAX_PROVIDER_ERROR_BODY_BYTES: usize = 64 * 1024;

async fn read_bounded_provider_error_body(response: reqwest::Response) -> String {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let Ok(chunk) = chunk else {
            break;
        };
        let remaining = MAX_PROVIDER_ERROR_BODY_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            break;
        }
        let take = remaining.min(chunk.len());
        body.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            break;
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

fn codex_failure_classification(status: StatusCode, detail: &str) -> CodexFailureClassification {
    let detail = detail.to_ascii_lowercase();
    let contains = |fragments: &[&str]| fragments.iter().any(|fragment| detail.contains(fragment));
    if status == StatusCode::UNAUTHORIZED {
        return CodexFailureClassification::Authentication;
    }
    if status == StatusCode::PAYMENT_REQUIRED
        || contains(&[
            "quota",
            "billing",
            "insufficient balance",
            "usage limit",
            "resource_exhausted",
        ])
    {
        return CodexFailureClassification::Quota;
    }
    if status == StatusCode::FORBIDDEN
        || contains(&[
            "invalid api key",
            "invalid_api_key",
            "invalid token",
            "authentication",
            "unauthenticated",
            "credential",
        ])
    {
        return CodexFailureClassification::Authentication;
    }
    if status == StatusCode::TOO_MANY_REQUESTS
        || contains(&["rate limit", "rate_limit", "ratelimit", "too many requests"])
    {
        return CodexFailureClassification::RateLimit;
    }
    if status == StatusCode::NOT_FOUND
        || contains(&[
            "model_not_found",
            "model not found",
            "invalid model",
            "unsupported model",
            "model unavailable",
        ])
    {
        return CodexFailureClassification::Model;
    }
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::CONFLICT
        || status.is_server_error()
    {
        return CodexFailureClassification::Transient;
    }
    CodexFailureClassification::Provider
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn retry_delay(attempt: usize) -> Duration {
    #[cfg(test)]
    {
        let _ = attempt;
        Duration::from_millis(1)
    }
    #[cfg(not(test))]
    {
        let exponent = u32::try_from(attempt).unwrap_or(u32::MAX).min(4);
        Duration::from_millis(500_u64.saturating_mul(2_u64.saturating_pow(exponent)))
    }
}

fn retryable_stream_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "server error",
        "internal error",
        "temporar",
        "overload",
        "rate limit",
        "try again",
        "timeout",
        "timed out",
        "connection",
        "unavailable",
    ]
    .iter()
    .any(|fragment| message.contains(fragment))
}

const MAX_CODEX_SSE_EVENT_BYTES: usize = 1024 * 1024;

fn append_codex_sse_chunk(pending: &mut String, chunk: &[u8]) -> Result<(), InferenceAttemptError> {
    let chunk = String::from_utf8_lossy(chunk);
    if pending.len().saturating_add(chunk.len()) > MAX_CODEX_SSE_EVENT_BYTES {
        return Err(InferenceAttemptError::terminal(
            "Codex stream event exceeded its safe size limit",
        ));
    }
    pending.push_str(&chunk);
    Ok(())
}

async fn parse_codex_sse(
    response: reqwest::Response,
    events: mpsc::Sender<InferenceEvent>,
    cancellation: CancellationToken,
) -> Result<InferenceOutput, InferenceAttemptError> {
    let mut stream = response.bytes_stream();
    let mut pending = String::new();
    let mut output = InferenceOutput::default();
    let mut completed = false;
    loop {
        let chunk = tokio::select! {
            chunk = stream.next() => chunk,
            () = cancellation.cancelled() => {
                return Err(InferenceAttemptError::terminal("turn interrupted"));
            },
        };
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|error| {
            let safe = sanitize_backend_failure_text(&error.to_string(), 256);
            let message = if safe.is_empty() {
                "Codex stream transport failed.".to_owned()
            } else {
                format!("Codex stream transport failed: {safe}")
            };
            if output.text.is_empty() && output.reasoning.is_empty() {
                InferenceAttemptError::transient(message)
            } else {
                InferenceAttemptError::terminal(message)
            }
        })?;
        append_codex_sse_chunk(&mut pending, &chunk)?;
        while let Some(boundary) = pending.find('\n') {
            let line = pending[..boundary].trim_end_matches('\r').to_owned();
            pending.drain(..=boundary);
            let Some(data) = line.strip_prefix("data:").map(str::trim) else {
                continue;
            };
            if data == "[DONE]" {
                completed = true;
                continue;
            }
            let event: Value = serde_json::from_str(data).map_err(|error| {
                InferenceAttemptError::terminal(format!("invalid Codex event: {error}"))
            })?;
            match apply_codex_event(&event, &events, &mut output).await {
                Ok(event_completed) => completed |= event_completed,
                Err(message) => {
                    let retryable = output.text.is_empty()
                        && output.reasoning.is_empty()
                        && retryable_stream_message(&message);
                    let classification = if retryable {
                        CodexFailureClassification::Transient
                    } else {
                        codex_failure_classification(StatusCode::BAD_REQUEST, &message)
                    };
                    let message = sanitize_backend_failure_text(&message, 512);
                    return Err(InferenceAttemptError {
                        message,
                        classification,
                        retryable,
                        retry_after: None,
                    });
                }
            }
        }
    }
    if !pending.is_empty() {
        return Err(InferenceAttemptError::terminal(
            "Codex stream ended in the middle of an event",
        ));
    }
    if !completed {
        return Err(InferenceAttemptError::terminal(
            "Codex stream ended before a completion marker",
        ));
    }
    Ok(output)
}

async fn apply_codex_event(
    event: &Value,
    events: &mpsc::Sender<InferenceEvent>,
    output: &mut InferenceOutput,
) -> Result<bool, String> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match event_type {
        "response.created" => {
            output.response_id = event
                .pointer("/response/id")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        "response.output_text.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                output.text.push_str(delta);
                events
                    .send(InferenceEvent::TextDelta(delta.to_owned()))
                    .await
                    .map_err(|_| "inference event receiver closed".to_owned())?;
            }
        }
        "response.reasoning_summary_text.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                output.reasoning.push_str(delta);
                let index = event
                    .get("summary_index")
                    .and_then(Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .unwrap_or_default();
                events
                    .send(InferenceEvent::ReasoningSummaryDelta {
                        delta: delta.to_owned(),
                        index,
                    })
                    .await
                    .map_err(|_| "inference event receiver closed".to_owned())?;
            }
        }
        "response.reasoning_text.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                output.reasoning.push_str(delta);
                events
                    .send(InferenceEvent::ReasoningDelta(delta.to_owned()))
                    .await
                    .map_err(|_| "inference event receiver closed".to_owned())?;
            }
        }
        "response.output_item.done" => {
            let item = event.get("item").unwrap_or(&Value::Null);
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_else(|| json!({}));
                output.tool_calls.push(ToolCall {
                    id: item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    parent_call_id: None,
                    name: item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    arguments,
                });
            } else if item.get("type").and_then(Value::as_str) == Some("reasoning") {
                output.provider_state.push(item.clone());
            }
        }
        "response.completed" => {
            let usage = event.pointer("/response/usage").unwrap_or(&Value::Null);
            output.usage.input_tokens = usage.get("input_tokens").and_then(Value::as_u64);
            output.usage.output_tokens = usage.get("output_tokens").and_then(Value::as_u64);
            output.usage.cached_input_tokens = usage
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(Value::as_u64);
            output.usage.cache_write_tokens = usage
                .pointer("/input_tokens_details/cache_write_tokens")
                .and_then(Value::as_u64);
        }
        "error" => {
            return Err(codex_error_message(event, "Codex stream error"));
        }
        "response.failed" => {
            return Err(codex_error_message(event, "Codex response failed"));
        }
        _ => {}
    }
    Ok(event_type == "response.completed")
}

fn codex_error_message(event: &Value, fallback: &str) -> String {
    let message = event
        .get("message")
        .or_else(|| event.pointer("/error/message"))
        .or_else(|| event.pointer("/response/error/message"))
        .or_else(|| event.get("error"))
        .and_then(Value::as_str);
    let code = event
        .get("code")
        .or_else(|| event.pointer("/error/code"))
        .or_else(|| event.pointer("/response/error/code"))
        .and_then(Value::as_str);
    sanitize_backend_failure_text(
        &match (code, message) {
            (Some(code), Some(message)) => format!("{code}: {message}"),
            (_, Some(message)) => message.to_owned(),
            (Some(code), None) => code.to_owned(),
            (None, None) => fallback.to_owned(),
        },
        512,
    )
}

fn safe_adapter_error(prefix: &str, error: &dyn std::fmt::Display) -> String {
    let detail = sanitize_backend_failure_text(&error.to_string(), 384);
    if detail.is_empty() {
        prefix.to_owned()
    } else {
        bounded_text(&format!("{prefix}: {detail}"), 512)
    }
}

const MAX_AUTH_RESPONSE_BYTES: usize = 1024 * 1024;

fn append_bounded_auth_response_chunk(
    body: &mut Vec<u8>,
    chunk: &[u8],
    context: &str,
) -> Result<(), String> {
    if body.len().saturating_add(chunk.len()) > MAX_AUTH_RESPONSE_BYTES {
        return Err(format!("{context}: response exceeded its safe size limit"));
    }
    body.extend_from_slice(chunk);
    Ok(())
}

async fn read_bounded_auth_json(
    response: reqwest::Response,
    context: &str,
) -> Result<Value, String> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| safe_adapter_error(context, &error))?;
        append_bounded_auth_response_chunk(&mut body, &chunk, context)?;
    }
    serde_json::from_slice(&body).map_err(|error| safe_adapter_error(context, &error))
}

async fn authenticate(
    client_context: crate::backend::ClientContext,
    config: BackendConfig,
    events: mpsc::Sender<BackendEvent>,
    callback_rx: mpsc::Receiver<String>,
) {
    if let Err(error) = authenticate_inner(&config, &events, client_context, callback_rx).await {
        request_failed(&events, BackendOperation::Authenticate, error).await;
    }
}

async fn authenticate_inner(
    config: &BackendConfig,
    events: &mpsc::Sender<BackendEvent>,
    client_context: crate::backend::ClientContext,
    callback_rx: mpsc::Receiver<String>,
) -> Result<(), String> {
    if matches!(client_context, crate::backend::ClientContext::Remote) {
        return authenticate_device_code(config, events).await;
    }
    match config.auth_flow {
        CodexAuthFlow::Browser => authenticate_browser(config, events, callback_rx).await,
        CodexAuthFlow::DeviceCode => authenticate_device_code(config, events).await,
    }
}

struct BrowserCallbackListeners {
    ipv4: TcpListener,
    ipv6: Option<TcpListener>,
}

impl BrowserCallbackListeners {
    fn port(&self) -> Result<u16, String> {
        self.ipv4
            .local_addr()
            .map(|address| address.port())
            .map_err(|error| format!("could not inspect Codex sign-in callback: {error}"))
    }

    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        if let Some(ipv6) = &self.ipv6 {
            tokio::select! {
                accepted = self.ipv4.accept() => accepted,
                accepted = ipv6.accept() => accepted,
            }
        } else {
            self.ipv4.accept().await
        }
    }
}

async fn authenticate_browser(
    config: &BackendConfig,
    events: &mpsc::Sender<BackendEvent>,
    callback_rx: mpsc::Receiver<String>,
) -> Result<(), String> {
    let listener = bind_browser_callback(&config.callback_ports).await?;
    let port = listener.port()?;
    let redirect_uri = format!("http://localhost:{port}{BROWSER_CALLBACK_PATH}");
    let state = Uuid::now_v7().simple().to_string();
    let verifier = pkce_verifier();
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let verification_url = browser_authorization_url(
        &config.auth_urls.authorize,
        &redirect_uri,
        &state,
        &challenge,
    )?;
    events
        .send(BackendEvent::AuthenticationChallenge {
            login_id: state.clone(),
            verification_url,
            user_code: String::new(),
            callback_url: Some(redirect_uri.clone()),
        })
        .await
        .map_err(|_| "Codex sign-in was cancelled".to_owned())?;
    let (code, mut callback) = timeout(
        BROWSER_AUTHENTICATION_TIMEOUT,
        receive_browser_authorization_code(&listener, &state, callback_rx),
    )
    .await
    .map_err(|_| "Codex sign-in timed out. Retry sign-in from Provider Auth.".to_owned())??;
    let credential = match exchange_browser_token(config, &code, &verifier, &redirect_uri).await {
        Ok(credential) => credential,
        Err(error) => {
            respond_to_browser_callback(
                &mut callback,
                "400 Bad Request",
                "Codex sign-in could not be completed. Return to Nakode and retry.",
            )
            .await;
            return Err(error);
        }
    };
    respond_to_browser_callback(
        &mut callback,
        "200 OK",
        "Codex sign-in is complete. Return to Nakode.",
    )
    .await;
    events
        .send(BackendEvent::AuthenticationCompleted {
            kind: "chatgpt_oauth".to_owned(),
            metadata: serde_json::to_value(credential)
                .map_err(|error| safe_adapter_error("credential serialization failed", &error))?,
        })
        .await
        .map_err(|_| "Codex sign-in was cancelled".to_owned())
}

async fn bind_browser_callback(ports: &[u16]) -> Result<BrowserCallbackListeners, String> {
    let mut last_error = None;
    for port in ports {
        match TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port)).await {
            Ok(ipv4) => {
                let bound_port = ipv4
                    .local_addr()
                    .map_err(|error| format!("could not inspect Codex sign-in callback: {error}"))?
                    .port();
                let ipv6 =
                    TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), bound_port))
                        .await
                        .ok();
                return Ok(BrowserCallbackListeners { ipv4, ipv6 });
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(format!(
        "could not start the Codex sign-in callback: {}",
        last_error.map_or_else(
            || "no callback ports configured".to_owned(),
            |error| error.to_string()
        )
    ))
}

fn pkce_verifier() -> String {
    format!(
        "{}{}{}",
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple(),
        Uuid::now_v7().simple()
    )
}

fn browser_authorization_url(
    authorize_url: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(authorize_url)
        .map_err(|error| format!("Codex sign-in URL is invalid: {error}"))?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", OPENAI_CLIENT_ID)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", OAUTH_SCOPES)
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("id_token_add_organizations", "true")
        .append_pair("codex_cli_simplified_flow", "true")
        .append_pair("state", state)
        .append_pair("originator", "codex_cli_rs");
    Ok(url.into())
}

async fn receive_browser_authorization_code(
    listener: &BrowserCallbackListeners,
    expected_state: &str,
    callback_rx: mpsc::Receiver<String>,
) -> Result<(String, TcpStream), String> {
    let mut callback_rx = callback_rx;
    loop {
        let (mut stream, _) = tokio::select! {
            accepted = listener.accept() => accepted
                .map_err(|error| format!("Codex sign-in callback failed: {error}"))?,
            callback = callback_rx.recv() => {
                let callback = callback.ok_or_else(|| "Codex sign-in callback channel closed".to_owned())?;
                relay_callback_to_listener(listener, &callback).await?;
                listener.accept().await
                    .map_err(|error| format!("Codex sign-in callback relay failed: {error}"))?
            }
        };
        match parse_browser_callback(&mut stream, expected_state).await {
            Ok(code) => return Ok((code, stream)),
            Err(message) => {
                respond_to_browser_callback(&mut stream, "400 Bad Request", &message).await;
                if message == "Codex sign-in was not completed. Retry sign-in from Provider Auth." {
                    return Err(message);
                }
            }
        }
    }
}

async fn relay_callback_to_listener(
    listener: &BrowserCallbackListeners,
    callback_url: &str,
) -> Result<(), String> {
    let url = reqwest::Url::parse(callback_url)
        .map_err(|_| "Codex sign-in callback URL was malformed".to_owned())?;
    let target = url.query().map_or_else(
        || url.path().to_owned(),
        |query| format!("{}?{query}", url.path()),
    );
    let address = listener
        .ipv4
        .local_addr()
        .map_err(|error| format!("could not inspect Codex callback listener: {error}"))?;
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| format!("could not relay Codex callback: {error}"))?;
    let request = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("could not relay Codex callback: {error}"))
}
async fn parse_browser_callback(
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
                .map_err(|error| format!("could not read Codex sign-in callback: {error}"))?;
            if bytes == 0 {
                return Err("Codex sign-in callback ended before its headers".to_owned());
            }
            request.extend_from_slice(&buffer[..bytes]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(request);
            }
            if request.len() >= 8_192 {
                return Err("Codex sign-in callback headers were too large".to_owned());
            }
        }
    })
    .await
    .map_err(|_| "Codex sign-in callback connection timed out".to_owned())??;
    let request = std::str::from_utf8(&request)
        .map_err(|_| "Codex sign-in callback was not valid UTF-8".to_owned())?;
    let mut request_line = request
        .lines()
        .next()
        .into_iter()
        .flat_map(str::split_whitespace);
    if request_line.next() != Some("GET") {
        return Err("Codex sign-in callback must use GET".to_owned());
    }
    let target = request_line
        .next()
        .ok_or_else(|| "Codex sign-in callback was malformed".to_owned())?;
    let url = reqwest::Url::parse(&format!("http://localhost{target}"))
        .map_err(|_| "Codex sign-in callback URL was malformed".to_owned())?;
    if url.path() != BROWSER_CALLBACK_PATH {
        return Err("Unexpected Codex sign-in callback path".to_owned());
    }
    let parameters = url.query_pairs().collect::<HashMap<_, _>>();
    if parameters.get("state").map(AsRef::as_ref) != Some(expected_state) {
        return Err("Codex sign-in callback state did not match".to_owned());
    }
    if parameters.contains_key("error") {
        return Err(
            "Codex sign-in was not completed. Retry sign-in from Provider Auth.".to_owned(),
        );
    }
    parameters
        .get("code")
        .filter(|code| !code.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| "Codex sign-in callback did not include a code".to_owned())
}

async fn respond_to_browser_callback(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!("<html><body><p>{message}</p></body></html>");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn exchange_browser_token(
    config: &BackendConfig,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<CodexCredential, String> {
    let response = config
        .client
        .post(&config.auth_urls.token)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", OPENAI_CLIENT_ID),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .map_err(|error| safe_adapter_error("token exchange failed", &error))?;
    if !response.status().is_success() {
        return Err(format!("token exchange returned {}", response.status()));
    }
    let payload = read_bounded_auth_json(response, "invalid token response").await?;
    credential_from_token_payload(&payload, None)
}

async fn authenticate_device_code(
    config: &BackendConfig,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), String> {
    let response = config
        .client
        .post(&config.auth_urls.user_code)
        .json(&json!({"client_id": OPENAI_CLIENT_ID}))
        .send()
        .await
        .map_err(|error| safe_adapter_error("device authorization failed", &error))?;
    if !response.status().is_success() {
        return Err(format!(
            "device authorization returned {}",
            response.status()
        ));
    }
    let payload = read_bounded_auth_json(response, "invalid device authorization").await?;
    let device_id = required_string(&payload, "device_auth_id")?;
    let user_code = required_string(&payload, "user_code")?;
    let interval = payload
        .get("interval")
        .and_then(Value::as_u64)
        .unwrap_or(5)
        .max(1);
    let login_id = Uuid::now_v7().to_string();
    events
        .send(BackendEvent::AuthenticationChallenge {
            login_id,
            verification_url: config.auth_urls.verification.clone(),
            user_code: user_code.to_owned(),
            callback_url: None,
        })
        .await
        .map_err(|_| "backend event receiver closed".to_owned())?;
    for _ in 0..MAX_DEVICE_POLLS {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let poll = config
            .client
            .post(&config.auth_urls.device_token)
            .json(&json!({"device_auth_id": device_id, "user_code": user_code}))
            .send()
            .await
            .map_err(|error| safe_adapter_error("device authorization poll failed", &error))?;
        if matches!(poll.status(), StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) {
            continue;
        }
        if !poll.status().is_success() {
            return Err(format!(
                "device authorization poll returned {}",
                poll.status()
            ));
        }
        let payload = read_bounded_auth_json(poll, "invalid device token response").await?;
        let authorization_code = required_string(&payload, "authorization_code")?;
        let verifier = required_string(&payload, "code_verifier")?;
        let credential = exchange_token(config, authorization_code, verifier).await?;
        events
            .send(BackendEvent::AuthenticationCompleted {
                kind: "chatgpt_oauth".to_owned(),
                metadata: serde_json::to_value(credential).map_err(|error| {
                    safe_adapter_error("credential serialization failed", &error)
                })?,
            })
            .await
            .map_err(|_| "backend event receiver closed".to_owned())?;
        return Ok(());
    }
    Err("device authorization timed out".to_owned())
}

async fn exchange_token(
    config: &BackendConfig,
    code: &str,
    verifier: &str,
) -> Result<CodexCredential, String> {
    let response = config
        .client
        .post(&config.auth_urls.token)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", OPENAI_CLIENT_ID),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", DEVICE_REDIRECT_URI),
        ])
        .send()
        .await
        .map_err(|error| safe_adapter_error("token exchange failed", &error))?;
    if !response.status().is_success() {
        return Err(format!("token exchange returned {}", response.status()));
    }
    let payload = read_bounded_auth_json(response, "invalid token response").await?;
    credential_from_token_payload(&payload, None)
}

async fn refresh_if_needed(
    config: &BackendConfig,
    credential: Option<CodexCredential>,
    events: &mpsc::Sender<BackendEvent>,
) -> Option<CodexCredential> {
    let credential = credential?;
    if credential.expires_at_ms > unix_time_ms().saturating_add(60_000) {
        return Some(credential);
    }
    match refresh_credential(config, &credential).await {
        Ok(refreshed) => {
            let metadata = serde_json::to_value(&refreshed).unwrap_or(Value::Null);
            let _ = events
                .send(BackendEvent::AuthenticationCompleted {
                    kind: "chatgpt_oauth".to_owned(),
                    metadata,
                })
                .await;
            Some(refreshed)
        }
        Err(error) => {
            request_failed(events, BackendOperation::Authenticate, error).await;
            None
        }
    }
}

async fn refresh_credential(
    config: &BackendConfig,
    credential: &CodexCredential,
) -> Result<CodexCredential, String> {
    let response = config
        .client
        .post(&config.auth_urls.token)
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", OPENAI_CLIENT_ID),
            ("refresh_token", credential.refresh_token.as_str()),
        ])
        .send()
        .await
        .map_err(|error| safe_adapter_error("token refresh failed", &error))?;
    if !response.status().is_success() {
        return Err(format!("token refresh returned {}", response.status()));
    }
    let payload = read_bounded_auth_json(response, "invalid token refresh").await?;
    credential_from_token_payload(&payload, Some(&credential.refresh_token))
}

fn credential_from_token_payload(
    payload: &Value,
    previous_refresh_token: Option<&str>,
) -> Result<CodexCredential, String> {
    let access_token = required_string(payload, "access_token")?.to_owned();
    let refresh_token = payload
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .or(previous_refresh_token)
        .ok_or_else(|| "token response omitted refresh_token".to_owned())?
        .to_owned();
    let expires_in = payload
        .get("expires_in")
        .and_then(Value::as_u64)
        .ok_or_else(|| "token response omitted expires_in".to_owned())?;
    let claims = jwt_claims(&access_token)?;
    let account_id = claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| "access token omitted ChatGPT account id".to_owned())?
        .to_owned();
    let email = claims
        .get("https://api.openai.com/profile")
        .and_then(|profile| profile.get("email"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(CodexCredential {
        access_token,
        refresh_token,
        account_id,
        email,
        expires_at_ms: unix_time_ms().saturating_add(expires_in.saturating_mul(1_000)),
    })
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn jwt_claims(token: &str) -> Result<Value, String> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| "access token is not a JWT".to_owned())?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|error| format!("invalid JWT payload: {error}"))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("invalid JWT claims: {error}"))
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("response omitted {field}"))
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[test]
    fn classifies_http_failures_without_leaking_codex_categories() {
        assert_eq!(
            codex_failure_classification(StatusCode::UNAUTHORIZED, "token expired"),
            CodexFailureClassification::Authentication
        );
        assert_eq!(
            codex_failure_classification(StatusCode::UNAUTHORIZED, "quota exhausted"),
            CodexFailureClassification::Authentication
        );
        assert_eq!(
            codex_failure_classification(StatusCode::TOO_MANY_REQUESTS, "slow down"),
            CodexFailureClassification::RateLimit
        );
        assert_eq!(
            codex_failure_classification(StatusCode::FORBIDDEN, "usage quota exhausted"),
            CodexFailureClassification::Quota
        );
        assert_eq!(
            codex_failure_classification(StatusCode::BAD_REQUEST, "model_not_found"),
            CodexFailureClassification::Model
        );
        assert_eq!(
            codex_failure_classification(StatusCode::INTERNAL_SERVER_ERROR, "overloaded"),
            CodexFailureClassification::Transient
        );
    }

    #[test]
    fn preserves_retry_after_from_http_headers() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "42".parse().expect("header"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(42)));
    }
    #[test]
    fn compaction_events_are_not_sent_to_codex_inference() {
        let event = ConversationItem::CompactionEvent {
            id: "compaction-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            reason: crate::backend::CompactionReason::Proactive,
            estimated_tokens_before: 220_000,
            estimated_tokens_after: Some(24_000),
            error: None,
        };

        assert!(conversation_input(&event).is_empty());
    }

    #[test]
    fn codex_receives_bounded_model_tool_output() {
        let input = conversation_input(&ConversationItem::ToolResult {
            call_id: "call-1".to_owned(),
            title: Some("read".to_owned()),
            output: "full transcript output".to_owned(),
            model_output: Some("bounded model output".to_owned()),
            failed: false,
            denied: false,
            denial_reason: None,
            name: None,
            arguments: None,
            audit_kind: None,
            duration_ms: None,
        });

        assert_eq!(input[0]["output"], "bounded model output");
    }

    #[test]
    fn failure_text_sanitization_removes_wrapped_queries_userinfo_and_credentials() {
        let sanitized = sanitize_failure_text(
            "send failed for url (HTTPS://person:password@example.test/models?client_version=secret#fragment)",
        );
        assert!(sanitized.contains("https://example.test/models"));
        assert!(!sanitized.contains("person"));
        assert!(!sanitized.contains("password"));
        assert!(!sanitized.contains("client_version"));
        assert!(!sanitized.contains("secret"));
        assert!(!sanitized.contains("fragment"));

        let non_http = sanitize_failure_text(
            "rpc failed for grpc://person:password@example.test/models?client_version=secret",
        );
        assert!(!non_http.contains("person"));
        assert!(!non_http.contains("password"));
        assert!(!non_http.contains("client_version"));
        assert!(!non_http.contains("secret"));
        assert!(non_http.contains("provider endpoint unavailable"));

        let ipv6 = sanitize_failure_text(
            "send failed for HTTPS://[2001:db8::1]:443/models?signature=secret-value",
        );
        assert!(ipv6.contains("https://[2001:db8::1]/models"));
        assert!(!ipv6.contains("signature"));
        assert!(!ipv6.contains("secret-value"));

        for credential in [
            "Authorization=secret-token",
            "cookie: session=secret",
            r#"{"authorization":"secret-token"}"#,
            r#"{"credential":"secret-token"}"#,
            r#"{"sessionToken":"secret-token"}"#,
            "token: secret-token",
            "access token: secret-token",
            "password=secret-token",
            "secret: secret-token",
        ] {
            assert_eq!(
                sanitize_failure_text(credential),
                "[redacted credential-bearing diagnostic]"
            );
        }
    }

    #[test]
    fn rejects_oversized_sse_events_before_accumulating_them() {
        let mut pending = String::new();
        let oversized = vec![b'x'; MAX_CODEX_SSE_EVENT_BYTES + 1];

        let error = append_codex_sse_chunk(&mut pending, &oversized)
            .expect_err("oversized SSE event must be rejected");

        assert!(pending.is_empty());
        assert!(error.message.contains("safe size limit"));
        assert!(error.message.chars().count() <= 512);
    }

    #[test]
    fn rejects_oversized_authentication_responses_before_accumulating_them() {
        let mut body = Vec::new();
        let oversized = vec![b'x'; MAX_AUTH_RESPONSE_BYTES + 1];

        let error =
            append_bounded_auth_response_chunk(&mut body, &oversized, "invalid token response")
                .expect_err("oversized authentication response must be rejected");

        assert!(body.is_empty());
        assert_eq!(
            error,
            "invalid token response: response exceeded its safe size limit"
        );
    }

    #[test]
    fn sanitizes_authentication_transport_errors() {
        let raw = "request failed for HTTPS://person:password@example.test/token?signature=private";
        let safe = safe_adapter_error("token exchange failed", &raw);

        assert!(safe.contains("https://example.test/token"));
        assert!(!safe.contains("person"));
        assert!(!safe.contains("password"));
        assert!(!safe.contains("signature"));
        assert!(!safe.contains("private"));
        assert!(safe.chars().count() <= 512);
    }

    #[test]
    fn sanitizes_provider_supplied_stream_errors_before_propagation() {
        let error = codex_error_message(
            &json!({
                "type": "response.failed",
                "response": {"error": {"code": "provider_error", "message": "credential=secret-token"}}
            }),
            "Codex response failed",
        );

        assert_eq!(error, "[redacted credential-bearing diagnostic]");
        assert!(!error.contains("secret-token"));
    }

    #[tokio::test]
    async fn start_session_reports_sanitized_model_discovery_transport_failure() {
        let (base_url, server) = drop_response_once().await;
        let credential = serde_json::to_value(test_credential()).expect("serialize credential");
        let config = BackendConfig::native(PathBuf::from("."))
            .with_base_url(base_url.clone())
            .with_credential(Some(credential));
        let mut handle = spawn(config).await.expect("native backend");
        assert!(matches!(
            handle.events.recv().await,
            Some(BackendEvent::Ready(_))
        ));

        handle
            .commands
            .send(BackendCommand::StartSession {
                model: Some("fixture-model".to_owned()),
                instructions: None,
                owner_session_id: Some("logical-session".to_owned()),
                parent_run_id: None,
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
            .expect("start command");
        let event = tokio::time::timeout(Duration::from_secs(2), handle.events.recv())
            .await
            .expect("bounded provider event")
            .expect("provider event");
        let request = server.await.expect("mock server task");
        let BackendEvent::RequestFailed {
            operation,
            code,
            message,
            detail: Some(detail),
        } = event
        else {
            panic!("expected structured request failure, got {event:?}");
        };

        assert_eq!(operation, BackendOperation::StartSession);
        assert_eq!(code, -1);
        assert_eq!(
            message,
            "Provider model discovery could not reach the provider."
        );
        assert_eq!(detail.phase, BackendFailurePhase::ModelDiscovery);
        assert!(matches!(
            detail.classification,
            BackendFailureClassification::Connectivity | BackendFailureClassification::Transport
        ));
        let expected_endpoint = format!("{base_url}/codex/models");
        assert_eq!(
            detail.safe_endpoint.as_deref(),
            Some(expected_endpoint.as_str())
        );
        assert!(detail.source_chain.len() <= 4);
        assert!(
            detail
                .source_chain
                .iter()
                .all(|source| source.chars().count() <= 257)
        );
        assert!(!format!("{detail:?}").contains("access-token"));
        assert!(!format!("{detail:?}").contains("client_version"));
        assert!(request.starts_with("GET /codex/models?client_version="));

        handle
            .commands
            .send(BackendCommand::Shutdown)
            .await
            .expect("shutdown command");
        handle.join().await.expect("backend shutdown");
    }

    #[tokio::test]
    async fn discovers_models_over_the_native_transport() {
        let (base_url, server) = serve_once(
            "application/json",
            r#"{"models":[{"slug":"gpt-native","is_default":true,"context_window":272000,"effective_context_window_percent":95}]}"#,
        )
        .await;
        let config = BackendConfig::native(PathBuf::from(".")).with_base_url(base_url);
        let credential = test_credential();

        let models = discover_models(&config, &credential)
            .await
            .expect("native model discovery");
        let request = server.await.expect("mock server task");

        assert!(request.starts_with("GET /codex/models?client_version="));
        assert!(request.contains("authorization: Bearer access-token"));
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].info.id, "gpt-native");
        assert!(models[0].info.is_default);
        assert_eq!(models[0].context_window, Some(258_400));
    }

    #[tokio::test]
    async fn falls_back_after_an_empty_primary_model_catalogue() {
        let (base_url, server) = serve_sequence(vec![
            (200, "application/json", r#"{"models":[]}"#),
            (
                200,
                "application/json",
                r#"{"models":[{"slug":"gpt-fallback","is_default":true}]}"#,
            ),
        ])
        .await;
        let config = BackendConfig::native(PathBuf::from(".")).with_base_url(base_url);

        let models = discover_models(&config, &test_credential())
            .await
            .expect("fallback model discovery");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /codex/models?client_version="));
        assert!(requests[1].starts_with("GET /models?client_version="));
        assert_eq!(models[0].info.id, "gpt-fallback");
    }

    #[tokio::test]
    async fn preserves_primary_authentication_failure_without_fallback_overwrite() {
        let (base_url, server) = serve_sequence(vec![(401, "application/json", "{}")]).await;
        let config = BackendConfig::native(PathBuf::from(".")).with_base_url(base_url);

        let failure = discover_models(&config, &test_credential())
            .await
            .expect_err("authentication failure");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), 1);
        assert_eq!(
            failure.detail.classification,
            BackendFailureClassification::Authentication
        );
        assert_eq!(failure.detail.http_status, Some(401));
        assert!(
            failure
                .detail
                .safe_endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.ends_with("/codex/models"))
        );
    }

    #[tokio::test]
    async fn streams_native_response_events() {
        let body = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"response-1\"}}\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n",
            "data: [DONE]\n"
        );
        let (base_url, server) = serve_once("text/event-stream", body).await;
        let provider = CodexProvider {
            client: Client::new(),
            base_url,
            credential: test_credential(),
        };
        let (event_tx, mut event_rx) = mpsc::channel(8);

        let output = provider
            .infer_response(test_request(), event_tx, CancellationToken::new())
            .await
            .expect("native response stream");
        let request = server.await.expect("mock server task");

        assert!(request.starts_with("POST /codex/responses HTTP/1.1"));
        assert!(request.contains("chatgpt-account-id: account-1"));
        assert_eq!(output.text, "hello");
        assert_eq!(output.response_id.as_deref(), Some("response-1"));
        assert!(
            matches!(event_rx.recv().await, Some(InferenceEvent::TextDelta(delta)) if delta == "hello")
        );
    }

    #[tokio::test]
    async fn rejects_native_streams_without_a_completion_marker() {
        let body = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n";
        let (base_url, server) = serve_once("text/event-stream", body).await;
        let response = Client::new()
            .get(base_url)
            .send()
            .await
            .expect("mock stream response");
        let (events, _receiver) = mpsc::channel(1);

        let error = parse_codex_sse(response, events, CancellationToken::new())
            .await
            .expect_err("truncated stream must fail");
        server.await.expect("mock server task");

        assert!(error.message.contains("before a completion marker"));
    }

    #[tokio::test]
    async fn rejects_native_streams_ending_mid_event() {
        let (base_url, server) =
            serve_once("text/event-stream", "data: {\"type\":\"response.created\"").await;
        let response = Client::new()
            .get(base_url)
            .send()
            .await
            .expect("mock stream response");
        let (events, _receiver) = mpsc::channel(1);

        let error = parse_codex_sse(response, events, CancellationToken::new())
            .await
            .expect_err("partial event must fail");
        server.await.expect("mock server task");

        assert!(error.message.contains("middle of an event"));
    }

    #[tokio::test]
    async fn distinguishes_summary_updates_from_reasoning_trace_deltas() {
        let (events, mut receiver) = mpsc::channel(2);
        let mut output = InferenceOutput::default();

        apply_codex_event(
            &json!({
                "type": "response.reasoning_summary_text.delta",
                "summary_index": 2,
                "delta": "Planning the implementation",
            }),
            &events,
            &mut output,
        )
        .await
        .expect("reasoning summary event");
        apply_codex_event(
            &json!({
                "type": "response.reasoning_text.delta",
                "delta": "private trace",
            }),
            &events,
            &mut output,
        )
        .await
        .expect("reasoning trace event");

        assert!(matches!(
            receiver.recv().await,
            Some(InferenceEvent::ReasoningSummaryDelta { delta, index: 2 })
                if delta == "Planning the implementation"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(InferenceEvent::ReasoningDelta(delta)) if delta == "private trace"
        ));
        assert_eq!(output.reasoning, "Planning the implementationprivate trace");
    }

    #[tokio::test]
    async fn retries_transient_model_failures_before_streaming_output() {
        let transient_stream = concat!(
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"temporary server error\"}}}\n",
            "data: [DONE]\n"
        );
        let success_stream = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"response-1\"}}\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"recovered\"}\n",
            "data: [DONE]\n"
        );
        let (base_url, server) = serve_sequence(vec![
            (500, "text/plain", "server unavailable"),
            (200, "text/event-stream", transient_stream),
            (200, "text/event-stream", success_stream),
        ])
        .await;
        let provider = CodexProvider {
            client: Client::new(),
            base_url,
            credential: test_credential(),
        };
        let (event_tx, mut event_rx) = mpsc::channel(8);

        let output = provider
            .infer_response(test_request(), event_tx, CancellationToken::new())
            .await
            .expect("transient failures are retried");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), 3);
        assert_eq!(output.text, "recovered");
        assert_eq!(output.retry_count, 2);
        assert!(
            matches!(event_rx.recv().await, Some(InferenceEvent::TextDelta(delta)) if delta == "recovered")
        );
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn does_not_retry_non_transient_model_failures() {
        let (base_url, server) = serve_sequence(vec![(
            400,
            "application/json",
            r#"{"error":"bad request"}"#,
        )])
        .await;
        let provider = CodexProvider {
            client: Client::new(),
            base_url,
            credential: test_credential(),
        };
        let (event_tx, _event_rx) = mpsc::channel(8);

        let error = provider
            .infer_response(test_request(), event_tx, CancellationToken::new())
            .await
            .expect_err("bad requests are terminal");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), 1);
        assert!(error.message.contains("400 Bad Request"));
        assert_eq!(
            error.classification,
            Some(ProviderFailureClassification::Provider)
        );
        assert_eq!(error.retry_count, 0);
    }

    #[tokio::test]
    async fn does_not_retry_after_streaming_visible_output() {
        let partial_stream = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"temporary server error\"}}}\n"
        );
        let (base_url, server) =
            serve_sequence(vec![(200, "text/event-stream", partial_stream)]).await;
        let provider = CodexProvider {
            client: Client::new(),
            base_url,
            credential: test_credential(),
        };
        let (event_tx, mut event_rx) = mpsc::channel(8);

        let error = provider
            .infer_response(test_request(), event_tx, CancellationToken::new())
            .await
            .expect_err("a visible partial stream must not be duplicated");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), 1);
        assert!(error.message.contains("temporary server error"));
        assert_eq!(error.retry_count, 0);
        assert!(
            matches!(event_rx.recv().await, Some(InferenceEvent::TextDelta(delta)) if delta == "partial")
        );
    }

    #[tokio::test]
    async fn exhausted_transient_failures_report_every_retry() {
        let (base_url, server) = serve_sequence(vec![
            (500, "application/json", r#"{"error":"temporary"}"#),
            (500, "application/json", r#"{"error":"temporary"}"#),
            (500, "application/json", r#"{"error":"temporary"}"#),
            (500, "application/json", r#"{"error":"temporary"}"#),
        ])
        .await;
        let provider = CodexProvider {
            client: Client::new(),
            base_url,
            credential: test_credential(),
        };
        let (event_tx, _event_rx) = mpsc::channel(8);

        let error = provider
            .infer_response(test_request(), event_tx, CancellationToken::new())
            .await
            .expect_err("transient failures eventually stop retrying");
        let requests = server.await.expect("mock server task");

        assert_eq!(requests.len(), MAX_INFERENCE_ATTEMPTS);
        assert_eq!(error.retry_count, MAX_INFERENCE_ATTEMPTS - 1);
        assert_eq!(
            error.classification,
            Some(ProviderFailureClassification::Transient)
        );
    }

    #[tokio::test]
    async fn reports_nested_stream_error_details() {
        let (events, _receiver) = mpsc::channel(1);
        let mut output = InferenceOutput::default();
        let error = apply_codex_event(
            &json!({
                "type": "error",
                "error": {
                    "code": "context_length_exceeded",
                    "message": "Your input exceeds the context window of this model."
                }
            }),
            &events,
            &mut output,
        )
        .await
        .expect_err("nested provider error is surfaced");

        assert_eq!(
            error,
            "context_length_exceeded: Your input exceeds the context window of this model."
        );
    }

    #[test]
    fn extracts_namespaced_chatgpt_claims_from_oauth_tokens() {
        let claims = json!({
            "https://api.openai.com/auth": {"chatgpt_account_id": "account-claims"},
            "https://api.openai.com/profile": {"email": "quill@example.test"}
        });
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims JSON"));
        let token = format!("header.{payload}.signature");

        let credential = credential_from_token_payload(
            &json!({
                "access_token": token,
                "refresh_token": "refresh-token",
                "expires_in": 3600
            }),
            None,
        )
        .expect("OAuth credential");

        assert_eq!(credential.account_id, "account-claims");
        assert_eq!(credential.email.as_deref(), Some("quill@example.test"));
    }

    #[tokio::test]
    async fn ordinary_authentication_uses_browser_pkce_and_completes_from_callback() {
        let token_body = r#"{"access_token":"header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjb3VudC1icm93c2VyIn0sImh0dHBzOi8vL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJicm93c2VyQGV4YW1wbGUudGVzdCJ9fQ.signature","refresh_token":"refresh-browser","expires_in":3600}"#;
        let (base_url, token_server) =
            serve_sequence(vec![(200, "application/json", token_body)]).await;
        let callback_port = available_local_port().await;
        let config = BackendConfig::native(PathBuf::from("."))
            .with_auth_urls(&base_url)
            .with_callback_port(callback_port);
        let (events, mut receiver) = mpsc::channel(4);
        let (_callback_tx, callback_rx) = mpsc::channel(1);
        let authentication = tokio::spawn(async move {
            authenticate_inner(
                &config,
                &events,
                crate::backend::ClientContext::Unspecified,
                callback_rx,
            )
            .await
        });

        let (verification_url, state) = match receiver.recv().await {
            Some(BackendEvent::AuthenticationChallenge {
                verification_url,
                login_id,
                user_code,
                ..
            }) => {
                assert!(user_code.is_empty());
                (verification_url, login_id)
            }
            event => panic!("expected browser authentication challenge, got {event:?}"),
        };
        let authorization_url = reqwest::Url::parse(&verification_url).expect("authorization URL");
        assert_eq!(authorization_url.path(), "/oauth/authorize");
        let parameters = authorization_url.query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(
            parameters.get("response_type").map(AsRef::as_ref),
            Some("code")
        );
        assert_eq!(
            parameters.get("state").map(AsRef::as_ref),
            Some(state.as_str())
        );
        assert_eq!(
            parameters.get("code_challenge_method").map(AsRef::as_ref),
            Some("S256")
        );
        assert!(
            parameters
                .get("code_challenge")
                .is_some_and(|value| !value.is_empty())
        );

        let mut callback = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, callback_port))
            .await
            .expect("connect callback");
        callback
            .write_all(
                format!(
                    "GET {BROWSER_CALLBACK_PATH}?code=browser-code&state={state} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("send callback");
        let mut callback_response = String::new();
        callback
            .read_to_string(&mut callback_response)
            .await
            .expect("read callback response");
        assert!(callback_response.starts_with("HTTP/1.1 200 OK"));

        authentication
            .await
            .expect("authentication task")
            .expect("browser authentication succeeds");
        match receiver.recv().await {
            Some(BackendEvent::AuthenticationCompleted { kind, metadata }) => {
                assert_eq!(kind, "chatgpt_oauth");
                assert_eq!(metadata["account_id"], "account-browser");
                assert_eq!(metadata["refresh_token"], "refresh-browser");
            }
            event => panic!("expected authentication completion, got {event:?}"),
        }
        let requests = token_server.await.expect("token server");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("POST /oauth/token "));
        assert!(requests[0].contains("grant_type=authorization_code"));
        assert!(requests[0].contains("code=browser-code"));
        assert!(requests[0].contains("redirect_uri=http%3A%2F%2Flocalhost%3A"));
        assert!(requests[0].contains("code_verifier="));
    }

    #[tokio::test]
    async fn browser_callback_accepts_ipv6_localhost_when_available() {
        let listeners = bind_browser_callback(&[0])
            .await
            .expect("callback listeners");
        if listeners.ipv6.is_none() {
            return;
        }
        let port = listeners.port().expect("callback port");
        let callback = tokio::spawn(async move {
            let (_callback_tx, callback_rx) = mpsc::channel(1);
            receive_browser_authorization_code(&listeners, "expected-state", callback_rx).await
        });
        let mut browser = tokio::net::TcpStream::connect((Ipv6Addr::LOCALHOST, port))
            .await
            .expect("connect IPv6 callback");
        browser
            .write_all(
                b"GET /auth/callback?code=ipv6-code&state=expected-state HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .expect("send IPv6 callback");

        let (code, _) = callback
            .await
            .expect("callback task")
            .expect("IPv6 callback succeeds");
        assert_eq!(code, "ipv6-code");
    }

    #[tokio::test]
    async fn device_code_authentication_requires_explicit_opt_in() {
        let token_body = r#"{"access_token":"header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjb3VudC1icm93c2VyIn0sImh0dHBzOi8vL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJicm93c2VyQGV4YW1wbGUudGVzdCJ9fQ.signature","refresh_token":"refresh-device","expires_in":3600}"#;
        let (base_url, server) = serve_sequence(vec![
            (
                200,
                "application/json",
                r#"{"device_auth_id":"device-id","user_code":"NAKODE-CODE","interval":1}"#,
            ),
            (
                200,
                "application/json",
                r#"{"authorization_code":"device-code","code_verifier":"device-verifier"}"#,
            ),
            (200, "application/json", token_body),
        ])
        .await;
        let config = BackendConfig::native(PathBuf::from("."))
            .with_auth_urls(&base_url)
            .with_device_code_authentication();
        let (events, mut receiver) = mpsc::channel(4);
        let (_callback_tx, callback_rx) = mpsc::channel(1);
        let authentication = tokio::spawn(async move {
            authenticate_inner(
                &config,
                &events,
                crate::backend::ClientContext::Unspecified,
                callback_rx,
            )
            .await
        });

        match receiver.recv().await {
            Some(BackendEvent::AuthenticationChallenge {
                verification_url,
                user_code,
                ..
            }) => {
                assert_eq!(verification_url, format!("{base_url}/codex/device"));
                assert_eq!(user_code, "NAKODE-CODE");
            }
            event => panic!("expected device authentication challenge, got {event:?}"),
        }
        authentication
            .await
            .expect("authentication task")
            .expect("device authentication succeeds");
        assert!(matches!(
            receiver.recv().await,
            Some(BackendEvent::AuthenticationCompleted { kind, metadata })
                if kind == "chatgpt_oauth" && metadata["refresh_token"] == "refresh-device"
        ));
        let requests = server.await.expect("device server");
        assert!(requests[0].starts_with("POST /device/usercode "));
        assert!(requests[1].starts_with("POST /device/token "));
        assert!(requests[2].starts_with("POST /oauth/token "));
        assert!(
            requests[2]
                .contains("redirect_uri=https%3A%2F%2Fauth.openai.com%2Fdeviceauth%2Fcallback")
        );
    }

    #[test]
    fn codex_request_registers_the_configured_dynamic_tools() {
        let mut request = test_request();
        request.tools = crate::tools::ToolRegistry::base()
            .definitions()
            .into_iter()
            .map(Into::into)
            .collect();

        let body = codex_request_body(&request);
        let names = body["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name"))
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "read",
                "read_skill",
                "read_skill_component",
                "write",
                "edit",
                "bash",
                "grep",
                "find",
                "ls",
                "eval",
                "ask",
                "todo"
            ]
        );
        assert!(!names.contains(&"task"));
        assert!(!names.contains(&"hub"));
        let edit = body["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .find(|tool| tool["name"] == "edit")
            .expect("edit tool");
        assert!(edit["parameters"]["properties"]["edits"].is_object());
        let ask = body["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .find(|tool| tool["name"] == "ask")
            .expect("ask tool");
        assert!(ask["parameters"]["properties"]["questions"].is_object());
    }

    #[test]
    fn codex_request_includes_attached_images() {
        let mut request = test_request();
        let ConversationItem::User { attachments, .. } = &mut request.history[0] else {
            panic!("test history should begin with a user message");
        };
        attachments.push(crate::backend::PromptAttachment {
            label: "Image".to_owned(),
            path: None,
            image: Some(crate::backend::PromptImage {
                mime_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            }),
        });

        let body = codex_request_body(&request);
        let content = body["input"][0]["content"]
            .as_array()
            .expect("user content array");

        assert_eq!(content[0]["type"], "input_text");
        assert_eq!(content[1]["type"], "input_image");
        assert_eq!(content[1]["image_url"], "data:image/png;base64,AQID");
    }

    #[test]
    fn codex_requests_disable_provider_storage() {
        let body = codex_request_body(&test_request());

        assert_eq!(body["store"], false);
    }

    #[test]
    fn codex_requests_parallel_tools_and_the_configured_reasoning_effort() {
        let mut request = test_request();
        request.reasoning_effort = Some("low".to_owned());

        let body = codex_request_body(&request);

        assert_eq!(body["parallel_tool_calls"], true);
        assert_eq!(body["reasoning"]["effort"], "low");
    }

    #[test]
    fn session_options_apply_immediately_or_queue_behind_an_active_turn() {
        let session = RuntimeSession::new("gpt-native".to_owned(), String::new());
        let session_id = session.id.clone();
        let mut sessions = HashMap::from([(session_id.clone(), session)]);
        let mut pending = HashMap::new();
        set_session_options(
            &mut sessions,
            &mut pending,
            None,
            None,
            &session_id,
            ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: true,
            },
        )
        .expect("apply options");
        let configured = sessions.get(&session_id).expect("configured session");
        assert_eq!(configured.reasoning_effort.as_deref(), Some("high"));
        assert!(configured.fast_mode);

        let active_session = sessions.remove(&session_id).expect("active session");
        let active = ActiveTurn {
            session_id: session_id.clone(),
            turn_id: "turn-1".to_owned(),
            cancellation: CancellationToken::new(),
        };
        set_session_options(
            &mut sessions,
            &mut pending,
            Some(&active),
            None,
            &session_id,
            ModelOptions {
                reasoning_effort: Some("low".to_owned()),
                fast_mode: false,
            },
        )
        .expect("queue options");
        assert_eq!(
            pending
                .get(&session_id)
                .and_then(|options| options.reasoning_effort.as_deref()),
            Some("low")
        );
        drop(active_session);
    }

    #[test]
    fn fast_mode_requests_priority_service_tier() {
        let mut request = test_request();
        assert!(codex_request_body(&request).get("service_tier").is_none());
        request.fast_mode = true;
        assert_eq!(codex_request_body(&request)["service_tier"], "priority");
    }

    #[tokio::test]
    async fn completed_response_records_provider_token_and_cache_usage() {
        let (events, _receiver) = mpsc::channel(1);
        let mut output = InferenceOutput::default();

        apply_codex_event(
            &json!({
                "type": "response.completed",
                "response": {
                    "usage": {
                        "input_tokens": 1200,
                        "output_tokens": 75,
                        "input_tokens_details": {
                            "cached_tokens": 900,
                            "cache_write_tokens": 100
                        }
                    }
                }
            }),
            &events,
            &mut output,
        )
        .await
        .expect("completed event");

        assert_eq!(output.usage.input_tokens, Some(1_200));
        assert_eq!(output.usage.output_tokens, Some(75));
        assert_eq!(output.usage.cached_input_tokens, Some(900));
        assert_eq!(output.usage.cache_write_tokens, Some(100));
    }

    async fn available_local_port() -> u16 {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("reserve callback port");
        listener.local_addr().expect("callback address").port()
    }

    async fn serve_sequence(
        responses: Vec<(u16, &'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for (status, content_type, body) in responses {
                let (mut socket, _) = listener.accept().await.expect("accept request");
                let mut request = vec![0; 16 * 1024];
                let read = socket.read(&mut request).await.expect("read request");
                request.truncate(read);
                requests.push(String::from_utf8(request).expect("UTF-8 request"));
                let reason = if status >= 500 {
                    "Server Error"
                } else if status >= 400 {
                    "Bad Request"
                } else {
                    "OK"
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
            requests
        });
        (format!("http://{address}"), task)
    }

    async fn drop_response_once() -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0; 16 * 1024];
            let read = socket.read(&mut request).await.expect("read request");
            request.truncate(read);
            String::from_utf8(request).expect("UTF-8 request")
        });
        (format!("http://{address}"), task)
    }

    async fn serve_once(
        content_type: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0; 16 * 1024];
            let read = socket.read(&mut request).await.expect("read request");
            request.truncate(read);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            String::from_utf8(request).expect("UTF-8 request")
        });
        (format!("http://{address}"), task)
    }

    fn test_credential() -> CodexCredential {
        CodexCredential {
            access_token: "access-token".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at_ms: u64::MAX,
            account_id: "account-1".to_owned(),
            email: None,
        }
    }

    fn test_request() -> InferenceRequest {
        InferenceRequest {
            session_id: "session-1".to_owned(),
            model: "gpt-native".to_owned(),
            instructions: "Be direct.".to_owned(),
            history: vec![ConversationItem::User {
                text: "Hi".to_owned(),
                attachments: Vec::new(),
            }],
            tools: Vec::new(),
            reasoning_effort: None,
            fast_mode: false,
        }
    }
}
