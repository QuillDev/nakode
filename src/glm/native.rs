use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    backend::{
        BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
        BackendIdentity, BackendOperation, CapabilitySupport, GLM_PROVIDER, ModelInfo, TurnOutcome,
        request_failed,
    },
    runtime::{
        AgentRuntime, ConversationItem, DEFAULT_COMPACTION_THRESHOLD_PERCENT, InferenceEvent,
        InferenceFailure, InferenceFuture, InferenceOutput, InferenceProvider, InferenceRequest,
        RuntimeSession, RuntimeSessionStore, ToolCall, TurnError, set_session_model,
    },
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const GLM_BASE_URL: &str = "https://api.z.ai/api/coding/paas/v4";
const MAX_INFERENCE_ATTEMPTS: usize = 4;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(8);
const DEFAULT_CONTEXT_WINDOW: usize = 204_800;

#[derive(Clone)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
    pub base_url: String,
    client: Client,
    session_database: Option<PathBuf>,
    compaction_threshold_percent: usize,
    web_config: Option<Arc<std::sync::RwLock<crate::web::WebConfig>>>,
    vision_config: Option<Arc<std::sync::RwLock<crate::vision::VisionConfig>>>,
    vision_service: Option<crate::vision::SharedVisionService>,
    memory_service: Option<crate::memory::SharedMemoryService>,
    native_delegation: Option<mpsc::Sender<crate::backend::NativeDelegationRequest>>,
}

impl BackendConfig {
    #[must_use]
    pub fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
            base_url: GLM_BASE_URL.to_owned(),
            client: Client::new(),
            session_database: None,
            compaction_threshold_percent: DEFAULT_COMPACTION_THRESHOLD_PERCENT,
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
    pub fn with_memory(mut self, service: crate::memory::SharedMemoryService) -> Self {
        self.memory_service = Some(service);
        self
    }

    #[must_use]
    pub fn with_native_delegation(
        mut self,
        requests: mpsc::Sender<crate::backend::NativeDelegationRequest>,
    ) -> Self {
        self.native_delegation = Some(requests);
        self
    }

    #[must_use]
    pub fn with_compaction_threshold_percent(mut self, threshold_percent: usize) -> Self {
        self.compaction_threshold_percent = threshold_percent;
        self
    }
}

#[derive(Debug)]
struct InferenceAttemptError {
    message: String,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl InferenceAttemptError {
    fn terminal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            retry_after: None,
        }
    }

    fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            retry_after: None,
        }
    }
}

#[derive(Clone)]
struct GlmProvider {
    client: Client,
    base_url: String,
    api_key: String,
}

#[derive(Clone, Debug)]
struct DiscoveredModel {
    info: ModelInfo,
    context_window: Option<usize>,
}

impl InferenceProvider for GlmProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move { self.infer_response(request, events, cancellation).await })
    }
}

impl GlmProvider {
    async fn infer_response(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> Result<InferenceOutput, InferenceFailure> {
        let mut last_error = InferenceAttemptError::terminal("GLM inference failed");
        for attempt in 0..MAX_INFERENCE_ATTEMPTS {
            if cancellation.is_cancelled() {
                return Err(InferenceFailure::new("turn interrupted", attempt));
            }
            match self
                .infer_attempt(&request, events.clone(), cancellation.clone())
                .await
            {
                Ok(mut output) => {
                    output.retry_count = attempt;
                    return Ok(output);
                }
                Err(error) => {
                    let retryable = error.retryable;
                    let retry_after = error.retry_after;
                    last_error = error;
                    if !retryable || attempt + 1 == MAX_INFERENCE_ATTEMPTS {
                        break;
                    }
                    let delay = retry_after
                        .unwrap_or_else(|| retry_delay(attempt))
                        .min(MAX_RETRY_DELAY);
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancellation.cancelled() => {
                            return Err(InferenceFailure::new("turn interrupted", attempt));
                        }
                    }
                }
            }
        }
        Err(InferenceFailure::new(
            last_error.message,
            MAX_INFERENCE_ATTEMPTS - 1,
        ))
    }

    async fn infer_attempt(
        &self,
        request: &InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> Result<InferenceOutput, InferenceAttemptError> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .header("accept", "text/event-stream")
            .json(&glm_request_body(request))
            .send()
            .await
            .map_err(|error| {
                InferenceAttemptError::transient(format!("GLM request failed: {error}"))
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let retry_after = retry_after(response.headers());
            let detail = response.text().await.unwrap_or_default();
            return Err(InferenceAttemptError {
                message: format!("GLM returned {status}: {detail}"),
                retryable: retryable_status(status),
                retry_after,
            });
        }
        parse_glm_sse(response, events, cancellation).await
    }
}

/// Starts the in-process z.ai GLM Coding Plan adapter.
///
/// # Errors
///
/// Returns an error when the stored API-key credential has an invalid shape.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let api_key = config
        .credential
        .as_ref()
        .map(|credential| {
            credential
                .get("api_key")
                .and_then(Value::as_str)
                .filter(|api_key| !api_key.trim().is_empty())
                .map(|api_key| api_key.trim().to_owned())
                .ok_or_else(|| BackendError::InvalidCredential {
                    provider: GLM_PROVIDER.to_owned(),
                    detail: "missing API key".to_owned(),
                })
        })
        .transpose()?;
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let task = tokio::spawn(run_supervisor(config, api_key, command_rx, event_tx));
    Ok(BackendHandle::new(command_tx, event_rx, task))
}

async fn run_supervisor(
    config: BackendConfig,
    api_key: Option<String>,
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let _ = events
        .send(BackendEvent::Ready(BackendIdentity {
            provider: GLM_PROVIDER.to_owned(),
            display_name: "GLM Coding Plan".to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            capabilities: native_capabilities(),
        }))
        .await;
    let provider = api_key.clone().map(|api_key| {
        Arc::new(GlmProvider {
            client: config.client.clone(),
            base_url: config.base_url.clone(),
            api_key,
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
        .map(|database| RuntimeSessionStore::new(database, GLM_PROVIDER));
    let mut sessions = HashMap::<String, RuntimeSession>::new();
    let mut active: Option<ActiveTurn> = None;
    let (completed_tx, mut completed_rx) = mpsc::channel::<CompletedTurn>(8);
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command, BackendCommand::Shutdown) {
                    if let Some(active) = active.take() { active.cancellation.cancel(); }
                    break;
                }
                let mut context = CommandContext {
                    api_key: api_key.as_deref(),
                    runtime: runtime.as_ref(),
                    sessions: &mut sessions,
                    active: &mut active,
                    completed: &completed_tx,
                    events: &events,
                    session_store: session_store.as_ref(),
                };
                handle_command(command, &mut context).await;
            }
            completed = completed_rx.recv() => {
                let Some(completed) = completed else { break };
                if let Some(store) = &session_store
                    && let Err(error) = store.save(&completed.session)
                {
                    let operation = match completed.kind {
                        CompletedWorkKind::Turn => BackendOperation::StartTurn,
                        CompletedWorkKind::Compaction => BackendOperation::CompactSession,
                    };
                    request_failed(&events, operation, error).await;
                }
                sessions.insert(completed.session.id.clone(), completed.session);
                if active.as_ref().is_some_and(|turn| turn.turn_id == completed.turn_id) { active = None; }
                if completed.kind == CompletedWorkKind::Turn {
                    let (outcome, error) = match completed.result {
                        Ok(()) => (TurnOutcome::Completed, None),
                        Err(TurnError::Interrupted) => (TurnOutcome::Interrupted, None),
                        Err(error) => (TurnOutcome::Failed, Some(error.to_string())),
                    };
                    let _ = events.send(BackendEvent::TurnCompleted {
                        turn_id: completed.turn_id,
                        outcome,
                        error,
                    }).await;
                }
            }
        }
    }
}

struct ActiveTurn {
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
    api_key: Option<&'a str>,
    runtime: Option<&'a AgentRuntime>,
    sessions: &'a mut HashMap<String, RuntimeSession>,
    active: &'a mut Option<ActiveTurn>,
    completed: &'a mpsc::Sender<CompletedTurn>,
    events: &'a mpsc::Sender<BackendEvent>,
    session_store: Option<&'a RuntimeSessionStore>,
}

#[allow(clippy::too_many_lines)]
async fn handle_command(command: BackendCommand, context: &mut CommandContext<'_>) {
    match command {
        BackendCommand::BeginAuthentication => api_key_auth_required(context.events).await,
        BackendCommand::Reload { .. } => match context.api_key {
            Some(_) => {
                let models = discover_models();
                let _ = context
                    .events
                    .send(BackendEvent::Models(model_infos(models)))
                    .await;
            }
            None => {
                request_failed(
                    context.events,
                    BackendOperation::Reload,
                    "GLM is not authenticated",
                )
                .await;
            }
        },
        BackendCommand::StartSession {
            model,
            instructions,
            external_tools,
            replace_builtin_tools,
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
        BackendCommand::StartTurn {
            provider_session_id,
            client_id,
            prompt,
            attachments,
            model,
        } => {
            start_turn(
                provider_session_id,
                client_id,
                prompt,
                attachments,
                model,
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
        BackendCommand::ResolveApproval { .. }
        | BackendCommand::SetSessionOptions { .. }
        | BackendCommand::Shutdown => {}
    }
}

async fn api_key_auth_required(events: &mpsc::Sender<BackendEvent>) {
    request_failed(
        events,
        BackendOperation::Authenticate,
        "GLM uses an API key configured in provider settings",
    )
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
        if session.context_window.is_none() && context.api_key.is_some() {
            session.context_window = discover_context_window(&session.model);
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

async fn compact_session(
    session_id: String,
    compaction_id: String,
    context: &mut CommandContext<'_>,
) {
    let Some(runtime) = context.runtime else {
        request_failed(
            context.events,
            BackendOperation::CompactSession,
            "GLM API key is not configured",
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
    context: &mut CommandContext<'_>,
) {
    let Some(runtime) = context.runtime else {
        request_failed(
            context.events,
            BackendOperation::StartTurn,
            "GLM is not authenticated",
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
    if let Some(model) = model {
        if session.model != model {
            session.context_window = None;
        }
        session.model = model;
    }
    if session.context_window.is_none() {
        session.context_window = discover_context_window(&session.model);
    }
    let cancellation = CancellationToken::new();
    *context.active = Some(ActiveTurn {
        turn_id: client_id.clone(),
        cancellation: cancellation.clone(),
    });
    let _ = context
        .events
        .send(BackendEvent::TurnAccepted {
            turn_id: client_id.clone(),
        })
        .await;
    let runtime = runtime.clone();
    let completed = context.completed.clone();
    let events = context.events.clone();
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
    allowed_builtin_tools: Option<Vec<String>>,
    max_turns: Option<u32>,
    finalization_reserve_turns: u32,
    timeout_seconds: Option<u32>,
    context: &mut CommandContext<'_>,
) {
    let Some(_api_key) = context.api_key else {
        request_failed(
            context.events,
            BackendOperation::StartSession,
            "GLM is not authenticated",
        )
        .await;
        return;
    };
    let models = discover_models();
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
            "GLM returned no usable models",
        )
        .await;
        return;
    };
    let selected_id = selected.info.id.clone();
    let session = RuntimeSession::new(selected_id.clone(), instructions.unwrap_or_default())
        .with_enabled_skill_ids(enabled_skill_ids)
        .with_provider(GLM_PROVIDER)
        .with_owner(owner_session_id, parent_run_id)
        .with_context_window(selected.context_window);
    let session_id = session.id.clone();
    if let Some(runtime) = context.runtime
        && let Err(error) = runtime
            .configure_external_tools(
                &session_id,
                external_tools,
                replace_builtin_tools,
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

fn discover_models() -> Vec<DiscoveredModel> {
    // Coding Plan exposes a deliberately limited model set. Keep this catalog
    // independent of the general z.ai model endpoint so a Coding Plan key never
    // accidentally selects a model billed outside the subscription.
    ["glm-5.2", "glm-5-turbo", "glm-4.7"]
        .into_iter()
        .map(|id| DiscoveredModel {
            info: ModelInfo {
                provider: GLM_PROVIDER.to_owned(),
                id: id.to_owned(),
                is_default: id == "glm-5.2",
                capabilities: crate::backend::ModelCapabilities::default(),
            },
            context_window: context_window_for_model(id),
        })
        .collect()
}

fn model_infos(models: Vec<DiscoveredModel>) -> Vec<ModelInfo> {
    models.into_iter().map(|model| model.info).collect()
}

fn discover_context_window(model: &str) -> Option<usize> {
    context_window_for_model(model)
}

fn context_window_for_model(model: &str) -> Option<usize> {
    if model.eq_ignore_ascii_case("glm-5.2[1m]") {
        return Some(1_000_000);
    }
    is_coding_plan_model(model).then_some(DEFAULT_CONTEXT_WINDOW)
}

fn is_coding_plan_model(model: &str) -> bool {
    ["glm-5.2", "glm-5-turbo", "glm-4.7"]
        .iter()
        .any(|candidate| model.eq_ignore_ascii_case(candidate))
}

fn glm_request_body(request: &InferenceRequest) -> Value {
    let mut messages = Vec::new();
    if !request.instructions.trim().is_empty() {
        messages.push(json!({"role": "system", "content": request.instructions}));
    }
    messages.extend(request.history.iter().filter_map(conversation_message));
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            json!({"type": "function", "function": {
                "name": tool.name, "description": tool.description, "parameters": tool.parameters
            }})
        })
        .collect::<Vec<_>>();
    let mut body = json!({"model": request.model, "messages": messages, "stream": true,
        "stream_options": {"include_usage": true}});
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
        body["tool_choice"] = Value::String("auto".to_owned());
        body["parallel_tool_calls"] = Value::Bool(true);
    }
    body["thinking"] = json!({"type": "enabled"});
    if request.model.eq_ignore_ascii_case("glm-5.2") {
        body["reasoning_effort"] =
            Value::String(glm_reasoning_effort(request.reasoning_effort.as_deref()));
    }
    body
}

fn glm_reasoning_effort(effort: Option<&str>) -> String {
    match effort.unwrap_or("high").to_ascii_lowercase().as_str() {
        "low" => "low",
        "xhigh" | "max" => "max",
        _ => "high",
    }
    .to_owned()
}

fn conversation_message(item: &ConversationItem) -> Option<Value> {
    match item {
        ConversationItem::User { text, attachments } => {
            if attachments
                .iter()
                .all(|attachment| attachment.image.is_none())
            {
                return Some(json!({"role": "user", "content": text}));
            }
            let mut content = vec![json!({"type": "text", "text": text})];
            content.extend(attachments.iter().filter_map(|attachment| {
                let image = attachment.image.as_ref()?;
                Some(json!({"type": "image_url", "image_url": {"url": format!("data:{};base64,{}", image.mime_type, STANDARD.encode(&image.data))}}))
            }));
            Some(json!({"role": "user", "content": content}))
        }
        ConversationItem::Assistant {
            text, tool_calls, ..
        } => {
            let calls = tool_calls.iter().map(|call| json!({"id": call.id, "type": "function", "function": {
                "name": call.name, "arguments": serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_owned())
            }})).collect::<Vec<_>>();
            let mut message = json!({"role": "assistant", "content": text});
            if !calls.is_empty() {
                message["tool_calls"] = Value::Array(calls);
            }
            Some(message)
        }
        ConversationItem::ToolResult {
            call_id,
            output,
            model_output,
            ..
        } => Some(json!({
            "role": "tool", "tool_call_id": call_id, "content": model_output.as_ref().unwrap_or(output)
        })),
        ConversationItem::Compaction { summary } => Some(
            json!({"role": "user", "content": format!("Context checkpoint from earlier work:\n\n{summary}")}),
        ),
        ConversationItem::CompactionEvent { .. } => None,
    }
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

#[derive(Default)]
struct ToolCallAccumulator {
    id: String,
    name: String,
    arguments: String,
}

async fn parse_glm_sse(
    response: reqwest::Response,
    events: mpsc::Sender<InferenceEvent>,
    cancellation: CancellationToken,
) -> Result<InferenceOutput, InferenceAttemptError> {
    let mut stream = response.bytes_stream();
    let mut pending = String::new();
    let mut output = InferenceOutput::default();
    let mut tool_calls = Vec::<ToolCallAccumulator>::new();
    let mut completed = false;
    loop {
        let chunk = tokio::select! { chunk = stream.next() => chunk, () = cancellation.cancelled() => return Err(InferenceAttemptError::terminal("turn interrupted")), };
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|error| {
            let message = format!("GLM stream failed: {error}");
            if output.text.is_empty() && output.reasoning.is_empty() {
                InferenceAttemptError::transient(message)
            } else {
                InferenceAttemptError::terminal(message)
            }
        })?;
        pending.push_str(&String::from_utf8_lossy(&chunk));
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
                InferenceAttemptError::terminal(format!("invalid GLM event: {error}"))
            })?;
            apply_glm_event(&event, &events, &mut output, &mut tool_calls)
                .await
                .map_err(InferenceAttemptError::terminal)?;
        }
    }
    if !pending.trim().is_empty() {
        return Err(InferenceAttemptError::terminal(
            "GLM stream ended in the middle of an event",
        ));
    }
    if !completed {
        return Err(InferenceAttemptError::terminal(
            "GLM stream ended before a completion marker",
        ));
    }
    output.tool_calls = tool_calls
        .into_iter()
        .filter(|call| !call.name.is_empty())
        .map(|call| ToolCall {
            id: if call.id.is_empty() {
                Uuid::now_v7().to_string()
            } else {
                call.id
            },
            name: call.name,
            arguments: serde_json::from_str(&call.arguments).unwrap_or_else(|_| json!({})),
        })
        .collect();
    Ok(output)
}

async fn apply_glm_event(
    event: &Value,
    events: &mpsc::Sender<InferenceEvent>,
    output: &mut InferenceOutput,
    tool_calls: &mut Vec<ToolCallAccumulator>,
) -> Result<(), String> {
    if let Some(message) = event.pointer("/error/message").and_then(Value::as_str) {
        return Err(format!("GLM stream error: {message}"));
    }
    if output.response_id.is_none() {
        output.response_id = event.get("id").and_then(Value::as_str).map(str::to_owned);
    }
    if let Some(usage) = event.get("usage") {
        output.usage.input_tokens = usage.get("prompt_tokens").and_then(Value::as_u64);
        output.usage.output_tokens = usage.get("completion_tokens").and_then(Value::as_u64);
        output.usage.cached_input_tokens = usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .and_then(Value::as_u64);
    }
    let Some(choices) = event.get("choices").and_then(Value::as_array) else {
        return Ok(());
    };
    for choice in choices {
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        if let Some(reasoning) = delta
            .get("reasoning_content")
            .or_else(|| delta.get("reasoning"))
            .or_else(|| delta.get("thinking"))
            .and_then(Value::as_str)
        {
            output.reasoning.push_str(reasoning);
            events
                .send(InferenceEvent::ReasoningDelta(reasoning.to_owned()))
                .await
                .map_err(|_| "inference event receiver closed".to_owned())?;
        }
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            output.text.push_str(text);
            events
                .send(InferenceEvent::TextDelta(text.to_owned()))
                .await
                .map_err(|_| "inference event receiver closed".to_owned())?;
        }
        for call in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let index = call
                .get("index")
                .and_then(Value::as_u64)
                .and_then(|index| usize::try_from(index).ok())
                .unwrap_or_default();
            if tool_calls.len() <= index {
                tool_calls.resize_with(index + 1, ToolCallAccumulator::default);
            }
            let accumulator = &mut tool_calls[index];
            if let Some(id) = call.get("id").and_then(Value::as_str) {
                id.clone_into(&mut accumulator.id);
            }
            if let Some(name) = call.pointer("/function/name").and_then(Value::as_str) {
                accumulator.name.push_str(name);
            }
            if let Some(arguments) = call.pointer("/function/arguments").and_then(Value::as_str) {
                accumulator.arguments.push_str(arguments);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        GlmProvider, ToolCallAccumulator, apply_glm_event, context_window_for_model,
        discover_models, glm_reasoning_effort, glm_request_body,
    };
    use crate::runtime::{
        ConversationItem, InferenceOutput, InferenceProvider, InferenceRequest, ToolCall,
    };

    fn request() -> InferenceRequest {
        InferenceRequest {
            session_id: "session-1".to_owned(),
            model: "glm-5.2".to_owned(),
            instructions: "Be direct.".to_owned(),
            history: vec![
                ConversationItem::User {
                    text: "Read it".to_owned(),
                    attachments: Vec::new(),
                },
                ConversationItem::Assistant {
                    text: String::new(),
                    reasoning: String::new(),
                    tool_calls: vec![ToolCall {
                        id: "call-1".to_owned(),
                        name: "read".to_owned(),
                        arguments: json!({"path":"README.md"}),
                    }],
                    provider_id: None,
                    model_id: None,
                    signature: None,
                    provider_state: Vec::new(),
                },
                ConversationItem::ToolResult {
                    call_id: "call-1".to_owned(),
                    title: None,
                    output: "full output".to_owned(),
                    model_output: Some("bounded output".to_owned()),
                    failed: false,
                    denied: false,
                    denial_reason: None,
                    name: None,
                    arguments: None,
                    audit_kind: None,
                    duration_ms: None,
                },
            ],
            tools: Vec::new(),
            reasoning_effort: Some("medium".to_owned()),
            fast_mode: false,
        }
    }

    #[test]
    fn request_uses_chat_messages_and_fixed_sampling_parameters() {
        let body = glm_request_body(&request());
        assert_eq!(body["model"], "glm-5.2");
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][2]["tool_calls"][0]["id"], "call-1");
        assert_eq!(body["messages"][3]["content"], "bounded output");
        assert!(body.get("temperature").is_none());
        assert!(body.get("top_p").is_none());
        assert_eq!(glm_reasoning_effort(Some("xhigh")), "max");
    }

    #[tokio::test]
    async fn stream_events_normalize_text_reasoning_tools_and_usage() {
        let (events, mut event_rx) = mpsc::channel(8);
        let mut output = InferenceOutput::default();
        let mut calls = Vec::<ToolCallAccumulator>::new();
        apply_glm_event(&json!({"id":"chat-1","choices":[{"delta":{"reasoning_content":"think","content":"done",
            "tool_calls":[{"index":0,"id":"call-1","function":{"name":"read","arguments":"{\"path\":"}}]}}]}),
            &events, &mut output, &mut calls).await.expect("first event");
        apply_glm_event(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"README.md\"}"}}]}}],
            "usage":{"prompt_tokens":12,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":3}}}),
            &events, &mut output, &mut calls).await.expect("second event");
        assert_eq!(output.response_id.as_deref(), Some("chat-1"));
        assert_eq!(output.text, "done");
        assert_eq!(output.reasoning, "think");
        assert_eq!(calls[0].arguments, "{\"path\":\"README.md\"}");
        assert_eq!(output.usage.input_tokens, Some(12));
        assert_eq!(output.usage.cached_input_tokens, Some(3));
        assert!(
            matches!(event_rx.recv().await, Some(crate::runtime::InferenceEvent::ReasoningDelta(text)) if text == "think")
        );
        assert!(
            matches!(event_rx.recv().await, Some(crate::runtime::InferenceEvent::TextDelta(text)) if text == "done")
        );
    }

    #[tokio::test]
    async fn provider_sends_bearer_authenticated_chat_request_and_parses_sse() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let address = listener.local_addr().expect("server address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request_bytes = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).await.expect("read request");
                if read == 0 {
                    break;
                }
                request_bytes.extend_from_slice(&buffer[..read]);
                let request = String::from_utf8_lossy(&request_bytes);
                let Some(headers_end) = request.find("\r\n\r\n") else {
                    continue;
                };
                let content_length = request[..headers_end]
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or_default();
                if request_bytes.len() >= headers_end + 4 + content_length {
                    break;
                }
            }
            let request = String::from_utf8(request_bytes).expect("UTF-8 request");
            assert!(request.starts_with("POST /chat/completions HTTP/1.1"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer secret")
            );
            assert!(request.contains("\"model\":\"glm-5.2\""));
            let payload = concat!(
                "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
                "data: [DONE]\n\n"
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                payload.len(),
                payload
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        let provider = GlmProvider {
            client: reqwest::Client::new(),
            base_url: format!("http://{address}"),
            api_key: "secret".to_owned(),
        };
        let (events, mut event_rx) = mpsc::channel(8);
        let output = provider
            .infer(request(), events, CancellationToken::new())
            .await
            .expect("GLM inference");
        server.await.expect("server task");
        assert_eq!(output.text, "hello");
        assert!(
            matches!(event_rx.recv().await, Some(crate::runtime::InferenceEvent::TextDelta(text)) if text == "hello")
        );
    }

    #[test]
    fn coding_plan_catalog_only_advertises_supported_models() {
        let models = discover_models();
        assert_eq!(
            models
                .iter()
                .map(|model| model.info.id.as_str())
                .collect::<Vec<_>>(),
            ["glm-5.2", "glm-5-turbo", "glm-4.7"]
        );
        assert!(models[0].info.is_default);
        assert!(models[1..].iter().all(|model| !model.info.is_default));
    }

    #[test]
    fn known_context_windows_are_used_when_catalog_omits_metadata() {
        assert_eq!(context_window_for_model("glm-5-turbo"), Some(204_800));
        assert_eq!(context_window_for_model("glm-5.2"), Some(204_800));
        assert_eq!(context_window_for_model("glm-5.2[1m]"), Some(1_000_000));
    }
}
