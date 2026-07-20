use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    backend::{
        BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
        BackendIdentity, BackendOperation, CODEX_PROVIDER, CapabilitySupport, ModelInfo,
        TurnOutcome,
    },
    runtime::{
        AgentRuntime, ConversationItem, DEFAULT_COMPACTION_THRESHOLD_PERCENT, InferenceEvent,
        InferenceFailure, InferenceFuture, InferenceOutput, InferenceProvider, InferenceRequest,
        RuntimeSession, RuntimeSessionStore, ToolCall,
    },
};

const COMMAND_CAPACITY: usize = 128;
const EVENT_CAPACITY: usize = 1_024;
const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api";
const CODEX_CLIENT_VERSION: &str = "0.144.6";
const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEVICE_USER_CODE_URL: &str = "https://auth.openai.com/api/accounts/deviceauth/usercode";
const DEVICE_TOKEN_URL: &str = "https://auth.openai.com/api/accounts/deviceauth/token";
const DEVICE_AUTH_URL: &str = "https://auth.openai.com/codex/device";
const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";
const MAX_DEVICE_POLLS: usize = 120;
const MAX_INFERENCE_ATTEMPTS: usize = 4;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(8);

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub workspace: PathBuf,
    pub credential: Option<Value>,
    pub base_url: String,
    client: Client,
    auth_urls: AuthUrls,
    session_database: Option<PathBuf>,
    compaction_threshold_percent: usize,
    reasoning_effort: Option<String>,
}

#[derive(Clone, Debug)]
struct AuthUrls {
    user_code: String,
    device_token: String,
    verification: String,
    token: String,
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
                user_code: DEVICE_USER_CODE_URL.to_owned(),
                device_token: DEVICE_TOKEN_URL.to_owned(),
                verification: DEVICE_AUTH_URL.to_owned(),
                token: TOKEN_URL.to_owned(),
            },
            session_database: None,
            compaction_threshold_percent: DEFAULT_COMPACTION_THRESHOLD_PERCENT,
            reasoning_effort: Some("medium".to_owned()),
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
    pub fn with_compaction_threshold_percent(mut self, threshold_percent: usize) -> Self {
        self.compaction_threshold_percent = threshold_percent;
        self
    }

    #[must_use]
    pub fn with_reasoning_effort(mut self, reasoning_effort: impl Into<String>) -> Self {
        self.reasoning_effort = Some(reasoning_effort.into());
        self
    }

    #[cfg(test)]
    fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
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
struct CodexProvider {
    client: Client,
    base_url: String,
    credential: CodexCredential,
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
                Err(error) => return Err(InferenceFailure::new(error.message, attempt)),
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
                let message = format!("Codex request failed: {error}");
                if error.is_connect() || error.is_timeout() || error.is_request() {
                    InferenceAttemptError::transient(message)
                } else {
                    InferenceAttemptError::terminal(message)
                }
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let retry_after = retry_after(response.headers());
            let detail = response.text().await.unwrap_or_default();
            let message = format!("Codex returned {status}: {detail}");
            return Err(InferenceAttemptError {
                message,
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
        AgentRuntime::new(config.workspace.clone(), provider)
            .with_compaction_threshold_percent(config.compaction_threshold_percent)
    });
    let session_store = config
        .session_database
        .clone()
        .map(|database| RuntimeSessionStore::new(database, CODEX_PROVIDER));
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
                    config: &config,
                    credential: credential.as_ref(),
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
                if active.as_ref().is_some_and(|turn| turn.turn_id == completed.turn_id) {
                    active = None;
                }
                if completed.kind == CompletedWorkKind::Turn {
                    let (outcome, error) = match completed.result {
                        Ok(()) => (TurnOutcome::Completed, None),
                        Err(error) if error == "turn interrupted" => (TurnOutcome::Interrupted, None),
                        Err(error) => (TurnOutcome::Failed, Some(error)),
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
    result: Result<(), String>,
    kind: CompletedWorkKind,
}

struct CommandContext<'a> {
    config: &'a BackendConfig,
    credential: Option<&'a CodexCredential>,
    runtime: Option<&'a AgentRuntime>,
    sessions: &'a mut HashMap<String, RuntimeSession>,
    active: &'a mut Option<ActiveTurn>,
    completed: &'a mpsc::Sender<CompletedTurn>,
    events: &'a mpsc::Sender<BackendEvent>,
    session_store: Option<&'a RuntimeSessionStore>,
}

async fn handle_command(command: BackendCommand, context: &mut CommandContext<'_>) {
    match command {
        BackendCommand::BeginAuthentication => {
            tokio::spawn(authenticate(context.config.clone(), context.events.clone()));
        }
        BackendCommand::Reload { .. } => match context.credential {
            Some(credential) => match discover_models(context.config, credential).await {
                Ok(models) => {
                    let _ = context
                        .events
                        .send(BackendEvent::Models(model_infos(models)))
                        .await;
                }
                Err(error) => request_failed(context.events, BackendOperation::Reload, error).await,
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
        } => {
            start_session(model, instructions, context).await;
        }
        BackendCommand::ResumeSession {
            provider_session_id,
        } => {
            resume_session(provider_session_id, context).await;
        }
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => {
            context.sessions.remove(&provider_session_id);
            let _ = context.events.send(BackendEvent::SessionUnsubscribed).await;
        }
        BackendCommand::CompactSession {
            session_id,
            compaction_id,
        } => compact_session(session_id, compaction_id, context).await,
        BackendCommand::SetSessionModel { session_id, model } => {
            if let Some(session) = context.sessions.get_mut(&session_id) {
                if session.model != model {
                    session.context_window = None;
                }
                session.model = model;
                if let Some(store) = context.session_store
                    && let Err(error) = store.save(session)
                {
                    request_failed(context.events, BackendOperation::SetSessionModel, error).await;
                }
            } else {
                request_failed(
                    context.events,
                    BackendOperation::SetSessionModel,
                    "unknown native session",
                )
                .await;
            }
        }
        BackendCommand::StartTurn {
            session_id,
            client_id,
            prompt,
            model,
        } => {
            start_turn(session_id, client_id, prompt, model, context).await;
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
        BackendCommand::ResolveApproval { .. } | BackendCommand::Shutdown => {}
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
        turn_id: compaction_id.clone(),
        cancellation: cancellation.clone(),
    });
    let completed = context.completed.clone();
    let events = context.events.clone();
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let result = runtime
            .force_compact(&mut session, &compaction_id, &events, cancellation)
            .await;
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
    model: Option<String>,
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
            .run_turn(&mut session, &client_id, prompt, &events, cancellation)
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

async fn start_session(
    model: Option<String>,
    instructions: Option<String>,
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
            request_failed(context.events, BackendOperation::StartSession, error).await;
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
        .with_context_window(selected.context_window)
        .with_reasoning_effort(context.config.reasoning_effort.clone());
    let session_id = session.id.clone();
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

async fn resume_session(provider_session_id: String, context: &mut CommandContext<'_>) {
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
        if session.context_window.is_none()
            && let Some(credential) = context.credential
        {
            session.context_window =
                discover_context_window(context.config, credential, &session.model).await;
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
        mcp: CapabilitySupport::Unsupported,
        close_session: CapabilitySupport::Supported,
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

async fn discover_models(
    config: &BackendConfig,
    credential: &CodexCredential,
) -> Result<Vec<DiscoveredModel>, String> {
    for path in ["codex/models", "models"] {
        let url = format!("{}/{path}", config.base_url.trim_end_matches('/'));
        let response = config
            .client
            .get(url)
            .query(&[("client_version", CODEX_CLIENT_VERSION)])
            .bearer_auth(&credential.access_token)
            .header("chatgpt-account-id", &credential.account_id)
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "nakode")
            .header("version", CODEX_CLIENT_VERSION)
            .send()
            .await
            .map_err(|error| format!("model discovery failed: {error}"))?;
        if !response.status().is_success() {
            continue;
        }
        let payload: Value = response
            .json()
            .await
            .map_err(|error| format!("invalid model catalog: {error}"))?;
        let entries = payload
            .get("models")
            .or_else(|| payload.get("data"))
            .and_then(Value::as_array);
        let Some(entries) = entries else { continue };
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
                if id.is_empty() {
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
                    },
                    context_window,
                })
            })
            .collect::<Vec<_>>();
        return Ok(models);
    }
    Err("OpenAI did not expose a usable Codex model catalog".to_owned())
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
    json!({
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
    })
}

fn conversation_input(item: &ConversationItem) -> Vec<Value> {
    match item {
        ConversationItem::User { text } => {
            vec![json!({"role": "user", "content": [{"type": "input_text", "text": text}]})]
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

async fn parse_codex_sse(
    response: reqwest::Response,
    events: mpsc::Sender<InferenceEvent>,
    cancellation: CancellationToken,
) -> Result<InferenceOutput, InferenceAttemptError> {
    let mut stream = response.bytes_stream();
    let mut pending = String::new();
    let mut output = InferenceOutput::default();
    loop {
        let chunk = tokio::select! {
            chunk = stream.next() => chunk,
            () = cancellation.cancelled() => {
                return Err(InferenceAttemptError::terminal("turn interrupted"));
            },
        };
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|error| {
            let message = format!("Codex stream failed: {error}");
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
                continue;
            }
            let event: Value = serde_json::from_str(data).map_err(|error| {
                InferenceAttemptError::terminal(format!("invalid Codex event: {error}"))
            })?;
            if let Err(message) = apply_codex_event(&event, &events, &mut output).await {
                let retryable = output.text.is_empty()
                    && output.reasoning.is_empty()
                    && retryable_stream_message(&message);
                return Err(InferenceAttemptError {
                    message,
                    retryable,
                    retry_after: None,
                });
            }
        }
    }
    Ok(output)
}

async fn apply_codex_event(
    event: &Value,
    events: &mpsc::Sender<InferenceEvent>,
    output: &mut InferenceOutput,
) -> Result<(), String> {
    match event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
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
    Ok(())
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
    match (code, message) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        (_, Some(message)) => message.to_owned(),
        (Some(code), None) => code.to_owned(),
        (None, None) => fallback.to_owned(),
    }
}

async fn authenticate(config: BackendConfig, events: mpsc::Sender<BackendEvent>) {
    if let Err(error) = authenticate_inner(&config, &events).await {
        request_failed(&events, BackendOperation::Authenticate, error).await;
    }
}

async fn authenticate_inner(
    config: &BackendConfig,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(), String> {
    let response = config
        .client
        .post(&config.auth_urls.user_code)
        .json(&json!({"client_id": OPENAI_CLIENT_ID}))
        .send()
        .await
        .map_err(|error| format!("device authorization failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "device authorization returned {}",
            response.status()
        ));
    }
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("invalid device authorization: {error}"))?;
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
            .map_err(|error| format!("device authorization poll failed: {error}"))?;
        if matches!(poll.status(), StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) {
            continue;
        }
        if !poll.status().is_success() {
            return Err(format!(
                "device authorization poll returned {}",
                poll.status()
            ));
        }
        let payload: Value = poll
            .json()
            .await
            .map_err(|error| format!("invalid device token response: {error}"))?;
        let authorization_code = required_string(&payload, "authorization_code")?;
        let verifier = required_string(&payload, "code_verifier")?;
        let credential = exchange_token(config, authorization_code, verifier).await?;
        events
            .send(BackendEvent::AuthenticationCompleted {
                kind: "chatgpt_oauth".to_owned(),
                metadata: serde_json::to_value(credential).map_err(|error| error.to_string())?,
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
        .map_err(|error| format!("token exchange failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("token exchange returned {}", response.status()));
    }
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("invalid token response: {error}"))?;
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
        .map_err(|error| format!("token refresh failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("token refresh returned {}", response.status()));
    }
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("invalid token refresh: {error}"))?;
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
        });

        assert_eq!(input[0]["output"], "bounded model output");
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

    #[test]
    fn codex_request_registers_the_configured_dynamic_tools() {
        let mut request = test_request();
        request.tools = crate::tools::ToolRegistry::base().definitions();

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
                "read", "write", "edit", "bash", "glob", "grep", "eval", "ask", "todo"
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
            }],
            tools: Vec::new(),
            reasoning_effort: None,
        }
    }
}
