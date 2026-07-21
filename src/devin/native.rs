use std::{
    collections::HashMap,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use futures_util::StreamExt;
use prost::Message;
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{client, protocol};
use crate::{
    backend::{
        BackendCapabilities, BackendCommand, BackendError, BackendEvent, BackendHandle,
        BackendIdentity, BackendOperation, CapabilitySupport, DEVIN_PROVIDER, ModelInfo,
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
const DEVIN_BASE_URL: &str = "https://server.codeium.com";
const IDE_VERSION: &str = "3.2.23";
const EXTENSION_VERSION: &str = "1.48.2";
const SESSION_TOKEN_PREFIX: &str = "devin-session-token$";
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const CONNECT_COMPRESSED: u8 = 0x01;
const CONNECT_END_STREAM: u8 = 0x02;

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
}

impl BackendConfig {
    #[must_use]
    pub fn native(workspace: PathBuf) -> Self {
        Self {
            workspace,
            credential: None,
            base_url: DEVIN_BASE_URL.to_owned(),
            client: Client::new(),
            session_database: None,
            compaction_threshold_percent: DEFAULT_COMPACTION_THRESHOLD_PERCENT,
            web_config: None,
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
    pub fn with_compaction_threshold_percent(mut self, threshold_percent: usize) -> Self {
        self.compaction_threshold_percent = threshold_percent;
        self
    }
}

#[derive(Clone)]
struct DevinProvider {
    client: Client,
    base_url: String,
    api_key: String,
}

#[derive(Clone, Debug)]
struct DiscoveredModel {
    info: ModelInfo,
    context_window: Option<usize>,
}

impl InferenceProvider for DevinProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move { self.infer_response(request, events, cancellation).await })
    }
}

impl DevinProvider {
    async fn infer_response(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> Result<InferenceOutput, InferenceFailure> {
        let auth = get_user_jwt(&self.client, &self.base_url, &self.api_key).await?;
        let base_url = if auth.custom_api_server_url.trim().is_empty() {
            self.base_url.as_str()
        } else {
            auth.custom_api_server_url.trim_end_matches('/')
        };
        let message = build_chat_request(&request, &self.api_key, &auth.user_jwt)?;
        let frame = encode_connect_frame(&message)?;
        let url = format!("{base_url}/exa.api_server_pb.ApiServerService/GetChatMessage");
        let response = self
            .client
            .post(url)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .header("connect-content-encoding", "gzip")
            .header("connect-accept-encoding", "gzip")
            .header("accept-encoding", "identity")
            .body(frame)
            .send()
            .await
            .map_err(|error| format!("Devin request failed: {error}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            return Err(format!("Devin returned {status}: {detail}").into());
        }
        parse_connect_stream(response, events, cancellation)
            .await
            .map_err(Into::into)
    }
}

/// Starts the in-process Devin adapter.
///
/// # Errors
///
/// Returns an error when the stored credential has an invalid shape.
pub async fn spawn(config: BackendConfig) -> Result<BackendHandle, BackendError> {
    let api_key = config
        .credential
        .as_ref()
        .map(|credential| {
            credential
                .get("token")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| BackendError::InvalidCredential {
                    provider: DEVIN_PROVIDER.to_owned(),
                    detail: "missing token".to_owned(),
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
            provider: DEVIN_PROVIDER.to_owned(),
            display_name: "Devin".to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            capabilities: native_capabilities(),
        }))
        .await;
    let provider = api_key.clone().map(|api_key| {
        Arc::new(DevinProvider {
            client: config.client.clone(),
            base_url: config.base_url.clone(),
            api_key,
        }) as Arc<dyn InferenceProvider>
    });
    let runtime = provider.map(|provider| {
        let mut runtime = AgentRuntime::new(config.workspace.clone(), provider)
            .with_compaction_threshold_percent(config.compaction_threshold_percent);
        if let Some(web_config) = &config.web_config {
            runtime = runtime.with_web_config(Arc::clone(web_config));
        }
        if let Some(vision_config) = &config.vision_config {
            runtime = runtime.with_vision(
                Arc::clone(vision_config),
                config.vision_service.clone(),
                false,
            );
        }
        runtime
    });
    let session_store = config
        .session_database
        .clone()
        .map(|database| RuntimeSessionStore::new(database, DEVIN_PROVIDER));
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
    api_key: Option<&'a str>,
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
            tokio::spawn(client::authenticate_native(context.events.clone()));
        }
        BackendCommand::Reload { .. } => match context.api_key {
            Some(api_key) => match discover_models(context.config, api_key).await {
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
                    "Devin is not authenticated",
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
            attachments,
            model,
        } => {
            start_turn(session_id, client_id, prompt, attachments, model, context).await;
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
            && let Some(api_key) = context.api_key
        {
            session.context_window =
                discover_context_window(context.config, api_key, &session.model).await;
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
            "Devin API key is not configured",
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
    attachments: Vec<crate::backend::PromptAttachment>,
    model: Option<String>,
    context: &mut CommandContext<'_>,
) {
    let Some(runtime) = context.runtime else {
        request_failed(
            context.events,
            BackendOperation::StartTurn,
            "Devin is not authenticated",
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
                .api_key
                .expect("authenticated runtime has an API key"),
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

async fn start_session(
    model: Option<String>,
    instructions: Option<String>,
    context: &mut CommandContext<'_>,
) {
    let Some(api_key) = context.api_key else {
        request_failed(
            context.events,
            BackendOperation::StartSession,
            "Devin is not authenticated",
        )
        .await;
        return;
    };
    let models = match discover_models(context.config, api_key).await {
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
            "Devin returned no usable models",
        )
        .await;
        return;
    };
    let selected_id = selected.info.id.clone();
    let session = RuntimeSession::new(selected_id.clone(), instructions.unwrap_or_default())
        .with_context_window(selected.context_window);
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
    api_key: &str,
) -> Result<Vec<DiscoveredModel>, String> {
    let request = protocol::GetCliModelConfigsRequest {
        metadata: Some(metadata(api_key, "")),
    };
    let response: protocol::GetCliModelConfigsResponse = unary_proto(
        &config.client,
        &config.base_url,
        "/exa.api_server_pb.ApiServerService/GetCliModelConfigs",
        &request,
    )
    .await?;
    let default_id = response
        .default_override_model_config
        .as_ref()
        .map(|config| config.model_uid.as_str());
    Ok(response
        .client_model_configs
        .into_iter()
        .filter(|config| !config.disabled && !config.model_uid.trim().is_empty())
        .map(|config| DiscoveredModel {
            context_window: usize::try_from(config.max_tokens)
                .ok()
                .filter(|tokens| *tokens > 0),
            info: ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                is_default: default_id == Some(config.model_uid.as_str()),
                id: config.model_uid,
            },
        })
        .collect())
}

fn model_infos(models: Vec<DiscoveredModel>) -> Vec<ModelInfo> {
    models.into_iter().map(|model| model.info).collect()
}

async fn discover_context_window(
    config: &BackendConfig,
    api_key: &str,
    model: &str,
) -> Option<usize> {
    discover_models(config, api_key)
        .await
        .ok()?
        .into_iter()
        .find(|candidate| candidate.info.id == model)
        .and_then(|candidate| candidate.context_window)
}

async fn get_user_jwt(
    client: &Client,
    base_url: &str,
    api_key: &str,
) -> Result<protocol::GetUserJwtResponse, String> {
    unary_proto(
        client,
        base_url,
        "/exa.auth_pb.AuthService/GetUserJwt",
        &protocol::GetUserJwtRequest {
            metadata: Some(metadata(api_key, "")),
        },
    )
    .await
}

async fn unary_proto<Request, Response>(
    client: &Client,
    base_url: &str,
    path: &str,
    request: &Request,
) -> Result<Response, String>
where
    Request: Message,
    Response: Message + Default,
{
    let response = client
        .post(format!("{}{path}", base_url.trim_end_matches('/')))
        .header("content-type", "application/proto")
        .header("connect-protocol-version", "1")
        .header("accept", "*/*")
        .body(request.encode_to_vec())
        .send()
        .await
        .map_err(|error| format!("Devin RPC failed: {error}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await.unwrap_or_default();
        return Err(format!("Devin RPC returned {status}: {detail}"));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read Devin RPC: {error}"))?;
    if let Ok(response) = Response::decode(bytes.as_ref()) {
        return Ok(response);
    }
    let mut decompressor = GzDecoder::new(bytes.as_ref());
    let mut payload = Vec::new();
    decompressor
        .read_to_end(&mut payload)
        .map_err(|error| format!("failed to decompress Devin RPC: {error}"))?;
    Response::decode(payload.as_slice())
        .map_err(|error| format!("failed to decode Devin RPC: {error}"))
}

fn metadata(api_key: &str, user_jwt: &str) -> protocol::Metadata {
    let api_key = if api_key.starts_with(SESSION_TOKEN_PREFIX) {
        api_key.to_owned()
    } else {
        format!("{SESSION_TOKEN_PREFIX}{api_key}")
    };
    protocol::Metadata {
        ide_name: "windsurf".to_owned(),
        ide_version: IDE_VERSION.to_owned(),
        extension_name: "windsurf".to_owned(),
        extension_version: EXTENSION_VERSION.to_owned(),
        api_key,
        locale: "en".to_owned(),
        user_jwt: user_jwt.to_owned(),
    }
}

fn build_chat_request(
    request: &InferenceRequest,
    api_key: &str,
    user_jwt: &str,
) -> Result<protocol::GetChatMessageRequest, String> {
    let prompts = request
        .history
        .iter()
        .enumerate()
        .flat_map(|(index, item)| conversation_prompts(&request.session_id, index, item))
        .collect();
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            Ok(protocol::ChatToolDefinition {
                name: tool.name.to_owned(),
                description: tool.description.to_owned(),
                json_schema_string: serde_json::to_string(&tool.parameters)
                    .map_err(|error| error.to_string())?,
                strict: false,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(protocol::GetChatMessageRequest {
        metadata: Some(metadata(api_key, user_jwt)),
        prompt: request.instructions.clone(),
        chat_message_prompts: prompts,
        chat_model_uid: request.model.clone(),
        request_type: 5,
        configuration: Some(protocol::CompletionConfiguration {
            num_completions: 1,
            max_tokens: 64_000,
            max_newlines: 200,
            temperature: 0.4,
            first_temperature: 0.4,
            top_k: 50,
            top_p: 1.0,
            stop_patterns: vec![
                "<|user|>".to_owned(),
                "<|bot|>".to_owned(),
                "<|end_of_turn|>".to_owned(),
            ],
            fim_eot_prob_threshold: 1.0,
        }),
        tools,
        disable_parallel_tool_calls: false,
        tool_choice: Some(protocol::ChatToolChoice {
            choice: Some(protocol::chat_tool_choice::Choice::OptionName(
                "auto".to_owned(),
            )),
        }),
        system_prompt_cache_options: Some(protocol::PromptCacheOptions { r#type: 1 }),
        cascade_id: request.session_id.clone(),
        planner_mode: 1,
        execution_id: Uuid::now_v7().to_string(),
    })
}

fn conversation_prompts(
    session_id: &str,
    index: usize,
    item: &ConversationItem,
) -> Vec<protocol::ChatMessagePrompt> {
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{session_id}:{index}").as_bytes(),
    )
    .to_string();
    match item {
        ConversationItem::User { text, .. } => vec![chat_prompt(message_id, 1, text.clone())],
        ConversationItem::Assistant {
            text,
            reasoning,
            tool_calls,
            signature,
            ..
        } => vec![protocol::ChatMessagePrompt {
            message_id,
            source: 2,
            prompt: text.clone(),
            thinking: reasoning.clone(),
            tool_calls: tool_calls
                .iter()
                .map(|call| protocol::ChatToolCall {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    arguments_json: serde_json::to_string(&call.arguments)
                        .unwrap_or_else(|_| "{}".to_owned()),
                })
                .collect(),
            tool_call_id: String::new(),
            tool_result_is_error: false,
            signature: signature.clone().unwrap_or_default(),
        }],
        ConversationItem::ToolResult {
            call_id,
            output,
            model_output,
            failed,
            ..
        } => vec![protocol::ChatMessagePrompt {
            message_id,
            source: 4,
            prompt: model_output.as_ref().unwrap_or(output).clone(),
            tool_calls: Vec::new(),
            tool_call_id: call_id.clone(),
            tool_result_is_error: *failed,
            thinking: String::new(),
            signature: String::new(),
        }],
        ConversationItem::Compaction { summary } => vec![chat_prompt(
            message_id,
            1,
            format!("Context checkpoint from earlier work:\n\n{summary}"),
        )],
        ConversationItem::CompactionEvent { .. } => Vec::new(),
    }
}

fn chat_prompt(message_id: String, source: i32, prompt: String) -> protocol::ChatMessagePrompt {
    protocol::ChatMessagePrompt {
        message_id,
        source,
        prompt,
        tool_calls: Vec::new(),
        tool_call_id: String::new(),
        tool_result_is_error: false,
        thinking: String::new(),
        signature: String::new(),
    }
}

fn encode_connect_frame(message: &protocol::GetChatMessageRequest) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&message.encode_to_vec())
        .map_err(|error| error.to_string())?;
    let payload = encoder.finish().map_err(|error| error.to_string())?;
    let length =
        u32::try_from(payload.len()).map_err(|_| "Devin request frame is too large".to_owned())?;
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(CONNECT_COMPRESSED);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

async fn parse_connect_stream(
    response: reqwest::Response,
    events: mpsc::Sender<InferenceEvent>,
    cancellation: CancellationToken,
) -> Result<InferenceOutput, String> {
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let mut output = InferenceOutput::default();
    let mut tool_arguments = HashMap::<String, String>::new();
    let mut tool_names = HashMap::<String, String>::new();
    let mut active_tool_id = None;
    loop {
        let chunk = tokio::select! {
            chunk = stream.next() => chunk,
            () = cancellation.cancelled() => return Err("turn interrupted".to_owned()),
        };
        let Some(chunk) = chunk else { break };
        pending.extend_from_slice(&chunk.map_err(|error| format!("Devin stream failed: {error}"))?);
        while pending.len() >= 5 {
            let flags = pending[0];
            let length =
                u32::from_be_bytes([pending[1], pending[2], pending[3], pending[4]]) as usize;
            if length > MAX_FRAME_BYTES {
                return Err(format!("Devin frame exceeds {MAX_FRAME_BYTES} bytes"));
            }
            if pending.len() < length + 5 {
                break;
            }
            let payload = pending[5..length + 5].to_vec();
            pending.drain(..length + 5);
            if flags & CONNECT_END_STREAM != 0 {
                let trailer = decode_frame_payload(flags, &payload)?;
                let text = String::from_utf8_lossy(&trailer);
                if text.contains("\"error\"") {
                    return Err(format!("Devin stream trailer: {text}"));
                }
                continue;
            }
            let decoded = decode_frame_payload(flags, &payload)?;
            let message = protocol::GetChatMessageResponse::decode(decoded.as_slice())
                .map_err(|error| format!("invalid Devin frame: {error}"))?;
            if output.response_id.is_none() && !message.message_id.is_empty() {
                output.response_id = Some(message.message_id);
            }
            if !message.delta_signature.is_empty() {
                output.signature = Some(message.delta_signature);
            }
            if !message.delta_thinking.is_empty() {
                output.reasoning.push_str(&message.delta_thinking);
                events
                    .send(InferenceEvent::ReasoningDelta(message.delta_thinking))
                    .await
                    .map_err(|_| "inference event receiver closed".to_owned())?;
            }
            if !message.delta_text.is_empty() {
                output.text.push_str(&message.delta_text);
                events
                    .send(InferenceEvent::TextDelta(message.delta_text))
                    .await
                    .map_err(|_| "inference event receiver closed".to_owned())?;
            }
            for tool in message.delta_tool_calls {
                let tool_id = if tool.id.is_empty() {
                    let Some(active_id) = active_tool_id.clone() else {
                        continue;
                    };
                    active_id
                } else {
                    active_tool_id = Some(tool.id.clone());
                    tool.id
                };
                if !tool.name.is_empty() {
                    tool_names.insert(tool_id.clone(), tool.name);
                }
                let current = tool_arguments.entry(tool_id).or_default();
                if tool.arguments_json.starts_with(current.as_str()) {
                    *current = tool.arguments_json;
                } else {
                    current.push_str(&tool.arguments_json);
                }
            }
        }
    }
    output.tool_calls = tool_arguments
        .into_iter()
        .map(|(id, arguments)| ToolCall {
            name: tool_names.remove(&id).unwrap_or_default(),
            id,
            arguments: serde_json::from_str(&arguments).unwrap_or_else(|_| json!({})),
        })
        .collect();
    Ok(output)
}

fn decode_frame_payload(flags: u8, payload: &[u8]) -> Result<Vec<u8>, String> {
    if flags & CONNECT_COMPRESSED == 0 {
        return Ok(payload.to_vec());
    }
    let mut decompressor = GzDecoder::new(payload);
    let mut uncompressed = Vec::new();
    decompressor
        .read_to_end(&mut uncompressed)
        .map_err(|error| format!("invalid gzip frame: {error}"))?;
    Ok(uncompressed)
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[test]
    fn compaction_events_are_not_sent_to_devin_inference() {
        let event = ConversationItem::CompactionEvent {
            id: "compaction-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            reason: crate::backend::CompactionReason::Proactive,
            estimated_tokens_before: 220_000,
            estimated_tokens_after: Some(24_000),
            error: None,
        };

        assert!(conversation_prompts("session-1", 0, &event).is_empty());
    }

    #[tokio::test]
    async fn discovers_devin_models_over_direct_protobuf_rpc() {
        let payload = protocol::GetCliModelConfigsResponse {
            client_model_configs: vec![protocol::ClientModelConfig {
                model_uid: "swe-native".to_owned(),
                disabled: false,
                max_tokens: 200_000,
                ..Default::default()
            }],
            default_override_model_config: Some(protocol::DefaultOverrideModelConfig {
                model_uid: "swe-native".to_owned(),
            }),
        }
        .encode_to_vec();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0; 16 * 1024];
            let read = socket.read(&mut request).await.expect("read request");
            request.truncate(read);
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                payload.len()
            );
            socket
                .write_all(header.as_bytes())
                .await
                .expect("write headers");
            socket.write_all(&payload).await.expect("write protobuf");
            String::from_utf8(request).expect("UTF-8 request")
        });
        let mut config = BackendConfig::native(PathBuf::from("."));
        config.base_url = format!("http://{address}");

        let models = discover_models(&config, "session-token")
            .await
            .expect("native Devin model discovery");
        let request = server.await.expect("mock server task");

        assert!(request.starts_with("POST /exa.api_server_pb.ApiServerService/GetCliModelConfigs"));
        assert!(request.contains("content-type: application/proto"));
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].info.id, "swe-native");
        assert!(models[0].info.is_default);
        assert_eq!(models[0].context_window, Some(200_000));
    }

    #[test]
    fn connect_frames_are_gzipped_and_length_delimited() {
        let mut inference_request = test_request();
        inference_request.tools = crate::tools::ToolRegistry::base().definitions();
        let request = build_chat_request(&inference_request, "token", "jwt")
            .expect("build Devin chat request");
        let frame = encode_connect_frame(&request).expect("encode Connect frame");
        let length = usize::try_from(u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]))
            .expect("frame length fits usize");
        let decoded = decode_frame_payload(frame[0], &frame[5..]).expect("decode Connect frame");
        let restored = protocol::GetChatMessageRequest::decode(decoded.as_slice())
            .expect("decode Devin request");

        assert_eq!(frame[0], CONNECT_COMPRESSED);
        assert_eq!(length, frame.len() - 5);
        assert_eq!(restored.chat_model_uid, "swe-native");
        assert!(!restored.disable_parallel_tool_calls);
        let tool_names = restored
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            tool_names,
            [
                "read", "write", "edit", "bash", "glob", "grep", "eval", "ask", "todo"
            ]
        );
        assert!(!tool_names.contains(&"task"));
        assert!(!tool_names.contains(&"hub"));
        let todo_schema: Value = serde_json::from_str(
            &restored
                .tools
                .iter()
                .find(|tool| tool.name == "todo")
                .expect("todo tool")
                .json_schema_string,
        )
        .expect("todo schema JSON");
        assert!(todo_schema["properties"]["op"].is_object());
        assert!(todo_schema["properties"]["list"].is_object());
        assert_eq!(
            restored.metadata.expect("metadata").api_key,
            "devin-session-token$token"
        );
    }

    #[test]
    fn devin_receives_bounded_model_tool_output() {
        let prompts = conversation_prompts(
            "session-1",
            0,
            &ConversationItem::ToolResult {
                call_id: "call-1".to_owned(),
                title: Some("read".to_owned()),
                output: "full transcript output".to_owned(),
                model_output: Some("bounded model output".to_owned()),
                failed: false,
            },
        );

        assert_eq!(prompts[0].prompt, "bounded model output");
    }

    fn test_request() -> InferenceRequest {
        InferenceRequest {
            session_id: "session-1".to_owned(),
            model: "swe-native".to_owned(),
            instructions: "Explore.".to_owned(),
            history: vec![ConversationItem::User {
                text: "Inspect.".to_owned(),
                attachments: Vec::new(),
            }],
            tools: Vec::new(),
            reasoning_effort: None,
        }
    }
}
