//! Server-owned provider and process supervision.
//!
//! This module contains no terminal, renderer, editor, or control-socket
//! dependencies. The native server actor will become the sole owner of these
//! resources. Frontends reach this owner only through the service protocol.

use std::{
    collections::{HashMap, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, mpsc};

use nakode_protocol::{
    BridgeContinuationDisposition, Command, ErrorCode, Query, QueryResult, ServiceCapabilities,
    ServiceCapability, ServiceError, Snapshot,
};
use nakode_server::{ServerEndpoint, ServerRequests};
use thiserror::Error;

use crate::{
    agent::{AgentCatalog, AgentCatalogError},
    backend::{BackendCommand, BackendError, BackendEvent, BackendHandle, NativeDelegationRequest},
    claude, codex,
    config::Config,
    credential::{
        Credential, CredentialError, CredentialStore, SecretValue, SqliteCredentialStore,
    },
    cursor, devin, glm, kimi,
    personality::{PromptAddenda, PromptAddendaError},
    service::ServiceEngine,
    session::{
        ProviderRecord, SessionError, SessionRecord, SessionRepository, SqliteSessionRepository,
    },
    shell::{ShellEvent, ShellProcesses},
    skill::{SkillCatalog, SkillCatalogError},
    state::{AgentBrowserStatus, DomainState, Effect},
};

use super::{BridgeStateCheckpoint, ServerCore};

const SESSION_BACKEND_STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum NativeRuntimeError {
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    Agents(#[from] AgentCatalogError),
    #[error(transparent)]
    Skills(#[from] SkillCatalogError),
    #[error(transparent)]
    PromptAddenda(#[from] PromptAddendaError),
    #[error(transparent)]
    Soul(#[from] crate::soul::SoulError),
    #[error("failed to locate the running Nakode executable: {0}")]
    CurrentExecutable(#[source] io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum SessionBackendError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error("backend command channel closed for {provider} session {session_id}")]
    CommandChannelClosed {
        session_id: nakode_protocol::SessionId,
        provider: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendSource {
    ProviderControl(String),
    Primary {
        session_id: nakode_protocol::SessionId,
        provider: String,
    },
    Subagent(String),
}

fn shell_event_id(event: &ShellEvent) -> &str {
    match event {
        ShellEvent::Output { id, .. }
        | ShellEvent::Finished { id, .. }
        | ShellEvent::Failed { id, .. } => id,
    }
}

#[derive(Clone)]
pub(crate) struct PersistenceServices {
    pub(crate) database: PathBuf,
    pub(crate) sessions: Arc<dyn SessionRepository>,
    pub(crate) credentials: Arc<dyn CredentialStore>,
}

pub(crate) struct EffectExecutor {
    pub(crate) backends: BackendRegistry,
    pub(crate) persistence: PersistenceServices,
    pub(crate) mcp: crate::mcp::McpClient,
    pub(crate) shell_processes: ShellProcesses,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectOrigin {
    ClientCommand,
    ProviderControl,
    PrimarySession,
    Subagent,
}

#[derive(Clone)]
pub(crate) struct NativeServerHandle {
    endpoint: ServerEndpoint,
    shutdown: mpsc::Sender<()>,
    quiesce: mpsc::Sender<QuiesceRequest>,
}

struct QuiesceRequest {
    respond: tokio::sync::oneshot::Sender<Result<(), String>>,
}

struct PendingMcpCall {
    source: BackendSource,
    session_id: nakode_protocol::SessionId,
    run_id: Option<String>,
    server_id: String,
    remote_name: String,
    arguments_json: String,
    started_at_ms: u64,
    started: Instant,
    cancellation: tokio_util::sync::CancellationToken,
}

struct McpCallCompletion {
    call_id: String,
    result: Result<String, crate::mcp::McpError>,
}

struct PendingMcpDiscovery {
    request_id: u64,
    server_id: String,
    cancellation: tokio_util::sync::CancellationToken,
}

struct McpDiscoveryCompletion {
    request_id: u64,
    server: crate::mcp::McpServerRecord,
    result: Result<crate::mcp::DiscoveryResult, crate::mcp::McpError>,
}

enum BridgeMutationRollback {
    /// Session creation/deletion, inbound continuation, and lifecycle reopening can also change
    /// logical session state by resuming a durable bridge inbox item.
    Full(Box<ServerCore>),
    /// Binding and delivery commands only change bridge metadata and idempotency state.
    BridgeOnly(Box<BridgeStateCheckpoint>),
}

impl BridgeMutationRollback {
    fn capture(request: &nakode_server::ServerRequest, core: &ServerCore) -> Option<Self> {
        let nakode_server::ServerRequest::Command { command, .. } = request else {
            return None;
        };
        match command {
            Command::CreateSession {
                bridge: Some(_), ..
            }
            | Command::ContinueSessionFromBridge { .. }
            | Command::DeleteSession { .. }
            | Command::SetSessionBridgeLifecycle { .. }
            | Command::SetWorkspaceBridgeLifecycle { .. } => {
                Some(Self::Full(Box::new(core.clone())))
            }
            Command::BindSessionBridgeThread { .. }
            | Command::ClearSessionBridgeThread { .. }
            | Command::PrepareBridgeDelivery { .. }
            | Command::CompleteBridgeDeliveryPart { .. }
            | Command::FinalizeBridgeDelivery { .. }
            | Command::SetBridgeLiveMessage { .. } => {
                Some(Self::BridgeOnly(Box::new(core.bridge_state_checkpoint())))
            }
            _ => None,
        }
    }

    fn restore(self, core: &mut ServerCore) {
        match self {
            Self::Full(previous) => *core = *previous,
            Self::BridgeOnly(checkpoint) => core.restore_bridge_state(*checkpoint),
        }
    }
}

pub(crate) struct NativeServerRuntime {
    core: ServerCore,
    endpoint: ServerEndpoint,
    requests: ServerRequests,
    effects: EffectExecutor,
    shell_owners: HashMap<String, nakode_protocol::SessionId>,
    shutdown: mpsc::Receiver<()>,
    quiesce: mpsc::Receiver<QuiesceRequest>,
    accepting_work: bool,
    pending_bridge_acknowledgements: HashMap<nakode_protocol::SessionId, String>,
    delegation_requests: mpsc::Receiver<NativeDelegationRequest>,
    native_cancellation_tx: mpsc::Sender<u64>,
    native_cancellations: mpsc::Receiver<u64>,
    pending_native_delegations: HashMap<u64, PendingNativeDelegation>,
    next_native_delegation_request: u64,
    mcp_call_tx: mpsc::Sender<McpCallCompletion>,
    mcp_calls: mpsc::Receiver<McpCallCompletion>,
    pending_mcp_calls: HashMap<String, PendingMcpCall>,
    mcp_discovery_tx: mpsc::Sender<McpDiscoveryCompletion>,
    mcp_discoveries: mpsc::Receiver<McpDiscoveryCompletion>,
    pending_mcp_discoveries: HashMap<String, PendingMcpDiscovery>,
    next_mcp_discovery_request: u64,
    skill_catalogue: SkillCatalog,
    skill_preferences: HashMap<String, Vec<crate::skill::SkillPreference>>,
}

pub(crate) struct PreparedRuntime {
    pub(crate) engine: ServiceEngine,
    pub(crate) effects: EffectExecutor,
    pub(crate) providers: Vec<ProviderRecord>,
    pub(crate) sessions: Vec<SessionRecord>,
    pub(crate) session_inventory_complete: bool,
    pub(crate) delegation_requests: mpsc::Receiver<NativeDelegationRequest>,
    pub(crate) soul_store: crate::soul::SoulStore,
    pub(crate) skill_catalogue: SkillCatalog,
    pub(crate) skill_preferences: HashMap<String, Vec<crate::skill::SkillPreference>>,
}

struct PendingNativeDelegation {
    session_id: nakode_protocol::SessionId,
    run_id: String,
    respond: tokio::sync::oneshot::Sender<Result<String, String>>,
    cancellation_task: tokio::task::JoinHandle<()>,
}

impl PreparedRuntime {
    pub(crate) fn into_actor(self) -> (NativeServerRuntime, NativeServerHandle) {
        let (mut runtime, handle) = NativeServerRuntime::from_parts_with_skill_authority(
            self.engine,
            self.providers,
            self.sessions,
            self.effects,
            self.delegation_requests,
            self.skill_catalogue,
            self.skill_preferences,
        );
        runtime
            .core
            .set_session_inventory_complete(self.session_inventory_complete);
        runtime.core.install_soul_store(self.soul_store);
        (runtime, handle)
    }
}

pub(crate) async fn prepare_runtime(
    config: &Config,
) -> Result<PreparedRuntime, NativeRuntimeError> {
    let nakode_executable =
        std::env::current_exe().map_err(NativeRuntimeError::CurrentExecutable)?;
    let session_repository = Arc::new(SqliteSessionRepository::open_default()?);
    let session_database = session_repository.database_path().to_path_buf();
    let credential_store = Arc::new(SqliteCredentialStore::open(&session_database)?);
    let mut providers = session_repository.list_providers()?;
    enable_e2e_fixture_provider(&mut providers);
    let (provider_credentials, credential_failures) =
        load_provider_credentials(&providers, credential_store.as_ref());
    let (delegation_tx, delegation_requests) = mpsc::channel(128);
    let mut backends = BackendRegistry::spawn(
        config,
        &providers,
        BackendRegistrySpawn {
            session_database: session_database.clone(),
            provider_credentials,
            web_config: shared_web_config(session_repository.as_ref())?,
            memory_config: shared_memory_config(session_repository.as_ref())?,
            vision_config: shared_vision_config(session_repository.as_ref())?,
            native_delegation: delegation_tx,
        },
    )
    .await;
    backends.failures.extend(credential_failures);

    let agents = AgentCatalog::load(&config.agents)?;
    let skills = SkillCatalog::load(&config.workspace)?;
    let skill_preferences = session_repository
        .list_all_skill_preferences()?
        .into_iter()
        .fold(
            HashMap::<String, Vec<_>>::new(),
            |mut profiles, preference| {
                profiles
                    .entry(preference.profile_id.clone())
                    .or_default()
                    .push(preference);
                profiles
            },
        );
    let prompt_addenda =
        PromptAddenda::load(config.personalities.as_deref(), config.soul.as_deref())?;
    let soul_store = crate::soul::SoulStore::configured(config.soul.as_deref())?;
    let mut state = initial_state(config, &providers, &backends, agents, skills.clone());
    state.install_prompt_addenda(prompt_addenda);
    let terminal_image_mode = session_repository.load_terminal_image_mode()?;
    state.install_terminal_image_mode(terminal_image_mode);
    state.install_invocation_telemetry_enabled(
        session_repository.load_invocation_telemetry_enabled()?,
    );
    state.set_nakode_executable(&nakode_executable);
    load_cached_provider_configuration(&mut state, &mut backends, session_repository.as_ref())
        .await;
    let sessions = session_repository.list_recent_all()?;
    let session_inventory_complete = true;
    let persistence = PersistenceServices {
        database: session_database,
        sessions: session_repository,
        credentials: credential_store,
    };
    Ok(PreparedRuntime {
        engine: ServiceEngine::new(state),
        effects: EffectExecutor::new(backends, persistence),
        providers,
        sessions,
        session_inventory_complete,
        delegation_requests,
        soul_store,
        skill_catalogue: skills,
        skill_preferences,
    })
}

impl NativeServerRuntime {
    #[cfg(test)]
    pub(crate) fn from_parts(
        engine: ServiceEngine,
        providers: Vec<ProviderRecord>,
        sessions: Vec<SessionRecord>,
        effects: EffectExecutor,
        delegation_requests: mpsc::Receiver<NativeDelegationRequest>,
    ) -> (Self, NativeServerHandle) {
        let skill_catalogue = engine.state().skill_catalogue();
        Self::from_parts_with_skill_authority(
            engine,
            providers,
            sessions,
            effects,
            delegation_requests,
            skill_catalogue,
            HashMap::new(),
        )
    }

    pub(crate) fn from_parts_with_skill_authority(
        engine: ServiceEngine,
        providers: Vec<ProviderRecord>,
        sessions: Vec<SessionRecord>,
        effects: EffectExecutor,
        delegation_requests: mpsc::Receiver<NativeDelegationRequest>,
        skill_catalogue: SkillCatalog,
        skill_preferences: HashMap<String, Vec<crate::skill::SkillPreference>>,
    ) -> (Self, NativeServerHandle) {
        let capabilities = native_service_capabilities();
        let (endpoint, requests) =
            ServerEndpoint::channel(env!("CARGO_PKG_VERSION"), capabilities, 256);
        let (shutdown_tx, shutdown) = mpsc::channel(1);
        let (quiesce_tx, quiesce) = mpsc::channel(1);
        let (native_cancellation_tx, native_cancellations) = mpsc::channel(128);
        let (mcp_call_tx, mcp_calls) = mpsc::channel(128);
        let (mcp_discovery_tx, mcp_discoveries) = mpsc::channel(32);
        let handle = NativeServerHandle {
            endpoint: endpoint.clone(),
            shutdown: shutdown_tx,
            quiesce: quiesce_tx,
        };
        let mut core = ServerCore::new(engine, providers, sessions);
        match effects.persistence.sessions.list_session_bridges_all() {
            Ok(bridges) => core.install_session_bridges(bridges),
            Err(error) => core
                .engine_mut()
                .state_mut()
                .session_store_failed(format!("failed to restore orchestrator bridges: {error}")),
        }
        let workspace =
            crate::state::projection::workspace_id(&core.engine().state().workspace).to_string();
        if let Ok(servers) = effects.persistence.sessions.list_mcp_servers(&workspace) {
            let servers = servers
                .into_iter()
                .map(|mut server| {
                    server.credential_kind = effects
                        .persistence
                        .credentials
                        .get_mcp(&workspace, &server.id)
                        .ok()
                        .flatten()
                        .map(|credential| credential.kind);
                    let normalized = crate::mcp::normalize_builtin_server(server);
                    let _ = effects.persistence.sessions.save_mcp_server(&normalized);
                    normalized
                })
                .collect();
            core.install_mcp_servers(servers);
        }
        (
            Self {
                core,
                endpoint,
                requests,
                effects,
                shell_owners: HashMap::new(),
                shutdown,
                quiesce,
                accepting_work: true,
                pending_bridge_acknowledgements: HashMap::new(),
                delegation_requests,
                native_cancellation_tx,
                native_cancellations,
                pending_native_delegations: HashMap::new(),
                next_native_delegation_request: 1,
                mcp_call_tx,
                mcp_calls,
                pending_mcp_calls: HashMap::new(),
                mcp_discovery_tx,
                mcp_discoveries,
                pending_mcp_discoveries: HashMap::new(),
                next_mcp_discovery_request: 1,
                skill_catalogue,
                skill_preferences,
            },
            handle,
        )
    }

    pub(crate) async fn run(mut self) {
        self.refresh_builtin_tool_availability();
        let mut backend_open = true;
        let mut shell_open = true;
        let mut shutdown_open = true;
        let mut provider_sync = tokio::time::interval(SHARED_PROVIDER_SYNC_INTERVAL);
        provider_sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        provider_sync.tick().await;
        loop {
            tokio::select! {
                request = self.requests.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    self.handle_request(request).await;
                }
                request = self.quiesce.recv() => {
                    if let Some(request) = request {
                        self.handle_quiesce(request);
                    }
                }
                request = self.delegation_requests.recv() => {
                    if let Some(request) = request {
                        self.handle_native_delegation(request).await;
                    }
                }
                request_id = self.native_cancellations.recv() => {
                    if let Some(request_id) = request_id {
                        self.cancel_native_delegation(request_id).await;
                    }
                }
                completion = self.mcp_calls.recv() => {
                    if let Some(completion) = completion {
                        self.complete_mcp_call(completion).await;
                    }
                }
                completion = self.mcp_discoveries.recv() => {
                    if let Some(completion) = completion {
                        self.complete_mcp_discovery(completion);
                    }
                }
                event = self.effects.backends.events.recv(), if backend_open => {
                    match event {
                        Some((source, event)) => self.handle_backend_event(source, event).await,
                        None => backend_open = false,
                    }
                }
                event = self.effects.shell_processes.events.recv(), if shell_open => {
                    match event {
                        Some(event) => {
                            let shell_id = shell_event_id(&event).to_owned();
                            let terminal = matches!(
                                event,
                                ShellEvent::Finished { .. } | ShellEvent::Failed { .. }
                            );
                            let session_id = self
                                .shell_owners
                                .get(&shell_id)
                                .cloned()
                                .unwrap_or_else(|| self.core.default_session_id().clone());
                            if let Some(engine) = self.core.engine_for_mut(&session_id) {
                                EffectExecutor::handle_shell_event(engine.state_mut(), event);
                                self.core
                                    .commit_and_publish_session(&self.endpoint, &session_id);
                            }
                            if terminal {
                                self.shell_owners.remove(&shell_id);
                                self.effects.shell_processes.complete(&shell_id);
                            }
                        }
                        None => shell_open = false,
                    }
                }
                shutdown = self.shutdown.recv(), if shutdown_open => {
                    match shutdown {
                        Some(()) => break,
                        None => shutdown_open = false,
                    }
                }
                _ = provider_sync.tick() => {
                    self.synchronize_shared_providers().await;
                    self.cancel_abandoned_native_delegations().await;
                },
            }
        }
        for (_, pending) in self.pending_native_delegations.drain() {
            pending.cancellation_task.abort();
            let _ = pending.respond.send(Err(
                "workspace service stopped before delegated work completed".to_owned(),
            ));
        }
        for pending in self.pending_mcp_calls.values() {
            pending.cancellation.cancel();
        }
        for pending in self.pending_mcp_discoveries.values() {
            pending.cancellation.cancel();
        }
        self.effects.shutdown().await;
    }

    fn handle_quiesce(&mut self, request: QuiesceRequest) {
        let mut running = self.core.live_work_session_ids();
        running.extend(
            self.pending_native_delegations
                .values()
                .map(|pending| pending.session_id.to_string()),
        );
        running.sort();
        running.dedup();
        if running.is_empty() {
            self.accepting_work = false;
            if request.respond.send(Ok(())).is_err() {
                // A bounded lifecycle caller abandoned the response. No other actor branch can run
                // between the fence and this rollback, so work was never accepted while ambiguous.
                self.accepting_work = true;
            }
        } else {
            let _ = request.respond.send(Err(format!(
                "live work is still owned by session(s) {}",
                running.join(", ")
            )));
        }
    }

    async fn handle_native_delegation(&mut self, request: NativeDelegationRequest) {
        if !self.accepting_work {
            let _ = request.respond.send(Err(
                "workspace service is fenced for executable replacement".to_owned(),
            ));
            return;
        }
        if request.cancellation.is_cancelled() || request.respond.is_closed() {
            return;
        }
        let session_id = nakode_protocol::SessionId::from(request.owner_session_id.clone());
        let request_id = self.next_native_delegation_request;
        self.next_native_delegation_request =
            self.next_native_delegation_request.wrapping_add(1).max(1);
        let delegated = self.core.delegate_agent_attributed(
            &session_id,
            &request.agent,
            &request.task,
            request.parent_run_id.as_deref(),
            request_id,
            Some(&request.invocation_turn_id),
            Some(&request.invocation_call_id),
        );
        let (run_id, effects) = match delegated {
            Ok(delegated) => delegated,
            Err(error) => {
                let _ = request.respond.send(Err(error.to_string()));
                return;
            }
        };
        let cancellation = request.cancellation.clone();
        let cancellation_tx = self.native_cancellation_tx.clone();
        let cancellation_task = tokio::spawn(async move {
            cancellation.cancelled().await;
            let _ = cancellation_tx.send(request_id).await;
        });
        self.pending_native_delegations.insert(
            request_id,
            PendingNativeDelegation {
                session_id: session_id.clone(),
                run_id,
                respond: request.respond,
                cancellation_task,
            },
        );
        self.register_effect_owners(&session_id, &effects);
        if let Some(engine) = self.core.engine_for_mut(&session_id) {
            self.effects
                .execute(
                    &session_id,
                    engine.state_mut(),
                    effects,
                    EffectOrigin::PrimarySession,
                )
                .await;
        }
        self.refresh_catalogs();
        self.core
            .commit_and_publish_session(&self.endpoint, &session_id);
    }

    fn complete_native_delegations(&mut self, effects: &[Effect]) {
        for effect in effects {
            let Effect::CompleteAgentRequest {
                request_id,
                result,
                success,
            } = effect
            else {
                continue;
            };
            if *request_id == 0 {
                continue;
            }
            if let Some(pending) = self.pending_native_delegations.remove(request_id) {
                pending.cancellation_task.abort();
                let terminal = if *success {
                    Ok(result.clone())
                } else {
                    Err(result.clone())
                };
                let _ = pending.respond.send(terminal);
            }
        }
    }

    async fn cancel_abandoned_native_delegations(&mut self) {
        let abandoned = self
            .pending_native_delegations
            .iter()
            .filter(|(_, pending)| pending.respond.is_closed())
            .map(|(request_id, _)| *request_id)
            .collect::<Vec<_>>();
        for request_id in abandoned {
            self.cancel_native_delegation(request_id).await;
        }
    }

    async fn cancel_native_delegation(&mut self, request_id: u64) {
        let Some(pending) = self.pending_native_delegations.remove(&request_id) else {
            return;
        };
        pending.cancellation_task.abort();
        let _ = pending.respond.send(Err(
            "native delegation cancelled with its provider turn".to_owned()
        ));
        let effects = self
            .core
            .cancel_attributed_run(&pending.session_id, &pending.run_id);
        let Ok(effects) = effects else {
            return;
        };
        if let Some(engine) = self.core.engine_for_mut(&pending.session_id) {
            self.effects
                .execute(
                    &pending.session_id,
                    engine.state_mut(),
                    effects,
                    EffectOrigin::PrimarySession,
                )
                .await;
        }
        self.core
            .commit_and_publish_session(&self.endpoint, &pending.session_id);
    }

    fn refresh_builtin_tool_availability(&mut self) {
        let vision_provider = self
            .core
            .configured_vision_model_provider()
            .map(str::to_owned);
        let availability = self
            .effects
            .backends
            .available_builtin_tools(self.core.provider_records(), vision_provider.as_deref());
        self.core.install_available_builtin_tools(&availability);
    }

    fn effective_skill_catalogue(&self, profile_id: &str) -> SkillCatalog {
        self.skill_catalogue.enabled_for(
            self.skill_preferences
                .get(profile_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            profile_id,
        )
    }

    #[allow(clippy::too_many_lines)]
    // The request dispatcher keeps fencing, typed routing, persistence rollback, and response
    // completion in one exhaustive match so a newly added public request cannot bypass a guard.
    async fn handle_request(&mut self, request: nakode_server::ServerRequest) {
        self.refresh_builtin_tool_availability();
        let request = if self.accepting_work {
            request
        } else {
            match request {
                nakode_server::ServerRequest::Command { respond, .. } => {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::Conflict,
                        message: "workspace service is fenced for executable replacement"
                            .to_owned(),
                        retryable: true,
                    }));
                    return;
                }
                request => request,
            }
        };
        let mut request = request;
        let skill_profile_error = match &mut request {
            nakode_server::ServerRequest::Command {
                command:
                    Command::CreateSession {
                        profile_id: Some(profile_id),
                        disabled_skill_ids,
                        ..
                    },
                ..
            } => {
                if profile_id.trim().is_empty() || profile_id.len() > 200 {
                    Some("profile_id must be non-empty and at most 200 bytes".to_owned())
                } else {
                    *disabled_skill_ids = self
                        .skill_preferences
                        .get(profile_id)
                        .into_iter()
                        .flatten()
                        .filter(|preference| !preference.enabled)
                        .map(|preference| preference.skill_id.clone())
                        .collect();
                    None
                }
            }
            nakode_server::ServerRequest::Command {
                command:
                    Command::OpenSession {
                        session_id,
                        profile_id,
                        enabled_skill_ids,
                        ..
                    },
                ..
            } => match self
                .effects
                .persistence
                .sessions
                .session_skill_profile(session_id.as_str())
            {
                Err(error) => Some(format!("failed to resolve session skill profile: {error}")),
                Ok(persisted_profile_id) => {
                    let invalid = profile_id
                        .as_deref()
                        .is_some_and(|profile| profile.trim().is_empty() || profile.len() > 200);
                    let mismatched = profile_id
                        .as_deref()
                        .zip(persisted_profile_id.as_deref())
                        .is_some_and(|(requested, persisted)| requested != persisted);
                    if invalid {
                        Some("profile_id must be non-empty and at most 200 bytes".to_owned())
                    } else if mismatched {
                        Some("session belongs to a different skill profile".to_owned())
                    } else {
                        if profile_id.is_none() {
                            profile_id.clone_from(&persisted_profile_id);
                        }
                        let bind_error = if persisted_profile_id.is_none() && profile_id.is_some() {
                            self.effects
                                .persistence
                                .sessions
                                .bind_session_skill_profile(
                                    session_id.as_str(),
                                    profile_id.as_deref().expect("profile is present"),
                                )
                                .err()
                        } else {
                            None
                        };
                        if let Some(error) = bind_error {
                            Some(format!("failed to persist session skill profile: {error}"))
                        } else {
                            match profile_id.as_deref() {
                                Some(profile_id) => {
                                    *enabled_skill_ids =
                                        self.effective_skill_catalogue(profile_id).stable_ids();
                                    None
                                }
                                None => None,
                            }
                        }
                    }
                }
            },
            _ => None,
        };
        if let Some(message) = skill_profile_error {
            if let nakode_server::ServerRequest::Command { respond, .. } = request {
                let _ = respond.send(Err(ServiceError {
                    code: ErrorCode::Internal,
                    message,
                    retryable: true,
                }));
            }
            return;
        }
        let request = match request {
            nakode_server::ServerRequest::Query {
                query:
                    Query::ListSkills {
                        workspace_id,
                        profile_id,
                        refresh,
                    },
                respond,
                ..
            } => {
                let current_workspace =
                    crate::state::projection::workspace_id(&self.core.engine().state().workspace);
                if workspace_id != current_workspace
                    || profile_id.trim().is_empty()
                    || profile_id.len() > 200
                {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: "workspace and a bounded profile_id are required".to_owned(),
                        retryable: false,
                    }));
                    return;
                }
                if refresh {
                    match SkillCatalog::load(Path::new(&self.core.engine().state().workspace)) {
                        Ok(catalogue) => {
                            self.skill_catalogue = catalogue;
                            self.core.install_skill_authority(
                                &self.skill_catalogue,
                                &self.skill_preferences,
                            );
                        }
                        Err(error) => {
                            let _ = respond.send(Err(ServiceError {
                                code: ErrorCode::Internal,
                                message: error.to_string(),
                                retryable: true,
                            }));
                            return;
                        }
                    }
                }
                let preferences = self
                    .skill_preferences
                    .get(&profile_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let catalogue = nakode_protocol::SkillCatalogueView {
                    skills: self
                        .skill_catalogue
                        .manageable(preferences, &profile_id)
                        .into_iter()
                        .map(|skill| nakode_protocol::ManageableSkillView {
                            id: skill.id,
                            name: skill.name,
                            description: skill.description,
                            enabled: skill.enabled,
                            available: skill.available,
                        })
                        .collect(),
                };
                let _ = respond.send(Ok(Snapshot {
                    cursor: self.endpoint.cursor(),
                    value: QueryResult::Skills(catalogue),
                }));
                return;
            }
            nakode_server::ServerRequest::Command {
                command:
                    Command::SetSkillEnabled {
                        workspace_id,
                        profile_id,
                        skill_id,
                        enabled,
                    },
                respond,
                ..
            } => {
                let current_workspace =
                    crate::state::projection::workspace_id(&self.core.engine().state().workspace);
                if workspace_id != current_workspace
                    || profile_id.trim().is_empty()
                    || profile_id.len() > 200
                    || skill_id.trim().is_empty()
                    || skill_id.len() > 128
                {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: "workspace, profile_id, and stable skill_id are required"
                            .to_owned(),
                        retryable: false,
                    }));
                    return;
                }
                let preferences = self
                    .skill_preferences
                    .get(&profile_id)
                    .cloned()
                    .unwrap_or_default();
                let Some(row) = self
                    .skill_catalogue
                    .manageable(&preferences, &profile_id)
                    .into_iter()
                    .find(|skill| skill.id == skill_id)
                else {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: format!(
                            "skill identity {skill_id:?} is not installed or retained"
                        ),
                        retryable: false,
                    }));
                    return;
                };
                let preference = crate::skill::SkillPreference {
                    profile_id: profile_id.clone(),
                    skill_id: row.id.clone(),
                    last_name: row.name,
                    last_description: row.description,
                    enabled,
                };
                if let Err(error) = self
                    .effects
                    .persistence
                    .sessions
                    .set_skill_preference(&preference)
                {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::Internal,
                        message: error.to_string(),
                        retryable: true,
                    }));
                    return;
                }
                let profile_preferences = self
                    .skill_preferences
                    .entry(profile_id.clone())
                    .or_default();
                if let Some(saved) = profile_preferences
                    .iter_mut()
                    .find(|saved| saved.skill_id == preference.skill_id)
                {
                    *saved = preference;
                } else {
                    profile_preferences.push(preference);
                }
                let effective = self.effective_skill_catalogue(&profile_id);
                self.core
                    .install_profile_skill_catalogue(&profile_id, &effective);
                let _ = respond.send(Ok(nakode_protocol::CommandAccepted {
                    resource_id: Some(row.id),
                    revision: None,
                    bridge_continuation: None,
                    replayed_bridge_continuation: None,
                    replayed_bridge_source_active: None,
                }));
                return;
            }
            nakode_server::ServerRequest::Query {
                query:
                    Query::GetDiagnostics {
                        days,
                        session_limit,
                        provider_id,
                    },
                respond,
                ..
            } => {
                let cursor = self.endpoint.cursor();
                let options = crate::diagnostics::DiagnosticsOptions {
                    days,
                    session_limit: usize::try_from(session_limit).unwrap_or(usize::MAX),
                    provider: provider_id.map(|provider| provider.to_string()),
                    json: false,
                };
                if !(1..=3_650).contains(&days) || !(1..=500).contains(&session_limit) {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: "diagnostics days must be 1-3650 and session limit must be 1-500"
                            .to_owned(),
                        retryable: false,
                    }));
                    return;
                }
                let database = self.effects.persistence.database.clone();
                tokio::task::spawn_blocking(move || {
                    let result = crate::diagnostics::collect(
                        &database,
                        &options,
                        crate::diagnostics::unix_time_ms(),
                    )
                    .map(|report| Snapshot {
                        cursor,
                        value: QueryResult::Diagnostics(Box::new(report)),
                    })
                    .map_err(|error| ServiceError {
                        code: ErrorCode::Internal,
                        message: error.to_string(),
                        retryable: false,
                    });
                    let _ = respond.send(result);
                });
                return;
            }
            nakode_server::ServerRequest::Query {
                query: Query::GetInvocationSummary,
                respond,
                ..
            } => {
                let cursor = self.endpoint.cursor();
                let catalogue = self.core.engine().state().invocation_catalogue();
                let sessions = Arc::clone(&self.effects.persistence.sessions);
                tokio::task::spawn_blocking(move || {
                    let result = sessions
                        .invocation_summary()
                        .map(|summary| merge_invocation_catalogue(summary, catalogue))
                        .map(|summary| Snapshot {
                            cursor,
                            value: QueryResult::InvocationSummary(Box::new(summary)),
                        })
                        .map_err(|error| ServiceError {
                            code: ErrorCode::Internal,
                            message: error.to_string(),
                            retryable: false,
                        });
                    let _ = respond.send(result);
                });
                return;
            }
            nakode_server::ServerRequest::Query {
                query:
                    Query::GetInvocationTimeline {
                        start_at_ms,
                        end_at_ms,
                        bucket_width_ms,
                    },
                respond,
                ..
            } => {
                const MIN_BUCKET_MS: u64 = 60 * 60 * 1_000;
                const MAX_RANGE_MS: u64 = 366 * 24 * 60 * 60 * 1_000;
                const MAX_BUCKETS: u64 = 1_000;
                let range = end_at_ms.saturating_sub(start_at_ms);
                let valid = start_at_ms < end_at_ms
                    && range <= MAX_RANGE_MS
                    && bucket_width_ms >= MIN_BUCKET_MS
                    && range.div_ceil(bucket_width_ms) <= MAX_BUCKETS;
                if !valid {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: "invocation timeline requires a positive range of at most 366 days, buckets of at least one hour, and no more than 1000 buckets".to_owned(),
                        retryable: false,
                    }));
                    return;
                }
                let cursor = self.endpoint.cursor();
                let sessions = Arc::clone(&self.effects.persistence.sessions);
                tokio::task::spawn_blocking(move || {
                    let result = sessions
                        .invocation_timeline(start_at_ms, end_at_ms, bucket_width_ms)
                        .map(|timeline| Snapshot {
                            cursor,
                            value: QueryResult::InvocationTimeline(Box::new(timeline)),
                        })
                        .map_err(|error| ServiceError {
                            code: ErrorCode::Internal,
                            message: error.to_string(),
                            retryable: false,
                        });
                    let _ = respond.send(result);
                });
                return;
            }
            request => request,
        };
        let mut inbound_event_to_claim = None;
        if let Some((session_id, external_event_id)) = bridge_inbound_event_identity(&request) {
            match self
                .effects
                .persistence
                .sessions
                .find_session_bridge_inbound_event(session_id.as_str(), &external_event_id)
            {
                Ok(Some(disposition)) => self.core.remember_durable_bridge_inbound_event(
                    &session_id,
                    &external_event_id,
                    disposition,
                ),
                Ok(None) => inbound_event_to_claim = Some((session_id, external_event_id)),
                Err(_) => {
                    if let nakode_server::ServerRequest::Command { respond, .. } = request {
                        let _ = respond.send(Err(ServiceError {
                            code: ErrorCode::Internal,
                            message: "the durable inbound replay ledger is unavailable; retry the operation"
                                .to_owned(),
                            retryable: true,
                        }));
                    }
                    return;
                }
            }
        }
        let mut rollback = BridgeMutationRollback::capture(&request, &self.core);
        let mut outcome = self.core.handle(&self.endpoint, request);
        let replay_disposition = outcome.bridge_continuation();
        let inbound_event_to_claim =
            inbound_event_to_claim.and_then(|(session_id, external_event_id)| {
                replay_disposition.map(|disposition| (session_id, external_event_id, disposition))
            });
        let mut effects = std::mem::take(&mut outcome.effects);
        let had_effects = !effects.is_empty();
        let updates_provider_model_filter = effects
            .iter()
            .any(|effect| matches!(effect, Effect::SetProviderModelFilter { .. }));
        let session_id = outcome
            .effect_session
            .clone()
            .unwrap_or_else(|| self.core.default_session_id().clone());
        if let Err(_error) = persist_bridge_effects(
            self.effects.persistence.sessions.as_ref(),
            &mut effects,
            inbound_event_to_claim.as_ref(),
        ) {
            // The external transport and provider must observe bridge work only after its durable
            // authority checkpoint. Restore both logical-session and idempotency state so a
            // same-process retry (including the same key) can execute rather than replaying an
            // un-dispatched success.
            match rollback {
                Some(rollback) => rollback.restore(&mut self.core),
                None => {
                    // A new bridge-producing command must opt into rollback before retries are safe.
                    self.accepting_work = false;
                }
            }
            outcome.respond_with_error(ServiceError {
                code: ErrorCode::Internal,
                message: "the durable orchestrator bridge checkpoint failed; retry the operation"
                    .to_owned(),
                retryable: true,
            });
            return;
        }
        if let Some(delete_session_id) = take_delete_session_effect(&mut effects) {
            let canonical_id = nakode_protocol::SessionId::from(delete_session_id.clone());
            remove_session_release_effect(&mut effects, &delete_session_id);
            let terminated_shell_ids = self.terminate_deleted_session_work(&canonical_id).await;
            // Provider tasks can persist native history during their terminal path. Fence and await
            // every backend before the repository transaction so a successful deletion cannot be
            // repopulated by a completion racing behind its acknowledgement.
            if let Err(error) = self.effects.backends.stop_session(&canonical_id).await {
                match rollback.take() {
                    Some(rollback) => rollback.restore(&mut self.core),
                    None => self.accepting_work = false,
                }
                self.fail_terminated_session_shells(
                    &canonical_id,
                    &terminated_shell_ids,
                    "session deletion stopped the shell before backend teardown failed",
                );
                outcome.respond_with_error(ServiceError {
                    code: ErrorCode::Internal,
                    message: format!(
                        "session backends did not stop cleanly; retry the operation: {error}"
                    ),
                    retryable: true,
                });
                return;
            }
            if let Err(error) = self.effects.persistence.sessions.delete(&delete_session_id) {
                // DeleteSession mutates the complete in-memory projection (including default-session
                // and bridge state) before effects run. Restore the pre-command checkpoint so
                // same-process retries, including the same idempotency key, execute instead of
                // replaying a false success.
                match rollback.take() {
                    Some(rollback) => rollback.restore(&mut self.core),
                    None => self.accepting_work = false,
                }
                if let Some(engine) = self.core.engine_for_mut(&canonical_id) {
                    let provider = engine.state().active_provider_id().to_owned();
                    engine.state_mut().handle_provider_backend(
                        &provider,
                        BackendEvent::Disconnected {
                            reason: "provider stopped before durable session deletion failed"
                                .to_owned(),
                        },
                    );
                }
                self.fail_terminated_session_shells(
                    &canonical_id,
                    &terminated_shell_ids,
                    "shell stopped before durable session deletion failed",
                );
                outcome.respond_with_error(ServiceError {
                    code: ErrorCode::Internal,
                    message: format!(
                        "session deletion was not durably committed; retry the operation: {error}"
                    ),
                    retryable: true,
                });
                return;
            }
        }
        self.complete_native_delegations(&effects);
        self.register_effect_owners(&session_id, &effects);
        self.execute_effects(&session_id, effects, EffectOrigin::ClientCommand)
            .await;
        if updates_provider_model_filter {
            self.synchronize_shared_providers().await;
        }
        self.refresh_builtin_tool_availability();
        self.refresh_mcp_servers();
        if had_effects {
            self.refresh_catalogs();
        }
        if outcome.changed {
            self.core
                .commit_and_publish_session(&self.endpoint, &session_id);
        }
        outcome.respond();
    }

    async fn synchronize_shared_providers(&mut self) {
        let mut providers = match self.effects.persistence.sessions.list_providers() {
            Ok(providers) => providers,
            Err(error) => {
                self.core
                    .engine_mut()
                    .state_mut()
                    .session_store_failed(error.to_string());
                return;
            }
        };
        enable_e2e_fixture_provider(&mut providers);
        if providers == self.core.provider_records() {
            return;
        }

        let enablement_changes =
            provider_enablement_changes(self.core.provider_records(), &providers);
        let session_id = self.core.default_session_id().clone();
        for (provider, enabled) in enablement_changes {
            if let Some(engine) = self.core.engine_for_mut(&session_id) {
                self.effects
                    .set_provider_enabled(engine.state_mut(), &provider, enabled)
                    .await;
            }
        }
        self.core.replace_provider_records(providers);
        self.core
            .commit_and_publish_session(&self.endpoint, &session_id);
    }

    #[allow(clippy::too_many_lines)]
    // Provider-event correlation, durable bridge acknowledgement, and downstream effect ordering
    // intentionally share this dispatcher so persistence failure can stop every dependent effect.
    async fn handle_backend_event(&mut self, source: BackendSource, event: BackendEvent) {
        let is_streaming_delta = matches!(&event, BackendEvent::ItemDelta { .. });
        if let BackendEvent::ExternalToolRequested(request) = &event
            && request.name.starts_with(nakode_protocol::MCP_TOOL_PREFIX)
        {
            self.handle_mcp_tool_request(source, request.clone()).await;
            return;
        }
        if let BackendEvent::SkillInvoked {
            invocation_key,
            identity,
            display_label,
            occurred_at_ms,
        } = &event
        {
            let invocation = crate::session::InvocationRecord {
                invocation_key: invocation_key.clone(),
                kind: nakode_protocol::InvocationKind::Skill,
                identity: identity.clone(),
                display_label: display_label.clone(),
                occurred_at_ms: *occurred_at_ms,
            };
            if let Err(error) = self
                .effects
                .persistence
                .sessions
                .record_invocation(&invocation)
            {
                self.core
                    .engine_mut()
                    .state_mut()
                    .session_store_failed(error.to_string());
            }
            return;
        }
        let event_session_id = match &source {
            BackendSource::ProviderControl(_) => Some(self.core.default_session_id().clone()),
            BackendSource::Primary { session_id, .. } => Some(session_id.clone()),
            BackendSource::Subagent(_) => None,
        };
        let history_was_rebuilt = matches!(&event, BackendEvent::SessionResumed { .. });
        let bridge_origin_turn_id = event_session_id.as_ref().and_then(|_| match &event {
            BackendEvent::TurnAccepted { turn_id }
            | BackendEvent::TurnStarted { turn_id }
            | BackendEvent::TurnCompleted { turn_id, .. } => Some(turn_id.clone()),
            _ => None,
        });
        let acknowledged_prompt_id = event_session_id.as_ref().and_then(|session_id| {
            self.pending_bridge_acknowledgements
                .get(session_id)
                .cloned()
                .or_else(|| match &event {
                    BackendEvent::TurnAccepted { turn_id }
                    | BackendEvent::TurnStarted { turn_id }
                    | BackendEvent::TurnCompleted { turn_id, .. } => self
                        .core
                        .bridge_prompt_acknowledgement_id(session_id, turn_id, true),
                    _ => None,
                })
        });
        let origin = match &source {
            BackendSource::ProviderControl(_) => EffectOrigin::ProviderControl,
            BackendSource::Primary { .. } => EffectOrigin::PrimarySession,
            BackendSource::Subagent(_) => EffectOrigin::Subagent,
        };
        self.effects
            .backends
            .observe_provider_event(&source, &event);
        let (session_id, mut effects) = match source {
            BackendSource::ProviderControl(provider) => {
                let session_id = self.core.default_session_id().clone();
                let effects = self
                    .core
                    .engine_for_mut(&session_id)
                    .map_or_else(Vec::new, |engine| {
                        engine.state_mut().handle_provider_backend(&provider, event)
                    });
                (session_id, effects)
            }
            BackendSource::Primary {
                session_id,
                provider,
            } => {
                let effects = self
                    .core
                    .engine_for_mut(&session_id)
                    .map_or_else(Vec::new, |engine| {
                        engine.state_mut().handle_provider_backend(&provider, event)
                    });
                (session_id, effects)
            }
            BackendSource::Subagent(run_id) => {
                let session_id = self
                    .core
                    .session_for_run_id(&run_id)
                    .unwrap_or_else(|| self.core.default_session_id().clone());
                let effects = self
                    .core
                    .engine_for_mut(&session_id)
                    .map_or_else(Vec::new, |engine| {
                        engine.state_mut().handle_subagent_backend(&run_id, event)
                    });
                (session_id, effects)
            }
        };
        if history_was_rebuilt {
            self.core.reapply_bridge_turn_origins(&session_id);
        }
        let bridge_checkpoint = (acknowledged_prompt_id.is_some()
            || bridge_origin_turn_id.is_some())
        .then(|| self.core.bridge_state_checkpoint());
        if let Some(turn_id) = bridge_origin_turn_id.as_deref()
            && let Some(effect) = self.core.record_bridge_turn_origin(&session_id, turn_id)
        {
            effects.push(effect);
        }
        if let Some(client_prompt_id) = acknowledged_prompt_id.as_deref()
            && let Some(effect) = self
                .core
                .acknowledge_bridge_prompt(&session_id, client_prompt_id)
        {
            effects.push(effect);
        }
        let bridge_checkpoint_deferred = if persist_bridge_effects(
            self.effects.persistence.sessions.as_ref(),
            &mut effects,
            None,
        )
        .is_err()
        {
            if let Some(checkpoint) = bridge_checkpoint {
                self.core.restore_bridge_state(checkpoint);
            }
            if let Some(client_prompt_id) = acknowledged_prompt_id {
                self.pending_bridge_acknowledgements
                    .insert(session_id.clone(), client_prompt_id);
            }
            // Keep the inbox item pending and retain its in-process acknowledgement correlation. A
            // later provider event or restart retries the same stable client turn identity.
            eprintln!("nakode bridge: inbound acknowledgement checkpoint deferred");
            true
        } else {
            if acknowledged_prompt_id.is_some() {
                self.pending_bridge_acknowledgements.remove(&session_id);
            }
            false
        };
        let had_effects = !effects.is_empty();
        self.complete_native_delegations(&effects);
        self.register_effect_owners(&session_id, &effects);
        if let Some(engine) = self.core.engine_for_mut(&session_id) {
            self.effects
                .execute(&session_id, engine.state_mut(), effects, origin)
                .await;
        }
        if had_effects {
            self.refresh_catalogs();
        }
        self.refresh_builtin_tool_availability();
        if !bridge_checkpoint_deferred {
            match self.core.resume_pending_bridge_prompt(&session_id) {
                Ok(pending_effects) if !pending_effects.is_empty() => {
                    self.register_effect_owners(&session_id, &pending_effects);
                    if let Some(engine) = self.core.engine_for_mut(&session_id) {
                        self.effects
                            .execute(
                                &session_id,
                                engine.state_mut(),
                                pending_effects,
                                EffectOrigin::PrimarySession,
                            )
                            .await;
                    }
                }
                Ok(_) => {}
                Err(_) => eprintln!("nakode bridge: durable inbound replay deferred"),
            }
        }
        if is_streaming_delta {
            self.core
                .commit_and_publish_session_delta(&self.endpoint, &session_id);
        } else {
            self.core
                .commit_and_publish_backend_session(&self.endpoint, &session_id);
        }
    }
    #[allow(clippy::too_many_lines)]
    async fn handle_mcp_tool_request(
        &mut self,
        source: BackendSource,
        request: crate::backend::ExternalToolRequest,
    ) {
        let Some((server, remote_name)) = self.core.mcp_servers().iter().find_map(|server| {
            server
                .tools
                .iter()
                .find(|tool| tool.exposed_name == request.name && !tool.app_only)
                .map(|tool| (server.clone(), tool.remote_name.clone()))
        }) else {
            self.resolve_mcp_tool(
                &source,
                &request.id,
                "MCP tool grant is no longer available".to_owned(),
                true,
            )
            .await;
            return;
        };
        let (session_id, run_id) = match &source {
            BackendSource::Primary { session_id, .. } => (session_id.clone(), None),
            BackendSource::Subagent(run_id) => (
                self.core
                    .session_for_run_id(run_id)
                    .unwrap_or_else(|| self.core.default_session_id().clone()),
                Some(run_id.clone()),
            ),
            BackendSource::ProviderControl(_) => return,
        };
        let currently_granted = self.core.engine_for(&session_id).is_some_and(|engine| {
            if let BackendSource::Subagent(run_id) = &source {
                engine.state().subagent_has_mcp_tool(run_id, &request.name)
            } else {
                engine.state().has_mcp_tool(&request.name)
            }
        });
        if !currently_granted || !server.usable() {
            self.resolve_mcp_tool(
                &source,
                &request.id,
                "MCP server is disabled, unavailable, or no longer granted".to_owned(),
                true,
            )
            .await;
            return;
        }
        let credential = self
            .effects
            .persistence
            .credentials
            .get_mcp(&server.workspace, &server.id)
            .ok()
            .flatten()
            .and_then(|stored| {
                let secret = stored.secret.expose().get("secret")?.as_str()?.to_owned();
                Some(crate::mcp::McpCredential {
                    kind: stored.kind,
                    secret,
                })
            });
        if server.credential_required && credential.is_none() {
            self.resolve_mcp_tool(
                &source,
                &request.id,
                "MCP server credential is missing or revoked".to_owned(),
                true,
            )
            .await;
            return;
        }
        let cancellation = tokio_util::sync::CancellationToken::new();
        let client = self.effects.mcp.clone();
        let completion_tx = self.mcp_call_tx.clone();
        let call_id = request.id.clone();
        let task_server = server.clone();
        let task_remote_name = remote_name.clone();
        let task_arguments = request.arguments_json.clone();
        let task_cancellation = cancellation.clone();
        tokio::spawn(async move {
            let result = client
                .invoke(
                    &task_server,
                    &task_remote_name,
                    &task_arguments,
                    credential.as_ref(),
                    &task_cancellation,
                )
                .await;
            let _ = completion_tx
                .send(McpCallCompletion { call_id, result })
                .await;
        });
        self.pending_mcp_calls.insert(
            request.id,
            PendingMcpCall {
                source,
                session_id,
                run_id,
                server_id: server.id,
                remote_name,
                arguments_json: request.arguments_json,
                started_at_ms: crate::mcp::unix_time_ms(),
                started: Instant::now(),
                cancellation,
            },
        );
    }

    async fn complete_mcp_call(&mut self, completion: McpCallCompletion) {
        let Some(pending) = self.pending_mcp_calls.remove(&completion.call_id) else {
            return;
        };
        // A delayed tool completion must never recreate a provider backend after its logical session
        // was deleted. Deletion removes owned calls proactively; this check also closes the race for a
        // completion already queued in the runtime mailbox.
        if self.core.engine_for(&pending.session_id).is_none() {
            pending.cancellation.cancel();
            return;
        }
        let (output, failed, status) = match completion.result {
            Ok(output) => (output, false, "succeeded"),
            Err(error) => (
                error.to_string(),
                true,
                if matches!(error, crate::mcp::McpError::Cancelled) {
                    "cancelled"
                } else {
                    "failed"
                },
            ),
        };
        let bounded_arguments = crate::tools::model_facing_output(&pending.arguments_json);
        let bounded_output = crate::tools::model_facing_output(&output);
        let _ = self.effects.persistence.sessions.audit_mcp_invocation(
            &crate::session::McpInvocationAudit {
                id: uuid::Uuid::now_v7().to_string(),
                workspace: crate::state::projection::workspace_id(
                    &self.core.engine().state().workspace,
                )
                .to_string(),
                session_id: pending.session_id.to_string(),
                run_id: pending.run_id,
                server_id: pending.server_id,
                tool_name: pending.remote_name,
                arguments_json: bounded_arguments,
                result_json: bounded_output.clone(),
                status: status.to_owned(),
                started_at_ms: pending.started_at_ms,
                duration_ms: u64::try_from(pending.started.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
            },
        );
        self.resolve_mcp_tool(&pending.source, &completion.call_id, bounded_output, failed)
            .await;
    }

    async fn resolve_mcp_tool(
        &mut self,
        source: &BackendSource,
        call_id: &str,
        output: String,
        failed: bool,
    ) {
        let command = BackendCommand::ResolveExternalTool {
            id: call_id.to_owned(),
            output,
            failed,
        };
        match source {
            BackendSource::Primary {
                session_id,
                provider,
            } => {
                let working_directory = self.core.engine_for(session_id).map_or_else(
                    || self.effects.backends.config.workspace.clone(),
                    |engine| PathBuf::from(&engine.state().working_directory),
                );
                let _ = self
                    .effects
                    .backends
                    .send_session(session_id, provider, &working_directory, command)
                    .await;
            }
            BackendSource::Subagent(run_id) => {
                let _ = self.effects.backends.send_subagent(run_id, command).await;
            }
            BackendSource::ProviderControl(_) => {}
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_effects(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        effects: Vec<Effect>,
        origin: EffectOrigin,
    ) {
        let mut ordinary = Vec::new();
        let mut sync_memory_config = false;
        for effect in effects {
            match effect {
                Effect::SaveMcpServer(server) => {
                    self.cancel_mcp_server_work(&server.id);
                    if self
                        .effects
                        .persistence
                        .sessions
                        .save_mcp_server(&server)
                        .is_ok()
                    {
                        self.core.replace_mcp_server(server);
                    }
                }
                Effect::DeleteMcpServer {
                    workspace,
                    server_id,
                } => {
                    self.cancel_mcp_server_work(&server_id);
                    let _ = self
                        .effects
                        .persistence
                        .credentials
                        .delete_mcp(&workspace, &server_id);
                    if self
                        .effects
                        .persistence
                        .sessions
                        .delete_mcp_server(&workspace, &server_id)
                        .is_ok()
                    {
                        self.core.remove_mcp_server(&server_id);
                    }
                }
                Effect::SaveMcpCredential {
                    workspace,
                    server_id,
                    kind,
                    secret,
                } => {
                    self.cancel_mcp_server_work(&server_id);
                    let credential = crate::credential::Credential {
                        kind: kind.clone(),
                        secret: crate::credential::SecretValue::new(
                            serde_json::json!({"secret": secret}),
                        ),
                    };
                    if self
                        .effects
                        .persistence
                        .credentials
                        .put_mcp(&workspace, &server_id, &credential)
                        .is_ok()
                        && let Some(existing) = self
                            .core
                            .mcp_servers()
                            .iter()
                            .find(|server| server.id == server_id)
                            .cloned()
                    {
                        let mut server = existing;
                        server.credential_kind = Some(kind);
                        server.updated_at_ms = crate::mcp::unix_time_ms();
                        let _ = self.effects.persistence.sessions.save_mcp_server(&server);
                        self.core.replace_mcp_server(server);
                    }
                }
                Effect::ClearMcpCredential {
                    workspace,
                    server_id,
                } => {
                    self.cancel_mcp_server_work(&server_id);
                    let _ = self
                        .effects
                        .persistence
                        .credentials
                        .delete_mcp(&workspace, &server_id);
                    if let Some(existing) = self
                        .core
                        .mcp_servers()
                        .iter()
                        .find(|server| server.id == server_id)
                        .cloned()
                    {
                        let mut server = existing;
                        server.credential_kind = None;
                        "credential_required".clone_into(&mut server.health);
                        server.server_name = None;
                        server.server_version = None;
                        server.last_error = None;
                        server.tools.clear();
                        server.updated_at_ms = crate::mcp::unix_time_ms();
                        let _ = self.effects.persistence.sessions.save_mcp_server(&server);
                        self.core.replace_mcp_server(server);
                    }
                }
                Effect::RefreshMcpServer(mut server) => {
                    self.cancel_mcp_discovery(&server.id);
                    "connecting".clone_into(&mut server.health);
                    server.last_error = None;
                    let credential = self
                        .effects
                        .persistence
                        .credentials
                        .get_mcp(&server.workspace, &server.id)
                        .ok()
                        .flatten()
                        .and_then(|stored| {
                            let secret = stored.secret.expose().get("secret")?.as_str()?.to_owned();
                            Some(crate::mcp::McpCredential {
                                kind: stored.kind,
                                secret,
                            })
                        });
                    let cancellation = tokio_util::sync::CancellationToken::new();
                    let client = self.effects.mcp.clone();
                    let completion_tx = self.mcp_discovery_tx.clone();
                    let task_server = server.clone();
                    let task_cancellation = cancellation.clone();
                    let server_id = server.id.clone();
                    let request_id = self.next_mcp_discovery_request;
                    self.next_mcp_discovery_request =
                        self.next_mcp_discovery_request.wrapping_add(1).max(1);
                    let _ = self.effects.persistence.sessions.save_mcp_server(&server);
                    self.core.replace_mcp_server(server);
                    tokio::spawn(async move {
                        let result = client
                            .discover(&task_server, credential.as_ref(), &task_cancellation)
                            .await;
                        let _ = completion_tx
                            .send(McpDiscoveryCompletion {
                                request_id,
                                server: task_server,
                                result,
                            })
                            .await;
                    });
                    self.pending_mcp_discoveries.insert(
                        server_id.clone(),
                        PendingMcpDiscovery {
                            request_id,
                            server_id,
                            cancellation,
                        },
                    );
                }
                Effect::SaveMemoryConfig(config) => {
                    sync_memory_config = true;
                    ordinary.push(Effect::SaveMemoryConfig(config));
                }
                Effect::ReleaseSessionBackends(id) => {
                    // DeleteSession removes a non-default engine before runtime effects execute. Backend
                    // release therefore cannot depend on finding that engine's DomainState; doing so
                    // would discard the shutdown effect and orphan its provider processes.
                    let _ = self
                        .effects
                        .backends
                        .stop_session(&nakode_protocol::SessionId::from(id))
                        .await;
                }
                effect => ordinary.push(effect),
            }
        }
        let mut executed = false;
        if let Some(engine) = self.core.engine_for_mut(session_id) {
            self.effects
                .execute(session_id, engine.state_mut(), ordinary, origin)
                .await;
            executed = true;
        }
        if executed && sync_memory_config {
            let memory_config = self.effects.backends.current_memory_config();
            self.core.install_memory_config(&memory_config);
        }
    }

    fn complete_mcp_discovery(&mut self, completion: McpDiscoveryCompletion) {
        let Some(pending) = self.pending_mcp_discoveries.remove(&completion.server.id) else {
            return;
        };
        if pending.cancellation.is_cancelled()
            || pending.server_id != completion.server.id
            || pending.request_id != completion.request_id
        {
            return;
        }
        let mut server = completion.server;
        match completion.result {
            Ok(discovery) => {
                "connected".clone_into(&mut server.health);
                server.server_name = discovery.server_name;
                server.server_version = discovery.server_version;
                server.tools = discovery.tools;
                server.last_error = None;
                server.last_connected_at_ms = Some(crate::mcp::unix_time_ms());
            }
            Err(error) => {
                "error".clone_into(&mut server.health);
                server.last_error = Some(error.to_string());
            }
        }
        server.updated_at_ms = crate::mcp::unix_time_ms();
        let _ = self.effects.persistence.sessions.save_mcp_server(&server);
        self.core.replace_mcp_server(server);
    }

    fn cancel_mcp_discovery(&mut self, server_id: &str) {
        if let Some(pending) = self.pending_mcp_discoveries.remove(server_id) {
            pending.cancellation.cancel();
        }
    }

    async fn terminate_deleted_session_work(
        &mut self,
        session_id: &nakode_protocol::SessionId,
    ) -> Vec<String> {
        self.cancel_session_mcp_calls(session_id);
        let delegation_ids = self
            .pending_native_delegations
            .iter()
            .filter(|(_, pending)| &pending.session_id == session_id)
            .map(|(request_id, _)| *request_id)
            .collect::<Vec<_>>();
        for request_id in delegation_ids {
            if let Some(pending) = self.pending_native_delegations.remove(&request_id) {
                pending.cancellation_task.abort();
                let _ = pending
                    .respond
                    .send(Err("parent session was deleted".to_owned()));
            }
        }
        let shell_ids = self
            .shell_owners
            .iter()
            .filter(|(_, owner)| *owner == session_id)
            .map(|(shell_id, _)| shell_id.clone())
            .collect::<Vec<_>>();
        for shell_id in &shell_ids {
            self.shell_owners.remove(shell_id);
            self.effects.shell_processes.terminate(shell_id).await;
        }
        shell_ids
    }

    fn fail_terminated_session_shells(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        shell_ids: &[String],
        reason: &str,
    ) {
        if shell_ids.is_empty() {
            return;
        }
        if let Some(engine) = self.core.engine_for_mut(session_id) {
            for shell_id in shell_ids {
                engine.state_mut().shell_failed(shell_id, reason);
            }
        }
        self.core
            .commit_and_publish_session(&self.endpoint, session_id);
    }

    fn cancel_session_mcp_calls(&mut self, session_id: &nakode_protocol::SessionId) {
        let call_ids = self
            .pending_mcp_calls
            .iter()
            .filter(|(_, pending)| &pending.session_id == session_id)
            .map(|(call_id, _)| call_id.clone())
            .collect::<Vec<_>>();
        for call_id in call_ids {
            if let Some(pending) = self.pending_mcp_calls.remove(&call_id) {
                pending.cancellation.cancel();
            }
        }
    }

    fn cancel_mcp_server_work(&mut self, server_id: &str) {
        self.cancel_mcp_discovery(server_id);
        let call_ids = self
            .pending_mcp_calls
            .iter()
            .filter(|(_, pending)| pending.server_id == server_id)
            .map(|(call_id, _)| call_id.clone())
            .collect::<Vec<_>>();
        for call_id in call_ids {
            if let Some(pending) = self.pending_mcp_calls.remove(&call_id) {
                pending.cancellation.cancel();
            }
        }
    }

    fn refresh_mcp_servers(&mut self) {
        let workspace =
            crate::state::projection::workspace_id(&self.core.engine().state().workspace)
                .to_string();
        if let Ok(servers) = self
            .effects
            .persistence
            .sessions
            .list_mcp_servers(&workspace)
        {
            let servers = servers
                .into_iter()
                .map(|mut server| {
                    server.credential_kind = self
                        .effects
                        .persistence
                        .credentials
                        .get_mcp(&workspace, &server.id)
                        .ok()
                        .flatten()
                        .map(|credential| credential.kind);
                    crate::mcp::normalize_builtin_server(server)
                })
                .collect();
            self.core.install_mcp_servers(servers);
        }
    }

    fn register_effect_owners(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        effects: &[Effect],
    ) {
        for effect in effects {
            if let Effect::RunShell { id, .. } = effect {
                self.shell_owners.insert(id.clone(), session_id.clone());
            }
        }
    }

    fn refresh_catalogs(&mut self) {
        match self.effects.persistence.sessions.list_providers() {
            Ok(mut providers) => {
                enable_e2e_fixture_provider(&mut providers);
                self.core.replace_provider_records(providers);
            }
            Err(error) => self
                .core
                .engine_mut()
                .state_mut()
                .session_store_failed(error.to_string()),
        }
        let workspace = self.core.engine().state().workspace.clone();
        match self
            .effects
            .persistence
            .sessions
            .list_session_bridges(&workspace)
        {
            Ok(bridges) => self.core.replace_session_bridges(bridges),
            Err(error) => self
                .core
                .engine_mut()
                .state_mut()
                .session_store_failed(error.to_string()),
        }
        match self
            .effects
            .persistence
            .sessions
            .list_recent(&workspace, 100)
        {
            Ok(sessions) => self.core.replace_session_records(sessions),
            Err(error) => self
                .core
                .engine_mut()
                .state_mut()
                .session_store_failed(error.to_string()),
        }
    }
}

fn provider_enablement_changes(
    current: &[ProviderRecord],
    shared: &[ProviderRecord],
) -> Vec<(String, bool)> {
    shared
        .iter()
        .filter_map(|provider| {
            let previous = current
                .iter()
                .find(|record| record.provider == provider.provider);
            previous
                .is_none_or(|record| record.enabled != provider.enabled)
                .then(|| (provider.provider.clone(), provider.enabled))
        })
        .collect()
}

fn merge_invocation_catalogue(
    mut summary: nakode_protocol::InvocationSummary,
    catalogue: Vec<(nakode_protocol::InvocationKind, String, String)>,
) -> nakode_protocol::InvocationSummary {
    let mut positions = summary
        .items
        .iter()
        .enumerate()
        .map(|(index, item)| ((item.kind, item.identity.clone()), index))
        .collect::<HashMap<_, _>>();
    for (kind, identity, display_label) in catalogue {
        if let Some(index) = positions.get(&(kind, identity.clone())).copied() {
            let item = &mut summary.items[index];
            item.currently_installed = true;
            item.display_label = display_label;
        } else {
            positions.insert((kind, identity.clone()), summary.items.len());
            summary.items.push(nakode_protocol::InvocationUsage {
                kind,
                identity,
                display_label,
                currently_installed: true,
                invocation_count: 0,
                first_used_at_ms: None,
                last_used_at_ms: None,
            });
        }
    }
    summary.items.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| right.currently_installed.cmp(&left.currently_installed))
            .then_with(|| right.invocation_count.cmp(&left.invocation_count))
            .then_with(|| left.display_label.cmp(&right.display_label))
            .then_with(|| left.identity.cmp(&right.identity))
    });
    summary
}

fn native_service_capabilities() -> ServiceCapabilities {
    ServiceCapabilities {
        supported: [
            ServiceCapability::Subscriptions,
            ServiceCapability::MultipleClients,
            ServiceCapability::ArtifactTransfer,
            ServiceCapability::ExternalTools,
            ServiceCapability::InitialSessionTools,
            ServiceCapability::BuiltinToolAllowlists,
            ServiceCapability::VisionAvailability,
            ServiceCapability::SessionWorkingDirectories,
            ServiceCapability::InitialSessionModel,
            ServiceCapability::InitialSessionInstructions,
            ServiceCapability::SessionDeletion,
            ServiceCapability::QuestionTextAnswers,
            ServiceCapability::QueuedPromptSteering,
            ServiceCapability::ArchetypeManagement,
            ServiceCapability::InvocationTelemetry,
            ServiceCapability::SkillAvailability,
            ServiceCapability::SoulManagement,
            ServiceCapability::McpManagement,
            ServiceCapability::OrchestratorThreadBridge,
        ]
        .into_iter()
        .collect(),
    }
}

impl NativeServerHandle {
    pub(crate) const fn endpoint(&self) -> &ServerEndpoint {
        &self.endpoint
    }

    pub(crate) async fn quiesce(&self) -> Result<(), String> {
        let (respond, response) = tokio::sync::oneshot::channel();
        self.quiesce
            .send(QuiesceRequest { respond })
            .await
            .map_err(|_| "native runtime stopped before quiescence could be checked".to_owned())?;
        response
            .await
            .map_err(|_| "native runtime dropped the quiescence response".to_owned())?
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.shutdown.send(()).await;
    }
}

pub(crate) struct BackendRegistrySpawn {
    pub(crate) session_database: PathBuf,
    pub(crate) provider_credentials: HashMap<String, serde_json::Value>,
    pub(crate) web_config: Arc<RwLock<crate::web::WebConfig>>,
    pub(crate) memory_config: Arc<RwLock<crate::memory::MemoryConfig>>,
    pub(crate) vision_config: Arc<RwLock<crate::vision::VisionConfig>>,
    pub(crate) native_delegation: mpsc::Sender<NativeDelegationRequest>,
}

struct SessionBackendTasks {
    backend: tokio::task::JoinHandle<()>,
    event_forwarder: tokio::task::JoinHandle<()>,
}

pub(crate) struct BackendRegistry {
    /// Provider-scoped handles own authentication, readiness, and model catalogs.
    pub(crate) commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    /// Session-scoped handles own native sessions and turns. A provider adapter
    /// may supervise only the logical session named by this key.
    pub(crate) session_commands:
        HashMap<(nakode_protocol::SessionId, String), mpsc::Sender<BackendCommand>>,
    /// Backend and event-forwarding tasks retained by canonical session/provider identity so a
    /// destructive delete can await provider termination before removing durable history.
    session_tasks: HashMap<(nakode_protocol::SessionId, String), Vec<SessionBackendTasks>>,
    pub(crate) subagent_commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    pub(crate) subagent_providers: HashMap<String, String>,
    subagent_parents: HashMap<String, nakode_protocol::SessionId>,
    subagent_tasks: HashMap<String, Vec<SessionBackendTasks>>,
    pub(crate) events: mpsc::Receiver<(BackendSource, BackendEvent)>,
    pub(crate) event_tx: mpsc::Sender<(BackendSource, BackendEvent)>,
    pub(crate) tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) failures: Vec<(String, String)>,
    pub(crate) config: Config,
    pub(crate) session_database: PathBuf,
    pub(crate) provider_credentials: HashMap<String, serde_json::Value>,
    pub(crate) provider_cooldowns: HashMap<String, ProviderCooldown>,
    pub(crate) web_config: Arc<RwLock<crate::web::WebConfig>>,
    pub(crate) memory_config: Arc<RwLock<crate::memory::MemoryConfig>>,
    pub(crate) memory_services: Mutex<HashMap<String, crate::memory::SharedMemoryService>>,
    pub(crate) vision_config: Arc<RwLock<crate::vision::VisionConfig>>,
    pub(crate) vision_service: Option<crate::vision::SharedVisionService>,
    pub(crate) native_delegation: mpsc::Sender<NativeDelegationRequest>,
}

pub(crate) struct ProviderCooldown {
    pub(crate) until: Instant,
    pub(crate) reason: String,
}

const PROVIDER_FATAL_ERROR_COOLDOWN: Duration = Duration::from_secs(15 * 60);
const SHARED_PROVIDER_SYNC_INTERVAL: Duration = Duration::from_secs(2);

#[cfg(feature = "e2e-fixture-provider")]
fn e2e_codex_fixture() -> Option<PathBuf> {
    std::env::var_os("NAKODE_E2E_CODEX_FIXTURE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn enable_e2e_fixture_provider(providers: &mut [ProviderRecord]) {
    #[cfg(feature = "e2e-fixture-provider")]
    if e2e_codex_fixture().is_some()
        && let Some(provider) = providers
            .iter_mut()
            .find(|provider| provider.provider == crate::backend::CODEX_PROVIDER)
    {
        provider.enabled = true;
    }
    #[cfg(not(feature = "e2e-fixture-provider"))]
    let _ = providers;
}

impl BackendRegistry {
    pub(crate) fn current_web_config(&self) -> crate::web::WebConfig {
        read_shared_config(&self.web_config)
    }

    pub(crate) fn current_memory_config(&self) -> crate::memory::MemoryConfig {
        read_shared_config(&self.memory_config)
    }

    pub(crate) fn current_vision_config(&self) -> crate::vision::VisionConfig {
        read_shared_config(&self.vision_config)
    }

    pub(crate) fn available_builtin_tools(
        &self,
        providers: &[ProviderRecord],
        vision_provider: Option<&str>,
    ) -> HashMap<String, Vec<String>> {
        // Availability depends on the configured memory backend, not on a particular project's bank.
        // Provider execution replaces this catalogue-only service with the session access root's
        // shared service before any tool call can run.
        let catalogue_memory = Arc::new(crate::memory::MemoryService::new(
            Arc::clone(&self.memory_config),
            "availability".to_owned(),
        ));
        let runtime_tools = crate::tools::ToolRegistry::base()
            .with_browser(Arc::clone(&self.web_config))
            .with_memory(catalogue_memory)
            .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone())
            .with_native_delegation()
            .definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<std::collections::HashSet<_>>();
        let canonical = crate::agent::CANONICAL_AGENT_TOOLS
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>();

        providers
            .iter()
            .map(|provider| {
                let available = if !provider.enabled
                    || !self.commands.contains_key(&provider.provider)
                {
                    Vec::new()
                } else {
                    let supported = if matches!(
                        provider.provider.as_str(),
                        crate::backend::CODEX_PROVIDER
                            | crate::backend::DEVIN_PROVIDER
                            | crate::backend::KIMI_PROVIDER
                            | crate::backend::GLM_PROVIDER
                    ) {
                        canonical.clone()
                    } else {
                        let projection = crate::backend::project_provider_tools(
                            &provider.provider,
                            Some(&canonical),
                        );
                        canonical
                            .iter()
                            .filter(|name| !projection.unsupported_canonical_tools.contains(name))
                            .cloned()
                            .collect()
                    };
                    supported
                        .into_iter()
                        .filter(|name| {
                            runtime_tools.contains(name.as_str())
                                && (name != "vision"
                                    || vision_provider == Some(provider.provider.as_str())
                                        && provider.provider == crate::backend::CODEX_PROVIDER)
                        })
                        .collect()
                };
                (provider.provider.clone(), available)
            })
            .collect()
    }

    pub(crate) async fn spawn(
        config: &Config,
        providers: &[ProviderRecord],
        spawn: BackendRegistrySpawn,
    ) -> Self {
        let BackendRegistrySpawn {
            session_database,
            provider_credentials,
            web_config,
            memory_config,
            vision_config,
            native_delegation,
        } = spawn;
        let (event_tx, events) = mpsc::channel(512);
        let mut failures = Vec::new();
        let vision_service = match codex::vision_service(
            provider_credentials
                .get(crate::backend::CODEX_PROVIDER)
                .cloned(),
            Arc::clone(&vision_config),
        ) {
            Ok(service) => service,
            Err(error) => {
                failures.push((crate::backend::CODEX_PROVIDER.to_owned(), error.to_string()));
                None
            }
        };
        let mut registry = Self {
            commands: HashMap::new(),
            session_commands: HashMap::new(),
            session_tasks: HashMap::new(),
            subagent_commands: HashMap::new(),
            subagent_providers: HashMap::new(),
            subagent_parents: HashMap::new(),
            subagent_tasks: HashMap::new(),
            events,
            event_tx: event_tx.clone(),
            tasks: Vec::new(),
            failures,
            config: config.clone(),
            session_database,
            provider_credentials,
            provider_cooldowns: HashMap::new(),
            web_config,
            memory_config: Arc::clone(&memory_config),
            memory_services: Mutex::new(HashMap::new()),
            vision_config,
            vision_service,
            native_delegation,
        };
        for provider in providers.iter().filter(|provider| provider.enabled) {
            if let Err(error) = registry.start_provider(&provider.provider).await {
                registry
                    .failures
                    .push((provider.provider.clone(), error.to_string()));
            }
        }
        drop(event_tx);
        registry
    }

    pub(crate) async fn start_provider(&mut self, provider: &str) -> Result<(), BackendError> {
        if self.commands.contains_key(provider) {
            return Ok(());
        }
        let handle = self
            .spawn_provider_handle(provider, &self.config.workspace)
            .await?;
        self.insert_provider_control(provider.to_owned(), handle);
        Ok(())
    }

    async fn spawn_provider_handle(
        &self,
        provider: &str,
        working_directory: &Path,
    ) -> Result<BackendHandle, BackendError> {
        let credential = self.provider_credentials.get(provider).cloned();
        let memory_service = self.memory_service_for(working_directory).await;
        let handle = match provider {
            crate::backend::CODEX_PROVIDER => {
                #[cfg(feature = "e2e-fixture-provider")]
                if let Some(fixture) = e2e_codex_fixture() {
                    return codex::spawn_compatibility(codex::CompatibilityBackendConfig {
                        program: PathBuf::from("python3"),
                        args: vec![fixture.into_os_string()],
                        workspace: working_directory.to_path_buf(),
                        credential_home: None,
                    })
                    .await;
                }
                codex::spawn(
                    codex::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_reasoning_effort(self.config.openai_reasoning_effort.as_str())
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_native_delegation(self.native_delegation.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CLAUDE_PROVIDER => {
                claude::spawn(
                    claude::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CURSOR_PROVIDER => {
                cursor::spawn(
                    cursor::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::KIMI_PROVIDER => {
                kimi::spawn(
                    kimi::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_native_delegation(self.native_delegation.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::GLM_PROVIDER => {
                glm::spawn(
                    glm::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_native_delegation(self.native_delegation.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::DEVIN_PROVIDER => {
                devin::spawn(
                    devin::BackendConfig::native(working_directory.to_path_buf())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_native_delegation(self.native_delegation.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            _ => {
                return Err(BackendError::UnsupportedProvider {
                    provider: provider.to_owned(),
                });
            }
        };
        Ok(handle)
    }

    async fn memory_service_for(
        &self,
        working_directory: &Path,
    ) -> crate::memory::SharedMemoryService {
        let project_bank = crate::memory::project_bank(working_directory);
        let mut services = self.memory_services.lock().await;
        Arc::clone(services.entry(project_bank.clone()).or_insert_with(|| {
            Arc::new(crate::memory::MemoryService::new(
                Arc::clone(&self.memory_config),
                project_bank,
            ))
        }))
    }

    pub(crate) async fn stop_provider(&mut self, provider: &str) {
        self.stop_provider_control(provider).await;
        let mut session_keys = self
            .session_commands
            .keys()
            .filter(|(_, session_provider)| session_provider == provider)
            .cloned()
            .collect::<Vec<_>>();
        for key in self
            .session_tasks
            .keys()
            .filter(|(_, session_provider)| session_provider == provider)
        {
            if !session_keys.contains(key) {
                session_keys.push(key.clone());
            }
        }
        for key in session_keys {
            let _ = self.stop_session_backend(key).await;
        }
    }

    async fn stop_provider_control(&mut self, provider: &str) {
        if let Some(commands) = self.commands.remove(provider) {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
    }

    /// Shuts down every provider backend supervising one logical session.
    ///
    /// Keyed on the session alone, across providers: a session may have been served by more than one
    /// adapter over its life, and a delete that left either behind would leave a provider child
    /// writing to history that has gone. Idempotent — a session with no backend attached is the
    /// normal case for a dead one, and finding nothing to stop is a success.
    pub(crate) async fn stop_session(
        &mut self,
        session_id: &nakode_protocol::SessionId,
    ) -> Result<(), String> {
        let mut keys = self
            .session_commands
            .keys()
            .filter(|(id, _)| id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in self.session_tasks.keys().filter(|(id, _)| id == session_id) {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        for key in keys {
            self.stop_session_backend(key).await?;
        }
        let run_ids = self
            .subagent_parents
            .iter()
            .filter(|(_, parent)| *parent == session_id)
            .map(|(run_id, _)| run_id.clone())
            .collect::<Vec<_>>();
        for run_id in run_ids {
            self.stop_subagent(&run_id).await?;
        }
        Ok(())
    }

    async fn stop_session_backend(
        &mut self,
        key: (nakode_protocol::SessionId, String),
    ) -> Result<(), String> {
        if let Some(commands) = self.session_commands.get(&key).cloned() {
            match tokio::time::timeout(
                SESSION_BACKEND_STOP_TIMEOUT,
                commands.send(BackendCommand::Shutdown),
            )
            .await
            {
                Ok(_) => {
                    self.session_commands.remove(&key);
                }
                Err(_) => {
                    return Err(format!(
                        "timed out sending shutdown to {} backend for session {}",
                        key.1, key.0
                    ));
                }
            }
        }
        let Some(task_sets) = self.session_tasks.remove(&key) else {
            return Ok(());
        };
        let mut task_sets = task_sets.into_iter();
        while let Some(mut tasks) = task_sets.next() {
            // The event forwarder can be blocked sending into the runtime that is currently awaiting
            // this teardown. Abort it first; the provider then observes its receiver closing in
            // addition to the explicit Shutdown command.
            tasks.event_forwarder.abort();
            if tokio::time::timeout(SESSION_BACKEND_STOP_TIMEOUT, &mut tasks.backend)
                .await
                .is_err()
            {
                let mut retained = vec![tasks];
                retained.extend(task_sets);
                self.session_tasks.insert(key.clone(), retained);
                return Err(format!(
                    "timed out waiting for {} backend to stop for session {}",
                    key.1, key.0
                ));
            }
            let _ = tasks.event_forwarder.await;
        }
        Ok(())
    }

    /// Drops the join handles of provider-control and subagent supervisors that have already exited.
    ///
    /// Session handles are retained separately by identity and are removed synchronously by
    /// `stop_session`, so destructive deletion can await them rather than merely reaping them later.
    fn reap_finished_tasks(&mut self) {
        self.tasks.retain(|task| !task.is_finished());
    }

    pub(crate) fn set_provider_credential(&mut self, provider: &str, metadata: serde_json::Value) {
        self.provider_credentials
            .insert(provider.to_owned(), metadata.clone());
        if provider == crate::backend::CODEX_PROVIDER {
            match codex::vision_service(Some(metadata), Arc::clone(&self.vision_config)) {
                Ok(service) => self.vision_service = service,
                Err(error) => {
                    self.vision_service = None;
                    self.failures.push((provider.to_owned(), error.to_string()));
                }
            }
        }
    }

    fn insert_provider_control(&mut self, provider: String, handle: BackendHandle) {
        let (commands, mut events, task) = handle.into_parts();
        self.reap_finished_tasks();
        self.commands.insert(provider.clone(), commands);
        self.tasks.push(task);
        let event_tx = self.event_tx.clone();
        self.tasks.push(tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx
                    .send((BackendSource::ProviderControl(provider.clone()), event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }));
    }

    fn insert_session(
        &mut self,
        session_id: nakode_protocol::SessionId,
        provider: String,
        handle: BackendHandle,
    ) {
        let (commands, mut events, task) = handle.into_parts();
        let key = (session_id.clone(), provider.clone());
        self.session_commands.insert(key.clone(), commands);
        let event_tx = self.event_tx.clone();
        let event_forwarder = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx
                    .send((
                        BackendSource::Primary {
                            session_id: session_id.clone(),
                            provider: provider.clone(),
                        },
                        event,
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        self.session_tasks
            .entry(key)
            .or_default()
            .push(SessionBackendTasks {
                backend: task,
                event_forwarder,
            });
    }

    pub(crate) async fn spawn_subagent(
        &mut self,
        parent_session_id: nakode_protocol::SessionId,
        run_id: String,
        provider: &str,
        working_directory: &Path,
    ) -> Result<(), BackendError> {
        if let Some(cooldown) = self.active_cooldown(provider) {
            return Err(BackendError::ProviderCoolingDown {
                provider: provider.to_owned(),
                remaining_seconds: cooldown.0,
                reason: cooldown.1,
            });
        }
        if !self.commands.contains_key(provider) {
            return Err(BackendError::ProviderUnavailable {
                provider: provider.to_owned(),
            });
        }
        let handle = self
            .spawn_provider_handle(provider, working_directory)
            .await?;
        let (commands, mut events, task) = handle.into_parts();
        self.subagent_commands.insert(run_id.clone(), commands);
        self.subagent_providers
            .insert(run_id.clone(), provider.to_owned());
        self.subagent_parents
            .insert(run_id.clone(), parent_session_id);
        let event_tx = self.event_tx.clone();
        let forwarded_run_id = run_id.clone();
        let event_forwarder = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx
                    .send((BackendSource::Subagent(forwarded_run_id.clone()), event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        self.subagent_tasks
            .entry(run_id)
            .or_default()
            .push(SessionBackendTasks {
                backend: task,
                event_forwarder,
            });
        Ok(())
    }

    pub(crate) fn observe_provider_event(&mut self, source: &BackendSource, event: &BackendEvent) {
        let provider = match source {
            BackendSource::ProviderControl(provider) | BackendSource::Primary { provider, .. } => {
                Some(provider.clone())
            }
            BackendSource::Subagent(run_id) => self.subagent_providers.get(run_id).cloned(),
        };
        if matches!(
            event,
            BackendEvent::TurnCompleted {
                outcome: crate::backend::TurnOutcome::Completed,
                ..
            }
        ) {
            if let Some(provider) = provider {
                self.provider_cooldowns.remove(&provider);
            }
            return;
        }
        let (BackendEvent::TurnCompleted {
            outcome: crate::backend::TurnOutcome::Failed,
            error: Some(message),
            ..
        }
        | BackendEvent::RequestFailed { message, .. }
        | BackendEvent::Disconnected { reason: message }) = event
        else {
            return;
        };
        if !is_fatal_provider_error(message) {
            return;
        }
        if let Some(provider) = provider {
            self.provider_cooldowns.insert(
                provider,
                ProviderCooldown {
                    until: Instant::now() + PROVIDER_FATAL_ERROR_COOLDOWN,
                    reason: summarize_provider_error(message),
                },
            );
        }
    }

    pub(crate) fn active_cooldown(&mut self, provider: &str) -> Option<(u64, String)> {
        let now = Instant::now();
        if self
            .provider_cooldowns
            .get(provider)
            .is_some_and(|cooldown| cooldown.until <= now)
        {
            self.provider_cooldowns.remove(provider);
            return None;
        }
        self.provider_cooldowns.get(provider).map(|cooldown| {
            (
                cooldown.until.saturating_duration_since(now).as_secs(),
                cooldown.reason.clone(),
            )
        })
    }

    pub(crate) async fn send(&mut self, provider: &str, command: BackendCommand) -> bool {
        let Some(commands) = self.commands.get(provider) else {
            return false;
        };
        if commands.send(command).await.is_err() {
            self.commands.remove(provider);
            return false;
        }
        true
    }

    pub(crate) async fn send_session(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        provider: &str,
        working_directory: &Path,
        command: BackendCommand,
    ) -> Result<(), SessionBackendError> {
        if !self.commands.contains_key(provider) {
            return Err(BackendError::ProviderUnavailable {
                provider: provider.to_owned(),
            }
            .into());
        }
        let key = (session_id.clone(), provider.to_owned());
        if !self.session_commands.contains_key(&key) {
            let handle = self
                .spawn_provider_handle(provider, working_directory)
                .await?;
            self.insert_session(session_id.clone(), provider.to_owned(), handle);
        }
        let Some(commands) = self.session_commands.get(&key) else {
            return Err(BackendError::ProviderUnavailable {
                provider: provider.to_owned(),
            }
            .into());
        };
        if commands.send(command).await.is_err() {
            self.session_commands.remove(&key);
            return Err(SessionBackendError::CommandChannelClosed {
                session_id: session_id.clone(),
                provider: provider.to_owned(),
            });
        }
        Ok(())
    }

    pub(crate) async fn send_subagent(&self, run_id: &str, command: BackendCommand) -> bool {
        let Some(commands) = self.subagent_commands.get(run_id) else {
            return false;
        };
        commands.send(command).await.is_ok()
    }

    pub(crate) async fn stop_subagent(&mut self, run_id: &str) -> Result<(), String> {
        if let Some(commands) = self.subagent_commands.get(run_id).cloned() {
            match tokio::time::timeout(
                SESSION_BACKEND_STOP_TIMEOUT,
                commands.send(BackendCommand::Shutdown),
            )
            .await
            {
                Ok(_) => {
                    self.subagent_commands.remove(run_id);
                }
                Err(_) => {
                    return Err(format!("timed out sending shutdown to subagent {run_id}"));
                }
            }
        }
        let Some(task_sets) = self.subagent_tasks.remove(run_id) else {
            self.subagent_providers.remove(run_id);
            self.subagent_parents.remove(run_id);
            return Ok(());
        };
        let mut task_sets = task_sets.into_iter();
        while let Some(mut tasks) = task_sets.next() {
            tasks.event_forwarder.abort();
            if tokio::time::timeout(SESSION_BACKEND_STOP_TIMEOUT, &mut tasks.backend)
                .await
                .is_err()
            {
                let mut retained = vec![tasks];
                retained.extend(task_sets);
                self.subagent_tasks.insert(run_id.to_owned(), retained);
                return Err(format!("timed out waiting for subagent {run_id} to stop"));
            }
            let _ = tasks.event_forwarder.await;
        }
        self.subagent_providers.remove(run_id);
        self.subagent_parents.remove(run_id);
        Ok(())
    }

    pub(crate) async fn clear_provider_credential(&mut self, provider: &str) -> io::Result<()> {
        self.stop_provider(provider).await;
        let run_ids = self
            .subagent_providers
            .iter()
            .filter(|(_, run_provider)| run_provider.as_str() == provider)
            .map(|(run_id, _)| run_id.clone())
            .collect::<Vec<_>>();
        for run_id in run_ids {
            let _ = self.stop_subagent(&run_id).await;
        }
        self.provider_credentials.remove(provider);
        if provider == crate::backend::CODEX_PROVIDER {
            self.vision_service = None;
        }
        Ok(())
    }

    pub(crate) async fn shutdown(mut self) {
        for commands in self.commands.values() {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
        let mut session_ids = self
            .session_commands
            .keys()
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in self.session_tasks.keys().map(|(session_id, _)| session_id) {
            if !session_ids.contains(session_id) {
                session_ids.push(session_id.clone());
            }
        }
        for session_id in session_ids {
            let _ = self.stop_session(&session_id).await;
        }
        // Process shutdown cannot leave a timed-out session task detached. Cancellation drops the
        // provider future and its supervised child resources even when graceful shutdown did not
        // finish inside the mutation-oriented deadline.
        for (_, task_sets) in self.session_tasks.drain() {
            for tasks in task_sets {
                tasks.event_forwarder.abort();
                tasks.backend.abort();
                let _ = tasks.event_forwarder.await;
                let _ = tasks.backend.await;
            }
        }
        let mut run_ids = self.subagent_commands.keys().cloned().collect::<Vec<_>>();
        for run_id in self.subagent_tasks.keys() {
            if !run_ids.contains(run_id) {
                run_ids.push(run_id.clone());
            }
        }
        for run_id in run_ids {
            let _ = self.stop_subagent(&run_id).await;
        }
        for (_, task_sets) in self.subagent_tasks.drain() {
            for tasks in task_sets {
                tasks.event_forwarder.abort();
                tasks.backend.abort();
                let _ = tasks.event_forwarder.await;
                let _ = tasks.backend.await;
            }
        }
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

impl EffectExecutor {
    pub(crate) fn new(backends: BackendRegistry, persistence: PersistenceServices) -> Self {
        Self {
            backends,
            persistence,
            mcp: crate::mcp::McpClient::default(),
            shell_processes: ShellProcesses::new(),
        }
    }

    async fn execute(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        state: &mut DomainState,
        effects: Vec<Effect>,
        origin: EffectOrigin,
    ) {
        let mut pending = VecDeque::from(effects);
        while let Some(effect) = pending.pop_front() {
            self.execute_one(session_id, state, effect, &mut pending, origin)
                .await;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_one(
        &mut self,
        session_id: &nakode_protocol::SessionId,
        state: &mut DomainState,
        effect: Effect,
        pending: &mut VecDeque<Effect>,
        origin: EffectOrigin,
    ) {
        let sessions = self.persistence.sessions.as_ref();
        match effect {
            Effect::Backend(command) => {
                send_backend_command(session_id, state, &mut self.backends, command).await;
            }
            Effect::RunShell { id, command } => {
                self.shell_processes
                    .spawn(PathBuf::from(&state.working_directory), id, command);
            }
            Effect::CancelShell(id) => {
                if !self.shell_processes.cancel(&id) {
                    state.shell_failed(&id, "shell command is no longer running");
                }
            }
            Effect::SpawnSubagent { run_id, provider } => {
                spawn_subagent(state, &mut self.backends, pending, &run_id, &provider).await;
            }
            Effect::SubagentBackend { run_id, command } => {
                send_subagent_command(state, &self.backends, pending, &run_id, command).await;
            }
            Effect::StopSubagent(run_id) => {
                let _ = self.backends.stop_subagent(&run_id).await;
            }
            Effect::ReleaseSessionBackends(id) => {
                let _ = self
                    .backends
                    .stop_session(&nakode_protocol::SessionId::from(id))
                    .await;
            }
            Effect::CompleteAgentRequest { .. } => {
                // Run completion is projected through RunView.
            }
            #[cfg(test)]
            Effect::ListSessions | Effect::ListProviders | Effect::OpenUrl(_) | Effect::Quit => {}
            Effect::SetProviderModelFilter {
                provider,
                enabled,
                selected_model_ids,
            } => {
                if let Err(error) =
                    sessions.set_provider_model_filter(&provider, enabled, &selected_model_ids)
                {
                    state.session_store_failed(error.to_string());
                }
            }
            Effect::SetProviderEnabled { provider, enabled } => {
                self.set_provider_enabled(state, &provider, enabled).await;
            }
            Effect::AuthenticateProvider(provider) => {
                authenticate_provider(state, &mut self.backends, &provider).await;
            }
            Effect::SaveProviderCredential {
                provider,
                kind,
                metadata,
            } => {
                self.save_provider_credential_effect(
                    state,
                    pending,
                    ProviderCredentialInput {
                        provider,
                        kind,
                        metadata,
                    },
                    origin,
                )
                .await;
            }
            Effect::ClearProviderCredential(provider) => {
                self.clear_provider_credential_effect(state, &provider)
                    .await;
            }
            Effect::ReloadProvider(provider) => {
                self.reload_provider(state, &provider).await;
            }
            Effect::SaveAgent {
                definition,
                previous_slug,
            } => save_agent_effect(state, &definition, previous_slug.as_deref()),
            Effect::DeleteAgent(slug) => delete_agent_definition(state, &slug),
            Effect::ReloadConfiguration => apply_configuration_reload(state, pending),
            #[cfg(test)]
            Effect::ResolveSession(id) => resolve_session(state, sessions, pending, &id),
            effect @ (Effect::PersistSession { .. }
            | Effect::PersistSessionSkillSnapshot { .. }
            | Effect::PersistSessionBridge(_)
            | Effect::PersistModels { .. }
            | Effect::SetDefaultModel { .. }
            | Effect::SaveModelOptions { .. }
            | Effect::PersistSubagent(_)
            | Effect::PersistSubagentContinuation(_)
            | Effect::LoadSubagents(_)
            | Effect::UpdateSessionModel { .. }
            | Effect::TransitionSessionPrimary { .. }
            | Effect::UpdateSessionLastTurn { .. }
            | Effect::RecordOwnerActivity(_)
            | Effect::TouchSession(_)) => {
                execute_persistence_effect(state, sessions, effect);
            }
            Effect::DeleteSession(_) => {
                unreachable!("session deletion is a required pre-effect durability checkpoint")
            }
            Effect::SaveWebConfig(config) => {
                save_web_config(state, &self.backends, sessions, config);
            }
            Effect::SaveMemoryConfig(config) => {
                save_memory_config(state, &self.backends, sessions, config).await;
            }
            Effect::SaveVisionConfig(config) => {
                save_vision_config(state, &self.backends, sessions, config);
            }
            Effect::SaveTerminalImageMode(mode) => {
                save_terminal_image_mode(state, sessions, mode);
            }
            Effect::SaveInvocationTelemetryEnabled(enabled) => {
                save_invocation_telemetry_enabled(state, sessions, enabled);
            }
            Effect::RecordInvocation(invocation) => {
                if let Err(error) = sessions.record_invocation(&invocation) {
                    state.session_store_failed(error.to_string());
                }
            }
            Effect::CheckAgentBrowser => check_agent_browser(state).await,
            Effect::SaveMcpServer(_)
            | Effect::RefreshMcpServer(_)
            | Effect::DeleteMcpServer { .. }
            | Effect::SaveMcpCredential { .. }
            | Effect::ClearMcpCredential { .. } => {
                unreachable!("MCP authority effects are handled by NativeServerRuntime")
            }
        }
    }

    async fn set_provider_enabled(
        &mut self,
        state: &mut DomainState,
        provider: &str,
        enabled: bool,
    ) {
        apply_provider_enablement(
            state,
            &mut self.backends,
            self.persistence.sessions.as_ref(),
            provider,
            enabled,
        )
        .await;
    }

    async fn save_provider_credential_effect(
        &mut self,
        state: &mut DomainState,
        pending: &mut VecDeque<Effect>,
        credential: ProviderCredentialInput,
        origin: EffectOrigin,
    ) {
        save_provider_credential(
            state,
            &mut self.backends,
            &self.persistence,
            pending,
            credential,
            origin,
        )
        .await;
    }

    async fn clear_provider_credential_effect(&mut self, state: &mut DomainState, provider: &str) {
        clear_provider_credential(
            state,
            &mut self.backends,
            self.persistence.sessions.as_ref(),
            self.persistence.credentials.as_ref(),
            provider,
        )
        .await;
    }

    async fn reload_provider(&mut self, state: &mut DomainState, provider: &str) {
        if let Err(error) = self.backends.start_provider(provider).await {
            state.provider_authentication_failed(provider, &error.to_string());
            return;
        }
        if !self
            .backends
            .send(
                provider,
                BackendCommand::Reload {
                    provider_session_id: None,
                },
            )
            .await
        {
            state.provider_authentication_failed(provider, "provider refresh channel closed");
        }
    }

    pub(crate) fn handle_shell_event(state: &mut DomainState, event: ShellEvent) {
        match event {
            ShellEvent::Output { id, output } => state.shell_output(&id, &output),
            ShellEvent::Finished {
                id,
                output,
                exit_code,
                interrupted,
            } => state.shell_finished(&id, &output, exit_code, interrupted),
            ShellEvent::Failed { id, message } => state.shell_failed(&id, &message),
        }
    }

    pub(crate) async fn shutdown(mut self) {
        self.shell_processes.shutdown().await;
        self.backends.shutdown().await;
    }
}

fn save_agent_effect(
    state: &mut DomainState,
    definition: &crate::agent::AgentDefinition,
    previous_slug: Option<&str>,
) {
    save_agent_definition(state, definition, previous_slug);
}

fn bridge_inbound_event_identity(
    request: &nakode_server::ServerRequest,
) -> Option<(nakode_protocol::SessionId, String)> {
    let nakode_server::ServerRequest::Command {
        command:
            Command::ContinueSessionFromBridge {
                session_id,
                external_event_id,
                ..
            },
        ..
    } = request
    else {
        return None;
    };
    Some((session_id.clone(), external_event_id.clone()))
}

fn persist_bridge_effects(
    sessions: &dyn SessionRepository,
    effects: &mut Vec<Effect>,
    inbound_event_to_claim: Option<&(
        nakode_protocol::SessionId,
        String,
        BridgeContinuationDisposition,
    )>,
) -> Result<(), SessionError> {
    let mut bridges = Vec::new();
    let mut remaining = Vec::with_capacity(effects.len());
    for effect in effects.drain(..) {
        match effect {
            Effect::PersistSessionBridge(bridge) => bridges.push(bridge),
            effect => remaining.push(effect),
        }
    }
    let result = if bridges.is_empty() {
        Ok(())
    } else if let Some((session_id, external_event_id, disposition)) = inbound_event_to_claim {
        sessions.save_session_bridges_with_inbound_event(
            &bridges,
            session_id.as_str(),
            external_event_id,
            *disposition,
        )
    } else {
        sessions.save_session_bridges(&bridges)
    };
    if let Err(error) = result {
        // The runtime fences this command before provider work or success acknowledgement. Retain
        // non-bridge effects only for diagnostic ownership; they must not be executed by the caller.
        *effects = remaining;
        return Err(error);
    }
    *effects = remaining;
    Ok(())
}

fn execute_persistence_effect(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    effect: Effect,
) {
    match effect {
        Effect::PersistSession {
            provider,
            provider_session_id,
            workspace,
            working_directory,
            title,
            model,
            options,
        } => persist_session(
            state,
            sessions,
            &provider,
            &provider_session_id,
            &workspace,
            &working_directory,
            &title,
            model.as_deref(),
            &options,
        ),
        Effect::PersistSessionSkillSnapshot {
            session_id,
            enabled_skill_ids,
        } => {
            if let Err(error) = sessions.set_session_skill_snapshot(&session_id, &enabled_skill_ids)
            {
                state.session_store_failed(error.to_string());
            }
        }
        Effect::PersistModels { provider, models } => {
            persist_models(state, sessions, &provider, &models);
        }
        Effect::SetDefaultModel { provider, model } => {
            set_default_model(state, sessions, &provider, &model);
        }
        Effect::SaveModelOptions {
            provider,
            model,
            options,
        } => save_model_options(state, sessions, &provider, &model, &options),
        Effect::PersistSubagent(record) => persist_subagent(state, sessions, &record),
        Effect::PersistSubagentContinuation(records) => {
            persist_subagent_continuation(state, sessions, &records.0, &records.1);
        }
        Effect::LoadSubagents(parent_session_id) => {
            load_subagents(state, sessions, &parent_session_id);
        }
        Effect::UpdateSessionModel {
            session_id,
            model,
            options,
        } => {
            update_session_model(state, sessions, &session_id, model.as_deref(), &options);
        }
        Effect::TransitionSessionPrimary {
            session_id,
            provider,
            provider_session_id,
            model,
            options,
        } => transition_session_primary(
            state,
            sessions,
            &session_id,
            &provider,
            &provider_session_id,
            model.as_deref(),
            &options,
        ),
        Effect::UpdateSessionLastTurn { session_id, turn } => {
            update_session_last_turn(state, sessions, &session_id, &turn);
        }
        Effect::RecordOwnerActivity(id) => record_owner_activity(state, sessions, &id),
        Effect::TouchSession(id) => touch_session(state, sessions, &id),
        Effect::PersistSessionBridge(bridge) => {
            if let Err(error) = sessions.save_session_bridge(&bridge) {
                state.session_store_failed(error.to_string());
            }
        }
        _ => unreachable!("only persistence effects are routed here"),
    }
}

#[allow(clippy::too_many_arguments)]
fn persist_session(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    provider: &str,
    provider_session_id: &str,
    workspace: &str,
    working_directory: &str,
    title: &str,
    model: Option<&str>,
    options: &crate::backend::ModelOptions,
) {
    let enabled_skill_ids = state.enabled_skill_ids();
    match sessions.create_with_id(
        &state.nakode_session_id,
        provider,
        provider_session_id,
        workspace,
        working_directory,
        title,
        model,
        options,
        Some(&enabled_skill_ids),
    ) {
        Ok(record) => {
            if let Some(profile_id) = state.skill_profile_id().map(str::to_owned)
                && let Err(error) = sessions.bind_session_skill_profile(&record.id, &profile_id)
            {
                state.session_store_failed(error.to_string());
                return;
            }
            state.session_persisted(&record);
        }
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn persist_models(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    provider: &str,
    models: &[crate::backend::ModelInfo],
) {
    if let Err(error) = sessions.replace_models(provider, models) {
        state.session_store_failed(error.to_string());
        return;
    }
    match sessions.list_models(provider) {
        Ok(models) => state.install_persisted_model_preferences(models),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn set_default_model(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    provider: &str,
    model: &str,
) {
    if let Err(error) = sessions.set_default_model(provider, model) {
        state.session_store_failed(error.to_string());
    }
}

fn save_model_options(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    provider: &str,
    model: &str,
    options: &crate::backend::ModelOptions,
) {
    if let Err(error) = sessions.save_model_options(provider, model, options) {
        state.session_store_failed(error.to_string());
    }
}

async fn send_backend_command(
    session_id: &nakode_protocol::SessionId,
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    command: BackendCommand,
) {
    let provider = state.backend_provider.clone();
    if let Err(error) = backends
        .send_session(
            session_id,
            &provider,
            Path::new(&state.working_directory),
            command,
        )
        .await
    {
        state.handle_provider_backend(
            &provider,
            BackendEvent::Disconnected {
                reason: error.to_string(),
            },
        );
    }
}

async fn spawn_subagent(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    pending: &mut VecDeque<Effect>,
    run_id: &str,
    provider: &str,
) {
    if let Err(error) = backends
        .spawn_subagent(
            nakode_protocol::SessionId::from(state.nakode_session_id.clone()),
            run_id.to_owned(),
            provider,
            Path::new(&state.working_directory),
        )
        .await
    {
        pending.extend(state.subagent_launch_failed(run_id, error.to_string()));
    }
}

async fn send_subagent_command(
    state: &mut DomainState,
    backends: &BackendRegistry,
    pending: &mut VecDeque<Effect>,
    run_id: &str,
    command: BackendCommand,
) {
    if !backends.send_subagent(run_id, command).await {
        pending.extend(
            state.subagent_launch_failed(run_id, "subagent command channel closed".to_owned()),
        );
    }
}

async fn authenticate_provider(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    provider: &str,
) {
    if let Err(error) = backends.start_provider(provider).await {
        state.provider_authentication_failed(provider, &error.to_string());
    } else if !backends
        .send(provider, BackendCommand::BeginAuthentication)
        .await
    {
        state.provider_authentication_failed(provider, "provider authentication channel closed");
    }
}

struct ProviderCredentialInput {
    provider: String,
    kind: String,
    metadata: serde_json::Value,
}

async fn save_provider_credential(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    persistence: &PersistenceServices,
    pending: &mut VecDeque<Effect>,
    credential: ProviderCredentialInput,
    origin: EffectOrigin,
) {
    let stored = Credential {
        kind: credential.kind,
        secret: SecretValue::new(credential.metadata.clone()),
    };
    if let Err(error) = persistence.credentials.put(&credential.provider, &stored) {
        state.session_store_failed(error.to_string());
        return;
    }
    backends.set_provider_credential(&credential.provider, credential.metadata);
    match origin {
        EffectOrigin::ClientCommand => backends.stop_provider(&credential.provider).await,
        EffectOrigin::ProviderControl | EffectOrigin::PrimarySession | EffectOrigin::Subagent => {
            // Provider events can carry a refreshed token from a live session adapter.
            // Replace only the catalog/auth control handle; the adapter that owns an
            // active logical session already has the refreshed token and must continue.
            backends.stop_provider_control(&credential.provider).await;
        }
    }
    pending.push_back(Effect::SetProviderEnabled {
        provider: credential.provider,
        enabled: true,
    });
}

async fn clear_provider_credential(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    credentials: &dyn CredentialStore,
    provider: &str,
) {
    if let Err(error) = backends.clear_provider_credential(provider).await {
        state.session_store_failed(format!("could not clear {provider} credentials: {error}"));
        return;
    }
    if let Err(error) = sessions.set_provider_enabled(provider, false) {
        state.session_store_failed(error.to_string());
        return;
    }
    if let Err(error) = sessions.replace_models(provider, &[]) {
        state.session_store_failed(error.to_string());
        return;
    }
    if let Err(error) = credentials.delete(provider) {
        state.session_store_failed(error.to_string());
        return;
    }
    state.provider_logged_out(provider);
}

async fn apply_provider_enablement(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    provider: &str,
    enabled: bool,
) {
    if let Err(error) = sessions.set_provider_enabled(provider, enabled) {
        state.session_store_failed(error.to_string());
        return;
    }
    let providers = match sessions.list_providers() {
        Ok(providers) => providers,
        Err(error) => {
            state.session_store_failed(error.to_string());
            return;
        }
    };
    let display_name = providers
        .iter()
        .find(|record| record.provider == provider)
        .map_or_else(|| provider.to_owned(), |record| record.display_name.clone());
    if !enabled {
        backends.stop_provider(provider).await;
        state.provider_disabled(provider);
        return;
    }

    state.provider_starting(provider, &display_name);
    if let Err(error) = backends.start_provider(provider).await {
        state.provider_start_failed(provider, &display_name, &error.to_string());
        return;
    }
    match sessions.list_model_options(provider) {
        Ok(profiles) => state.install_model_option_profiles(provider, profiles),
        Err(error) => state.session_store_failed(error.to_string()),
    }
    match sessions.list_models(provider) {
        Ok(models) => state.install_cached_models(models),
        Err(error) => state.session_store_failed(error.to_string()),
    }
    let _ = backends
        .send(
            provider,
            BackendCommand::Reload {
                provider_session_id: None,
            },
        )
        .await;
}

#[cfg(test)]
fn resolve_session(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    pending: &mut VecDeque<Effect>,
    id: &str,
) {
    match sessions.find(id) {
        Ok(Some(record)) => pending.extend(state.begin_resume(record)),
        Ok(None) => state.session_store_failed(format!("no session matches {id:?}")),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn apply_configuration_reload(state: &mut DomainState, pending: &mut VecDeque<Effect>) {
    let reload_backend = state.connection.is_ready() && !state.backend_provider.is_empty();
    let session_id = state.provider_session_id.clone();
    match reload_local_configuration(state) {
        Ok((agent_count, skill_count)) => {
            state.configuration_reloaded(agent_count, skill_count, reload_backend);
            if reload_backend {
                pending.push_front(Effect::Backend(BackendCommand::Reload {
                    provider_session_id: session_id,
                }));
            }
        }
        Err(error) => state.configuration_reload_failed(&error),
    }
}

pub(crate) fn reload_local_configuration(
    state: &mut DomainState,
) -> Result<(usize, usize), String> {
    let agents = AgentCatalog::load(state.agent_directory())
        .map_err(|error| format!("could not reload agents: {error}"))?;
    let skills = SkillCatalog::load(Path::new(&state.workspace))
        .map_err(|error| format!("could not reload skills: {error}"))?;
    let agent_count = agents.definitions().len();
    let skill_count = skills.definitions().len();
    state
        .reload_prompt_addenda()
        .map_err(|error| format!("could not reload personalities or Soul: {error}"))?;
    state.install_agents(agents);
    state.install_skills(skills);
    Ok((agent_count, skill_count))
}

pub(crate) fn save_agent_definition(
    state: &mut DomainState,
    definition: &crate::agent::AgentDefinition,
    previous_slug: Option<&str>,
) -> bool {
    let directory = state.agent_directory().to_path_buf();
    let result = AgentCatalog::load(&directory).and_then(|catalog| {
        catalog.save(&directory, definition, previous_slug)?;
        AgentCatalog::load(&directory)
    });
    match result {
        Ok(catalog) => {
            state.install_agents(catalog);
            state.set_status("Agent changes saved.");
            true
        }
        Err(error) => {
            state.session_store_failed(error.to_string());
            false
        }
    }
}

fn delete_agent_definition(state: &mut DomainState, slug: &str) {
    let directory = state.agent_directory().to_path_buf();
    let result = AgentCatalog::load(&directory).and_then(|catalog| {
        catalog.delete(&directory, slug)?;
        AgentCatalog::load(&directory)
    });
    install_changed_agent_catalog(state, result, "Agent archetype deleted.");
}

fn install_changed_agent_catalog(
    state: &mut DomainState,
    result: Result<AgentCatalog, AgentCatalogError>,
    success_message: &str,
) {
    match result {
        Ok(catalog) => {
            state.install_agents(catalog);
            state.set_status(success_message);
        }
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn take_delete_session_effect(effects: &mut Vec<Effect>) -> Option<String> {
    let index = effects
        .iter()
        .position(|effect| matches!(effect, Effect::DeleteSession(_)))?;
    let Effect::DeleteSession(session_id) = effects.remove(index) else {
        unreachable!("the located effect is a session deletion")
    };
    debug_assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::DeleteSession(_))),
        "one command must not delete multiple sessions"
    );
    Some(session_id)
}

fn remove_session_release_effect(effects: &mut Vec<Effect>, session_id: &str) {
    effects.retain(|effect| {
        !matches!(effect, Effect::ReleaseSessionBackends(released) if released == session_id)
    });
}

fn record_owner_activity(state: &mut DomainState, sessions: &dyn SessionRepository, id: &str) {
    if let Err(error) = sessions.record_owner_activity(id) {
        state.session_store_failed(error.to_string());
    }
}

fn touch_session(state: &mut DomainState, sessions: &dyn SessionRepository, id: &str) {
    if let Err(error) = sessions.touch(id) {
        state.session_store_failed(error.to_string());
    }
}

fn update_session_model(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    id: &str,
    model: Option<&str>,
    options: &crate::backend::ModelOptions,
) {
    if let Err(error) = sessions.update_model(id, model, options) {
        state.session_store_failed(error.to_string());
    }
}

#[allow(clippy::too_many_arguments)]
fn transition_session_primary(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    id: &str,
    provider: &str,
    provider_session_id: &str,
    model: Option<&str>,
    options: &crate::backend::ModelOptions,
) {
    if let Err(error) =
        sessions.transition_primary(id, provider, provider_session_id, model, options)
    {
        state.session_store_failed(error.to_string());
    }
}

fn update_session_last_turn(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    id: &str,
    turn: &crate::session::PersistedTurnConfiguration,
) {
    if let Err(error) = sessions.update_last_turn(id, turn) {
        state.session_store_failed(error.to_string());
    }
}

fn save_web_config(
    state: &mut DomainState,
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
    config: crate::web::WebConfig,
) {
    if let Err(error) = sessions.save_web_config(&config) {
        state.session_store_failed(error.to_string());
        return;
    }
    if let Err(error) = replace_shared_config(&backends.web_config, config.clone(), "browser") {
        state.session_store_failed(error);
        return;
    }
    state.install_web_config(config);
    state.set_status("Browser add-on settings saved.");
}

async fn save_memory_config(
    state: &mut DomainState,
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
    config: crate::memory::MemoryConfig,
) {
    if let Err(error) = sessions.save_memory_config(&config) {
        state.session_store_failed(error.to_string());
        return;
    }
    if let Err(error) = replace_shared_config(&backends.memory_config, config.clone(), "memory") {
        state.session_store_failed(error);
        return;
    }
    state.install_memory_config(config);
    let memory_services = {
        let services = backends.memory_services.lock().await;
        services.values().cloned().collect::<Vec<_>>()
    };
    for service in memory_services {
        service.reset().await;
    }
    state.set_status("Memory add-on settings saved.");
}

fn save_vision_config(
    state: &mut DomainState,
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
    config: crate::vision::VisionConfig,
) {
    if let Err(error) = sessions.save_vision_config(&config) {
        state.session_store_failed(error.to_string());
        return;
    }
    if let Err(error) = replace_shared_config(&backends.vision_config, config.clone(), "vision") {
        state.session_store_failed(error);
        return;
    }
    state.install_vision_config(config);
    state.set_status("Vision add-on settings saved.");
}

fn replace_shared_config<T>(shared: &RwLock<T>, config: T, name: &str) -> Result<(), String> {
    let mut current = shared
        .write()
        .map_err(|_| format!("{name} settings lock is unavailable"))?;
    *current = config;
    Ok(())
}

fn save_invocation_telemetry_enabled(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    enabled: bool,
) {
    if let Err(error) = sessions.save_invocation_telemetry_enabled(enabled) {
        state.session_store_failed(error.to_string());
        return;
    }
    state.install_invocation_telemetry_enabled(enabled);
    state.set_status(if enabled {
        "Local invocation telemetry enabled."
    } else {
        "Local invocation telemetry disabled."
    });
}

fn save_terminal_image_mode(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    mode: crate::settings::TerminalImageMode,
) {
    if let Err(error) = sessions.save_terminal_image_mode(mode) {
        state.session_store_failed(error.to_string());
        return;
    }
    state.install_terminal_image_mode(mode);
    state.set_status("Terminal image setting saved; changes apply on next launch.");
}

fn persist_subagent(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    record: &crate::session::SubagentRecord,
) {
    if let Err(error) = sessions.save_subagent(record) {
        state.session_store_failed(error.to_string());
    }
}

fn persist_subagent_continuation(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    source: &crate::session::SubagentRecord,
    successor: &crate::session::SubagentRecord,
) {
    if let Err(error) = sessions.save_subagent_continuation(source, successor) {
        state.session_store_failed(error.to_string());
    }
}

fn load_subagents(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    parent_session_id: &str,
) {
    match sessions.list_subagents(parent_session_id) {
        Ok(records) => {
            for corrected in state.install_subagents(records) {
                if let Err(error) = sessions.save_subagent(&corrected) {
                    state.session_store_failed(error.to_string());
                    return;
                }
            }
        }
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

async fn check_agent_browser(state: &mut DomainState) {
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::process::Command::new("agent-browser")
            .arg("--version")
            .kill_on_drop(true)
            .output(),
    )
    .await;
    let status = match result {
        Ok(Ok(output)) if output.status.success() => {
            let text = if output.stdout.is_empty() {
                &output.stderr
            } else {
                &output.stdout
            };
            let version = String::from_utf8_lossy(text)
                .lines()
                .next()
                .unwrap_or("installed")
                .trim()
                .chars()
                .take(80)
                .collect::<String>();
            AgentBrowserStatus::Available(if version.is_empty() {
                "installed".to_owned()
            } else {
                version
            })
        }
        _ => AgentBrowserStatus::Unavailable,
    };
    state.set_agent_browser_status(status);
}

fn initial_state(
    config: &Config,
    providers: &[ProviderRecord],
    backends: &BackendRegistry,
    agents: AgentCatalog,
    skills: SkillCatalog,
) -> DomainState {
    let active_provider = if backends
        .commands
        .contains_key(crate::backend::CODEX_PROVIDER)
    {
        crate::backend::CODEX_PROVIDER.to_owned()
    } else {
        backends.commands.keys().next().cloned().unwrap_or_default()
    };
    let mut state = if active_provider.is_empty() {
        DomainState::new_unconfigured(
            config.workspace.to_string_lossy(),
            config.model.clone(),
            config.scrollback,
        )
    } else {
        let active_name = providers
            .iter()
            .find(|record| record.provider == active_provider)
            .map_or(active_provider.as_str(), |record| {
                record.display_name.as_str()
            });
        DomainState::new_for_backend(
            config.workspace.to_string_lossy(),
            config.model.clone(),
            config.scrollback,
            &active_provider,
            active_name,
        )
    };
    state.set_default_model_options(crate::backend::ModelOptions {
        reasoning_effort: Some(config.openai_reasoning_effort.as_str().to_owned()),
        fast_mode: false,
    });
    state.install_web_config(backends.current_web_config());
    state.install_memory_config(backends.current_memory_config());
    state.install_vision_config(backends.current_vision_config());
    state.install_agents(agents);
    state.install_skills(skills);
    state.set_agent_directory(config.agents.clone());
    for (provider, error) in &backends.failures {
        let display_name = providers
            .iter()
            .find(|record| record.provider == *provider)
            .map_or(provider.as_str(), |record| record.display_name.as_str());
        state.provider_start_failed(provider, display_name, error);
    }
    state
}

async fn load_cached_provider_configuration(
    state: &mut DomainState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
) {
    let providers = backends.commands.keys().cloned().collect::<Vec<_>>();
    for provider in providers {
        match sessions.list_model_options(&provider) {
            Ok(profiles) => state.install_model_option_profiles(&provider, profiles),
            Err(error) => state.session_store_failed(error.to_string()),
        }
        match sessions.list_models(&provider) {
            Ok(models) => state.install_cached_models(models),
            Err(error) => state.session_store_failed(error.to_string()),
        }
        let _ = backends
            .send(
                &provider,
                BackendCommand::Reload {
                    provider_session_id: None,
                },
            )
            .await;
    }
}

fn load_provider_credentials(
    providers: &[ProviderRecord],
    credentials: &dyn CredentialStore,
) -> (HashMap<String, serde_json::Value>, Vec<(String, String)>) {
    let mut failures = Vec::new();
    let loaded = providers
        .iter()
        .filter(|provider| provider.credential.is_some())
        .filter_map(|provider| match credentials.get(&provider.provider) {
            Ok(Some(credential)) => {
                Some((provider.provider.clone(), credential.secret.into_inner()))
            }
            Ok(None) => None,
            Err(error) => {
                failures.push((provider.provider.clone(), error.to_string()));
                None
            }
        })
        .collect();
    (loaded, failures)
}

fn shared_web_config(
    sessions: &dyn SessionRepository,
) -> Result<Arc<RwLock<crate::web::WebConfig>>, SessionError> {
    sessions
        .load_web_config()
        .map(|config| Arc::new(RwLock::new(config)))
}

fn shared_memory_config(
    sessions: &dyn SessionRepository,
) -> Result<Arc<RwLock<crate::memory::MemoryConfig>>, SessionError> {
    sessions
        .load_memory_config()
        .map(|config| Arc::new(RwLock::new(config)))
}

fn shared_vision_config(
    sessions: &dyn SessionRepository,
) -> Result<Arc<RwLock<crate::vision::VisionConfig>>, SessionError> {
    sessions
        .load_vision_config()
        .map(|config| Arc::new(RwLock::new(config)))
}

fn read_shared_config<T: Clone + Default>(shared: &RwLock<T>) -> T {
    shared
        .read()
        .map_or_else(|_| T::default(), |config| config.clone())
}

fn is_fatal_provider_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "quota has been exhausted",
        "usage quota",
        "resource_exhausted",
        "invalid api key",
        "invalid credential",
        "authentication failed",
        "unauthenticated",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

fn summarize_provider_error(message: &str) -> String {
    const MAX_CHARS: usize = 240;
    let compact = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CHARS {
        compact
    } else {
        format!("{}…", compact.chars().take(MAX_CHARS).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet, VecDeque},
        path::Path,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    };

    use nakode_protocol::{
        BridgeContinuationDisposition, BridgeLifecycle, ClientId, Command, CredentialInput,
        ErrorCode, IdempotencyKey, InvocationKind, InvocationSummary, InvocationUsage,
        McpGrantPolicy, OrchestratorKind, PromptInput, Query, QueryResult, ServiceCapability,
        SessionId, TranscriptEntryStatus,
    };
    use tokio::sync::mpsc;

    use super::{
        BackendRegistry, BackendSource, EffectExecutor, EffectOrigin, McpCallCompletion,
        NativeServerRuntime, PendingMcpCall, PendingNativeDelegation, PersistenceServices,
        ProviderCredentialInput, SessionBackendTasks, load_subagents, merge_invocation_catalogue,
        native_service_capabilities, provider_enablement_changes, save_provider_credential,
    };
    use crate::{
        backend::{
            BackendCapabilities, BackendCommand, BackendEvent, BackendHandle, BackendIdentity,
            CODEX_PROVIDER, DEVIN_PROVIDER, ModelInfo,
        },
        config::{Config, OpenAiReasoningEffort},
        credential::{CredentialStore, SqliteCredentialStore},
        domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
        service::ServiceEngine,
        session::{
            BridgeInboundTurnOriginRecord, BridgePendingInboundRecord, InvocationRecord,
            SessionBridgeRecord, SessionRepository, SqliteSessionRepository, SubagentObservability,
            SubagentRecord,
        },
        state::{DomainState, Effect, SubagentStatus},
    };

    fn provider(provider: &str, enabled: bool, credential: bool) -> crate::session::ProviderRecord {
        crate::session::ProviderRecord {
            provider: provider.to_owned(),
            display_name: provider.to_owned(),
            enabled,
            credential: credential.then(|| crate::credential::CredentialMetadata {
                provider: provider.to_owned(),
                kind: "api-key".to_owned(),
                updated_at: 1,
            }),
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
        }
    }

    #[test]
    fn invocation_catalogue_merge_preserves_history_refreshes_labels_and_adds_zero_use_items() {
        let summary = InvocationSummary {
            enabled: true,
            items: vec![
                InvocationUsage {
                    kind: InvocationKind::Archetype,
                    identity: "installed".to_owned(),
                    display_label: "Old label".to_owned(),
                    currently_installed: false,
                    invocation_count: 3,
                    first_used_at_ms: Some(10),
                    last_used_at_ms: Some(30),
                },
                InvocationUsage {
                    kind: InvocationKind::Skill,
                    identity: "deleted".to_owned(),
                    display_label: "Deleted skill".to_owned(),
                    currently_installed: false,
                    invocation_count: 1,
                    first_used_at_ms: Some(20),
                    last_used_at_ms: Some(20),
                },
            ],
        };
        let merged = merge_invocation_catalogue(
            summary,
            vec![
                (
                    InvocationKind::Skill,
                    "zero".to_owned(),
                    "Zero skill".to_owned(),
                ),
                (
                    InvocationKind::Archetype,
                    "installed".to_owned(),
                    "Current label".to_owned(),
                ),
            ],
        );

        assert_eq!(merged.items.len(), 3);
        let installed = merged
            .items
            .iter()
            .find(|item| item.identity == "installed")
            .expect("installed item");
        assert!(installed.currently_installed);
        assert_eq!(installed.display_label, "Current label");
        assert_eq!(installed.invocation_count, 3);
        let deleted = merged
            .items
            .iter()
            .find(|item| item.identity == "deleted")
            .expect("historical item");
        assert!(!deleted.currently_installed);
        let zero = merged
            .items
            .iter()
            .find(|item| item.identity == "zero")
            .expect("zero-use item");
        assert!(zero.currently_installed);
        assert_eq!(zero.invocation_count, 0);
        assert_eq!(zero.first_used_at_ms, None);
    }

    #[test]
    fn restoring_active_subagents_durably_marks_them_interrupted() {
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = SqliteSessionRepository::open(workspace.path().join("sessions.sqlite3"))
            .expect("session repository");
        let parent = sessions
            .create(
                CODEX_PROVIDER,
                "provider-parent",
                workspace.path().to_str().expect("utf8 workspace"),
                "Parent",
                None,
            )
            .expect("parent session");
        sessions
            .save_subagent(&SubagentRecord {
                parent_session_id: parent.id.clone(),
                id: "run-active".to_owned(),
                agent: "repo-explorer".to_owned(),
                provider: CODEX_PROVIDER.to_owned(),
                model: None,
                provider_session_id: Some("provider-child".to_owned()),
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
                objective: "Inspect restore".to_owned(),
                status: SubagentStatus::Working,
                latest_activity: "Working".to_owned(),
                transcript: Vec::new(),
                observability: SubagentObservability {
                    started_at_ms: 100,
                    ..SubagentObservability::default()
                },
            })
            .expect("active run");

        let mut state = DomainState::new_unconfigured(
            workspace.path().to_str().expect("utf8 workspace"),
            None,
            100,
        );
        load_subagents(&mut state, &sessions, &parent.id);

        let restored = sessions
            .list_subagents(&parent.id)
            .expect("restored runs")
            .pop()
            .expect("run");
        assert_eq!(restored.status, SubagentStatus::Interrupted);
        assert_eq!(
            restored.observability.termination_kind.as_deref(),
            Some("interrupted")
        );
        assert!(restored.observability.ended_at_ms.is_some());
        assert_eq!(state.subagents[0].status, SubagentStatus::Interrupted);
    }

    #[test]
    fn restoring_active_subagents_durably_persists_verified_interrupted_salvage() {
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = SqliteSessionRepository::open(workspace.path().join("sessions.sqlite3"))
            .expect("session repository");
        let parent = sessions
            .create(
                CODEX_PROVIDER,
                "provider-parent",
                workspace.path().to_str().expect("utf8 workspace"),
                "Parent",
                None,
            )
            .expect("parent session");
        sessions
            .save_subagent(&SubagentRecord {
                parent_session_id: parent.id.clone(),
                id: "run-active-evidence".to_owned(),
                agent: "repo-explorer".to_owned(),
                provider: CODEX_PROVIDER.to_owned(),
                model: None,
                provider_session_id: Some("provider-child".to_owned()),
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
                objective: "Inspect restore salvage".to_owned(),
                status: SubagentStatus::Working,
                latest_activity: "Working".to_owned(),
                transcript: vec![TranscriptEntry {
                    id: "tool-evidence".to_owned(),
                    key: None,
                    kind: EntryKind::Tool,
                    title: "read lifecycle".to_owned(),
                    body: "authoritative retained evidence".to_owned(),
                    status: EntryStatus::Complete,
                    created_at_ms: Some(101),
                    provider_id: Some(CODEX_PROVIDER.to_owned()),
                    model_id: None,
                    owner_turn_id: None,
                    reasoning_effort: None,
                    fast_mode: None,
                    source_transport: None,
                    tool_audit_json: None,
                }],
                observability: SubagentObservability {
                    started_at_ms: 100,
                    ..SubagentObservability::default()
                },
            })
            .expect("active run");

        let mut state = DomainState::new_unconfigured(
            workspace.path().to_str().expect("utf8 workspace"),
            None,
            100,
        );
        load_subagents(&mut state, &sessions, &parent.id);

        let restored = sessions
            .list_subagents(&parent.id)
            .expect("restored runs")
            .pop()
            .expect("run");
        assert_eq!(restored.status, SubagentStatus::Interrupted);
        let salvage = restored.observability.salvage.expect("persisted salvage");
        assert_eq!(salvage.verified_evidence.len(), 1);
        assert_eq!(
            salvage.verified_evidence[0].body,
            "authoritative retained evidence"
        );
        assert_eq!(state.subagents[0].status, SubagentStatus::Interrupted);
        assert!(state.subagents[0].observability.salvage.is_some());
    }

    #[test]
    fn shared_provider_sync_detects_enablement_without_restarting_for_metadata() {
        let current = vec![
            provider("openai-codex", true, false),
            provider("zai-coding", false, false),
        ];
        let shared = vec![
            provider("openai-codex", true, true),
            provider("zai-coding", true, true),
        ];

        assert_eq!(
            provider_enablement_changes(&current, &shared),
            vec![("zai-coding".to_owned(), true)]
        );
    }

    fn config_for(workspace: &Path) -> Config {
        Config {
            command: None,
            tui: false,
            update: false,
            workspace: workspace.to_path_buf(),
            model: None,
            resume: None,
            scrollback: 2_000,
            compaction_threshold_percent: 85,
            openai_reasoning_effort: OpenAiReasoningEffort::Medium,
            personalities: None,
            soul: None,
            agents: workspace.join(".nakode/agents"),
        }
    }

    async fn empty_registry(workspace: &Path) -> BackendRegistry {
        let web_config = Arc::new(RwLock::new(crate::web::WebConfig::default()));
        let memory_config = Arc::new(RwLock::new(crate::memory::MemoryConfig::default()));
        let vision_config = Arc::new(RwLock::new(crate::vision::VisionConfig::default()));
        let (delegation, _requests) = mpsc::channel(1);
        BackendRegistry::spawn(
            &config_for(workspace),
            &[],
            super::BackendRegistrySpawn {
                session_database: workspace.join("sessions.sqlite3"),
                provider_credentials: HashMap::new(),
                web_config,
                memory_config,
                vision_config,
                native_delegation: delegation,
            },
        )
        .await
    }

    #[tokio::test]
    async fn builtin_availability_requires_addon_enablement_and_runtime_readiness() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let (commands, _command_rx) = mpsc::channel(1);
        registry
            .commands
            .insert(CODEX_PROVIDER.to_owned(), commands);

        registry.web_config.write().expect("web config").backend =
            crate::web::WebBackend::Firecrawl;
        *registry.memory_config.write().expect("memory config") = crate::memory::MemoryConfig {
            backend: crate::memory::MemoryBackend::Mnemosyne,
            executable: "missing-mnemosyne".to_owned(),
            global_bank: String::new(),
            data_directory: String::new(),
        };
        registry.vision_config.write().expect("vision config").model =
            Some("openai-codex/vision-test".to_owned());

        let availability = registry.available_builtin_tools(
            &[
                provider(CODEX_PROVIDER, true, true),
                provider(DEVIN_PROVIDER, true, true),
            ],
            Some(CODEX_PROVIDER),
        );
        let codex = availability
            .get(CODEX_PROVIDER)
            .expect("Codex availability");
        assert!(!codex.iter().any(|name| name == "browser"));
        assert!(!codex.iter().any(|name| name == "memory_search"));
        assert!(!codex.iter().any(|name| name == "memory_store"));
        assert!(!codex.iter().any(|name| name == "vision"));

        registry.web_config.write().expect("web config").backend = crate::web::WebBackend::Disabled;
        registry
            .memory_config
            .write()
            .expect("memory config")
            .backend = crate::memory::MemoryBackend::Disabled;
        registry.vision_config.write().expect("vision config").model = None;
        let disabled =
            registry.available_builtin_tools(&[provider(CODEX_PROVIDER, true, true)], None);
        let codex_disabled = disabled.get(CODEX_PROVIDER).expect("Codex availability");
        assert!(codex_disabled.iter().all(|name| name != "browser"));
        assert!(
            codex_disabled
                .iter()
                .all(|name| !matches!(name.as_str(), "memory_search" | "memory_store"))
        );
        assert!(codex_disabled.iter().all(|name| name != "vision"));

        assert!(
            availability
                .get(DEVIN_PROVIDER)
                .expect("unstarted Devin availability")
                .is_empty(),
            "an enabled provider without a live command channel is unavailable"
        );
    }

    fn fake_backend() -> (
        BackendHandle,
        mpsc::Receiver<BackendCommand>,
        mpsc::Sender<BackendEvent>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(8);
        let task = tokio::spawn(async {});
        (
            BackendHandle::new(command_tx, event_rx, task),
            command_rx,
            event_tx,
        )
    }

    #[tokio::test]
    async fn running_workspace_synchronizes_shared_provider_enablement() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        persistence
            .sessions
            .set_provider_enabled(CODEX_PROVIDER, true)
            .expect("enable shared provider");
        let providers = persistence
            .sessions
            .list_providers()
            .expect("provider records");
        let shared = Arc::clone(&persistence.sessions);
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            providers,
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );

        shared
            .set_provider_enabled(CODEX_PROVIDER, false)
            .expect("disable shared provider");
        runtime.synchronize_shared_providers().await;

        assert!(
            runtime
                .core
                .provider_records()
                .iter()
                .any(|provider| provider.provider == CODEX_PROVIDER && !provider.enabled)
        );
    }

    #[tokio::test]
    async fn native_cancellation_settles_waiter_and_removes_correlation() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let (respond, response) = tokio::sync::oneshot::channel();
        runtime.pending_native_delegations.insert(
            91,
            PendingNativeDelegation {
                session_id: SessionId::from("missing-owner"),
                run_id: "missing-run".to_owned(),
                respond,
                cancellation_task: tokio::spawn(std::future::pending()),
            },
        );

        runtime.cancel_native_delegation(91).await;

        assert!(runtime.pending_native_delegations.is_empty());
        let error = response
            .await
            .expect("waiter settled")
            .expect_err("cancellation is terminal failure");
        assert!(error.contains("cancelled with its provider turn"));
    }

    fn test_persistence(workspace: &Path) -> (PersistenceServices, Arc<SqliteCredentialStore>) {
        let database = workspace.join("sessions.sqlite3");
        let sessions =
            Arc::new(SqliteSessionRepository::open(&database).expect("session repository"));
        let credentials =
            Arc::new(SqliteCredentialStore::open(&database).expect("credential repository"));
        (
            PersistenceServices {
                database,
                sessions,
                credentials: credentials.clone(),
            },
            credentials,
        )
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_registration_and_credentials_share_the_canonical_workspace_id() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        endpoint
            .execute_command(
                ClientId::from("mcp-setup-test"),
                IdempotencyKey::from("register-excalidraw"),
                None,
                false,
                Command::SaveMcpServer {
                    workspace_id: workspace_id.clone(),
                    server: crate::mcp::excalidraw_input(),
                    grants: McpGrantPolicy::default(),
                },
            )
            .await
            .expect("register Excalidraw");
        endpoint
            .execute_command(
                ClientId::from("mcp-setup-test"),
                IdempotencyKey::from("credential-excalidraw"),
                None,
                false,
                Command::SetMcpServerCredential {
                    workspace_id: workspace_id.clone(),
                    server_id: crate::mcp::EXCALIDRAW_SERVER_ID.to_owned(),
                    kind: "bearer".to_owned(),
                    credential: CredentialInput("test-token".to_owned()),
                },
            )
            .await
            .expect("set Excalidraw credential");

        let QueryResult::McpManagement(management) = endpoint
            .execute_query(
                ClientId::from("mcp-setup-test"),
                Query::GetMcpManagement {
                    workspace_id: workspace_id.clone(),
                },
            )
            .await
            .expect("MCP management")
            .value
        else {
            panic!("MCP management result");
        };
        assert_eq!(management.workspace_id, workspace_id);
        assert_eq!(management.servers.len(), 1);
        assert_eq!(management.servers[0].id, crate::mcp::EXCALIDRAW_SERVER_ID);
        assert!(management.servers[0].credential_configured);
        assert_eq!(
            sessions
                .list_mcp_servers(workspace_id.as_str())
                .expect("persisted MCP servers")
                .len(),
            1
        );
        assert_eq!(
            credentials
                .get_mcp(workspace_id.as_str(), crate::mcp::EXCALIDRAW_SERVER_ID)
                .expect("credential lookup")
                .expect("persisted credential")
                .kind,
            "bearer"
        );
        assert!(
            sessions
                .list_mcp_servers(workspace.path().to_string_lossy().as_ref())
                .expect("legacy path lookup")
                .is_empty(),
            "MCP state must not be persisted under the raw workspace path"
        );

        handle.shutdown().await;
        runtime.await.expect("runtime task");

        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let restarted_state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (restarted, restarted_handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(restarted_state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = restarted_handle.endpoint().clone();
        let restarted = tokio::spawn(restarted.run());
        let QueryResult::McpManagement(management) = endpoint
            .execute_query(
                ClientId::from("mcp-restart-test"),
                Query::GetMcpManagement { workspace_id },
            )
            .await
            .expect("restarted MCP management")
            .value
        else {
            panic!("MCP management result after restart");
        };
        assert_eq!(management.servers.len(), 1);
        assert!(management.servers[0].credential_configured);

        restarted_handle.shutdown().await;
        restarted.await.expect("restarted runtime task");
    }

    #[tokio::test]
    async fn missing_mcp_server_credentials_remain_actionable_not_found_errors() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        let error = endpoint
            .execute_command(
                ClientId::from("mcp-missing-test"),
                IdempotencyKey::from("credential-missing"),
                None,
                false,
                Command::SetMcpServerCredential {
                    workspace_id: workspace_id.clone(),
                    server_id: "unknown-server".to_owned(),
                    kind: "bearer".to_owned(),
                    credential: CredentialInput("test-token".to_owned()),
                },
            )
            .await
            .expect_err("unknown server must be rejected");
        assert_eq!(error.code, ErrorCode::NotFound);
        assert!(error.message.contains("MCP server unknown-server"));
        assert!(
            credentials
                .get_mcp(workspace_id.as_str(), "unknown-server")
                .expect("credential lookup")
                .is_none()
        );

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    async fn diagnostics_are_queried_through_the_native_server() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        let response = endpoint
            .execute_query(
                ClientId::from("diagnostics-test"),
                Query::GetDiagnostics {
                    days: 30,
                    session_limit: 20,
                    provider_id: None,
                },
            )
            .await
            .expect("diagnostics query");
        assert!(matches!(
            response.value,
            QueryResult::Diagnostics(report)
                if report.period_days == 30
                && report.sessions_scanned == 0
        ));

        let error = endpoint
            .execute_query(
                ClientId::from("diagnostics-test"),
                Query::GetDiagnostics {
                    days: 0,
                    session_limit: 20,
                    provider_id: None,
                },
            )
            .await
            .expect_err("invalid diagnostics query");
        assert_eq!(error.code, ErrorCode::InvalidRequest);

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    async fn invocation_queries_are_bounded_and_served_from_durable_projection() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        sessions
            .save_invocation_telemetry_enabled(true)
            .expect("enable telemetry");
        sessions
            .record_invocation(&InvocationRecord {
                invocation_key: "archetype:agent-1".to_owned(),
                kind: nakode_protocol::InvocationKind::Archetype,
                identity: "deleted-agent".to_owned(),
                display_label: "Deleted agent".to_owned(),
                occurred_at_ms: 10_000,
            })
            .expect("record invocation");
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let mut state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        state.install_invocation_telemetry_enabled(true);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        let summary = endpoint
            .execute_query(
                ClientId::from("invocation-test"),
                Query::GetInvocationSummary,
            )
            .await
            .expect("summary query");
        assert!(matches!(
            summary.value,
            QueryResult::InvocationSummary(summary)
                if summary.enabled
                    && summary.items.len() == 1
                    && summary.items[0].identity == "deleted-agent"
                    && !summary.items[0].currently_installed
        ));
        let timeline = endpoint
            .execute_query(
                ClientId::from("invocation-test"),
                Query::GetInvocationTimeline {
                    start_at_ms: 0,
                    end_at_ms: 3_600_000,
                    bucket_width_ms: 3_600_000,
                },
            )
            .await
            .expect("timeline query");
        assert!(matches!(
            timeline.value,
            QueryResult::InvocationTimeline(timeline)
                if timeline.buckets.len() == 1
                    && timeline.buckets[0].archetype_count == 1
        ));
        let error = endpoint
            .execute_query(
                ClientId::from("invocation-test"),
                Query::GetInvocationTimeline {
                    start_at_ms: 0,
                    end_at_ms: 3_600_000,
                    bucket_width_ms: 1,
                },
            )
            .await
            .expect_err("tiny bucket must be rejected");
        assert_eq!(error.code, ErrorCode::InvalidRequest);

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    async fn bridge_restore_failure_is_visible_in_authoritative_status() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let breaker =
            rusqlite::Connection::open(&persistence.database).expect("breaker connection");
        breaker
            .execute(
                "INSERT INTO session_bridges
                 (session_id, workspace, kind, lifecycle, display_title, revision, updated_at_ms)
                 VALUES ('invalid-bridge', ?1, 'invalid-kind', 'open', 'Invalid', 1, 1)",
                [workspace.path().to_string_lossy().as_ref()],
            )
            .expect("invalid persisted bridge");
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );

        let (runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );

        assert!(
            runtime
                .core
                .engine()
                .state()
                .status_message
                .contains("failed to restore orchestrator bridges")
        );
        runtime.effects.shutdown().await;
    }

    #[tokio::test]
    async fn abandoned_quiescence_does_not_leave_runtime_fenced() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let (respond, response) = tokio::sync::oneshot::channel();
        drop(response);

        runtime.handle_quiesce(super::QuiesceRequest { respond });

        assert!(runtime.accepting_work);
    }

    #[tokio::test]
    async fn quiescence_fences_new_mutations_before_shutdown() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        handle.quiesce().await.expect("idle runtime quiesces");
        let error = endpoint
            .execute_command(
                ClientId::from("quiescence-test"),
                IdempotencyKey::from("after-fence"),
                None,
                false,
                Command::CreateSession {
                    workspace_id,
                    working_directory: None,
                    title: None,
                    model_id: None,
                    options: nakode_protocol::ModelOptions::default(),
                    tools: None,
                    initial_instructions: None,
                    bridge: None,
                    mcp_grant: None,
                    profile_id: None,
                    disabled_skill_ids: Vec::new(),
                },
            )
            .await
            .expect_err("mutations after the fence are rejected");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("fenced"));

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    async fn deleting_session_cancels_mcp_calls_and_delayed_completion_cannot_respawn_backend() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let cancellation = tokio_util::sync::CancellationToken::new();
        runtime.pending_mcp_calls.insert(
            "owned-call".to_owned(),
            PendingMcpCall {
                source: BackendSource::Primary {
                    session_id: session_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                session_id: session_id.clone(),
                run_id: None,
                server_id: "server".to_owned(),
                remote_name: "tool".to_owned(),
                arguments_json: "{}".to_owned(),
                started_at_ms: 1,
                started: Instant::now(),
                cancellation: cancellation.clone(),
            },
        );

        runtime.cancel_session_mcp_calls(&session_id);

        assert!(cancellation.is_cancelled());
        assert!(runtime.pending_mcp_calls.is_empty());

        let deleted_id = SessionId::from("already-deleted-session");
        let delayed_cancellation = tokio_util::sync::CancellationToken::new();
        runtime.pending_mcp_calls.insert(
            "delayed-call".to_owned(),
            PendingMcpCall {
                source: BackendSource::Primary {
                    session_id: deleted_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                session_id: deleted_id.clone(),
                run_id: None,
                server_id: "server".to_owned(),
                remote_name: "tool".to_owned(),
                arguments_json: "{}".to_owned(),
                started_at_ms: 1,
                started: Instant::now(),
                cancellation: delayed_cancellation.clone(),
            },
        );
        runtime
            .complete_mcp_call(McpCallCompletion {
                call_id: "delayed-call".to_owned(),
                result: Ok("late result".to_owned()),
            })
            .await;

        assert!(delayed_cancellation.is_cancelled());
        assert!(
            !runtime
                .effects
                .backends
                .session_commands
                .keys()
                .any(|(candidate, _)| candidate == &deleted_id),
            "a delayed MCP completion must not recreate a deleted session backend"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn stalled_backend_teardown_fails_delete_boundedly_and_retry_converges() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        sessions
            .create_with_id(
                session_id.as_str(),
                CODEX_PROVIDER,
                "stalled-delete-provider-session",
                &state.workspace,
                &state.working_directory,
                "Stalled delete test",
                None,
                &crate::backend::ModelOptions::default(),
                None,
            )
            .expect("persist session before deletion");

        let (commands, mut command_rx) = mpsc::channel(1);
        let (_event_tx, events) = mpsc::channel(1);
        let (release, released) = tokio::sync::oneshot::channel();
        let stalled_task = tokio::spawn(async move {
            assert!(matches!(
                command_rx.recv().await,
                Some(BackendCommand::Shutdown)
            ));
            let _ = released.await;
        });
        let mut registry = empty_registry(workspace.path()).await;
        registry.insert_session(
            session_id.clone(),
            CODEX_PROVIDER.to_owned(),
            BackendHandle::new(commands, events, stalled_task),
        );
        let effects = EffectExecutor::new(registry, persistence);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());
        let key = IdempotencyKey::from("stalled-delete-retry");
        let command = Command::DeleteSession {
            session_id: session_id.clone(),
        };

        let started = Instant::now();
        let failure = endpoint
            .execute_command(
                ClientId::from("stalled-delete-test"),
                key.clone(),
                None,
                false,
                command.clone(),
            )
            .await
            .expect_err("stalled backend prevents durable deletion");
        assert_eq!(failure.code, ErrorCode::Internal);
        assert!(failure.retryable);
        assert!(failure.message.contains("did not stop cleanly"));
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(
            sessions
                .find(session_id.as_str())
                .expect("durable session lookup")
                .is_some()
        );
        endpoint
            .execute_query(
                ClientId::from("stalled-delete-test"),
                Query::GetSession {
                    session_id: session_id.clone(),
                },
            )
            .await
            .expect("runtime continues serving after bounded teardown failure");

        release.send(()).expect("release stalled backend");
        endpoint
            .execute_command(
                ClientId::from("stalled-delete-test"),
                key,
                None,
                false,
                command,
            )
            .await
            .expect("same-key retry waits for termination and deletes");
        assert_eq!(
            sessions.find(session_id.as_str()).expect("session lookup"),
            None
        );

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn session_delete_waits_for_backend_terminal_persistence_before_durable_purge() {
        let workspace = tempfile::tempdir().expect("workspace");
        let database = workspace.path().join("sessions.sqlite3");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let provider_session_id = "terminal-persistence-provider-session";
        let subagent_provider_session_id = "terminal-persistence-subagent-session";
        let run_id = "terminal-persistence-subagent";
        sessions
            .create_with_id(
                session_id.as_str(),
                CODEX_PROVIDER,
                provider_session_id,
                &state.workspace,
                &state.working_directory,
                "Terminal persistence delete test",
                None,
                &crate::backend::ModelOptions::default(),
                None,
            )
            .expect("persist session before deletion");
        sessions
            .save_subagent(&SubagentRecord {
                parent_session_id: session_id.to_string(),
                id: run_id.to_owned(),
                agent: "reviewer".to_owned(),
                provider: CODEX_PROVIDER.to_owned(),
                model: None,
                provider_session_id: Some(subagent_provider_session_id.to_owned()),
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
                objective: "Finish before deletion".to_owned(),
                status: SubagentStatus::Working,
                latest_activity: "Working".to_owned(),
                transcript: Vec::new(),
                observability: SubagentObservability::default(),
            })
            .expect("persist owned subagent");

        let (commands, mut command_rx) = mpsc::channel(8);
        let (_event_tx, events) = mpsc::channel(8);
        let terminal_database = database.clone();
        let terminal_task = tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                if matches!(command, BackendCommand::Shutdown) {
                    let connection = rusqlite::Connection::open(terminal_database)
                        .expect("terminal persistence connection");
                    connection
                        .execute(
                            "INSERT INTO native_runtime_sessions
                             (provider, session_id, session_json, updated_at)
                             VALUES (?1, ?2, '{}', unixepoch())",
                            rusqlite::params![CODEX_PROVIDER, provider_session_id],
                        )
                        .expect("backend terminal persistence");
                    break;
                }
            }
        });
        let mut registry = empty_registry(workspace.path()).await;
        registry.insert_session(
            session_id.clone(),
            CODEX_PROVIDER.to_owned(),
            BackendHandle::new(commands, events, terminal_task),
        );
        let (subagent_commands, mut subagent_command_rx) = mpsc::channel(8);
        let child_terminal_database = database.clone();
        let child_terminal_task = tokio::spawn(async move {
            while let Some(command) = subagent_command_rx.recv().await {
                if matches!(command, BackendCommand::Shutdown) {
                    let connection = rusqlite::Connection::open(child_terminal_database)
                        .expect("subagent terminal persistence connection");
                    connection
                        .execute(
                            "INSERT INTO native_runtime_sessions
                             (provider, session_id, session_json, updated_at)
                             VALUES (?1, ?2, '{}', unixepoch())",
                            rusqlite::params![CODEX_PROVIDER, subagent_provider_session_id],
                        )
                        .expect("subagent terminal persistence");
                    break;
                }
            }
        });
        registry
            .subagent_commands
            .insert(run_id.to_owned(), subagent_commands);
        registry
            .subagent_providers
            .insert(run_id.to_owned(), CODEX_PROVIDER.to_owned());
        registry
            .subagent_parents
            .insert(run_id.to_owned(), session_id.clone());
        registry.subagent_tasks.insert(
            run_id.to_owned(),
            vec![SessionBackendTasks {
                backend: child_terminal_task,
                event_forwarder: tokio::spawn(std::future::pending()),
            }],
        );
        let effects = EffectExecutor::new(registry, persistence);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());

        endpoint
            .execute_command(
                ClientId::from("terminal-persistence-delete-test"),
                IdempotencyKey::from("terminal-persistence-delete"),
                None,
                false,
                Command::DeleteSession {
                    session_id: session_id.clone(),
                },
            )
            .await
            .expect("delete waits for backend termination then commits");

        assert_eq!(
            sessions.find(session_id.as_str()).expect("session lookup"),
            None
        );
        let connection = rusqlite::Connection::open(&database).expect("verification connection");
        let native_rows = |provider_session_id: &str| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM native_runtime_sessions
                     WHERE provider = ?1 AND session_id = ?2",
                    rusqlite::params![CODEX_PROVIDER, provider_session_id],
                    |row| row.get::<_, i64>(0),
                )
                .expect("native history count")
        };
        assert_eq!(
            native_rows(provider_session_id),
            0,
            "primary terminal persistence must land before the deleting transaction purges native history"
        );
        assert_eq!(
            native_rows(subagent_provider_session_id),
            0,
            "subagent terminal persistence must land before the deleting transaction purges owned native history"
        );

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn failed_session_delete_is_not_confirmed_and_same_key_retry_can_converge() {
        let workspace = tempfile::tempdir().expect("workspace");
        let database = workspace.path().join("sessions.sqlite3");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let mut state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let shell_effects = state
            .run_shell_command("sleep 30".to_owned())
            .expect("start shell before deletion");
        let [
            Effect::RunShell {
                id: shell_id,
                command: shell_command,
            },
        ] = shell_effects.as_slice()
        else {
            panic!("shell effect")
        };
        let shell_id = shell_id.clone();
        let shell_command = shell_command.clone();
        let session_id = SessionId::from(state.nakode_session_id.clone());
        sessions
            .create_with_id(
                session_id.as_str(),
                CODEX_PROVIDER,
                "durable-delete-provider-session",
                &state.workspace,
                &state.working_directory,
                "Durable delete test",
                None,
                &crate::backend::ModelOptions::default(),
                None,
            )
            .expect("persist session before deletion");
        let breaker = rusqlite::Connection::open(&database).expect("breaker connection");
        breaker
            .execute_batch(
                "CREATE TRIGGER fail_session_delete \
                 BEFORE DELETE ON sessions \
                 BEGIN SELECT RAISE(ABORT, 'forced session delete failure'); END;",
            )
            .expect("failure trigger");

        let (mut runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        runtime
            .shell_owners
            .insert(shell_id.clone(), session_id.clone());
        runtime.effects.shell_processes.spawn(
            workspace.path().to_path_buf(),
            shell_id.clone(),
            shell_command,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());
        let key = IdempotencyKey::from("durable-delete-retry");
        let command = Command::DeleteSession {
            session_id: session_id.clone(),
        };

        let failure = endpoint
            .execute_command(
                ClientId::from("durable-delete-test"),
                key.clone(),
                None,
                false,
                command.clone(),
            )
            .await
            .expect_err("durable delete failure must fail the mutation");
        assert_eq!(failure.code, ErrorCode::Internal);
        assert!(failure.retryable);
        assert!(failure.message.contains("not durably committed"));
        assert!(
            sessions
                .find(session_id.as_str())
                .expect("durable session lookup")
                .is_some(),
            "the failed transaction retains durable history"
        );
        let QueryResult::Session(restored) = endpoint
            .execute_query(
                ClientId::from("durable-delete-test"),
                Query::GetSession {
                    session_id: session_id.clone(),
                },
            )
            .await
            .expect("failed deletion restores the live projection")
            .value
        else {
            panic!("session query result")
        };
        assert_eq!(restored.id, session_id);
        let restored_shell = restored
            .transcript
            .entries
            .iter()
            .find(|entry| entry.title == "$ sleep 30")
            .expect("terminated shell remains as settled transcript evidence");
        assert_eq!(restored_shell.status, TranscriptEntryStatus::Failed);
        assert!(
            restored_shell
                .body
                .contains("durable session deletion failed")
        );

        breaker
            .execute_batch("DROP TRIGGER fail_session_delete;")
            .expect("drop failure trigger");
        endpoint
            .execute_command(
                ClientId::from("durable-delete-test"),
                key,
                None,
                false,
                command,
            )
            .await
            .expect("same-key retry executes after rollback");
        assert_eq!(
            sessions
                .find(session_id.as_str())
                .expect("durable session lookup"),
            None
        );
        let missing = endpoint
            .execute_query(
                ClientId::from("durable-delete-test"),
                Query::GetSession { session_id },
            )
            .await
            .expect_err("successful deletion evicts the live projection");
        assert_eq!(missing.code, ErrorCode::NotFound);

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn failed_bridge_checkpoint_rolls_back_same_process_for_same_and_new_keys() {
        let workspace = tempfile::tempdir().expect("workspace");
        let database = workspace.path().join("sessions.sqlite3");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        sessions
            .save_session_bridge(&SessionBridgeRecord {
                session_id: session_id.to_string(),
                workspace: state.workspace.clone(),
                kind: OrchestratorKind::Chat,
                lifecycle: BridgeLifecycle::Open,
                display_title: "Rollback test".to_owned(),
                revision: 1,
                transport: None,
                external_parent_id: None,
                external_thread_id: None,
                last_projected: None,
                delivery: None,
                live_turn_id: None,
                live_external_message_id: None,
                active_source_message_id: None,
                recent_inbound_event_ids: Vec::new(),
                pending_inbound: None,
                inbound_turn_origins: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("initial bridge");
        let breaker = rusqlite::Connection::open(&database).expect("breaker connection");
        breaker
            .execute_batch(
                "CREATE TRIGGER fail_bridge_checkpoint \
                 BEFORE UPDATE ON session_bridges \
                 BEGIN SELECT RAISE(ABORT, 'forced bridge failure'); END;",
            )
            .expect("failure trigger");

        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());
        let key = IdempotencyKey::from("bridge-checkpoint-retry");
        let command = Command::SetSessionBridgeLifecycle {
            session_id: session_id.clone(),
            lifecycle: BridgeLifecycle::Archived,
        };
        let first = endpoint
            .execute_command(
                ClientId::from("bridge-checkpoint-test"),
                key.clone(),
                None,
                false,
                command.clone(),
            )
            .await
            .expect_err("trigger rejects durable checkpoint");
        assert_eq!(first.code, ErrorCode::Internal);
        assert!(first.retryable);
        let second = endpoint
            .execute_command(
                ClientId::from("bridge-checkpoint-test"),
                IdempotencyKey::from("bridge-checkpoint-new-key"),
                None,
                false,
                command.clone(),
            )
            .await
            .expect_err("a new-key retry reaches the checkpoint instead of a process fence");
        assert_eq!(second.code, ErrorCode::Internal);
        assert!(second.retryable);
        assert_eq!(
            sessions
                .list_session_bridges(&workspace.path().to_string_lossy())
                .expect("stored bridge")[0]
                .lifecycle,
            BridgeLifecycle::Open
        );

        breaker
            .execute_batch("DROP TRIGGER fail_bridge_checkpoint;")
            .expect("drop failure trigger");
        endpoint
            .execute_command(
                ClientId::from("bridge-checkpoint-test"),
                key,
                None,
                false,
                command,
            )
            .await
            .expect("same-key retry executes after rollback");
        assert_eq!(
            sessions
                .list_session_bridges(&workspace.path().to_string_lossy())
                .expect("stored bridge")[0]
                .lifecycle,
            BridgeLifecycle::Archived
        );

        handle.shutdown().await;
        runtime.await.expect("runtime task");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn failed_bridge_origin_and_acknowledgement_checkpoint_retries_on_terminal_event() {
        let workspace = tempfile::tempdir().expect("workspace");
        let database = workspace.path().join("sessions.sqlite3");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let sessions = Arc::clone(&persistence.sessions);
        let mut state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            version: None,
            capabilities: BackendCapabilities::default(),
        }));
        state.handle_backend(BackendEvent::Models(vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model".to_owned(),
            is_default: true,
            capabilities: crate::codex::model_capabilities(),
        }]));
        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "provider-session".to_owned(),
            model: "model".to_owned(),
        });
        state
            .submit_prompt_with_id_and_source(
                "bridge-stable-prompt".to_owned(),
                "continue".to_owned(),
                Vec::new(),
                Some("thread-transport".to_owned()),
            )
            .expect("stable prompt starts");
        sessions
            .save_session_bridge(&SessionBridgeRecord {
                session_id: session_id.to_string(),
                workspace: state.workspace.clone(),
                kind: OrchestratorKind::Chat,
                lifecycle: BridgeLifecycle::Open,
                display_title: "Acknowledgement retry".to_owned(),
                revision: 1,
                transport: Some("thread-transport".to_owned()),
                external_parent_id: Some("100".to_owned()),
                external_thread_id: Some("101".to_owned()),
                last_projected: None,
                delivery: None,
                live_turn_id: None,
                live_external_message_id: None,
                active_source_message_id: Some("message-1".to_owned()),
                recent_inbound_event_ids: Vec::new(),
                pending_inbound: Some(BridgePendingInboundRecord {
                    external_event_id: "event-1".to_owned(),
                    source_message_id: "message-1".to_owned(),
                    client_prompt_id: "bridge-stable-prompt".to_owned(),
                    text: "continue".to_owned(),
                    attachments: Vec::new(),
                }),
                inbound_turn_origins: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("initial bridge");
        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let breaker = rusqlite::Connection::open(&database).expect("breaker connection");
        breaker
            .execute_batch(
                "CREATE TRIGGER fail_bridge_acknowledgement \
                 BEFORE UPDATE ON session_bridges \
                 BEGIN SELECT RAISE(ABORT, 'forced acknowledgement failure'); END;",
            )
            .expect("failure trigger");

        runtime
            .handle_backend_event(
                BackendSource::Primary {
                    session_id: session_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                BackendEvent::TurnStarted {
                    turn_id: "provider-generated-turn".to_owned(),
                },
            )
            .await;
        assert_eq!(
            runtime.pending_bridge_acknowledgements.get(&session_id),
            Some(&"bridge-stable-prompt".to_owned())
        );
        assert!(
            runtime
                .core
                .session_bridge(&session_id)
                .expect("runtime bridge")
                .pending_inbound
                .is_some()
        );
        assert!(
            sessions
                .list_session_bridges(&workspace.path().to_string_lossy())
                .expect("stored bridge")[0]
                .pending_inbound
                .is_some()
        );

        breaker
            .execute_batch("DROP TRIGGER fail_bridge_acknowledgement;")
            .expect("drop failure trigger");
        runtime
            .handle_backend_event(
                BackendSource::Primary {
                    session_id: session_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                BackendEvent::TurnCompleted {
                    turn_id: "provider-generated-turn".to_owned(),
                    outcome: crate::backend::TurnOutcome::Completed,
                    error: None,
                },
            )
            .await;
        assert!(
            !runtime
                .pending_bridge_acknowledgements
                .contains_key(&session_id)
        );
        assert!(
            runtime
                .core
                .session_bridge(&session_id)
                .expect("runtime bridge")
                .pending_inbound
                .is_none()
        );
        let stored_bridge = &sessions
            .list_session_bridges(&workspace.path().to_string_lossy())
            .expect("stored bridge")[0];
        assert!(stored_bridge.pending_inbound.is_none());
        assert_eq!(
            stored_bridge.inbound_turn_origins,
            [BridgeInboundTurnOriginRecord {
                turn_id: "provider-generated-turn".to_owned(),
                transport: "thread-transport".to_owned(),
            }],
            "terminal retry persists source provenance and acknowledgement as one final bridge state"
        );
        runtime.effects.shutdown().await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn normalized_inbound_ledger_deduplicates_after_restart_without_cross_route_acceptance() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let restarted_persistence = persistence.clone();
        let sessions = Arc::clone(&persistence.sessions);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let restarted_state = state.clone();
        let session_id = SessionId::from(state.nakode_session_id.clone());
        sessions
            .save_session_bridge(&SessionBridgeRecord {
                session_id: session_id.to_string(),
                workspace: state.workspace.clone(),
                kind: OrchestratorKind::Chat,
                lifecycle: BridgeLifecycle::Open,
                display_title: "Inbound ledger".to_owned(),
                revision: 1,
                transport: Some("thread-transport".to_owned()),
                external_parent_id: Some("100".to_owned()),
                external_thread_id: Some("101".to_owned()),
                last_projected: None,
                delivery: None,
                live_turn_id: None,
                live_external_message_id: None,
                active_source_message_id: None,
                recent_inbound_event_ids: Vec::new(),
                pending_inbound: None,
                inbound_turn_origins: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("initial bridge");

        let effects = EffectExecutor::new(empty_registry(workspace.path()).await, persistence);
        let (runtime, handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = handle.endpoint().clone();
        let runtime = tokio::spawn(runtime.run());
        let first = endpoint
            .execute_command(
                ClientId::from("bridge-ledger-test"),
                IdempotencyKey::from("bridge-ledger-first"),
                None,
                false,
                Command::ContinueSessionFromBridge {
                    session_id: session_id.clone(),
                    transport: "thread-transport".to_owned(),
                    external_thread_id: "101".to_owned(),
                    external_event_id: "event-1".to_owned(),
                    source_message_id: "message-1".to_owned(),
                    prompt: PromptInput {
                        text: String::new(),
                        attachments: Vec::new(),
                    },
                    consume_as_busy: true,
                },
            )
            .await
            .expect("first event is consumed as busy");
        assert_eq!(
            first.bridge_continuation,
            Some(BridgeContinuationDisposition::Busy)
        );
        assert_eq!(
            sessions
                .find_session_bridge_inbound_event(session_id.as_str(), "event-1")
                .expect("ledger query"),
            Some(BridgeContinuationDisposition::Busy)
        );
        handle.shutdown().await;
        runtime.await.expect("runtime task");

        let effects = EffectExecutor::new(
            empty_registry(workspace.path()).await,
            restarted_persistence,
        );
        let (restarted, restarted_handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(restarted_state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let endpoint = restarted_handle.endpoint().clone();
        let restarted = tokio::spawn(restarted.run());
        let wrong_route = endpoint
            .execute_command(
                ClientId::from("bridge-ledger-test"),
                IdempotencyKey::from("bridge-ledger-wrong-route"),
                None,
                false,
                Command::ContinueSessionFromBridge {
                    session_id: session_id.clone(),
                    transport: "thread-transport".to_owned(),
                    external_thread_id: "999".to_owned(),
                    external_event_id: "event-1".to_owned(),
                    source_message_id: "message-1".to_owned(),
                    prompt: PromptInput {
                        text: "must not run".to_owned(),
                        attachments: Vec::new(),
                    },
                    consume_as_busy: false,
                },
            )
            .await
            .expect_err("durable identity never bypasses exact route authorization");
        assert_eq!(wrong_route.code, ErrorCode::Conflict);
        let duplicate = endpoint
            .execute_command(
                ClientId::from("bridge-ledger-test"),
                IdempotencyKey::from("bridge-ledger-duplicate"),
                None,
                false,
                Command::ContinueSessionFromBridge {
                    session_id: session_id.clone(),
                    transport: "thread-transport".to_owned(),
                    external_thread_id: "101".to_owned(),
                    external_event_id: "event-1".to_owned(),
                    source_message_id: "message-1".to_owned(),
                    prompt: PromptInput {
                        text: "must not run".to_owned(),
                        attachments: Vec::new(),
                    },
                    consume_as_busy: false,
                },
            )
            .await
            .expect("replayed event is typed duplicate");
        assert_eq!(
            duplicate.bridge_continuation,
            Some(BridgeContinuationDisposition::Duplicate)
        );
        assert_eq!(
            duplicate.replayed_bridge_continuation,
            Some(BridgeContinuationDisposition::Busy),
            "a lost response restores the exact durable Busy reaction after restart"
        );

        restarted_handle.shutdown().await;
        restarted.await.expect("restarted runtime task");
    }

    async fn assert_start_session_routed(
        registry: &mut BackendRegistry,
        session_id: &SessionId,
        commands: &mut mpsc::Receiver<BackendCommand>,
        model: &str,
    ) {
        registry
            .send_session(
                session_id,
                CODEX_PROVIDER,
                Path::new("/tmp"),
                BackendCommand::StartSession {
                    model: Some(model.to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    allowed_builtin_tools: None,
                    max_turns: None,
                    finalization_reserve_turns: 0,
                    timeout_seconds: None,
                    owner_session_id: None,
                    parent_run_id: None,
                    enabled_skill_ids: Vec::new(),
                },
            )
            .await
            .expect("session remains routable");
        assert!(matches!(
            commands.recv().await,
            Some(BackendCommand::StartSession {
                model: Some(actual),
                ..
            }) if actual == model
        ));
    }

    #[test]
    fn native_server_advertises_artifact_transfer() {
        assert!(
            native_service_capabilities().supports(ServiceCapability::ArtifactTransfer),
            "frontends must be able to reconnect and fetch transcript artifacts"
        );
    }

    #[tokio::test]
    async fn same_provider_session_handles_dispatch_and_attribute_independently() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let (control_tx, _control_rx) = mpsc::channel(1);
        registry
            .commands
            .insert(CODEX_PROVIDER.to_owned(), control_tx);

        let first_id = SessionId::from("session-first");
        let second_id = SessionId::from("session-second");
        let (first, mut first_commands, first_events) = fake_backend();
        let (second, mut second_commands, second_events) = fake_backend();
        registry.insert_session(first_id.clone(), CODEX_PROVIDER.to_owned(), first);
        registry.insert_session(second_id.clone(), CODEX_PROVIDER.to_owned(), second);

        registry
            .send_session(
                &first_id,
                CODEX_PROVIDER,
                Path::new("/tmp/first"),
                BackendCommand::StartSession {
                    model: Some("model-first".to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    allowed_builtin_tools: None,
                    max_turns: None,
                    finalization_reserve_turns: 0,
                    timeout_seconds: None,
                    owner_session_id: None,
                    parent_run_id: None,
                    enabled_skill_ids: Vec::new(),
                },
            )
            .await
            .expect("first session command");
        registry
            .send_session(
                &second_id,
                CODEX_PROVIDER,
                Path::new("/tmp/second"),
                BackendCommand::StartSession {
                    model: Some("model-second".to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    allowed_builtin_tools: None,
                    max_turns: None,
                    finalization_reserve_turns: 0,
                    timeout_seconds: None,
                    owner_session_id: None,
                    parent_run_id: None,
                    enabled_skill_ids: Vec::new(),
                },
            )
            .await
            .expect("second session command");

        assert!(matches!(
            first_commands.recv().await,
            Some(BackendCommand::StartSession {
                model: Some(model),
                ..
            }) if model == "model-first"
        ));
        assert!(matches!(
            second_commands.recv().await,
            Some(BackendCommand::StartSession {
                model: Some(model),
                ..
            }) if model == "model-second"
        ));

        let first_event = BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "First adapter".to_owned(),
            version: None,
            capabilities: crate::backend::BackendCapabilities::default(),
        });
        let second_event = BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Second adapter".to_owned(),
            version: None,
            capabilities: crate::backend::BackendCapabilities::default(),
        });
        let (first_sent, second_sent) = tokio::join!(
            first_events.send(first_event),
            second_events.send(second_event)
        );
        first_sent.expect("first event");
        second_sent.expect("second event");

        let mut attributed = HashSet::new();
        for _ in 0..2 {
            let (source, _) = registry.events.recv().await.expect("attributed event");
            let BackendSource::Primary {
                session_id,
                provider,
            } = source
            else {
                panic!("session adapters must emit primary events");
            };
            assert_eq!(provider, CODEX_PROVIDER);
            attributed.insert(session_id);
        }
        assert_eq!(attributed, HashSet::from([first_id, second_id]));
    }

    #[tokio::test]
    async fn provider_stop_closes_control_and_all_session_handles() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let (control_tx, mut control_rx) = mpsc::channel(1);
        registry
            .commands
            .insert(CODEX_PROVIDER.to_owned(), control_tx);
        let first_id = SessionId::from("session-first");
        let second_id = SessionId::from("session-second");
        let (first, mut first_commands, _first_events) = fake_backend();
        let (second, mut second_commands, _second_events) = fake_backend();
        registry.insert_session(first_id, CODEX_PROVIDER.to_owned(), first);
        registry.insert_session(second_id, CODEX_PROVIDER.to_owned(), second);

        registry.stop_provider(CODEX_PROVIDER).await;

        assert!(matches!(
            control_rx.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(matches!(
            first_commands.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(matches!(
            second_commands.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(registry.session_commands.is_empty());
        assert!(!registry.commands.contains_key(CODEX_PROVIDER));
    }

    #[tokio::test]
    async fn memory_services_are_shared_within_an_access_root_and_isolated_between_roots() {
        let authority = tempfile::tempdir().expect("authority");
        let first_root = tempfile::tempdir().expect("first access root");
        let second_root = tempfile::tempdir().expect("second access root");
        let registry = empty_registry(authority.path()).await;

        let first = registry.memory_service_for(first_root.path()).await;
        let repeated = registry.memory_service_for(first_root.path()).await;
        let second = registry.memory_service_for(second_root.path()).await;

        assert!(Arc::ptr_eq(&first, &repeated));
        assert!(!Arc::ptr_eq(&first, &second));
    }

    /// Releasing one session stops its provider children and leaves every other session alone.
    ///
    /// What `Effect::ReleaseSessionBackends` runs on a delete. Keyed on the session across providers, so
    /// a session served by two adapters leaves neither behind — an orphaned child would go on writing to
    /// history that the same delete is removing.
    #[tokio::test]
    async fn releasing_one_session_stops_only_its_own_backends() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let doomed = SessionId::from("session-doomed");
        let survivor = SessionId::from("session-survivor");
        let (codex, mut codex_commands, _codex_events) = fake_backend();
        let (claude, mut claude_commands, _claude_events) = fake_backend();
        let (kept, mut kept_commands, _kept_events) = fake_backend();
        registry.insert_session(doomed.clone(), CODEX_PROVIDER.to_owned(), codex);
        registry.insert_session(
            doomed.clone(),
            crate::backend::CLAUDE_PROVIDER.to_owned(),
            claude,
        );
        registry.insert_session(survivor.clone(), CODEX_PROVIDER.to_owned(), kept);

        registry
            .stop_session(&doomed)
            .await
            .expect("doomed backends stop");

        assert!(matches!(
            codex_commands.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(matches!(
            claude_commands.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(
            !registry
                .session_commands
                .keys()
                .any(|(id, _)| id == &doomed),
            "the released session must keep no handle"
        );
        assert!(
            registry
                .session_commands
                .contains_key(&(survivor, CODEX_PROVIDER.to_owned())),
            "releasing one session must not touch another"
        );
        assert!(
            kept_commands.try_recv().is_err(),
            "the surviving session must not be told to shut down"
        );
    }

    /// The delete path evicts a non-default engine before dispatching its release effect. Runtime
    /// release must therefore not require a surviving `DomainState` projection.
    #[tokio::test]
    async fn release_effect_stops_backends_after_the_session_projection_is_absent() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, _credentials) = test_persistence(workspace.path());
        let mut registry = empty_registry(workspace.path()).await;
        let absent = SessionId::from("deleted-before-effects");
        let (backend, mut commands, _events) = fake_backend();
        registry.insert_session(absent.clone(), CODEX_PROVIDER.to_owned(), backend);
        let effects = EffectExecutor::new(registry, persistence);
        let state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        assert!(runtime.core.engine_for(&absent).is_none());

        runtime
            .execute_effects(
                &absent,
                vec![Effect::ReleaseSessionBackends(absent.to_string())],
                EffectOrigin::ClientCommand,
            )
            .await;

        assert!(matches!(
            commands.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(
            !runtime
                .effects
                .backends
                .session_commands
                .keys()
                .any(|(session_id, _)| session_id == &absent)
        );
    }

    /// Releasing a session with nothing attached is a success, which is the dead-session case.
    #[tokio::test]
    async fn releasing_a_session_with_no_backend_is_idempotent() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let absent = SessionId::from("session-absent");

        registry
            .stop_session(&absent)
            .await
            .expect("first absent stop");
        registry
            .stop_session(&absent)
            .await
            .expect("repeated absent stop");

        assert!(registry.session_commands.is_empty());
    }

    /// Supervisor handles do not pile up across session churn.
    ///
    /// `tasks` is awaited once, in `shutdown`, and every session and subagent adds two handles to it. It
    /// was only ever pushed to, so a long-lived server kept one per session that had already ended.
    #[tokio::test]
    async fn finished_supervisor_handles_do_not_accumulate() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let baseline = registry.tasks.len();

        for cycle in 0..12 {
            let session_id = SessionId::from(format!("session-{cycle}"));
            let (handle, commands, events) = fake_backend();
            registry.insert_session(session_id.clone(), CODEX_PROVIDER.to_owned(), handle);
            registry
                .stop_session(&session_id)
                .await
                .expect("session backend stops");
            // Dropping both ends is what ends the forwarder and the supervisor this test is counting.
            drop(commands);
            drop(events);
            tokio::task::yield_now().await;
        }
        // One more insert to run the reaper over everything the cycles left finished.
        let (handle, _commands, _events) = fake_backend();
        registry.insert_session(
            SessionId::from("session-last"),
            CODEX_PROVIDER.to_owned(),
            handle,
        );

        assert!(
            registry.tasks.len() < baseline + 12,
            "supervisor handles accumulated across session churn: {} handles",
            registry.tasks.len()
        );
    }

    #[tokio::test]
    async fn session_token_refresh_preserves_all_live_session_handles() {
        let workspace = tempfile::tempdir().expect("workspace");
        let (persistence, credentials) = test_persistence(workspace.path());
        let mut registry = empty_registry(workspace.path()).await;
        let (control_tx, mut control_rx) = mpsc::channel(1);
        registry
            .commands
            .insert(CODEX_PROVIDER.to_owned(), control_tx);
        let first_id = SessionId::from("session-first");
        let second_id = SessionId::from("session-second");
        let (first, mut first_commands, _first_events) = fake_backend();
        let (second, mut second_commands, _second_events) = fake_backend();
        registry.insert_session(first_id.clone(), CODEX_PROVIDER.to_owned(), first);
        registry.insert_session(second_id.clone(), CODEX_PROVIDER.to_owned(), second);
        let mut state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let mut pending = VecDeque::new();

        save_provider_credential(
            &mut state,
            &mut registry,
            &persistence,
            &mut pending,
            ProviderCredentialInput {
                provider: CODEX_PROVIDER.to_owned(),
                kind: "chatgpt_oauth".to_owned(),
                metadata: serde_json::json!({
                    "access_token": "refreshed",
                    "refresh_token": "rotated",
                    "expires_at_ms": 9_999_999_999_999_u64,
                }),
            },
            EffectOrigin::PrimarySession,
        )
        .await;

        assert!(matches!(
            control_rx.recv().await,
            Some(BackendCommand::Shutdown)
        ));
        assert!(first_commands.try_recv().is_err());
        assert!(second_commands.try_recv().is_err());
        let (refreshed_control_tx, _refreshed_control_rx) = mpsc::channel(1);
        registry
            .commands
            .insert(CODEX_PROVIDER.to_owned(), refreshed_control_tx);
        assert_start_session_routed(
            &mut registry,
            &first_id,
            &mut first_commands,
            "after-refresh-first",
        )
        .await;
        assert_start_session_routed(
            &mut registry,
            &second_id,
            &mut second_commands,
            "after-refresh-second",
        )
        .await;
        assert!(
            registry
                .session_commands
                .contains_key(&(first_id, CODEX_PROVIDER.to_owned()))
        );
        assert!(
            registry
                .session_commands
                .contains_key(&(second_id, CODEX_PROVIDER.to_owned()))
        );
        assert!(matches!(
            pending.pop_front(),
            Some(Effect::SetProviderEnabled {
                provider,
                enabled: true,
            }) if provider == CODEX_PROVIDER
        ));
        assert!(
            credentials
                .get(CODEX_PROVIDER)
                .expect("credential lookup")
                .is_some()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn attributed_primary_events_never_cross_logical_sessions() {
        let workspace = tempfile::tempdir().expect("workspace");
        let database = workspace.path().join("sessions.sqlite3");
        let sessions =
            Arc::new(SqliteSessionRepository::open(&database).expect("session repository"));
        let credentials =
            Arc::new(SqliteCredentialStore::open(&database).expect("credential repository"));
        let registry = empty_registry(workspace.path()).await;
        let effects = EffectExecutor::new(
            registry,
            PersistenceServices {
                database,
                sessions,
                credentials,
            },
        );
        let mut state = DomainState::new_for_backend(
            workspace.path().to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: crate::backend::BackendCapabilities::default(),
            }),
        );
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let (mut runtime, _handle) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            Vec::new(),
            effects,
            mpsc::channel(1).1,
        );
        let (created, _) = runtime
            .core
            .create_session_command(
                &workspace_id,
                None,
                &nakode_protocol::ModelOptions::default(),
                None,
            )
            .expect("second logical session");
        let second_id = SessionId::from(created.resource_id.expect("second logical session id"));

        runtime
            .handle_backend_event(
                BackendSource::Primary {
                    session_id: first_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                BackendEvent::Warning("first-only warning".to_owned()),
            )
            .await;
        runtime
            .handle_backend_event(
                BackendSource::Primary {
                    session_id: second_id.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                },
                BackendEvent::Warning("second-only warning".to_owned()),
            )
            .await;

        let first_entries = runtime
            .core
            .engine_for(&first_id)
            .expect("first state")
            .state()
            .transcript
            .entries();
        assert!(
            first_entries
                .iter()
                .any(|entry| entry.body == "first-only warning")
        );
        assert!(
            !first_entries
                .iter()
                .any(|entry| entry.body == "second-only warning")
        );
        let second_entries = runtime
            .core
            .engine_for(&second_id)
            .expect("second state")
            .state()
            .transcript
            .entries();
        assert!(
            second_entries
                .iter()
                .any(|entry| entry.body == "second-only warning")
        );
        assert!(
            !second_entries
                .iter()
                .any(|entry| entry.body == "first-only warning")
        );
    }
}
