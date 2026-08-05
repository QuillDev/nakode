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

use tokio::sync::mpsc;

use nakode_protocol::{
    ErrorCode, Query, QueryResult, ServiceCapabilities, ServiceCapability, ServiceError, Snapshot,
};
use nakode_server::{ServerEndpoint, ServerRequests};
use thiserror::Error;

use crate::{
    agent::{AgentCatalog, AgentCatalogError},
    backend::{BackendCommand, BackendError, BackendEvent, BackendHandle},
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

use super::ServerCore;

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
}

pub(crate) struct NativeServerRuntime {
    core: ServerCore,
    endpoint: ServerEndpoint,
    requests: ServerRequests,
    effects: EffectExecutor,
    shell_owners: HashMap<String, nakode_protocol::SessionId>,
    shutdown: mpsc::Receiver<()>,
}

pub(crate) struct PreparedRuntime {
    pub(crate) engine: ServiceEngine,
    pub(crate) effects: EffectExecutor,
    pub(crate) providers: Vec<ProviderRecord>,
    pub(crate) sessions: Vec<SessionRecord>,
}

impl PreparedRuntime {
    pub(crate) fn into_actor(self) -> (NativeServerRuntime, NativeServerHandle) {
        NativeServerRuntime::from_parts(self.engine, self.providers, self.sessions, self.effects)
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
    let providers = session_repository.list_providers()?;
    let (provider_credentials, credential_failures) =
        load_provider_credentials(&providers, credential_store.as_ref());
    let mut backends = BackendRegistry::spawn(
        config,
        &providers,
        session_database.clone(),
        provider_credentials,
        shared_web_config(session_repository.as_ref())?,
        shared_memory_config(session_repository.as_ref())?,
        shared_vision_config(session_repository.as_ref())?,
    )
    .await;
    backends.failures.extend(credential_failures);

    let agents = AgentCatalog::load(&config.agents)?;
    let skills = SkillCatalog::load(&config.workspace)?;
    let prompt_addenda =
        PromptAddenda::load(config.personalities.as_deref(), config.soul.as_deref())?;
    let mut state = initial_state(config, &providers, &backends, agents, skills);
    state.install_prompt_addenda(prompt_addenda);
    let terminal_image_mode = session_repository.load_terminal_image_mode()?;
    state.install_terminal_image_mode(terminal_image_mode);
    state.set_nakode_executable(&nakode_executable);
    load_cached_provider_configuration(&mut state, &mut backends, session_repository.as_ref())
        .await;
    let sessions = session_repository.list_recent(&state.workspace, 100)?;
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
    })
}

impl NativeServerRuntime {
    pub(crate) fn from_parts(
        engine: ServiceEngine,
        providers: Vec<ProviderRecord>,
        sessions: Vec<SessionRecord>,
        effects: EffectExecutor,
    ) -> (Self, NativeServerHandle) {
        let capabilities = native_service_capabilities();
        let (endpoint, requests) =
            ServerEndpoint::channel(env!("CARGO_PKG_VERSION"), capabilities, 256);
        let (shutdown_tx, shutdown) = mpsc::channel(1);
        let handle = NativeServerHandle {
            endpoint: endpoint.clone(),
            shutdown: shutdown_tx,
        };
        (
            Self {
                core: ServerCore::new(engine, providers, sessions),
                endpoint,
                requests,
                effects,
                shell_owners: HashMap::new(),
                shutdown,
            },
            handle,
        )
    }

    pub(crate) async fn run(mut self) {
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
                _ = provider_sync.tick() => self.synchronize_shared_providers().await,
            }
        }
        self.effects.shutdown().await;
    }

    async fn handle_request(&mut self, request: nakode_server::ServerRequest) {
        let request = match request {
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
            request => request,
        };
        let mut outcome = self.core.handle(&self.endpoint, request);
        let effects = std::mem::take(&mut outcome.effects);
        let had_effects = !effects.is_empty();
        let session_id = outcome
            .effect_session
            .clone()
            .unwrap_or_else(|| self.core.default_session_id().clone());
        self.register_effect_owners(&session_id, &effects);
        if let Some(engine) = self.core.engine_for_mut(&session_id) {
            self.effects
                .execute(
                    &session_id,
                    engine.state_mut(),
                    effects,
                    EffectOrigin::ClientCommand,
                )
                .await;
        }
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
        let providers = match self.effects.persistence.sessions.list_providers() {
            Ok(providers) => providers,
            Err(error) => {
                self.core
                    .engine_mut()
                    .state_mut()
                    .session_store_failed(error.to_string());
                return;
            }
        };
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

    async fn handle_backend_event(&mut self, source: BackendSource, event: BackendEvent) {
        let origin = match &source {
            BackendSource::ProviderControl(_) => EffectOrigin::ProviderControl,
            BackendSource::Primary { .. } => EffectOrigin::PrimarySession,
            BackendSource::Subagent(_) => EffectOrigin::Subagent,
        };
        self.effects
            .backends
            .observe_provider_event(&source, &event);
        let (session_id, effects) = match source {
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
        let had_effects = !effects.is_empty();
        self.register_effect_owners(&session_id, &effects);
        if let Some(engine) = self.core.engine_for_mut(&session_id) {
            self.effects
                .execute(&session_id, engine.state_mut(), effects, origin)
                .await;
        }
        if had_effects {
            self.refresh_catalogs();
        }
        self.core
            .commit_and_publish_session(&self.endpoint, &session_id);
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
            Ok(providers) => self.core.replace_provider_records(providers),
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

fn native_service_capabilities() -> ServiceCapabilities {
    ServiceCapabilities {
        supported: [
            ServiceCapability::Subscriptions,
            ServiceCapability::MultipleClients,
            ServiceCapability::ArtifactTransfer,
            ServiceCapability::ExternalTools,
            ServiceCapability::InitialSessionModel,
            ServiceCapability::SessionDeletion,
            ServiceCapability::QueuedPromptSteering,
        ]
        .into_iter()
        .collect(),
    }
}

impl NativeServerHandle {
    pub(crate) const fn endpoint(&self) -> &ServerEndpoint {
        &self.endpoint
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.shutdown.send(()).await;
    }
}

pub(crate) struct BackendRegistry {
    /// Provider-scoped handles own authentication, readiness, and model catalogs.
    pub(crate) commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    /// Session-scoped handles own native sessions and turns. A provider adapter
    /// may supervise only the logical session named by this key.
    pub(crate) session_commands:
        HashMap<(nakode_protocol::SessionId, String), mpsc::Sender<BackendCommand>>,
    pub(crate) subagent_commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    pub(crate) subagent_providers: HashMap<String, String>,
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
    pub(crate) memory_service: crate::memory::SharedMemoryService,
    pub(crate) vision_config: Arc<RwLock<crate::vision::VisionConfig>>,
    pub(crate) vision_service: Option<crate::vision::SharedVisionService>,
}

pub(crate) struct ProviderCooldown {
    pub(crate) until: Instant,
    pub(crate) reason: String,
}

const PROVIDER_FATAL_ERROR_COOLDOWN: Duration = Duration::from_secs(15 * 60);
const SHARED_PROVIDER_SYNC_INTERVAL: Duration = Duration::from_secs(2);

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

    pub(crate) async fn spawn(
        config: &Config,
        providers: &[ProviderRecord],
        session_database: PathBuf,
        provider_credentials: HashMap<String, serde_json::Value>,
        web_config: Arc<RwLock<crate::web::WebConfig>>,
        memory_config: Arc<RwLock<crate::memory::MemoryConfig>>,
        vision_config: Arc<RwLock<crate::vision::VisionConfig>>,
    ) -> Self {
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
            subagent_commands: HashMap::new(),
            subagent_providers: HashMap::new(),
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
            memory_service: Arc::new(crate::memory::MemoryService::new(
                memory_config,
                crate::memory::project_bank(&config.workspace),
            )),
            vision_config,
            vision_service,
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
        let handle = self.spawn_provider_handle(provider).await?;
        self.insert_provider_control(provider.to_owned(), handle);
        Ok(())
    }

    async fn spawn_provider_handle(&self, provider: &str) -> Result<BackendHandle, BackendError> {
        let credential = self.provider_credentials.get(provider).cloned();
        let handle = match provider {
            crate::backend::CODEX_PROVIDER => {
                codex::spawn(
                    codex::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_reasoning_effort(self.config.openai_reasoning_effort.as_str())
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&self.memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CLAUDE_PROVIDER => {
                claude::spawn(
                    claude::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CURSOR_PROVIDER => {
                cursor::spawn(
                    cursor::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::KIMI_PROVIDER => {
                kimi::spawn(
                    kimi::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&self.memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::GLM_PROVIDER => {
                glm::spawn(
                    glm::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&self.memory_service))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::DEVIN_PROVIDER => {
                devin::spawn(
                    devin::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_memory(Arc::clone(&self.memory_service))
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

    pub(crate) async fn stop_provider(&mut self, provider: &str) {
        self.stop_provider_control(provider).await;
        let session_keys = self
            .session_commands
            .keys()
            .filter(|(_, session_provider)| session_provider == provider)
            .cloned()
            .collect::<Vec<_>>();
        for key in session_keys {
            if let Some(commands) = self.session_commands.remove(&key) {
                let _ = commands.send(BackendCommand::Shutdown).await;
            }
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
    pub(crate) async fn stop_session(&mut self, session_id: &nakode_protocol::SessionId) {
        let keys = self
            .session_commands
            .keys()
            .filter(|(id, _)| id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(commands) = self.session_commands.remove(&key) {
                let _ = commands.send(BackendCommand::Shutdown).await;
            }
        }
    }

    /// Drops the join handles of supervisors that have already exited.
    ///
    /// `tasks` is only ever pushed to and is awaited once, in `shutdown`. Every session and every
    /// subagent adds two handles, so without this the vector grows with churn for the life of the
    /// process and keeps a `JoinHandle` per session that has long since ended.
    fn reap_finished_tasks(&mut self) {
        self.tasks.retain(|task| !task.is_finished());
    }

    pub(crate) fn set_provider_credential(&mut self, provider: &str, metadata: serde_json::Value) {
        self.provider_credentials
            .insert(provider.to_owned(), metadata.clone());
        if provider == crate::backend::CODEX_PROVIDER
            && let Ok(service) =
                codex::vision_service(Some(metadata), Arc::clone(&self.vision_config))
        {
            self.vision_service = service;
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
        self.reap_finished_tasks();
        self.session_commands
            .insert((session_id.clone(), provider.clone()), commands);
        self.tasks.push(task);
        let event_tx = self.event_tx.clone();
        self.tasks.push(tokio::spawn(async move {
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
        }));
    }

    pub(crate) async fn spawn_subagent(
        &mut self,
        run_id: String,
        provider: &str,
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
        let handle = self.spawn_provider_handle(provider).await?;
        let (commands, mut events, task) = handle.into_parts();
        self.reap_finished_tasks();
        self.subagent_commands.insert(run_id.clone(), commands);
        self.subagent_providers
            .insert(run_id.clone(), provider.to_owned());
        self.tasks.push(task);
        let event_tx = self.event_tx.clone();
        self.tasks.push(tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx
                    .send((BackendSource::Subagent(run_id.clone()), event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }));
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
            let handle = self.spawn_provider_handle(provider).await?;
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

    pub(crate) async fn stop_subagent(&mut self, run_id: &str) {
        self.subagent_providers.remove(run_id);
        if let Some(commands) = self.subagent_commands.remove(run_id) {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
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
            self.stop_subagent(&run_id).await;
        }
        self.provider_credentials.remove(provider);
        Ok(())
    }

    pub(crate) async fn shutdown(self) {
        for commands in self.commands.values() {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
        for commands in self.session_commands.values() {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
        for commands in self.subagent_commands.values() {
            let _ = commands.send(BackendCommand::Shutdown).await;
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
                    .spawn(PathBuf::from(&state.workspace), id, command);
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
            Effect::StopSubagent(run_id) => self.backends.stop_subagent(&run_id).await,
            Effect::ReleaseSessionBackends(id) => {
                self.backends
                    .stop_session(&nakode_protocol::SessionId::from(id))
                    .await;
            }
            Effect::CompleteAgentRequest { .. } => {
                // Run completion is projected through RunView.
            }
            #[cfg(test)]
            Effect::ListSessions | Effect::ListProviders | Effect::OpenUrl(_) | Effect::Quit => {}
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
            persistence_effect @ (Effect::PersistSession { .. }
            | Effect::PersistModels { .. }
            | Effect::SetDefaultModel { .. }
            | Effect::SaveModelOptions { .. }
            | Effect::PersistSubagent(_)
            | Effect::LoadSubagents(_)
            | Effect::UpdateSessionModel { .. }
            | Effect::TouchSession(_)
            | Effect::DeleteSession(_)) => {
                execute_persistence_effect(state, sessions, persistence_effect);
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
            Effect::CheckAgentBrowser => check_agent_browser(state).await,
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
            title,
            model,
        } => persist_session(
            state,
            sessions,
            &provider,
            &provider_session_id,
            &workspace,
            &title,
            model.as_deref(),
        ),
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
        Effect::LoadSubagents(parent_session_id) => {
            load_subagents(state, sessions, &parent_session_id);
        }
        Effect::UpdateSessionModel { session_id, model } => {
            update_session_model(state, sessions, &session_id, model.as_deref());
        }
        Effect::TouchSession(id) => touch_session(state, sessions, &id),
        Effect::DeleteSession(id) => delete_session(state, sessions, &id),
        _ => unreachable!("only persistence effects are routed here"),
    }
}

fn persist_session(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    provider: &str,
    provider_session_id: &str,
    workspace: &str,
    title: &str,
    model: Option<&str>,
) {
    match sessions.create_with_id(
        &state.nakode_session_id,
        provider,
        provider_session_id,
        workspace,
        title,
        model,
    ) {
        Ok(record) => state.session_persisted(&record),
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
    if let Err(error) = backends.send_session(session_id, &provider, command).await {
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
    if let Err(error) = backends.spawn_subagent(run_id.to_owned(), provider).await {
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

fn touch_session(state: &mut DomainState, sessions: &dyn SessionRepository, id: &str) {
    if let Err(error) = sessions.touch(id) {
        state.session_store_failed(error.to_string());
    }
}

fn delete_session(state: &mut DomainState, sessions: &dyn SessionRepository, id: &str) {
    if let Err(error) = sessions.delete(id) {
        state.session_store_failed(error.to_string());
    }
}

fn update_session_model(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    id: &str,
    model: Option<&str>,
) {
    if let Err(error) = sessions.update_model(id, model) {
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
    backends.memory_service.reset().await;
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

fn load_subagents(
    state: &mut DomainState,
    sessions: &dyn SessionRepository,
    parent_session_id: &str,
) {
    match sessions.list_subagents(parent_session_id) {
        Ok(records) => state.install_subagents(records),
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
    };

    use nakode_protocol::{ClientId, ErrorCode, Query, QueryResult, ServiceCapability, SessionId};
    use tokio::sync::mpsc;

    use super::{
        BackendRegistry, BackendSource, EffectExecutor, EffectOrigin, NativeServerRuntime,
        PersistenceServices, ProviderCredentialInput, native_service_capabilities,
        provider_enablement_changes, save_provider_credential,
    };
    use crate::{
        backend::{BackendCommand, BackendEvent, BackendHandle, BackendIdentity, CODEX_PROVIDER},
        config::{Config, OpenAiReasoningEffort},
        credential::{CredentialStore, SqliteCredentialStore},
        service::ServiceEngine,
        session::SqliteSessionRepository,
        state::{DomainState, Effect},
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
        }
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
        BackendRegistry::spawn(
            &config_for(workspace),
            &[],
            workspace.join("sessions.sqlite3"),
            HashMap::new(),
            web_config,
            memory_config,
            vision_config,
        )
        .await
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
                BackendCommand::StartSession {
                    model: Some(model.to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    owner_session_id: None,
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
                BackendCommand::StartSession {
                    model: Some("model-first".to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    owner_session_id: None,
                },
            )
            .await
            .expect("first session command");
        registry
            .send_session(
                &second_id,
                CODEX_PROVIDER,
                BackendCommand::StartSession {
                    model: Some("model-second".to_owned()),
                    instructions: None,
                    external_tools: Vec::new(),
                    replace_builtin_tools: false,
                    owner_session_id: None,
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

        registry.stop_session(&doomed).await;

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

    /// Releasing a session with nothing attached is a success, which is the dead-session case.
    #[tokio::test]
    async fn releasing_a_session_with_no_backend_is_idempotent() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = empty_registry(workspace.path()).await;
        let absent = SessionId::from("session-absent");

        registry.stop_session(&absent).await;
        registry.stop_session(&absent).await;

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
            registry.stop_session(&session_id).await;
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
        );
        let (created, _) = runtime
            .core
            .create_session_command(
                &workspace_id,
                None,
                &nakode_protocol::ModelOptions::default(),
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
