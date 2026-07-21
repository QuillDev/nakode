use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use thiserror::Error;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    agent::{AgentCatalog, AgentCatalogError},
    backend::{BackendCommand, BackendError, BackendEvent, BackendHandle},
    clipboard, codex,
    config::Config,
    control::{AgentResponse, ControlError, ControlServer, IncomingInvocation},
    controls::{self, ControlAction, ControlContext},
    credential::{
        Credential, CredentialError, CredentialStore, SecretValue, SqliteCredentialStore,
    },
    cursor, devin, render,
    selection::ScreenPoint,
    session::{ProviderRecord, SessionError, SessionRepository, SqliteSessionRepository},
    skill::{SkillCatalog, SkillCatalogError},
    state::{AgentBrowserStatus, AppState, ApprovalDecision, Effect},
    terminal::{TerminalSession, Tui},
};

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error("terminal error: {0}")]
    Terminal(#[from] io::Error),
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Agents(#[from] AgentCatalogError),
    #[error(transparent)]
    Skills(#[from] SkillCatalogError),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error("failed to locate the running Nakode executable: {0}")]
    CurrentExecutable(io::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackendSource {
    Primary(String),
    Subagent(String),
}

struct BackendRegistry {
    commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    subagent_commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    subagent_providers: HashMap<String, String>,
    events: mpsc::Receiver<(BackendSource, BackendEvent)>,
    event_tx: mpsc::Sender<(BackendSource, BackendEvent)>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    failures: Vec<(String, String)>,
    config: Config,
    session_database: PathBuf,
    provider_credentials: HashMap<String, serde_json::Value>,
    provider_cooldowns: HashMap<String, ProviderCooldown>,
    web_config: Arc<RwLock<crate::web::WebConfig>>,
    vision_config: Arc<RwLock<crate::vision::VisionConfig>>,
    vision_service: Option<crate::vision::SharedVisionService>,
}

struct PersistenceServices<'a> {
    sessions: &'a dyn SessionRepository,
    credentials: &'a dyn CredentialStore,
}

struct InteractiveServices<'a> {
    control: &'a mut ControlServer,
    signals: &'a mut ShutdownSignals,
    image_renderer: Option<&'a mut crate::terminal_image::TerminalImageRenderer>,
}

struct ProviderCooldown {
    until: Instant,
    reason: String,
}

const PROVIDER_FATAL_ERROR_COOLDOWN: Duration = Duration::from_secs(15 * 60);

impl BackendRegistry {
    fn current_web_config(&self) -> crate::web::WebConfig {
        self.web_config.read().map_or_else(
            |_| crate::web::WebConfig::default(),
            |config| config.clone(),
        )
    }

    fn current_vision_config(&self) -> crate::vision::VisionConfig {
        self.vision_config.read().map_or_else(
            |_| crate::vision::VisionConfig::default(),
            |config| config.clone(),
        )
    }

    async fn spawn(
        config: &Config,
        providers: &[ProviderRecord],
        session_database: PathBuf,
        provider_credentials: HashMap<String, serde_json::Value>,
        web_config: Arc<RwLock<crate::web::WebConfig>>,
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

    async fn start_provider(&mut self, provider: &str) -> Result<(), BackendError> {
        if self.commands.contains_key(provider) {
            return Ok(());
        }
        let handle = match provider {
            crate::backend::CODEX_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                codex::spawn(
                    codex::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_reasoning_effort(self.config.openai_reasoning_effort.as_str())
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CURSOR_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                cursor::spawn(
                    cursor::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::DEVIN_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                devin::spawn(
                    devin::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
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
        self.insert_primary(provider.to_owned(), handle);
        Ok(())
    }

    async fn stop_provider(&mut self, provider: &str) {
        if let Some(commands) = self.commands.remove(provider) {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
    }

    fn set_provider_credential(&mut self, provider: &str, metadata: serde_json::Value) {
        self.provider_credentials
            .insert(provider.to_owned(), metadata.clone());
        if provider == crate::backend::CODEX_PROVIDER
            && let Ok(service) =
                codex::vision_service(Some(metadata), Arc::clone(&self.vision_config))
        {
            self.vision_service = service;
        }
    }

    fn insert_primary(&mut self, provider: String, handle: BackendHandle) {
        let (commands, mut events, task) = handle.into_parts();
        self.commands.insert(provider.clone(), commands);
        self.tasks.push(task);
        let event_tx = self.event_tx.clone();
        self.tasks.push(tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx
                    .send((BackendSource::Primary(provider.clone()), event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }));
    }

    async fn spawn_subagent(&mut self, run_id: String, provider: &str) -> Result<(), BackendError> {
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
        let handle = match provider {
            crate::backend::CODEX_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                codex::spawn(
                    codex::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_reasoning_effort(self.config.openai_reasoning_effort.as_str())
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::CURSOR_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                cursor::spawn(
                    cursor::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_vision(Arc::clone(&self.vision_config), self.vision_service.clone()),
                )
                .await?
            }
            crate::backend::DEVIN_PROVIDER => {
                let credential = self.provider_credentials.get(provider).cloned();
                devin::spawn(
                    devin::BackendConfig::native(self.config.workspace.clone())
                        .with_credential(credential)
                        .with_compaction_threshold_percent(usize::from(
                            self.config.compaction_threshold_percent,
                        ))
                        .with_session_database(self.session_database.clone())
                        .with_web_config(Arc::clone(&self.web_config))
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
        let (commands, mut events, task) = handle.into_parts();
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

    fn observe_provider_event(&mut self, source: &BackendSource, event: &BackendEvent) {
        let provider = match source {
            BackendSource::Primary(provider) => Some(provider.clone()),
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

    fn active_cooldown(&mut self, provider: &str) -> Option<(u64, String)> {
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

    async fn send(&self, provider: &str, command: BackendCommand) -> bool {
        let Some(commands) = self.commands.get(provider) else {
            return false;
        };
        commands.send(command).await.is_ok()
    }

    async fn send_subagent(&self, run_id: &str, command: BackendCommand) -> bool {
        let Some(commands) = self.subagent_commands.get(run_id) else {
            return false;
        };
        commands.send(command).await.is_ok()
    }

    async fn stop_subagent(&mut self, run_id: &str) {
        self.subagent_providers.remove(run_id);
        if let Some(commands) = self.subagent_commands.remove(run_id) {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
    }

    async fn clear_provider_credential(&mut self, provider: &str) -> io::Result<()> {
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

    async fn shutdown(self) {
        for commands in self.commands.values() {
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

/// Runs the interactive application until the user exits or a subsystem fails.
///
/// # Errors
///
/// Returns an error when provider startup, persistence, signal handling, or
/// terminal ownership fails.
pub async fn run(config: Config) -> Result<(), AppError> {
    let nakode_executable = std::env::current_exe().map_err(AppError::CurrentExecutable)?;
    let mut signals = ShutdownSignals::install()?;
    let sessions = SqliteSessionRepository::open_default()?;
    let session_database = sessions.database_path().to_path_buf();
    let credentials = SqliteCredentialStore::open(&session_database)?;
    let (providers, mut backends) = start_backends(&config, &sessions, &credentials).await?;
    let agents = AgentCatalog::load(&config.agents)?;
    let skills = SkillCatalog::load(&config.workspace)?;
    let mut state = initial_state(&config, &providers, &backends, agents, skills);
    let image_mode = sessions.load_terminal_image_mode()?;
    state.install_terminal_image_mode(image_mode);
    state.set_nakode_executable(&nakode_executable);
    for provider in backends.commands.keys() {
        match sessions.list_models(provider) {
            Ok(models) => state.install_cached_models(models),
            Err(error) => state.session_store_failed(error.to_string()),
        }
        let _ = backends
            .send(provider, BackendCommand::Reload { session_id: None })
            .await;
    }
    state.set_startup_resume(config.resume);

    let (control_path, mut control, registration) =
        match start_tui_control(&nakode_executable, &state.nakode_session_id).await {
            Ok(control) => control,
            Err(error) => {
                backends.shutdown().await;
                return Err(AppError::Control(error));
            }
        };
    let mut terminal = match TerminalSession::enter() {
        Ok(terminal) => terminal,
        Err(error) => {
            backends.shutdown().await;
            registration.shutdown().await;
            control.shutdown(&control_path);
            return Err(AppError::Terminal(error));
        }
    };
    let mut image_renderer = crate::terminal_image::TerminalImageRenderer::detect(image_mode);
    state
        .transcript
        .set_image_previews_enabled(image_renderer.is_some());
    let mut herdr = crate::herdr::Reporter::from_environment();

    let persistence = PersistenceServices {
        sessions: &sessions,
        credentials: &credentials,
    };
    let loop_result = {
        let mut interactive = InteractiveServices {
            control: &mut control,
            signals: &mut signals,
            image_renderer: image_renderer.as_mut(),
        };
        run_loop(
            terminal.terminal_mut(),
            &mut state,
            &mut backends,
            &persistence,
            &mut interactive,
            herdr.as_mut(),
        )
        .await
    };

    if let Some(reporter) = herdr {
        reporter.shutdown().await;
    }
    let restore_result = terminal.restore();
    backends.shutdown().await;
    registration.shutdown().await;
    control.shutdown(&control_path);

    loop_result?;
    restore_result.map_err(AppError::Terminal)?;

    print_resume_hint(&nakode_executable, &state).map_err(AppError::Terminal)
}

fn initial_state(
    config: &Config,
    providers: &[ProviderRecord],
    backends: &BackendRegistry,
    agents: AgentCatalog,
    skills: SkillCatalog,
) -> AppState {
    let active_provider = if backends
        .commands
        .contains_key(crate::backend::CODEX_PROVIDER)
    {
        crate::backend::CODEX_PROVIDER.to_owned()
    } else {
        backends.commands.keys().next().cloned().unwrap_or_default()
    };
    let mut state = if active_provider.is_empty() {
        AppState::new_unconfigured(
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
        AppState::new_for_backend(
            config.workspace.to_string_lossy(),
            config.model.clone(),
            config.scrollback,
            &active_provider,
            active_name,
        )
    };
    state.install_web_config(backends.current_web_config());
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

async fn start_backends(
    config: &Config,
    sessions: &SqliteSessionRepository,
    credentials: &dyn CredentialStore,
) -> Result<(Vec<ProviderRecord>, BackendRegistry), SessionError> {
    let providers = sessions.list_providers()?;
    let (provider_credentials, credential_failures) =
        load_provider_credentials(&providers, credentials);
    let web_config = shared_web_config(sessions)?;
    let vision_config = shared_vision_config(sessions)?;
    let mut backends = BackendRegistry::spawn(
        config,
        &providers,
        sessions.database_path().to_path_buf(),
        provider_credentials,
        web_config,
        vision_config,
    )
    .await;
    backends.failures.extend(credential_failures);
    Ok((providers, backends))
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

async fn start_tui_control(
    executable: &Path,
    session_id: &str,
) -> Result<(PathBuf, ControlServer, crate::control::ControlRegistration), ControlError> {
    let path = crate::control::tui_socket_path()?;
    let control = ControlServer::bind(&path).await?;
    match crate::control::ControlRegistration::start(executable, session_id, &path).await {
        Ok(registration) => Ok((path, control, registration)),
        Err(error) => {
            control.shutdown(&path);
            Err(error)
        }
    }
}

fn shared_web_config(
    sessions: &dyn SessionRepository,
) -> Result<Arc<RwLock<crate::web::WebConfig>>, SessionError> {
    sessions
        .load_web_config()
        .map(|config| Arc::new(RwLock::new(config)))
}

fn shared_vision_config(
    sessions: &dyn SessionRepository,
) -> Result<Arc<RwLock<crate::vision::VisionConfig>>, SessionError> {
    sessions
        .load_vision_config()
        .map(|config| Arc::new(RwLock::new(config)))
}

fn print_resume_hint(executable: &Path, state: &AppState) -> io::Result<()> {
    write_resume_hint(
        &mut io::stdout().lock(),
        executable,
        Path::new(&state.workspace),
        state.session_id.as_deref(),
    )
}

fn write_resume_hint(
    output: &mut impl Write,
    executable: &Path,
    workspace: &Path,
    session_id: Option<&str>,
) -> io::Result<()> {
    let Some(session_id) = session_id else {
        return Ok(());
    };
    writeln!(output, "\nResume this session with:")?;
    writeln!(
        output,
        "  {} --workspace {} --resume {session_id}",
        quote_command_argument(executable),
        quote_command_argument(workspace),
    )
}

fn quote_command_argument(argument: &Path) -> String {
    let argument = argument.to_string_lossy();
    if argument
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "_@%+=:,./-".contains(character))
    {
        return argument.into_owned();
    }

    #[cfg(unix)]
    {
        format!("'{}'", argument.replace('\'', "'\\''"))
    }
    #[cfg(not(unix))]
    {
        format!("\"{}\"", argument.replace('"', "\\\""))
    }
}

async fn run_loop(
    terminal: &mut Tui,
    state: &mut AppState,
    backends: &mut BackendRegistry,
    persistence: &PersistenceServices<'_>,
    interactive: &mut InteractiveServices<'_>,
    mut herdr: Option<&mut crate::herdr::Reporter>,
) -> io::Result<()> {
    let mut input = EventStream::new();
    let mut render_tick = tokio::time::interval(Duration::from_millis(33));
    render_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut backend_open = true;
    let mut dirty = true;
    let mut agent_requests = HashMap::<u64, IncomingInvocation>::new();

    if let Some(reporter) = &mut herdr {
        reporter.sync(state);
    }

    loop {
        tokio::select! {
            input_event = input.next() => {
                match input_event {
                    Some(Ok(event)) => {
                        let effects = handle_terminal_event(state, event);
                        flush_pending_clipboard(terminal, state);
                        if apply_effects(state, effects, backends, persistence, &mut agent_requests).await {
                            break;
                        }
                        dirty = true;
                    }
                    Some(Err(error)) => return Err(error),
                    None => {
                        state.should_quit = true;
                        break;
                    }
                }
            }
            backend_event = backends.events.recv(), if backend_open => {
                if let Some((source, event)) = backend_event {
                    backends.observe_provider_event(&source, &event);
                    let should_chime = should_chime_for_backend_event(&source, &event);
                    let effects = match source {
                        BackendSource::Primary(provider) => state.handle_provider_backend(&provider, event),
                        BackendSource::Subagent(run_id) => state.handle_subagent_backend(&run_id, event),
                    };
                    if should_chime {
                        crate::terminal::ring_bell(terminal.backend_mut())?;
                    }
                    if apply_effects(state, effects, backends, persistence, &mut agent_requests).await {
                        break;
                    }
                    dirty = true;
                } else {
                    backend_open = false;
                    state.set_status("All provider event channels closed.");
                    dirty = true;
                }
            }
            request = interactive.control.requests.recv() => {
                if let Some(request) = request {
                    if request.invocation.session_id == state.nakode_session_id {
                        let id = request.id;
                        let invocation = crate::state::AgentRequest { id, agent: request.invocation.agent.clone(), task: request.invocation.task.clone() };
                        agent_requests.insert(id, request);
                        let effects = state.invoke_agent(&invocation);
                        if apply_effects(state, effects, backends, persistence, &mut agent_requests).await { break; }
                    } else {
                        request.respond(AgentResponse { success: false, result: "Nakode session id does not match this TUI.".to_owned() });
                    }
                    dirty = true;
                }
            }
            () = interactive.signals.recv() => {
                state.should_quit = true;
                break;
            }
            _ = render_tick.tick() => {
                if dirty || state.is_busy() {
                    terminal.draw(|frame| {
                        render::draw_with_images(
                            frame,
                            state,
                            interactive.image_renderer.as_deref_mut(),
                        );
                    })?;
                    dirty = false;
                }
            }
        }

        if let Some(reporter) = &mut herdr {
            reporter.sync(state);
        }

        if state.should_quit {
            break;
        }
    }

    Ok(())
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

fn should_chime_for_backend_event(source: &BackendSource, event: &BackendEvent) -> bool {
    matches!(source, BackendSource::Primary(_))
        && matches!(event, BackendEvent::TurnCompleted { .. })
}

async fn apply_effects(
    state: &mut AppState,
    effects: Vec<Effect>,
    backends: &mut BackendRegistry,
    persistence: &PersistenceServices<'_>,
    agent_requests: &mut HashMap<u64, IncomingInvocation>,
) -> bool {
    let sessions = persistence.sessions;
    let credentials = persistence.credentials;
    let (mut quit, mut pending) = (false, std::collections::VecDeque::from(effects));
    while let Some(effect) = pending.pop_front() {
        match effect {
            Effect::Backend(command) => send_backend_command(state, backends, command).await,
            Effect::SpawnSubagent { run_id, provider } => {
                spawn_subagent(state, backends, &mut pending, &run_id, &provider).await;
            }
            Effect::SubagentBackend { run_id, command } => {
                send_subagent_command(state, backends, &mut pending, &run_id, command).await;
            }
            Effect::StopSubagent(run_id) => backends.stop_subagent(&run_id).await,
            Effect::CompleteAgentRequest {
                request_id,
                result,
                success,
            } => complete_agent_request(agent_requests, request_id, result, success),
            Effect::ListSessions => list_sessions(state, sessions),
            Effect::ListProviders => install_provider_records(state, sessions),
            Effect::SetProviderEnabled { provider, enabled } => {
                apply_provider_enablement(state, backends, sessions, &provider, enabled).await;
            }
            Effect::AuthenticateProvider(provider) => {
                authenticate_provider(state, backends, &provider).await;
            }
            Effect::SaveProviderCredential {
                provider,
                kind,
                metadata,
            } => {
                save_provider_credential_effect(
                    state,
                    backends,
                    persistence,
                    &mut pending,
                    (provider, kind, metadata),
                )
                .await;
            }
            Effect::ClearProviderCredential(provider) => {
                clear_provider_credential(state, backends, sessions, credentials, &provider).await;
            }
            Effect::OpenUrl(url) => open_url(state, &url),
            Effect::SaveAgent {
                definition,
                previous_slug,
            } => save_agent_definition(state, &definition, previous_slug.as_deref()),
            Effect::DeleteAgent(slug) => delete_agent_definition(state, &slug),
            Effect::ReloadConfiguration => apply_configuration_reload(state, &mut pending),
            Effect::ResolveSession(id) => match sessions.find(&id) {
                Ok(Some(record)) => pending.extend(state.begin_resume(record)),
                Ok(None) => state.session_store_failed(format!("no session matches {id:?}")),
                Err(error) => state.session_store_failed(error.to_string()),
            },
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
            Effect::PersistSubagent(record) => persist_subagent(state, sessions, &record),
            Effect::LoadSubagents(parent_session_id) => {
                load_subagents(state, sessions, &parent_session_id);
            }
            Effect::UpdateSessionModel { session_id, model } => {
                update_session_model(state, sessions, &session_id, model.as_deref());
            }
            Effect::TouchSession(id) => touch_session(state, sessions, &id),
            Effect::SaveWebConfig(config) => {
                save_web_config_effect(state, backends, sessions, config);
            }
            Effect::SaveVisionConfig(config) => {
                save_vision_config_effect(state, backends, sessions, config);
            }
            Effect::SaveTerminalImageMode(mode) => {
                save_terminal_image_mode_effect(state, sessions, mode);
            }
            Effect::CheckAgentBrowser => check_agent_browser(state).await,
            Effect::Quit => quit = true,
        }
    }
    quit
}

fn persist_session(
    state: &mut AppState,
    sessions: &dyn SessionRepository,
    provider: &str,
    provider_session_id: &str,
    workspace: &str,
    title: &str,
    model: Option<&str>,
) {
    match sessions.create(provider, provider_session_id, workspace, title, model) {
        Ok(record) => state.session_persisted(&record),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn persist_models(
    state: &mut AppState,
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
    state: &mut AppState,
    sessions: &dyn SessionRepository,
    provider: &str,
    model: &str,
) {
    if let Err(error) = sessions.set_default_model(provider, model) {
        state.session_store_failed(error.to_string());
    }
}

fn open_url(state: &mut AppState, url: &str) {
    match open::that(url) {
        Ok(()) => state.set_status("Opened the authentication page in your browser."),
        Err(error) => {
            state.set_status(&format!("Could not open the authentication page: {error}"));
        }
    }
}

fn install_provider_records(state: &mut AppState, sessions: &dyn SessionRepository) {
    match sessions.list_providers() {
        Ok(providers) => state.install_providers(providers),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn list_sessions(state: &mut AppState, sessions: &dyn SessionRepository) {
    match sessions.list_recent(&state.workspace, 100) {
        Ok(records) => state.install_sessions(records),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn complete_agent_request(
    agent_requests: &mut HashMap<u64, IncomingInvocation>,
    request_id: u64,
    result: String,
    success: bool,
) {
    if let Some(request) = agent_requests.remove(&request_id) {
        request.respond(AgentResponse { success, result });
    }
}

async fn send_backend_command(
    state: &mut AppState,
    backends: &BackendRegistry,
    command: BackendCommand,
) {
    let provider = state.backend_provider.clone();
    if !backends.send(&provider, command).await {
        state.handle_provider_backend(
            &provider,
            BackendEvent::Disconnected {
                reason: "backend command channel closed".to_owned(),
            },
        );
    }
}

async fn spawn_subagent(
    state: &mut AppState,
    backends: &mut BackendRegistry,
    pending: &mut std::collections::VecDeque<Effect>,
    run_id: &str,
    provider: &str,
) {
    if let Err(error) = backends.spawn_subagent(run_id.to_owned(), provider).await {
        pending.extend(state.subagent_launch_failed(run_id, error.to_string()));
    }
}

async fn send_subagent_command(
    state: &mut AppState,
    backends: &BackendRegistry,
    pending: &mut std::collections::VecDeque<Effect>,
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
    state: &mut AppState,
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

async fn save_provider_credential_effect(
    state: &mut AppState,
    backends: &mut BackendRegistry,
    persistence: &PersistenceServices<'_>,
    pending: &mut std::collections::VecDeque<Effect>,
    credential: (String, String, serde_json::Value),
) {
    let (provider, kind, metadata) = credential;
    let credential = ProviderCredentialInput {
        provider,
        kind,
        metadata,
    };
    persist_provider_credential(
        state,
        backends,
        persistence.sessions,
        persistence.credentials,
        pending,
        credential,
    )
    .await;
}

async fn persist_provider_credential(
    state: &mut AppState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    credentials: &dyn CredentialStore,
    pending: &mut std::collections::VecDeque<Effect>,
    credential: ProviderCredentialInput,
) {
    let stored = Credential {
        kind: credential.kind,
        secret: SecretValue::new(credential.metadata.clone()),
    };
    if let Err(error) = credentials.put(&credential.provider, &stored) {
        state.session_store_failed(error.to_string());
        return;
    }
    backends.set_provider_credential(&credential.provider, credential.metadata.clone());
    backends.stop_provider(&credential.provider).await;
    match sessions.list_providers() {
        Ok(providers) => state.install_providers(providers),
        Err(error) => state.session_store_failed(error.to_string()),
    }
    pending.push_back(Effect::SetProviderEnabled {
        provider: credential.provider,
        enabled: true,
    });
}

async fn clear_provider_credential(
    state: &mut AppState,
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
    if let Err(error) = credentials.delete(provider) {
        state.session_store_failed(error.to_string());
        return;
    }
    match sessions.list_providers() {
        Ok(providers) => state.install_providers(providers),
        Err(error) => {
            state.session_store_failed(error.to_string());
            return;
        }
    }
    state.provider_logged_out(provider);
}

async fn apply_provider_enablement(
    state: &mut AppState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    provider: &str,
    enabled: bool,
) {
    if let Err(error) = sessions.set_provider_enabled(provider, enabled) {
        state.session_store_failed(error.to_string());
        return;
    }
    match sessions.list_providers() {
        Ok(providers) => state.install_providers(providers),
        Err(error) => state.session_store_failed(error.to_string()),
    }
    if !enabled {
        backends.stop_provider(provider).await;
        state.provider_disabled(provider);
        return;
    }

    let display_name = state.provider_display_name(provider);
    state.provider_starting(provider, &display_name);
    if let Err(error) = backends.start_provider(provider).await {
        state.provider_start_failed(provider, &display_name, &error.to_string());
        return;
    }
    match sessions.list_models(provider) {
        Ok(models) => state.install_cached_models(models),
        Err(error) => state.session_store_failed(error.to_string()),
    }
    let _ = backends
        .send(provider, BackendCommand::Reload { session_id: None })
        .await;
}

fn apply_configuration_reload(
    state: &mut AppState,
    pending: &mut std::collections::VecDeque<Effect>,
) {
    let reload_backend = state.connection.is_ready() && !state.backend_provider.is_empty();
    let session_id = state.provider_session_id.clone();
    match reload_local_configuration(state) {
        Ok((agent_count, skill_count)) => {
            state.configuration_reloaded(agent_count, skill_count, reload_backend);
            if reload_backend {
                pending.push_front(Effect::Backend(BackendCommand::Reload { session_id }));
            }
        }
        Err(error) => state.configuration_reload_failed(&error),
    }
}

fn reload_local_configuration(state: &mut AppState) -> Result<(usize, usize), String> {
    let agents = AgentCatalog::load(state.agent_directory())
        .map_err(|error| format!("could not reload agents: {error}"))?;
    let skills = SkillCatalog::load(Path::new(&state.workspace))
        .map_err(|error| format!("could not reload skills: {error}"))?;
    let agent_count = agents.definitions().len();
    let skill_count = skills.definitions().len();
    state.install_agents(agents);
    state.install_skills(skills);
    Ok((agent_count, skill_count))
}

fn save_agent_definition(
    state: &mut AppState,
    definition: &crate::agent::AgentDefinition,
    previous_slug: Option<&str>,
) {
    let directory = state.agent_directory().to_path_buf();
    let result = AgentCatalog::load(&directory).and_then(|catalog| {
        catalog.save(&directory, definition, previous_slug)?;
        AgentCatalog::load(&directory)
    });
    install_changed_agent_catalog(state, result, "Agent archetype saved.");
}

fn delete_agent_definition(state: &mut AppState, slug: &str) {
    let directory = state.agent_directory().to_path_buf();
    let result = AgentCatalog::load(&directory).and_then(|catalog| {
        catalog.delete(&directory, slug)?;
        AgentCatalog::load(&directory)
    });
    install_changed_agent_catalog(state, result, "Agent archetype deleted.");
}

fn install_changed_agent_catalog(
    state: &mut AppState,
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

fn touch_session(state: &mut AppState, sessions: &dyn SessionRepository, id: &str) {
    if let Err(error) = sessions.touch(id) {
        state.session_store_failed(error.to_string());
    }
}

fn update_session_model(
    state: &mut AppState,
    sessions: &dyn SessionRepository,
    id: &str,
    model: Option<&str>,
) {
    if let Err(error) = sessions.update_model(id, model) {
        state.session_store_failed(error.to_string());
    }
}

fn save_web_config_effect(
    state: &mut AppState,
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
    config: crate::web::WebConfig,
) {
    if let Err(error) = sessions.save_web_config(&config) {
        state.session_store_failed(error.to_string());
        return;
    }
    let Ok(mut shared) = backends.web_config.write() else {
        state.session_store_failed("browser settings lock is unavailable".to_owned());
        return;
    };
    *shared = config.clone();
    drop(shared);
    state.install_web_config(config);
    state.set_status("Browser add-on settings saved.");
}

fn save_vision_config_effect(
    state: &mut AppState,
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
    config: crate::vision::VisionConfig,
) {
    if let Err(error) = sessions.save_vision_config(&config) {
        state.session_store_failed(error.to_string());
        return;
    }
    let Ok(mut shared) = backends.vision_config.write() else {
        state.session_store_failed("vision settings lock is unavailable".to_owned());
        return;
    };
    *shared = config.clone();
    drop(shared);
    state.install_vision_config(config);
    state.set_status("Vision add-on settings saved.");
}

fn save_terminal_image_mode_effect(
    state: &mut AppState,
    sessions: &dyn SessionRepository,
    mode: crate::terminal_image::TerminalImageMode,
) {
    if let Err(error) = sessions.save_terminal_image_mode(mode) {
        state.session_store_failed(error.to_string());
        return;
    }
    state.install_terminal_image_mode(mode);
    state.set_status("Terminal image setting saved; changes apply on next launch.");
}

fn persist_subagent(
    state: &mut AppState,
    sessions: &dyn SessionRepository,
    record: &crate::session::SubagentRecord,
) {
    if let Err(error) = sessions.save_subagent(record) {
        state.session_store_failed(error.to_string());
    }
}

fn load_subagents(state: &mut AppState, sessions: &dyn SessionRepository, parent_session_id: &str) {
    match sessions.list_subagents(parent_session_id) {
        Ok(records) => state.install_subagents(records),
        Err(error) => state.session_store_failed(error.to_string()),
    }
}

fn flush_pending_clipboard(terminal: &mut Tui, state: &mut AppState) {
    let Some(text) = state.take_pending_clipboard() else {
        return;
    };
    let inside_tmux = std::env::var_os("TMUX").is_some();
    match clipboard::write_osc52(terminal.backend_mut(), &text, inside_tmux) {
        Ok(bytes) => state.clipboard_copied(bytes),
        Err(error) => state.clipboard_failed(&error.to_string()),
    }
}

fn handle_terminal_event(state: &mut AppState, event: Event) -> Vec<Effect> {
    match event {
        Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
            state.clear_text_selection();
            handle_key(state, key)
        }
        Event::Paste(text) => {
            state.clear_text_selection();
            if state.provider_api_key_input_active() {
                state.provider_api_key_insert_str(&text);
            } else if state
                .agent_picker
                .as_ref()
                .is_some_and(|picker| picker.editor.is_some())
            {
                state.agent_editor_insert_str(&text);
            } else if state.settings.is_some() {
                for character in text.chars() {
                    state.settings_insert(character);
                }
            } else if state.model_picker.is_none()
                && state.session_picker.is_none()
                && state.provider_picker.is_none()
                && state.agent_picker.is_none()
                && !state.show_help
                && state.approvals.is_empty()
            {
                if let Some(attachments) = clipboard::attachments_from_terminal_paste(&text) {
                    state.insert_attachments(attachments);
                } else {
                    state.editor.insert_str(&text);
                    state.set_status("Pasted text into the draft.");
                }
            }
            Vec::new()
        }
        Event::Mouse(mouse) => {
            let point = ScreenPoint::new(mouse.column, mouse.row);
            let action = controls::resolve_mouse(mouse.kind);
            if action == controls::MouseAction::PrimaryDown
                && let Some(url) = state.oauth_url_at(point)
            {
                state.clear_text_selection();
                return vec![Effect::OpenUrl(url)];
            }
            if action == controls::MouseAction::PrimaryDown
                && state.focus_provider_api_key_at(point)
            {
                state.clear_text_selection();
                return Vec::new();
            }
            if action == controls::MouseAction::PrimaryDown && state.toggle_tool_at(point) {
                return Vec::new();
            }
            match action {
                controls::MouseAction::PrimaryDown
                    if state.subagent_modal.is_some() || !state.open_subagent_at(point) =>
                {
                    state.begin_text_selection(point);
                }
                controls::MouseAction::PrimaryDown | controls::MouseAction::Ignore => {}
                controls::MouseAction::PrimaryDrag => state.update_text_selection(point),
                controls::MouseAction::PrimaryUp => state.finish_text_selection(point),
                controls::MouseAction::ScrollUp => {
                    state.clear_text_selection();
                    state.scroll_active_chat(3);
                }
                controls::MouseAction::ScrollDown => {
                    state.clear_text_selection();
                    state.scroll_active_chat(-3);
                }
                controls::MouseAction::ClearSelection => state.clear_text_selection(),
            }
            Vec::new()
        }
        Event::Resize(_, _) => {
            state.clear_text_selection();
            Vec::new()
        }
        Event::FocusGained | Event::FocusLost | Event::Key(_) => Vec::new(),
    }
}

fn handle_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    if let Some(effects) = handle_modal_key(state, key) {
        return effects;
    }

    if let Some(effects) = handle_command_completion_key(state, key) {
        return effects;
    }
    if handle_editor_navigation(state, key) {
        return Vec::new();
    }

    match controls::resolve(ControlContext::Global, key) {
        Some(ControlAction::CancelOrQuit) => state.cancel_or_quit(),
        Some(ControlAction::Quit) => state.request_quit(),
        Some(ControlAction::QueueDraft) => state.enqueue_editor(),
        Some(ControlAction::Steer) => state.steer_editor(),
        Some(ControlAction::Latest) => {
            state.reset_active_chat_scroll();
            state.set_status("Jumped to the latest output.");
            Vec::new()
        }
        Some(ControlAction::Newline) => {
            state.editor.insert_newline();
            Vec::new()
        }
        Some(ControlAction::Paste) => {
            paste_desktop_clipboard(state);
            Vec::new()
        }
        Some(ControlAction::OpenModelPicker) => state.open_model_picker(),
        Some(ControlAction::ScrollUp) => {
            state.scroll_active_chat(10);
            Vec::new()
        }
        Some(ControlAction::ScrollDown) => {
            state.scroll_active_chat(-10);
            Vec::new()
        }
        Some(ControlAction::QueuePrevious) => {
            state.move_queue_selection(-1);
            Vec::new()
        }
        Some(ControlAction::QueueNext) => {
            state.move_queue_selection(1);
            Vec::new()
        }
        Some(ControlAction::QueueRemove) => {
            state.remove_selected_queue_item();
            Vec::new()
        }
        Some(ControlAction::Submit) => state.submit_editor(),
        Some(ControlAction::SteerOrSubmit) => state.submit_or_steer_editor(),
        Some(ControlAction::BackspaceWord) => {
            state.editor.delete_word_backward();
            Vec::new()
        }
        Some(ControlAction::BackspaceLine) => {
            state.editor.delete_to_line_start();
            Vec::new()
        }
        Some(ControlAction::Backspace) => {
            state.editor.backspace();
            Vec::new()
        }
        Some(ControlAction::Delete) => {
            state.editor.delete();
            Vec::new()
        }
        Some(ControlAction::InsertTab) => {
            state.editor.insert_char('\t');
            Vec::new()
        }
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                state.editor.insert_char(character);
            }
            Vec::new()
        }
        Some(_) => Vec::new(),
    }
}

fn paste_desktop_clipboard(state: &mut AppState) {
    match clipboard::read_desktop() {
        Ok(clipboard::ClipboardPayload::Attachments(attachments)) => {
            state.insert_attachments(attachments);
        }
        Ok(clipboard::ClipboardPayload::Text(text)) => {
            state.editor.insert_str(&text);
            state.set_status("Pasted text into the draft.");
        }
        Err(error) => state.set_status(&format!("Could not paste: {error}")),
    }
}

fn handle_editor_navigation(state: &mut AppState, key: KeyEvent) -> bool {
    match controls::resolve(ControlContext::Navigation, key) {
        Some(ControlAction::MoveWordLeft) => state.editor.move_word_left(),
        Some(ControlAction::MoveWordRight) => state.editor.move_word_right(),
        Some(ControlAction::MoveLineStart) => state.editor.move_home(),
        Some(ControlAction::MoveLineEnd) => state.editor.move_end(),
        Some(ControlAction::MoveDocumentStart) => state.editor.move_document_start(),
        Some(ControlAction::MoveDocumentEnd) => state.editor.move_document_end(),
        Some(ControlAction::MoveLeft) => state.editor.move_left(),
        Some(ControlAction::MoveRight) => state.editor.move_right(),
        Some(ControlAction::MoveUp) => state.editor.move_up(),
        Some(ControlAction::MoveDown) => state.editor.move_down(),
        _ => return false,
    }
    true
}

fn handle_command_completion_key(state: &mut AppState, key: KeyEvent) -> Option<Vec<Effect>> {
    if state.command_completions().is_empty() {
        return None;
    }

    match controls::resolve(ControlContext::CommandCompletion, key) {
        Some(ControlAction::CompletionPrevious) => state.move_command_completion(-1),
        Some(ControlAction::CompletionNext) => state.move_command_completion(1),
        Some(ControlAction::CompletionAccept) if !state.command_completion_is_exact() => {
            state.accept_command_completion();
        }
        _ => return None,
    }
    Some(Vec::new())
}

fn handle_modal_key(state: &mut AppState, key: KeyEvent) -> Option<Vec<Effect>> {
    if !state.questions.is_empty() {
        return Some(handle_question_key(state, key));
    }
    if !state.approvals.is_empty() {
        return Some(match controls::resolve(ControlContext::Approval, key) {
            Some(ControlAction::ApprovalOnce) => {
                state.resolve_approval(ApprovalDecision::AcceptOnce)
            }
            Some(ControlAction::ApprovalSession) => {
                state.resolve_approval(ApprovalDecision::AcceptForSession)
            }
            Some(ControlAction::ApprovalDecline) => {
                state.resolve_approval(ApprovalDecision::Decline)
            }
            _ => Vec::new(),
        });
    }

    if state.subagent_modal.is_some() {
        return Some(handle_subagent_modal_key(state, key));
    }

    if state.show_help {
        if controls::resolve(ControlContext::Help, key).is_some() {
            state.show_help = false;
        }
        return Some(Vec::new());
    }

    if controls::resolve(ControlContext::Global, key) == Some(ControlAction::ToggleHelp) {
        state.model_picker = None;
        state.session_picker = None;
        state.agent_picker = None;
        state.settings = None;
        state.show_help = true;
        return Some(Vec::new());
    }

    if state.settings.is_some() {
        return Some(handle_settings_key(state, key));
    }

    if state.agent_picker.is_some() {
        return Some(handle_agent_picker_key(state, key));
    }

    if state.session_picker.is_some() {
        return Some(
            match controls::resolve(ControlContext::SessionPicker, key) {
                Some(ControlAction::Close) => {
                    state.close_session_picker();
                    Vec::new()
                }
                Some(ControlAction::Select) => state.select_session(),
                Some(ControlAction::Previous) => {
                    state.session_picker_move(-1);
                    Vec::new()
                }
                Some(ControlAction::Next) => {
                    state.session_picker_move(1);
                    Vec::new()
                }
                _ => Vec::new(),
            },
        );
    }

    if state.provider_picker.is_some() {
        return Some(handle_provider_picker_key(state, key));
    }

    if state.model_picker.is_some() {
        match controls::resolve(ControlContext::ModelPicker, key) {
            Some(ControlAction::Select) => return Some(state.picker_select()),
            Some(ControlAction::Close) => state.close_model_picker(),
            Some(ControlAction::Previous) => state.picker_move(-1),
            Some(ControlAction::Next) => state.picker_move(1),
            Some(ControlAction::Backspace) => state.picker_backspace(),
            Some(ControlAction::Clear) => {
                if let Some(picker) = &mut state.model_picker {
                    picker.filter.clear();
                    picker.selected = 0;
                }
            }
            None => {
                if let KeyCode::Char(character) = key.code
                    && !key.modifiers.intersects(
                        KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER,
                    )
                {
                    state.picker_insert(character);
                }
            }
            Some(_) => {}
        }
        return Some(Vec::new());
    }
    None
}

fn handle_question_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    match controls::resolve(ControlContext::Question, key) {
        Some(ControlAction::QuestionPrevious) => {
            state.move_question_selection(-1);
            Vec::new()
        }
        Some(ControlAction::QuestionNext) => {
            state.move_question_selection(1);
            Vec::new()
        }
        Some(ControlAction::QuestionToggle) => {
            state.toggle_question_selection();
            Vec::new()
        }
        Some(ControlAction::QuestionConfirm) => state.resolve_question(),
        Some(ControlAction::QuestionQuickSelect) => {
            let KeyCode::Char(character) = key.code else {
                return Vec::new();
            };
            let selected = usize::try_from(character.to_digit(10).unwrap_or(1))
                .unwrap_or(1)
                .saturating_sub(1);
            if let Some(question) = state.questions.front_mut() {
                question.selected = selected.min(question.request.options.len().saturating_sub(1));
            }
            if state
                .questions
                .front()
                .is_some_and(|question| question.request.multi)
            {
                state.toggle_question_selection();
                Vec::new()
            } else {
                state.resolve_question()
            }
        }
        _ => Vec::new(),
    }
}

async fn check_agent_browser(state: &mut AppState) {
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

fn handle_settings_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    match controls::resolve(ControlContext::Settings, key) {
        Some(ControlAction::Close) => return state.settings_back(),
        Some(ControlAction::Select) => return state.select_setting(),
        Some(ControlAction::Previous) => state.settings_move(-1),
        Some(ControlAction::Next) => state.settings_move(1),
        Some(ControlAction::MoveLeft) => return state.settings_cycle_choice(-1),
        Some(ControlAction::MoveRight) => return state.settings_cycle_choice(1),
        Some(ControlAction::Clear) => return state.disable_vision_addon(),
        Some(ControlAction::Backspace) => state.settings_backspace(),
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                state.settings_insert(character);
            }
        }
        Some(_) => {}
    }
    Vec::new()
}

fn handle_provider_picker_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    let showing_details = state
        .provider_picker
        .as_ref()
        .is_some_and(|picker| picker.showing_details);
    let api_key_input = state.provider_api_key_input_active();
    let context = if api_key_input {
        ControlContext::ProviderCredential
    } else if showing_details {
        ControlContext::ProviderDetails
    } else {
        ControlContext::ProviderList
    };
    match controls::resolve(context, key) {
        Some(ControlAction::Close) => {
            if !state.cancel_provider_api_key_input() && !state.close_provider_details() {
                state.close_provider_picker();
            }
            Vec::new()
        }
        Some(ControlAction::Backspace) if api_key_input => {
            state.provider_api_key_backspace();
            Vec::new()
        }
        Some(ControlAction::Submit) if api_key_input => state.submit_provider_api_key(),
        Some(ControlAction::OpenUrl) => state.open_provider_authentication_url(),
        Some(ControlAction::CopyUrl) => state.copy_provider_authentication_url(),
        Some(ControlAction::Logout) => state.logout_provider(),
        Some(ControlAction::Toggle) => state.toggle_provider(),
        Some(ControlAction::Focus) => {
            state.focus_provider_api_key();
            Vec::new()
        }
        Some(ControlAction::Open) => {
            state.open_provider_details();
            Vec::new()
        }
        Some(ControlAction::Previous) => {
            state.provider_picker_move(-1);
            Vec::new()
        }
        Some(ControlAction::Next) => {
            state.provider_picker_move(1);
            Vec::new()
        }
        None if api_key_input => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            {
                state.provider_api_key_insert_str(&character.to_string());
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn handle_agent_picker_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    let editing = state
        .agent_picker
        .as_ref()
        .is_some_and(|picker| picker.editor.is_some());
    if editing {
        return handle_agent_editor_key(state, key);
    }
    match controls::resolve(ControlContext::AgentList, key) {
        Some(ControlAction::Close) => state.close_agent_picker(),
        Some(ControlAction::Open) => state.edit_selected_agent(),
        Some(ControlAction::Create) => state.create_agent(),
        Some(ControlAction::Delete) => return state.delete_selected_agent(),
        Some(ControlAction::Previous) => state.agent_picker_move(-1),
        Some(ControlAction::Next) => state.agent_picker_move(1),
        _ => {}
    }
    Vec::new()
}

fn handle_agent_editor_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    match controls::resolve(ControlContext::AgentEditor, key) {
        Some(ControlAction::Previous) => state.agent_editor_move(-1),
        Some(ControlAction::Close) => {
            state.cancel_agent_edit();
        }
        Some(ControlAction::Save) => return state.save_agent_edit(),
        Some(ControlAction::Next) => state.agent_editor_move(1),
        Some(ControlAction::Backspace) => state.agent_editor_backspace(),
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                state.agent_editor_insert(character);
            }
        }
        Some(_) => {}
    }
    Vec::new()
}

fn handle_subagent_modal_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    match controls::resolve(ControlContext::Subagent, key) {
        Some(ControlAction::CancelOrQuit) => return state.cancel_or_quit(),
        Some(ControlAction::Latest) => state.reset_active_chat_scroll(),
        Some(ControlAction::ScrollUp) => state.scroll_active_chat(10),
        Some(ControlAction::ScrollDown) => state.scroll_active_chat(-10),
        Some(ControlAction::Close) => state.close_subagent_modal(),
        _ => {}
    }
    Vec::new()
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
            _ = self.hangup.recv() => {}
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use clap::Parser;
    use crossterm::event::{
        Event, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };
    use ratatui::{Terminal, backend::TestBackend};

    use crate::{
        backend::{BackendEvent, CODEX_PROVIDER, CURSOR_PROVIDER, CapabilitySupport, TurnOutcome},
        render,
        session::ProviderRecord,
        state::{ActiveTurn, AgentRequest, AppState, ConnectionState, Effect},
        transcript::{EntryKind, EntryStatus},
    };

    #[test]
    fn fatal_provider_quota_errors_open_a_cooldown_for_new_workers() {
        let directory = tempfile::tempdir().expect("workspace");
        let config = crate::config::Config::try_parse_from([
            "nakode",
            "--workspace",
            directory.path().to_str().expect("workspace path"),
        ])
        .expect("config")
        .validated()
        .expect("validated config");
        let (event_tx, events) = tokio::sync::mpsc::channel(1);
        let mut registry = super::BackendRegistry {
            commands: HashMap::new(),
            subagent_commands: HashMap::new(),
            subagent_providers: HashMap::from([(
                "run-1".to_owned(),
                crate::backend::DEVIN_PROVIDER.to_owned(),
            )]),
            events,
            event_tx,
            tasks: Vec::new(),
            failures: Vec::new(),
            config,
            session_database: directory.path().join("sessions.sqlite3"),
            provider_credentials: HashMap::new(),
            provider_cooldowns: HashMap::new(),
            web_config: std::sync::Arc::new(std::sync::RwLock::new(
                crate::web::WebConfig::default(),
            )),
            vision_config: std::sync::Arc::new(std::sync::RwLock::new(
                crate::vision::VisionConfig::default(),
            )),
            vision_service: None,
        };

        registry.observe_provider_event(
            &super::BackendSource::Subagent("run-1".to_owned()),
            &BackendEvent::TurnCompleted {
                turn_id: "turn-1".to_owned(),
                outcome: TurnOutcome::Failed,
                error: Some("Your daily usage quota has been exhausted".to_owned()),
            },
        );

        let cooldown = registry
            .active_cooldown(crate::backend::DEVIN_PROVIDER)
            .expect("provider cooldown");
        assert!(cooldown.0 > 0);
        assert!(cooldown.1.contains("quota has been exhausted"));
        assert!(
            registry
                .active_cooldown(crate::backend::CODEX_PROVIDER)
                .is_none()
        );

        registry.observe_provider_event(
            &super::BackendSource::Subagent("run-1".to_owned()),
            &BackendEvent::TurnCompleted {
                turn_id: "turn-2".to_owned(),
                outcome: TurnOutcome::Completed,
                error: None,
            },
        );
        assert!(
            registry
                .active_cooldown(crate::backend::DEVIN_PROVIDER)
                .is_none()
        );
    }

    #[test]
    fn only_primary_turn_completion_requests_a_chime() {
        let completed = BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        };
        assert!(super::should_chime_for_backend_event(
            &super::BackendSource::Primary(CODEX_PROVIDER.to_owned()),
            &completed,
        ));
        assert!(!super::should_chime_for_backend_event(
            &super::BackendSource::Subagent("agent-1".to_owned()),
            &completed,
        ));
        assert!(!super::should_chime_for_backend_event(
            &super::BackendSource::Primary(CODEX_PROVIDER.to_owned()),
            &BackendEvent::TurnStarted {
                turn_id: "turn-1".to_owned(),
            },
        ));
    }

    #[test]
    fn exit_hint_includes_executable_workspace_and_full_session_id() {
        let mut output = Vec::new();
        super::write_resume_hint(
            &mut output,
            std::path::Path::new("/opt/Nakode/nakode"),
            std::path::Path::new("/tmp/user's project"),
            Some("019f7bf1-3a18-7793-b9d6-206a1aa7ac0c"),
        )
        .expect("write resume hint");

        let output = String::from_utf8(output).expect("hint is UTF-8");
        assert!(output.contains("Resume this session with:"));
        assert!(output.contains("--workspace"));
        assert!(output.contains("--resume 019f7bf1-3a18-7793-b9d6-206a1aa7ac0c"));
        assert!(output.contains("Nakode/nakode"));
        assert!(output.contains("user"));
    }

    #[test]
    fn exit_hint_is_omitted_before_a_session_is_persisted() {
        let mut output = Vec::new();
        super::write_resume_hint(
            &mut output,
            std::path::Path::new("nakode"),
            std::path::Path::new("/tmp/project"),
            None,
        )
        .expect("skip resume hint");

        assert!(output.is_empty());
    }

    #[test]
    fn reload_local_configuration_discovers_new_skills_and_agents() {
        let workspace = tempfile::tempdir().expect("workspace");
        let skill_directory = workspace.path().join(".agents/skills/review");
        std::fs::create_dir_all(&skill_directory).expect("skill directory");
        std::fs::write(
            skill_directory.join("SKILL.md"),
            "---\nname: review\ndescription: review carefully\n---\n\nReview every change.\n",
        )
        .expect("skill definition");

        let agent_directory = workspace.path().join(".nakode/agents");
        std::fs::create_dir_all(&agent_directory).expect("agent directory");
        std::fs::write(
            agent_directory.join("reviewer.toml"),
            r#"slug = "reviewer"
description = "Reviews changes"
system_prompt = "Review carefully."
first_message = "Inspect the change."
"#,
        )
        .expect("agent definition");

        let mut state = AppState::new(workspace.path().to_string_lossy(), None, 100);
        state.set_agent_directory(agent_directory);
        let (agent_count, skill_count) =
            super::reload_local_configuration(&mut state).expect("reload configuration");

        assert_eq!(agent_count, 1);
        assert!(skill_count >= 1);
        state.editor.set_text("/skill:rev");
        assert_eq!(
            state
                .selected_command_completion()
                .map(crate::state::PromptCompletion::replacement),
            Some("/skill:review".to_owned())
        );
        state.editor.clear();
        state.open_agent_picker();
        assert_eq!(
            state
                .agent_picker
                .as_ref()
                .and_then(|picker| picker.agents.first())
                .map(|agent| agent.slug.as_str()),
            Some("reviewer")
        );
    }

    #[test]
    fn f1_toggles_help_without_editing_the_draft() {
        let mut state = AppState::new("/tmp/project", None, 100);
        super::handle_key(&mut state, KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE));
        assert!(state.show_help);
        assert!(state.editor.is_blank());

        super::handle_key(&mut state, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!state.show_help);
    }

    #[test]
    fn mouse_drag_selects_rendered_text_for_clipboard_copy() {
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render selection source");

        for kind in [
            MouseEventKind::Down(MouseButton::Left),
            MouseEventKind::Drag(MouseButton::Left),
            MouseEventKind::Up(MouseButton::Left),
        ] {
            let column = if matches!(kind, MouseEventKind::Down(_)) {
                1
            } else {
                6
            };
            super::handle_terminal_event(
                &mut state,
                Event::Mouse(MouseEvent {
                    kind,
                    column,
                    row: 0,
                    modifiers: KeyModifiers::NONE,
                }),
            );
        }

        assert_eq!(state.take_pending_clipboard().as_deref(), Some("NAKODE"));
    }

    #[test]
    fn clicking_a_tool_row_expands_and_collapses_its_output() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        let output = (0..40)
            .map(|index| format!("test case_{index} ... ok"))
            .collect::<Vec<_>>()
            .join("\n");
        state.transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "bash · cargo test",
            output,
            EntryStatus::Complete,
        );
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render collapsed tool output");
        let marker_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("▸ bash cargo test")
            })
            .expect("collapsed tool row");

        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 5,
                row: u16::try_from(marker_row).expect("test row fits"),
                modifiers: KeyModifiers::NONE,
            }),
        );
        assert_eq!(state.status_message, "Expanded tool output.");

        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render expanded tool output");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("click to collapse"));
    }

    #[test]
    fn clicking_the_tool_history_row_shows_all_tool_calls() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        for index in 1..=7 {
            state.transcript.upsert(
                format!("tool-{index}"),
                EntryKind::Tool,
                format!("bash · command {index}"),
                format!("output {index}"),
                EntryStatus::Complete,
            );
        }
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render limited tool history");
        let marker_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("earlier tool calls hidden")
            })
            .expect("tool history toggle row");

        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 5,
                row: u16::try_from(marker_row).expect("test row fits"),
                modifiers: KeyModifiers::NONE,
            }),
        );
        assert_eq!(state.status_message, "Showing all tool calls.");

        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render all tool calls");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("command 1"));
        assert!(rendered.contains("all tool calls shown"));
    }

    #[test]
    fn clicking_a_subagent_row_opens_and_escape_closes_its_chat() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render subagent row");
        let objective_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("Map authentication")
            })
            .expect("inline objective row");

        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 2,
                row: u16::try_from(objective_row).expect("test row fits in terminal"),
                modifiers: KeyModifiers::NONE,
            }),
        );
        assert!(state.subagent_modal.is_some());

        super::handle_key(&mut state, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(state.subagent_modal.is_none());
    }

    #[test]
    fn clicking_an_oauth_link_requests_the_system_browser() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.set_oauth_link_hit_region(Some((
            "https://app.example.test/auth".to_owned(),
            crate::selection::ScreenPoint::new(10, 5),
            crate::selection::ScreenPoint::new(70, 6),
        )));

        let effects = super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 20,
                row: 5,
                modifiers: KeyModifiers::NONE,
            }),
        );

        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::OpenUrl(url)] if url == "https://app.example.test/auth"
        ));
    }

    #[test]
    fn provider_authentication_hotkeys_open_and_copy_the_full_url() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();
        state
            .provider_picker
            .as_mut()
            .expect("provider picker")
            .authentication = Some(crate::state::ProviderAuthentication::Challenge {
            verification_url: "https://app.example.test/full/oauth/url".to_owned(),
            user_code: String::new(),
        });

        let open = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Char('o'), KeyModifiers::NONE),
        );
        assert!(matches!(
            open.as_slice(),
            [crate::state::Effect::OpenUrl(url)]
                if url == "https://app.example.test/full/oauth/url"
        ));

        let copy = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE),
        );
        assert!(copy.is_empty());
        assert_eq!(
            state.take_pending_clipboard().as_deref(),
            Some("https://app.example.test/full/oauth/url")
        );
    }

    #[test]
    fn mouse_wheel_scrolls_whichever_chat_is_open() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        state.subagent_modal = Some(state.subagents[0].id.clone());
        let (transcript, _) = state
            .selected_subagent_transcript_mut()
            .expect("selected subagent transcript");
        transcript.push(
            EntryKind::Assistant,
            "ASSISTANT",
            (0..80)
                .map(|line| format!("report line {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
            EntryStatus::Complete,
        );
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render long subagent chat");

        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 50,
                row: 12,
                modifiers: KeyModifiers::NONE,
            }),
        );
        terminal
            .draw(|frame| render::draw(frame, &mut state))
            .expect("render scrolled subagent chat");
        let (_, child_scroll) = state
            .selected_subagent_transcript_mut()
            .expect("selected subagent transcript");
        assert_eq!(*child_scroll, 3);
        assert_eq!(state.scroll_from_bottom, 0);

        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 50,
                row: 12,
                modifiers: KeyModifiers::NONE,
            }),
        );
        let (_, child_scroll) = state
            .selected_subagent_transcript_mut()
            .expect("selected subagent transcript");
        assert_eq!(*child_scroll, 0);

        state.close_subagent_modal();
        super::handle_terminal_event(
            &mut state,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 50,
                row: 12,
                modifiers: KeyModifiers::NONE,
            }),
        );
        assert_eq!(state.scroll_from_bottom, 3);
    }

    #[test]
    fn shift_enter_inserts_a_newline() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("first");
        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT),
        );
        state.editor.insert_str("second");

        assert_eq!(state.editor.text(), "first\nsecond");
    }

    #[test]
    fn enter_queues_during_an_active_turn() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.connection = ConnectionState::Ready {
            server: "test".to_owned(),
        };
        state.active_turn = Some(ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("later");

        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );

        assert!(state.editor.is_blank());
        assert_eq!(
            state.queue.front().map(|prompt| prompt.text.as_str()),
            Some("later")
        );
    }

    #[test]
    fn alt_enter_steers_during_an_active_turn() {
        let mut state =
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
        state.connection = ConnectionState::Ready {
            server: "test".to_owned(),
        };
        state.backend_capabilities.steering = CapabilitySupport::Supported;
        state.provider_session_id = Some("session-1".to_owned());
        state.active_turn = Some(ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("focus here");

        let effects =
            super::handle_key(&mut state, KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT));

        assert_eq!(state.editor.text(), "focus here");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(crate::backend::BackendCommand::SteerTurn {
                session_id,
                turn_id,
                prompt,
                ..
            })] if session_id == "session-1" && turn_id == "turn-1" && prompt == "focus here"
        ));
    }

    #[test]
    fn modified_arrows_navigate_words_and_boundaries() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("one two");
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
        state.editor.insert_char('|');
        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Left, KeyModifiers::CONTROL),
        );
        state.editor.insert_char('^');
        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Right, KeyModifiers::SUPER),
        );
        state.editor.insert_char('$');

        assert_eq!(state.editor.text(), "^one |two$");
    }

    #[test]
    fn modified_backspace_deletes_by_word_and_line() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("one two");
        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::ALT),
        );
        assert_eq!(state.editor.text(), "one ");

        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::SUPER),
        );
        assert!(state.editor.is_blank());
    }

    #[test]
    fn control_question_mark_toggles_help() {
        let mut state = AppState::new("/tmp/project", None, 100);
        let help_key = KeyEvent::new(KeyCode::Char('?'), KeyModifiers::CONTROL);

        super::handle_key(&mut state, help_key);
        assert!(state.show_help);
        super::handle_key(&mut state, help_key);
        assert!(!state.show_help);
    }

    #[test]
    fn tab_completes_commands_only_where_their_placement_allows() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/pro");
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(state.editor.text(), "/providers");

        state.editor.set_text("please /pro");
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(state.editor.text(), "please /pro\t");

        state.editor.set_text("please(/sk");
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(state.editor.text(), "please(/skill:");
    }

    #[test]
    fn cursor_provider_accepts_pasted_api_key_and_submits_it() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![ProviderRecord {
            provider: CURSOR_PROVIDER.to_owned(),
            display_name: "Cursor".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        let open = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Char('o'), KeyModifiers::NONE),
        );
        assert!(matches!(
            open.as_slice(),
            [Effect::OpenUrl(url)] if url == "https://cursor.com/dashboard/api"
        ));
        let _ = super::handle_terminal_event(
            &mut state,
            Event::Paste("ignored-before-focus".to_owned()),
        );
        assert!(!state.provider_api_key_input_active());
        let _ = super::handle_key(&mut state, KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert!(state.provider_api_key_input_active());
        let _ =
            super::handle_terminal_event(&mut state, Event::Paste("cursor-pasted-key".to_owned()));
        let effects = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SaveProviderCredential { provider, metadata, .. }]
                if provider == CURSOR_PROVIDER
                    && metadata == &serde_json::json!({"api_key":"cursor-pasted-key"})
        ));
    }

    #[test]
    fn provider_menu_enters_details_and_escape_returns_to_the_list() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: Some(crate::credential::CredentialMetadata {
                provider: CODEX_PROVIDER.to_owned(),
                kind: "chatgpt_device_code".to_owned(),
                updated_at: 1,
            }),
        }]);

        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert!(
            state
                .provider_picker
                .as_ref()
                .is_some_and(|picker| picker.showing_details)
        );
        let effects = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
        );
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::SetProviderEnabled { enabled: false, .. }]
        ));
        let logout = super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE),
        );
        assert!(matches!(
            logout.as_slice(),
            [crate::state::Effect::ClearProviderCredential(provider)]
                if provider == CODEX_PROVIDER
        ));

        super::handle_key(&mut state, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(state.provider_picker.is_some());
        assert!(
            !state
                .provider_picker
                .as_ref()
                .is_some_and(|picker| picker.showing_details)
        );
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(state.provider_picker.is_none());
    }
}
