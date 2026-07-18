use std::{collections::HashMap, io, time::Duration};

use crossterm::event::{
    Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind,
};
use futures_util::StreamExt;
use thiserror::Error;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    agent::{AgentCatalog, AgentCatalogError},
    backend::{BackendCommand, BackendError, BackendEvent, BackendHandle},
    clipboard, codex,
    config::Config,
    control::{AgentResponse, ControlError, ControlServer, IncomingInvocation},
    devin, render,
    selection::ScreenPoint,
    session::{ProviderRecord, SessionError, SessionRepository, SqliteSessionRepository},
    state::{AppState, ApprovalDecision, Effect},
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
    #[error("no enabled provider could be started: {0}")]
    NoProviders(String),
    #[error(transparent)]
    Agents(#[from] AgentCatalogError),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error("failed to locate the running Nako Agent executable: {0}")]
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
    events: mpsc::Receiver<(BackendSource, BackendEvent)>,
    event_tx: mpsc::Sender<(BackendSource, BackendEvent)>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    failures: Vec<String>,
    config: Config,
}

impl BackendRegistry {
    async fn spawn(config: &Config, providers: &[ProviderRecord]) -> Self {
        let (event_tx, events) = mpsc::channel(512);
        let mut registry = Self {
            commands: HashMap::new(),
            subagent_commands: HashMap::new(),
            events,
            event_tx: event_tx.clone(),
            tasks: Vec::new(),
            failures: Vec::new(),
            config: config.clone(),
        };
        for provider in providers.iter().filter(|provider| provider.enabled) {
            let result = match provider.provider.as_str() {
                crate::backend::CODEX_PROVIDER => {
                    codex::spawn(codex::BackendConfig::codex(
                        config.codex.clone(),
                        config.workspace.clone(),
                    ))
                    .await
                }
                crate::backend::DEVIN_PROVIDER => {
                    devin::spawn(devin::BackendConfig::devin(
                        config.devin.clone(),
                        config.workspace.clone(),
                    ))
                    .await
                }
                unknown => {
                    registry
                        .failures
                        .push(format!("unknown enabled provider {unknown}"));
                    continue;
                }
            };
            match result {
                Ok(handle) => registry.insert_primary(provider.provider.clone(), handle),
                Err(error) => registry.failures.push(error.to_string()),
            }
        }
        drop(event_tx);
        registry
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
        if !self.commands.contains_key(provider) {
            return Err(BackendError::ProviderUnavailable {
                provider: provider.to_owned(),
            });
        }
        let handle = match provider {
            crate::backend::CODEX_PROVIDER => {
                codex::spawn(codex::BackendConfig::codex(
                    self.config.codex.clone(),
                    self.config.workspace.clone(),
                ))
                .await?
            }
            crate::backend::DEVIN_PROVIDER => {
                devin::spawn(devin::BackendConfig::devin(
                    self.config.devin.clone(),
                    self.config.workspace.clone(),
                ))
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
        if let Some(commands) = self.subagent_commands.remove(run_id) {
            let _ = commands.send(BackendCommand::Shutdown).await;
        }
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
    let nako_executable = std::env::current_exe().map_err(AppError::CurrentExecutable)?;
    let mut signals = ShutdownSignals::install()?;
    let sessions = SqliteSessionRepository::open_default()?;
    let providers = sessions.list_providers()?;
    let agents = AgentCatalog::load(&config.agents)?;
    let control_path = crate::control::socket_path(&config.workspace);
    if let Some(parent) = control_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ControlError::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    let mut backends = BackendRegistry::spawn(&config, &providers).await;
    let active_provider = if backends
        .commands
        .contains_key(crate::backend::CODEX_PROVIDER)
    {
        crate::backend::CODEX_PROVIDER.to_owned()
    } else if let Some(provider) = backends.commands.keys().next() {
        provider.clone()
    } else {
        let failures = backends.failures.join("; ");
        backends.shutdown().await;
        return Err(AppError::NoProviders(failures));
    };
    let mut control = match ControlServer::bind(&control_path).await {
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
            control.shutdown(&control_path);
            return Err(AppError::Terminal(error));
        }
    };
    let active_name = providers
        .iter()
        .find(|p| p.provider == active_provider)
        .map_or(active_provider.as_str(), |p| p.display_name.as_str());
    let mut state = AppState::new_for_backend(
        config.workspace.to_string_lossy(),
        config.model,
        config.scrollback,
        &active_provider,
        active_name,
    );
    state.install_agents(agents);
    state.set_nako_executable(&nako_executable);
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

    let loop_result = run_loop(
        terminal.terminal_mut(),
        &mut state,
        &mut backends,
        &sessions,
        &mut control,
        &mut signals,
    )
    .await;

    let restore_result = terminal.restore();
    backends.shutdown().await;
    control.shutdown(&control_path);

    loop_result?;
    restore_result.map_err(AppError::Terminal)
}

async fn run_loop(
    terminal: &mut Tui,
    state: &mut AppState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    control: &mut ControlServer,
    signals: &mut ShutdownSignals,
) -> io::Result<()> {
    let mut input = EventStream::new();
    let mut render_tick = tokio::time::interval(Duration::from_millis(33));
    render_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut backend_open = true;
    let mut dirty = true;
    let mut agent_requests = HashMap::<u64, IncomingInvocation>::new();

    loop {
        tokio::select! {
            input_event = input.next() => {
                match input_event {
                    Some(Ok(event)) => {
                        let effects = handle_terminal_event(state, event);
                        flush_pending_clipboard(terminal, state);
                        if apply_effects(state, effects, backends, sessions, &mut agent_requests).await {
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
                    let effects = match source {
                        BackendSource::Primary(provider) => state.handle_provider_backend(&provider, event),
                        BackendSource::Subagent(run_id) => state.handle_subagent_backend(&run_id, event),
                    };
                    if apply_effects(state, effects, backends, sessions, &mut agent_requests).await {
                        break;
                    }
                    dirty = true;
                } else {
                    backend_open = false;
                    state.set_status("All provider event channels closed.");
                    dirty = true;
                }
            }
            request = control.requests.recv() => {
                if let Some(request) = request {
                    if request.invocation.session_id == state.nako_session_id {
                        let id = request.id;
                        let invocation = crate::state::AgentRequest { id, agent: request.invocation.agent.clone(), task: request.invocation.task.clone() };
                        agent_requests.insert(id, request);
                        let effects = state.invoke_agent(&invocation);
                        if apply_effects(state, effects, backends, sessions, &mut agent_requests).await { break; }
                    } else {
                        request.respond(AgentResponse { success: false, result: "Nako Agent session id does not match this TUI.".to_owned() });
                    }
                    dirty = true;
                }
            }
            () = signals.recv() => {
                state.should_quit = true;
                break;
            }
            _ = render_tick.tick() => {
                if dirty || state.has_running_subagents() {
                    terminal.draw(|frame| render::draw(frame, state))?;
                    dirty = false;
                }
            }
        }

        if state.should_quit {
            break;
        }
    }

    Ok(())
}

async fn apply_effects(
    state: &mut AppState,
    effects: Vec<Effect>,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    agent_requests: &mut HashMap<u64, IncomingInvocation>,
) -> bool {
    let mut quit = false;
    let mut pending = std::collections::VecDeque::from(effects);
    while let Some(effect) = pending.pop_front() {
        match effect {
            Effect::Backend(command) => {
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
            Effect::SpawnSubagent { run_id, provider } => {
                if let Err(error) = backends.spawn_subagent(run_id.clone(), &provider).await {
                    pending.extend(state.subagent_launch_failed(&run_id, error.to_string()));
                }
            }
            Effect::SubagentBackend { run_id, command } => {
                if !backends.send_subagent(&run_id, command).await {
                    pending.extend(state.subagent_launch_failed(
                        &run_id,
                        "subagent command channel closed".to_owned(),
                    ));
                }
            }
            Effect::StopSubagent(run_id) => backends.stop_subagent(&run_id).await,
            Effect::CompleteAgentRequest {
                request_id,
                result,
                success,
            } => {
                if let Some(request) = agent_requests.remove(&request_id) {
                    request.respond(AgentResponse { success, result });
                }
            }
            Effect::ListSessions => match sessions.list_recent(&state.workspace, 100) {
                Ok(records) => state.install_sessions(records),
                Err(error) => state.session_store_failed(error.to_string()),
            },
            Effect::ListProviders => match sessions.list_providers() {
                Ok(providers) => state.install_providers(providers),
                Err(error) => state.session_store_failed(error.to_string()),
            },
            Effect::SetProviderEnabled { provider, enabled } => {
                if let Err(error) = sessions.set_provider_enabled(&provider, enabled) {
                    state.session_store_failed(error.to_string());
                }
            }
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
            } => match sessions.create(
                &provider,
                &provider_session_id,
                &workspace,
                &title,
                model.as_deref(),
            ) {
                Ok(record) => state.session_persisted(&record),
                Err(error) => state.session_store_failed(error.to_string()),
            },
            Effect::PersistModels { provider, models } => {
                if let Err(error) = sessions.replace_models(&provider, &models) {
                    state.session_store_failed(error.to_string());
                }
            }
            Effect::PersistSubagent(record) => persist_subagent(state, sessions, &record),
            Effect::LoadSubagents(parent_session_id) => {
                load_subagents(state, sessions, &parent_session_id);
            }
            Effect::UpdateSessionModel { session_id, model } => {
                if let Err(error) = sessions.update_model(&session_id, model.as_deref()) {
                    state.session_store_failed(error.to_string());
                }
            }
            Effect::TouchSession(id) => {
                if let Err(error) = sessions.touch(&id) {
                    state.session_store_failed(error.to_string());
                }
            }
            Effect::Quit => quit = true,
        }
    }
    quit
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
            if state.model_picker.is_none()
                && state.session_picker.is_none()
                && state.provider_picker.is_none()
                && !state.show_help
                && state.approvals.is_empty()
            {
                state.editor.insert_str(&text);
                state.set_status("Pasted text into the draft.");
            }
            Vec::new()
        }
        Event::Mouse(mouse) => {
            let point = ScreenPoint::new(mouse.column, mouse.row);
            match mouse.kind {
                MouseEventKind::Down(MouseButton::Left)
                    if state.subagent_modal.is_some() || !state.open_subagent_at(point) =>
                {
                    state.begin_text_selection(point);
                }
                MouseEventKind::Down(MouseButton::Left) => {}
                MouseEventKind::Drag(MouseButton::Left) => state.update_text_selection(point),
                MouseEventKind::Up(MouseButton::Left) => state.finish_text_selection(point),
                MouseEventKind::ScrollUp => {
                    state.clear_text_selection();
                    state.scroll_active_chat(3);
                }
                MouseEventKind::ScrollDown => {
                    state.clear_text_selection();
                    state.scroll_active_chat(-3);
                }
                MouseEventKind::Down(_) => state.clear_text_selection(),
                _ => {}
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

    let control = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);
    let shift = key.modifiers.contains(KeyModifiers::SHIFT);
    let boundary = control || key.modifiers.contains(KeyModifiers::SUPER);
    if let Some(effects) = handle_command_completion_key(state, key, boundary, alt, shift) {
        return effects;
    }
    if handle_editor_navigation(state, key.code, boundary, alt) {
        return Vec::new();
    }

    match key.code {
        KeyCode::Char('c') if control => state.cancel_or_quit(),
        KeyCode::Char('d') if control => state.request_quit(),
        KeyCode::Char('q') if control => state.enqueue_editor(),
        KeyCode::Char('s') if control => state.steer_editor(),
        KeyCode::Char('l') if control => {
            state.reset_active_chat_scroll();
            state.set_status("Jumped to the latest output.");
            Vec::new()
        }
        KeyCode::Char('j') if control => {
            state.editor.insert_newline();
            Vec::new()
        }
        KeyCode::F(2) => state.open_model_picker(),
        KeyCode::PageUp => {
            state.scroll_active_chat(10);
            Vec::new()
        }
        KeyCode::PageDown => {
            state.scroll_active_chat(-10);
            Vec::new()
        }
        KeyCode::Up if alt => {
            state.move_queue_selection(-1);
            Vec::new()
        }
        KeyCode::Down if alt => {
            state.move_queue_selection(1);
            Vec::new()
        }
        KeyCode::Delete if alt => {
            state.remove_selected_queue_item();
            Vec::new()
        }
        KeyCode::Enter if shift => {
            state.editor.insert_newline();
            Vec::new()
        }
        KeyCode::Enter if alt => state.enqueue_editor(),
        KeyCode::Enter if !boundary => state.submit_or_steer_editor(),
        KeyCode::Backspace if alt => {
            state.editor.delete_word_backward();
            Vec::new()
        }
        KeyCode::Backspace if boundary => {
            state.editor.delete_to_line_start();
            Vec::new()
        }
        KeyCode::Backspace => {
            state.editor.backspace();
            Vec::new()
        }
        KeyCode::Delete => {
            state.editor.delete();
            Vec::new()
        }
        KeyCode::Tab => {
            state.editor.insert_char('\t');
            Vec::new()
        }
        KeyCode::Char(character)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER) =>
        {
            state.editor.insert_char(character);
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn handle_editor_navigation(
    state: &mut AppState,
    key_code: KeyCode,
    boundary: bool,
    alt: bool,
) -> bool {
    match key_code {
        KeyCode::Left if alt => state.editor.move_word_left(),
        KeyCode::Right if alt => state.editor.move_word_right(),
        KeyCode::Left if boundary => state.editor.move_home(),
        KeyCode::Right if boundary => state.editor.move_end(),
        KeyCode::Up if boundary => state.editor.move_document_start(),
        KeyCode::Down if boundary => state.editor.move_document_end(),
        KeyCode::Left => state.editor.move_left(),
        KeyCode::Right => state.editor.move_right(),
        KeyCode::Up if !alt => state.editor.move_up(),
        KeyCode::Down if !alt => state.editor.move_down(),
        KeyCode::Home => state.editor.move_home(),
        KeyCode::End => state.editor.move_end(),
        _ => return false,
    }
    true
}

fn handle_command_completion_key(
    state: &mut AppState,
    key: KeyEvent,
    boundary: bool,
    alt: bool,
    shift: bool,
) -> Option<Vec<Effect>> {
    if state.command_completions().is_empty() {
        return None;
    }

    match key.code {
        KeyCode::Up if !boundary && !alt => state.move_command_completion(-1),
        KeyCode::Down if !boundary && !alt => state.move_command_completion(1),
        KeyCode::Tab if !boundary && !alt => state.accept_command_completion(),
        KeyCode::Enter if !boundary && !alt && !shift && !state.command_completion_is_exact() => {
            state.accept_command_completion();
        }
        _ => return None,
    }
    Some(Vec::new())
}

fn handle_modal_key(state: &mut AppState, key: KeyEvent) -> Option<Vec<Effect>> {
    if !state.approvals.is_empty() {
        return Some(match key.code {
            KeyCode::Char('y') => state.resolve_approval(ApprovalDecision::AcceptOnce),
            KeyCode::Char('a') => state.resolve_approval(ApprovalDecision::AcceptForSession),
            KeyCode::Char('n') | KeyCode::Esc => state.resolve_approval(ApprovalDecision::Decline),
            _ => Vec::new(),
        });
    }

    if state.subagent_modal.is_some() {
        return Some(handle_subagent_modal_key(state, key));
    }

    if state.show_help {
        if key.code == KeyCode::Esc || is_help_key(key) {
            state.show_help = false;
        }
        return Some(Vec::new());
    }

    if is_help_key(key) {
        state.model_picker = None;
        state.session_picker = None;
        state.show_help = true;
        return Some(Vec::new());
    }

    if state.session_picker.is_some() {
        return Some(match key.code {
            KeyCode::Esc => {
                state.close_session_picker();
                Vec::new()
            }
            KeyCode::Enter => state.select_session(),
            KeyCode::Up => {
                state.session_picker_move(-1);
                Vec::new()
            }
            KeyCode::Down => {
                state.session_picker_move(1);
                Vec::new()
            }
            _ => Vec::new(),
        });
    }

    if state.provider_picker.is_some() {
        let showing_details = state
            .provider_picker
            .as_ref()
            .is_some_and(|picker| picker.showing_details);
        return Some(match key.code {
            KeyCode::Esc => {
                if !state.close_provider_details() {
                    state.close_provider_picker();
                }
                Vec::new()
            }
            KeyCode::Enter | KeyCode::Char(' ') if showing_details => state.toggle_provider(),
            KeyCode::Enter => {
                state.open_provider_details();
                Vec::new()
            }
            KeyCode::Up if !showing_details => {
                state.provider_picker_move(-1);
                Vec::new()
            }
            KeyCode::Down if !showing_details => {
                state.provider_picker_move(1);
                Vec::new()
            }
            _ => Vec::new(),
        });
    }

    if state.model_picker.is_some() {
        if key.code == KeyCode::Enter {
            return Some(state.picker_select());
        }
        match key.code {
            KeyCode::Esc => state.close_model_picker(),
            KeyCode::Up => state.picker_move(-1),
            KeyCode::Down => state.picker_move(1),
            KeyCode::Backspace => state.picker_backspace(),
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(picker) = &mut state.model_picker {
                    picker.filter.clear();
                    picker.selected = 0;
                }
            }
            KeyCode::Char(character)
                if !key.modifiers.intersects(
                    KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER,
                ) =>
            {
                state.picker_insert(character);
            }
            _ => {}
        }
        return Some(Vec::new());
    }
    None
}

fn handle_subagent_modal_key(state: &mut AppState, key: KeyEvent) -> Vec<Effect> {
    let control = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char('c') if control => return state.cancel_or_quit(),
        KeyCode::Char('l') if control => state.reset_active_chat_scroll(),
        KeyCode::PageUp => state.scroll_active_chat(10),
        KeyCode::PageDown => state.scroll_active_chat(-10),
        KeyCode::Esc => state.close_subagent_modal(),
        _ => {}
    }
    Vec::new()
}

fn is_help_key(key: KeyEvent) -> bool {
    key.code == KeyCode::F(1)
        || (key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('?' | '/' | '_')))
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
    use crossterm::event::{
        Event, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };
    use ratatui::{Terminal, backend::TestBackend};

    use crate::{
        backend::{CODEX_PROVIDER, CapabilitySupport},
        render,
        session::ProviderRecord,
        state::{ActiveTurn, AgentRequest, AppState},
        transcript::{EntryKind, EntryStatus},
    };

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
                5
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

        assert_eq!(state.take_pending_clipboard().as_deref(), Some("NAKO"));
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
    fn alt_enter_queues_during_an_active_turn() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.active_turn = Some(ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("later");

        super::handle_key(&mut state, KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT));

        assert!(state.editor.is_blank());
        assert_eq!(
            state.queue.front().map(|prompt| prompt.text.as_str()),
            Some("later")
        );
    }

    #[test]
    fn enter_steers_during_an_active_turn() {
        let mut state =
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
        state.backend_capabilities.steering = CapabilitySupport::Supported;
        state.active_turn = Some(ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("focus here");

        super::handle_key(
            &mut state,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );

        assert_eq!(state.editor.text(), "focus here");
        assert_eq!(
            state.status_message,
            "The active provider session is unavailable."
        );
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
    fn provider_menu_enters_details_and_escape_returns_to_the_list() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
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
