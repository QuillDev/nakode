use std::{collections::HashMap, io, time::Duration};

use crossterm::event::{
    Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind,
};
use futures_util::StreamExt;
use thiserror::Error;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    backend::{BackendCommand, BackendError, BackendEvent, BackendHandle},
    clipboard, codex,
    config::Config,
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
}

struct BackendRegistry {
    commands: HashMap<String, mpsc::Sender<BackendCommand>>,
    events: mpsc::Receiver<(String, BackendEvent)>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    failures: Vec<String>,
}

impl BackendRegistry {
    async fn spawn(config: &Config, providers: &[ProviderRecord]) -> Self {
        let (event_tx, events) = mpsc::channel(512);
        let mut registry = Self {
            commands: HashMap::new(),
            events,
            tasks: Vec::new(),
            failures: Vec::new(),
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
                Ok(handle) => registry.insert(provider.provider.clone(), handle, event_tx.clone()),
                Err(error) => registry.failures.push(error.to_string()),
            }
        }
        drop(event_tx);
        registry
    }

    fn insert(
        &mut self,
        provider: String,
        handle: BackendHandle,
        event_tx: mpsc::Sender<(String, BackendEvent)>,
    ) {
        let (commands, mut events, task) = handle.into_parts();
        self.commands.insert(provider.clone(), commands);
        self.tasks.push(task);
        self.tasks.push(tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_tx.send((provider.clone(), event)).await.is_err() {
                    break;
                }
            }
        }));
    }

    async fn send(&self, provider: &str, command: BackendCommand) -> bool {
        let Some(commands) = self.commands.get(provider) else {
            return false;
        };
        commands.send(command).await.is_ok()
    }

    async fn shutdown(self) {
        for commands in self.commands.values() {
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
    let mut signals = ShutdownSignals::install()?;
    let sessions = SqliteSessionRepository::open_default()?;
    let providers = sessions.list_providers()?;
    let mut backends = BackendRegistry::spawn(&config, &providers).await;
    let active_provider = if backends
        .commands
        .contains_key(crate::backend::CODEX_PROVIDER)
    {
        crate::backend::CODEX_PROVIDER
    } else {
        backends
            .commands
            .keys()
            .next()
            .map(String::as_str)
            .ok_or_else(|| AppError::NoProviders(backends.failures.join("; ")))?
    }
    .to_owned();
    let mut terminal = match TerminalSession::enter() {
        Ok(terminal) => terminal,
        Err(error) => {
            backends.shutdown().await;
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
        &mut signals,
    )
    .await;

    let restore_result = terminal.restore();
    backends.shutdown().await;

    loop_result?;
    restore_result.map_err(AppError::Terminal)
}

async fn run_loop(
    terminal: &mut Tui,
    state: &mut AppState,
    backends: &mut BackendRegistry,
    sessions: &dyn SessionRepository,
    signals: &mut ShutdownSignals,
) -> io::Result<()> {
    let mut input = EventStream::new();
    let mut render_tick = tokio::time::interval(Duration::from_millis(33));
    render_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut backend_open = true;
    let mut dirty = true;

    loop {
        tokio::select! {
            input_event = input.next() => {
                match input_event {
                    Some(Ok(event)) => {
                        let effects = handle_terminal_event(state, event);
                        flush_pending_clipboard(terminal, state);
                        if apply_effects(state, effects, backends, sessions).await {
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
                if let Some((provider, event)) = backend_event {
                    let effects = state.handle_provider_backend(&provider, event);
                    if apply_effects(state, effects, backends, sessions).await {
                        break;
                    }
                    dirty = true;
                } else {
                    backend_open = false;
                    state.set_status("All provider event channels closed.");
                    dirty = true;
                }
            }
            () = signals.recv() => {
                state.should_quit = true;
                break;
            }
            _ = render_tick.tick() => {
                if dirty {
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
    backends: &BackendRegistry,
    sessions: &dyn SessionRepository,
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
                MouseEventKind::Down(MouseButton::Left) => state.begin_text_selection(point),
                MouseEventKind::Drag(MouseButton::Left) => state.update_text_selection(point),
                MouseEventKind::Up(MouseButton::Left) => state.finish_text_selection(point),
                MouseEventKind::ScrollUp => {
                    state.clear_text_selection();
                    state.scroll_from_bottom = state.scroll_from_bottom.saturating_add(3);
                }
                MouseEventKind::ScrollDown => {
                    state.clear_text_selection();
                    state.scroll_from_bottom = state.scroll_from_bottom.saturating_sub(3);
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

    match key.code {
        KeyCode::Char('c') if control => state.cancel_or_quit(),
        KeyCode::Char('d') if control => state.request_quit(),
        KeyCode::Char('q') if control => state.enqueue_editor(),
        KeyCode::Char('s') if control => state.steer_editor(),
        KeyCode::Char('l') if control => {
            state.scroll_from_bottom = 0;
            state.set_status("Jumped to the latest output.");
            Vec::new()
        }
        KeyCode::Char('j') if control => {
            state.editor.insert_newline();
            Vec::new()
        }
        KeyCode::F(2) => state.open_model_picker(),
        KeyCode::PageUp => {
            state.scroll_from_bottom = state.scroll_from_bottom.saturating_add(10);
            Vec::new()
        }
        KeyCode::PageDown => {
            state.scroll_from_bottom = state.scroll_from_bottom.saturating_sub(10);
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
        KeyCode::Enter if alt || shift => {
            state.editor.insert_newline();
            Vec::new()
        }
        KeyCode::Enter => state.submit_editor(),
        KeyCode::Backspace => {
            state.editor.backspace();
            Vec::new()
        }
        KeyCode::Delete => {
            state.editor.delete();
            Vec::new()
        }
        KeyCode::Left => {
            state.editor.move_left();
            Vec::new()
        }
        KeyCode::Right => {
            state.editor.move_right();
            Vec::new()
        }
        KeyCode::Up => {
            state.editor.move_up();
            Vec::new()
        }
        KeyCode::Down => {
            state.editor.move_down();
            Vec::new()
        }
        KeyCode::Home => {
            state.editor.move_home();
            Vec::new()
        }
        KeyCode::End => {
            state.editor.move_end();
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

fn handle_modal_key(state: &mut AppState, key: KeyEvent) -> Option<Vec<Effect>> {
    if !state.approvals.is_empty() {
        return Some(match key.code {
            KeyCode::Char('y') => state.resolve_approval(ApprovalDecision::AcceptOnce),
            KeyCode::Char('a') => state.resolve_approval(ApprovalDecision::AcceptForSession),
            KeyCode::Char('n') | KeyCode::Esc => state.resolve_approval(ApprovalDecision::Decline),
            _ => Vec::new(),
        });
    }

    if state.show_help {
        if matches!(key.code, KeyCode::Esc | KeyCode::F(1)) {
            state.show_help = false;
        }
        return Some(Vec::new());
    }

    if key.code == KeyCode::F(1) {
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
        return Some(match key.code {
            KeyCode::Esc => {
                state.close_provider_picker();
                Vec::new()
            }
            KeyCode::Enter | KeyCode::Char(' ') => state.toggle_provider(),
            KeyCode::Up => {
                state.provider_picker_move(-1);
                Vec::new()
            }
            KeyCode::Down => {
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

    use crate::{render, state::AppState};

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

        assert_eq!(state.take_pending_clipboard().as_deref(), Some("FLOCK"));
    }

    #[test]
    fn alt_enter_inserts_a_newline() {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("first");
        super::handle_key(&mut state, KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT));
        state.editor.insert_str("second");

        assert_eq!(state.editor.text(), "first\nsecond");
    }
}
