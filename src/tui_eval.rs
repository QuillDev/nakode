//! Deterministic, headless evaluation of Nakode's real TUI reducer and renderer.
//!
//! The JSON Lines protocol is intentionally small enough to drive from a shell
//! while retaining the event semantics used by the interactive application.

use std::{
    error::Error,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::{Terminal, backend::TestBackend, style::Style};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    render,
    tui_input::{self, DeviceIntent},
    tui_state::{SettingsView, TuiState},
};

/// Command-line options for the headless TUI evaluator.
#[derive(Clone, Debug)]
pub struct Options {
    pub workspace: PathBuf,
    pub scenario: Option<PathBuf>,
    pub width: u16,
    pub height: u16,
}

/// Runs JSON Lines actions from a scenario file or standard input.
///
/// # Errors
///
/// Returns an error for malformed actions, failed assertions, invalid input,
/// or a terminal rendering failure.
pub fn run(options: &Options) -> Result<(), Box<dyn Error>> {
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    if let Some(path) = &options.scenario {
        let reader = BufReader::new(File::open(path)?);
        run_script(reader, &mut writer, options)?;
    } else {
        let stdin = io::stdin();
        run_script(stdin.lock(), &mut writer, options)?;
    }
    writer.flush()?;
    Ok(())
}

fn run_script(
    reader: impl BufRead,
    writer: &mut impl Write,
    options: &Options,
) -> Result<(), Box<dyn Error>> {
    let mut harness = Harness::new(options)?;
    let mut step = 0;
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line?;
        let source = line.trim();
        if source.is_empty() || source.starts_with('#') {
            continue;
        }
        let action =
            serde_json::from_str::<Action>(source).map_err(|source| ProtocolError::Json {
                line: line_number,
                source,
            })?;
        step += 1;
        let action_name = action.name();
        let styles = action.includes_styles();
        let expected = action.assertion().cloned();
        harness
            .apply(action)
            .map_err(|message| ProtocolError::Action {
                line: line_number,
                message,
            })?;
        let observation = harness.observe(step, action_name, styles);
        serde_json::to_writer(&mut *writer, &observation)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        if let Some(assertion) = expected {
            assertion
                .check(&observation)
                .map_err(|message| ProtocolError::Assertion {
                    line: line_number,
                    message,
                })?;
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
enum ProtocolError {
    #[error("TUI evaluation JSON is invalid on line {line}: {source}")]
    Json {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("TUI evaluation action failed on line {line}: {message}")]
    Action { line: usize, message: String },
    #[error("TUI evaluation assertion failed on line {line}: {message}")]
    Assertion { line: usize, message: String },
}

struct Harness {
    terminal: Terminal<TestBackend>,
    state: TuiState,
    view: nakode_protocol::BootstrapView,
    commands: Vec<crate::api_projection::TuiAction>,
    command_history: Vec<crate::api_projection::TuiAction>,
    devices: Vec<Value>,
    should_quit: bool,
}

fn initial_bootstrap(
    workspace: &std::path::Path,
) -> Result<nakode_protocol::BootstrapView, serde_json::Error> {
    let workspace_path = workspace.to_string_lossy();
    serde_json::from_value(json!({
        "workspace_id": "tui-eval-workspace",
        "workspace_path": workspace_path,
        "providers": [],
        "models": [],
        "agents": [],
        "skills": [],
        "settings": {
            "web": {
                "backend": "disabled",
                "credential_configured": false,
                "agent_browser": {"state": "unavailable"}
            },
            "memory": {
                "backend": "disabled",
                "executable": "",
                "global_bank": "",
                "data_directory": "",
                "configured": false,
                "available": false
            },
            "vision": {"model_id": null},
            "terminal_images": "auto"
        },
        "sessions": [{
            "id": "tui-eval-session",
            "workspace_id": "tui-eval-workspace",
            "title": "TUI evaluation",
            "active_provider_id": null,
            "active_model_id": null,
            "updated_at_ms": 0
        }],
        "active_session": {
            "id": "tui-eval-session",
            "revision": 0,
            "workspace_id": "tui-eval-workspace",
            "title": "TUI evaluation",
            "status_message": "",
            "diagnostic_count": 0,
            "activity": "idle",
            "selected_provider_id": null,
            "selected_model_id": null,
            "active_agent_session": null,
            "active_turn": null,
            "context_usage": null,
            "transcript": {
                "entries": [],
                "has_earlier": false,
                "stream_active": false,
                "stream_label": ""
            },
            "queue": [],
            "interactions": [],
            "todos": [],
            "runs": [],
            "notices": []
        }
    }))
}

impl Harness {
    fn new(options: &Options) -> Result<Self, Box<dyn Error>> {
        let terminal = Terminal::new(TestBackend::new(options.width, options.height))?;
        let view = initial_bootstrap(&options.workspace)?;
        let state = TuiState::from_bootstrap(&view, 2_000);
        Ok(Self {
            terminal,
            state,
            view,
            commands: Vec::new(),
            command_history: Vec::new(),
            devices: Vec::new(),
            should_quit: false,
        })
    }

    fn apply(&mut self, action: Action) -> Result<(), String> {
        if !matches!(action, Action::Snapshot { .. } | Action::Assert { .. }) {
            self.commands.clear();
            self.devices.clear();
        }
        match action {
            Action::Snapshot { .. } | Action::Assert { .. } => {}
            Action::Key { key, modifiers } => {
                let event = key_event(&key, &modifiers)?;
                self.apply_input(Event::Key(event));
            }
            Action::Type { text } => {
                for character in text.chars() {
                    self.apply_input(Event::Key(KeyEvent::new(
                        KeyCode::Char(character),
                        KeyModifiers::NONE,
                    )));
                }
            }
            Action::Paste { text } => {
                self.apply_input(Event::Paste(text));
            }
            Action::Mouse {
                kind,
                column,
                row,
                modifiers,
            } => {
                let kind = mouse_event_kind(&kind)?;
                let modifiers = key_modifiers(&modifiers)?;
                self.apply_input(Event::Mouse(MouseEvent {
                    kind,
                    column,
                    row,
                    modifiers,
                }));
            }
            Action::Resize { width, height } => {
                if width < 20 || height < 8 {
                    return Err("terminal dimensions must be at least 20x8".to_owned());
                }
                self.terminal.backend_mut().resize(width, height);
                self.terminal
                    .autoresize()
                    .map_err(|error| error.to_string())?;
                self.apply_input(Event::Resize(width, height));
            }
            Action::Service { event } => {
                event.apply_view(&mut self.view, &self.command_history)?;
                self.state.install_bootstrap(&self.view);
            }
        }
        Ok(())
    }

    fn apply_input(&mut self, event: Event) {
        let outcome = tui_input::handle_terminal(&mut self.state, &self.view, event);
        let commands = outcome
            .commands
            .into_iter()
            .map(|intent| intent.command)
            .collect::<Vec<_>>();
        self.command_history.extend(commands.iter().cloned());
        self.commands.extend(commands);
        self.devices
            .extend(outcome.devices.into_iter().map(|intent| match intent {
                DeviceIntent::OpenUrl(url) => json!({"type": "open_url", "url": url}),
                DeviceIntent::Copy(text) => {
                    json!({"type": "copy", "byte_length": text.len()})
                }
            }));
        self.should_quit |= outcome.quit;
    }

    fn observe(&mut self, step: usize, action: &'static str, styles: bool) -> Observation {
        self.terminal
            .draw(|frame| render::draw(frame, &mut self.state))
            .expect("the in-memory terminal should render");
        let screen = Screen::capture(self.terminal.backend(), styles);
        let commands = self.commands.iter().map(command_view).collect();
        Observation {
            step,
            action,
            screen,
            state: StateView::capture(&self.state, &self.view, self.should_quit),
            commands,
            devices: self.devices.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Action {
    Snapshot {
        #[serde(default)]
        styles: bool,
    },
    Key {
        key: String,
        #[serde(default)]
        modifiers: Vec<String>,
    },
    Type {
        text: String,
    },
    Paste {
        text: String,
    },
    Mouse {
        kind: String,
        column: u16,
        row: u16,
        #[serde(default)]
        modifiers: Vec<String>,
    },
    Resize {
        width: u16,
        height: u16,
    },
    Service {
        event: FixtureEvent,
    },
    Assert {
        #[serde(flatten)]
        assertion: Assertion,
    },
}

impl Action {
    const fn name(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::Key { .. } => "key",
            Self::Type { .. } => "type",
            Self::Paste { .. } => "paste",
            Self::Mouse { .. } => "mouse",
            Self::Resize { .. } => "resize",
            Self::Service { .. } => "service",
            Self::Assert { .. } => "assert",
        }
    }

    const fn includes_styles(&self) -> bool {
        matches!(self, Self::Snapshot { styles: true })
    }

    const fn assertion(&self) -> Option<&Assertion> {
        match self {
            Self::Assert { assertion } => Some(assertion),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct Assertion {
    #[serde(default)]
    screen_contains: Vec<String>,
    #[serde(default)]
    screen_excludes: Vec<String>,
    status: Option<String>,
    status_contains: Option<String>,
    modal: Option<String>,
    draft: Option<String>,
    connection: Option<String>,
    #[serde(default)]
    commands_include: Vec<String>,
    #[serde(default)]
    commands_exclude: Vec<String>,
    cursor_visible: Option<bool>,
    screen_width: Option<u16>,
    screen_height: Option<u16>,
}

impl Assertion {
    fn check(&self, observation: &Observation) -> Result<(), String> {
        self.check_screen(&observation.screen)?;
        self.check_state(&observation.state)?;
        self.check_commands(&observation.commands)
    }

    fn check_commands(&self, commands: &[Value]) -> Result<(), String> {
        let command_names = commands
            .iter()
            .filter_map(|command| command.get("type").and_then(Value::as_str))
            .collect::<Vec<_>>();
        for expected in &self.commands_include {
            if !command_names.contains(&expected.as_str()) {
                return Err(format!(
                    "commands {command_names:?} do not include {expected:?}"
                ));
            }
        }
        for excluded in &self.commands_exclude {
            if command_names.contains(&excluded.as_str()) {
                return Err(format!(
                    "commands {command_names:?} unexpectedly include {excluded:?}"
                ));
            }
        }
        Ok(())
    }

    fn check_screen(&self, screen: &Screen) -> Result<(), String> {
        let text = screen.lines.join("\n");
        for expected in &self.screen_contains {
            if !text.contains(expected) {
                return Err(format!("screen does not contain {expected:?}"));
            }
        }
        for excluded in &self.screen_excludes {
            if text.contains(excluded) {
                return Err(format!("screen unexpectedly contains {excluded:?}"));
            }
        }
        if self
            .cursor_visible
            .is_some_and(|visible| visible != screen.cursor.visible)
        {
            return Err(format!(
                "cursor visibility is {}, expected {:?}",
                screen.cursor.visible, self.cursor_visible
            ));
        }
        if self.screen_width.is_some_and(|width| width != screen.width) {
            return Err(format!(
                "screen width is {}, expected {:?}",
                screen.width, self.screen_width
            ));
        }
        if self
            .screen_height
            .is_some_and(|height| height != screen.height)
        {
            return Err(format!(
                "screen height is {}, expected {:?}",
                screen.height, self.screen_height
            ));
        }
        Ok(())
    }

    fn check_state(&self, state: &StateView) -> Result<(), String> {
        if self
            .status
            .as_ref()
            .is_some_and(|value| value != &state.status)
        {
            return Err(format!(
                "status is {:?}, expected {:?}",
                state.status, self.status
            ));
        }
        if self
            .status_contains
            .as_ref()
            .is_some_and(|value| !state.status.contains(value))
        {
            return Err(format!(
                "status {:?} does not contain {:?}",
                state.status, self.status_contains
            ));
        }
        if self.modal.as_ref() != Some(&state.modal) && self.modal.is_some() {
            return Err(format!(
                "modal is {:?}, expected {:?}",
                state.modal, self.modal
            ));
        }
        if self
            .draft
            .as_ref()
            .is_some_and(|value| value != &state.draft)
        {
            return Err(format!(
                "draft is {:?}, expected {:?}",
                state.draft, self.draft
            ));
        }
        if self
            .connection
            .as_ref()
            .is_some_and(|value| value != &state.connection)
        {
            return Err(format!(
                "connection is {:?}, expected {:?}",
                state.connection, self.connection
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FixtureEvent {
    Ready {
        #[serde(default = "default_display_name")]
        display_name: String,
        #[serde(default, rename = "version")]
        _version: Option<String>,
        #[serde(default)]
        capabilities: Vec<String>,
    },
    Models {
        models: Vec<FixtureModel>,
    },
    SessionCreated {
        provider_session_id: String,
        model: String,
    },
    TurnStarted {
        turn_id: String,
    },
    Item {
        #[serde(rename = "turn_id")]
        _turn_id: String,
        id: String,
        kind: String,
        title: String,
        body: String,
        #[serde(default = "default_item_status")]
        status: String,
    },
    Delta {
        #[serde(rename = "turn_id")]
        _turn_id: String,
        item_id: String,
        #[serde(rename = "kind")]
        _kind: String,
        delta: String,
    },
    Approval {
        #[serde(default = "default_request_id")]
        id: Value,
        #[serde(default = "default_approval_method")]
        #[serde(rename = "method")]
        _method: String,
        #[serde(rename = "kind")]
        _kind: String,
        title: String,
        detail: String,
    },
    Question {
        id: String,
        title: String,
        question: String,
        options: Vec<FixtureQuestionOption>,
        #[serde(default)]
        multi: bool,
        recommended: Option<usize>,
    },
    InteractionResolved {
        id: String,
    },
    Todo {
        phases: Vec<nakode_protocol::TodoPhaseView>,
    },
    TurnCompleted {
        #[serde(rename = "turn_id")]
        _turn_id: String,
        #[serde(default = "default_turn_outcome")]
        outcome: String,
        error: Option<String>,
    },
    ContextUsage {
        estimated_tokens: usize,
        context_window: Option<usize>,
    },
    Warning {
        message: String,
    },
    RequestFailed {
        operation: String,
        #[serde(default = "default_error_code")]
        code: i64,
        message: String,
    },
    Disconnected {
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
struct FixtureModel {
    id: String,
    #[serde(default)]
    is_default: bool,
    #[serde(default)]
    configuration: nakode_protocol::ModelConfigurationView,
}

#[derive(Clone, Debug, Deserialize)]
struct FixtureQuestionOption {
    label: String,
    description: Option<String>,
}

fn default_display_name() -> String {
    "Codex".to_owned()
}

const FIXTURE_PROVIDER_ID: &str = "fixture-provider";

fn default_item_status() -> String {
    "complete".to_owned()
}

fn default_request_id() -> Value {
    json!("approval-1")
}

fn default_approval_method() -> String {
    "item/commandExecution/requestApproval".to_owned()
}

fn default_turn_outcome() -> String {
    "completed".to_owned()
}

const fn default_error_code() -> i64 {
    -1
}

impl FixtureEvent {
    fn apply_view(
        self,
        view: &mut nakode_protocol::BootstrapView,
        commands: &[crate::api_projection::TuiAction],
    ) -> Result<(), String> {
        match self {
            Self::Ready {
                display_name,
                capabilities,
                ..
            } => install_ready_view(view, &display_name, &capabilities)?,
            Self::Models { models } => install_model_views(view, models),
            Self::SessionCreated {
                provider_session_id,
                model,
            } => install_created_session_view(view, commands, &provider_session_id, model)?,
            Self::TurnStarted { turn_id } => install_started_turn_view(view, turn_id)?,
            Self::Item {
                id,
                kind,
                title,
                body,
                status,
                ..
            } => install_item_view(view, &id, &kind, title, body, &status)?,
            Self::Delta { item_id, delta, .. } => install_delta_view(view, &item_id, &delta)?,
            Self::Approval {
                id, title, detail, ..
            } => install_approval_view(view, &id, title, detail)?,
            Self::Question {
                id,
                title,
                question,
                options,
                multi,
                recommended,
            } => install_question_view(view, id, title, question, options, multi, recommended)?,
            Self::InteractionResolved { id } => resolve_interaction_view(view, &id)?,
            Self::Todo { phases } => install_todo_view(view, phases)?,
            Self::TurnCompleted { outcome, error, .. } => {
                complete_turn_view(view, &outcome, error)?;
            }
            Self::ContextUsage {
                estimated_tokens,
                context_window,
            } => install_context_usage_view(view, estimated_tokens, context_window)?,
            Self::Warning { message } => {
                install_notice(view, nakode_protocol::NoticeLevel::Warning, message)?;
            }
            Self::RequestFailed {
                operation,
                code,
                message,
            } => {
                install_notice(
                    view,
                    nakode_protocol::NoticeLevel::Error,
                    format!("{operation} failed ({code}): {message}"),
                )?;
            }
            Self::Disconnected { reason } => install_disconnected_view(view, reason),
        }
        Ok(())
    }
}

fn active_session_mut(
    view: &mut nakode_protocol::BootstrapView,
) -> Result<&mut nakode_protocol::SessionView, String> {
    view.active_session
        .as_mut()
        .ok_or_else(|| "service fixture requires an active session".to_owned())
}

fn bump_session(session: &mut nakode_protocol::SessionView) {
    session.revision = session.revision.saturating_add(1);
}

fn install_delta_view(
    view: &mut nakode_protocol::BootstrapView,
    item_id: &str,
    delta: &str,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    let item = session
        .transcript
        .entries
        .iter_mut()
        .find(|item| item.id.as_str() == item_id)
        .ok_or_else(|| format!("unknown transcript item {item_id:?}"))?;
    item.body.push_str(delta);
    bump_session(session);
    Ok(())
}

fn install_approval_view(
    view: &mut nakode_protocol::BootstrapView,
    id: &Value,
    title: String,
    detail: String,
) -> Result<(), String> {
    let interaction_id = id
        .as_str()
        .ok_or_else(|| "service approval ids must be strings".to_owned())?;
    let session = active_session_mut(view)?;
    session.interactions.push(nakode_protocol::InteractionView {
        id: interaction_id.to_owned().into(),
        revision: session.revision.saturating_add(1),
        kind: nakode_protocol::InteractionKind::Approval,
        status: nakode_protocol::InteractionStatus::Pending,
        title,
        detail,
        options: Vec::new(),
        multiple: false,
        questions: Vec::new(),
    });
    bump_session(session);
    Ok(())
}

fn install_question_view(
    view: &mut nakode_protocol::BootstrapView,
    id: String,
    title: String,
    question: String,
    options: Vec<FixtureQuestionOption>,
    multi: bool,
    recommended: Option<usize>,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    session.interactions.push(nakode_protocol::InteractionView {
        id: id.into(),
        revision: session.revision.saturating_add(1),
        kind: nakode_protocol::InteractionKind::Question,
        status: nakode_protocol::InteractionStatus::Pending,
        title,
        detail: question,
        options: options
            .into_iter()
            .enumerate()
            .map(|(index, option)| nakode_protocol::InteractionOptionView {
                id: format!("option-{}", index + 1),
                label: option.label,
                description: option.description,
                recommended: recommended == Some(index),
            })
            .collect(),
        multiple: multi,
        questions: Vec::new(),
    });
    bump_session(session);
    Ok(())
}

fn resolve_interaction_view(
    view: &mut nakode_protocol::BootstrapView,
    id: &str,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    let interaction = session
        .interactions
        .iter_mut()
        .find(|interaction| interaction.id.as_str() == id)
        .ok_or_else(|| format!("unknown interaction {id:?}"))?;
    interaction.status = nakode_protocol::InteractionStatus::Resolved;
    interaction.revision = interaction.revision.saturating_add(1);
    bump_session(session);
    Ok(())
}

fn install_todo_view(
    view: &mut nakode_protocol::BootstrapView,
    phases: Vec<nakode_protocol::TodoPhaseView>,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    session.todos = phases;
    bump_session(session);
    Ok(())
}

fn complete_turn_view(
    view: &mut nakode_protocol::BootstrapView,
    outcome: &str,
    error: Option<String>,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    session.active_turn = None;
    session.activity = nakode_protocol::SessionActivity::Idle;
    session.transcript.stream_active = false;
    session.transcript.stream_label.clear();
    session.status_message = match outcome {
        "completed" => "Turn completed.".to_owned(),
        "interrupted" => "Turn interrupted.".to_owned(),
        "failed" => error.unwrap_or_else(|| "Turn failed.".to_owned()),
        _ => return Err(format!("unknown turn outcome {outcome:?}")),
    };
    bump_session(session);
    Ok(())
}

fn install_context_usage_view(
    view: &mut nakode_protocol::BootstrapView,
    estimated_tokens: usize,
    context_window: Option<usize>,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    session.context_usage = Some(nakode_protocol::ContextUsageView {
        estimated_tokens: u64::try_from(estimated_tokens).unwrap_or(u64::MAX),
        context_window: context_window.map(|window| u64::try_from(window).unwrap_or(u64::MAX)),
        compacting: false,
    });
    bump_session(session);
    Ok(())
}

fn install_disconnected_view(view: &mut nakode_protocol::BootstrapView, reason: String) {
    let provider_id = selected_provider_id(view);
    if let Some(provider) = view
        .providers
        .iter_mut()
        .find(|provider| provider.id == provider_id)
    {
        provider.connection = nakode_protocol::ConnectionView::Disconnected { message: reason };
    }
    if let Some(session) = &mut view.active_session {
        if let Some(agent) = &mut session.active_agent_session {
            agent.connection = nakode_protocol::ConnectionView::Disconnected {
                message: "provider disconnected".to_owned(),
            };
        }
        "Provider disconnected.".clone_into(&mut session.status_message);
        bump_session(session);
    }
}

fn selected_provider_id(view: &nakode_protocol::BootstrapView) -> nakode_protocol::ProviderId {
    view.active_session
        .as_ref()
        .and_then(|session| session.selected_provider_id.clone())
        .or_else(|| view.providers.first().map(|provider| provider.id.clone()))
        .unwrap_or_else(|| nakode_protocol::ProviderId::from(FIXTURE_PROVIDER_ID.to_owned()))
}

fn install_ready_view(
    view: &mut nakode_protocol::BootstrapView,
    display_name: &str,
    capability_names: &[String],
) -> Result<(), String> {
    let provider_id = nakode_protocol::ProviderId::from(FIXTURE_PROVIDER_ID.to_owned());
    let capabilities = fixture_protocol_capabilities(capability_names)?;
    let provider = nakode_protocol::ProviderView {
        id: provider_id.clone(),
        display_name: display_name.to_owned(),
        enabled: true,
        credential_configured: true,
        credential_kind: None,
        connection: nakode_protocol::ConnectionView::Ready,
        capabilities: capabilities.clone(),
        authentication: None,
        model_filter_enabled: false,
        selected_model_ids: Vec::new(),
        model_candidates: Vec::new(),
        supported_builtin_tools: Some(Vec::new()),
        available_builtin_tools: Some(Vec::new()),
    };
    if let Some(existing) = view
        .providers
        .iter_mut()
        .find(|existing| existing.id == provider_id)
    {
        *existing = provider;
    } else {
        view.providers.push(provider);
    }
    let session = active_session_mut(view)?;
    session.selected_provider_id = Some(provider_id.clone());
    session.active_agent_session = Some(nakode_protocol::AgentSessionView {
        id: nakode_protocol::AgentSessionId::from("agent-session-1".to_owned()),
        provider_id,
        model_id: None,
        role: "primary".to_owned(),
        capabilities,
        connection: nakode_protocol::ConnectionView::Ready,
        native_session_id: None,
        transcript: session.transcript.clone(),
        usage: nakode_protocol::TokenUsageView {
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
        },
    });
    session.activity = nakode_protocol::SessionActivity::Idle;
    "Ready.".clone_into(&mut session.status_message);
    bump_session(session);
    Ok(())
}

fn fixture_protocol_capabilities(
    names: &[String],
) -> Result<nakode_protocol::ProviderCapabilities, String> {
    let mut supported = std::collections::BTreeSet::new();
    for name in names {
        supported.insert(match name.as_str() {
            "resume" => nakode_protocol::ProviderCapability::Resume,
            "steering" => nakode_protocol::ProviderCapability::Steering,
            "interruption" => nakode_protocol::ProviderCapability::Interruption,
            "model_catalog" => nakode_protocol::ProviderCapability::ModelCatalog,
            "models_require_session" => nakode_protocol::ProviderCapability::ModelsRequireSession,
            "session_model_config" => {
                nakode_protocol::ProviderCapability::SessionModelConfiguration
            }
            "context_compaction" => nakode_protocol::ProviderCapability::ContextCompaction,
            "approvals" => nakode_protocol::ProviderCapability::Approvals,
            "native_tools" => nakode_protocol::ProviderCapability::NativeTools,
            "mcp" => nakode_protocol::ProviderCapability::Mcp,
            "close_session" => nakode_protocol::ProviderCapability::CloseSession,
            "external_tools" => nakode_protocol::ProviderCapability::ExternalTools,
            _ => return Err(format!("unknown capability {name:?}")),
        });
    }
    Ok(nakode_protocol::ProviderCapabilities { supported })
}

fn install_model_views(view: &mut nakode_protocol::BootstrapView, models: Vec<FixtureModel>) {
    let provider_id = selected_provider_id(view);
    let projected = models
        .into_iter()
        .map(|model| {
            let id = if model.id.contains('/') {
                model.id.clone()
            } else {
                format!("{provider_id}/{}", model.id)
            };
            nakode_protocol::ModelView {
                id: nakode_protocol::ModelId::from(id),
                provider_id: provider_id.clone(),
                model_slug: model.id,
                display_name: "Model".to_owned(),
                is_default: model.is_default,
                reasoning_effort: None,
                fast_mode: false,
                configuration: model.configuration,
            }
        })
        .collect::<Vec<_>>();
    view.models.clone_from(&projected);
    for provider in &mut view.providers {
        provider.model_candidates = projected
            .iter()
            .filter(|model| model.provider_id == provider.id)
            .cloned()
            .collect();
    }
}

fn install_created_session_view(
    view: &mut nakode_protocol::BootstrapView,
    commands: &[crate::api_projection::TuiAction],
    provider_session_id: &str,
    model: String,
) -> Result<(), String> {
    let provider_id = selected_provider_id(view);
    let model_id = nakode_protocol::ModelId::from(if model.contains('/') {
        model
    } else {
        format!("{provider_id}/{model}")
    });
    let prompt = commands.iter().rev().find_map(|command| match command {
        crate::api_projection::TuiAction::SendPrompt { prompt, .. } => Some(prompt.text.clone()),
        _ => None,
    });
    let session = active_session_mut(view)?;
    session.selected_model_id = Some(model_id.clone());
    let capabilities = session
        .active_agent_session
        .as_ref()
        .map_or_else(nakode_protocol::ProviderCapabilities::default, |agent| {
            agent.capabilities.clone()
        });
    session.active_agent_session = Some(nakode_protocol::AgentSessionView {
        id: nakode_protocol::AgentSessionId::from(provider_session_id.to_owned()),
        provider_id,
        model_id: Some(model_id),
        role: "primary".to_owned(),
        capabilities,
        connection: nakode_protocol::ConnectionView::Ready,
        native_session_id: Some(provider_session_id.to_owned()),
        transcript: session.transcript.clone(),
        usage: nakode_protocol::TokenUsageView {
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
        },
    });
    if let Some(prompt) = prompt {
        session
            .transcript
            .entries
            .push(nakode_protocol::TranscriptEntryView {
                id: nakode_protocol::EntryId::from(format!("user-{}", session.revision + 1)),
                kind: nakode_protocol::TranscriptEntryKind::User,
                title: "YOU".to_owned(),
                body_total_bytes: u64::try_from(prompt.len()).unwrap_or(u64::MAX),
                body_start_byte: 0,
                body: prompt,
                status: nakode_protocol::TranscriptEntryStatus::Complete,
                artifacts: Vec::new(),
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                resolved_reasoning_effort: None,
                resolved_fast_mode: None,
                source_transport: None,
                tool_audit_json: None,
                created_at_ms: None,
            });
    }
    session.activity = nakode_protocol::SessionActivity::StartingTurn;
    "Starting turn…".clone_into(&mut session.status_message);
    bump_session(session);
    Ok(())
}

fn install_started_turn_view(
    view: &mut nakode_protocol::BootstrapView,
    turn_id: String,
) -> Result<(), String> {
    let provider_id = selected_provider_id(view);
    let provider_name = view
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .map_or("Provider", |provider| provider.display_name.as_str())
        .to_owned();
    let session = active_session_mut(view)?;
    let agent = session
        .active_agent_session
        .as_ref()
        .ok_or_else(|| "turn_started requires an active agent session".to_owned())?;
    session.active_turn = Some(nakode_protocol::TurnView {
        id: nakode_protocol::TurnId::from(turn_id),
        agent_session_id: agent.id.clone(),
        model_id: agent.model_id.clone(),
        resolved_model_options: nakode_protocol::ModelOptions::default(),
        status: nakode_protocol::TurnStatus::Running,
    });
    session.activity = nakode_protocol::SessionActivity::RunningTurn;
    session.transcript.stream_active = true;
    "working".clone_into(&mut session.transcript.stream_label);
    session.status_message = format!("{provider_name} is working…");
    bump_session(session);
    Ok(())
}

fn install_item_view(
    view: &mut nakode_protocol::BootstrapView,
    id: &str,
    kind: &str,
    title: String,
    body: String,
    status: &str,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    let entry = nakode_protocol::TranscriptEntryView {
        id: nakode_protocol::EntryId::from(id.to_owned()),
        kind: match kind {
            "user" => nakode_protocol::TranscriptEntryKind::User,
            "assistant" => nakode_protocol::TranscriptEntryKind::Assistant,
            "reasoning" => nakode_protocol::TranscriptEntryKind::Reasoning,
            "tool" => nakode_protocol::TranscriptEntryKind::Tool,
            "diff" => nakode_protocol::TranscriptEntryKind::Diff,
            "system" => nakode_protocol::TranscriptEntryKind::System,
            _ => return Err(format!("unknown item kind {kind:?}")),
        },
        title,
        body_total_bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
        body_start_byte: 0,
        body,
        status: match status {
            "running" => nakode_protocol::TranscriptEntryStatus::Running,
            "complete" => nakode_protocol::TranscriptEntryStatus::Complete,
            "failed" | "declined" => nakode_protocol::TranscriptEntryStatus::Failed,
            _ => return Err(format!("unknown item status {status:?}")),
        },
        artifacts: Vec::new(),
        provider_id: None,
        model_id: None,
        owner_turn_id: None,
        resolved_reasoning_effort: None,
        resolved_fast_mode: None,
        source_transport: None,
        tool_audit_json: None,
        created_at_ms: None,
    };
    if let Some(existing) = session
        .transcript
        .entries
        .iter_mut()
        .find(|entry| entry.id.as_str() == id)
    {
        *existing = entry;
    } else {
        session.transcript.entries.push(entry);
    }
    bump_session(session);
    Ok(())
}

fn install_notice(
    view: &mut nakode_protocol::BootstrapView,
    level: nakode_protocol::NoticeLevel,
    message: String,
) -> Result<(), String> {
    let session = active_session_mut(view)?;
    session.notices.push(nakode_protocol::NoticeView {
        id: format!("notice-{}", session.notices.len() + 1),
        level,
        message: message.clone(),
    });
    session.status_message = message;
    bump_session(session);
    Ok(())
}

fn key_event(key: &str, modifiers: &[String]) -> Result<KeyEvent, String> {
    Ok(KeyEvent::new(key_code(key)?, key_modifiers(modifiers)?))
}

fn key_code(key: &str) -> Result<KeyCode, String> {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "enter" => Ok(KeyCode::Enter),
        "esc" | "escape" => Ok(KeyCode::Esc),
        "tab" => Ok(KeyCode::Tab),
        "backtab" => Ok(KeyCode::BackTab),
        "backspace" => Ok(KeyCode::Backspace),
        "delete" => Ok(KeyCode::Delete),
        "insert" => Ok(KeyCode::Insert),
        "up" => Ok(KeyCode::Up),
        "down" => Ok(KeyCode::Down),
        "left" => Ok(KeyCode::Left),
        "right" => Ok(KeyCode::Right),
        "home" => Ok(KeyCode::Home),
        "end" => Ok(KeyCode::End),
        "page_up" | "pageup" => Ok(KeyCode::PageUp),
        "page_down" | "pagedown" => Ok(KeyCode::PageDown),
        "space" => Ok(KeyCode::Char(' ')),
        _ if normalized.starts_with('f') => normalized[1..]
            .parse::<u8>()
            .ok()
            .filter(|number| (1..=24).contains(number))
            .map(KeyCode::F)
            .ok_or_else(|| format!("unknown key {key:?}")),
        _ => {
            let mut characters = key.chars();
            let Some(character) = characters.next() else {
                return Err("key cannot be empty".to_owned());
            };
            if characters.next().is_some() {
                return Err(format!("unknown key {key:?}"));
            }
            Ok(KeyCode::Char(character))
        }
    }
}

fn key_modifiers(modifiers: &[String]) -> Result<KeyModifiers, String> {
    let mut value = KeyModifiers::NONE;
    for modifier in modifiers {
        value |= match modifier.as_str() {
            "shift" => KeyModifiers::SHIFT,
            "control" | "ctrl" => KeyModifiers::CONTROL,
            "alt" => KeyModifiers::ALT,
            "super" => KeyModifiers::SUPER,
            "hyper" => KeyModifiers::HYPER,
            "meta" => KeyModifiers::META,
            _ => return Err(format!("unknown key modifier {modifier:?}")),
        };
    }
    Ok(value)
}

fn mouse_event_kind(value: &str) -> Result<MouseEventKind, String> {
    match value {
        "down" => Ok(MouseEventKind::Down(MouseButton::Left)),
        "drag" => Ok(MouseEventKind::Drag(MouseButton::Left)),
        "up" => Ok(MouseEventKind::Up(MouseButton::Left)),
        "scroll_up" => Ok(MouseEventKind::ScrollUp),
        "scroll_down" => Ok(MouseEventKind::ScrollDown),
        _ => Err(format!("unknown mouse event {value:?}")),
    }
}

#[derive(Debug, Serialize)]
struct Observation {
    step: usize,
    action: &'static str,
    screen: Screen,
    state: StateView,
    commands: Vec<Value>,
    devices: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct Screen {
    width: u16,
    height: u16,
    lines: Vec<String>,
    cursor: CursorView,
    #[serde(skip_serializing_if = "Option::is_none")]
    styled_lines: Option<Vec<Vec<StyledRun>>>,
}

impl Screen {
    fn capture(backend: &TestBackend, styles: bool) -> Self {
        let buffer = backend.buffer();
        let width = buffer.area.width;
        let height = buffer.area.height;
        let rows = buffer.content().chunks(usize::from(width));
        let lines = rows
            .clone()
            .map(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect();
        let styled_lines = styles.then(|| rows.map(styled_runs).collect());
        let position = backend.cursor_position();
        Self {
            width,
            height,
            lines,
            cursor: CursorView {
                visible: backend.cursor_visible(),
                column: position.x,
                row: position.y,
            },
            styled_lines,
        }
    }
}

#[derive(Debug, Serialize)]
struct CursorView {
    visible: bool,
    column: u16,
    row: u16,
}

#[derive(Debug, Serialize)]
struct StyledRun {
    start: usize,
    text: String,
    style: String,
}

fn styled_runs(row: &[ratatui::buffer::Cell]) -> Vec<StyledRun> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut text = String::new();
    let mut style: Option<Style> = None;
    for (column, cell) in row.iter().enumerate() {
        let cell_style = cell.style();
        if style.is_some_and(|current| current != cell_style) {
            runs.push(StyledRun {
                start,
                text: std::mem::take(&mut text),
                style: format!("{:?}", style.expect("style exists")),
            });
            start = column;
        }
        style = Some(cell_style);
        text.push_str(cell.symbol());
    }
    if let Some(style) = style {
        runs.push(StyledRun {
            start,
            text,
            style: format!("{style:?}"),
        });
    }
    runs
}

#[derive(Debug, Serialize)]
struct StateView {
    connection: String,
    provider: String,
    model: Option<String>,
    session_active: bool,
    turn: Option<String>,
    modal: String,
    status: String,
    draft: String,
    queue_length: usize,
    transcript: Vec<TranscriptEntryView>,
    diagnostics: u64,
    should_quit: bool,
}

impl StateView {
    fn capture(state: &TuiState, view: &nakode_protocol::BootstrapView, should_quit: bool) -> Self {
        let active_agent = view
            .active_session
            .as_ref()
            .and_then(|session| session.active_agent_session.as_ref());
        let provider = active_agent
            .map(|agent| &agent.provider_id)
            .and_then(|provider_id| {
                view.providers
                    .iter()
                    .find(|provider| &provider.id == provider_id)
                    .map(|provider| provider.display_name.clone())
                    .or_else(|| Some(provider_id.to_string()))
            })
            .unwrap_or_default();
        Self {
            connection: active_agent
                .map_or("disabled", |agent| {
                    crate::tui_state::connection_label(&agent.connection)
                })
                .to_owned(),
            provider,
            model: state.selected_model.as_ref().map(ToString::to_string),
            session_active: state.session_id.is_some(),
            turn: state.active_turn.as_ref().map(|turn| turn.id.to_string()),
            modal: active_modal(state),
            status: state.status_message.clone(),
            draft: state.client.editor.text(),
            queue_length: state.queue.len(),
            transcript: state
                .transcript
                .entries()
                .iter()
                .rev()
                .take(20)
                .rev()
                .map(|entry| TranscriptEntryView {
                    kind: format!("{:?}", entry.kind).to_ascii_lowercase(),
                    status: format!("{:?}", entry.status).to_ascii_lowercase(),
                    title: entry.title.clone(),
                    body: entry.body.clone(),
                })
                .collect(),
            diagnostics: state.diagnostic_count,
            should_quit,
        }
    }
}

#[derive(Debug, Serialize)]
struct TranscriptEntryView {
    kind: String,
    status: String,
    title: String,
    body: String,
}

fn active_modal(state: &TuiState) -> String {
    if state.questions.front().is_some() {
        "question".to_owned()
    } else if state.approvals.front().is_some() {
        "approval".to_owned()
    } else if state.client.show_help {
        "help".to_owned()
    } else if state.client.session_picker.is_some() {
        "sessions".to_owned()
    } else if state.client.provider_picker.is_some() {
        "providers".to_owned()
    } else if let Some(settings) = &state.client.settings {
        match settings.view {
            SettingsView::Menu => "settings".to_owned(),
            SettingsView::Addons => "settings:addons".to_owned(),
            SettingsView::WebBrowsing => "settings:web".to_owned(),
            SettingsView::Vision => "settings:vision".to_owned(),
            SettingsView::Memory => "settings:memory".to_owned(),
            SettingsView::TerminalImages => "settings:terminal_images".to_owned(),
        }
    } else if state.client.agent_picker.is_some() {
        "agents".to_owned()
    } else if state.client.model_picker.is_some() {
        "models".to_owned()
    } else if state.client.subagent_modal.is_some() {
        "subagent".to_owned()
    } else {
        "none".to_owned()
    }
}

fn command_view(command: &crate::api_projection::TuiAction) -> Value {
    json!({"type": command_name(command)})
}

const fn command_name(command: &crate::api_projection::TuiAction) -> &'static str {
    use crate::api_projection::TuiAction;
    match command {
        TuiAction::CreateSession { .. } => "create_session",
        TuiAction::OpenSession { .. } => "open_session",
        TuiAction::SendPrompt { .. } => "send_prompt",
        TuiAction::EnqueuePrompt { .. } => "enqueue_prompt",
        TuiAction::RemoveQueuedPrompt { .. } => "remove_queued_prompt",
        TuiAction::SteerTurn { .. } => "steer_turn",
        TuiAction::CancelSessionWork { .. } => "cancel_session_work",
        TuiAction::CompactContext { .. } => "compact_context",
        TuiAction::SelectModel { .. } => "select_model",
        TuiAction::ResolveInteraction { .. } => "resolve_interaction",
        TuiAction::CancelRun { .. } => "cancel_run",
        TuiAction::RunShell { .. } => "run_shell",
        TuiAction::SetProviderEnabled { .. } => "set_provider_enabled",
        TuiAction::BeginProviderAuthentication { .. } => "begin_provider_authentication",
        TuiAction::SetProviderCredential { .. } => "set_provider_credential",
        TuiAction::ClearProviderCredential { .. } => "clear_provider_credential",
        TuiAction::SaveAgent { .. } => "save_agent",
        TuiAction::DeleteAgent { .. } => "delete_agent",
        TuiAction::UpdateSettings { .. } => "update_settings",
        TuiAction::CheckAgentBrowser { .. } => "check_agent_browser",
        TuiAction::SetProviderModelFilter { .. } => "set_provider_model_filter",
        TuiAction::ReloadWorkspace { .. } => "reload_workspace",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(workspace: &std::path::Path) -> Options {
        Options {
            workspace: workspace.to_owned(),
            scenario: None,
            width: 100,
            height: 28,
        }
    }

    #[test]
    fn script_drives_real_controls_and_renderer() {
        let workspace = tempfile::tempdir().expect("workspace");
        let source = concat!(
            "{\"action\":\"type\",\"text\":\"/settings\"}\n",
            "{\"action\":\"key\",\"key\":\"enter\"}\n",
            "{\"action\":\"assert\",\"modal\":\"settings\",",
            "\"screen_contains\":[\"Settings\",\"General\"],",
            "\"status\":\"Settings opened.\",\"cursor_visible\":false}\n",
        );
        let mut output = Vec::new();
        run_script(
            io::Cursor::new(source),
            &mut output,
            &options(workspace.path()),
        )
        .expect("scenario");
        let observations = String::from_utf8(output).expect("JSONL");
        assert_eq!(observations.lines().count(), 3);
        assert!(observations.contains("\"modal\":\"settings\""));
    }

    #[test]
    fn service_fixtures_make_approval_interactions_deterministic() {
        let workspace = tempfile::tempdir().expect("workspace");
        let source = concat!(
            "{\"action\":\"service\",\"event\":{\"type\":\"approval\",",
            "\"kind\":\"command\",\"title\":\"Run tests\",\"detail\":\"cargo test\"}}\n",
            "{\"action\":\"assert\",\"modal\":\"approval\",",
            "\"screen_contains\":[\"Run tests\",\"cargo test\"]}\n",
            "{\"action\":\"key\",\"key\":\"y\"}\n",
            "{\"action\":\"assert\",\"modal\":\"approval\",",
            "\"commands_include\":[\"resolve_interaction\"]}\n",
            "{\"action\":\"service\",\"event\":{\"type\":\"interaction_resolved\",",
            "\"id\":\"approval-1\"}}\n",
            "{\"action\":\"assert\",\"modal\":\"none\"}\n",
        );
        let mut output = Vec::new();
        run_script(
            io::Cursor::new(source),
            &mut output,
            &options(workspace.path()),
        )
        .expect("scenario");
        assert_eq!(String::from_utf8(output).expect("JSONL").lines().count(), 6);
    }

    #[test]
    fn failed_assertions_report_the_scenario_line() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut output = Vec::new();
        let error = run_script(
            io::Cursor::new("{\"action\":\"assert\",\"modal\":\"help\"}\n"),
            &mut output,
            &options(workspace.path()),
        )
        .expect_err("assertion should fail");
        assert!(error.to_string().contains("line 1"));
        assert!(error.to_string().contains("modal is"));
        assert!(!output.is_empty(), "failure should retain its observation");
    }

    #[test]
    fn committed_provider_model_filter_scenario_passes() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut output = Vec::new();
        run_script(
            io::Cursor::new(include_str!(
                "../tests/tui_scenarios/provider_model_filter.jsonl"
            )),
            &mut output,
            &options(workspace.path()),
        )
        .expect("provider model filter scenario");
        assert_eq!(
            String::from_utf8(output).expect("JSONL").lines().count(),
            14
        );
    }

    #[test]
    fn committed_agent_smoke_scenario_passes() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut output = Vec::new();
        run_script(
            io::Cursor::new(include_str!("../tests/tui_scenarios/agent_smoke.jsonl")),
            &mut output,
            &options(workspace.path()),
        )
        .expect("committed smoke scenario");
        assert_eq!(
            String::from_utf8(output).expect("JSONL").lines().count(),
            36
        );
    }
}
