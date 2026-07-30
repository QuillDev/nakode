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
    app,
    backend::{
        ApprovalKind, ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent,
        BackendIdentity, BackendOperation, CapabilitySupport, DeltaKind, ItemKind, ItemStatus,
        ModelInfo, NormalizedItem, QuestionOption, QuestionRequest, TodoPhase, TurnOutcome,
    },
    render,
    state::{AppState, Effect, SettingsView},
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
    state: AppState,
    effects: Vec<Effect>,
}

impl Harness {
    fn new(options: &Options) -> Result<Self, Box<dyn Error>> {
        let terminal = Terminal::new(TestBackend::new(options.width, options.height))?;
        let state = AppState::new(options.workspace.to_string_lossy(), None, 2_000);
        Ok(Self {
            terminal,
            state,
            effects: Vec::new(),
        })
    }

    fn apply(&mut self, action: Action) -> Result<(), String> {
        if !matches!(action, Action::Snapshot { .. } | Action::Assert { .. }) {
            self.effects.clear();
        }
        match action {
            Action::Snapshot { .. } | Action::Assert { .. } => {}
            Action::Key { key, modifiers } => {
                let event = key_event(&key, &modifiers)?;
                self.effects = app::handle_terminal_event(&mut self.state, Event::Key(event));
            }
            Action::Type { text } => {
                for character in text.chars() {
                    self.effects.extend(app::handle_terminal_event(
                        &mut self.state,
                        Event::Key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE)),
                    ));
                }
            }
            Action::Paste { text } => {
                self.effects = app::handle_terminal_event(&mut self.state, Event::Paste(text));
            }
            Action::Mouse {
                kind,
                column,
                row,
                modifiers,
            } => {
                let kind = mouse_event_kind(&kind)?;
                let modifiers = key_modifiers(&modifiers)?;
                self.effects = app::handle_terminal_event(
                    &mut self.state,
                    Event::Mouse(MouseEvent {
                        kind,
                        column,
                        row,
                        modifiers,
                    }),
                );
            }
            Action::Resize { width, height } => {
                if width < 20 || height < 8 {
                    return Err("terminal dimensions must be at least 20x8".to_owned());
                }
                self.terminal.backend_mut().resize(width, height);
                self.terminal
                    .autoresize()
                    .map_err(|error| error.to_string())?;
                self.effects =
                    app::handle_terminal_event(&mut self.state, Event::Resize(width, height));
            }
            Action::Backend { provider, event } => {
                let provider = provider.unwrap_or_else(|| self.state.backend_provider.clone());
                self.effects = self
                    .state
                    .handle_provider_backend(&provider, event.into_backend(&provider)?);
            }
        }
        Ok(())
    }

    fn observe(&mut self, step: usize, action: &'static str, styles: bool) -> Observation {
        self.terminal
            .draw(|frame| render::draw(frame, &mut self.state))
            .expect("the in-memory terminal should render");
        let screen = Screen::capture(self.terminal.backend(), styles);
        let effects = self.effects.iter().map(effect_view).collect();
        Observation {
            step,
            action,
            screen,
            state: StateView::capture(&self.state),
            effects,
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
    Backend {
        provider: Option<String>,
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
            Self::Backend { .. } => "backend",
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
    effects_include: Vec<String>,
    #[serde(default)]
    effects_exclude: Vec<String>,
    cursor_visible: Option<bool>,
    screen_width: Option<u16>,
    screen_height: Option<u16>,
}

impl Assertion {
    fn check(&self, observation: &Observation) -> Result<(), String> {
        self.check_screen(&observation.screen)?;
        self.check_state(&observation.state)?;
        self.check_effects(&observation.effects)
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

    fn check_effects(&self, effects: &[Value]) -> Result<(), String> {
        let effect_names = effects
            .iter()
            .filter_map(|effect| effect.get("type").and_then(Value::as_str))
            .collect::<Vec<_>>();
        for expected in &self.effects_include {
            if !effect_names.contains(&expected.as_str()) {
                return Err(format!(
                    "effects {effect_names:?} do not include {expected:?}"
                ));
            }
        }
        for excluded in &self.effects_exclude {
            if effect_names.contains(&excluded.as_str()) {
                return Err(format!(
                    "effects {effect_names:?} unexpectedly include {excluded:?}"
                ));
            }
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
        #[serde(default)]
        version: Option<String>,
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
        turn_id: String,
        id: String,
        kind: String,
        title: String,
        body: String,
        #[serde(default = "default_item_status")]
        status: String,
    },
    Delta {
        turn_id: String,
        item_id: String,
        kind: String,
        delta: String,
    },
    Approval {
        #[serde(default = "default_request_id")]
        id: Value,
        #[serde(default = "default_approval_method")]
        method: String,
        kind: String,
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
    Todo {
        phases: Vec<TodoPhase>,
    },
    TurnCompleted {
        turn_id: String,
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
}

#[derive(Clone, Debug, Deserialize)]
struct FixtureQuestionOption {
    label: String,
    description: Option<String>,
}

fn default_display_name() -> String {
    "Codex".to_owned()
}

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
    fn into_backend(self, provider: &str) -> Result<BackendEvent, String> {
        Ok(match self {
            Self::Ready {
                display_name,
                version,
                capabilities,
            } => BackendEvent::Ready(BackendIdentity {
                provider: provider.to_owned(),
                display_name,
                version,
                capabilities: fixture_capabilities(&capabilities)?,
            }),
            Self::Models { models } => BackendEvent::Models(
                models
                    .into_iter()
                    .map(|model| ModelInfo {
                        provider: provider.to_owned(),
                        id: model.id,
                        is_default: model.is_default,
                    })
                    .collect(),
            ),
            Self::SessionCreated {
                provider_session_id,
                model,
            } => BackendEvent::SessionCreated {
                provider_session_id,
                model,
            },
            Self::TurnStarted { turn_id } => BackendEvent::TurnStarted { turn_id },
            Self::Item {
                turn_id,
                id,
                kind,
                title,
                body,
                status,
            } => fixture_item_event(turn_id, id, &kind, title, body, &status)?,
            Self::Delta {
                turn_id,
                item_id,
                kind,
                delta,
            } => BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind: fixture_delta_kind(&kind)?,
                delta,
            },
            Self::Approval {
                id,
                method,
                kind,
                title,
                detail,
            } => BackendEvent::ApprovalRequested(ApprovalRequest {
                id,
                method,
                kind: fixture_approval_kind(&kind)?,
                title,
                detail,
            }),
            Self::Question {
                id,
                title,
                question,
                options,
                multi,
                recommended,
            } => fixture_question_event(id, title, question, options, multi, recommended),
            Self::Todo { phases } => BackendEvent::TodoUpdated { phases },
            Self::TurnCompleted {
                turn_id,
                outcome,
                error,
            } => BackendEvent::TurnCompleted {
                turn_id,
                outcome: fixture_turn_outcome(&outcome)?,
                error,
            },
            Self::ContextUsage {
                estimated_tokens,
                context_window,
            } => BackendEvent::ContextUsageUpdated {
                estimated_tokens,
                context_window,
            },
            Self::Warning { message } => BackendEvent::Warning(message),
            Self::RequestFailed {
                operation,
                code,
                message,
            } => BackendEvent::RequestFailed {
                operation: fixture_backend_operation(&operation)?,
                code,
                message,
            },
            Self::Disconnected { reason } => BackendEvent::Disconnected { reason },
        })
    }
}

fn fixture_item_event(
    turn_id: String,
    id: String,
    kind: &str,
    title: String,
    body: String,
    status: &str,
) -> Result<BackendEvent, String> {
    let item = NormalizedItem {
        id,
        kind: fixture_item_kind(kind)?,
        title,
        body,
        status: fixture_item_status(status)?,
    };
    Ok(if status == "running" {
        BackendEvent::ItemStarted { turn_id, item }
    } else {
        BackendEvent::ItemCompleted { turn_id, item }
    })
}

fn fixture_question_event(
    id: String,
    title: String,
    question: String,
    options: Vec<FixtureQuestionOption>,
    multi: bool,
    recommended: Option<usize>,
) -> BackendEvent {
    BackendEvent::QuestionRequested(QuestionRequest {
        id,
        title,
        question,
        options: options
            .into_iter()
            .map(|option| QuestionOption {
                label: option.label,
                description: option.description,
            })
            .collect(),
        multi,
        recommended,
    })
}

fn fixture_capabilities(names: &[String]) -> Result<BackendCapabilities, String> {
    let mut capabilities = BackendCapabilities::default();
    for name in names {
        let value = CapabilitySupport::Supported;
        match name.as_str() {
            "resume" => capabilities.resume = value,
            "steering" => capabilities.steering = value,
            "interruption" => capabilities.interruption = value,
            "model_catalog" => capabilities.model_catalog = value,
            "models_require_session" => capabilities.models_require_session = value,
            "session_model_config" => capabilities.session_model_config = value,
            "context_compaction" => capabilities.context_compaction = value,
            "approvals" => capabilities.approvals = value,
            "native_tools" => capabilities.native_tools = value,
            "mcp" => capabilities.mcp = value,
            "close_session" => capabilities.close_session = value,
            _ => return Err(format!("unknown capability {name:?}")),
        }
    }
    Ok(capabilities)
}

fn fixture_item_kind(value: &str) -> Result<ItemKind, String> {
    match value {
        "user" => Ok(ItemKind::User),
        "assistant" => Ok(ItemKind::Assistant),
        "reasoning" => Ok(ItemKind::Reasoning),
        "tool" => Ok(ItemKind::Tool),
        "diff" => Ok(ItemKind::Diff),
        "system" => Ok(ItemKind::System),
        _ => Err(format!("unknown item kind {value:?}")),
    }
}

fn fixture_item_status(value: &str) -> Result<ItemStatus, String> {
    match value {
        "running" => Ok(ItemStatus::Running),
        "complete" => Ok(ItemStatus::Complete),
        "failed" => Ok(ItemStatus::Failed),
        "declined" => Ok(ItemStatus::Declined),
        _ => Err(format!("unknown item status {value:?}")),
    }
}

fn fixture_delta_kind(value: &str) -> Result<DeltaKind, String> {
    match value {
        "assistant" => Ok(DeltaKind::Assistant),
        "plan" => Ok(DeltaKind::Plan),
        "reasoning" => Ok(DeltaKind::Reasoning),
        "tool" => Ok(DeltaKind::Tool),
        _ => value
            .strip_prefix("reasoning_summary:")
            .and_then(|index| index.parse::<usize>().ok())
            .map(|index| DeltaKind::ReasoningSummary { index })
            .ok_or_else(|| format!("unknown delta kind {value:?}")),
    }
}

fn fixture_approval_kind(value: &str) -> Result<ApprovalKind, String> {
    match value {
        "command" => Ok(ApprovalKind::Command),
        "file_change" => Ok(ApprovalKind::FileChange),
        "other" => Ok(ApprovalKind::Other),
        _ => Err(format!("unknown approval kind {value:?}")),
    }
}

fn fixture_turn_outcome(value: &str) -> Result<TurnOutcome, String> {
    match value {
        "completed" => Ok(TurnOutcome::Completed),
        "interrupted" => Ok(TurnOutcome::Interrupted),
        "failed" => Ok(TurnOutcome::Failed),
        _ => Err(format!("unknown turn outcome {value:?}")),
    }
}

fn fixture_backend_operation(value: &str) -> Result<BackendOperation, String> {
    match value {
        "initialize" => Ok(BackendOperation::Initialize),
        "authenticate" => Ok(BackendOperation::Authenticate),
        "model_list" => Ok(BackendOperation::ModelList),
        "reload" => Ok(BackendOperation::Reload),
        "set_session_model" => Ok(BackendOperation::SetSessionModel),
        "start_session" => Ok(BackendOperation::StartSession),
        "resume_session" => Ok(BackendOperation::ResumeSession),
        "unsubscribe_session" => Ok(BackendOperation::UnsubscribeSession),
        "compact_session" => Ok(BackendOperation::CompactSession),
        "start_turn" => Ok(BackendOperation::StartTurn),
        "steer_turn" => Ok(BackendOperation::SteerTurn),
        "interrupt_turn" => Ok(BackendOperation::InterruptTurn),
        _ => Err(format!("unknown backend operation {value:?}")),
    }
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
    effects: Vec<Value>,
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
    diagnostics: usize,
    should_quit: bool,
}

impl StateView {
    fn capture(state: &AppState) -> Self {
        Self {
            connection: state.connection.label().to_owned(),
            provider: state.backend_provider.clone(),
            model: state.selected_model.clone(),
            session_active: state.provider_session_id.is_some(),
            turn: state.active_turn.as_ref().map(|turn| turn.id.clone()),
            modal: active_modal(state),
            status: state.status_message.clone(),
            draft: state.editor.text(),
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
            should_quit: state.should_quit,
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

fn active_modal(state: &AppState) -> String {
    if state.questions.front().is_some() {
        "question".to_owned()
    } else if state.approvals.front().is_some() {
        "approval".to_owned()
    } else if state.show_help {
        "help".to_owned()
    } else if state.session_picker.is_some() {
        "sessions".to_owned()
    } else if state.provider_picker.is_some() {
        "providers".to_owned()
    } else if let Some(settings) = &state.settings {
        match settings.view {
            SettingsView::Menu => "settings".to_owned(),
            SettingsView::Addons => "settings:addons".to_owned(),
            SettingsView::WebBrowsing => "settings:web".to_owned(),
            SettingsView::Vision => "settings:vision".to_owned(),
            SettingsView::TerminalImages => "settings:terminal_images".to_owned(),
        }
    } else if state.agent_picker.is_some() {
        "agents".to_owned()
    } else if state.model_picker.is_some() {
        "models".to_owned()
    } else if state.subagent_modal.is_some() {
        "subagent".to_owned()
    } else {
        "none".to_owned()
    }
}

#[allow(clippy::too_many_lines)]
fn effect_view(effect: &Effect) -> Value {
    match effect {
        Effect::Backend(command) => backend_command_view(command),
        Effect::RunShell { id, command } => {
            json!({"type": "run_shell", "id": id, "command": command})
        }
        Effect::SpawnSubagent { run_id, provider } => {
            json!({"type": "spawn_subagent", "run_id": run_id, "provider": provider})
        }
        Effect::SubagentBackend { run_id, command } => {
            let mut value = backend_command_view(command);
            value["type"] = json!("subagent_backend");
            value["run_id"] = json!(run_id);
            value
        }
        Effect::StopSubagent(run_id) => json!({"type": "stop_subagent", "run_id": run_id}),
        Effect::CompleteAgentRequest {
            request_id,
            result,
            success,
        } => json!({
            "type": "complete_agent_request",
            "request_id": request_id,
            "result": result,
            "success": success
        }),
        Effect::ListSessions => json!({"type": "list_sessions"}),
        Effect::ListProviders => json!({"type": "list_providers"}),
        Effect::SetProviderEnabled { provider, enabled } => {
            json!({"type": "set_provider_enabled", "provider": provider, "enabled": enabled})
        }
        Effect::AuthenticateProvider(provider) => {
            json!({"type": "authenticate_provider", "provider": provider})
        }
        Effect::SaveProviderCredential { provider, kind, .. } => {
            json!({"type": "save_provider_credential", "provider": provider, "kind": kind})
        }
        Effect::ClearProviderCredential(provider) => {
            json!({"type": "clear_provider_credential", "provider": provider})
        }
        Effect::OpenUrl(url) => json!({"type": "open_url", "url": url}),
        Effect::SaveAgent {
            definition,
            previous_slug,
        } => json!({
            "type": "save_agent",
            "slug": definition.slug,
            "previous_slug": previous_slug
        }),
        Effect::DeleteAgent(slug) => json!({"type": "delete_agent", "slug": slug}),
        Effect::ReloadConfiguration => json!({"type": "reload_configuration"}),
        Effect::ResolveSession(id) => json!({"type": "resolve_session", "id": id}),
        Effect::PersistSession {
            provider,
            provider_session_id,
            title,
            model,
            ..
        } => json!({
            "type": "persist_session",
            "provider": provider,
            "provider_session_id": provider_session_id,
            "title": title,
            "model": model
        }),
        Effect::PersistModels { provider, models } => {
            json!({"type": "persist_models", "provider": provider, "count": models.len()})
        }
        Effect::SetDefaultModel { provider, model } => {
            json!({"type": "set_default_model", "provider": provider, "model": model})
        }
        Effect::SaveModelOptions {
            provider,
            model,
            options,
        } => json!({
            "type": "save_model_options",
            "provider": provider,
            "model": model,
            "reasoning_effort": options.reasoning_effort,
            "fast_mode": options.fast_mode
        }),
        Effect::PersistSubagent(record) => {
            json!({"type": "persist_subagent", "id": record.id})
        }
        Effect::LoadSubagents(session_id) => {
            json!({"type": "load_subagents", "session_id": session_id})
        }
        Effect::UpdateSessionModel { session_id, model } => {
            json!({"type": "update_session_model", "session_id": session_id, "model": model})
        }
        Effect::TouchSession(session_id) => {
            json!({"type": "touch_session", "session_id": session_id})
        }
        Effect::SaveWebConfig(_) => json!({"type": "save_web_config"}),
        Effect::SaveVisionConfig(_) => json!({"type": "save_vision_config"}),
        Effect::SaveTerminalImageMode(mode) => {
            json!({
                "type": "save_terminal_image_mode",
                "mode": format!("{mode:?}").to_ascii_lowercase()
            })
        }
        Effect::CheckAgentBrowser => json!({"type": "check_agent_browser"}),
        Effect::Quit => json!({"type": "quit"}),
    }
}

fn backend_command_view(command: &BackendCommand) -> Value {
    match command {
        BackendCommand::BeginAuthentication => json!({"type": "backend:begin_authentication"}),
        BackendCommand::StartSession {
            model,
            instructions,
        } => json!({
            "type": "backend:start_session",
            "model": model,
            "has_instructions": instructions.is_some()
        }),
        BackendCommand::ResumeSession {
            provider_session_id,
        } => json!({
            "type": "backend:resume_session",
            "provider_session_id": provider_session_id
        }),
        BackendCommand::UnsubscribeSession {
            provider_session_id,
        } => json!({
            "type": "backend:unsubscribe_session",
            "provider_session_id": provider_session_id
        }),
        BackendCommand::StartTurn {
            provider_session_id,
            client_id,
            prompt,
            attachments,
            model,
        } => json!({
            "type": "backend:start_turn",
            "provider_session_id": provider_session_id,
            "client_id": client_id,
            "prompt": prompt,
            "attachment_count": attachments.len(),
            "model": model
        }),
        BackendCommand::SteerTurn {
            provider_session_id,
            turn_id,
            prompt,
            ..
        } => json!({
            "type": "backend:steer_turn",
            "provider_session_id": provider_session_id,
            "turn_id": turn_id,
            "prompt": prompt
        }),
        BackendCommand::InterruptTurn {
            provider_session_id,
            turn_id,
        } => json!({
            "type": "backend:interrupt_turn",
            "provider_session_id": provider_session_id,
            "turn_id": turn_id
        }),
        BackendCommand::CompactSession {
            provider_session_id,
            compaction_id,
        } => json!({
            "type": "backend:compact_session",
            "provider_session_id": provider_session_id,
            "compaction_id": compaction_id
        }),
        BackendCommand::SetSessionModel {
            provider_session_id,
            model,
        } => json!({
            "type": "backend:set_session_model",
            "provider_session_id": provider_session_id,
            "model": model
        }),
        BackendCommand::SetSessionOptions {
            provider_session_id,
            options,
        } => json!({
            "type": "backend:set_session_options",
            "provider_session_id": provider_session_id,
            "reasoning_effort": options.reasoning_effort,
            "fast_mode": options.fast_mode
        }),
        BackendCommand::Reload {
            provider_session_id,
        } => json!({
            "type": "backend:reload",
            "provider_session_id": provider_session_id
        }),
        BackendCommand::ResolveApproval { id, decision } => json!({
            "type": "backend:resolve_approval",
            "id": id,
            "decision": format!("{decision:?}").to_ascii_lowercase()
        }),
        BackendCommand::ResolveQuestion { id, answer } => json!({
            "type": "backend:resolve_question",
            "id": id,
            "answer": answer
        }),
        BackendCommand::Shutdown => json!({"type": "backend:shutdown"}),
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
    fn backend_fixtures_make_approval_interactions_deterministic() {
        let workspace = tempfile::tempdir().expect("workspace");
        let source = concat!(
            "{\"action\":\"backend\",\"event\":{\"type\":\"approval\",",
            "\"kind\":\"command\",\"title\":\"Run tests\",\"detail\":\"cargo test\"}}\n",
            "{\"action\":\"assert\",\"modal\":\"approval\",",
            "\"screen_contains\":[\"Run tests\",\"cargo test\"]}\n",
            "{\"action\":\"key\",\"key\":\"y\"}\n",
            "{\"action\":\"assert\",\"modal\":\"none\",",
            "\"effects_include\":[\"backend:resolve_approval\"]}\n",
        );
        let mut output = Vec::new();
        run_script(
            io::Cursor::new(source),
            &mut output,
            &options(workspace.path()),
        )
        .expect("scenario");
        assert_eq!(String::from_utf8(output).expect("JSONL").lines().count(), 4);
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
            32
        );
    }
}
