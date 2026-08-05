use std::{collections::HashMap, path::Path};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use nakode_protocol::{
    BootstrapView, CredentialInput, InteractionKind, InteractionResolution, InteractionStatus,
    ModelId, ModelOptions, ModelTarget, PromptAttachment as ProtocolAttachment, PromptInput,
    ProviderAuthenticationView, ProviderCapability, RunStatus, SessionActivity, SessionId,
    SettingsPatch, TerminalImageModeView, TurnStatus,
};

use crate::{
    api_projection::TuiAction as Command,
    clipboard,
    commands::{ParsedPromptCommand, parse_prompt_command},
    controls::{self, ControlAction, ControlContext},
    selection::ScreenPoint,
    tui_state::{
        AgentEditor, AgentEditorField, AgentModelOption, AgentPendingOptions, ComposerDraft,
        ModelPickerStage, ModelSelectionScope, ProviderAuthentication, SettingsSection,
        SettingsView, TuiState, model_supports_fast_mode, model_supports_options,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandFollowup {
    SelectResourceSession,
    AgentSaved,
}

pub(crate) struct CommandIntent {
    pub(crate) command: Command,
    pub(crate) restore: Option<ComposerDraft>,
    pub(crate) followup: Option<CommandFollowup>,
}

impl CommandIntent {
    fn new(command: Command) -> Self {
        Self {
            command,
            restore: None,
            followup: None,
        }
    }

    fn restoring(command: Command, draft: ComposerDraft) -> Self {
        Self {
            command,
            restore: Some(draft),
            followup: None,
        }
    }

    fn selecting(command: Command) -> Self {
        Self {
            command,
            restore: None,
            followup: Some(CommandFollowup::SelectResourceSession),
        }
    }

    fn saving_agent(command: Command) -> Self {
        Self {
            command,
            restore: None,
            followup: Some(CommandFollowup::AgentSaved),
        }
    }
}

pub(crate) enum DeviceIntent {
    OpenUrl(String),
    Copy(String),
}

#[derive(Default)]
pub(crate) struct InputOutcome {
    pub(crate) commands: Vec<CommandIntent>,
    pub(crate) devices: Vec<DeviceIntent>,
    pub(crate) quit: bool,
}

pub(crate) fn handle_terminal(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    event: Event,
) -> InputOutcome {
    match event {
        Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
            state.clear_text_selection();
            handle_key(state, bootstrap, key)
        }
        Event::Paste(text) => handle_paste(state, bootstrap, &text),
        Event::Mouse(mouse) => handle_mouse(state, mouse),
        Event::Resize(_, _) => {
            state.clear_text_selection();
            InputOutcome::default()
        }
        Event::FocusGained | Event::FocusLost | Event::Key(_) => InputOutcome::default(),
    }
}

fn handle_paste(state: &mut TuiState, bootstrap: &BootstrapView, text: &str) -> InputOutcome {
    state.clear_text_selection();
    if state.provider_api_key_input_active() {
        state.provider_api_key_insert_str(text);
        return InputOutcome::default();
    }
    if agent_editor_is_open(state) {
        insert_agent_text(state, text);
        return save_agent_outcome(state, bootstrap);
    }
    if state.client.settings.is_some() {
        for character in text.chars() {
            state.settings_insert(character);
        }
        return InputOutcome::default();
    }
    if no_modal_is_open(state) && state.approvals.is_empty() && state.questions.is_empty() {
        if let Some(attachments) = clipboard::attachments_from_terminal_paste(text) {
            state.insert_attachments(protocol_attachments(attachments, &bootstrap.workspace_path));
        } else {
            state.client.editor.insert_str(text);
            state.set_status("Pasted text into the draft.");
        }
    }
    InputOutcome::default()
}

fn handle_mouse(state: &mut TuiState, mouse: crossterm::event::MouseEvent) -> InputOutcome {
    let point = ScreenPoint::new(mouse.column, mouse.row);
    let action = controls::resolve_mouse(mouse.kind);
    if action == controls::MouseAction::PrimaryDown
        && let Some(url) = state.oauth_url_at(point)
    {
        state.clear_text_selection();
        return InputOutcome {
            devices: vec![DeviceIntent::OpenUrl(url)],
            ..InputOutcome::default()
        };
    }
    if action == controls::MouseAction::PrimaryDown && state.focus_provider_api_key_at(point) {
        state.clear_text_selection();
        return InputOutcome::default();
    }
    if action == controls::MouseAction::PrimaryDown && state.toggle_tool_at(point) {
        return InputOutcome::default();
    }
    match action {
        controls::MouseAction::PrimaryDown
            if state.client.subagent_modal.is_some() || !state.open_subagent_at(point) =>
        {
            state.begin_text_selection(point);
        }
        controls::MouseAction::PrimaryDown | controls::MouseAction::Ignore => {}
        controls::MouseAction::PrimaryDrag => state.update_text_selection(point),
        controls::MouseAction::PrimaryUp => state.finish_text_selection(point),
        controls::MouseAction::ScrollUp => {
            state.clear_text_selection();
            if !scroll_open_overlay(state, -1) {
                state.scroll_active_chat(3);
            }
        }
        controls::MouseAction::ScrollDown => {
            state.clear_text_selection();
            if !scroll_open_overlay(state, 1) {
                state.scroll_active_chat(-3);
            }
        }
        controls::MouseAction::ClearSelection => state.clear_text_selection(),
    }
    let devices = state
        .take_pending_clipboard()
        .map(DeviceIntent::Copy)
        .into_iter()
        .collect();
    InputOutcome {
        devices,
        ..InputOutcome::default()
    }
}

/// Move the selection in whichever list overlay is on top, and say whether one took the wheel.
///
/// A list longer than its popup scrolls by moving its selection — that index IS the scroll position
/// (`render::scroll_start`) — so the wheel has to reach the same mover the arrow keys do. Left to fall
/// through it scrolled the transcript UNDERNEATH an open catalogue instead, which is the one surface the
/// owner cannot see while choosing a model.
///
/// Ordered the way the overlays are drawn, innermost first, so the wheel lands on the list actually on
/// screen. The agent editor's options popup takes the wheel and does nothing with it, for the same
/// reason ↑/↓ do nothing there: it has one row, and its choices are ←/→.
fn scroll_open_overlay(state: &mut TuiState, delta: isize) -> bool {
    if state.client.model_picker.is_some() {
        state.picker_move(delta);
    } else if state.agent_model_options_are_open() {
        return true;
    } else if state.agent_model_dropdown_is_open() {
        state.agent_model_dropdown_move(delta);
    } else if state.client.agent_picker.is_some() {
        state.agent_picker_move(delta);
    } else if state.client.session_picker.is_some() {
        state.session_picker_move(delta);
    } else if state.client.provider_picker.is_some() {
        state.provider_picker_move(delta);
    } else {
        return false;
    }
    true
}

fn handle_key(state: &mut TuiState, bootstrap: &BootstrapView, key: KeyEvent) -> InputOutcome {
    if let Some(outcome) = handle_modal_key(state, bootstrap, key) {
        return outcome;
    }
    if handle_command_completion_key(state, key) || handle_editor_navigation(state, key) {
        return InputOutcome::default();
    }

    match controls::resolve(ControlContext::Global, key) {
        Some(ControlAction::CancelOrQuit) => cancel_session_work_intent(bootstrap),
        Some(ControlAction::Quit) => request_quit(),
        Some(ControlAction::QueueDraft) => submit_draft(state, bootstrap, SubmitMode::Queue),
        Some(ControlAction::Steer) => submit_draft(state, bootstrap, SubmitMode::Steer),
        Some(ControlAction::Latest) => {
            state.reset_active_chat_scroll();
            state.set_status("Jumped to the latest output.");
            InputOutcome::default()
        }
        Some(ControlAction::Newline) => {
            state.client.editor.insert_newline();
            InputOutcome::default()
        }
        Some(ControlAction::Paste) => {
            paste_desktop_clipboard(state, &bootstrap.workspace_path);
            InputOutcome::default()
        }
        Some(ControlAction::OpenModelPicker) => {
            open_model_picker(state, ModelSelectionScope::Session);
            InputOutcome::default()
        }
        Some(ControlAction::ScrollUp) => {
            state.scroll_active_chat(10);
            InputOutcome::default()
        }
        Some(ControlAction::ScrollDown) => {
            state.scroll_active_chat(-10);
            InputOutcome::default()
        }
        Some(ControlAction::QueuePrevious) => {
            state.move_queue_selection(-1);
            InputOutcome::default()
        }
        Some(ControlAction::QueueNext) => {
            state.move_queue_selection(1);
            InputOutcome::default()
        }
        Some(ControlAction::QueueRemove) => remove_selected_queue_item(state, bootstrap),
        Some(ControlAction::Submit) => submit_draft(state, bootstrap, SubmitMode::Send),
        Some(ControlAction::SteerOrSubmit) => {
            let text = state.client.editor.text();
            let local = text.starts_with('!') || parse_prompt_command(&text).is_some();
            if active_turn(bootstrap).is_some() && !local {
                submit_draft(state, bootstrap, SubmitMode::Steer)
            } else {
                submit_draft(state, bootstrap, SubmitMode::Send)
            }
        }
        Some(ControlAction::BackspaceWord) => {
            state.client.editor.delete_word_backward();
            InputOutcome::default()
        }
        Some(ControlAction::BackspaceLine) => {
            state.client.editor.delete_to_line_start();
            InputOutcome::default()
        }
        Some(ControlAction::Backspace) => {
            state.client.editor.backspace();
            InputOutcome::default()
        }
        Some(ControlAction::Delete) => {
            state.client.editor.delete();
            InputOutcome::default()
        }
        Some(ControlAction::InsertTab) => {
            state.client.editor.insert_char('\t');
            InputOutcome::default()
        }
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                state.client.editor.insert_char(character);
            }
            InputOutcome::default()
        }
        Some(_) => InputOutcome::default(),
    }
}

fn handle_modal_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> Option<InputOutcome> {
    if !state.questions.is_empty() {
        return Some(handle_question_key(state, bootstrap, key));
    }
    if !state.approvals.is_empty() {
        return Some(handle_approval_key(bootstrap, key));
    }
    if state.client.show_help {
        if controls::resolve(ControlContext::Help, key).is_some() {
            state.client.show_help = false;
        }
        return Some(InputOutcome::default());
    }
    if controls::resolve(ControlContext::Global, key) == Some(ControlAction::ToggleHelp) {
        state.close_all_menus();
        state.client.show_help = true;
        return Some(InputOutcome::default());
    }
    if state.client.session_picker.is_some() {
        return Some(handle_session_picker_key(state, bootstrap, key));
    }
    if state.client.provider_picker.is_some() {
        return Some(handle_provider_picker_key(state, bootstrap, key));
    }
    if state.client.settings.is_some() {
        return Some(handle_settings_key(state, bootstrap, key));
    }
    if state.client.agent_picker.is_some() {
        return Some(handle_agent_picker_key(state, bootstrap, key));
    }
    if state.client.model_picker.is_some() {
        return Some(handle_model_picker_key(state, bootstrap, key));
    }
    if state.client.subagent_modal.is_some() {
        return Some(handle_subagent_modal_key(state, bootstrap, key));
    }
    None
}

fn handle_editor_navigation(state: &mut TuiState, key: KeyEvent) -> bool {
    match controls::resolve(ControlContext::Navigation, key) {
        Some(ControlAction::MoveWordLeft) => state.client.editor.move_word_left(),
        Some(ControlAction::MoveWordRight) => state.client.editor.move_word_right(),
        Some(ControlAction::MoveLineStart) => state.client.editor.move_home(),
        Some(ControlAction::MoveLineEnd) => state.client.editor.move_end(),
        Some(ControlAction::MoveDocumentStart) => state.client.editor.move_document_start(),
        Some(ControlAction::MoveDocumentEnd) => state.client.editor.move_document_end(),
        Some(ControlAction::MoveLeft) => state.client.editor.move_left(),
        Some(ControlAction::MoveRight) => state.client.editor.move_right(),
        Some(ControlAction::MoveUp) => state.client.editor.move_up(),
        Some(ControlAction::MoveDown) => state.client.editor.move_down(),
        _ => return false,
    }
    true
}

fn handle_command_completion_key(state: &mut TuiState, key: KeyEvent) -> bool {
    if state.command_completions().is_empty() {
        return false;
    }
    match controls::resolve(ControlContext::CommandCompletion, key) {
        Some(ControlAction::CompletionPrevious) => state.move_command_completion(-1),
        Some(ControlAction::CompletionNext) => state.move_command_completion(1),
        Some(ControlAction::CompletionAccept) if !state.command_completion_is_exact() => {
            state.accept_command_completion();
        }
        _ => return false,
    }
    true
}

#[derive(Clone, Copy)]
enum SubmitMode {
    Send,
    Queue,
    Steer,
}

fn submit_draft(state: &mut TuiState, bootstrap: &BootstrapView, mode: SubmitMode) -> InputOutcome {
    if state.client.editor.is_blank() {
        state.set_status(match mode {
            SubmitMode::Queue => "Write a message before queueing.",
            SubmitMode::Steer => "Write steering guidance first.",
            SubmitMode::Send => "Write a message before sending.",
        });
        return InputOutcome::default();
    }
    let draft = state.client.composer_draft();
    let text = draft.editor.text();
    if !matches!(mode, SubmitMode::Steer) {
        if let Some(outcome) = local_prompt_command(state, bootstrap, &text) {
            return outcome;
        }
        if let Some(command) = text.strip_prefix('!') {
            return submit_shell(state, bootstrap, draft, command);
        }
    }
    let Some(session) = bootstrap.active_session.as_ref() else {
        state.set_status("No Nakode session is selected.");
        return InputOutcome::default();
    };
    if matches!(mode, SubmitMode::Steer) {
        if !draft.attachments.is_empty() {
            state.set_status("Attachments can be sent or queued, but not used for steering.");
            return InputOutcome::default();
        }
        let Some(turn) = active_turn(bootstrap) else {
            state.set_status("There is no active turn to steer.");
            return InputOutcome::default();
        };
        if turn.status == TurnStatus::Cancelling {
            state.set_status("The active turn is being cancelled.");
            return InputOutcome::default();
        }
        let supports_steering = session
            .active_agent_session
            .as_ref()
            .is_some_and(|agent| agent.capabilities.supports(ProviderCapability::Steering));
        if !supports_steering {
            state.set_status("The selected provider does not support steering.");
            return InputOutcome::default();
        }
        clear_draft(state, &draft);
        return InputOutcome {
            commands: vec![CommandIntent::restoring(
                Command::SteerTurn {
                    turn_id: turn.id.clone(),
                    text,
                },
                draft,
            )],
            ..InputOutcome::default()
        };
    }

    clear_draft(state, &draft);
    let command = if matches!(mode, SubmitMode::Queue) {
        Command::EnqueuePrompt {
            session_id: session.id.clone(),
            prompt: protocol_prompt(&draft),
        }
    } else {
        Command::SendPrompt {
            session_id: session.id.clone(),
            prompt: protocol_prompt(&draft),
        }
    };
    InputOutcome {
        commands: vec![CommandIntent::restoring(command, draft)],
        ..InputOutcome::default()
    }
}

fn submit_shell(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    draft: ComposerDraft,
    command: &str,
) -> InputOutcome {
    if command.trim().is_empty() {
        state.set_status("Write a shell command after !.");
        return InputOutcome::default();
    }
    if !draft.attachments.is_empty() {
        state.set_status("Attachments cannot be used with shell commands.");
        return InputOutcome::default();
    }
    let Some(session_id) = bootstrap
        .active_session
        .as_ref()
        .map(|session| session.id.clone())
    else {
        state.set_status("No Nakode session is selected.");
        return InputOutcome::default();
    };
    clear_draft(state, &draft);
    InputOutcome {
        commands: vec![CommandIntent::restoring(
            Command::RunShell {
                session_id,
                command: command.to_owned(),
            },
            draft,
        )],
        ..InputOutcome::default()
    }
}

fn local_prompt_command(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    text: &str,
) -> Option<InputOutcome> {
    let command = parse_prompt_command(text)?;
    state.client.editor.clear();
    Some(match command {
        ParsedPromptCommand::Agents => {
            state.open_agent_picker();
            InputOutcome::default()
        }
        ParsedPromptCommand::Settings => {
            state.open_settings();
            InputOutcome::default()
        }
        ParsedPromptCommand::Compress => {
            let Some(agent_session_id) = bootstrap
                .active_session
                .as_ref()
                .and_then(|session| session.active_agent_session.as_ref())
                .map(|session| session.id.clone())
            else {
                state.set_status("Send a message before compressing this chat.");
                return Some(InputOutcome::default());
            };
            InputOutcome {
                commands: vec![CommandIntent::new(Command::CompactContext {
                    agent_session_id,
                })],
                ..InputOutcome::default()
            }
        }
        ParsedPromptCommand::Models => {
            open_model_picker(state, ModelSelectionScope::Default);
            InputOutcome::default()
        }
        ParsedPromptCommand::New => InputOutcome {
            commands: vec![CommandIntent::selecting(Command::CreateSession {
                workspace_id: bootstrap.workspace_id.clone(),
                title: None,
            })],
            ..InputOutcome::default()
        },
        ParsedPromptCommand::Providers => {
            open_provider_picker(state);
            InputOutcome::default()
        }
        ParsedPromptCommand::Reload => {
            let Some(session_id) = bootstrap
                .active_session
                .as_ref()
                .map(|session| session.id.clone())
            else {
                state.set_status("No Nakode session is selected.");
                return Some(InputOutcome::default());
            };
            InputOutcome {
                commands: vec![CommandIntent::new(Command::ReloadWorkspace {
                    workspace_id: bootstrap.workspace_id.clone(),
                    session_id,
                })],
                ..InputOutcome::default()
            }
        }
        ParsedPromptCommand::Resume(Some(session_id)) => InputOutcome {
            commands: vec![CommandIntent::selecting(Command::OpenSession {
                session_id: SessionId::from(session_id.to_owned()),
            })],
            ..InputOutcome::default()
        },
        ParsedPromptCommand::Resume(None) => {
            state.open_session_picker();
            InputOutcome::default()
        }
        ParsedPromptCommand::Switch => {
            open_model_picker(state, ModelSelectionScope::Session);
            InputOutcome::default()
        }
    })
}

fn protocol_prompt(draft: &ComposerDraft) -> PromptInput {
    let text = draft.editor.text();
    let mut remaining_labels = HashMap::<String, usize>::new();
    for attachment in &draft.attachments {
        let token = format!("[{}]", prompt_attachment_label(attachment));
        remaining_labels
            .entry(token.clone())
            .or_insert_with(|| text.matches(&token).count());
    }
    let attachments = draft
        .attachments
        .iter()
        .filter(|attachment| {
            let token = format!("[{}]", prompt_attachment_label(attachment));
            let Some(remaining) = remaining_labels.get_mut(&token) else {
                return false;
            };
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
            true
        })
        .cloned()
        .collect();
    PromptInput { text, attachments }
}

fn prompt_attachment_label(attachment: &ProtocolAttachment) -> &str {
    match attachment {
        ProtocolAttachment::Artifact { label, .. }
        | ProtocolAttachment::LocalFile { label, .. }
        | ProtocolAttachment::InlineImage { label, .. } => label,
    }
}

fn protocol_attachments(
    attachments: Vec<crate::clipboard::ClipboardAttachment>,
    workspace: &str,
) -> Vec<ProtocolAttachment> {
    attachments
        .into_iter()
        .filter_map(|attachment| {
            if let Some(image) = attachment.image {
                return Some(ProtocolAttachment::InlineImage {
                    label: attachment.label,
                    media_type: image.mime_type,
                    data: image.data,
                });
            }
            let path = attachment.path?;
            let path = path
                .strip_prefix(Path::new(workspace))
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            Some(ProtocolAttachment::LocalFile {
                label: attachment.label,
                path,
            })
        })
        .collect()
}

fn clear_draft(state: &mut TuiState, draft: &ComposerDraft) {
    let mut cleared = draft.clone();
    cleared.editor.clear();
    cleared.attachments.clear();
    state.client.restore_composer(cleared);
}

fn active_turn(bootstrap: &BootstrapView) -> Option<&nakode_protocol::TurnView> {
    bootstrap
        .active_session
        .as_ref()
        .and_then(|session| session.active_turn.as_ref())
}

fn remove_selected_queue_item(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(session) = bootstrap.active_session.as_ref() else {
        return InputOutcome::default();
    };
    let selected = state.client.queue_selection.unwrap_or_default();
    let Some(item) = session.queue.get(selected) else {
        state.set_status("No queued message is selected.");
        return InputOutcome::default();
    };
    InputOutcome {
        commands: vec![CommandIntent::new(Command::RemoveQueuedPrompt {
            session_id: session.id.clone(),
            prompt_id: item.id.clone(),
        })],
        ..InputOutcome::default()
    }
}

fn cancel_session_work_intent(bootstrap: &BootstrapView) -> InputOutcome {
    let Some(session) = bootstrap.active_session.as_ref() else {
        return InputOutcome {
            quit: true,
            ..InputOutcome::default()
        };
    };
    if session
        .active_turn
        .as_ref()
        .is_some_and(|turn| turn.status == TurnStatus::Cancelling)
    {
        return InputOutcome {
            quit: true,
            ..InputOutcome::default()
        };
    }
    let has_cancellable_work = session.active_turn.is_some()
        || session.activity == SessionActivity::CompactingContext
        || session.activity == SessionActivity::RunningShell
        || session
            .runs
            .iter()
            .any(|run| matches!(run.status, RunStatus::Starting | RunStatus::Working));
    let commands = has_cancellable_work
        .then(|| {
            CommandIntent::new(Command::CancelSessionWork {
                session_id: session.id.clone(),
            })
        })
        .into_iter()
        .collect::<Vec<_>>();
    InputOutcome {
        quit: commands.is_empty(),
        commands,
        ..InputOutcome::default()
    }
}

fn request_quit() -> InputOutcome {
    InputOutcome {
        quit: true,
        ..InputOutcome::default()
    }
}

fn handle_approval_key(bootstrap: &BootstrapView, key: KeyEvent) -> InputOutcome {
    let resolution = match controls::resolve(ControlContext::Approval, key) {
        Some(ControlAction::ApprovalOnce) => InteractionResolution::ApproveOnce,
        Some(ControlAction::ApprovalSession) => InteractionResolution::ApproveForSession,
        Some(ControlAction::ApprovalDecline) => InteractionResolution::Decline,
        _ => return InputOutcome::default(),
    };
    let Some(interaction) = pending_interaction(bootstrap, InteractionKind::Approval) else {
        return InputOutcome::default();
    };
    InputOutcome {
        commands: vec![CommandIntent::new(Command::ResolveInteraction {
            interaction_id: interaction.id.clone(),
            resolution,
        })],
        ..InputOutcome::default()
    }
}

fn handle_question_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::Question, key) {
        Some(ControlAction::QuestionPrevious) => state.move_question_selection(-1),
        Some(ControlAction::QuestionNext) => state.move_question_selection(1),
        Some(ControlAction::QuestionToggle) => state.toggle_question_selection(),
        Some(ControlAction::QuestionConfirm) => return resolve_question(state, bootstrap),
        Some(ControlAction::QuestionQuickSelect) => {
            let KeyCode::Char(character) = key.code else {
                return InputOutcome::default();
            };
            let selected = usize::try_from(character.to_digit(10).unwrap_or(1))
                .unwrap_or(1)
                .saturating_sub(1);
            if let Some(question) = state.questions.front_mut() {
                question.selected =
                    selected.min(question.interaction.options.len().saturating_sub(1));
            }
            if state
                .questions
                .front()
                .is_some_and(|question| question.interaction.multiple)
            {
                state.toggle_question_selection();
            } else {
                return resolve_question(state, bootstrap);
            }
        }
        _ => {}
    }
    InputOutcome::default()
}

fn resolve_question(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(interaction) = pending_interaction(bootstrap, InteractionKind::Question) else {
        return InputOutcome::default();
    };
    let Some(question) = state.questions.front() else {
        return InputOutcome::default();
    };
    let option_ids = if interaction.multiple {
        interaction
            .options
            .iter()
            .zip(&question.selections)
            .filter(|(_, selected)| **selected)
            .map(|(option, _)| option.id.clone())
            .collect::<Vec<_>>()
    } else {
        interaction
            .options
            .get(question.selected)
            .map(|option| vec![option.id.clone()])
            .unwrap_or_default()
    };
    if option_ids.is_empty() {
        state.set_status("Select at least one answer.");
        return InputOutcome::default();
    }
    InputOutcome {
        commands: vec![CommandIntent::new(Command::ResolveInteraction {
            interaction_id: interaction.id.clone(),
            resolution: InteractionResolution::Answer { option_ids },
        })],
        ..InputOutcome::default()
    }
}

fn pending_interaction(
    bootstrap: &BootstrapView,
    kind: InteractionKind,
) -> Option<&nakode_protocol::InteractionView> {
    bootstrap
        .active_session
        .as_ref()?
        .interactions
        .iter()
        .find(|interaction| {
            interaction.kind == kind && interaction.status == InteractionStatus::Pending
        })
}

fn handle_session_picker_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::SessionPicker, key) {
        Some(ControlAction::Close) => state.close_session_picker(),
        Some(ControlAction::Previous) => state.session_picker_move(-1),
        Some(ControlAction::Next) => state.session_picker_move(1),
        Some(ControlAction::Select) => {
            let selected = state
                .client
                .session_picker
                .as_ref()
                .map_or(0, |picker| picker.selected);
            let Some(session) = bootstrap.sessions.get(selected) else {
                state.set_status("No session is selected.");
                return InputOutcome::default();
            };
            state.client.session_picker = None;
            return InputOutcome {
                commands: vec![CommandIntent::selecting(Command::OpenSession {
                    session_id: session.id.clone(),
                })],
                ..InputOutcome::default()
            };
        }
        _ => {}
    }
    InputOutcome::default()
}

fn open_provider_picker(state: &mut TuiState) {
    state.open_provider_picker();
}

fn handle_provider_picker_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    let showing_details = state
        .client
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
        }
        Some(ControlAction::Backspace) if api_key_input => state.provider_api_key_backspace(),
        Some(ControlAction::Submit) if api_key_input => {
            return submit_provider_credential(state, bootstrap);
        }
        Some(ControlAction::OpenUrl) => {
            if let Some(url) = selected_provider_url(state, bootstrap) {
                return InputOutcome {
                    devices: vec![DeviceIntent::OpenUrl(url)],
                    ..InputOutcome::default()
                };
            }
        }
        Some(ControlAction::CopyUrl) => {
            if let Some(url) = selected_provider_url(state, bootstrap) {
                return InputOutcome {
                    devices: vec![DeviceIntent::Copy(url)],
                    ..InputOutcome::default()
                };
            }
        }
        Some(ControlAction::Logout) => {
            if let Some(provider) = selected_provider(state, bootstrap) {
                return InputOutcome {
                    commands: vec![CommandIntent::new(Command::ClearProviderCredential {
                        provider_id: provider.id.clone(),
                    })],
                    ..InputOutcome::default()
                };
            }
        }
        Some(ControlAction::Toggle) => return toggle_provider(state, bootstrap),
        Some(ControlAction::Focus) => {
            state.focus_provider_api_key();
        }
        Some(ControlAction::Open) => state.open_provider_details(),
        Some(ControlAction::Previous) => state.provider_picker_move(-1),
        Some(ControlAction::Next) => state.provider_picker_move(1),
        None if api_key_input => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            {
                state.provider_api_key_insert_str(&character.to_string());
            }
        }
        _ => {}
    }
    InputOutcome::default()
}

fn selected_provider<'a>(
    state: &TuiState,
    bootstrap: &'a BootstrapView,
) -> Option<&'a nakode_protocol::ProviderView> {
    let selected = state
        .client
        .provider_picker
        .as_ref()
        .map_or(0, |picker| picker.selected);
    bootstrap.providers.get(selected)
}

fn selected_provider_url(state: &TuiState, bootstrap: &BootstrapView) -> Option<String> {
    match selected_provider(state, bootstrap)?
        .authentication
        .as_ref()?
    {
        ProviderAuthenticationView::ApiKeyRequired { dashboard_url, .. } => {
            Some(dashboard_url.clone())
        }
        ProviderAuthenticationView::Challenge {
            verification_url, ..
        } => Some(verification_url.clone()),
        ProviderAuthenticationView::Starting => None,
    }
}

fn toggle_provider(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(provider) = selected_provider(state, bootstrap).cloned() else {
        return InputOutcome::default();
    };
    if provider.credential_configured {
        return InputOutcome {
            commands: vec![CommandIntent::new(Command::SetProviderEnabled {
                provider_id: provider.id,
                enabled: !provider.enabled,
            })],
            ..InputOutcome::default()
        };
    }
    if let Some(ProviderAuthenticationView::ApiKeyRequired { .. }) = provider.authentication {
        if let Some(picker) = &mut state.client.provider_picker {
            picker.showing_details = true;
            picker.authentication = Some(ProviderAuthentication::ApiKeyInput {
                value: String::new(),
                focused: true,
            });
        }
        return InputOutcome::default();
    }
    if let Some(picker) = &mut state.client.provider_picker {
        picker.authentication = Some(ProviderAuthentication::Starting);
    }
    InputOutcome {
        commands: vec![CommandIntent::new(Command::BeginProviderAuthentication {
            provider_id: provider.id,
        })],
        ..InputOutcome::default()
    }
}

fn submit_provider_credential(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(provider) = selected_provider(state, bootstrap).cloned() else {
        return InputOutcome::default();
    };
    let credential_kind = match provider.authentication {
        Some(ProviderAuthenticationView::ApiKeyRequired {
            credential_kind, ..
        }) => credential_kind,
        _ => provider
            .credential_kind
            .unwrap_or_else(|| "api_key".to_owned()),
    };
    let value = state
        .client
        .provider_picker
        .as_ref()
        .and_then(|picker| picker.authentication.as_ref())
        .and_then(|authentication| match authentication {
            ProviderAuthentication::ApiKeyInput {
                value,
                focused: true,
            } => Some(value.trim().to_owned()),
            _ => None,
        })
        .unwrap_or_default();
    if value.is_empty() {
        state.set_status("The API key cannot be empty.");
        return InputOutcome::default();
    }
    if let Some(picker) = &mut state.client.provider_picker {
        picker.authentication = Some(ProviderAuthentication::Starting);
    }
    InputOutcome {
        commands: vec![CommandIntent::new(Command::SetProviderCredential {
            provider_id: provider.id,
            kind: credential_kind,
            credential: CredentialInput(value),
        })],
        ..InputOutcome::default()
    }
}

fn open_model_picker(state: &mut TuiState, scope: ModelSelectionScope) {
    state.open_model_picker(scope);
    if state.filtered_models().is_empty() {
        state.close_model_picker();
        state.set_status("No configured models are available.");
    }
}

fn handle_model_picker_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::ModelPicker, key) {
        Some(ControlAction::Select) => return select_model_intent(state, bootstrap),
        Some(ControlAction::Close) => state.close_model_picker(),
        Some(ControlAction::Previous) => state.picker_move(-1),
        Some(ControlAction::Next) => state.picker_move(1),
        Some(ControlAction::MoveLeft) => state.picker_adjust(-1),
        Some(ControlAction::MoveRight) => state.picker_adjust(1),
        Some(ControlAction::Backspace) => state.picker_backspace(),
        Some(ControlAction::Clear) => {
            if let Some(picker) = &mut state.client.model_picker {
                picker.filter.clear();
                picker.selected = 0;
            }
        }
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                state.picker_insert(character);
            }
        }
        Some(_) => {}
    }
    InputOutcome::default()
}

fn select_model_intent(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some((scope, picker_stage)) = state
        .client
        .model_picker
        .as_ref()
        .map(|picker| (picker.scope, picker.stage))
    else {
        return InputOutcome::default();
    };
    let Some(model) = state.selected_picker_model().cloned() else {
        return InputOutcome::default();
    };
    if picker_stage == ModelPickerStage::Models
        && scope != ModelSelectionScope::Vision
        && model_supports_options(&model)
    {
        if let Some(picker) = &mut state.client.model_picker {
            picker.stage = ModelPickerStage::Options;
            picker.option_selected = 0;
            picker.options = ModelOptions {
                reasoning_effort: model.reasoning_effort.clone(),
                fast_mode: model.fast_mode,
            };
            picker.options_fast_only = model_supports_fast_mode(&model)
                && !model.configuration.reasoning_is_configurable();
            if picker.options_fast_only {
                picker.options.reasoning_effort = None;
            }
        }
        return InputOutcome::default();
    }
    let Some(target) = model_target(bootstrap, scope, &model) else {
        state.set_status("Start a session before selecting its model.");
        return InputOutcome::default();
    };
    let options = state
        .client
        .model_picker
        .as_ref()
        .map(|picker| picker.options.clone())
        .unwrap_or_default();
    state.client.model_picker = None;
    InputOutcome {
        commands: vec![CommandIntent::new(Command::SelectModel {
            target,
            model_id: model.id,
            options,
        })],
        ..InputOutcome::default()
    }
}

fn model_target(
    bootstrap: &BootstrapView,
    scope: ModelSelectionScope,
    model: &nakode_protocol::ModelView,
) -> Option<ModelTarget> {
    match scope {
        ModelSelectionScope::Default => Some(ModelTarget::ProviderDefault {
            provider_id: model.provider_id.clone(),
        }),
        ModelSelectionScope::Vision => Some(ModelTarget::Vision),
        ModelSelectionScope::Session => {
            bootstrap
                .active_session
                .as_ref()
                .map(|session| ModelTarget::Session {
                    session_id: session.id.clone(),
                })
        }
    }
}

fn handle_settings_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::Settings, key) {
        Some(ControlAction::Close) => return close_settings_view(state),
        Some(ControlAction::Select) => return select_setting(state, bootstrap),
        Some(ControlAction::Previous) => state.settings_move(-1),
        Some(ControlAction::Next) => state.settings_move(1),
        Some(ControlAction::MoveLeft) => return cycle_setting(state, -1),
        Some(ControlAction::MoveRight) => return cycle_setting(state, 1),
        Some(ControlAction::Clear) => return clear_setting(state),
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
    InputOutcome::default()
}

fn select_setting(state: &mut TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(settings) = state.client.settings.as_ref() else {
        return InputOutcome::default();
    };
    match settings.view {
        SettingsView::Menu => {
            let section = settings.filtered_sections().get(settings.selected).copied();
            match section {
                Some(SettingsSection::General) => open_provider_picker(state),
                Some(SettingsSection::Agents) => state.open_agent_picker(),
                Some(SettingsSection::Models) => {
                    open_model_picker(state, ModelSelectionScope::Default);
                }
                Some(SettingsSection::Addons) => {
                    if let Some(settings) = &mut state.client.settings {
                        settings.view = SettingsView::Addons;
                        settings.selected = 0;
                        settings.addon_field = 0;
                    }
                }
                None => {}
            }
            InputOutcome::default()
        }
        SettingsView::Addons => {
            let check_agent_browser = settings.selected == 0;
            if let Some(settings) = &mut state.client.settings {
                settings.view = match settings.selected {
                    0 => SettingsView::WebBrowsing,
                    1 => SettingsView::Vision,
                    2 => SettingsView::Memory,
                    _ => SettingsView::TerminalImages,
                };
                settings.addon_field = 0;
            }
            InputOutcome {
                commands: check_agent_browser
                    .then(|| {
                        CommandIntent::new(Command::CheckAgentBrowser {
                            workspace_id: bootstrap.workspace_id.clone(),
                        })
                    })
                    .into_iter()
                    .collect(),
                ..InputOutcome::default()
            }
        }
        SettingsView::Vision => {
            open_model_picker(state, ModelSelectionScope::Vision);
            InputOutcome::default()
        }
        SettingsView::WebBrowsing | SettingsView::Memory | SettingsView::TerminalImages => {
            cycle_setting(state, 1)
        }
    }
}

fn close_settings_view(state: &mut TuiState) -> InputOutcome {
    let Some(view) = state.client.settings.as_ref().map(|settings| settings.view) else {
        return InputOutcome::default();
    };
    if matches!(view, SettingsView::Menu) {
        state.close_settings();
        return InputOutcome::default();
    }
    let outcome = save_settings_view(state, view);
    if let Some(settings) = &mut state.client.settings {
        settings.view = if matches!(
            view,
            SettingsView::WebBrowsing
                | SettingsView::Vision
                | SettingsView::Memory
                | SettingsView::TerminalImages
        ) {
            SettingsView::Addons
        } else {
            SettingsView::Menu
        };
        settings.selected = 0;
        settings.addon_field = 0;
    }
    outcome
}

fn cycle_setting(state: &mut TuiState, delta: isize) -> InputOutcome {
    let Some(view) = state.client.settings.as_ref().map(|settings| settings.view) else {
        return InputOutcome::default();
    };
    match view {
        SettingsView::WebBrowsing => state.settings_cycle_web_backend(delta),
        SettingsView::Memory => state.settings_cycle_memory_backend(delta),
        SettingsView::TerminalImages => state.settings_cycle_terminal_images(delta),
        SettingsView::Menu | SettingsView::Addons | SettingsView::Vision => {
            return InputOutcome::default();
        }
    }
    save_settings_view(state, view)
}

fn clear_setting(state: &mut TuiState) -> InputOutcome {
    let Some(settings) = &mut state.client.settings else {
        return InputOutcome::default();
    };
    if settings.view != SettingsView::Vision {
        return InputOutcome::default();
    }
    settings.vision.model = None;
    InputOutcome {
        commands: vec![CommandIntent::new(Command::UpdateSettings {
            patch: SettingsPatch::Vision { model_id: None },
        })],
        ..InputOutcome::default()
    }
}

fn save_settings_view(state: &TuiState, view: SettingsView) -> InputOutcome {
    let Some(settings) = state.client.settings.as_ref() else {
        return InputOutcome::default();
    };
    let patch = match view {
        SettingsView::WebBrowsing => {
            let credential = (!settings.web.firecrawl_api_key.is_empty()
                && settings.web.firecrawl_api_key != "••••••••")
                .then(|| CredentialInput(settings.web.firecrawl_api_key.clone()));
            SettingsPatch::Web {
                backend: settings.web.backend.slug().to_owned(),
                credential,
            }
        }
        SettingsView::Memory => SettingsPatch::Memory {
            backend: settings.memory.backend.slug().to_owned(),
            executable: Some(settings.memory.executable.clone()),
            global_bank: Some(settings.memory.global_bank.clone()),
            data_directory: Some(settings.memory.data_directory.clone()),
        },
        SettingsView::TerminalImages => SettingsPatch::TerminalImages {
            mode: match settings.terminal_images {
                TerminalImageModeView::Auto => "auto",
                TerminalImageModeView::On => "on",
                TerminalImageModeView::Off => "off",
            }
            .to_owned(),
        },
        SettingsView::Vision => SettingsPatch::Vision {
            model_id: settings.vision.model.clone().map(ModelId::from),
        },
        SettingsView::Menu | SettingsView::Addons => return InputOutcome::default(),
    };
    InputOutcome {
        commands: vec![CommandIntent::new(Command::UpdateSettings { patch })],
        ..InputOutcome::default()
    }
}

fn handle_agent_picker_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    if agent_editor_is_open(state) {
        return handle_agent_editor_key(state, bootstrap, key);
    }
    match controls::resolve(ControlContext::AgentList, key) {
        Some(ControlAction::Close) => state.close_agent_picker(),
        Some(ControlAction::Open) => state.edit_selected_agent(),
        Some(ControlAction::Create) => state.create_agent(),
        Some(ControlAction::Delete) => {
            let slug = state
                .client
                .agent_picker
                .as_ref()
                .and_then(|picker| picker.agents.get(picker.selected))
                .map(|agent| agent.slug.clone());
            if let Some(slug) = slug {
                return InputOutcome {
                    commands: vec![CommandIntent::new(Command::DeleteAgent {
                        workspace_id: bootstrap.workspace_id.clone(),
                        slug,
                    })],
                    ..InputOutcome::default()
                };
            }
        }
        Some(ControlAction::Previous) => state.agent_picker_move(-1),
        Some(ControlAction::Next) => state.agent_picker_move(1),
        _ => {}
    }
    InputOutcome::default()
}

fn handle_agent_editor_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    if state.agent_model_options_are_open() {
        match controls::resolve(ControlContext::ModelPicker, key) {
            Some(ControlAction::Select) => {
                if let Some(editor) = agent_editor_mut(state)
                    && let Some(pending) = editor.pending_options.take()
                {
                    editor.fast_mode = pending.options.fast_mode;
                    editor.reasoning_effort = pending.options.reasoning_effort;
                }
                return save_agent_outcome(state, bootstrap);
            }
            Some(ControlAction::Close) => {
                state.cancel_agent_edit();
            }
            Some(ControlAction::Previous) => state.move_agent_model_options(-1),
            Some(ControlAction::Next) => state.move_agent_model_options(1),
            Some(ControlAction::MoveLeft) => state.adjust_agent_model_options(-1),
            Some(ControlAction::MoveRight) => state.adjust_agent_model_options(1),
            _ => {}
        }
        return InputOutcome::default();
    }
    if state.agent_model_dropdown_is_open() {
        return handle_agent_model_dropdown_key(state, bootstrap, key);
    }
    match controls::resolve(ControlContext::AgentEditor, key) {
        Some(ControlAction::Previous) => state.agent_editor_move(-1),
        Some(ControlAction::Close) => {
            state.cancel_agent_edit();
        }
        Some(ControlAction::Open) => state.open_agent_model_dropdown(),
        Some(ControlAction::Next) => state.agent_editor_move(1),
        Some(ControlAction::Backspace) => {
            if let Some(editor) = agent_editor_mut(state) {
                agent_editor_value_mut(editor).pop();
            }
            return save_agent_outcome(state, bootstrap);
        }
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
            {
                if let Some(editor) = agent_editor_mut(state) {
                    agent_editor_value_mut(editor).push(character);
                }
                return save_agent_outcome(state, bootstrap);
            }
        }
        Some(_) => {}
    }
    InputOutcome::default()
}

fn handle_agent_model_dropdown_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::SearchableDropdown, key) {
        Some(ControlAction::Select) => {
            let selected = agent_editor_mut(state)
                .and_then(|editor| editor.model_dropdown.as_ref())
                .and_then(|dropdown| {
                    dropdown
                        .selected_item(AgentModelOption::search_text)
                        .cloned()
                });
            let Some(selected) = selected else {
                return InputOutcome::default();
            };
            // What the chosen model can be given decides the step: its own levels, and fast mode if
            // it takes one. "Inherit the parent model" names no model, so it can be given neither.
            let (model_id, efforts, supports_fast_mode) = match selected {
                AgentModelOption::Inherit => (String::new(), Vec::new(), false),
                AgentModelOption::Model(model) => (
                    model.id.to_string(),
                    model.configuration.reasoning_efforts.clone(),
                    model_supports_fast_mode(&model),
                ),
            };
            let configurable = !efforts.is_empty() || supports_fast_mode;
            if let Some(editor) = agent_editor_mut(state) {
                editor.model = model_id;
                editor.model_dropdown = None;
                // A level the new model does not offer is dropped rather than carried over: the
                // level lists differ per model, and a stale one would be refused at run time.
                let kept = editor
                    .reasoning_effort
                    .take()
                    .filter(|effort| efforts.contains(effort));
                if !supports_fast_mode {
                    editor.fast_mode = false;
                }
                editor.pending_options = configurable.then_some(AgentPendingOptions {
                    reasoning_efforts: efforts,
                    fast_mode_configurable: supports_fast_mode,
                    options: ModelOptions {
                        reasoning_effort: kept.clone(),
                        fast_mode: editor.fast_mode,
                    },
                    selected: 0,
                });
                editor.reasoning_effort = kept;
            }
            if configurable {
                return InputOutcome::default();
            }
            return save_agent_outcome(state, bootstrap);
        }
        Some(ControlAction::Close) => {
            state.cancel_agent_edit();
        }
        Some(ControlAction::Previous) => state.agent_model_dropdown_move(-1),
        Some(ControlAction::Next) => state.agent_model_dropdown_move(1),
        Some(ControlAction::Backspace) => {
            if let Some(dropdown) =
                agent_editor_mut(state).and_then(|editor| editor.model_dropdown.as_mut())
            {
                dropdown.backspace();
            }
        }
        Some(ControlAction::Clear) => state.clear_agent_model_dropdown_query(),
        None => {
            if let KeyCode::Char(character) = key.code
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER)
                && let Some(dropdown) =
                    agent_editor_mut(state).and_then(|editor| editor.model_dropdown.as_mut())
            {
                dropdown.insert(character);
            }
        }
        Some(_) => {}
    }
    InputOutcome::default()
}

fn agent_editor_is_open(state: &TuiState) -> bool {
    state
        .client
        .agent_picker
        .as_ref()
        .is_some_and(|picker| picker.editor.is_some())
}

fn agent_editor_mut(state: &mut TuiState) -> Option<&mut AgentEditor> {
    state
        .client
        .agent_picker
        .as_mut()
        .and_then(|picker| picker.editor.as_mut())
}

fn agent_editor_value_mut(editor: &mut AgentEditor) -> &mut String {
    match editor.field {
        AgentEditorField::Slug => &mut editor.slug,
        AgentEditorField::Description => &mut editor.description,
        AgentEditorField::SystemPrompt => &mut editor.system_prompt,
        AgentEditorField::FirstMessage => &mut editor.first_message,
        AgentEditorField::Model => &mut editor.model,
        AgentEditorField::FallbackModels => &mut editor.fallback_models,
    }
}

fn insert_agent_text(state: &mut TuiState, text: &str) {
    let Some(editor) = agent_editor_mut(state) else {
        return;
    };
    if let Some(dropdown) = &mut editor.model_dropdown {
        dropdown.insert_str(text);
    } else {
        agent_editor_value_mut(editor).push_str(text);
    }
}

fn save_agent_outcome(state: &TuiState, bootstrap: &BootstrapView) -> InputOutcome {
    let Some(editor) = state
        .client
        .agent_picker
        .as_ref()
        .and_then(|picker| picker.editor.as_ref())
    else {
        return InputOutcome::default();
    };
    let Some(definition) = editor.definition_input() else {
        return InputOutcome::default();
    };
    InputOutcome {
        commands: vec![CommandIntent::saving_agent(Command::SaveAgent {
            workspace_id: bootstrap.workspace_id.clone(),
            definition,
            previous_slug: editor.original_slug.clone(),
        })],
        ..InputOutcome::default()
    }
}

fn handle_subagent_modal_key(
    state: &mut TuiState,
    bootstrap: &BootstrapView,
    key: KeyEvent,
) -> InputOutcome {
    match controls::resolve(ControlContext::Subagent, key) {
        Some(ControlAction::CancelOrQuit) => {
            let Some(run_id) = state.client.subagent_modal.as_deref() else {
                return InputOutcome::default();
            };
            let Some(run) = bootstrap
                .active_session
                .as_ref()
                .and_then(|session| session.runs.iter().find(|run| run.id.as_str() == run_id))
                .filter(|run| matches!(run.status, RunStatus::Starting | RunStatus::Working))
            else {
                return InputOutcome::default();
            };
            return InputOutcome {
                commands: vec![CommandIntent::new(Command::CancelRun {
                    run_id: run.id.clone(),
                })],
                ..InputOutcome::default()
            };
        }
        Some(ControlAction::Latest) => state.reset_active_chat_scroll(),
        Some(ControlAction::ScrollUp) => state.scroll_active_chat(10),
        Some(ControlAction::ScrollDown) => state.scroll_active_chat(-10),
        Some(ControlAction::Close) => state.close_subagent_modal(),
        _ => {}
    }
    InputOutcome::default()
}

fn paste_desktop_clipboard(state: &mut TuiState, workspace: &str) {
    match clipboard::read_desktop() {
        Ok(clipboard::ClipboardPayload::Attachments(attachments)) => {
            state.insert_attachments(protocol_attachments(attachments, workspace));
        }
        Ok(clipboard::ClipboardPayload::Text(text)) => {
            state.client.editor.insert_str(&text);
            state.set_status("Pasted text into the draft.");
        }
        Err(error) => state.set_status(format!("Could not paste: {error}")),
    }
}

fn no_modal_is_open(state: &TuiState) -> bool {
    state.client.model_picker.is_none()
        && state.client.session_picker.is_none()
        && state.client.provider_picker.is_none()
        && state.client.agent_picker.is_none()
        && state.client.settings.is_none()
        && state.client.subagent_modal.is_none()
        && !state.client.show_help
}

#[cfg(test)]
mod tests {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEventKind};
    use nakode_protocol::{
        AgentSessionId, ModelConfigurationView, ModelId, ModelView, ProviderId, SessionActivity,
        TurnId, TurnStatus, TurnView,
    };

    use super::{handle_terminal, open_model_picker};
    use crate::{
        api_projection::TuiAction as Command,
        tui_state::{ModelPickerStage, ModelSelectionScope, SettingsView, TuiState},
    };

    fn bootstrap() -> nakode_protocol::BootstrapView {
        serde_json::from_value(serde_json::json!({
            "workspace_id": "workspace-1",
            "workspace_path": "/workspace",
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
                "id": "session-1",
                "workspace_id": "workspace-1",
                "title": "Session",
                "active_provider_id": null,
                "active_model_id": null,
                "updated_at_ms": 0
            }],
            "active_session": {
                "id": "session-1",
                "revision": 1,
                "workspace_id": "workspace-1",
                "title": "Session",
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
        .expect("valid bootstrap")
    }

    fn state(view: &nakode_protocol::BootstrapView) -> TuiState {
        TuiState::from_bootstrap(view, 100)
    }

    #[test]
    fn submit_emits_a_semantic_prompt_without_mutating_server_projection() {
        let view = bootstrap();
        let mut state = state(&view);
        state.client.editor.insert_str("hello");

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );

        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(
            &outcome.commands[0].command,
            Command::SendPrompt { session_id, prompt }
                if session_id.as_str() == "session-1" && prompt.text == "hello"
        ));
        assert!(state.client.editor.is_blank());
        assert!(state.queue.is_empty());
    }

    #[test]
    fn cancel_without_work_is_a_local_quit() {
        let view = bootstrap();
        let mut state = state(&view);

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        );

        assert!(outcome.quit);
        assert!(outcome.commands.is_empty());
    }

    #[test]
    fn quit_detaches_while_server_work_is_active() {
        let mut view = bootstrap();
        let session = view.active_session.as_mut().expect("active session");
        session.activity = SessionActivity::RunningTurn;
        session.active_turn = Some(TurnView {
            id: TurnId::from("turn-1"),
            agent_session_id: AgentSessionId::from("agent-session-1"),
            model_id: None,
            status: TurnStatus::Running,
        });
        let mut state = state(&view);

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL)),
        );

        assert!(outcome.quit);
        assert!(outcome.commands.is_empty());
    }

    #[test]
    fn control_c_cancels_active_server_work_instead_of_detaching() {
        let mut view = bootstrap();
        let session = view.active_session.as_mut().expect("active session");
        session.activity = SessionActivity::RunningTurn;
        session.active_turn = Some(TurnView {
            id: TurnId::from("turn-1"),
            agent_session_id: AgentSessionId::from("agent-session-1"),
            model_id: None,
            status: TurnStatus::Running,
        });
        let mut state = state(&view);

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        );

        assert!(!outcome.quit);
        assert!(matches!(
            &outcome.commands[0].command,
            Command::CancelSessionWork { session_id } if session_id.as_str() == "session-1"
        ));
    }

    #[test]
    fn control_c_cancels_context_compaction_instead_of_detaching() {
        let mut view = bootstrap();
        view.active_session
            .as_mut()
            .expect("active session")
            .activity = SessionActivity::CompactingContext;
        let mut state = state(&view);

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        );

        assert!(!outcome.quit);
        assert!(matches!(
            &outcome.commands[0].command,
            Command::CancelSessionWork { session_id } if session_id.as_str() == "session-1"
        ));
    }

    #[test]
    fn control_c_cancels_a_server_owned_shell_without_blocking_prompt_submission() {
        let mut view = bootstrap();
        view.active_session
            .as_mut()
            .expect("active session")
            .activity = SessionActivity::RunningShell;
        let mut state = state(&view);

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        );

        assert!(!outcome.quit);
        assert!(matches!(
            &outcome.commands[0].command,
            Command::CancelSessionWork { session_id } if session_id.as_str() == "session-1"
        ));
    }

    #[test]
    fn new_session_is_a_server_command_not_a_projection_mutation() {
        let view = bootstrap();
        let mut state = state(&view);
        state.client.editor.insert_str("/new");

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );

        assert!(matches!(
            &outcome.commands[0].command,
            Command::CreateSession { workspace_id, .. }
                if workspace_id.as_str() == "workspace-1"
        ));
        assert_eq!(
            state
                .session_id
                .as_ref()
                .map(nakode_protocol::SessionId::as_str),
            Some("session-1")
        );
    }

    #[test]
    fn reload_targets_the_selected_logical_session() {
        let view = bootstrap();
        let mut state = state(&view);
        state.client.editor.insert_str("/reload");

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );

        assert!(matches!(
            &outcome.commands[0].command,
            Command::ReloadWorkspace {
                workspace_id,
                session_id,
            } if workspace_id.as_str() == "workspace-1" && session_id.as_str() == "session-1"
        ));
    }

    #[test]
    fn opening_web_settings_requests_a_server_owned_browser_check() {
        let view = bootstrap();
        let mut state = state(&view);
        state.open_settings();
        let settings = state.client.settings.as_mut().expect("settings");
        settings.view = SettingsView::Addons;
        settings.selected = 0;

        let outcome = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );

        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(SettingsView::WebBrowsing)
        );
        assert!(matches!(
            &outcome.commands[0].command,
            Command::CheckAgentBrowser { workspace_id }
                if workspace_id.as_str() == "workspace-1"
        ));
    }

    #[test]
    fn nested_picker_close_restores_the_settings_menu() {
        let view = bootstrap();
        let mut state = state(&view);
        state.open_settings();

        let _ = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );
        assert!(state.client.settings.is_none());
        assert!(state.client.provider_picker.is_some());

        let _ = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        );
        assert!(state.client.provider_picker.is_none());
        assert!(state.client.settings.is_some());
    }

    #[test]
    fn model_options_and_vision_filter_use_semantic_configuration() {
        let mut view = bootstrap();
        view.models = vec![
            ModelView {
                id: ModelId::from("custom/capable"),
                provider_id: ProviderId::from("custom"),
                model_slug: "capable".to_owned(),
                display_name: "Capable".to_owned(),
                is_default: true,
                reasoning_effort: Some("small".to_owned()),
                fast_mode: false,
                configuration: ModelConfigurationView {
                    reasoning_efforts: vec!["small".to_owned(), "large".to_owned()],
                    fast_mode_configurable: true,
                    vision_eligible: true,
                },
            },
            ModelView {
                id: ModelId::from("custom/text"),
                provider_id: ProviderId::from("custom"),
                model_slug: "text".to_owned(),
                display_name: "Text".to_owned(),
                is_default: false,
                reasoning_effort: None,
                fast_mode: false,
                configuration: ModelConfigurationView::default(),
            },
        ];
        let mut state = state(&view);

        open_model_picker(&mut state, ModelSelectionScope::Session);
        let first = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );
        assert!(first.commands.is_empty());
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.stage),
            Some(ModelPickerStage::Options)
        );

        let _ = handle_terminal(
            &mut state,
            &view,
            Event::Key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE)),
        );
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .and_then(|picker| picker.options.reasoning_effort.as_deref()),
            Some("large")
        );

        state.close_model_picker();
        open_model_picker(&mut state, ModelSelectionScope::Vision);
        assert_eq!(state.filtered_models().len(), 1);
        assert_eq!(state.filtered_models()[0].id.as_str(), "custom/capable");
    }

    /// The wheel moves the open catalogue's selection — which is its scroll position — and only reaches
    /// the transcript when no list is over it.
    #[test]
    fn the_wheel_scrolls_an_open_model_catalogue_and_not_the_transcript_under_it() {
        let mut view = bootstrap();
        view.models = (0..12)
            .map(|index| ModelView {
                id: ModelId::from(format!("custom/model-{index:02}")),
                provider_id: ProviderId::from("custom"),
                model_slug: format!("model-{index:02}"),
                display_name: format!("Model {index:02}"),
                is_default: index == 0,
                reasoning_effort: None,
                fast_mode: false,
                configuration: ModelConfigurationView::default(),
            })
            .collect();
        let mut state = state(&view);
        open_model_picker(&mut state, ModelSelectionScope::Default);

        let wheel = |kind| {
            Event::Mouse(crossterm::event::MouseEvent {
                kind,
                column: 10,
                row: 10,
                modifiers: KeyModifiers::NONE,
            })
        };
        let _ = handle_terminal(&mut state, &view, wheel(MouseEventKind::ScrollDown));
        let _ = handle_terminal(&mut state, &view, wheel(MouseEventKind::ScrollDown));
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.selected),
            Some(2),
            "two notches down must walk two rows down the catalogue"
        );

        let _ = handle_terminal(&mut state, &view, wheel(MouseEventKind::ScrollUp));
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.selected),
            Some(1)
        );

        // With no overlay open the wheel is the transcript's again.
        state.close_model_picker();
        let _ = handle_terminal(&mut state, &view, wheel(MouseEventKind::ScrollUp));
        assert!(state.client.model_picker.is_none());
    }
}
