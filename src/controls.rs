use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEventKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlContext {
    Global,
    Navigation,
    CommandCompletion,
    Help,
    Approval,
    Question,
    SessionPicker,
    ModelPicker,
    ProviderList,
    ProviderDetails,
    ProviderCredential,
    AgentList,
    AgentEditor,
    Settings,
    Subagent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlAction {
    ToggleHelp,
    Close,
    CancelOrQuit,
    Quit,
    QueueDraft,
    Steer,
    Latest,
    Newline,
    OpenModelPicker,
    ScrollUp,
    ScrollDown,
    QueuePrevious,
    QueueNext,
    QueueRemove,
    Submit,
    BackspaceWord,
    BackspaceLine,
    Backspace,
    Delete,
    InsertTab,
    MoveWordLeft,
    MoveWordRight,
    MoveLineStart,
    MoveLineEnd,
    MoveDocumentStart,
    MoveDocumentEnd,
    MoveLeft,
    MoveRight,
    MoveUp,
    MoveDown,
    CompletionPrevious,
    CompletionNext,
    CompletionAccept,
    ApprovalOnce,
    ApprovalSession,
    ApprovalDecline,
    QuestionPrevious,
    QuestionNext,
    QuestionToggle,
    QuestionConfirm,
    QuestionQuickSelect,
    Select,
    Previous,
    Next,
    Clear,
    OpenUrl,
    CopyUrl,
    Logout,
    Toggle,
    Focus,
    Open,
    Create,
    Save,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModifierMatch {
    Any,
    None,
    Control,
    Alt,
    Shift,
    Boundary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyMatch {
    Code(KeyCode, ModifierMatch),
    QuestionShortcut,
    QuickSelect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HelpEntry {
    pub group: &'static str,
    pub keys: &'static str,
    pub description: &'static str,
}

const EXTRA_HELP: &[HelpEntry] = &[HelpEntry {
    group: "Navigate",
    keys: "Mouse drag / wheel",
    description: "select text or scroll the active transcript",
}];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KeyControl {
    context: ControlContext,
    action: ControlAction,
    binding: KeyMatch,
    help: Option<HelpEntry>,
}

macro_rules! control {
    ($context:ident, $action:ident, $code:expr, $modifiers:ident) => {
        KeyControl {
            context: ControlContext::$context,
            action: ControlAction::$action,
            binding: KeyMatch::Code($code, ModifierMatch::$modifiers),
            help: None,
        }
    };
    ($context:ident, $action:ident, $code:expr, $modifiers:ident, $group:literal, $keys:literal, $description:literal) => {
        KeyControl {
            context: ControlContext::$context,
            action: ControlAction::$action,
            binding: KeyMatch::Code($code, ModifierMatch::$modifiers),
            help: Some(HelpEntry {
                group: $group,
                keys: $keys,
                description: $description,
            }),
        }
    };
}

const KEY_CONTROLS: &[KeyControl] = &[
    control!(
        Global,
        ToggleHelp,
        KeyCode::F(1),
        Any,
        "General",
        "F1 / Ctrl+?",
        "toggle this control reference"
    ),
    KeyControl {
        context: ControlContext::Global,
        action: ControlAction::ToggleHelp,
        binding: KeyMatch::QuestionShortcut,
        help: None,
    },
    control!(
        Global,
        CancelOrQuit,
        KeyCode::Char('c'),
        Control,
        "Active turn",
        "Ctrl+C",
        "interrupt the turn and subagents; press again to exit"
    ),
    control!(
        Global,
        Quit,
        KeyCode::Char('d'),
        Control,
        "General",
        "Ctrl+D",
        "exit while idle"
    ),
    control!(
        Global,
        QueueDraft,
        KeyCode::Char('q'),
        Control,
        "Active turn",
        "Ctrl+Q",
        "queue the draft"
    ),
    control!(
        Global,
        Steer,
        KeyCode::Char('s'),
        Control,
        "Active turn",
        "Ctrl+S",
        "steer now"
    ),
    control!(
        Global,
        Latest,
        KeyCode::Char('l'),
        Control,
        "Navigate",
        "Ctrl+L",
        "jump to latest output"
    ),
    control!(
        Global,
        Newline,
        KeyCode::Char('j'),
        Control,
        "Compose",
        "Shift+Enter / Ctrl+J",
        "insert a newline"
    ),
    control!(Global, Newline, KeyCode::Enter, Shift),
    control!(
        Global,
        OpenModelPicker,
        KeyCode::F(2),
        Any,
        "Navigate",
        "F2",
        "switch the current session model"
    ),
    control!(
        Global,
        ScrollUp,
        KeyCode::PageUp,
        Any,
        "Navigate",
        "PageUp / PageDown",
        "scroll the transcript"
    ),
    control!(Global, ScrollDown, KeyCode::PageDown, Any),
    control!(
        Global,
        QueuePrevious,
        KeyCode::Up,
        Alt,
        "Navigate",
        "Alt+↑ / Alt+↓",
        "select a queued message"
    ),
    control!(Global, QueueNext, KeyCode::Down, Alt),
    control!(
        Global,
        QueueRemove,
        KeyCode::Delete,
        Alt,
        "Navigate",
        "Alt+Delete",
        "remove the selected queued message"
    ),
    control!(
        Global,
        QueueDraft,
        KeyCode::Enter,
        Alt,
        "Compose",
        "Alt+Enter",
        "send while idle or queue during a turn"
    ),
    control!(
        Global,
        Submit,
        KeyCode::Enter,
        None,
        "Compose",
        "Enter / Ctrl+Enter",
        "send while idle or steer during a turn"
    ),
    control!(Global, Submit, KeyCode::Enter, Control),
    control!(
        Global,
        BackspaceWord,
        KeyCode::Backspace,
        Alt,
        "Navigate",
        "Alt+Backspace",
        "delete the previous word"
    ),
    control!(
        Global,
        BackspaceLine,
        KeyCode::Backspace,
        Boundary,
        "Navigate",
        "Ctrl/Cmd+Backspace",
        "delete to the line start"
    ),
    control!(Global, Backspace, KeyCode::Backspace, None),
    control!(Global, Delete, KeyCode::Delete, None),
    control!(Global, InsertTab, KeyCode::Tab, None),
    control!(
        Navigation,
        MoveWordLeft,
        KeyCode::Left,
        Alt,
        "Navigate",
        "Alt+← / Alt+→",
        "move by word"
    ),
    control!(Navigation, MoveWordRight, KeyCode::Right, Alt),
    control!(
        Navigation,
        MoveLineStart,
        KeyCode::Left,
        Boundary,
        "Navigate",
        "Ctrl/Cmd+← / →",
        "move to the line edge"
    ),
    control!(Navigation, MoveLineEnd, KeyCode::Right, Boundary),
    control!(
        Navigation,
        MoveDocumentStart,
        KeyCode::Up,
        Boundary,
        "Navigate",
        "Ctrl/Cmd+↑ / ↓",
        "move to the prompt edge"
    ),
    control!(Navigation, MoveDocumentEnd, KeyCode::Down, Boundary),
    control!(Navigation, MoveLeft, KeyCode::Left, None),
    control!(Navigation, MoveRight, KeyCode::Right, None),
    control!(Navigation, MoveUp, KeyCode::Up, None),
    control!(Navigation, MoveDown, KeyCode::Down, None),
    control!(Navigation, MoveLineStart, KeyCode::Home, Any),
    control!(Navigation, MoveLineEnd, KeyCode::End, Any),
    control!(CommandCompletion, CompletionPrevious, KeyCode::Up, None),
    control!(CommandCompletion, CompletionNext, KeyCode::Down, None),
    control!(CommandCompletion, CompletionAccept, KeyCode::Tab, None),
    control!(CommandCompletion, CompletionAccept, KeyCode::Enter, None),
    control!(Help, Close, KeyCode::Esc, Any),
    control!(Help, ToggleHelp, KeyCode::F(1), Any),
    KeyControl {
        context: ControlContext::Help,
        action: ControlAction::ToggleHelp,
        binding: KeyMatch::QuestionShortcut,
        help: None,
    },
    control!(Approval, ApprovalOnce, KeyCode::Char('y'), Any),
    control!(Approval, ApprovalSession, KeyCode::Char('a'), Any),
    control!(Approval, ApprovalDecline, KeyCode::Char('n'), Any),
    control!(Approval, ApprovalDecline, KeyCode::Esc, Any),
    control!(Question, QuestionPrevious, KeyCode::Up, Any),
    control!(Question, QuestionNext, KeyCode::Down, Any),
    control!(Question, QuestionToggle, KeyCode::Char(' '), Any),
    control!(Question, QuestionConfirm, KeyCode::Enter, Any),
    KeyControl {
        context: ControlContext::Question,
        action: ControlAction::QuestionQuickSelect,
        binding: KeyMatch::QuickSelect,
        help: None,
    },
    control!(SessionPicker, Close, KeyCode::Esc, Any),
    control!(SessionPicker, Select, KeyCode::Enter, Any),
    control!(SessionPicker, Previous, KeyCode::Up, Any),
    control!(SessionPicker, Next, KeyCode::Down, Any),
    control!(ModelPicker, Select, KeyCode::Enter, Any),
    control!(ModelPicker, Close, KeyCode::Esc, Any),
    control!(ModelPicker, Previous, KeyCode::Up, Any),
    control!(ModelPicker, Next, KeyCode::Down, Any),
    control!(ModelPicker, Backspace, KeyCode::Backspace, Any),
    control!(ModelPicker, Clear, KeyCode::Char('u'), Control),
    control!(ProviderList, Close, KeyCode::Esc, Any),
    control!(ProviderList, Open, KeyCode::Enter, Any),
    control!(ProviderList, Previous, KeyCode::Up, Any),
    control!(ProviderList, Next, KeyCode::Down, Any),
    control!(ProviderDetails, Close, KeyCode::Esc, Any),
    control!(ProviderDetails, OpenUrl, KeyCode::Char('o'), Any),
    control!(ProviderDetails, CopyUrl, KeyCode::Char('c'), Any),
    control!(ProviderDetails, Logout, KeyCode::Char('l'), Any),
    control!(ProviderDetails, Toggle, KeyCode::Enter, Any),
    control!(ProviderDetails, Toggle, KeyCode::Char(' '), Any),
    control!(ProviderDetails, Focus, KeyCode::Tab, Any),
    control!(ProviderCredential, Close, KeyCode::Esc, Any),
    control!(ProviderCredential, Submit, KeyCode::Enter, Any),
    control!(ProviderCredential, Backspace, KeyCode::Backspace, Any),
    control!(AgentList, Close, KeyCode::Esc, Any),
    control!(AgentList, Open, KeyCode::Enter, Any),
    control!(AgentList, Create, KeyCode::Char('n'), Any),
    control!(AgentList, Delete, KeyCode::Char('d'), Any),
    control!(AgentList, Delete, KeyCode::Delete, Any),
    control!(AgentList, Previous, KeyCode::Up, Any),
    control!(AgentList, Next, KeyCode::Down, Any),
    control!(AgentEditor, Previous, KeyCode::BackTab, Any),
    control!(AgentEditor, Previous, KeyCode::Up, Any),
    control!(AgentEditor, Previous, KeyCode::Tab, Shift),
    control!(AgentEditor, Close, KeyCode::Esc, Any),
    control!(AgentEditor, Save, KeyCode::Char('s'), Control),
    control!(AgentEditor, Next, KeyCode::Tab, None),
    control!(AgentEditor, Next, KeyCode::Down, Any),
    control!(AgentEditor, Backspace, KeyCode::Backspace, Any),
    control!(Settings, Close, KeyCode::Esc, Any),
    control!(Settings, Select, KeyCode::Enter, Any),
    control!(Settings, Previous, KeyCode::Up, Any),
    control!(Settings, Next, KeyCode::Down, Any),
    control!(Settings, MoveLeft, KeyCode::Left, Any),
    control!(Settings, MoveRight, KeyCode::Right, Any),
    control!(Settings, Backspace, KeyCode::Backspace, Any),
    control!(Subagent, CancelOrQuit, KeyCode::Char('c'), Control),
    control!(Subagent, Latest, KeyCode::Char('l'), Control),
    control!(Subagent, ScrollUp, KeyCode::PageUp, Any),
    control!(Subagent, ScrollDown, KeyCode::PageDown, Any),
    control!(Subagent, Close, KeyCode::Esc, Any),
];

#[must_use]
pub fn resolve(context: ControlContext, key: KeyEvent) -> Option<ControlAction> {
    KEY_CONTROLS
        .iter()
        .find(|control| control.context == context && control.binding.matches(key))
        .map(|control| control.action)
}

pub fn help_entries() -> impl Iterator<Item = HelpEntry> {
    KEY_CONTROLS
        .iter()
        .filter_map(|control| control.help)
        .chain(EXTRA_HELP.iter().copied())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MouseAction {
    PrimaryDown,
    PrimaryDrag,
    PrimaryUp,
    ScrollUp,
    ScrollDown,
    ClearSelection,
    Ignore,
}

#[must_use]
pub const fn resolve_mouse(kind: MouseEventKind) -> MouseAction {
    match kind {
        MouseEventKind::Down(MouseButton::Left) => MouseAction::PrimaryDown,
        MouseEventKind::Drag(MouseButton::Left) => MouseAction::PrimaryDrag,
        MouseEventKind::Up(MouseButton::Left) => MouseAction::PrimaryUp,
        MouseEventKind::ScrollUp => MouseAction::ScrollUp,
        MouseEventKind::ScrollDown => MouseAction::ScrollDown,
        MouseEventKind::Down(_) => MouseAction::ClearSelection,
        _ => MouseAction::Ignore,
    }
}

impl KeyMatch {
    fn matches(self, key: KeyEvent) -> bool {
        match self {
            Self::Code(code, modifiers) => code == key.code && modifiers.matches(key.modifiers),
            Self::QuestionShortcut => {
                key.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(key.code, KeyCode::Char('?' | '/' | '_'))
            }
            Self::QuickSelect => {
                matches!(key.code, KeyCode::Char(character) if ('1'..='8').contains(&character))
            }
        }
    }
}

impl ModifierMatch {
    fn matches(self, modifiers: KeyModifiers) -> bool {
        match self {
            Self::Any => true,
            Self::None => !modifiers.intersects(
                KeyModifiers::CONTROL
                    | KeyModifiers::ALT
                    | KeyModifiers::SHIFT
                    | KeyModifiers::SUPER
                    | KeyModifiers::HYPER,
            ),
            Self::Control => modifiers.contains(KeyModifiers::CONTROL),
            Self::Alt => modifiers.contains(KeyModifiers::ALT),
            Self::Shift => modifiers.contains(KeyModifiers::SHIFT),
            Self::Boundary => modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPlacement {
    PromptStart,
    Anywhere,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlashAction {
    Agents,
    Settings,
    Compress,
    Models,
    New,
    Providers,
    Reload,
    Resume,
    Switch,
    Skill,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlashControl {
    pub action: SlashAction,
    pub invocation: &'static str,
    pub description: &'static str,
    pub placement: CommandPlacement,
}

pub const SKILL_PREFIX: &str = "/skill:";

const SLASH_CONTROLS: &[SlashControl] = &[
    SlashControl {
        action: SlashAction::Agents,
        invocation: "/agents",
        description: "manage delegated agent archetypes",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Settings,
        invocation: "/settings",
        description: "search and manage Nakode settings",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Compress,
        invocation: "/compress",
        description: "compress the current chat context now",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Models,
        invocation: "/models",
        description: "choose the default model for future sessions",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::New,
        invocation: "/new",
        description: "start a fresh session",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Providers,
        invocation: "/providers",
        description: "manage enabled agent backends",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Reload,
        invocation: "/reload",
        description: "refresh backend metadata and models",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Resume,
        invocation: "/resume",
        description: "choose or name a session to resume",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Switch,
        invocation: "/switch",
        description: "switch models for this session only",
        placement: CommandPlacement::PromptStart,
    },
    SlashControl {
        action: SlashAction::Skill,
        invocation: SKILL_PREFIX,
        description: "reference a skill anywhere in the prompt",
        placement: CommandPlacement::Anywhere,
    },
];

#[must_use]
pub const fn slash_controls() -> &'static [SlashControl] {
    SLASH_CONTROLS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_help_entry_comes_from_a_registered_control() {
        assert!(help_entries().count() >= 10);
        assert!(
            slash_controls()
                .iter()
                .any(|control| control.invocation == "/models")
        );
    }

    #[test]
    fn resolves_terminal_variants_of_the_help_shortcut() {
        for character in ['?', '/', '_'] {
            assert_eq!(
                resolve(
                    ControlContext::Global,
                    KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL)
                ),
                Some(ControlAction::ToggleHelp)
            );
        }
    }

    #[test]
    fn contexts_do_not_register_ambiguous_bindings() {
        for (index, control) in KEY_CONTROLS.iter().enumerate() {
            assert!(
                KEY_CONTROLS.iter().skip(index + 1).all(|candidate| {
                    candidate.context != control.context || candidate.binding != control.binding
                }),
                "duplicate binding in {:?}",
                control.context
            );
        }
    }
}
