use std::collections::{HashMap, VecDeque};

use nakode_protocol::{
    AgentBrowserView, AgentDefinitionInput, AgentDefinitionView, ArtifactView, BootstrapView,
    EntryId, InteractionKind, InteractionStatus, InteractionView, ModelId, ModelOptions, ModelView,
    PromptAttachment, PromptId, ProviderAuthenticationView, ProviderCapability, ProviderView,
    QueueItemView, RunId, RunStatus, RunView, SessionActivity, SessionId, SessionSummary,
    SessionView, SkillView, TerminalImageModeView, TodoPhaseView, TurnView, WorkspaceId,
};

use crate::{
    commands::{self, CommandSpec},
    editor::EditorState,
    searchable_dropdown::SearchableDropdown,
    selection::{ScreenPoint, ScreenSnapshot, TextSelection},
    settings::TerminalImageMode,
    transcript::{EntryKind, EntryStatus, TOOL_HISTORY_TOGGLE_KEY, Transcript},
};

const FRONTEND_CREDENTIAL_SENTINEL: &str = "••••••••";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptCompletion<'a> {
    Command(&'static CommandSpec),
    Skill(&'a SkillView),
}

fn frontend_picker_authentication(
    authentication: &ProviderAuthenticationView,
) -> ProviderAuthentication {
    match authentication {
        ProviderAuthenticationView::Starting => ProviderAuthentication::Starting,
        ProviderAuthenticationView::ApiKeyRequired { .. } => ProviderAuthentication::ApiKeyInput {
            value: String::new(),
            focused: false,
        },
        ProviderAuthenticationView::Challenge {
            verification_url,
            user_code,
        } => ProviderAuthentication::Challenge {
            verification_url: verification_url.clone(),
            user_code: user_code.clone(),
        },
    }
}

fn settings_state(settings: &nakode_protocol::SettingsView) -> SettingsState {
    SettingsState {
        query: String::new(),
        selected: 0,
        view: SettingsView::Menu,
        web: WebSettingsState {
            backend: match settings.web.backend.as_str() {
                "agent-browser" => WebBackend::AgentBrowser,
                "firecrawl" => WebBackend::Firecrawl,
                _ => WebBackend::Disabled,
            },
            firecrawl_api_key: if settings.web.credential_configured {
                FRONTEND_CREDENTIAL_SENTINEL.to_owned()
            } else {
                String::new()
            },
        },
        vision: VisionSettingsState {
            model: settings.vision.model_id.as_ref().map(ToString::to_string),
        },
        memory: MemorySettingsState {
            backend: if settings.memory.backend == "mnemosyne" {
                MemoryBackend::Mnemosyne
            } else {
                MemoryBackend::Disabled
            },
            executable: settings.memory.executable.clone(),
            global_bank: settings.memory.global_bank.clone(),
            data_directory: settings.memory.data_directory.clone(),
            configured: settings.memory.configured,
            available: settings.memory.available,
        },
        terminal_images: settings.terminal_images,
        addon_field: 0,
        agent_browser_status: match &settings.web.agent_browser {
            AgentBrowserView::Checking => AgentBrowserStatus::Checking,
            AgentBrowserView::Available { version } => {
                AgentBrowserStatus::Available(version.clone())
            }
            AgentBrowserView::Unavailable => AgentBrowserStatus::Unavailable,
        },
    }
}

fn valid_bank_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 64
        && name
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '-' | '_'))
}

fn valid_model_id(model: &str) -> Option<ModelId> {
    model
        .split_once('/')
        .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        .map(|_| ModelId::from(model.to_owned()))
}

fn attachment_label(attachment: &PromptAttachment) -> &str {
    match attachment {
        PromptAttachment::Artifact { label, .. }
        | PromptAttachment::LocalFile { label, .. }
        | PromptAttachment::InlineImage { label, .. } => label,
    }
}

#[must_use]
pub const fn terminal_image_mode_label(mode: TerminalImageModeView) -> &'static str {
    match mode {
        TerminalImageModeView::Auto => "Automatic",
        TerminalImageModeView::On => "On",
        TerminalImageModeView::Off => "Off",
    }
}

#[must_use]
pub const fn connection_label(connection: &nakode_protocol::ConnectionView) -> &'static str {
    match connection {
        nakode_protocol::ConnectionView::Disabled => "disabled",
        nakode_protocol::ConnectionView::Starting => "connecting",
        nakode_protocol::ConnectionView::Ready => "ready",
        nakode_protocol::ConnectionView::Failed { .. } => "failed",
        nakode_protocol::ConnectionView::Disconnected { .. } => "disconnected",
    }
}

#[must_use]
pub fn provider_dashboard_url(provider: &ProviderView) -> Option<&str> {
    match provider.authentication.as_ref()? {
        ProviderAuthenticationView::ApiKeyRequired { dashboard_url, .. } => Some(dashboard_url),
        ProviderAuthenticationView::Starting | ProviderAuthenticationView::Challenge { .. } => None,
    }
}

#[must_use]
pub fn model_supports_options(model: &ModelView) -> bool {
    model.configuration.reasoning_is_configurable() || model.configuration.fast_mode_configurable
}

#[must_use]
pub const fn model_supports_fast_mode(model: &ModelView) -> bool {
    model.configuration.fast_mode_configurable
}

#[must_use]
pub const fn provider_capability_rows() -> [(&'static str, ProviderCapability); 11] {
    [
        ("Resume", ProviderCapability::Resume),
        ("Steering", ProviderCapability::Steering),
        ("Interruption", ProviderCapability::Interruption),
        ("Model catalog", ProviderCapability::ModelCatalog),
        (
            "Models need session",
            ProviderCapability::ModelsRequireSession,
        ),
        (
            "Session model config",
            ProviderCapability::SessionModelConfiguration,
        ),
        ("Context compression", ProviderCapability::ContextCompaction),
        ("Approvals", ProviderCapability::Approvals),
        ("Native tools", ProviderCapability::NativeTools),
        ("MCP", ProviderCapability::Mcp),
        ("Close session", ProviderCapability::CloseSession),
    ]
}

fn contains(point: ScreenPoint, top_left: ScreenPoint, bottom_right: ScreenPoint) -> bool {
    point.column >= top_left.column
        && point.column < bottom_right.column
        && point.row >= top_left.row
        && point.row < bottom_right.row
}

fn offset_index(index: usize, length: usize, delta: isize) -> usize {
    debug_assert!(length > 0);
    let distance = delta.unsigned_abs() % length;
    if delta.is_negative() {
        (index + length - distance) % length
    } else {
        (index + distance) % length
    }
}

#[cfg(test)]
mod tests {
    use nakode_protocol::{
        AgentBrowserView, ArtifactId, InteractionId, InteractionKind, InteractionOptionView,
        InteractionStatus, MemorySettingsView, ModelConfigurationView, ModelId, ModelView,
        PromptAttachment, PromptId, ProviderId, QueueItemView, RecoverablePromptView,
        SessionActivity, SessionId, SessionView, SettingsView, TerminalImageModeView,
        TranscriptPage, VisionSettingsView, WebSettingsView, WorkspaceId,
    };

    use super::{AgentPendingOptions, ModelSelectionScope, TuiState};

    /// The step cycles the CHOSEN model's own levels, with the model's default as a real position:
    /// an archetype has to be able to go back to "whatever the model does", because that is what
    /// every definition written before the field existed says.
    #[test]
    fn an_agent_effort_cycles_the_models_levels_and_its_default() {
        let mut pending = AgentPendingOptions {
            reasoning_efforts: vec!["low".to_owned(), "high".to_owned()],
            fast_mode_configurable: false,
            options: nakode_protocol::ModelOptions::default(),
            selected: 0,
        };
        assert_eq!(pending.options.reasoning_effort, None);
        pending.step_effort(1);
        assert_eq!(pending.options.reasoning_effort.as_deref(), Some("low"));
        pending.step_effort(1);
        assert_eq!(pending.options.reasoning_effort.as_deref(), Some("high"));
        // Round the end, back to the default it started on.
        pending.step_effort(1);
        assert_eq!(pending.options.reasoning_effort, None);
        pending.step_effort(-1);
        assert_eq!(pending.options.reasoning_effort.as_deref(), Some("high"));
    }

    /// A model that reports no levels has nothing to cycle, and its row is not drawn either.
    #[test]
    fn a_model_with_no_levels_has_no_effort_to_step() {
        let mut pending = AgentPendingOptions {
            reasoning_efforts: Vec::new(),
            fast_mode_configurable: true,
            options: nakode_protocol::ModelOptions::default(),
            selected: 0,
        };
        assert_eq!(pending.row_count(), 1);
        assert!(pending.on_fast_mode_row());
        pending.step_effort(1);
        assert_eq!(pending.options.reasoning_effort, None);
    }

    #[test]
    fn snapshots_replace_semantic_state_without_replacing_local_presentation() {
        let mut first = bootstrap();
        first.active_session = Some(session("one", "Waiting"));
        let mut state = TuiState::from_bootstrap(&first, 100);
        state.client.editor.set_text("unsent draft");
        state.client.scroll_from_bottom = 7;
        state.questions.front_mut().expect("question").selected = 1;

        let mut second = first.clone();
        let session = second.active_session.as_mut().expect("session");
        session.status_message = "Working".to_owned();
        session.queue.push(QueueItemView {
            id: "prompt-2".into(),
            summary: "second".to_owned(),
            text: "second".to_owned(),
            attachment_count: 0,
        });
        state.install_bootstrap(&second);

        assert_eq!(state.client.editor.text(), "unsent draft");
        assert_eq!(state.client.scroll_from_bottom, 7);
        assert_eq!(state.questions.front().expect("question").selected, 1);
        assert_eq!(state.status_message, "Working");
        assert_eq!(state.queue.len(), 2);
    }

    #[test]
    fn semantic_prompt_recovery_restores_one_blank_local_draft() {
        let mut view = bootstrap();
        let mut active = session("one", "Prompt failed.");
        active.recoverable_prompt = Some(RecoverablePromptView {
            id: PromptId::from("prompt-1"),
            text: "Inspect [diagram.png]".to_owned(),
            attachments: vec![PromptAttachment::Artifact {
                artifact_id: ArtifactId::from("artifact-1"),
                label: "diagram.png".to_owned(),
            }],
        });
        view.active_session = Some(active);

        let mut state = TuiState::from_bootstrap(&view, 100);

        assert_eq!(state.client.editor.text(), "Inspect [diagram.png]");
        assert_eq!(state.client.draft_attachments.len(), 1);
        assert_eq!(state.status_message, "Draft restored.");

        state.client.editor.set_text("replacement");
        state.install_bootstrap(&view);
        assert_eq!(state.client.editor.text(), "replacement");
    }

    #[test]
    fn semantic_prompt_recovery_waits_for_a_blank_editor() {
        let mut without_recovery = bootstrap();
        without_recovery.active_session = Some(session("one", "Ready"));
        let mut state = TuiState::from_bootstrap(&without_recovery, 100);
        state.client.editor.set_text("keep this");

        let mut with_recovery = without_recovery.clone();
        with_recovery
            .active_session
            .as_mut()
            .expect("session")
            .recoverable_prompt = Some(RecoverablePromptView {
            id: PromptId::from("prompt-2"),
            text: "Retry".to_owned(),
            attachments: vec![PromptAttachment::LocalFile {
                label: "notes.txt".to_owned(),
                path: "notes.txt".to_owned(),
            }],
        });
        state.install_bootstrap(&with_recovery);
        assert_eq!(state.client.editor.text(), "keep this");

        state.client.editor.clear();
        state.install_bootstrap(&with_recovery);
        assert_eq!(state.client.editor.text(), "Retry [notes.txt]");
        assert_eq!(state.client.draft_attachments.len(), 1);
    }

    #[test]
    fn vision_picker_uses_protocol_capability_metadata() {
        let mut view = bootstrap();
        view.models = vec![
            model("provider/text", false),
            model("provider/vision", true),
        ];
        let mut state = TuiState::from_bootstrap(&view, 100);

        state.open_model_picker(ModelSelectionScope::Vision);

        assert_eq!(
            state
                .filtered_models()
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            ["provider/vision"]
        );
    }

    fn bootstrap() -> nakode_protocol::BootstrapView {
        nakode_protocol::BootstrapView {
            workspace_id: WorkspaceId::from("workspace"),
            workspace_path: "/workspace".to_owned(),
            providers: Vec::new(),
            models: Vec::new(),
            agents: Vec::new(),
            skills: Vec::new(),
            settings: SettingsView {
                web: WebSettingsView {
                    backend: "disabled".to_owned(),
                    credential_configured: false,
                    agent_browser: AgentBrowserView::Unavailable,
                },
                memory: MemorySettingsView {
                    backend: "disabled".to_owned(),
                    executable: String::new(),
                    global_bank: String::new(),
                    data_directory: String::new(),
                    configured: false,
                    available: false,
                },
                vision: VisionSettingsView { model_id: None },
                terminal_images: TerminalImageModeView::Auto,
            },
            sessions: Vec::new(),
            active_session: None,
        }
    }

    fn session(id: &str, status: &str) -> SessionView {
        SessionView {
            id: SessionId::from(id),
            revision: 1,
            workspace_id: WorkspaceId::from("workspace"),
            title: "Session".to_owned(),
            status_message: status.to_owned(),
            diagnostic_count: 0,
            activity: SessionActivity::Idle,
            selected_provider_id: None,
            selected_model_id: None,
            active_agent_session: None,
            active_turn: None,
            context_usage: None,
            transcript: TranscriptPage {
                entries: Vec::new(),
                has_earlier: false,
                stream_active: false,
                stream_label: "Nakode".to_owned(),
            },
            recoverable_prompt: None,
            queue: vec![QueueItemView {
                id: "prompt-1".into(),
                summary: "first".to_owned(),
                text: "first".to_owned(),
                attachment_count: 0,
            }],
            interactions: vec![question()],
            todos: Vec::new(),
            runs: Vec::new(),
            runs_has_earlier: false,
            notices: Vec::new(),
            external_tool_calls: Vec::new(),
        }
    }

    fn question() -> nakode_protocol::InteractionView {
        nakode_protocol::InteractionView {
            id: InteractionId::from("question-1"),
            revision: 1,
            kind: InteractionKind::Question,
            status: InteractionStatus::Pending,
            title: "Choice".to_owned(),
            detail: "Choose one".to_owned(),
            options: vec![
                InteractionOptionView {
                    id: "one".to_owned(),
                    label: "One".to_owned(),
                    description: None,
                    recommended: true,
                },
                InteractionOptionView {
                    id: "two".to_owned(),
                    label: "Two".to_owned(),
                    description: None,
                    recommended: false,
                },
            ],
            multiple: false,
        }
    }

    fn model(id: &str, vision_eligible: bool) -> ModelView {
        let (provider, model_slug) = id.split_once('/').expect("qualified model");
        ModelView {
            id: ModelId::from(id),
            provider_id: ProviderId::from(provider),
            model_slug: model_slug.to_owned(),
            display_name: model_slug.to_owned(),
            is_default: false,
            reasoning_effort: None,
            fast_mode: false,
            configuration: ModelConfigurationView {
                vision_eligible,
                ..ModelConfigurationView::default()
            },
        }
    }
}

impl<'a> PromptCompletion<'a> {
    #[must_use]
    pub fn replacement(self) -> String {
        match self {
            Self::Command(command) => command.invocation.to_owned(),
            Self::Skill(skill) => format!("/skill:{}", skill.name),
        }
    }

    #[must_use]
    pub fn description(self) -> &'a str {
        match self {
            Self::Command(command) => command.description,
            Self::Skill(skill) => skill.description.as_str(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelPickerStage {
    Models,
    Options,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelSelectionScope {
    Default,
    Session,
    Vision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelPicker {
    pub filter: String,
    pub selected: usize,
    pub scope: ModelSelectionScope,
    pub stage: ModelPickerStage,
    pub option_selected: usize,
    pub options: ModelOptions,
    pub options_fast_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionPrompt {
    pub interaction: InteractionView,
    pub selected: usize,
    pub selections: Vec<bool>,
}

#[derive(Clone, Debug)]
pub struct SessionPicker {
    pub sessions: Vec<SessionSummary>,
    pub selected: usize,
    pub loading: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderPicker {
    pub providers: Vec<ProviderView>,
    pub selected: usize,
    pub loading: bool,
    pub showing_details: bool,
    pub authentication: Option<ProviderAuthentication>,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProviderAuthentication {
    Starting,
    ApiKeyInput {
        value: String,
        focused: bool,
    },
    Challenge {
        verification_url: String,
        user_code: String,
    },
}

impl std::fmt::Debug for ProviderAuthentication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => formatter.write_str("Starting"),
            Self::ApiKeyInput { value, focused } => formatter
                .debug_struct("ApiKeyInput")
                .field("characters", &value.chars().count())
                .field("focused", focused)
                .finish_non_exhaustive(),
            Self::Challenge {
                verification_url,
                user_code,
            } => formatter
                .debug_struct("Challenge")
                .field("verification_url", verification_url)
                .field("user_code", user_code)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentEditorField {
    Slug,
    Description,
    SystemPrompt,
    FirstMessage,
    Model,
    FallbackModels,
}

impl AgentEditorField {
    pub const ALL: [Self; 6] = [
        Self::Slug,
        Self::Description,
        Self::SystemPrompt,
        Self::FirstMessage,
        Self::Model,
        Self::FallbackModels,
    ];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Slug => "Slug",
            Self::Description => "Description",
            Self::SystemPrompt => "System prompt (optional)",
            Self::FirstMessage => "First message (optional)",
            Self::Model => "Model",
            Self::FallbackModels => "Fallbacks",
        }
    }
}

/// The options step of the agent editor: what the model just chosen can be given, and what it is.
///
/// It exists only between choosing a model and applying it, and that is what ties a level to a
/// model: the levels on offer are the chosen model's own (`ModelConfigurationView`), and there is no
/// other way in. A model that takes neither a level nor fast mode produces no step at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentPendingOptions {
    /// The chosen model's own levels, in its own order. Empty for a model that takes none.
    pub reasoning_efforts: Vec<String>,
    pub fast_mode_configurable: bool,
    pub options: ModelOptions,
    pub selected: usize,
}

impl AgentPendingOptions {
    /// How many rows the step draws — the effort row is shown first, when there is one.
    #[must_use]
    pub fn row_count(&self) -> usize {
        usize::from(!self.reasoning_efforts.is_empty()) + usize::from(self.fast_mode_configurable)
    }

    #[must_use]
    pub fn on_fast_mode_row(&self) -> bool {
        self.fast_mode_configurable && (self.reasoning_efforts.is_empty() || self.selected == 1)
    }

    /// Steps the level, with the model's DEFAULT as the first position rather than a level.
    ///
    /// Default has to be reachable: a definition with no level means the model's own, which is what
    /// every definition written before this field existed means, and the editor cannot be a one-way
    /// door out of it.
    fn step_effort(&mut self, delta: isize) {
        if self.reasoning_efforts.is_empty() {
            return;
        }
        let current = self
            .options
            .reasoning_effort
            .as_deref()
            .map_or(0, |effort| {
                self.reasoning_efforts
                    .iter()
                    .position(|candidate| candidate == effort)
                    .map_or(0, |index| index.saturating_add(1))
            });
        let next = offset_index(
            current,
            self.reasoning_efforts.len().saturating_add(1),
            delta,
        );
        self.options.reasoning_effort = match next.checked_sub(1) {
            None => None,
            Some(index) => self.reasoning_efforts.get(index).cloned(),
        };
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentModelOption {
    Inherit,
    Model(ModelView),
}

impl AgentModelOption {
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Inherit => "Inherit parent model".to_owned(),
            Self::Model(model) => model.display_name.clone(),
        }
    }

    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::Inherit => "uses the parent session model".to_owned(),
            Self::Model(model) => model.id.to_string(),
        }
    }

    #[must_use]
    pub fn search_text(&self) -> String {
        format!("{} {}", self.label(), self.detail())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentEditor {
    pub original_slug: Option<String>,
    pub field: AgentEditorField,
    pub slug: String,
    pub description: String,
    pub system_prompt: String,
    pub first_message: String,
    pub model: String,
    pub fallback_models: String,
    pub fast_mode: bool,
    /// The level this archetype runs at, or `None` for the chosen model's own default.
    pub reasoning_effort: Option<String>,
    pub pending_options: Option<AgentPendingOptions>,
    pub model_dropdown: Option<SearchableDropdown<AgentModelOption>>,
}

impl AgentEditor {
    fn new() -> Self {
        Self {
            original_slug: None,
            field: AgentEditorField::Slug,
            slug: String::new(),
            description: String::new(),
            system_prompt: String::new(),
            first_message: String::new(),
            model: String::new(),
            fallback_models: String::new(),
            fast_mode: false,
            reasoning_effort: None,
            pending_options: None,
            model_dropdown: None,
        }
    }

    fn from_definition(definition: &AgentDefinitionView) -> Self {
        Self {
            original_slug: Some(definition.slug.clone()),
            field: AgentEditorField::Slug,
            slug: definition.slug.clone(),
            description: definition.description.clone(),
            system_prompt: definition.system_prompt.clone(),
            first_message: definition.first_message.clone(),
            model: definition
                .model_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            fallback_models: definition
                .fallback_models
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            fast_mode: definition.fast_mode,
            reasoning_effort: definition.reasoning_effort.clone(),
            pending_options: None,
            model_dropdown: None,
        }
    }

    #[must_use]
    pub fn definition_input(&self) -> Option<AgentDefinitionInput> {
        let slug = self.slug.trim();
        let description = self.description.trim();
        let system_prompt = self.system_prompt.trim();
        let first_message = self.first_message.trim();
        if slug.is_empty()
            || !slug.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
            })
            || description.is_empty()
        {
            return None;
        }
        let model = self.model.trim();
        let model = if model.is_empty() {
            None
        } else {
            Some(valid_model_id(model)?)
        };
        let fallback_models = self
            .fallback_models
            .split(',')
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(valid_model_id)
            .collect::<Option<Vec<_>>>()?;
        Some(AgentDefinitionInput {
            slug: slug.to_owned(),
            description: description.to_owned(),
            system_prompt: system_prompt.to_owned(),
            first_message: first_message.to_owned(),
            // A level travels with a model and never without one, so an editor left on "inherit the
            // parent model" sends none — the run gets the parent's model at its own default level.
            reasoning_effort: model.as_ref().and(self.reasoning_effort.clone()),
            model,
            fallback_models,
            fast_mode: self.fast_mode,
        })
    }
}

#[derive(Clone, Debug)]
pub struct AgentPicker {
    pub agents: Vec<AgentDefinitionView>,
    pub selected: usize,
    pub editor: Option<AgentEditor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsSection {
    General,
    Agents,
    Models,
    Addons,
}

impl SettingsSection {
    pub const ALL: [Self; 4] = [Self::General, Self::Agents, Self::Models, Self::Addons];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Agents => "Agents",
            Self::Models => "Models",
            Self::Addons => "Add-ons",
        }
    }

    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::General => "Providers and connections",
            Self::Agents => "Delegated agent archetypes",
            Self::Models => "Default models",
            Self::Addons => "Optional tools and web browsing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsView {
    Menu,
    Addons,
    WebBrowsing,
    Vision,
    Memory,
    TerminalImages,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebBackend {
    Disabled,
    AgentBrowser,
    Firecrawl,
}

impl WebBackend {
    pub const ALL: [Self; 3] = [Self::Disabled, Self::AgentBrowser, Self::Firecrawl];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::AgentBrowser => "agent-browser",
            Self::Firecrawl => "Firecrawl",
        }
    }

    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::AgentBrowser => "agent-browser",
            Self::Firecrawl => "firecrawl",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryBackend {
    Disabled,
    Mnemosyne,
}

impl MemoryBackend {
    pub const ALL: [Self; 2] = [Self::Disabled, Self::Mnemosyne];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::Mnemosyne => "Mnemosyne",
        }
    }

    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Mnemosyne => "mnemosyne",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentBrowserStatus {
    Checking,
    Available(String),
    Unavailable,
}

#[derive(Clone, Eq, PartialEq)]
pub struct WebSettingsState {
    pub backend: WebBackend,
    pub firecrawl_api_key: String,
}

impl std::fmt::Debug for WebSettingsState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSettingsState")
            .field("backend", &self.backend)
            .field(
                "firecrawl_api_key",
                &if self.firecrawl_api_key.is_empty() {
                    "not set"
                } else {
                    "configured"
                },
            )
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemorySettingsState {
    pub backend: MemoryBackend,
    pub executable: String,
    pub global_bank: String,
    pub data_directory: String,
    pub configured: bool,
    pub available: bool,
}

impl MemorySettingsState {
    #[must_use]
    pub fn configured(&self) -> bool {
        self.backend == MemoryBackend::Mnemosyne
            && !self.executable.trim().is_empty()
            && valid_bank_name(self.global_bank.trim())
    }

    #[must_use]
    pub const fn available(&self) -> bool {
        self.available
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisionSettingsState {
    pub model: Option<String>,
}

impl VisionSettingsState {
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.model.as_ref().is_some_and(|model| !model.is_empty())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsState {
    pub query: String,
    pub selected: usize,
    pub view: SettingsView,
    pub web: WebSettingsState,
    pub vision: VisionSettingsState,
    pub memory: MemorySettingsState,
    pub terminal_images: TerminalImageModeView,
    pub addon_field: usize,
    pub agent_browser_status: AgentBrowserStatus,
}

impl SettingsState {
    #[must_use]
    pub fn filtered_sections(&self) -> Vec<SettingsSection> {
        let query = self.query.to_ascii_lowercase();
        SettingsSection::ALL
            .into_iter()
            .filter(|section| {
                query.is_empty()
                    || section.label().to_ascii_lowercase().contains(&query)
                    || section.description().to_ascii_lowercase().contains(&query)
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct SubagentHitRegion {
    run_id: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Debug)]
struct ToolToggleHitRegion {
    key: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Debug)]
struct OAuthLinkHitRegion {
    url: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Copy, Debug)]
struct ApiKeyInputHitRegion {
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Debug)]
enum MenuSnapshot {
    Settings(SettingsState),
}

#[derive(Clone, Debug, Default)]
pub struct ClientPresentationState {
    pub editor: EditorState,
    draft_attachments: Vec<PromptAttachment>,
    pub queue_selection: Option<usize>,
    pub model_picker: Option<ModelPicker>,
    pub session_picker: Option<SessionPicker>,
    pub provider_picker: Option<ProviderPicker>,
    pub agent_picker: Option<AgentPicker>,
    pub settings: Option<SettingsState>,
    menu_history: Vec<MenuSnapshot>,
    command_completion_selection: usize,
    pub show_help: bool,
    pub text_selection: Option<TextSelection>,
    pub scroll_from_bottom: usize,
    subagent_scroll_from_bottom: HashMap<String, usize>,
    pub subagent_modal: Option<String>,
    screen_snapshot: Option<ScreenSnapshot>,
    pending_clipboard: Option<String>,
    subagent_hit_regions: Vec<SubagentHitRegion>,
    tool_toggle_hit_regions: Vec<ToolToggleHitRegion>,
    oauth_link_hit_region: Option<OAuthLinkHitRegion>,
    api_key_input_hit_region: Option<ApiKeyInputHitRegion>,
}

#[derive(Clone, Debug)]
pub(crate) struct ComposerDraft {
    pub(crate) editor: EditorState,
    pub(crate) attachments: Vec<PromptAttachment>,
}

impl ClientPresentationState {
    #[must_use]
    pub(crate) fn composer_draft(&self) -> ComposerDraft {
        ComposerDraft {
            editor: self.editor.clone(),
            attachments: self.draft_attachments.clone(),
        }
    }

    pub(crate) fn restore_composer(&mut self, draft: ComposerDraft) {
        self.editor = draft.editor;
        self.draft_attachments = draft.attachments;
    }
}

/// State owned by one terminal client.
///
/// Canonical fields are projections of the versioned service protocol. The
/// remaining fields are terminal-local editor, modal, scroll, and selection
/// state. This type cannot execute providers, tools, persistence, or runtime
/// effects.
#[derive(Clone, Debug)]
pub struct TuiState {
    pub client: ClientPresentationState,
    pub workspace_id: WorkspaceId,
    pub workspace: String,
    pub session_id: Option<SessionId>,
    pub active_turn: Option<TurnView>,
    pub activity: SessionActivity,
    pub context_usage: Option<nakode_protocol::ContextUsageView>,
    pub transcript: Transcript,
    pub queue: Vec<QueueItemView>,
    pub models: Vec<ModelView>,
    pub selected_model: Option<ModelId>,
    pub approvals: VecDeque<InteractionView>,
    pub questions: VecDeque<QuestionPrompt>,
    pub todo_phases: Vec<TodoPhaseView>,
    pub status_message: String,
    pub diagnostic_count: u64,
    pub subagents: Vec<RunView>,
    providers: Vec<ProviderView>,
    sessions: Vec<SessionSummary>,
    agents: Vec<AgentDefinitionView>,
    skills: Vec<SkillView>,
    settings: nakode_protocol::SettingsView,
    subagent_chats: HashMap<String, Transcript>,
    seen_recoveries: VecDeque<(SessionId, PromptId)>,
    transcript_limit: usize,
    image_previews_enabled: bool,
}

impl TuiState {
    #[must_use]
    pub fn from_bootstrap(view: &BootstrapView, scrollback: usize) -> Self {
        let mut state = Self {
            client: ClientPresentationState::default(),
            workspace_id: view.workspace_id.clone(),
            workspace: view.workspace_path.clone(),
            session_id: None,
            active_turn: None,
            activity: SessionActivity::Idle,
            context_usage: None,
            transcript: Transcript::new(scrollback),
            queue: Vec::new(),
            models: Vec::new(),
            selected_model: None,
            approvals: VecDeque::new(),
            questions: VecDeque::new(),
            todo_phases: Vec::new(),
            status_message: String::new(),
            diagnostic_count: 0,
            subagents: Vec::new(),
            providers: Vec::new(),
            sessions: Vec::new(),
            agents: Vec::new(),
            skills: Vec::new(),
            settings: view.settings.clone(),
            subagent_chats: HashMap::new(),
            seen_recoveries: VecDeque::new(),
            transcript_limit: scrollback,
            image_previews_enabled: false,
        };
        state.install_bootstrap(view);
        state
    }

    /// Replaces server-owned projections while preserving terminal-local state.
    pub fn install_bootstrap(&mut self, view: &BootstrapView) {
        self.workspace_id.clone_from(&view.workspace_id);
        self.workspace.clone_from(&view.workspace_path);
        self.models.clone_from(&view.models);
        self.providers.clone_from(&view.providers);
        self.sessions.clone_from(&view.sessions);
        self.agents.clone_from(&view.agents);
        self.skills.clone_from(&view.skills);
        self.settings.clone_from(&view.settings);
        self.install_session(view.active_session.as_ref());
        self.synchronize_pickers();
    }

    /// Installs one complete semantic session snapshot.
    pub fn install_session(&mut self, session: Option<&SessionView>) {
        let Some(session) = session else {
            self.session_id = None;
            self.active_turn = None;
            self.activity = SessionActivity::Idle;
            self.context_usage = None;
            self.selected_model = None;
            self.transcript.clear();
            self.queue.clear();
            self.approvals.clear();
            self.questions.clear();
            self.todo_phases.clear();
            self.subagents.clear();
            self.subagent_chats.clear();
            self.status_message.clear();
            self.diagnostic_count = 0;
            self.client.queue_selection = None;
            self.client.subagent_modal = None;
            self.client.subagent_scroll_from_bottom.clear();
            return;
        };

        self.session_id = Some(session.id.clone());
        self.active_turn.clone_from(&session.active_turn);
        self.activity = session.activity;
        self.context_usage = session.context_usage;
        self.selected_model.clone_from(&session.selected_model_id);
        self.transcript.install_projection(&session.transcript);
        self.queue.clone_from(&session.queue);
        self.client.queue_selection = self
            .client
            .queue_selection
            .filter(|selection| *selection < self.queue.len());
        self.install_interactions(&session.interactions);
        self.todo_phases.clone_from(&session.todos);
        self.install_runs(&session.runs);
        self.status_message.clone_from(&session.status_message);
        self.diagnostic_count = session.diagnostic_count;
        self.restore_recoverable_prompt(session);
    }

    fn restore_recoverable_prompt(&mut self, session: &SessionView) {
        let Some(recovery) = session.recoverable_prompt.as_ref() else {
            return;
        };
        let recovery_key = (session.id.clone(), recovery.id.clone());
        if self.seen_recoveries.contains(&recovery_key) || !self.client.editor.is_blank() {
            return;
        }

        let mut editor = EditorState::default();
        editor.set_text(&recovery.text);
        let mut available_tokens = HashMap::<String, usize>::new();
        for attachment in &recovery.attachments {
            let token = format!("[{}]", attachment_label(attachment));
            available_tokens
                .entry(token.clone())
                .or_insert_with(|| recovery.text.matches(&token).count());
        }
        for attachment in &recovery.attachments {
            let token = format!("[{}]", attachment_label(attachment));
            let remaining = available_tokens.entry(token.clone()).or_default();
            if *remaining > 0 {
                *remaining -= 1;
                continue;
            }
            if !editor.is_blank() {
                editor.insert_char(' ');
            }
            editor.insert_str(&token);
        }
        self.client.restore_composer(ComposerDraft {
            editor,
            attachments: recovery.attachments.clone(),
        });
        if self.seen_recoveries.len() >= 128 {
            self.seen_recoveries.pop_front();
        }
        self.seen_recoveries.push_back(recovery_key);
        "Draft restored.".clone_into(&mut self.status_message);
    }

    pub(crate) fn install_session_entry_artifacts(
        &mut self,
        session_id: &SessionId,
        entry_id: &EntryId,
        artifacts: &[ArtifactView],
    ) -> bool {
        if self.session_id.as_ref() != Some(session_id) {
            return false;
        }
        self.transcript
            .install_artifacts(entry_id.to_string(), artifacts)
    }

    pub(crate) fn install_run_entry_artifacts(
        &mut self,
        session_id: &SessionId,
        run_id: &RunId,
        entry_id: &EntryId,
        artifacts: &[ArtifactView],
    ) -> bool {
        if self.session_id.as_ref() != Some(session_id) {
            return false;
        }
        let Some(transcript) = self.subagent_chats.get_mut(run_id.as_str()) else {
            return false;
        };
        transcript.install_artifacts(entry_id.to_string(), artifacts)
    }

    fn install_interactions(&mut self, interactions: &[InteractionView]) {
        let previous = self
            .questions
            .iter()
            .map(|question| {
                (
                    question.interaction.id.clone(),
                    (question.selected, question.selections.clone()),
                )
            })
            .collect::<HashMap<_, _>>();
        self.approvals.clear();
        self.questions.clear();
        for interaction in interactions
            .iter()
            .filter(|interaction| interaction.status == InteractionStatus::Pending)
        {
            match interaction.kind {
                InteractionKind::Approval => self.approvals.push_back(interaction.clone()),
                InteractionKind::Question => {
                    let (selected, selections) =
                        previous.get(&interaction.id).cloned().unwrap_or_else(|| {
                            (
                                interaction
                                    .options
                                    .iter()
                                    .position(|option| option.recommended)
                                    .unwrap_or_default(),
                                vec![false; interaction.options.len()],
                            )
                        });
                    self.questions.push_back(QuestionPrompt {
                        interaction: interaction.clone(),
                        selected: selected.min(interaction.options.len().saturating_sub(1)),
                        selections,
                    });
                }
            }
        }
    }

    fn install_runs(&mut self, runs: &[RunView]) {
        let mut previous = std::mem::take(&mut self.subagent_chats);
        self.subagents = runs.to_vec();
        self.subagent_chats = runs
            .iter()
            .map(|run| {
                let mut transcript = previous
                    .remove(run.id.as_str())
                    .unwrap_or_else(|| Transcript::new(self.transcript_limit));
                transcript.install_projection(&run.transcript);
                transcript.set_image_previews_enabled(self.image_previews_enabled);
                (run.id.to_string(), transcript)
            })
            .collect();
        self.client
            .subagent_scroll_from_bottom
            .retain(|run_id, _| self.subagent_chats.contains_key(run_id));
        if self
            .client
            .subagent_modal
            .as_ref()
            .is_some_and(|run_id| !self.subagent_chats.contains_key(run_id))
        {
            self.client.subagent_modal = None;
        }
        for run in runs {
            let running = matches!(run.status, RunStatus::Starting | RunStatus::Working);
            self.transcript.upsert(
                format!("subagent:{}", run.id),
                EntryKind::System,
                if running { "pending" } else { "completed" },
                run.objective.clone(),
                if running {
                    EntryStatus::Running
                } else {
                    EntryStatus::Complete
                },
            );
        }
    }

    fn synchronize_pickers(&mut self) {
        if let Some(picker) = &mut self.client.session_picker {
            let selected_id = picker
                .sessions
                .get(picker.selected)
                .map(|session| session.id.clone());
            picker.sessions.clone_from(&self.sessions);
            picker.selected = selected_id
                .and_then(|id| picker.sessions.iter().position(|session| session.id == id))
                .unwrap_or_default()
                .min(picker.sessions.len().saturating_sub(1));
            picker.loading = false;
        }

        if let Some(picker) = &mut self.client.provider_picker {
            let selected_id = picker
                .providers
                .get(picker.selected)
                .map(|provider| provider.id.clone());
            picker.providers.clone_from(&self.providers);
            picker.selected = selected_id
                .and_then(|id| {
                    picker
                        .providers
                        .iter()
                        .position(|provider| provider.id == id)
                })
                .unwrap_or_default()
                .min(picker.providers.len().saturating_sub(1));
            picker.loading = false;
            if !matches!(
                picker.authentication,
                Some(ProviderAuthentication::ApiKeyInput {
                    ref value,
                    focused: true,
                }) if !value.is_empty()
            ) {
                picker.authentication = picker
                    .providers
                    .get(picker.selected)
                    .and_then(|provider| provider.authentication.as_ref())
                    .map(frontend_picker_authentication);
            }
        }

        if let Some(picker) = &mut self.client.agent_picker {
            let selected_slug = picker
                .agents
                .get(picker.selected)
                .map(|agent| agent.slug.clone());
            picker.agents.clone_from(&self.agents);
            picker.selected = selected_slug
                .and_then(|slug| picker.agents.iter().position(|agent| agent.slug == slug))
                .unwrap_or_default()
                .min(picker.agents.len().saturating_sub(1));
        }
    }

    #[must_use]
    pub fn providers(&self) -> &[ProviderView] {
        &self.providers
    }

    #[must_use]
    pub fn sessions(&self) -> &[SessionSummary] {
        &self.sessions
    }

    #[must_use]
    pub fn agents(&self) -> &[AgentDefinitionView] {
        &self.agents
    }

    #[must_use]
    pub const fn terminal_image_mode(&self) -> TerminalImageMode {
        match self.settings.terminal_images {
            TerminalImageModeView::Auto => TerminalImageMode::Auto,
            TerminalImageModeView::On => TerminalImageMode::On,
            TerminalImageModeView::Off => TerminalImageMode::Off,
        }
    }

    pub fn set_image_previews_enabled(&mut self, enabled: bool) {
        self.image_previews_enabled = enabled;
        self.transcript.set_image_previews_enabled(enabled);
        for transcript in self.subagent_chats.values_mut() {
            transcript.set_image_previews_enabled(enabled);
        }
    }

    #[must_use]
    pub fn selected_model_display_name(&self) -> Option<String> {
        let selected = self.selected_model.as_ref()?;
        self.models
            .iter()
            .find(|model| &model.id == selected)
            .map(|model| model.display_name.clone())
            .or_else(|| Some(selected.to_string()))
    }

    #[must_use]
    pub const fn model_uses_fast_mode(&self, model: &ModelView) -> bool {
        model.fast_mode
    }

    #[must_use]
    pub fn selected_model_uses_fast_mode(&self) -> bool {
        self.selected_model.as_ref().is_some_and(|selected| {
            self.models
                .iter()
                .find(|model| &model.id == selected)
                .is_some_and(|model| model.fast_mode)
        })
    }

    #[must_use]
    pub fn filtered_models(&self) -> Vec<&ModelView> {
        let picker = self.client.model_picker.as_ref();
        let filter = picker
            .map(|picker| picker.filter.to_ascii_lowercase())
            .unwrap_or_default();
        let scope = picker.map(|picker| picker.scope);
        self.models
            .iter()
            .filter(|model| {
                scope != Some(ModelSelectionScope::Vision) || model.configuration.vision_eligible
            })
            .filter(|model| {
                filter.is_empty()
                    || model.display_name.to_ascii_lowercase().contains(&filter)
                    || model.id.as_str().to_ascii_lowercase().contains(&filter)
                    || model
                        .provider_id
                        .as_str()
                        .to_ascii_lowercase()
                        .contains(&filter)
            })
            .collect()
    }

    pub fn open_model_picker(&mut self, scope: ModelSelectionScope) {
        self.suspend_settings();
        self.client.model_picker = Some(ModelPicker {
            filter: String::new(),
            selected: 0,
            scope,
            stage: ModelPickerStage::Models,
            option_selected: 0,
            options: ModelOptions::default(),
            options_fast_only: false,
        });
        let selected_id = match scope {
            ModelSelectionScope::Session => self.selected_model.as_ref(),
            ModelSelectionScope::Vision => self.settings.vision.model_id.as_ref(),
            ModelSelectionScope::Default => None,
        };
        let selected = self
            .filtered_models()
            .iter()
            .position(|model| {
                selected_id == Some(&model.id)
                    || (scope == ModelSelectionScope::Default && model.is_default)
            })
            .unwrap_or_default();
        if let Some(picker) = &mut self.client.model_picker {
            picker.selected = selected;
        }
    }

    #[must_use]
    pub fn selected_picker_model(&self) -> Option<&ModelView> {
        let picker = self.client.model_picker.as_ref()?;
        self.filtered_models().get(picker.selected).copied()
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.activity != SessionActivity::Idle
            || self.active_turn.is_some()
            || self
                .subagents
                .iter()
                .any(|run| matches!(run.status, RunStatus::Starting | RunStatus::Working))
    }

    #[must_use]
    pub fn is_shell_mode(&self) -> bool {
        self.client.editor.text().starts_with('!')
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.status_message = status.into();
    }

    #[must_use]
    pub fn current_menu_has_parent(&self) -> bool {
        !self.client.menu_history.is_empty()
    }

    pub fn open_settings(&mut self) {
        self.client.settings = Some(settings_state(&self.settings));
        self.client.menu_history.clear();
        self.set_status("Settings opened.");
    }

    pub fn close_settings(&mut self) {
        self.client.settings = None;
        self.restore_previous_menu();
    }

    pub fn close_all_menus(&mut self) {
        self.client.model_picker = None;
        self.client.session_picker = None;
        self.client.provider_picker = None;
        self.client.agent_picker = None;
        self.client.settings = None;
        self.client.menu_history.clear();
    }

    fn suspend_settings(&mut self) {
        if let Some(settings) = self.client.settings.take() {
            self.client
                .menu_history
                .push(MenuSnapshot::Settings(settings));
        }
    }

    fn restore_previous_menu(&mut self) -> bool {
        let Some(menu) = self.client.menu_history.pop() else {
            return false;
        };
        match menu {
            MenuSnapshot::Settings(settings) => self.client.settings = Some(settings),
        }
        true
    }

    pub fn settings_insert(&mut self, character: char) {
        if let Some(settings) = &mut self.client.settings {
            if settings.view == SettingsView::Menu {
                settings.query.push(character);
            } else if settings.view == SettingsView::WebBrowsing
                && settings.addon_field == 1
                && settings.web.backend == WebBackend::Firecrawl
            {
                settings.web.firecrawl_api_key.push(character);
            } else if settings.view == SettingsView::Memory
                && settings.memory.backend == MemoryBackend::Mnemosyne
            {
                match settings.addon_field {
                    1 => settings.memory.executable.push(character),
                    2 => settings.memory.global_bank.push(character),
                    3 => settings.memory.data_directory.push(character),
                    _ => {}
                }
            }
            settings.selected = 0;
        }
    }

    pub fn settings_backspace(&mut self) {
        if let Some(settings) = &mut self.client.settings {
            if settings.view == SettingsView::Menu {
                settings.query.pop();
            } else if settings.view == SettingsView::WebBrowsing
                && settings.addon_field == 1
                && settings.web.backend == WebBackend::Firecrawl
            {
                settings.web.firecrawl_api_key.pop();
            } else if settings.view == SettingsView::Memory
                && settings.memory.backend == MemoryBackend::Mnemosyne
            {
                match settings.addon_field {
                    1 => {
                        settings.memory.executable.pop();
                    }
                    2 => {
                        settings.memory.global_bank.pop();
                    }
                    3 => {
                        settings.memory.data_directory.pop();
                    }
                    _ => {}
                }
            }
            settings.selected = 0;
        }
    }

    pub fn settings_move(&mut self, delta: isize) {
        let Some(settings) = &mut self.client.settings else {
            return;
        };
        let length = match settings.view {
            SettingsView::Menu => settings.filtered_sections().len(),
            SettingsView::Addons => 4,
            SettingsView::Memory if settings.memory.backend == MemoryBackend::Mnemosyne => 4,
            SettingsView::WebBrowsing if settings.web.backend == WebBackend::Firecrawl => 2,
            SettingsView::Vision
            | SettingsView::Memory
            | SettingsView::TerminalImages
            | SettingsView::WebBrowsing => 1,
        };
        if length > 0 {
            if matches!(settings.view, SettingsView::Menu | SettingsView::Addons) {
                settings.selected = offset_index(settings.selected, length, delta);
            } else {
                settings.addon_field = offset_index(settings.addon_field, length, delta);
            }
        }
    }

    pub fn settings_cycle_web_backend(&mut self, delta: isize) {
        let Some(settings) = &mut self.client.settings else {
            return;
        };
        if settings.view != SettingsView::WebBrowsing || settings.addon_field != 0 {
            return;
        }
        let index = WebBackend::ALL
            .iter()
            .position(|backend| *backend == settings.web.backend)
            .unwrap_or_default();
        settings.web.backend = WebBackend::ALL[offset_index(index, WebBackend::ALL.len(), delta)];
    }

    pub fn settings_cycle_memory_backend(&mut self, delta: isize) {
        let Some(settings) = &mut self.client.settings else {
            return;
        };
        if settings.view != SettingsView::Memory || settings.addon_field != 0 {
            return;
        }
        let index = MemoryBackend::ALL
            .iter()
            .position(|backend| *backend == settings.memory.backend)
            .unwrap_or_default();
        settings.memory.backend =
            MemoryBackend::ALL[offset_index(index, MemoryBackend::ALL.len(), delta)];
    }

    pub fn settings_cycle_terminal_images(&mut self, delta: isize) {
        let Some(settings) = &mut self.client.settings else {
            return;
        };
        if settings.view != SettingsView::TerminalImages {
            return;
        }
        let modes = [
            TerminalImageModeView::Auto,
            TerminalImageModeView::On,
            TerminalImageModeView::Off,
        ];
        let index = modes
            .iter()
            .position(|mode| *mode == settings.terminal_images)
            .unwrap_or_default();
        settings.terminal_images = modes[offset_index(index, modes.len(), delta)];
    }

    pub fn open_agent_picker(&mut self) {
        self.suspend_settings();
        self.client.agent_picker = Some(AgentPicker {
            agents: self.agents.clone(),
            selected: 0,
            editor: None,
        });
    }

    pub fn close_agent_picker(&mut self) {
        self.client.agent_picker = None;
        self.restore_previous_menu();
    }

    pub fn agent_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.agent_picker else {
            return;
        };
        if !picker.agents.is_empty() && picker.editor.is_none() {
            picker.selected = offset_index(picker.selected, picker.agents.len(), delta);
        }
    }

    pub fn edit_selected_agent(&mut self) {
        let Some(picker) = &mut self.client.agent_picker else {
            return;
        };
        picker.editor = picker
            .agents
            .get(picker.selected)
            .map(AgentEditor::from_definition);
    }

    pub fn create_agent(&mut self) {
        if let Some(picker) = &mut self.client.agent_picker {
            picker.editor = Some(AgentEditor::new());
        }
    }

    pub fn cancel_agent_edit(&mut self) -> bool {
        let Some(picker) = &mut self.client.agent_picker else {
            return false;
        };
        let Some(editor) = &mut picker.editor else {
            return false;
        };
        if editor.model_dropdown.take().is_some() || editor.pending_options.take().is_some() {
            return true;
        }
        picker.editor = None;
        true
    }

    #[must_use]
    pub fn agent_model_options_are_open(&self) -> bool {
        self.client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .is_some_and(|editor| editor.pending_options.is_some())
    }

    /// The rows of the options step, when one is open — the effort row first, fast mode after it.
    #[must_use]
    pub fn agent_pending_options(&self) -> Option<&AgentPendingOptions> {
        self.client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .and_then(|editor| editor.pending_options.as_ref())
    }

    /// Moves between the step's rows. A step with one row has nothing to move to.
    pub fn move_agent_model_options(&mut self, delta: isize) {
        let Some(pending) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .and_then(|editor| editor.pending_options.as_mut())
        else {
            return;
        };
        let rows = pending.row_count();
        if rows > 1 {
            pending.selected = offset_index(pending.selected, rows, delta);
        }
    }

    #[must_use]
    pub fn agent_model_dropdown_is_open(&self) -> bool {
        self.client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .is_some_and(|editor| editor.model_dropdown.is_some())
    }

    pub fn open_agent_model_dropdown(&mut self) {
        let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        else {
            return;
        };
        if editor.field != AgentEditorField::Model {
            return;
        }
        let mut items = vec![AgentModelOption::Inherit];
        items.extend(self.models.iter().cloned().map(AgentModelOption::Model));
        editor.model_dropdown = Some(SearchableDropdown::new(items));
    }

    pub fn agent_model_dropdown_move(&mut self, delta: isize) {
        if let Some(dropdown) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .and_then(|editor| editor.model_dropdown.as_mut())
        {
            dropdown.move_selection(delta, AgentModelOption::search_text);
        }
    }

    pub fn clear_agent_model_dropdown_query(&mut self) {
        if let Some(dropdown) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .and_then(|editor| editor.model_dropdown.as_mut())
        {
            dropdown.clear();
        }
    }

    pub fn adjust_agent_model_options(&mut self, delta: isize) {
        let Some(pending) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .and_then(|editor| editor.pending_options.as_mut())
        else {
            return;
        };
        if delta == 0 {
            return;
        }
        if pending.on_fast_mode_row() {
            pending.options.fast_mode = !pending.options.fast_mode;
        } else {
            pending.step_effort(delta);
        }
    }

    pub fn agent_editor_move(&mut self, delta: isize) {
        let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        else {
            return;
        };
        let index = AgentEditorField::ALL
            .iter()
            .position(|field| *field == editor.field)
            .unwrap_or_default();
        editor.field =
            AgentEditorField::ALL[offset_index(index, AgentEditorField::ALL.len(), delta)];
    }

    pub fn open_provider_picker(&mut self) {
        self.suspend_settings();
        self.client.provider_picker = Some(ProviderPicker {
            providers: self.providers.clone(),
            selected: 0,
            loading: false,
            showing_details: false,
            authentication: self
                .providers
                .first()
                .and_then(|provider| provider.authentication.as_ref())
                .map(frontend_picker_authentication),
        });
    }

    pub fn provider_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.provider_picker else {
            return;
        };
        if picker.providers.is_empty() {
            return;
        }
        picker.selected = offset_index(picker.selected, picker.providers.len(), delta);
        picker.authentication = picker
            .providers
            .get(picker.selected)
            .and_then(|provider| provider.authentication.as_ref())
            .map(frontend_picker_authentication);
    }

    pub fn open_provider_details(&mut self) {
        let Some(picker) = &mut self.client.provider_picker else {
            return;
        };
        let Some(provider) = picker.providers.get(picker.selected) else {
            return;
        };
        picker.showing_details = true;
        if matches!(
            provider.authentication,
            Some(ProviderAuthenticationView::ApiKeyRequired { .. })
        ) && !provider.credential_configured
        {
            picker.authentication = Some(ProviderAuthentication::ApiKeyInput {
                value: String::new(),
                focused: false,
            });
        }
    }

    pub fn close_provider_details(&mut self) -> bool {
        let Some(picker) = &mut self.client.provider_picker else {
            return false;
        };
        if !picker.showing_details {
            return false;
        }
        picker.showing_details = false;
        if matches!(
            picker.authentication,
            Some(ProviderAuthentication::ApiKeyInput { .. })
        ) {
            picker.authentication = None;
        }
        true
    }

    pub fn close_provider_picker(&mut self) {
        self.client.provider_picker = None;
        self.restore_previous_menu();
    }

    pub fn focus_provider_api_key(&mut self) -> bool {
        let Some(ProviderAuthentication::ApiKeyInput { focused, .. }) = self
            .client
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        else {
            return false;
        };
        *focused = true;
        true
    }

    pub fn focus_provider_api_key_at(&mut self, point: ScreenPoint) -> bool {
        let contains = self
            .client
            .api_key_input_hit_region
            .is_some_and(|region| contains(point, region.top_left, region.bottom_right));
        contains && self.focus_provider_api_key()
    }

    #[must_use]
    pub fn provider_api_key_input_active(&self) -> bool {
        self.client.provider_picker.as_ref().is_some_and(|picker| {
            picker.showing_details
                && matches!(
                    picker.authentication,
                    Some(ProviderAuthentication::ApiKeyInput { focused: true, .. })
                )
        })
    }

    pub fn provider_api_key_insert_str(&mut self, text: &str) {
        let Some(ProviderAuthentication::ApiKeyInput {
            value,
            focused: true,
        }) = self
            .client
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        else {
            return;
        };
        let remaining = 4_096_usize.saturating_sub(value.chars().count());
        value.extend(
            text.chars()
                .filter(|character| !character.is_control())
                .take(remaining),
        );
        self.set_status("Editing provider API key.");
    }

    pub fn provider_api_key_backspace(&mut self) {
        if let Some(ProviderAuthentication::ApiKeyInput {
            value,
            focused: true,
        }) = self
            .client
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        {
            value.pop();
        }
    }

    pub fn cancel_provider_api_key_input(&mut self) -> bool {
        let Some(ProviderAuthentication::ApiKeyInput { value, focused }) = self
            .client
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        else {
            return false;
        };
        if !*focused {
            return false;
        }
        value.clear();
        *focused = false;
        self.set_status("API key entry cancelled.");
        true
    }

    #[must_use]
    pub fn selected_provider(&self) -> Option<&ProviderView> {
        let picker = self.client.provider_picker.as_ref()?;
        picker.providers.get(picker.selected)
    }

    pub fn open_session_picker(&mut self) {
        self.client.session_picker = Some(SessionPicker {
            sessions: self.sessions.clone(),
            selected: 0,
            loading: false,
        });
    }

    pub fn session_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.session_picker else {
            return;
        };
        if !picker.sessions.is_empty() {
            picker.selected = picker
                .selected
                .saturating_add_signed(delta)
                .min(picker.sessions.len() - 1);
        }
    }

    pub fn close_session_picker(&mut self) {
        self.client.session_picker = None;
        self.set_status("Session selection cancelled.");
    }

    pub fn close_model_picker(&mut self) {
        self.client.model_picker = None;
        self.restore_previous_menu();
    }

    pub fn picker_insert(&mut self, character: char) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Models
            && !character.is_control()
        {
            picker.filter.push(character);
            picker.selected = 0;
        }
    }

    pub fn picker_backspace(&mut self) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Models
        {
            picker.filter.pop();
            picker.selected = 0;
        }
    }

    pub fn picker_move(&mut self, delta: isize) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Options
        {
            let option_count = if picker.options_fast_only { 1 } else { 2 };
            picker.option_selected = offset_index(picker.option_selected, option_count, delta);
            return;
        }
        let length = self.filtered_models().len();
        if let Some(picker) = &mut self.client.model_picker
            && length > 0
        {
            picker.selected = offset_index(picker.selected, length, delta);
        }
    }

    pub fn picker_adjust(&mut self, delta: isize) {
        let reasoning_efforts = self
            .client
            .model_picker
            .as_ref()
            .and_then(|picker| self.filtered_models().get(picker.selected).copied())
            .map(|model| model.configuration.reasoning_efforts.clone())
            .unwrap_or_default();
        let Some(picker) = &mut self.client.model_picker else {
            return;
        };
        if picker.stage != ModelPickerStage::Options || delta == 0 {
            return;
        }
        if picker.options_fast_only || picker.option_selected == 1 {
            picker.options.fast_mode = !picker.options.fast_mode;
        } else if !reasoning_efforts.is_empty() {
            let selected = picker
                .options
                .reasoning_effort
                .as_deref()
                .and_then(|effort| {
                    reasoning_efforts
                        .iter()
                        .position(|candidate| candidate == effort)
                })
                .unwrap_or_default();
            picker.options.reasoning_effort = Some(
                reasoning_efforts[offset_index(selected, reasoning_efforts.len(), delta)].clone(),
            );
        }
    }

    pub fn move_queue_selection(&mut self, delta: isize) {
        if self.queue.is_empty() {
            self.client.queue_selection = None;
            return;
        }
        let selected = self.client.queue_selection.unwrap_or_default();
        self.client.queue_selection = Some(offset_index(selected, self.queue.len(), delta));
    }

    pub fn move_question_selection(&mut self, delta: isize) {
        let Some(question) = self.questions.front_mut() else {
            return;
        };
        if !question.interaction.options.is_empty() {
            question.selected =
                offset_index(question.selected, question.interaction.options.len(), delta);
        }
    }

    pub fn toggle_question_selection(&mut self) {
        let Some(question) = self.questions.front_mut() else {
            return;
        };
        if question.interaction.multiple
            && let Some(selected) = question.selections.get_mut(question.selected)
        {
            *selected = !*selected;
        }
    }

    #[must_use]
    pub fn selected_subagent_summary(&self) -> Option<(String, String)> {
        let run_id = self.client.subagent_modal.as_deref()?;
        let run = self
            .subagents
            .iter()
            .find(|run| run.id.as_str() == run_id)?;
        Some((run.agent_slug.clone(), run.objective.clone()))
    }

    #[must_use]
    pub fn selected_subagent_transcript(&self) -> Option<&Transcript> {
        let run_id = self.client.subagent_modal.as_deref()?;
        self.subagent_chats.get(run_id)
    }

    #[must_use]
    pub fn selected_subagent_scroll(&self) -> usize {
        self.client
            .subagent_modal
            .as_ref()
            .and_then(|run_id| self.client.subagent_scroll_from_bottom.get(run_id))
            .copied()
            .unwrap_or_default()
    }

    pub fn set_selected_subagent_scroll(&mut self, scroll: usize) {
        if let Some(run_id) = self.client.subagent_modal.clone() {
            self.client
                .subagent_scroll_from_bottom
                .insert(run_id, scroll);
        }
    }

    fn selected_subagent_transcript_mut(&mut self) -> Option<(&mut Transcript, &mut usize)> {
        let run_id = self.client.subagent_modal.clone()?;
        let transcript = self.subagent_chats.get_mut(&run_id)?;
        let scroll = self
            .client
            .subagent_scroll_from_bottom
            .entry(run_id)
            .or_default();
        Some((transcript, scroll))
    }

    pub fn set_subagent_hit_regions(&mut self, regions: Vec<(String, ScreenPoint, ScreenPoint)>) {
        self.client.subagent_hit_regions = regions
            .into_iter()
            .map(|(run_id, top_left, bottom_right)| SubagentHitRegion {
                run_id,
                top_left,
                bottom_right,
            })
            .collect();
    }

    pub fn set_tool_toggle_hit_regions(
        &mut self,
        regions: Vec<(String, ScreenPoint, ScreenPoint)>,
    ) {
        self.client.tool_toggle_hit_regions = regions
            .into_iter()
            .map(|(key, top_left, bottom_right)| ToolToggleHitRegion {
                key,
                top_left,
                bottom_right,
            })
            .collect();
    }

    pub fn set_oauth_link_hit_region(
        &mut self,
        region: Option<(String, ScreenPoint, ScreenPoint)>,
    ) {
        self.client.oauth_link_hit_region =
            region.map(|(url, top_left, bottom_right)| OAuthLinkHitRegion {
                url,
                top_left,
                bottom_right,
            });
    }

    pub fn set_api_key_input_hit_region(&mut self, region: Option<(ScreenPoint, ScreenPoint)>) {
        self.client.api_key_input_hit_region =
            region.map(|(top_left, bottom_right)| ApiKeyInputHitRegion {
                top_left,
                bottom_right,
            });
    }

    pub fn set_screen_snapshot(&mut self, snapshot: ScreenSnapshot) {
        self.client.screen_snapshot = Some(snapshot);
    }

    pub fn toggle_tool_at(&mut self, point: ScreenPoint) -> bool {
        let Some(key) = self
            .client
            .tool_toggle_hit_regions
            .iter()
            .find(|region| contains(point, region.top_left, region.bottom_right))
            .map(|region| region.key.clone())
        else {
            return false;
        };
        let toggles_history = key == TOOL_HISTORY_TOGGLE_KEY;
        let transcript = if let Some(run_id) = self.client.subagent_modal.as_deref() {
            let Some(transcript) = self.subagent_chats.get_mut(run_id) else {
                return false;
            };
            transcript
        } else {
            &mut self.transcript
        };
        let expanded = if toggles_history {
            transcript.toggle_tool_history()
        } else {
            transcript.toggle_tool_output(&key)
        };
        let Some(expanded) = expanded else {
            return false;
        };
        self.clear_text_selection();
        self.set_status(match (toggles_history, expanded) {
            (true, true) => "Showing all tool calls.",
            (true, false) => "Showing the latest 5 tool calls.",
            (false, true) => "Expanded tool output.",
            (false, false) => "Collapsed tool output.",
        });
        true
    }

    pub fn open_subagent_at(&mut self, point: ScreenPoint) -> bool {
        let Some(run_id) = self
            .client
            .subagent_hit_regions
            .iter()
            .find(|region| contains(point, region.top_left, region.bottom_right))
            .map(|region| region.run_id.clone())
        else {
            return false;
        };
        self.client.subagent_modal = Some(run_id);
        self.clear_text_selection();
        true
    }

    #[must_use]
    pub fn oauth_url_at(&self, point: ScreenPoint) -> Option<String> {
        self.client
            .oauth_link_hit_region
            .as_ref()
            .filter(|region| contains(point, region.top_left, region.bottom_right))
            .map(|region| region.url.clone())
    }

    pub fn close_subagent_modal(&mut self) {
        self.client.subagent_modal = None;
        self.clear_text_selection();
    }

    pub fn scroll_active_chat(&mut self, delta: isize) {
        if self.client.subagent_modal.is_some() {
            if let Some((_, scroll)) = self.selected_subagent_transcript_mut() {
                *scroll = scroll.saturating_add_signed(delta);
            }
        } else {
            self.client.scroll_from_bottom =
                self.client.scroll_from_bottom.saturating_add_signed(delta);
        }
    }

    pub fn reset_active_chat_scroll(&mut self) {
        if self.client.subagent_modal.is_some() {
            if let Some((_, scroll)) = self.selected_subagent_transcript_mut() {
                *scroll = 0;
            }
        } else {
            self.client.scroll_from_bottom = 0;
        }
    }

    pub fn begin_text_selection(&mut self, point: ScreenPoint) {
        self.client.text_selection = Some(TextSelection::new(point));
        self.client.pending_clipboard = None;
    }

    pub fn update_text_selection(&mut self, point: ScreenPoint) {
        if let Some(selection) = &mut self.client.text_selection {
            selection.update(point);
        }
    }

    pub fn finish_text_selection(&mut self, point: ScreenPoint) {
        self.update_text_selection(point);
        self.client.pending_clipboard = self
            .client
            .text_selection
            .filter(|selection| selection.is_range())
            .and_then(|selection| {
                self.client
                    .screen_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.selected_text(selection))
            });
    }

    pub fn clear_text_selection(&mut self) {
        self.client.text_selection = None;
        self.client.pending_clipboard = None;
    }

    pub fn take_pending_clipboard(&mut self) -> Option<String> {
        self.client.pending_clipboard.take()
    }

    pub fn insert_attachments(&mut self, attachments: Vec<PromptAttachment>) {
        for attachment in &attachments {
            let label = match attachment {
                PromptAttachment::Artifact { label, .. }
                | PromptAttachment::LocalFile { label, .. }
                | PromptAttachment::InlineImage { label, .. } => label,
            };
            if !self.client.editor.is_blank() {
                self.client.editor.insert_char(' ');
            }
            self.client.editor.insert_str(&format!("[{label}]"));
            self.client.editor.insert_char(' ');
        }
        self.client.draft_attachments.extend(attachments);
    }

    #[must_use]
    pub fn command_completions(&self) -> Vec<PromptCompletion<'_>> {
        let token = self.client.editor.token_before_cursor();
        if let Some(prefix) = token.text.strip_prefix(crate::controls::SKILL_PREFIX) {
            let completions = self
                .skills
                .iter()
                .filter(|skill| skill.name.starts_with(prefix))
                .map(PromptCompletion::Skill)
                .collect::<Vec<_>>();
            if !completions.is_empty() {
                return completions;
            }
        }
        commands::matching(&token.text, token.at_prompt_start)
            .into_iter()
            .map(PromptCompletion::Command)
            .collect()
    }

    #[must_use]
    pub fn selected_command_completion(&self) -> Option<PromptCompletion<'_>> {
        let completions = self.command_completions();
        let selected = self
            .client
            .command_completion_selection
            .min(completions.len().saturating_sub(1));
        completions.get(selected).copied()
    }

    #[must_use]
    pub fn command_completion_is_exact(&self) -> bool {
        self.selected_command_completion()
            .is_some_and(|completion| {
                completion.replacement() == self.client.editor.token_before_cursor().text
            })
    }

    pub fn move_command_completion(&mut self, delta: isize) {
        let count = self.command_completions().len();
        if count == 0 {
            self.client.command_completion_selection = 0;
            return;
        }
        self.client.command_completion_selection = self
            .client
            .command_completion_selection
            .min(count - 1)
            .saturating_add_signed(delta)
            .min(count - 1);
    }

    pub fn accept_command_completion(&mut self) {
        let Some(completion) = self.selected_command_completion() else {
            return;
        };
        let replacement = completion.replacement();
        self.client.editor.replace_token_before_cursor(&replacement);
        self.client.command_completion_selection = 0;
        self.set_status(format!("Inserted {replacement}."));
    }
}
