use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
};

pub use crate::{backend::ApprovalDecision, session::SubagentStatus};

use crate::{
    agent::{AgentCatalog, AgentDefinition},
    backend::{
        ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent, BackendOperation,
        CODEX_PROVIDER, CompactionReason, DeltaKind, ItemKind, ItemStatus, ModelInfo,
        NormalizedItem, QuestionRequest, SessionHistoryItem, TodoPhase, TurnOutcome,
    },
    commands::{self, CommandSpec, ParsedPromptCommand},
    editor::EditorState,
    handoff::HandoffPackage,
    selection::{ScreenPoint, ScreenSnapshot, TextSelection},
    session::{ProviderRecord, SessionRecord, SubagentRecord},
    transcript::{EntryKind, EntryStatus, TOOL_HISTORY_TOGGLE_KEY, Transcript},
    web::{WebBackend, WebConfig},
};

const MAX_CONCURRENT_SUBAGENTS: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Starting,
    Ready { server: String },
    Failed(String),
    Disconnected(String),
}

impl ConnectionState {
    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::Starting => "connecting",
            Self::Ready { .. } => "ready",
            Self::Failed(_) => "failed",
            Self::Disconnected(_) => "disconnected",
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveTurn {
    pub id: String,
    pub model: Option<String>,
    pub cancelling: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextUsageState {
    pub estimated_tokens: usize,
    pub context_window: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextCompactionState {
    pub id: String,
    pub turn_id: String,
    pub reason: CompactionReason,
    pub estimated_tokens: usize,
    pub context_window: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedPrompt {
    pub id: String,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutgoingPrompt {
    id: String,
    text: String,
    model: Option<String>,
    handoff: Option<HandoffPackage>,
}

impl OutgoingPrompt {
    fn wire_text(&self) -> String {
        self.handoff.as_ref().map_or_else(
            || self.text.clone(),
            |handoff| handoff.render_with_prompt(&self.text),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingSteer {
    id: String,
    text: String,
    turn_id: String,
    editor_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelPicker {
    pub filter: String,
    pub selected: usize,
    pub scope: ModelSelectionScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelSelectionScope {
    Default,
    Session,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionPrompt {
    pub request: QuestionRequest,
    pub selected: usize,
    pub selections: Vec<bool>,
}

#[derive(Clone, Debug)]
pub struct SessionPicker {
    pub sessions: Vec<SessionRecord>,
    pub selected: usize,
    pub loading: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderPicker {
    pub providers: Vec<ProviderRecord>,
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
    pub fn label(self) -> &'static str {
        match self {
            Self::Slug => "Slug",
            Self::Description => "Description",
            Self::SystemPrompt => "System prompt",
            Self::FirstMessage => "First message",
            Self::Model => "Model",
            Self::FallbackModels => "Fallbacks",
        }
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
        }
    }

    fn from_definition(definition: &AgentDefinition) -> Self {
        Self {
            original_slug: Some(definition.slug.clone()),
            field: AgentEditorField::Slug,
            slug: definition.slug.clone(),
            description: definition.description.clone(),
            system_prompt: definition.system_prompt.clone(),
            first_message: definition.first_message.clone(),
            model: definition.model.clone().unwrap_or_default(),
            fallback_models: definition.fallback_models.join(", "),
        }
    }

    fn value_mut(&mut self) -> &mut String {
        match self.field {
            AgentEditorField::Slug => &mut self.slug,
            AgentEditorField::Description => &mut self.description,
            AgentEditorField::SystemPrompt => &mut self.system_prompt,
            AgentEditorField::FirstMessage => &mut self.first_message,
            AgentEditorField::Model => &mut self.model,
            AgentEditorField::FallbackModels => &mut self.fallback_models,
        }
    }

    fn definition(&self) -> AgentDefinition {
        AgentDefinition {
            slug: self.slug.trim().to_owned(),
            description: self.description.trim().to_owned(),
            system_prompt: self.system_prompt.trim().to_owned(),
            first_message: self.first_message.trim().to_owned(),
            model: (!self.model.trim().is_empty()).then(|| self.model.trim().to_owned()),
            fallback_models: self
                .fallback_models
                .split(',')
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AgentPicker {
    pub agents: Vec<AgentDefinition>,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentBrowserStatus {
    Checking,
    Available(String),
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsState {
    pub query: String,
    pub selected: usize,
    pub view: SettingsView,
    pub web: WebConfig,
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
struct ProviderContext {
    name: String,
    capabilities: BackendCapabilities,
    connection: ConnectionState,
    provider_session_id: Option<String>,
    session_id: Option<String>,
    context_usage: Option<ContextUsageState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentRun {
    pub id: String,
    pub agent: String,
    pub provider: String,
    pub provider_session_id: Option<String>,
    pub objective: String,
    pub status: SubagentStatus,
    pub latest_activity: String,
}

#[derive(Debug, Default)]
struct ReasoningSummaryTracker {
    turns: HashMap<String, ReasoningSummaryTurn>,
}

#[derive(Debug, Default)]
struct ReasoningSummaryTurn {
    latest_item: Option<String>,
    streams: HashMap<String, ReasoningSummaryStream>,
}

#[derive(Debug)]
struct ReasoningSummaryStream {
    index: usize,
    text: String,
}

struct ReasoningSummaryUpdate {
    replaced_item: Option<String>,
    text: String,
}

impl ReasoningSummaryTracker {
    fn append_delta(
        &mut self,
        turn_id: &str,
        item_id: &str,
        index: usize,
        delta: &str,
    ) -> ReasoningSummaryUpdate {
        let turn = self.turns.entry(turn_id.to_owned()).or_default();
        let replaced_item = turn
            .latest_item
            .replace(item_id.to_owned())
            .filter(|previous| previous != item_id);
        let stream =
            turn.streams
                .entry(item_id.to_owned())
                .or_insert_with(|| ReasoningSummaryStream {
                    index,
                    text: String::new(),
                });
        if stream.index != index {
            stream.index = index;
            stream.text.clear();
        }
        stream.text.push_str(delta);
        ReasoningSummaryUpdate {
            replaced_item,
            text: latest_reasoning_summary(&stream.text).to_owned(),
        }
    }

    fn contains(&self, turn_id: &str, item_id: &str) -> bool {
        self.turns
            .get(turn_id)
            .is_some_and(|turn| turn.streams.contains_key(item_id))
    }

    fn is_superseded(&self, turn_id: &str, item_id: &str) -> bool {
        self.turns.get(turn_id).is_some_and(|turn| {
            turn.streams.contains_key(item_id) && turn.latest_item.as_deref() != Some(item_id)
        })
    }

    fn remove_turn(&mut self, turn_id: &str) {
        self.turns.remove(turn_id);
    }
}

#[derive(Debug)]
struct SubagentChat {
    transcript: Transcript,
    scroll_from_bottom: usize,
    reasoning_summaries: ReasoningSummaryTracker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SubagentHitRegion {
    run_id: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolToggleHitRegion {
    key: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
struct SubagentExecution {
    run: SubagentRun,
    definition: AgentDefinition,
    request_id: u64,
    task: String,
    session_id: Option<String>,
    response: String,
    model_targets: Vec<AgentModelTarget>,
    model_target_index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgentModelTarget {
    provider: String,
    model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentRequest {
    pub id: u64,
    pub agent: String,
    pub task: String,
}

#[derive(Clone, Debug)]
pub enum Effect {
    Backend(BackendCommand),
    SpawnSubagent {
        run_id: String,
        provider: String,
    },
    SubagentBackend {
        run_id: String,
        command: BackendCommand,
    },
    StopSubagent(String),
    CompleteAgentRequest {
        request_id: u64,
        result: String,
        success: bool,
    },
    ListSessions,
    ListProviders,
    SetProviderEnabled {
        provider: String,
        enabled: bool,
    },
    AuthenticateProvider(String),
    SaveProviderCredential {
        provider: String,
        kind: String,
        metadata: serde_json::Value,
    },
    ClearProviderCredential(String),
    OpenUrl(String),
    SaveAgent {
        definition: AgentDefinition,
        previous_slug: Option<String>,
    },
    DeleteAgent(String),
    ResolveSession(String),
    PersistSession {
        provider: String,
        provider_session_id: String,
        workspace: String,
        title: String,
        model: Option<String>,
    },
    PersistModels {
        provider: String,
        models: Vec<ModelInfo>,
    },
    SetDefaultModel {
        provider: String,
        model: String,
    },
    PersistSubagent(SubagentRecord),
    LoadSubagents(String),
    UpdateSessionModel {
        session_id: String,
        model: Option<String>,
    },
    TouchSession(String),
    SaveWebConfig(WebConfig),
    CheckAgentBrowser,
    Quit,
}

#[derive(Debug)]
pub struct AppState {
    pub connection: ConnectionState,
    pub workspace: String,
    pub backend_provider: String,
    pub backend_name: String,
    pub backend_capabilities: BackendCapabilities,
    provider_contexts: HashMap<String, ProviderContext>,
    pub provider_session_id: Option<String>,
    pub session_id: Option<String>,
    pub active_turn: Option<ActiveTurn>,
    pub context_usage: Option<ContextUsageState>,
    pub context_compaction: Option<ContextCompactionState>,
    pub editor: EditorState,
    pub transcript: Transcript,
    pub queue: VecDeque<QueuedPrompt>,
    pub queue_selection: Option<usize>,
    pub models: Vec<ModelInfo>,
    pub selected_model: Option<String>,
    session_model_override: bool,
    pub model_picker: Option<ModelPicker>,
    pub session_picker: Option<SessionPicker>,
    pub provider_picker: Option<ProviderPicker>,
    pub agent_picker: Option<AgentPicker>,
    pub settings: Option<SettingsState>,
    command_completion_selection: usize,
    pending_model_picker: Option<ModelSelectionScope>,
    pub show_help: bool,
    pub text_selection: Option<TextSelection>,
    pub approvals: VecDeque<ApprovalRequest>,
    pub questions: VecDeque<QuestionPrompt>,
    pub todo_phases: Vec<TodoPhase>,
    pub scroll_from_bottom: usize,
    pub status_message: String,
    pub diagnostic_count: usize,
    pub nakode_session_id: String,
    nakode_executable: String,
    pub subagents: Vec<SubagentRun>,
    pub subagent_modal: Option<String>,
    pub should_quit: bool,
    creating_session: Option<()>,
    pending_session_prompt: Option<OutgoingPrompt>,
    starting_turn: Option<OutgoingPrompt>,
    pending_steer: Option<PendingSteer>,
    pending_handoff: Option<HandoffPackage>,
    resuming_session: Option<SessionRecord>,
    startup_resume: Option<String>,
    item_turns: HashMap<String, String>,
    reasoning_summaries: ReasoningSummaryTracker,
    subagent_result_items: HashSet<String>,
    initial_model: Option<String>,
    next_local_id: u64,
    screen_snapshot: Option<ScreenSnapshot>,
    pending_clipboard: Option<String>,
    agents: AgentCatalog,
    agent_directory: PathBuf,
    subagent_executions: HashMap<String, SubagentExecution>,
    subagent_chats: HashMap<String, SubagentChat>,
    subagent_hit_regions: Vec<SubagentHitRegion>,
    tool_toggle_hit_regions: Vec<ToolToggleHitRegion>,
    oauth_link_hit_region: Option<OAuthLinkHitRegion>,
    api_key_input_hit_region: Option<ApiKeyInputHitRegion>,
    transcript_limit: usize,
    web_config: WebConfig,
}

impl AppState {
    pub fn install_web_config(&mut self, config: WebConfig) {
        self.web_config = config.clone();
        if let Some(settings) = &mut self.settings {
            settings.web = config;
        }
    }

    pub fn open_settings(&mut self) {
        self.settings = Some(SettingsState {
            query: String::new(),
            selected: 0,
            view: SettingsView::Menu,
            web: self.web_config.clone(),
            addon_field: 0,
            agent_browser_status: AgentBrowserStatus::Checking,
        });
        self.set_status("Settings opened.");
    }

    pub fn close_settings(&mut self) {
        self.settings = None;
    }

    pub fn settings_insert(&mut self, character: char) {
        if let Some(settings) = &mut self.settings {
            if settings.view == SettingsView::Menu {
                settings.query.push(character);
            } else if settings.view == SettingsView::WebBrowsing
                && settings.addon_field == 1
                && settings.web.backend == WebBackend::Firecrawl
            {
                settings.web.firecrawl_api_key.push(character);
            }
            settings.selected = 0;
        }
    }

    pub fn settings_backspace(&mut self) {
        if let Some(settings) = &mut self.settings {
            if settings.view == SettingsView::Menu {
                settings.query.pop();
            } else if settings.view == SettingsView::WebBrowsing
                && settings.addon_field == 1
                && settings.web.backend == WebBackend::Firecrawl
            {
                settings.web.firecrawl_api_key.pop();
            }
            settings.selected = 0;
        }
    }

    pub fn settings_move(&mut self, delta: isize) {
        let Some(settings) = &mut self.settings else {
            return;
        };
        let length = match settings.view {
            SettingsView::Menu => settings.filtered_sections().len(),
            SettingsView::WebBrowsing if settings.web.backend == WebBackend::Firecrawl => 2,
            SettingsView::Addons | SettingsView::WebBrowsing => 1,
        };
        if length > 0 {
            if settings.view == SettingsView::Menu || settings.view == SettingsView::Addons {
                settings.selected = offset_index(settings.selected, length, delta);
            } else {
                settings.addon_field = offset_index(settings.addon_field, length, delta);
            }
        }
    }

    pub fn settings_cycle_backend(&mut self, delta: isize) {
        let Some(settings) = &mut self.settings else {
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

    pub fn select_setting(&mut self) -> Vec<Effect> {
        if self
            .settings
            .as_ref()
            .is_some_and(|settings| settings.view == SettingsView::WebBrowsing)
        {
            self.settings_cycle_backend(1);
            return self.save_web_settings();
        }
        if let Some(settings) = &mut self.settings
            && settings.view == SettingsView::Addons
        {
            settings.view = SettingsView::WebBrowsing;
            settings.addon_field = 0;
            settings.agent_browser_status = AgentBrowserStatus::Checking;
            return vec![Effect::CheckAgentBrowser];
        }
        let section = self
            .settings
            .as_ref()
            .and_then(|settings| settings.filtered_sections().get(settings.selected).copied());
        self.settings = None;
        match section {
            Some(SettingsSection::General) => {
                self.provider_picker = Some(ProviderPicker {
                    providers: Vec::new(),
                    selected: 0,
                    loading: true,
                    showing_details: false,
                    authentication: None,
                });
                vec![Effect::ListProviders]
            }
            Some(SettingsSection::Agents) => {
                self.open_agent_picker();
                Vec::new()
            }
            Some(SettingsSection::Models) => self.open_default_model_picker(),
            Some(SettingsSection::Addons) => {
                self.open_settings();
                if let Some(settings) = &mut self.settings {
                    settings.view = SettingsView::Addons;
                }
                Vec::new()
            }
            None => Vec::new(),
        }
    }

    pub fn settings_back(&mut self) -> Vec<Effect> {
        let Some(view) = self.settings.as_ref().map(|settings| settings.view) else {
            return Vec::new();
        };
        match view {
            SettingsView::Menu => {
                self.settings = None;
                Vec::new()
            }
            SettingsView::Addons => {
                if let Some(settings) = &mut self.settings {
                    settings.view = SettingsView::Menu;
                    settings.selected = 0;
                }
                Vec::new()
            }
            SettingsView::WebBrowsing => {
                let effects = self.save_web_settings();
                if let Some(settings) = &mut self.settings {
                    settings.view = SettingsView::Addons;
                    settings.selected = 0;
                }
                effects
            }
        }
    }

    pub fn set_agent_browser_status(&mut self, status: AgentBrowserStatus) {
        if let Some(settings) = &mut self.settings {
            settings.agent_browser_status = status;
        }
    }

    pub fn save_web_settings(&mut self) -> Vec<Effect> {
        let Some(settings) = &self.settings else {
            return Vec::new();
        };
        let config = settings.web.clone();
        self.set_status("Saving browser add-on settings…");
        vec![Effect::SaveWebConfig(config)]
    }

    pub fn set_status(&mut self, message: &str) {
        self.status_message.clear();
        self.status_message.push_str(message);
    }

    pub fn new(
        workspace: impl Into<String>,
        initial_model: Option<String>,
        scrollback: usize,
    ) -> Self {
        Self::new_for_backend(
            workspace,
            initial_model,
            scrollback,
            CODEX_PROVIDER,
            "Codex",
        )
    }

    pub fn new_for_backend(
        workspace: impl Into<String>,
        initial_model: Option<String>,
        scrollback: usize,
        provider: impl Into<String>,
        backend_name: impl Into<String>,
    ) -> Self {
        let backend_name = backend_name.into();
        let transcript = Transcript::new(scrollback);
        let provider = provider.into();
        let mut provider_contexts = HashMap::new();
        provider_contexts.insert(
            provider.clone(),
            ProviderContext {
                name: backend_name.clone(),
                capabilities: BackendCapabilities::default(),
                connection: ConnectionState::Starting,
                provider_session_id: None,
                session_id: None,
                context_usage: None,
            },
        );
        Self {
            connection: ConnectionState::Starting,
            workspace: workspace.into(),
            backend_provider: provider,
            backend_name: backend_name.clone(),
            backend_capabilities: BackendCapabilities::default(),
            provider_contexts,
            provider_session_id: None,
            session_id: None,
            active_turn: None,
            context_usage: None,
            context_compaction: None,
            editor: EditorState::default(),
            transcript,
            queue: VecDeque::new(),
            queue_selection: None,
            models: Vec::new(),
            selected_model: initial_model.clone(),
            session_model_override: initial_model.is_some(),
            model_picker: None,
            session_picker: None,
            provider_picker: None,
            agent_picker: None,
            settings: None,
            command_completion_selection: 0,
            pending_model_picker: None,
            show_help: false,
            text_selection: None,
            approvals: VecDeque::new(),
            questions: VecDeque::new(),
            todo_phases: Vec::new(),
            scroll_from_bottom: 0,
            status_message: format!("Connecting to {backend_name}…"),
            diagnostic_count: 0,
            nakode_session_id: uuid::Uuid::now_v7().to_string(),
            nakode_executable: "nakode".to_owned(),
            subagents: Vec::new(),
            subagent_modal: None,
            should_quit: false,
            creating_session: None,
            pending_session_prompt: None,
            starting_turn: None,
            pending_steer: None,
            pending_handoff: None,
            resuming_session: None,
            startup_resume: None,
            item_turns: HashMap::new(),
            reasoning_summaries: ReasoningSummaryTracker::default(),
            subagent_result_items: HashSet::new(),
            initial_model,
            next_local_id: 1,
            screen_snapshot: None,
            pending_clipboard: None,
            agents: AgentCatalog::default(),
            agent_directory: PathBuf::from(".nakode/agents"),
            subagent_executions: HashMap::new(),
            subagent_chats: HashMap::new(),
            subagent_hit_regions: Vec::new(),
            tool_toggle_hit_regions: Vec::new(),
            oauth_link_hit_region: None,
            api_key_input_hit_region: None,
            transcript_limit: scrollback,
            web_config: WebConfig::default(),
        }
    }

    pub fn new_unconfigured(
        workspace: impl Into<String>,
        initial_model: Option<String>,
        scrollback: usize,
    ) -> Self {
        let mut state = Self::new_for_backend(
            workspace,
            initial_model,
            scrollback,
            String::new(),
            "No provider",
        );
        state.provider_contexts.clear();
        state.connection = ConnectionState::Disconnected("no provider enabled".to_owned());
        state.backend_provider.clear();
        "No provider is enabled. Open /providers to configure one."
            .clone_into(&mut state.status_message);
        state
    }

    pub fn install_agents(&mut self, agents: AgentCatalog) {
        self.agents = agents;
        if let Some(picker) = &mut self.agent_picker {
            picker.agents = self.agents.definitions().to_vec();
            picker.selected = picker.selected.min(picker.agents.len().saturating_sub(1));
            picker.editor = None;
        }
    }

    pub fn set_agent_directory(&mut self, directory: PathBuf) {
        self.agent_directory = directory;
    }

    #[must_use]
    pub fn agent_directory(&self) -> &Path {
        &self.agent_directory
    }

    pub fn open_agent_picker(&mut self) {
        self.agent_picker = Some(AgentPicker {
            agents: self.agents.definitions().to_vec(),
            selected: 0,
            editor: None,
        });
        self.set_status("Agent archetypes opened.");
    }

    pub fn close_agent_picker(&mut self) {
        self.agent_picker = None;
        self.set_status("Agent settings closed.");
    }

    pub fn agent_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.agent_picker else {
            return;
        };
        if !picker.agents.is_empty() && picker.editor.is_none() {
            picker.selected = offset_index(picker.selected, picker.agents.len(), delta);
        }
    }

    pub fn edit_selected_agent(&mut self) {
        let Some(picker) = &mut self.agent_picker else {
            return;
        };
        if let Some(definition) = picker.agents.get(picker.selected) {
            picker.editor = Some(AgentEditor::from_definition(definition));
        }
    }

    pub fn create_agent(&mut self) {
        if let Some(picker) = &mut self.agent_picker {
            picker.editor = Some(AgentEditor::new());
        }
    }

    pub fn cancel_agent_edit(&mut self) -> bool {
        let Some(picker) = &mut self.agent_picker else {
            return false;
        };
        picker.editor.take().is_some()
    }

    pub fn agent_editor_move(&mut self, delta: isize) {
        let Some(editor) = self
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

    pub fn agent_editor_insert(&mut self, character: char) {
        if let Some(editor) = self
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            editor.value_mut().push(character);
        }
    }

    pub fn agent_editor_insert_str(&mut self, text: &str) {
        if let Some(editor) = self
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            editor.value_mut().push_str(text);
        }
    }

    pub fn agent_editor_backspace(&mut self) {
        if let Some(editor) = self
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            editor.value_mut().pop();
        }
    }

    pub fn save_agent_edit(&mut self) -> Vec<Effect> {
        let Some(editor) = self
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
        else {
            return Vec::new();
        };
        vec![Effect::SaveAgent {
            definition: editor.definition(),
            previous_slug: editor.original_slug.clone(),
        }]
    }

    pub fn delete_selected_agent(&mut self) -> Vec<Effect> {
        self.agent_picker
            .as_ref()
            .and_then(|picker| picker.agents.get(picker.selected))
            .map_or_else(Vec::new, |agent| {
                vec![Effect::DeleteAgent(agent.slug.clone())]
            })
    }

    pub fn set_nakode_executable(&mut self, executable: &Path) {
        self.nakode_executable = executable.to_string_lossy().into_owned();
    }

    #[must_use]
    pub fn has_running_subagents(&self) -> bool {
        self.subagents.iter().any(|run| {
            matches!(
                run.status,
                SubagentStatus::Starting | SubagentStatus::Working
            )
        })
    }

    #[must_use]
    pub fn selected_subagent_summary(&self) -> Option<(String, String)> {
        let run_id = self.subagent_modal.as_deref()?;
        let run = self.subagents.iter().find(|run| run.id == run_id)?;
        Some((run.agent.clone(), run.objective.clone()))
    }

    pub fn selected_subagent_transcript_mut(&mut self) -> Option<(&mut Transcript, &mut usize)> {
        let run_id = self.subagent_modal.as_deref()?;
        let chat = self.subagent_chats.get_mut(run_id)?;
        Some((&mut chat.transcript, &mut chat.scroll_from_bottom))
    }

    pub fn set_subagent_hit_regions(&mut self, regions: Vec<(String, ScreenPoint, ScreenPoint)>) {
        self.subagent_hit_regions = regions
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
        self.tool_toggle_hit_regions = regions
            .into_iter()
            .map(|(key, top_left, bottom_right)| ToolToggleHitRegion {
                key,
                top_left,
                bottom_right,
            })
            .collect();
    }

    pub fn toggle_tool_at(&mut self, point: ScreenPoint) -> bool {
        let Some(key) = self
            .tool_toggle_hit_regions
            .iter()
            .find(|region| {
                point.column >= region.top_left.column
                    && point.column < region.bottom_right.column
                    && point.row >= region.top_left.row
                    && point.row < region.bottom_right.row
            })
            .map(|region| region.key.clone())
        else {
            return false;
        };
        let toggles_history = key == TOOL_HISTORY_TOGGLE_KEY;
        let transcript = if let Some(run_id) = self.subagent_modal.as_deref() {
            let Some(chat) = self.subagent_chats.get_mut(run_id) else {
                return false;
            };
            &mut chat.transcript
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
            .subagent_hit_regions
            .iter()
            .find(|region| {
                point.column >= region.top_left.column
                    && point.column < region.bottom_right.column
                    && point.row >= region.top_left.row
                    && point.row < region.bottom_right.row
            })
            .map(|region| region.run_id.clone())
        else {
            return false;
        };
        self.subagent_modal = Some(run_id);
        self.clear_text_selection();
        true
    }

    pub fn set_oauth_link_hit_region(
        &mut self,
        region: Option<(String, ScreenPoint, ScreenPoint)>,
    ) {
        self.oauth_link_hit_region =
            region.map(|(url, top_left, bottom_right)| OAuthLinkHitRegion {
                url,
                top_left,
                bottom_right,
            });
    }

    #[must_use]
    pub fn oauth_url_at(&self, point: ScreenPoint) -> Option<String> {
        self.oauth_link_hit_region
            .as_ref()
            .filter(|region| {
                point.column >= region.top_left.column
                    && point.column < region.bottom_right.column
                    && point.row >= region.top_left.row
                    && point.row < region.bottom_right.row
            })
            .map(|region| region.url.clone())
    }

    pub fn set_api_key_input_hit_region(&mut self, region: Option<(ScreenPoint, ScreenPoint)>) {
        self.api_key_input_hit_region =
            region.map(|(top_left, bottom_right)| ApiKeyInputHitRegion {
                top_left,
                bottom_right,
            });
    }

    pub fn focus_provider_api_key_at(&mut self, point: ScreenPoint) -> bool {
        let contains = self.api_key_input_hit_region.is_some_and(|region| {
            point.column >= region.top_left.column
                && point.column < region.bottom_right.column
                && point.row >= region.top_left.row
                && point.row < region.bottom_right.row
        });
        contains && self.focus_provider_api_key()
    }

    pub fn focus_provider_api_key(&mut self) -> bool {
        let Some(ProviderAuthentication::ApiKeyInput { focused, .. }) = self
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        else {
            return false;
        };
        *focused = true;
        self.set_status("Editing Cursor API key.");
        true
    }

    pub fn open_provider_authentication_url(&mut self) -> Vec<Effect> {
        let Some(url) = self.provider_authentication_url().map(str::to_owned) else {
            self.set_status("No provider authentication URL is available.");
            return Vec::new();
        };
        vec![Effect::OpenUrl(url)]
    }

    pub fn copy_provider_authentication_url(&mut self) -> Vec<Effect> {
        let Some(url) = self.provider_authentication_url().map(str::to_owned) else {
            self.set_status("No provider authentication URL is available.");
            return Vec::new();
        };
        self.pending_clipboard = Some(url);
        Vec::new()
    }

    fn provider_authentication_url(&self) -> Option<&str> {
        let picker = self.provider_picker.as_ref()?;
        if !picker.showing_details {
            return None;
        }
        match picker.authentication.as_ref()? {
            ProviderAuthentication::Challenge {
                verification_url, ..
            } => Some(verification_url),
            ProviderAuthentication::ApiKeyInput { .. } => Some("https://cursor.com/dashboard/api"),
            ProviderAuthentication::Starting => None,
        }
    }

    pub fn close_subagent_modal(&mut self) {
        self.subagent_modal = None;
        self.clear_text_selection();
    }

    pub fn scroll_subagent_modal(&mut self, delta: isize) {
        let Some((_, scroll)) = self.selected_subagent_transcript_mut() else {
            return;
        };
        *scroll = scroll.saturating_add_signed(delta);
    }

    pub fn scroll_active_chat(&mut self, delta: isize) {
        if self.subagent_modal.is_some() {
            self.scroll_subagent_modal(delta);
        } else {
            self.scroll_from_bottom = self.scroll_from_bottom.saturating_add_signed(delta);
        }
    }

    pub fn reset_subagent_scroll(&mut self) {
        if let Some((_, scroll)) = self.selected_subagent_transcript_mut() {
            *scroll = 0;
        }
    }

    pub fn reset_active_chat_scroll(&mut self) {
        if self.subagent_modal.is_some() {
            self.reset_subagent_scroll();
        } else {
            self.scroll_from_bottom = 0;
        }
    }

    pub fn set_startup_resume(&mut self, session_id: Option<String>) {
        self.startup_resume = session_id;
    }

    pub fn install_sessions(&mut self, sessions: Vec<SessionRecord>) {
        let picker = self.session_picker.get_or_insert(SessionPicker {
            sessions: Vec::new(),
            selected: 0,
            loading: false,
        });
        picker.sessions = sessions;
        picker.selected = 0;
        picker.loading = false;
        self.status_message = if picker.sessions.is_empty() {
            "No saved sessions for this workspace.".to_owned()
        } else {
            format!("{} saved session(s).", picker.sessions.len())
        };
    }

    pub fn install_subagents(&mut self, records: Vec<SubagentRecord>) {
        self.subagents.clear();
        self.subagent_executions.clear();
        self.subagent_chats.clear();
        self.subagent_hit_regions.clear();
        self.subagent_modal = None;
        for record in records {
            let mut status = record.status;
            let mut latest_activity = record.latest_activity;
            if matches!(status, SubagentStatus::Starting | SubagentStatus::Working) {
                status = SubagentStatus::Interrupted;
                "Interrupted when the previous client exited".clone_into(&mut latest_activity);
            }
            let mut transcript = Transcript::new(self.transcript_limit);
            transcript.set_stream_label(record.agent.clone());
            for entry in record.transcript {
                if let Some(key) = entry.key {
                    transcript.upsert(key, entry.kind, entry.title, entry.body, entry.status);
                } else {
                    transcript.push(entry.kind, entry.title, entry.body, entry.status);
                }
            }
            if status == SubagentStatus::Interrupted {
                transcript.finish_running(EntryStatus::Interrupted);
            }
            let run = SubagentRun {
                id: record.id.clone(),
                agent: record.agent,
                provider: record.provider,
                provider_session_id: record.provider_session_id,
                objective: record.objective,
                status,
                latest_activity,
            };
            self.subagents.push(run.clone());
            self.sync_inline_subagent(&run);
            self.subagent_chats.insert(
                record.id,
                SubagentChat {
                    transcript,
                    scroll_from_bottom: 0,
                    reasoning_summaries: ReasoningSummaryTracker::default(),
                },
            );
        }
    }

    pub fn session_store_failed(&mut self, message: impl Into<String>) {
        if let Some(picker) = &mut self.session_picker {
            picker.loading = false;
        }
        self.resuming_session = None;
        self.status_message = format!("Session error: {}", message.into());
    }

    pub fn install_providers(&mut self, providers: Vec<ProviderRecord>) {
        let picker = self.provider_picker.get_or_insert(ProviderPicker {
            providers: Vec::new(),
            selected: 0,
            loading: false,
            showing_details: false,
            authentication: None,
        });
        picker.providers = providers;
        picker.selected = 0;
        picker.loading = false;
    }

    pub fn provider_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.provider_picker else {
            return;
        };
        if picker.providers.is_empty() {
            return;
        }
        picker.selected = offset_index(picker.selected, picker.providers.len(), delta);
    }

    pub fn open_provider_details(&mut self) {
        let Some(picker) = &mut self.provider_picker else {
            return;
        };
        if let Some(provider) = picker.providers.get(picker.selected) {
            picker.showing_details = true;
            if provider.provider == crate::backend::CURSOR_PROVIDER && provider.credential.is_none()
            {
                picker.authentication = Some(ProviderAuthentication::ApiKeyInput {
                    value: String::new(),
                    focused: false,
                });
            }
        }
    }

    pub fn close_provider_details(&mut self) -> bool {
        let Some(picker) = &mut self.provider_picker else {
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

    #[must_use]
    pub fn provider_capabilities(&self, provider: &str) -> Option<&BackendCapabilities> {
        if provider == self.backend_provider {
            return Some(&self.backend_capabilities);
        }
        self.provider_contexts
            .get(provider)
            .map(|context| &context.capabilities)
    }

    #[must_use]
    pub fn provider_connection(&self, provider: &str) -> Option<&ConnectionState> {
        if provider == self.backend_provider {
            return Some(&self.connection);
        }
        self.provider_contexts
            .get(provider)
            .map(|context| &context.connection)
    }

    #[must_use]
    pub fn provider_display_name(&self, provider: &str) -> String {
        self.provider_picker
            .as_ref()
            .and_then(|picker| {
                picker
                    .providers
                    .iter()
                    .find(|record| record.provider == provider)
            })
            .map_or_else(|| provider.to_owned(), |record| record.display_name.clone())
    }

    pub fn close_provider_picker(&mut self) {
        self.provider_picker = None;
        self.set_status("Provider settings closed.");
    }

    pub fn toggle_provider(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get_mut(picker.selected) else {
            return Vec::new();
        };
        if provider.credential.is_none() {
            if provider.provider == crate::backend::CURSOR_PROVIDER {
                match &mut picker.authentication {
                    Some(ProviderAuthentication::ApiKeyInput { focused, .. }) => {
                        *focused = true;
                    }
                    _ => {
                        picker.authentication = Some(ProviderAuthentication::ApiKeyInput {
                            value: String::new(),
                            focused: true,
                        });
                    }
                }
                self.set_status("Enter your Cursor API key.");
                return Vec::new();
            }
            picker.authentication = Some(ProviderAuthentication::Starting);
            self.status_message = format!("Starting {} authentication…", provider.display_name);
            return vec![Effect::AuthenticateProvider(provider.provider.clone())];
        }
        provider.enabled = !provider.enabled;
        self.status_message = format!(
            "{} {}.",
            provider.display_name,
            if provider.enabled {
                "enabled"
            } else {
                "disabled"
            }
        );
        vec![Effect::SetProviderEnabled {
            provider: provider.provider.clone(),
            enabled: provider.enabled,
        }]
    }

    #[must_use]
    pub fn provider_api_key_input_active(&self) -> bool {
        self.provider_picker.as_ref().is_some_and(|picker| {
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
        self.set_status("Editing Cursor API key.");
    }

    pub fn provider_api_key_backspace(&mut self) {
        if let Some(ProviderAuthentication::ApiKeyInput {
            value,
            focused: true,
        }) = self
            .provider_picker
            .as_mut()
            .and_then(|picker| picker.authentication.as_mut())
        {
            value.pop();
        }
    }

    pub fn submit_provider_api_key(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get(picker.selected) else {
            return Vec::new();
        };
        if provider.provider != crate::backend::CURSOR_PROVIDER {
            return Vec::new();
        }
        let Some(ProviderAuthentication::ApiKeyInput {
            value,
            focused: true,
        }) = &picker.authentication
        else {
            return Vec::new();
        };
        let api_key = value.trim().to_owned();
        if api_key.is_empty() {
            self.set_status("Cursor API key cannot be empty.");
            return Vec::new();
        }
        let provider = provider.provider.clone();
        picker.authentication = Some(ProviderAuthentication::Starting);
        self.set_status("Saving Cursor API key…");
        vec![Effect::SaveProviderCredential {
            provider,
            kind: "cursor_api_key".to_owned(),
            metadata: serde_json::json!({"api_key": api_key}),
        }]
    }

    pub fn cancel_provider_api_key_input(&mut self) -> bool {
        let Some(picker) = &mut self.provider_picker else {
            return false;
        };
        let Some(ProviderAuthentication::ApiKeyInput { value, focused }) =
            &mut picker.authentication
        else {
            return false;
        };
        if !*focused {
            return false;
        }
        value.clear();
        *focused = false;
        self.set_status("Cursor API key entry cancelled.");
        true
    }

    pub fn logout_provider(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get(picker.selected) else {
            return Vec::new();
        };
        if provider.credential.is_none() {
            self.set_status("This provider has no credentials to clear.");
            return Vec::new();
        }
        self.status_message = format!("Logging out of {}…", provider.display_name);
        vec![Effect::ClearProviderCredential(provider.provider.clone())]
    }

    pub fn provider_logged_out(&mut self, provider: &str) {
        if let Some(picker) = &mut self.provider_picker {
            picker.authentication = None;
        }
        self.provider_contexts.remove(provider);
        if provider == self.backend_provider {
            self.connection = ConnectionState::Disconnected("logged out".to_owned());
            self.provider_session_id = None;
            self.session_id = None;
            self.context_usage = None;
        }
        self.set_status(&format!(
            "Logged out of {}.",
            self.provider_display_name(provider)
        ));
    }

    pub fn provider_authentication_failed(&mut self, provider: &str, message: &str) {
        if let Some(picker) = &mut self.provider_picker {
            picker.authentication = None;
        }
        self.provider_contexts.remove(provider);
        if provider == self.backend_provider {
            self.context_usage = None;
        }
        self.set_status(&format!("Authentication failed for {provider}: {message}"));
    }

    fn provider_is_authenticating(&self, provider: &str) -> bool {
        self.provider_picker.as_ref().is_some_and(|picker| {
            picker.authentication.is_some()
                && picker
                    .providers
                    .get(picker.selected)
                    .is_some_and(|record| record.provider == provider)
        })
    }

    pub fn provider_starting(&mut self, provider: &str, display_name: &str) {
        self.provider_contexts.insert(
            provider.to_owned(),
            ProviderContext {
                name: display_name.to_owned(),
                capabilities: BackendCapabilities::default(),
                connection: ConnectionState::Starting,
                provider_session_id: None,
                session_id: None,
                context_usage: None,
            },
        );
        if self.backend_provider.is_empty() {
            provider.clone_into(&mut self.backend_provider);
            display_name.clone_into(&mut self.backend_name);
            self.connection = ConnectionState::Starting;
            self.context_usage = None;
        }
        self.set_status(&format!("Connecting to {display_name}…"));
    }

    pub fn provider_start_failed(&mut self, provider: &str, display_name: &str, message: &str) {
        self.provider_contexts.insert(
            provider.to_owned(),
            ProviderContext {
                name: display_name.to_owned(),
                capabilities: BackendCapabilities::default(),
                connection: ConnectionState::Failed(message.to_owned()),
                provider_session_id: None,
                session_id: None,
                context_usage: None,
            },
        );
        if provider == self.backend_provider {
            self.connection = ConnectionState::Failed(message.to_owned());
            self.context_usage = None;
        }
        self.set_status(&format!("Could not start {provider}: {message}"));
    }

    pub fn provider_disabled(&mut self, provider: &str) {
        self.provider_contexts.remove(provider);
        let model_prefix = format!("{provider}/");
        self.models
            .retain(|model| model.provider != provider && !model.id.starts_with(&model_prefix));
        if self
            .selected_model
            .as_deref()
            .is_some_and(|model| model.starts_with(&model_prefix))
        {
            self.selected_model = None;
        }
        if provider != self.backend_provider {
            return;
        }
        if let Some((next_provider, context)) = self.provider_contexts.iter().next() {
            self.backend_provider.clone_from(next_provider);
            self.backend_name.clone_from(&context.name);
            self.backend_capabilities = context.capabilities.clone();
            self.connection = context.connection.clone();
            self.provider_session_id
                .clone_from(&context.provider_session_id);
            self.session_id.clone_from(&context.session_id);
            self.context_usage = context.context_usage;
        } else {
            self.backend_provider.clear();
            "No provider".clone_into(&mut self.backend_name);
            self.backend_capabilities = BackendCapabilities::default();
            self.connection = ConnectionState::Disconnected("no provider enabled".to_owned());
            self.provider_session_id = None;
            self.session_id = None;
            self.context_usage = None;
            self.set_status("No provider is enabled. Open /providers to configure one.");
        }
    }

    pub fn session_persisted(&mut self, session: &SessionRecord) {
        self.session_id = Some(session.id.clone());
        self.status_message = format!("Session {} started.", short_id(&session.id));
    }

    pub fn session_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.session_picker else {
            return;
        };
        if picker.sessions.is_empty() {
            return;
        }
        picker.selected = picker
            .selected
            .saturating_add_signed(delta)
            .min(picker.sessions.len() - 1);
    }

    pub fn close_session_picker(&mut self) {
        self.session_picker = None;
        self.set_status("Session selection cancelled.");
    }

    pub fn select_session(&mut self) -> Vec<Effect> {
        let session = self
            .session_picker
            .as_ref()
            .and_then(|picker| picker.sessions.get(picker.selected))
            .cloned();
        let Some(session) = session else {
            self.set_status("No session is selected.");
            return Vec::new();
        };
        self.begin_resume(session)
    }

    pub fn begin_resume(&mut self, session: SessionRecord) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot switch sessions while a turn is active.");
            return Vec::new();
        }
        if session.workspace != self.workspace {
            self.set_status("That session belongs to a different workspace.");
            return Vec::new();
        }
        if !self.activate_provider(&session.provider) {
            return Vec::new();
        }
        if !self.backend_capabilities.resume.is_supported() {
            self.status_message = format!("{} does not support session resume.", self.backend_name);
            return Vec::new();
        }
        self.pending_handoff = None;
        let old_provider_session = self.provider_session_id.clone();
        self.resuming_session = Some(session.clone());
        self.session_picker = None;
        self.status_message = format!("Resuming session {}…", short_id(&session.id));
        let mut effects = Vec::new();
        if let Some(provider_session_id) =
            old_provider_session.filter(|current| current != &session.provider_session_id)
        {
            effects.push(Effect::Backend(BackendCommand::UnsubscribeSession {
                provider_session_id,
            }));
        }
        effects.push(Effect::Backend(BackendCommand::ResumeSession {
            provider_session_id: session.provider_session_id,
        }));
        effects
    }

    pub fn begin_text_selection(&mut self, point: ScreenPoint) {
        self.text_selection = Some(TextSelection::new(point));
        self.pending_clipboard = None;
    }

    pub fn update_text_selection(&mut self, point: ScreenPoint) {
        if let Some(selection) = &mut self.text_selection {
            selection.update(point);
        }
    }

    pub fn finish_text_selection(&mut self, point: ScreenPoint) {
        self.update_text_selection(point);
        self.pending_clipboard = self
            .text_selection
            .filter(|selection| selection.is_range())
            .and_then(|selection| {
                self.screen_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.selected_text(selection))
            });
    }

    pub fn clear_text_selection(&mut self) {
        self.text_selection = None;
        self.pending_clipboard = None;
    }

    pub fn set_screen_snapshot(&mut self, snapshot: ScreenSnapshot) {
        self.screen_snapshot = Some(snapshot);
    }

    pub fn take_pending_clipboard(&mut self) -> Option<String> {
        self.pending_clipboard.take()
    }

    pub fn clipboard_copied(&mut self, bytes: usize) {
        self.status_message = format!("Copied selection to clipboard ({bytes} bytes).");
    }

    pub fn clipboard_failed(&mut self, error: &str) {
        self.status_message = format!("Could not copy selection: {error}");
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.creating_session.is_some()
            || self.starting_turn.is_some()
            || self.active_turn.is_some()
            || self.context_compaction.is_some()
            || self.has_running_subagents()
    }

    #[must_use]
    pub fn command_completions(&self) -> Vec<&'static CommandSpec> {
        let token = self.editor.token_before_cursor();
        commands::matching(&token.text, token.at_prompt_start)
    }

    #[must_use]
    pub fn selected_command_completion(&self) -> Option<&'static CommandSpec> {
        let completions = self.command_completions();
        let selected = self
            .command_completion_selection
            .min(completions.len().saturating_sub(1));
        completions.get(selected).copied()
    }

    #[must_use]
    pub fn command_completion_is_exact(&self) -> bool {
        self.selected_command_completion()
            .is_some_and(|completion| {
                completion.invocation == self.editor.token_before_cursor().text
            })
    }

    pub fn move_command_completion(&mut self, delta: isize) {
        let completion_count = self.command_completions().len();
        if completion_count == 0 {
            self.command_completion_selection = 0;
            return;
        }
        let selected = self
            .command_completion_selection
            .min(completion_count - 1)
            .saturating_add_signed(delta)
            .min(completion_count - 1);
        self.command_completion_selection = selected;
    }

    pub fn accept_command_completion(&mut self) {
        let Some(completion) = self.selected_command_completion() else {
            return;
        };
        self.editor
            .replace_token_before_cursor(completion.invocation);
        self.command_completion_selection = 0;
        self.status_message = format!("Inserted {}.", completion.invocation);
    }

    pub fn submit_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write a message before sending.");
            return Vec::new();
        }
        let editor_text = self.editor.text();
        if let Some(command) = commands::parse_prompt_command(&editor_text) {
            match command {
                ParsedPromptCommand::Agents => {
                    self.editor.clear();
                    self.open_agent_picker();
                    return Vec::new();
                }
                ParsedPromptCommand::Settings => {
                    self.editor.clear();
                    self.open_settings();
                    return Vec::new();
                }
                ParsedPromptCommand::Compress => {
                    self.editor.clear();
                    return self.compress_session_context();
                }
                ParsedPromptCommand::Models => {
                    self.editor.clear();
                    return self.open_default_model_picker();
                }
                ParsedPromptCommand::New => {
                    self.editor.clear();
                    return self.new_session();
                }
                ParsedPromptCommand::Providers => {
                    self.editor.clear();
                    self.provider_picker = Some(ProviderPicker {
                        providers: Vec::new(),
                        selected: 0,
                        loading: true,
                        showing_details: false,
                        authentication: None,
                    });
                    self.set_status("Loading providers…");
                    return vec![Effect::ListProviders];
                }
                ParsedPromptCommand::Reload => {
                    self.editor.clear();
                    return self.reload_backend();
                }
                ParsedPromptCommand::Resume(session_id) => {
                    if self.is_busy() {
                        self.set_status("Cannot switch sessions while a turn is active.");
                        return Vec::new();
                    }
                    self.editor.clear();
                    if let Some(session_id) = session_id {
                        self.status_message = format!("Looking up session {session_id}…");
                        return vec![Effect::ResolveSession(session_id.to_owned())];
                    }
                    self.session_picker = Some(SessionPicker {
                        sessions: Vec::new(),
                        selected: 0,
                        loading: true,
                    });
                    self.set_status("Loading sessions…");
                    return vec![Effect::ListSessions];
                }
                ParsedPromptCommand::Switch => {
                    self.editor.clear();
                    return self.open_model_picker();
                }
            }
        }

        if !self.connection.is_ready() {
            self.set_status("The backend is not ready; the draft was preserved.");
            return Vec::new();
        }

        if self.is_busy() {
            self.enqueue_editor()
        } else {
            let prompt = self.take_editor_prompt();
            self.begin_prompt(prompt)
        }
    }

    fn compress_session_context(&mut self) -> Vec<Effect> {
        if !self.connection.is_ready() {
            self.set_status("The backend is not ready; context cannot be compressed.");
            return Vec::new();
        }
        if self.is_busy() {
            self.set_status("Cannot compress context while the chat is busy.");
            return Vec::new();
        }
        if !self.backend_capabilities.context_compaction.is_supported() {
            self.status_message = format!(
                "{} does not support manual context compression.",
                self.backend_name
            );
            return Vec::new();
        }
        let Some(session_id) = self.provider_session_id.clone() else {
            self.set_status("Send a message before compressing this chat.");
            return Vec::new();
        };
        let compaction_id = uuid::Uuid::now_v7().to_string();
        self.context_compaction = Some(ContextCompactionState {
            id: compaction_id.clone(),
            turn_id: compaction_id.clone(),
            reason: CompactionReason::Manual,
            estimated_tokens: 0,
            context_window: None,
        });
        self.transcript.upsert(
            compaction_id.clone(),
            EntryKind::System,
            "Compressing context",
            "Preparing a continuity checkpoint for the current chat.",
            EntryStatus::Running,
        );
        self.set_status("Compressing the current chat context…");
        vec![Effect::Backend(BackendCommand::CompactSession {
            session_id,
            compaction_id,
        })]
    }

    fn reload_backend(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot reload while a turn is active.");
            return Vec::new();
        }
        if self
            .backend_capabilities
            .models_require_session
            .is_supported()
            && self.provider_session_id.is_none()
        {
            self.creating_session = Some(());
        }
        self.status_message = format!("Reloading {} metadata…", self.backend_name);
        vec![Effect::Backend(BackendCommand::Reload {
            session_id: self.provider_session_id.clone(),
        })]
    }

    fn new_session(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot start a new session while a turn is active.");
            return Vec::new();
        }
        let previous = self.provider_session_id.take();
        self.session_id = None;
        self.session_model_override = false;
        self.selected_model = self.default_model();
        self.active_turn = None;
        self.context_usage = None;
        self.context_compaction = None;
        self.creating_session = None;
        self.pending_session_prompt = None;
        self.starting_turn = None;
        self.pending_steer = None;
        self.pending_handoff = None;
        self.resuming_session = None;
        self.item_turns.clear();
        self.reasoning_summaries = ReasoningSummaryTracker::default();
        self.subagent_result_items.clear();
        self.approvals.clear();
        self.queue.clear();
        self.queue_selection = None;
        self.subagents.clear();
        self.subagent_executions.clear();
        self.subagent_chats.clear();
        self.subagent_hit_regions.clear();
        self.subagent_modal = None;
        self.transcript.clear();
        self.transcript.push(
            EntryKind::System,
            "NAKODE",
            "New session. Send a message to begin.",
            EntryStatus::Complete,
        );
        self.set_status("New session ready.");
        previous
            .map(|provider_session_id| {
                vec![Effect::Backend(BackendCommand::UnsubscribeSession {
                    provider_session_id,
                })]
            })
            .unwrap_or_default()
    }

    pub fn enqueue_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write a message before queueing.");
            return Vec::new();
        }
        if !self.is_busy() {
            return self.submit_editor();
        }

        let prompt = self.take_editor_prompt();
        self.queue.push_back(prompt);
        self.queue_selection = Some(self.queue.len() - 1);
        self.status_message = format!("Queued message {}.", self.queue.len());
        Vec::new()
    }

    pub fn submit_or_steer_editor(&mut self) -> Vec<Effect> {
        let is_prompt_command = commands::parse_prompt_command(&self.editor.text()).is_some();
        if self.active_turn.is_some() && !is_prompt_command {
            self.steer_editor()
        } else {
            self.submit_editor()
        }
    }

    pub fn steer_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write steering guidance first.");
            return Vec::new();
        }
        if !self.backend_capabilities.steering.is_supported() {
            self.status_message = format!("{} does not support steering.", self.backend_name);
            return Vec::new();
        }
        if self.pending_steer.is_some() {
            self.set_status("A steer request is already awaiting the backend.");
            return Vec::new();
        }
        let Some(active) = self.active_turn.as_ref() else {
            self.set_status("There is no active turn to steer.");
            return Vec::new();
        };
        if active.cancelling {
            self.set_status("The active turn is being cancelled.");
            return Vec::new();
        }
        let turn_id = active.id.clone();
        let Some(provider_session_id) = self.provider_session_id.clone() else {
            self.set_status("The active provider session is unavailable.");
            return Vec::new();
        };

        let id = self.next_id("steer");
        let text = self.editor.text();
        self.pending_steer = Some(PendingSteer {
            id: id.clone(),
            text: text.clone(),
            turn_id: turn_id.clone(),
            editor_revision: self.editor.revision(),
        });
        self.set_status("Sending steering guidance…");
        vec![Effect::Backend(BackendCommand::SteerTurn {
            session_id: provider_session_id,
            turn_id,
            client_id: id,
            prompt: text,
        })]
    }

    pub fn cancel_or_quit(&mut self) -> Vec<Effect> {
        let (interrupted_subagents, mut effects) = self.interrupt_subagents();
        if self.active_turn.is_none()
            && let Some(compaction) = self.context_compaction.as_ref()
        {
            if !self.backend_capabilities.interruption.is_supported() {
                self.status_message =
                    format!("{} does not support interruption.", self.backend_name);
                return effects;
            }
            let Some(provider_session_id) = self.provider_session_id.clone() else {
                self.set_status("Cannot cancel: the provider session id is unavailable.");
                return effects;
            };
            let turn_id = compaction.turn_id.clone();
            self.set_status("Interrupting context compression…");
            effects.push(Effect::Backend(BackendCommand::InterruptTurn {
                session_id: provider_session_id,
                turn_id,
            }));
            return effects;
        }
        let Some(active) = self.active_turn.as_mut() else {
            if interrupted_subagents > 0 {
                self.status_message =
                    format!("Interrupted {interrupted_subagents} running subagent(s).");
                return effects;
            }
            self.should_quit = true;
            return vec![Effect::Backend(BackendCommand::Shutdown), Effect::Quit];
        };
        if !self.backend_capabilities.interruption.is_supported() {
            self.status_message = format!("{} does not support interruption.", self.backend_name);
            return effects;
        }
        if active.cancelling {
            self.should_quit = true;
            effects.extend([Effect::Backend(BackendCommand::Shutdown), Effect::Quit]);
            return effects;
        }
        let Some(provider_session_id) = self.provider_session_id.clone() else {
            self.set_status("Cannot cancel: the provider session id is unavailable.");
            return effects;
        };

        active.cancelling = true;
        self.status_message = if interrupted_subagents == 0 {
            "Interrupting active turn… Press Ctrl+C again to exit Nakode.".to_owned()
        } else {
            format!(
                "Interrupting active turn and {interrupted_subagents} subagent(s)… Press Ctrl+C again to exit Nakode."
            )
        };
        effects.push(Effect::Backend(BackendCommand::InterruptTurn {
            session_id: provider_session_id,
            turn_id: active.id.clone(),
        }));
        effects
    }

    pub fn request_quit(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("A turn is active. Cancel it with Ctrl+C before exiting.");
            Vec::new()
        } else {
            self.should_quit = true;
            vec![Effect::Backend(BackendCommand::Shutdown), Effect::Quit]
        }
    }

    pub fn install_cached_models(&mut self, models: Vec<ModelInfo>) {
        if models.is_empty() {
            return;
        }
        self.install_models(models);
        self.status_message = format!("Loaded cached {} models.", self.backend_name);
    }

    pub fn install_persisted_model_preferences(&mut self, models: Vec<ModelInfo>) {
        if !models.is_empty() {
            self.install_models(models);
        }
    }

    pub fn open_model_picker(&mut self) -> Vec<Effect> {
        self.open_model_picker_for(ModelSelectionScope::Session)
    }

    pub fn open_default_model_picker(&mut self) -> Vec<Effect> {
        self.open_model_picker_for(ModelSelectionScope::Default)
    }

    fn open_model_picker_for(&mut self, scope: ModelSelectionScope) -> Vec<Effect> {
        if self.pending_model_picker.is_some()
            || (self.creating_session.is_some() && self.provider_session_id.is_none())
        {
            self.status_message = format!("Loading {} models…", self.backend_name);
            return Vec::new();
        }
        if !self.models.is_empty() {
            self.show_model_picker(scope);
            return Vec::new();
        }
        if !self.backend_capabilities.model_catalog.is_supported() {
            self.status_message = format!("{} does not expose model selection.", self.backend_name);
            return Vec::new();
        }
        self.pending_model_picker = Some(scope);
        self.status_message = format!("Loading {} models…", self.backend_name);
        if self
            .backend_capabilities
            .models_require_session
            .is_supported()
            && self.provider_session_id.is_none()
        {
            self.creating_session = Some(());
        }
        vec![Effect::Backend(BackendCommand::Reload {
            session_id: self.provider_session_id.clone(),
        })]
    }

    fn show_model_picker(&mut self, scope: ModelSelectionScope) {
        let selected = self
            .selected_model
            .as_ref()
            .and_then(|selected| {
                self.models
                    .iter()
                    .position(|model| &model.qualified_id() == selected)
            })
            .unwrap_or(0);
        self.model_picker = Some(ModelPicker {
            filter: String::new(),
            selected,
            scope,
        });
        self.pending_model_picker = None;
    }

    pub fn picker_insert(&mut self, character: char) {
        if let Some(picker) = &mut self.model_picker
            && !character.is_control()
        {
            picker.filter.push(character);
            picker.selected = 0;
        }
    }

    pub fn picker_backspace(&mut self) {
        if let Some(picker) = &mut self.model_picker {
            picker.filter.pop();
            picker.selected = 0;
        }
    }

    pub fn picker_move(&mut self, delta: isize) {
        let count = self.filtered_models().len();
        let Some(picker) = &mut self.model_picker else {
            return;
        };
        if count == 0 {
            picker.selected = 0;
            return;
        }
        picker.selected = offset_index(picker.selected, count, delta);
    }

    pub fn picker_select(&mut self) -> Vec<Effect> {
        let filtered = self.filtered_models();
        let selected = self
            .model_picker
            .as_ref()
            .and_then(|picker| filtered.get(picker.selected))
            .copied()
            .cloned();
        if let Some(selected) = selected {
            let scope = self
                .model_picker
                .as_ref()
                .map_or(ModelSelectionScope::Session, |picker| picker.scope);
            let provider_changed = selected.provider != self.backend_provider;
            let source_provider = self.backend_provider.clone();
            let source_name = self.backend_name.clone();
            let source_model = self.selected_model.clone();
            let source_session = self.provider_session_id.clone();
            let target_name = self
                .provider_contexts
                .get(&selected.provider)
                .map_or_else(|| selected.provider.clone(), |context| context.name.clone());
            let handoff = provider_changed.then(|| {
                HandoffPackage::from_transcript(
                    source_provider.clone(),
                    source_model,
                    source_session,
                    selected.provider.clone(),
                    self.transcript.entries(),
                )
            });
            if !self.activate_provider(&selected.provider) {
                return Vec::new();
            }
            if provider_changed {
                self.provider_session_id = None;
                self.session_id = None;
                self.context_usage = None;
                self.pending_handoff = handoff.flatten();
                self.sync_active_provider_context();
                if self.pending_handoff.is_some() {
                    self.transcript.push(
                        EntryKind::System,
                        format!("HANDOFF · {source_name} → {target_name}"),
                        "The next message will continue in a fresh provider-native session.",
                        EntryStatus::Complete,
                    );
                }
            }
            let active = self.active_turn.is_some();
            let qualified = selected.qualified_id();
            self.selected_model = Some(qualified.clone());
            self.session_model_override = scope == ModelSelectionScope::Session;
            if scope == ModelSelectionScope::Default {
                for model in &mut self.models {
                    if model.provider == selected.provider {
                        model.is_default = model.id == selected.id;
                    }
                }
            }
            self.status_message = if provider_changed && self.pending_handoff.is_some() {
                format!("Selected {qualified}. The next message includes a continuity handoff.")
            } else if active {
                format!("Next model: {qualified}. The active turn is unchanged.")
            } else if scope == ModelSelectionScope::Default {
                format!("Default model: {qualified}.")
            } else {
                format!("Selected model: {qualified}.")
            };
            self.model_picker = None;
            let mut effects = Vec::new();
            if scope == ModelSelectionScope::Default {
                effects.push(Effect::SetDefaultModel {
                    provider: selected.provider.clone(),
                    model: selected.id.clone(),
                });
            }
            if self
                .backend_capabilities
                .session_model_config
                .is_supported()
                && let Some(session_id) = self.provider_session_id.clone()
            {
                effects.push(Effect::Backend(BackendCommand::SetSessionModel {
                    session_id,
                    model: selected.id,
                }));
            } else if let Some(session_id) = self.session_id.clone() {
                effects.push(Effect::UpdateSessionModel {
                    session_id,
                    model: Some(qualified),
                });
            }
            return effects;
        }
        Vec::new()
    }

    #[must_use]
    pub fn filtered_models(&self) -> Vec<&ModelInfo> {
        let Some(picker) = &self.model_picker else {
            return self.models.iter().collect();
        };
        let filter = picker.filter.to_lowercase();
        self.models
            .iter()
            .filter(|model| {
                filter.is_empty() || model.qualified_id().to_lowercase().contains(&filter)
            })
            .collect()
    }

    pub fn close_model_picker(&mut self) {
        self.model_picker = None;
        self.set_status("Model selection cancelled.");
    }

    pub fn move_queue_selection(&mut self, delta: isize) {
        if self.queue.is_empty() {
            self.queue_selection = None;
            return;
        }
        let current = self.queue_selection.unwrap_or(0);
        self.queue_selection = Some(offset_index(current, self.queue.len(), delta));
    }

    pub fn remove_selected_queue_item(&mut self) {
        let Some(index) = self.queue_selection else {
            return;
        };
        if let Some(prompt) = self.queue.remove(index) {
            self.status_message = format!("Removed queued message {}.", prompt.id);
        }
        self.queue_selection = if self.queue.is_empty() {
            None
        } else {
            Some(index.min(self.queue.len() - 1))
        };
    }

    pub fn resolve_approval(&mut self, decision: ApprovalDecision) -> Vec<Effect> {
        let Some(approval) = self.approvals.pop_front() else {
            return Vec::new();
        };
        let decision_name = match decision {
            ApprovalDecision::AcceptOnce => "accepted",
            ApprovalDecision::AcceptForSession => "accepted for this session",
            ApprovalDecision::Decline => "declined",
        };

        self.transcript.push(
            EntryKind::System,
            "APPROVAL",
            format!("{}: {decision_name}", approval.title),
            EntryStatus::Complete,
        );
        self.status_message = format!("Approval {decision_name}.");
        vec![Effect::Backend(BackendCommand::ResolveApproval {
            id: approval.id,
            decision,
        })]
    }

    pub fn move_question_selection(&mut self, delta: isize) {
        if let Some(question) = self.questions.front_mut() {
            question.selected =
                offset_index(question.selected, question.request.options.len(), delta);
        }
    }

    pub fn toggle_question_selection(&mut self) {
        let Some(question) = self.questions.front_mut() else {
            return;
        };
        if question.request.multi
            && let Some(selected) = question.selections.get_mut(question.selected)
        {
            *selected = !*selected;
        }
    }

    pub fn resolve_question(&mut self) -> Vec<Effect> {
        let Some(question) = self.questions.pop_front() else {
            return Vec::new();
        };
        let answers = if question.request.multi {
            question
                .request
                .options
                .iter()
                .zip(question.selections)
                .filter(|(_, selected)| *selected)
                .map(|(option, _)| option.label.clone())
                .collect::<Vec<_>>()
        } else {
            question
                .request
                .options
                .get(question.selected)
                .map(|option| vec![option.label.clone()])
                .unwrap_or_default()
        };
        if answers.is_empty() {
            return Vec::new();
        }
        let answer = serde_json::to_string(&answers).unwrap_or_else(|_| answers.join(", "));
        self.status_message = format!("Answered: {}", answers.join(", "));
        vec![Effect::Backend(BackendCommand::ResolveQuestion {
            id: question.request.id,
            answer,
        })]
    }

    pub fn handle_provider_backend(&mut self, provider: &str, event: BackendEvent) -> Vec<Effect> {
        if let Some(effects) = self.handle_provider_authentication(provider, &event) {
            return effects;
        }
        match &event {
            BackendEvent::Ready(identity) => {
                self.provider_contexts.insert(
                    provider.to_owned(),
                    ProviderContext {
                        name: identity.display_name.clone(),
                        capabilities: identity.capabilities.clone(),
                        connection: ConnectionState::Ready {
                            server: identity.display_name.clone(),
                        },
                        provider_session_id: self
                            .provider_contexts
                            .get(provider)
                            .and_then(|context| context.provider_session_id.clone()),
                        session_id: self
                            .provider_contexts
                            .get(provider)
                            .and_then(|context| context.session_id.clone()),
                        context_usage: self
                            .provider_contexts
                            .get(provider)
                            .and_then(|context| context.context_usage),
                    },
                );
                if self.backend_provider.is_empty() && !self.provider_is_authenticating(provider) {
                    provider.clone_into(&mut self.backend_provider);
                    self.backend_name.clone_from(&identity.display_name);
                    self.backend_capabilities = identity.capabilities.clone();
                    self.connection = ConnectionState::Ready {
                        server: identity.display_name.clone(),
                    };
                }
            }
            BackendEvent::Models(models) => {
                let mut models = models.clone();
                for model in &mut models {
                    provider.clone_into(&mut model.provider);
                }
                if !models.is_empty() {
                    self.install_models(models.clone());
                }
                if let Some(scope) = self.pending_model_picker
                    && !self.models.is_empty()
                {
                    self.show_model_picker(scope);
                }
                return vec![Effect::PersistModels {
                    provider: provider.to_owned(),
                    models,
                }];
            }
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            }
            | BackendEvent::SessionResumed {
                provider_session_id,
                ..
            }
            | BackendEvent::SessionObserved {
                provider_session_id,
            } if provider != self.backend_provider => {
                if let Some(context) = self.provider_contexts.get_mut(provider) {
                    if context.provider_session_id.as_deref() != Some(provider_session_id) {
                        context.context_usage = None;
                    }
                    context.provider_session_id = Some(provider_session_id.clone());
                }
                return Vec::new();
            }
            BackendEvent::ContextUsageUpdated {
                estimated_tokens,
                context_window,
            } if provider != self.backend_provider => {
                self.set_provider_context_usage(provider, *estimated_tokens, *context_window);
                return Vec::new();
            }
            BackendEvent::Warning(message) if provider != self.backend_provider => {
                self.diagnostic_count += 1;
                self.transcript.push(
                    EntryKind::System,
                    format!("{provider} WARNING"),
                    message,
                    EntryStatus::Complete,
                );
                self.status_message.clone_from(message);
                return Vec::new();
            }
            _ => {}
        }

        if provider != self.backend_provider {
            return Vec::new();
        }
        let effects = self.handle_backend(event);
        self.sync_active_provider_context();
        effects
    }

    fn set_provider_context_usage(
        &mut self,
        provider: &str,
        estimated_tokens: usize,
        context_window: Option<usize>,
    ) {
        if let Some(context) = self.provider_contexts.get_mut(provider) {
            context.context_usage = Some(ContextUsageState {
                estimated_tokens,
                context_window,
            });
        }
    }

    fn handle_provider_authentication(
        &mut self,
        provider: &str,
        event: &BackendEvent,
    ) -> Option<Vec<Effect>> {
        match event {
            BackendEvent::AuthenticationChallenge {
                verification_url,
                user_code,
                ..
            } => {
                if let Some(picker) = &mut self.provider_picker {
                    picker.authentication = Some(ProviderAuthentication::Challenge {
                        verification_url: verification_url.clone(),
                        user_code: user_code.clone(),
                    });
                }
                self.set_status("Complete the provider device authentication in your browser.");
                Some(Vec::new())
            }
            BackendEvent::AuthenticationCompleted { kind, metadata } => {
                if let Some(picker) = &mut self.provider_picker {
                    picker.authentication = None;
                }
                self.set_status("Provider authentication completed.");
                Some(vec![Effect::SaveProviderCredential {
                    provider: provider.to_owned(),
                    kind: kind.clone(),
                    metadata: metadata.clone(),
                }])
            }
            BackendEvent::RequestFailed {
                operation: BackendOperation::Authenticate,
                message,
                ..
            } => {
                self.provider_authentication_failed(provider, message);
                Some(Vec::new())
            }
            _ => None,
        }
    }

    fn sync_active_provider_context(&mut self) {
        let context = self
            .provider_contexts
            .entry(self.backend_provider.clone())
            .or_insert_with(|| ProviderContext {
                name: self.backend_name.clone(),
                capabilities: self.backend_capabilities.clone(),
                connection: self.connection.clone(),
                provider_session_id: None,
                session_id: None,
                context_usage: None,
            });
        context.name.clone_from(&self.backend_name);
        context.capabilities = self.backend_capabilities.clone();
        context.connection = self.connection.clone();
        context
            .provider_session_id
            .clone_from(&self.provider_session_id);
        context.session_id.clone_from(&self.session_id);
        context.context_usage = self.context_usage;
    }

    fn activate_provider(&mut self, provider: &str) -> bool {
        if provider == self.backend_provider {
            return true;
        }
        if self.is_busy() {
            self.set_status("Cannot change provider while a turn is active.");
            return false;
        }
        self.sync_active_provider_context();
        let Some(context) = self.provider_contexts.get(provider).cloned() else {
            self.status_message = format!("Provider {provider} is not available.");
            return false;
        };
        provider.clone_into(&mut self.backend_provider);
        self.backend_name = context.name;
        self.backend_capabilities = context.capabilities;
        self.connection = context.connection;
        self.provider_session_id = context.provider_session_id;
        self.session_id = context.session_id;
        self.context_usage = context.context_usage;
        true
    }

    pub fn handle_backend(&mut self, event: BackendEvent) -> Vec<Effect> {
        let event = match self.reduce_context_compaction_event(event) {
            Ok(effects) => return effects,
            Err(event) => event,
        };
        match event {
            BackendEvent::Ready(identity) => return self.handle_ready(identity),
            BackendEvent::Models(models) => return self.handle_models(models),
            BackendEvent::SessionCreated {
                provider_session_id,
                model,
            } => {
                self.todo_phases.clear();
                return self.handle_session_created(provider_session_id, &model);
            }
            BackendEvent::SessionResumed {
                provider_session_id,
                model,
                history,
            } => {
                self.todo_phases.clear();
                return self.handle_session_resumed(provider_session_id, &model, history);
            }
            BackendEvent::TodoUpdated { phases } => self.todo_phases = phases,
            BackendEvent::AuthenticationChallenge { .. }
            | BackendEvent::AuthenticationCompleted { .. }
            | BackendEvent::ContextUsageUpdated { .. }
            | BackendEvent::ContextCompactionStarted { .. }
            | BackendEvent::ContextCompactionCompleted { .. }
            | BackendEvent::ContextCompactionFailed { .. }
            | BackendEvent::SessionUnsubscribed => {}
            BackendEvent::SessionObserved {
                provider_session_id,
            } => self.observe_session(provider_session_id),
            BackendEvent::TurnAccepted { turn_id } | BackendEvent::TurnStarted { turn_id } => {
                if turn_id.is_empty() {
                    return self.protocol_problem("turn event returned an empty turn id");
                }
                self.observe_turn_started(turn_id);
            }
            BackendEvent::TurnCompleted {
                turn_id,
                outcome,
                error,
            } => return self.complete_turn(&turn_id, outcome, error),
            BackendEvent::ItemStarted { turn_id, item } => {
                self.observe_item(&turn_id, item, false);
            }
            BackendEvent::ItemCompleted { turn_id, item } => {
                self.observe_item(&turn_id, item, true);
            }
            BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind,
                delta,
            } => self.observe_delta(&turn_id, item_id, kind, &delta),
            BackendEvent::TurnDiff { turn_id, diff } => {
                self.observe_turn_artifact(&turn_id, diff, EntryKind::Diff, "TURN DIFF", "diff");
            }
            BackendEvent::TurnPlan { turn_id, plan } => {
                self.observe_turn_artifact(&turn_id, plan, EntryKind::Reasoning, "PLAN", "plan");
            }
            BackendEvent::ApprovalRequested(approval) => {
                self.status_message = format!("Approval required: {}", approval.title);
                self.approvals.push_back(approval);
            }
            BackendEvent::QuestionRequested(request) => self.handle_question_request(request),
            BackendEvent::ApprovalResolved { request_id } => {
                self.resolve_external_approval(&request_id);
            }
            BackendEvent::SteerAccepted { turn_id } => self.handle_steer_accepted(&turn_id),
            BackendEvent::InterruptAccepted => {
                self.set_status("Interrupt accepted; waiting for the turn to stop…");
            }
            BackendEvent::ModelRerouted { turn_id, from, to } => {
                self.handle_model_rerouted(&turn_id, &from, &to);
            }
            BackendEvent::Warning(message) => self.handle_warning(message),
            BackendEvent::TurnError {
                turn_id,
                message,
                will_retry,
            } => self.handle_turn_error(&turn_id, message, will_retry),
            BackendEvent::RequestFailed {
                operation,
                code,
                message,
            } => return self.request_failed(operation, code, message),
            BackendEvent::ProtocolDiagnostic(message) => {
                self.diagnostic_count += 1;
                self.status_message = format!("Protocol diagnostic: {message}");
            }
            BackendEvent::SessionClosed {
                provider_session_id,
            } => self.handle_session_closed(&provider_session_id),
            BackendEvent::Disconnected { reason } => self.handle_disconnected(reason),
        }
        Vec::new()
    }

    fn reduce_context_compaction_event(
        &mut self,
        event: BackendEvent,
    ) -> Result<Vec<Effect>, BackendEvent> {
        match event {
            BackendEvent::ContextUsageUpdated {
                estimated_tokens,
                context_window,
            } => {
                self.context_usage = Some(ContextUsageState {
                    estimated_tokens,
                    context_window,
                });
                Ok(Vec::new())
            }
            BackendEvent::ContextCompactionStarted {
                compaction_id,
                turn_id,
                reason,
                estimated_tokens,
                context_window,
            } => {
                self.context_compaction_started(
                    compaction_id,
                    turn_id,
                    reason,
                    estimated_tokens,
                    context_window,
                );
                Ok(Vec::new())
            }
            BackendEvent::ContextCompactionCompleted {
                compaction_id,
                turn_id,
                estimated_tokens_before,
                estimated_tokens_after,
            } => {
                self.context_compaction_completed(
                    &compaction_id,
                    &turn_id,
                    estimated_tokens_before,
                    estimated_tokens_after,
                );
                Ok(Vec::new())
            }
            BackendEvent::ContextCompactionFailed {
                compaction_id,
                turn_id,
                message,
            } => {
                self.context_compaction_failed(&compaction_id, &turn_id, &message);
                Ok(Vec::new())
            }
            event => Err(event),
        }
    }

    fn handle_ready(&mut self, identity: crate::backend::BackendIdentity) -> Vec<Effect> {
        self.backend_provider = identity.provider;
        self.backend_name = identity.display_name;
        self.backend_capabilities = identity.capabilities;
        self.connection = ConnectionState::Ready {
            server: self.backend_name.clone(),
        };
        self.set_status("Ready.");
        self.startup_resume
            .take()
            .map_or_else(Vec::new, |id| vec![Effect::ResolveSession(id)])
    }

    fn observe_session(&mut self, provider_session_id: String) {
        if self.provider_session_id.is_none() && !provider_session_id.is_empty() {
            self.provider_session_id = Some(provider_session_id);
        }
    }

    fn handle_question_request(&mut self, request: QuestionRequest) {
        self.status_message = format!("Question: {}", request.title);
        let selected = request
            .recommended
            .unwrap_or_default()
            .min(request.options.len().saturating_sub(1));
        let selections = vec![false; request.options.len()];
        self.questions.push_back(QuestionPrompt {
            request,
            selected,
            selections,
        });
    }

    fn resolve_external_approval(&mut self, request_id: &serde_json::Value) {
        if let Some(index) = self
            .approvals
            .iter()
            .position(|approval| &approval.id == request_id)
        {
            self.approvals.remove(index);
            self.set_status("Approval was resolved by another client.");
        }
    }

    fn handle_warning(&mut self, message: String) {
        self.transcript.push(
            EntryKind::Warning,
            "BACKEND WARNING",
            &message,
            EntryStatus::Complete,
        );
        self.status_message = message;
    }

    fn handle_models(&mut self, models: Vec<ModelInfo>) -> Vec<Effect> {
        if models.is_empty() {
            self.pending_model_picker = None;
            if self.models.is_empty() {
                self.install_models(models);
            } else {
                self.set_status("Model refresh returned no choices; kept the cached catalog.");
            }
            return Vec::new();
        }
        let cached = models.clone();
        self.install_models(models);
        if let Some(scope) = self.pending_model_picker {
            self.show_model_picker(scope);
        }
        let mut effects = vec![Effect::PersistModels {
            provider: self.backend_provider.clone(),
            models: cached,
        }];
        if let (Some(session_id), Some(model)) =
            (self.session_id.clone(), self.selected_model.clone())
        {
            effects.push(Effect::UpdateSessionModel {
                session_id,
                model: Some(model),
            });
        }
        effects
    }

    fn handle_session_created(&mut self, provider_session_id: String, model: &str) -> Vec<Effect> {
        if provider_session_id.is_empty() {
            return self.protocol_problem("session creation returned an empty provider id");
        }
        self.provider_session_id = Some(provider_session_id.clone());
        self.context_usage = None;
        self.context_compaction = None;
        self.creating_session = None;
        if !model.is_empty() {
            let qualified = self.qualify_active_model(model);
            if self.selected_model.as_deref() != Some(qualified.as_str()) {
                self.selected_model = Some(qualified.clone());
                self.status_message = format!("{} selected model {qualified}.", self.backend_name);
            }
        }
        let Some(prompt) = self.pending_session_prompt.take() else {
            return Vec::new();
        };
        let mut effects = vec![Effect::PersistSession {
            provider: self.backend_provider.clone(),
            provider_session_id: provider_session_id.clone(),
            workspace: self.workspace.clone(),
            title: prompt.text.clone(),
            model: self.selected_model.clone(),
        }];
        effects.extend(self.start_prompt_on_session(prompt, provider_session_id));
        effects
    }

    fn handle_session_resumed(
        &mut self,
        provider_session_id: String,
        model: &str,
        history: Vec<SessionHistoryItem>,
    ) -> Vec<Effect> {
        let Some(session) = self.resuming_session.take() else {
            return self.protocol_problem("received an unexpected session resume response");
        };
        if provider_session_id.is_empty() {
            return self.protocol_problem("session resume returned an empty provider id");
        }
        self.provider_session_id = Some(provider_session_id);
        self.session_id = Some(session.id.clone());
        self.context_usage = None;
        self.context_compaction = None;
        if !model.is_empty() {
            self.selected_model = Some(self.qualify_active_model(model));
            self.session_model_override = true;
        }
        self.install_history(history);
        self.install_subagents(Vec::new());
        self.status_message = format!("Resumed session {}.", short_id(&session.id));
        vec![
            Effect::TouchSession(session.id.clone()),
            Effect::LoadSubagents(session.id),
        ]
    }

    fn context_compaction_started(
        &mut self,
        compaction_id: String,
        turn_id: String,
        reason: CompactionReason,
        estimated_tokens: usize,
        context_window: Option<usize>,
    ) {
        let expected_manual_compaction = reason == CompactionReason::Manual
            && self.context_compaction.as_ref().is_some_and(|compaction| {
                compaction.id == compaction_id && compaction.turn_id == turn_id
            });
        if !self.turn_is_current(&turn_id) && !expected_manual_compaction {
            self.diagnostic_count += 1;
            return;
        }
        self.context_usage = Some(ContextUsageState {
            estimated_tokens,
            context_window,
        });
        self.context_compaction = Some(ContextCompactionState {
            id: compaction_id.clone(),
            turn_id,
            reason,
            estimated_tokens,
            context_window,
        });
        let (reason_label, title) = match reason {
            CompactionReason::Manual => ("manual compression was requested", "Compressing context"),
            CompactionReason::Proactive => ("proactive threshold reached", "Compacting context"),
            CompactionReason::ContextOverflow => ("context limit reached", "Compacting context"),
        };
        let body = context_window.map_or_else(
            || format!("Reducing approximately {estimated_tokens} tokens because the {reason_label}."),
            |context_window| {
                format!(
                    "Reducing approximately {estimated_tokens} of {context_window} context tokens because the {reason_label}."
                )
            },
        );
        self.transcript.upsert(
            compaction_id,
            EntryKind::System,
            title,
            body,
            EntryStatus::Running,
        );
    }

    fn context_compaction_completed(
        &mut self,
        compaction_id: &str,
        turn_id: &str,
        estimated_tokens_before: usize,
        estimated_tokens_after: usize,
    ) {
        if self.context_compaction.as_ref().is_none_or(|compaction| {
            compaction.turn_id != turn_id || compaction.id != compaction_id
        }) {
            self.diagnostic_count += 1;
            return;
        }
        let context_window = self
            .context_compaction
            .as_ref()
            .and_then(|compaction| compaction.context_window)
            .or_else(|| self.context_usage.and_then(|usage| usage.context_window));
        let compaction_reason = self
            .context_compaction
            .take()
            .map_or(CompactionReason::Proactive, |compaction| compaction.reason);
        self.context_usage = Some(ContextUsageState {
            estimated_tokens: estimated_tokens_after,
            context_window,
        });
        let (reason, title) = match compaction_reason {
            CompactionReason::Manual => (
                "manual context compression was requested",
                "Context compressed",
            ),
            CompactionReason::Proactive => (
                "the proactive context threshold was reached",
                "Context compacted",
            ),
            CompactionReason::ContextOverflow => {
                ("the provider reported a context limit", "Context compacted")
            }
        };
        self.transcript.upsert(
            compaction_id,
            EntryKind::System,
            title,
            format!(
                "Reduced estimated context from {estimated_tokens_before} to {estimated_tokens_after} tokens because {reason}."
            ),
            EntryStatus::Complete,
        );
        if compaction_reason == CompactionReason::Manual {
            self.set_status("Context compressed; ready.");
        }
    }

    fn context_compaction_failed(&mut self, compaction_id: &str, turn_id: &str, message: &str) {
        if self.context_compaction.as_ref().is_none_or(|compaction| {
            compaction.turn_id != turn_id || compaction.id != compaction_id
        }) {
            self.diagnostic_count += 1;
            return;
        }
        let manual = self
            .context_compaction
            .as_ref()
            .is_some_and(|compaction| compaction.reason == CompactionReason::Manual);
        self.context_compaction = None;
        self.diagnostic_count += 1;
        let (title, body) = if manual {
            (
                "Context compression failed",
                format!("Could not compress context: {message}"),
            )
        } else {
            (
                "Context compaction failed",
                format!("Could not compact context: {message}"),
            )
        };
        self.transcript.upsert(
            compaction_id,
            EntryKind::Warning,
            title,
            body,
            EntryStatus::Failed,
        );
        if manual {
            self.status_message = format!("Context compression failed: {message}");
        }
    }

    fn observe_turn_artifact(
        &mut self,
        turn_id: &str,
        body: String,
        kind: EntryKind,
        title: &str,
        suffix: &str,
    ) {
        if self.turn_is_current(turn_id) {
            self.transcript.upsert(
                format!("turn:{turn_id}:{suffix}"),
                kind,
                title,
                body,
                EntryStatus::Running,
            );
        }
    }

    fn handle_steer_accepted(&mut self, turn_id: &str) {
        let Some(pending) = self.pending_steer.take() else {
            self.set_status("A late steer response arrived after the turn ended.");
            return;
        };
        if pending.turn_id != turn_id || !self.turn_is_current(turn_id) {
            self.set_status("A late steer response was ignored; the draft was preserved.");
            return;
        }
        if self.editor.revision() == pending.editor_revision {
            self.editor.clear();
        }
        self.transcript.push(
            EntryKind::Steering,
            format!("STEER · {}", pending.id),
            pending.text,
            EntryStatus::Complete,
        );
        self.set_status("Steering guidance accepted.");
    }

    fn handle_model_rerouted(&mut self, turn_id: &str, from: &str, to: &str) {
        let Some(active) = self
            .active_turn
            .as_mut()
            .filter(|active| active.id == turn_id)
        else {
            self.diagnostic_count += 1;
            return;
        };
        active.model = Some(to.to_owned());
        self.transcript.push(
            EntryKind::Warning,
            "MODEL REROUTED",
            format!(
                "{} changed this turn from {from} to {to}.",
                self.backend_name
            ),
            EntryStatus::Complete,
        );
        self.status_message = format!("Model rerouted to {to}.");
    }

    fn handle_turn_error(&mut self, turn_id: &str, message: String, will_retry: bool) {
        let body = if will_retry {
            format!("{message}\n{} will retry.", self.backend_name)
        } else {
            message.clone()
        };
        let status = if will_retry {
            EntryStatus::Running
        } else {
            EntryStatus::Failed
        };
        self.transcript
            .push(EntryKind::Error, "BACKEND ERROR", body, status);
        self.status_message = if will_retry {
            format!("{} error on {turn_id}; retrying…", self.backend_name)
        } else {
            message
        };
    }

    fn handle_session_closed(&mut self, provider_session_id: &str) {
        if self.provider_session_id.as_deref() != Some(provider_session_id) {
            return;
        }
        let pending_prompt = self
            .pending_session_prompt
            .take()
            .or_else(|| self.starting_turn.take());
        self.transcript.set_stream_active(false);
        self.provider_session_id = None;
        self.active_turn = None;
        self.context_usage = None;
        self.context_compaction = None;
        self.creating_session = None;
        self.pending_steer = None;
        self.approvals.clear();
        self.set_status("The provider session was closed.");
        if let Some(prompt) = pending_prompt {
            self.restore_failed_prompt(&prompt);
        }
    }

    fn handle_disconnected(&mut self, reason: String) {
        let pending_prompt = self
            .pending_session_prompt
            .take()
            .or_else(|| self.starting_turn.take());
        self.transcript.set_stream_active(false);
        self.connection = ConnectionState::Disconnected(reason.clone());
        self.active_turn = None;
        self.context_compaction = None;
        self.creating_session = None;
        self.pending_steer = None;
        self.transcript.push(
            EntryKind::Error,
            "BACKEND DISCONNECTED",
            &reason,
            EntryStatus::Failed,
        );
        self.status_message = reason;
        if let Some(prompt) = pending_prompt {
            self.restore_failed_prompt(&prompt);
        }
    }

    fn take_editor_prompt(&mut self) -> QueuedPrompt {
        let prompt = QueuedPrompt {
            id: self.next_id("msg"),
            text: self.editor.text(),
        };
        self.editor.clear();
        prompt
    }

    fn begin_prompt(&mut self, prompt: QueuedPrompt) -> Vec<Effect> {
        let prompt = OutgoingPrompt {
            id: prompt.id,
            text: prompt.text,
            model: self
                .backend_capabilities
                .model_catalog
                .is_supported()
                .then(|| self.selected_model_for_active_provider())
                .flatten(),
            handoff: self.pending_handoff.take(),
        };
        self.transcript.upsert(
            format!("user:{}", prompt.id),
            EntryKind::User,
            format!("YOU · {}", prompt.id),
            &prompt.text,
            EntryStatus::Complete,
        );
        self.transcript.set_stream_active(true);
        self.scroll_from_bottom = 0;

        if let Some(provider_session_id) = self.provider_session_id.clone() {
            let persist = self.session_id.is_none().then(|| Effect::PersistSession {
                provider: self.backend_provider.clone(),
                provider_session_id: provider_session_id.clone(),
                workspace: self.workspace.clone(),
                title: prompt.text.clone(),
                model: self.selected_model.clone(),
            });
            let mut effects = self.start_prompt_on_session(prompt, provider_session_id);
            if let Some(persist) = persist {
                effects.insert(0, persist);
            }
            effects
        } else {
            self.creating_session = Some(());
            self.pending_session_prompt = Some(prompt.clone());
            self.status_message = format!("Creating a {} session…", self.backend_name);
            vec![Effect::Backend(BackendCommand::StartSession {
                model: prompt.model,
                instructions: Some(self.nakode_system_instructions()),
            })]
        }
    }

    fn start_prompt_on_session(
        &mut self,
        prompt: OutgoingPrompt,
        provider_session_id: String,
    ) -> Vec<Effect> {
        let wire_text = prompt.wire_text();
        self.starting_turn = Some(prompt.clone());
        self.set_status("Starting turn…");
        vec![Effect::Backend(BackendCommand::StartTurn {
            session_id: provider_session_id,
            client_id: prompt.id,
            prompt: wire_text,
            model: prompt.model,
        })]
    }

    fn observe_turn_started(&mut self, turn_id: String) {
        if let Some(active) = &self.active_turn {
            if active.id == turn_id {
                return;
            }
            self.diagnostic_count += 1;
            return;
        }
        let model = self
            .starting_turn
            .take()
            .and_then(|prompt| prompt.model)
            .or_else(|| self.selected_model.clone());
        self.active_turn = Some(ActiveTurn {
            id: turn_id,
            model,
            cancelling: false,
        });
        self.status_message = format!("{} is working…", self.backend_name);
    }

    fn complete_turn(
        &mut self,
        turn_id: &str,
        outcome: TurnOutcome,
        error: Option<String>,
    ) -> Vec<Effect> {
        if self.active_turn.is_none() && self.starting_turn.is_some() {
            self.observe_turn_started(turn_id.to_owned());
        }
        if !self.turn_is_current(turn_id) {
            self.diagnostic_count += 1;
            self.status_message = format!("Ignored completion for inactive turn {turn_id}.");
            return Vec::new();
        }

        let final_item_status = match outcome {
            TurnOutcome::Completed => EntryStatus::Complete,
            TurnOutcome::Interrupted => EntryStatus::Interrupted,
            TurnOutcome::Failed => EntryStatus::Failed,
        };
        let item_ids = self
            .item_turns
            .iter()
            .filter(|(_, item_turn_id)| item_turn_id.as_str() == turn_id)
            .map(|(item_id, _)| item_id.clone())
            .collect::<Vec<_>>();
        for item_id in item_ids {
            self.transcript.set_status(&item_id, final_item_status);
            self.subagent_result_items.remove(&item_id);
        }
        self.reasoning_summaries.remove_turn(turn_id);
        self.transcript
            .set_status(&format!("turn:{turn_id}:diff"), final_item_status);
        self.transcript
            .set_status(&format!("turn:{turn_id}:plan"), final_item_status);
        self.item_turns
            .retain(|_, item_turn_id| item_turn_id != turn_id);

        self.active_turn = None;
        self.context_compaction = None;
        self.starting_turn = None;
        self.transcript.set_stream_active(false);
        if self
            .pending_steer
            .as_ref()
            .is_some_and(|pending| pending.turn_id == turn_id)
        {
            self.pending_steer = None;
            self.set_status("Steer was too late; the draft was preserved.");
        }

        match outcome {
            TurnOutcome::Completed => {
                self.set_status("Turn completed.");
            }
            TurnOutcome::Interrupted => {
                self.transcript.push(
                    EntryKind::System,
                    "TURN INTERRUPTED",
                    "The active turn was cancelled.",
                    EntryStatus::Interrupted,
                );
                self.set_status("Turn interrupted.");
            }
            TurnOutcome::Failed => {
                let message = error.unwrap_or_else(|| "The turn failed.".to_owned());
                self.transcript.push(
                    EntryKind::Error,
                    "TURN FAILED",
                    &message,
                    EntryStatus::Failed,
                );
                self.status_message = message;
            }
        }

        let mut effects = self
            .session_id
            .clone()
            .map(Effect::TouchSession)
            .into_iter()
            .collect::<Vec<_>>();
        effects.extend(self.drain_queue());
        effects
    }

    fn drain_queue(&mut self) -> Vec<Effect> {
        if !self.connection.is_ready() || self.is_busy() {
            return Vec::new();
        }
        let Some(prompt) = self.queue.pop_front() else {
            self.queue_selection = None;
            return Vec::new();
        };
        self.queue_selection = if self.queue.is_empty() { None } else { Some(0) };
        self.begin_prompt(prompt)
    }

    fn install_history(&mut self, history: Vec<SessionHistoryItem>) {
        self.transcript.clear();
        self.item_turns.clear();
        self.reasoning_summaries = ReasoningSummaryTracker::default();
        self.subagent_result_items.clear();
        for history_item in history {
            let item = history_item.item;
            if hides_subagent_item(&item) {
                continue;
            }
            self.item_turns
                .insert(item.id.clone(), history_item.turn_id);
            self.transcript.upsert(
                item.id,
                entry_kind(item.kind),
                item.title,
                item.body,
                entry_status(item.status),
            );
        }
        if self.transcript.entries().is_empty() {
            self.transcript.push(
                EntryKind::System,
                "NAKODE",
                "Resumed session has no visible history.",
                EntryStatus::Complete,
            );
        }
        self.scroll_from_bottom = 0;
    }

    fn observe_item(&mut self, turn_id: &str, item: NormalizedItem, completed: bool) {
        if item.kind == ItemKind::User || !self.turn_is_current(turn_id) {
            return;
        }
        let hides_subagent_result = item.kind == ItemKind::Tool
            && (self.subagent_result_items.contains(&item.id)
                || is_subagent_invocation(&item.title)
                || is_subagent_invocation(&item.body)
                || item.body.contains("[Subagent Result]"));
        if hides_subagent_result {
            self.subagent_result_items.insert(item.id.clone());
        }
        self.item_turns.insert(item.id.clone(), turn_id.to_owned());
        if item.kind == ItemKind::Reasoning
            && self.reasoning_summaries.is_superseded(turn_id, &item.id)
        {
            self.transcript.remove(&item.id);
            return;
        }
        if hides_subagent_result {
            self.transcript.remove(&item.id);
            return;
        }
        let status = if completed {
            entry_status(item.status)
        } else {
            EntryStatus::Running
        };
        let body = if self.reasoning_summaries.contains(turn_id, &item.id) {
            latest_reasoning_summary(&item.body).to_owned()
        } else {
            item.body
        };
        self.transcript
            .upsert(item.id, entry_kind(item.kind), item.title, body, status);
    }

    fn observe_delta(&mut self, turn_id: &str, item_id: String, kind: DeltaKind, delta: &str) {
        if !self.turn_is_current(turn_id) {
            self.diagnostic_count += 1;
            return;
        }
        self.item_turns.insert(item_id.clone(), turn_id.to_owned());
        if self.subagent_result_items.contains(&item_id)
            || (kind == DeltaKind::Tool && delta.contains("[Subagent Result]"))
        {
            self.subagent_result_items.insert(item_id.clone());
            self.transcript.remove(&item_id);
            return;
        }
        let (entry_kind, title) = match kind {
            DeltaKind::ReasoningSummary { index } => {
                record_reasoning_summary_delta(
                    &mut self.transcript,
                    &mut self.reasoning_summaries,
                    turn_id,
                    &item_id,
                    index,
                    delta,
                );
                return;
            }
            DeltaKind::Assistant => (EntryKind::Assistant, "ASSISTANT"),
            DeltaKind::Plan => (EntryKind::Reasoning, "PLAN"),
            DeltaKind::Reasoning => (EntryKind::Reasoning, "REASONING"),
            DeltaKind::Tool => (EntryKind::Tool, "TOOL OUTPUT"),
        };
        self.transcript
            .append_delta(item_id, entry_kind, title, delta);
    }

    fn request_failed(
        &mut self,
        operation: BackendOperation,
        code: i64,
        message: String,
    ) -> Vec<Effect> {
        let display = format!("{} failed ({code}): {message}", operation.label());
        self.transcript.push(
            EntryKind::Error,
            "REQUEST FAILED",
            &display,
            EntryStatus::Failed,
        );
        self.status_message = display;

        match operation {
            BackendOperation::Initialize => {
                self.connection = ConnectionState::Failed(message);
            }
            BackendOperation::Authenticate
            | BackendOperation::ModelList
            | BackendOperation::SetSessionModel
            | BackendOperation::UnsubscribeSession => {}
            BackendOperation::CompactSession => {
                if let Some(compaction) = self.context_compaction.take() {
                    self.transcript
                        .set_status(&compaction.id, EntryStatus::Failed);
                }
            }
            BackendOperation::Reload => {
                self.creating_session = None;
                self.pending_model_picker = None;
            }
            BackendOperation::ResumeSession => {
                self.resuming_session = None;
            }
            BackendOperation::StartSession => {
                if code == -32001 {
                    self.set_status(
                        "Session start timed out; waiting for a definitive backend event.",
                    );
                } else {
                    self.creating_session = None;
                    if let Some(prompt) = self.pending_session_prompt.take() {
                        self.restore_failed_prompt(&prompt);
                    }
                }
            }
            BackendOperation::StartTurn => {
                if code == -32001 {
                    self.set_status(
                        "Turn start timed out; waiting for a definitive backend event.",
                    );
                } else {
                    if let Some(prompt) = self.starting_turn.take() {
                        self.restore_failed_prompt(&prompt);
                    }
                    return self.drain_queue();
                }
            }
            BackendOperation::SteerTurn => {
                self.pending_steer = None;
            }
            BackendOperation::InterruptTurn => {
                if let Some(active) = &mut self.active_turn {
                    active.cancelling = false;
                }
            }
        }
        Vec::new()
    }

    fn restore_failed_prompt(&mut self, prompt: &OutgoingPrompt) {
        self.transcript.set_stream_active(false);
        self.transcript
            .set_status(&format!("user:{}", prompt.id), EntryStatus::Failed);
        self.pending_handoff.clone_from(&prompt.handoff);
        if self.editor.is_blank() {
            self.editor.set_text(&prompt.text);
            self.status_message.push_str(" Draft restored.");
        } else {
            self.status_message
                .push_str(" The original text remains in the transcript.");
        }
    }

    pub fn invoke_agent(&mut self, request: &AgentRequest) -> Vec<Effect> {
        if self
            .subagents
            .iter()
            .filter(|run| {
                matches!(
                    run.status,
                    SubagentStatus::Starting | SubagentStatus::Working
                )
            })
            .count()
            >= MAX_CONCURRENT_SUBAGENTS
        {
            return vec![Effect::CompleteAgentRequest {
                request_id: request.id,
                result: format!(
                    "The concurrent subagent limit ({MAX_CONCURRENT_SUBAGENTS}) is already in use. Wait for a running subagent to finish."
                ),
                success: false,
            }];
        }
        let agent_slug = request.agent.as_str();
        let task = request.task.trim();
        let Some(definition) = self.agents.find(agent_slug).cloned() else {
            return vec![Effect::CompleteAgentRequest {
                request_id: request.id,
                result: format!("Unknown predefined agent {agent_slug:?}."),
                success: false,
            }];
        };
        if task.is_empty() {
            return vec![Effect::CompleteAgentRequest {
                request_id: request.id,
                result: "Agent invocation requires a non-empty task.".to_owned(),
                success: false,
            }];
        }

        let run_id = self.next_id("agent");
        let model_targets = agent_model_targets(&definition, &self.backend_provider);
        let provider = model_targets[0].provider.clone();
        let run = SubagentRun {
            id: run_id.clone(),
            agent: definition.slug.clone(),
            provider: provider.clone(),
            provider_session_id: None,
            objective: task.to_owned(),
            status: SubagentStatus::Starting,
            latest_activity: "Starting provider…".to_owned(),
        };
        self.subagents.push(run.clone());
        self.sync_inline_subagent(&run);
        let mut transcript = Transcript::new(self.transcript_limit);
        transcript.set_stream_label(definition.slug.clone());
        transcript.set_stream_active(true);
        transcript.push(
            EntryKind::User,
            "PARENT",
            definition.initial_prompt(task),
            EntryStatus::Complete,
        );
        self.subagent_chats.insert(
            run_id.clone(),
            SubagentChat {
                transcript,
                scroll_from_bottom: 0,
                reasoning_summaries: ReasoningSummaryTracker::default(),
            },
        );
        self.subagent_executions.insert(
            run_id.clone(),
            SubagentExecution {
                run,
                definition,
                request_id: request.id,
                task: task.to_owned(),
                session_id: None,
                response: String::new(),
                model_targets,
                model_target_index: 0,
            },
        );
        self.status_message = format!("Spawned subagent {agent_slug} as {run_id}.");
        let mut effects = vec![Effect::SpawnSubagent {
            run_id: run_id.clone(),
            provider,
        }];
        if let Some(effect) = self.persist_subagent_effect(&run_id) {
            effects.push(effect);
        }
        effects
    }

    fn nakode_system_instructions(&self) -> String {
        let executable = shell_quote(&self.nakode_executable);
        let agents = self
            .agents
            .definitions()
            .iter()
            .map(|agent| {
                format!(
                    "- {}: {}\n  Command: {} agent {} --session-id={} --task '<bounded task>'",
                    agent.slug,
                    agent.description.trim(),
                    executable,
                    agent.slug,
                    self.nakode_session_id,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let model = self.selected_model.as_ref().map_or_else(
            || format!("{}/provider-default", self.backend_provider),
            |model| format!("{}/{}", self.backend_provider, model),
        );
        format!(
            "[Nakode System Instructions]\nYou are operating inside Nakode.\nSession ID: {}\nModel: {}\nProvider: {}\nNakode invocation is available through the native shell. It is a Nakode control-plane command, not a provider tool.\nAvailable agents:\n{}\nTo delegate a concrete bounded task, execute the matching absolute-path command exactly with the native shell. Do not merely describe delegation when the user asks you to perform it. Do not claim that an agent is unavailable when it is listed above. Use only these Nakode commands for delegation; do not use provider-native subagent or collaboration features because Nakode cannot supervise or attribute those children. Up to {MAX_CONCURRENT_SUBAGENTS} subagents may run concurrently. When several independent tasks would benefit from parallel investigation, launch one command per task concurrently using the provider's native shell facilities. Keep each objective distinct and bounded. Each command returns its own agent result on stdout when that child finishes; incorporate all relevant results into your response.\n[/Nakode System Instructions]",
            self.nakode_session_id,
            model,
            self.backend_provider,
            if agents.is_empty() { "- none" } else { &agents },
        )
    }

    pub fn subagent_launch_failed(&mut self, run_id: &str, message: String) -> Vec<Effect> {
        let mut effects = self.retry_subagent_or_finish(run_id, message);
        if let Some(effect) = self.persist_subagent_effect(run_id) {
            effects.push(effect);
        }
        effects
    }

    pub fn handle_subagent_backend(&mut self, run_id: &str, event: BackendEvent) -> Vec<Effect> {
        if !self.subagent_executions.contains_key(run_id) {
            return Vec::new();
        }
        let persistence_boundary = is_subagent_persistence_boundary(&event);
        let mut effects = self.reduce_subagent_backend(run_id, event);
        if persistence_boundary && let Some(effect) = self.persist_subagent_effect(run_id) {
            effects.push(effect);
        }
        effects
    }

    fn reduce_subagent_backend(&mut self, run_id: &str, event: BackendEvent) -> Vec<Effect> {
        let event = match self.reduce_subagent_compaction_event(run_id, event) {
            Ok(effects) => return effects,
            Err(event) => event,
        };
        let event = match self.reduce_subagent_artifact_event(run_id, event) {
            Ok(effects) => return effects,
            Err(event) => event,
        };
        match event {
            BackendEvent::Ready(_) => self.start_subagent_session(run_id),
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            } => self.start_subagent_turn(run_id, provider_session_id),
            BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind,
                delta,
            } => self.handle_subagent_delta(run_id, &turn_id, &item_id, kind, &delta),
            BackendEvent::ItemStarted { turn_id, item }
            | BackendEvent::ItemCompleted { turn_id, item } => {
                self.record_subagent_item(run_id, &turn_id, &item);
                self.observe_subagent_item(run_id, item);
                Vec::new()
            }
            BackendEvent::ApprovalRequested(approval) => vec![Effect::SubagentBackend {
                run_id: run_id.to_owned(),
                command: BackendCommand::ResolveApproval {
                    id: approval.id,
                    decision: ApprovalDecision::AcceptForSession,
                },
            }],
            BackendEvent::QuestionRequested(request) => vec![Effect::SubagentBackend {
                run_id: run_id.to_owned(),
                command: BackendCommand::ResolveQuestion {
                    id: request.id,
                    answer: "No interactive user is attached to this subagent; continue with best judgment.".to_owned(),
                },
            }],
            BackendEvent::TurnCompleted { outcome, error, .. } => {
                self.complete_subagent_turn(run_id, outcome, error)
            }
            BackendEvent::RequestFailed {
                operation: BackendOperation::StartSession,
                message,
                ..
            }
            | BackendEvent::Disconnected { reason: message } => {
                self.retry_subagent_or_finish(run_id, message)
            }
            BackendEvent::RequestFailed { message, .. }
            | BackendEvent::TurnError {
                message,
                will_retry: false,
                ..
            } => self.fail_subagent(run_id, message),
            BackendEvent::TurnError {
                message,
                will_retry: true,
                ..
            } => {
                self.record_subagent_message(run_id, EntryKind::Warning, "RETRYING", &message);
                Vec::new()
            }
            BackendEvent::Warning(message) | BackendEvent::ProtocolDiagnostic(message) => {
                self.record_subagent_message(run_id, EntryKind::Warning, "WARNING", &message);
                let Some(execution) = self.subagent_executions.get_mut(run_id) else {
                    return Vec::new();
                };
                execution.run.latest_activity = summarize_activity(&message, "Provider warning");
                self.sync_subagent(run_id);
                Vec::new()
            }
            BackendEvent::Models(_)
            | BackendEvent::AuthenticationChallenge { .. }
            | BackendEvent::AuthenticationCompleted { .. }
            | BackendEvent::SessionResumed { .. }
            | BackendEvent::TodoUpdated { .. }
            | BackendEvent::SessionUnsubscribed
            | BackendEvent::SessionObserved { .. }
            | BackendEvent::TurnAccepted { .. }
            | BackendEvent::TurnStarted { .. }
            | BackendEvent::ContextUsageUpdated { .. }
            | BackendEvent::ContextCompactionStarted { .. }
            | BackendEvent::ContextCompactionCompleted { .. }
            | BackendEvent::ContextCompactionFailed { .. }
            | BackendEvent::TurnDiff { .. }
            | BackendEvent::TurnPlan { .. }
            | BackendEvent::ApprovalResolved { .. }
            | BackendEvent::SteerAccepted { .. }
            | BackendEvent::InterruptAccepted
            | BackendEvent::ModelRerouted { .. }
            | BackendEvent::SessionClosed { .. } => Vec::new(),
        }
    }

    fn reduce_subagent_compaction_event(
        &mut self,
        run_id: &str,
        event: BackendEvent,
    ) -> Result<Vec<Effect>, BackendEvent> {
        let activity = match event {
            BackendEvent::ContextCompactionStarted { .. } => "Compacting context…".to_owned(),
            BackendEvent::ContextCompactionCompleted {
                estimated_tokens_before,
                estimated_tokens_after,
                ..
            } => format!(
                "Context compacted ({estimated_tokens_before} → {estimated_tokens_after} estimated tokens)"
            ),
            BackendEvent::ContextCompactionFailed { message, .. } => {
                self.record_subagent_message(
                    run_id,
                    EntryKind::Warning,
                    "COMPACTION FAILED",
                    &message,
                );
                summarize_activity(&message, "Compaction failed")
            }
            event => return Err(event),
        };
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Ok(Vec::new());
        };
        activity.clone_into(&mut execution.run.latest_activity);
        self.sync_subagent(run_id);
        Ok(Vec::new())
    }

    fn reduce_subagent_artifact_event(
        &mut self,
        run_id: &str,
        event: BackendEvent,
    ) -> Result<Vec<Effect>, BackendEvent> {
        let (id, kind, title, body) = match event {
            BackendEvent::TurnDiff { turn_id, diff } => (
                format!("turn:{turn_id}:diff"),
                EntryKind::Diff,
                "DIFF",
                diff,
            ),
            BackendEvent::TurnPlan { turn_id, plan } => (
                format!("turn:{turn_id}:plan"),
                EntryKind::System,
                "PLAN",
                plan,
            ),
            event => return Err(event),
        };
        self.record_subagent_artifact(run_id, id, kind, title, body);
        Ok(Vec::new())
    }

    fn handle_subagent_delta(
        &mut self,
        run_id: &str,
        turn_id: &str,
        item_id: &str,
        kind: DeltaKind,
        delta: &str,
    ) -> Vec<Effect> {
        self.record_subagent_delta(run_id, turn_id, item_id, kind, delta);
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        if kind == DeltaKind::Assistant {
            execution.response.push_str(delta);
            execution.run.latest_activity = summarize_activity(delta, "Responding…");
        } else if kind == DeltaKind::Tool {
            execution.run.latest_activity = summarize_activity(delta, "Using a tool…");
        }
        self.sync_subagent(run_id);
        Vec::new()
    }

    fn complete_subagent_turn(
        &mut self,
        run_id: &str,
        outcome: TurnOutcome,
        error: Option<String>,
    ) -> Vec<Effect> {
        let status = match outcome {
            TurnOutcome::Completed => EntryStatus::Complete,
            TurnOutcome::Interrupted => EntryStatus::Interrupted,
            TurnOutcome::Failed => EntryStatus::Failed,
        };
        self.finish_subagent_transcript(run_id, status);
        match outcome {
            TurnOutcome::Completed => self.finish_subagent(run_id, Ok(())),
            TurnOutcome::Interrupted | TurnOutcome::Failed => self.finish_subagent(
                run_id,
                Err(error.unwrap_or_else(|| "Subagent turn failed.".to_owned())),
            ),
        }
    }

    fn retry_subagent_or_finish(&mut self, run_id: &str, message: String) -> Vec<Effect> {
        let fallback = {
            let Some(execution) = self.subagent_executions.get_mut(run_id) else {
                return Vec::new();
            };
            let next_index = execution.model_target_index.saturating_add(1);
            let Some(target) = execution.model_targets.get(next_index).cloned() else {
                return self.finish_subagent(run_id, Err(message));
            };
            execution.model_target_index = next_index;
            target.provider.clone_into(&mut execution.run.provider);
            execution.run.provider_session_id = None;
            execution.run.status = SubagentStatus::Starting;
            execution.run.latest_activity = format!(
                "Retrying with {} after: {}",
                agent_model_target_label(&target),
                summarize_activity(&message, "provider failure")
            );
            execution.session_id = None;
            execution.response.clear();
            target
        };
        self.record_subagent_message(
            run_id,
            EntryKind::Warning,
            "FALLBACK",
            &format!(
                "The previous model target failed: {message}\nRetrying with {}.",
                agent_model_target_label(&fallback)
            ),
        );
        self.sync_subagent(run_id);
        vec![
            Effect::StopSubagent(run_id.to_owned()),
            Effect::SpawnSubagent {
                run_id: run_id.to_owned(),
                provider: fallback.provider,
            },
        ]
    }

    fn fail_subagent(&mut self, run_id: &str, message: String) -> Vec<Effect> {
        self.record_subagent_message(run_id, EntryKind::Error, "ERROR", &message);
        self.finish_subagent_transcript(run_id, EntryStatus::Failed);
        self.finish_subagent(run_id, Err(message))
    }

    fn start_subagent_session(&mut self, run_id: &str) -> Vec<Effect> {
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        execution.run.status = SubagentStatus::Starting;
        "Creating native session…".clone_into(&mut execution.run.latest_activity);
        let model = execution.model_targets[execution.model_target_index]
            .model
            .clone();
        let instructions = Some(execution.definition.system_prompt.clone());
        self.sync_subagent(run_id);
        vec![Effect::SubagentBackend {
            run_id: run_id.to_owned(),
            command: BackendCommand::StartSession {
                model,
                instructions,
            },
        }]
    }

    fn start_subagent_turn(&mut self, run_id: &str, provider_session_id: String) -> Vec<Effect> {
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        execution.session_id = Some(provider_session_id.clone());
        execution.run.provider_session_id = Some(provider_session_id.clone());
        execution.run.status = SubagentStatus::Working;
        "Working…".clone_into(&mut execution.run.latest_activity);
        let prompt = format!(
            "# Agent role instructions\n\n{}\n\n{}",
            execution.definition.system_prompt.trim(),
            execution.definition.initial_prompt(&execution.task)
        );
        let model = execution.model_targets[execution.model_target_index]
            .model
            .clone();
        self.sync_subagent(run_id);
        vec![Effect::SubagentBackend {
            run_id: run_id.to_owned(),
            command: BackendCommand::StartTurn {
                session_id: provider_session_id,
                client_id: format!("{run_id}-prompt"),
                prompt,
                model,
            },
        }]
    }

    fn record_subagent_delta(
        &mut self,
        run_id: &str,
        turn_id: &str,
        item_id: &str,
        kind: DeltaKind,
        delta: &str,
    ) {
        let Some(chat) = self.subagent_chats.get_mut(run_id) else {
            return;
        };
        let (entry_kind, title) = match kind {
            DeltaKind::ReasoningSummary { index } => {
                record_reasoning_summary_delta(
                    &mut chat.transcript,
                    &mut chat.reasoning_summaries,
                    turn_id,
                    item_id,
                    index,
                    delta,
                );
                return;
            }
            DeltaKind::Assistant => (EntryKind::Assistant, "ASSISTANT"),
            DeltaKind::Reasoning => (EntryKind::Reasoning, "REASONING"),
            DeltaKind::Tool => (EntryKind::Tool, "TOOL"),
            DeltaKind::Plan => (EntryKind::System, "PLAN"),
        };
        chat.transcript
            .append_delta(item_id, entry_kind, title, delta);
    }

    fn record_subagent_item(&mut self, run_id: &str, turn_id: &str, item: &NormalizedItem) {
        let Some(chat) = self.subagent_chats.get_mut(run_id) else {
            return;
        };
        if item.kind == ItemKind::Reasoning
            && chat.reasoning_summaries.is_superseded(turn_id, &item.id)
        {
            chat.transcript.remove(&item.id);
            return;
        }
        let body = if chat.reasoning_summaries.contains(turn_id, &item.id) {
            latest_reasoning_summary(&item.body).to_owned()
        } else {
            item.body.clone()
        };
        chat.transcript.upsert(
            item.id.clone(),
            entry_kind(item.kind),
            item.title.clone(),
            body,
            entry_status(item.status),
        );
    }

    fn record_subagent_artifact(
        &mut self,
        run_id: &str,
        key: String,
        kind: EntryKind,
        title: &str,
        body: String,
    ) {
        let Some(chat) = self.subagent_chats.get_mut(run_id) else {
            return;
        };
        chat.transcript
            .upsert(key, kind, title, body, EntryStatus::Running);
    }

    fn record_subagent_message(&mut self, run_id: &str, kind: EntryKind, title: &str, body: &str) {
        let Some(chat) = self.subagent_chats.get_mut(run_id) else {
            return;
        };
        chat.transcript
            .push(kind, title, body, EntryStatus::Complete);
    }

    fn finish_subagent_transcript(&mut self, run_id: &str, status: EntryStatus) {
        if let Some(chat) = self.subagent_chats.get_mut(run_id) {
            chat.transcript.finish_running(status);
        }
    }

    fn observe_subagent_item(&mut self, run_id: &str, item: NormalizedItem) {
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return;
        };
        match item.kind {
            ItemKind::Assistant if !item.body.is_empty() => {
                execution.response = item.body;
                "Finishing response…".clone_into(&mut execution.run.latest_activity);
            }
            ItemKind::Tool | ItemKind::Diff => {
                execution.run.latest_activity = if item.body.trim().is_empty() {
                    item.title
                } else {
                    summarize_activity(&item.body, &item.title)
                };
            }
            ItemKind::User | ItemKind::Reasoning | ItemKind::System | ItemKind::Assistant => {}
        }
        self.sync_subagent(run_id);
    }

    fn sync_subagent(&mut self, run_id: &str) {
        let Some(run) = self
            .subagent_executions
            .get(run_id)
            .map(|execution| execution.run.clone())
        else {
            return;
        };
        if let Some(displayed) = self
            .subagents
            .iter_mut()
            .find(|displayed| displayed.id == run_id)
        {
            displayed.clone_from(&run);
        }
        self.sync_inline_subagent(&run);
    }

    fn interrupt_subagents(&mut self) -> (usize, Vec<Effect>) {
        let run_ids = self
            .subagent_executions
            .iter()
            .filter(|(_, execution)| {
                matches!(
                    execution.run.status,
                    SubagentStatus::Starting | SubagentStatus::Working
                )
            })
            .map(|(run_id, _)| run_id.clone())
            .collect::<Vec<_>>();
        let mut effects = Vec::with_capacity(run_ids.len() * 2);
        for run_id in &run_ids {
            let Some(mut execution) = self.subagent_executions.remove(run_id) else {
                continue;
            };
            execution.run.status = SubagentStatus::Interrupted;
            "Interrupted by parent".clone_into(&mut execution.run.latest_activity);
            if let Some(displayed) = self.subagents.iter_mut().find(|run| run.id == *run_id) {
                displayed.clone_from(&execution.run);
            }
            self.sync_inline_subagent(&execution.run);
            let result = format!(
                "[Subagent Result] [{}] [{}]\nInterrupted by the parent agent.",
                execution.run.id, execution.run.agent
            );
            if let Some(chat) = self.subagent_chats.get_mut(run_id) {
                chat.transcript.push(
                    EntryKind::System,
                    "INTERRUPTED",
                    "Interrupted by the parent agent.",
                    EntryStatus::Interrupted,
                );
                chat.transcript.finish_running(EntryStatus::Interrupted);
            }
            if let Some(effect) = self.persist_subagent_effect(run_id) {
                effects.push(effect);
            }
            effects.push(Effect::CompleteAgentRequest {
                request_id: execution.request_id,
                result,
                success: false,
            });
            effects.push(Effect::StopSubagent(run_id.clone()));
        }
        (run_ids.len(), effects)
    }

    fn finish_subagent(&mut self, run_id: &str, outcome: Result<(), String>) -> Vec<Effect> {
        let Some(mut execution) = self.subagent_executions.remove(run_id) else {
            return Vec::new();
        };
        let (success, body) = match outcome {
            Ok(()) if !execution.response.trim().is_empty() => {
                execution.run.status = SubagentStatus::Completed;
                "Completed".clone_into(&mut execution.run.latest_activity);
                (true, execution.response.trim().to_owned())
            }
            Ok(()) => {
                execution.run.status = SubagentStatus::Failed;
                "Returned no response".clone_into(&mut execution.run.latest_activity);
                if let Some(chat) = self.subagent_chats.get_mut(run_id) {
                    chat.transcript.push(
                        EntryKind::Error,
                        "ERROR",
                        "Subagent returned no assistant response.",
                        EntryStatus::Failed,
                    );
                    chat.transcript.finish_running(EntryStatus::Failed);
                }
                (false, "Subagent returned no assistant response.".to_owned())
            }
            Err(message) => {
                execution.run.status = SubagentStatus::Failed;
                execution.run.latest_activity = summarize_activity(&message, "Failed");
                if let Some(chat) = self.subagent_chats.get_mut(run_id) {
                    if !chat
                        .transcript
                        .entries()
                        .iter()
                        .any(|entry| entry.kind == EntryKind::Error && entry.body == message)
                    {
                        chat.transcript.push(
                            EntryKind::Error,
                            "ERROR",
                            message.clone(),
                            EntryStatus::Failed,
                        );
                    }
                    chat.transcript.finish_running(EntryStatus::Failed);
                }
                (false, message)
            }
        };
        if let Some(displayed) = self.subagents.iter_mut().find(|run| run.id == run_id) {
            displayed.clone_from(&execution.run);
        }
        self.sync_inline_subagent(&execution.run);
        let result = format!(
            "[Subagent Result] [{}] [{}]\n{}",
            execution.run.id, execution.run.agent, body
        );
        vec![
            Effect::CompleteAgentRequest {
                request_id: execution.request_id,
                result,
                success,
            },
            Effect::StopSubagent(run_id.to_owned()),
        ]
    }

    fn persist_subagent_effect(&self, run_id: &str) -> Option<Effect> {
        let parent_session_id = self.session_id.clone()?;
        let run = self.subagents.iter().find(|run| run.id == run_id)?;
        let chat = self.subagent_chats.get(run_id)?;
        Some(Effect::PersistSubagent(SubagentRecord {
            parent_session_id,
            id: run.id.clone(),
            agent: run.agent.clone(),
            provider: run.provider.clone(),
            provider_session_id: run.provider_session_id.clone(),
            objective: run.objective.clone(),
            status: run.status,
            latest_activity: run.latest_activity.clone(),
            transcript: chat.transcript.entries().to_vec(),
        }))
    }

    fn sync_inline_subagent(&mut self, run: &SubagentRun) {
        let running = matches!(
            run.status,
            SubagentStatus::Starting | SubagentStatus::Working
        );
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

    fn selected_model_for_active_provider(&self) -> Option<String> {
        self.selected_model.as_ref().and_then(|qualified| {
            self.models
                .iter()
                .find(|model| {
                    model.provider == self.backend_provider && model.qualified_id() == *qualified
                })
                .map(|model| model.id.clone())
        })
    }

    fn qualify_active_model(&self, model: &str) -> String {
        if model.contains('/') {
            model.to_owned()
        } else {
            format!("{}/{}", self.backend_provider, model)
        }
    }

    fn install_models(&mut self, models: Vec<ModelInfo>) {
        let providers: std::collections::HashSet<_> =
            models.iter().map(|model| model.provider.clone()).collect();
        self.models
            .retain(|model| !providers.contains(&model.provider));
        self.models.extend(models);
        self.models.sort_by_key(ModelInfo::qualified_id);
        if self.models.is_empty() {
            self.status_message = format!("{} returned an empty model catalog.", self.backend_name);
            self.pending_model_picker = None;
            return;
        }

        if let Some(initial) = self.initial_model.clone() {
            let initial_provider = initial.split_once('/').map(|(provider, _)| provider);
            if initial_provider.is_none_or(|provider| providers.contains(provider)) {
                self.initial_model = None;
                if self
                    .models
                    .iter()
                    .any(|model| model.qualified_id() == initial)
                {
                    self.selected_model = Some(initial);
                } else {
                    let fallback = self.default_model();
                    self.selected_model.clone_from(&fallback);
                    self.status_message = match fallback {
                        Some(fallback) => {
                            format!("Model {initial} is unavailable; using {fallback}.")
                        }
                        None => format!("Model {initial} is unavailable."),
                    };
                }
            }
        } else if self.selected_model.as_ref().is_none_or(|selected| {
            !self
                .models
                .iter()
                .any(|model| &model.qualified_id() == selected)
        }) || (!self.session_model_override
            && self
                .backend_capabilities
                .session_model_config
                .is_supported())
        {
            self.session_model_override = false;
            self.selected_model = self.default_model();
        }
    }

    fn default_model(&self) -> Option<String> {
        self.models
            .iter()
            .filter(|model| model.provider == self.backend_provider)
            .find(|model| model.is_default)
            .or_else(|| {
                self.models
                    .iter()
                    .find(|model| model.provider == self.backend_provider)
            })
            .map(ModelInfo::qualified_id)
    }

    fn protocol_problem(&mut self, message: &str) -> Vec<Effect> {
        self.diagnostic_count += 1;
        message.clone_into(&mut self.status_message);
        self.transcript.push(
            EntryKind::Error,
            "PROTOCOL ERROR",
            message,
            EntryStatus::Failed,
        );
        Vec::new()
    }

    fn turn_is_current(&self, turn_id: &str) -> bool {
        self.active_turn
            .as_ref()
            .is_some_and(|active| active.id == turn_id)
    }

    fn next_id(&mut self, kind: &str) -> String {
        let id = format!("nakode-{kind}-{:06}", self.next_local_id);
        self.next_local_id = self.next_local_id.wrapping_add(1);
        id
    }
}

fn offset_index(index: usize, len: usize, delta: isize) -> usize {
    debug_assert!(len > 0);
    let distance = delta.unsigned_abs() % len;
    if delta.is_negative() {
        (index + len - distance) % len
    } else {
        (index + distance) % len
    }
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

fn entry_kind(kind: ItemKind) -> EntryKind {
    match kind {
        ItemKind::User => EntryKind::User,
        ItemKind::Assistant => EntryKind::Assistant,
        ItemKind::Reasoning => EntryKind::Reasoning,
        ItemKind::Tool => EntryKind::Tool,
        ItemKind::Diff => EntryKind::Diff,
        ItemKind::System => EntryKind::System,
    }
}

fn entry_status(status: ItemStatus) -> EntryStatus {
    match status {
        ItemStatus::Running => EntryStatus::Running,
        ItemStatus::Complete => EntryStatus::Complete,
        ItemStatus::Failed => EntryStatus::Failed,
        ItemStatus::Declined => EntryStatus::Interrupted,
    }
}

fn record_reasoning_summary_delta(
    transcript: &mut Transcript,
    summaries: &mut ReasoningSummaryTracker,
    turn_id: &str,
    item_id: &str,
    index: usize,
    delta: &str,
) {
    let update = summaries.append_delta(turn_id, item_id, index, delta);
    if let Some(replaced_item) = update.replaced_item {
        transcript.remove(&replaced_item);
    }
    transcript.upsert(
        item_id,
        EntryKind::Reasoning,
        "REASONING",
        update.text,
        EntryStatus::Running,
    );
}

fn latest_reasoning_summary(text: &str) -> &str {
    text.lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .unwrap_or_default()
}

fn summarize_activity(text: &str, fallback: &str) -> String {
    let summary = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(fallback);
    summary.chars().take(120).collect()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn agent_model_targets(
    definition: &AgentDefinition,
    parent_provider: &str,
) -> Vec<AgentModelTarget> {
    let mut targets = Vec::with_capacity(definition.fallback_models.len().saturating_add(1));
    if let Some(model) = definition.model.as_deref() {
        push_agent_model_target(&mut targets, model);
    } else {
        targets.push(AgentModelTarget {
            provider: parent_provider.to_owned(),
            model: None,
        });
    }
    for model in &definition.fallback_models {
        push_agent_model_target(&mut targets, model);
    }
    targets
}

fn push_agent_model_target(targets: &mut Vec<AgentModelTarget>, qualified_model: &str) {
    let Some((provider, model)) = qualified_model.split_once('/') else {
        return;
    };
    let target = AgentModelTarget {
        provider: provider.to_owned(),
        model: Some(model.to_owned()),
    };
    if !targets.contains(&target) {
        targets.push(target);
    }
}

fn agent_model_target_label(target: &AgentModelTarget) -> String {
    target.model.as_ref().map_or_else(
        || format!("{}/provider-default", target.provider),
        |model| format!("{}/{model}", target.provider),
    )
}

fn is_subagent_invocation(text: &str) -> bool {
    text.contains("nakode") && text.contains(" agent ")
}

fn hides_subagent_item(item: &NormalizedItem) -> bool {
    item.kind == ItemKind::Tool
        && (is_subagent_invocation(&item.title)
            || is_subagent_invocation(&item.body)
            || item.body.contains("[Subagent Result]"))
}

fn is_subagent_persistence_boundary(event: &BackendEvent) -> bool {
    matches!(
        event,
        BackendEvent::SessionCreated { .. }
            | BackendEvent::ContextCompactionStarted { .. }
            | BackendEvent::ContextCompactionCompleted { .. }
            | BackendEvent::ContextCompactionFailed { .. }
            | BackendEvent::ItemCompleted { .. }
            | BackendEvent::TurnDiff { .. }
            | BackendEvent::TurnPlan { .. }
            | BackendEvent::TurnCompleted { .. }
            | BackendEvent::RequestFailed { .. }
            | BackendEvent::Disconnected { .. }
            | BackendEvent::TurnError { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs, path::Path};

    use crate::{
        agent::AgentCatalog,
        backend::{
            ApprovalKind, ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent,
            BackendIdentity, BackendOperation, CODEX_PROVIDER, CURSOR_PROVIDER, CapabilitySupport,
            CompactionReason, DEVIN_PROVIDER, DeltaKind, ItemKind, ItemStatus, ModelInfo,
            NormalizedItem, QuestionOption, QuestionRequest, SessionHistoryItem, TodoItem,
            TodoPhase, TodoStatus, TurnOutcome,
        },
        session::{SessionRecord, SubagentRecord},
        transcript::{EntryKind, EntryStatus, TranscriptEntry},
    };
    use tempfile::tempdir;

    use super::{AgentRequest, AppState, ApprovalDecision, Effect, SubagentStatus};

    fn explorer_catalog() -> AgentCatalog {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores code context"
system_prompt = "Explore carefully and report concrete context."
first_message = "Inspect the delegated question."
model = "openai-codex/model-a"
"#,
        )
        .expect("agent definition");
        AgentCatalog::load(directory.path()).expect("agent catalog")
    }

    fn ready_state() -> AppState {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "codex-test".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                resume: CapabilitySupport::Supported,
                steering: CapabilitySupport::Supported,
                interruption: CapabilitySupport::Supported,
                model_catalog: CapabilitySupport::Supported,
                models_require_session: CapabilitySupport::Unsupported,
                session_model_config: CapabilitySupport::Unsupported,
                context_compaction: CapabilitySupport::Supported,
                approvals: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                mcp: CapabilitySupport::Supported,
                close_session: CapabilitySupport::Supported,
            },
        }));
        state.handle_backend(BackendEvent::Models(vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-a".to_owned(),
            is_default: true,
        }]));
        state
    }

    #[test]
    fn unsupported_backend_capabilities_are_not_simulated() {
        let mut state = AppState::new_for_backend(
            "/tmp/project",
            None,
            100,
            crate::backend::DEVIN_PROVIDER,
            "Devin",
        );
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: crate::backend::DEVIN_PROVIDER.to_owned(),
            display_name: "Devin".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                interruption: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                approvals: CapabilitySupport::Supported,
                ..BackendCapabilities::default()
            },
        }));
        state.provider_session_id = Some("devin-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("steer");

        assert!(state.steer_editor().is_empty());
        assert_eq!(state.editor.text(), "steer");
        assert!(state.status_message.contains("does not support steering"));

        state.active_turn = None;
        state.editor.set_text("/compress");
        assert!(state.submit_editor().is_empty());
        assert!(
            state
                .status_message
                .contains("does not support manual context compression")
        );
    }

    #[test]
    fn compress_command_requests_manual_compaction_for_the_current_chat() {
        let mut state = ready_state();
        state.provider_session_id = Some("native-session".to_owned());
        state.editor.set_text("/compress");

        let effects = state.submit_editor();

        let [
            Effect::Backend(BackendCommand::CompactSession {
                session_id,
                compaction_id,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected one manual compaction effect");
        };
        assert_eq!(session_id, "native-session");
        let compaction_id = compaction_id.clone();
        let pending = state
            .context_compaction
            .as_ref()
            .expect("pending manual compaction");
        assert_eq!(pending.id, compaction_id);
        assert_eq!(pending.turn_id, compaction_id);
        assert_eq!(pending.reason, CompactionReason::Manual);
        assert!(state.is_busy());
        assert!(state.editor.text().is_empty());
        assert!(state.transcript.entries().iter().any(|entry| {
            entry.key.as_deref() == Some(compaction_id.as_str())
                && entry.title == "Compressing context"
                && entry.status == EntryStatus::Running
        }));

        state.handle_backend(BackendEvent::ContextCompactionStarted {
            compaction_id: compaction_id.clone(),
            turn_id: compaction_id.clone(),
            reason: CompactionReason::Manual,
            estimated_tokens: 42_000,
            context_window: Some(100_000),
        });
        assert_eq!(
            state.context_usage,
            Some(super::ContextUsageState {
                estimated_tokens: 42_000,
                context_window: Some(100_000),
            })
        );
        let interrupt = state.cancel_or_quit();
        assert!(matches!(
            interrupt.as_slice(),
            [Effect::Backend(BackendCommand::InterruptTurn {
                session_id,
                turn_id,
            })] if session_id == "native-session" && turn_id == &compaction_id
        ));

        state.handle_backend(BackendEvent::ContextCompactionCompleted {
            compaction_id: compaction_id.clone(),
            turn_id: compaction_id.clone(),
            estimated_tokens_before: 42_000,
            estimated_tokens_after: 12_000,
        });

        assert!(!state.is_busy());
        assert_eq!(state.status_message, "Context compressed; ready.");
        assert_eq!(
            state.context_usage,
            Some(super::ContextUsageState {
                estimated_tokens: 12_000,
                context_window: Some(100_000),
            })
        );
        assert!(state.transcript.entries().iter().any(|entry| {
            entry.key.as_deref() == Some(compaction_id.as_str())
                && entry.title == "Context compressed"
                && entry.status == EntryStatus::Complete
        }));
    }

    #[test]
    fn compaction_lifecycle_updates_ui_state_without_exposing_the_checkpoint() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-compact".to_owned(),
        });
        state.handle_backend(BackendEvent::ContextCompactionStarted {
            compaction_id: "compact-1".to_owned(),
            turn_id: "turn-compact".to_owned(),
            reason: CompactionReason::Proactive,
            estimated_tokens: 220_000,
            context_window: Some(258_400),
        });

        let compaction = state
            .context_compaction
            .as_ref()
            .expect("active compaction state");
        assert_eq!(compaction.reason, CompactionReason::Proactive);
        assert_eq!(compaction.id, "compact-1");
        let running = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.key.as_deref() == Some("compact-1"))
            .expect("running compaction entry");
        assert_eq!(running.title, "Compacting context");
        assert_eq!(running.status, EntryStatus::Running);
        assert!(running.body.contains("220000 of 258400"));

        state.handle_backend(BackendEvent::ContextCompactionCompleted {
            compaction_id: "compact-1".to_owned(),
            turn_id: "turn-compact".to_owned(),
            estimated_tokens_before: 220_000,
            estimated_tokens_after: 24_000,
        });

        assert!(state.context_compaction.is_none());
        let completed = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.key.as_deref() == Some("compact-1"))
            .expect("completed compaction entry");
        assert_eq!(completed.title, "Context compacted");
        assert_eq!(completed.status, EntryStatus::Complete);
        assert!(completed.body.contains("220000 to 24000"));
        assert!(!state.status_message.contains("ompact"));
    }

    #[test]
    fn compaction_failure_clears_ui_state_and_surfaces_a_warning() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-compact".to_owned(),
        });
        state.handle_backend(BackendEvent::ContextCompactionStarted {
            compaction_id: "compact-failed".to_owned(),
            turn_id: "turn-compact".to_owned(),
            reason: CompactionReason::ContextOverflow,
            estimated_tokens: 300_000,
            context_window: Some(258_400),
        });
        state.handle_backend(BackendEvent::ContextCompactionFailed {
            compaction_id: "compact-failed".to_owned(),
            turn_id: "turn-compact".to_owned(),
            message: "summary request failed".to_owned(),
        });

        assert!(state.context_compaction.is_none());
        assert!(state.transcript.entries().iter().any(|entry| {
            entry.key.as_deref() == Some("compact-failed")
                && entry.title == "Context compaction failed"
                && entry.body.contains("summary request failed")
                && entry.status == EntryStatus::Failed
        }));
        assert!(!state.status_message.contains("ompaction"));
    }

    #[test]
    fn devin_model_picker_lazily_creates_session_and_applies_selection() {
        let mut state = AppState::new_for_backend(
            "/tmp/project",
            None,
            100,
            crate::backend::DEVIN_PROVIDER,
            "Devin",
        );
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: crate::backend::DEVIN_PROVIDER.to_owned(),
            display_name: "Devin".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                model_catalog: CapabilitySupport::Supported,
                models_require_session: CapabilitySupport::Supported,
                session_model_config: CapabilitySupport::Supported,
                interruption: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                approvals: CapabilitySupport::Supported,
                ..BackendCapabilities::default()
            },
        }));

        assert!(matches!(
            state.open_model_picker().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: None })]
        ));
        assert!(state.status_message.contains("Loading Devin models"));
        assert!(state.open_model_picker().is_empty());
        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "devin-session".to_owned(),
            model: "model-a".to_owned(),
        });
        state.handle_backend(BackendEvent::Models(vec![
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-a".to_owned(),
                is_default: true,
            },
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
            },
        ]));
        assert!(state.model_picker.is_some());
        state.picker_move(1);
        assert!(matches!(
            state.picker_select().as_slice(),
            [Effect::Backend(BackendCommand::SetSessionModel { session_id, model })]
                if session_id == "devin-session" && model == "model-b"
        ));

        state.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: Some(session_id) })]
                if session_id == "devin-session"
        ));

        state.editor.set_text("first real prompt");
        let effects = state.submit_editor();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { provider_session_id, model, .. },
                Effect::Backend(BackendCommand::StartTurn { .. })
            ] if provider_session_id == "devin-session" && model.as_deref() == Some("devin-acp/model-b")
        ));
    }

    #[test]
    fn empty_model_refresh_keeps_cached_catalog() {
        let mut state = ready_state();
        let cached = state.models.clone();

        assert!(
            state
                .handle_backend(BackendEvent::Models(Vec::new()))
                .is_empty()
        );
        assert_eq!(state.models, cached);
        assert!(state.status_message.contains("kept the cached catalog"));
    }

    #[test]
    fn queue_drains_fifo_after_terminal_turn_event() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state.editor.set_text("first");
        let first = state.submit_editor();
        assert!(matches!(first.as_slice(), [Effect::Backend(_)]));
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-1".to_owned(),
        });

        state.editor.set_text("second");
        state.submit_editor();
        state.editor.set_text("third");
        state.submit_editor();
        assert_eq!(state.queue.len(), 2);

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        assert_eq!(state.queue.len(), 1);
        assert!(matches!(
            effects.as_slice(),
            [Effect::TouchSession(id), Effect::Backend(_)] if id == "nakode-session-1"
        ));
    }

    #[test]
    fn steer_clears_only_after_acceptance() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("focus on tests");

        let effects = state.steer_editor();
        assert!(matches!(effects.as_slice(), [Effect::Backend(_)]));
        assert_eq!(state.editor.text(), "focus on tests");

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });
        assert!(state.editor.is_blank());
    }

    #[test]
    fn completed_turn_wins_race_with_late_steer_response() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("too late");
        state.steer_editor();

        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.editor.text(), "too late");
        assert!(state.status_message.contains("late steer"));
    }

    #[test]
    fn session_start_timeout_preserves_the_pending_prompt() {
        let mut state = ready_state();
        state.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32001,
            message: "timeout".to_owned(),
        });
        assert!(effects.is_empty());
        assert!(state.is_busy());
        assert!(state.editor.is_blank());

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "thread-late".to_owned(),
            model: "model-a".to_owned(),
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistSession { .. }, Effect::Backend(_)]
        ));
    }

    #[test]
    fn rejected_session_start_restores_the_draft() {
        let mut state = ready_state();
        state.editor.set_text("first");
        state.submit_editor();

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32602,
            message: "rejected".to_owned(),
        });

        assert!(!state.is_busy());
        assert_eq!(state.editor.text(), "first");
        let user = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.kind == crate::transcript::EntryKind::User)
            .expect("failed user entry");
        assert_eq!(user.status, crate::transcript::EntryStatus::Failed);
    }

    #[test]
    fn backend_prompt_failure_completion_clears_active_turn() {
        let mut state = ready_state();
        state.provider_session_id = Some("session-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state.editor.set_text("fail prompt");
        state.submit_editor();
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-failed".to_owned(),
        });
        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32602,
            message: "prompt failed".to_owned(),
        });
        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-failed".to_owned(),
            outcome: TurnOutcome::Failed,
            error: Some("prompt failed".to_owned()),
        });

        assert!(!state.is_busy());
        assert_eq!(state.status_message, "prompt failed");
        assert!(matches!(
            effects.as_slice(),
            [Effect::TouchSession(id)] if id == "nakode-session-1"
        ));
    }

    #[test]
    fn prompt_lifecycle_drives_the_nakode_stream_spinner() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.editor.set_text("inspect the project");
        state.submit_editor();

        let active = state.transcript.visible(80, 10, 0);
        assert!(
            active
                .lines
                .iter()
                .any(|line| line.tone == crate::transcript::LineTone::AgentPending)
        );

        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-1".to_owned(),
        });
        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });

        let complete = state.transcript.visible(80, 10, 0);
        assert!(complete.lines.iter().any(
            |line| line.text == "Nakode" && line.tone == crate::transcript::LineTone::Assistant
        ));
        assert!(
            !complete
                .lines
                .iter()
                .any(|line| line.tone == crate::transcript::LineTone::AgentPending)
        );
    }

    #[test]
    fn start_turn_timeout_does_not_launch_the_next_queued_prompt() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state.editor.set_text("first");
        state.submit_editor();
        state.editor.set_text("second");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32001,
            message: "timeout".to_owned(),
        });
        assert!(effects.is_empty());
        assert!(state.is_busy());
        assert_eq!(state.queue.len(), 1);

        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-late".to_owned(),
        });
        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-late".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::TouchSession(id), Effect::Backend(_)] if id == "nakode-session-1"
        ));
    }

    #[test]
    fn session_close_clears_all_busy_state() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.editor.set_text("first");
        state.submit_editor();
        assert!(state.is_busy());

        state.handle_backend(BackendEvent::SessionClosed {
            provider_session_id: "thread-1".to_owned(),
        });

        assert!(!state.is_busy());
        assert!(state.provider_session_id.is_none());
    }

    #[test]
    fn steer_ack_does_not_clear_a_draft_edited_back_to_the_same_text() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("focus");
        state.steer_editor();
        state.editor.insert_char('x');
        state.editor.backspace();

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.editor.text(), "focus");
    }

    #[test]
    fn approval_decisions_are_provider_neutral() {
        let mut state = ready_state();
        state.approvals.push_back(ApprovalRequest {
            id: serde_json::json!("v2-request"),
            method: "item/commandExecution/requestApproval".to_owned(),
            kind: ApprovalKind::Command,
            title: "command".to_owned(),
            detail: "cargo test".to_owned(),
        });
        let effects = state.resolve_approval(ApprovalDecision::AcceptOnce);
        let [Effect::Backend(BackendCommand::ResolveApproval { decision, .. })] =
            effects.as_slice()
        else {
            panic!("expected approval response");
        };
        assert_eq!(*decision, ApprovalDecision::AcceptOnce);

        state.approvals.push_back(ApprovalRequest {
            id: serde_json::json!("legacy-request"),
            method: "execCommandApproval".to_owned(),
            kind: ApprovalKind::Command,
            title: "command".to_owned(),
            detail: "cargo test".to_owned(),
        });
        let effects = state.resolve_approval(ApprovalDecision::AcceptForSession);
        let [Effect::Backend(BackendCommand::ResolveApproval { decision, .. })] =
            effects.as_slice()
        else {
            panic!("expected approval response");
        };
        assert_eq!(*decision, ApprovalDecision::AcceptForSession);
    }

    #[test]
    fn tool_questions_are_queued_and_resolved_through_backend_commands() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::QuestionRequested(QuestionRequest {
            id: "question-1".to_owned(),
            title: "Direction".to_owned(),
            question: "Which path?".to_owned(),
            options: vec![
                QuestionOption {
                    label: "Direct".to_owned(),
                    description: None,
                },
                QuestionOption {
                    label: "Flexible".to_owned(),
                    description: None,
                },
            ],
            multi: false,
            recommended: None,
        }));

        state.move_question_selection(1);
        assert!(matches!(
            state.resolve_question().as_slice(),
            [Effect::Backend(BackendCommand::ResolveQuestion { id, answer })]
                if id == "question-1" && answer == "[\"Flexible\"]"
        ));
        assert!(state.questions.is_empty());
    }

    #[test]
    fn multi_select_tool_questions_preserve_recommendations_and_descriptions() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::QuestionRequested(QuestionRequest {
            id: "question-2".to_owned(),
            title: "Targets".to_owned(),
            question: "Which targets?".to_owned(),
            options: vec![
                QuestionOption {
                    label: "Library".to_owned(),
                    description: Some("Core implementation".to_owned()),
                },
                QuestionOption {
                    label: "CLI".to_owned(),
                    description: Some("Command-line surface".to_owned()),
                },
            ],
            multi: true,
            recommended: Some(1),
        }));

        assert_eq!(state.questions.front().expect("question").selected, 1);
        state.toggle_question_selection();
        state.move_question_selection(-1);
        state.toggle_question_selection();
        assert!(matches!(
            state.resolve_question().as_slice(),
            [Effect::Backend(BackendCommand::ResolveQuestion { answer, .. })]
                if answer == "[\"Library\",\"CLI\"]"
        ));
    }

    #[test]
    fn successful_connection_does_not_add_transcript_noise() {
        let state = ready_state();
        assert!(state.transcript.entries().is_empty());
    }

    #[test]
    fn reasoning_summaries_shift_in_place_while_reasoning_traces_are_preserved() {
        let mut state = ready_state();
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });

        for (item_id, kind, delta) in [
            ("trace-1", DeltaKind::Reasoning, "Detailed trace"),
            (
                "summary-1",
                DeltaKind::ReasoningSummary { index: 0 },
                "Planning transcript changes",
            ),
            (
                "summary-2",
                DeltaKind::ReasoningSummary { index: 0 },
                "Implementing transcript changes",
            ),
            (
                "summary-2",
                DeltaKind::ReasoningSummary { index: 0 },
                " safely",
            ),
            (
                "summary-2",
                DeltaKind::ReasoningSummary { index: 1 },
                "Running focused tests",
            ),
        ] {
            state.handle_backend(BackendEvent::ItemDelta {
                turn_id: "turn-1".to_owned(),
                item_id: item_id.to_owned(),
                kind,
                delta: delta.to_owned(),
            });
        }

        let entries = state.transcript.entries();
        assert!(entries.iter().any(|entry| {
            entry.key.as_deref() == Some("trace-1") && entry.body == "Detailed trace"
        }));
        assert!(
            entries
                .iter()
                .all(|entry| entry.key.as_deref() != Some("summary-1"))
        );
        assert!(entries.iter().any(|entry| {
            entry.key.as_deref() == Some("summary-2") && entry.body == "Running focused tests"
        }));

        state.handle_backend(BackendEvent::ItemCompleted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: "summary-2".to_owned(),
                kind: ItemKind::Reasoning,
                title: "REASONING".to_owned(),
                body: "Implementing transcript changes\nRunning focused tests\nVerifying results"
                    .to_owned(),
                status: ItemStatus::Complete,
            },
        });
        assert!(state.transcript.entries().iter().any(|entry| {
            entry.key.as_deref() == Some("summary-2") && entry.body == "Verifying results"
        }));

        state.handle_backend(BackendEvent::ItemCompleted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: "summary-1".to_owned(),
                kind: ItemKind::Reasoning,
                title: "REASONING".to_owned(),
                body: "Planning transcript changes".to_owned(),
                status: ItemStatus::Complete,
            },
        });
        assert!(
            state
                .transcript
                .entries()
                .iter()
                .all(|entry| entry.key.as_deref() != Some("summary-1"))
        );
    }

    #[test]
    fn turn_completion_finalizes_running_item_entries() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.handle_backend(BackendEvent::ItemStarted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: "item-1".to_owned(),
                kind: ItemKind::Tool,
                title: "TOOL".to_owned(),
                body: "running".to_owned(),
                status: ItemStatus::Running,
            },
        });
        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });

        let item = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.key.as_deref() == Some("item-1"))
            .expect("tool transcript entry");
        assert_eq!(item.status, crate::transcript::EntryStatus::Complete);
    }

    #[test]
    fn parent_transcript_hides_raw_subagent_command_results() {
        let mut state = ready_state();
        state.provider_session_id = Some("parent-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "parent-turn".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.handle_backend(BackendEvent::ItemStarted {
            turn_id: "parent-turn".to_owned(),
            item: NormalizedItem {
                id: "agent-command".to_owned(),
                kind: ItemKind::Tool,
                title: "bash".to_owned(),
                body: "'/opt/nakode' agent explorer --session-id=session-1".to_owned(),
                status: ItemStatus::Running,
            },
        });
        state.handle_backend(BackendEvent::ItemDelta {
            turn_id: "parent-turn".to_owned(),
            item_id: "agent-command".to_owned(),
            kind: DeltaKind::Tool,
            delta: "[Subagent Result] [run-1] [explorer]\nsecret report".to_owned(),
        });

        assert!(
            state
                .transcript
                .entries()
                .iter()
                .all(|entry| entry.key.as_deref() != Some("agent-command"))
        );
    }

    #[test]
    fn slash_commands_are_not_sent_as_prompts() {
        let mut state = ready_state();
        state.editor.set_text("/resume");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListSessions]
        ));
        assert!(state.session_picker.is_some());

        state.close_session_picker();
        state.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: None })]
        ));

        state.editor.set_text("/resume abc123");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ResolveSession(id)] if id == "abc123"
        ));
    }

    #[test]
    fn configuration_commands_work_without_an_enabled_provider() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);

        state.editor.set_text("/providers");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListProviders]
        ));

        state.close_provider_picker();
        state.editor.set_text("/agents");
        assert!(state.submit_editor().is_empty());
        state.close_agent_picker();
        state.editor.set_text("/settings");
        assert!(state.submit_editor().is_empty());
        let settings = state.settings.as_ref().expect("settings menu");
        assert_eq!(settings.filtered_sections(), super::SettingsSection::ALL);
        state.settings_insert('w');
        state.settings_insert('e');
        state.settings_insert('b');
        assert_eq!(
            state
                .settings
                .as_ref()
                .expect("settings")
                .filtered_sections(),
            vec![super::SettingsSection::Addons]
        );

        state.open_settings();
        state.settings_move(3);
        assert!(state.select_setting().is_empty());
        assert_eq!(
            state.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::Addons)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::CheckAgentBrowser]
        ));
        assert_eq!(
            state.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::WebBrowsing)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::SaveWebConfig(config)]
                if config.backend == crate::web::WebBackend::AgentBrowser
        ));
        assert_eq!(
            state.settings.as_ref().map(|settings| settings.web.backend),
            Some(crate::web::WebBackend::AgentBrowser)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::SaveWebConfig(config)]
                if config.backend == crate::web::WebBackend::Firecrawl
        ));
        state.settings_move(1);
        state.settings_insert('k');
        assert_eq!(
            state
                .settings
                .as_ref()
                .map(|settings| settings.web.firecrawl_api_key.as_str()),
            Some("k")
        );
    }

    #[test]
    fn provider_lifecycle_can_move_from_unconfigured_to_ready_and_back() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        state.provider_starting(CODEX_PROVIDER, "Codex");
        assert_eq!(state.backend_provider, CODEX_PROVIDER);
        assert!(matches!(state.connection, super::ConnectionState::Starting));

        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert!(state.connection.is_ready());

        state.provider_disabled(CODEX_PROVIDER);
        assert!(state.backend_provider.is_empty());
        assert!(!state.connection.is_ready());
    }

    #[test]
    fn resumed_session_rebuilds_transcript_and_touches_metadata() {
        let mut state = ready_state();
        let session = SessionRecord {
            id: "01950000-0000-7000-8000-000000000000".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-resumed".to_owned(),
            workspace: "/tmp/project".to_owned(),
            title: "Previous work".to_owned(),
            model: Some("model-a".to_owned()),
            created_at: 1,
            updated_at: 2,
        };
        assert!(matches!(
            state.begin_resume(session.clone()).as_slice(),
            [Effect::Backend(BackendCommand::ResumeSession { .. })]
        ));

        let effects = state.handle_backend(BackendEvent::SessionResumed {
            provider_session_id: "thread-resumed".to_owned(),
            model: "model-a".to_owned(),
            history: vec![SessionHistoryItem {
                turn_id: "turn-1".to_owned(),
                item: NormalizedItem {
                    id: "user-1".to_owned(),
                    kind: ItemKind::User,
                    title: "YOU".to_owned(),
                    body: "hello".to_owned(),
                    status: ItemStatus::Complete,
                },
            }],
        });

        assert!(matches!(
            effects.as_slice(),
            [Effect::TouchSession(touched), Effect::LoadSubagents(loaded)]
                if touched == &session.id && loaded == &session.id
        ));
        assert_eq!(state.session_id.as_deref(), Some(session.id.as_str()));
        assert_eq!(state.provider_session_id.as_deref(), Some("thread-resumed"));
        assert_eq!(state.transcript.entries()[0].body, "hello");
    }

    #[test]
    fn todo_updates_replace_the_visible_session_projection() {
        let mut state = ready_state();
        let phases = vec![TodoPhase {
            name: "Build".to_owned(),
            tasks: vec![TodoItem {
                content: "Render todos".to_owned(),
                status: TodoStatus::InProgress,
            }],
        }];

        state.handle_backend(BackendEvent::TodoUpdated {
            phases: phases.clone(),
        });
        assert_eq!(state.todo_phases, phases);

        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "new-native-session".to_owned(),
            model: "model-a".to_owned(),
        });
        assert!(state.todo_phases.is_empty());
    }

    #[test]
    fn persisted_subagents_restore_their_clickable_chat_projection() {
        let mut state = ready_state();
        state.session_id = Some("parent-session".to_owned());
        state.install_subagents(vec![SubagentRecord {
            parent_session_id: "parent-session".to_owned(),
            id: "agent-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: Some("child-session".to_owned()),
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![TranscriptEntry {
                key: Some("assistant-1".to_owned()),
                kind: EntryKind::Assistant,
                title: "ASSISTANT".to_owned(),
                body: "The session store owns orchestration metadata.".to_owned(),
                status: EntryStatus::Complete,
            }],
        }]);

        assert_eq!(state.subagents.len(), 1);
        assert_eq!(state.subagents[0].objective, "Map persistence");
        state.subagent_modal = Some("agent-1".to_owned());
        let (transcript, scroll) = state
            .selected_subagent_transcript_mut()
            .expect("restored subagent chat");
        assert_eq!(*scroll, 0);
        assert_eq!(
            transcript.entries()[0].body,
            "The session store owns orchestration metadata."
        );
    }

    #[test]
    fn model_switch_does_not_mutate_active_turn() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
        });
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        let _ = state.open_model_picker();
        state.picker_move(1);
        let _ = state.picker_select();

        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
        assert_eq!(
            state
                .active_turn
                .as_ref()
                .and_then(|turn| turn.model.as_deref()),
            Some("model-a")
        );
    }

    #[test]
    fn models_command_persists_the_default_for_new_sessions() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
        });
        state.editor.set_text("/models");

        assert!(state.submit_editor().is_empty());
        assert_eq!(
            state.model_picker.as_ref().map(|picker| picker.scope),
            Some(super::ModelSelectionScope::Default)
        );
        state.picker_move(1);
        assert!(matches!(
            state.picker_select().as_slice(),
            [Effect::SetDefaultModel { provider, model }]
                if provider == CODEX_PROVIDER && model == "model-b"
        ));

        state.editor.set_text("/new");
        let _ = state.submit_editor();
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
    }

    #[test]
    fn switch_command_applies_only_to_the_current_session() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
        });
        state.editor.set_text("/switch");

        assert!(state.submit_editor().is_empty());
        assert_eq!(
            state.model_picker.as_ref().map(|picker| picker.scope),
            Some(super::ModelSelectionScope::Session)
        );
        state.picker_move(1);
        assert!(state.picker_select().is_empty());
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );

        state.editor.set_text("/new");
        let _ = state.submit_editor();
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-a")
        );
    }

    #[test]
    fn model_picker_merges_provider_qualified_catalogs_and_routes_selection() {
        let mut state = AppState::new("/tmp/project", None, 100);
        for (provider, name) in [(CODEX_PROVIDER, "Codex"), (DEVIN_PROVIDER, "Devin")] {
            state.handle_provider_backend(
                provider,
                BackendEvent::Ready(BackendIdentity {
                    provider: provider.to_owned(),
                    display_name: name.to_owned(),
                    version: None,
                    capabilities: BackendCapabilities {
                        model_catalog: CapabilitySupport::Supported,
                        ..BackendCapabilities::default()
                    },
                }),
            );
        }
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Models(vec![ModelInfo {
                provider: String::new(),
                id: "shared".to_owned(),
                is_default: true,
            }]),
        );
        state.handle_provider_backend(
            DEVIN_PROVIDER,
            BackendEvent::Models(vec![ModelInfo {
                provider: String::new(),
                id: "shared".to_owned(),
                is_default: true,
            }]),
        );

        let _ = state.open_model_picker();
        assert_eq!(
            state
                .filtered_models()
                .iter()
                .map(|model| model.qualified_id())
                .collect::<Vec<_>>(),
            vec!["devin-acp/shared", "openai-codex/shared"]
        );
        state.picker_move(-1);
        let _ = state.picker_select();
        assert_eq!(state.backend_provider, DEVIN_PROVIDER);
        assert_eq!(state.selected_model.as_deref(), Some("devin-acp/shared"));
    }

    #[test]
    fn cross_provider_model_switch_hands_visible_dialogue_to_a_fresh_session() {
        let mut state = AppState::new("/tmp/project", None, 100);
        for (provider, name) in [(CODEX_PROVIDER, "Codex"), (DEVIN_PROVIDER, "Devin")] {
            state.handle_provider_backend(
                provider,
                BackendEvent::Ready(BackendIdentity {
                    provider: provider.to_owned(),
                    display_name: name.to_owned(),
                    version: None,
                    capabilities: BackendCapabilities {
                        model_catalog: CapabilitySupport::Supported,
                        ..BackendCapabilities::default()
                    },
                }),
            );
            state.handle_provider_backend(
                provider,
                BackendEvent::Models(vec![ModelInfo {
                    provider: String::new(),
                    id: "shared".to_owned(),
                    is_default: true,
                }]),
            );
        }
        state.provider_session_id = Some("codex-thread".to_owned());
        state.transcript.push(
            crate::transcript::EntryKind::User,
            "YOU",
            "My name is Quill.",
            crate::transcript::EntryStatus::Complete,
        );
        state.transcript.push(
            crate::transcript::EntryKind::Assistant,
            "ASSISTANT",
            "Nice to meet you.",
            crate::transcript::EntryStatus::Complete,
        );

        let _ = state.open_model_picker();
        state.picker_move(-1);
        assert!(state.picker_select().is_empty());

        assert_eq!(state.backend_provider, DEVIN_PROVIDER);
        assert!(state.provider_session_id.is_none());
        assert!(state.session_id.is_none());
        assert!(state.status_message.contains("continuity handoff"));
        assert!(state.transcript.entries().iter().any(|entry| {
            entry.title == "HANDOFF · Codex → Devin"
                && entry.body.contains("fresh provider-native session")
        }));

        state.editor.set_text("What is my name?");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::Backend(BackendCommand::StartSession { .. })]
        ));
        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "devin-thread".to_owned(),
            model: "shared".to_owned(),
        });
        let [
            Effect::PersistSession { title, .. },
            Effect::Backend(BackendCommand::StartTurn { prompt, .. }),
        ] = effects.as_slice()
        else {
            panic!("expected a persisted target session and its first turn");
        };

        assert_eq!(title, "What is my name?");
        assert!(prompt.contains("# Nakode continuity handoff"));
        assert!(prompt.contains("My name is Quill."));
        assert!(prompt.contains("Nice to meet you."));
        assert!(prompt.ends_with("What is my name?"));
        let displayed_user = state
            .transcript
            .entries()
            .iter()
            .rev()
            .find(|entry| entry.kind == crate::transcript::EntryKind::User)
            .expect("displayed user prompt");
        assert_eq!(displayed_user.body, "What is my name?");
    }

    fn begin_mocked_subagent(state: &mut AppState) -> String {
        let effects = state.invoke_agent(&AgentRequest {
            id: 42,
            agent: "explorer".to_owned(),
            task: "Map auth".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, provider }] = effects.as_slice() else {
            panic!("expected a subagent launch");
        };
        assert_eq!(provider, CODEX_PROVIDER);
        let run_id = run_id.clone();
        assert!(state.has_running_subagents());

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::StartSession {
                    instructions: Some(instructions),
                    ..
                },
                ..
            }] if instructions.contains("Explore carefully")
        ));
        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::SessionCreated {
                provider_session_id: "child-session".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::StartTurn { prompt, .. },
                ..
            }] if prompt.contains("Inspect the delegated question") && prompt.contains("Map auth")
        ));
        run_id
    }

    #[test]
    fn new_session_receives_nakode_identity_and_agent_instructions() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.set_nakode_executable(Path::new("/opt/nakode/bin/nakode"));
        state.selected_model = Some("model-a".to_owned());
        state.editor.set_text("Start work");

        let effects = state.submit_editor();
        let [
            Effect::Backend(BackendCommand::StartSession {
                instructions: Some(instructions),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected session creation with Nakode instructions");
        };

        assert!(instructions.starts_with("[Nakode System Instructions]"));
        assert!(instructions.contains(&format!("Session ID: {}", state.nakode_session_id)));
        assert!(instructions.contains("Model: openai-codex/model-a"));
        assert!(instructions.contains("Provider: openai-codex"));
        assert!(instructions.contains("- explorer: Explores code context"));
        assert!(instructions.contains(&format!(
            "'/opt/nakode/bin/nakode' agent explorer --session-id={}",
            state.nakode_session_id
        )));
        assert!(instructions.contains("execute the matching absolute-path command exactly"));
        assert!(instructions.contains("Up to 4 subagents may run concurrently"));
        assert!(instructions.contains("launch one command per task concurrently"));
        assert!(instructions.contains("do not use provider-native subagent"));
        assert!(instructions.ends_with("[/Nakode System Instructions]"));
    }

    #[test]
    fn bounded_fan_out_accepts_four_independent_subagents() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        let mut run_ids = HashSet::new();

        for request_id in 1..=super::MAX_CONCURRENT_SUBAGENTS {
            let effects = state.invoke_agent(&AgentRequest {
                id: u64::try_from(request_id).expect("bounded request id"),
                agent: "explorer".to_owned(),
                task: format!("Independent investigation {request_id}"),
            });
            let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
                panic!("fan-out request should launch a child");
            };
            assert!(run_ids.insert(run_id.clone()));
        }

        assert_eq!(state.subagents.len(), super::MAX_CONCURRENT_SUBAGENTS);
        assert!(state.has_running_subagents());
        let rejected = state.invoke_agent(&AgentRequest {
            id: 99,
            agent: "explorer".to_owned(),
            task: "One investigation too many".to_owned(),
        });
        assert!(matches!(
            rejected.as_slice(),
            [Effect::CompleteAgentRequest {
                success: false,
                result,
                ..
            }] if result.contains("concurrent subagent limit (4)")
        ));
    }

    #[test]
    fn configured_explorer_routes_to_devin_lightning() {
        let mut state = ready_state();
        let effects = state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, provider }] = effects.as_slice() else {
            panic!("expected explorer launch");
        };
        assert_eq!(provider, DEVIN_PROVIDER);

        let effects = state.handle_subagent_backend(
            run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: DEVIN_PROVIDER.to_owned(),
                display_name: "Devin".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::StartSession {
                    model: Some(model),
                    ..
                },
                ..
            }] if model == "swe-1-7-lightning"
        ));
    }

    #[test]
    fn configured_explorer_falls_back_to_codex_luna() {
        let mut state = ready_state();
        let effects = state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
            panic!("expected explorer launch");
        };
        let run_id = run_id.clone();

        let retry = state.subagent_launch_failed(&run_id, "Devin is unavailable".to_owned());
        assert!(matches!(
            retry.as_slice(),
            [
                Effect::StopSubagent(stopped),
                Effect::SpawnSubagent { run_id: spawned, provider }
            ] if stopped == &run_id && spawned == &run_id && provider == CODEX_PROVIDER
        ));
        assert_eq!(state.subagents[0].provider, CODEX_PROVIDER);
        assert!(state.subagents[0].latest_activity.contains("gpt-5.6-luna"));

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::StartSession {
                    model: Some(model),
                    ..
                },
                ..
            }] if model == "gpt-5.6-luna"
        ));
    }

    #[test]
    fn explorer_falls_back_when_native_session_creation_fails() {
        let mut state = ready_state();
        let effects = state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
            panic!("expected explorer launch");
        };
        let run_id = run_id.clone();
        let _ = state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: DEVIN_PROVIDER.to_owned(),
                display_name: "Devin".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );

        let retry = state.handle_subagent_backend(
            &run_id,
            BackendEvent::RequestFailed {
                operation: BackendOperation::StartSession,
                code: -1,
                message: "model unavailable".to_owned(),
            },
        );
        assert!(matches!(
            retry.as_slice(),
            [
                Effect::StopSubagent(stopped),
                Effect::SpawnSubagent { provider, .. }
            ] if stopped == &run_id && provider == CODEX_PROVIDER
        ));
        let child_chat = state.subagent_chats.get(&run_id).expect("child chat");
        assert!(child_chat.transcript.entries().iter().any(|entry| {
            entry.title == "FALLBACK"
                && entry.body.contains("model unavailable")
                && entry.body.contains("openai-codex/gpt-5.6-luna")
        }));
    }

    #[test]
    fn subagent_invocation_persists_under_the_logical_parent_session() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.session_id = Some("logical-parent".to_owned());

        let effects = state.invoke_agent(&AgentRequest {
            id: 42,
            agent: "explorer".to_owned(),
            task: "Map persistence".to_owned(),
        });

        let [
            Effect::SpawnSubagent { run_id, .. },
            Effect::PersistSubagent(record),
        ] = effects.as_slice()
        else {
            panic!("expected child launch and durable orchestration projection");
        };
        assert_eq!(&record.parent_session_id, "logical-parent");
        assert_eq!(&record.id, run_id);
        assert_eq!(record.objective, "Map persistence");
        assert_eq!(record.transcript.len(), 1);
    }

    #[test]
    fn mocked_subagent_lifecycle_returns_a_parseable_result_to_the_parent() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.provider_session_id = Some("parent-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "parent-turn".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        let run_id = begin_mocked_subagent(&mut state);

        let approval_effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::ApprovalRequested(ApprovalRequest {
                id: serde_json::json!("child-approval"),
                method: "approval".to_owned(),
                kind: ApprovalKind::Command,
                title: "command".to_owned(),
                detail: "test".to_owned(),
            }),
        );
        assert!(matches!(
            approval_effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::ResolveApproval {
                    decision: ApprovalDecision::AcceptForSession,
                    ..
                },
                ..
            }]
        ));

        state.handle_subagent_backend(
            &run_id,
            BackendEvent::ItemStarted {
                turn_id: "child-turn".to_owned(),
                item: NormalizedItem {
                    id: "tool".to_owned(),
                    kind: ItemKind::Tool,
                    title: "cargo test".to_owned(),
                    body: "tests passed".to_owned(),
                    status: ItemStatus::Complete,
                },
            },
        );
        assert!(state.subagents[0].latest_activity.contains("tests passed"));
        state.handle_subagent_backend(
            &run_id,
            BackendEvent::ItemDelta {
                turn_id: "child-turn".to_owned(),
                item_id: "answer".to_owned(),
                kind: DeltaKind::Assistant,
                delta: "No findings.".to_owned(),
            },
        );
        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::TurnCompleted {
                turn_id: "child-turn".to_owned(),
                outcome: TurnOutcome::Completed,
                error: None,
            },
        );
        let [
            Effect::CompleteAgentRequest {
                result, success, ..
            },
            Effect::StopSubagent(stopped_run),
        ] = effects.as_slice()
        else {
            panic!("expected parent result and child shutdown");
        };
        assert!(*success);
        assert!(result.starts_with(&format!("[Subagent Result] [{run_id}] [explorer]")));
        assert!(result.contains("No findings."));
        assert_eq!(stopped_run, &run_id);
        assert_eq!(state.subagents[0].status, SubagentStatus::Completed);
        assert!(!state.has_running_subagents());
        assert!(
            !state
                .transcript
                .entries()
                .iter()
                .any(|entry| entry.body.contains("[Subagent Result]"))
        );
        let child_chat = state.subagent_chats.get(&run_id).expect("child chat");
        assert!(child_chat.transcript.entries().iter().any(|entry| {
            entry.kind == crate::transcript::EntryKind::Assistant && entry.body == "No findings."
        }));
    }

    #[test]
    fn interrupt_stops_a_subagent_when_the_parent_has_no_active_turn() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        let run_id = begin_mocked_subagent(&mut state);

        let effects = state.cancel_or_quit();

        let [
            Effect::CompleteAgentRequest {
                result,
                success: false,
                ..
            },
            Effect::StopSubagent(stopped_run),
        ] = effects.as_slice()
        else {
            panic!("expected interrupted result and immediate child shutdown");
        };
        assert_eq!(stopped_run, &run_id);
        assert!(result.contains("Interrupted by the parent agent"));
        assert_eq!(state.subagents[0].status, SubagentStatus::Interrupted);
        assert!(!state.has_running_subagents());
        assert!(!state.should_quit);
    }

    #[test]
    fn interrupt_stops_the_parent_turn_and_all_subagents_together() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.provider_session_id = Some("parent-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "parent-turn".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        let run_id = begin_mocked_subagent(&mut state);

        let effects = state.cancel_or_quit();

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::CompleteAgentRequest { success: false, .. },
                Effect::StopSubagent(stopped_run),
                Effect::Backend(BackendCommand::InterruptTurn {
                    session_id,
                    turn_id,
                }),
            ] if stopped_run == &run_id
                && session_id == "parent-session"
                && turn_id == "parent-turn"
        ));
        assert!(
            state
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.cancelling)
        );
        assert_eq!(state.subagents[0].status, SubagentStatus::Interrupted);
        assert!(!state.has_running_subagents());
        assert!(state.status_message.contains("active turn and 1 subagent"));
    }

    #[test]
    fn provider_menu_opens_details_before_toggling_state() {
        let mut state = ready_state();
        state.editor.set_text("/providers");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListProviders]
        ));
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: Some(crate::session::ProviderCredentialRecord {
                provider: CODEX_PROVIDER.to_owned(),
                kind: "chatgpt_device_code".to_owned(),
                metadata: serde_json::json!({}),
                updated_at: 1,
            }),
        }]);

        state.open_provider_details();
        assert!(
            state
                .provider_picker
                .as_ref()
                .is_some_and(|picker| picker.showing_details)
        );
        assert!(
            state
                .provider_capabilities(CODEX_PROVIDER)
                .is_some_and(|capabilities| capabilities.resume.is_supported())
        );
        assert!(matches!(
            state.toggle_provider().as_slice(),
            [Effect::SetProviderEnabled { provider, enabled: false }]
                if provider == CODEX_PROVIDER
        ));
        assert!(state.close_provider_details());
        assert!(!state.close_provider_details());
    }

    #[test]
    fn cursor_setup_collects_and_saves_an_api_key_without_starting_oauth() {
        let mut state = ready_state();
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: CURSOR_PROVIDER.to_owned(),
            display_name: "Cursor".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        assert!(!state.provider_api_key_input_active());
        assert!(matches!(
            state.open_provider_authentication_url().as_slice(),
            [Effect::OpenUrl(url)] if url == "https://cursor.com/dashboard/api"
        ));
        assert!(state.toggle_provider().is_empty());
        assert!(state.provider_api_key_input_active());
        state.provider_api_key_insert_str("  cursor-secret-key  ");
        assert!(matches!(
            state.submit_provider_api_key().as_slice(),
            [Effect::SaveProviderCredential { provider, kind, metadata }]
                if provider == CURSOR_PROVIDER
                    && kind == "cursor_api_key"
                    && metadata == &serde_json::json!({"api_key":"cursor-secret-key"})
        ));
        assert!(!state.provider_api_key_input_active());
    }

    #[test]
    fn cursor_api_key_input_rejects_empty_values_and_can_be_cancelled() {
        let mut state = ready_state();
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: CURSOR_PROVIDER.to_owned(),
            display_name: "Cursor".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();
        let _ = state.toggle_provider();

        assert!(state.submit_provider_api_key().is_empty());
        assert!(state.provider_api_key_input_active());
        state.provider_api_key_insert_str("secret");
        let debug = format!("{:?}", state.provider_picker);
        assert!(!debug.contains("secret"));
        state.provider_api_key_backspace();
        assert!(state.cancel_provider_api_key_input());
        assert!(!state.provider_api_key_input_active());
        assert!(!state.cancel_provider_api_key_input());
    }

    #[test]
    fn unconfigured_provider_starts_authentication_before_enablement() {
        let mut state = ready_state();
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        assert!(matches!(
            state.toggle_provider().as_slice(),
            [Effect::AuthenticateProvider(provider)] if provider == CODEX_PROVIDER
        ));
        assert!(matches!(
            state
                .provider_picker
                .as_ref()
                .and_then(|picker| picker.authentication.as_ref()),
            Some(super::ProviderAuthentication::Starting)
        ));

        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::AuthenticationChallenge {
                login_id: "login-1".to_owned(),
                verification_url: "https://example.test/device".to_owned(),
                user_code: "NAKODE-CODE".to_owned(),
            },
        );
        assert!(matches!(
            state
                .provider_picker
                .as_ref()
                .and_then(|picker| picker.authentication.as_ref()),
            Some(super::ProviderAuthentication::Challenge { user_code, .. })
                if user_code == "NAKODE-CODE"
        ));
    }
}
