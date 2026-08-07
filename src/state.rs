use std::fmt::Write as _;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
};

pub(crate) mod projection;

pub use crate::{backend::ApprovalDecision, session::SubagentStatus};

use crate::{
    agent::{AgentCatalog, AgentDefinition, AgentFallbackPolicy, AgentToolProfile},
    backend::{
        ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent, BackendOperation,
        CODEX_PROVIDER, CURSOR_PROVIDER, CompactionReason, DEVIN_PROVIDER, DeltaKind, GLM_PROVIDER,
        ItemKind, ItemStatus, KIMI_PROVIDER, ModelInfo, ModelOptions, NormalizedItem,
        PromptAttachment, QuestionRequest, SessionHistoryItem, TodoPhase, TurnOutcome,
        display_qualified_model_name,
    },
    domain_transcript::{DomainTranscript, EntryKind, EntryStatus},
    handoff::HandoffPackage,
    memory::{MemoryBackend, MemoryConfig},
    personality::PromptAddenda,
    session::{SessionRecord, SubagentObservability, SubagentRecord},
    settings::TerminalImageMode,
    skill::SkillCatalog,
    tools::NAKODE_AGENT_TOOL_NAME,
    web::{WebBackend, WebConfig},
};

#[cfg(test)]
use crate::{
    commands::{self, CommandSpec, ParsedPromptCommand},
    editor::EditorState,
    searchable_dropdown::SearchableDropdown,
    selection::{ScreenPoint, ScreenSnapshot, TextSelection},
    session::ProviderRecord,
    skill::Skill,
};

const MAX_CONCURRENT_SUBAGENTS: usize = 4;

fn model_supports_options(model: &ModelInfo) -> bool {
    let configuration = projection::model_configuration(model);
    configuration.fast_mode_configurable || !configuration.reasoning_efforts.is_empty()
}

fn append_archetype_policy_instructions(instructions: &mut String, policy: &AgentDefinition) {
    let allowed_tools = if policy.allowed_tools.is_empty() {
        "none".to_owned()
    } else {
        policy.allowed_tools.join(", ")
    };
    let denied_tools = if policy.denied_tools.is_empty() {
        "none".to_owned()
    } else {
        policy.denied_tools.join(", ")
    };
    let network = if policy
        .allowed_capabilities
        .iter()
        .any(|name| name == "network")
    {
        "allowed"
    } else {
        "denied"
    };
    let file_writes = if policy
        .allowed_tools
        .iter()
        .any(|name| name == "write" || name == "edit" || name == "bash")
    {
        "allowed only through listed tools"
    } else {
        "denied"
    };
    let delegation = if policy.can_delegate {
        "allowed"
    } else {
        "denied"
    };
    let _ = write!(
        instructions,
        "\n\n[Nakode Archetype Policy]\nTool profile: {:?}\nAllowed tools: {allowed_tools}\nDenied tools: {denied_tools}\nNetwork is {network}. File writes are {file_writes}. Recursive delegation is {delegation} (maximum depth {}). Parent attribution is required.\nExpected task shape: {}\nOutput contract: {}\n[/Nakode Archetype Policy]",
        policy.tool_profile, policy.max_delegation_depth, policy.task_shape, policy.output_contract,
    );
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptCompletion<'a> {
    Command(&'static CommandSpec),
    Skill(&'a Skill),
}

#[cfg(test)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Starting,
    Ready { server: String },
    Failed(String),
    Disconnected(String),
}

impl ConnectionState {
    #[must_use]
    #[cfg(test)]
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
    pub attachments: Vec<PromptAttachment>,
}

/// A prompt that the server definitively failed to start and can offer to any
/// connected client for an explicit retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoverablePrompt {
    pub(crate) id: String,
    pub(crate) text: String,
    pub(crate) attachments: Vec<PromptAttachment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutgoingPrompt {
    id: String,
    text: String,
    wire_text: String,
    model: Option<String>,
    handoff: Option<HandoffPackage>,
    attachments: Vec<PromptAttachment>,
}

impl OutgoingPrompt {
    fn wire_text(&self) -> String {
        let mut text = self.handoff.as_ref().map_or_else(
            || self.wire_text.clone(),
            |handoff| handoff.render_with_prompt(&self.wire_text),
        );
        let paths = self
            .attachments
            .iter()
            .filter_map(|attachment| {
                attachment
                    .path
                    .as_ref()
                    .map(|path| format!("- [{}]: {}", attachment.label, path.display()))
            })
            .collect::<Vec<_>>();
        if !paths.is_empty() {
            text.push_str("\n\nAttached local files:\n");
            text.push_str(&paths.join("\n"));
        }
        text
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingSteer {
    id: String,
    text: String,
    turn_id: String,
    queued_origin: Option<QueuedSteerOrigin>,
    #[cfg(test)]
    editor_revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueuedSteerOrigin {
    prompt_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingRedirect {
    prompt_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RedirectStart {
    prompt: QueuedPrompt,
    index: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelPickerStage {
    Models,
    Options,
}

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelSelectionScope {
    Default,
    Session,
    Vision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelSelectionTarget {
    ProviderDefault,
    Session,
    Vision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionPrompt {
    pub request: QuestionRequest,
    #[cfg(test)]
    pub selected: usize,
    #[cfg(test)]
    pub selections: Vec<bool>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SessionPicker {
    pub sessions: Vec<SessionRecord>,
    pub selected: usize,
    pub loading: bool,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct ProviderPicker {
    pub providers: Vec<ProviderRecord>,
    pub selected: usize,
    pub loading: bool,
    pub showing_details: bool,
    pub authentication: Option<ProviderAuthentication>,
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentEditorField {
    Slug,
    Description,
    SystemPrompt,
    FirstMessage,
    Model,
    FallbackModels,
}

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentModelOption {
    Inherit,
    Model(ModelInfo),
}

#[cfg(test)]
impl AgentModelOption {
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Inherit => "Inherit parent model".to_owned(),
            Self::Model(model) => model.display_name(),
        }
    }

    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::Inherit => "uses the parent session model".to_owned(),
            Self::Model(model) => model.qualified_id(),
        }
    }

    #[must_use]
    pub fn search_text(&self) -> String {
        format!("{} {}", self.label(), self.detail())
    }

    #[must_use]
    fn qualified_id(&self) -> Option<String> {
        match self {
            Self::Inherit => None,
            Self::Model(model) => Some(model.qualified_id()),
        }
    }
}

#[cfg(test)]
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
    pub pending_fast_mode: Option<bool>,
    pub model_dropdown: Option<SearchableDropdown<AgentModelOption>>,
}

#[cfg(test)]
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
            pending_fast_mode: None,
            model_dropdown: None,
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
            fast_mode: definition.fast_mode,
            pending_fast_mode: None,
            model_dropdown: None,
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
            fast_mode: self.fast_mode,
            reasoning_effort: None,
            ..AgentDefinition::default()
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct AgentPicker {
    pub agents: Vec<AgentDefinition>,
    pub selected: usize,
    pub editor: Option<AgentEditor>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsSection {
    General,
    Agents,
    Models,
    Addons,
}

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsView {
    Menu,
    Addons,
    WebBrowsing,
    Vision,
    Memory,
    TerminalImages,
}

/// A reusable history for hierarchical menus. Callers store the complete node
/// state needed to restore focus rather than reconstructing a parent menu.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct MenuHistory<T> {
    previous: Vec<T>,
}

#[cfg(test)]
impl<T> Default for MenuHistory<T> {
    fn default() -> Self {
        Self {
            previous: Vec::new(),
        }
    }
}

#[cfg(test)]
impl<T> MenuHistory<T> {
    fn push(&mut self, node: T) {
        self.previous.push(node);
    }

    fn pop(&mut self) -> Option<T> {
        self.previous.pop()
    }

    fn clear(&mut self) {
        self.previous.clear();
    }

    fn is_empty(&self) -> bool {
        self.previous.is_empty()
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SettingsNode {
    view: SettingsView,
    selected: usize,
    addon_field: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentBrowserStatus {
    Checking,
    Available(String),
    Unavailable,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsState {
    pub query: String,
    pub selected: usize,
    pub view: SettingsView,
    pub web: WebConfig,
    pub vision: crate::vision::VisionConfig,
    pub memory: MemoryConfig,
    pub terminal_images: TerminalImageMode,
    pub addon_field: usize,
    pub agent_browser_status: AgentBrowserStatus,
    history: MenuHistory<SettingsNode>,
}

#[cfg(test)]
impl SettingsState {
    fn enter(&mut self, view: SettingsView, selected: usize, addon_field: usize) {
        self.history.push(SettingsNode {
            view: self.view,
            selected: self.selected,
            addon_field: self.addon_field,
        });
        self.view = view;
        self.selected = selected;
        self.addon_field = addon_field;
    }

    fn back(&mut self) -> bool {
        let Some(node) = self.history.pop() else {
            return false;
        };
        self.view = node.view;
        self.selected = node.selected;
        self.addon_field = node.addon_field;
        true
    }

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
enum ProviderAuthenticationState {
    Starting,
    #[cfg(test)]
    ApiKeyRequired {
        dashboard_url: String,
        credential_kind: String,
    },
    Challenge {
        verification_url: String,
        user_code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentRun {
    pub id: String,
    pub agent: String,
    pub provider: String,
    pub model: Option<String>,
    pub provider_session_id: Option<String>,
    pub usage: crate::backend::BackendTokenUsage,
    pub objective: String,
    pub status: SubagentStatus,
    pub latest_activity: String,
    pub observability: SubagentObservability,
}

#[derive(Clone, Debug, Default)]
struct ReasoningSummaryTracker {
    turns: HashMap<String, ReasoningSummaryTurn>,
}

#[derive(Clone, Debug, Default)]
struct ReasoningSummaryTurn {
    latest_item: Option<String>,
    streams: HashMap<String, ReasoningSummaryStream>,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct SubagentChat {
    transcript: DomainTranscript,
    reasoning_summaries: ReasoningSummaryTracker,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct SubagentHitRegion {
    run_id: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct OAuthLinkHitRegion {
    url: String,
    top_left: ScreenPoint,
    bottom_right: ScreenPoint,
}

#[cfg(test)]
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
    parent_run_id: Option<String>,
    remaining_delegation_depth: u32,
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

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DomainCommandError {
    #[error("invalid command: {0}")]
    Invalid(String),
    #[error("command conflicts with current state: {0}")]
    Conflict(String),
    #[error("resource was not found: {0}")]
    NotFound(String),
    #[error("capability is unsupported: {0}")]
    Unsupported(String),
}

#[derive(Clone, Debug)]
pub enum Effect {
    Backend(BackendCommand),
    RunShell {
        id: String,
        command: String,
    },
    CancelShell(String),
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
    #[cfg(test)]
    ListSessions,
    #[cfg(test)]
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
    ReloadProvider(String),
    #[cfg(test)]
    OpenUrl(String),
    SaveAgent {
        definition: AgentDefinition,
        previous_slug: Option<String>,
    },
    DeleteAgent(String),
    /// Shuts down every provider backend supervising one logical session.
    ///
    /// Emitted by `DeleteSession` for a session that was still attached, because the protocol has
    /// no close command for a caller to issue first. Ordered BEFORE `DeleteSession` so the provider
    /// child is gone before the history it was writing to is.
    ReleaseSessionBackends(String),
    /// Removes a logical session and its persisted history. Routed with the persistence effects.
    DeleteSession(String),
    ReloadConfiguration,
    #[cfg(test)]
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
    SaveModelOptions {
        provider: String,
        model: String,
        options: ModelOptions,
    },
    PersistSubagent(SubagentRecord),
    LoadSubagents(String),
    UpdateSessionModel {
        session_id: String,
        model: Option<String>,
    },
    TouchSession(String),
    SaveWebConfig(WebConfig),
    SaveMemoryConfig(MemoryConfig),
    SaveVisionConfig(crate::vision::VisionConfig),
    SaveTerminalImageMode(TerminalImageMode),
    CheckAgentBrowser,
    #[cfg(test)]
    Quit,
}

#[cfg(test)]
#[derive(Clone, Debug)]
enum MenuSnapshot {
    Settings(SettingsState),
}

#[cfg(test)]
/// State owned by one interactive client rather than the Nakode service.
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
    menu_history: MenuHistory<MenuSnapshot>,
    command_completion_selection: usize,
    pending_model_picker: Option<ModelSelectionScope>,
    pub show_help: bool,
    pub text_selection: Option<TextSelection>,
    pub scroll_from_bottom: usize,
    subagent_scroll_from_bottom: HashMap<String, usize>,
    pub subagent_modal: Option<String>,
    screen_snapshot: Option<ScreenSnapshot>,
    pending_clipboard: Option<String>,
    subagent_hit_regions: Vec<SubagentHitRegion>,
    oauth_link_hit_region: Option<OAuthLinkHitRegion>,
    api_key_input_hit_region: Option<ApiKeyInputHitRegion>,
}

#[derive(Clone, Debug)]
pub struct DomainState {
    #[cfg(test)]
    pub client: ClientPresentationState,
    pub connection: ConnectionState,
    pub workspace: String,
    pub backend_provider: String,
    pub backend_name: String,
    pub backend_capabilities: BackendCapabilities,
    provider_contexts: HashMap<String, ProviderContext>,
    provider_authentication: HashMap<String, ProviderAuthenticationState>,
    pub provider_session_id: Option<String>,
    pub session_id: Option<String>,
    pub active_turn: Option<ActiveTurn>,
    pub context_usage: Option<ContextUsageState>,
    pub provider_usage: crate::backend::BackendTokenUsage,
    pub context_compaction: Option<ContextCompactionState>,
    pub transcript: DomainTranscript,
    active_shells: HashSet<String>,
    pub queue: VecDeque<QueuedPrompt>,
    pub models: Vec<ModelInfo>,
    pub selected_model: Option<String>,
    pub model_options: HashMap<String, ModelOptions>,
    default_model_options: ModelOptions,
    session_model_override: bool,
    session_model_options_override: Option<(String, ModelOptions)>,
    pub approvals: VecDeque<ApprovalRequest>,
    pub questions: VecDeque<QuestionPrompt>,
    pub external_tool_calls: Vec<crate::backend::ExternalToolRequest>,
    external_tools: Vec<nakode_protocol::ExternalToolDefinition>,
    replace_builtin_tools: bool,
    pub todo_phases: Vec<TodoPhase>,
    pub status_message: String,
    pub diagnostic_count: usize,
    pub nakode_session_id: String,
    nakode_executable: String,
    pub subagents: Vec<SubagentRun>,
    #[cfg(test)]
    pub should_quit: bool,
    creating_session: Option<()>,
    pending_session_prompt: Option<OutgoingPrompt>,
    starting_turn: Option<OutgoingPrompt>,
    recoverable_prompt: Option<RecoverablePrompt>,
    pending_steer: Option<PendingSteer>,
    /// A queued follow-up reserved in place and promoted after interruption.
    pending_redirect: Option<PendingRedirect>,
    /// A promoted follow-up removed from the queue only while its replacement turn is starting.
    redirect_start: Option<RedirectStart>,
    pending_handoff: Option<HandoffPackage>,
    resuming_session: Option<SessionRecord>,
    item_turns: HashMap<String, String>,
    reasoning_summaries: ReasoningSummaryTracker,
    subagent_result_items: HashSet<String>,
    initial_model: Option<String>,
    agents: AgentCatalog,
    skills: SkillCatalog,
    prompt_addenda: PromptAddenda,
    agent_directory: PathBuf,
    subagent_executions: HashMap<String, SubagentExecution>,
    subagent_chats: HashMap<String, SubagentChat>,
    transcript_limit: usize,
    web_config: WebConfig,
    memory_config: MemoryConfig,
    vision_config: crate::vision::VisionConfig,
    terminal_image_mode: TerminalImageMode,
    agent_browser_status: AgentBrowserStatus,
}

/// Legacy test-only alias for reducer tests that still exercise old client
/// convenience methods.
#[cfg(test)]
pub type AppState = DomainState;

impl DomainState {
    pub(crate) fn synchronize_workspace_configuration(&mut self, source: &Self) {
        let mut local_contexts = std::mem::take(&mut self.provider_contexts);
        self.provider_contexts = source
            .provider_contexts
            .iter()
            .map(|(provider, shared)| {
                let local = local_contexts.remove(provider);
                (
                    provider.clone(),
                    ProviderContext {
                        name: shared.name.clone(),
                        capabilities: shared.capabilities.clone(),
                        connection: shared.connection.clone(),
                        provider_session_id: local
                            .as_ref()
                            .and_then(|context| context.provider_session_id.clone()),
                        session_id: local
                            .as_ref()
                            .and_then(|context| context.session_id.clone()),
                        context_usage: local.and_then(|context| context.context_usage),
                    },
                )
            })
            .collect();
        let active_provider = if self.provider_contexts.contains_key(&self.backend_provider)
            && !self.backend_provider.is_empty()
        {
            Some(self.backend_provider.clone())
        } else if self
            .provider_contexts
            .contains_key(&source.backend_provider)
            && !source.backend_provider.is_empty()
        {
            Some(source.backend_provider.clone())
        } else {
            self.provider_contexts.keys().min().cloned()
        };
        if let Some(active_provider) = active_provider {
            let provider_changed = active_provider != self.backend_provider;
            let active = self
                .provider_contexts
                .get(&active_provider)
                .expect("selected provider context exists");
            let name = active.name.clone();
            let capabilities = active.capabilities.clone();
            let connection = active.connection.clone();
            let provider_session_id = active.provider_session_id.clone();
            let session_id = active.session_id.clone();
            let context_usage = active.context_usage;
            self.backend_provider = active_provider;
            self.backend_name = name;
            self.backend_capabilities = capabilities;
            self.connection = connection;
            if provider_changed {
                self.provider_session_id = provider_session_id;
                self.session_id = session_id;
                self.context_usage = context_usage;
            }
        } else {
            self.backend_provider.clear();
            "No provider".clone_into(&mut self.backend_name);
            self.backend_capabilities = BackendCapabilities::default();
            self.connection = ConnectionState::Disconnected("no provider enabled".to_owned());
            self.provider_session_id = None;
            self.session_id = None;
            self.context_usage = None;
        }
        self.provider_authentication
            .clone_from(&source.provider_authentication);
        self.models.clone_from(&source.models);
        self.model_options.clone_from(&source.model_options);
        self.default_model_options
            .clone_from(&source.default_model_options);
        self.agents.clone_from(&source.agents);
        self.skills.clone_from(&source.skills);
        self.prompt_addenda.clone_from(&source.prompt_addenda);
        self.agent_directory.clone_from(&source.agent_directory);
        self.nakode_executable.clone_from(&source.nakode_executable);
        self.web_config.clone_from(&source.web_config);
        self.memory_config.clone_from(&source.memory_config);
        self.vision_config.clone_from(&source.vision_config);
        self.terminal_image_mode = source.terminal_image_mode;
        self.agent_browser_status
            .clone_from(&source.agent_browser_status);
    }

    pub fn install_model_options(&mut self, provider: &str, model: &str, options: ModelOptions) {
        self.model_options
            .insert(format!("{provider}/{model}"), options);
    }

    pub fn install_model_option_profiles(
        &mut self,
        provider: &str,
        profiles: Vec<(String, ModelOptions)>,
    ) {
        for (model, options) in profiles {
            self.install_model_options(provider, &model, options);
        }
    }

    pub fn set_default_model_options(&mut self, options: ModelOptions) {
        self.default_model_options = options;
    }

    pub fn install_terminal_image_mode(&mut self, mode: TerminalImageMode) {
        self.terminal_image_mode = mode;
    }

    #[must_use]
    pub const fn terminal_image_mode(&self) -> TerminalImageMode {
        self.terminal_image_mode
    }

    pub fn install_vision_config(&mut self, config: crate::vision::VisionConfig) {
        self.vision_config = config;
    }

    pub fn install_web_config(&mut self, config: WebConfig) {
        self.web_config = config;
    }

    pub fn install_memory_config(&mut self, config: MemoryConfig) {
        self.memory_config = config;
    }

    #[cfg(test)]
    pub fn open_settings(&mut self) {
        self.client.settings = Some(SettingsState {
            query: String::new(),
            selected: 0,
            view: SettingsView::Menu,
            web: self.web_config.clone(),
            vision: self.vision_config.clone(),
            memory: self.memory_config.clone(),
            terminal_images: self.terminal_image_mode,
            addon_field: 0,
            agent_browser_status: self.agent_browser_status.clone(),
            history: MenuHistory::default(),
        });
        self.client.menu_history.clear();
        self.set_status("Settings opened.");
    }

    #[cfg(test)]
    pub fn close_settings(&mut self) {
        self.client.settings = None;
        self.restore_previous_menu();
    }

    #[cfg(test)]
    pub fn close_all_menus(&mut self) {
        self.client.model_picker = None;
        self.client.session_picker = None;
        self.client.provider_picker = None;
        self.client.agent_picker = None;
        self.client.settings = None;
        self.client.pending_model_picker = None;
        self.client.menu_history.clear();
    }

    #[must_use]
    #[cfg(test)]
    pub fn current_menu_has_parent(&self) -> bool {
        !self.client.menu_history.is_empty()
    }

    #[cfg(test)]
    fn suspend_settings(&mut self) {
        if let Some(settings) = self.client.settings.take() {
            self.client
                .menu_history
                .push(MenuSnapshot::Settings(settings));
        }
    }

    #[cfg(test)]
    fn restore_previous_menu(&mut self) -> bool {
        let Some(menu) = self.client.menu_history.pop() else {
            return false;
        };
        match menu {
            MenuSnapshot::Settings(mut settings) => {
                settings.web = self.web_config.clone();
                settings.vision = self.vision_config.clone();
                settings.terminal_images = self.terminal_image_mode;
                self.client.settings = Some(settings);
            }
        }
        true
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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
            if settings.view == SettingsView::Menu || settings.view == SettingsView::Addons {
                settings.selected = offset_index(settings.selected, length, delta);
            } else {
                settings.addon_field = offset_index(settings.addon_field, length, delta);
            }
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn settings_cycle_terminal_images(&mut self, delta: isize) {
        let Some(settings) = &mut self.client.settings else {
            return;
        };
        if settings.view != SettingsView::TerminalImages {
            return;
        }
        let index = TerminalImageMode::ALL
            .iter()
            .position(|mode| *mode == settings.terminal_images)
            .unwrap_or_default();
        settings.terminal_images =
            TerminalImageMode::ALL[offset_index(index, TerminalImageMode::ALL.len(), delta)];
    }

    #[cfg(test)]
    pub fn save_terminal_image_mode(&mut self) -> Vec<Effect> {
        let Some(mode) = self
            .client
            .settings
            .as_ref()
            .map(|settings| settings.terminal_images)
        else {
            return Vec::new();
        };
        vec![Effect::SaveTerminalImageMode(mode)]
    }

    #[cfg(test)]
    pub fn settings_cycle_choice(&mut self, delta: isize) -> Vec<Effect> {
        match self.client.settings.as_ref().map(|settings| settings.view) {
            Some(SettingsView::WebBrowsing) => {
                self.settings_cycle_web_backend(delta);
                self.save_web_settings()
            }
            Some(SettingsView::Memory) => {
                self.settings_cycle_memory_backend(delta);
                self.save_memory_settings()
            }
            Some(SettingsView::TerminalImages) => {
                self.settings_cycle_terminal_images(delta);
                self.save_terminal_image_mode()
            }
            _ => Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn select_setting(&mut self) -> Vec<Effect> {
        if self
            .client
            .settings
            .as_ref()
            .is_some_and(|settings| settings.view == SettingsView::WebBrowsing)
        {
            self.settings_cycle_web_backend(1);
            return self.save_web_settings();
        }
        if self.client.settings.as_ref().is_some_and(|settings| {
            settings.view == SettingsView::Memory && settings.addon_field == 0
        }) {
            self.settings_cycle_memory_backend(1);
            return self.save_memory_settings();
        }
        if self
            .client
            .settings
            .as_ref()
            .is_some_and(|settings| settings.view == SettingsView::Memory)
        {
            return Vec::new();
        }
        if let Some(settings) = &mut self.client.settings
            && settings.view == SettingsView::Addons
        {
            let (view, effects) = match settings.selected {
                0 => (SettingsView::WebBrowsing, vec![Effect::CheckAgentBrowser]),
                1 => (SettingsView::Vision, Vec::new()),
                2 => (SettingsView::Memory, Vec::new()),
                _ => (SettingsView::TerminalImages, Vec::new()),
            };
            settings.enter(view, 0, 0);
            if view == SettingsView::WebBrowsing {
                settings.agent_browser_status = AgentBrowserStatus::Checking;
            }
            return effects;
        }
        if self
            .client
            .settings
            .as_ref()
            .is_some_and(|settings| settings.view == SettingsView::TerminalImages)
        {
            self.settings_cycle_terminal_images(1);
            return self.save_terminal_image_mode();
        }
        if self
            .client
            .settings
            .as_ref()
            .is_some_and(|settings| settings.view == SettingsView::Vision)
        {
            self.suspend_settings();
            let effects = self.open_vision_model_picker();
            if self.client.model_picker.is_none() {
                self.restore_previous_menu();
            }
            return effects;
        }
        let section = self
            .client
            .settings
            .as_ref()
            .and_then(|settings| settings.filtered_sections().get(settings.selected).copied());
        match section {
            Some(SettingsSection::General) => {
                self.suspend_settings();
                self.client.provider_picker = Some(ProviderPicker {
                    providers: Vec::new(),
                    selected: 0,
                    loading: true,
                    showing_details: false,
                    authentication: None,
                });
                vec![Effect::ListProviders]
            }
            Some(SettingsSection::Agents) => {
                self.suspend_settings();
                self.open_agent_picker();
                Vec::new()
            }
            Some(SettingsSection::Models) => {
                self.suspend_settings();
                let effects = self.open_default_model_picker();
                if self.client.model_picker.is_none() && self.client.pending_model_picker.is_none()
                {
                    self.restore_previous_menu();
                }
                effects
            }
            Some(SettingsSection::Addons) => {
                if let Some(settings) = &mut self.client.settings {
                    settings.enter(SettingsView::Addons, 0, 0);
                }
                Vec::new()
            }
            None => Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn disable_vision_addon(&mut self) -> Vec<Effect> {
        if self
            .client
            .settings
            .as_ref()
            .is_none_or(|settings| settings.view != SettingsView::Vision)
        {
            return Vec::new();
        }
        self.vision_config.model = None;
        if let Some(settings) = &mut self.client.settings {
            settings.vision = self.vision_config.clone();
        }
        self.set_status("Vision add-on disabled.");
        vec![Effect::SaveVisionConfig(self.vision_config.clone())]
    }

    #[cfg(test)]
    pub fn settings_back(&mut self) -> Vec<Effect> {
        let Some(view) = self.client.settings.as_ref().map(|settings| settings.view) else {
            return Vec::new();
        };
        let effects = match view {
            SettingsView::WebBrowsing => self.save_web_settings(),
            SettingsView::Memory => self.save_memory_settings(),
            _ => Vec::new(),
        };
        if self
            .client
            .settings
            .as_mut()
            .is_some_and(SettingsState::back)
        {
            return effects;
        }
        self.client.settings = None;
        self.restore_previous_menu();
        effects
    }

    pub fn set_agent_browser_status(&mut self, status: AgentBrowserStatus) {
        self.agent_browser_status = status;
    }

    #[cfg(test)]
    pub fn save_web_settings(&mut self) -> Vec<Effect> {
        let Some(settings) = &self.client.settings else {
            return Vec::new();
        };
        let config = settings.web.clone();
        self.set_status("Saving browser add-on settings…");
        vec![Effect::SaveWebConfig(config)]
    }

    #[cfg(test)]
    pub fn save_memory_settings(&mut self) -> Vec<Effect> {
        let Some(settings) = self.client.settings.as_ref() else {
            return Vec::new();
        };
        let config = settings.memory.clone();
        self.set_status("Saving memory add-on settings…");
        vec![Effect::SaveMemoryConfig(config)]
    }

    pub fn set_status(&mut self, message: &str) {
        self.status_message.clear();
        self.status_message.push_str(message);
    }

    #[cfg(test)]
    pub fn insert_attachments(&mut self, attachments: Vec<PromptAttachment>) {
        if attachments.is_empty() {
            return;
        }
        for attachment in &attachments {
            let text = self.client.editor.text();
            if !text.is_empty() && !text.ends_with(char::is_whitespace) {
                self.client.editor.insert_char(' ');
            }
            self.client
                .editor
                .insert_str(&format!("[{}]", attachment.label));
            self.client.editor.insert_char(' ');
        }
        let count = attachments.len();
        self.client.draft_attachments.extend(attachments);
        self.status_message = if count == 1 {
            "Attached 1 file.".to_owned()
        } else {
            format!("Attached {count} files.")
        };
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
        let transcript = DomainTranscript::new(scrollback);
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
            #[cfg(test)]
            client: ClientPresentationState::default(),
            connection: ConnectionState::Starting,
            workspace: workspace.into(),
            backend_provider: provider,
            backend_name: backend_name.clone(),
            backend_capabilities: BackendCapabilities::default(),
            provider_contexts,
            provider_authentication: HashMap::new(),
            provider_session_id: None,
            session_id: None,
            active_turn: None,
            context_usage: None,
            provider_usage: crate::backend::BackendTokenUsage::default(),
            context_compaction: None,
            transcript,
            active_shells: HashSet::new(),
            queue: VecDeque::new(),
            models: Vec::new(),
            selected_model: initial_model.clone(),
            model_options: HashMap::new(),
            default_model_options: ModelOptions {
                reasoning_effort: Some("medium".to_owned()),
                fast_mode: false,
            },
            session_model_override: initial_model.is_some(),
            session_model_options_override: None,
            approvals: VecDeque::new(),
            questions: VecDeque::new(),
            external_tool_calls: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            todo_phases: Vec::new(),
            status_message: format!("Connecting to {backend_name}…"),
            diagnostic_count: 0,
            nakode_session_id: uuid::Uuid::now_v7().to_string(),
            nakode_executable: "nakode".to_owned(),
            subagents: Vec::new(),
            #[cfg(test)]
            should_quit: false,
            creating_session: None,
            pending_session_prompt: None,
            starting_turn: None,
            recoverable_prompt: None,
            pending_steer: None,
            pending_redirect: None,
            redirect_start: None,
            pending_handoff: None,
            resuming_session: None,
            item_turns: HashMap::new(),
            reasoning_summaries: ReasoningSummaryTracker::default(),
            subagent_result_items: HashSet::new(),
            initial_model,
            agents: AgentCatalog::default(),
            skills: SkillCatalog::default(),
            prompt_addenda: PromptAddenda::default(),
            agent_directory: PathBuf::from(".nakode/agents"),
            subagent_executions: HashMap::new(),
            subagent_chats: HashMap::new(),
            transcript_limit: scrollback,
            web_config: WebConfig::default(),
            memory_config: MemoryConfig::default(),
            vision_config: crate::vision::VisionConfig::default(),
            terminal_image_mode: TerminalImageMode::default(),
            agent_browser_status: AgentBrowserStatus::Unavailable,
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
        "No provider is enabled. Configure a provider to continue."
            .clone_into(&mut state.status_message);
        state
    }

    pub fn install_agents(&mut self, agents: AgentCatalog) {
        self.agents = agents;
    }

    pub fn install_prompt_addenda(&mut self, prompt_addenda: PromptAddenda) {
        self.prompt_addenda = prompt_addenda;
    }

    /// Reloads personality and Soul content from their original sources.
    ///
    /// # Errors
    ///
    /// Returns an error when either source can no longer be read or parsed.
    pub fn reload_prompt_addenda(&mut self) -> Result<(), crate::personality::PromptAddendaError> {
        self.prompt_addenda = self.prompt_addenda.reload()?;
        Ok(())
    }

    pub fn install_skills(&mut self, skills: SkillCatalog) {
        self.skills = skills;
    }

    pub fn configuration_reloaded(
        &mut self,
        agent_count: usize,
        skill_count: usize,
        refreshing_backend: bool,
    ) {
        self.status_message = if refreshing_backend {
            format!(
                "Reloaded {skill_count} skills and {agent_count} agents; refreshing {} metadata…",
                self.backend_name
            )
        } else {
            format!("Reloaded {skill_count} skills and {agent_count} agents.")
        };
    }

    pub fn configuration_reload_failed(&mut self, error: &str) {
        self.creating_session = None;
        self.status_message = format!("Reload failed: {error}");
    }

    pub fn set_agent_directory(&mut self, directory: PathBuf) {
        self.agent_directory = directory;
    }

    #[must_use]
    pub fn agent_directory(&self) -> &Path {
        &self.agent_directory
    }

    #[cfg(test)]
    pub fn open_agent_picker(&mut self) {
        self.client.agent_picker = Some(AgentPicker {
            agents: self.agents.definitions().to_vec(),
            selected: 0,
            editor: None,
        });
        self.set_status("Agent archetypes opened.");
    }

    #[cfg(test)]
    pub fn close_agent_picker(&mut self) {
        self.client.agent_picker = None;
        if !self.restore_previous_menu() {
            self.set_status("Agent settings closed.");
        }
    }

    #[cfg(test)]
    pub fn agent_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.agent_picker else {
            return;
        };
        if !picker.agents.is_empty() && picker.editor.is_none() {
            picker.selected = offset_index(picker.selected, picker.agents.len(), delta);
        }
    }

    #[cfg(test)]
    pub fn edit_selected_agent(&mut self) {
        let Some(picker) = &mut self.client.agent_picker else {
            return;
        };
        if let Some(definition) = picker.agents.get(picker.selected) {
            picker.editor = Some(AgentEditor::from_definition(definition));
        }
    }

    #[cfg(test)]
    pub fn create_agent(&mut self) {
        if let Some(picker) = &mut self.client.agent_picker {
            picker.editor = Some(AgentEditor::new());
        }
    }

    #[cfg(test)]
    pub fn cancel_agent_edit(&mut self) -> bool {
        let Some(picker) = &mut self.client.agent_picker else {
            return false;
        };
        let Some(editor) = &mut picker.editor else {
            return false;
        };
        if editor.pending_fast_mode.take().is_some() {
            return true;
        }
        if editor.model_dropdown.take().is_some() {
            return true;
        }
        picker.editor = None;
        true
    }

    #[must_use]
    #[cfg(test)]
    pub fn agent_model_options_are_open(&self) -> bool {
        self.client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .is_some_and(|editor| editor.pending_fast_mode.is_some())
    }

    #[cfg(test)]
    pub fn adjust_agent_model_options(&mut self, delta: isize) {
        if delta == 0 {
            return;
        }
        if let Some(fast_mode) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .and_then(|editor| editor.pending_fast_mode.as_mut())
        {
            *fast_mode = !*fast_mode;
        }
    }

    #[cfg(test)]
    pub fn apply_agent_model_options(&mut self) -> Vec<Effect> {
        let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        else {
            return Vec::new();
        };
        if let Some(fast_mode) = editor.pending_fast_mode.take() {
            editor.fast_mode = fast_mode;
        }
        self.autosave_agent_edit()
    }

    #[must_use]
    #[cfg(test)]
    pub fn agent_model_dropdown_is_open(&self) -> bool {
        self.client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .is_some_and(|editor| editor.model_dropdown.is_some())
    }

    #[cfg(test)]
    pub fn open_agent_model_dropdown(&mut self) {
        let Some((field, current)) = self
            .client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .map(|editor| (editor.field, editor.model.trim().to_owned()))
        else {
            return;
        };
        if field != AgentEditorField::Model {
            return;
        }

        let mut items = vec![AgentModelOption::Inherit];
        items.extend(self.models.iter().cloned().map(AgentModelOption::Model));
        if !current.is_empty()
            && !items
                .iter()
                .any(|option| option.qualified_id().as_deref() == Some(current.as_str()))
            && let Some((provider, id)) = current.split_once('/')
            && !provider.is_empty()
            && !id.is_empty()
        {
            items.push(AgentModelOption::Model(ModelInfo {
                provider: provider.to_owned(),
                id: id.to_owned(),
                is_default: false,
                capabilities: crate::backend::ModelCapabilities::default(),
            }));
        }
        let selected = items
            .iter()
            .position(|option| option.qualified_id().as_deref() == Some(current.as_str()))
            .unwrap_or(0);
        if let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            editor.model_dropdown = Some(SearchableDropdown::with_selected(items, selected));
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn select_agent_model_dropdown(&mut self) -> Vec<Effect> {
        let selected = self
            .client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .and_then(|editor| editor.model_dropdown.as_ref())
            .and_then(|dropdown| {
                dropdown
                    .selected_item(AgentModelOption::search_text)
                    .cloned()
            });
        let Some(selected) = selected else {
            return Vec::new();
        };
        let supports_fast_mode = match &selected {
            AgentModelOption::Model(model) => {
                model.provider == CURSOR_PROVIDER && model_supports_options(model)
            }
            AgentModelOption::Inherit => self
                .selected_model
                .as_deref()
                .and_then(|qualified| {
                    self.models
                        .iter()
                        .find(|model| model.qualified_id() == qualified)
                })
                .is_some_and(|model| {
                    model.provider == CURSOR_PROVIDER && model_supports_options(model)
                }),
        };
        if let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            editor.model = selected.qualified_id().unwrap_or_default();
            editor.model_dropdown = None;
            editor.pending_fast_mode = supports_fast_mode.then_some(editor.fast_mode);
            if !supports_fast_mode {
                editor.fast_mode = false;
            }
        }
        if supports_fast_mode {
            Vec::new()
        } else {
            self.autosave_agent_edit()
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn agent_editor_insert(&mut self, character: char) -> Vec<Effect> {
        let mut edited_field = false;
        if let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            if let Some(dropdown) = &mut editor.model_dropdown {
                dropdown.insert(character);
            } else {
                editor.value_mut().push(character);
                edited_field = true;
            }
        }
        if edited_field {
            self.autosave_agent_edit()
        } else {
            Vec::new()
        }
    }

    #[cfg(test)]
    pub fn agent_editor_insert_str(&mut self, text: &str) -> Vec<Effect> {
        let mut edited_field = false;
        if let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            if let Some(dropdown) = &mut editor.model_dropdown {
                dropdown.insert_str(text);
            } else {
                editor.value_mut().push_str(text);
                edited_field = true;
            }
        }
        if edited_field {
            self.autosave_agent_edit()
        } else {
            Vec::new()
        }
    }

    #[cfg(test)]
    pub fn agent_editor_backspace(&mut self) -> Vec<Effect> {
        let mut edited_field = false;
        if let Some(editor) = self
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
        {
            if let Some(dropdown) = &mut editor.model_dropdown {
                dropdown.backspace();
            } else {
                editor.value_mut().pop();
                edited_field = true;
            }
        }
        if edited_field {
            self.autosave_agent_edit()
        } else {
            Vec::new()
        }
    }

    #[cfg(test)]
    fn autosave_agent_edit(&self) -> Vec<Effect> {
        let Some(editor) = self
            .client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
        else {
            return Vec::new();
        };
        let definition = editor.definition();
        if AgentCatalog::validate_definition(&definition).is_err() {
            return Vec::new();
        }
        vec![Effect::SaveAgent {
            definition,
            previous_slug: editor.original_slug.clone(),
        }]
    }

    #[cfg(test)]
    pub fn delete_selected_agent(&mut self) -> Vec<Effect> {
        self.client
            .agent_picker
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
    #[cfg(test)]
    pub fn selected_subagent_summary(&self) -> Option<(String, String)> {
        let run_id = self.client.subagent_modal.as_deref()?;
        let run = self.subagents.iter().find(|run| run.id == run_id)?;
        Some((run.agent.clone(), run.objective.clone()))
    }

    #[must_use]
    #[cfg(test)]
    pub fn selected_subagent_transcript(&self) -> Option<&DomainTranscript> {
        let run_id = self.client.subagent_modal.as_deref()?;
        self.subagent_chats.get(run_id).map(|chat| &chat.transcript)
    }

    #[must_use]
    #[cfg(test)]
    pub fn selected_subagent_scroll(&self) -> usize {
        self.client
            .subagent_modal
            .as_ref()
            .and_then(|run_id| self.client.subagent_scroll_from_bottom.get(run_id))
            .copied()
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn set_selected_subagent_scroll(&mut self, scroll: usize) {
        if let Some(run_id) = self.client.subagent_modal.clone() {
            self.client
                .subagent_scroll_from_bottom
                .insert(run_id, scroll);
        }
    }

    #[cfg(test)]
    pub fn selected_subagent_transcript_mut(
        &mut self,
    ) -> Option<(&mut DomainTranscript, &mut usize)> {
        let run_id = self.client.subagent_modal.clone()?;
        let chat = self.subagent_chats.get_mut(&run_id)?;
        let scroll = self
            .client
            .subagent_scroll_from_bottom
            .entry(run_id)
            .or_default();
        Some((&mut chat.transcript, scroll))
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn open_subagent_at(&mut self, point: ScreenPoint) -> bool {
        let Some(run_id) = self
            .client
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
        self.client.subagent_modal = Some(run_id);
        self.clear_text_selection();
        true
    }

    #[cfg(test)]
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

    #[must_use]
    #[cfg(test)]
    pub fn oauth_url_at(&self, point: ScreenPoint) -> Option<String> {
        self.client
            .oauth_link_hit_region
            .as_ref()
            .filter(|region| {
                point.column >= region.top_left.column
                    && point.column < region.bottom_right.column
                    && point.row >= region.top_left.row
                    && point.row < region.bottom_right.row
            })
            .map(|region| region.url.clone())
    }

    #[cfg(test)]
    pub fn set_api_key_input_hit_region(&mut self, region: Option<(ScreenPoint, ScreenPoint)>) {
        self.client.api_key_input_hit_region =
            region.map(|(top_left, bottom_right)| ApiKeyInputHitRegion {
                top_left,
                bottom_right,
            });
    }

    #[cfg(test)]
    pub fn focus_provider_api_key_at(&mut self, point: ScreenPoint) -> bool {
        let contains = self.client.api_key_input_hit_region.is_some_and(|region| {
            point.column >= region.top_left.column
                && point.column < region.bottom_right.column
                && point.row >= region.top_left.row
                && point.row < region.bottom_right.row
        });
        contains && self.focus_provider_api_key()
    }

    #[cfg(test)]
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
        self.set_status("Editing provider API key.");
        true
    }

    #[cfg(test)]
    pub fn open_provider_authentication_url(&mut self) -> Vec<Effect> {
        let Some(url) = self.provider_authentication_url().map(str::to_owned) else {
            self.set_status("No provider authentication URL is available.");
            return Vec::new();
        };
        vec![Effect::OpenUrl(url)]
    }

    #[cfg(test)]
    pub fn copy_provider_authentication_url(&mut self) -> Vec<Effect> {
        let Some(url) = self.provider_authentication_url().map(str::to_owned) else {
            self.set_status("No provider authentication URL is available.");
            return Vec::new();
        };
        self.client.pending_clipboard = Some(url);
        Vec::new()
    }

    #[cfg(test)]
    fn provider_authentication_url(&self) -> Option<&str> {
        let picker = self.client.provider_picker.as_ref()?;
        if !picker.showing_details {
            return None;
        }
        match picker.authentication.as_ref()? {
            ProviderAuthentication::Challenge {
                verification_url, ..
            } => Some(verification_url),
            ProviderAuthentication::ApiKeyInput { .. } => picker
                .providers
                .get(picker.selected)
                .and_then(|provider| crate::backend::api_key_provider_setup(&provider.provider))
                .map(|setup| setup.dashboard_url),
            ProviderAuthentication::Starting => None,
        }
    }

    #[cfg(test)]
    pub fn close_subagent_modal(&mut self) {
        self.client.subagent_modal = None;
        self.clear_text_selection();
    }

    #[cfg(test)]
    pub fn scroll_subagent_modal(&mut self, delta: isize) {
        let Some((_, scroll)) = self.selected_subagent_transcript_mut() else {
            return;
        };
        *scroll = scroll.saturating_add_signed(delta);
    }

    #[cfg(test)]
    pub fn scroll_active_chat(&mut self, delta: isize) {
        if self.client.subagent_modal.is_some() {
            self.scroll_subagent_modal(delta);
        } else {
            self.client.scroll_from_bottom =
                self.client.scroll_from_bottom.saturating_add_signed(delta);
        }
    }

    #[cfg(test)]
    pub fn reset_subagent_scroll(&mut self) {
        if let Some((_, scroll)) = self.selected_subagent_transcript_mut() {
            *scroll = 0;
        }
    }

    #[cfg(test)]
    pub fn reset_active_chat_scroll(&mut self) {
        if self.client.subagent_modal.is_some() {
            self.reset_subagent_scroll();
        } else {
            self.client.scroll_from_bottom = 0;
        }
    }

    #[cfg(test)]
    pub fn install_sessions(&mut self, sessions: Vec<SessionRecord>) {
        let picker = self.client.session_picker.get_or_insert(SessionPicker {
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
        for mut record in records {
            let mut status = record.status;
            let mut latest_activity = record.latest_activity;
            if matches!(status, SubagentStatus::Starting | SubagentStatus::Working) {
                status = SubagentStatus::Interrupted;
                "Interrupted when the previous server stopped".clone_into(&mut latest_activity);
                record.observability.ended_at_ms = Some(unix_time_ms());
                record.observability.termination_kind = Some("interrupted".to_owned());
                record.observability.termination_detail =
                    Some("Interrupted when the previous server stopped".to_owned());
            }
            let mut transcript = DomainTranscript::new(self.transcript_limit);
            transcript.set_stream_label(record.agent.clone());
            for entry in record.transcript {
                transcript.restore(entry);
            }
            if status == SubagentStatus::Interrupted {
                transcript.finish_running(EntryStatus::Interrupted);
            }
            let run = SubagentRun {
                id: record.id.clone(),
                agent: record.agent,
                provider: record.provider,
                model: record.model,
                provider_session_id: record.provider_session_id,
                usage: crate::backend::BackendTokenUsage {
                    input_tokens: record.input_tokens,
                    output_tokens: record.output_tokens,
                    cached_input_tokens: record.cached_input_tokens,
                    cache_write_tokens: record.cache_write_tokens,
                },
                objective: record.objective,
                status,
                latest_activity,
                observability: record.observability,
            };
            self.subagents.push(run.clone());
            self.sync_inline_subagent(&run);
            self.subagent_chats.insert(
                record.id,
                SubagentChat {
                    transcript,
                    reasoning_summaries: ReasoningSummaryTracker::default(),
                },
            );
        }
    }

    pub fn session_store_failed(&mut self, message: impl Into<String>) {
        self.resuming_session = None;
        self.status_message = format!("Session error: {}", message.into());
    }

    #[cfg(test)]
    pub fn install_providers(&mut self, providers: Vec<ProviderRecord>) {
        let picker = self.client.provider_picker.get_or_insert(ProviderPicker {
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

    #[cfg(test)]
    pub fn provider_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.provider_picker else {
            return;
        };
        if picker.providers.is_empty() {
            return;
        }
        picker.selected = offset_index(picker.selected, picker.providers.len(), delta);
    }

    #[cfg(test)]
    pub fn open_provider_details(&mut self) {
        let Some(picker) = &mut self.client.provider_picker else {
            return;
        };
        if let Some(provider) = picker.providers.get(picker.selected) {
            picker.showing_details = true;
            if crate::backend::api_key_provider_setup(&provider.provider).is_some()
                && provider.credential.is_none()
            {
                picker.authentication = Some(ProviderAuthentication::ApiKeyInput {
                    value: String::new(),
                    focused: false,
                });
            }
        }
    }

    #[cfg(test)]
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
        self.provider_contexts
            .get(provider)
            .map_or_else(|| provider.to_owned(), |context| context.name.clone())
    }

    #[cfg(test)]
    pub fn close_provider_picker(&mut self) {
        self.client.provider_picker = None;
        if !self.restore_previous_menu() {
            self.set_status("Provider settings closed.");
        }
    }

    #[cfg(test)]
    pub fn toggle_provider(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.client.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get_mut(picker.selected) else {
            return Vec::new();
        };
        if provider.credential.is_none() {
            if let Some(setup) = crate::backend::api_key_provider_setup(&provider.provider) {
                let provider_id = provider.provider.clone();
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
                self.provider_authentication.insert(
                    provider_id,
                    ProviderAuthenticationState::ApiKeyRequired {
                        dashboard_url: setup.dashboard_url.to_owned(),
                        credential_kind: setup.credential_kind.to_owned(),
                    },
                );
                self.set_status(&format!("Enter your {} API key.", setup.display_name));
                return Vec::new();
            }
            let provider_id = provider.provider.clone();
            picker.authentication = Some(ProviderAuthentication::Starting);
            self.provider_authentication
                .insert(provider_id, ProviderAuthenticationState::Starting);
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
    #[cfg(test)]
    pub fn provider_api_key_input_active(&self) -> bool {
        self.client.provider_picker.as_ref().is_some_and(|picker| {
            picker.showing_details
                && matches!(
                    picker.authentication,
                    Some(ProviderAuthentication::ApiKeyInput { focused: true, .. })
                )
        })
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn submit_provider_api_key(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.client.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get(picker.selected) else {
            return Vec::new();
        };
        let Some(setup) = crate::backend::api_key_provider_setup(&provider.provider) else {
            return Vec::new();
        };
        let Some(ProviderAuthentication::ApiKeyInput {
            value,
            focused: true,
        }) = &picker.authentication
        else {
            return Vec::new();
        };
        let api_key = value.trim().to_owned();
        if api_key.is_empty() {
            self.set_status(&format!("{} API key cannot be empty.", setup.display_name));
            return Vec::new();
        }
        let provider = provider.provider.clone();
        picker.authentication = Some(ProviderAuthentication::Starting);
        self.provider_authentication
            .insert(provider.clone(), ProviderAuthenticationState::Starting);
        self.set_status(&format!("Saving {} API key…", setup.display_name));
        vec![Effect::SaveProviderCredential {
            provider,
            kind: setup.credential_kind.to_owned(),
            metadata: serde_json::json!({"api_key": api_key}),
        }]
    }

    #[cfg(test)]
    pub fn cancel_provider_api_key_input(&mut self) -> bool {
        let Some(picker) = &mut self.client.provider_picker else {
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
        self.set_status("API key entry cancelled.");
        true
    }

    #[cfg(test)]
    pub fn logout_provider(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.client.provider_picker else {
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

    pub fn begin_provider_authentication(
        &mut self,
        provider: &str,
        display_name: &str,
    ) -> Vec<Effect> {
        self.provider_authentication
            .insert(provider.to_owned(), ProviderAuthenticationState::Starting);
        self.set_status(&format!("Starting {display_name} authentication…"));
        vec![Effect::AuthenticateProvider(provider.to_owned())]
    }

    pub fn provider_logged_out(&mut self, provider: &str) {
        let display_name = self.provider_display_name(provider);
        self.provider_disabled(provider);
        self.set_status(&format!("Logged out of {display_name}."));
    }

    pub fn provider_authentication_failed(&mut self, provider: &str, message: &str) {
        self.provider_contexts.remove(provider);
        self.provider_authentication.remove(provider);
        if provider == self.backend_provider {
            self.context_usage = None;
        }
        self.set_status(&format!("Authentication failed for {provider}: {message}"));
    }

    fn provider_is_authenticating(&self, provider: &str) -> bool {
        self.provider_authentication.contains_key(provider)
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
        self.provider_authentication.remove(provider);
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
            self.set_status("No provider is enabled. Configure a provider to continue.");
        }
    }

    pub fn session_persisted(&mut self, session: &SessionRecord) {
        self.nakode_session_id.clone_from(&session.id);
        self.session_id = Some(session.id.clone());
        self.status_message = format!("Session {} started.", short_id(&session.id));
    }

    #[cfg(test)]
    pub fn session_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.session_picker else {
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

    #[cfg(test)]
    pub fn close_session_picker(&mut self) {
        self.client.session_picker = None;
        self.set_status("Session selection cancelled.");
    }

    #[cfg(test)]
    pub fn select_session(&mut self) -> Vec<Effect> {
        let session = self
            .client
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
        self.nakode_session_id.clone_from(&session.id);
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
            owner_session_id: Some(self.nakode_session_id.clone()),
            external_tools: self.external_tools.clone(),
            replace_builtin_tools: self.replace_builtin_tools,
        }));
        effects
    }

    #[cfg(test)]
    pub fn begin_text_selection(&mut self, point: ScreenPoint) {
        self.client.text_selection = Some(TextSelection::new(point));
        self.client.pending_clipboard = None;
    }

    #[cfg(test)]
    pub fn update_text_selection(&mut self, point: ScreenPoint) {
        if let Some(selection) = &mut self.client.text_selection {
            selection.update(point);
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn clear_text_selection(&mut self) {
        self.client.text_selection = None;
        self.client.pending_clipboard = None;
    }

    #[cfg(test)]
    pub fn set_screen_snapshot(&mut self, snapshot: ScreenSnapshot) {
        self.client.screen_snapshot = Some(snapshot);
    }

    #[cfg(test)]
    pub fn take_pending_clipboard(&mut self) -> Option<String> {
        self.client.pending_clipboard.take()
    }

    #[cfg(test)]
    pub fn clipboard_copied(&mut self, bytes: usize) {
        self.status_message = format!("Copied selection to clipboard ({bytes} bytes).");
    }

    #[cfg(test)]
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

    /// Whether a provider backend is still behind this session.
    ///
    /// `is_busy` says what the session was DOING; this says whether anything is left to do it, and
    /// the two must be read together before refusing an operation for work in flight. A backend that
    /// dies mid-turn leaves state behind that nothing ever clears: `handle_disconnected` drops the
    /// turn but never marks a running subagent stopped, so `is_busy` can stay true forever. Asking
    /// such a session to cancel its work first is asking it to do something it cannot.
    #[must_use]
    pub fn provider_is_live(&self) -> bool {
        matches!(
            self.connection,
            ConnectionState::Starting | ConnectionState::Ready { .. }
        )
    }

    #[must_use]
    pub(crate) fn active_shell_ids(&self) -> Vec<String> {
        let mut ids = self.active_shells.iter().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }

    #[must_use]
    #[cfg(test)]
    pub fn command_completions(&self) -> Vec<PromptCompletion<'_>> {
        let token = self.client.editor.token_before_cursor();
        if let Some(prefix) = token.text.strip_prefix(crate::controls::SKILL_PREFIX) {
            let completions = self
                .skills
                .definitions()
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
    #[cfg(test)]
    pub fn selected_command_completion(&self) -> Option<PromptCompletion<'_>> {
        let completions = self.command_completions();
        let selected = self
            .client
            .command_completion_selection
            .min(completions.len().saturating_sub(1));
        completions.get(selected).copied()
    }

    #[must_use]
    #[cfg(test)]
    pub fn command_completion_is_exact(&self) -> bool {
        self.selected_command_completion()
            .is_some_and(|completion| {
                completion.replacement() == self.client.editor.token_before_cursor().text
            })
    }

    #[cfg(test)]
    pub fn move_command_completion(&mut self, delta: isize) {
        let completion_count = self.command_completions().len();
        if completion_count == 0 {
            self.client.command_completion_selection = 0;
            return;
        }
        let selected = self
            .client
            .command_completion_selection
            .min(completion_count - 1)
            .saturating_add_signed(delta)
            .min(completion_count - 1);
        self.client.command_completion_selection = selected;
    }

    #[cfg(test)]
    pub fn accept_command_completion(&mut self) {
        let Some(completion) = self.selected_command_completion() else {
            return;
        };
        let replacement = completion.replacement();
        self.client.editor.replace_token_before_cursor(&replacement);
        self.client.command_completion_selection = 0;
        self.status_message = format!("Inserted {replacement}.");
    }

    #[must_use]
    #[cfg(test)]
    pub fn is_shell_mode(&self) -> bool {
        self.client.editor.text().starts_with('!')
    }

    /// Submits a complete semantic prompt without consulting any client editor.
    ///
    /// # Errors
    /// Rejects blank prompts, unknown skills, unavailable providers, or a busy
    /// session. Queueing while busy is an explicit separate command.
    pub fn submit_prompt(
        &mut self,
        text: String,
        attachments: Vec<PromptAttachment>,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        self.validate_prompt(&text)?;
        if !self.connection.is_ready() {
            return Err(DomainCommandError::Conflict(
                "the selected provider is not ready".to_owned(),
            ));
        }
        if self.is_busy() {
            return Err(DomainCommandError::Conflict(
                "the session is busy; enqueue the prompt instead".to_owned(),
            ));
        }
        if !self.external_tools.is_empty()
            && !self.backend_capabilities.external_tools.is_supported()
        {
            return Err(DomainCommandError::Invalid(format!(
                "{} does not support externally executed tools; select a native Nakode provider before sending this prompt",
                self.backend_name
            )));
        }
        let prompt = QueuedPrompt {
            id: Self::next_id("msg"),
            text,
            attachments,
        };
        self.recoverable_prompt = None;
        Ok(self.begin_prompt(prompt))
    }

    /// Adds a complete semantic prompt to the server-owned queue.
    ///
    /// # Errors
    /// Rejects blank prompts or unknown skill references.
    pub fn enqueue_prompt(
        &mut self,
        text: String,
        attachments: Vec<PromptAttachment>,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        self.validate_prompt(&text)?;
        if !self.is_busy() {
            return self.submit_prompt(text, attachments);
        }
        self.recoverable_prompt = None;
        let id = Self::next_id("msg");
        self.queue.push_back(QueuedPrompt {
            id,
            text,
            attachments,
        });
        self.status_message = format!("Queued message {}.", self.queue.len());
        Ok(Vec::new())
    }

    /// Removes one queued prompt by its stable server-owned identity.
    ///
    /// # Errors
    /// Returns not found when the prompt is no longer queued.
    pub fn remove_queued_prompt(
        &mut self,
        prompt_id: &str,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let position = self
            .queue
            .iter()
            .position(|prompt| prompt.id == prompt_id)
            .ok_or_else(|| DomainCommandError::NotFound(prompt_id.to_owned()))?;
        let reserved = self
            .pending_redirect
            .as_ref()
            .is_some_and(|pending| pending.prompt_id == prompt_id)
            || self.pending_steer.as_ref().is_some_and(|pending| {
                pending
                    .queued_origin
                    .as_ref()
                    .is_some_and(|origin| origin.prompt_id == prompt_id)
            });
        if reserved {
            return Err(DomainCommandError::Conflict(
                "the queued message is already being redirected".to_owned(),
            ));
        }
        let Some(removed) = self.queue.remove(position) else {
            return Err(DomainCommandError::NotFound(prompt_id.to_owned()));
        };
        self.status_message = format!("Removed queued message {}.", removed.id);
        Ok(Vec::new())
    }

    /// Atomically converts one queued text prompt into guidance for the active turn.
    ///
    /// Providers with native steering receive the prompt in the current turn. Providers that can
    /// interrupt but not steer instead stop the current turn and run the selected prompt next. The
    /// selected prompt remains reserved in the ordinary queue while interruption settles, so
    /// completion and cancellation races cannot run a sibling first or submit it twice.
    ///
    /// # Errors
    /// Returns not found for a stale prompt identity and rejects providers that support neither
    /// native steering nor ordered interruption. Queued attachments use stop-and-send because native
    /// steering accepts text only.
    pub fn steer_queued_prompt(
        &mut self,
        prompt_id: &str,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let position = self
            .queue
            .iter()
            .position(|prompt| prompt.id == prompt_id)
            .ok_or_else(|| DomainCommandError::NotFound(prompt_id.to_owned()))?;
        let prompt = self
            .queue
            .get(position)
            .cloned()
            .ok_or_else(|| DomainCommandError::NotFound(prompt_id.to_owned()))?;
        let turn_id = self
            .active_turn
            .as_ref()
            .map(|active| active.id.clone())
            .ok_or_else(|| {
                DomainCommandError::Conflict("there is no active turn to steer".to_owned())
            })?;

        if self.pending_redirect.is_some() {
            return Err(DomainCommandError::Conflict(
                "a queued redirect is already pending".to_owned(),
            ));
        }

        if self.backend_capabilities.steering.is_supported() && prompt.attachments.is_empty() {
            let effects = self.steer_turn(&turn_id, &prompt.text)?;
            if let Some(pending) = &mut self.pending_steer {
                pending.queued_origin = Some(QueuedSteerOrigin {
                    prompt_id: prompt.id,
                });
            }
            return Ok(effects);
        }

        if !self.backend_capabilities.interruption.is_supported() {
            return Err(DomainCommandError::Unsupported(format!(
                "{} cannot redirect this queued message: native steering accepts text only and interruption is unavailable",
                self.backend_name
            )));
        }
        let already_cancelling = self
            .active_turn
            .as_ref()
            .is_some_and(|active| active.cancelling);
        let effects = if already_cancelling {
            Vec::new()
        } else {
            self.cancel_turn(&turn_id)?
        };
        self.pending_redirect = Some(PendingRedirect {
            prompt_id: prompt.id,
        });
        self.set_status("Interrupting active turn; selected follow-up is reserved to run next…");
        Ok(effects)
    }

    #[must_use]
    pub(crate) const fn recoverable_prompt(&self) -> Option<&RecoverablePrompt> {
        self.recoverable_prompt.as_ref()
    }

    /// Starts a supervised workspace shell command without using a TUI draft.
    ///
    /// # Errors
    /// Rejects an empty command.
    pub fn run_shell_command(
        &mut self,
        command: String,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if command.trim().is_empty() {
            return Err(DomainCommandError::Invalid(
                "shell command cannot be empty".to_owned(),
            ));
        }
        let id = Self::next_id("shell");
        self.active_shells.insert(id.clone());
        self.transcript.upsert(
            id.clone(),
            EntryKind::System,
            format!("$ {}", command.trim()),
            "",
            EntryStatus::Running,
        );
        self.status_message = format!("Running {}…", command.trim());
        Ok(vec![Effect::RunShell { id, command }])
    }

    /// Steers the active provider turn using complete semantic text.
    ///
    /// # Errors
    /// Rejects invalid text, stale turn IDs, unsupported steering, or another
    /// steer already awaiting acknowledgement.
    pub fn steer_turn(
        &mut self,
        provider_turn_id: &str,
        text: &str,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        self.validate_prompt(text)?;
        if !self.backend_capabilities.steering.is_supported() {
            return Err(DomainCommandError::Unsupported(format!(
                "{} does not support steering",
                self.backend_name
            )));
        }
        if self.pending_steer.is_some() {
            return Err(DomainCommandError::Conflict(
                "a steer request is already pending".to_owned(),
            ));
        }
        let active = self
            .active_turn
            .as_ref()
            .ok_or_else(|| DomainCommandError::NotFound(provider_turn_id.to_owned()))?;
        if active.id != provider_turn_id {
            return Err(DomainCommandError::NotFound(provider_turn_id.to_owned()));
        }
        if active.cancelling {
            return Err(DomainCommandError::Conflict(
                "the active turn is being cancelled".to_owned(),
            ));
        }
        let provider_session_id = self.provider_session_id.clone().ok_or_else(|| {
            DomainCommandError::Conflict("the active provider session is unavailable".to_owned())
        })?;
        let id = Self::next_id("steer");
        self.pending_steer = Some(PendingSteer {
            id: id.clone(),
            text: text.to_owned(),
            turn_id: provider_turn_id.to_owned(),
            queued_origin: None,
            #[cfg(test)]
            editor_revision: None,
        });
        self.set_status("Sending steering guidance…");
        Ok(vec![Effect::Backend(BackendCommand::SteerTurn {
            provider_session_id,
            turn_id: provider_turn_id.to_owned(),
            client_id: id,
            prompt: self
                .skills
                .render_prompt(text)
                .unwrap_or_else(|_| text.to_owned()),
        })])
    }

    /// Cancels one active provider turn without changing server lifecycle.
    ///
    /// # Errors
    /// Rejects stale IDs, unsupported interruption, or unavailable native state.
    pub fn cancel_turn(
        &mut self,
        provider_turn_id: &str,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if !self.backend_capabilities.interruption.is_supported() {
            return Err(DomainCommandError::Unsupported(format!(
                "{} does not support interruption",
                self.backend_name
            )));
        }
        let active = self
            .active_turn
            .as_mut()
            .ok_or_else(|| DomainCommandError::NotFound(provider_turn_id.to_owned()))?;
        if active.id != provider_turn_id {
            return Err(DomainCommandError::NotFound(provider_turn_id.to_owned()));
        }
        if active.cancelling {
            return Err(DomainCommandError::Conflict(
                "the turn is already being cancelled".to_owned(),
            ));
        }
        let provider_session_id = self.provider_session_id.clone().ok_or_else(|| {
            DomainCommandError::Conflict("the active provider session is unavailable".to_owned())
        })?;
        active.cancelling = true;
        self.set_status("Interrupting active turn…");
        Ok(vec![Effect::Backend(BackendCommand::InterruptTurn {
            provider_session_id,
            turn_id: provider_turn_id.to_owned(),
        })])
    }

    /// Cancels the cancellable work owned by this logical session.
    ///
    /// This is the server-side policy boundary used by thin clients: a
    /// frontend does not need to enumerate provider turns and delegated runs
    /// or decide which internal cancellation operations are required.
    ///
    /// # Errors
    /// Rejects the request when no cancellable work exists or when the active
    /// provider cannot interrupt its turn.
    pub fn cancel_session_work(&mut self) -> Result<Vec<Effect>, DomainCommandError> {
        let active_turn = self.active_turn.as_ref().map(|turn| turn.id.clone());
        let context_compaction = self
            .context_compaction
            .as_ref()
            .map(|compaction| compaction.turn_id.clone());
        let active_runs = self
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
        let active_shells = self.active_shell_ids();
        if active_turn.is_none()
            && context_compaction.is_none()
            && active_runs.is_empty()
            && active_shells.is_empty()
        {
            return Err(DomainCommandError::NotFound(
                "no cancellable work is active for this session".to_owned(),
            ));
        }

        let mut effects = Vec::new();
        if let Some(turn_id) = active_turn {
            effects.extend(self.cancel_turn(&turn_id)?);
        } else if let Some(turn_id) = context_compaction {
            if !self.backend_capabilities.interruption.is_supported() {
                return Err(DomainCommandError::Unsupported(format!(
                    "{} does not support interruption",
                    self.backend_name
                )));
            }
            let provider_session_id = self.provider_session_id.clone().ok_or_else(|| {
                DomainCommandError::Conflict(
                    "the active provider session is unavailable".to_owned(),
                )
            })?;
            self.set_status("Interrupting context compression…");
            effects.push(Effect::Backend(BackendCommand::InterruptTurn {
                provider_session_id,
                turn_id,
            }));
        }
        for run_id in active_runs {
            effects.extend(self.cancel_run(&run_id)?);
        }
        if !active_shells.is_empty() && effects.is_empty() {
            self.set_status("Interrupting shell command…");
        }
        effects.extend(active_shells.into_iter().map(Effect::CancelShell));
        Ok(effects)
    }

    /// Starts manual context compaction without consulting client presentation.
    ///
    /// # Errors
    /// Rejects unavailable, busy, unsupported, or not-yet-started sessions.
    pub fn compact_context(&mut self) -> Result<Vec<Effect>, DomainCommandError> {
        if !self.connection.is_ready() {
            return Err(DomainCommandError::Conflict(
                "the selected provider is not ready".to_owned(),
            ));
        }
        if self.is_busy() {
            return Err(DomainCommandError::Conflict(
                "the session is busy".to_owned(),
            ));
        }
        if !self.backend_capabilities.context_compaction.is_supported() {
            return Err(DomainCommandError::Unsupported(format!(
                "{} does not support context compaction",
                self.backend_name
            )));
        }
        if self.provider_session_id.is_none() {
            return Err(DomainCommandError::Conflict(
                "send a prompt before compacting context".to_owned(),
            ));
        }
        Ok(self.compress_session_context())
    }

    fn validate_prompt(&self, text: &str) -> Result<(), DomainCommandError> {
        if text.trim().is_empty() {
            return Err(DomainCommandError::Invalid(
                "prompt cannot be empty".to_owned(),
            ));
        }
        self.skills
            .referenced(text)
            .map(|_| ())
            .map_err(|name| DomainCommandError::Invalid(format!("unknown skill /skill:{name}")))
    }

    #[cfg(test)]
    pub fn submit_editor(&mut self) -> Vec<Effect> {
        if self.client.editor.is_blank() {
            self.set_status("Write a message before sending.");
            return Vec::new();
        }
        let editor_text = self.client.editor.text();
        if let Some(effects) = self.submit_shell_editor(&editor_text) {
            return effects;
        }
        if let Some(command) = commands::parse_prompt_command(&editor_text) {
            match command {
                ParsedPromptCommand::Agents => {
                    self.client.editor.clear();
                    self.open_agent_picker();
                    return Vec::new();
                }
                ParsedPromptCommand::Settings => {
                    self.client.editor.clear();
                    self.open_settings();
                    return Vec::new();
                }
                ParsedPromptCommand::Compress => {
                    self.client.editor.clear();
                    return self.compress_session_context();
                }
                ParsedPromptCommand::Models => {
                    self.client.editor.clear();
                    return self.open_default_model_picker();
                }
                ParsedPromptCommand::New => {
                    self.client.editor.clear();
                    return self.new_session();
                }
                ParsedPromptCommand::Providers => {
                    self.client.editor.clear();
                    self.client.provider_picker = Some(ProviderPicker {
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
                    self.client.editor.clear();
                    return self.reload_configuration();
                }
                ParsedPromptCommand::Resume(session_id) => {
                    if self.is_busy() {
                        self.set_status("Cannot switch sessions while a turn is active.");
                        return Vec::new();
                    }
                    self.client.editor.clear();
                    if let Some(session_id) = session_id {
                        self.status_message = format!("Looking up session {session_id}…");
                        return vec![Effect::ResolveSession(session_id.to_owned())];
                    }
                    self.client.session_picker = Some(SessionPicker {
                        sessions: Vec::new(),
                        selected: 0,
                        loading: true,
                    });
                    self.set_status("Loading sessions…");
                    return vec![Effect::ListSessions];
                }
                ParsedPromptCommand::Switch => {
                    self.client.editor.clear();
                    return self.open_model_picker();
                }
            }
        }

        if let Err(name) = self.skills.referenced(&editor_text) {
            self.status_message = format!(
                "Unknown skill /skill:{name}. Install it under .agents/skills or ~/.agents/skills."
            );
            return Vec::new();
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

    #[cfg(test)]
    fn submit_shell_editor(&mut self, editor_text: &str) -> Option<Vec<Effect>> {
        let command = editor_text.strip_prefix('!')?;
        if command.trim().is_empty() {
            self.set_status("Write a shell command after !.");
            return Some(Vec::new());
        }
        if !self.client.draft_attachments.is_empty() {
            self.set_status("Attachments cannot be used with shell commands.");
            return Some(Vec::new());
        }
        let command = command.to_owned();
        let id = Self::next_id("shell");
        self.client.editor.clear();
        self.transcript.upsert(
            id.clone(),
            EntryKind::System,
            format!("$ {}", command.trim()),
            "",
            EntryStatus::Running,
        );
        self.status_message = format!("Running {}…", command.trim());
        Some(vec![Effect::RunShell { id, command }])
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
            provider_session_id: session_id,
            compaction_id,
        })]
    }

    #[cfg(test)]
    fn reload_configuration(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot reload while a turn is active.");
            return Vec::new();
        }
        let reload_backend = self.connection.is_ready() && !self.backend_provider.is_empty();
        if reload_backend
            && self
                .backend_capabilities
                .models_require_session
                .is_supported()
            && self.provider_session_id.is_none()
        {
            self.creating_session = Some(());
        }
        self.set_status("Reloading skills, agents, and backend metadata…");
        vec![Effect::ReloadConfiguration]
    }

    fn new_session(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot start a new session while a turn is active.");
            return Vec::new();
        }
        let previous = self.provider_session_id.take();
        self.nakode_session_id = uuid::Uuid::now_v7().to_string();
        self.session_id = None;
        self.session_model_override = false;
        self.session_model_options_override = None;
        self.selected_model = self.default_model();
        self.active_turn = None;
        self.context_usage = None;
        self.context_compaction = None;
        self.creating_session = None;
        self.pending_session_prompt = None;
        self.starting_turn = None;
        self.recoverable_prompt = None;
        self.pending_steer = None;
        self.pending_redirect = None;
        self.redirect_start = None;
        self.pending_handoff = None;
        self.resuming_session = None;
        self.item_turns.clear();
        self.reasoning_summaries = ReasoningSummaryTracker::default();
        self.subagent_result_items.clear();
        self.approvals.clear();
        self.queue.clear();
        self.subagents.clear();
        self.subagent_executions.clear();
        self.subagent_chats.clear();
        self.active_shells.clear();
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

    /// Starts a fresh logical Nakode session without any client presentation action.
    ///
    /// # Errors
    /// Rejects the transition while current work is active.
    pub fn create_logical_session(&mut self) -> Result<Vec<Effect>, DomainCommandError> {
        if self.is_busy() {
            return Err(DomainCommandError::Conflict(
                "cannot create a session while work is active".to_owned(),
            ));
        }
        Ok(self.new_session())
    }

    /// Applies one canonical model selection without relying on a client picker.
    ///
    /// # Errors
    /// Rejects unknown models, mismatched targets, and options unsupported by
    /// the selected provider.
    pub fn select_model_intent(
        &mut self,
        target: &nakode_protocol::ModelTarget,
        model_id: &nakode_protocol::ModelId,
        options: &nakode_protocol::ModelOptions,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let selected = self
            .models
            .iter()
            .find(|model| model.qualified_id() == model_id.as_str())
            .cloned()
            .ok_or_else(|| DomainCommandError::NotFound(model_id.to_string()))?;
        let scope = match target {
            nakode_protocol::ModelTarget::ProviderDefault { provider_id } => {
                if selected.provider != provider_id.as_str() {
                    return Err(DomainCommandError::Invalid(format!(
                        "model {model_id} does not belong to provider {provider_id}"
                    )));
                }
                ModelSelectionTarget::ProviderDefault
            }
            nakode_protocol::ModelTarget::Session { session_id } => {
                if session_id.as_str() != self.nakode_session_id {
                    return Err(DomainCommandError::NotFound(session_id.to_string()));
                }
                ModelSelectionTarget::Session
            }
            nakode_protocol::ModelTarget::AgentSession { .. } => ModelSelectionTarget::Session,
            nakode_protocol::ModelTarget::Vision => {
                if selected.provider != CODEX_PROVIDER {
                    return Err(DomainCommandError::Unsupported(
                        "vision currently requires an OpenAI model".to_owned(),
                    ));
                }
                ModelSelectionTarget::Vision
            }
        };
        Self::validate_model_selection_options(&selected, model_id, options)?;
        let options = ModelOptions {
            reasoning_effort: options.reasoning_effort.clone(),
            fast_mode: options.fast_mode,
        };
        match scope {
            ModelSelectionTarget::ProviderDefault => {
                Ok(self.apply_default_model_selection(&selected, options))
            }
            ModelSelectionTarget::Session => self.apply_session_model_selection(&selected, options),
            ModelSelectionTarget::Vision => Ok(self.apply_vision_model_selection(&selected)),
        }
    }

    fn validate_model_selection_options(
        selected: &ModelInfo,
        model_id: &nakode_protocol::ModelId,
        options: &nakode_protocol::ModelOptions,
    ) -> Result<(), DomainCommandError> {
        if !model_supports_options(selected)
            && (options.reasoning_effort.is_some() || options.fast_mode)
        {
            return Err(DomainCommandError::Unsupported(format!(
                "model {model_id} does not support configurable inference options"
            )));
        }
        if selected.provider == CURSOR_PROVIDER && options.reasoning_effort.is_some() {
            return Err(DomainCommandError::Unsupported(
                "Cursor models do not expose reasoning-effort selection".to_owned(),
            ));
        }
        if let Some(effort) = options.reasoning_effort.as_deref()
            && !matches!(effort, "none" | "low" | "medium" | "high" | "xhigh" | "max")
        {
            return Err(DomainCommandError::Invalid(format!(
                "unknown reasoning effort {effort:?}"
            )));
        }
        Ok(())
    }

    fn apply_default_model_selection(
        &mut self,
        selected: &ModelInfo,
        options: ModelOptions,
    ) -> Vec<Effect> {
        for model in &mut self.models {
            if model.provider == selected.provider {
                model.is_default = model.id == selected.id;
            }
        }
        self.status_message = format!("Default model: {}.", selected.display_name());
        let mut effects = vec![Effect::SetDefaultModel {
            provider: selected.provider.clone(),
            model: selected.id.clone(),
        }];
        if model_supports_options(selected) {
            self.install_model_options(&selected.provider, &selected.id, options.clone());
            effects.push(Effect::SaveModelOptions {
                provider: selected.provider.clone(),
                model: selected.id.clone(),
                options,
            });
        }
        effects
    }

    fn apply_session_model_selection(
        &mut self,
        selected: &ModelInfo,
        options: ModelOptions,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let provider_changed = selected.provider != self.backend_provider;
        if provider_changed && self.is_busy() {
            return Err(DomainCommandError::Conflict(
                "cannot change provider while session work is active".to_owned(),
            ));
        }
        if provider_changed && !self.provider_contexts.contains_key(&selected.provider) {
            return Err(DomainCommandError::NotFound(format!(
                "provider {}",
                selected.provider
            )));
        }

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
                source_provider,
                source_model,
                source_session,
                selected.provider.clone(),
                self.transcript.entries(),
            )
        });
        if !self.activate_provider(&selected.provider) {
            return Err(DomainCommandError::Conflict(
                "provider could not be activated for this session".to_owned(),
            ));
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

        let qualified = selected.qualified_id();
        let display = selected.display_name();
        self.selected_model = Some(qualified.clone());
        self.session_model_override = true;
        self.session_model_options_override = Some((qualified.clone(), options.clone()));
        self.status_message = if provider_changed && self.pending_handoff.is_some() {
            format!("Selected {display}. The next message includes a continuity handoff.")
        } else {
            format!("Selected model: {display}.")
        };

        let mut effects = Vec::new();
        if self
            .backend_capabilities
            .session_model_config
            .is_supported()
            && self.native_session_accepts_model_mutation()
            && let Some(provider_session_id) = self.provider_session_id.clone()
        {
            effects.push(Effect::Backend(BackendCommand::SetSessionModel {
                provider_session_id,
                model: selected.id.clone(),
            }));
        } else if let Some(session_id) = self.session_id.clone() {
            // Native adapters temporarily move a session out of their idle-session map while a turn
            // or compaction owns it. The selected model is already attached to every future
            // StartTurn, so persist the logical override now instead of addressing a native session
            // that cannot accept an out-of-band model mutation until its work completes.
            effects.push(Effect::UpdateSessionModel {
                session_id,
                model: Some(qualified),
            });
        }
        if model_supports_options(selected)
            && let Some(provider_session_id) = self.provider_session_id.clone()
        {
            effects.push(Effect::Backend(BackendCommand::SetSessionOptions {
                provider_session_id,
                options,
            }));
        }
        Ok(effects)
    }

    /// A native session is temporarily owned by its turn/compaction task while work is in flight.
    /// Adapters cannot address it through their idle-session map until that task returns it.
    fn native_session_accepts_model_mutation(&self) -> bool {
        self.starting_turn.is_none()
            && self.active_turn.is_none()
            && self.context_compaction.is_none()
    }

    fn apply_vision_model_selection(&mut self, selected: &ModelInfo) -> Vec<Effect> {
        self.vision_config.model = Some(selected.qualified_id());
        self.status_message = format!("Vision model: {}.", selected.display_name());
        vec![Effect::SaveVisionConfig(self.vision_config.clone())]
    }

    /// Applies one server-owned settings patch.
    ///
    /// # Errors
    /// Rejects unknown backend or terminal-image identifiers.
    pub fn update_settings_intent(
        &mut self,
        patch: &nakode_protocol::SettingsPatch,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        match patch {
            nakode_protocol::SettingsPatch::Web {
                backend,
                credential,
            } => {
                let backend = match backend.as_str() {
                    "disabled" => WebBackend::Disabled,
                    "agent-browser" => WebBackend::AgentBrowser,
                    "firecrawl" => WebBackend::Firecrawl,
                    _ => {
                        return Err(DomainCommandError::Invalid(format!(
                            "unknown browser backend {backend:?}"
                        )));
                    }
                };
                let mut config = self.web_config.clone();
                config.backend = backend;
                if let Some(credential) = credential {
                    config.firecrawl_api_key.clone_from(&credential.0);
                }
                let mut effects = vec![Effect::SaveWebConfig(config)];
                if backend == WebBackend::AgentBrowser {
                    effects.push(Effect::CheckAgentBrowser);
                }
                Ok(effects)
            }
            nakode_protocol::SettingsPatch::Memory {
                backend,
                executable,
                global_bank,
                data_directory,
            } => {
                let backend = match backend.as_str() {
                    "disabled" => MemoryBackend::Disabled,
                    "mnemosyne" => MemoryBackend::Mnemosyne,
                    _ => {
                        return Err(DomainCommandError::Invalid(format!(
                            "unknown memory backend {backend:?}"
                        )));
                    }
                };
                let mut config = self.memory_config.clone();
                config.backend = backend;
                if let Some(executable) = executable {
                    config.executable.clone_from(executable);
                }
                if let Some(global_bank) = global_bank {
                    config.global_bank.clone_from(global_bank);
                }
                if let Some(data_directory) = data_directory {
                    config.data_directory.clone_from(data_directory);
                }
                Ok(vec![Effect::SaveMemoryConfig(config)])
            }
            nakode_protocol::SettingsPatch::Vision { model_id } => {
                let config = crate::vision::VisionConfig {
                    model: model_id.as_ref().map(ToString::to_string),
                };
                Ok(vec![Effect::SaveVisionConfig(config)])
            }
            nakode_protocol::SettingsPatch::TerminalImages { mode } => {
                let mode = match mode.as_str() {
                    "auto" => TerminalImageMode::Auto,
                    "on" => TerminalImageMode::On,
                    "off" => TerminalImageMode::Off,
                    _ => {
                        return Err(DomainCommandError::Invalid(format!(
                            "unknown terminal image mode {mode:?}"
                        )));
                    }
                };
                Ok(vec![Effect::SaveTerminalImageMode(mode)])
            }
        }
    }

    /// Cancels one delegated run without affecting other work or server lifecycle.
    ///
    /// # Errors
    /// Returns not found after a run has completed or when the ID is unknown.
    pub fn cancel_run(&mut self, run_id: &str) -> Result<Vec<Effect>, DomainCommandError> {
        let is_active = self
            .subagent_executions
            .get(run_id)
            .map(|execution| {
                matches!(
                    execution.run.status,
                    SubagentStatus::Starting | SubagentStatus::Working
                )
            })
            .ok_or_else(|| DomainCommandError::NotFound(run_id.to_owned()))?;
        if !is_active {
            return Err(DomainCommandError::Conflict(
                "the delegated run is no longer active".to_owned(),
            ));
        }
        let Some(mut execution) = self.subagent_executions.remove(run_id) else {
            return Err(DomainCommandError::NotFound(run_id.to_owned()));
        };
        execution.run.status = SubagentStatus::Interrupted;
        "Interrupted".clone_into(&mut execution.run.latest_activity);
        execution.run.observability.ended_at_ms = Some(unix_time_ms());
        execution.run.observability.termination_kind = Some("cancelled".to_owned());
        execution.run.observability.termination_detail =
            Some("Interrupted by a client.".to_owned());
        if let Some(run) = self.subagents.iter_mut().find(|run| run.id == run_id) {
            run.clone_from(&execution.run);
        }
        self.sync_inline_subagent(&execution.run);
        if let Some(chat) = self.subagent_chats.get_mut(run_id) {
            chat.transcript.push(
                EntryKind::System,
                "INTERRUPTED",
                "Interrupted by a client.",
                EntryStatus::Interrupted,
            );
            chat.transcript.finish_running(EntryStatus::Interrupted);
        }
        self.status_message = format!("Interrupted delegated run {run_id}.");
        let mut effects = self
            .persist_subagent_effect(run_id)
            .into_iter()
            .collect::<Vec<_>>();
        effects.push(Effect::CompleteAgentRequest {
            request_id: execution.request_id,
            result: format!(
                "[Subagent Result] [{}] [{}]\nInterrupted by a client.",
                execution.run.id, execution.run.agent
            ),
            success: false,
        });
        effects.push(Effect::StopSubagent(run_id.to_owned()));
        Ok(effects)
    }

    #[cfg(test)]
    pub fn enqueue_editor(&mut self) -> Vec<Effect> {
        if self.client.editor.is_blank() {
            self.set_status("Write a message before queueing.");
            return Vec::new();
        }
        if let Err(name) = self.skills.referenced(&self.client.editor.text()) {
            self.status_message = format!(
                "Unknown skill /skill:{name}. Install it under .agents/skills or ~/.agents/skills."
            );
            return Vec::new();
        }
        if !self.is_busy() {
            return self.submit_editor();
        }

        let prompt = self.take_editor_prompt();
        self.queue.push_back(prompt);
        self.client.queue_selection = Some(self.queue.len() - 1);
        self.status_message = format!("Queued message {}.", self.queue.len());
        Vec::new()
    }

    #[cfg(test)]
    pub fn submit_or_steer_editor(&mut self) -> Vec<Effect> {
        let is_local_command = self.is_shell_mode()
            || commands::parse_prompt_command(&self.client.editor.text()).is_some();
        if self.active_turn.is_some() && !is_local_command {
            self.steer_editor()
        } else {
            self.submit_editor()
        }
    }

    #[cfg(test)]
    pub fn steer_editor(&mut self) -> Vec<Effect> {
        if !self.client.draft_attachments.is_empty() {
            self.set_status("Attachments can be sent or queued, but not used for steering.");
            return Vec::new();
        }
        if self.client.editor.is_blank() {
            self.set_status("Write steering guidance first.");
            return Vec::new();
        }
        if let Err(name) = self.skills.referenced(&self.client.editor.text()) {
            self.status_message = format!(
                "Unknown skill /skill:{name}. Install it under .agents/skills or ~/.agents/skills."
            );
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

        let id = Self::next_id("steer");
        let text = self.client.editor.text();
        self.pending_steer = Some(PendingSteer {
            id: id.clone(),
            text: text.clone(),
            turn_id: turn_id.clone(),
            queued_origin: None,
            editor_revision: Some(self.client.editor.revision()),
        });
        self.set_status("Sending steering guidance…");
        vec![Effect::Backend(BackendCommand::SteerTurn {
            provider_session_id,
            turn_id,
            client_id: id,
            prompt: self
                .skills
                .render_prompt(&text)
                .unwrap_or_else(|_| text.clone()),
        })]
    }

    #[cfg(test)]
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
                provider_session_id,
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
            provider_session_id,
            turn_id: active.id.clone(),
        }));
        effects
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn open_model_picker(&mut self) -> Vec<Effect> {
        self.open_model_picker_for(ModelSelectionScope::Session)
    }

    #[cfg(test)]
    pub fn open_default_model_picker(&mut self) -> Vec<Effect> {
        self.open_model_picker_for(ModelSelectionScope::Default)
    }

    #[cfg(test)]
    pub fn open_vision_model_picker(&mut self) -> Vec<Effect> {
        if !self
            .models
            .iter()
            .any(|model| model.provider == CODEX_PROVIDER)
        {
            self.set_status("No configured vision-capable models are available.");
            return Vec::new();
        }
        self.show_model_picker(ModelSelectionScope::Vision);
        Vec::new()
    }

    #[cfg(test)]
    fn open_model_picker_for(&mut self, scope: ModelSelectionScope) -> Vec<Effect> {
        if self.client.pending_model_picker.is_some()
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
        self.client.pending_model_picker = Some(scope);
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
            provider_session_id: self.provider_session_id.clone(),
        })]
    }

    #[must_use]
    #[cfg(test)]
    pub fn selected_model_display_name(&self) -> Option<String> {
        let selected = self.selected_model.as_deref()?;
        Some(
            self.models
                .iter()
                .find(|model| model.qualified_id() == selected)
                .map_or_else(
                    || display_qualified_model_name(selected),
                    ModelInfo::display_name,
                ),
        )
    }

    #[must_use]
    #[cfg(test)]
    pub fn model_uses_fast_mode(&self, model: &ModelInfo) -> bool {
        self.model_options_for(model).fast_mode
    }

    #[must_use]
    #[cfg(test)]
    pub fn selected_model_uses_fast_mode(&self) -> bool {
        self.selected_model.is_some() && self.selected_model_options().fast_mode
    }

    /// Whether `model` under `provider` reports `effort` among its own levels.
    ///
    /// The vocabulary differs per model and belongs to the provider, so this asks the one place that
    /// decides it (`projection::model_configuration`) rather than keeping a second list here. A model
    /// this workspace has never heard of offers nothing, which is the safe answer: the level is
    /// dropped and the model's own default is used.
    fn model_offers_reasoning_effort(&self, provider: &str, model: &str, effort: &str) -> bool {
        self.models
            .iter()
            .find(|candidate| candidate.provider == provider && candidate.id == model)
            .is_some_and(|candidate| {
                projection::model_configuration(candidate)
                    .reasoning_efforts
                    .iter()
                    .any(|candidate| candidate == effort)
            })
    }

    fn model_options_for_qualified(&self, qualified: &str) -> ModelOptions {
        self.model_options
            .get(qualified)
            .or_else(|| {
                qualified
                    .split_once('/')
                    .and_then(|(provider, _)| self.model_options.get(&format!("{provider}/*")))
            })
            .cloned()
            .unwrap_or_else(|| self.default_model_options.clone())
    }

    fn model_options_for(&self, model: &ModelInfo) -> ModelOptions {
        let mut options = self.model_options_for_qualified(&model.qualified_id());
        let configuration = projection::model_configuration(model);
        if options.reasoning_effort.as_ref().is_some_and(|effort| {
            !configuration
                .reasoning_efforts
                .iter()
                .any(|candidate| candidate == effort)
        }) {
            options.reasoning_effort = None;
        }
        if !configuration.fast_mode_configurable {
            options.fast_mode = false;
        }
        options
    }

    pub(crate) fn selected_model_options(&self) -> ModelOptions {
        let selected = self.selected_model.as_deref();
        let model = selected.and_then(|selected| {
            self.models
                .iter()
                .find(|model| model.qualified_id() == selected)
        });
        if let Some(selected) = selected
            && let Some((override_model, options)) = &self.session_model_options_override
            && override_model == selected
        {
            let mut options = options.clone();
            if let Some(model) = model {
                let configuration = projection::model_configuration(model);
                if options.reasoning_effort.as_ref().is_some_and(|effort| {
                    !configuration
                        .reasoning_efforts
                        .iter()
                        .any(|candidate| candidate == effort)
                }) {
                    options.reasoning_effort = None;
                }
                if !configuration.fast_mode_configurable {
                    options.fast_mode = false;
                }
            }
            return options;
        }
        model.map_or_else(ModelOptions::default, |model| self.model_options_for(model))
    }

    #[cfg(test)]
    fn show_model_picker(&mut self, scope: ModelSelectionScope) {
        let selected_model = if scope == ModelSelectionScope::Vision {
            self.vision_config.model.as_ref()
        } else {
            self.selected_model.as_ref()
        };
        let selected = selected_model
            .and_then(|selected| {
                self.models
                    .iter()
                    .filter(|model| {
                        scope != ModelSelectionScope::Vision || model.provider == CODEX_PROVIDER
                    })
                    .position(|model| &model.qualified_id() == selected)
            })
            .unwrap_or(0);
        let options = self
            .models
            .iter()
            .filter(|model| {
                scope != ModelSelectionScope::Vision || model.provider == CODEX_PROVIDER
            })
            .nth(selected)
            .map_or_else(
                || self.default_model_options.clone(),
                |model| self.model_options_for(model),
            );
        self.client.model_picker = Some(ModelPicker {
            filter: String::new(),
            selected,
            scope,
            stage: ModelPickerStage::Models,
            option_selected: 0,
            options,
            options_fast_only: false,
        });
        self.client.pending_model_picker = None;
    }

    #[cfg(test)]
    pub fn picker_insert(&mut self, character: char) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Models
            && !character.is_control()
        {
            picker.filter.push(character);
            picker.selected = 0;
        }
    }

    #[cfg(test)]
    pub fn picker_backspace(&mut self) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Models
        {
            picker.filter.pop();
            picker.selected = 0;
        }
    }

    #[cfg(test)]
    pub fn picker_move(&mut self, delta: isize) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Options
        {
            let option_count = if picker.options_fast_only { 1 } else { 2 };
            picker.option_selected = offset_index(picker.option_selected, option_count, delta);
            return;
        }
        let count = self.filtered_models().len();
        let Some(picker) = &mut self.client.model_picker else {
            return;
        };
        if count == 0 {
            picker.selected = 0;
            return;
        }
        picker.selected = offset_index(picker.selected, count, delta);
    }

    #[cfg(test)]
    pub fn picker_adjust(&mut self, delta: isize) {
        let Some(picker) = &mut self.client.model_picker else {
            return;
        };
        if picker.stage != ModelPickerStage::Options {
            return;
        }
        if picker.options_fast_only {
            if delta != 0 {
                picker.options.fast_mode = !picker.options.fast_mode;
            }
        } else if picker.option_selected == 0 {
            const EFFORTS: [&str; 6] = ["none", "low", "medium", "high", "xhigh", "max"];
            let current = picker
                .options
                .reasoning_effort
                .as_deref()
                .and_then(|effort| EFFORTS.iter().position(|candidate| *candidate == effort))
                .unwrap_or(2);
            picker.options.reasoning_effort =
                Some(EFFORTS[offset_index(current, EFFORTS.len(), delta)].to_owned());
        } else if delta != 0 {
            picker.options.fast_mode = !picker.options.fast_mode;
        }
    }

    #[cfg(test)]
    #[allow(clippy::too_many_lines)]
    pub fn picker_select(&mut self) -> Vec<Effect> {
        let filtered = self.filtered_models();
        let selected = self
            .client
            .model_picker
            .as_ref()
            .and_then(|picker| filtered.get(picker.selected))
            .copied()
            .cloned();
        if let Some(selected) = selected {
            let scope = self
                .client
                .model_picker
                .as_ref()
                .map_or(ModelSelectionScope::Session, |picker| picker.scope);
            let stage = self
                .client
                .model_picker
                .as_ref()
                .map_or(ModelPickerStage::Models, |picker| picker.stage);
            if scope != ModelSelectionScope::Vision
                && model_supports_options(&selected)
                && stage == ModelPickerStage::Models
            {
                let mut options = self.model_options_for(&selected);
                let fast_only = selected.provider == CURSOR_PROVIDER;
                if fast_only {
                    options.reasoning_effort = None;
                }
                if let Some(picker) = &mut self.client.model_picker {
                    picker.stage = ModelPickerStage::Options;
                    picker.option_selected = 0;
                    picker.options = options;
                    picker.options_fast_only = fast_only;
                }
                return Vec::new();
            }
            if scope == ModelSelectionScope::Vision {
                return self.select_vision_model(&selected);
            }
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
            let selected_options = if stage == ModelPickerStage::Options {
                self.client.model_picker.as_ref().map_or_else(
                    || self.model_options_for(&selected),
                    |picker| picker.options.clone(),
                )
            } else {
                self.model_options_for(&selected)
            };
            let active = self.active_turn.is_some();
            let qualified = selected.qualified_id();
            let display = selected.display_name();
            self.selected_model = Some(qualified.clone());
            self.session_model_override = scope == ModelSelectionScope::Session;
            self.session_model_options_override = (scope == ModelSelectionScope::Session)
                .then(|| (qualified.clone(), selected_options.clone()));
            if scope == ModelSelectionScope::Default {
                for model in &mut self.models {
                    if model.provider == selected.provider {
                        model.is_default = model.id == selected.id;
                    }
                }
            }
            self.status_message = if provider_changed && self.pending_handoff.is_some() {
                format!("Selected {display}. The next message includes a continuity handoff.")
            } else if active {
                format!("Next model: {display}. The active turn is unchanged.")
            } else if scope == ModelSelectionScope::Default {
                format!("Default model: {display}.")
            } else {
                format!("Selected model: {display}.")
            };
            self.finish_model_picker();
            let mut effects = Vec::new();
            if scope == ModelSelectionScope::Default {
                effects.push(Effect::SetDefaultModel {
                    provider: selected.provider.clone(),
                    model: selected.id.clone(),
                });
                if model_supports_options(&selected) {
                    self.install_model_options(
                        &selected.provider,
                        &selected.id,
                        selected_options.clone(),
                    );
                    effects.push(Effect::SaveModelOptions {
                        provider: selected.provider.clone(),
                        model: selected.id.clone(),
                        options: selected_options.clone(),
                    });
                }
            }
            if self
                .backend_capabilities
                .session_model_config
                .is_supported()
                && self.native_session_accepts_model_mutation()
                && let Some(session_id) = self.provider_session_id.clone()
            {
                effects.push(Effect::Backend(BackendCommand::SetSessionModel {
                    provider_session_id: session_id,
                    model: selected.id.clone(),
                }));
            } else if let Some(session_id) = self.session_id.clone() {
                effects.push(Effect::UpdateSessionModel {
                    session_id,
                    model: Some(qualified),
                });
            }
            if model_supports_options(&selected)
                && let Some(provider_session_id) = self.provider_session_id.clone()
            {
                effects.push(Effect::Backend(BackendCommand::SetSessionOptions {
                    provider_session_id,
                    options: selected_options,
                }));
            }
            return effects;
        }
        Vec::new()
    }

    #[cfg(test)]
    fn select_vision_model(&mut self, selected: &ModelInfo) -> Vec<Effect> {
        let qualified = selected.qualified_id();
        self.vision_config.model = Some(qualified.clone());
        self.finish_model_picker();
        if let Some(settings) = &mut self.client.settings {
            settings.vision = self.vision_config.clone();
        }
        self.status_message = format!("Vision model: {}.", selected.display_name());
        vec![Effect::SaveVisionConfig(self.vision_config.clone())]
    }

    #[must_use]
    #[cfg(test)]
    pub fn filtered_models(&self) -> Vec<&ModelInfo> {
        let Some(picker) = &self.client.model_picker else {
            return self.models.iter().collect();
        };
        let filter = picker.filter.to_lowercase();
        self.models
            .iter()
            .filter(|model| {
                let vision_model =
                    picker.scope != ModelSelectionScope::Vision || model.provider == CODEX_PROVIDER;
                vision_model
                    && (filter.is_empty()
                        || model.qualified_id().to_lowercase().contains(&filter)
                        || model.display_name().to_lowercase().contains(&filter))
            })
            .collect()
    }

    #[cfg(test)]
    fn finish_model_picker(&mut self) {
        self.client.model_picker = None;
        self.restore_previous_menu();
    }

    #[cfg(test)]
    pub fn close_model_picker(&mut self) {
        if let Some(picker) = &mut self.client.model_picker
            && picker.stage == ModelPickerStage::Options
        {
            picker.stage = ModelPickerStage::Models;
            picker.option_selected = 0;
            return;
        }
        self.finish_model_picker();
        if self.client.settings.is_none() {
            self.set_status("Model selection cancelled.");
        }
    }

    #[cfg(test)]
    pub fn move_queue_selection(&mut self, delta: isize) {
        if self.queue.is_empty() {
            self.client.queue_selection = None;
            return;
        }
        let current = self.client.queue_selection.unwrap_or(0);
        self.client.queue_selection = Some(offset_index(current, self.queue.len(), delta));
    }

    #[cfg(test)]
    pub fn remove_selected_queue_item(&mut self) {
        let Some(index) = self.client.queue_selection else {
            return;
        };
        if let Some(prompt) = self.queue.remove(index) {
            self.status_message = format!("Removed queued message {}.", prompt.id);
        }
        self.client.queue_selection = if self.queue.is_empty() {
            None
        } else {
            Some(index.min(self.queue.len() - 1))
        };
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn move_question_selection(&mut self, delta: isize) {
        if let Some(question) = self.questions.front_mut() {
            question.selected =
                offset_index(question.selected, question.request.options.len(), delta);
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
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
        self.status_message = format!("Answered: {}", answers.join(", "));
        vec![Effect::Backend(BackendCommand::ResolveQuestion {
            id: question.request.id,
            answer: crate::backend::QuestionAnswer::Options(answers),
        })]
    }

    /// Resolves one server-owned approval or question by stable interaction ID.
    ///
    /// # Errors
    /// Rejects stale IDs, a resolution of the wrong interaction kind, invalid
    /// question option IDs, or an empty answer.
    pub fn resolve_interaction(
        &mut self,
        interaction_id: &nakode_protocol::InteractionId,
        resolution: &nakode_protocol::InteractionResolution,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if let Some(position) = self.approvals.iter().position(|approval| {
            projection::approval_interaction_id(&self.nakode_session_id, &approval.id)
                == *interaction_id
        }) {
            let decision = match resolution {
                nakode_protocol::InteractionResolution::ApproveOnce => ApprovalDecision::AcceptOnce,
                nakode_protocol::InteractionResolution::ApproveForSession => {
                    ApprovalDecision::AcceptForSession
                }
                nakode_protocol::InteractionResolution::Decline => ApprovalDecision::Decline,
                nakode_protocol::InteractionResolution::Answer { .. }
                | nakode_protocol::InteractionResolution::AnswerQuestions { .. } => {
                    return Err(DomainCommandError::Invalid(
                        "an approval cannot be answered as a question".to_owned(),
                    ));
                }
            };
            let Some(approval) = self.approvals.remove(position) else {
                return Err(DomainCommandError::NotFound(interaction_id.to_string()));
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
            return Ok(vec![Effect::Backend(BackendCommand::ResolveApproval {
                id: approval.id,
                decision,
            })]);
        }

        self.resolve_question_interaction(interaction_id, resolution)
    }

    fn resolve_question_interaction(
        &mut self,
        interaction_id: &nakode_protocol::InteractionId,
        resolution: &nakode_protocol::InteractionResolution,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let Some(group_id) = self
            .questions
            .iter()
            .map(|question| question.request.group_id.as_str())
            .find(|group_id| {
                projection::question_interaction_id(&self.nakode_session_id, group_id)
                    == *interaction_id
            })
            .map(str::to_owned)
        else {
            return Err(DomainCommandError::NotFound(interaction_id.to_string()));
        };
        let mut group = self
            .questions
            .iter()
            .enumerate()
            .filter(|(_, question)| question.request.group_id == group_id)
            .collect::<Vec<_>>();
        group.sort_by_key(|(_, question)| question.request.order);

        let responses = match resolution {
            nakode_protocol::InteractionResolution::Answer { option_ids } => {
                if group.len() != 1 {
                    return Err(DomainCommandError::Invalid(
                        "this interaction contains multiple questions; submit one structured answer for each question"
                            .to_owned(),
                    ));
                }
                vec![nakode_protocol::QuestionResponse {
                    question_id: group[0].1.request.logical_id.clone(),
                    option_ids: option_ids.clone(),
                    text: None,
                }]
            }
            nakode_protocol::InteractionResolution::AnswerQuestions { answers } => answers.clone(),
            _ => {
                return Err(DomainCommandError::Invalid(
                    "a question interaction must be answered".to_owned(),
                ));
            }
        };
        if responses.len() != group.len() {
            return Err(DomainCommandError::Invalid(format!(
                "expected answers for {} questions, received {}; every question must be answered",
                group.len(),
                responses.len()
            )));
        }
        let mut response_ids = HashSet::new();
        for response in &responses {
            if !response_ids.insert(response.question_id.as_str()) {
                return Err(DomainCommandError::Invalid(format!(
                    "duplicate answer for question {:?}",
                    response.question_id
                )));
            }
            if !group
                .iter()
                .any(|(_, question)| question.request.logical_id == response.question_id)
            {
                return Err(DomainCommandError::Invalid(format!(
                    "unknown question {:?}",
                    response.question_id
                )));
            }
        }

        let resolved = Self::build_question_resolutions(&group, &responses)?;
        // Validation above is side-effect free. Only after every item succeeds do we remove the
        // parked questions and dispatch all provider waiters.
        let mut positions = resolved
            .iter()
            .map(|(position, ..)| *position)
            .collect::<Vec<_>>();
        positions.sort_unstable_by(|left, right| right.cmp(left));
        for position in positions {
            self.questions.remove(position);
        }
        self.status_message = format!(
            "Answered: {}",
            resolved
                .iter()
                .map(|(_, _, _, shown)| shown.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
        Ok(resolved
            .into_iter()
            .map(|(_, id, answer, _)| {
                Effect::Backend(BackendCommand::ResolveQuestion { id, answer })
            })
            .collect())
    }

    fn build_question_resolutions(
        group: &[(usize, &QuestionPrompt)],
        responses: &[nakode_protocol::QuestionResponse],
    ) -> Result<Vec<(usize, String, crate::backend::QuestionAnswer, String)>, DomainCommandError>
    {
        let mut resolved = Vec::with_capacity(group.len());
        for (position, question) in group {
            let id = &question.request.logical_id;
            let response = responses
                .iter()
                .find(|response| response.question_id == *id)
                .ok_or_else(|| {
                    DomainCommandError::Invalid(format!("question {id:?} is unanswered"))
                })?;
            let text = response.text.as_deref();
            if text.is_some_and(|text| text.trim().is_empty()) {
                return Err(DomainCommandError::Invalid(format!(
                    "question {id:?} has a blank free-text answer"
                )));
            }
            if text.is_some() && !response.option_ids.is_empty() {
                return Err(DomainCommandError::Invalid(format!(
                    "question {id:?} must use option labels or free text, not both"
                )));
            }
            let (answer, shown) = if let Some(text) = text {
                let text = text.trim().to_owned();
                (crate::backend::QuestionAnswer::Text(text.clone()), text)
            } else {
                if response.option_ids.is_empty() {
                    return Err(DomainCommandError::Invalid(format!(
                        "question {id:?} requires an option or free-text answer"
                    )));
                }
                if !question.request.multi && response.option_ids.len() != 1 {
                    return Err(DomainCommandError::Invalid(format!(
                        "question {id:?} accepts exactly one option"
                    )));
                }
                let mut indexes = response
                    .option_ids
                    .iter()
                    .map(|option_id| {
                        option_id
                            .parse::<usize>()
                            .ok()
                            .filter(|index| *index < question.request.options.len())
                            .ok_or_else(|| {
                                DomainCommandError::Invalid(format!(
                                    "question {id:?} has unknown option {option_id:?}"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                indexes.sort_unstable();
                indexes.dedup();
                if indexes.len() != response.option_ids.len() {
                    return Err(DomainCommandError::Invalid(format!(
                        "question {id:?} option IDs must be unique"
                    )));
                }
                let labels = indexes
                    .iter()
                    .map(|index| question.request.options[*index].label.clone())
                    .collect::<Vec<_>>();
                (
                    crate::backend::QuestionAnswer::Options(labels.clone()),
                    labels.join(", "),
                )
            };
            resolved.push((*position, question.request.id.clone(), answer, shown));
        }

        Ok(resolved)
    }

    /// Configures the externally executed tools exposed to this session.
    ///
    /// # Errors
    ///
    /// Returns an error after the session has started or when the tool definitions are invalid.
    pub fn configure_external_tools(
        &mut self,
        tools: Vec<nakode_protocol::ExternalToolDefinition>,
        replace_builtin_tools: bool,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if self.is_busy() || self.provider_session_id.is_some() {
            return Err(DomainCommandError::Invalid(
                "session tools must be configured before the first prompt".to_owned(),
            ));
        }
        self.validate_and_install_external_tools(tools, replace_builtin_tools)
    }

    /// Verifies that an already-loaded session has the exact requested tool boundary.
    ///
    /// # Errors
    /// Rejects every attempt to mutate the table after the logical session was published, even when
    /// no prompt has started yet. Atomic create/open is the only installation boundary.
    pub fn configure_or_validate_external_tools(
        &mut self,
        tools: &[nakode_protocol::ExternalToolDefinition],
        replace_builtin_tools: bool,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if self.external_tools == tools && self.replace_builtin_tools == replace_builtin_tools {
            return Ok(Vec::new());
        }
        Err(DomainCommandError::Invalid(
            "the attached session already started with a different tool table".to_owned(),
        ))
    }

    fn validate_and_install_external_tools(
        &mut self,
        tools: Vec<nakode_protocol::ExternalToolDefinition>,
        replace_builtin_tools: bool,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        if tools.is_empty() {
            return Err(DomainCommandError::Invalid(
                "at least one external tool is required".to_owned(),
            ));
        }
        let mut names = HashSet::new();
        for tool in &tools {
            if tool.name.trim().is_empty() || !names.insert(tool.name.as_str()) {
                return Err(DomainCommandError::Invalid(
                    "external tool names must be non-empty and unique".to_owned(),
                ));
            }
            serde_json::from_str::<serde_json::Value>(&tool.input_schema_json).map_err(
                |error| {
                    DomainCommandError::Invalid(format!(
                        "invalid schema for external tool {}: {error}",
                        tool.name
                    ))
                },
            )?;
        }
        self.external_tools = tools;
        self.replace_builtin_tools = replace_builtin_tools;
        Ok(Vec::new())
    }

    /// Resolves a pending external tool request.
    ///
    /// # Errors
    ///
    /// Returns an error when the call is not pending for this session.
    pub fn submit_external_tool_result(
        &mut self,
        call_id: &str,
        output: String,
        failed: bool,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let Some(position) = self
            .external_tool_calls
            .iter()
            .position(|call| call.id == call_id)
        else {
            return Err(DomainCommandError::NotFound(call_id.to_owned()));
        };
        self.external_tool_calls.remove(position);
        self.status_message = format!("External tool completed: {call_id}");
        Ok(vec![Effect::Backend(BackendCommand::ResolveExternalTool {
            id: call_id.to_owned(),
            output,
            failed,
        })])
    }

    pub fn handle_provider_backend(&mut self, provider: &str, event: BackendEvent) -> Vec<Effect> {
        if let Some(effects) = self.handle_provider_authentication(provider, &event) {
            return effects;
        }
        match &event {
            BackendEvent::Ready(identity) => {
                self.provider_authentication.remove(provider);
                let context = self
                    .provider_contexts
                    .entry(provider.to_owned())
                    .or_insert_with(|| ProviderContext {
                        name: String::new(),
                        capabilities: BackendCapabilities::default(),
                        connection: ConnectionState::Starting,
                        provider_session_id: None,
                        session_id: None,
                        context_usage: None,
                    });
                context.name.clone_from(&identity.display_name);
                context.capabilities = identity.capabilities.clone();
                context.connection = ConnectionState::Ready {
                    server: identity.display_name.clone(),
                };
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
                if models.iter().any(|model| model.provider != provider) {
                    self.diagnostic_count += 1;
                    return Vec::new();
                }
                let models = models.clone();
                if !models.is_empty() {
                    self.install_models(models.clone());
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
                self.provider_authentication.insert(
                    provider.to_owned(),
                    ProviderAuthenticationState::Challenge {
                        verification_url: verification_url.clone(),
                        user_code: user_code.clone(),
                    },
                );
                self.set_status("Complete the provider device authentication in your browser.");
                Some(Vec::new())
            }
            BackendEvent::AuthenticationCompleted { kind, metadata } => {
                self.provider_authentication.remove(provider);
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
                self.provider_authentication.remove(provider);
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

    #[allow(clippy::too_many_lines)]
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
            BackendEvent::TokenUsageUpdated { usage } => {
                self.provider_usage.input_tokens = self
                    .provider_usage
                    .input_tokens
                    .saturating_add(usage.input_tokens);
                self.provider_usage.output_tokens = self
                    .provider_usage
                    .output_tokens
                    .saturating_add(usage.output_tokens);
                self.provider_usage.cached_input_tokens = self
                    .provider_usage
                    .cached_input_tokens
                    .saturating_add(usage.cached_input_tokens);
                self.provider_usage.cache_write_tokens = self
                    .provider_usage
                    .cache_write_tokens
                    .saturating_add(usage.cache_write_tokens);
            }
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
            } => {
                self.external_tool_calls.clear();
                return self.complete_turn(&turn_id, outcome, error);
            }
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
            } => self.observe_delta(&turn_id, &item_id, kind, &delta),
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
            BackendEvent::QuestionRequested(request) => self.handle_question_request(*request),
            BackendEvent::ExternalToolRequested(request) => {
                self.status_message = format!("External tool requested: {}", request.name);
                self.external_tool_calls.push(request);
            }
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
        Vec::new()
    }

    fn observe_session(&mut self, provider_session_id: String) {
        if self.provider_session_id.is_none() && !provider_session_id.is_empty() {
            self.provider_session_id = Some(provider_session_id);
        }
    }

    fn handle_question_request(&mut self, request: QuestionRequest) {
        self.status_message = format!("Question: {}", request.title);
        #[cfg(test)]
        let selected = request
            .recommended
            .unwrap_or_default()
            .min(request.options.len().saturating_sub(1));
        #[cfg(test)]
        let selections = vec![false; request.options.len()];
        self.questions.push_back(QuestionPrompt {
            request,
            #[cfg(test)]
            selected,
            #[cfg(test)]
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
            if self.models.is_empty() {
                self.install_models(models);
            } else {
                self.set_status("Model refresh returned no choices; kept the cached catalog.");
            }
            return Vec::new();
        }
        let cached = models.clone();
        self.install_models(models);
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
        self.provider_usage = crate::backend::BackendTokenUsage::default();
        self.context_compaction = None;
        self.creating_session = None;
        if !model.is_empty() {
            let qualified = self.qualify_active_model(model);
            if self.selected_model.as_deref() != Some(qualified.as_str()) {
                self.selected_model = Some(qualified.clone());
                self.status_message = format!(
                    "{} selected model {}.",
                    self.backend_name,
                    display_qualified_model_name(&qualified)
                );
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
        if self
            .selected_model
            .as_deref()
            .and_then(|selected| {
                self.models
                    .iter()
                    .find(|model| model.qualified_id() == selected)
            })
            .is_some_and(model_supports_options)
        {
            effects.push(Effect::Backend(BackendCommand::SetSessionOptions {
                provider_session_id: provider_session_id.clone(),
                options: self.selected_model_options(),
            }));
        }
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
        let provider_session_id_for_options = provider_session_id.clone();
        self.provider_session_id = Some(provider_session_id);
        self.nakode_session_id.clone_from(&session.id);
        self.session_id = Some(session.id.clone());
        self.context_usage = None;
        self.provider_usage = crate::backend::BackendTokenUsage::default();
        self.context_compaction = None;
        self.session_model_options_override = None;
        if !model.is_empty() {
            self.selected_model = Some(self.qualify_active_model(model));
            self.session_model_override = true;
        }
        self.install_history(history);
        self.install_subagents(Vec::new());
        self.status_message = format!("Resumed session {}.", short_id(&session.id));
        let mut effects = vec![
            Effect::TouchSession(session.id.clone()),
            Effect::LoadSubagents(session.id),
        ];
        if self
            .selected_model
            .as_deref()
            .and_then(|selected| {
                self.models
                    .iter()
                    .find(|model| model.qualified_id() == selected)
            })
            .is_some_and(model_supports_options)
        {
            effects.push(Effect::Backend(BackendCommand::SetSessionOptions {
                provider_session_id: provider_session_id_for_options,
                options: self.selected_model_options(),
            }));
        }
        effects
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
            let queued = pending.queued_origin.is_some();
            self.set_status(if queued {
                "A late steer response was ignored; the queued message remains a follow-up."
            } else {
                "A late steer response was ignored."
            });
            return;
        }
        if let Some(origin) = &pending.queued_origin {
            let Some(position) = self
                .queue
                .iter()
                .position(|prompt| prompt.id == origin.prompt_id)
            else {
                self.set_status(
                    "Steering was accepted, but its reserved queued message was unavailable.",
                );
                return;
            };
            self.queue.remove(position);
        }
        self.transcript.push(
            EntryKind::Steering,
            format!("STEER · {}", pending.id),
            pending.text,
            EntryStatus::Complete,
        );
        self.set_status("Steering guidance accepted.");
    }

    fn restore_redirect_start(&mut self, pending: RedirectStart) -> bool {
        if self
            .queue
            .iter()
            .any(|prompt| prompt.id == pending.prompt.id)
        {
            return false;
        }
        let index = pending.index.min(self.queue.len());
        self.queue.insert(index, pending.prompt);
        true
    }

    fn begin_pending_redirect(&mut self, pending: &PendingRedirect) -> Vec<Effect> {
        let Some(position) = self
            .queue
            .iter()
            .position(|prompt| prompt.id == pending.prompt_id)
        else {
            self.status_message.push_str(
                " The selected follow-up could not be found, so no replacement turn was started.",
            );
            return Vec::new();
        };
        let Some(prompt) = self.queue.remove(position) else {
            self.status_message.push_str(
                " The selected follow-up could not be reserved, so no replacement turn was started.",
            );
            return Vec::new();
        };
        self.redirect_start = Some(RedirectStart {
            prompt: prompt.clone(),
            index: position,
        });
        self.begin_prompt(prompt)
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
        self.pending_redirect = None;
        let redirected_prompt = self.redirect_start.take();
        let redirected_id = redirected_prompt
            .as_ref()
            .map(|pending| pending.prompt.id.clone());
        if let Some(pending) = redirected_prompt {
            self.restore_redirect_start(pending);
        }
        self.approvals.clear();
        self.set_status("The provider session was closed.");
        if let Some(prompt) = pending_prompt
            && redirected_id.as_deref() != Some(prompt.id.as_str())
        {
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
        self.pending_redirect = None;
        let redirected_prompt = self.redirect_start.take();
        let redirected_id = redirected_prompt
            .as_ref()
            .map(|pending| pending.prompt.id.clone());
        if let Some(pending) = redirected_prompt {
            self.restore_redirect_start(pending);
        }
        self.transcript.push(
            EntryKind::Error,
            "BACKEND DISCONNECTED",
            &reason,
            EntryStatus::Failed,
        );
        self.status_message = reason;
        if let Some(prompt) = pending_prompt
            && redirected_id.as_deref() != Some(prompt.id.as_str())
        {
            self.restore_failed_prompt(&prompt);
        }
    }

    #[cfg(test)]
    fn take_editor_prompt(&mut self) -> QueuedPrompt {
        let text = self.client.editor.text();
        let mut remaining_labels = HashMap::<String, usize>::new();
        for attachment in &self.client.draft_attachments {
            let token = format!("[{}]", attachment.label);
            remaining_labels
                .entry(token.clone())
                .or_insert_with(|| text.matches(&token).count());
        }
        let attachments = std::mem::take(&mut self.client.draft_attachments)
            .into_iter()
            .filter(|attachment| {
                let token = format!("[{}]", attachment.label);
                let Some(remaining) = remaining_labels.get_mut(&token) else {
                    return false;
                };
                if *remaining == 0 {
                    return false;
                }
                *remaining -= 1;
                true
            })
            .collect();
        let prompt = QueuedPrompt {
            id: Self::next_id("msg"),
            text,
            attachments,
        };
        self.client.editor.clear();
        prompt
    }

    fn begin_prompt(&mut self, prompt: QueuedPrompt) -> Vec<Effect> {
        let mut wire_text = self
            .skills
            .render_prompt(&prompt.text)
            .unwrap_or_else(|_| prompt.text.clone());
        // Provider sessions retain their original system instructions. Repeat the live catalogue on
        // later turns so agents added or removed after session creation are represented accurately.
        if self.provider_session_id.is_some() {
            wire_text.push_str("\n\n");
            wire_text.push_str(&self.nakode_current_agent_catalogue());
        }
        let prompt = OutgoingPrompt {
            id: prompt.id,
            text: prompt.text,
            wire_text,
            model: self
                .backend_capabilities
                .model_catalog
                .is_supported()
                .then(|| self.selected_model_for_active_provider())
                .flatten(),
            handoff: self.pending_handoff.take(),
            attachments: prompt.attachments,
        };
        let user_key = format!("user:{}", prompt.id);
        let images = prompt
            .attachments
            .iter()
            .filter_map(|attachment| {
                attachment
                    .image
                    .clone()
                    .map(|image| (attachment.label.clone(), image))
            })
            .collect::<Vec<_>>();
        self.transcript.set_labeled_images(user_key.clone(), images);
        self.transcript.upsert(
            user_key,
            EntryKind::User,
            format!("YOU · {}", prompt.id),
            &prompt.text,
            EntryStatus::Complete,
        );
        self.transcript.set_stream_active(true);

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
                owner_session_id: Some(self.nakode_session_id.clone()),
                parent_run_id: None,
                external_tools: self.external_tools.clone(),
                replace_builtin_tools: self.replace_builtin_tools,
                allowed_builtin_tools: None,
                max_turns: None,
                timeout_seconds: None,
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
            provider_session_id,
            client_id: prompt.id,
            prompt: wire_text,
            attachments: prompt.attachments,
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
        let starting = self.starting_turn.take();
        let model = starting
            .as_ref()
            .and_then(|prompt| prompt.model.clone())
            .or_else(|| self.selected_model.clone());
        if let Some(started) = &starting
            && self
                .redirect_start
                .as_ref()
                .is_some_and(|pending| pending.prompt.id == started.id)
        {
            self.redirect_start = None;
        }
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
            // A queued native steer was never accepted, so its message remains queued in place.
            self.pending_steer = None;
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
        if outcome == TurnOutcome::Failed && self.pending_redirect.is_some() {
            self.pending_redirect = None;
            self.status_message.push_str(" The selected follow-up remains queued; retry Steer now after the provider recovers.");
            return effects;
        }
        if let Some(pending) = self.pending_redirect.take() {
            effects.extend(self.begin_pending_redirect(&pending));
        } else {
            effects.extend(self.drain_queue());
        }
        effects
    }

    fn drain_queue(&mut self) -> Vec<Effect> {
        if !self.connection.is_ready() || self.is_busy() {
            return Vec::new();
        }
        let Some(prompt) = self.queue.pop_front() else {
            return Vec::new();
        };
        self.begin_prompt(prompt)
    }

    fn install_history(&mut self, history: Vec<SessionHistoryItem>) {
        self.active_shells.clear();
        self.transcript.clear();
        self.item_turns.clear();
        self.reasoning_summaries = ReasoningSummaryTracker::default();
        self.subagent_result_items.clear();
        for history_item in history {
            let SessionHistoryItem {
                turn_id,
                provider_id,
                model_id,
                item,
            } = history_item;
            if hides_subagent_item(&item) {
                continue;
            }
            self.item_turns.insert(item.id.clone(), turn_id);
            let item_id = item.id.clone();
            let tool_audit_json = item.tool_audit_json;
            self.transcript.upsert(
                item.id,
                entry_kind(item.kind),
                item.title,
                item.body,
                entry_status(item.status),
            );
            self.transcript
                .set_tool_audit(&item_id, tool_audit_json.map(Into::into));
            self.transcript
                .set_origin(&item_id, provider_id.as_deref(), model_id.as_deref());
        }
        if self.transcript.entries().is_empty() {
            self.transcript.push(
                EntryKind::System,
                "NAKODE",
                "Resumed session has no visible history.",
                EntryStatus::Complete,
            );
        }
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
        let item_id = item.id.clone();
        let tool_audit_json = item.tool_audit_json;
        self.transcript
            .upsert(item.id, entry_kind(item.kind), item.title, body, status);
        self.transcript
            .set_tool_audit(&item_id, tool_audit_json.map(Into::into));
        self.set_entry_turn_origin(&item_id, turn_id);
    }

    fn observe_delta(&mut self, turn_id: &str, item_id: &str, kind: DeltaKind, delta: &str) {
        if !self.turn_is_current(turn_id) {
            self.diagnostic_count += 1;
            return;
        }
        self.item_turns
            .insert(item_id.to_owned(), turn_id.to_owned());
        if self.subagent_result_items.contains(item_id)
            || (kind == DeltaKind::Tool && delta.contains("[Subagent Result]"))
        {
            self.subagent_result_items.insert(item_id.to_owned());
            self.transcript.remove(item_id);
            return;
        }
        let (entry_kind, title) = match kind {
            DeltaKind::ReasoningSummary { index } => {
                record_reasoning_summary_delta(
                    &mut self.transcript,
                    &mut self.reasoning_summaries,
                    turn_id,
                    item_id,
                    index,
                    delta,
                );
                self.set_entry_turn_origin(item_id, turn_id);
                return;
            }
            DeltaKind::Assistant => (EntryKind::Assistant, "ASSISTANT"),
            DeltaKind::Plan => (EntryKind::Reasoning, "PLAN"),
            DeltaKind::Reasoning => (EntryKind::Reasoning, "REASONING"),
            DeltaKind::Tool => (EntryKind::Tool, "TOOL OUTPUT"),
        };
        let assistant_anchor = if kind == DeltaKind::Reasoning {
            assistant_item_id_for_reasoning(item_id)
        } else {
            None
        };
        self.transcript
            .append_delta(item_id, entry_kind, title, delta);
        self.set_entry_turn_origin(item_id, turn_id);
        if let Some(anchor) = assistant_anchor {
            self.transcript.move_before(item_id, &anchor);
        }
    }

    fn set_entry_turn_origin(&mut self, item_id: &str, turn_id: &str) {
        let Some(turn) = self.active_turn.as_ref().filter(|turn| turn.id == turn_id) else {
            return;
        };
        self.transcript.set_origin(
            item_id,
            (!self.backend_provider.is_empty()).then_some(self.backend_provider.as_str()),
            turn.model.as_deref(),
        );
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
                } else if let Some(prompt) = self.starting_turn.take() {
                    let redirected = self
                        .redirect_start
                        .as_ref()
                        .is_some_and(|pending| pending.prompt.id == prompt.id);
                    if redirected {
                        if let Some(pending) = self.redirect_start.take() {
                            self.restore_redirect_start(pending);
                        }
                        self.status_message.push_str(
                            " The selected follow-up remains queued; retry Steer now after fixing the provider error.",
                        );
                        return Vec::new();
                    }
                    self.restore_failed_prompt(&prompt);
                    return self.drain_queue();
                } else {
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
                self.pending_redirect = None;
            }
        }
        Vec::new()
    }

    fn restore_failed_prompt(&mut self, prompt: &OutgoingPrompt) {
        self.transcript.set_stream_active(false);
        self.transcript
            .set_status(&format!("user:{}", prompt.id), EntryStatus::Failed);
        self.pending_handoff.clone_from(&prompt.handoff);
        self.recoverable_prompt = Some(RecoverablePrompt {
            id: prompt.id.clone(),
            text: prompt.text.clone(),
            attachments: prompt.attachments.clone(),
        });
        self.status_message
            .push_str(" Prompt is available to retry.");
    }

    /// Validates an agent catalog mutation before any persistence effect runs.
    ///
    /// # Errors
    /// Rejects malformed definitions, duplicate slugs, and stale rename
    /// targets.
    pub fn validate_agent_definition(
        &self,
        definition: &AgentDefinition,
        previous_slug: Option<&str>,
    ) -> Result<(), DomainCommandError> {
        AgentCatalog::validate_definition(definition)
            .map_err(|error| DomainCommandError::Invalid(error.to_string()))?;
        if let Some(existing_slug) = previous_slug.or(Some(definition.slug.as_str()))
            && let Some(existing) = self.agents.find(existing_slug)
        {
            if existing.ownership == crate::agent::AgentOwnership::BuiltIn {
                return Err(DomainCommandError::Invalid(format!(
                    "built-in agent {existing_slug:?} is immutable"
                )));
            }
            if definition.ownership != crate::agent::AgentOwnership::OwnerDefined {
                return Err(DomainCommandError::Invalid(
                    "owner-defined archetypes cannot become built-ins".to_owned(),
                ));
            }
        } else if definition.ownership != crate::agent::AgentOwnership::OwnerDefined {
            return Err(DomainCommandError::Invalid(
                "new archetypes must be owner-defined".to_owned(),
            ));
        }
        for model in definition
            .model
            .iter()
            .chain(definition.fallback_models.iter())
        {
            if !self
                .models
                .iter()
                .any(|candidate| candidate.qualified_id() == *model)
            {
                return Err(DomainCommandError::Invalid(format!(
                    "model {model} is not present in authoritative discovery"
                )));
            }
        }
        // A level is only ever valid FOR a model, so it is checked against the one this definition
        // names. Refused here rather than at run time: a definition that cannot run as written is
        // worth failing at the moment it is written, while the level list is on screen.
        if let Some(effort) = definition.reasoning_effort.as_deref()
            && let Some(model) = definition.model.as_deref()
        {
            let offered = model.split_once('/').is_some_and(|(provider, model)| {
                self.model_offers_reasoning_effort(provider, model, effort)
            });
            if !offered {
                return Err(DomainCommandError::Invalid(format!(
                    "model {model} does not offer reasoning effort {effort}"
                )));
            }
        }
        if self.agents.definitions().iter().any(|existing| {
            existing.slug == definition.slug
                && previous_slug.is_none_or(|previous| previous != existing.slug)
        }) {
            return Err(DomainCommandError::Conflict(format!(
                "agent {} already exists",
                definition.slug
            )));
        }
        if let Some(previous_slug) = previous_slug
            && previous_slug != definition.slug
            && self.agents.find(previous_slug).is_none()
        {
            return Err(DomainCommandError::NotFound(format!(
                "agent {previous_slug}"
            )));
        }
        Ok(())
    }

    /// Validates deletion while the authoritative catalogue is still in memory.
    ///
    /// # Errors
    ///
    /// Rejects an unknown definition or a built-in definition, which is immutable.
    pub fn validate_agent_deletion(&self, slug: &str) -> Result<(), DomainCommandError> {
        let definition = self
            .agents
            .find(slug)
            .ok_or_else(|| DomainCommandError::NotFound(format!("agent {slug}")))?;
        if definition.ownership == crate::agent::AgentOwnership::BuiltIn {
            return Err(DomainCommandError::Invalid(format!(
                "built-in agent {slug:?} is immutable; disable availability only through shipped policy"
            )));
        }
        Ok(())
    }

    /// Validates a delegated-agent request without mutating session state.
    ///
    /// # Errors
    /// Rejects an unknown agent, empty task, or exhausted concurrency budget.
    pub fn validate_agent_request(
        &self,
        agent_slug: &str,
        task: &str,
    ) -> Result<(), DomainCommandError> {
        let Some(definition) = self.agents.find(agent_slug) else {
            return Err(DomainCommandError::NotFound(format!(
                "predefined agent {agent_slug:?}"
            )));
        };
        if !definition.enabled {
            return Err(DomainCommandError::Invalid(format!(
                "agent {agent_slug:?} is disabled; enable it before delegation"
            )));
        }
        let running_total = self
            .subagents
            .iter()
            .filter(|run| {
                matches!(
                    run.status,
                    SubagentStatus::Starting | SubagentStatus::Working
                )
            })
            .count();
        if running_total >= MAX_CONCURRENT_SUBAGENTS {
            return Err(DomainCommandError::Conflict(format!(
                "The concurrent subagent limit ({MAX_CONCURRENT_SUBAGENTS}) is already in use. Wait for a running subagent to finish."
            )));
        }
        let running_for_archetype = self
            .subagents
            .iter()
            .filter(|run| {
                run.agent == agent_slug
                    && matches!(
                        run.status,
                        SubagentStatus::Starting | SubagentStatus::Working
                    )
            })
            .count();
        if running_for_archetype >= definition.max_concurrency as usize {
            return Err(DomainCommandError::Conflict(format!(
                "agent {agent_slug:?} already has its configured {} concurrent run(s)",
                definition.max_concurrency
            )));
        }
        let task = task.trim();
        if task.is_empty() {
            return Err(DomainCommandError::Invalid(
                "agent invocation requires a non-empty task".to_owned(),
            ));
        }
        let validator_slug = std::env::var("NAKODE_SECURITY_VALIDATOR_AGENT")
            .unwrap_or_else(|_| "security-validator".to_owned());
        if agent_slug == validator_slug
            && (definition
                .model
                .as_ref()
                .is_none_or(|model| !model.to_ascii_lowercase().contains("sonnet"))
                || definition
                    .fallback_models
                    .iter()
                    .any(|model| !model.to_ascii_lowercase().contains("sonnet")))
        {
            return Err(DomainCommandError::Invalid(format!(
                "security validator {validator_slug:?} must configure only Sonnet-tier models"
            )));
        }
        Ok(())
    }

    /// Creates one attributed delegated run and returns its stable identity.
    ///
    /// # Errors
    /// Rejects invalid requests before mutating the session.
    pub fn delegate_agent(
        &mut self,
        agent_slug: &str,
        task: &str,
    ) -> Result<(String, Vec<Effect>), DomainCommandError> {
        self.delegate_agent_attributed(agent_slug, task, None)
    }

    /// Creates one delegated run linked to its active parent and enforces the parent's recursion
    /// budget before mutating state.
    ///
    /// # Errors
    /// Rejects missing/inactive parents, disallowed delegation, exhausted depth, and invalid
    /// archetype requests.
    #[allow(clippy::too_many_lines)]
    pub fn delegate_agent_attributed(
        &mut self,
        agent_slug: &str,
        task: &str,
        parent_run_id: Option<&str>,
    ) -> Result<(String, Vec<Effect>), DomainCommandError> {
        self.delegate_agent_attributed_for_request(agent_slug, task, parent_run_id, 0)
    }

    /// Creates one delegated run whose terminal effect is correlated to a native runtime waiter.
    /// Existing UI/CLI delegations use request id zero and keep their historical projection path.
    pub(crate) fn delegate_agent_attributed_for_request(
        &mut self,
        agent_slug: &str,
        task: &str,
        parent_run_id: Option<&str>,
        request_id: u64,
    ) -> Result<(String, Vec<Effect>), DomainCommandError> {
        self.validate_agent_request(agent_slug, task)?;
        let task = task.trim();
        let Some(definition) = self.agents.find(agent_slug).cloned() else {
            return Err(DomainCommandError::NotFound(format!(
                "predefined agent {agent_slug:?}"
            )));
        };
        let (parent_run_id, remaining_delegation_depth) =
            self.delegation_context(parent_run_id, &definition)?;

        let run_id = Self::next_id("agent");
        let model_targets = agent_model_targets(&definition, &self.backend_provider);
        let provider = model_targets[0].provider.clone();
        let run = SubagentRun {
            id: run_id.clone(),
            agent: definition.slug.clone(),
            provider: provider.clone(),
            model: model_targets[0].model.clone(),
            provider_session_id: None,
            usage: crate::backend::BackendTokenUsage::default(),
            objective: task.to_owned(),
            status: SubagentStatus::Starting,
            latest_activity: "Starting provider…".to_owned(),
            observability: SubagentObservability {
                parent_run_id: parent_run_id.clone(),
                archetype_purpose: definition.description.clone(),
                policy_json: serde_json::to_string(&definition).unwrap_or_else(|_| "{}".to_owned()),
                remaining_delegation_depth,
                started_at_ms: unix_time_ms(),
                ..SubagentObservability::default()
            },
        };
        self.subagents.push(run.clone());
        self.sync_inline_subagent(&run);
        let mut transcript = DomainTranscript::new(self.transcript_limit);
        transcript.set_stream_label(definition.slug.clone());
        transcript.set_stream_active(true);
        transcript.push(
            EntryKind::User,
            "PARENT",
            format!(
                "{}\n\n[Nakode Run Attribution]\nRun ID: {run_id}\nParent run: {}\nRemaining delegation depth: {remaining_delegation_depth}\n[/Nakode Run Attribution]",
                definition.initial_prompt(task),
                parent_run_id.as_deref().unwrap_or("root"),
            ),
            EntryStatus::Complete,
        );
        self.subagent_chats.insert(
            run_id.clone(),
            SubagentChat {
                transcript,
                reasoning_summaries: ReasoningSummaryTracker::default(),
            },
        );
        self.subagent_executions.insert(
            run_id.clone(),
            SubagentExecution {
                run,
                definition,
                request_id,
                task: task.to_owned(),
                parent_run_id,
                remaining_delegation_depth,
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
        Ok((run_id, effects))
    }

    fn delegation_context(
        &self,
        parent_run_id: Option<&str>,
        definition: &AgentDefinition,
    ) -> Result<(Option<String>, u32), DomainCommandError> {
        let Some(parent_run_id) = parent_run_id else {
            return Ok((None, definition.max_delegation_depth));
        };
        let parent = self.subagent_executions.get(parent_run_id).ok_or_else(|| {
            DomainCommandError::NotFound(format!("parent agent run `{parent_run_id}`"))
        })?;
        if !matches!(
            parent.run.status,
            SubagentStatus::Starting | SubagentStatus::Working
        ) {
            return Err(DomainCommandError::Conflict(format!(
                "parent agent run `{parent_run_id}` is no longer active"
            )));
        }
        if !parent.definition.can_delegate {
            return Err(DomainCommandError::Unsupported(format!(
                "agent `{}` is not permitted to delegate",
                parent.definition.slug
            )));
        }
        let remaining = parent
            .remaining_delegation_depth
            .checked_sub(1)
            .ok_or_else(|| {
                DomainCommandError::Unsupported(format!(
                    "agent `{}` exhausted its maximum delegation depth",
                    parent.definition.slug
                ))
            })?;
        Ok((
            Some(parent_run_id.to_owned()),
            remaining.min(definition.max_delegation_depth),
        ))
    }

    pub fn invoke_agent(&mut self, request: &AgentRequest) -> Vec<Effect> {
        match self.delegate_agent_attributed_for_request(
            &request.agent,
            &request.task,
            None,
            request.id,
        ) {
            Ok((_, effects)) => effects,
            Err(error) => vec![Effect::CompleteAgentRequest {
                request_id: request.id,
                result: error.to_string(),
                success: false,
            }],
        }
    }

    fn native_delegation_callable(&self) -> bool {
        matches!(
            self.backend_provider.as_str(),
            CODEX_PROVIDER | KIMI_PROVIDER | GLM_PROVIDER | DEVIN_PROVIDER
        ) && self.backend_capabilities.native_tools.is_supported()
            && !self.replace_builtin_tools
    }

    fn rendered_agent_catalogue(&self) -> String {
        if !self.native_delegation_callable() {
            return "- none (this provider session has no callable Nakode delegation tool)"
                .to_owned();
        }
        let agents = self
            .agents
            .definitions()
            .iter()
            .map(|agent| {
                format!(
                    "- {}: {}\n  Callable: {}({{\"agent\":\"{}\",\"task\":\"<bounded task>\"}})",
                    agent.slug,
                    agent.description.trim(),
                    NAKODE_AGENT_TOOL_NAME,
                    agent.slug,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        if agents.is_empty() {
            "- none".to_owned()
        } else {
            agents
        }
    }

    fn nakode_current_agent_catalogue(&self) -> String {
        format!(
            "[Nakode Current Agent Catalogue]\nThis authoritative list supersedes the initial Available agents list for this turn.\nAvailable agents:\n{}\n[/Nakode Current Agent Catalogue]",
            self.rendered_agent_catalogue()
        )
    }

    fn nakode_system_instructions(&self) -> String {
        let agents = self.rendered_agent_catalogue();
        let model = self.selected_model.as_ref().map_or_else(
            || format!("{}/provider-default", self.backend_provider),
            Clone::clone,
        );
        let base = format!(
            "[Nakode System Instructions]\nYou are operating inside Nakode.\nSession ID: {}\nModel: {}\nProvider: {}\nNakode delegation is exposed only when the provider's callable schema contains the session-bound `{tool}` tool. It routes through the Nakode control plane, not provider-native collaboration or a shell subprocess.\nInitial available agents:\n{}\nThis catalogue can change during a session; a later [Nakode Current Agent Catalogue] block supersedes this initial list.\nWhen `{tool}` is callable, use it for a concrete bounded delegation request; owner session and parent-run attribution are bound by the server and must not be supplied by you. Do not claim that an agent is available when this catalogue says the callable is absent. Do not use provider-native subagent or collaboration features because Nakode cannot supervise or attribute those children. Up to {MAX_CONCURRENT_SUBAGENTS} subagents may run concurrently. When several independent tasks would benefit from parallel investigation, launch one Nakode delegation per task concurrently. Keep each objective distinct and bounded. Each delegation returns its attributed terminal result when the child finishes; incorporate all relevant results into your response.\n[/Nakode System Instructions]",
            self.nakode_session_id,
            model,
            self.backend_provider,
            agents,
            tool = NAKODE_AGENT_TOOL_NAME,
        );
        self.prompt_addenda
            .apply(&base, self.selected_model.as_deref())
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

    fn answer_subagent_question(run_id: &str, question_id: String) -> Vec<Effect> {
        vec![Effect::SubagentBackend {
            run_id: run_id.to_owned(),
            command: BackendCommand::ResolveQuestion {
                id: question_id,
                answer: crate::backend::QuestionAnswer::Text(
                    "No interactive user is attached to this subagent; continue with best judgment."
                        .to_owned(),
                ),
            },
        }]
    }

    fn apply_subagent_usage(
        &mut self,
        run_id: &str,
        usage: crate::backend::BackendTokenUsage,
    ) -> Vec<Effect> {
        if let Some(execution) = self.subagent_executions.get_mut(run_id) {
            execution.run.usage = usage;
        }
        self.sync_subagent(run_id);
        Vec::new()
    }

    fn record_subagent_warning(&mut self, run_id: &str, message: &str) -> Vec<Effect> {
        self.record_subagent_message(run_id, EntryKind::Warning, "WARNING", message);
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        execution.run.latest_activity = summarize_activity(message, "Provider warning");
        self.sync_subagent(run_id);
        Vec::new()
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
            BackendEvent::Ready(identity) => {
                self.start_subagent_session(run_id, &identity.capabilities)
            }
            BackendEvent::SessionCreated {
                provider_session_id,
                model,
            } => self.start_subagent_turn(run_id, provider_session_id, &model),
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
            BackendEvent::QuestionRequested(request) => {
                Self::answer_subagent_question(run_id, request.id)
            }
            BackendEvent::TokenUsageUpdated { usage } => self.apply_subagent_usage(run_id, usage),
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
                self.record_subagent_warning(run_id, &message)
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
            | BackendEvent::ExternalToolRequested(_)
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
            execution.run.model.clone_from(&target.model);
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

    #[allow(clippy::format_push_string)]
    fn start_subagent_session(
        &mut self,
        run_id: &str,
        capabilities: &BackendCapabilities,
    ) -> Vec<Effect> {
        let unsupported = self.subagent_executions.get(run_id).and_then(|execution| {
            (execution.definition.requires_scoped_runtime_policy()
                && !capabilities.scoped_runtime_policy.is_supported())
            .then(|| {
                format!(
                    "provider {} cannot enforce the scoped tool/turn policy required by archetype {:?}",
                    execution.run.provider, execution.definition.slug
                )
            })
        });
        if let Some(message) = unsupported {
            return self.retry_subagent_or_finish(run_id, message);
        }
        let prompt_addenda = self.prompt_addenda.clone();
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        execution.run.status = SubagentStatus::Starting;
        "Creating native session…".clone_into(&mut execution.run.latest_activity);
        let target = &execution.model_targets[execution.model_target_index];
        let model = target.model.clone();
        let qualified_model = model
            .as_ref()
            .map(|model| format!("{}/{model}", target.provider));
        let mut validator_instructions = execution.definition.instructions().to_owned();
        let policy = &execution.definition;
        append_archetype_policy_instructions(&mut validator_instructions, policy);
        validator_instructions.push_str(
            "\n\nIf the delegated objective materially requires capabilities this archetype does not have, do not attempt it. Return this exact bounded handoff block as the final report:\n[Nakode Objective Mismatch]\nMissing capability: <one concise line>\nBetter archetype: <slug or concise archetype description>\n[/Nakode Objective Mismatch]",
        );
        let _ = write!(
            validator_instructions,
            "\n\n[Nakode Run Attribution]\nRun ID: {run_id}\nParent run: {}\nRemaining delegation depth: {}\n[/Nakode Run Attribution]",
            execution.parent_run_id.as_deref().unwrap_or("root"),
            execution.remaining_delegation_depth,
        );
        let validator_slug = std::env::var("NAKODE_SECURITY_VALIDATOR_AGENT")
            .unwrap_or_else(|_| "security-validator".to_owned());
        if execution.definition.slug == validator_slug {
            validator_instructions.insert_str(
                0,
                "[Nakode Security Validator]\nDelegation and security re-validation are disabled for this scoped validator run.\n[/Nakode Security Validator]\n\n",
            );
        }
        let replace_builtin_tools = execution.definition.tool_profile == AgentToolProfile::None;
        let allowed_builtin_tools = execution.definition.builtin_tool_allowlist();
        let max_turns = execution.definition.max_turns;
        let timeout_seconds = execution.definition.timeout_seconds;
        let instructions =
            Some(prompt_addenda.apply(&validator_instructions, qualified_model.as_deref()));
        self.sync_subagent(run_id);
        vec![Effect::SubagentBackend {
            run_id: run_id.to_owned(),
            command: BackendCommand::StartSession {
                model,
                instructions,
                owner_session_id: Some(self.nakode_session_id.clone()),
                parent_run_id: Some(run_id.to_owned()),
                external_tools: Vec::new(),
                replace_builtin_tools,
                allowed_builtin_tools,
                max_turns,
                timeout_seconds,
            },
        }]
    }

    fn start_subagent_turn(
        &mut self,
        run_id: &str,
        provider_session_id: String,
        reported_model: &str,
    ) -> Vec<Effect> {
        let Some((target, agent_fast_mode, agent_effort)) =
            self.subagent_executions.get(run_id).map(|execution| {
                (
                    execution.model_targets[execution.model_target_index].clone(),
                    execution.definition.fast_mode,
                    execution.definition.reasoning_effort.clone(),
                )
            })
        else {
            return Vec::new();
        };
        let model = target.model.clone();
        let options_model = model
            .as_deref()
            .or((!reported_model.is_empty()).then_some(reported_model));
        let mut options = options_model
            .map(|model| self.model_options_for_qualified(&format!("{}/{model}", target.provider)))
            .unwrap_or_default();
        options.fast_mode |= agent_fast_mode;
        // The archetype's own level beats whatever the workspace has saved for this model — that is
        // what defining one on the definition means. `None` changes nothing, so a definition written
        // before the field existed runs exactly as it did: at the model's own default.
        //
        // It is applied only if the model actually starting takes it. Saving already refuses a level
        // the definition's OWN model does not offer, so what this catches is a FALLBACK model with a
        // different vocabulary, which is worth saying out loud rather than sending to be refused.
        let mut defined_effort_applied = false;
        let mismatch = if let Some(effort) = agent_effort {
            let offers = options_model.is_some_and(|model| {
                self.model_offers_reasoning_effort(&target.provider, model, &effort)
            });
            if offers {
                options.reasoning_effort = Some(effort);
                defined_effort_applied = true;
                None
            } else {
                Some(effort)
            }
        } else {
            None
        };
        if let Some(effort) = mismatch {
            self.record_subagent_message(
                run_id,
                EntryKind::Warning,
                "MODEL",
                &format!(
                    "reasoning effort {effort:?} is not available on {}; running at the model's own default",
                    options_model.unwrap_or(&target.provider)
                ),
            );
        }
        let Some(execution) = self.subagent_executions.get_mut(run_id) else {
            return Vec::new();
        };
        execution.session_id = Some(provider_session_id.clone());
        execution.run.provider_session_id = Some(provider_session_id.clone());
        execution.run.model = options_model.map(str::to_owned);
        execution.run.status = SubagentStatus::Working;
        "Working…".clone_into(&mut execution.run.latest_activity);
        let prompt = execution.definition.initial_prompt(&execution.task);
        self.sync_subagent(run_id);
        let mut effects = Vec::new();
        // Cursor as before, plus the one new case: an archetype that DEFINES a level has to have it
        // delivered, and this command is how a level reaches a session.
        //
        // Deliberately not widened past that. Every other provider's delegated runs have never been
        // sent these options — the workspace's own saved level and fast mode are still computed above
        // and still dropped for them — and starting to send them is a change to how existing runs
        // behave, which is not this change's business.
        if defined_effort_applied || target.provider == CURSOR_PROVIDER {
            effects.push(Effect::SubagentBackend {
                run_id: run_id.to_owned(),
                command: BackendCommand::SetSessionOptions {
                    provider_session_id: provider_session_id.clone(),
                    options,
                },
            });
        }
        effects.push(Effect::SubagentBackend {
            run_id: run_id.to_owned(),
            command: BackendCommand::StartTurn {
                provider_session_id,
                client_id: format!("{run_id}-prompt"),
                prompt,
                attachments: Vec::new(),
                model,
            },
        });
        effects
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
        if kind == DeltaKind::Reasoning
            && let Some(anchor) = assistant_item_id_for_reasoning(item_id)
        {
            chat.transcript.move_before(item_id, &anchor);
        }
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

    #[cfg(test)]
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
            execution.run.observability.ended_at_ms = Some(unix_time_ms());
            execution.run.observability.termination_kind = Some("cancelled".to_owned());
            execution.run.observability.termination_detail =
                Some("Interrupted by the parent agent.".to_owned());
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
        execution.run.observability.objective_mismatch_handoff =
            objective_mismatch_handoff(&execution.response);
        let (success, body) = match outcome {
            Ok(()) if !execution.response.trim().is_empty() => {
                execution.run.status = SubagentStatus::Completed;
                "Completed".clone_into(&mut execution.run.latest_activity);
                execution.run.observability.termination_kind = Some("completed".to_owned());
                execution.run.observability.termination_detail = None;
                (true, execution.response.trim().to_owned())
            }
            Ok(()) => {
                execution.run.status = SubagentStatus::Failed;
                "Returned no response".clone_into(&mut execution.run.latest_activity);
                execution.run.observability.termination_kind = Some("failed".to_owned());
                execution.run.observability.termination_detail =
                    Some("Subagent returned no assistant response.".to_owned());
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
                execution.run.observability.termination_kind = Some(
                    if message.starts_with("archetype runtime exceeded its configured") {
                        "timed_out"
                    } else {
                        "failed"
                    }
                    .to_owned(),
                );
                execution.run.observability.termination_detail = Some(message.clone());
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
        execution.run.observability.ended_at_ms = Some(unix_time_ms());
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
            model: run.model.clone(),
            provider_session_id: run.provider_session_id.clone(),
            input_tokens: run.usage.input_tokens,
            output_tokens: run.usage.output_tokens,
            cached_input_tokens: run.usage.cached_input_tokens,
            cache_write_tokens: run.usage.cache_write_tokens,
            objective: run.objective.clone(),
            status: run.status,
            latest_activity: run.latest_activity.clone(),
            observability: run.observability.clone(),
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
            self.session_model_options_override = None;
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

    pub fn shell_output(&mut self, id: &str, output: &str) {
        self.transcript
            .replace_body(id, output, EntryStatus::Running);
    }

    pub fn shell_finished(
        &mut self,
        id: &str,
        output: &str,
        exit_code: Option<i32>,
        interrupted: bool,
    ) {
        self.active_shells.remove(id);
        let status = if interrupted {
            EntryStatus::Interrupted
        } else if exit_code == Some(0) {
            EntryStatus::Complete
        } else {
            EntryStatus::Failed
        };
        self.transcript.replace_body(id, output, status);
        self.status_message = if interrupted {
            "Shell command interrupted.".to_owned()
        } else if let Some(exit_code) = exit_code {
            format!("Shell command exited with code {exit_code}.")
        } else {
            "Shell command ended without an exit code.".to_owned()
        };
    }

    pub fn shell_failed(&mut self, id: &str, message: &str) {
        self.active_shells.remove(id);
        self.transcript
            .replace_body(id, message, EntryStatus::Failed);
        message.clone_into(&mut self.status_message);
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

    fn next_id(kind: &str) -> String {
        format!("nakode-{kind}-{}", uuid::Uuid::now_v7())
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
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

fn assistant_item_id_for_reasoning(item_id: &str) -> Option<String> {
    let (prefix, round) = item_id.rsplit_once(":reasoning:")?;
    Some(format!("{prefix}:assistant:{round}"))
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
    transcript: &mut DomainTranscript,
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

fn objective_mismatch_handoff(text: &str) -> Option<String> {
    const START: &str = "[Nakode Objective Mismatch]";
    const END: &str = "[/Nakode Objective Mismatch]";
    let block = text.split_once(START)?.1.split_once(END)?.0;
    let missing = block
        .lines()
        .find_map(|line| line.trim().strip_prefix("Missing capability:"))?
        .trim();
    let better = block
        .lines()
        .find_map(|line| line.trim().strip_prefix("Better archetype:"))?
        .trim();
    if missing.is_empty() || better.is_empty() {
        return None;
    }
    let missing = missing.chars().take(512).collect::<String>();
    let better = better.chars().take(512).collect::<String>();
    Some(format!(
        "Missing capability: {missing}\nBetter archetype: {better}"
    ))
}

fn summarize_activity(text: &str, fallback: &str) -> String {
    let summary = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(fallback);
    summary.chars().take(120).collect()
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
    let fallback_models: &[String] =
        if definition.fallback_policy == AgentFallbackPolicy::ConfiguredOnly {
            &definition.fallback_models
        } else {
            &[]
        };
    for model in fallback_models {
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
            | BackendEvent::TokenUsageUpdated { .. }
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
            BackendIdentity, BackendOperation, CLAUDE_PROVIDER, CODEX_PROVIDER, CURSOR_PROVIDER,
            CapabilitySupport, CompactionReason, DEVIN_PROVIDER, DeltaKind, ItemKind, ItemStatus,
            ModelCapabilities, ModelInfo, ModelOptions, NormalizedItem, PromptAttachment,
            PromptImage, QuestionOption, QuestionRequest, SessionHistoryItem, TodoItem, TodoPhase,
            TodoStatus, TurnOutcome,
        },
        domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
        personality::PromptAddenda,
        session::{SessionRecord, SubagentObservability, SubagentRecord},
        skill::SkillCatalog,
        state::projection,
    };
    use tempfile::tempdir;

    use super::{
        AgentEditorField, AgentRequest, AppState, ApprovalDecision, Effect, SubagentStatus,
        model_supports_options, objective_mismatch_handoff,
    };

    #[test]
    fn objective_mismatch_handoff_requires_the_exact_bounded_report_protocol() {
        let report = "Context\n[Nakode Objective Mismatch]\nMissing capability: a bounded command deadline\nBetter archetype: test-runner\n[/Nakode Objective Mismatch]";
        assert_eq!(
            objective_mismatch_handoff(report).as_deref(),
            Some("Missing capability: a bounded command deadline\nBetter archetype: test-runner")
        );
        assert_eq!(
            objective_mismatch_handoff("Missing capability: shell\nBetter archetype: test-runner"),
            None
        );
        let oversized = format!(
            "[Nakode Objective Mismatch]\nMissing capability: {}\nBetter archetype: {}\n[/Nakode Objective Mismatch]",
            "x".repeat(2_000),
            "y".repeat(2_000)
        );
        let handoff = objective_mismatch_handoff(&oversized).expect("structured handoff");
        assert!(handoff.len() <= 1_100);
    }

    #[test]
    fn primary_system_instructions_include_model_personality_and_soul() {
        let directory = tempdir().expect("config directory");
        let personalities = directory.path().join("personalities.toml");
        let soul = directory.path().join("SOUL.md");
        fs::write(
            &personalities,
            "default = \"Default personality\"\n[models]\n\"openai-codex/model-a\" = \"Model A personality\"\n",
        )
        .expect("personalities");
        fs::write(&soul, "Agent identity").expect("soul");
        let mut state = ready_state();
        state.selected_model = Some("openai-codex/model-a".to_owned());
        state.install_prompt_addenda(
            PromptAddenda::load(Some(&personalities), Some(&soul)).expect("addenda"),
        );

        let instructions = state.nakode_system_instructions();
        assert!(instructions.contains("[Nakode System Instructions]"));
        assert!(instructions.contains("[Personality]\nModel A personality"));
        assert!(!instructions.contains("Default personality"));
        assert!(instructions.contains("[Soul]\nAgent identity"));
    }

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

    fn recursive_catalog() -> AgentCatalog {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("recursive.toml"),
            r#"
slug = "recursive"
description = "Delegates exactly one level"
system_prompt = "Perform bounded work."
first_message = "Complete the delegated question."
model = "openai-codex/model-a"
can_delegate = true
max_delegation_depth = 1
"#,
        )
        .expect("agent definition");
        fs::write(
            directory.path().join("leaf.toml"),
            r#"
slug = "leaf"
description = "Does not delegate"
system_prompt = "Perform bounded work."
first_message = "Complete the delegated question."
model = "openai-codex/model-a"
"#,
        )
        .expect("leaf agent definition");
        AgentCatalog::load(directory.path()).expect("agent catalog")
    }

    fn routed_explorer_catalog() -> AgentCatalog {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores code context"
system_prompt = "Explore carefully and report concrete context."
first_message = "Inspect the delegated question."
model = "devin-acp/swe-1-7-lightning"
fallback_models = ["openai-codex/gpt-5.6-luna"]
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
                external_tools: CapabilitySupport::Supported,
                scoped_runtime_policy: CapabilitySupport::Supported,
                mcp: CapabilitySupport::Supported,
                close_session: CapabilitySupport::Supported,
            },
        }));
        state.handle_backend(BackendEvent::Models(vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-a".to_owned(),
            is_default: true,
            capabilities: crate::codex::model_capabilities(),
        }]));
        state
    }

    #[test]
    fn external_tools_reject_a_provider_that_cannot_execute_them() {
        let mut state = ready_state();
        state
            .configure_external_tools(
                vec![nakode_protocol::ExternalToolDefinition {
                    name: "dashboard_read".to_owned(),
                    description: "Read dashboard state".to_owned(),
                    input_schema_json: r#"{"type":"object"}"#.to_owned(),
                }],
                true,
            )
            .expect("external tools configure before the first prompt");
        state.backend_capabilities.external_tools = CapabilitySupport::Unsupported;

        let error = state
            .submit_prompt("inspect the dashboard".to_owned(), Vec::new())
            .expect_err("unsupported providers must reject external tool sessions");

        assert!(error.to_string().contains("native Nakode provider"));
    }

    #[test]
    fn leading_bang_submits_a_local_shell_command_without_a_backend_turn() {
        let mut state = ready_state();
        state.client.editor.set_text("!./install.sh --check");
        assert!(state.is_shell_mode());

        let effects = state.submit_editor();
        let [Effect::RunShell { id, command }] = effects.as_slice() else {
            panic!("expected a shell effect");
        };
        assert_eq!(command, "./install.sh --check");
        assert!(state.client.editor.is_blank());
        let entry = state.transcript.entries().last().expect("shell entry");
        assert_eq!(entry.key.as_deref(), Some(id.as_str()));
        assert_eq!(entry.title, "$ ./install.sh --check");
        assert_eq!(entry.kind, EntryKind::System);
        assert_eq!(entry.status, EntryStatus::Running);
    }

    #[test]
    fn shell_command_runs_locally_while_an_agent_turn_is_active() {
        let mut state = ready_state();
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.client.editor.set_text("!pwd");

        assert!(matches!(
            state.submit_or_steer_editor().as_slice(),
            [Effect::RunShell { command, .. }] if command == "pwd"
        ));
    }

    #[test]
    fn shell_output_updates_the_ephemeral_transcript_entry() {
        let mut state = ready_state();
        state.client.editor.set_text("!printf hello");
        let effects = state.submit_editor();
        let [Effect::RunShell { id, .. }] = effects.as_slice() else {
            panic!("expected a shell effect");
        };

        state.shell_output(id, "hel");
        state.shell_output(id, "hello");
        state.shell_finished(id, "hello", Some(0), false);

        let entry = state.transcript.entries().last().expect("shell entry");
        assert_eq!(entry.body, "hello");
        assert_eq!(entry.status, EntryStatus::Complete);
        assert_eq!(state.status_message, "Shell command exited with code 0.");
    }

    #[test]
    fn cancelling_session_work_interrupts_active_shell_commands() {
        let mut state = ready_state();
        let effects = state
            .run_shell_command("sleep 30".to_owned())
            .expect("shell command");
        let [Effect::RunShell { id, .. }] = effects.as_slice() else {
            panic!("expected a shell effect");
        };
        let id = id.clone();

        let effects = state.cancel_session_work().expect("shell cancellation");

        assert!(matches!(effects.as_slice(), [Effect::CancelShell(cancelled)] if cancelled == &id));
        assert_eq!(state.status_message, "Interrupting shell command…");
    }

    #[test]
    fn empty_shell_command_keeps_the_draft() {
        let mut state = ready_state();
        state.client.editor.set_text("!  ");

        assert!(state.submit_editor().is_empty());
        assert_eq!(state.client.editor.text(), "!  ");
        assert_eq!(state.status_message, "Write a shell command after !.");
    }

    #[test]
    fn cursor_agent_model_selection_opens_shared_fast_mode_options() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.models.push(ModelInfo {
            provider: CURSOR_PROVIDER.to_owned(),
            id: "composer-2.5".to_owned(),
            is_default: true,
            capabilities: crate::backend::ModelCapabilities::default(),
        });
        state.open_agent_picker();
        state.edit_selected_agent();
        state
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .expect("agent editor")
            .field = AgentEditorField::Model;

        state.open_agent_model_dropdown();
        state.agent_editor_insert_str("composer");
        state.select_agent_model_dropdown();
        assert!(state.agent_model_options_are_open());
        state.adjust_agent_model_options(1);
        let effects = state.apply_agent_model_options();
        assert!(!state.agent_model_options_are_open());
        assert!(matches!(
            effects.as_slice(),
            [Effect::SaveAgent { definition, .. }]
                if definition.model.as_deref() == Some("cursor-sdk/composer-2.5")
                && definition.fast_mode
        ));
    }

    #[test]
    fn agent_model_dropdown_searches_catalog_and_applies_selection() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.open_agent_picker();
        state.edit_selected_agent();
        state
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .expect("agent editor")
            .field = AgentEditorField::Model;

        state.open_agent_model_dropdown();
        assert!(state.agent_model_dropdown_is_open());
        state.agent_editor_insert_str("model a");
        state.select_agent_model_dropdown();
        let editor = state
            .client
            .agent_picker
            .as_ref()
            .and_then(|picker| picker.editor.as_ref())
            .expect("agent editor");
        assert_eq!(editor.model, "openai-codex/model-a");
        assert!(editor.model_dropdown.is_none());

        state.open_agent_model_dropdown();
        state.agent_editor_insert_str("inherit");
        state.select_agent_model_dropdown();
        assert_eq!(
            state
                .client
                .agent_picker
                .as_ref()
                .and_then(|picker| picker.editor.as_ref())
                .expect("agent editor")
                .model,
            ""
        );
    }

    fn install_review_skill(state: &mut AppState) {
        let workspace = tempdir().expect("skill workspace");
        let directory = workspace.path().join(".agents/skills/review");
        fs::create_dir_all(&directory).expect("skill directory");
        fs::write(
            directory.join("SKILL.md"),
            "---\nname: review\ndescription: Review code carefully\n---\n\nCheck correctness and tests.\n",
        )
        .expect("skill definition");
        state.install_skills(SkillCatalog::load(workspace.path()).expect("skill catalog"));
    }

    #[test]
    fn attached_image_is_labeled_and_sent_with_the_turn() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "session-with-image".to_owned(),
            model: "model-a".to_owned(),
        });
        state.insert_attachments(vec![PromptAttachment {
            label: "Image".to_owned(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            }),
        }]);

        assert_eq!(state.client.editor.text(), "[Image] ");
        let effects = state.submit_editor();
        let Effect::Backend(BackendCommand::StartTurn { attachments, .. }) =
            effects.last().unwrap()
        else {
            panic!("expected image prompt to start a turn");
        };
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].label, "Image");
        let user_key = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.kind == EntryKind::User)
            .and_then(|entry| entry.key.as_deref())
            .expect("user transcript key");
        assert_eq!(
            state.transcript.image(user_key, 0),
            attachments[0].image.as_ref()
        );
    }

    #[test]
    fn discovered_skills_complete_and_attach_to_wire_prompts() {
        let mut state = ready_state();
        install_review_skill(&mut state);
        state.client.editor.set_text("Please use /skill:rev");
        let completion = state
            .selected_command_completion()
            .expect("skill completion");
        assert_eq!(completion.replacement(), "/skill:review");
        state.accept_command_completion();
        assert_eq!(state.client.editor.text(), "Please use /skill:review");

        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "session-with-skills".to_owned(),
            model: "model-a".to_owned(),
        });
        let effects = state.submit_editor();
        let Effect::Backend(BackendCommand::StartTurn { prompt, .. }) = effects.last().unwrap()
        else {
            panic!("expected skill prompt to start a turn");
        };
        assert!(prompt.contains("# Nakode attached skills"));
        assert!(prompt.contains("Check correctness and tests."));
        assert_eq!(
            state.transcript.entries().last().unwrap().body,
            "Please use /skill:review"
        );
    }

    #[test]
    fn unknown_skill_preserves_the_draft() {
        let mut state = ready_state();
        state.client.editor.set_text("Use /skill:missing");
        assert!(state.submit_editor().is_empty());
        assert_eq!(state.client.editor.text(), "Use /skill:missing");
        assert!(
            state
                .status_message
                .contains("Unknown skill /skill:missing")
        );
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
        state.client.editor.set_text("steer");

        assert!(state.steer_editor().is_empty());
        assert_eq!(state.client.editor.text(), "steer");
        assert!(state.status_message.contains("does not support steering"));

        state.active_turn = None;
        state.client.editor.set_text("/compress");
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
        state.client.editor.set_text("/compress");

        let effects = state.submit_editor();

        let [
            Effect::Backend(BackendCommand::CompactSession {
                provider_session_id: session_id,
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
        assert!(state.client.editor.text().is_empty());
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
                provider_session_id: session_id,
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
    fn fresh_session_effort_reaches_provider_before_the_first_turn() {
        let mut state = ready_state();
        let session_id = nakode_protocol::SessionId::from(state.nakode_session_id.clone());
        state
            .select_model_intent(
                &nakode_protocol::ModelTarget::Session { session_id },
                &nakode_protocol::ModelId::from("openai-codex/model-a"),
                &nakode_protocol::ModelOptions {
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: false,
                },
            )
            .expect("model selection");

        let projected = projection::bootstrap(&state, 1, &[], &[])
            .active_session
            .expect("active session");
        assert_eq!(
            projected.selected_model_options.reasoning_effort.as_deref(),
            Some("high")
        );

        let effects = state
            .submit_prompt("first real prompt".to_owned(), Vec::new())
            .expect("prompt accepted");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::StartSession {
                model: Some(model),
                ..
            })] if model == "model-a"
        ));

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "thread-high".to_owned(),
            model: "model-a".to_owned(),
        });
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { .. },
                Effect::Backend(BackendCommand::SetSessionOptions {
                    provider_session_id,
                    options,
                }),
                Effect::Backend(BackendCommand::StartTurn { .. })
            ] if provider_session_id == "thread-high"
                && options.reasoning_effort.as_deref() == Some("high")
                && !options.fast_mode
        ));
    }

    #[test]
    fn devin_model_selection_is_applied_before_native_session_creation() {
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
        state.handle_backend(BackendEvent::Models(vec![
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-a".to_owned(),
                is_default: true,
                capabilities: crate::backend::ModelCapabilities::default(),
            },
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
                capabilities: crate::backend::ModelCapabilities::default(),
            },
        ]));
        let session_id = nakode_protocol::SessionId::from(state.nakode_session_id.clone());
        let selection = state
            .select_model_intent(
                &nakode_protocol::ModelTarget::Session { session_id },
                &nakode_protocol::ModelId::from("devin-acp/model-b"),
                &nakode_protocol::ModelOptions::default(),
            )
            .expect("model selection");
        assert!(selection.is_empty());

        let effects = state
            .submit_prompt("first real prompt".to_owned(), Vec::new())
            .expect("prompt accepted");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::StartSession {
                model: Some(model),
                ..
            })] if model == "model-b"
        ));

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "devin-session".to_owned(),
            model: "model-b".to_owned(),
        });
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
        state.client.editor.set_text("first");
        let first = state.submit_editor();
        assert!(matches!(first.as_slice(), [Effect::Backend(_)]));
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-1".to_owned(),
        });

        state.client.editor.set_text("second");
        state.submit_editor();
        state.client.editor.set_text("third");
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
    fn queued_prompt_conversion_is_atomic_ordered_and_dispatched_once() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first follow-up", "repeat me", "repeat me"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let middle_id = state.queue.get(1).expect("middle queue item").id.clone();

        let effects = state
            .steer_queued_prompt(&middle_id)
            .expect("convert queued prompt");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::SteerTurn { prompt, .. })] if prompt == "repeat me"
        ));
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["first follow-up", "repeat me", "repeat me"]
        );
        assert!(
            super::projection::queue_views(&state)
                .iter()
                .find(|item| item.id.as_str() == middle_id)
                .is_some_and(|item| item.redirecting)
        );

        let duplicate = state
            .steer_queued_prompt(&middle_id)
            .expect_err("the same queue identity cannot dispatch twice");
        assert!(duplicate.to_string().contains("already pending"));

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });
        assert_eq!(state.queue.len(), 2);
        assert!(
            state
                .transcript
                .entries()
                .iter()
                .any(|entry| { entry.kind == EntryKind::Steering && entry.body == "repeat me" })
        );
    }

    #[test]
    fn interruption_fallback_promotes_selected_follow_up_without_limbo() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first", "redirect me", "third"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let selected_id = state.queue[1].id.clone();

        let effects = state
            .steer_queued_prompt(&selected_id)
            .expect("interrupt and promote selected follow-up");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::InterruptTurn { turn_id, .. })]
                if turn_id == "turn-1"
        ));
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "redirect me", "third"]
        );
        assert_eq!(
            state
                .pending_redirect
                .as_ref()
                .map(|pending| pending.prompt_id.as_str()),
            Some(selected_id.as_str())
        );
        assert!(
            super::projection::queue_views(&state)
                .iter()
                .find(|item| item.id.as_str() == selected_id)
                .is_some_and(|item| item.redirecting)
        );
        assert!(
            state
                .handle_backend(BackendEvent::InterruptAccepted)
                .is_empty()
        );
        assert!(state.redirect_start.is_none());

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Interrupted,
            error: None,
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::StartTurn { client_id, prompt, .. })
                if client_id == &selected_id && prompt.starts_with("redirect me")
        )));
        assert!(state.pending_redirect.is_none());
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "third"]
        );
    }

    #[test]
    fn ordinary_stop_starts_the_first_queued_follow_up_once() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first next", "second next"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let first_id = state.queue[0].id.clone();

        let effects = state.cancel_turn("turn-1").expect("begin interruption");
        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::InterruptTurn { turn_id, .. })]
                if turn_id == "turn-1"
        ));
        assert!(matches!(
            state.cancel_turn("turn-1"),
            Err(super::DomainCommandError::Conflict(message))
                if message == "the turn is already being cancelled"
        ));

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Interrupted,
            error: None,
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::StartTurn { client_id, .. })
                if client_id == &first_id
        )));
        assert_eq!(
            state.queue.front().map(|prompt| prompt.text.as_str()),
            Some("second next")
        );
    }

    #[test]
    fn redirect_during_stop_uses_the_selected_message_as_the_continuation() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["ordinary next", "chosen next"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let chosen_id = state.queue[1].id.clone();
        state.cancel_session_work().expect("begin stop");

        let effects = state
            .steer_queued_prompt(&chosen_id)
            .expect("promote while interruption is already pending");
        assert!(effects.is_empty());

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::StartTurn { client_id, .. })
                if client_id == &chosen_id
        )));
        assert_eq!(
            state.queue.front().map(|prompt| prompt.text.as_str()),
            Some("ordinary next")
        );
    }

    #[test]
    fn failed_interrupt_restores_promoted_follow_up_at_its_original_position() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first", "redirect me", "third"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let selected_id = state.queue[1].id.clone();
        state
            .steer_queued_prompt(&selected_id)
            .expect("begin redirect");

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::InterruptTurn,
            code: -1,
            message: "turn already ended".to_owned(),
        });

        assert!(state.pending_redirect.is_none());
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "redirect me", "third"]
        );
        assert!(
            state
                .active_turn
                .as_ref()
                .is_some_and(|turn| !turn.cancelling)
        );
    }

    #[test]
    fn fallback_reservation_rejects_repeated_redirect_and_selected_removal_but_allows_siblings() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first", "selected", "third"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let first_id = state.queue[0].id.clone();
        let selected_id = state.queue[1].id.clone();
        state
            .steer_queued_prompt(&selected_id)
            .expect("reserve selected follow-up");

        assert!(matches!(
            state.steer_queued_prompt(&selected_id),
            Err(super::DomainCommandError::Conflict(message))
                if message == "a queued redirect is already pending"
        ));
        assert!(matches!(
            state.remove_queued_prompt(&selected_id),
            Err(super::DomainCommandError::Conflict(message))
                if message == "the queued message is already being redirected"
        ));
        state
            .remove_queued_prompt(&first_id)
            .expect("an unrelated queue item remains independently removable");
        state
            .enqueue_prompt("new sibling".to_owned(), Vec::new())
            .expect("concurrent queue append");

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Interrupted,
            error: None,
        });
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(
                    effect,
                    Effect::Backend(BackendCommand::StartTurn { .. })
                ))
                .count(),
            1
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::StartTurn { client_id, .. })
                if client_id == &selected_id
        )));
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["third", "new sibling"]
        );
    }

    #[test]
    fn provider_exit_failure_retains_reserved_follow_up_without_starting_a_replacement() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("claude-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state
            .enqueue_prompt("safe next turn".to_owned(), Vec::new())
            .expect("queue follow-up");
        let selected_id = state.queue[0].id.clone();
        state
            .steer_queued_prompt(&selected_id)
            .expect("begin fallback");

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Failed,
            error: Some("Claude Code process exited with code 1".to_owned()),
        });

        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, Effect::Backend(BackendCommand::StartTurn { .. })))
        );
        assert_eq!(state.queue[0].id, selected_id);
        assert!(state.status_message.contains("remains queued"));
        assert!(state.pending_redirect.is_none());
    }

    #[test]
    fn replacement_start_failure_restores_the_reserved_follow_up_without_retrying_it() {
        let mut state = ready_state();
        state.backend_capabilities.steering = CapabilitySupport::Unsupported;
        state.provider_session_id = Some("claude-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first", "selected", "third"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let selected_id = state.queue[1].id.clone();
        state
            .steer_queued_prompt(&selected_id)
            .expect("begin fallback");
        let start = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Interrupted,
            error: None,
        });
        assert!(start.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::StartTurn { client_id, .. })
                if client_id == &selected_id
        )));

        let retry = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -1,
            message: "provider process failed before the turn started".to_owned(),
        });

        assert!(retry.is_empty(), "a failed replacement must not auto-retry");
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "selected", "third"]
        );
        assert!(state.status_message.contains("remains queued"));
        assert!(state.recoverable_prompt().is_none());
    }

    #[test]
    fn failed_queued_steer_restores_the_exact_follow_up_position() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        for text in ["first", "second", "third"] {
            state
                .enqueue_prompt(text.to_owned(), Vec::new())
                .expect("queue follow-up");
        }
        let second_id = state.queue.get(1).expect("second queue item").id.clone();
        state
            .steer_queued_prompt(&second_id)
            .expect("begin queued steer");

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::SteerTurn,
            code: -32603,
            message: "provider refused steering".to_owned(),
        });

        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| (prompt.id.as_str(), prompt.text.as_str()))
                .collect::<Vec<_>>(),
            [
                (state.queue[0].id.as_str(), "first"),
                (second_id.as_str(), "second"),
                (state.queue[2].id.as_str(), "third"),
            ]
        );
        assert!(state.status_message.contains("provider refused steering"));
    }

    #[test]
    fn queued_prompt_controls_preserve_siblings_and_fallback_keeps_attachments() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state
            .enqueue_prompt("keep".to_owned(), Vec::new())
            .expect("queue first");
        state
            .enqueue_prompt(
                "image follow-up".to_owned(),
                vec![PromptAttachment {
                    label: "context.png".to_owned(),
                    path: None,
                    image: Some(PromptImage {
                        mime_type: "image/png".to_owned(),
                        data: vec![1, 2, 3],
                    }),
                }],
            )
            .expect("queue image");
        state
            .enqueue_prompt("keep too".to_owned(), Vec::new())
            .expect("queue third");
        let first_id = state.queue[0].id.clone();
        let image_id = state.queue[1].id.clone();

        state
            .remove_queued_prompt(&first_id)
            .expect("dequeue independently");
        let effects = state
            .steer_queued_prompt(&image_id)
            .expect("an image follow-up uses ordered stop-and-send");

        assert!(matches!(
            effects.as_slice(),
            [Effect::Backend(BackendCommand::InterruptTurn { turn_id, .. })]
                if turn_id == "turn-1"
        ));
        assert_eq!(
            state
                .queue
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["image follow-up", "keep too"]
        );
        assert_eq!(
            state
                .pending_redirect
                .as_ref()
                .map(|pending| pending.prompt_id.as_str()),
            Some(image_id.as_str())
        );
        assert!(state.pending_steer.is_none());
    }

    #[test]
    fn steer_is_recorded_only_after_provider_acceptance() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });

        let effects = state
            .steer_turn("turn-1", "focus on tests")
            .expect("steer accepted");
        assert!(matches!(effects.as_slice(), [Effect::Backend(_)]));
        assert!(
            state
                .transcript
                .entries()
                .iter()
                .all(|entry| entry.kind != EntryKind::Steering)
        );

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });
        assert!(
            state.transcript.entries().iter().any(|entry| {
                entry.kind == EntryKind::Steering && entry.body == "focus on tests"
            })
        );
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
        state.client.editor.set_text("too late");
        state.steer_editor();

        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.client.editor.text(), "too late");
        assert!(state.status_message.contains("late steer"));
    }

    #[test]
    fn session_start_timeout_preserves_the_pending_prompt() {
        let mut state = ready_state();
        state.client.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32001,
            message: "timeout".to_owned(),
        });
        assert!(effects.is_empty());
        assert!(state.is_busy());
        assert!(state.client.editor.is_blank());
        assert!(state.recoverable_prompt().is_none());

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "thread-late".to_owned(),
            model: "model-a".to_owned(),
        });
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { .. },
                Effect::Backend(BackendCommand::SetSessionOptions { .. }),
                Effect::Backend(BackendCommand::StartTurn { .. })
            ]
        ));
    }

    #[test]
    fn claude_session_creation_applies_supported_catalogue_effort() {
        let mut state = ready_state();
        state.backend_provider = CLAUDE_PROVIDER.to_owned();
        state.models = vec![ModelInfo {
            provider: CLAUDE_PROVIDER.to_owned(),
            id: "opus".to_owned(),
            is_default: true,
            capabilities: ModelCapabilities {
                reasoning_efforts: ["low", "medium", "high"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            },
        }];
        state.selected_model = Some(format!("{CLAUDE_PROVIDER}/opus"));
        state.install_model_options(
            CLAUDE_PROVIDER,
            "opus",
            ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: false,
            },
        );
        state.client.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "claude-session".to_owned(),
            model: "opus".to_owned(),
        });

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { .. },
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. }),
                Effect::Backend(BackendCommand::StartTurn { .. })
            ] if options.reasoning_effort.as_deref() == Some("high") && !options.fast_mode
        ));
    }

    #[test]
    fn stale_claude_effort_is_cleared_before_session_creation() {
        let mut state = ready_state();
        state.backend_provider = CLAUDE_PROVIDER.to_owned();
        state.models = vec![ModelInfo {
            provider: CLAUDE_PROVIDER.to_owned(),
            id: "haiku".to_owned(),
            is_default: true,
            capabilities: ModelCapabilities {
                reasoning_efforts: vec!["low".to_owned()],
            },
        }];
        state.selected_model = Some(format!("{CLAUDE_PROVIDER}/haiku"));
        state.install_model_options(
            CLAUDE_PROVIDER,
            "haiku",
            ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: true,
            },
        );
        state.client.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "claude-session".to_owned(),
            model: "haiku".to_owned(),
        });

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { .. },
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. }),
                Effect::Backend(BackendCommand::StartTurn { .. })
            ] if options.reasoning_effort.is_none() && !options.fast_mode
        ));
    }

    #[test]
    fn a_model_without_configurable_options_omits_session_options() {
        let mut state = ready_state();
        state.backend_provider = CLAUDE_PROVIDER.to_owned();
        state.models = vec![ModelInfo {
            provider: CLAUDE_PROVIDER.to_owned(),
            id: "haiku".to_owned(),
            is_default: true,
            capabilities: ModelCapabilities::default(),
        }];
        state.selected_model = Some(format!("{CLAUDE_PROVIDER}/haiku"));
        state.install_model_options(
            CLAUDE_PROVIDER,
            "haiku",
            ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: true,
            },
        );
        state.client.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "claude-session".to_owned(),
            model: "haiku".to_owned(),
        });

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { .. },
                Effect::Backend(BackendCommand::StartTurn { .. })
            ]
        ));
    }

    #[test]
    fn rejected_session_start_exposes_a_semantic_recoverable_prompt() {
        let mut state = ready_state();
        let attachments = vec![PromptAttachment {
            label: "context.png".to_owned(),
            path: Some(Path::new("/tmp/context.png").to_path_buf()),
            image: Some(PromptImage {
                mime_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            }),
        }];
        state
            .submit_prompt("first".to_owned(), attachments.clone())
            .expect("accepted prompt");

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32602,
            message: "rejected".to_owned(),
        });

        assert!(!state.is_busy());
        let recovery = state.recoverable_prompt().expect("recoverable prompt");
        assert!(!recovery.id.is_empty());
        assert_eq!(recovery.text, "first");
        assert_eq!(recovery.attachments, attachments);
        let user = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.kind == EntryKind::User)
            .expect("failed user entry");
        assert_eq!(user.status, EntryStatus::Failed);

        state
            .submit_prompt("second".to_owned(), Vec::new())
            .expect("replacement prompt accepted");
        assert!(state.recoverable_prompt().is_none());
    }

    #[test]
    fn backend_prompt_failure_completion_clears_active_turn() {
        let mut state = ready_state();
        state.provider_session_id = Some("session-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state.client.editor.set_text("fail prompt");
        state.submit_editor();
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-failed".to_owned(),
        });
        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32602,
            message: "prompt failed".to_owned(),
        });
        assert!(state.recoverable_prompt().is_none());
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
    fn rejected_turn_start_exposes_a_semantic_recoverable_prompt() {
        let mut state = ready_state();
        state.provider_session_id = Some("session-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state
            .submit_prompt("fail before acceptance".to_owned(), Vec::new())
            .expect("prompt accepted");

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32602,
            message: "prompt failed".to_owned(),
        });

        assert!(!state.is_busy());
        assert_eq!(
            state
                .recoverable_prompt()
                .map(|prompt| prompt.text.as_str()),
            Some("fail before acceptance")
        );
    }

    #[test]
    fn prompt_lifecycle_drives_semantic_stream_state() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.client.editor.set_text("inspect the project");
        state.submit_editor();

        assert!(state.transcript.stream_active());
        assert_eq!(state.transcript.stream_label(), "Nakode");

        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-1".to_owned(),
        });
        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });

        assert!(!state.transcript.stream_active());
        assert_eq!(state.transcript.stream_label(), "Nakode");
    }

    #[test]
    fn start_turn_timeout_does_not_launch_the_next_queued_prompt() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.session_id = Some("nakode-session-1".to_owned());
        state.client.editor.set_text("first");
        state.submit_editor();
        state.client.editor.set_text("second");
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
        state.client.editor.set_text("first");
        state.submit_editor();
        assert!(state.is_busy());

        state.handle_backend(BackendEvent::SessionClosed {
            provider_session_id: "thread-1".to_owned(),
        });

        assert!(!state.is_busy());
        assert!(state.provider_session_id.is_none());
        assert_eq!(
            state
                .recoverable_prompt()
                .map(|prompt| prompt.text.as_str()),
            Some("first")
        );
    }

    #[test]
    fn disconnect_exposes_the_prompt_that_was_still_starting() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state
            .submit_prompt("survive disconnect".to_owned(), Vec::new())
            .expect("accepted prompt");

        state.handle_backend(BackendEvent::Disconnected {
            reason: "transport closed".to_owned(),
        });

        assert_eq!(
            state
                .recoverable_prompt()
                .map(|prompt| prompt.text.as_str()),
            Some("survive disconnect")
        );
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
        state.client.editor.set_text("focus");
        state.steer_editor();
        state.client.editor.insert_char('x');
        state.client.editor.backspace();

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.client.editor.text(), "focus");
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
        state.handle_backend(BackendEvent::QuestionRequested(Box::new(QuestionRequest {
            id: "question-1".to_owned(),
            logical_id: "question-1".to_owned(),
            group_id: "question-1".to_owned(),
            order: 0,
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
        })));

        state.move_question_selection(1);
        assert!(matches!(
            state.resolve_question().as_slice(),
            [Effect::Backend(BackendCommand::ResolveQuestion { id, answer })]
                if id == "question-1"
                    && answer
                        == &crate::backend::QuestionAnswer::Options(vec!["Flexible".to_owned()])
        ));
        assert!(state.questions.is_empty());
    }

    #[test]
    fn multi_select_tool_questions_preserve_recommendations_and_descriptions() {
        let mut state = ready_state();
        state.handle_backend(BackendEvent::QuestionRequested(Box::new(QuestionRequest {
            id: "question-2".to_owned(),
            logical_id: "question-2".to_owned(),
            group_id: "question-2".to_owned(),
            order: 0,
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
        })));

        assert_eq!(state.questions.front().expect("question").selected, 1);
        state.toggle_question_selection();
        state.move_question_selection(-1);
        state.toggle_question_selection();
        assert!(matches!(
            state.resolve_question().as_slice(),
            [Effect::Backend(BackendCommand::ResolveQuestion { answer, .. })]
                if answer
                    == &crate::backend::QuestionAnswer::Options(vec![
                        "Library".to_owned(),
                        "CLI".to_owned(),
                    ])
        ));
    }

    #[test]
    fn grouped_questions_accept_mixed_text_and_labels_atomically() {
        let mut state = ready_state();
        for (id, order, label) in [("format", 0, "JSON"), ("note", 1, "Brief")] {
            state.handle_backend(BackendEvent::QuestionRequested(Box::new(QuestionRequest {
                id: format!("runtime-{id}"),
                logical_id: id.to_owned(),
                group_id: "ask-group".to_owned(),
                order,
                title: id.to_owned(),
                question: format!("Choose {id}"),
                options: vec![
                    QuestionOption {
                        label: label.to_owned(),
                        description: None,
                    },
                    QuestionOption {
                        label: "Other".to_owned(),
                        description: None,
                    },
                ],
                multi: false,
                recommended: None,
            })));
        }
        let interaction =
            projection::question_interaction_id(&state.nakode_session_id, "ask-group");
        let effects = state
            .resolve_interaction(
                &interaction,
                &nakode_protocol::InteractionResolution::AnswerQuestions {
                    answers: vec![
                        nakode_protocol::QuestionResponse {
                            question_id: "format".to_owned(),
                            option_ids: vec!["0".to_owned()],
                            text: None,
                        },
                        nakode_protocol::QuestionResponse {
                            question_id: "note".to_owned(),
                            option_ids: Vec::new(),
                            text: Some("Use the owner wording".to_owned()),
                        },
                    ],
                },
            )
            .expect("valid grouped answer");
        assert_eq!(effects.len(), 2);
        assert!(matches!(
            &effects[0],
            Effect::Backend(BackendCommand::ResolveQuestion {
                answer: crate::backend::QuestionAnswer::Options(labels), ..
            }) if labels == &["JSON".to_owned()]
        ));
        assert!(matches!(
            &effects[1],
            Effect::Backend(BackendCommand::ResolveQuestion {
                answer: crate::backend::QuestionAnswer::Text(text), ..
            }) if text == "Use the owner wording"
        ));
        assert!(state.questions.is_empty());
    }

    #[test]
    fn rejected_partial_grouped_answer_preserves_every_pending_question_for_retry() {
        let mut state = ready_state();
        for (id, order) in [("first", 0), ("second", 1)] {
            state.handle_backend(BackendEvent::QuestionRequested(Box::new(QuestionRequest {
                id: format!("runtime-{id}"),
                logical_id: id.to_owned(),
                group_id: "retry-group".to_owned(),
                order,
                title: id.to_owned(),
                question: id.to_owned(),
                options: vec![
                    QuestionOption {
                        label: "Yes".to_owned(),
                        description: None,
                    },
                    QuestionOption {
                        label: "No".to_owned(),
                        description: None,
                    },
                ],
                multi: false,
                recommended: None,
            })));
        }
        let interaction =
            projection::question_interaction_id(&state.nakode_session_id, "retry-group");
        let error = state
            .resolve_interaction(
                &interaction,
                &nakode_protocol::InteractionResolution::AnswerQuestions {
                    answers: vec![nakode_protocol::QuestionResponse {
                        question_id: "first".to_owned(),
                        option_ids: Vec::new(),
                        text: Some("   ".to_owned()),
                    }],
                },
            )
            .expect_err("partial answer must be rejected");
        assert!(error.to_string().contains("every question"));
        assert_eq!(state.questions.len(), 2);
    }

    #[test]
    fn successful_connection_does_not_add_transcript_noise() {
        let state = ready_state();
        assert!(state.transcript.entries().is_empty());
    }

    #[test]
    fn same_round_reasoning_is_placed_before_an_earlier_assistant_delta() {
        let mut state = ready_state();
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("kimi-coding/k3-256k".to_owned()),
            cancelling: false,
        });
        for (item_id, kind, delta) in [
            ("turn-1:assistant:0", DeltaKind::Assistant, "Final answer"),
            ("turn-1:reasoning:0", DeltaKind::Reasoning, "Thinking first"),
        ] {
            state.handle_backend(BackendEvent::ItemDelta {
                turn_id: "turn-1".to_owned(),
                item_id: item_id.to_owned(),
                kind,
                delta: delta.to_owned(),
            });
            // Presentation state may move ahead while late deltas arrive; the active turn origin does not.
            state.selected_model = Some("openai-codex/model-b".to_owned());
        }

        let entries = state.transcript.entries();
        assert_eq!(entries[0].key.as_deref(), Some("turn-1:reasoning:0"));
        assert_eq!(entries[0].provider_id.as_deref(), Some("openai-codex"));
        assert_eq!(entries[0].model_id.as_deref(), Some("kimi-coding/k3-256k"));
        assert_eq!(entries[1].key.as_deref(), Some("turn-1:assistant:0"));
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
                tool_audit_json: None,
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
                tool_audit_json: None,
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
    fn interleaved_content_blocks_converge_across_live_updates_and_history() {
        let mut state = ready_state();
        state.backend_provider = crate::backend::CLAUDE_PROVIDER.to_owned();
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("claude-agent/opus".to_owned()),
            cancelling: false,
        });

        let started = |id: &str, kind: ItemKind, title: &str| BackendEvent::ItemStarted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: id.to_owned(),
                kind,
                title: title.to_owned(),
                body: String::new(),
                status: ItemStatus::Running,
                tool_audit_json: None,
            },
        };
        let delta = |id: &str, kind: DeltaKind, text: &str| BackendEvent::ItemDelta {
            turn_id: "turn-1".to_owned(),
            item_id: id.to_owned(),
            kind,
            delta: text.to_owned(),
        };
        let completed = |id: &str, name: &str, body: &str| BackendEvent::ItemCompleted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: id.to_owned(),
                kind: ItemKind::Tool,
                title: name.to_owned(),
                body: body.to_owned(),
                status: ItemStatus::Complete,
                tool_audit_json: None,
            },
        };

        for event in [
            delta("think", DeltaKind::Reasoning, "Inspecting"),
            delta("intro", DeltaKind::Assistant, "Before tools."),
            started("tool-a", ItemKind::Tool, "Read"),
            started("tool-b", ItemKind::Tool, "Grep"),
            completed("tool-b", "Grep", "second result"),
            completed("tool-a", "Read", "first result"),
            delta("final", DeltaKind::Assistant, "After tools."),
            // A duplicate delayed status patch updates in place rather than jumping to the tail.
            completed("tool-a", "Read", "first result"),
        ] {
            state.handle_backend(event);
        }

        let live = state
            .transcript
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry.key.as_deref().unwrap_or_default().to_owned(),
                    entry.kind,
                    entry.body.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            live.iter()
                .map(|entry| entry.0.as_str())
                .collect::<Vec<_>>(),
            ["think", "intro", "tool-a", "tool-b", "final"]
        );
        assert_eq!(live[0].1, EntryKind::Reasoning);
        assert_eq!(live[4].2, "After tools.");

        let history = live
            .iter()
            .map(|(id, kind, body)| SessionHistoryItem {
                turn_id: "turn-1".to_owned(),
                provider_id: Some(crate::backend::CLAUDE_PROVIDER.to_owned()),
                model_id: Some("claude-agent/opus".to_owned()),
                item: NormalizedItem {
                    id: id.clone(),
                    kind: match kind {
                        EntryKind::Reasoning => ItemKind::Reasoning,
                        EntryKind::Assistant => ItemKind::Assistant,
                        EntryKind::Tool => ItemKind::Tool,
                        _ => ItemKind::System,
                    },
                    title: id.clone(),
                    body: body.clone(),
                    status: ItemStatus::Complete,
                    tool_audit_json: None,
                },
            })
            .collect::<Vec<_>>();
        let mut resumed = ready_state();
        resumed.install_history(history.clone());
        resumed.install_history(history);
        assert_eq!(
            resumed
                .transcript
                .entries()
                .iter()
                .map(|entry| entry.key.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            ["think", "intro", "tool-a", "tool-b", "final"]
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
                tool_audit_json: None,
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
        assert_eq!(item.status, EntryStatus::Complete);
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
                tool_audit_json: None,
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
    fn settings_child_menus_restore_the_exact_parent_node() {
        let mut state = ready_state();

        state.open_settings();
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::ListProviders]
        ));
        assert!(state.client.settings.is_none());
        state.close_provider_picker();
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Menu, 0))
        );

        state.settings_move(1);
        assert!(state.select_setting().is_empty());
        assert!(state.client.settings.is_none());
        state.close_agent_picker();
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Menu, 1))
        );

        state.settings_move(1);
        assert!(state.select_setting().is_empty());
        assert!(state.client.model_picker.is_some());
        state.close_model_picker();
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Menu, 2))
        );

        state.settings_move(1);
        assert!(state.select_setting().is_empty());
        state.settings_move(1);
        assert!(state.select_setting().is_empty());
        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::Vision)
        );
        assert!(state.select_setting().is_empty());
        assert!(state.client.model_picker.is_some());
        state.close_model_picker();
        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::Vision)
        );

        assert!(state.settings_back().is_empty());
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Addons, 1))
        );
        assert!(state.settings_back().is_empty());
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Menu, 3))
        );
    }

    #[test]
    fn model_options_escape_returns_to_model_list_before_parent_menu() {
        let mut state = ready_state();
        assert!(state.open_model_picker().is_empty());
        let picker = state.client.model_picker.as_mut().expect("model picker");
        picker.stage = super::ModelPickerStage::Options;

        state.close_model_picker();
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.stage),
            Some(super::ModelPickerStage::Models)
        );
        state.close_model_picker();
        assert!(state.client.model_picker.is_none());
    }

    #[test]
    fn memory_setting_is_disabled_by_default_and_emits_provider_neutral_config() {
        let mut state = ready_state();
        state.open_settings();
        state.settings_move(3);
        assert!(state.select_setting().is_empty());
        state.settings_move(2);
        assert!(state.select_setting().is_empty());
        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::Memory)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::SaveMemoryConfig(config)]
                if config.backend == crate::memory::MemoryBackend::Mnemosyne
        ));
        assert!(matches!(
            state.settings_back().as_slice(),
            [Effect::SaveMemoryConfig(config)]
                if config.backend == crate::memory::MemoryBackend::Mnemosyne
        ));
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Addons, 2))
        );
    }

    #[test]
    fn terminal_image_setting_cycles_and_emits_persistence_effect() {
        let mut state = ready_state();
        state.open_settings();
        state.settings_move(3);
        assert!(state.select_setting().is_empty());
        state.settings_move(3);
        assert!(state.select_setting().is_empty());
        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::TerminalImages)
        );

        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::SaveTerminalImageMode(
                crate::settings::TerminalImageMode::On
            )]
        ));
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| settings.terminal_images),
            Some(crate::settings::TerminalImageMode::On)
        );

        assert!(matches!(
            state.settings_cycle_choice(-1).as_slice(),
            [Effect::SaveTerminalImageMode(
                crate::settings::TerminalImageMode::Auto
            )]
        ));
        assert!(matches!(
            state.settings_cycle_choice(1).as_slice(),
            [Effect::SaveTerminalImageMode(
                crate::settings::TerminalImageMode::On
            )]
        ));

        assert!(state.settings_back().is_empty());
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| (settings.view, settings.selected)),
            Some((super::SettingsView::Addons, 3))
        );
    }

    #[test]
    fn slash_commands_are_not_sent_as_prompts() {
        let mut state = ready_state();
        state.client.editor.set_text("/resume");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListSessions]
        ));
        assert!(state.client.session_picker.is_some());

        state.close_session_picker();
        state.client.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ReloadConfiguration]
        ));

        state.client.editor.set_text("/resume abc123");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ResolveSession(id)] if id == "abc123"
        ));
    }

    #[test]
    fn configuration_commands_work_without_an_enabled_provider() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);

        state.client.editor.set_text("/providers");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListProviders]
        ));

        state.close_provider_picker();
        state.client.editor.set_text("/agents");
        assert!(state.submit_editor().is_empty());
        assert!(state.client.agent_picker.is_some());

        state.close_agent_picker();
        state.client.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ReloadConfiguration]
        ));

        state.client.editor.set_text("/settings");
        assert!(state.submit_editor().is_empty());
        let settings = state.client.settings.as_ref().expect("settings menu");
        assert_eq!(settings.filtered_sections(), super::SettingsSection::ALL);
        state.settings_insert('w');
        state.settings_insert('e');
        state.settings_insert('b');
        assert_eq!(
            state
                .client
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
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::Addons)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::CheckAgentBrowser]
        ));
        assert_eq!(
            state.client.settings.as_ref().map(|settings| settings.view),
            Some(super::SettingsView::WebBrowsing)
        );
        assert!(matches!(
            state.select_setting().as_slice(),
            [Effect::SaveWebConfig(config)]
                if config.backend == crate::web::WebBackend::AgentBrowser
        ));
        assert_eq!(
            state
                .client
                .settings
                .as_ref()
                .map(|settings| settings.web.backend),
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
                .client
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
    fn provider_logout_removes_its_models_immediately() {
        let mut state = ready_state();
        state.install_models(vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "gpt-test".to_owned(),
            is_default: true,
            capabilities: crate::codex::model_capabilities(),
        }]);

        state.provider_logged_out(CODEX_PROVIDER);

        assert!(state.models.is_empty());
        assert!(state.backend_provider.is_empty());
    }

    #[test]
    fn workspace_configuration_sync_preserves_session_local_provider_context() {
        let mut source = ready_state();
        source.provider_session_id = Some("source-native-session".to_owned());
        source.session_id = Some("source-logical-session".to_owned());
        source.context_usage = Some(super::ContextUsageState {
            estimated_tokens: 10,
            context_window: Some(100),
        });
        source.sync_active_provider_context();

        let mut target = AppState::new("/tmp/project", None, 100);
        target.provider_session_id = Some("target-native-session".to_owned());
        target.session_id = Some("target-logical-session".to_owned());
        target.context_usage = Some(super::ContextUsageState {
            estimated_tokens: 20,
            context_window: Some(200),
        });
        target.sync_active_provider_context();

        target.synchronize_workspace_configuration(&source);

        assert!(target.connection.is_ready());
        assert_eq!(target.backend_name, "codex-test");
        assert!(
            target
                .backend_capabilities
                .context_compaction
                .is_supported()
        );
        assert_eq!(
            target.provider_session_id.as_deref(),
            Some("target-native-session")
        );
        assert_eq!(target.session_id.as_deref(), Some("target-logical-session"));
        assert_eq!(
            target.context_usage,
            Some(super::ContextUsageState {
                estimated_tokens: 20,
                context_window: Some(200),
            })
        );
        let context = target
            .provider_contexts
            .get(CODEX_PROVIDER)
            .expect("target provider context");
        assert_eq!(
            context.provider_session_id.as_deref(),
            Some("target-native-session")
        );
        assert_eq!(
            context.session_id.as_deref(),
            Some("target-logical-session")
        );
        assert_eq!(
            context.context_usage,
            Some(super::ContextUsageState {
                estimated_tokens: 20,
                context_window: Some(200),
            })
        );
    }

    #[test]
    fn workspace_configuration_sync_initializes_a_new_session_template() {
        let mut source = ready_state();
        source.provider_session_id = Some("source-native-session".to_owned());
        source.session_id = Some("source-logical-session".to_owned());
        source.sync_active_provider_context();
        let mut template = AppState::new_unconfigured("/tmp/project", None, 100);

        template.synchronize_workspace_configuration(&source);

        assert_eq!(template.backend_provider, CODEX_PROVIDER);
        assert!(template.connection.is_ready());
        assert!(template.provider_session_id.is_none());
        assert!(template.session_id.is_none());
        assert!(template.context_usage.is_none());
    }

    #[test]
    fn workspace_configuration_sync_replaces_a_missing_active_provider() {
        let mut source = ready_state();
        source.handle_provider_backend(
            DEVIN_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: DEVIN_PROVIDER.to_owned(),
                display_name: "Devin".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        source.provider_disabled(CODEX_PROVIDER);
        assert_eq!(source.backend_provider, DEVIN_PROVIDER);

        let mut target = ready_state();
        target.handle_provider_backend(
            DEVIN_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: DEVIN_PROVIDER.to_owned(),
                display_name: "Devin".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        let context = target
            .provider_contexts
            .get_mut(DEVIN_PROVIDER)
            .expect("target Devin context");
        context.provider_session_id = Some("target-devin-native".to_owned());
        context.session_id = Some("target-devin-logical".to_owned());
        context.context_usage = Some(super::ContextUsageState {
            estimated_tokens: 30,
            context_window: Some(300),
        });

        target.synchronize_workspace_configuration(&source);

        assert_eq!(target.backend_provider, DEVIN_PROVIDER);
        assert!(target.connection.is_ready());
        assert_eq!(
            target.provider_session_id.as_deref(),
            Some("target-devin-native")
        );
        assert_eq!(target.session_id.as_deref(), Some("target-devin-logical"));
        assert_eq!(
            target.context_usage,
            Some(super::ContextUsageState {
                estimated_tokens: 30,
                context_window: Some(300),
            })
        );
    }

    #[test]
    fn resumed_session_rebuilds_transcript_and_touches_metadata() {
        let mut state = ready_state();
        state.install_model_options(
            CODEX_PROVIDER,
            "model-a",
            ModelOptions {
                reasoning_effort: Some("unsupported".to_owned()),
                fast_mode: true,
            },
        );
        let session = SessionRecord {
            id: "01950000-0000-7000-8000-000000000000".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-resumed".to_owned(),
            workspace: "/tmp/project".to_owned(),
            title: "Previous work".to_owned(),
            model: Some("model-a".to_owned()),
            created_at: 1,
            updated_at: 2,
            owned_provider_sessions: Vec::new(),
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
                provider_id: None,
                model_id: None,
                item: NormalizedItem {
                    id: "user-1".to_owned(),
                    kind: ItemKind::User,
                    title: "YOU".to_owned(),
                    body: "hello".to_owned(),
                    status: ItemStatus::Complete,
                    tool_audit_json: None,
                },
            }],
        });

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::TouchSession(touched),
                Effect::LoadSubagents(loaded),
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. })
            ] if touched == &session.id
                && loaded == &session.id
                && options.reasoning_effort.is_none()
                && options.fast_mode
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
            model: None,
            provider_session_id: Some("child-session".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "assistant-entry-1".to_owned(),
                key: Some("assistant-1".to_owned()),
                kind: EntryKind::Assistant,
                title: "ASSISTANT".to_owned(),
                body: "The session store owns orchestration metadata.".to_owned(),
                status: EntryStatus::Complete,
                provider_id: None,
                model_id: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability {
                parent_run_id: Some("root-run".to_owned()),
                archetype_purpose: "Read-only architecture scout".to_owned(),
                started_at_ms: 100,
                ended_at_ms: Some(180),
                termination_kind: Some("completed".to_owned()),
                ..SubagentObservability::default()
            },
        }]);

        assert_eq!(state.subagents.len(), 1);
        assert_eq!(state.subagents[0].objective, "Map persistence");
        assert_eq!(
            state.subagents[0].observability.parent_run_id.as_deref(),
            Some("root-run")
        );
        state.client.subagent_modal = Some("agent-1".to_owned());
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
    fn vision_model_selection_persists_without_switching_the_session_model() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "vision-model".to_owned(),
            is_default: false,
            capabilities: crate::codex::model_capabilities(),
        });
        let session_model = state.selected_model.clone();

        assert!(state.open_vision_model_picker().is_empty());
        state.picker_move(1);
        assert!(matches!(
            state.picker_select().as_slice(),
            [Effect::SaveVisionConfig(config)]
                if config.model.as_deref() == Some("openai-codex/vision-model")
        ));
        assert_eq!(state.selected_model, session_model);
    }

    #[test]
    fn model_switch_does_not_mutate_active_turn() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
            capabilities: crate::codex::model_capabilities(),
        });
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        let _ = state.open_model_picker();
        state.picker_move(1);
        let _ = state.picker_select();
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
            capabilities: crate::codex::model_capabilities(),
        });
        state.client.editor.set_text("/models");

        assert!(state.submit_editor().is_empty());
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.scope),
            Some(super::ModelSelectionScope::Default)
        );
        state.picker_move(1);
        assert!(state.picker_select().is_empty());
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.stage),
            Some(super::ModelPickerStage::Options)
        );
        state.picker_adjust(1);
        state.picker_move(1);
        state.picker_adjust(1);
        let effects = state.picker_select();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::SetDefaultModel { provider, model },
                Effect::SaveModelOptions { .. }
            ] if provider == CODEX_PROVIDER && model == "model-b"
        ));
        let model_b_options = state
            .model_options
            .get("openai-codex/model-b")
            .expect("model-b options");
        assert_eq!(model_b_options.reasoning_effort.as_deref(), Some("high"));
        assert!(model_b_options.fast_mode);

        state.client.editor.set_text("/new");
        let _ = state.submit_editor();
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
    }

    #[test]
    fn cursor_model_options_are_limited_to_fast_capable_families() {
        let cursor_model = |id: &str| ModelInfo {
            provider: CURSOR_PROVIDER.to_owned(),
            id: id.to_owned(),
            is_default: false,
            capabilities: crate::backend::ModelCapabilities::default(),
        };

        assert!(model_supports_options(&cursor_model("composer-2.5")));
        assert!(model_supports_options(&cursor_model("grok-4.5")));
        assert!(!model_supports_options(&cursor_model("claude-4.6-opus")));
    }

    #[test]
    fn cursor_models_can_persist_fast_mode_as_their_default() {
        let mut state = ready_state();
        state.backend_provider = CURSOR_PROVIDER.to_owned();
        state.backend_name = "Cursor".to_owned();
        state.models = vec![ModelInfo {
            provider: CURSOR_PROVIDER.to_owned(),
            id: "composer-2.5".to_owned(),
            is_default: true,
            capabilities: crate::backend::ModelCapabilities::default(),
        }];
        state.selected_model = Some("cursor-sdk/composer-2.5".to_owned());
        state.client.editor.set_text("/models");

        assert!(state.submit_editor().is_empty());
        assert!(state.picker_select().is_empty());
        let picker = state
            .client
            .model_picker
            .as_ref()
            .expect("Cursor options picker");
        assert_eq!(picker.stage, super::ModelPickerStage::Options);
        assert!(picker.options_fast_only);
        assert!(picker.options.reasoning_effort.is_none());

        state.picker_adjust(1);
        let effects = state.picker_select();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::SetDefaultModel { provider, model },
                Effect::SaveModelOptions { options, .. }
            ] if provider == CURSOR_PROVIDER
                && model == "composer-2.5"
                && options.fast_mode
                && options.reasoning_effort.is_none()
        ));
        assert!(state.selected_model_uses_fast_mode());
    }

    #[test]
    fn switch_uses_the_shared_cursor_fast_mode_selector() {
        let mut state = ready_state();
        state.backend_provider = CURSOR_PROVIDER.to_owned();
        state.backend_name = "Cursor".to_owned();
        state.backend_capabilities.session_model_config = CapabilitySupport::Supported;
        state.models = vec![ModelInfo {
            provider: CURSOR_PROVIDER.to_owned(),
            id: "composer-2.5".to_owned(),
            is_default: true,
            capabilities: crate::backend::ModelCapabilities::default(),
        }];
        state.selected_model = Some("cursor-sdk/composer-2.5".to_owned());
        state.provider_session_id = Some("cursor-session-1".to_owned());
        state.client.editor.set_text("/switch");

        assert!(state.submit_editor().is_empty());
        assert!(state.picker_select().is_empty());
        let picker = state
            .client
            .model_picker
            .as_ref()
            .expect("Cursor options picker");
        assert_eq!(picker.scope, super::ModelSelectionScope::Session);
        assert_eq!(picker.stage, super::ModelPickerStage::Options);
        assert!(picker.options_fast_only);

        state.picker_adjust(1);
        let effects = state.picker_select();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Backend(BackendCommand::SetSessionModel { model, .. }),
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. })
            ] if model == "composer-2.5" && options.fast_mode
        ));
        assert!(state.selected_model_uses_fast_mode());

        state.client.editor.set_text("/new");
        let _ = state.submit_editor();
        assert!(!state.selected_model_uses_fast_mode());
    }

    #[test]
    fn models_picker_loads_options_for_the_selected_model() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
            capabilities: crate::codex::model_capabilities(),
        });
        state.install_model_options(
            CODEX_PROVIDER,
            "model-a",
            ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: true,
            },
        );
        state.install_model_options(
            CODEX_PROVIDER,
            "model-b",
            ModelOptions {
                reasoning_effort: Some("low".to_owned()),
                fast_mode: false,
            },
        );
        state.client.editor.set_text("/models");
        state.submit_editor();
        state.picker_move(1);
        state.picker_select();

        let picker = state.client.model_picker.as_ref().expect("options picker");
        assert_eq!(picker.options.reasoning_effort.as_deref(), Some("low"));
        assert!(!picker.options.fast_mode);
    }

    #[test]
    fn model_changes_during_native_work_are_persisted_for_the_next_turn() {
        for report_turn_started in [false, true] {
            let mut state = ready_state();
            state.models.push(ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
                capabilities: crate::codex::model_capabilities(),
            });
            state.backend_capabilities.session_model_config = CapabilitySupport::Supported;
            state.handle_backend(BackendEvent::SessionCreated {
                provider_session_id: "thread-1".to_owned(),
                model: "model-a".to_owned(),
            });
            let logical_session_id = state.nakode_session_id.clone();
            state.session_id = Some(logical_session_id.clone());

            let turn_effects = state
                .submit_prompt("work".to_owned(), Vec::new())
                .expect("turn accepted");
            let client_id = turn_effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::Backend(BackendCommand::StartTurn { client_id, .. }) => {
                        Some(client_id.clone())
                    }
                    _ => None,
                })
                .expect("native turn starts");
            if report_turn_started {
                state.handle_backend(BackendEvent::TurnStarted { turn_id: client_id });
            }

            let effects = state
                .select_model_intent(
                    &nakode_protocol::ModelTarget::Session {
                        session_id: logical_session_id.into(),
                    },
                    &nakode_protocol::ModelId::from("openai-codex/model-b"),
                    &nakode_protocol::ModelOptions {
                        reasoning_effort: Some("high".to_owned()),
                        fast_mode: false,
                    },
                )
                .expect("next-turn model selection");

            assert!(effects.iter().any(|effect| matches!(
                effect,
                Effect::UpdateSessionModel { session_id, model }
                    if session_id == &state.nakode_session_id
                        && model.as_deref() == Some("openai-codex/model-b")
            )));
            assert!(effects.iter().any(|effect| matches!(
                effect,
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. })
                    if options.reasoning_effort.as_deref() == Some("high")
            )));
            assert!(!effects.iter().any(|effect| matches!(
                effect,
                Effect::Backend(BackendCommand::SetSessionModel { .. })
            )));
            assert_eq!(
                state.selected_model.as_deref(),
                Some("openai-codex/model-b")
            );
        }
    }

    #[test]
    fn switch_command_applies_only_to_the_current_session() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
            capabilities: crate::codex::model_capabilities(),
        });
        state.backend_capabilities.session_model_config = CapabilitySupport::Supported;
        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "thread-1".to_owned(),
            model: "model-a".to_owned(),
        });
        state.install_model_options(
            CODEX_PROVIDER,
            "model-b",
            ModelOptions {
                reasoning_effort: Some("low".to_owned()),
                fast_mode: true,
            },
        );
        state.client.editor.set_text("/switch");

        assert!(state.submit_editor().is_empty());
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.scope),
            Some(super::ModelSelectionScope::Session)
        );
        state.picker_move(1);
        assert!(state.picker_select().is_empty());
        assert_eq!(
            state
                .client
                .model_picker
                .as_ref()
                .map(|picker| picker.stage),
            Some(super::ModelPickerStage::Options)
        );
        let effects = state.picker_select();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Backend(BackendCommand::SetSessionModel { model, .. }),
                Effect::Backend(BackendCommand::SetSessionOptions { options, .. })
            ] if model == "model-b"
                && options.reasoning_effort.as_deref() == Some("low")
                && options.fast_mode
        ));
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );

        state.client.editor.set_text("/new");
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
                provider: CODEX_PROVIDER.to_owned(),
                id: "shared".to_owned(),
                is_default: true,
                capabilities: crate::codex::model_capabilities(),
            }]),
        );
        state.handle_provider_backend(
            DEVIN_PROVIDER,
            BackendEvent::Models(vec![ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "shared".to_owned(),
                is_default: true,
                capabilities: crate::backend::ModelCapabilities::default(),
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
                    provider: provider.to_owned(),
                    id: "shared".to_owned(),
                    is_default: true,
                    capabilities: crate::backend::ModelCapabilities::default(),
                }]),
            );
        }
        state.provider_session_id = Some("codex-thread".to_owned());
        state.transcript.push(
            EntryKind::User,
            "YOU",
            "My name is Quill.",
            EntryStatus::Complete,
        );
        state.transcript.push(
            EntryKind::Assistant,
            "ASSISTANT",
            "Nice to meet you.",
            EntryStatus::Complete,
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

        state.client.editor.set_text("What is my name?");
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
            .find(|entry| entry.kind == EntryKind::User)
            .expect("displayed user prompt");
        assert_eq!(displayed_user.body, "What is my name?");
    }

    #[test]
    fn cursor_subagent_applies_saved_fast_mode_before_its_first_turn() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("cursor-explorer.toml"),
            r#"
slug = "cursor-explorer"
description = "Explores with Cursor"
system_prompt = "Explore carefully."
first_message = "Inspect the delegated question."
model = "cursor-sdk/composer-2.5"
fast_mode = true
"#,
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        let effects = state.invoke_agent(&AgentRequest {
            id: 42,
            agent: "cursor-explorer".to_owned(),
            task: "Map auth".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, provider }] = effects.as_slice() else {
            panic!("expected Cursor subagent launch");
        };
        assert_eq!(provider, CURSOR_PROVIDER);
        let run_id = run_id.clone();

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: CURSOR_PROVIDER.to_owned(),
                display_name: "Cursor".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::SubagentBackend {
                command: BackendCommand::StartSession { model: Some(model), .. },
                ..
            }] if model == "composer-2.5"
        ));

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::SessionCreated {
                provider_session_id: "cursor-child".to_owned(),
                model: "composer-2.5".to_owned(),
            },
        );
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::SubagentBackend {
                    command: BackendCommand::SetSessionOptions {
                        provider_session_id,
                        options,
                    },
                    ..
                },
                Effect::SubagentBackend {
                    command: BackendCommand::StartTurn { model: Some(model), .. },
                    ..
                }
            ] if provider_session_id == "cursor-child"
                && options.fast_mode
                && model == "composer-2.5"
        ));
    }

    /// An archetype's own level is what its run starts at — the definition is where "how hard this
    /// agent thinks" is written, and nothing else in the workspace may override it.
    #[test]
    fn a_subagent_runs_at_the_effort_its_definition_defines() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("thinker.toml"),
            format!(
                r#"
slug = "thinker"
description = "Thinks hard"
system_prompt = "Think carefully."
first_message = "Consider the delegated question."
model = "{CODEX_PROVIDER}/model-a"
reasoning_effort = "xhigh"
"#
            ),
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        let run_id = launch_codex_subagent(&mut state, "thinker");

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::SessionCreated {
                provider_session_id: "codex-child".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        let options = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::SubagentBackend {
                    command: BackendCommand::SetSessionOptions { options, .. },
                    ..
                } => Some(options.clone()),
                _ => None,
            })
            .expect("the run's options");
        assert_eq!(options.reasoning_effort.as_deref(), Some("xhigh"));
    }

    /// No level written means the model's own default, which is what every definition that predates
    /// the field says. No effort option is sent, so the provider keeps its model default.
    #[test]
    fn a_subagent_with_no_effort_defined_runs_at_the_models_default() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("plain.toml"),
            format!(
                r#"
slug = "plain"
description = "Takes what it is given"
system_prompt = "Work."
first_message = "Consider the delegated question."
model = "{CODEX_PROVIDER}/model-a"
"#
            ),
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        let run_id = launch_codex_subagent(&mut state, "plain");

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::SessionCreated {
                provider_session_id: "codex-child".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::SubagentBackend {
                command: BackendCommand::SetSessionOptions { .. },
                ..
            }
        )));
    }

    #[test]
    fn a_stale_subagent_effort_falls_back_to_the_models_default() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("stale.toml"),
            format!(
                r#"
slug = "stale"
description = "Has an outdated effort"
system_prompt = "Work."
model = "{CODEX_PROVIDER}/model-a"
reasoning_effort = "unsupported"
"#
            ),
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        let run_id = launch_codex_subagent(&mut state, "stale");

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::SessionCreated {
                provider_session_id: "codex-child".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::SubagentBackend {
                command: BackendCommand::SetSessionOptions { .. },
                ..
            }
        )));
        state.client.subagent_modal = Some(run_id.clone());
        assert!(
            state
                .selected_subagent_transcript()
                .expect("the stale-effort warning transcript")
                .entries()
                .iter()
                .any(|entry| {
                    entry.kind == EntryKind::Warning
                        && entry.body.contains("running at the model's own default")
                })
        );
    }

    #[test]
    fn attributed_delegation_enforces_parent_permission_and_depth() {
        let mut state = ready_state();
        state.install_agents(recursive_catalog());

        let (leaf, _) = state
            .delegate_agent("leaf", "Leaf task")
            .expect("leaf delegation");
        let permission_error = state
            .delegate_agent_attributed("recursive", "Forbidden child", Some(&leaf))
            .expect_err("leaf cannot delegate");
        assert!(
            permission_error
                .to_string()
                .contains("not permitted to delegate")
        );

        let (root, _) = state
            .delegate_agent("recursive", "Root task")
            .expect("root delegation");
        let (child, _) = state
            .delegate_agent_attributed("recursive", "Child task", Some(&root))
            .expect("one attributed child");
        let error = state
            .delegate_agent_attributed("recursive", "Grandchild task", Some(&child))
            .expect_err("depth exhausted");

        assert!(
            error
                .to_string()
                .contains("exhausted its maximum delegation depth")
        );
        assert_eq!(state.subagents.len(), 3);
        assert_eq!(
            state.subagent_executions[&child].parent_run_id.as_deref(),
            Some(root.as_str())
        );
    }

    #[test]
    fn restrictive_archetypes_refuse_backends_that_cannot_enforce_runtime_policy() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("restricted.toml"),
            r#"
slug = "restricted"
description = "No tools"
tool_profile = "none"
"#,
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        let effects = state.invoke_agent(&AgentRequest {
            id: 8,
            agent: "restricted".to_owned(),
            task: "Inspect nothing".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
            panic!("expected a subagent launch");
        };
        let run_id = run_id.clone();

        let effects = state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: CURSOR_PROVIDER.to_owned(),
                display_name: "Unsupported compatibility backend".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );

        assert!(effects.iter().all(|effect| !matches!(
            effect,
            Effect::SubagentBackend {
                command: BackendCommand::StartSession { .. },
                ..
            }
        )));
        let run = state
            .subagents
            .iter()
            .find(|run| run.id == run_id)
            .expect("restricted run");
        assert_eq!(run.status, SubagentStatus::Failed);
        assert!(
            run.latest_activity
                .contains("cannot enforce the scoped tool/turn policy")
        );
    }

    /// Gets a delegated run as far as its provider session, so a test can inspect what the first turn
    /// is configured with.
    fn launch_codex_subagent(state: &mut AppState, slug: &str) -> String {
        let effects = state.invoke_agent(&AgentRequest {
            id: 7,
            agent: slug.to_owned(),
            task: "Map auth".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
            panic!("expected a subagent launch, got {effects:?}");
        };
        let run_id = run_id.clone();
        state.handle_subagent_backend(
            &run_id,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        run_id
    }

    #[test]
    fn subagent_system_instructions_include_model_personality_and_soul() {
        let directory = tempdir().expect("config directory");
        let personalities = directory.path().join("personalities.toml");
        let soul = directory.path().join("SOUL.md");
        fs::write(
            &personalities,
            "default = \"Default personality\"\n[models]\n\"openai-codex/model-a\" = \"Explorer personality\"\n",
        )
        .expect("personalities");
        fs::write(&soul, "Shared identity").expect("soul");
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.install_prompt_addenda(
            PromptAddenda::load(Some(&personalities), Some(&soul)).expect("addenda"),
        );
        let effects = state.invoke_agent(&AgentRequest {
            id: 42,
            agent: "explorer".to_owned(),
            task: "Map auth".to_owned(),
        });
        let [Effect::SpawnSubagent { run_id, .. }] = effects.as_slice() else {
            panic!("expected a subagent launch");
        };

        let effects = state.handle_subagent_backend(
            run_id,
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
                && instructions.contains("[Personality]\nExplorer personality")
                && instructions.contains("[Soul]\nShared identity")
        ));
    }

    #[test]
    fn native_delegation_request_id_survives_to_terminal_effect() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        let (run_id, launch) = state
            .delegate_agent_attributed_for_request("explorer", "Inspect native routing", None, 77)
            .expect("native delegation");
        assert!(matches!(launch.as_slice(), [Effect::SpawnSubagent { .. }]));
        let effects = state.cancel_run(&run_id).expect("cancel native child");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::CompleteAgentRequest {
                request_id: 77,
                success: false,
                ..
            }
        )));
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
            }] if prompt.contains("Inspect the delegated question")
                && prompt.contains("Map auth")
                && !prompt.contains("Explore carefully")
        ));
        run_id
    }

    #[test]
    fn new_session_receives_nakode_identity_and_agent_instructions() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.set_nakode_executable(Path::new("/opt/nakode/bin/nakode"));
        state.selected_model = Some("openai-codex/model-a".to_owned());
        state.client.editor.set_text("Start work");

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
        assert!(instructions.contains(
            "Callable: nakode_agent({\"agent\":\"explorer\",\"task\":\"<bounded task>\"})"
        ));
        assert!(instructions.contains("not provider-native collaboration or a shell subprocess"));
        assert!(instructions.contains("Up to 4 subagents may run concurrently"));
        assert!(instructions.contains("launch one Nakode delegation per task concurrently"));
        assert!(instructions.contains("Do not use provider-native subagent"));
        assert!(instructions.ends_with("[/Nakode System Instructions]"));
    }

    #[test]
    fn existing_session_turn_receives_the_current_agent_catalogue() {
        let mut state = ready_state();
        state.provider_session_id = Some("existing-provider-session".to_owned());
        state.install_agents(explorer_catalog());
        state.set_nakode_executable(Path::new("/opt/nakode/bin/nakode"));
        state
            .client
            .editor
            .set_text("Use a sub-agent to inspect auth");

        let effects = state.submit_editor();
        let prompt = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Backend(BackendCommand::StartTurn { prompt, .. }) => Some(prompt),
                _ => None,
            })
            .expect("expected a turn on the existing provider session");

        assert!(prompt.contains("[Nakode Current Agent Catalogue]"));
        assert!(prompt.contains("supersedes the initial Available agents list"));
        assert!(prompt.contains("- explorer: Explores code context"));
        assert!(prompt.contains(
            "Callable: nakode_agent({\"agent\":\"explorer\",\"task\":\"<bounded task>\"})"
        ));
    }

    #[test]
    fn catalogue_never_claims_native_route_for_bridge_only_providers() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        for provider in [CLAUDE_PROVIDER, CURSOR_PROVIDER] {
            state.backend_provider = provider.to_owned();
            let catalogue = state.rendered_agent_catalogue();
            assert!(catalogue.contains("no callable Nakode delegation tool"));
            assert!(!catalogue.contains("Callable: nakode_agent"));
        }
    }

    #[test]
    fn catalogue_omits_native_route_when_builtins_are_replaced() {
        let mut state = ready_state();
        state.install_agents(explorer_catalog());
        state.replace_builtin_tools = true;
        assert!(
            state
                .rendered_agent_catalogue()
                .contains("no callable Nakode delegation tool")
        );
    }

    #[test]
    fn security_validator_requires_an_explicit_sonnet_tier() {
        let directory = tempdir().expect("agent directory");
        fs::write(
            directory.path().join("security-validator.toml"),
            r#"
slug = "security-validator"
description = "Validate one sensitive operation"
model = "openai-codex/model-a"
"#,
        )
        .expect("agent definition");
        let mut state = ready_state();
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));

        let error = state
            .validate_agent_request("security-validator", "Validate Bash")
            .expect_err("non-Sonnet validator must fail closed");
        assert!(error.to_string().contains("Sonnet-tier"));

        fs::write(
            directory.path().join("security-validator.toml"),
            r#"
slug = "security-validator"
description = "Validate one sensitive operation"
model = "claude-agent/sonnet"
"#,
        )
        .expect("agent definition");
        state.install_agents(AgentCatalog::load(directory.path()).expect("agent catalog"));
        state
            .validate_agent_request("security-validator", "Validate Bash")
            .expect("Sonnet validator");
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
        state.install_agents(routed_explorer_catalog());
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
        state.install_agents(routed_explorer_catalog());
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
        state.install_agents(routed_explorer_catalog());
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
                    tool_audit_json: None,
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
        assert!(
            child_chat.transcript.entries().iter().any(|entry| {
                entry.kind == EntryKind::Assistant && entry.body == "No findings."
            })
        );
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
                    provider_session_id: session_id,
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
        state.client.editor.set_text("/providers");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListProviders]
        ));
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: Some(crate::credential::CredentialMetadata {
                provider: CODEX_PROVIDER.to_owned(),
                kind: "chatgpt_device_code".to_owned(),
                updated_at: 1,
            }),
        }]);

        state.open_provider_details();
        assert!(
            state
                .client
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
        state.client.editor.set_text("/providers");
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
    fn kimi_setup_uses_coding_plan_console_and_credential_kind() {
        let mut state = ready_state();
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: crate::backend::KIMI_PROVIDER.to_owned(),
            display_name: "Kimi For Coding".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        assert!(matches!(
            state.open_provider_authentication_url().as_slice(),
            [Effect::OpenUrl(url)] if url == "https://www.kimi.com/code/console"
        ));
        assert!(state.toggle_provider().is_empty());
        state.provider_api_key_insert_str(" kimi-secret ");
        assert!(matches!(
            state.submit_provider_api_key().as_slice(),
            [Effect::SaveProviderCredential { provider, kind, metadata }]
                if provider == crate::backend::KIMI_PROVIDER
                    && kind == "kimi_coding_api_key"
                    && metadata == &serde_json::json!({"api_key":"kimi-secret"})
        ));
    }

    #[test]
    fn glm_setup_uses_coding_plan_console_and_credential_kind() {
        let mut state = ready_state();
        state.install_providers(vec![crate::session::ProviderRecord {
            provider: crate::backend::GLM_PROVIDER.to_owned(),
            display_name: "GLM Coding Plan (z.ai)".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        assert!(matches!(
            state.open_provider_authentication_url().as_slice(),
            [Effect::OpenUrl(url)] if url == "https://z.ai/manage-apikey/apikey-list"
        ));
        assert!(state.toggle_provider().is_empty());
        state.provider_api_key_insert_str(" zai-secret ");
        assert!(matches!(
            state.submit_provider_api_key().as_slice(),
            [Effect::SaveProviderCredential { provider, kind, metadata }]
                if provider == crate::backend::GLM_PROVIDER
                    && kind == "zai_coding_api_key"
                    && metadata == &serde_json::json!({"api_key":"zai-secret"})
        ));
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
        let debug = format!("{:?}", state.client.provider_picker);
        assert!(!debug.contains("secret"));
        state.provider_api_key_backspace();
        assert!(state.cancel_provider_api_key_input());
        assert!(!state.provider_api_key_input_active());
        assert!(!state.cancel_provider_api_key_input());
    }

    #[test]
    fn unconfigured_provider_starts_authentication_before_enablement() {
        let mut state = ready_state();
        state.client.editor.set_text("/providers");
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
            state.provider_authentication.get(CODEX_PROVIDER),
            Some(super::ProviderAuthenticationState::Starting)
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
            state.provider_authentication.get(CODEX_PROVIDER),
            Some(super::ProviderAuthenticationState::Challenge { user_code, .. })
                if user_code == "NAKODE-CODE"
        ));
    }
}
