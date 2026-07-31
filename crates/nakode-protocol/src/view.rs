use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    AgentSessionId, ArtifactId, EntryId, InteractionId, ModelId, PromptId, ProviderId, RunId,
    SessionId, TurnId, WorkspaceId,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapability {
    Resume,
    Steering,
    Interruption,
    ModelCatalog,
    ModelsRequireSession,
    SessionModelConfiguration,
    ContextCompaction,
    Approvals,
    NativeTools,
    Mcp,
    CloseSession,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderCapabilities {
    #[serde(default)]
    pub supported: BTreeSet<ProviderCapability>,
}

impl ProviderCapabilities {
    #[must_use]
    pub fn supports(&self, capability: ProviderCapability) -> bool {
        self.supported.contains(&capability)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelView {
    pub id: ModelId,
    pub provider_id: ProviderId,
    pub model_slug: String,
    pub display_name: String,
    pub is_default: bool,
    pub reasoning_effort: Option<String>,
    pub fast_mode: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ConnectionView {
    Disabled,
    Starting,
    Ready,
    Failed { message: String },
    Disconnected { message: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderAuthenticationView {
    Starting,
    ApiKeyRequired {
        dashboard_url: String,
        credential_kind: String,
    },
    Challenge {
        verification_url: String,
        user_code: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderView {
    pub id: ProviderId,
    pub display_name: String,
    pub enabled: bool,
    pub credential_configured: bool,
    pub credential_kind: Option<String>,
    pub connection: ConnectionView,
    pub capabilities: ProviderCapabilities,
    pub authentication: Option<ProviderAuthenticationView>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionSummary {
    pub id: SessionId,
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub active_provider_id: Option<ProviderId>,
    pub active_model_id: Option<ModelId>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentSessionView {
    pub id: AgentSessionId,
    pub provider_id: ProviderId,
    pub model_id: Option<ModelId>,
    pub role: String,
    pub capabilities: ProviderCapabilities,
    pub connection: ConnectionView,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Starting,
    Running,
    Cancelling,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnView {
    pub id: TurnId,
    pub agent_session_id: AgentSessionId,
    pub model_id: Option<ModelId>,
    pub status: TurnStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextUsageView {
    pub estimated_tokens: u64,
    pub context_window: Option<u64>,
    pub compacting: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptEntryKind {
    System,
    User,
    Assistant,
    Steering,
    Reasoning,
    Tool,
    Diff,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptEntryStatus {
    Running,
    Complete,
    Failed,
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptEntryView {
    pub id: EntryId,
    pub kind: TranscriptEntryKind,
    pub title: String,
    pub body: String,
    pub status: TranscriptEntryStatus,
    #[serde(default)]
    pub artifacts: Vec<ArtifactId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptPage {
    pub entries: Vec<TranscriptEntryView>,
    pub has_earlier: bool,
    pub stream_active: bool,
    pub stream_label: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QueueItemView {
    pub id: PromptId,
    pub summary: String,
    pub attachment_count: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Approval,
    Question,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionStatus {
    Pending,
    Resolved,
    Declined,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InteractionOptionView {
    pub id: String,
    pub label: String,
    pub description: Option<String>,
    pub recommended: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InteractionView {
    pub id: InteractionId,
    pub revision: u64,
    pub kind: InteractionKind,
    pub status: InteractionStatus,
    pub title: String,
    pub detail: String,
    pub options: Vec<InteractionOptionView>,
    pub multiple: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatusView {
    Pending,
    InProgress,
    Completed,
    Abandoned,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TodoItemView {
    pub content: String,
    pub status: TodoStatusView,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TodoPhaseView {
    pub name: String,
    pub tasks: Vec<TodoItemView>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Starting,
    Working,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunView {
    pub id: RunId,
    pub agent_slug: String,
    pub provider_id: ProviderId,
    pub objective: String,
    pub status: RunStatus,
    pub latest_activity: String,
    pub result: Option<String>,
    pub transcript: TranscriptPage,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactView {
    pub id: ArtifactId,
    pub label: String,
    pub media_type: String,
    pub byte_length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeLevel {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NoticeView {
    pub id: String,
    pub level: NoticeLevel,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentDefinitionView {
    pub slug: String,
    pub description: String,
    pub system_prompt: String,
    pub first_message: String,
    pub model_id: Option<ModelId>,
    pub fallback_models: Vec<ModelId>,
    pub fast_mode: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkillView {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AgentBrowserView {
    Checking,
    Available { version: String },
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WebSettingsView {
    pub backend: String,
    pub credential_configured: bool,
    pub agent_browser: AgentBrowserView,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemorySettingsView {
    pub backend: String,
    pub executable: String,
    pub global_bank: String,
    pub data_directory: String,
    pub configured: bool,
    pub available: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VisionSettingsView {
    pub model_id: Option<ModelId>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalImageModeView {
    Auto,
    On,
    Off,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SettingsView {
    pub web: WebSettingsView,
    pub memory: MemorySettingsView,
    pub vision: VisionSettingsView,
    pub terminal_images: TerminalImageModeView,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionActivity {
    Idle,
    CreatingAgentSession,
    StartingTurn,
    RunningTurn,
    CompactingContext,
    RunningDelegates,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionView {
    pub id: SessionId,
    pub revision: u64,
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub status_message: String,
    pub diagnostic_count: u64,
    pub activity: SessionActivity,
    pub selected_provider_id: Option<ProviderId>,
    pub selected_model_id: Option<ModelId>,
    pub active_agent_session: Option<AgentSessionView>,
    pub active_turn: Option<TurnView>,
    pub context_usage: Option<ContextUsageView>,
    pub transcript: TranscriptPage,
    pub queue: Vec<QueueItemView>,
    pub interactions: Vec<InteractionView>,
    pub todos: Vec<TodoPhaseView>,
    pub runs: Vec<RunView>,
    pub notices: Vec<NoticeView>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BootstrapView {
    pub workspace_id: WorkspaceId,
    pub workspace_path: String,
    pub providers: Vec<ProviderView>,
    pub models: Vec<ModelView>,
    pub agents: Vec<AgentDefinitionView>,
    pub skills: Vec<SkillView>,
    pub settings: SettingsView,
    pub sessions: Vec<SessionSummary>,
    pub active_session: Option<SessionView>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ViewEvent {
    SessionUpserted {
        session: SessionSummary,
    },
    SessionChanged {
        session: Box<SessionView>,
    },
    TranscriptEntryCreated {
        session_id: SessionId,
        entry: TranscriptEntryView,
    },
    TranscriptEntryPatched {
        session_id: SessionId,
        entry: TranscriptEntryView,
    },
    QueueChanged {
        session_id: SessionId,
        queue: Vec<QueueItemView>,
    },
    InteractionChanged {
        session_id: SessionId,
        interaction: InteractionView,
    },
    TodosChanged {
        session_id: SessionId,
        phases: Vec<TodoPhaseView>,
    },
    RunChanged {
        session_id: SessionId,
        run: RunView,
    },
    ProviderChanged {
        provider: ProviderView,
    },
    BootstrapChanged {
        snapshot: Box<BootstrapView>,
    },
}
