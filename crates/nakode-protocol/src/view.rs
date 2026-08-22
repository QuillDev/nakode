use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    AgentSessionId, ArtifactId, EntryId, InteractionId, ModelId, ModelOptions, PromptId,
    ProviderId, RunId, SessionId, TurnId, WorkspaceId,
};
use crate::{PromptAttachment, RunTextField};

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
    ExternalTools,
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
    #[serde(default)]
    pub configuration: ModelConfigurationView,
}

/// Frontend-neutral controls and roles supported by one model.
///
/// Current option values remain on [`ModelView`]; this metadata tells clients
/// which controls they may present without knowing provider identities.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelConfigurationView {
    #[serde(default)]
    pub reasoning_efforts: Vec<String>,
    #[serde(default)]
    pub fast_mode_configurable: bool,
    #[serde(default)]
    pub vision_eligible: bool,
    #[serde(default)]
    pub accepts_image_input: bool,
}

impl ModelConfigurationView {
    #[must_use]
    pub fn reasoning_is_configurable(&self) -> bool {
        !self.reasoning_efforts.is_empty()
    }
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
    #[serde(default)]
    pub model_filter_enabled: bool,
    #[serde(default)]
    pub selected_model_ids: Vec<ModelId>,
    /// Full discovered catalogue used by settings; BootstrapView.models may be filtered.
    #[serde(default)]
    pub model_candidates: Vec<ModelView>,
    /// Canonical built-ins this provider adapter can project, independent of add-on enablement.
    /// `None` means an older or non-authoritative projection.
    #[serde(default)]
    pub supported_builtin_tools: Option<Vec<String>>,
    /// Canonical built-ins this provider can expose under the current runtime/add-on settings.
    /// `None` means an older or non-authoritative projection, never an unrestricted default.
    #[serde(default)]
    pub available_builtin_tools: Option<Vec<String>>,
}

/// Product surface that owns the user-facing projection of one logical session.
///
/// This is intentionally transport-neutral: Discord, Slack, or another thread transport may map
/// the same Chat/Agent distinction to different destinations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestratorKind {
    Chat,
    Agent,
}

/// Desired lifecycle of an external thread paired with a logical session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeLifecycle {
    Open,
    Archived,
}

/// User-visible transcript role projected through an external session bridge.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeProjectionKind {
    User,
    Assistant,
}

/// Stable position in one bridge's ordered user/assistant transcript projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeProjectionView {
    pub kind: BridgeProjectionKind,
    pub turn_id: TurnId,
}

/// Crash-recoverable, constant-size delivery checkpoint for one transcript projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeDeliveryView {
    pub projection: BridgeProjectionView,
    pub previous_projection: Option<BridgeProjectionView>,
    pub body_sha256: String,
    pub part_count: u64,
    pub completed_parts: u64,
    pub last_external_message_id: Option<String>,
}

/// Authoritative provider-neutral pairing intent and adapter-owned external identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionBridgeView {
    pub session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub kind: OrchestratorKind,
    pub lifecycle: BridgeLifecycle,
    pub display_title: String,
    pub revision: u64,
    pub transport: Option<String>,
    pub external_parent_id: Option<String>,
    pub external_thread_id: Option<String>,
    pub last_projected: Option<BridgeProjectionView>,
    pub delivery: Option<BridgeDeliveryView>,
    pub live_turn_id: Option<TurnId>,
    pub live_external_message_id: Option<String>,
    pub active_source_message_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionSummary {
    pub id: SessionId,
    pub workspace_id: WorkspaceId,
    pub title: String,
    /// Canonical filesystem/provider process root, independent from logical workspace ownership.
    #[serde(default)]
    pub working_directory: String,
    pub active_provider_id: Option<ProviderId>,
    pub active_model_id: Option<ModelId>,
    pub updated_at_ms: i64,
    /// Immutable logical-session creation boundary, in Unix epoch milliseconds.
    #[serde(default)]
    pub created_at_ms: i64,
    /// Latest accepted or terminal owner-turn boundary, in Unix epoch milliseconds.
    /// Zero means unavailable for compatibility with projections predating this field.
    #[serde(default)]
    pub last_owner_activity_at_ms: i64,
    /// Provider-native resources whose lifecycle belongs to this logical session.
    #[serde(default)]
    pub owned_provider_sessions: Vec<OwnedProviderSessionView>,
    /// True only while the service still owns live work for this logical session.
    #[serde(default)]
    pub running: bool,
}

/// An opaque provider resource claimed by one Nakode logical session.
///
/// Clients use this identity only to reconcile provider-native discovery. Mutations continue to
/// address the owning Nakode session or agent session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OwnedProviderSessionView {
    pub provider_id: ProviderId,
    pub native_session_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenUsageView {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentSessionView {
    pub id: AgentSessionId,
    pub provider_id: ProviderId,
    pub model_id: Option<ModelId>,
    pub role: String,
    pub capabilities: ProviderCapabilities,
    pub connection: ConnectionView,
    /// Opaque provider resume identity. Nakode is its lifecycle owner.
    pub native_session_id: Option<String>,
    /// The provider worker's normalized transcript, suitable for a read-only child view.
    pub transcript: TranscriptPage,
    #[serde(default)]
    pub usage: TokenUsageView,
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
    /// Concrete options captured at owner-turn acceptance.
    #[serde(default)]
    pub resolved_model_options: ModelOptions,
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
    #[serde(default)]
    pub body_start_byte: u64,
    #[serde(default)]
    pub body_total_bytes: u64,
    pub status: TranscriptEntryStatus,
    #[serde(default)]
    pub artifacts: Vec<ArtifactId>,
    /// Immutable server entry-creation boundary in Unix epoch milliseconds.
    #[serde(default)]
    pub created_at_ms: Option<u64>,
    /// Provider identity captured when this turn began. Absent for legacy or non-inference entries.
    #[serde(default)]
    pub provider_id: Option<String>,
    /// Canonical provider-qualified model captured when this turn began.
    #[serde(default)]
    pub model_id: Option<ModelId>,
    /// Immutable owner turn that produced this entry.
    #[serde(default)]
    pub owner_turn_id: Option<TurnId>,
    /// Concrete reasoning effort resolved when that owner turn began.
    #[serde(default)]
    pub resolved_reasoning_effort: Option<String>,
    /// Concrete fast-mode value resolved when that owner turn began.
    #[serde(default)]
    pub resolved_fast_mode: Option<bool>,
    /// External transport that originated this user turn. Absent for dashboard/SDK-origin input.
    #[serde(default)]
    pub source_transport: Option<String>,
    /// Versioned, bounded tool audit JSON. Clients must render it as inert data.
    #[serde(default)]
    pub tool_audit_json: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptPage {
    pub entries: Vec<TranscriptEntryView>,
    pub has_earlier: bool,
    pub stream_active: bool,
    pub stream_label: String,
    /// Additive latest-owner anchor; its stable ID may also occur in `entries`.
    /// Presentation clients that pin it should render that ID once.
    #[serde(default)]
    pub current_owner_entry: Option<TranscriptEntryView>,
    /// Exact tool/diff rows from that owner turn omitted from `entries`.
    #[serde(default)]
    pub current_owner_omitted_tool_calls: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptBodyWindow {
    pub entry_id: EntryId,
    pub body: String,
    pub start_byte: u64,
    pub total_bytes: u64,
    pub has_earlier: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptWindowView {
    pub entry_ids: Vec<EntryId>,
    pub has_earlier: bool,
    pub stream_active: bool,
    pub stream_label: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QueueItemView {
    pub id: PromptId,
    pub summary: String,
    /// Complete semantic text, so a client can distinguish and control queued work without caching drafts.
    pub text: String,
    pub attachment_count: u32,
    /// Reserved by an ordered native-steer or stop-and-send operation, but not yet consumed.
    pub redirecting: bool,
}

/// A server-owned prompt that definitively failed before inference began.
///
/// Clients may present this semantic value and retry it through
/// `Command::SendPrompt`; no client-local draft or binary attachment cache is
/// needed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoverablePromptView {
    pub id: PromptId,
    pub text: String,
    pub attachments: Vec<PromptAttachment>,
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
pub struct InteractionQuestionView {
    pub id: String,
    pub title: String,
    pub detail: String,
    pub options: Vec<InteractionOptionView>,
    pub multiple: bool,
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
    /// Present for grouped question interactions. Legacy scalar fields above mirror the first item.
    #[serde(default)]
    pub questions: Vec<InteractionQuestionView>,
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
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunOutcome {
    Completed { body: String },
    Failed { reason: String },
    Interrupted { reason: String },
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct RunPolicyView {
    pub allowed_capabilities: Vec<String>,
    pub denied_capabilities: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub denied_tools: Vec<String>,
    /// Provider adapter and exact call identities enforced for this immutable launch snapshot.
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub policy_available: bool,
    #[serde(default)]
    pub provider_tools_restricted: bool,
    #[serde(default)]
    pub provider_allowed_tools: Vec<String>,
    #[serde(default)]
    pub unsupported_canonical_tools: Vec<String>,
    pub tool_profile: String,
    pub task_shape: String,
    pub output_contract: String,
    pub timeout_seconds: Option<u32>,
    pub max_turns: Option<u32>,
    pub can_delegate: bool,
    pub max_delegation_depth: u32,
    pub remaining_delegation_depth: u32,
    pub require_parent_attribution: bool,
    #[serde(default)]
    pub truncated_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunToolDenialView {
    pub entry_id: String,
    pub tool: String,
    #[serde(default)]
    pub tool_start_byte: u64,
    #[serde(default)]
    pub tool_total_bytes: u64,
    pub reason: String,
    #[serde(default)]
    pub reason_start_byte: u64,
    #[serde(default)]
    pub reason_total_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunView {
    pub id: RunId,
    #[serde(default)]
    pub parent_run_id: Option<RunId>,
    pub agent_slug: String,
    #[serde(default)]
    pub archetype_purpose: String,
    pub provider_id: ProviderId,
    pub model_id: Option<ModelId>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub fast_mode: bool,
    #[serde(default)]
    pub started_at_ms: u64,
    #[serde(default)]
    pub ended_at_ms: Option<u64>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub termination_kind: Option<String>,
    #[serde(default)]
    pub termination_detail: Option<String>,
    #[serde(default)]
    pub objective_mismatch_handoff: Option<String>,
    #[serde(default)]
    pub policy: RunPolicyView,
    #[serde(default)]
    pub tool_denials: Vec<RunToolDenialView>,
    /// Total denials still present in the authoritative retained transcript before projection caps.
    #[serde(default)]
    pub tool_denials_retained_total: u32,
    /// Provider-native child resource owned by this run, never independently mutable.
    pub native_session_id: Option<String>,
    #[serde(default)]
    pub usage: TokenUsageView,
    pub objective: String,
    #[serde(default)]
    pub objective_start_byte: u64,
    #[serde(default)]
    pub objective_total_bytes: u64,
    pub status: RunStatus,
    pub latest_activity: String,
    #[serde(default)]
    pub latest_activity_start_byte: u64,
    #[serde(default)]
    pub latest_activity_total_bytes: u64,
    #[serde(default)]
    pub outcome: Option<RunOutcome>,
    #[serde(default)]
    pub outcome_start_byte: u64,
    #[serde(default)]
    pub outcome_total_bytes: u64,
    pub result: Option<String>,
    #[serde(default)]
    pub result_start_byte: u64,
    #[serde(default)]
    pub result_total_bytes: u64,
    pub transcript: TranscriptPage,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunPage {
    pub runs: Vec<RunView>,
    pub has_earlier: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunMetadataView {
    pub id: RunId,
    pub agent_slug: String,
    pub provider_id: ProviderId,
    pub objective: String,
    pub objective_start_byte: u64,
    pub objective_total_bytes: u64,
    pub status: RunStatus,
    pub latest_activity: String,
    pub latest_activity_start_byte: u64,
    pub latest_activity_total_bytes: u64,
    #[serde(default)]
    pub outcome: Option<RunOutcome>,
    pub outcome_start_byte: u64,
    pub outcome_total_bytes: u64,
    pub result: Option<String>,
    pub result_start_byte: u64,
    pub result_total_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunTextWindow {
    pub run_id: RunId,
    pub field: RunTextField,
    pub text: String,
    pub start_byte: u64,
    pub total_bytes: u64,
    pub has_earlier: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactView {
    pub id: ArtifactId,
    pub label: String,
    pub media_type: String,
    pub byte_length: u64,
    #[serde(with = "crate::base64_bytes")]
    pub data: Vec<u8>,
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
// Frontend-neutral projection mirrors independent configured/effective protobuf flags. A combined
// state enum would erase the stable wire distinction between these policy dimensions.
#[allow(clippy::struct_excessive_bools)]
pub struct AgentDefinitionView {
    pub slug: String,
    pub description: String,
    pub system_prompt: String,
    pub first_message: String,
    pub model_id: Option<ModelId>,
    pub fallback_models: Vec<ModelId>,
    pub fast_mode: bool,
    /// The level this archetype runs at, or `None` for the model's own default.
    ///
    /// A level is a property OF a model, so this is only ever set alongside `model_id`, and a
    /// definition written before the field existed reads back as `None` — the default.
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub ownership: String,
    #[serde(default = "true_by_default")]
    pub enabled: bool,
    #[serde(default)]
    pub allowed_capabilities: Vec<String>,
    #[serde(default)]
    pub denied_capabilities: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
    #[serde(default)]
    pub tool_profile: String,
    #[serde(default)]
    pub task_shape: String,
    #[serde(default)]
    pub output_contract: String,
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
    #[serde(default)]
    pub poll_interval_ms: Option<u32>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub max_concurrency: u32,
    #[serde(default)]
    pub fallback_policy: String,
    #[serde(default)]
    pub can_delegate: bool,
    #[serde(default)]
    pub max_delegation_depth: u32,
    #[serde(default = "true_by_default")]
    pub require_parent_attribution: bool,
    /// Nakode's interpreted runtime boundary. `None` preserves a legacy provider-default tool set.
    #[serde(default)]
    pub effective_builtin_tools: Option<Vec<String>>,
    /// Effective declared capabilities, or `None` when legacy provider defaults are ambiguous.
    #[serde(default)]
    pub effective_capabilities: Option<Vec<String>>,
    /// Authoritative reconciliation and high-impact policy findings.
    #[serde(default)]
    pub policy_warnings: Vec<String>,
    /// Delegated runs intentionally do not receive a frontend's dashboard/control-plane tool table.
    #[serde(default)]
    pub dashboard_tools_injected: bool,
    /// Nonzero when the server projected every interpreted-policy field above.
    #[serde(default)]
    pub policy_projection_version: u32,
}

fn true_by_default() -> bool {
    true
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
    #[serde(default)]
    pub invocation_telemetry_enabled: bool,
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
    RunningShell,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionView {
    pub id: SessionId,
    pub revision: u64,
    pub workspace_id: WorkspaceId,
    /// Canonical filesystem/provider process root, independent from logical workspace ownership.
    #[serde(default)]
    pub working_directory: String,
    pub title: String,
    pub status_message: String,
    pub diagnostic_count: u64,
    pub activity: SessionActivity,
    pub selected_provider_id: Option<ProviderId>,
    pub selected_model_id: Option<ModelId>,
    /** Effective model options for this logical session, including a session-local override. */
    #[serde(default)]
    pub selected_model_options: ModelOptions,
    pub active_agent_session: Option<AgentSessionView>,
    pub active_turn: Option<TurnView>,
    /// Most recent terminal owner turn. Delegated runs never populate this field.
    #[serde(default)]
    pub last_turn: Option<TurnView>,
    /// Server-owned indication that selected next-turn configuration differs from active work.
    #[serde(default)]
    pub next_turn_configuration_pending: bool,
    /// Server-authored provider/native-session transition semantics for the next turn.
    #[serde(default)]
    pub next_turn_transition: Option<String>,
    pub context_usage: Option<ContextUsageView>,
    pub transcript: TranscriptPage,
    #[serde(default)]
    pub recoverable_prompt: Option<RecoverablePromptView>,
    pub queue: Vec<QueueItemView>,
    pub interactions: Vec<InteractionView>,
    pub todos: Vec<TodoPhaseView>,
    pub runs: Vec<RunView>,
    /// All delegated runs owned by this logical session before the bounded `runs` projection.
    /// Includes both direct runs and recursively delegated descendants.
    #[serde(default)]
    pub runs_total: Option<u64>,
    #[serde(default)]
    pub runs_has_earlier: bool,
    pub notices: Vec<NoticeView>,
    #[serde(default)]
    pub external_tool_calls: Vec<ExternalToolCallView>,
    /// Persisted logical-session creation and latest-update boundaries, in Unix epoch milliseconds.
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub updated_at_ms: i64,
    /// Latest accepted or terminal owner-turn boundary. Generic touches never change it.
    /// Zero means unavailable for compatibility with projections predating this field.
    #[serde(default)]
    pub last_owner_activity_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExternalToolCallView {
    pub id: String,
    pub name: String,
    pub arguments_json: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionMetadataView {
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub status_message: String,
    pub diagnostic_count: u64,
    pub activity: SessionActivity,
    pub selected_provider_id: Option<ProviderId>,
    pub selected_model_id: Option<ModelId>,
    #[serde(default)]
    pub selected_model_options: ModelOptions,
    pub active_agent_session: Option<AgentSessionView>,
    pub active_turn: Option<TurnView>,
    #[serde(default)]
    pub last_turn: Option<TurnView>,
    #[serde(default)]
    pub next_turn_configuration_pending: bool,
    #[serde(default)]
    pub next_turn_transition: Option<String>,
    pub context_usage: Option<ContextUsageView>,
    #[serde(default)]
    pub recoverable_prompt: Option<RecoverablePromptView>,
    pub notices: Vec<NoticeView>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BootstrapView {
    pub workspace_id: WorkspaceId,
    pub workspace_path: String,
    /// Durable external-thread bridge records, including archived and currently unbound intents.
    #[serde(default)]
    pub session_bridges: Vec<SessionBridgeView>,
    pub providers: Vec<ProviderView>,
    /// The ordinary catalogue: each provider's discovered models, narrowed by its model filter
    /// when the filter is enabled and still selects at least one discovered model. A filter whose
    /// selection matches nothing discovered fails open, so this never silently empties a provider.
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
        revision: u64,
        session: SessionSummary,
    },
    SessionRemoved {
        session_id: SessionId,
    },
    SessionChanged {
        session: Box<SessionView>,
    },
    SessionMetadataChanged {
        session_id: SessionId,
        revision: u64,
        metadata: Box<SessionMetadataView>,
    },
    TranscriptEntryCreated {
        session_id: SessionId,
        revision: u64,
        entry: TranscriptEntryView,
    },
    TranscriptEntryPatched {
        session_id: SessionId,
        revision: u64,
        entry: TranscriptEntryView,
    },
    TranscriptEntryDelta {
        session_id: SessionId,
        revision: u64,
        entry_id: EntryId,
        append_at_byte: u64,
        delta: String,
        status: TranscriptEntryStatus,
    },
    TranscriptWindowChanged {
        session_id: SessionId,
        revision: u64,
        window: TranscriptWindowView,
    },
    QueueChanged {
        session_id: SessionId,
        revision: u64,
        queue: Vec<QueueItemView>,
    },
    InteractionsChanged {
        session_id: SessionId,
        revision: u64,
        interactions: Vec<InteractionView>,
    },
    TodosChanged {
        session_id: SessionId,
        revision: u64,
        phases: Vec<TodoPhaseView>,
    },
    RunChanged {
        session_id: SessionId,
        revision: u64,
        run: Box<RunView>,
    },
    RunRemoved {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
    },
    RunWindowChanged {
        session_id: SessionId,
        revision: u64,
        run_ids: Vec<RunId>,
        has_earlier: bool,
    },
    RunMetadataChanged {
        session_id: SessionId,
        revision: u64,
        run: Box<RunMetadataView>,
    },
    RunTranscriptEntryCreated {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
        entry: TranscriptEntryView,
    },
    RunTranscriptEntryPatched {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
        entry: TranscriptEntryView,
    },
    RunTranscriptEntryDelta {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
        entry_id: EntryId,
        append_at_byte: u64,
        delta: String,
        status: TranscriptEntryStatus,
    },
    RunTranscriptWindowChanged {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
        window: TranscriptWindowView,
    },
    ProviderChanged {
        provider: ProviderView,
    },
    ProviderRemoved {
        provider_id: ProviderId,
    },
    BootstrapChanged {
        snapshot: Box<BootstrapView>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ArtifactView, ModelConfigurationView, ModelView, RunOutcome, RunView, TranscriptEntryKind,
        TranscriptEntryStatus, TranscriptEntryView,
    };
    use crate::{
        ArtifactId, EntryId, MAX_API_MESSAGE_BYTES, MAX_ARTIFACT_BYTES, ModelId, ProviderId,
    };

    #[test]
    fn artifact_payloads_use_base64_and_leave_frame_headroom() {
        let artifact = ArtifactView {
            id: ArtifactId::from("artifact-1"),
            label: "clipboard.png".to_owned(),
            media_type: "image/png".to_owned(),
            byte_length: 4,
            data: vec![0, 1, 2, 255],
        };

        let encoded = serde_json::to_value(&artifact).expect("serialize artifact");
        assert_eq!(encoded["data"], "AAEC/w==");
        assert_eq!(
            serde_json::from_value::<ArtifactView>(encoded).expect("decode artifact"),
            artifact
        );
        const {
            assert!(MAX_ARTIFACT_BYTES < MAX_API_MESSAGE_BYTES);
        }
    }

    #[test]
    fn model_configuration_has_a_stable_wire_shape() {
        let model = ModelView {
            id: ModelId::from("openai-codex/gpt-5.6"),
            provider_id: ProviderId::from("openai-codex"),
            model_slug: "gpt-5.6".to_owned(),
            display_name: "GPT 5.6".to_owned(),
            is_default: true,
            reasoning_effort: Some("high".to_owned()),
            fast_mode: false,
            configuration: ModelConfigurationView {
                reasoning_efforts: vec!["none".to_owned(), "high".to_owned()],
                fast_mode_configurable: true,
                vision_eligible: true,
                accepts_image_input: true,
            },
        };

        assert_eq!(
            serde_json::to_value(model).expect("serialize model view"),
            json!({
                "id": "openai-codex/gpt-5.6",
                "provider_id": "openai-codex",
                "model_slug": "gpt-5.6",
                "display_name": "GPT 5.6",
                "is_default": true,
                "reasoning_effort": "high",
                "fast_mode": false,
                "configuration": {
                    "reasoning_efforts": ["none", "high"],
                    "fast_mode_configurable": true,
                    "vision_eligible": true,
                    "accepts_image_input": true,
                },
            })
        );
    }

    #[test]
    fn legacy_model_view_defaults_to_no_configurable_features() {
        let model: ModelView = serde_json::from_value(json!({
            "id": "other/basic",
            "provider_id": "other",
            "model_slug": "basic",
            "display_name": "Basic",
            "is_default": false,
            "reasoning_effort": null,
            "fast_mode": false,
        }))
        .expect("deserialize legacy model view");

        assert_eq!(model.configuration, ModelConfigurationView::default());
        assert!(!model.configuration.reasoning_is_configurable());
    }

    #[test]
    fn run_outcome_has_a_semantic_tag_and_payload() {
        assert_eq!(
            serde_json::to_value(RunOutcome::Completed {
                body: "Implemented the migration.".to_owned(),
            })
            .expect("serialize run outcome"),
            json!({
                "status": "completed",
                "body": "Implemented the migration.",
            })
        );
        assert_eq!(
            serde_json::to_value(RunOutcome::Failed {
                reason: "Provider disconnected.".to_owned(),
            })
            .expect("serialize run outcome"),
            json!({
                "status": "failed",
                "reason": "Provider disconnected.",
            })
        );
    }

    #[test]
    fn legacy_run_view_without_outcome_still_deserializes() {
        let run: RunView = serde_json::from_value(json!({
            "id": "run-1",
            "agent_slug": "reviewer",
            "provider_id": "openai-codex",
            "objective": "Review the migration",
            "status": "completed",
            "latest_activity": "Completed",
            "result": "Legacy result",
            "transcript": {
                "entries": [],
                "has_earlier": false,
                "stream_active": false,
                "stream_label": "reviewer",
            },
        }))
        .expect("deserialize legacy run view");

        assert_eq!(run.outcome, None);
        assert_eq!(run.result.as_deref(), Some("Legacy result"));
    }

    #[test]
    fn transcript_body_windows_are_explicit() {
        let entry = TranscriptEntryView {
            id: EntryId::from("entry-1"),
            kind: TranscriptEntryKind::Assistant,
            title: "Nakode".to_owned(),
            body: "tail".to_owned(),
            body_start_byte: 96,
            body_total_bytes: 100,
            status: TranscriptEntryStatus::Running,
            artifacts: Vec::new(),
            created_at_ms: None,
            provider_id: Some("openai-codex".to_owned()),
            model_id: Some(ModelId::from("openai-codex/gpt-5.4")),
            owner_turn_id: None,
            resolved_reasoning_effort: None,
            resolved_fast_mode: None,
            tool_audit_json: None,
        };
        let value = serde_json::to_value(entry).expect("serialize transcript entry");
        assert_eq!(value["body_start_byte"], 96);
        assert_eq!(value["body_total_bytes"], 100);
        assert_eq!(value["provider_id"], "openai-codex");
        assert_eq!(value["model_id"], "openai-codex/gpt-5.4");
    }
}
