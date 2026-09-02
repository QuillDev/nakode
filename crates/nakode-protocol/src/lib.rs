//! Internal provider-neutral semantic types used behind Nakode's public API.
//!
//! This crate intentionally contains only semantic data-transfer types. It has
//! no dependency on a terminal, renderer, provider adapter, process runtime, or
//! persistence implementation.

mod base64_bytes;
mod command;
mod diagnostics;
mod error;
mod ids;
mod invocation;
mod mcp;
mod service;
mod view;

pub use command::{
    AgentDefinitionInput, Command, CredentialInput, ExternalToolDefinition, InteractionResolution,
    ModelOptions, ModelTarget, PromptAttachment, PromptInput, Query, QuestionResponse,
    RunTextField, SessionBridgeIntent, SessionToolConfiguration, SettingsPatch, TranscriptOwner,
};
pub use diagnostics::{
    DiagnosticsDailyUsage, DiagnosticsReport, DiagnosticsSessionUsage, DiagnosticsToolUsage,
    DiagnosticsUsageTotals,
};
pub use error::{ErrorCode, ServiceError};
pub use ids::{
    AgentSessionId, ArtifactId, ClientId, EntryId, IdempotencyKey, InteractionId, ModelId,
    PromptId, ProviderId, RequestId, RunId, ServerEpoch, SessionId, SubscriptionId, TurnId,
    WorkspaceId,
};
pub use invocation::{
    InvocationBucket, InvocationKind, InvocationSummary, InvocationTimeline, InvocationUsage,
};
pub use mcp::{
    MCP_TOOL_PREFIX, McpGrantPolicy, McpManagementView, McpServerInput, McpServerView,
    McpSessionGrant, McpSessionSurface, McpTemplateView, McpToolView,
};
pub use service::{
    BridgeContinuationDisposition, CommandAccepted, Cursor, ManageableSkillView, QueryResult,
    ServiceCapabilities, ServiceCapability, SessionInventory, SkillCatalogueView, Snapshot,
    SoulDocumentView, SubscriptionScope, SubscriptionView, WorkspacePathInspectionView,
};
pub use view::{
    AgentBrowserView, AgentDefinitionView, AgentSessionView, ArtifactView, BootstrapView,
    BridgeDeliveryView, BridgeLifecycle, BridgeProjectionKind, BridgeProjectionView,
    ConnectionView, ContextUsageView, ContinuationPropositionView, ExternalToolCallView,
    InteractionKind, InteractionOptionView, InteractionQuestionView, InteractionStatus,
    InteractionView, MemorySettingsView, ModelConfigurationView, ModelView, NoticeLevel,
    NoticeView, OrchestratorKind, OwnedProviderSessionView, ProviderAccountHealthState,
    ProviderAccountHealthView, ProviderAccountRoutingDiagnosticView, ProviderAccountRoutingMode,
    ProviderAccountView, ProviderAuthenticationView, ProviderCapabilities, ProviderCapability,
    ProviderView, QueueItemView, RecoverablePromptView, RunMetadataView, RunOutcome, RunPage,
    RunPolicyView, RunSalvageView, RunStatus, RunTextWindow, RunToolDenialView, RunView,
    SalvagedEvidenceView, SessionActivity, SessionBridgeView, SessionFailureClassification,
    SessionFailurePhase, SessionFailureView, SessionMetadataView, SessionSummary, SessionView,
    SettingsView, SharedContextEntryView, SharedContextUtilizationView, SkillView,
    TerminalImageModeView, TodoItemView, TodoPhaseView, TodoStatusView, TokenUsageView,
    TranscriptBodyWindow, TranscriptEntryKind, TranscriptEntryStatus, TranscriptEntryView,
    TranscriptPage, TranscriptWindowView, TurnStatus, TurnView, ViewEvent, VisionAvailabilityView,
    VisionSettingsView, WebSettingsView,
};

/// Maximum encoded Protobuf request or response accepted by Nakode's API.
pub const MAX_API_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum raw byte payload returned for one artifact.
///
/// The raw limit preserves Nakode's existing 20 MiB attachment contract while
/// leaving headroom for surrounding Protobuf metadata.
pub const MAX_ARTIFACT_BYTES: usize = 20 * 1024 * 1024;

/// Maximum number of semantic entries carried in one transcript snapshot.
pub const MAX_TRANSCRIPT_PAGE_ENTRIES: usize = 128;

/// Maximum combined UTF-8 body bytes carried in one paged transcript query.
pub const MAX_TRANSCRIPT_PAGE_BODY_BYTES: usize = 512 * 1024;

/// Maximum number of semantic entries carried in a live session snapshot.
pub const MAX_TRANSCRIPT_SNAPSHOT_ENTRIES: usize = 256;

/// Maximum combined UTF-8 body bytes carried in a live session snapshot.
pub const MAX_TRANSCRIPT_SNAPSHOT_BODY_BYTES: usize = 1024 * 1024;

/// Maximum UTF-8 body bytes retained for one projected transcript entry.
pub const MAX_TRANSCRIPT_ENTRY_BODY_BYTES: usize = 256 * 1024;

/// Maximum UTF-8 bytes carried by one append-only transcript event.
pub const MAX_TRANSCRIPT_DELTA_BYTES: usize = 64 * 1024;

/// Maximum number of run projections embedded in one session or run page.
pub const MAX_SESSION_RUNS: usize = 64;

/// Maximum aggregate JSON bytes used by embedded run projections.
pub const MAX_SESSION_RUNS_BYTES: usize = 4 * 1024 * 1024;

/// Maximum UTF-8 bytes carried by one run metadata text field.
pub const MAX_RUN_TEXT_BYTES: usize = 64 * 1024;

/// Maximum denial records embedded in one run projection; the total remains explicit.
pub const MAX_RUN_TOOL_DENIALS: usize = 50;

/// Maximum UTF-8 bytes carried by one denial tool name or reason window.
pub const MAX_RUN_TOOL_DENIAL_TEXT_BYTES: usize = 4 * 1024;

/// Maximum values projected from any one run policy array.
pub const MAX_RUN_POLICY_ITEMS: usize = 50;

/// Maximum UTF-8 bytes projected from one run policy text field.
pub const MAX_RUN_POLICY_TEXT_BYTES: usize = 4 * 1024;
