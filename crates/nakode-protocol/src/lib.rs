//! Frontend-neutral protocol shared by Nakode servers and clients.
//!
//! This crate intentionally contains only semantic data-transfer types. It has
//! no dependency on a terminal, renderer, provider adapter, process runtime, or
//! persistence implementation.

mod command;
mod error;
mod frame;
mod ids;
mod view;

pub use command::{
    AgentDefinitionInput, Command, CredentialInput, InteractionResolution, ModelOptions,
    ModelTarget, PromptAttachment, PromptInput, Query, SettingsPatch,
};
pub use error::{ErrorCode, ServiceError};
pub use frame::{
    ClientDescriptor, ClientFrame, CommandAccepted, Cursor, PROTOCOL_VERSION, QueryResult,
    ServerFrame, ServiceCapabilities, ServiceCapability, Snapshot, SubscriptionScope,
    SubscriptionView, VersionRange,
};
pub use ids::{
    AgentSessionId, ArtifactId, ClientId, EntryId, IdempotencyKey, InteractionId, ModelId,
    PromptId, ProviderId, RequestId, RunId, ServerEpoch, SessionId, SubscriptionId, TurnId,
    WorkspaceId,
};
pub use view::{
    AgentBrowserView, AgentDefinitionView, AgentSessionView, ArtifactView, BootstrapView,
    ConnectionView, ContextUsageView, InteractionKind, InteractionOptionView, InteractionStatus,
    InteractionView, MemorySettingsView, ModelView, NoticeLevel, NoticeView,
    ProviderAuthenticationView, ProviderCapabilities, ProviderCapability, ProviderView,
    QueueItemView, RunStatus, RunView, SessionActivity, SessionSummary, SessionView, SettingsView,
    SkillView, TerminalImageModeView, TodoItemView, TodoPhaseView, TodoStatusView,
    TranscriptEntryKind, TranscriptEntryStatus, TranscriptEntryView, TranscriptPage, TurnStatus,
    TurnView, ViewEvent, VisionSettingsView, WebSettingsView,
};
