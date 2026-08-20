use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    ArtifactView, BootstrapView, McpManagementView, RunId, RunTextWindow, RunView, ServerEpoch,
    SessionId, SessionSummary, SessionView, TranscriptBodyWindow, WorkspaceId,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceCapability {
    Subscriptions,
    MultipleClients,
    ArtifactTransfer,
    ExternalTools,
    /// `CreateSession` and `OpenSession` atomically install client-owned tools before provider work.
    InitialSessionTools,
    /// Session tool configuration supports canonical builtin allowlists and provider projection validation.
    BuiltinToolAllowlists,
    /// Session creation persists a filesystem/provider root independent from logical workspace ownership.
    SessionWorkingDirectories,
    /// `CreateSession` can validate and apply an initial model/options before publication.
    InitialSessionModel,
    /// `CreateSession` accepts bounded client context merged into provider system instructions.
    InitialSessionInstructions,
    /// `DeleteSession` is served: a logical session and its persisted history can be removed.
    ///
    /// Declared rather than assumed so a client can degrade its own affordance instead of offering a
    /// delete that an older server answers with `Unimplemented`.
    SessionDeletion,
    /// Structured, atomic per-question answers, including free text.
    QuestionTextAnswers,
    /// `SteerQueuedPrompt` atomically redirects active work to a server-owned follow-up.
    QueuedPromptSteering,
    /// Owner-facing clients may inspect and atomically mutate the authoritative archetype catalogue.
    ArchetypeManagement,
    /// Owner-facing clients may inspect and atomically mutate Nakode's one configured SOUL.md.
    SoulManagement,
    /// Nakode owns MCP configuration, credentials, discovery, grants, invocation, and audit.
    McpManagement,
    /// Typed Chat/Agent external-thread intent, lifecycle, binding, and delivery checkpoints.
    OrchestratorThreadBridge,
    /// Redacted Discord configuration/status plus write-only credential and transport controls.
    DiscordManagement,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceCapabilities {
    #[serde(default)]
    pub supported: BTreeSet<ServiceCapability>,
}

impl ServiceCapabilities {
    #[must_use]
    pub fn supports(&self, capability: ServiceCapability) -> bool {
        self.supported.contains(&capability)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor {
    pub server_epoch: ServerEpoch,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Snapshot<Value> {
    pub cursor: Cursor,
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum SubscriptionScope {
    Workspace { workspace_id: WorkspaceId },
    Session { session_id: SessionId },
    Run { run_id: RunId },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SoulDocumentView {
    pub workspace_id: WorkspaceId,
    pub content: String,
    pub path: String,
    pub source: String,
    pub exists: bool,
    pub digest: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeContinuationDisposition {
    /// The event atomically started the next provider-neutral user turn.
    Accepted,
    /// This external event was already handled and did not mutate the session again.
    Duplicate,
    /// The event was durably rejected because the session was not ready for a new turn.
    Busy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandAccepted {
    pub resource_id: Option<String>,
    pub revision: Option<u64>,
    /// Present only for `ContinueSessionFromBridge`; retained in idempotency replay results.
    #[serde(default)]
    pub bridge_continuation: Option<BridgeContinuationDisposition>,
    /// Original Accepted/Busy result when `bridge_continuation` is Duplicate. This lets a transport
    /// restore the same reaction after a lost response without inferring from mutable turn state.
    #[serde(default)]
    pub replayed_bridge_continuation: Option<BridgeContinuationDisposition>,
    /// Whether the replayed Accepted message still owns live/final reaction routing.
    #[serde(default)]
    pub replayed_bridge_source_active: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum QueryResult {
    Bootstrap(Box<BootstrapView>),
    SoulDocument(SoulDocumentView),
    McpManagement(McpManagementView),
    Sessions(Vec<SessionSummary>),
    Session(Box<SessionView>),
    Transcript(crate::TranscriptPage),
    TranscriptBody(TranscriptBodyWindow),
    Run(Box<RunView>),
    Runs(crate::RunPage),
    RunText(RunTextWindow),
    Artifact(ArtifactView),
    Diagnostics(Box<crate::DiagnosticsReport>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope", content = "value", rename_all = "snake_case")]
pub enum SubscriptionView {
    Workspace(Box<BootstrapView>),
    Session(Box<SessionView>),
    Run(Box<RunView>),
}
