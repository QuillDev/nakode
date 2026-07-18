use std::path::PathBuf;

use serde_json::Value;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};

pub const CODEX_PROVIDER: &str = "openai-codex";
pub const DEVIN_PROVIDER: &str = "devin-acp";

/// Features declared by the active provider adapter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CapabilitySupport {
    #[default]
    Unsupported,
    Supported,
}

impl CapabilitySupport {
    #[must_use]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackendCapabilities {
    pub resume: CapabilitySupport,
    pub steering: CapabilitySupport,
    pub interruption: CapabilitySupport,
    pub model_catalog: CapabilitySupport,
    pub models_require_session: CapabilitySupport,
    pub session_model_config: CapabilitySupport,
    pub approvals: CapabilitySupport,
    pub native_tools: CapabilitySupport,
    pub mcp: CapabilitySupport,
    pub close_session: CapabilitySupport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendIdentity {
    pub provider: String,
    pub display_name: String,
    pub version: Option<String>,
    pub capabilities: BackendCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelInfo {
    pub provider: String,
    pub id: String,
    pub is_default: bool,
}

impl ModelInfo {
    #[must_use]
    pub fn qualified_id(&self) -> String {
        format!("{}/{}", self.provider, self.id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemKind {
    User,
    Assistant,
    Reasoning,
    Tool,
    Diff,
    System,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemStatus {
    Running,
    Complete,
    Failed,
    Declined,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedItem {
    pub id: String,
    pub kind: ItemKind,
    pub title: String,
    pub body: String,
    pub status: ItemStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaKind {
    Assistant,
    Plan,
    Reasoning,
    Tool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalKind {
    Command,
    FileChange,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDecision {
    AcceptOnce,
    AcceptForSession,
    Decline,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalRequest {
    pub id: Value,
    pub method: String,
    pub kind: ApprovalKind,
    pub title: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionHistoryItem {
    pub turn_id: String,
    pub item: NormalizedItem,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendOperation {
    Initialize,
    ModelList,
    Reload,
    SetSessionModel,
    StartSession,
    ResumeSession,
    UnsubscribeSession,
    StartTurn,
    SteerTurn,
    InterruptTurn,
}

impl BackendOperation {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Initialize => "initialize backend",
            Self::ModelList => "list models",
            Self::Reload => "reload backend metadata",
            Self::SetSessionModel => "set session model",
            Self::StartSession => "start session",
            Self::ResumeSession => "resume session",
            Self::UnsubscribeSession => "close session",
            Self::StartTurn => "start turn",
            Self::SteerTurn => "steer turn",
            Self::InterruptTurn => "interrupt turn",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BackendEvent {
    Ready(BackendIdentity),
    Models(Vec<ModelInfo>),
    SessionCreated {
        provider_session_id: String,
        model: String,
    },
    SessionResumed {
        provider_session_id: String,
        model: String,
        history: Vec<SessionHistoryItem>,
    },
    SessionUnsubscribed,
    SessionObserved {
        provider_session_id: String,
    },
    TurnAccepted {
        turn_id: String,
    },
    TurnStarted {
        turn_id: String,
    },
    TurnCompleted {
        turn_id: String,
        outcome: TurnOutcome,
        error: Option<String>,
    },
    ItemStarted {
        turn_id: String,
        item: NormalizedItem,
    },
    ItemCompleted {
        turn_id: String,
        item: NormalizedItem,
    },
    ItemDelta {
        turn_id: String,
        item_id: String,
        kind: DeltaKind,
        delta: String,
    },
    TurnDiff {
        turn_id: String,
        diff: String,
    },
    TurnPlan {
        turn_id: String,
        plan: String,
    },
    ApprovalRequested(ApprovalRequest),
    ApprovalResolved {
        request_id: Value,
    },
    SteerAccepted {
        turn_id: String,
    },
    InterruptAccepted,
    ModelRerouted {
        turn_id: String,
        from: String,
        to: String,
    },
    Warning(String),
    TurnError {
        turn_id: String,
        message: String,
        will_retry: bool,
    },
    RequestFailed {
        operation: BackendOperation,
        code: i64,
        message: String,
    },
    ProtocolDiagnostic(String),
    SessionClosed {
        provider_session_id: String,
    },
    Disconnected {
        reason: String,
    },
}

/// Provider-neutral commands understood by an agent backend adapter.
#[derive(Clone, Debug)]
pub enum BackendCommand {
    StartSession {
        model: Option<String>,
        instructions: Option<String>,
    },
    ResumeSession {
        provider_session_id: String,
    },
    UnsubscribeSession {
        provider_session_id: String,
    },
    StartTurn {
        session_id: String,
        client_id: String,
        prompt: String,
        model: Option<String>,
    },
    SteerTurn {
        session_id: String,
        turn_id: String,
        client_id: String,
        prompt: String,
    },
    InterruptTurn {
        session_id: String,
        turn_id: String,
    },
    SetSessionModel {
        session_id: String,
        model: String,
    },
    Reload {
        session_id: Option<String>,
    },
    ResolveApproval {
        id: Value,
        decision: ApprovalDecision,
    },
    Shutdown,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("unsupported provider {provider}")]
    UnsupportedProvider { provider: String },
    #[error("provider {provider} is not enabled for new work")]
    ProviderUnavailable { provider: String },
    #[error("failed to launch {backend} at {program}: {source}")]
    Spawn {
        backend: &'static str,
        program: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{backend} child did not expose piped {pipe}")]
    MissingPipe {
        backend: &'static str,
        pipe: &'static str,
    },
    #[error("failed to write {backend} initialization request: {source}")]
    InitializeWrite {
        backend: &'static str,
        #[source]
        source: std::io::Error,
    },
}

/// Running provider adapter with a uniform command/event boundary.
pub struct BackendHandle {
    pub commands: mpsc::Sender<BackendCommand>,
    pub events: mpsc::Receiver<BackendEvent>,
    task: JoinHandle<()>,
}

impl BackendHandle {
    pub(crate) fn new(
        commands: mpsc::Sender<BackendCommand>,
        events: mpsc::Receiver<BackendEvent>,
        task: JoinHandle<()>,
    ) -> Self {
        Self {
            commands,
            events,
            task,
        }
    }

    pub async fn join(self) {
        let _ = self.task.await;
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        mpsc::Sender<BackendCommand>,
        mpsc::Receiver<BackendEvent>,
        JoinHandle<()>,
    ) {
        (self.commands, self.events, self.task)
    }
}
