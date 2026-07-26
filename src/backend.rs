use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};

pub const CODEX_PROVIDER: &str = "openai-codex";
pub const DEVIN_PROVIDER: &str = "devin-acp";
pub const CURSOR_PROVIDER: &str = "cursor-sdk";

pub(crate) async fn request_failed(
    events: &mpsc::Sender<BackendEvent>,
    operation: BackendOperation,
    message: impl Into<String>,
) {
    let _ = events
        .send(BackendEvent::RequestFailed {
            operation,
            code: -1,
            message: message.into(),
        })
        .await;
}

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
    pub context_compaction: CapabilitySupport,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModelOptions {
    pub reasoning_effort: Option<String>,
    pub fast_mode: bool,
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

    #[must_use]
    pub fn display_name(&self) -> String {
        display_model_name(&self.provider, &self.id)
    }
}

/// Converts a provider model identifier into a human-readable display name while
/// preserving the original identifier for all provider requests and persistence.
#[must_use]
pub fn display_model_name(provider: &str, model: &str) -> String {
    let parts = model
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if provider == DEVIN_PROVIDER
        && parts.len() >= 3
        && parts[0].eq_ignore_ascii_case("swe")
        && parts[1].chars().all(|character| character.is_ascii_digit())
        && parts[2].chars().all(|character| character.is_ascii_digit())
    {
        let mut display = vec!["SWE".to_owned(), format!("{}.{}", parts[1], parts[2])];
        display.extend(
            parts[3..]
                .iter()
                .map(|part| display_model_part(provider, part)),
        );
        return display.join(" ");
    }
    parts
        .into_iter()
        .map(|part| display_model_part(provider, part))
        .collect::<Vec<_>>()
        .join(" ")
}

#[must_use]
pub fn display_qualified_model_name(qualified: &str) -> String {
    qualified.split_once('/').map_or_else(
        || display_model_name("", qualified),
        |(provider, model)| display_model_name(provider, model),
    )
}

fn display_model_part(provider: &str, part: &str) -> String {
    let lower = part.to_ascii_lowercase();
    match (provider, lower.as_str()) {
        (_, "gpt") => "GPT".to_owned(),
        (DEVIN_PROVIDER, "swe") => "SWE".to_owned(),
        (_, "ai") => "AI".to_owned(),
        _ if part
            .chars()
            .all(|character| character.is_ascii_digit() || character == '.') =>
        {
            part.to_owned()
        }
        _ if lower.starts_with('o')
            && lower[1..]
                .chars()
                .all(|character| character.is_ascii_digit()) =>
        {
            lower
        }
        _ => {
            let mut characters = lower.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(characters).collect()
            })
        }
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
    ReasoningSummary { index: usize },
    Tool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    Manual,
    Proactive,
    ContextOverflow,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionOption {
    pub label: String,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionRequest {
    pub id: String,
    pub title: String,
    pub question: String,
    pub options: Vec<QuestionOption>,
    pub multi: bool,
    pub recommended: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TodoItem {
    pub content: String,
    pub status: TodoStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TodoPhase {
    pub name: String,
    pub tasks: Vec<TodoItem>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
    Abandoned,
}

impl TodoStatus {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in progress",
            Self::Completed => "completed",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionHistoryItem {
    pub turn_id: String,
    pub item: NormalizedItem,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendOperation {
    Initialize,
    Authenticate,
    ModelList,
    Reload,
    SetSessionModel,
    StartSession,
    ResumeSession,
    UnsubscribeSession,
    CompactSession,
    StartTurn,
    SteerTurn,
    InterruptTurn,
}

impl BackendOperation {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Initialize => "initialize backend",
            Self::Authenticate => "authenticate provider",
            Self::ModelList => "list models",
            Self::Reload => "reload backend metadata",
            Self::SetSessionModel => "set session model",
            Self::StartSession => "start session",
            Self::ResumeSession => "resume session",
            Self::UnsubscribeSession => "close session",
            Self::CompactSession => "compact session context",
            Self::StartTurn => "start turn",
            Self::SteerTurn => "steer turn",
            Self::InterruptTurn => "interrupt turn",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BackendEvent {
    Ready(BackendIdentity),
    AuthenticationChallenge {
        login_id: String,
        verification_url: String,
        user_code: String,
    },
    AuthenticationCompleted {
        kind: String,
        metadata: Value,
    },
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
    TodoUpdated {
        phases: Vec<TodoPhase>,
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
    ContextUsageUpdated {
        estimated_tokens: usize,
        context_window: Option<usize>,
    },
    ContextCompactionStarted {
        compaction_id: String,
        turn_id: String,
        reason: CompactionReason,
        estimated_tokens: usize,
        context_window: Option<usize>,
    },
    ContextCompactionCompleted {
        compaction_id: String,
        turn_id: String,
        estimated_tokens_before: usize,
        estimated_tokens_after: usize,
    },
    ContextCompactionFailed {
        compaction_id: String,
        turn_id: String,
        message: String,
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
    QuestionRequested(QuestionRequest),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromptAttachment {
    pub label: String,
    pub path: Option<PathBuf>,
    pub image: Option<PromptImage>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromptImage {
    pub mime_type: String,
    pub data: Vec<u8>,
}

/// Provider-neutral commands understood by an agent backend adapter.
#[derive(Clone, Debug)]
pub enum BackendCommand {
    BeginAuthentication,
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
        provider_session_id: String,
        client_id: String,
        prompt: String,
        attachments: Vec<PromptAttachment>,
        model: Option<String>,
    },
    SteerTurn {
        provider_session_id: String,
        turn_id: String,
        client_id: String,
        prompt: String,
    },
    InterruptTurn {
        provider_session_id: String,
        turn_id: String,
    },
    CompactSession {
        provider_session_id: String,
        compaction_id: String,
    },
    SetSessionModel {
        provider_session_id: String,
        model: String,
    },
    SetSessionOptions {
        provider_session_id: String,
        options: ModelOptions,
    },
    Reload {
        provider_session_id: Option<String>,
    },
    ResolveApproval {
        id: Value,
        decision: ApprovalDecision,
    },
    ResolveQuestion {
        id: String,
        answer: String,
    },
    Shutdown,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("unsupported provider {provider}")]
    UnsupportedProvider { provider: String },
    #[error("provider {provider} is not enabled for new work")]
    ProviderUnavailable { provider: String },
    #[error(
        "provider {provider} is temporarily unavailable for {remaining_seconds}s after: {reason}"
    )]
    ProviderCoolingDown {
        provider: String,
        remaining_seconds: u64,
        reason: String,
    },
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
    #[error("failed to prepare {provider} credential store at {path}: {source}")]
    CredentialStore {
        provider: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("stored credential for {provider} is invalid: {detail}")]
    InvalidCredential { provider: String, detail: String },
    #[error("failed to prepare {provider} SDK bridge: {detail}")]
    BridgeSetup { provider: String, detail: String },
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

    /// Waits for the provider supervisor to exit.
    ///
    /// # Errors
    ///
    /// Returns the supervisor task's cancellation or panic error.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.task.await
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

#[cfg(test)]
mod tests {
    use super::{CODEX_PROVIDER, DEVIN_PROVIDER, display_model_name};

    #[test]
    fn model_ids_are_normalized_for_display_without_changing_provider_ids() {
        assert_eq!(
            display_model_name(CODEX_PROVIDER, "gpt-5.6-sol"),
            "GPT 5.6 Sol"
        );
        assert_eq!(
            display_model_name(CODEX_PROVIDER, "gpt-5.1-codex-mini"),
            "GPT 5.1 Codex Mini"
        );
        assert_eq!(
            display_model_name(DEVIN_PROVIDER, "swe-1-6-fast"),
            "SWE 1.6 Fast"
        );
        assert_eq!(display_model_name(CODEX_PROVIDER, "o3"), "o3");
    }
}
