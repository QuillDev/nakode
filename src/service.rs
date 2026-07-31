//! Headless, server-owned Nakode domain engine.
//!
//! This module is deliberately independent of terminal and rendering types. It
//! is the ownership boundary that transports will drive as the remaining
//! effect executors move out of the legacy TUI application loop.

use std::{
    collections::{HashMap, VecDeque},
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{
    backend::BackendEvent,
    service_protocol::{ClientCommand, ClientRequest, CommandResult, ProtocolError},
    session::{ProviderRecord, SessionRecord},
    state::{AgentRequest, AppState, Effect, projection},
    transcript::{EntryKind, EntryStatus},
};

const EVENT_BUFFER: usize = 256;
const IDEMPOTENCY_BUFFER: usize = 1_024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceSnapshot {
    pub revision: u64,
    pub session_id: String,
    pub logical_session_id: Option<String>,
    pub workspace: String,
    pub provider: String,
    pub provider_name: String,
    pub selected_model: Option<String>,
    pub status: String,
    pub busy: bool,
    pub transcript: Vec<ServiceTranscriptEntry>,
    pub queued_prompts: Vec<String>,
    pub approval_pending: bool,
    pub question_pending: bool,
    pub subagent_count: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceTranscriptEntry {
    pub key: Option<String>,
    pub kind: String,
    pub title: String,
    pub body: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ServiceEvent {
    StateChanged { revision: u64 },
}

#[derive(Debug)]
pub struct ServiceSubscription {
    id: u64,
    receiver: broadcast::Receiver<ServiceEvent>,
}

impl ServiceSubscription {
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the next ordered event.
    ///
    /// # Errors
    /// Returns a lag error if this subscriber did not keep up with the bounded
    /// queue, or a closed error after engine shutdown.
    pub async fn recv(&mut self) -> Result<ServiceEvent, broadcast::error::RecvError> {
        self.receiver.recv().await
    }

    /// Receives one event without waiting.
    ///
    /// # Errors
    /// Returns empty, lagged, or closed according to the bounded subscriber
    /// queue state.
    pub fn try_recv(&mut self) -> Result<ServiceEvent, broadcast::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResumeError {
    #[error("revision {requested} is older than retained revision {oldest}")]
    SnapshotRequired { requested: u64, oldest: u64 },
    #[error("revision {requested} is newer than current revision {current}")]
    FutureRevision { requested: u64, current: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommandExecutionError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("request id {request_id} was already used for a different command")]
    IdempotencyConflict { request_id: String },
}

#[derive(Clone, Debug)]
struct CachedCommand {
    command: ClientCommand,
    result: CommandResult,
}

/// Owns canonical Nakode state without depending on a particular client.
pub struct ServiceEngine {
    state: AppState,
    revision: u64,
    events: broadcast::Sender<ServiceEvent>,
    event_log: VecDeque<ServiceEvent>,
    next_subscription_id: u64,
    command_cache: HashMap<String, CachedCommand>,
    command_order: VecDeque<String>,
}

impl ServiceEngine {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        let (events, _) = broadcast::channel(EVENT_BUFFER);
        Self {
            state,
            revision: 1,
            events,
            event_log: VecDeque::with_capacity(EVENT_BUFFER),
            next_subscription_id: 1,
            command_cache: HashMap::new(),
            command_order: VecDeque::new(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> &AppState {
        &self.state
    }

    pub const fn state_mut(&mut self) -> &mut AppState {
        &mut self.state
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn bootstrap_view(
        &self,
        providers: &[ProviderRecord],
        sessions: &[SessionRecord],
    ) -> nakode_protocol::BootstrapView {
        projection::bootstrap(&self.state, self.revision, providers, sessions)
    }

    #[must_use]
    pub fn snapshot(&self) -> ServiceSnapshot {
        ServiceSnapshot {
            revision: self.revision,
            session_id: self.state.nakode_session_id.clone(),
            logical_session_id: self.state.session_id.clone(),
            workspace: self.state.workspace.clone(),
            provider: self.state.backend_provider.clone(),
            provider_name: self.state.backend_name.clone(),
            selected_model: self.state.selected_model.clone(),
            status: self.state.status_message.clone(),
            busy: self.state.is_busy(),
            transcript: self
                .state
                .transcript
                .entries()
                .iter()
                .map(|entry| ServiceTranscriptEntry {
                    key: entry.key.clone(),
                    kind: entry_kind(entry.kind).to_owned(),
                    title: entry.title.clone(),
                    body: entry.body.clone(),
                    status: entry_status(entry.status).to_owned(),
                })
                .collect(),
            queued_prompts: self
                .state
                .queue
                .iter()
                .map(|prompt| prompt.text.clone())
                .collect(),
            approval_pending: !self.state.approvals.is_empty(),
            question_pending: !self.state.questions.is_empty(),
            subagent_count: self.state.subagents.len(),
        }
    }

    #[must_use]
    pub fn subscribe(&mut self) -> ServiceSubscription {
        let id = self.next_subscription_id;
        self.next_subscription_id = self.next_subscription_id.wrapping_add(1);
        ServiceSubscription {
            id,
            receiver: self.events.subscribe(),
        }
    }

    pub fn unsubscribe(&self, subscription: ServiceSubscription) {
        drop(subscription);
    }

    /// Returns events after `revision`, or requires a fresh snapshot when the
    /// bounded history can no longer satisfy the resume request.
    ///
    /// # Errors
    /// Returns [`ResumeError`] when the revision is outside the retained range.
    pub fn resume_from(&self, revision: u64) -> Result<Vec<ServiceEvent>, ResumeError> {
        if revision > self.revision {
            return Err(ResumeError::FutureRevision {
                requested: revision,
                current: self.revision,
            });
        }
        let oldest = self
            .event_log
            .front()
            .map_or(self.revision, ServiceEvent::revision);
        if revision.saturating_add(1) < oldest {
            return Err(ResumeError::SnapshotRequired {
                requested: revision,
                oldest,
            });
        }
        Ok(self
            .event_log
            .iter()
            .filter(|event| event.revision() > revision)
            .cloned()
            .collect())
    }

    /// Executes a request at most once for a given request identifier.
    ///
    /// # Errors
    /// Rejects invalid protocol versions and reuse of an identifier with a
    /// different command.
    pub fn execute_idempotent(
        &mut self,
        request: &ClientRequest,
        execute: impl FnOnce(&ClientCommand) -> CommandResult,
    ) -> Result<CommandResult, CommandExecutionError> {
        request.validate()?;
        if let Some(cached) = self.command_cache.get(&request.request_id) {
            if cached.command == request.command {
                return Ok(cached.result.clone());
            }
            return Err(CommandExecutionError::IdempotencyConflict {
                request_id: request.request_id.clone(),
            });
        }
        let result = execute(&request.command);
        self.command_cache.insert(
            request.request_id.clone(),
            CachedCommand {
                command: request.command.clone(),
                result: result.clone(),
            },
        );
        self.command_order.push_back(request.request_id.clone());
        if self.command_order.len() > IDEMPOTENCY_BUFFER
            && let Some(expired) = self.command_order.pop_front()
        {
            self.command_cache.remove(&expired);
        }
        Ok(result)
    }

    pub fn invoke_agent(&mut self, request: &AgentRequest) -> Vec<Effect> {
        let effects = self.state.invoke_agent(request);
        self.changed();
        effects
    }

    pub fn handle_provider_backend(&mut self, provider: &str, event: BackendEvent) -> Vec<Effect> {
        let effects = self.state.handle_provider_backend(provider, event);
        self.changed();
        effects
    }

    pub fn handle_subagent_backend(&mut self, run_id: &str, event: BackendEvent) -> Vec<Effect> {
        let effects = self.state.handle_subagent_backend(run_id, event);
        self.changed();
        effects
    }

    pub fn note_state_change(&mut self) {
        self.changed();
    }

    #[must_use]
    pub fn into_state(self) -> AppState {
        self.state
    }

    fn changed(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("service revision overflow");
        let event = ServiceEvent::StateChanged {
            revision: self.revision,
        };
        self.event_log.push_back(event.clone());
        if self.event_log.len() > EVENT_BUFFER {
            self.event_log.pop_front();
        }
        let _ = self.events.send(event);
    }
}

impl ServiceEvent {
    #[must_use]
    pub const fn revision(&self) -> u64 {
        match self {
            Self::StateChanged { revision } => *revision,
        }
    }
}

const fn entry_kind(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::System => "system",
        EntryKind::User => "user",
        EntryKind::Assistant => "assistant",
        EntryKind::Steering => "steering",
        EntryKind::Reasoning => "reasoning",
        EntryKind::Tool => "tool",
        EntryKind::Diff => "diff",
        EntryKind::Warning => "warning",
        EntryKind::Error => "error",
    }
}

const fn entry_status(status: EntryStatus) -> &'static str {
    match status {
        EntryStatus::Running => "running",
        EntryStatus::Complete => "complete",
        EntryStatus::Failed => "failed",
        EntryStatus::Interrupted => "interrupted",
    }
}

impl Deref for ServiceEngine {
    type Target = AppState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for ServiceEngine {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{BackendCapabilities, BackendEvent, BackendIdentity},
        service_protocol::{
            AgentInvocation, AgentResponse, ClientCommand, ClientRequest, CommandResult,
        },
        state::AppState,
    };

    use super::{CommandExecutionError, ResumeError, ServiceEngine, ServiceEvent};

    #[tokio::test]
    async fn headless_engine_publishes_ordered_revisions() {
        let state = AppState::new("/tmp/project", None, 100);
        let mut engine = ServiceEngine::new(state);
        let mut events = engine.subscribe();
        let initial = engine.snapshot();

        engine.handle_provider_backend(
            "openai-codex",
            BackendEvent::Ready(BackendIdentity {
                provider: "openai-codex".to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );

        let ServiceEvent::StateChanged { revision } =
            events.recv().await.expect("state change event");
        assert_eq!(revision, initial.revision + 1);
        assert_eq!(engine.snapshot().revision, revision);
    }

    #[test]
    fn snapshot_contains_no_terminal_or_renderer_state() {
        let state = AppState::new("/tmp/project", None, 100);
        let engine = ServiceEngine::new(state);
        let snapshot = engine.snapshot();

        assert!(!snapshot.session_id.is_empty());
        assert_eq!(snapshot.logical_session_id, None);
        assert!(!snapshot.busy);
        assert!(snapshot.transcript.is_empty());
    }

    #[test]
    fn revisions_can_resume_and_require_snapshot_after_history_expires() {
        let state = AppState::new("/tmp/project", None, 100);
        let mut engine = ServiceEngine::new(state);
        let start = engine.snapshot().revision;
        engine.note_state_change();
        engine.note_state_change();
        assert_eq!(engine.resume_from(start).expect("retained events").len(), 2);

        for _ in 0..=super::EVENT_BUFFER {
            engine.note_state_change();
        }
        assert!(matches!(
            engine.resume_from(start),
            Err(ResumeError::SnapshotRequired { .. })
        ));
    }

    #[test]
    fn duplicate_commands_are_idempotent_and_conflicts_are_deterministic() {
        let state = AppState::new("/tmp/project", None, 100);
        let mut engine = ServiceEngine::new(state);
        let request = ClientRequest::new(ClientCommand::InvokeAgent(AgentInvocation {
            agent: "explorer".to_owned(),
            session_id: "session".to_owned(),
            task: "inspect".to_owned(),
        }));
        let expected = CommandResult::Agent(AgentResponse {
            success: true,
            result: "done".to_owned(),
        });
        let first = engine
            .execute_idempotent(&request, |_| expected.clone())
            .expect("first execution");
        let duplicate = engine
            .execute_idempotent(&request, |_| panic!("must not execute twice"))
            .expect("cached execution");
        assert_eq!(first, duplicate);

        let mut conflict = request.clone();
        conflict.command = ClientCommand::InvokeAgent(AgentInvocation {
            agent: "explorer".to_owned(),
            session_id: "session".to_owned(),
            task: "different".to_owned(),
        });
        assert!(matches!(
            engine.execute_idempotent(&conflict, |_| CommandResult::Accepted),
            Err(CommandExecutionError::IdempotencyConflict { .. })
        ));
    }

    #[tokio::test]
    async fn slow_subscriber_is_disconnected_from_bounded_queue() {
        let state = AppState::new("/tmp/project", None, 100);
        let mut engine = ServiceEngine::new(state);
        let mut subscription = engine.subscribe();
        for _ in 0..=super::EVENT_BUFFER {
            engine.note_state_change();
        }
        assert!(matches!(
            subscription.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
        engine.unsubscribe(subscription);
    }
}
