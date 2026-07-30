//! Headless, server-owned Nakode domain engine.
//!
//! This module is deliberately independent of terminal and rendering types. It
//! is the ownership boundary that transports will drive as the remaining
//! effect executors move out of the legacy TUI application loop.

use std::ops::{Deref, DerefMut};

use tokio::sync::broadcast;

use crate::{
    backend::BackendEvent,
    state::{AgentRequest, AppState, Effect},
};

const EVENT_BUFFER: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSnapshot {
    pub revision: u64,
    pub session_id: String,
    pub logical_session_id: Option<String>,
    pub status: String,
    pub busy: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceEvent {
    StateChanged { revision: u64 },
}

/// Owns canonical Nakode state without depending on a particular client.
pub struct ServiceEngine {
    state: AppState,
    revision: u64,
    events: broadcast::Sender<ServiceEvent>,
}

impl ServiceEngine {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        let (events, _) = broadcast::channel(EVENT_BUFFER);
        Self {
            state,
            revision: 1,
            events,
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
    pub fn snapshot(&self) -> ServiceSnapshot {
        ServiceSnapshot {
            revision: self.revision,
            session_id: self.state.nakode_session_id.clone(),
            logical_session_id: self.state.session_id.clone(),
            status: self.state.status_message.clone(),
            busy: self.state.is_busy(),
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ServiceEvent> {
        self.events.subscribe()
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
        self.revision = self.revision.wrapping_add(1);
        let _ = self.events.send(ServiceEvent::StateChanged {
            revision: self.revision,
        });
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
        state::AppState,
    };

    use super::{ServiceEngine, ServiceEvent};

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
    }
}
