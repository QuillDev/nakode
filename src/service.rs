//! Server-owned state container for one logical Nakode session.
//!
//! Transport idempotency, subscriptions, replay, queries, and publications
//! belong to `ServerCore` and `nakode-server`. This type deliberately owns only
//! one session's canonical domain state and revision.

use std::ops::{Deref, DerefMut};

use crate::{
    session::{ProviderRecord, SessionRecord},
    state::{DomainState, projection},
};

/// Canonical state and revision for one server-managed logical session.
pub struct ServiceEngine {
    state: DomainState,
    revision: u64,
}

impl ServiceEngine {
    #[must_use]
    pub const fn new(state: DomainState) -> Self {
        Self { state, revision: 1 }
    }

    #[must_use]
    pub const fn state(&self) -> &DomainState {
        &self.state
    }

    pub const fn state_mut(&mut self) -> &mut DomainState {
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

    /// Advances the revision after one semantic state change.
    ///
    /// # Panics
    /// Panics if the service processes enough changes to overflow a `u64`.
    pub fn note_state_change(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("service revision overflow");
    }

    #[must_use]
    pub fn into_state(self) -> DomainState {
        self.state
    }
}

impl Deref for ServiceEngine {
    type Target = DomainState;

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
    use super::ServiceEngine;
    use crate::state::DomainState;

    #[test]
    fn revisions_advance_only_when_the_server_commits_a_change() {
        let mut engine = ServiceEngine::new(DomainState::new("/tmp/project", None, 100));

        assert_eq!(engine.revision(), 1);
        engine.note_state_change();
        assert_eq!(engine.revision(), 2);
    }

    #[test]
    fn production_domain_state_has_no_presentation_fields() {
        let source = include_str!("state.rs");
        let definition = source
            .split_once("pub struct DomainState {")
            .expect("DomainState definition")
            .1
            .split_once("/// Legacy test-only alias")
            .expect("end of DomainState definition")
            .0;
        let forbidden = [
            "ClientPresentationState",
            "EditorState",
            "ModelPicker",
            "SessionPicker",
            "ProviderPicker",
            "AgentPicker",
            "SettingsState",
            "ScreenPoint",
            "ScreenSnapshot",
            "TextSelection",
            "clipboard",
            "hit_region",
            "scroll_from_bottom",
        ];
        let mut previous = "";
        for line in definition.lines() {
            for name in &forbidden {
                if line.contains(*name) {
                    assert_eq!(
                        previous.trim(),
                        "#[cfg(test)]",
                        "{name} leaked into the production DomainState definition"
                    );
                }
            }
            if !line.trim().is_empty() {
                previous = line;
            }
        }
    }
}
