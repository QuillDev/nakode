//! Scalar status inventory. Never bootstrap sessions, read transcripts, or restore providers here.
use std::collections::{BTreeMap, HashSet};

use nakode_protocol::{SessionActivity, SessionId, SessionStatusInventory, SessionStatusSummary};

use super::ServerCore;

impl ServerCore {
    pub(super) fn session_statuses(&self, limit: u32) -> SessionStatusInventory {
        let mut sessions: BTreeMap<_, _> = self
            .sessions
            .iter()
            .map(|record| {
                let id = SessionId::from(record.id.clone());
                (
                    id.clone(),
                    SessionStatusSummary {
                        id,
                        revision: 0,
                        activity: SessionActivity::Idle,
                        owner_turn_running: false,
                        has_interactions: false,
                        has_failure: false,
                    },
                )
            })
            .collect();
        let initial_is_persisted = sessions.contains_key(&self.default_session);
        let mut successors = HashSet::new();
        for (id, engine) in &self.sessions_by_id {
            // The unpersisted initial engine hosts the control plane, not a logical conversation.
            if *id == self.default_session && !initial_is_persisted {
                continue;
            }
            let mut status =
                crate::state::projection::session_status(engine.state(), engine.revision());
            if status.id != *id {
                successors.insert(status.id.clone());
            }
            status.id = id.clone();
            sessions.insert(id.clone(), status);
        }
        for id in successors {
            sessions.remove(&id);
        }
        let limit = usize::try_from(limit).unwrap_or(usize::MAX).min(500);
        SessionStatusInventory {
            complete: self.session_inventory_complete && sessions.len() <= limit,
            sessions: sessions.into_values().take(limit).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{service::ServiceEngine, state::DomainState};

    #[test]
    fn compact_status_matches_full_projection_without_copying_detail() {
        use crate::backend::{
            ApprovalKind, ApprovalRequest, BackendFailureClassification, BackendFailureDetail,
            BackendFailurePhase, CompactionReason,
        };
        use crate::state::{ActiveTurn, ContextCompactionState, SessionFailureState};
        let mut state = DomainState::new("/workspace", None, 100);
        state.backend_provider = "provider".to_owned();
        let compare = |state: &DomainState| {
            let compact = crate::state::projection::session_status(state, 7);
            let engine = ServiceEngine::new(state.clone());
            let full = engine
                .bootstrap_view(&[], &[])
                .active_session
                .expect("active session");
            assert_eq!(compact.activity, full.activity);
            assert_eq!(compact.owner_turn_running, full.active_turn.is_some());
            assert_eq!(compact.has_interactions, !full.interactions.is_empty());
            assert_eq!(compact.has_failure, full.failure.is_some());
            assert_eq!(compact.revision, 7);
            compact
        };
        assert!(!compare(&state).owner_turn_running);
        state.active_turn = Some(ActiveTurn {
            id: "turn".to_owned(),
            model: None,
            options: crate::backend::ModelOptions::default(),
            cancelling: false,
        });
        assert!(compare(&state).owner_turn_running);
        state.active_turn.as_mut().expect("turn").cancelling = true;
        assert!(compare(&state).owner_turn_running);
        state.active_turn = None;
        state.context_compaction = Some(ContextCompactionState {
            id: "compact".to_owned(),
            turn_id: "turn".to_owned(),
            reason: CompactionReason::Manual,
            estimated_tokens: 100,
            context_window: None,
        });
        let compacting = compare(&state);
        assert_eq!(compacting.activity, SessionActivity::CompactingContext);
        assert!(!compacting.owner_turn_running);
        state.approvals.push_back(ApprovalRequest {
            id: serde_json::json!("approval"),
            method: "test".to_owned(),
            kind: ApprovalKind::Command,
            title: "private question".to_owned(),
            detail: "private detail".to_owned(),
        });
        assert!(compare(&state).has_interactions);
        state.latest_failure = Some(SessionFailureState {
            initial_start: false,
            detail: BackendFailureDetail {
                phase: BackendFailurePhase::Unknown,
                classification: BackendFailureClassification::Unknown,
                summary: "private failure".to_owned(),
                operation: "test".to_owned(),
                safe_endpoint: None,
                http_status: None,
                source_chain: Vec::new(),
                correlation_id: None,
            },
        });
        let status = compare(&state);
        assert!(status.has_failure);
        let encoded = serde_json::to_string(&status).expect("scalar status JSON");
        assert!(!encoded.contains("private"));
    }

    #[test]
    fn status_inventory_excludes_control_plane_and_preserves_completeness() {
        let mut core = ServerCore::new(
            ServiceEngine::new(DomainState::new("/workspace", None, 100)),
            Vec::new(),
            Vec::new(),
        );
        assert!(core.session_statuses(500).sessions.is_empty());
        assert!(core.session_statuses(500).complete);
        let id = SessionId::from("logical-session");
        let mut state = DomainState::new("/workspace", None, 100);
        state.nakode_session_id = id.to_string();
        core.sessions_by_id
            .insert(id.clone(), ServiceEngine::new(state));
        let inventory = core.session_statuses(500);
        assert!(inventory.complete);
        assert_eq!(inventory.sessions.len(), 1);
        assert_eq!(inventory.sessions[0].id, id);
        assert_eq!(inventory.sessions[0].activity, SessionActivity::Idle);
        assert!(!inventory.sessions[0].owner_turn_running);
        assert!(!core.session_statuses(0).complete);
        core.set_session_inventory_complete(false);
        assert!(!core.session_statuses(500).complete);
    }

    #[test]
    fn status_inventory_is_sorted_and_capped_without_hydrating_sessions() {
        let mut core = ServerCore::new(
            ServiceEngine::new(DomainState::new("/workspace", None, 100)),
            Vec::new(),
            Vec::new(),
        );
        for index in (0..501).rev() {
            let id = format!("session-{index:04}");
            let mut state = DomainState::new("/workspace", None, 100);
            state.nakode_session_id.clone_from(&id);
            core.sessions_by_id
                .insert(SessionId::from(id), ServiceEngine::new(state));
        }
        let inventory = core.session_statuses(u32::MAX);
        assert!(!inventory.complete);
        assert_eq!(inventory.sessions.len(), 500);
        assert_eq!(inventory.sessions[0].id.as_str(), "session-0000");
        assert_eq!(inventory.sessions[499].id.as_str(), "session-0499");
    }
}
