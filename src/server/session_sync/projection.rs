//! Diff the bounded projection, not invalidation events (which need not describe every field).
use super::{
    Instant, ServerCore, ServiceError, SessionChanges, SessionHistories, SessionId, SessionView,
};
use crate::server::{PendingPublication, PublishedSessionProjection};
use nakode_protocol::{
    TranscriptChanges,
    sync::{Batch, Read, Reply},
};
use std::collections::HashMap;

pub(super) type Patch = (Box<SessionView>, TranscriptChanges);

impl SessionHistories {
    pub(in crate::server) fn publish(
        &mut self,
        previous: Option<&PublishedSessionProjection>,
        current: &PublishedSessionProjection,
        events: &[PendingPublication],
    ) {
        let current = &current.view;
        if !self.observes(&current.id) {
            return;
        }
        let Some(previous) = previous.map(|p| &p.view) else {
            self.remove(&current.id);
            return;
        };
        if previous == current {
            return;
        }
        let old = &previous.transcript.entries;
        let entries = &current.transcript.entries;
        let changed_order = !old.iter().map(|e| &e.id).eq(entries.iter().map(|e| &e.id));
        let by_id: HashMap<_, _> = old.iter().map(|e| (&e.id, e)).collect();
        let mut projection = Box::new(current.clone());
        projection
            .transcript
            .entries
            .retain(|entry| by_id.get(&entry.id).copied() != Some(entry));
        // The active owner agent and logical session share one canonical transcript projection.
        if let Some(agent) = &mut projection.active_agent_session {
            agent.transcript.entries.clear();
        }
        let changes = TranscriptChanges {
            entry_ids: changed_order.then(|| entries.iter().map(|e| e.id.clone()).collect()),
        };
        let events = if self.sessions[&current.id].native_events {
            events.iter().map(|p| &p.event).collect()
        } else {
            Vec::new()
        };
        self.record_patch(
            &current.id,
            Some(previous.revision),
            current.revision,
            &(&events, Some((projection, changes))),
            Instant::now(),
        );
    }
}

impl ServerCore {
    pub(in crate::server) fn read_session_sync(
        &mut self,
        id: &SessionId,
        read: Read,
    ) -> Result<Reply, ServiceError> {
        self.ensure_session(id)
            .map_err(crate::server::domain_error)?;
        match read {
            Read::Snapshot => {
                let snapshot = self.projection_snapshot(id)?;
                Ok(Reply::Snapshot(snapshot.cursor, Box::new(snapshot.session)))
            }
            Read::After(Some(cursor)) if &cursor.session_id == id => {
                let SessionChanges::Changes { batches, .. } = self.session_sync_changes(&cursor)?
                else {
                    return Ok(Reply::Reset);
                };
                let batches = batches
                    .into_iter()
                    .map(|b| {
                        let (projection, changes) = b.projection?;
                        Some(Batch {
                            base: b.base,
                            next: b.next,
                            projection,
                            changes,
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                Ok(batches.map_or(Reply::Reset, Reply::Changes))
            }
            Read::After(_) => Ok(Reply::Reset),
        }
    }
}
