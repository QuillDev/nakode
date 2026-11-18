//! Bounded replay history for the public `SessionSyncV1` stream.
//!
//! Stores complete semantic publication batches, before SDK body/artifact hydration. JSON is
//! private in-memory storage, not a second public transport. The public transport is Protobuf.
//! A cursor is meaningful only for this server history incarnation and logical session.

use nakode_protocol::{SessionId, SessionView, ViewEvent};
use std::{
    collections::{HashMap, VecDeque},
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

use super::{ServerCore, ServiceError};

#[cfg(test)]
mod tests;

pub use nakode_protocol::sync::Cursor as SessionSyncCursor;
mod projection;
#[derive(Clone, Debug)]
pub struct SessionSyncSnapshot {
    pub cursor: SessionSyncCursor,
    pub session: SessionView,
}

#[derive(Clone, Debug)]
pub struct SessionChangeBatch {
    pub base: SessionSyncCursor,
    pub next: SessionSyncCursor,
    pub events: Vec<ViewEvent>,
    projection: Option<projection::Patch>,
}

#[derive(Clone, Debug)]
pub enum SessionChanges {
    /// Includes every complete batch through `cursor`, or no batches when already current.
    Changes {
        cursor: SessionSyncCursor,
        batches: Vec<SessionChangeBatch>,
    },
    /// No prefix is usable. Fetch an authorized fresh bounded snapshot, not the durable history.
    ResetRequired,
}

#[derive(Clone)]
struct StoredBatch {
    base: u64,
    next: u64,
    at: Instant,
    encoded: Arc<[u8]>,
}

#[derive(Clone)]
struct History {
    cursor: SessionSyncCursor,
    batches: VecDeque<StoredBatch>,
    native_events: bool,
}

#[derive(Clone)]
pub(super) struct SessionHistories {
    sessions: HashMap<SessionId, History>,
    order: VecDeque<SessionId>,
    max_bytes: usize,
    max_batches: usize,
    max_sessions: usize,
    max_age: Duration,
}

impl Default for SessionHistories {
    fn default() -> Self {
        Self {
            sessions: HashMap::new(),
            order: VecDeque::new(),
            max_bytes: 8 * 1024 * 1024,
            max_batches: 256,
            max_sessions: 64,
            max_age: Duration::from_secs(120),
        }
    }
}

/// Stops serialization at the retention budget instead of first allocating an oversized batch.
struct BoundedEncoding {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedEncoding {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other(
                "session change batch exceeds retention budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SessionHistories {
    pub(super) fn observes(&self, session_id: &SessionId) -> bool {
        self.sessions.contains_key(session_id)
    }

    pub(super) fn remove(&mut self, session_id: &SessionId) {
        self.sessions.remove(session_id);
        self.order.retain(|id| id != session_id);
    }

    fn establish(&mut self, session_id: &SessionId, revision: u64) -> SessionSyncCursor {
        if let Some(history) = self.sessions.get(session_id)
            && history.cursor.revision == revision
        {
            return history.cursor.clone();
        }
        self.remove(session_id);
        let cursor = SessionSyncCursor {
            session_id: session_id.clone(),
            incarnation: Uuid::now_v7().to_string(),
            revision,
        };
        self.sessions.insert(
            session_id.clone(),
            History {
                cursor: cursor.clone(),
                batches: VecDeque::new(),
                native_events: false,
            },
        );
        self.order.push_back(session_id.clone());
        self.enforce_limits();
        cursor
    }

    fn enforce_limits(&mut self) {
        loop {
            let bytes: usize = self
                .sessions
                .values()
                .flat_map(|h| &h.batches)
                .map(|b| b.encoded.len())
                .sum();
            let batches: usize = self.sessions.values().map(|h| h.batches.len()).sum();
            if bytes <= self.max_bytes
                && batches <= self.max_batches
                && self.sessions.len() <= self.max_sessions
            {
                break;
            }
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.sessions.remove(&oldest);
        }
    }

    fn expire(&mut self, now: Instant) {
        for history in self.sessions.values_mut() {
            while history
                .batches
                .front()
                .is_some_and(|b| now.saturating_duration_since(b.at) >= self.max_age)
            {
                history.batches.pop_front();
            }
        }
    }

    #[cfg(test)]
    fn record(
        &mut self,
        id: &SessionId,
        base: Option<u64>,
        next: u64,
        events: &[&ViewEvent],
        now: Instant,
    ) {
        self.record_patch(id, base, next, &(events, None), now);
    }

    fn record_patch(
        &mut self,
        session_id: &SessionId,
        base: Option<u64>,
        next: u64,
        payload: &(&[&ViewEvent], Option<projection::Patch>),
        now: Instant,
    ) {
        self.expire(now);
        let Some(history) = self.sessions.get(session_id) else {
            return;
        };
        if base != Some(history.cursor.revision) || next <= history.cursor.revision {
            // Missing publication baseline or a non-monotonic mutation cannot be replayed safely.
            self.remove(session_id);
            return;
        }
        let mut encoding = BoundedEncoding {
            bytes: Vec::new(),
            limit: self.max_bytes,
        };
        if serde_json::to_writer(&mut encoding, payload).is_err() {
            self.remove(session_id);
            return;
        }
        let Some(history) = self.sessions.get_mut(session_id) else {
            return;
        };
        history.batches.push_back(StoredBatch {
            base: history.cursor.revision,
            next,
            at: now,
            encoded: encoding.bytes.into(),
        });
        history.cursor.revision = next;
        self.order.retain(|id| id != session_id);
        self.order.push_back(session_id.clone());
        self.enforce_limits();
    }

    fn changes(&mut self, cursor: &SessionSyncCursor, now: Instant) -> SessionChanges {
        self.expire(now);
        let Some(history) = self.sessions.get(&cursor.session_id) else {
            return SessionChanges::ResetRequired;
        };
        if cursor.incarnation != history.cursor.incarnation
            || cursor.revision > history.cursor.revision
        {
            return SessionChanges::ResetRequired;
        }
        let mut revision = cursor.revision;
        let mut batches = Vec::new();
        for batch in history
            .batches
            .iter()
            .filter(|batch| batch.next > cursor.revision)
        {
            if batch.base != revision {
                return SessionChanges::ResetRequired;
            }
            let Ok((events, projection)) = serde_json::from_slice(&batch.encoded) else {
                return SessionChanges::ResetRequired;
            };
            let mut base = cursor.clone();
            base.revision = batch.base;
            let mut next = cursor.clone();
            next.revision = batch.next;
            batches.push(SessionChangeBatch {
                base,
                next,
                events,
                projection,
            });
            revision = batch.next;
        }
        if revision != history.cursor.revision {
            return SessionChanges::ResetRequired;
        }
        SessionChanges::Changes {
            cursor: history.cursor.clone(),
            batches,
        }
    }
}

impl ServerCore {
    /// Establishes a native semantic-event observer at the public projection boundary.
    ///
    /// # Errors
    /// Returns the existing session lookup error for a missing session, or a retryable conflict
    /// until the canonical projection has reached the publication boundary.
    pub fn session_sync_snapshot(
        &mut self,
        session_id: &SessionId,
    ) -> Result<SessionSyncSnapshot, ServiceError> {
        let snapshot = self.projection_snapshot(session_id)?;
        if let Some(history) = self.session_histories.sessions.get_mut(session_id) {
            history.native_events = true;
        }
        Ok(snapshot)
    }

    fn projection_snapshot(
        &mut self,
        session_id: &SessionId,
    ) -> Result<SessionSyncSnapshot, ServiceError> {
        let session = self.session_view(session_id)?;
        if self
            .published_sessions
            .get(session_id)
            .is_none_or(|published| published.view != session)
        {
            // Never promise replay from a snapshot newer than the diff producer's baseline.
            // Do not update the legacy baseline here: that would suppress its next invalidation.
            return Err(super::service_error(
                nakode_protocol::ErrorCode::Conflict,
                "the incremental snapshot is awaiting session publication",
                true,
            ));
        }
        let cursor = self
            .session_histories
            .establish(session_id, session.revision);
        Ok(SessionSyncSnapshot { cursor, session })
    }

    /// Reads complete batches only. The transport must authorize the logical session first.
    /// No command is executed, no provider is contacted, and no historical bodies are hydrated.
    ///
    /// # Errors
    /// Returns the existing session lookup error after genuine removal; never treats it as reset.
    pub fn session_sync_changes(
        &mut self,
        cursor: &SessionSyncCursor,
    ) -> Result<SessionChanges, ServiceError> {
        self.ensure_session(&cursor.session_id)
            .map_err(super::domain_error)?;
        Ok(self.session_histories.changes(cursor, Instant::now()))
    }
}
