//! Actor-local requests and replies for resumable bounded session projections.
use crate::{SessionId, SessionView, TranscriptChanges};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor {
    pub session_id: SessionId,
    pub incarnation: String,
    pub revision: u64,
}
#[derive(Debug)]
pub enum Read {
    Snapshot,
    After(Option<Cursor>),
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Reply {
    Snapshot(Cursor, Box<SessionView>),
    Changes(Vec<Batch>),
    Reset,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Batch {
    pub base: Cursor,
    pub next: Cursor,
    pub projection: Box<SessionView>,
    pub changes: TranscriptChanges,
}
