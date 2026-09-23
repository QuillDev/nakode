use serde::{Deserialize, Serialize};

use crate::{InteractionView, SessionId};

/// Availability of the original child question broker on this runtime. Retained history never
/// reconstructs a pending waiter; a stopped/restarted child must be explicitly resumed by its owner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildQuestionAvailability {
    Live,
    Unavailable,
    Closed,
}

/// A replacement projection of one durable linked child, not a second set of questions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildQuestions {
    pub child_session_id: SessionId,
    pub child_title: String,
    pub availability: ChildQuestionAvailability,
    pub interactions: Vec<InteractionView>,
}

/// Same-runtime only. Reads do not open children, start providers or suspend the parent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildQuestionSnapshot {
    pub parent_session_id: SessionId,
    pub children: Vec<ChildQuestions>,
}
