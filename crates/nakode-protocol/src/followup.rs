use serde::{Deserialize, Serialize};

use crate::SessionId;

/// A metadata/text view. Attachment bytes remain in the durable inbox until dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FollowupItem {
    pub sequence: u64,
    pub message_id: String,
    pub submitted_by: String,
    pub received_at_ms: i64,
    pub text: String,
    pub attachment_labels: Vec<String>,
    pub state: String,
    pub batch_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FollowupInbox {
    pub session_id: SessionId,
    pub items: Vec<FollowupItem>,
    pub has_more: bool,
    pub pending_count: u64,
    pub paused: bool,
    /// Dispatch may have reached the provider. Never automatically retry this batch on restart.
    pub unsettled_batch_id: Option<String>,
    pub blocked_reason: Option<String>,
}
