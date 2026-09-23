use serde::{Deserialize, Serialize};

/// Server-attributed evidence from one linked logical child session, never a controller message.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildReport {
    pub sequence: u64,
    pub report_id: String,
    pub parent_session_id: String,
    pub child_session_id: String,
    pub child_title: String,
    pub state: String,
    pub body: String,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildReportPage {
    pub reports: Vec<ChildReport>,
    pub has_more: bool,
}
