use serde::{Deserialize, Serialize};

/// One runtime-managed background process tree and its durable lifecycle record.
///
/// `state` is one of `launching`, `running`, `stopping`, `exited`, `stopped`,
/// `failed_to_start`, `cleanup_failed` or `unknown`. Running means the process tree is alive on
/// this machine; it never means the service inside it is ready.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskView {
    pub task_id: String,
    pub name: String,
    pub owner_session_id: String,
    /// Native delegated run that started the task inside its owner session, if any.
    pub origin_run_id: Option<String>,
    pub origin_turn_id: Option<String>,
    pub command: String,
    pub cwd: String,
    /// Hostname of the machine whose runtime supervises the task.
    pub machine: String,
    pub state: String,
    /// `child` while this runtime process is the OS parent; `adopted` after a runtime restart.
    pub supervision: String,
    pub process_id: Option<u32>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    /// Human-readable explanation of the latest transition or cleanup outcome.
    pub detail: Option<String>,
    /// `requested` or `owner_deleted` once a stop began.
    pub stop_reason: Option<String>,
    /// Set when the owning logical session was deleted; the record is then an orphan.
    pub owner_deleted_at_ms: Option<i64>,
    /// Endpoints declared at launch, for example `http://127.0.0.1:3000`, relative to `machine`.
    pub endpoints: Vec<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub ended_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

/// A complete inventory of this runtime's managed background tasks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskInventory {
    pub machine: String,
    pub observed_at_ms: i64,
    pub tasks: Vec<BackgroundTaskView>,
}

/// One declared endpoint and whether a TCP connection succeeded from the supervising machine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskEndpointView {
    pub endpoint: String,
    /// `accepting`, `refused` or `unknown`. Accepting connections still does not mean ready.
    pub probe: String,
}

/// One task with a bounded tail of its rotated log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskDetail {
    pub task: BackgroundTaskView,
    pub log_tail: String,
    /// Earlier log content exists beyond the returned tail.
    pub log_truncated: bool,
    pub endpoints: Vec<BackgroundTaskEndpointView>,
    pub observed_at_ms: i64,
}
