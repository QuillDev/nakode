//! Runtime-managed background tasks: long-running non-AI processes owned by logical sessions.
//!
//! The Nakode server process spawns every task itself, so neither a provider process nor a
//! transient tool shell is ever its parent. Each task gets a durable registry row before it is
//! launched, its own process group, and an append-only rotated log file instead of pipes, so it
//! keeps running across tool calls, finished turns, closed sessions and runtime restarts.
//!
//! Ownership comes only from the authenticated caller scope the runtime already holds for a
//! session; a tool argument can never name an owner. Deleting the owner terminates its tasks.
//! Containment, identity and restart limits are documented in `docs/background-tasks.md`.

#[cfg(unix)]
mod os;
mod store;
mod tool;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use nakode_protocol::{
    BackgroundTaskDetail, BackgroundTaskEndpointView, BackgroundTaskInventory, BackgroundTaskView,
};
use tokio::sync::Notify;

pub(crate) use store::TaskState;
use store::{TaskRecord, TaskStore};
pub(crate) use tool::{BACKGROUND_TASKS_TOOL_NAME, BackgroundTasksTool, execute_tool_call};

/// Unended tasks one owner session may hold at once.
const MAX_ACTIVE_TASKS_PER_OWNER: usize = 32;
const MAX_COMMAND_BYTES: usize = 16 * 1024;
const MAX_NAME_CHARS: usize = 120;
const MAX_ENDPOINTS: usize = 8;
const MAX_ENDPOINT_BYTES: usize = 512;
/// Largest log tail one inspection returns.
pub(crate) const MAX_LOG_TAIL_BYTES: u32 = 64 * 1024;
pub(crate) const DEFAULT_LOG_TAIL_BYTES: u32 = 8 * 1024;
/// Failure diagnostics expire after seven days; actionable records do not.
const DIAGNOSTIC_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(5);
const OWNER_DELETION_GRACE: Duration = Duration::from_secs(3);
pub(crate) const MAX_STOP_GRACE: Duration = Duration::from_secs(60);
/// How long SIGKILL may take to be observed before cleanup is reported as failed.
const KILL_SETTLE: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Interval between process checks and log size checks.
const MONITOR_INTERVAL: Duration = Duration::from_secs(2);

static SERVICE: OnceLock<Arc<BackgroundTaskService>> = OnceLock::new();

/// The runtime-wide service, once the server has initialized it.
pub(crate) fn service() -> Option<Arc<BackgroundTaskService>> {
    SERVICE.get().cloned()
}

/// Opens the registry beside the sessions database, reconciles it, and starts supervision.
///
/// Called once by the server at startup. Later calls return the existing service.
///
/// # Errors
/// Returns an error when the registry or log directory cannot be opened.
pub(crate) async fn initialize(database: &Path) -> Result<Arc<BackgroundTaskService>, String> {
    if let Some(existing) = service() {
        return Ok(existing);
    }
    let log_directory = database
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("background-tasks");
    let created = BackgroundTaskService::open(database, &log_directory)?;
    let service = SERVICE.get_or_init(|| created).clone();
    let reconciled = service.reconcile().await;
    service.clone().spawn_monitor(reconciled);
    Ok(service)
}

/// Authenticated identity of a tool caller, taken from runtime-held session state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CallerScope {
    /// Logical owner session the call acts for.
    pub(crate) owner: String,
    /// Native delegated run acting inside the owner session. A run sees only its own tasks.
    pub(crate) run_id: Option<String>,
    pub(crate) turn_id: Option<String>,
    pub(crate) call_id: Option<String>,
}

impl CallerScope {
    fn may_access(&self, record: &TaskRecord) -> bool {
        record.owner_session_id == self.owner
            && self
                .run_id
                .as_ref()
                .is_none_or(|run| record.origin_run_id.as_ref() == Some(run))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StartRequest {
    pub(crate) name: String,
    pub(crate) command: String,
    pub(crate) cwd: PathBuf,
    pub(crate) endpoints: Vec<String>,
    pub(crate) environment: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StopReason {
    Requested,
    OwnerDeleted,
}

impl StopReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::OwnerDeleted => "owner_deleted",
        }
    }
}

/// How an owner's tasks ended when the owner was deleted.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct OwnerCleanup {
    pub(crate) stopped: Vec<String>,
    /// Tasks whose processes could not be proven gone; they remain as orphan recovery records.
    pub(crate) incomplete: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LeaderExit {
    code: Option<i32>,
    signal: Option<i32>,
}

#[derive(Default)]
struct Coordination {
    /// Owners whose deletion began; they may not start new tasks.
    deleting_owners: HashSet<String>,
    /// Tasks with a stop in progress in this process.
    stopping: HashSet<String>,
}

pub(crate) struct BackgroundTaskService {
    store: Mutex<TaskStore>,
    log_directory: PathBuf,
    machine: String,
    /// Serializes launches, stop intents and owner deletion fences.
    coordination: tokio::sync::Mutex<Coordination>,
    leader_exits: Mutex<HashMap<String, LeaderExit>>,
    wake: Notify,
}

impl BackgroundTaskService {
    pub(crate) fn open(database: &Path, log_directory: &Path) -> Result<Arc<Self>, String> {
        std::fs::create_dir_all(log_directory).map_err(|error| {
            format!(
                "could not create background task log directory {}: {error}",
                log_directory.display()
            )
        })?;
        Ok(Arc::new(Self {
            store: Mutex::new(TaskStore::open(database)?),
            log_directory: log_directory.to_path_buf(),
            machine: hostname::get().map_or_else(
                |_| "unknown".to_owned(),
                |name| name.to_string_lossy().into_owned(),
            ),
            coordination: tokio::sync::Mutex::default(),
            leader_exits: Mutex::default(),
            wake: Notify::new(),
        }))
    }

    fn with_store<T>(
        &self,
        operation: impl FnOnce(&TaskStore) -> Result<T, String>,
    ) -> Result<T, String> {
        let store = self
            .store
            .lock()
            .map_err(|_| "background task registry is unavailable".to_owned())?;
        operation(&store)
    }

    fn save(&self, record: &mut TaskRecord) -> Result<(), String> {
        record.updated_at_ms = now_ms();
        self.with_store(|store| store.update(record))
    }

    fn record(&self, task_id: &str) -> Result<TaskRecord, String> {
        self.with_store(|store| store.get(task_id))?
            .ok_or_else(|| format!("unknown background task {task_id}"))
    }

    // ---- Caller-scoped operations used by agent tools ----

    /// Starts one task for the authenticated caller.
    pub(crate) async fn start(
        self: &Arc<Self>,
        scope: &CallerScope,
        request: StartRequest,
    ) -> Result<BackgroundTaskView, String> {
        validate_start(&request)?;
        let coordination = self.coordination.lock().await;
        if coordination.deleting_owners.contains(&scope.owner) {
            return Err("the owning session is being deleted; no task was started".to_owned());
        }
        let active = self
            .with_store(|store| store.list(Some(&scope.owner)))?
            .into_iter()
            .filter(|record| !record.state.ended())
            .count();
        if active >= MAX_ACTIVE_TASKS_PER_OWNER {
            return Err(format!(
                "this session already has {active} unended background tasks; stop some first"
            ));
        }
        let task_id = format!("bgt_{}", uuid::Uuid::now_v7().simple());
        let now = now_ms();
        let mut record = TaskRecord {
            log_path: self
                .log_directory
                .join(format!("{task_id}.log"))
                .display()
                .to_string(),
            task_id,
            name: request.name.trim().to_owned(),
            owner_session_id: scope.owner.clone(),
            origin_run_id: scope.run_id.clone(),
            origin_turn_id: scope.turn_id.clone(),
            origin_call_id: scope.call_id.clone(),
            command: request.command.clone(),
            cwd: request.cwd.display().to_string(),
            machine: self.machine.clone(),
            state: TaskState::Launching,
            supervision: "child".to_owned(),
            process_id: None,
            process_start: None,
            exit_code: None,
            exit_signal: None,
            detail: None,
            stop_reason: None,
            stop_requested_at_ms: None,
            owner_deleted_at_ms: None,
            endpoints: request.endpoints.clone(),
            created_at_ms: now,
            started_at_ms: None,
            ended_at_ms: None,
            updated_at_ms: now,
        };
        // The launch intent is durable before any process exists.
        self.with_store(|store| store.insert(&record))?;
        let launched = self.launch(&mut record, &request).await;
        if let Err(error) = &launched {
            record.state = TaskState::FailedToStart;
            record.detail = Some(error.clone());
            record.ended_at_ms = Some(now_ms());
        }
        self.save(&mut record)?;
        drop(coordination);
        self.wake.notify_one();
        launched.map(|()| view(&record))
    }

    pub(crate) fn list_for(&self, scope: &CallerScope) -> Result<Vec<BackgroundTaskView>, String> {
        let records = self.with_store(|store| store.list(Some(&scope.owner)))?;
        Ok(Self::bounded_views(
            records
                .into_iter()
                .filter(|record| scope.may_access(record))
                .collect(),
        ))
    }

    pub(crate) async fn inspect_for(
        &self,
        scope: &CallerScope,
        task_id: &str,
        log_tail_bytes: u32,
    ) -> Result<BackgroundTaskDetail, String> {
        let record = self.authorized(scope, task_id)?;
        Ok(self.detail(record, log_tail_bytes).await)
    }

    pub(crate) async fn stop_for(
        self: &Arc<Self>,
        scope: &CallerScope,
        task_id: &str,
        grace: Option<Duration>,
    ) -> Result<BackgroundTaskView, String> {
        self.authorized(scope, task_id)?;
        self.stop(task_id, StopReason::Requested, grace).await
    }

    fn authorized(&self, scope: &CallerScope, task_id: &str) -> Result<TaskRecord, String> {
        // An inaccessible task is reported exactly like a missing one.
        self.with_store(|store| store.get(task_id))?
            .filter(|record| scope.may_access(record))
            .ok_or_else(|| format!("unknown background task {task_id}"))
    }

    /// Idempotent exact-ID stop acknowledgement for integrations and tools.
    pub(crate) async fn stop_ack(
        self: &Arc<Self>,
        scope: Option<&CallerScope>,
        task_id: &str,
        grace: Option<Duration>,
    ) -> Result<(), String> {
        if let Some(scope) = scope
            && self.authorized(scope, task_id).is_err()
        {
            return self.dismiss(Some(scope), task_id).await;
        }
        match self.stop(task_id, StopReason::Requested, grace).await {
            Ok(_) => Ok(()),
            Err(error) => {
                // A concurrent successful exit/stop/delete may already have removed the row.
                // A receipt is required; arbitrary missing identities never acknowledge success.
                if self.with_store(|store| store.get(task_id))?.is_none() {
                    self.dismiss(scope, task_id).await
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Acknowledges only a confirmed terminal failure; never sends a signal.
    pub(crate) async fn dismiss(
        &self,
        scope: Option<&CallerScope>,
        task_id: &str,
    ) -> Result<(), String> {
        let coordination = self.coordination.lock().await;
        let Some(record) = self.with_store(|store| store.get(task_id))? else {
            let receipt = self.with_store(|store| store.receipt(task_id))?;
            return match receipt {
                Some((owner, run))
                    if scope.is_none_or(|scope| {
                        scope.owner == owner
                            && scope
                                .run_id
                                .as_ref()
                                .is_none_or(|id| run.as_ref() == Some(id))
                    }) =>
                {
                    Ok(())
                }
                _ => Err(format!("unknown background task {task_id}")),
            };
        };
        if scope.is_some_and(|scope| !scope.may_access(&record)) {
            return Err(format!("unknown background task {task_id}"));
        }
        if !record.state.ended() || coordination.stopping.contains(task_id) {
            return Err("background task is not a confirmed terminal failure".to_owned());
        }
        self.confirm_gone(&record).await?;
        self.remove(&record)
    }

    fn remove(&self, record: &TaskRecord) -> Result<(), String> {
        self.with_store(|store| store.remove(record))?;
        self.purge_logs(&record.task_id);
        if let Ok(mut exits) = self.leader_exits.lock() {
            exits.remove(&record.task_id);
        }
        Ok(())
    }

    /// Never trust persisted paths for deletion: only generated task-owned diagnostic names.
    fn purge_logs(&self, task_id: &str) {
        if !task_id.starts_with("bgt_")
            || !task_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return;
        }
        let log = self.log_directory.join(format!("{task_id}.log"));
        for path in [&log, &crate::service_log::archive_path(&log)] {
            if let Err(error) = std::fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                report_failure(&format!("could not remove task diagnostic: {error}"));
            }
        }
    }

    fn collect_diagnostics(&self) {
        let Ok(records) = self.with_store(|store| store.list(None)) else {
            return;
        };
        let cutoff = now_ms().saturating_sub(DIAGNOSTIC_RETENTION_MS);
        for record in records {
            if record.state.ended() && record.ended_at_ms.is_some_and(|ended| ended < cutoff) {
                self.purge_logs(&record.task_id);
            }
        }
        // A crash between the registry transaction and unlink must not leak diagnostics.
        let Ok(entries) = std::fs::read_dir(&self.log_directory) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = name
                .strip_suffix(".log.1")
                .or_else(|| name.strip_suffix(".log"))
            else {
                continue;
            };
            if self
                .with_store(|store| store.receipt(id))
                .is_ok_and(|receipt| receipt.is_some())
            {
                self.purge_logs(id);
            }
        }
    }

    // ---- Owner-integration operations used by the public API ----

    /// Every task in this runtime, or every task of one owner, including orphans.
    pub(crate) fn inventory(
        &self,
        owner_session_id: Option<&str>,
    ) -> Result<BackgroundTaskInventory, String> {
        let records = self.with_store(|store| store.list(owner_session_id))?;
        Ok(BackgroundTaskInventory {
            machine: self.machine.clone(),
            observed_at_ms: now_ms(),
            tasks: Self::bounded_views(records),
        })
    }

    pub(crate) async fn inspect(
        &self,
        task_id: &str,
        log_tail_bytes: u32,
    ) -> Result<BackgroundTaskDetail, String> {
        let record = self.record(task_id)?;
        Ok(self.detail(record, log_tail_bytes).await)
    }

    /// Terminates one task's whole process tree and records the outcome.
    ///
    /// An ended task is returned unchanged. A stop already in progress is not duplicated.
    pub(crate) async fn stop(
        self: &Arc<Self>,
        task_id: &str,
        reason: StopReason,
        grace: Option<Duration>,
    ) -> Result<BackgroundTaskView, String> {
        let mut record = {
            let mut coordination = self.coordination.lock().await;
            let mut record = self.record(task_id)?;
            if record.state.ended() || coordination.stopping.contains(task_id) {
                return Ok(view(&record));
            }
            coordination.stopping.insert(task_id.to_owned());
            record.state = TaskState::Stopping;
            record.stop_reason = Some(reason.as_str().to_owned());
            record.stop_requested_at_ms = Some(now_ms());
            if let Err(error) = self.save(&mut record) {
                coordination.stopping.remove(task_id);
                return Err(error);
            }
            record
        };
        append_log_note(
            &record.log_path,
            &format!("stop requested ({})", reason.as_str()),
        );
        self.terminate(&mut record, grace.unwrap_or(DEFAULT_STOP_GRACE))
            .await;
        let _ = crate::service_log::rotate_if_oversized(Path::new(&record.log_path));
        let mut coordination = self.coordination.lock().await;
        // Owner deletion may have marked the task while it was stopping.
        if let Ok(current) = self.record(task_id) {
            record.owner_deleted_at_ms = current.owner_deleted_at_ms;
        }
        let saved = if record.state.ended() {
            self.remove(&record)
        } else {
            self.save(&mut record)
        };
        coordination.stopping.remove(task_id);
        saved.map(|()| view(&record))
    }

    /// Fences new launches for `owner` and marks its tasks as belonging to a deleted owner.
    ///
    /// Called before a logical session's durable deletion. Nothing is signalled yet, so a failed
    /// deletion leaves the session's tasks running. Should the runtime stop before the deletion
    /// settles, restart reconciliation stops the marked tasks.
    pub(crate) async fn begin_owner_deletion(&self, owner: &str) {
        let mut coordination = self.coordination.lock().await;
        coordination.deleting_owners.insert(owner.to_owned());
        self.mark_owner_deleted(owner, Some(now_ms()));
    }

    /// Records the owner deletion outcome.
    ///
    /// A failed deletion lifts the fence and the marks. A committed one stops every unended task
    /// of the owner in the background, so deletion never waits for process grace periods; tasks
    /// that cannot be proven gone remain `cleanup_failed` orphan records. Returns that cleanup.
    pub(crate) async fn finish_owner_deletion(
        self: &Arc<Self>,
        owner: &str,
        committed: bool,
    ) -> Option<tokio::task::JoinHandle<OwnerCleanup>> {
        if !committed {
            let mut coordination = self.coordination.lock().await;
            coordination.deleting_owners.remove(owner);
            self.mark_owner_deleted(owner, None);
            return None;
        }
        let service = Arc::clone(self);
        let owner = owner.to_owned();
        Some(tokio::spawn(async move {
            service.stop_owner_tasks(&owner).await
        }))
    }

    /// Callers hold the coordination lock so the mark cannot race a stop's final save.
    fn mark_owner_deleted(&self, owner: &str, deleted_at_ms: Option<i64>) {
        let records = match self.with_store(|store| store.list(Some(owner))) {
            Ok(records) => records,
            Err(error) => {
                report_failure(&error);
                return;
            }
        };
        for mut record in records {
            if record.owner_deleted_at_ms.is_some() != deleted_at_ms.is_some() {
                record.owner_deleted_at_ms = deleted_at_ms;
                if let Err(error) = self.save(&mut record) {
                    report_failure(&error);
                }
            }
        }
    }

    async fn stop_owner_tasks(self: &Arc<Self>, owner: &str) -> OwnerCleanup {
        let records = match self.with_store(|store| store.list(Some(owner))) {
            Ok(records) => records,
            Err(error) => {
                report_failure(&error);
                return OwnerCleanup::default();
            }
        };
        // Stop concurrently so cleanup takes one grace period, not one per task.
        let stops = records.into_iter().map(|record| async move {
            if record.state.ended() {
                let coordination = self.coordination.lock().await;
                let gone = self.confirm_gone(&record).await.is_ok() && self.remove(&record).is_ok();
                drop(coordination);
                return (record.task_id, gone);
            }
            let _ = self
                .stop(
                    &record.task_id,
                    StopReason::OwnerDeleted,
                    Some(OWNER_DELETION_GRACE),
                )
                .await;
            // A stop that was already in progress settles on its own schedule.
            self.wait_for_stop(&record.task_id).await;
            let gone = self
                .with_store(|store| store.receipt(&record.task_id))
                .is_ok_and(|receipt| receipt.is_some());
            (record.task_id, gone)
        });
        let mut cleanup = OwnerCleanup::default();
        for (task_id, gone) in futures_util::future::join_all(stops).await {
            if gone {
                cleanup.stopped.push(task_id);
            } else {
                cleanup.incomplete.push(task_id);
            }
        }
        cleanup
    }

    async fn wait_for_stop(&self, task_id: &str) {
        let deadline = tokio::time::Instant::now() + MAX_STOP_GRACE + KILL_SETTLE * 2;
        while self.coordination.lock().await.stopping.contains(task_id)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    // ---- Views ----
}

fn view(record: &TaskRecord) -> BackgroundTaskView {
    BackgroundTaskView {
        task_id: record.task_id.clone(),
        name: record.name.clone(),
        owner_session_id: record.owner_session_id.clone(),
        origin_run_id: record.origin_run_id.clone(),
        origin_turn_id: record.origin_turn_id.clone(),
        command: record.command.clone(),
        cwd: record.cwd.clone(),
        machine: record.machine.clone(),
        state: record.state.as_str().to_owned(),
        supervision: record.supervision.clone(),
        process_id: record.process_id.and_then(|pid| u32::try_from(pid).ok()),
        exit_code: record.exit_code,
        exit_signal: record.exit_signal,
        detail: record.detail.clone(),
        stop_reason: record.stop_reason.clone(),
        owner_deleted_at_ms: record.owner_deleted_at_ms,
        endpoints: record.endpoints.clone(),
        created_at_ms: record.created_at_ms,
        started_at_ms: record.started_at_ms,
        ended_at_ms: record.ended_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

impl BackgroundTaskService {
    /// Failures remain actionable until acknowledged, without a recent-history cap.
    fn bounded_views(records: Vec<TaskRecord>) -> Vec<BackgroundTaskView> {
        records.into_iter().map(|record| view(&record)).collect()
    }

    async fn detail(&self, record: TaskRecord, log_tail_bytes: u32) -> BackgroundTaskDetail {
        let (log_tail, log_truncated) = read_log_tail(
            Path::new(&record.log_path),
            log_tail_bytes.min(MAX_LOG_TAIL_BYTES),
        );
        let mut endpoints = Vec::with_capacity(record.endpoints.len());
        for endpoint in &record.endpoints {
            let probe = if record.state == TaskState::Running {
                probe_endpoint(endpoint).await
            } else {
                "unknown"
            };
            endpoints.push(BackgroundTaskEndpointView {
                endpoint: endpoint.clone(),
                probe: probe.to_owned(),
            });
        }
        BackgroundTaskDetail {
            task: view(&record),
            log_tail,
            log_truncated,
            endpoints,
            observed_at_ms: now_ms(),
        }
    }
}

// ---- Process lifecycle (Unix) ----

#[cfg(unix)]
impl BackgroundTaskService {
    fn processes(
        &self,
        record: &TaskRecord,
        table: &os::ProcessTable,
    ) -> Result<os::TaskProcesses, String> {
        let Some(group) = record.process_id else {
            if record.state == TaskState::FailedToStart || record.state == TaskState::Stopped {
                return Ok(os::TaskProcesses::default());
            }
            return Err("background task process identity is unknown".to_owned());
        };
        let start = record
            .process_start
            .as_deref()
            .filter(|start| !start.is_empty())
            .ok_or_else(|| "background task process identity is unknown".to_owned())?;
        let mut tracked = self.with_store(|store| store.tracked(&record.task_id))?;
        let processes = table.tracked_task_processes(group, start, &mut tracked);
        self.with_store(|store| store.track(&record.task_id, &tracked))?;
        Ok(processes)
    }

    async fn confirm_gone(&self, record: &TaskRecord) -> Result<(), String> {
        let table = os::ProcessTable::capture().await?;
        if self.processes(record, &table)?.is_empty() {
            Ok(())
        } else {
            Err("background task still has live processes".to_owned())
        }
    }

    /// Spawns the leader in its own process group with output appended to the task log.
    async fn launch(
        self: &Arc<Self>,
        record: &mut TaskRecord,
        request: &StartRequest,
    ) -> Result<(), String> {
        use std::process::Stdio;

        let log_path = Path::new(&record.log_path);
        append_log_note(&record.log_path, &format!("starting: {}", record.command));
        let log = crate::service_log::open_for_append(log_path)
            .map_err(|error| format!("could not open task log {}: {error}", log_path.display()))?;
        let error_log = log
            .try_clone()
            .map_err(|error| format!("could not open task log: {error}"))?;
        let (program, arguments) = gated_shell(&request.command, &request.environment);
        let mut command = tokio::process::Command::new(program);
        command
            .args(arguments)
            .current_dir(&request.cwd)
            .envs(&request.environment)
            .env(os::TASK_MARKER, &record.task_id)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .process_group(0)
            .kill_on_drop(false);
        let mut child = command
            .spawn()
            .map_err(|error| format!("could not start the command: {error}"))?;
        let pid = child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .ok_or_else(|| "the started process has no id".to_owned())?;
        // The gated leader waits on stdin. Its identity is durable before the command may run,
        // so an interrupted launch either never ran the command or is recorded and adoptable.
        let mut release = child.stdin.take();
        record.process_id = Some(pid);
        let identified = os::ProcessTable::capture().await.and_then(|table| {
            table
                .start_of(pid)
                .map(str::to_owned)
                .ok_or_else(|| format!("process {pid} was not visible in the process list"))
        });
        let saved = identified.and_then(|start| {
            record.process_start = Some(start);
            self.save(record)
        });
        if let Err(error) = saved {
            // Without a durable identity the task could not be stopped safely later. Closing the
            // gate without a release line makes the leader exit without running the command.
            drop(release);
            let _ = child.wait().await;
            return Err(format!("{error}; the command never ran"));
        }
        if let Some(mut gate) = release.take() {
            use tokio::io::AsyncWriteExt as _;
            // A leader that already died is observed by the monitor like any other exit.
            let _ = gate.write_all(b"\n").await;
        }
        record.state = TaskState::Running;
        record.started_at_ms = Some(now_ms());
        let service = Arc::clone(self);
        let task_id = record.task_id.clone();
        tokio::spawn(async move {
            use std::os::unix::process::ExitStatusExt as _;
            let exit = match child.wait().await {
                Ok(status) => LeaderExit {
                    code: status.code(),
                    signal: status.signal(),
                },
                Err(_) => LeaderExit {
                    code: None,
                    signal: None,
                },
            };
            if let Ok(mut exits) = service.leader_exits.lock()
                && service
                    .with_store(|store| store.get(&task_id))
                    .is_ok_and(|record| record.is_some())
            {
                exits.insert(task_id, exit);
            }
            service.wake.notify_one();
        });
        Ok(())
    }

    /// Signals the task tree until it is gone, escalating to SIGKILL after `grace`.
    async fn terminate(&self, record: &mut TaskRecord, grace: Duration) {
        use nix::sys::signal::Signal;

        let Some(group) = Self::resolve_identity(record) else {
            return;
        };
        let leader_start = record.process_start.clone().unwrap_or_default();
        let mut tracked = match self.with_store(|store| store.tracked(&record.task_id)) {
            Ok(tracked) => tracked,
            Err(error) => {
                fail_cleanup(record, &error);
                return;
            }
        };
        let mut failures = Vec::new();
        for (signal, wait) in [(Signal::SIGTERM, grace), (Signal::SIGKILL, KILL_SETTLE)] {
            let processes = match os::ProcessTable::capture().await {
                Ok(table) => table.tracked_task_processes(group, &leader_start, &mut tracked),
                Err(error) => {
                    fail_cleanup(record, &error);
                    return;
                }
            };
            if let Err(error) = self.with_store(|store| store.track(&record.task_id, &tracked)) {
                fail_cleanup(record, &error);
                return;
            }
            if processes.is_empty() {
                self.finish_stopped(record);
                return;
            }
            failures.extend(os::signal_task(group, &processes, signal));
            if wait_until_gone(group, &leader_start, &mut tracked, wait).await {
                self.finish_stopped(record);
                return;
            }
        }
        let remaining = match os::ProcessTable::capture().await {
            Ok(table) => table
                .tracked_task_processes(group, &leader_start, &mut tracked)
                .all(),
            Err(_) => Vec::new(),
        };
        let mut detail = format!(
            "processes remained after SIGKILL: {}",
            remaining
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !failures.is_empty() {
            detail.push_str("; signal failures: ");
            detail.push_str(&failures.join("; "));
        }
        fail_cleanup(record, &detail);
    }

    /// The task's process group, or `None` after recording that no process was ever started.
    fn resolve_identity(record: &mut TaskRecord) -> Option<i32> {
        if record.process_id.is_none() {
            fail_cleanup(
                record,
                "no process identity was recorded; cleanup cannot be verified",
            );
        }
        record.process_id
    }

    fn finish_stopped(&self, record: &mut TaskRecord) {
        let exit = self.leader_exit(&record.task_id);
        record.state = TaskState::Stopped;
        record.exit_code = exit.and_then(|exit| exit.code);
        record.exit_signal = exit.and_then(|exit| exit.signal);
        record.detail = Some("process tree terminated".to_owned());
        record.ended_at_ms = Some(now_ms());
    }

    fn leader_exit(&self, task_id: &str) -> Option<LeaderExit> {
        self.leader_exits
            .lock()
            .ok()
            .and_then(|exits| exits.get(task_id).copied())
    }

    /// Restart reconciliation: every unended row is verified against the live process table.
    ///
    /// Returns `false` when processes could not be listed; the monitor then retries. Until then
    /// running tasks are reported `unknown`, never ended.
    async fn reconcile(self: &Arc<Self>) -> bool {
        let coordination = self.coordination.lock().await;
        let records = match self.with_store(|store| store.list(None)) {
            Ok(records) => records,
            Err(error) => {
                report_failure(&error);
                return false;
            }
        };
        if records.is_empty() {
            return true;
        }
        let table = match os::ProcessTable::capture().await {
            Ok(table) => table,
            Err(error) => {
                for mut record in records {
                    if record.state == TaskState::Running {
                        record.state = TaskState::Unknown;
                        record.detail = Some(format!("runtime restarted; {error}"));
                        let _ = self.save(&mut record);
                    }
                }
                return false;
            }
        };
        let mut stops = Vec::new();
        for mut record in records {
            if coordination.stopping.contains(&record.task_id) {
                continue;
            }
            match record.state {
                TaskState::Launching => Self::reconcile_launch(&mut record, &table),
                TaskState::Running | TaskState::CleanupFailed | TaskState::Unknown => {
                    match self.processes(&record, &table) {
                        Ok(processes) => reconcile_running(&mut record, &processes),
                        Err(error) => {
                            record.state = TaskState::Unknown;
                            record.detail = Some(error);
                        }
                    }
                }
                TaskState::Stopping => {
                    // Resume the interrupted stop with its original reason.
                    let reason = if record.stop_reason.as_deref() == Some("owner_deleted") {
                        StopReason::OwnerDeleted
                    } else {
                        StopReason::Requested
                    };
                    record.state = TaskState::Running;
                    record.supervision = String::from("adopted");
                    stops.push((record.task_id.clone(), reason));
                }
                TaskState::Exited | TaskState::Stopped | TaskState::FailedToStart => {
                    if self.confirm_gone(&record).await.is_err() {
                        record.state = TaskState::Unknown;
                        record.ended_at_ms = None;
                        record.detail = Some(
                            "terminal record could not be verified; cleanup required".to_owned(),
                        );
                    }
                }
            }
            if record.state.ended()
                && (record.stop_reason.is_some()
                    || record.exit_code == Some(0)
                    || record.owner_deleted_at_ms.is_some())
            {
                if let Err(error) = self.remove(&record) {
                    report_failure(&error);
                }
                continue;
            }
            // A deleted owner's surviving tasks are stopped, whether or not the deletion settled.
            if record.state == TaskState::Running
                && record.owner_deleted_at_ms.is_some()
                && !stops.iter().any(|(task_id, _)| task_id == &record.task_id)
            {
                stops.push((record.task_id.clone(), StopReason::OwnerDeleted));
            }
            if let Err(error) = self.save(&mut record) {
                report_failure(&error);
            }
        }
        drop(coordination);
        for (task_id, reason) in stops {
            let service = Arc::clone(self);
            tokio::spawn(async move {
                let _ = service.stop(&task_id, reason, None).await;
            });
        }
        true
    }

    /// A launch interrupted before release never ran its command; after release it is adopted.
    fn reconcile_launch(record: &mut TaskRecord, table: &os::ProcessTable) {
        let Some(group) = record.process_id else {
            record.state = TaskState::FailedToStart;
            record.detail = Some(
                "launch was interrupted before the process was released; the command never ran"
                    .to_owned(),
            );
            record.ended_at_ms = Some(now_ms());
            return;
        };
        let processes =
            table.task_processes(group, record.process_start.as_deref().unwrap_or_default());
        if processes.is_empty() {
            record.state = TaskState::Exited;
            record.detail = Some(
                "launch was interrupted and its process is gone; the command may not have run"
                    .to_owned(),
            );
            record.ended_at_ms = Some(now_ms());
        } else {
            record.state = TaskState::Running;
            record.supervision = String::from("adopted");
            record.started_at_ms.get_or_insert(record.created_at_ms);
            record.detail = Some(
                "launch was interrupted by a runtime restart after release; adopted".to_owned(),
            );
        }
    }
}

#[cfg(unix)]
fn fail_cleanup(record: &mut TaskRecord, detail: &str) {
    record.state = TaskState::CleanupFailed;
    record.detail = Some(detail.to_owned());
}

#[cfg(unix)]
fn reconcile_running(record: &mut TaskRecord, processes: &os::TaskProcesses) {
    if processes.is_empty() {
        let failed_cleanup = record.state == TaskState::CleanupFailed;
        record.state = if failed_cleanup && record.stop_reason.is_some() {
            TaskState::Stopped
        } else {
            TaskState::Exited
        };
        record.detail =
            Some("ended while the runtime was not supervising it; exit status unknown".to_owned());
        record.ended_at_ms = Some(now_ms());
    } else if matches!(record.state, TaskState::Running | TaskState::Unknown) {
        record.state = TaskState::Running;
        record.supervision = String::from("adopted");
        record.detail = Some(format!(
            "adopted after a runtime restart with {} live processes; exit status will be unknown",
            processes.count()
        ));
    }
}

#[cfg(unix)]
impl BackgroundTaskService {
    fn spawn_monitor(self: Arc<Self>, reconciled: bool) {
        tokio::spawn(async move {
            let mut reconciled = reconciled;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(MONITOR_INTERVAL) => {}
                    () = self.wake.notified() => {}
                }
                if !reconciled {
                    reconciled = self.reconcile().await;
                    continue;
                }
                self.observe().await;
            }
        });
    }

    /// Settles running tasks whose process tree has gone and keeps live logs bounded.
    async fn observe(&self) {
        self.collect_diagnostics();
        let unended = match self.with_store(TaskStore::list_unended) {
            Ok(records) => records,
            Err(error) => {
                report_failure(&error);
                return;
            }
        };
        for record in &unended {
            let _ = crate::service_log::rotate_if_oversized(Path::new(&record.log_path));
        }
        // Unknown tasks with an identity are re-verified, never assumed ended.
        let watched = unended
            .into_iter()
            .filter(|record| {
                matches!(record.state, TaskState::Running | TaskState::CleanupFailed)
                    || (record.state == TaskState::Unknown && record.process_id.is_some())
            })
            .collect::<Vec<_>>();
        if watched.is_empty() {
            return;
        }
        let table = match os::ProcessTable::capture().await {
            Ok(table) => table,
            Err(error) => {
                for record in watched {
                    self.update_observed(&record.task_id, TaskState::Running, |record| {
                        record.state = TaskState::Unknown;
                        record.detail = Some(format!("could not verify processes: {error}"));
                    })
                    .await;
                }
                return;
            }
        };
        for record in watched {
            let Ok(processes) = self.processes(&record, &table) else {
                continue;
            };
            let exit = self.leader_exit(&record.task_id);
            if processes.is_empty() {
                self.settle_exit(&record.task_id, exit).await;
            } else if record.state == TaskState::Unknown {
                self.update_observed(&record.task_id, TaskState::Unknown, |record| {
                    record.state = TaskState::Running;
                    record.detail = Some(format!(
                        "process identity re-verified with {} live processes",
                        processes.count()
                    ));
                })
                .await;
            } else if let Some(exit) = exit
                && record.detail.is_none()
            {
                self.update_observed(&record.task_id, TaskState::Running, |record| {
                    record.detail = Some(format!(
                        "leader exited ({}); {} processes still running",
                        describe_exit(exit),
                        processes.count()
                    ));
                })
                .await;
            }
        }
    }

    /// Applies a monitor observation to the current record unless its state changed or a stop
    /// claimed it after the observation was made.
    async fn update_observed(
        &self,
        task_id: &str,
        expected: TaskState,
        change: impl FnOnce(&mut TaskRecord),
    ) {
        let coordination = self.coordination.lock().await;
        if coordination.stopping.contains(task_id) {
            return;
        }
        let Ok(mut record) = self.record(task_id) else {
            return;
        };
        if record.state != expected {
            return;
        }
        change(&mut record);
        if let Err(error) = self.save(&mut record) {
            report_failure(&error);
        }
    }

    /// Records a natural exit unless a stop claimed the task meanwhile.
    async fn settle_exit(&self, task_id: &str, exit: Option<LeaderExit>) {
        let coordination = self.coordination.lock().await;
        if coordination.stopping.contains(task_id) {
            return;
        }
        let Ok(mut record) = self.record(task_id) else {
            return;
        };
        if !matches!(
            record.state,
            TaskState::Running | TaskState::Unknown | TaskState::CleanupFailed
        ) {
            return;
        }
        // A child waiter owns the exit status. Do not race it and lose a known successful exit.
        if record.supervision == "child" && exit.is_none() {
            return;
        }
        record.state = TaskState::Exited;
        record.exit_code = exit.and_then(|exit| exit.code);
        record.exit_signal = exit.and_then(|exit| exit.signal);
        record.detail = Some(match exit {
            Some(exit) => format!("exited ({})", describe_exit(exit)),
            None => "process tree ended; exit status unknown".to_owned(),
        });
        record.ended_at_ms = Some(now_ms());
        append_log_note(
            &record.log_path,
            record.detail.as_deref().unwrap_or_default(),
        );
        let _ = crate::service_log::rotate_if_oversized(Path::new(&record.log_path));
        let result = if record.exit_code == Some(0) || record.stop_reason.is_some() {
            self.remove(&record)
        } else {
            self.save(&mut record)
        };
        if let Err(error) = result {
            report_failure(&error);
        }
        drop(coordination);
    }
}

#[cfg(not(unix))]
impl BackgroundTaskService {
    async fn confirm_gone(&self, _record: &TaskRecord) -> Result<(), String> {
        Err("background task process state is unavailable".to_owned())
    }

    async fn launch(
        self: &Arc<Self>,
        _record: &mut TaskRecord,
        _request: &StartRequest,
    ) -> Result<(), String> {
        Err("managed background tasks are not supported on this operating system".to_owned())
    }

    async fn terminate(&self, record: &mut TaskRecord, _grace: Duration) {
        record.state = TaskState::CleanupFailed;
        record.detail =
            Some("managed background tasks are not supported on this operating system".to_owned());
    }

    async fn reconcile(self: &Arc<Self>) -> bool {
        true
    }

    fn spawn_monitor(self: Arc<Self>, _reconciled: bool) {}
}

/// Whether managed background tasks can run on this platform.
pub(crate) const fn supported() -> bool {
    cfg!(unix)
}

#[cfg(unix)]
async fn wait_until_gone(
    group: i32,
    leader_start: &str,
    tracked: &mut HashMap<i32, String>,
    limit: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if let Ok(table) = os::ProcessTable::capture().await
            && table
                .tracked_task_processes(group, leader_start, tracked)
                .is_empty()
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(unix)]
fn describe_exit(exit: LeaderExit) -> String {
    match (exit.code, exit.signal) {
        (Some(code), _) => format!("code {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "unknown status".to_owned(),
    }
}

/// Exit code of a gated leader whose release never arrived.
#[cfg(unix)]
const UNRELEASED_EXIT_CODE: i32 = 125;

/// A leader that runs `command` with the `bash` tool's shell only after one release line on
/// stdin. End of input before that line exits without running anything.
#[cfg(unix)]
fn gated_shell(
    command: &str,
    environment: &HashMap<String, String>,
) -> (&'static str, Vec<String>) {
    let shell = if environment.contains_key("PATH") {
        "/bin/sh -c"
    } else {
        "sh -lc"
    };
    (
        "/bin/sh",
        vec![
            "-c".to_owned(),
            format!("IFS= read -r _ || exit {UNRELEASED_EXIT_CODE}; exec {shell} \"$0\""),
            command.to_owned(),
        ],
    )
}

fn validate_start(request: &StartRequest) -> Result<(), String> {
    if request.command.trim().is_empty() {
        return Err("command must not be empty".to_owned());
    }
    if request.command.len() > MAX_COMMAND_BYTES || request.command.contains('\0') {
        return Err(format!(
            "command must be at most {MAX_COMMAND_BYTES} bytes without NUL characters"
        ));
    }
    if request.name.trim().is_empty() || request.name.chars().count() > MAX_NAME_CHARS {
        return Err(format!("name must be 1 to {MAX_NAME_CHARS} characters"));
    }
    if request.endpoints.len() > MAX_ENDPOINTS
        || request
            .endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty() || endpoint.len() > MAX_ENDPOINT_BYTES)
    {
        return Err(format!(
            "at most {MAX_ENDPOINTS} non-empty endpoints of up to {MAX_ENDPOINT_BYTES} bytes are allowed"
        ));
    }
    if !request.cwd.is_dir() {
        return Err(format!(
            "working directory {} does not exist",
            request.cwd.display()
        ));
    }
    Ok(())
}

/// The last `limit` bytes of the live log, preceded by the archived generation when the live
/// file alone is shorter. Reports whether earlier content was left out. Reads at most `limit`
/// bytes from each file, however large it is.
fn read_log_tail(log: &Path, limit: u32) -> (String, bool) {
    let limit = u64::from(limit);
    let (live, mut truncated) = read_file_tail(log, limit);
    let mut tail = Vec::new();
    let missing = limit.saturating_sub(u64::try_from(live.len()).unwrap_or(u64::MAX));
    if !truncated && missing > 0 {
        let (archive, archive_truncated) =
            read_file_tail(&crate::service_log::archive_path(log), missing);
        truncated = archive_truncated;
        tail = archive;
    } else if !truncated {
        truncated = crate::service_log::archive_path(log).exists();
    }
    tail.extend_from_slice(&live);
    (String::from_utf8_lossy(&tail).into_owned(), truncated)
}

/// The last `limit` bytes of a file, and whether it holds more. A missing file is empty.
fn read_file_tail(path: &Path, limit: u64) -> (Vec<u8>, bool) {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let Ok(mut file) = std::fs::File::open(path) else {
        return (Vec::new(), false);
    };
    let length = file.metadata().map_or(0, |metadata| metadata.len());
    let start = length.saturating_sub(limit);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return (Vec::new(), false);
    }
    let mut bytes = Vec::new();
    let _ = file.take(limit).read_to_end(&mut bytes);
    (bytes, start > 0)
}

fn append_log_note(log: &str, note: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = crate::service_log::open_for_append(Path::new(log)) {
        let _ = writeln!(file, "[nakode {}] {note}", now_ms());
    }
}

/// A TCP connection attempt from this machine to a declared endpoint's host and port.
async fn probe_endpoint(endpoint: &str) -> &'static str {
    let Some(address) = endpoint_address(endpoint) else {
        return "unknown";
    };
    match tokio::time::timeout(
        Duration::from_millis(500),
        tokio::net::TcpStream::connect(address),
    )
    .await
    {
        Ok(Ok(_)) => "accepting",
        Ok(Err(_)) => "refused",
        Err(_) => "unknown",
    }
}

fn endpoint_address(endpoint: &str) -> Option<String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .map_or(("", endpoint), |(scheme, rest)| (scheme, rest));
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit('@').next()?;
    let has_port = authority.rsplit_once(':').is_some_and(|(_, port)| {
        !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
    }) && !authority.ends_with(']');
    if has_port {
        return Some(authority.to_owned());
    }
    let port = match scheme {
        "http" | "ws" => 80,
        "https" | "wss" => 443,
        _ => return None,
    };
    Some(format!("{authority}:{port}"))
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn report_failure(error: &str) {
    eprintln!("nakode: background tasks: {error}");
}

#[cfg(test)]
mod tests;
