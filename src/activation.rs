//! Durable installation update activation.
//!
//! Installing a binary and activating it are separate operations. When an older installation
//! service owns live work, the newly installed binary runs one detached helper. The helper journals
//! status, serves the public `ActivationService` on its own socket, and retries the existing atomic
//! quiescent-restart fence until the whole service can be replaced safely.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use futures_util::{Stream, StreamExt};
use nakode_sdk::v1 as api;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    config::Config,
    control_service::{
        ActivationLease, ControlError, ExecutableIdentity, ServerReport, ServicePaths,
        ServiceRuntimeRecord, activation_owner_is_alive, bind_service_listener,
        capture_service_output, detach_service_process, executable_identity, installation_config,
        read_runtime_record, restart_service_conditionally, restart_service_quiescent,
        server_report, session_has_live_work, socket_is_live, wait_for_api, write_private_file,
    },
    server::runtime::QuiescenceBlocker,
};

const JOURNAL_SCHEMA_VERSION: u32 = 1;
const CHECK_CADENCE: Duration = Duration::from_secs(15);
const CHECK_CADENCE_MS: u64 = 15_000;
const HELPER_START_GRACE: Duration = Duration::from_secs(5);
const HELPER_HEARTBEAT_STALE_MS: u64 = CHECK_CADENCE_MS * 2 + 5_000;
const BLOCKER_QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const BLOCKER_QUERY_CONCURRENCY: usize = 16;
const WATCH_CADENCE: Duration = Duration::from_millis(250);
const HISTORY_LIMIT: usize = 50;
const IDEMPOTENCY_LIMIT: usize = 50;
const CONDITIONAL_FORCE_CAPABILITY: &str = "ConditionalActivationForce";

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error("failed to read activation state at {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode activation state at {path}: {source}")]
    Decode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("unsupported activation journal schema {0}")]
    UnsupportedSchema(u32),
    #[error("failed to persist activation state at {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("activation helper is already running")]
    HelperAlreadyRunning,
    #[error("activation helper command channel closed")]
    HelperStopped,
    #[error("activation request was rejected: {0}")]
    Rejected(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Current,
    InstalledPending,
    Checking,
    Blocked,
    Activating,
    Forcing,
    Activated,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckTrigger {
    Installed,
    Scheduled,
    Manual,
    Forced,
    Recovered,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckResult {
    Blocked,
    Activated,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InstalledIdentity {
    #[serde(flatten)]
    executable: ExecutableIdentity,
    version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RunningIdentity {
    runtime: ServiceRuntimeRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server: Option<ServerReport>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HelperIdentity {
    pid: u32,
    instance_id: String,
    started_at_unix_ms: u64,
    heartbeat_at_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Blocker {
    session_id: String,
    title: String,
    session_revision: u64,
    activity: i32,
    queue_count: u32,
    reasons: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CheckRecord {
    sequence: u64,
    trigger: CheckTrigger,
    started_at_unix_ms: u64,
    finished_at_unix_ms: u64,
    result: CheckResult,
    blocker_count: u32,
    detail: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Failure {
    message: String,
    retryable: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum IdempotencyRejectionCode {
    Aborted,
    Unimplemented,
    Unavailable,
    Internal,
}

impl IdempotencyRejectionCode {
    fn from_status(status: &Status) -> Option<Self> {
        match status.code() {
            tonic::Code::Aborted => Some(Self::Aborted),
            tonic::Code::Unimplemented => Some(Self::Unimplemented),
            tonic::Code::Unavailable => Some(Self::Unavailable),
            tonic::Code::Internal => Some(Self::Internal),
            _ => None,
        }
    }

    fn status(self, message: String) -> Status {
        match self {
            Self::Aborted => Status::aborted(message),
            Self::Unimplemented => Status::unimplemented(message),
            Self::Unavailable => Status::unavailable(message),
            Self::Internal => Status::internal(message),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IdempotencyRejection {
    key: String,
    code: IdempotencyRejectionCode,
    message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Journal {
    schema_version: u32,
    attempt_id: String,
    revision: u64,
    phase: Phase,
    installed: InstalledIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    running: Option<RunningIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    helper: Option<HelperIdentity>,
    cadence_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_check_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_check_at_unix_ms: Option<u64>,
    #[serde(default)]
    blockers: Vec<Blocker>,
    #[serde(default)]
    history: Vec<CheckRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<Failure>,
    #[serde(default)]
    idempotency_keys: Vec<String>,
    #[serde(default)]
    idempotency_rejections: Vec<IdempotencyRejection>,
}

impl Journal {
    fn new(installed: InstalledIdentity, running: Option<RunningIdentity>) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            attempt_id: Uuid::now_v7().to_string(),
            revision: 0,
            phase: Phase::InstalledPending,
            installed,
            running,
            helper: None,
            cadence_ms: CHECK_CADENCE_MS,
            last_check_at_unix_ms: None,
            next_check_at_unix_ms: Some(now_ms()),
            blockers: Vec::new(),
            history: Vec::new(),
            failure: None,
            idempotency_keys: Vec::new(),
            idempotency_rejections: Vec::new(),
        }
    }

    fn supports_force(&self) -> bool {
        self.running
            .as_ref()
            .and_then(|running| running.server.as_ref())
            .is_some_and(|server| {
                server
                    .capabilities
                    .iter()
                    .any(|capability| capability == CONDITIONAL_FORCE_CAPABILITY)
            })
    }

    fn has_idempotency_key(&self, key: &str) -> bool {
        self.idempotency_keys.iter().any(|seen| seen == key)
    }

    fn remember_idempotency_key(&mut self, key: String) -> bool {
        if key.is_empty() || self.has_idempotency_key(&key) {
            return false;
        }
        self.idempotency_keys.push(key);
        if self.idempotency_keys.len() > IDEMPOTENCY_LIMIT {
            let remove = self.idempotency_keys.len() - IDEMPOTENCY_LIMIT;
            self.idempotency_keys.drain(..remove);
            let retained = self
                .idempotency_keys
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            self.idempotency_rejections
                .retain(|rejection| retained.contains(&rejection.key));
        }
        true
    }

    fn remember_idempotency_rejection(&mut self, key: String, status: &Status) -> bool {
        let Some(code) = IdempotencyRejectionCode::from_status(status) else {
            return false;
        };
        if !self.remember_idempotency_key(key.clone()) {
            return false;
        }
        self.idempotency_rejections.push(IdempotencyRejection {
            key,
            code,
            message: status.message().to_owned(),
        });
        true
    }

    fn remember_accepted_idempotency_rejection(&mut self, key: &str, status: &Status) -> bool {
        let Some(code) = IdempotencyRejectionCode::from_status(status) else {
            return false;
        };
        if key.is_empty()
            || !self.has_idempotency_key(key)
            || self
                .idempotency_rejections
                .iter()
                .any(|rejection| rejection.key == key)
        {
            return false;
        }
        self.idempotency_rejections.push(IdempotencyRejection {
            key: key.to_owned(),
            code,
            message: status.message().to_owned(),
        });
        true
    }

    fn record(
        &mut self,
        trigger: CheckTrigger,
        started_at_unix_ms: u64,
        result: CheckResult,
        detail: impl Into<String>,
    ) {
        let sequence = self
            .history
            .last()
            .map_or(1, |entry| entry.sequence.saturating_add(1));
        self.history.push(CheckRecord {
            sequence,
            trigger,
            started_at_unix_ms,
            finished_at_unix_ms: now_ms(),
            result,
            blocker_count: u32::try_from(self.blockers.len()).unwrap_or(u32::MAX),
            detail: detail.into(),
        });
        if self.history.len() > HISTORY_LIMIT {
            let remove = self.history.len() - HISTORY_LIMIT;
            self.history.drain(..remove);
        }
    }
}

fn now_ms() -> u64 {
    crate::diagnostics::unix_time_ms()
}

fn read_journal(paths: &ServicePaths) -> Result<Option<Journal>, ActivationError> {
    let path = paths.activation_journal();
    let encoded = match std::fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ActivationError::Read {
                path: path.display().to_string(),
                source,
            });
        }
    };
    let journal: Journal =
        serde_json::from_slice(&encoded).map_err(|source| ActivationError::Decode {
            path: path.display().to_string(),
            source,
        })?;
    if journal.schema_version != JOURNAL_SCHEMA_VERSION {
        return Err(ActivationError::UnsupportedSchema(journal.schema_version));
    }
    Ok(Some(journal))
}

fn quarantine_corrupt_journal(paths: &ServicePaths) -> Result<PathBuf, ActivationError> {
    let source = paths.activation_journal();
    let destination = source.with_file_name(format!(
        "activation.corrupt-{}-{}.json",
        now_ms(),
        Uuid::now_v7()
    ));
    std::fs::rename(source, &destination).map_err(|source| ActivationError::Write {
        path: destination.display().to_string(),
        source,
    })?;
    Ok(destination)
}

fn write_journal(paths: &ServicePaths, journal: &mut Journal) -> Result<(), ActivationError> {
    journal.schema_version = JOURNAL_SCHEMA_VERSION;
    journal.revision = journal.revision.saturating_add(1);
    let encoded = serde_json::to_vec_pretty(journal).expect("activation journal is serializable");
    write_private_file(paths.activation_journal(), &encoded).map_err(|source| {
        ActivationError::Write {
            path: paths.activation_journal().display().to_string(),
            source,
        }
    })
}

async fn running_identity(paths: &ServicePaths) -> Option<RunningIdentity> {
    let runtime = read_runtime_record(paths.runtime())?;
    let server = server_report(paths.api()).await;
    Some(RunningIdentity { runtime, server })
}

fn running_service_has_build(running: &RunningIdentity, installed: &ExecutableIdentity) -> bool {
    running.server.is_some()
        && running
            .runtime
            .executable
            .as_ref()
            .is_some_and(|identity| identity.same_build(installed))
}

async fn synthesized_journal(
    paths: &ServicePaths,
    executable: &Path,
) -> Result<Journal, ActivationError> {
    let installed = InstalledIdentity {
        executable: executable_identity(executable)?,
        version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    let running = running_identity(paths).await;
    let is_current = running.as_ref().is_none_or(|running| {
        running.server.is_some()
            && running
                .runtime
                .executable
                .as_ref()
                .is_none_or(|identity| identity.same_build(&installed.executable))
    });
    let unreachable_runtime = running
        .as_ref()
        .is_some_and(|identity| identity.server.is_none());
    let mut journal = Journal::new(installed, running);
    "current".clone_into(&mut journal.attempt_id);
    journal.phase = if is_current {
        Phase::Current
    } else {
        journal.failure = Some(Failure {
            message: if unreachable_runtime {
                "the service runtime record exists, but its API is not reachable; activation state cannot be verified"
                    .to_owned()
            } else {
                "the installed binary differs from the running service, but no activation helper is recorded"
                    .to_owned()
            },
            retryable: true,
        });
        Phase::Failed
    };
    journal.next_check_at_unix_ms = None;
    Ok(journal)
}

async fn status_journal(
    paths: &ServicePaths,
    executable: &Path,
) -> Result<Journal, ActivationError> {
    match read_journal(paths)? {
        Some(journal) => Ok(journal),
        None => synthesized_journal(paths, executable).await,
    }
}

/// Reconciles a pending journal after endpoint discovery proves that its installed build is now
/// the live service. This is also the startup handoff from the helper-owned socket to the API socket.
pub(crate) async fn observe_current_service(
    paths: &ServicePaths,
    installed: &ExecutableIdentity,
    runtime: &ServiceRuntimeRecord,
    server: Option<&ServerReport>,
) -> Result<(), ActivationError> {
    if server.is_none()
        || !runtime
            .executable
            .as_ref()
            .is_some_and(|running| running.same_build(installed))
    {
        return Ok(());
    }
    let _activation = ActivationLease::acquire(paths.activation()).await?;
    let mut journal = match read_journal(paths) {
        Ok(Some(journal)) => journal,
        Ok(None) => return Ok(()),
        Err(ActivationError::Decode { .. }) => {
            let quarantined = quarantine_corrupt_journal(paths)?;
            let started = now_ms();
            let mut recovered = Journal::new(
                InstalledIdentity {
                    executable: installed.clone(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                },
                Some(RunningIdentity {
                    runtime: runtime.clone(),
                    server: server.cloned(),
                }),
            );
            recovered.phase = Phase::Activated;
            recovered.next_check_at_unix_ms = None;
            recovered.record(
                CheckTrigger::Recovered,
                started,
                CheckResult::Activated,
                format!(
                    "endpoint discovery reconstructed current activation state; corrupt journal quarantined at {}",
                    quarantined.display()
                ),
            );
            write_journal(paths, &mut recovered)?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if !journal.installed.executable.same_build(installed) {
        return Ok(());
    }
    if !matches!(journal.phase, Phase::Current | Phase::Activated) {
        let started = journal.last_check_at_unix_ms.unwrap_or_else(now_ms);
        journal.phase = Phase::Activated;
        journal.running = Some(RunningIdentity {
            runtime: runtime.clone(),
            server: server.cloned(),
        });
        journal.blockers.clear();
        journal.failure = None;
        journal.next_check_at_unix_ms = None;
        journal.record(
            CheckTrigger::Recovered,
            started,
            CheckResult::Activated,
            "endpoint discovery verified the installed service",
        );
        write_journal(paths, &mut journal)?;
    }
    Ok(())
}

/// Records a stale running service and makes sure one detached helper will retry activation.
///
/// This function never signals the running service. The helper's first and later checks all use the
/// same quiescent lifecycle fence as an immediate stale-service replacement.
pub(crate) async fn schedule_deferred_activation(
    paths: &ServicePaths,
    executable: &Path,
    detail: impl Into<String>,
) -> Result<(), ActivationError> {
    let detail = detail.into();
    let installed = InstalledIdentity {
        executable: executable_identity(executable)?,
        version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    let running = running_identity(paths).await;
    if running
        .as_ref()
        .is_some_and(|identity| running_service_has_build(identity, &installed.executable))
    {
        return Ok(());
    }

    let activation = ActivationLease::acquire(paths.activation()).await?;
    let (mut journal, existing_attempt, recovered_corruption) = match read_journal(paths) {
        Ok(Some(existing))
            if existing
                .installed
                .executable
                .same_build(&installed.executable) =>
        {
            (existing, true, None)
        }
        Ok(_) => (
            Journal::new(installed.clone(), running.clone()),
            false,
            None,
        ),
        Err(ActivationError::Decode { .. }) => {
            let quarantined = quarantine_corrupt_journal(paths)?;
            (
                Journal::new(installed.clone(), running.clone()),
                false,
                Some(quarantined),
            )
        }
        Err(error) => return Err(error),
    };
    journal.installed = installed;
    journal.running = running;
    if !existing_attempt
        || matches!(
            journal.phase,
            Phase::Current | Phase::Activated | Phase::Cancelled
        )
    {
        let started = now_ms();
        journal.phase = Phase::InstalledPending;
        journal.next_check_at_unix_ms = Some(started);
        journal.failure = Some(Failure {
            message: recovered_corruption.as_ref().map_or_else(
                || detail.clone(),
                |path| {
                    format!(
                        "{detail}; corrupt activation journal was quarantined at {} and the pending attempt was reconstructed",
                        path.display()
                    )
                },
            ),
            retryable: true,
        });
        journal.helper = None;
        if let Some(path) = recovered_corruption {
            journal.record(
                CheckTrigger::Recovered,
                started,
                CheckResult::Failed,
                format!(
                    "reconstructed pending activation after quarantining corrupt journal at {}",
                    path.display()
                ),
            );
        }
        write_journal(paths, &mut journal)?;
    }
    drop(activation);
    ensure_helper_process(paths, executable).await?;
    for _ in 0..40 {
        if tokio::net::UnixStream::connect(paths.activation_api())
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(ActivationError::Rejected(format!(
        "activation helper did not publish {}",
        paths.activation_api().display()
    )))
}

async fn ensure_helper_process(
    paths: &ServicePaths,
    executable: &Path,
) -> Result<(), ActivationError> {
    if helper_lease_is_healthy(paths).await {
        return Ok(());
    }
    reclaim_unhealthy_helper_lock(paths).await;
    let mut command = tokio::process::Command::new(executable);
    command.arg("activation-helper").kill_on_drop(false);
    capture_service_output(&mut command, paths.activation_log());
    detach_service_process(&mut command);
    command.spawn().map_err(|source| ActivationError::Write {
        path: executable.display().to_string(),
        source,
    })?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HelperLeaseIdentity {
    pid: u32,
    instance_id: String,
}

fn helper_lock_identity(path: &Path) -> Option<HelperLeaseIdentity> {
    let encoded = std::fs::read_to_string(path).ok()?;
    let (pid, instance_id) = encoded.trim().split_once(':')?;
    Some(HelperLeaseIdentity {
        pid: pid.parse().ok()?,
        instance_id: instance_id.to_owned(),
    })
}

async fn reclaim_unhealthy_helper_lock(paths: &ServicePaths) {
    if helper_lease_is_healthy(paths).await {
        return;
    }
    let path = paths.activation_helper_lock();
    let Some(observed) = std::fs::read_to_string(path).ok() else {
        return;
    };
    if helper_lease_is_healthy(paths).await {
        return;
    }
    if std::fs::read_to_string(path).is_ok_and(|current| current == observed) {
        let _ = std::fs::remove_file(path);
    }
}

async fn helper_lease_is_healthy(paths: &ServicePaths) -> bool {
    let lock = paths.activation_helper_lock();
    let Some(owner) =
        helper_lock_identity(lock).filter(|owner| activation_owner_is_alive(owner.pid))
    else {
        return false;
    };
    if std::fs::metadata(lock)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age <= HELPER_START_GRACE)
    {
        return true;
    }
    let socket_reachable = socket_is_live(paths.activation_api()).await;
    let heartbeat_recent = read_journal(paths)
        .ok()
        .flatten()
        .and_then(|journal| journal.helper)
        .is_some_and(|helper| helper_heartbeat_is_recent(&helper, &owner, now_ms()));
    socket_reachable && heartbeat_recent
}

fn helper_heartbeat_is_recent(
    helper: &HelperIdentity,
    owner: &HelperLeaseIdentity,
    observed_at_ms: u64,
) -> bool {
    helper.pid == owner.pid
        && helper.instance_id == owner.instance_id
        && observed_at_ms.saturating_sub(helper.heartbeat_at_unix_ms) <= HELPER_HEARTBEAT_STALE_MS
}

struct HelperLease {
    path: PathBuf,
    owner: String,
}

impl HelperLease {
    async fn acquire(paths: &ServicePaths, instance_id: &str) -> Result<Self, ActivationError> {
        use std::io::Write;

        let path = paths.activation_helper_lock();
        for _ in 0..2 {
            let owner = format!("{}:{instance_id}\n", std::process::id());
            let mut options = std::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(path) {
                Ok(mut file) => {
                    file.write_all(owner.as_bytes())
                        .and_then(|()| file.sync_all())
                        .map_err(|source| ActivationError::Write {
                            path: path.display().to_string(),
                            source,
                        })?;
                    return Ok(Self {
                        path: path.to_owned(),
                        owner,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let observed = std::fs::read_to_string(path).ok();
                    if helper_lease_is_healthy(paths).await {
                        return Err(ActivationError::HelperAlreadyRunning);
                    }
                    if let Some(observed) = observed
                        && std::fs::read_to_string(path).is_ok_and(|current| current == observed)
                    {
                        let _ = std::fs::remove_file(path);
                    }
                }
                Err(source) => {
                    return Err(ActivationError::Write {
                        path: path.display().to_string(),
                        source,
                    });
                }
            }
        }
        Err(ActivationError::HelperAlreadyRunning)
    }

    fn still_owns_path(&self) -> bool {
        std::fs::read_to_string(&self.path).is_ok_and(|current| current == self.owner)
    }
}

impl Drop for HelperLease {
    fn drop(&mut self) {
        if self.still_owns_path() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct ActivationSocketLease {
    path: PathBuf,
    owner_path: PathBuf,
    owner: String,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl ActivationSocketLease {
    fn capture(path: &Path, helper_lease: &HelperLease) -> Result<Self, ActivationError> {
        let metadata = std::fs::metadata(path).map_err(|source| ActivationError::Read {
            path: path.display().to_string(),
            source,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                path: path.to_owned(),
                owner_path: helper_lease.path.clone(),
                owner: helper_lease.owner.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Ok(Self {
                path: path.to_owned(),
                owner_path: helper_lease.path.clone(),
                owner: helper_lease.owner.clone(),
            })
        }
    }

    fn still_owns_path(&self) -> bool {
        if !std::fs::read_to_string(&self.owner_path).is_ok_and(|owner| owner == self.owner) {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&self.path)
                .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
        }
        #[cfg(not(unix))]
        {
            self.path.exists()
        }
    }
}

impl Drop for ActivationSocketLease {
    fn drop(&mut self) {
        if self.still_owns_path() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

enum HelperCommand {
    Recheck {
        idempotency_key: String,
        response: oneshot::Sender<Result<api::ActivationStatus, Status>>,
    },
    Force {
        request: api::ForceActivateRequest,
        response: oneshot::Sender<Result<api::ActivationStatus, Status>>,
    },
}

/// Runs the detached singleton activation helper until activation succeeds or is cancelled.
///
/// # Errors
///
/// Returns an error when the helper cannot acquire its isolated installation state, serve its
/// activation endpoint, persist status, check quiescence, or activate the installed service.
pub async fn run_helper(config: Config) -> Result<(), ActivationError> {
    let config = installation_config(&config)?;
    let paths = ServicePaths::of(&config)?;
    let instance_id = Uuid::now_v7().to_string();
    let helper_lease = match HelperLease::acquire(&paths, &instance_id).await {
        Ok(lease) => lease,
        Err(ActivationError::HelperAlreadyRunning) => return Ok(()),
        Err(error) => return Err(error),
    };
    let listener = bind_service_listener(paths.activation_api()).await?;
    let _socket_lease = ActivationSocketLease::capture(paths.activation_api(), &helper_lease)?;
    let executable = std::env::current_exe().map_err(|source| ActivationError::Write {
        path: "current executable".to_owned(),
        source,
    })?;
    install_helper_identity(&paths, &instance_id).await?;

    let (commands, mut command_rx) = mpsc::channel(8);
    let service = ActivationGrpcService::new(paths.clone(), executable.clone(), Some(commands));
    let (stop_tx, stop_rx) = oneshot::channel();
    let incoming = UnixListenerStream::new(listener);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await
    });

    let mut ticker = tokio::time::interval(CHECK_CADENCE);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if !helper_lease.still_owns_path() {
            break;
        }
        let activated = tokio::select! {
            _ = ticker.tick() => {
                if !helper_lease.still_owns_path() {
                    break;
                }
                check_once(&paths, &executable, &config, CheckTrigger::Scheduled).await?
            }
            command = command_rx.recv() => {
                let Some(command) = command else { break; };
                if !helper_lease.still_owns_path() {
                    let error = Status::unavailable(
                        "activation helper ownership changed; rediscover the activation endpoint",
                    );
                    match command {
                        HelperCommand::Recheck { response, .. }
                        | HelperCommand::Force { response, .. } => {
                            let _ = response.send(Err(error));
                        }
                    }
                    break;
                }
                match command {
                    HelperCommand::Recheck { idempotency_key, response } => {
                        let result = manual_recheck(&paths, &executable, &config, idempotency_key).await;
                        let activated = result.as_ref().is_ok_and(|status| {
                            status.phase == api::ActivationPhase::Activated as i32
                        });
                        let _ = response.send(result);
                        activated
                    }
                    HelperCommand::Force { request, response } => {
                        let result = force_activate(&paths, &executable, &config, request).await;
                        let activated = result.as_ref().is_ok_and(|status| {
                            status.phase == api::ActivationPhase::Activated as i32
                        });
                        let _ = response.send(result);
                        activated
                    }
                }
            }
        };
        if activated {
            // A scheduled check can win the select while mutation RPCs are already queued. Resolve
            // those handlers under the activation lease before beginning transport handoff;
            // otherwise graceful gRPC shutdown waits forever on handlers whose command responses
            // are still owned by this receiver. Processing each queued mutation also preserves its
            // idempotency and audit semantics instead of returning an unrecorded terminal success.
            resolve_queued_terminal_commands(&paths, &mut command_rx).await?;
            // Give current watches time to receive the terminal snapshot. Clients then rediscover
            // the API socket, where the replacement service serves this same public contract.
            tokio::time::sleep(Duration::from_millis(500)).await;
            break;
        }
    }
    let _ = stop_tx.send(());
    let _ = server.await;
    Ok(())
}

async fn resolve_queued_terminal_commands(
    paths: &ServicePaths,
    commands: &mut mpsc::Receiver<HelperCommand>,
) -> Result<(), ActivationError> {
    commands.close();
    let _activation = ActivationLease::acquire(paths.activation()).await?;
    while let Ok(command) = commands.try_recv() {
        match command {
            HelperCommand::Recheck {
                idempotency_key,
                response,
            } => {
                let _ = response.send(complete_terminal_recheck(paths, idempotency_key));
            }
            HelperCommand::Force { request, response } => {
                let _ = response.send(complete_terminal_force(paths, request));
            }
        }
    }
    Ok(())
}

fn complete_terminal_recheck(
    paths: &ServicePaths,
    idempotency_key: String,
) -> Result<api::ActivationStatus, Status> {
    let mut journal = read_required_journal(paths).map_err(|error| internal_status(&error))?;
    if !matches!(journal.phase, Phase::Activated) {
        return Err(Status::unavailable(
            "activation changed before the queued recheck could run; rediscover the endpoint",
        ));
    }
    match idempotency_replay(&journal, &idempotency_key)? {
        IdempotencyReplay::New => {}
        IdempotencyReplay::Accepted => return Ok(journal_to_api(journal)),
        IdempotencyReplay::Rejected(error) => return Err(error),
    }
    remember_idempotency_acceptance(&mut journal, idempotency_key)?;
    let completed = now_ms();
    journal.record(
        CheckTrigger::Manual,
        completed,
        CheckResult::Activated,
        "activation completed before the queued recheck ran",
    );
    write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
    Ok(journal_to_api(journal))
}

fn complete_terminal_force(
    paths: &ServicePaths,
    request: api::ForceActivateRequest,
) -> Result<api::ActivationStatus, Status> {
    let mut journal = read_required_journal(paths).map_err(|error| internal_status(&error))?;
    if !matches!(journal.phase, Phase::Activated) {
        return Err(Status::unavailable(
            "activation changed before the queued force request could run; rediscover the endpoint",
        ));
    }
    match idempotency_replay(&journal, &request.idempotency_key)? {
        IdempotencyReplay::New => {}
        IdempotencyReplay::Accepted => return Ok(journal_to_api(journal)),
        IdempotencyReplay::Rejected(error) => return Err(error),
    }
    let completed = now_ms();
    if !journal.supports_force() {
        let error = Status::unimplemented(
            "the running service does not advertise conditional activation force",
        );
        journal.record(
            CheckTrigger::Forced,
            completed,
            CheckResult::Cancelled,
            error.message(),
        );
        remember_idempotency_rejection(&mut journal, request.idempotency_key, &error)?;
        write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
        return Err(error);
    }
    if let Err(error) = validate_force_fence(&journal, &request) {
        journal.record(
            CheckTrigger::Forced,
            completed,
            CheckResult::Cancelled,
            error.message(),
        );
        remember_idempotency_rejection(&mut journal, request.idempotency_key, &error)?;
        write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
        return Err(error);
    }
    remember_idempotency_acceptance(&mut journal, request.idempotency_key)?;
    journal.record(
        CheckTrigger::Forced,
        completed,
        CheckResult::Activated,
        "activation completed before the queued conditional force ran",
    );
    write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
    Ok(journal_to_api(journal))
}

async fn install_helper_identity(
    paths: &ServicePaths,
    instance_id: &str,
) -> Result<(), ActivationError> {
    let _activation = ActivationLease::acquire(paths.activation()).await?;
    let mut journal = read_journal(paths)?.ok_or_else(|| {
        ActivationError::Rejected("activation helper has no pending journal".to_owned())
    })?;
    let now = now_ms();
    journal.helper = Some(HelperIdentity {
        pid: std::process::id(),
        instance_id: instance_id.to_owned(),
        started_at_unix_ms: now,
        heartbeat_at_unix_ms: now,
    });
    journal.failure = None;
    write_journal(paths, &mut journal)
}

async fn manual_recheck(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    idempotency_key: String,
) -> Result<api::ActivationStatus, Status> {
    let _activation = ActivationLease::acquire(paths.activation())
        .await
        .map_err(|error| Status::aborted(error.to_string()))?;
    let mut journal = read_journal(paths)
        .map_err(|error| internal_status(&error))?
        .ok_or_else(|| Status::failed_precondition("no activation is pending"))?;
    match idempotency_replay(&journal, &idempotency_key)? {
        IdempotencyReplay::New => {}
        IdempotencyReplay::Accepted => return Ok(journal_to_api(journal)),
        IdempotencyReplay::Rejected(error) => return Err(error),
    }
    remember_idempotency_acceptance(&mut journal, idempotency_key.clone())?;
    write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
    if let Err(error) = check_once_fenced(paths, executable, config, CheckTrigger::Manual).await {
        let rejection = internal_status(&error);
        persist_accepted_execution_rejection(paths, &idempotency_key, &rejection)?;
        return Err(rejection);
    }
    match status_journal(paths, executable).await {
        Ok(status) => Ok(journal_to_api(status)),
        Err(error) => {
            let rejection = internal_status(&error);
            persist_accepted_execution_rejection(paths, &idempotency_key, &rejection)?;
            Err(rejection)
        }
    }
}

async fn force_activate(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    request: api::ForceActivateRequest,
) -> Result<api::ActivationStatus, Status> {
    let _activation = ActivationLease::acquire(paths.activation())
        .await
        .map_err(|error| Status::aborted(error.to_string()))?;
    let mut journal = read_journal(paths)
        .map_err(|error| internal_status(&error))?
        .ok_or_else(|| Status::failed_precondition("no activation is pending"))?;
    match idempotency_replay(&journal, &request.idempotency_key)? {
        IdempotencyReplay::New => {}
        IdempotencyReplay::Accepted => return Ok(journal_to_api(journal)),
        IdempotencyReplay::Rejected(error) => return Err(error),
    }
    let started = now_ms();
    if !journal.supports_force() {
        let error = Status::unimplemented(
            "the running service does not advertise conditional activation force",
        );
        journal.record(
            CheckTrigger::Forced,
            started,
            CheckResult::Cancelled,
            error.message(),
        );
        remember_idempotency_rejection(&mut journal, request.idempotency_key.clone(), &error)?;
        write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
        return Err(error);
    }
    let expected = match validate_force_fence(&journal, &request) {
        Ok(expected) => expected,
        Err(error) => {
            journal.record(
                CheckTrigger::Forced,
                started,
                CheckResult::Cancelled,
                error.message(),
            );
            remember_idempotency_rejection(&mut journal, request.idempotency_key.clone(), &error)?;
            write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;
            return Err(error);
        }
    };
    let idempotency_key = request.idempotency_key.clone();
    remember_idempotency_acceptance(&mut journal, request.idempotency_key)?;

    journal.phase = Phase::Forcing;
    journal.failure = None;
    journal.next_check_at_unix_ms = None;
    heartbeat(&mut journal);
    write_journal(paths, &mut journal).map_err(|error| internal_status(&error))?;

    let expected_runtime = expected
        .into_iter()
        .map(|(session_id, session_revision)| QuiescenceBlocker {
            session_id,
            session_revision,
        })
        .collect();
    perform_conditional_force(
        paths,
        executable,
        config,
        expected_runtime,
        &idempotency_key,
        started,
    )
    .await?;
    match status_journal(paths, executable).await {
        Ok(status) => Ok(journal_to_api(status)),
        Err(error) => {
            let rejection = internal_status(&error);
            persist_accepted_execution_rejection(paths, &idempotency_key, &rejection)?;
            Err(rejection)
        }
    }
}

async fn perform_conditional_force(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    expected_runtime: Vec<QuiescenceBlocker>,
    idempotency_key: &str,
    started: u64,
) -> Result<(), Status> {
    match restart_service_conditionally(executable, config, expected_runtime).await {
        Ok(()) => {
            let activated = match finish_activation_or_record_failure(
                paths,
                CheckTrigger::Forced,
                started,
                finish_activation(paths, executable, config, CheckTrigger::Forced, started),
            )
            .await
            {
                Ok(activated) => activated,
                Err(error) => {
                    let rejection = internal_status(&error);
                    persist_accepted_execution_rejection(paths, idempotency_key, &rejection)?;
                    return Err(rejection);
                }
            };
            if !activated {
                let journal =
                    read_required_journal(paths).map_err(|error| internal_status(&error))?;
                let message = journal.failure.map_or_else(
                    || "replacement service could not be verified".to_owned(),
                    |failure| failure.message,
                );
                let rejection = Status::unavailable(message);
                persist_accepted_execution_rejection(paths, idempotency_key, &rejection)?;
                return Err(rejection);
            }
        }
        Err(error) => {
            let blockers = collect_blockers(paths, config).await.unwrap_or_default();
            record_retryable_activation_failure(
                paths,
                CheckTrigger::Forced,
                started,
                blockers,
                error.to_string(),
            )
            .map_err(|error| internal_status(&error))?;
            let rejection = Status::aborted(error.to_string());
            persist_accepted_execution_rejection(paths, idempotency_key, &rejection)?;
            return Err(rejection);
        }
    }
    Ok(())
}

#[derive(Debug)]
enum IdempotencyReplay {
    New,
    Accepted,
    Rejected(Status),
}

fn idempotency_replay(journal: &Journal, key: &str) -> Result<IdempotencyReplay, Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument(
            "idempotency_key must not be empty",
        ));
    }
    if !journal.has_idempotency_key(key) {
        return Ok(IdempotencyReplay::New);
    }
    if let Some(rejection) = journal
        .idempotency_rejections
        .iter()
        .find(|rejection| rejection.key == key)
    {
        return Ok(IdempotencyReplay::Rejected(
            rejection.code.status(rejection.message.clone()),
        ));
    }
    Ok(IdempotencyReplay::Accepted)
}

fn remember_idempotency_acceptance(
    journal: &mut Journal,
    idempotency_key: String,
) -> Result<(), Status> {
    if journal.remember_idempotency_key(idempotency_key) {
        Ok(())
    } else {
        Err(Status::internal(
            "idempotency key changed while the activation mutation lease was held",
        ))
    }
}

fn remember_idempotency_rejection(
    journal: &mut Journal,
    idempotency_key: String,
    rejection: &Status,
) -> Result<(), Status> {
    if journal.remember_idempotency_rejection(idempotency_key, rejection) {
        Ok(())
    } else {
        Err(Status::internal(
            "idempotency rejection changed while the activation mutation lease was held",
        ))
    }
}

fn persist_accepted_execution_rejection(
    paths: &ServicePaths,
    idempotency_key: &str,
    rejection: &Status,
) -> Result<(), Status> {
    let mut journal = read_required_journal(paths).map_err(|error| internal_status(&error))?;
    if !journal.remember_accepted_idempotency_rejection(idempotency_key, rejection) {
        return Err(Status::internal(
            "accepted idempotency outcome changed while the activation mutation lease was held",
        ));
    }
    write_journal(paths, &mut journal).map_err(|error| internal_status(&error))
}

fn validate_force_fence(
    journal: &Journal,
    request: &api::ForceActivateRequest,
) -> Result<BTreeSet<(String, u64)>, Status> {
    if journal.attempt_id != request.expected_attempt_id {
        return Err(Status::aborted(format!(
            "activation attempt changed (expected {}, current {})",
            request.expected_attempt_id, journal.attempt_id
        )));
    }
    if journal.revision != request.expected_activation_revision {
        return Err(Status::aborted(format!(
            "activation status changed (expected revision {}, current revision {})",
            request.expected_activation_revision, journal.revision
        )));
    }
    let expected = normalized_blocker_set(&request.expected_blockers);
    let current = normalized_blocker_set(
        &journal
            .blockers
            .iter()
            .cloned()
            .map(blocker_to_api)
            .collect::<Vec<_>>(),
    );
    if expected != current {
        return Err(Status::aborted(
            "the confirmed blocker set does not match current activation status",
        ));
    }
    Ok(expected)
}

fn normalized_blocker_set(blockers: &[api::ActivationBlocker]) -> BTreeSet<(String, u64)> {
    blockers
        .iter()
        .map(|blocker| (blocker.session_id.clone(), blocker.session_revision))
        .collect()
}

async fn check_once(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    trigger: CheckTrigger,
) -> Result<bool, ActivationError> {
    let _activation = ActivationLease::acquire(paths.activation()).await?;
    check_once_fenced(paths, executable, config, trigger).await
}

fn fail_changed_installed_executable(
    paths: &ServicePaths,
    mut journal: Journal,
    trigger: CheckTrigger,
    started: u64,
) -> Result<bool, ActivationError> {
    journal.phase = Phase::Failed;
    journal.failure = Some(Failure {
        message: "the installed executable changed after this activation attempt was recorded"
            .to_owned(),
        retryable: false,
    });
    journal.next_check_at_unix_ms = None;
    journal.record(
        trigger,
        started,
        CheckResult::Failed,
        "installed executable identity changed",
    );
    heartbeat(&mut journal);
    write_journal(paths, &mut journal)?;
    Ok(false)
}

async fn check_once_fenced(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    trigger: CheckTrigger,
) -> Result<bool, ActivationError> {
    let started = now_ms();
    let mut journal = read_required_journal(paths)?;
    let installed_now = executable_identity(executable)?;
    if !installed_now.same_build(&journal.installed.executable) {
        return fail_changed_installed_executable(paths, journal, trigger, started);
    }

    journal.phase = Phase::Checking;
    journal.last_check_at_unix_ms = Some(started);
    journal.next_check_at_unix_ms = None;
    journal.failure = None;
    heartbeat(&mut journal);
    write_journal(paths, &mut journal)?;

    journal.running = running_identity(paths).await;
    if journal
        .running
        .as_ref()
        .is_some_and(|running| running_service_has_build(running, &journal.installed.executable))
    {
        journal.phase = Phase::Activated;
        journal.blockers.clear();
        journal.next_check_at_unix_ms = None;
        journal.record(
            trigger,
            started,
            CheckResult::Activated,
            "installed service was already active",
        );
        write_journal(paths, &mut journal)?;
        return Ok(true);
    }

    let running_api_is_reachable = journal
        .running
        .as_ref()
        .is_some_and(|running| running.server.is_some());
    if !running_api_is_reachable
        && !socket_is_live(paths.lifecycle()).await
        && runtime_owner_is_disproven(paths)
    {
        journal.phase = Phase::Activating;
        journal.blockers.clear();
        heartbeat(&mut journal);
        write_journal(paths, &mut journal)?;
        return activate_quiescent_service(paths, executable, config, trigger, started).await;
    }

    match collect_blockers(paths, config).await {
        Ok(blockers) if !blockers.is_empty() => {
            journal.phase = Phase::Blocked;
            journal.blockers = blockers;
            journal.next_check_at_unix_ms = Some(now_ms().saturating_add(CHECK_CADENCE_MS));
            journal.record(
                trigger,
                started,
                CheckResult::Blocked,
                "activation is waiting for whole-service quiescence",
            );
            heartbeat(&mut journal);
            write_journal(paths, &mut journal)?;
            Ok(false)
        }
        Ok(_) => {
            journal.phase = Phase::Activating;
            journal.blockers.clear();
            heartbeat(&mut journal);
            write_journal(paths, &mut journal)?;
            activate_quiescent_service(paths, executable, config, trigger, started).await
        }
        Err(error) => {
            journal.phase = Phase::Failed;
            journal.failure = Some(Failure {
                message: error.clone(),
                retryable: true,
            });
            journal.next_check_at_unix_ms = Some(now_ms().saturating_add(CHECK_CADENCE_MS));
            journal.record(trigger, started, CheckResult::Failed, error);
            heartbeat(&mut journal);
            write_journal(paths, &mut journal)?;
            Ok(false)
        }
    }
}

fn runtime_owner_is_disproven(paths: &ServicePaths) -> bool {
    match read_runtime_record(paths.runtime()) {
        Some(runtime) => !activation_owner_is_alive(runtime.pid),
        None => !paths.runtime().exists(),
    }
}

async fn activate_quiescent_service(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    trigger: CheckTrigger,
    started: u64,
) -> Result<bool, ActivationError> {
    match restart_service_quiescent(executable, config).await {
        Ok(()) => {
            finish_activation_or_record_failure(
                paths,
                trigger,
                started,
                finish_activation(paths, executable, config, trigger, started),
            )
            .await
        }
        Err(error) => {
            let blockers = collect_blockers(paths, config).await.unwrap_or_default();
            record_retryable_activation_failure(
                paths,
                trigger,
                started,
                blockers,
                error.to_string(),
            )
        }
    }
}

async fn finish_activation_or_record_failure(
    paths: &ServicePaths,
    trigger: CheckTrigger,
    started: u64,
    completion: impl std::future::Future<Output = Result<(), ActivationError>>,
) -> Result<bool, ActivationError> {
    match completion.await {
        Ok(()) => Ok(true),
        Err(error) => record_retryable_activation_failure(
            paths,
            trigger,
            started,
            Vec::new(),
            error.to_string(),
        ),
    }
}

fn record_retryable_activation_failure(
    paths: &ServicePaths,
    trigger: CheckTrigger,
    started: u64,
    blockers: Vec<Blocker>,
    message: String,
) -> Result<bool, ActivationError> {
    let mut journal = read_required_journal(paths)?;
    journal.blockers = blockers;
    journal.phase = if journal.blockers.is_empty() {
        Phase::Failed
    } else {
        Phase::Blocked
    };
    journal.failure = Some(Failure {
        message: message.clone(),
        retryable: true,
    });
    journal.next_check_at_unix_ms = Some(now_ms().saturating_add(CHECK_CADENCE_MS));
    journal.record(trigger, started, CheckResult::Failed, message);
    heartbeat(&mut journal);
    write_journal(paths, &mut journal)?;
    Ok(false)
}

async fn finish_activation(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    trigger: CheckTrigger,
    started: u64,
) -> Result<(), ActivationError> {
    wait_for_api(paths.api(), &config.workspace).await?;
    let running = running_identity(paths).await;
    let expected = executable_identity(executable)?;
    if !running
        .as_ref()
        .is_some_and(|running| running_service_has_build(running, &expected))
    {
        return Err(ActivationError::Rejected(
            "replacement service became ready with an unexpected executable identity".to_owned(),
        ));
    }
    let mut journal = read_required_journal(paths)?;
    journal.running = running;
    journal.phase = Phase::Activated;
    journal.blockers.clear();
    journal.failure = None;
    journal.next_check_at_unix_ms = None;
    journal.record(
        trigger,
        started,
        CheckResult::Activated,
        "installed binary activated and verified",
    );
    heartbeat(&mut journal);
    write_journal(paths, &mut journal)
}

fn heartbeat(journal: &mut Journal) {
    if let Some(helper) = journal.helper.as_mut() {
        helper.heartbeat_at_unix_ms = now_ms();
    }
}

fn read_required_journal(paths: &ServicePaths) -> Result<Journal, ActivationError> {
    read_journal(paths)?.ok_or_else(|| {
        ActivationError::Rejected("activation helper has no pending journal".to_owned())
    })
}

async fn collect_blockers(paths: &ServicePaths, config: &Config) -> Result<Vec<Blocker>, String> {
    tokio::time::timeout(
        BLOCKER_QUERY_TIMEOUT,
        collect_blockers_unbounded(paths, config),
    )
    .await
    .map_err(|_| {
        format!(
            "timed out after {} seconds while querying whole-service activation blockers",
            BLOCKER_QUERY_TIMEOUT.as_secs()
        )
    })?
}

async fn collect_blockers_unbounded(
    paths: &ServicePaths,
    config: &Config,
) -> Result<Vec<Blocker>, String> {
    let client = nakode_sdk::NakodeClient::connect_unix(paths.api().to_owned())
        .await
        .map_err(|error| error.to_string())?;
    let workspace = client
        .get_workspace(config.workspace.to_string_lossy(), None)
        .await
        .map_err(|error| error.to_string())?;
    let mut blockers = Vec::new();
    // Workspace summaries are a projection and their `running` bit can lag the authoritative
    // session snapshot during turn startup. Inspect every listed session so a stale summary can
    // never make the helper attempt cutover. Bound parallelism and the enclosing deadline keep
    // large inventories from serially consuming the helper's activation lease.
    let inspections = futures_util::stream::iter(workspace.sessions).map(|summary| {
        let client = client.clone();
        async move {
            let session_id = summary.id;
            match client.get_session(session_id.clone()).await {
                Ok(state) => Ok(Some(state)),
                Err(nakode_sdk::SdkError::Status(status))
                    if status.code() == tonic::Code::NotFound =>
                {
                    Ok(None)
                }
                Err(error) => Err(format!("failed to inspect session {session_id}: {error}")),
            }
        }
    });
    let mut inspections = inspections.buffer_unordered(BLOCKER_QUERY_CONCURRENCY);
    while let Some(state) = inspections.next().await {
        let Some(state) = state? else {
            continue;
        };
        if !session_has_live_work(&state) {
            continue;
        }
        let mut reasons = Vec::new();
        if state.activity != api::SessionActivity::Idle as i32 {
            reasons.push(activity_reason(state.activity).to_owned());
        }
        if !state.queue.is_empty() {
            reasons.push(format!("{} queued prompt(s)", state.queue.len()));
        }
        if state
            .interactions
            .iter()
            .any(|interaction| interaction.status == api::InteractionStatus::Pending as i32)
        {
            reasons.push("waiting for an owner interaction".to_owned());
        }
        if reasons.is_empty() {
            reasons.push("service-owned live work".to_owned());
        }
        blockers.push(Blocker {
            session_id: state.id,
            title: state.title,
            session_revision: state.revision,
            activity: state.activity,
            queue_count: u32::try_from(state.queue.len()).unwrap_or(u32::MAX),
            reasons,
        });
    }
    blockers.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    Ok(blockers)
}

fn activity_reason(activity: i32) -> &'static str {
    match api::SessionActivity::try_from(activity) {
        Ok(api::SessionActivity::CreatingAgentSession) => "creating a provider session",
        Ok(api::SessionActivity::StartingTurn) => "starting an active turn",
        Ok(api::SessionActivity::RunningTurn) => "running an active turn",
        Ok(api::SessionActivity::CompactingContext) => "compacting context",
        Ok(api::SessionActivity::RunningDelegates) => "running delegated work",
        Ok(api::SessionActivity::RunningShell) => "running a shell process",
        Ok(api::SessionActivity::Idle | api::SessionActivity::Unspecified) | Err(_) => {
            "service-owned live work"
        }
    }
}

fn activation_status_follows(
    last_attempt_id: &str,
    last_revision: u64,
    status: &api::ActivationStatus,
) -> bool {
    last_attempt_id.is_empty()
        || status.attempt_id != last_attempt_id
        || status.revision > last_revision
}

fn internal_status(error: &ActivationError) -> Status {
    Status::internal(error.to_string())
}

#[derive(Clone)]
pub(crate) struct ActivationGrpcService {
    paths: ServicePaths,
    executable: PathBuf,
    commands: Option<mpsc::Sender<HelperCommand>>,
}

impl ActivationGrpcService {
    pub(crate) fn read_only(paths: ServicePaths, executable: PathBuf) -> Self {
        Self::new(paths, executable, None)
    }

    fn new(
        paths: ServicePaths,
        executable: PathBuf,
        commands: Option<mpsc::Sender<HelperCommand>>,
    ) -> Self {
        Self {
            paths,
            executable,
            commands,
        }
    }

    pub(crate) fn into_server(
        self,
    ) -> api::activation_service_server::ActivationServiceServer<Self> {
        api::activation_service_server::ActivationServiceServer::new(self)
    }

    async fn status(&self) -> Result<api::ActivationStatus, Status> {
        status_journal(&self.paths, &self.executable)
            .await
            .map(journal_to_api)
            .map_err(|error| internal_status(&error))
    }
}

#[tonic::async_trait]
impl api::activation_service_server::ActivationService for ActivationGrpcService {
    async fn get_activation_status(
        &self,
        _request: Request<api::GetActivationStatusRequest>,
    ) -> Result<Response<api::ActivationStatus>, Status> {
        Ok(Response::new(self.status().await?))
    }

    type WatchActivationStatusStream =
        Pin<Box<dyn Stream<Item = Result<api::ActivationStatus, Status>> + Send + 'static>>;

    async fn watch_activation_status(
        &self,
        request: Request<api::WatchActivationStatusRequest>,
    ) -> Result<Response<Self::WatchActivationStatusStream>, Status> {
        let request = request.into_inner();
        let after_revision = request.after_revision.unwrap_or(0);
        let after_attempt_id = request.after_attempt_id;
        let service = self.clone();
        let helper_owned = service.commands.is_some();
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut last_revision = after_revision;
            let mut last_attempt_id = after_attempt_id;
            loop {
                match service.status().await {
                    Ok(status)
                        if activation_status_follows(&last_attempt_id, last_revision, &status) =>
                    {
                        last_revision = status.revision;
                        last_attempt_id.clone_from(&status.attempt_id);
                        let terminal =
                            helper_owned && status.phase == api::ActivationPhase::Activated as i32;
                        if tx.send(Ok(status)).await.is_err() || terminal {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                }
                tokio::time::sleep(WATCH_CADENCE).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn force_activation_recheck(
        &self,
        request: Request<api::ActivationMutationRequest>,
    ) -> Result<Response<api::ActivationStatus>, Status> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "activation mutations must use the helper endpoint advertised by endpoint discovery",
            )
        })?;
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send(HelperCommand::Recheck {
                idempotency_key: request.into_inner().idempotency_key,
                response: response_tx,
            })
            .await
            .map_err(|_| Status::unavailable("activation helper stopped"))?;
        response_rx
            .await
            .map_err(|_| Status::unavailable("activation helper stopped"))?
            .map(Response::new)
    }

    async fn force_activate(
        &self,
        request: Request<api::ForceActivateRequest>,
    ) -> Result<Response<api::ActivationStatus>, Status> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "activation mutations must use the helper endpoint advertised by endpoint discovery",
            )
        })?;
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send(HelperCommand::Force {
                request: request.into_inner(),
                response: response_tx,
            })
            .await
            .map_err(|_| Status::unavailable("activation helper stopped"))?;
        response_rx
            .await
            .map_err(|_| Status::unavailable("activation helper stopped"))?
            .map(Response::new)
    }
}

fn journal_to_api(journal: Journal) -> api::ActivationStatus {
    let supports_force = journal.supports_force();
    api::ActivationStatus {
        schema_version: journal.schema_version,
        attempt_id: journal.attempt_id,
        revision: journal.revision,
        phase: phase_to_api(journal.phase) as i32,
        installed: Some(installed_to_api(journal.installed)),
        running: journal.running.map(running_to_api),
        helper: journal.helper.map(|helper| api::ActivationHelperIdentity {
            pid: helper.pid,
            instance_id: helper.instance_id,
            started_at_unix_ms: helper.started_at_unix_ms,
            heartbeat_at_unix_ms: helper.heartbeat_at_unix_ms,
        }),
        cadence_ms: journal.cadence_ms,
        last_check_at_unix_ms: journal.last_check_at_unix_ms,
        next_check_at_unix_ms: journal.next_check_at_unix_ms,
        blockers: journal.blockers.into_iter().map(blocker_to_api).collect(),
        history: journal
            .history
            .into_iter()
            .map(|record| api::ActivationCheckRecord {
                sequence: record.sequence,
                trigger: trigger_to_api(record.trigger) as i32,
                started_at_unix_ms: record.started_at_unix_ms,
                finished_at_unix_ms: record.finished_at_unix_ms,
                result: result_to_api(record.result) as i32,
                blocker_count: record.blocker_count,
                detail: record.detail,
            })
            .collect(),
        failure: journal.failure.map(|failure| api::ActivationFailure {
            message: failure.message,
            retryable: failure.retryable,
        }),
        supports_force,
    }
}

fn installed_to_api(installed: InstalledIdentity) -> api::ActivationExecutableIdentity {
    let identity = installed.executable;
    api::ActivationExecutableIdentity {
        path: identity.path.to_string_lossy().into_owned(),
        sha256: identity.sha256,
        size: identity.size,
        modified_at_unix_ms: identity.modified_at_unix_ms,
        device: identity.device,
        inode: identity.inode,
        version: installed.version,
    }
}

fn running_to_api(running: RunningIdentity) -> api::ActivationRunningService {
    let runtime = running.runtime;
    let version = runtime.version;
    let executable_version = version.clone();
    let (api_version, capabilities) = running.server.map_or_else(
        || (None, Vec::new()),
        |server| (Some(server.api_version), server.capabilities),
    );
    api::ActivationRunningService {
        pid: runtime.pid,
        started_at_unix_ms: runtime.started_at_unix_ms,
        version,
        executable: runtime.executable.map(|executable| {
            installed_to_api(InstalledIdentity {
                executable,
                version: executable_version,
            })
        }),
        api_version,
        capabilities,
    }
}

fn blocker_to_api(blocker: Blocker) -> api::ActivationBlocker {
    api::ActivationBlocker {
        session_id: blocker.session_id,
        title: blocker.title,
        session_revision: blocker.session_revision,
        activity: blocker.activity,
        queue_count: blocker.queue_count,
        reasons: blocker.reasons,
    }
}

const fn phase_to_api(phase: Phase) -> api::ActivationPhase {
    match phase {
        Phase::Current => api::ActivationPhase::Current,
        Phase::InstalledPending => api::ActivationPhase::InstalledPending,
        Phase::Checking => api::ActivationPhase::Checking,
        Phase::Blocked => api::ActivationPhase::Blocked,
        Phase::Activating => api::ActivationPhase::Activating,
        Phase::Forcing => api::ActivationPhase::Forcing,
        Phase::Activated => api::ActivationPhase::Activated,
        Phase::Failed => api::ActivationPhase::Failed,
        Phase::Cancelled => api::ActivationPhase::Cancelled,
    }
}

const fn trigger_to_api(trigger: CheckTrigger) -> api::ActivationCheckTrigger {
    match trigger {
        CheckTrigger::Installed => api::ActivationCheckTrigger::Installed,
        CheckTrigger::Scheduled => api::ActivationCheckTrigger::Scheduled,
        CheckTrigger::Manual => api::ActivationCheckTrigger::Manual,
        CheckTrigger::Forced => api::ActivationCheckTrigger::Forced,
        CheckTrigger::Recovered => api::ActivationCheckTrigger::Recovered,
    }
}

const fn result_to_api(result: CheckResult) -> api::ActivationCheckResult {
    match result {
        CheckResult::Blocked => api::ActivationCheckResult::Blocked,
        CheckResult::Activated => api::ActivationCheckResult::Activated,
        CheckResult::Failed => api::ActivationCheckResult::Failed,
        CheckResult::Cancelled => api::ActivationCheckResult::Cancelled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(hash: &str) -> ExecutableIdentity {
        ExecutableIdentity {
            path: PathBuf::from("/tmp/nakode"),
            sha256: hash.to_owned(),
            size: 100,
            modified_at_unix_ms: Some(1),
            device: Some("1".to_owned()),
            inode: Some("2".to_owned()),
        }
    }

    fn journal() -> Journal {
        Journal::new(
            InstalledIdentity {
                executable: identity("new"),
                version: "1.0.0".to_owned(),
            },
            None,
        )
    }

    fn terminal_journal(supports_force: bool) -> Journal {
        let mut journal = journal();
        journal.phase = Phase::Activated;
        journal.next_check_at_unix_ms = None;
        journal.running = Some(RunningIdentity {
            runtime: ServiceRuntimeRecord {
                pid: 42,
                started_at_unix_ms: 10,
                version: "1.0.0".to_owned(),
                workspace: None,
                executable: Some(identity("new")),
            },
            server: Some(ServerReport {
                server_version: "1.0.0".to_owned(),
                api_version: "nakode.v1".to_owned(),
                capabilities: supports_force
                    .then(|| CONDITIONAL_FORCE_CAPABILITY.to_owned())
                    .into_iter()
                    .collect(),
            }),
        });
        journal
    }

    #[test]
    fn changed_executable_failure_keeps_the_live_helper_heartbeat_authoritative() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut pending = journal();
        pending.helper = Some(HelperIdentity {
            pid: std::process::id(),
            instance_id: "helper-a".to_owned(),
            started_at_unix_ms: 1,
            heartbeat_at_unix_ms: 1,
        });

        assert!(
            !fail_changed_installed_executable(&paths, pending, CheckTrigger::Scheduled, now_ms(),)
                .expect("record changed executable")
        );

        let failed = read_required_journal(&paths).expect("failed journal");
        assert_eq!(failed.phase, Phase::Failed);
        assert!(
            failed
                .helper
                .is_some_and(|helper| helper.heartbeat_at_unix_ms > 1)
        );
    }

    #[tokio::test]
    async fn post_cutover_verification_failures_are_retryable_and_never_stay_in_progress() {
        for (pending_phase, trigger) in [
            (Phase::Activating, CheckTrigger::Scheduled),
            (Phase::Forcing, CheckTrigger::Forced),
        ] {
            let directory = tempfile::tempdir().expect("activation directory");
            let paths = ServicePaths::in_directory(directory.path());
            let mut pending = journal();
            pending.phase = pending_phase;
            pending.helper = Some(HelperIdentity {
                pid: std::process::id(),
                instance_id: "helper-a".to_owned(),
                started_at_unix_ms: 1,
                heartbeat_at_unix_ms: 1,
            });
            write_journal(&paths, &mut pending).expect("pending journal");

            let activated = finish_activation_or_record_failure(&paths, trigger, now_ms(), async {
                Err(ActivationError::Rejected(
                    "replacement identity did not match".to_owned(),
                ))
            })
            .await
            .expect("verification failure is recorded");

            assert!(!activated);
            let failed = read_required_journal(&paths).expect("failed journal");
            assert_eq!(failed.phase, Phase::Failed);
            assert!(failed.next_check_at_unix_ms.is_some());
            assert!(failed.failure.is_some_and(|failure| {
                failure.retryable && failure.message.contains("identity did not match")
            }));
            assert_eq!(
                failed.history.last().map(|record| record.result),
                Some(CheckResult::Failed)
            );
            assert!(
                failed
                    .helper
                    .is_some_and(|helper| helper.heartbeat_at_unix_ms > 1)
            );
        }
    }

    #[tokio::test]
    async fn matching_runtime_record_without_a_reachable_api_is_never_current_or_activated() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let executable = directory.path().join("nakode");
        std::fs::write(&executable, b"installed-build").expect("fake executable");
        let installed = executable_identity(&executable).expect("installed identity");
        let runtime = ServiceRuntimeRecord {
            pid: std::process::id(),
            started_at_unix_ms: now_ms(),
            version: "1.0.0".to_owned(),
            workspace: None,
            executable: Some(installed.clone()),
        };
        write_private_file(
            paths.runtime(),
            &serde_json::to_vec(&runtime).expect("runtime record encoding"),
        )
        .expect("runtime record");

        let synthesized = synthesized_journal(&paths, &executable)
            .await
            .expect("synthesized status");
        assert_eq!(synthesized.phase, Phase::Failed);
        assert!(synthesized.failure.is_some_and(|failure| {
            failure.retryable && failure.message.contains("API is not reachable")
        }));

        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let config = Config::for_workspace(workspace).expect("test config");
        let mut pending = Journal::new(
            InstalledIdentity {
                executable: installed,
                version: "1.0.0".to_owned(),
            },
            Some(RunningIdentity {
                runtime,
                server: None,
            }),
        );
        pending.phase = Phase::Blocked;
        write_journal(&paths, &mut pending).expect("pending journal");
        assert!(
            !runtime_owner_is_disproven(&paths),
            "a live runtime PID must prevent an unfenced replacement when both sockets are unreachable"
        );

        assert!(
            !check_once_fenced(&paths, &executable, &config, CheckTrigger::Scheduled)
                .await
                .expect("unreachable service is a retryable check failure")
        );
        let checked = read_required_journal(&paths).expect("checked journal");
        assert_eq!(checked.phase, Phase::Failed);
        assert!(
            checked
                .running
                .is_some_and(|running| running.server.is_none())
        );
    }

    #[tokio::test]
    async fn blocker_inventory_has_a_bounded_service_query() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let listener = tokio::net::UnixListener::bind(paths.api()).expect("hung API listener");
        let hung_server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept SDK connection");
            std::future::pending::<()>().await;
        });
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let config = Config::for_workspace(workspace).expect("test config");

        let error = collect_blockers(&paths, &config)
            .await
            .expect_err("hung service query must time out");
        assert!(error.contains("timed out after 3 seconds"));
        hung_server.abort();
    }

    #[test]
    fn journal_round_trip_is_versioned_and_private_writer_replaces_atomically() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut expected = journal();
        write_journal(&paths, &mut expected).expect("write journal");
        assert_eq!(read_journal(&paths).expect("read journal"), Some(expected));
        let permissions = std::fs::metadata(paths.activation_journal())
            .expect("journal metadata")
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(permissions.mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn history_and_idempotency_are_bounded() {
        let mut journal = journal();
        for index in 0..100 {
            assert!(journal.remember_idempotency_key(format!("key-{index}")));
            journal.record(
                CheckTrigger::Scheduled,
                index,
                CheckResult::Blocked,
                "blocked",
            );
        }
        assert_eq!(journal.history.len(), HISTORY_LIMIT);
        assert_eq!(journal.idempotency_keys.len(), IDEMPOTENCY_LIMIT);
        assert_eq!(
            journal.history.first().map(|entry| entry.sequence),
            Some(51)
        );
    }

    fn assert_accepted_execution_rejection_replays(original: &Status) {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut journal = journal();
        assert!(journal.remember_idempotency_key("operation-a".to_owned()));
        write_journal(&paths, &mut journal).expect("accepted mutation journal");
        persist_accepted_execution_rejection(&paths, "operation-a", original)
            .expect("execution rejection persistence");

        let journal = read_required_journal(&paths).expect("persisted journal");
        let IdempotencyReplay::Rejected(replayed) =
            idempotency_replay(&journal, "operation-a").expect("idempotency replay")
        else {
            panic!("execution failure was not replayed as a rejection");
        };
        assert_eq!(replayed.code(), original.code());
        assert_eq!(replayed.message(), original.message());
    }

    #[test]
    fn accepted_manual_execution_failure_replays_internal() {
        assert_accepted_execution_rejection_replays(&Status::internal(
            "manual recheck could not persist its activation result",
        ));
    }

    #[test]
    fn accepted_force_shutdown_failure_replays_aborted() {
        assert_accepted_execution_rejection_replays(&Status::aborted(
            "conditional quiesce shutdown rejected the force fence",
        ));
    }

    #[test]
    fn accepted_replacement_verification_failure_replays_unavailable() {
        assert_accepted_execution_rejection_replays(&Status::unavailable(
            "replacement identity did not match",
        ));
    }

    #[tokio::test]
    async fn stale_helper_lease_is_reclaimed_without_removing_a_replacement_owner() {
        let directory = tempfile::tempdir().expect("helper directory");
        let paths = ServicePaths::in_directory(directory.path());
        let path = paths.activation_helper_lock().to_owned();
        std::fs::write(&path, "99999999:stale\n").expect("stale lease");
        let lease = HelperLease::acquire(&paths, "reclaimed")
            .await
            .expect("reclaimed lease");
        std::fs::write(&path, "99999998:replacement\n").expect("replacement lease");
        drop(lease);
        assert_eq!(
            std::fs::read_to_string(path).expect("replacement remains"),
            "99999998:replacement\n"
        );
    }

    #[test]
    fn missing_corrupt_and_future_journals_have_explicit_recovery_boundaries() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        assert!(read_journal(&paths).expect("missing journal").is_none());

        std::fs::write(paths.activation_journal(), b"{truncated")
            .expect("corrupt activation journal");
        assert!(matches!(
            read_journal(&paths),
            Err(ActivationError::Decode { .. })
        ));
        let quarantined = quarantine_corrupt_journal(&paths).expect("quarantine corrupt journal");
        assert_eq!(
            std::fs::read(&quarantined).expect("quarantined bytes"),
            b"{truncated"
        );
        assert!(!paths.activation_journal().exists());

        let mut future = journal();
        future.schema_version = JOURNAL_SCHEMA_VERSION + 1;
        std::fs::write(
            paths.activation_journal(),
            serde_json::to_vec(&future).expect("future journal encoding"),
        )
        .expect("future activation journal");
        assert!(matches!(
            read_journal(&paths),
            Err(ActivationError::UnsupportedSchema(version))
                if version == JOURNAL_SCHEMA_VERSION + 1
        ));
    }

    #[tokio::test]
    async fn endpoint_observation_reconciles_an_interrupted_cutover() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let installed = identity("new");
        let mut pending = journal();
        pending.phase = Phase::Activating;
        pending.blockers.push(Blocker {
            session_id: "session-a".to_owned(),
            title: "Session A".to_owned(),
            session_revision: 7,
            activity: api::SessionActivity::RunningTurn as i32,
            queue_count: 0,
            reasons: vec!["running".to_owned()],
        });
        write_journal(&paths, &mut pending).expect("pending journal");
        let runtime = ServiceRuntimeRecord {
            pid: 42,
            started_at_unix_ms: 10,
            version: "1.0.0".to_owned(),
            workspace: None,
            executable: Some(installed.clone()),
        };
        let server = ServerReport {
            server_version: "1.0.0".to_owned(),
            api_version: "nakode.v1".to_owned(),
            capabilities: vec![CONDITIONAL_FORCE_CAPABILITY.to_owned()],
        };

        observe_current_service(&paths, &installed, &runtime, Some(&server))
            .await
            .expect("reconcile current service");
        let recovered = read_required_journal(&paths).expect("recovered journal");
        assert_eq!(recovered.phase, Phase::Activated);
        assert!(recovered.blockers.is_empty());
        assert_eq!(
            recovered.running.map(|running| running.runtime),
            Some(runtime)
        );
        assert!(recovered.history.last().is_some_and(|record| {
            record.trigger == CheckTrigger::Recovered && record.result == CheckResult::Activated
        }));
    }

    #[tokio::test]
    async fn endpoint_observation_quarantines_corruption_and_reconstructs_current_state() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        std::fs::write(paths.activation_journal(), b"{truncated")
            .expect("corrupt activation journal");
        let installed = identity("current");
        let runtime = ServiceRuntimeRecord {
            pid: 42,
            started_at_unix_ms: 10,
            version: "1.0.0".to_owned(),
            workspace: None,
            executable: Some(installed.clone()),
        };

        let server = ServerReport {
            server_version: "1.0.0".to_owned(),
            api_version: "nakode.v1".to_owned(),
            capabilities: Vec::new(),
        };

        observe_current_service(&paths, &installed, &runtime, Some(&server))
            .await
            .expect("recover current service");

        let recovered = read_required_journal(&paths).expect("recovered journal");
        assert_eq!(recovered.phase, Phase::Activated);
        assert!(recovered.history.last().is_some_and(|record| {
            record.trigger == CheckTrigger::Recovered
                && record.result == CheckResult::Activated
                && record.detail.contains("corrupt journal quarantined")
        }));
        let quarantined = std::fs::read_dir(directory.path())
            .expect("activation directory")
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("activation.corrupt-")
            });
        assert!(quarantined.is_some());
    }

    #[test]
    fn force_fence_rejects_empty_keys_and_any_authoritative_change() {
        let mut pending = journal();
        pending.revision = 12;
        pending.blockers = vec![Blocker {
            session_id: "session-a".to_owned(),
            title: "Session A".to_owned(),
            session_revision: 7,
            activity: api::SessionActivity::RunningTurn as i32,
            queue_count: 1,
            reasons: vec!["running".to_owned()],
        }];
        let exact_blocker = blocker_to_api(pending.blockers[0].clone());
        let exact = api::ForceActivateRequest {
            idempotency_key: "force-a".to_owned(),
            expected_activation_revision: 12,
            expected_blockers: vec![exact_blocker.clone()],
            expected_attempt_id: pending.attempt_id.clone(),
        };
        assert_eq!(
            validate_force_fence(&pending, &exact).expect("exact fence"),
            BTreeSet::from([("session-a".to_owned(), 7)])
        );
        assert_eq!(
            idempotency_replay(&pending, "")
                .expect_err("empty key")
                .code(),
            tonic::Code::InvalidArgument
        );

        let mut changed_attempt = exact.clone();
        changed_attempt.expected_attempt_id = "another-attempt".to_owned();
        assert_eq!(
            validate_force_fence(&pending, &changed_attempt)
                .expect_err("activation attempt changed")
                .code(),
            tonic::Code::Aborted
        );

        let mut changed_revision = exact.clone();
        changed_revision.expected_activation_revision += 1;
        assert_eq!(
            validate_force_fence(&pending, &changed_revision)
                .expect_err("activation revision changed")
                .code(),
            tonic::Code::Aborted
        );
        let mut changed_blocker = exact.clone();
        changed_blocker.expected_blockers[0].session_revision += 1;
        assert_eq!(
            validate_force_fence(&pending, &changed_blocker)
                .expect_err("blocker revision changed")
                .code(),
            tonic::Code::Aborted
        );
        let mut removed_blocker = exact.clone();
        removed_blocker.expected_blockers.clear();
        assert_eq!(
            validate_force_fence(&pending, &removed_blocker)
                .expect_err("blocker removed")
                .code(),
            tonic::Code::Aborted
        );
        let mut added_blocker = exact;
        added_blocker
            .expected_blockers
            .push(api::ActivationBlocker {
                session_id: "session-b".to_owned(),
                session_revision: 1,
                ..api::ActivationBlocker::default()
            });
        assert_eq!(
            validate_force_fence(&pending, &added_blocker)
                .expect_err("blocker added")
                .code(),
            tonic::Code::Aborted
        );

        assert!(pending.remember_idempotency_key("force-a".to_owned()));
        assert!(matches!(
            idempotency_replay(&pending, "force-a").expect("durable replay"),
            IdempotencyReplay::Accepted
        ));

        let rejected = Status::aborted("captured blocker set changed");
        assert!(pending.remember_idempotency_rejection("force-rejected".to_owned(), &rejected,));
        match idempotency_replay(&pending, "force-rejected").expect("rejected replay") {
            IdempotencyReplay::Rejected(replayed) => {
                assert_eq!(replayed.code(), tonic::Code::Aborted);
                assert_eq!(replayed.message(), rejected.message());
            }
            IdempotencyReplay::New | IdempotencyReplay::Accepted => {
                panic!("rejected force was not durably replayed")
            }
        }
    }

    fn assert_empty_terminal_recheck_is_not_audited(
        paths: &ServicePaths,
        expected_history_len: usize,
    ) {
        let empty =
            complete_terminal_recheck(paths, String::new()).expect_err("empty idempotency key");
        assert_eq!(empty.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            read_required_journal(paths)
                .expect("empty-key journal")
                .history
                .len(),
            expected_history_len
        );
    }

    #[tokio::test]
    async fn terminal_helper_drains_queued_mutations_with_durable_audit_and_replay() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut terminal = terminal_journal(true);
        write_journal(&paths, &mut terminal).expect("terminal journal");
        let observed_attempt = terminal.attempt_id.clone();
        let observed_revision = terminal.revision;
        let (commands, mut command_rx) = mpsc::channel(8);
        let (recheck_tx, recheck_rx) = oneshot::channel();
        let (recheck_replay_tx, recheck_replay_rx) = oneshot::channel();
        let stale_force = api::ForceActivateRequest {
            idempotency_key: "queued-force".to_owned(),
            expected_activation_revision: observed_revision,
            expected_blockers: Vec::new(),
            expected_attempt_id: observed_attempt,
        };
        let (force_tx, force_rx) = oneshot::channel();
        let (force_replay_tx, force_replay_rx) = oneshot::channel();
        commands
            .send(HelperCommand::Recheck {
                idempotency_key: "queued-recheck".to_owned(),
                response: recheck_tx,
            })
            .await
            .expect("queue recheck");
        commands
            .send(HelperCommand::Recheck {
                idempotency_key: "queued-recheck".to_owned(),
                response: recheck_replay_tx,
            })
            .await
            .expect("queue recheck replay");
        commands
            .send(HelperCommand::Force {
                request: stale_force.clone(),
                response: force_tx,
            })
            .await
            .expect("queue force");
        commands
            .send(HelperCommand::Force {
                request: stale_force,
                response: force_replay_tx,
            })
            .await
            .expect("queue force replay");

        resolve_queued_terminal_commands(&paths, &mut command_rx)
            .await
            .expect("drain terminal mutations");
        assert!(recheck_rx.await.expect("recheck response").is_ok());
        assert!(
            recheck_replay_rx
                .await
                .expect("recheck replay response")
                .is_ok()
        );
        let force_error = force_rx
            .await
            .expect("force response")
            .expect_err("intervening revision must reject force");
        assert_eq!(force_error.code(), tonic::Code::Aborted);
        let replayed_error = force_replay_rx
            .await
            .expect("force replay response")
            .expect_err("rejected force must replay");
        assert_eq!(replayed_error.code(), force_error.code());
        assert_eq!(replayed_error.message(), force_error.message());
        assert!(command_rx.is_closed());

        let after_queued = read_required_journal(&paths).expect("queued mutation journal");
        assert_eq!(after_queued.history.len(), 2);
        assert_eq!(after_queued.history[0].trigger, CheckTrigger::Manual);
        assert_eq!(after_queued.history[0].result, CheckResult::Activated);
        assert_eq!(after_queued.history[1].trigger, CheckTrigger::Forced);
        assert_eq!(after_queued.history[1].result, CheckResult::Cancelled);

        let exact_force = api::ForceActivateRequest {
            idempotency_key: "terminal-force".to_owned(),
            expected_activation_revision: after_queued.revision,
            expected_blockers: Vec::new(),
            expected_attempt_id: after_queued.attempt_id.clone(),
        };
        let accepted = complete_terminal_force(&paths, exact_force.clone())
            .expect("exact terminal force is an audited no-op");
        assert_eq!(accepted.phase, api::ActivationPhase::Activated as i32);
        let history_after_accept = read_required_journal(&paths)
            .expect("accepted terminal force")
            .history
            .len();
        assert!(complete_terminal_force(&paths, exact_force).is_ok());
        assert_eq!(
            read_required_journal(&paths)
                .expect("accepted force replay")
                .history
                .len(),
            history_after_accept
        );

        assert_empty_terminal_recheck_is_not_audited(&paths, history_after_accept);
    }

    #[test]
    fn terminal_force_without_capability_is_durably_rejected_once() {
        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut terminal = terminal_journal(false);
        write_journal(&paths, &mut terminal).expect("terminal journal");
        let request = api::ForceActivateRequest {
            idempotency_key: "unsupported-force".to_owned(),
            expected_activation_revision: terminal.revision,
            expected_blockers: Vec::new(),
            expected_attempt_id: terminal.attempt_id,
        };
        let error = complete_terminal_force(&paths, request.clone())
            .expect_err("force capability is unavailable");
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        let history_len = read_required_journal(&paths)
            .expect("unsupported force journal")
            .history
            .len();
        let replay = complete_terminal_force(&paths, request)
            .expect_err("unsupported force rejection replays");
        assert_eq!(replay.code(), error.code());
        assert_eq!(replay.message(), error.message());
        assert_eq!(
            read_required_journal(&paths)
                .expect("unsupported force replay")
                .history
                .len(),
            history_len
        );
    }

    #[tokio::test]
    async fn helper_owned_terminal_watch_delivers_activated_then_closes() {
        use futures_util::StreamExt;

        let directory = tempfile::tempdir().expect("activation directory");
        let paths = ServicePaths::in_directory(directory.path());
        let mut terminal = terminal_journal(true);
        write_journal(&paths, &mut terminal).expect("terminal journal");
        let (commands, _command_rx) = mpsc::channel(1);
        let service =
            ActivationGrpcService::new(paths, PathBuf::from("/tmp/fake-nakode"), Some(commands));
        let response = api::activation_service_server::ActivationService::watch_activation_status(
            &service,
            Request::new(api::WatchActivationStatusRequest {
                after_revision: None,
                after_attempt_id: String::new(),
            }),
        )
        .await
        .expect("open helper watch");
        let mut stream = response.into_inner();
        let status = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("terminal status deadline")
            .expect("terminal status")
            .expect("valid terminal status");
        assert_eq!(status.phase, api::ActivationPhase::Activated as i32);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("terminal watch closure deadline")
                .is_none()
        );
    }

    #[test]
    fn activation_watch_cursor_is_attempt_qualified() {
        let mut status = api::ActivationStatus {
            attempt_id: "attempt-b".to_owned(),
            revision: 1,
            ..api::ActivationStatus::default()
        };
        assert!(activation_status_follows("attempt-a", 12, &status));
        assert!(!activation_status_follows("attempt-b", 1, &status));
        status.revision = 2;
        assert!(activation_status_follows("attempt-b", 1, &status));
        assert!(activation_status_follows("", 99, &status));
    }

    #[test]
    fn socket_lease_drop_preserves_a_replacement_owner() {
        let directory = tempfile::tempdir().expect("activation directory");
        let socket = directory.path().join("activation.sock");
        let helper_lock = directory.path().join("activation-helper.lock");
        std::fs::write(&helper_lock, "first").expect("first helper owner");
        let helper_lease = HelperLease {
            path: helper_lock.clone(),
            owner: "first".to_owned(),
        };
        std::fs::write(&socket, "first").expect("first socket owner");
        let lease =
            ActivationSocketLease::capture(&socket, &helper_lease).expect("capture socket owner");
        std::fs::remove_file(&socket).expect("unlink first socket owner");
        std::fs::write(&helper_lock, "replacement").expect("replacement helper owner");
        std::fs::write(&socket, "replacement").expect("replacement socket owner");

        drop(lease);
        assert_eq!(
            std::fs::read_to_string(socket).expect("replacement socket remains"),
            "replacement"
        );
    }

    #[test]
    fn helper_heartbeat_cannot_make_a_reused_pid_permanently_authoritative() {
        let helper = HelperIdentity {
            pid: 42,
            instance_id: "helper-a".to_owned(),
            started_at_unix_ms: 1,
            heartbeat_at_unix_ms: 10_000,
        };
        let owner = HelperLeaseIdentity {
            pid: 42,
            instance_id: "helper-a".to_owned(),
        };
        assert!(helper_heartbeat_is_recent(
            &helper,
            &owner,
            10_000 + HELPER_HEARTBEAT_STALE_MS
        ));
        assert!(!helper_heartbeat_is_recent(
            &helper,
            &owner,
            10_001 + HELPER_HEARTBEAT_STALE_MS
        ));
        let other_pid = HelperLeaseIdentity {
            pid: 43,
            instance_id: "helper-a".to_owned(),
        };
        let other_instance = HelperLeaseIdentity {
            pid: 42,
            instance_id: "helper-b".to_owned(),
        };
        assert!(!helper_heartbeat_is_recent(&helper, &other_pid, 10_000));
        assert!(!helper_heartbeat_is_recent(
            &helper,
            &other_instance,
            10_000
        ));
    }

    #[tokio::test]
    async fn live_helper_lease_is_a_singleton() {
        let directory = tempfile::tempdir().expect("helper directory");
        let paths = ServicePaths::in_directory(directory.path());
        let path = paths.activation_helper_lock().to_owned();
        let lease = HelperLease::acquire(&paths, "first")
            .await
            .expect("first lease");
        assert!(matches!(
            HelperLease::acquire(&paths, "second").await,
            Err(ActivationError::HelperAlreadyRunning)
        ));
        drop(lease);
        assert!(!path.exists());
    }

    #[test]
    fn force_confirmation_compares_only_authoritative_identity_and_revision() {
        let first = api::ActivationBlocker {
            session_id: "session-a".to_owned(),
            session_revision: 7,
            title: "Old title".to_owned(),
            reasons: vec!["running".to_owned()],
            ..api::ActivationBlocker::default()
        };
        let mut changed_presentation = first.clone();
        changed_presentation.title = "New title".to_owned();
        changed_presentation.reasons = vec!["queued".to_owned()];
        assert_eq!(
            normalized_blocker_set(&[first]),
            normalized_blocker_set(&[changed_presentation])
        );
        let changed_revision = api::ActivationBlocker {
            session_id: "session-a".to_owned(),
            session_revision: 8,
            ..api::ActivationBlocker::default()
        };
        assert_ne!(
            normalized_blocker_set(&[api::ActivationBlocker {
                session_id: "session-a".to_owned(),
                session_revision: 7,
                ..api::ActivationBlocker::default()
            }]),
            normalized_blocker_set(&[changed_revision])
        );
    }
}
