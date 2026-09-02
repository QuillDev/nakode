//! Durable, typed remote-machine self-update authority.
//!
//! The remote API can request only the installed platform updater. A detached systemd user unit
//! owns the updater process so replacing/restarting the Nakode service cannot orphan the update.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use nakode_sdk::v1 as api;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const SCHEMA_VERSION: u32 = 1;
const MAX_KEY_BYTES: usize = 200;
const MAX_RECENT_IDEMPOTENCY_KEYS: usize = 16;
const POLL_INTERVAL: Duration = Duration::from_millis(400);
const ACTIVE_ATTEMPT_TIMEOUT_MS: u64 = 60 * 60 * 1_000;

#[derive(Clone, Debug)]
pub struct RemoteUpdatePaths {
    state: PathBuf,
    lock: PathBuf,
    execution_lock: PathBuf,
    log: PathBuf,
    fstack_home: PathBuf,
}

impl RemoteUpdatePaths {
    /// Discovers the private update-state paths for the current installation.
    ///
    /// # Errors
    ///
    /// Returns an error when the process has no home directory.
    pub fn discover() -> Result<Self, String> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| "HOME is not set".to_owned())?;
        let nakode_home =
            std::env::var_os("NAKODE_HOME").map_or_else(|| home.join(".nakode"), PathBuf::from);
        let fstack_home =
            std::env::var_os("FSTACK_HOME").map_or_else(|| home.join(".fstack"), PathBuf::from);
        let root = nakode_home.join("remote-update");
        Ok(Self {
            state: root.join("status.json"),
            lock: root.join("status.lock"),
            execution_lock: root.join("execution.lock"),
            log: root.join("update.log"),
            fstack_home,
        })
    }

    #[cfg(test)]
    fn isolated(root: &Path) -> Self {
        Self {
            state: root.join("nakode/remote-update/status.json"),
            lock: root.join("nakode/remote-update/status.lock"),
            execution_lock: root.join("nakode/remote-update/execution.lock"),
            log: root.join("nakode/remote-update/update.log"),
            fstack_home: root.join("fstack"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Idle,
    Requested,
    Checking,
    Downloading,
    Installing,
    Restarting,
    Reconnected,
    Succeeded,
    Failed,
}

impl Phase {
    const fn active(self) -> bool {
        matches!(
            self,
            Self::Requested
                | Self::Checking
                | Self::Downloading
                | Self::Installing
                | Self::Restarting
                | Self::Reconnected
        )
    }

    const fn api(self) -> api::RemoteUpdatePhase {
        match self {
            Self::Idle => api::RemoteUpdatePhase::Idle,
            Self::Requested => api::RemoteUpdatePhase::Requested,
            Self::Checking => api::RemoteUpdatePhase::Checking,
            Self::Downloading => api::RemoteUpdatePhase::Downloading,
            Self::Installing => api::RemoteUpdatePhase::Installing,
            Self::Restarting => api::RemoteUpdatePhase::Restarting,
            Self::Reconnected => api::RemoteUpdatePhase::Reconnected,
            Self::Succeeded => api::RemoteUpdatePhase::Succeeded,
            Self::Failed => api::RemoteUpdatePhase::Failed,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Failure {
    code: String,
    message: String,
    recovery: String,
    retryable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableStatus {
    schema_version: u32,
    server_id: String,
    idempotency_key: String,
    #[serde(default)]
    recent_idempotency_keys: Vec<String>,
    attempt_id: String,
    revision: u64,
    phase: Phase,
    current_version: String,
    current_build_revision: Option<String>,
    target_version: Option<String>,
    target_build_revision: Option<String>,
    update_available: bool,
    started_at_unix_ms: Option<u64>,
    updated_at_unix_ms: u64,
    failure: Option<Failure>,
}

impl DurableStatus {
    fn requested(server_id: String, idempotency_key: String) -> Self {
        let now = now_ms();
        Self {
            schema_version: SCHEMA_VERSION,
            server_id,
            idempotency_key,
            recent_idempotency_keys: Vec::new(),
            attempt_id: uuid::Uuid::now_v7().to_string(),
            revision: 1,
            phase: Phase::Requested,
            current_version: env!("CARGO_PKG_VERSION").to_owned(),
            current_build_revision: crate::BUILD_REVISION.map(str::to_owned),
            target_version: None,
            target_build_revision: None,
            update_available: true,
            started_at_unix_ms: Some(now),
            updated_at_unix_ms: now,
            failure: None,
        }
    }

    fn advance(&mut self, phase: Phase) {
        self.revision = self.revision.saturating_add(1);
        self.phase = phase;
        self.updated_at_unix_ms = now_ms();
    }

    fn fail(&mut self, code: &str, message: &str, recovery: &str, retryable: bool) {
        self.advance(Phase::Failed);
        self.failure = Some(Failure {
            code: bounded(code, 64),
            message: bounded(message, 500),
            recovery: bounded(recovery, 500),
            retryable,
        });
    }
}

#[derive(Clone, Debug)]
struct Capability {
    supported: bool,
    reason: String,
    fstack: Option<PathBuf>,
}

impl Capability {
    fn detect(paths: &RemoteUpdatePaths) -> Self {
        if !cfg!(target_os = "linux") {
            return Self::unsupported("Self-update is supported only for headless Linux installs.");
        }
        let mode =
            fs::read_to_string(paths.fstack_home.join("state/install-mode")).unwrap_or_default();
        if mode.trim() != "headless" {
            return Self::unsupported(
                "This machine is not a managed headless install; update it from its local owner UI.",
            );
        }
        let Some(install_root) = std::env::var_os("CARGO_INSTALL_ROOT").map(PathBuf::from) else {
            return Self::unsupported(
                "The managed install prefix is missing from the supervisor configuration.",
            );
        };
        let fstack = install_root.join("bin/fstack");
        if !is_executable(&fstack) {
            return Self::unsupported(
                "The managed fstack updater is missing from the supervised install prefix.",
            );
        }
        if systemd_run_executable().is_none() {
            return Self::unsupported(
                "A systemd user manager is required for a restart-safe update handoff.",
            );
        }
        Self {
            supported: true,
            reason: String::new(),
            fstack: Some(fstack),
        }
    }

    fn unsupported(reason: &str) -> Self {
        Self {
            supported: false,
            reason: reason.to_owned(),
            fstack: None,
        }
    }
}

#[derive(Clone)]
pub struct RemoteUpdateService {
    paths: RemoteUpdatePaths,
    server_id: Arc<str>,
}

impl RemoteUpdateService {
    /// Creates a service authority and reconciles any persisted update attempt.
    ///
    /// # Errors
    ///
    /// Returns an error when the private update-state paths cannot be discovered.
    pub fn new(server_id: impl Into<String>) -> Result<Self, String> {
        let service = Self {
            paths: RemoteUpdatePaths::discover()?,
            server_id: Arc::from(server_id.into()),
        };
        service.reconcile_restarted_attempt();
        service.recover_stale_attempt();
        service.start_initial_check();
        Ok(service)
    }

    fn reconcile_restarted_attempt(&self) {
        let Ok(_guard) = lock_exclusive(&self.paths.lock) else {
            return;
        };
        let Ok(Some(mut status)) = read_status(&self.paths.state) else {
            return;
        };
        let verified_revision = match (
            status.target_build_revision.as_deref(),
            crate::BUILD_REVISION,
        ) {
            (Some(target), Some(current)) if target == current => Some(current),
            _ => None,
        };
        if matches!(status.phase, Phase::Restarting | Phase::Reconnected)
            && verified_revision.is_some()
        {
            status.advance(Phase::Reconnected);
            env!("CARGO_PKG_VERSION").clone_into(&mut status.current_version);
            status.current_build_revision = verified_revision.map(str::to_owned);
            status.update_available = false;
            status.advance(Phase::Succeeded);
            let _ = write_status(&self.paths.state, &status);
        }
    }

    fn recover_stale_attempt(&self) {
        let Ok(Some(_execution_guard)) = try_lock_exclusive(&self.paths.execution_lock) else {
            return;
        };
        let Ok(_guard) = lock_exclusive(&self.paths.lock) else {
            return;
        };
        let Ok(Some(mut status)) = read_status(&self.paths.state) else {
            return;
        };
        if status.phase.active()
            && now_ms().saturating_sub(status.updated_at_unix_ms) > ACTIVE_ATTEMPT_TIMEOUT_MS
        {
            status.fail(
                "update_interrupted",
                "The machine update stopped before completion.",
                "The existing installation remains available. Refresh status, then retry the update.",
                true,
            );
            let _ = write_status(&self.paths.state, &status);
        }
    }

    fn start_initial_check(&self) {
        let capability = Capability::detect(&self.paths);
        if !capability.supported || self.paths.state.exists() {
            return;
        }
        let Some(fstack) = capability.fstack else {
            return;
        };
        let Ok(guard) = lock_exclusive(&self.paths.lock) else {
            return;
        };
        if self.paths.state.exists() {
            return;
        }
        let paths = self.paths.clone();
        let mut status = DurableStatus::requested(self.server_id.to_string(), String::new());
        status.attempt_id = format!("check-{}", uuid::Uuid::now_v7());
        status.phase = Phase::Checking;
        if write_status(&paths.state, &status).is_err() {
            return;
        }
        drop(guard);
        let _ = thread::spawn(move || {
            match Command::new(fstack)
                .args(["update", "--check", "--json"])
                .stdin(Stdio::null())
                .output()
            {
                Ok(output) if output.status.success() => {
                    apply_check_report(&mut status, &output.stdout);
                    status.advance(Phase::Idle);
                }
                _ => status.fail(
                    "update_check_failed",
                    "Update availability could not be verified.",
                    "Check this machine's network and managed source checkout, then refresh.",
                    true,
                ),
            }
            let _ = write_status(&paths.state, &status);
        });
    }

    #[cfg(test)]
    fn isolated(server_id: &str, root: &Path) -> Self {
        Self {
            paths: RemoteUpdatePaths::isolated(root),
            server_id: Arc::from(server_id),
        }
    }

    #[must_use]
    pub fn into_authenticated_server(
        self,
        api_key: impl Into<String>,
    ) -> tonic::service::interceptor::InterceptedService<
        api::remote_update_service_server::RemoteUpdateServiceServer<Self>,
        nakode_server::grpc::ApiKeyInterceptor,
    > {
        let server = api::remote_update_service_server::RemoteUpdateServiceServer::new(self);
        tonic::service::interceptor::InterceptedService::new(
            server,
            nakode_server::grpc::ApiKeyInterceptor::new(api_key),
        )
    }

    fn status(&self) -> Result<api::RemoteUpdateStatus, Status> {
        let capability = Capability::detect(&self.paths);
        let durable = read_status(&self.paths.state).map_err(|error| internal_status(&error))?;
        Ok(status_to_api(
            &self.server_id,
            &capability,
            durable.as_ref(),
        ))
    }

    fn start(
        &self,
        request: api::StartRemoteUpdateRequest,
    ) -> Result<api::StartRemoteUpdateResponse, Status> {
        validate_request(&request)?;
        let _lock = lock_exclusive(&self.paths.lock).map_err(|error| internal_status(&error))?;
        let capability = Capability::detect(&self.paths);
        let existing = read_status(&self.paths.state).map_err(|error| internal_status(&error))?;

        if let Some(status) = existing.as_ref() {
            if status.idempotency_key == request.idempotency_key
                || status
                    .recent_idempotency_keys
                    .iter()
                    .any(|key| key == &request.idempotency_key)
            {
                return Ok(response(
                    api::StartRemoteUpdateOutcome::Replayed,
                    status_to_api(&self.server_id, &capability, Some(status)),
                ));
            }
            if status.phase.active() {
                return Ok(response(
                    api::StartRemoteUpdateOutcome::AlreadyInProgress,
                    status_to_api(&self.server_id, &capability, Some(status)),
                ));
            }
        }
        if request.expected_server_id != self.server_id.as_ref()
            || request
                .expected_build_revision
                .as_deref()
                .is_some_and(|expected| Some(expected) != crate::BUILD_REVISION)
        {
            return Ok(response(
                api::StartRemoteUpdateOutcome::StaleTarget,
                status_to_api(&self.server_id, &capability, existing.as_ref()),
            ));
        }
        if !capability.supported {
            return Ok(response(
                api::StartRemoteUpdateOutcome::Unsupported,
                status_to_api(&self.server_id, &capability, existing.as_ref()),
            ));
        }

        let previous_keys = existing
            .as_ref()
            .map(|status| {
                let mut keys = Vec::new();
                if !status.idempotency_key.is_empty() {
                    keys.push(status.idempotency_key.clone());
                }
                keys.extend(status.recent_idempotency_keys.iter().cloned());
                keys.truncate(MAX_RECENT_IDEMPOTENCY_KEYS);
                keys
            })
            .unwrap_or_default();
        let mut status =
            DurableStatus::requested(self.server_id.to_string(), request.idempotency_key);
        status.recent_idempotency_keys = previous_keys;
        write_status(&self.paths.state, &status).map_err(|error| internal_status(&error))?;
        if launch_helper(&self.paths, &status.attempt_id).is_err() {
            status.fail(
                "handoff_failed",
                "The restart-safe update helper could not be started.",
                "Confirm the user systemd manager is running, then try again.",
                true,
            );
            write_status(&self.paths.state, &status).map_err(|error| internal_status(&error))?;
            return Ok(response(
                api::StartRemoteUpdateOutcome::Accepted,
                status_to_api(&self.server_id, &capability, Some(&status)),
            ));
        }
        Ok(response(
            api::StartRemoteUpdateOutcome::Accepted,
            status_to_api(&self.server_id, &capability, Some(&status)),
        ))
    }
}

type StatusStream = std::pin::Pin<
    Box<dyn futures_util::Stream<Item = Result<api::RemoteUpdateStatus, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl api::remote_update_service_server::RemoteUpdateService for RemoteUpdateService {
    type WatchRemoteUpdateStatusStream = StatusStream;

    async fn get_remote_update_status(
        &self,
        _request: Request<api::GetRemoteUpdateStatusRequest>,
    ) -> Result<Response<api::RemoteUpdateStatus>, Status> {
        self.status().map(Response::new)
    }

    async fn watch_remote_update_status(
        &self,
        request: Request<api::WatchRemoteUpdateStatusRequest>,
    ) -> Result<Response<Self::WatchRemoteUpdateStatusStream>, Status> {
        let after = request.into_inner();
        let service = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut cursor = if after.after_attempt_id.is_empty() {
                None
            } else {
                Some((
                    after.after_attempt_id,
                    after.after_revision.unwrap_or_default(),
                ))
            };
            loop {
                let status = match service.status() {
                    Ok(status) => status,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                let next = (status.attempt_id.clone(), status.revision);
                if cursor.as_ref() != Some(&next) {
                    cursor = Some(next);
                    if sender.send(Ok(status)).await.is_err() {
                        return;
                    }
                }
                tokio::select! {
                    () = sender.closed() => return,
                    () = tokio::time::sleep(POLL_INTERVAL) => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn start_remote_update(
        &self,
        request: Request<api::StartRemoteUpdateRequest>,
    ) -> Result<Response<api::StartRemoteUpdateResponse>, Status> {
        self.start(request.into_inner()).map(Response::new)
    }
}

fn response(
    outcome: api::StartRemoteUpdateOutcome,
    status: api::RemoteUpdateStatus,
) -> api::StartRemoteUpdateResponse {
    api::StartRemoteUpdateResponse {
        outcome: outcome.into(),
        status: Some(status),
    }
}

fn status_to_api(
    server_id: &str,
    capability: &Capability,
    durable: Option<&DurableStatus>,
) -> api::RemoteUpdateStatus {
    let now = now_ms();
    let capability_value = if capability.supported {
        api::RemoteUpdateCapability::Supported
    } else {
        api::RemoteUpdateCapability::Unsupported
    };
    match durable {
        Some(status) => api::RemoteUpdateStatus {
            schema_version: status.schema_version,
            server_id: server_id.to_owned(),
            attempt_id: status.attempt_id.clone(),
            revision: status.revision,
            capability: capability_value.into(),
            unsupported_reason: capability.reason.clone(),
            phase: status.phase.api().into(),
            current_version: env!("CARGO_PKG_VERSION").to_owned(),
            current_build_revision: crate::BUILD_REVISION.map(str::to_owned),
            target_version: status.target_version.clone(),
            target_build_revision: status.target_build_revision.clone(),
            update_available: status.update_available,
            started_at_unix_ms: status.started_at_unix_ms,
            updated_at_unix_ms: status.updated_at_unix_ms,
            failure: status
                .failure
                .as_ref()
                .map(|failure| api::RemoteUpdateFailure {
                    code: failure.code.clone(),
                    message: failure.message.clone(),
                    recovery: failure.recovery.clone(),
                    retryable: failure.retryable,
                }),
        },
        None => api::RemoteUpdateStatus {
            schema_version: SCHEMA_VERSION,
            server_id: server_id.to_owned(),
            attempt_id: String::new(),
            revision: 0,
            capability: capability_value.into(),
            unsupported_reason: capability.reason.clone(),
            phase: api::RemoteUpdatePhase::Idle.into(),
            current_version: env!("CARGO_PKG_VERSION").to_owned(),
            current_build_revision: crate::BUILD_REVISION.map(str::to_owned),
            target_version: None,
            target_build_revision: None,
            update_available: false,
            started_at_unix_ms: None,
            updated_at_unix_ms: now,
            failure: None,
        },
    }
}

fn validate_request(request: &api::StartRemoteUpdateRequest) -> Result<(), Status> {
    if request.idempotency_key.is_empty() || request.idempotency_key.len() > MAX_KEY_BYTES {
        return Err(Status::invalid_argument(
            "idempotency_key must contain 1-200 bytes",
        ));
    }
    if request.expected_server_id.is_empty() {
        return Err(Status::invalid_argument("expected_server_id is required"));
    }
    Ok(())
}

fn launch_helper(paths: &RemoteUpdatePaths, attempt_id: &str) -> Result<(), String> {
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let unit = format!("nakode-remote-update-{attempt_id}");
    let systemd_run = systemd_run_executable()
        .ok_or_else(|| "systemd-run is not installed in a trusted system location".to_owned())?;
    let mut command = Command::new(systemd_run);
    command.args([
        "--user",
        "--collect",
        "--quiet",
        "--property=Type=exec",
        "--property=UMask=0077",
        "--property=NoNewPrivileges=yes",
    ]);
    for key in ["HOME", "NAKODE_HOME", "FSTACK_HOME", "CARGO_INSTALL_ROOT"] {
        let value = std::env::var_os(key)
            .ok_or_else(|| format!("{key} is missing from the supervisor environment"))?;
        command.arg(format!("--setenv={key}={}", value.to_string_lossy()));
    }
    let output = command
        .arg(format!("--unit={unit}"))
        .arg("--")
        .arg(executable)
        .arg("remote-update-helper")
        .arg("--state")
        .arg(&paths.state)
        .arg("--attempt")
        .arg(attempt_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| error.to_string())?;
    if output.success() {
        Ok(())
    } else {
        Err(format!("systemd-run exited with {output}"))
    }
}

/// Runs inside a transient systemd user unit and survives replacement of the serving process.
///
/// # Errors
///
/// Returns an error when the invocation is stale, the updater cannot run, or the restarted
/// service cannot verify the exact target build.
pub fn run_helper(state_path: &Path, attempt_id: &str) -> Result<(), String> {
    let paths = RemoteUpdatePaths::discover()?;
    let result = run_helper_inner(&paths, state_path, attempt_id);
    if let Err(error) = &result {
        fail_active_attempt(&paths, attempt_id, error);
    }
    result
}

fn run_helper_inner(
    paths: &RemoteUpdatePaths,
    state_path: &Path,
    attempt_id: &str,
) -> Result<(), String> {
    if paths.state != state_path {
        return Err("remote update state path does not match this installation".to_owned());
    }
    let _execution_guard = try_lock_exclusive(&paths.execution_lock)?
        .ok_or_else(|| "another remote update helper is already running".to_owned())?;
    {
        let _lock = lock_exclusive(&paths.lock)?;
        let status =
            read_status(state_path)?.ok_or_else(|| "remote update status is missing".to_owned())?;
        if status.attempt_id != attempt_id || !status.phase.active() {
            return Err("remote update attempt is stale".to_owned());
        }
    }
    let mut status =
        read_status(state_path)?.ok_or_else(|| "remote update status is missing".to_owned())?;
    let capability = Capability::detect(paths);
    let fstack = capability.fstack.ok_or_else(|| capability.reason.clone())?;

    status.advance(Phase::Checking);
    publish_attempt_status(paths, attempt_id, &mut status)?;
    if let Ok(report) = Command::new(&fstack)
        .args(["update", "--check", "--json"])
        .stdin(Stdio::null())
        .output()
    {
        apply_check_report(&mut status, &report.stdout);
        publish_attempt_status(paths, attempt_id, &mut status)?;
        if report.status.success() && !status.update_available {
            status.advance(Phase::Succeeded);
            publish_attempt_status(paths, attempt_id, &mut status)?;
            return Ok(());
        }
    }

    let log = private_log(&paths.log)?;
    let stderr = log.try_clone().map_err(|error| error.to_string())?;
    let mut child = Command::new(fstack)
        .arg("update")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| error.to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "fstack update stdout was unavailable".to_owned())?;
    let mut writer = log;
    for line in BufReader::new(stdout).lines() {
        let line = line.map_err(|error| error.to_string())?;
        let _ = writeln!(writer, "{line}");
        if let Some(stage) = line.strip_prefix("fstack-update-stage ")
            && let Some(phase) = phase_for_stage(stage.trim())
        {
            status.advance(phase);
            publish_attempt_status(paths, attempt_id, &mut status)?;
        }
    }
    let exit = child.wait().map_err(|error| error.to_string())?;
    if !exit.success() {
        return Err(format!("fstack update exited with {exit}"));
    }

    let current = read_status(state_path)?
        .ok_or_else(|| "remote update status disappeared during restart".to_owned())?;
    if current.attempt_id == attempt_id && current.phase == Phase::Succeeded {
        return Ok(());
    }
    status.advance(Phase::Restarting);
    publish_attempt_status(paths, attempt_id, &mut status)?;
    for _ in 0..120 {
        thread::sleep(Duration::from_secs(1));
        let current = read_status(state_path)?
            .ok_or_else(|| "remote update status disappeared during restart".to_owned())?;
        if current.attempt_id != attempt_id {
            return Err("remote update attempt changed during restart".to_owned());
        }
        if current.phase == Phase::Succeeded {
            return Ok(());
        }
        if current.phase == Phase::Failed {
            return Err("the restarted service could not verify the update".to_owned());
        }
    }
    Err("the updated Nakode service did not reconnect and verify its build in time".to_owned())
}

fn phase_for_stage(stage: &str) -> Option<Phase> {
    match stage {
        "preflight-complete" | "nakode-update" | "fstack-pull" => Some(Phase::Downloading),
        "nakode-complete" => Some(Phase::Installing),
        // The supervised installer may replace Nakode while this stage is still running, so publish
        // the handoff state before entering install.sh rather than waiting for fstack-complete.
        "fstack-install" | "fstack-complete" => Some(Phase::Restarting),
        _ => None,
    }
}

fn publish_attempt_status(
    paths: &RemoteUpdatePaths,
    attempt_id: &str,
    status: &mut DurableStatus,
) -> Result<(), String> {
    let _lock = lock_exclusive(&paths.lock)?;
    let current =
        read_status(&paths.state)?.ok_or_else(|| "remote update status is missing".to_owned())?;
    if current.attempt_id != attempt_id || !current.phase.active() {
        return Err("remote update helper no longer owns the active attempt".to_owned());
    }
    status.revision = status.revision.max(current.revision.saturating_add(1));
    status.updated_at_unix_ms = now_ms();
    write_status(&paths.state, status)
}

fn fail_active_attempt(paths: &RemoteUpdatePaths, attempt_id: &str, _detail: &str) {
    let Ok(_lock) = lock_exclusive(&paths.lock) else {
        return;
    };
    let Ok(Some(mut status)) = read_status(&paths.state) else {
        return;
    };
    if status.attempt_id != attempt_id || !status.phase.active() {
        return;
    }
    status.fail(
        "update_helper_failed",
        "The managed update could not complete.",
        "The existing installation remains available when possible. Inspect the private machine-local update log, repair the reported issue, then retry.",
        true,
    );
    let _ = write_status(&paths.state, &status);
}

fn apply_check_report(status: &mut DurableStatus, bytes: &[u8]) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return;
    };
    status.update_available = value
        .get("updateAvailable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let Some(repositories) = value
        .get("repositories")
        .and_then(serde_json::Value::as_array)
    else {
        return;
    };
    for repository in repositories {
        if repository
            .get("component")
            .and_then(serde_json::Value::as_str)
            == Some("nakode")
        {
            status.target_build_revision = repository
                .get("targetRevision")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            status.target_version = repository
                .get("targetVersion")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
    }
}

fn private_log(path: &Path) -> Result<File, String> {
    create_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    set_private(&file)?;
    Ok(file)
}

fn lock_exclusive(path: &Path) -> Result<File, String> {
    create_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|error| error.to_string())?;
    set_private(&file)?;
    file.lock_exclusive().map_err(|error| error.to_string())?;
    Ok(file)
}

fn try_lock_exclusive(path: &Path) -> Result<Option<File>, String> {
    create_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|error| error.to_string())?;
    set_private(&file)?;
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn read_status(path: &Path) -> Result<Option<DurableStatus>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("remote update status is malformed: {error}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn write_status(path: &Path, status: &DurableStatus) -> Result<(), String> {
    create_parent(path)?;
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    set_private(&file)?;
    serde_json::to_writer(&mut file, status).map_err(|error| error.to_string())?;
    file.write_all(b"\n").map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())
}

fn create_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "remote update path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private(file: &File) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())
}

#[cfg(not(unix))]
fn set_private(_file: &File) -> Result<(), String> {
    Ok(())
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

fn systemd_run_executable() -> Option<PathBuf> {
    ["/usr/bin/systemd-run", "/bin/systemd-run"]
        .into_iter()
        .map(PathBuf::from)
        .find(|candidate| is_executable(candidate))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn bounded(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn internal_status(error: &str) -> Status {
    Status::internal(bounded(error, 500))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: &str, server_id: &str) -> api::StartRemoteUpdateRequest {
        api::StartRemoteUpdateRequest {
            idempotency_key: key.to_owned(),
            expected_server_id: server_id.to_owned(),
            expected_build_revision: crate::BUILD_REVISION.map(str::to_owned),
        }
    }

    #[test]
    fn stale_target_is_rejected_without_creating_an_attempt() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let result = service
            .start(request("one", "machine-b"))
            .expect("response");
        assert_eq!(
            result.outcome,
            i32::from(api::StartRemoteUpdateOutcome::StaleTarget)
        );
        assert!(!service.paths.state.exists());
    }

    #[test]
    fn unsupported_install_is_explicit_and_non_mutating() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let result = service
            .start(request("one", "machine-a"))
            .expect("response");
        assert_eq!(
            result.outcome,
            i32::from(api::StartRemoteUpdateOutcome::Unsupported)
        );
        let status = result.status.expect("status");
        assert_eq!(
            status.capability,
            i32::from(api::RemoteUpdateCapability::Unsupported)
        );
        assert!(!service.paths.state.exists());
    }

    #[test]
    fn duplicate_keys_replay_the_same_durable_attempt() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let mut status = DurableStatus::requested("machine-a".to_owned(), "same".to_owned());
        status.phase = Phase::Failed;
        write_status(&service.paths.state, &status).expect("write status");

        let result = service
            .start(request("same", "machine-a"))
            .expect("response");
        assert_eq!(
            result.outcome,
            i32::from(api::StartRemoteUpdateOutcome::Replayed)
        );
        assert_eq!(result.status.expect("status").attempt_id, status.attempt_id);
    }

    #[test]
    fn delayed_retry_of_a_recent_attempt_cannot_start_again() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let mut status = DurableStatus::requested("machine-a".to_owned(), "new-key".to_owned());
        status.phase = Phase::Succeeded;
        status.recent_idempotency_keys.push("old-key".to_owned());
        write_status(&service.paths.state, &status).expect("write status");

        let result = service
            .start(request("old-key", "machine-a"))
            .expect("response");

        assert_eq!(
            result.outcome,
            i32::from(api::StartRemoteUpdateOutcome::Replayed)
        );
        assert_eq!(result.status.expect("status").attempt_id, status.attempt_id);
    }

    #[test]
    fn another_request_coalesces_while_an_attempt_is_active() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let status = DurableStatus::requested("machine-a".to_owned(), "first".to_owned());
        write_status(&service.paths.state, &status).expect("write status");

        let result = service
            .start(request("second", "machine-a"))
            .expect("response");
        assert_eq!(
            result.outcome,
            i32::from(api::StartRemoteUpdateOutcome::AlreadyInProgress)
        );
        assert_eq!(result.status.expect("status").attempt_id, status.attempt_id);
    }

    #[test]
    fn helper_progress_cannot_overwrite_a_verified_terminal_snapshot() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let mut verified = DurableStatus::requested("machine-a".to_owned(), "first".to_owned());
        verified.phase = Phase::Succeeded;
        verified.revision = 20;
        write_status(&service.paths.state, &verified).expect("write verified status");
        let mut stale = verified.clone();
        stale.phase = Phase::Restarting;
        stale.revision = 10;

        let error = publish_attempt_status(&service.paths, &verified.attempt_id, &mut stale)
            .expect_err("stale helper must stop");

        assert!(error.contains("no longer owns"));
        let current = read_status(&service.paths.state)
            .expect("read status")
            .expect("persisted status");
        assert_eq!(current.phase, Phase::Succeeded);
        assert_eq!(current.revision, 20);
    }

    #[test]
    fn supervised_fstack_install_publishes_restart_before_service_replacement() {
        assert_eq!(phase_for_stage("nakode-complete"), Some(Phase::Installing));
        assert_eq!(phase_for_stage("fstack-install"), Some(Phase::Restarting));
    }

    #[test]
    fn malformed_durable_status_fails_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        create_parent(&service.paths.state).expect("create state parent");
        fs::write(&service.paths.state, b"not-json").expect("write malformed state");

        let error = service.status().expect_err("malformed status must fail");

        assert_eq!(error.code(), tonic::Code::Internal);
    }

    #[test]
    fn restarted_service_succeeds_only_after_matching_build_verification() {
        let Some(revision) = crate::BUILD_REVISION else {
            return;
        };
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let mut status = DurableStatus::requested("machine-a".to_owned(), "first".to_owned());
        status.phase = Phase::Restarting;
        status.target_build_revision = Some(revision.to_owned());
        write_status(&service.paths.state, &status).expect("write status");

        service.reconcile_restarted_attempt();

        let verified = read_status(&service.paths.state)
            .expect("read status")
            .expect("persisted status");
        assert_eq!(verified.phase, Phase::Succeeded);
        assert_eq!(verified.current_build_revision.as_deref(), Some(revision));
        assert!(!verified.update_available);
    }

    #[test]
    fn stale_active_attempt_becomes_a_recoverable_failure() {
        let root = tempfile::tempdir().expect("tempdir");
        let service = RemoteUpdateService::isolated("machine-a", root.path());
        let mut status = DurableStatus::requested("machine-a".to_owned(), "first".to_owned());
        status.updated_at_unix_ms = now_ms().saturating_sub(ACTIVE_ATTEMPT_TIMEOUT_MS + 1);
        write_status(&service.paths.state, &status).expect("write status");

        service.recover_stale_attempt();

        let recovered = read_status(&service.paths.state)
            .expect("read status")
            .expect("persisted status");
        assert_eq!(recovered.phase, Phase::Failed);
        let failure = recovered.failure.expect("failure");
        assert_eq!(failure.code, "update_interrupted");
        assert!(failure.retryable);
    }

    #[test]
    fn check_report_records_the_authoritative_target_revision() {
        let mut status = DurableStatus::requested("machine-a".to_owned(), "key".to_owned());
        apply_check_report(
            &mut status,
            br#"{"updateAvailable":true,"repositories":[{"component":"nakode","targetRevision":"abc123","targetVersion":"0.4.0"}]}"#,
        );
        assert!(status.update_available);
        assert_eq!(status.target_build_revision.as_deref(), Some("abc123"));
        assert_eq!(status.target_version.as_deref(), Some("0.4.0"));
    }
}
