use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
    time::SystemTime,
};

use directories::ProjectDirs;
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

use crate::config::Config;

const SERVICE_START_ATTEMPTS: usize = 40;
const SERVICE_STOP_ATTEMPTS: usize = 100;
const ACTIVATION_LOCK_ATTEMPTS: usize = 120;
const ACTIVATION_LOCK_STALE_AFTER: Duration = Duration::from_secs(30);
const SERVICE_START_RETRY: Duration = Duration::from_millis(50);
const RESUME_ENVIRONMENT_KEYS: [&str; 2] = ["NAKODE_RESUME", "NAKO_AGENT_RESUME"];
const SERVICE_EXECUTABLE_IDENTITY_ENVIRONMENT: &str = "NAKODE_SERVICE_EXECUTABLE_IDENTITY";

/// Workspace service state returned by the lifecycle CLI.
///
/// Every field describes the service reached for the named workspace. The
/// process record and API metadata are read from the private directory and
/// socket serving that workspace, so a report never covers one the caller did
/// not name.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceStatus {
    pub running: bool,
    pub workspace: PathBuf,
    /// Version of the `nakode` executable that produced this report.
    pub nakode_version: String,
    /// Concrete installed/CLI executable that produced this report.
    pub nakode_executable: ExecutableIdentity,
    /// Concrete executable captured by the live service at startup.
    pub service_executable: Option<ExecutableIdentity>,
    /// Process identifier recorded by the running service, when it published one.
    pub pid: Option<u32>,
    pub started_at_unix_ms: Option<u64>,
    pub started_at_utc: Option<String>,
    pub uptime_seconds: Option<u64>,
    /// Socket implementing the generated Nakode API for native frontends.
    pub endpoint: PathBuf,
    pub lifecycle_socket: PathBuf,
    pub log: PathBuf,
    /// API metadata reported by the running service, absent when it is stopped.
    pub server: Option<ServerReport>,
}

/// Server and API identity reported by a running service through `GetServerInfo`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServerReport {
    pub server_version: String,
    pub api_version: String,
    pub capabilities: Vec<String>,
}

/// Process identity published by the installation-wide service.
///
/// The file lives beside the installation's sockets, is written when the service
/// acquires its lease, and is removed when that lease is released. It is only
/// trusted while the lifecycle socket answers, so a record left behind by a
/// killed process is never reported as a live one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceRuntimeRecord {
    pub pid: u32,
    pub started_at_unix_ms: u64,
    pub version: String,
    /// Canonical workspace served by this process. Old records omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<PathBuf>,
    /// Immutable identity captured from the executable vnode when this process started.
    ///
    /// Old runtime records omit this field. That is intentionally distinct from a match: a new
    /// connector cannot prove an identity for an old process from the path after an in-place update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<ExecutableIdentity>,
}

/// Content and filesystem identity of one concrete Nakode executable.
///
/// SHA-256 is the compatibility identity. The remaining fields make stale-process diagnostics
/// actionable and prove when a process still maps a replaced vnode at the same display path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutableIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<String>,
}

impl ExecutableIdentity {
    #[must_use]
    pub(crate) fn same_build(&self, other: &Self) -> bool {
        self.sha256 == other.sha256 && self.size == other.size
    }
}

/// How endpoint discovery obtained the verified installation service.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointActivation {
    Reused,
    Started,
    RestartedStaleService,
}

/// Verified endpoint and identities returned to native frontend connectors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendEndpoint {
    pub workspace: PathBuf,
    pub endpoint: PathBuf,
    /// Installation-scoped activation status endpoint. It may be helper-owned while `endpoint`
    /// remains owned by an older API-compatible service.
    pub activation_endpoint: PathBuf,
    pub lifecycle_socket: PathBuf,
    pub cli: ExecutableIdentity,
    pub service: ServiceRuntimeRecord,
    pub server: Option<ServerReport>,
    pub activation: EndpointActivation,
}

/// Result of starting the installation service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartOutcome {
    Started,
    AlreadyRunning,
}

/// Result of stopping the installation service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopOutcome {
    Stopped,
    AlreadyStopped,
}

/// Where the installation-wide service keeps its runtime state.
///
/// Caller workspaces intentionally do not participate in this identity. Resolving from any
/// workspace therefore yields the same sockets, lease, log, and process record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServicePaths {
    lifecycle: PathBuf,
    api: PathBuf,
    log: PathBuf,
    runtime: PathBuf,
    activation: PathBuf,
    activation_api: PathBuf,
    activation_journal: PathBuf,
    activation_helper_lock: PathBuf,
    activation_log: PathBuf,
}

impl ServicePaths {
    /// Resolves the installation-wide service runtime.
    ///
    /// The workspace argument is retained for source compatibility with callers that select a
    /// session access root. It never participates in daemon identity.
    ///
    /// # Errors
    /// Returns an error when the private runtime directory cannot be prepared.
    pub fn resolve(workspace: &Path) -> Result<Self, ControlError> {
        Self::resolve_in(&control_directory()?, &installation_workspace()?, workspace)
    }

    fn resolve_in(
        control_root: &Path,
        installation_workspace: &Path,
        _session_access_root: &Path,
    ) -> Result<Self, ControlError> {
        Ok(Self::in_directory(&workspace_runtime_directory_in(
            control_root,
            installation_workspace,
        )?))
    }

    /// Names the runtime files a service keeps in one prepared directory.
    #[must_use]
    pub fn in_directory(directory: &Path) -> Self {
        Self {
            lifecycle: directory.join("c.sock"),
            api: directory.join("api.sock"),
            log: directory.join("service.log"),
            runtime: directory.join("service.json"),
            activation: directory.join("activation.lock"),
            activation_api: directory.join("activation.sock"),
            activation_journal: directory.join("activation.json"),
            activation_helper_lock: directory.join("activation-helper.lock"),
            activation_log: directory.join("activation.log"),
        }
    }

    /// Resolves the service addressed by a validated configuration.
    ///
    /// # Errors
    /// Returns an error when the private runtime directory cannot be prepared.
    pub fn of(config: &Config) -> Result<Self, ControlError> {
        Self::resolve(&config.workspace)
    }

    /// Socket carrying lifecycle requests.
    #[must_use]
    pub fn lifecycle(&self) -> &Path {
        &self.lifecycle
    }

    /// Socket implementing the generated Nakode API.
    #[must_use]
    pub fn api(&self) -> &Path {
        &self.api
    }

    /// Captured standard output and standard error of a background service.
    #[must_use]
    pub fn log(&self) -> &Path {
        &self.log
    }

    /// Process record published by the running service.
    #[must_use]
    pub fn runtime(&self) -> &Path {
        &self.runtime
    }

    /// Connector lease serializing stale-build activation for this workspace.
    #[must_use]
    pub fn activation(&self) -> &Path {
        &self.activation
    }

    /// Helper-owned `ActivationService` socket while activation is pending.
    #[must_use]
    pub fn activation_api(&self) -> &Path {
        &self.activation_api
    }

    /// Durable, versioned deferred-activation journal.
    #[must_use]
    pub fn activation_journal(&self) -> &Path {
        &self.activation_journal
    }

    /// Long-lived singleton lease owned by the installed activation helper.
    #[must_use]
    pub fn activation_helper_lock(&self) -> &Path {
        &self.activation_helper_lock
    }

    /// Captured output from the detached activation helper.
    #[must_use]
    pub fn activation_log(&self) -> &Path {
        &self.activation_log
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BulkServiceShutdownReport {
    /// Live workspace services that acknowledged shutdown and released their sockets.
    pub stopped: usize,
    /// Stale workspace socket sets removed after no service accepted a connection.
    pub stale: usize,
    /// Runtime socket sets that could not be stopped or safely classified.
    pub failures: Vec<String>,
}

/// Result of the post-install stale-service refresh.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StaleServiceRefreshReport {
    pub current: usize,
    pub restarted: usize,
    /// Legacy per-workspace daemons or stale runtime sets retired during singleton migration.
    pub retired: usize,
    pub active: Vec<String>,
    pub inactive: Vec<String>,
    pub unavailable: Vec<String>,
    pub unknown: Vec<String>,
    pub failures: Vec<String>,
}

const STALE_SERVICE_CONCURRENCY: usize = 4;

/// Runtime state reported for a frontend transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransportStatus {
    pub name: String,
    pub enabled: bool,
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Runtime operation applied to an independent frontend transport.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportAction {
    Start,
    Stop,
    Restart,
    Status,
}

pub(crate) trait TransportController: Send + Sync {
    fn autostart(&self) -> BoxFuture<'_, Result<TransportStatus, String>>;
    fn start(&self) -> BoxFuture<'_, Result<TransportStatus, String>>;
    fn stop(&self) -> BoxFuture<'_, Result<TransportStatus, String>>;
    fn restart(&self) -> BoxFuture<'_, Result<TransportStatus, String>>;
    fn status(&self) -> BoxFuture<'_, Result<TransportStatus, String>>;
}

#[derive(Clone, Default)]
pub(crate) struct TransportSupervisor {
    transports: Arc<HashMap<String, Arc<dyn TransportController>>>,
}

impl TransportSupervisor {
    #[cfg(test)]
    pub(crate) fn new(
        transports: impl IntoIterator<Item = (String, Arc<dyn TransportController>)>,
    ) -> Self {
        Self {
            transports: Arc::new(transports.into_iter().collect()),
        }
    }

    async fn autostart(&self) {
        for (name, transport) in self.transports.iter() {
            if let Err(error) = transport.autostart().await {
                eprintln!("nakode {name}: could not start transport: {error}");
            }
        }
    }

    pub(crate) async fn control(
        &self,
        name: &str,
        action: TransportAction,
    ) -> Result<TransportStatus, String> {
        let transport = self
            .transports
            .get(name)
            .ok_or_else(|| format!("unknown transport {name:?}"))?;
        match action {
            TransportAction::Start => transport.start().await,
            TransportAction::Stop => transport.stop().await,
            TransportAction::Restart => transport.restart().await,
            TransportAction::Status => transport.status().await,
        }
    }

    async fn stop_all(&self) {
        for (name, transport) in self.transports.iter() {
            if let Err(error) = transport.stop().await {
                eprintln!("nakode {name}: could not stop transport: {error}");
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("Nakode server socket error at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid Nakode lifecycle message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Nakode server closed without a lifecycle response")]
    MissingResponse,
    #[error("a Nakode server is already running at {0}")]
    AlreadyRunning(String),
    #[error("this platform does not expose an application data directory")]
    MissingDataDirectory,
    #[error("could not start the Nakode server: {0}")]
    SpawnService(#[source] std::io::Error),
    #[error("Nakode server did not become ready at {0}")]
    ServiceStartup(String),
    #[error("Nakode server did not stop at {0}")]
    ServiceShutdown(String),
    #[error("Nakode server rejected the lifecycle request: {0}")]
    ServiceRejected(String),
    #[error(
        "the running Nakode server uses different server configuration; stop it before changing server-owned options"
    )]
    ConfigurationMismatch,
    #[error(transparent)]
    NativeRuntime(#[from] crate::server::runtime::NativeRuntimeError),
    #[error("Nakode gRPC service failed: {0}")]
    Grpc(#[from] tonic::transport::Error),
    #[error("Nakode server component stopped unexpectedly: {0}")]
    ComponentStopped(&'static str),
    #[error("could not identify Nakode executable at {path}: {source}")]
    ExecutableIdentity {
        path: String,
        source: std::io::Error,
    },
    #[error("cannot activate stale Nakode installation service: {0}")]
    StaleServiceActivation(String),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LifecycleRequest {
    Ping,
    Shutdown,
    QuiesceShutdown,
    ForceShutdown {
        expected: Vec<crate::server::runtime::QuiescenceBlocker>,
    },
    Transport {
        name: String,
        action: TransportAction,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LifecycleResponse {
    Ok,
    Ready { configuration: String },
    Transport { status: TransportStatus },
    Error { message: String },
}

/// Runs the native workspace server until it receives a lifecycle shutdown
/// request, its terminal interrupts it, or one of its required components
/// stops.
///
/// # Errors
/// Returns when socket acquisition, runtime preparation, or a server component
/// fails.
pub async fn run_service(config: Config) -> Result<(), ControlError> {
    let config = installation_config(&config)?;
    let paths = ServicePaths::of(&config)?;
    let mut lease = WorkspaceServerLease::acquire(&paths, &config.workspace).await?;
    // Only a backgrounded service writes into the captured log, and only it may
    // rotate that file. A foreground run owns a terminal instead.
    if let Some(log) = std::env::var_os(crate::service_log::LOG_PATH_ENVIRONMENT) {
        tokio::spawn(crate::service_log::supervise_size(PathBuf::from(log)));
    }
    let configuration = service_configuration_fingerprint(&config);
    let prepared = crate::server::runtime::prepare_runtime(&config).await?;
    let (runtime, handle) = prepared.into_actor();
    eprintln!(
        "nakode service started for workspace {}",
        config.workspace.display()
    );
    let lifecycle_path = lease.lifecycle_path.clone();
    let grpc_path = lease.grpc_path.clone();
    let lifecycle_listener = lease
        .lifecycle
        .take()
        .ok_or(ControlError::ComponentStopped(
            "workspace lifecycle listener lease",
        ))?;
    let grpc_listener = lease.grpc.take().ok_or(ControlError::ComponentStopped(
        "workspace gRPC listener lease",
    ))?;
    let endpoint = handle.endpoint().clone();
    let service_executable =
        std::env::current_exe().map_err(|source| ControlError::ExecutableIdentity {
            path: "current executable".to_owned(),
            source,
        })?;
    let remote_config = crate::remote::load()
        .map_err(|error| ControlError::ServiceRejected(error.to_string()))?
        .filter(|value| value.enabled);
    let server_id = crate::remote::installation_server_id()
        .map_err(|error| ControlError::ServiceRejected(error.to_string()))?;
    let transports = TransportSupervisor::default();
    let mut lifecycle = tokio::spawn(run_lifecycle_listener(
        lifecycle_listener,
        lifecycle_path,
        configuration,
        transports.clone(),
        Some(handle.clone()),
    ));
    let mut grpc = tokio::spawn(run_grpc_listener(
        grpc_listener,
        grpc_path.clone(),
        endpoint.clone(),
        paths.clone(),
        service_executable,
        server_id.clone(),
    ));
    let mut remote_grpc =
        tokio::spawn(run_remote_grpc_listener(remote_config, endpoint, server_id));
    let mut actor = tokio::spawn(runtime.run());
    transports.autostart().await;

    let result = tokio::select! {
        result = &mut lifecycle => flatten_component(result, "lifecycle listener"),
        result = &mut grpc => flatten_component(result, "gRPC listener"),
        result = &mut remote_grpc => flatten_component(result, "remote gRPC listener"),
        result = &mut actor => match result {
            Ok(()) => Err(ControlError::ComponentStopped("native runtime")),
            Err(_) => Err(ControlError::ComponentStopped("native runtime task")),
        },
        // A foreground service is stopped from its terminal. Leaving the
        // interrupt to the default disposition would kill the process before
        // the lease released its sockets and process record.
        signal = tokio::signal::ctrl_c() => match signal {
            Ok(()) => Ok(()),
            Err(_) => Err(ControlError::ComponentStopped("interrupt handler")),
        },
    };
    handle.shutdown().await;
    lifecycle.abort();
    grpc.abort();
    remote_grpc.abort();
    transports.stop_all().await;
    if !actor.is_finished() {
        let _ = actor.await;
    }
    eprintln!(
        "nakode service stopped for workspace {}",
        config.workspace.display()
    );
    result
}

async fn run_lifecycle_listener(
    listener: UnixListener,
    path: PathBuf,
    configuration: String,
    transports: TransportSupervisor,
    runtime: Option<crate::server::runtime::NativeServerHandle>,
) -> Result<(), ControlError> {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|source| socket_error(&path, source))?;
                let shutdown_tx = shutdown_tx.clone();
                let configuration = configuration.clone();
                let transports = transports.clone();
                let runtime = runtime.clone();
                connections.spawn(async move {
                    handle_lifecycle_connection(
                        stream,
                        shutdown_tx,
                        &configuration,
                        &transports,
                        runtime.as_ref(),
                    )
                    .await;
                });
            }
            shutdown = shutdown_rx.recv() => {
                if shutdown.is_some() {
                    break;
                }
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                let _ = completed;
            }
        }
    }
    connections.abort_all();
    drop(listener);
    Ok(())
}

async fn handle_lifecycle_connection(
    stream: UnixStream,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    configuration: &str,
    transports: &TransportSupervisor,
    runtime: Option<&crate::server::runtime::NativeServerHandle>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    if BufReader::new(reader).read_line(&mut line).await.is_err() {
        return;
    }
    let request = match serde_json::from_str(&line) {
        Ok(request) => request,
        Err(error) => {
            write_lifecycle_response(
                &mut writer,
                &LifecycleResponse::Error {
                    message: error.to_string(),
                },
            )
            .await;
            return;
        }
    };
    let mut should_shutdown = matches!(request, LifecycleRequest::Shutdown);
    let response = match request {
        LifecycleRequest::Ping => LifecycleResponse::Ready {
            configuration: configuration.to_owned(),
        },
        LifecycleRequest::Shutdown => LifecycleResponse::Ok,
        LifecycleRequest::QuiesceShutdown => match runtime {
            Some(runtime) => {
                match tokio::time::timeout(Duration::from_secs(3), runtime.quiesce()).await {
                    Ok(Ok(())) => {
                        should_shutdown = true;
                        LifecycleResponse::Ok
                    }
                    Ok(Err(message)) => LifecycleResponse::Error { message },
                    Err(_) => LifecycleResponse::Error {
                        message: "timed out while atomically fencing the installation service"
                            .to_owned(),
                    },
                }
            }
            None => LifecycleResponse::Error {
                message: "atomic quiescent shutdown is unavailable".to_owned(),
            },
        },
        LifecycleRequest::ForceShutdown { expected } => match runtime {
            Some(runtime) => {
                match tokio::time::timeout(Duration::from_secs(3), runtime.force_quiesce(expected))
                    .await
                {
                    Ok(Ok(())) => {
                        should_shutdown = true;
                        LifecycleResponse::Ok
                    }
                    Ok(Err(message)) => LifecycleResponse::Error { message },
                    Err(_) => LifecycleResponse::Error {
                        message: "timed out while atomically comparing the activation blockers"
                            .to_owned(),
                    },
                }
            }
            None => LifecycleResponse::Error {
                message: "conditional activation is unavailable".to_owned(),
            },
        },
        LifecycleRequest::Transport { name, action } => {
            match transports.control(&name, action).await {
                Ok(status) => LifecycleResponse::Transport { status },
                Err(message) => LifecycleResponse::Error { message },
            }
        }
    };
    write_lifecycle_response(&mut writer, &response).await;
    if should_shutdown {
        let _ = shutdown_tx.send(()).await;
    }
}

async fn write_lifecycle_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &LifecycleResponse,
) {
    if let Ok(encoded) = serde_json::to_string(response) {
        let _ = writer.write_all(encoded.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

async fn run_grpc_listener(
    listener: UnixListener,
    _path: PathBuf,
    endpoint: nakode_server::ServerEndpoint,
    paths: ServicePaths,
    executable: PathBuf,
    server_id: String,
) -> Result<(), ControlError> {
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(
            nakode_server::grpc::GrpcService::new(endpoint)
                .with_server_id(server_id)
                .into_server(),
        )
        .add_service(
            crate::activation::ActivationGrpcService::read_only(paths, executable).into_server(),
        )
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

async fn run_remote_grpc_listener(
    config: Option<crate::remote::RemoteConfig>,
    endpoint: nakode_server::ServerEndpoint,
    server_id: String,
) -> Result<(), ControlError> {
    let Some(config) = config else {
        std::future::pending::<()>().await;
        return Ok(());
    };
    let identity = tonic::transport::Identity::from_pem(
        config.certificate_pem.as_bytes(),
        config.private_key_pem.as_bytes(),
    );
    let tls = tonic::transport::ServerTlsConfig::new().identity(identity);
    eprintln!("nakode remote API listening at {}", config.bind);
    tonic::transport::Server::builder()
        .tls_config(tls)?
        .add_service(
            nakode_server::grpc::GrpcService::new(endpoint)
                .with_server_id(server_id)
                .into_authenticated_server(config.api_key),
        )
        .serve(config.bind)
        .await?;
    Ok(())
}

fn flatten_component(
    result: Result<Result<(), ControlError>, tokio::task::JoinError>,
    name: &'static str,
) -> Result<(), ControlError> {
    match result {
        Ok(result) => result,
        Err(_) => Err(ControlError::ComponentStopped(name)),
    }
}

struct WorkspaceServerLease {
    lifecycle_path: PathBuf,
    grpc_path: PathBuf,
    runtime_path: PathBuf,
    lifecycle: Option<UnixListener>,
    grpc: Option<UnixListener>,
}

impl WorkspaceServerLease {
    async fn acquire(paths: &ServicePaths, workspace: &Path) -> Result<Self, ControlError> {
        let lifecycle_path = paths.lifecycle().to_path_buf();
        let grpc_path = paths.api().to_path_buf();
        let runtime_path = paths.runtime().to_path_buf();
        let lifecycle = bind_service_listener(&lifecycle_path).await?;
        let grpc = match bind_service_listener(&grpc_path).await {
            Ok(listener) => listener,
            Err(error) => {
                drop(lifecycle);
                let _ = std::fs::remove_file(&lifecycle_path);
                return Err(error);
            }
        };
        // Never leave a prior owner's identity beside newly acquired sockets if publication fails.
        let _ = std::fs::remove_file(&runtime_path);
        publish_runtime_record(&runtime_path, workspace);
        Ok(Self {
            lifecycle_path,
            grpc_path,
            runtime_path,
            lifecycle: Some(lifecycle),
            grpc: Some(grpc),
        })
    }
}

impl Drop for WorkspaceServerLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.lifecycle_path);
        let _ = std::fs::remove_file(&self.grpc_path);
        let _ = std::fs::remove_file(&self.runtime_path);
    }
}

/// Records this process as the owner of the installation service.
///
/// A service that cannot publish its process record still serves clients, so a
/// write failure only costs `nakode status` its process detail.
fn publish_runtime_record(runtime_path: &Path, workspace: &Path) {
    let executable = std::env::var(SERVICE_EXECUTABLE_IDENTITY_ENVIRONMENT)
        .ok()
        .and_then(|encoded| serde_json::from_str::<ExecutableIdentity>(&encoded).ok())
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|path| match executable_identity(&path) {
                    Ok(identity) => Some(identity),
                    Err(error) => {
                        eprintln!(
                            "nakode: could not identify the running service executable: {error}"
                        );
                        None
                    }
                })
        });
    let record = ServiceRuntimeRecord {
        pid: std::process::id(),
        started_at_unix_ms: crate::diagnostics::unix_time_ms(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        workspace: Some(workspace.to_path_buf()),
        executable,
    };
    match serde_json::to_vec(&record) {
        Ok(encoded) => {
            if let Err(error) = write_private_file(runtime_path, &encoded) {
                eprintln!(
                    "nakode: could not record the service process at {}: {error}",
                    runtime_path.display()
                );
            }
        }
        Err(error) => eprintln!("nakode: could not encode the service process record: {error}"),
    }
}

/// Writes a file that only the desktop user can read, matching the sockets and
/// log it sits beside.
pub(crate) fn write_private_file(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    #[cfg(windows)]
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    match std::fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            Err(error)
        }
    }
}

pub(crate) fn read_runtime_record(runtime_path: &Path) -> Option<ServiceRuntimeRecord> {
    let encoded = std::fs::read(runtime_path).ok()?;
    serde_json::from_slice(&encoded).ok()
}

/// Computes the immutable content identity used by endpoint activation.
///
/// The service calls this once while acquiring its installation lease. Connectors compute it for the
/// executable they are about to spawn. No running process ever re-hashes a path after publication.
pub(crate) fn executable_identity(path: &Path) -> Result<ExecutableIdentity, ControlError> {
    use std::io::Read;

    let canonical =
        std::fs::canonicalize(path).map_err(|source| ControlError::ExecutableIdentity {
            path: path.display().to_string(),
            source,
        })?;
    let mut file =
        std::fs::File::open(&canonical).map_err(|source| ControlError::ExecutableIdentity {
            path: canonical.display().to_string(),
            source,
        })?;
    let metadata = file
        .metadata()
        .map_err(|source| ControlError::ExecutableIdentity {
            path: canonical.display().to_string(),
            source,
        })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ControlError::ExecutableIdentity {
                path: canonical.display().to_string(),
                source,
            })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let modified_at_unix_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    #[cfg(unix)]
    let (device, inode) = {
        use std::os::unix::fs::MetadataExt;
        (
            Some(metadata.dev().to_string()),
            Some(metadata.ino().to_string()),
        )
    };
    #[cfg(not(unix))]
    let (device, inode) = (None, None);
    Ok(ExecutableIdentity {
        path: canonical,
        sha256: format!("{:x}", digest.finalize()),
        size: metadata.len(),
        modified_at_unix_ms,
        device,
        inode,
    })
}

/// Returns the process record published by this workspace's service.
///
/// An absent or unreadable record is reported as `None`: it only costs the
/// caller a process detail, never correctness.
///
/// # Errors
/// Returns an error when the platform data directory cannot be prepared.
#[must_use]
pub fn service_runtime_record(paths: &ServicePaths) -> Option<ServiceRuntimeRecord> {
    read_runtime_record(paths.runtime())
}

/// Reports whether a bound socket path is still served by a live listener.
///
/// A listener whose owner has just released it can keep completing connections for a short window,
/// so one successful connect does not distinguish a live server from an already-stale socket.
/// Confirm with a second probe: a live listener stays connectable, while a released one refuses the
/// retry. Only a failing connect can classify a socket as dead, so this never steals the path from
/// a busy server.
pub(crate) async fn socket_is_live(path: &Path) -> bool {
    if UnixStream::connect(path).await.is_err() {
        return false;
    }
    tokio::time::sleep(SERVICE_START_RETRY).await;
    UnixStream::connect(path).await.is_ok()
}

pub(crate) async fn bind_service_listener(path: &Path) -> Result<UnixListener, ControlError> {
    if path.exists() {
        if socket_is_live(path).await {
            return Err(ControlError::AlreadyRunning(path.display().to_string()));
        }
        std::fs::remove_file(path).map_err(|source| socket_error(path, source))?;
    }
    UnixListener::bind(path).map_err(|source| socket_error(path, source))
}

async fn exchange<Request, Response>(
    path: &Path,
    request: &Request,
) -> Result<Response, ControlError>
where
    Request: Serialize,
    Response: for<'de> Deserialize<'de>,
{
    let display_path = path.display().to_string();
    let stream = UnixStream::connect(path)
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    let (reader, mut writer) = stream.into_split();
    let encoded = serde_json::to_string(request)?;
    writer
        .write_all(encoded.as_bytes())
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .map_err(|source| ControlError::Io {
            path: display_path,
            source,
        })?;
    if line.is_empty() {
        return Err(ControlError::MissingResponse);
    }
    Ok(serde_json::from_str(&line)?)
}

async fn expect_ok(path: &Path, request: &LifecycleRequest) -> Result<(), ControlError> {
    match exchange(path, request).await? {
        LifecycleResponse::Ok => Ok(()),
        LifecycleResponse::Ready { .. } => Err(ControlError::ServiceRejected(
            "unexpected readiness response".to_owned(),
        )),
        LifecycleResponse::Transport { .. } => Err(ControlError::ServiceRejected(
            "unexpected transport response".to_owned(),
        )),
        LifecycleResponse::Error { message } => Err(ControlError::ServiceRejected(message)),
    }
}

async fn ping_at(service_path: &Path, config: &Config) -> Result<(), ControlError> {
    let configuration = running_configuration_at(service_path).await?;
    if configuration == service_configuration_fingerprint(config) {
        Ok(())
    } else {
        Err(ControlError::ConfigurationMismatch)
    }
}

async fn running_configuration_at(service_path: &Path) -> Result<String, ControlError> {
    match exchange(service_path, &LifecycleRequest::Ping).await? {
        LifecycleResponse::Ready { configuration } => Ok(configuration),
        LifecycleResponse::Ok => Err(ControlError::ServiceRejected(
            "unexpected lifecycle readiness response".to_owned(),
        )),
        LifecycleResponse::Transport { .. } => Err(ControlError::ServiceRejected(
            "unexpected transport readiness response".to_owned(),
        )),
        LifecycleResponse::Error { message } => Err(ControlError::ServiceRejected(message)),
    }
}

async fn ensure_service(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
) -> Result<(), ControlError> {
    ensure_service_at(paths, executable, config).await
}

async fn ensure_service_at(
    service_path: &ServicePaths,
    executable: &Path,
    config: &Config,
) -> Result<(), ControlError> {
    match ping_at(service_path.lifecycle(), config).await {
        Ok(()) => return Ok(()),
        Err(ControlError::ConfigurationMismatch) => {
            return Err(ControlError::ConfigurationMismatch);
        }
        Err(_) => {}
    }

    let mut command = service_command(executable, config);
    capture_service_output(&mut command, service_path.log());
    let mut child = command.spawn().map_err(ControlError::SpawnService)?;
    tokio::spawn(async move {
        let _ = child.wait().await;
    });

    for _ in 0..SERVICE_START_ATTEMPTS {
        tokio::time::sleep(SERVICE_START_RETRY).await;
        if ping_at(service_path.lifecycle(), config).await.is_ok() {
            return Ok(());
        }
    }
    Err(ControlError::ServiceStartup(
        service_path.lifecycle().display().to_string(),
    ))
}

fn service_command(executable: &Path, config: &Config) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(executable);
    command.args(service_arguments(config)).stdin(Stdio::null());
    if let Ok(identity) = executable_identity(executable)
        && let Ok(encoded) = serde_json::to_string(&identity)
    {
        command.env(SERVICE_EXECUTABLE_IDENTITY_ENVIRONMENT, encoded);
    }
    detach_service_process(&mut command);
    for key in RESUME_ENVIRONMENT_KEYS {
        command.env_remove(key);
    }
    command
}

/// Directs a service about to be spawned into this workspace's captured log.
///
/// A background service has no terminal, so the log file is the only place its
/// lifecycle output can go. Standard output and standard error receive
/// descriptors onto the same appending file description, so the two streams
/// interleave in write order instead of overwriting each other. A workspace
/// whose log cannot be opened still gets a service; it only loses `nakode logs`.
pub(crate) fn capture_service_output(command: &mut tokio::process::Command, log: &Path) {
    match open_service_log(log) {
        Ok((log, output, errors)) => {
            command
                .stdout(output)
                .stderr(errors)
                .env(crate::service_log::LOG_PATH_ENVIRONMENT, log);
        }
        Err(error) => {
            eprintln!("nakode: could not capture service output: {error}");
            command
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .env_remove(crate::service_log::LOG_PATH_ENVIRONMENT);
        }
    }
}

fn open_service_log(log: &Path) -> Result<(PathBuf, Stdio, Stdio), ControlError> {
    let log = log.to_path_buf();
    crate::service_log::rotate_if_oversized(&log).map_err(|source| socket_error(&log, source))?;
    let file =
        crate::service_log::open_for_append(&log).map_err(|source| socket_error(&log, source))?;
    let errors = file
        .try_clone()
        .map_err(|source| socket_error(&log, source))?;
    Ok((log, Stdio::from(file), Stdio::from(errors)))
}

#[cfg(unix)]
pub(crate) fn detach_service_process(command: &mut tokio::process::Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: `setsid` is a single async-signal-safe system call. The closure
    // neither allocates nor accesses shared process state after `fork`.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(|error| std::io::Error::from_raw_os_error(error as i32))
        });
    }
}

#[cfg(windows)]
pub(crate) fn detach_service_process(command: &mut tokio::process::Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    command
        .as_std_mut()
        .creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn detach_service_process(_command: &mut tokio::process::Command) {}

fn service_arguments(config: &Config) -> Vec<OsString> {
    let mut arguments = vec![
        OsString::from("--scrollback"),
        OsString::from(config.scrollback.to_string()),
        OsString::from("--compaction-threshold-percent"),
        OsString::from(config.compaction_threshold_percent.to_string()),
        OsString::from("--openai-reasoning-effort"),
        OsString::from(config.openai_reasoning_effort.as_str()),
        OsString::from("--agents"),
        config.agents.as_os_str().to_owned(),
    ];
    if let Some(personalities) = &config.personalities {
        arguments.push(OsString::from("--personalities"));
        arguments.push(personalities.as_os_str().to_owned());
    }
    if let Some(soul) = &config.soul {
        arguments.push(OsString::from("--soul"));
        arguments.push(soul.as_os_str().to_owned());
    }
    arguments.push(OsString::from("run"));
    arguments
}

fn service_configuration_fingerprint(config: &Config) -> String {
    let components = [
        env!("CARGO_PKG_VERSION").to_owned(),
        config.scrollback.to_string(),
        config.compaction_threshold_percent.to_string(),
        config.openai_reasoning_effort.as_str().to_owned(),
        config.agents.to_string_lossy().into_owned(),
        config
            .personalities
            .as_ref()
            .map_or_else(String::new, |path| path.to_string_lossy().into_owned()),
        config
            .soul
            .as_ref()
            .map_or_else(String::new, |path| path.to_string_lossy().into_owned()),
    ];
    let mut digest = Sha256::new();
    for component in components {
        digest.update(component.len().to_le_bytes());
        digest.update(component.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

pub(crate) struct ActivationLease {
    path: PathBuf,
    owner: String,
}

impl ActivationLease {
    pub(crate) async fn acquire(path: &Path) -> Result<Self, ControlError> {
        use std::io::Write;

        for _ in 0..ACTIVATION_LOCK_ATTEMPTS {
            let owner = format!(
                "{}:{}\n",
                std::process::id(),
                crate::diagnostics::unix_time_ms()
            );
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
                        .map_err(|source| socket_error(path, source))?;
                    return Ok(Self {
                        path: path.to_path_buf(),
                        owner,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if activation_lock_is_abandoned(path) {
                        let _ = std::fs::remove_file(path);
                        continue;
                    }
                    tokio::time::sleep(SERVICE_START_RETRY).await;
                }
                Err(source) => return Err(socket_error(path, source)),
            }
        }
        let timeout_ms = u128::try_from(ACTIVATION_LOCK_ATTEMPTS)
            .unwrap_or(u128::MAX)
            .saturating_mul(SERVICE_START_RETRY.as_millis());
        Err(ControlError::StaleServiceActivation(format!(
            "another connector kept the workspace activation lease at {} for more than {timeout_ms}ms",
            path.display(),
        )))
    }
}

impl Drop for ActivationLease {
    fn drop(&mut self) {
        if std::fs::read_to_string(&self.path).is_ok_and(|owner| owner == self.owner) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn activation_lock_is_abandoned(path: &Path) -> bool {
    let stale = std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= ACTIVATION_LOCK_STALE_AFTER);
    if !stale {
        return false;
    }
    let owner = std::fs::read_to_string(path).ok();
    activation_lock_owner_is_abandoned(owner.as_deref())
}

fn activation_lock_owner_is_abandoned(owner: Option<&str>) -> bool {
    let owner_pid = owner
        .and_then(|owner| owner.split(':').next()?.trim().parse::<u32>().ok())
        .filter(|pid| *pid > 0);
    owner_pid.is_none_or(|pid| !activation_owner_is_alive(pid))
}

#[cfg(unix)]
pub(crate) fn activation_owner_is_alive(pid: u32) -> bool {
    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};

    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    match kill(Pid::from_raw(pid), None) {
        Ok(()) | Err(Errno::EPERM) => true,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
pub(crate) fn activation_owner_is_alive(_pid: u32) -> bool {
    false
}

async fn ensure_api_service(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
) -> Result<PathBuf, ControlError> {
    let api_path = paths.api().to_path_buf();
    ensure_service(paths, executable, config).await?;
    for _ in 0..SERVICE_START_ATTEMPTS {
        if api_ready(&api_path, &config.workspace).await {
            return Ok(api_path);
        }
        tokio::time::sleep(SERVICE_START_RETRY).await;
    }
    Err(ControlError::ServiceStartup(api_path.display().to_string()))
}

const FRONTEND_API_VERSION: &str = "nakode.v1";

fn runtime_matches_cli(record: Option<&ServiceRuntimeRecord>, cli: &ExecutableIdentity) -> bool {
    record
        .and_then(|record| record.executable.as_ref())
        .is_some_and(|running| running.same_build(cli))
}

fn server_is_api_compatible(server: Option<&ServerReport>) -> bool {
    // Additive features are negotiated through capabilities. A wire-incompatible service must
    // publish a different API version rather than forcing a restart solely for build identity.
    server.is_some_and(|server| server.api_version == FRONTEND_API_VERSION)
}

fn service_can_be_reused(
    record: Option<&ServiceRuntimeRecord>,
    cli: &ExecutableIdentity,
    server: Option<&ServerReport>,
) -> bool {
    runtime_matches_cli(record, cli) || server_is_api_compatible(server)
}

/// Returns a compatible live endpoint or starts/activates the workspace server.
///
/// Endpoint discovery is serialized per workspace. A responsive API-compatible service is reused
/// even when its executable differs from the invoking CLI, preserving server-owned live work
/// across installs. A live incompatible service is replaced at most once, and the replacement is
/// verified before its descriptor is returned.
///
/// # Errors
/// Returns when compatibility cannot be established, live work makes replacement unsafe, or a
/// bounded start/restart cannot produce a ready compatible service.
pub async fn frontend_api_endpoint_report(
    executable: &Path,
    config: &Config,
) -> Result<FrontendEndpoint, ControlError> {
    let config = installation_config(config)?;
    let config = &config;
    let paths = ServicePaths::of(config)?;
    let cli = executable_identity(executable)?;
    let activation_lease = match ActivationLease::acquire(paths.activation()).await {
        Ok(lease) => lease,
        Err(error) => {
            let record = read_runtime_record(paths.runtime());
            let server = server_report(paths.api()).await;
            return Err(ControlError::StaleServiceActivation(activation_diagnostic(
                &paths,
                &cli,
                record.as_ref(),
                server,
                "activation lease unavailable",
                &error.to_string(),
            )));
        }
    };

    if discover_running_api_at(paths.lifecycle(), paths.api(), &config.workspace)
        .await?
        .is_some()
    {
        let old_record = read_runtime_record(paths.runtime());
        let old_server = server_report(paths.api()).await;
        if service_can_be_reused(old_record.as_ref(), &cli, old_server.as_ref()) {
            drop(activation_lease);
            return verified_frontend_endpoint(
                &paths,
                &config.workspace,
                cli,
                EndpointActivation::Reused,
                "reusing compatible service",
            )
            .await;
        }

        return activate_stale_service(
            &paths,
            executable,
            config,
            cli,
            old_record,
            activation_lease,
        )
        .await;
    }

    ensure_api_service(&paths, executable, config).await?;
    drop(activation_lease);
    verified_frontend_endpoint(
        &paths,
        &config.workspace,
        cli,
        EndpointActivation::Started,
        "service was started",
    )
    .await
}

async fn activate_stale_service(
    paths: &ServicePaths,
    executable: &Path,
    config: &Config,
    cli: ExecutableIdentity,
    old_record: Option<ServiceRuntimeRecord>,
    activation_lease: ActivationLease,
) -> Result<FrontendEndpoint, ControlError> {
    let old_server = server_report(paths.api()).await;
    ensure_stale_service_is_quiescent(paths.api(), &config.workspace)
        .await
        .map_err(|reason| {
            ControlError::StaleServiceActivation(activation_diagnostic(
                paths,
                &cli,
                old_record.as_ref(),
                old_server.clone(),
                "restart refused",
                &reason,
            ))
        })?;
    eprintln!(
        "nakode: activating updated service for {} (old {}; cli {})",
        config.workspace.display(),
        runtime_identity_label(old_record.as_ref()),
        executable_identity_label(&cli),
    );
    if let Err(error) = restart_service_quiescent(executable, config).await {
        return Err(stale_replacement_error(
            paths,
            &cli,
            old_record.as_ref(),
            old_server.clone(),
            "restart failed",
            &error,
        )
        .await);
    }
    if let Err(error) = wait_for_api(paths.api(), &config.workspace).await {
        return Err(stale_replacement_error(
            paths,
            &cli,
            old_record.as_ref(),
            old_server,
            "replacement did not become ready",
            &error,
        )
        .await);
    }
    drop(activation_lease);
    verified_frontend_endpoint(
        paths,
        &config.workspace,
        cli,
        EndpointActivation::RestartedStaleService,
        "stale service was restarted",
    )
    .await
}

async fn stale_replacement_error(
    paths: &ServicePaths,
    cli: &ExecutableIdentity,
    old_record: Option<&ServiceRuntimeRecord>,
    old_server: Option<ServerReport>,
    action: &str,
    error: &ControlError,
) -> ControlError {
    let replacement = read_runtime_record(paths.runtime());
    let replacement_server = server_report_label(server_report(paths.api()).await);
    let reason = format!(
        "{error}; replacement {}; replacement_server {replacement_server}",
        runtime_identity_label(replacement.as_ref()),
    );
    ControlError::StaleServiceActivation(activation_diagnostic(
        paths, cli, old_record, old_server, action, &reason,
    ))
}

fn server_report_label(server: Option<ServerReport>) -> String {
    server.map_or_else(
        || "unavailable".to_owned(),
        |server| {
            format!(
                "version={} api={} capabilities=[{}]",
                server.server_version,
                server.api_version,
                server.capabilities.join(",")
            )
        },
    )
}

/// Returns only the verified socket for in-process clients.
///
/// # Errors
/// Returns when endpoint activation or identity verification fails.
pub async fn frontend_api_endpoint(
    executable: &Path,
    config: &Config,
) -> Result<PathBuf, ControlError> {
    Ok(frontend_api_endpoint_report(executable, config)
        .await?
        .endpoint)
}

pub(crate) async fn wait_for_api(api_path: &Path, workspace: &Path) -> Result<(), ControlError> {
    for _ in 0..SERVICE_START_ATTEMPTS {
        if api_ready(api_path, workspace).await {
            return Ok(());
        }
        tokio::time::sleep(SERVICE_START_RETRY).await;
    }
    Err(ControlError::ServiceStartup(api_path.display().to_string()))
}

async fn verified_frontend_endpoint(
    paths: &ServicePaths,
    workspace: &Path,
    cli: ExecutableIdentity,
    activation: EndpointActivation,
    action: &str,
) -> Result<FrontendEndpoint, ControlError> {
    let service = read_runtime_record(paths.runtime()).ok_or_else(|| {
        ControlError::StaleServiceActivation(activation_diagnostic(
            paths,
            &cli,
            None,
            None,
            action,
            "the ready service did not publish service.json",
        ))
    })?;
    let server = server_report(paths.api()).await;
    let matches = service_can_be_reused(Some(&service), &cli, server.as_ref());
    if !matches {
        return Err(ControlError::StaleServiceActivation(activation_diagnostic(
            paths,
            &cli,
            Some(&service),
            server,
            action,
            "the ready service is neither the invoking CLI build nor API-compatible",
        )));
    }
    let installed_is_running = service
        .executable
        .as_ref()
        .is_some_and(|running| running.same_build(&cli));
    let activation_endpoint = if installed_is_running {
        crate::activation::observe_current_service(paths, &cli, &service, server.as_ref())
            .await
            .map_err(|error| ControlError::ServiceRejected(error.to_string()))?;
        paths.api().to_path_buf()
    } else {
        crate::activation::schedule_deferred_activation(
            paths,
            &cli.path,
            "installed binary is waiting for the API-compatible running service to become quiescent",
        )
        .await
        .map_err(|error| ControlError::ServiceRejected(error.to_string()))?;
        paths.activation_api().to_path_buf()
    };
    Ok(FrontendEndpoint {
        workspace: workspace.to_path_buf(),
        endpoint: paths.api().to_path_buf(),
        activation_endpoint,
        lifecycle_socket: paths.lifecycle().to_path_buf(),
        cli,
        service,
        server,
        activation,
    })
}

async fn ensure_stale_service_is_quiescent(
    api_path: &Path,
    workspace: &Path,
) -> Result<(), String> {
    let query = async {
        let client = nakode_sdk::NakodeClient::connect_unix(api_path.to_path_buf())
            .await
            .map_err(|error| error.to_string())?;
        let workspace_state = client
            .get_workspace(workspace.to_string_lossy(), None)
            .await
            .map_err(|error| error.to_string())?;
        let mut running = Vec::new();
        for summary in workspace_state.sessions {
            let session_id = summary.id;
            if summary.running {
                running.push(session_id.clone());
            }
            match client.get_session(session_id.clone()).await {
                Ok(session) if session_has_live_work(&session) => {
                    if !running.contains(&session_id) {
                        running.push(session_id);
                    }
                }
                Ok(_) => {}
                Err(nakode_sdk::SdkError::Status(status))
                    if status.code() == tonic::Code::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        quiescence_refusal(&running)
    };
    tokio::time::timeout(Duration::from_secs(3), query)
        .await
        .map_err(|_| "timed out while proving that the stale service has no live work".to_owned())?
}

pub(crate) fn session_has_live_work(session: &nakode_sdk::v1::SessionState) -> bool {
    session.activity != nakode_sdk::v1::SessionActivity::Idle as i32 || !session.queue.is_empty()
}

fn quiescence_refusal(running: &[String]) -> Result<(), String> {
    if running.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "live work is still owned by session(s) {}; stop or finish that work before activating the updated service",
            running.join(", ")
        ))
    }
}

fn executable_identity_label(identity: &ExecutableIdentity) -> String {
    format!(
        "path={} sha256={} size={} device={} inode={}",
        identity.path.display(),
        identity.sha256,
        identity.size,
        identity.device.as_deref().unwrap_or("unknown"),
        identity.inode.as_deref().unwrap_or("unknown"),
    )
}

fn runtime_identity_label(record: Option<&ServiceRuntimeRecord>) -> String {
    record.map_or_else(
        || "pid=unknown version=unknown executable=unpublished".to_owned(),
        |record| {
            format!(
                "pid={} version={} executable={}",
                record.pid,
                record.version,
                record
                    .executable
                    .as_ref()
                    .map_or_else(|| "unpublished".to_owned(), executable_identity_label)
            )
        },
    )
}

fn activation_diagnostic(
    paths: &ServicePaths,
    cli: &ExecutableIdentity,
    service: Option<&ServiceRuntimeRecord>,
    server: Option<ServerReport>,
    action: &str,
    reason: &str,
) -> String {
    let server = server.map_or_else(
        || "server=unavailable".to_owned(),
        |server| {
            format!(
                "server_version={} api_version={} capabilities=[{}]",
                server.server_version,
                server.api_version,
                server.capabilities.join(",")
            )
        },
    );
    format!(
        "{action}: {reason}; cli={}; service={}; api_endpoint={}; lifecycle_endpoint={}; {server}",
        executable_identity_label(cli),
        runtime_identity_label(service),
        paths.api().display(),
        paths.lifecycle().display(),
    )
}

#[cfg(test)]
async fn frontend_api_endpoint_at<Start, StartFuture>(
    lifecycle_path: &Path,
    api_path: &Path,
    workspace: &Path,
    start_missing: Start,
) -> Result<PathBuf, ControlError>
where
    Start: FnOnce() -> StartFuture,
    StartFuture: std::future::Future<Output = Result<PathBuf, ControlError>>,
{
    if let Some(endpoint) = discover_running_api_at(lifecycle_path, api_path, workspace).await? {
        return Ok(endpoint);
    }
    start_missing().await
}

async fn discover_running_api_at(
    lifecycle_path: &Path,
    api_path: &Path,
    workspace: &Path,
) -> Result<Option<PathBuf>, ControlError> {
    if api_ready(api_path, workspace).await {
        return Ok(Some(api_path.to_owned()));
    }

    match running_configuration_at(lifecycle_path).await {
        Ok(_) => {}
        Err(ControlError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    }

    // A lifecycle-ready server may still be bringing up its API listener.
    // Wait for that live server instead of treating connector defaults as
    // authority and attempting a conflicting start.
    for _ in 0..SERVICE_START_ATTEMPTS {
        tokio::time::sleep(SERVICE_START_RETRY).await;
        if api_ready(api_path, workspace).await {
            return Ok(Some(api_path.to_owned()));
        }
    }
    Err(ControlError::ServiceStartup(api_path.display().to_string()))
}

async fn api_ready(path: &Path, workspace: &Path) -> bool {
    let readiness = async {
        let client = nakode_sdk::NakodeClient::connect_unix(path.to_owned())
            .await
            .map_err(|error| ControlError::ServiceRejected(error.to_string()))?;
        let state = client
            .get_workspace(workspace.to_string_lossy(), None)
            .await
            .map_err(|error| ControlError::ServiceRejected(error.to_string()))?;
        if state.workspace_path == workspace.to_string_lossy() {
            Ok(())
        } else {
            Err(ControlError::ServiceRejected(
                "workspace response did not match the requested workspace".to_owned(),
            ))
        }
    };
    tokio::time::timeout(Duration::from_secs(1), readiness)
        .await
        .is_ok_and(|result| result.is_ok())
}

/// Reports whether the workspace server currently responds to lifecycle requests.
///
/// A missing or stale lifecycle socket is reported as a stopped service. Other
/// socket and protocol failures remain errors so an unhealthy service is not
/// mistaken for one that can be safely replaced.
///
/// # Errors
/// Returns an error when lifecycle state cannot be read reliably.
pub async fn service_status(config: &Config) -> Result<ServiceStatus, ControlError> {
    let config = installation_config(config)?;
    let paths = ServicePaths::of(&config)?;
    let current = std::env::current_exe().map_err(|source| ControlError::ExecutableIdentity {
        path: "current executable".to_owned(),
        source,
    })?;
    let nakode_executable = executable_identity(&current)?;
    let running = service_running_at(paths.lifecycle()).await?;
    let record = if running {
        read_runtime_record(paths.runtime())
    } else {
        None
    };
    let now = crate::diagnostics::unix_time_ms();
    let server = if running {
        server_report(paths.api()).await
    } else {
        None
    };
    Ok(ServiceStatus {
        running,
        workspace: config.workspace.clone(),
        nakode_version: env!("CARGO_PKG_VERSION").to_owned(),
        nakode_executable,
        service_executable: record.as_ref().and_then(|record| record.executable.clone()),
        pid: record.as_ref().map(|record| record.pid),
        started_at_unix_ms: record.as_ref().map(|record| record.started_at_unix_ms),
        started_at_utc: record
            .as_ref()
            .map(|record| format_utc_timestamp(record.started_at_unix_ms)),
        uptime_seconds: record
            .as_ref()
            .map(|record| now.saturating_sub(record.started_at_unix_ms) / 1_000),
        endpoint: paths.api().to_path_buf(),
        lifecycle_socket: paths.lifecycle().to_path_buf(),
        log: paths.log().to_path_buf(),
        server,
    })
}

/// Reads API identity from a running service without starting one.
///
/// Status reporting must never bring a service up, so an unreachable or
/// still-starting API is reported as absent rather than retried.
pub(crate) async fn server_report(api_path: &Path) -> Option<ServerReport> {
    let query = async {
        let client = nakode_sdk::NakodeClient::connect_unix(api_path.to_owned())
            .await
            .ok()?;
        let info = client.get_server_info().await.ok()?;
        Some(ServerReport {
            server_version: info.server_version,
            api_version: info.api_version,
            capabilities: info.capabilities,
        })
    };
    tokio::time::timeout(Duration::from_secs(2), query)
        .await
        .ok()
        .flatten()
}

fn format_utc_timestamp(unix_ms: u64) -> String {
    let day = crate::diagnostics::format_utc_day(crate::diagnostics::day_number(unix_ms));
    let seconds_of_day = (unix_ms / 1_000) % 86_400;
    format!(
        "{day}T{:02}:{:02}:{:02}Z",
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60
    )
}

/// Starts the installation service in the background when it is not already
/// running, and waits until it accepts connections.
///
/// # Errors
/// Returns an error when the service cannot be started, or when a service is
/// already running with conflicting server-owned configuration.
pub async fn start_service(
    executable: &Path,
    config: &Config,
) -> Result<StartOutcome, ControlError> {
    let config = installation_config(config)?;
    let paths = ServicePaths::of(&config)?;
    if ping_at(paths.lifecycle(), &config).await.is_ok() {
        return Ok(StartOutcome::AlreadyRunning);
    }
    ensure_api_service(&paths, executable, &config).await?;
    Ok(StartOutcome::Started)
}

/// Stops the installation service and waits until it releases its sockets.
///
/// # Errors
/// Returns an error when a live service rejects the request or keeps its
/// sockets after acknowledging shutdown.
pub async fn stop_service(paths: &ServicePaths) -> Result<StopOutcome, ControlError> {
    if !service_running_at(paths.lifecycle()).await? {
        // An unclean exit can leave socket files behind without a service.
        let _ = std::fs::remove_file(paths.lifecycle());
        let _ = std::fs::remove_file(paths.api());
        let _ = std::fs::remove_file(paths.runtime());
        return Ok(StopOutcome::AlreadyStopped);
    }
    shutdown_service(paths).await?;
    for _ in 0..SERVICE_STOP_ATTEMPTS {
        if !paths.lifecycle().exists() && !paths.api().exists() {
            return Ok(StopOutcome::Stopped);
        }
        tokio::time::sleep(SERVICE_START_RETRY).await;
    }
    Err(ControlError::ServiceShutdown(
        paths.lifecycle().display().to_string(),
    ))
}

async fn service_running_at(service_path: &Path) -> Result<bool, ControlError> {
    match running_configuration_at(service_path).await {
        Ok(_) => Ok(true),
        Err(ControlError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// Restarts the workspace server as a detached background process, or starts it
/// when it is currently stopped.
///
/// # Errors
/// Returns an error when the current service cannot be stopped or its
/// replacement cannot be started.
pub async fn restart_service(executable: &Path, config: &Config) -> Result<(), ControlError> {
    restart_service_with_request(executable, config, LifecycleRequest::Shutdown).await
}

pub(crate) async fn restart_service_quiescent(
    executable: &Path,
    config: &Config,
) -> Result<(), ControlError> {
    restart_service_with_request(executable, config, LifecycleRequest::QuiesceShutdown).await
}

pub(crate) async fn restart_service_conditionally(
    executable: &Path,
    config: &Config,
    expected: Vec<crate::server::runtime::QuiescenceBlocker>,
) -> Result<(), ControlError> {
    restart_service_with_request(
        executable,
        config,
        LifecycleRequest::ForceShutdown { expected },
    )
    .await
}

async fn restart_service_with_request(
    executable: &Path,
    config: &Config,
    request: LifecycleRequest,
) -> Result<(), ControlError> {
    let config = installation_config(config)?;
    let paths = ServicePaths::of(&config)?;
    let running = service_running_at(paths.lifecycle()).await?;
    shutdown_service_with_request(&paths, &request).await?;

    if running {
        for _ in 0..SERVICE_STOP_ATTEMPTS {
            if !paths.lifecycle().exists() && !paths.api().exists() {
                return ensure_service(&paths, executable, &config).await;
            }
            tokio::time::sleep(SERVICE_START_RETRY).await;
        }
        return Err(ControlError::ServiceShutdown(
            paths.lifecycle().display().to_string(),
        ));
    }

    // A stopped process can leave socket files behind after an unclean exit.
    let _ = std::fs::remove_file(paths.lifecycle());
    let _ = std::fs::remove_file(paths.api());
    ensure_service(&paths, executable, &config).await
}

/// Stops the workspace server if one is currently running.
///
/// # Errors
/// Returns an error when a live server rejects or cannot read the request.
pub async fn shutdown_service(paths: &ServicePaths) -> Result<(), ControlError> {
    shutdown_service_with_request(paths, &LifecycleRequest::Shutdown).await
}

async fn shutdown_service_with_request(
    paths: &ServicePaths,
    request: &LifecycleRequest,
) -> Result<(), ControlError> {
    if !paths.lifecycle().exists() {
        return Ok(());
    }
    match expect_ok(paths.lifecycle(), request).await {
        Ok(()) => Ok(()),
        Err(ControlError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            let _ = std::fs::remove_file(paths.lifecycle());
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Stops every discoverable workspace service before global session storage is purged.
///
/// Service sockets are the authoritative registry for server processes. A live server owns all of
/// its provider children, shell processes, delegated runs, and frontend transports; its normal
/// shutdown path terminates and joins those resources before releasing both sockets. Stale socket
/// files are removed only when no process accepts the lifecycle connection.
///
/// # Errors
/// Returns an error when the private control directory cannot be enumerated.
pub async fn shutdown_all_services() -> Result<BulkServiceShutdownReport, ControlError> {
    shutdown_all_services_in(&control_directory()?).await
}

/// Reconciles the installation service and legacy workspace runtimes after a new executable is
/// installed.
///
/// Runtime directories are scanned once. A current singleton is preserved; a stale quiescent
/// singleton is restarted. Quiescent legacy runtimes are retired instead of restarted, while legacy
/// runtimes with live work are preserved and reported. Dead legacy socket sets are removed. Partly
/// reachable, unidentified, or otherwise unsafe runtimes are left untouched and reported.
///
/// # Errors
///
/// Returns an error when the executable identity or private control directory cannot be read.
pub async fn restart_stale_services(
    executable: &Path,
) -> Result<StaleServiceRefreshReport, ControlError> {
    let cli = executable_identity(executable)?;
    let control_root = control_directory()?;
    let installation_workspace = installation_workspace()?;
    let singleton_directory =
        workspace_runtime_directory_in(&control_root, &installation_workspace)?;
    let directories = runtime_directories(&control_root)?;
    let mut report = StaleServiceRefreshReport::default();
    let mut candidates = Vec::new();

    for directory in directories {
        let paths = ServicePaths::in_directory(&directory);
        if !paths.lifecycle().exists() && !paths.api().exists() {
            continue;
        }
        let record = read_runtime_record(paths.runtime());
        let is_singleton = directory == singleton_directory;
        if is_singleton && runtime_matches_cli(record.as_ref(), &cli) {
            report.current += 1;
            continue;
        }
        let workspace = if is_singleton {
            Some(installation_workspace.clone())
        } else {
            record
                .as_ref()
                .and_then(|record| record.workspace.clone())
                .or_else(|| workspace_from_log(paths.log()))
        };
        candidates.push((directory, workspace, is_singleton));
    }

    let results = stream::iter(
        candidates
            .into_iter()
            .map(|(directory, workspace, is_singleton)| {
                let executable = executable.to_path_buf();
                async move {
                    refresh_stale_service(&executable, &directory, workspace, is_singleton).await
                }
            }),
    )
    .buffer_unordered(STALE_SERVICE_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    for result in results {
        match result {
            StaleServiceRefreshOutcome::Restarted => report.restarted += 1,
            StaleServiceRefreshOutcome::Retired => report.retired += 1,
            StaleServiceRefreshOutcome::Active(workspace) => report.active.push(workspace),
            StaleServiceRefreshOutcome::Inactive(detail) => report.inactive.push(detail),
            StaleServiceRefreshOutcome::Unavailable(detail) => report.unavailable.push(detail),
            StaleServiceRefreshOutcome::Unknown(detail) => report.unknown.push(detail),
            StaleServiceRefreshOutcome::Failure(detail) => report.failures.push(detail),
        }
    }
    report.active.sort_unstable();
    report.inactive.sort_unstable();
    report.unavailable.sort_unstable();
    report.unknown.sort_unstable();
    report.failures.sort_unstable();
    Ok(report)
}

#[derive(Debug)]
enum StaleServiceRefreshOutcome {
    Restarted,
    Retired,
    Active(String),
    Inactive(String),
    Unavailable(String),
    Unknown(String),
    Failure(String),
}

async fn classify_stale_socket_state(
    executable: &Path,
    paths: &ServicePaths,
    workspace: Option<&Path>,
    is_singleton: bool,
) -> Option<StaleServiceRefreshOutcome> {
    let lifecycle_reachable = socket_is_live(paths.lifecycle()).await;
    let api_reachable = socket_is_live(paths.api()).await;
    match (lifecycle_reachable, api_reachable) {
        (false, false) if !is_singleton => {
            let _ = std::fs::remove_file(paths.lifecycle());
            let _ = std::fs::remove_file(paths.api());
            let _ = std::fs::remove_file(paths.runtime());
            Some(StaleServiceRefreshOutcome::Retired)
        }
        (false, false) => Some(StaleServiceRefreshOutcome::Inactive(
            stale_service_diagnostic(paths, workspace),
        )),
        (true, true) => None,
        _ => {
            if is_singleton {
                let reason = format!(
                    "lifecycle_reachable={lifecycle_reachable} api_reachable={api_reachable}"
                );
                if let Err(error) =
                    crate::activation::schedule_deferred_activation(paths, executable, reason).await
                {
                    return Some(StaleServiceRefreshOutcome::Failure(format!(
                        "{}; could not schedule deferred activation: {error}",
                        stale_service_diagnostic(paths, workspace)
                    )));
                }
            }
            Some(StaleServiceRefreshOutcome::Unavailable(format!(
                "{} lifecycle_reachable={lifecycle_reachable} api_reachable={api_reachable}",
                stale_service_diagnostic(paths, workspace)
            )))
        }
    }
}

async fn refresh_stale_service(
    executable: &Path,
    directory: &Path,
    workspace: Option<PathBuf>,
    is_singleton: bool,
) -> StaleServiceRefreshOutcome {
    let paths = ServicePaths::in_directory(directory);
    if let Some(outcome) =
        classify_stale_socket_state(executable, &paths, workspace.as_deref(), is_singleton).await
    {
        return outcome;
    }
    let Some(workspace) = workspace else {
        return StaleServiceRefreshOutcome::Unknown(stale_service_diagnostic(&paths, None));
    };
    let config = match crate::config::Config::for_workspace(workspace.clone()) {
        Ok(config) => config,
        Err(error) => {
            return StaleServiceRefreshOutcome::Failure(format!(
                "{}: {error}",
                workspace.display()
            ));
        }
    };
    if let Err(reason) = ensure_stale_service_is_quiescent(paths.api(), &config.workspace).await {
        if reason.starts_with("live work is still owned") {
            if is_singleton
                && let Err(error) = crate::activation::schedule_deferred_activation(
                    &paths,
                    executable,
                    reason.clone(),
                )
                .await
            {
                return StaleServiceRefreshOutcome::Failure(format!(
                    "{}: installed successfully, but could not schedule deferred activation: {error}",
                    workspace.display()
                ));
            }
            return StaleServiceRefreshOutcome::Active(workspace.display().to_string());
        }
        if is_singleton {
            if let Err(error) =
                crate::activation::schedule_deferred_activation(&paths, executable, reason.clone())
                    .await
            {
                return StaleServiceRefreshOutcome::Failure(format!(
                    "{}: {reason}; could not schedule deferred activation: {error}",
                    workspace.display()
                ));
            }
            return StaleServiceRefreshOutcome::Unavailable(format!(
                "{}: {reason}",
                workspace.display()
            ));
        }
        return StaleServiceRefreshOutcome::Failure(format!("{}: {reason}", workspace.display()));
    }

    if !is_singleton {
        let shutdown = match expect_ok(paths.lifecycle(), &LifecycleRequest::QuiesceShutdown).await
        {
            Ok(()) => Ok(()),
            Err(error) if is_legacy_quiescent_rejection(&error) => {
                expect_ok(paths.lifecycle(), &LifecycleRequest::Shutdown).await
            }
            Err(error) => Err(error),
        };
        return match shutdown {
            Ok(()) => {
                for _ in 0..SERVICE_STOP_ATTEMPTS {
                    if !paths.lifecycle().exists() && !paths.api().exists() {
                        return StaleServiceRefreshOutcome::Retired;
                    }
                    tokio::time::sleep(SERVICE_START_RETRY).await;
                }
                StaleServiceRefreshOutcome::Failure(format!(
                    "{}: legacy service acknowledged shutdown but retained sockets",
                    workspace.display()
                ))
            }
            Err(error) => StaleServiceRefreshOutcome::Failure(format!(
                "{}: failed to retire legacy service: {error}",
                workspace.display()
            )),
        };
    }

    let restart = match restart_service_quiescent(executable, &config).await {
        Ok(()) => Ok(()),
        Err(error) if is_legacy_quiescent_rejection(&error) => {
            restart_service(executable, &config).await
        }
        Err(error) => Err(error),
    };
    match restart {
        Ok(()) => StaleServiceRefreshOutcome::Restarted,
        Err(error) => {
            StaleServiceRefreshOutcome::Failure(format!("{}: {error}", workspace.display()))
        }
    }
}

fn stale_service_diagnostic(paths: &ServicePaths, workspace: Option<&Path>) -> String {
    format!(
        "workspace={} runtime_record={} lifecycle={} api={}",
        workspace.map_or_else(|| "unknown".to_owned(), |path| path.display().to_string()),
        paths.runtime().display(),
        paths.lifecycle().display(),
        paths.api().display(),
    )
}

fn is_legacy_quiescent_rejection(error: &ControlError) -> bool {
    matches!(
        error,
        ControlError::ServiceRejected(message)
            if message.contains("unknown variant") && message.contains("quiesce_shutdown")
    )
}

fn workspace_from_log(log: &Path) -> Option<PathBuf> {
    use std::io::{Read, Seek, SeekFrom};

    const LOG_TAIL_BYTES: u64 = 64 * 1024;
    let mut file = std::fs::File::open(log).ok()?;
    let length = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(LOG_TAIL_BYTES)))
        .ok()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    String::from_utf8_lossy(&bytes)
        .lines()
        .rev()
        .find_map(|line| {
            line.strip_prefix("nakode service started for workspace ")
                .map(PathBuf::from)
        })
}

fn runtime_directories(control_root: &Path) -> Result<Vec<PathBuf>, ControlError> {
    let workspace_root = control_root.join("w");
    let entries = match std::fs::read_dir(&workspace_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(socket_error(&workspace_root, source)),
    };
    let mut directories = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| socket_error(&workspace_root, source))?;
        let entry_path = entry.path();
        if entry
            .file_type()
            .map_err(|source| socket_error(&entry_path, source))?
            .is_dir()
        {
            directories.push(entry_path);
        }
    }
    directories.sort_unstable();
    Ok(directories)
}

async fn shutdown_all_services_in(
    control_root: &Path,
) -> Result<BulkServiceShutdownReport, ControlError> {
    let directories = runtime_directories(control_root)?;

    let mut report = BulkServiceShutdownReport::default();
    for directory in directories {
        let lifecycle_path = directory.join("c.sock");
        let api_path = directory.join("api.sock");
        if !lifecycle_path.exists() && !api_path.exists() {
            continue;
        }
        match expect_ok(&lifecycle_path, &LifecycleRequest::Shutdown).await {
            Ok(()) => {
                let mut released = false;
                for _ in 0..SERVICE_STOP_ATTEMPTS {
                    if !lifecycle_path.exists() && !api_path.exists() {
                        released = true;
                        break;
                    }
                    tokio::time::sleep(SERVICE_START_RETRY).await;
                }
                if released {
                    report.stopped += 1;
                } else {
                    report.failures.push(format!(
                        "service at {} acknowledged shutdown but retained runtime sockets",
                        directory.display()
                    ));
                }
            }
            Err(ControlError::Io { source, .. })
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                if api_path.exists() && socket_is_live(&api_path).await {
                    report.failures.push(format!(
                        "runtime API at {} is still active without a lifecycle service",
                        directory.display()
                    ));
                    continue;
                }
                let lifecycle_removed = remove_stale_socket(&lifecycle_path);
                let api_removed = remove_stale_socket(&api_path);
                match (lifecycle_removed, api_removed) {
                    (Ok(_), Ok(_)) => report.stale += 1,
                    (lifecycle, api) => report.failures.push(format!(
                        "could not clear stale runtime sockets at {}: {}{}",
                        directory.display(),
                        lifecycle
                            .err()
                            .map_or_else(String::new, |error| error.to_string()),
                        api.err()
                            .map_or_else(String::new, |error| format!("; {error}"))
                    )),
                }
            }
            Err(error) => report.failures.push(format!(
                "could not stop service at {}: {error}",
                directory.display()
            )),
        }
    }
    Ok(report)
}

fn remove_stale_socket(path: &Path) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Controls one frontend transport in the running installation service without
/// changing the native server lifecycle.
///
/// # Errors
/// Returns an error when the service is not reachable or rejects the request.
pub async fn transport_action(
    paths: &ServicePaths,
    name: &str,
    action: TransportAction,
) -> Result<TransportStatus, ControlError> {
    match exchange(
        paths.lifecycle(),
        &LifecycleRequest::Transport {
            name: name.to_owned(),
            action,
        },
    )
    .await?
    {
        LifecycleResponse::Transport { status } => Ok(status),
        LifecycleResponse::Error { message } => Err(ControlError::ServiceRejected(message)),
        LifecycleResponse::Ok | LifecycleResponse::Ready { .. } => Err(
            ControlError::ServiceRejected("unexpected lifecycle transport response".to_owned()),
        ),
    }
}

/// Returns the private directory holding the installation service and discoverable legacy runtimes.
fn control_directory() -> Result<PathBuf, ControlError> {
    let directory = if let Some(configured) = std::env::var_os("NAKODE_CONTROL_DIR") {
        PathBuf::from(configured)
    } else {
        ProjectDirs::from("dev", "nakode", "Nakode")
            .map(|project| project.data_local_dir().to_path_buf())
            .ok_or(ControlError::MissingDataDirectory)?
    };
    prepare_private_directory(&directory)?;
    Ok(directory)
}

fn prepare_private_directory(directory: &Path) -> Result<(), ControlError> {
    std::fs::create_dir_all(directory).map_err(|source| socket_error(directory, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
            .map_err(|source| socket_error(directory, source))?;
    }
    Ok(())
}

fn installation_workspace() -> Result<PathBuf, ControlError> {
    let workspace = crate::config::nakode_home().map_err(|error| {
        ControlError::ServiceRejected(format!(
            "failed to resolve the Nakode installation home: {error}"
        ))
    })?;
    prepare_private_directory(&workspace)?;
    std::fs::canonicalize(&workspace).map_err(|source| socket_error(&workspace, source))
}

pub(crate) fn installation_config(config: &Config) -> Result<Config, ControlError> {
    let mut config = config.clone();
    config.workspace = installation_workspace()?;
    Ok(config)
}

fn workspace_runtime_directory_in(
    control_root: &Path,
    workspace: &Path,
) -> Result<PathBuf, ControlError> {
    let canonical =
        std::fs::canonicalize(workspace).map_err(|source| socket_error(workspace, source))?;
    let digest = Sha256::digest(canonical.to_string_lossy().as_bytes());
    let mut key = String::with_capacity(16);
    for byte in &digest[..8] {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    let directory = control_root.join("w").join(key);
    prepare_private_directory(&directory)?;
    Ok(directory)
}

fn socket_error(path: &Path, source: std::io::Error) -> ControlError {
    ControlError::Io {
        path: path.display().to_string(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::future::BoxFuture;
    use tokio::io::AsyncWriteExt;

    use super::{
        ControlError, ExecutableIdentity, LifecycleRequest, LifecycleResponse,
        RESUME_ENVIRONMENT_KEYS, ServicePaths, ServiceRuntimeRecord, StaleServiceRefreshOutcome,
        TransportAction, TransportController, TransportStatus, TransportSupervisor, UnixListener,
        activation_lock_owner_is_abandoned, bind_service_listener, detach_service_process,
        ensure_service_at, exchange, executable_identity, expect_ok, frontend_api_endpoint_at,
        ping_at, quiescence_refusal, refresh_stale_service, run_lifecycle_listener,
        runtime_matches_cli, server_is_api_compatible, service_arguments, service_can_be_reused,
        service_command, service_configuration_fingerprint, service_running_at,
        session_has_live_work, shutdown_all_services_in, workspace_from_log,
        workspace_runtime_directory_in,
    };
    use crate::config::{Config, OpenAiReasoningEffort};

    fn identity(path: &str, sha256: &str, size: u64) -> ExecutableIdentity {
        ExecutableIdentity {
            path: PathBuf::from(path),
            sha256: sha256.to_owned(),
            size,
            modified_at_unix_ms: Some(123),
            device: Some("7".to_owned()),
            inode: Some("11".to_owned()),
        }
    }

    #[test]
    fn runtime_identity_requires_published_matching_content_not_semver() {
        let cli = identity("/installed/nakode", "new-hash", 200);
        let matching = ServiceRuntimeRecord {
            pid: 42,
            started_at_unix_ms: 10,
            version: "0.3.0".to_owned(),
            workspace: None,
            executable: Some(identity("/same/display/path", "new-hash", 200)),
        };
        let same_version_old_build = ServiceRuntimeRecord {
            executable: Some(identity("/installed/nakode", "old-hash", 190)),
            ..matching.clone()
        };
        let old_record: ServiceRuntimeRecord =
            serde_json::from_str(r#"{"pid":41,"started_at_unix_ms":9,"version":"0.3.0"}"#)
                .expect("old records remain readable");

        assert!(runtime_matches_cli(Some(&matching), &cli));
        assert!(!runtime_matches_cli(Some(&same_version_old_build), &cli));
        assert!(!runtime_matches_cli(Some(&old_record), &cli));
        assert!(!runtime_matches_cli(None, &cli));
    }

    #[test]
    fn api_compatible_service_is_reused_across_executable_identity_drift() {
        let cli = identity("/installed/nakode", "new-hash", 200);
        let old_build = ServiceRuntimeRecord {
            pid: 42,
            started_at_unix_ms: 10,
            version: "0.3.0".to_owned(),
            workspace: None,
            executable: Some(identity("/installed/nakode", "old-hash", 190)),
        };
        let compatible = super::ServerReport {
            server_version: "0.3.0".to_owned(),
            api_version: "nakode.v1".to_owned(),
            capabilities: vec!["Subscriptions".to_owned()],
        };
        let incompatible = super::ServerReport {
            api_version: "nakode.v2".to_owned(),
            ..compatible.clone()
        };

        assert!(server_is_api_compatible(Some(&compatible)));
        assert!(service_can_be_reused(
            Some(&old_build),
            &cli,
            Some(&compatible)
        ));
        assert!(!service_can_be_reused(
            Some(&old_build),
            &cli,
            Some(&incompatible)
        ));
        assert!(!service_can_be_reused(Some(&old_build), &cli, None));
    }

    #[test]
    fn legacy_service_workspace_is_recovered_from_the_latest_log_start() {
        let directory = tempfile::tempdir().expect("log directory");
        let log = directory.path().join("service.log");
        std::fs::write(
            &log,
            "nakode service started for workspace /old\n".to_owned()
                + "nakode service stopped for workspace /old\n"
                + "nakode service started for workspace /new\n",
        )
        .expect("service log");

        assert_eq!(workspace_from_log(&log), Some(PathBuf::from("/new")));
    }

    #[tokio::test]
    async fn stale_refresh_reports_unidentified_inactive_sockets_without_touching_state() {
        let directory = tempfile::tempdir().expect("runtime directory");
        let paths = ServicePaths::in_directory(directory.path());
        drop(std::os::unix::net::UnixListener::bind(paths.lifecycle()).expect("lifecycle socket"));
        drop(std::os::unix::net::UnixListener::bind(paths.api()).expect("API socket"));
        std::fs::write(paths.log(), "preserved log\n").expect("service log");
        std::fs::write(paths.runtime(), "preserved record\n").expect("runtime record");

        let outcome =
            refresh_stale_service(Path::new("/unused/nakode"), directory.path(), None, true).await;

        assert!(
            matches!(outcome, StaleServiceRefreshOutcome::Inactive(detail)
            if detail.contains("workspace=unknown")
                && detail.contains("service.json")
                && detail.contains("c.sock")
                && detail.contains("api.sock"))
        );
        assert!(paths.lifecycle().exists());
        assert!(paths.api().exists());
        assert_eq!(
            std::fs::read_to_string(paths.log()).expect("preserved log"),
            "preserved log\n"
        );
        assert_eq!(
            std::fs::read_to_string(paths.runtime()).expect("preserved runtime record"),
            "preserved record\n"
        );
    }

    #[tokio::test]
    async fn stale_refresh_retires_inactive_legacy_runtime_without_restarting_it() {
        let directory = tempfile::tempdir().expect("runtime directory");
        let paths = ServicePaths::in_directory(directory.path());
        drop(std::os::unix::net::UnixListener::bind(paths.lifecycle()).expect("lifecycle socket"));
        drop(std::os::unix::net::UnixListener::bind(paths.api()).expect("API socket"));
        std::fs::write(paths.runtime(), "legacy record\n").expect("runtime record");

        let outcome = refresh_stale_service(
            Path::new("/missing/nakode"),
            directory.path(),
            Some(PathBuf::from("/legacy-workspace")),
            false,
        )
        .await;

        assert!(matches!(outcome, StaleServiceRefreshOutcome::Retired));
        assert!(!paths.lifecycle().exists());
        assert!(!paths.api().exists());
        assert!(!paths.runtime().exists());
    }

    #[tokio::test]
    async fn stale_refresh_reports_identified_inactive_sockets_without_starting_service() {
        let directory = tempfile::tempdir().expect("runtime directory");
        let paths = ServicePaths::in_directory(directory.path());
        drop(std::os::unix::net::UnixListener::bind(paths.lifecycle()).expect("lifecycle socket"));
        drop(std::os::unix::net::UnixListener::bind(paths.api()).expect("API socket"));
        let workspace = directory.path().join("removed-workspace");

        let outcome = refresh_stale_service(
            Path::new("/missing/nakode"),
            directory.path(),
            Some(workspace.clone()),
            true,
        )
        .await;

        assert!(
            matches!(outcome, StaleServiceRefreshOutcome::Inactive(detail)
            if detail.contains(&format!("workspace={}", workspace.display())))
        );
        assert!(paths.lifecycle().exists());
        assert!(paths.api().exists());
        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn stale_refresh_reports_partly_reachable_legacy_runtime_as_unavailable() {
        let directory = tempfile::tempdir().expect("runtime directory");
        let paths = ServicePaths::in_directory(directory.path());
        let lifecycle = std::os::unix::net::UnixListener::bind(paths.lifecycle())
            .expect("live lifecycle socket");
        drop(std::os::unix::net::UnixListener::bind(paths.api()).expect("dead API socket"));

        let workspace = directory.path().join("workspace");
        let outcome = refresh_stale_service(
            Path::new("/unused/nakode"),
            directory.path(),
            Some(workspace.clone()),
            false,
        )
        .await;

        assert!(
            matches!(outcome, StaleServiceRefreshOutcome::Unavailable(detail)
            if detail.contains(&format!("workspace={}", workspace.display()))
                && detail.contains("lifecycle_reachable=true")
                && detail.contains("api_reachable=false"))
        );
        assert!(paths.lifecycle().exists());
        assert!(paths.api().exists());
        drop(lifecycle);
    }

    #[test]
    fn executable_identity_is_content_sensitive_at_equal_size() {
        let directory = tempfile::tempdir().expect("identity directory");
        let executable = directory.path().join("nakode");
        std::fs::write(&executable, b"build-one").expect("first build");
        let first = executable_identity(&executable).expect("first identity");
        std::fs::write(&executable, b"build-two").expect("replacement build");
        let second = executable_identity(&executable).expect("second identity");

        assert_eq!(first.path, second.path);
        assert_eq!(first.size, second.size);
        assert_ne!(first.sha256, second.sha256);
        assert!(!first.same_build(&second));
    }

    #[test]
    fn stale_activation_refuses_identity_rich_live_work() {
        let sessions = vec!["session-a".to_owned(), "session-b".to_owned()];
        let error = quiescence_refusal(&sessions).expect_err("live work blocks replacement");
        assert!(error.contains("session-a, session-b"));
        assert!(error.contains("before activating the updated service"));
        assert!(quiescence_refusal(&[]).is_ok());
    }

    #[test]
    fn stale_activation_detects_activity_and_queued_prompts() {
        let idle = nakode_sdk::v1::SessionState {
            activity: nakode_sdk::v1::SessionActivity::Idle as i32,
            ..nakode_sdk::v1::SessionState::default()
        };
        assert!(!session_has_live_work(&idle));

        let running = nakode_sdk::v1::SessionState {
            activity: nakode_sdk::v1::SessionActivity::RunningTurn as i32,
            ..nakode_sdk::v1::SessionState::default()
        };
        assert!(session_has_live_work(&running));

        let queued = nakode_sdk::v1::SessionState {
            activity: nakode_sdk::v1::SessionActivity::Idle as i32,
            queue: vec![nakode_sdk::v1::QueueItem::default()],
            ..nakode_sdk::v1::SessionState::default()
        };
        assert!(session_has_live_work(&queued));

        assert!(session_has_live_work(
            &nakode_sdk::v1::SessionState::default()
        ));
    }

    #[test]
    fn activation_lock_never_expires_while_its_owner_is_alive() {
        let live_owner = format!("{}:1\n", std::process::id());
        assert!(!activation_lock_owner_is_abandoned(Some(&live_owner)));
        assert!(activation_lock_owner_is_abandoned(Some(
            "not-a-valid-owner"
        )));
        assert!(activation_lock_owner_is_abandoned(None));
    }

    #[test]
    fn activation_lease_drop_cannot_remove_a_replacement_owners_lock() {
        let directory = tempfile::tempdir().expect("activation directory");
        let path = directory.path().join("activation.lock");
        std::fs::write(&path, "first-owner").expect("first owner");
        let lease = super::ActivationLease {
            path: path.clone(),
            owner: "first-owner".to_owned(),
        };
        std::fs::write(&path, "replacement-owner").expect("replacement owner");
        drop(lease);
        assert_eq!(
            std::fs::read_to_string(path).expect("replacement lock remains"),
            "replacement-owner"
        );
    }

    struct FakeTransport {
        running: Arc<AtomicBool>,
    }

    impl FakeTransport {
        fn status(&self) -> TransportStatus {
            TransportStatus {
                name: "fake".to_owned(),
                enabled: true,
                running: self.running.load(Ordering::SeqCst),
                error: None,
            }
        }

        fn set_running(&self, running: bool) -> BoxFuture<'_, Result<TransportStatus, String>> {
            let state = Arc::clone(&self.running);
            Box::pin(async move {
                state.store(running, Ordering::SeqCst);
                Ok(TransportStatus {
                    name: "fake".to_owned(),
                    enabled: true,
                    running,
                    error: None,
                })
            })
        }
    }

    impl TransportController for FakeTransport {
        fn autostart(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
            self.set_running(true)
        }

        fn start(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
            self.set_running(true)
        }

        fn stop(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
            self.set_running(false)
        }

        fn restart(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
            self.set_running(true)
        }

        fn status(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
            let status = self.status();
            Box::pin(async move { Ok(status) })
        }
    }

    fn config_for(workspace: &Path) -> Config {
        Config {
            command: None,
            tui: false,
            update: false,
            workspace: workspace.to_path_buf(),
            model: None,
            resume: None,
            scrollback: 2_000,
            compaction_threshold_percent: 85,
            openai_reasoning_effort: OpenAiReasoningEffort::Medium,
            personalities: None,
            soul: None,
            agents: workspace.join(".nakode/agents"),
        }
    }

    #[test]
    fn server_launch_preserves_configuration_without_resume() {
        let workspace = tempfile::tempdir().expect("workspace");
        let config = Config {
            command: None,
            tui: false,
            update: false,
            workspace: workspace.path().to_path_buf(),
            model: Some("openai-codex/gpt-5".to_owned()),
            resume: Some("session-that-must-not-cross-the-boundary".to_owned()),
            scrollback: 4_321,
            compaction_threshold_percent: 73,
            openai_reasoning_effort: OpenAiReasoningEffort::High,
            personalities: Some(workspace.path().join("personalities.toml")),
            soul: Some(workspace.path().join("SOUL.md")),
            agents: workspace.path().join("agents"),
        };

        let arguments = service_arguments(&config);
        let rendered = arguments
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let expected = vec![
            "--scrollback".to_owned(),
            "4321".to_owned(),
            "--compaction-threshold-percent".to_owned(),
            "73".to_owned(),
            "--openai-reasoning-effort".to_owned(),
            "high".to_owned(),
            "--agents".to_owned(),
            workspace
                .path()
                .join("agents")
                .to_string_lossy()
                .into_owned(),
            "--personalities".to_owned(),
            workspace
                .path()
                .join("personalities.toml")
                .to_string_lossy()
                .into_owned(),
            "--soul".to_owned(),
            workspace
                .path()
                .join("SOUL.md")
                .to_string_lossy()
                .into_owned(),
            "run".to_owned(),
        ];
        assert_eq!(rendered, expected);
        assert!(!rendered.iter().any(|argument| argument == "--resume"));
        assert!(!rendered.iter().any(|argument| argument == "--model"));
        assert!(
            !rendered
                .iter()
                .any(|argument| argument == "session-that-must-not-cross-the-boundary")
        );
        assert!(
            !rendered
                .iter()
                .any(|argument| argument == "openai-codex/gpt-5")
        );

        let command = service_command(Path::new("nakode"), &config);
        for key in RESUME_ENVIRONMENT_KEYS {
            assert!(
                command
                    .as_std()
                    .get_envs()
                    .any(|(name, value)| name == OsStr::new(key) && value.is_none())
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn spawned_server_starts_in_an_independent_unix_session() {
        use nix::unistd::{Pid, getsid};
        use std::process::Stdio;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("5")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        detach_service_process(&mut command);
        let mut child = command.spawn().expect("spawn detached helper");
        let child_pid = child.id().expect("detached helper pid");
        let child_pid = i32::try_from(child_pid).expect("pid fits i32");
        let child_session = getsid(Some(Pid::from_raw(child_pid))).expect("child session");
        assert_eq!(
            child_session.as_raw(),
            child_pid,
            "setsid must make the service its own session leader"
        );
        child.start_kill().expect("stop detached helper");
        child.wait().await.expect("reap detached helper");
    }

    #[test]
    fn workspace_paths_are_stable_private_and_distinct() {
        let root = tempfile::tempdir().expect("control root");
        let first_workspace = tempfile::tempdir().expect("first workspace");
        let second_workspace = tempfile::tempdir().expect("second workspace");

        let first = workspace_runtime_directory_in(root.path(), first_workspace.path())
            .expect("first path");
        let repeated = workspace_runtime_directory_in(root.path(), first_workspace.path())
            .expect("repeated path");
        let second = workspace_runtime_directory_in(root.path(), second_workspace.path())
            .expect("second path");

        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert_eq!(
            first.parent().and_then(Path::file_name),
            Some(OsStr::new("w"))
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(first)
                    .expect("workspace control directory")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn service_paths_are_independent_of_session_access_root() {
        let control_root = tempfile::tempdir().expect("control root");
        let installation_workspace = tempfile::tempdir().expect("installation workspace");
        let first_access_root = tempfile::tempdir().expect("first access root");
        let second_access_root = tempfile::tempdir().expect("second access root");

        let first = ServicePaths::resolve_in(
            control_root.path(),
            installation_workspace.path(),
            first_access_root.path(),
        )
        .expect("first service paths");
        let second = ServicePaths::resolve_in(
            control_root.path(),
            installation_workspace.path(),
            second_access_root.path(),
        )
        .expect("second service paths");

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn lifecycle_accepts_ping_and_stops_after_shutdown() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lifecycle.sock");
        let listener = bind_service_listener(&path)
            .await
            .expect("lifecycle listener");
        let config = config_for(directory.path());
        let configuration = service_configuration_fingerprint(&config);
        let task_path = path.clone();
        let task = tokio::spawn(async move {
            run_lifecycle_listener(
                listener,
                task_path,
                configuration,
                TransportSupervisor::default(),
                None,
            )
            .await
        });

        ping_at(&path, &config).await.expect("ping response");
        assert!(
            service_running_at(&path)
                .await
                .expect("running service status")
        );
        let mut changed_config = config.clone();
        changed_config.scrollback += 1;
        assert!(matches!(
            ping_at(&path, &changed_config).await,
            Err(ControlError::ConfigurationMismatch)
        ));
        assert!(matches!(
            expect_ok(&path, &LifecycleRequest::QuiesceShutdown)
                .await
                .expect_err("legacy lifecycle cannot promise an atomic fence"),
            ControlError::ServiceRejected(message) if message.contains("unavailable")
        ));
        assert!(
            service_running_at(&path)
                .await
                .expect("refused quiescence leaves service running")
        );
        expect_ok(&path, &LifecycleRequest::Shutdown)
            .await
            .expect("shutdown response");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("lifecycle stops promptly")
            .expect("lifecycle task")
            .expect("lifecycle result");
        std::fs::remove_file(&path).expect("remove stopped lifecycle socket");
        assert!(
            !service_running_at(&path)
                .await
                .expect("stopped service status")
        );
    }

    #[tokio::test]
    async fn lifecycle_controls_a_transport_without_stopping_the_service() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lifecycle.sock");
        let listener = bind_service_listener(&path)
            .await
            .expect("lifecycle listener");
        let config = config_for(directory.path());
        let configuration = service_configuration_fingerprint(&config);
        let fake = Arc::new(FakeTransport {
            running: Arc::new(AtomicBool::new(false)),
        });
        let supervisor = TransportSupervisor::new([(
            "fake".to_owned(),
            Arc::clone(&fake) as Arc<dyn TransportController>,
        )]);
        let task_path = path.clone();
        let task = tokio::spawn(async move {
            run_lifecycle_listener(listener, task_path, configuration, supervisor, None).await
        });

        let start = exchange(
            &path,
            &LifecycleRequest::Transport {
                name: "fake".to_owned(),
                action: TransportAction::Start,
            },
        )
        .await
        .expect("transport start response");
        assert!(matches!(
            start,
            LifecycleResponse::Transport { status } if status.running
        ));
        assert!(fake.running.load(Ordering::SeqCst));

        let status = exchange(
            &path,
            &LifecycleRequest::Transport {
                name: "fake".to_owned(),
                action: TransportAction::Status,
            },
        )
        .await
        .expect("transport status response");
        assert!(matches!(
            status,
            LifecycleResponse::Transport { status } if status.running
        ));
        ping_at(&path, &config)
            .await
            .expect("native service remains available");

        let stop = exchange(
            &path,
            &LifecycleRequest::Transport {
                name: "fake".to_owned(),
                action: TransportAction::Stop,
            },
        )
        .await
        .expect("transport stop response");
        assert!(matches!(
            stop,
            LifecycleResponse::Transport { status } if !status.running
        ));
        assert!(!fake.running.load(Ordering::SeqCst));
        ping_at(&path, &config)
            .await
            .expect("native service remains available after transport stop");

        expect_ok(&path, &LifecycleRequest::Shutdown)
            .await
            .expect("shutdown response");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("lifecycle stops promptly")
            .expect("lifecycle task")
            .expect("lifecycle result");
    }
    #[tokio::test]
    async fn frontend_discovery_starts_a_missing_server_once() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let lifecycle_path = directory.path().join("missing-lifecycle.sock");
        let api_path = directory.path().join("missing-api.sock");
        let starts = Arc::new(AtomicUsize::new(0));
        let starts_for_request = Arc::clone(&starts);
        let started_path = api_path.clone();

        let resolved = frontend_api_endpoint_at(
            &lifecycle_path,
            &api_path,
            directory.path(),
            move || async move {
                starts_for_request.fetch_add(1, Ordering::SeqCst);
                Ok(started_path)
            },
        )
        .await
        .expect("missing server starts");

        assert_eq!(resolved, api_path);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn strict_startup_refuses_a_live_server_with_conflicting_configuration() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let paths = ServicePaths::in_directory(directory.path());
        let lifecycle_path = paths.lifecycle().to_path_buf();
        let listener = bind_service_listener(&lifecycle_path)
            .await
            .expect("lifecycle listener");
        let default_config = config_for(directory.path());
        let mut live_config = default_config.clone();
        live_config.scrollback += 1;
        let configuration = service_configuration_fingerprint(&live_config);
        let task_path = lifecycle_path.clone();
        let task = tokio::spawn(async move {
            run_lifecycle_listener(
                listener,
                task_path,
                configuration,
                TransportSupervisor::default(),
                None,
            )
            .await
        });

        let error = ensure_service_at(&paths, Path::new("/missing/nakode"), &default_config)
            .await
            .expect_err("conflicting live service must not be replaced");
        assert!(matches!(error, ControlError::ConfigurationMismatch));

        expect_ok(&lifecycle_path, &LifecycleRequest::Shutdown)
            .await
            .expect("shutdown response");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("lifecycle stops promptly")
            .expect("lifecycle task")
            .expect("lifecycle result");
    }

    #[tokio::test]
    async fn bulk_shutdown_handles_live_stale_and_failed_runtime_sets_deterministically() {
        let root = tempfile::tempdir().expect("control root");
        let workspace_root = root.path().join("w");
        let live = workspace_root.join("a-live");
        let stale = workspace_root.join("b-stale");
        let failed = workspace_root.join("c-failed");
        for directory in [&live, &stale, &failed] {
            std::fs::create_dir_all(directory).expect("workspace control directory");
        }

        let live_lifecycle_path = live.join("c.sock");
        let live_api_path = live.join("api.sock");
        let live_lifecycle = UnixListener::bind(&live_lifecycle_path).expect("live lifecycle");
        let live_api = UnixListener::bind(&live_api_path).expect("live API");
        let live_task = tokio::spawn(async move {
            let (stream, _) = live_lifecycle.accept().await.expect("shutdown connection");
            let (_, mut writer) = stream.into_split();
            writer
                .write_all(b"{\"type\":\"ok\"}\n")
                .await
                .expect("shutdown response");
            drop(writer);
            drop(live_api);
            std::fs::remove_file(&live_lifecycle_path).expect("remove lifecycle socket");
            std::fs::remove_file(&live_api_path).expect("remove API socket");
        });

        let stale_lifecycle_path = stale.join("c.sock");
        let stale_api_path = stale.join("api.sock");
        drop(
            std::os::unix::net::UnixListener::bind(&stale_lifecycle_path).expect("stale lifecycle"),
        );
        drop(std::os::unix::net::UnixListener::bind(&stale_api_path).expect("stale API"));
        std::thread::sleep(Duration::from_millis(10));

        let failed_lifecycle_path = failed.join("c.sock");
        let failed_api_path = failed.join("api.sock");
        let failed_lifecycle =
            UnixListener::bind(&failed_lifecycle_path).expect("failed lifecycle");
        let failed_api = UnixListener::bind(&failed_api_path).expect("failed API");
        let failed_task = tokio::spawn(async move {
            let (stream, _) = failed_lifecycle.accept().await.expect("failed connection");
            let (_, mut writer) = stream.into_split();
            writer.write_all(b"not-json\n").await.expect("bad response");
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(failed_api);
        });

        let report = shutdown_all_services_in(root.path())
            .await
            .expect("enumerate runtime sets");
        assert_eq!(report.stopped, 1);
        assert_eq!(report.stale, 1);
        assert_eq!(report.failures.len(), 1);
        assert!(report.failures[0].contains("c-failed"));
        assert!(!stale_lifecycle_path.exists());
        assert!(!stale_api_path.exists());
        live_task.await.expect("live task");
        failed_task.await.expect("failed task");
    }

    #[tokio::test]
    async fn binding_reclaims_stale_socket_without_replacing_a_live_server() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lifecycle.sock");
        let listener = bind_service_listener(&path).await.expect("first listener");

        let error = bind_service_listener(&path)
            .await
            .expect_err("live listener must not be replaced");
        assert!(matches!(error, ControlError::AlreadyRunning(_)));

        drop(listener);
        let replacement = bind_service_listener(&path)
            .await
            .expect("stale socket is reclaimed");
        drop(replacement);
    }
}
