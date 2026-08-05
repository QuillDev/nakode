use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use directories::ProjectDirs;
use futures_util::future::BoxFuture;
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
const SERVICE_START_RETRY: Duration = Duration::from_millis(50);
const RESUME_ENVIRONMENT_KEYS: [&str; 2] = ["NAKODE_RESUME", "NAKO_AGENT_RESUME"];

/// Workspace service state returned by the lifecycle CLI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceStatus {
    pub running: bool,
    pub workspace: PathBuf,
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

    async fn control(
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
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LifecycleRequest {
    Ping,
    Shutdown,
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
/// request or one of its required components stops.
///
/// # Errors
/// Returns when socket acquisition, runtime preparation, or a server component
/// fails.
pub async fn run_service(config: Config) -> Result<(), ControlError> {
    let mut lease = WorkspaceServerLease::acquire(&config.workspace).await?;
    let configuration = service_configuration_fingerprint(&config);
    let prepared = crate::server::runtime::prepare_runtime(&config).await?;
    let (runtime, handle) = prepared.into_actor();
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
    let transports = crate::discord::transport_supervisor(&config.workspace, grpc_path.clone());
    let mut lifecycle = tokio::spawn(run_lifecycle_listener(
        lifecycle_listener,
        lifecycle_path,
        configuration,
        transports.clone(),
    ));
    let mut grpc = tokio::spawn(run_grpc_listener(
        grpc_listener,
        grpc_path.clone(),
        endpoint,
    ));
    let mut actor = tokio::spawn(runtime.run());
    transports.autostart().await;

    let result = tokio::select! {
        result = &mut lifecycle => flatten_component(result, "lifecycle listener"),
        result = &mut grpc => flatten_component(result, "gRPC listener"),
        result = &mut actor => match result {
            Ok(()) => Err(ControlError::ComponentStopped("native runtime")),
            Err(_) => Err(ControlError::ComponentStopped("native runtime task")),
        },
    };
    handle.shutdown().await;
    lifecycle.abort();
    grpc.abort();
    transports.stop_all().await;
    if !actor.is_finished() {
        let _ = actor.await;
    }
    result
}

async fn run_lifecycle_listener(
    listener: UnixListener,
    path: PathBuf,
    configuration: String,
    transports: TransportSupervisor,
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
                connections.spawn(async move {
                    handle_lifecycle_connection(
                        stream,
                        shutdown_tx,
                        &configuration,
                        &transports,
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
    let should_shutdown = matches!(request, LifecycleRequest::Shutdown);
    let response = match request {
        LifecycleRequest::Ping => LifecycleResponse::Ready {
            configuration: configuration.to_owned(),
        },
        LifecycleRequest::Shutdown => LifecycleResponse::Ok,
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
) -> Result<(), ControlError> {
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(nakode_server::grpc::GrpcService::new(endpoint).into_server())
        .serve_with_incoming(incoming)
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
    lifecycle: Option<UnixListener>,
    grpc: Option<UnixListener>,
}

impl WorkspaceServerLease {
    async fn acquire(workspace: &Path) -> Result<Self, ControlError> {
        let lifecycle_path = service_socket_path(workspace)?;
        let grpc_path = grpc_service_socket_path(workspace)?;
        let lifecycle = bind_service_listener(&lifecycle_path).await?;
        let grpc = match bind_service_listener(&grpc_path).await {
            Ok(listener) => listener,
            Err(error) => {
                drop(lifecycle);
                let _ = std::fs::remove_file(&lifecycle_path);
                return Err(error);
            }
        };
        Ok(Self {
            lifecycle_path,
            grpc_path,
            lifecycle: Some(lifecycle),
            grpc: Some(grpc),
        })
    }
}

impl Drop for WorkspaceServerLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.lifecycle_path);
        let _ = std::fs::remove_file(&self.grpc_path);
    }
}

/// Reports whether a bound socket path is still served by a live listener.
///
/// A listener whose owner has just released it can keep completing connections for a short window,
/// so one successful connect does not distinguish a live server from an already-stale socket.
/// Confirm with a second probe: a live listener stays connectable, while a released one refuses the
/// retry. Only a failing connect can classify a socket as dead, so this never steals the path from
/// a busy server.
async fn socket_is_live(path: &Path) -> bool {
    if UnixStream::connect(path).await.is_err() {
        return false;
    }
    tokio::time::sleep(SERVICE_START_RETRY).await;
    UnixStream::connect(path).await.is_ok()
}

async fn bind_service_listener(path: &Path) -> Result<UnixListener, ControlError> {
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

async fn ensure_service(executable: &Path, config: &Config) -> Result<(), ControlError> {
    let service_path = service_socket_path(&config.workspace)?;
    ensure_service_at(&service_path, executable, config).await
}

async fn ensure_service_at(
    service_path: &Path,
    executable: &Path,
    config: &Config,
) -> Result<(), ControlError> {
    match ping_at(service_path, config).await {
        Ok(()) => return Ok(()),
        Err(ControlError::ConfigurationMismatch) => {
            return Err(ControlError::ConfigurationMismatch);
        }
        Err(_) => {}
    }

    let mut child = service_command(executable, config)
        .spawn()
        .map_err(ControlError::SpawnService)?;
    tokio::spawn(async move {
        let _ = child.wait().await;
    });

    for _ in 0..SERVICE_START_ATTEMPTS {
        tokio::time::sleep(SERVICE_START_RETRY).await;
        if ping_at(service_path, config).await.is_ok() {
            return Ok(());
        }
    }
    Err(ControlError::ServiceStartup(
        service_path.display().to_string(),
    ))
}

fn service_command(executable: &Path, config: &Config) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(executable);
    command
        .args(service_arguments(config))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    detach_service_process(&mut command);
    for key in RESUME_ENVIRONMENT_KEYS {
        command.env_remove(key);
    }
    command
}

#[cfg(unix)]
fn detach_service_process(command: &mut tokio::process::Command) {
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
fn detach_service_process(command: &mut tokio::process::Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    command
        .as_std_mut()
        .creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
}

#[cfg(not(any(unix, windows)))]
fn detach_service_process(_command: &mut tokio::process::Command) {}

fn service_arguments(config: &Config) -> Vec<OsString> {
    let mut arguments = vec![
        OsString::from("--workspace"),
        config.workspace.as_os_str().to_owned(),
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
    arguments.push(OsString::from("service"));
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

async fn ensure_api_service(executable: &Path, config: &Config) -> Result<PathBuf, ControlError> {
    let api_path = grpc_service_socket_path(&config.workspace)?;
    ensure_service(executable, config).await?;
    for _ in 0..SERVICE_START_ATTEMPTS {
        if api_ready(&api_path, &config.workspace).await {
            return Ok(api_path);
        }
        tokio::time::sleep(SERVICE_START_RETRY).await;
    }
    Err(ControlError::ServiceStartup(api_path.display().to_string()))
}

/// Returns a compatible live endpoint or starts the workspace server when none
/// exists.
///
/// Discovery deliberately does not compare the live server's startup
/// configuration with the connector process's defaults. The running server is
/// authoritative for its own configuration. A missing server is still started
/// through [`ensure_api_service`], whose fingerprint check prevents racing
/// or replacing a server started with conflicting options.
///
/// # Errors
/// Returns when a live server does not expose a compatible API endpoint or
/// when a missing server cannot be started with the supplied configuration.
pub async fn frontend_api_endpoint(
    executable: &Path,
    config: &Config,
) -> Result<PathBuf, ControlError> {
    let lifecycle_path = service_socket_path(&config.workspace)?;
    let api_path = grpc_service_socket_path(&config.workspace)?;
    frontend_api_endpoint_at(&lifecycle_path, &api_path, &config.workspace, || {
        ensure_api_service(executable, config)
    })
    .await
}

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
pub async fn service_status(workspace: &Path) -> Result<ServiceStatus, ControlError> {
    let service_path = service_socket_path(workspace)?;
    let running = service_running_at(&service_path).await?;
    Ok(ServiceStatus {
        running,
        workspace: workspace.to_path_buf(),
    })
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
    let status = service_status(&config.workspace).await?;
    let lifecycle_path = service_socket_path(&config.workspace)?;
    let api_path = grpc_service_socket_path(&config.workspace)?;
    shutdown_service(&config.workspace).await?;

    if status.running {
        for _ in 0..SERVICE_STOP_ATTEMPTS {
            if !lifecycle_path.exists() && !api_path.exists() {
                return ensure_service(executable, config).await;
            }
            tokio::time::sleep(SERVICE_START_RETRY).await;
        }
        return Err(ControlError::ServiceShutdown(
            lifecycle_path.display().to_string(),
        ));
    }

    // A stopped process can leave socket files behind after an unclean exit.
    let _ = std::fs::remove_file(&lifecycle_path);
    let _ = std::fs::remove_file(api_path);
    ensure_service(executable, config).await
}

/// Stops the workspace server if one is currently running.
///
/// # Errors
/// Returns an error when a live server rejects or cannot read the request.
pub async fn shutdown_service(workspace: &Path) -> Result<(), ControlError> {
    let service_path = service_socket_path(workspace)?;
    if !service_path.exists() {
        return Ok(());
    }
    match expect_ok(&service_path, &LifecycleRequest::Shutdown).await {
        Ok(()) => Ok(()),
        Err(ControlError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            let _ = std::fs::remove_file(service_path);
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

async fn shutdown_all_services_in(
    control_root: &Path,
) -> Result<BulkServiceShutdownReport, ControlError> {
    let workspace_root = control_root.join("w");
    let entries = match std::fs::read_dir(&workspace_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BulkServiceShutdownReport::default());
        }
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

/// Controls one frontend transport in a running workspace service without
/// changing the native server lifecycle.
///
/// # Errors
/// Returns an error when the service is not reachable or rejects the request.
pub async fn transport_action(
    workspace: &Path,
    name: &str,
    action: TransportAction,
) -> Result<TransportStatus, ControlError> {
    let service_path = service_socket_path(workspace)?;
    match exchange(
        &service_path,
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

/// Returns the private lifecycle socket for one canonical workspace.
///
/// # Errors
/// Returns an error when the platform data directory cannot be prepared.
pub fn service_socket_path(workspace: &Path) -> Result<PathBuf, ControlError> {
    Ok(workspace_control_directory(workspace)?.join("c.sock"))
}

/// Returns the socket implementing the generated Nakode API.
///
/// # Errors
/// Returns an error when the platform data directory cannot be prepared.
pub fn grpc_service_socket_path(workspace: &Path) -> Result<PathBuf, ControlError> {
    Ok(workspace_control_directory(workspace)?.join("api.sock"))
}

fn workspace_control_directory(workspace: &Path) -> Result<PathBuf, ControlError> {
    workspace_control_directory_in(&control_directory()?, workspace)
}

fn workspace_control_directory_in(
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
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::future::BoxFuture;
    use tokio::io::AsyncWriteExt;

    use super::{
        ControlError, LifecycleRequest, LifecycleResponse, RESUME_ENVIRONMENT_KEYS,
        TransportAction, TransportController, TransportStatus, TransportSupervisor, UnixListener,
        bind_service_listener, detach_service_process, ensure_service_at, exchange, expect_ok,
        frontend_api_endpoint_at, ping_at, run_lifecycle_listener, service_arguments,
        service_command, service_configuration_fingerprint, service_running_at,
        shutdown_all_services_in, workspace_control_directory_in,
    };
    use crate::config::{Config, OpenAiReasoningEffort};

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
            "--workspace".to_owned(),
            workspace.path().to_string_lossy().into_owned(),
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
            "service".to_owned(),
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

        let first = workspace_control_directory_in(root.path(), first_workspace.path())
            .expect("first path");
        let repeated = workspace_control_directory_in(root.path(), first_workspace.path())
            .expect("repeated path");
        let second = workspace_control_directory_in(root.path(), second_workspace.path())
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
            run_lifecycle_listener(listener, task_path, configuration, supervisor).await
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
        let lifecycle_path = directory.path().join("lifecycle.sock");
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
            )
            .await
        });

        let error = ensure_service_at(
            &lifecycle_path,
            Path::new("/missing/nakode"),
            &default_config,
        )
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
