use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};

const SERVICE_START_ATTEMPTS: usize = 40;
const SERVICE_START_RETRY: Duration = Duration::from_millis(50);
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(1);
const REGISTRATION_TTL: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
struct RegisteredTui {
    socket_path: PathBuf,
    refreshed_at: tokio::time::Instant,
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("Nakode control socket error at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid Nakode control message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Nakode control service closed without a result")]
    MissingResponse,
    #[error("a Nakode control service is already running at {0}")]
    AlreadyRunning(String),
    #[error("this platform does not expose an application data directory")]
    MissingDataDirectory,
    #[error("could not start the Nakode control service: {0}")]
    SpawnService(#[source] std::io::Error),
    #[error("Nakode control service did not become ready at {0}")]
    ServiceStartup(String),
    #[error("Nakode control service rejected the request: {0}")]
    ServiceRejected(String),
    #[error("unexpected response from the Nakode control service")]
    UnexpectedServiceResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentInvocation {
    pub agent: String,
    pub session_id: String,
    pub task: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentResponse {
    pub success: bool,
    pub result: String,
}

pub struct IncomingInvocation {
    pub id: u64,
    pub invocation: AgentInvocation,
    response: tokio::sync::oneshot::Sender<AgentResponse>,
}

pub struct ControlServer {
    pub requests: tokio::sync::mpsc::Receiver<IncomingInvocation>,
    task: tokio::task::JoinHandle<()>,
}

/// Keeps one TUI registered with the user-level control service.
pub struct ControlRegistration {
    session_id: String,
    socket_path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServiceRequest {
    Ping,
    Register {
        session_id: String,
        socket_path: PathBuf,
    },
    Unregister {
        session_id: String,
        socket_path: PathBuf,
    },
    Invoke {
        invocation: AgentInvocation,
    },
    Shutdown,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServiceResponse {
    Ok,
    Agent { response: AgentResponse },
    Error { message: String },
}

impl ControlServer {
    /// Binds a TUI-local control socket.
    ///
    /// # Errors
    /// Returns an error when the socket cannot be prepared or bound.
    pub async fn bind(path: &Path) -> Result<Self, ControlError> {
        if path.exists() {
            if UnixStream::connect(path).await.is_ok() {
                return Err(ControlError::AlreadyRunning(path.display().to_string()));
            }
            std::fs::remove_file(path).map_err(|source| socket_error(path, source))?;
        }
        let listener = UnixListener::bind(path).map_err(|source| socket_error(path, source))?;
        let (tx, requests) = tokio::sync::mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut next_id = 1_u64;
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                let id = next_id;
                next_id = next_id.wrapping_add(1);
                tokio::spawn(async move {
                    handle_tui_connection(stream, id, tx).await;
                });
            }
        });
        Ok(Self { requests, task })
    }

    pub fn shutdown(self, path: &Path) {
        self.task.abort();
        let _ = std::fs::remove_file(path);
    }
}

impl IncomingInvocation {
    pub fn respond(self, response: AgentResponse) {
        let _ = self.response.send(response);
    }
}

impl ControlRegistration {
    /// Registers a TUI with the shared control service and keeps the registration alive.
    ///
    /// # Errors
    /// Returns an error when the service cannot be started or the initial registration fails.
    pub async fn start(
        executable: &Path,
        session_id: &str,
        socket_path: &Path,
    ) -> Result<Self, ControlError> {
        ensure_service(executable).await?;
        let service_path = service_socket_path()?;
        register_at(&service_path, session_id, socket_path).await?;

        let executable = executable.to_path_buf();
        let session_id = session_id.to_owned();
        let socket_path = socket_path.to_path_buf();
        let task_session_id = session_id.clone();
        let task_socket_path = socket_path.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(REGISTRATION_INTERVAL).await;
                if ensure_service(&executable).await.is_ok()
                    && let Ok(service_path) = service_socket_path()
                {
                    let _ = register_at(&service_path, &task_session_id, &task_socket_path).await;
                }
            }
        });

        Ok(Self {
            session_id,
            socket_path,
            task,
        })
    }

    pub async fn shutdown(self) {
        self.task.abort();
        if let Ok(service_path) = service_socket_path() {
            let _ = unregister_at(&service_path, &self.session_id, &self.socket_path).await;
        }
    }
}

async fn handle_tui_connection(
    stream: UnixStream,
    id: u64,
    tx: tokio::sync::mpsc::Sender<IncomingInvocation>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    if BufReader::new(reader).read_line(&mut line).await.is_err() {
        return;
    }
    let Ok(invocation) = serde_json::from_str(&line) else {
        return;
    };
    let (response, receive) = tokio::sync::oneshot::channel();
    if tx
        .send(IncomingInvocation {
            id,
            invocation,
            response,
        })
        .await
        .is_err()
    {
        return;
    }
    if let Ok(response) = receive.await
        && let Ok(encoded) = serde_json::to_string(&response)
    {
        let _ = writer.write_all(encoded.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

/// Sends an invocation directly to one TUI and waits for its result.
///
/// # Errors
/// Returns an error when the TUI cannot be reached or exchanges an invalid response.
pub async fn invoke(
    path: &Path,
    invocation: &AgentInvocation,
) -> Result<AgentResponse, ControlError> {
    exchange(path, invocation).await
}

/// Routes an invocation through the shared service, with compatibility fallback
/// to a control socket owned by an older TUI.
///
/// # Errors
/// Returns an error when no compatible control service can be reached.
pub async fn invoke_via_service(
    workspace: &Path,
    invocation: &AgentInvocation,
) -> Result<AgentResponse, ControlError> {
    let service_path = service_socket_path()?;
    match invoke_at(&service_path, invocation).await {
        Ok(response) => Ok(response),
        Err(service_error) => {
            let compatibility_path = client_socket_path(workspace);
            if compatibility_path.exists() {
                invoke(&compatibility_path, invocation).await
            } else {
                Err(service_error)
            }
        }
    }
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

/// Runs the user-level control service until it receives a shutdown request.
///
/// # Errors
/// Returns an error when its socket cannot be created or served.
pub async fn run_service() -> Result<(), ControlError> {
    let path = service_socket_path()?;
    run_service_at(&path).await
}

async fn run_service_at(path: &Path) -> Result<(), ControlError> {
    if path.exists() {
        if ping_at(path).await.is_ok() {
            return Err(ControlError::AlreadyRunning(path.display().to_string()));
        }
        std::fs::remove_file(path).map_err(|source| socket_error(path, source))?;
    }
    let listener = UnixListener::bind(path).map_err(|source| socket_error(path, source))?;
    let routes = Arc::new(Mutex::new(HashMap::<String, RegisteredTui>::new()));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
    let mut stale_route_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + REGISTRATION_TTL,
        REGISTRATION_TTL,
    );

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|source| socket_error(path, source))?;
                let routes = Arc::clone(&routes);
                let shutdown_tx = shutdown_tx.clone();
                tokio::spawn(async move {
                    handle_service_connection(stream, routes, shutdown_tx).await;
                });
            }
            _ = shutdown_rx.recv() => break,
            _ = stale_route_tick.tick() => {
                let cutoff = tokio::time::Instant::now() - REGISTRATION_TTL;
                let mut routes = routes.lock().await;
                routes.retain(|_, route| route.refreshed_at >= cutoff);
                if routes.is_empty() {
                    break;
                }
            }
        }
    }

    drop(listener);
    let _ = std::fs::remove_file(path);
    Ok(())
}

async fn handle_service_connection(
    stream: UnixStream,
    routes: Arc<Mutex<HashMap<String, RegisteredTui>>>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    if BufReader::new(reader).read_line(&mut line).await.is_err() {
        return;
    }
    let request = match serde_json::from_str(&line) {
        Ok(request) => request,
        Err(error) => {
            write_service_response(
                &mut writer,
                &ServiceResponse::Error {
                    message: error.to_string(),
                },
            )
            .await;
            return;
        }
    };

    let mut should_shutdown = false;
    let response = match request {
        ServiceRequest::Ping => ServiceResponse::Ok,
        ServiceRequest::Register {
            session_id,
            socket_path,
        } => {
            routes.lock().await.insert(
                session_id,
                RegisteredTui {
                    socket_path,
                    refreshed_at: tokio::time::Instant::now(),
                },
            );
            ServiceResponse::Ok
        }
        ServiceRequest::Unregister {
            session_id,
            socket_path,
        } => {
            let mut routes = routes.lock().await;
            if routes
                .get(&session_id)
                .is_some_and(|route| route.socket_path == socket_path)
            {
                routes.remove(&session_id);
            }
            ServiceResponse::Ok
        }
        ServiceRequest::Invoke { invocation } => route_invocation(&routes, &invocation).await,
        ServiceRequest::Shutdown => {
            should_shutdown = true;
            ServiceResponse::Ok
        }
    };

    write_service_response(&mut writer, &response).await;
    if should_shutdown {
        let _ = shutdown_tx.send(()).await;
    }
}

async fn route_invocation(
    routes: &Mutex<HashMap<String, RegisteredTui>>,
    invocation: &AgentInvocation,
) -> ServiceResponse {
    let socket_path = routes
        .lock()
        .await
        .get(&invocation.session_id)
        .map(|route| route.socket_path.clone());
    let Some(socket_path) = socket_path else {
        return ServiceResponse::Agent {
            response: AgentResponse {
                success: false,
                result: "No running TUI is registered for this Nakode session.".to_owned(),
            },
        };
    };

    match invoke(&socket_path, invocation).await {
        Ok(response) => ServiceResponse::Agent { response },
        Err(error) => {
            let mut routes = routes.lock().await;
            if routes
                .get(&invocation.session_id)
                .is_some_and(|route| route.socket_path == socket_path)
            {
                routes.remove(&invocation.session_id);
            }
            ServiceResponse::Agent {
                response: AgentResponse {
                    success: false,
                    result: format!("The TUI registered for this session is unavailable: {error}"),
                },
            }
        }
    }
}

async fn write_service_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &ServiceResponse,
) {
    if let Ok(encoded) = serde_json::to_string(response) {
        let _ = writer.write_all(encoded.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

async fn invoke_at(
    service_path: &Path,
    invocation: &AgentInvocation,
) -> Result<AgentResponse, ControlError> {
    match exchange(
        service_path,
        &ServiceRequest::Invoke {
            invocation: invocation.clone(),
        },
    )
    .await?
    {
        ServiceResponse::Agent { response } => Ok(response),
        ServiceResponse::Error { message } => Err(ControlError::ServiceRejected(message)),
        ServiceResponse::Ok => Err(ControlError::UnexpectedServiceResponse),
    }
}

async fn register_at(
    service_path: &Path,
    session_id: &str,
    socket_path: &Path,
) -> Result<(), ControlError> {
    expect_ok(
        service_path,
        &ServiceRequest::Register {
            session_id: session_id.to_owned(),
            socket_path: socket_path.to_path_buf(),
        },
    )
    .await
}

async fn unregister_at(
    service_path: &Path,
    session_id: &str,
    socket_path: &Path,
) -> Result<(), ControlError> {
    expect_ok(
        service_path,
        &ServiceRequest::Unregister {
            session_id: session_id.to_owned(),
            socket_path: socket_path.to_path_buf(),
        },
    )
    .await
}

async fn ping_at(service_path: &Path) -> Result<(), ControlError> {
    expect_ok(service_path, &ServiceRequest::Ping).await
}

async fn expect_ok(path: &Path, request: &ServiceRequest) -> Result<(), ControlError> {
    match exchange(path, request).await? {
        ServiceResponse::Ok => Ok(()),
        ServiceResponse::Error { message } => Err(ControlError::ServiceRejected(message)),
        ServiceResponse::Agent { .. } => Err(ControlError::UnexpectedServiceResponse),
    }
}

async fn ensure_service(executable: &Path) -> Result<(), ControlError> {
    let service_path = service_socket_path()?;
    if ping_at(&service_path).await.is_ok() {
        return Ok(());
    }

    let mut child = tokio::process::Command::new(executable)
        .args(["service", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(ControlError::SpawnService)?;
    tokio::spawn(async move {
        let _ = child.wait().await;
    });

    for _ in 0..SERVICE_START_ATTEMPTS {
        tokio::time::sleep(SERVICE_START_RETRY).await;
        if ping_at(&service_path).await.is_ok() {
            return Ok(());
        }
    }
    Err(ControlError::ServiceStartup(
        service_path.display().to_string(),
    ))
}

/// Stops the user-level service if one is currently running.
///
/// # Errors
/// Returns an error when a live service rejects or cannot read the request.
pub async fn shutdown_service() -> Result<(), ControlError> {
    let service_path = service_socket_path()?;
    if !service_path.exists() {
        return Ok(());
    }
    match expect_ok(&service_path, &ServiceRequest::Shutdown).await {
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

fn control_directory() -> Result<PathBuf, ControlError> {
    let directory = if let Some(configured) = std::env::var_os("NAKODE_CONTROL_DIR") {
        PathBuf::from(configured)
    } else {
        ProjectDirs::from("dev", "nakode", "Nakode")
            .map(|project| project.data_local_dir().to_path_buf())
            .ok_or(ControlError::MissingDataDirectory)?
    };
    std::fs::create_dir_all(&directory).map_err(|source| socket_error(&directory, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .map_err(|source| socket_error(&directory, source))?;
    }
    Ok(directory)
}

/// Returns the socket used by the shared user-level control service.
///
/// # Errors
/// Returns an error when the platform data directory cannot be prepared.
pub fn service_socket_path() -> Result<PathBuf, ControlError> {
    Ok(control_directory()?.join("control.sock"))
}

/// Returns the private socket used by the current TUI process.
///
/// # Errors
/// Returns an error when the platform data directory cannot be prepared.
pub fn tui_socket_path() -> Result<PathBuf, ControlError> {
    Ok(control_directory()?.join(format!("tui-{}.sock", std::process::id())))
}

fn socket_error(path: &Path, source: std::io::Error) -> ControlError {
    ControlError::Io {
        path: path.display().to_string(),
        source,
    }
}

#[must_use]
pub fn socket_path(workspace: &Path) -> PathBuf {
    workspace.join(".nakode").join("control.sock")
}

#[must_use]
pub fn client_socket_path(workspace: &Path) -> PathBuf {
    let current = socket_path(workspace);
    if current.exists() {
        return current;
    }
    let legacy = workspace.join(".nako-agent").join("control.sock");
    if legacy.exists() { legacy } else { current }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentInvocation, AgentResponse, ControlError, ControlServer, ServiceRequest,
        client_socket_path, expect_ok, invoke, invoke_at, register_at, run_service_at, socket_path,
    };

    #[test]
    fn client_socket_falls_back_to_the_legacy_workspace_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let legacy = directory.path().join(".nako-agent/control.sock");
        std::fs::create_dir_all(legacy.parent().expect("legacy parent"))
            .expect("legacy control directory");
        std::fs::write(&legacy, []).expect("legacy socket placeholder");

        assert_eq!(client_socket_path(directory.path()), legacy);
        assert_eq!(
            socket_path(directory.path()),
            directory.path().join(".nakode/control.sock")
        );
    }

    #[tokio::test]
    async fn invocation_round_trips_through_one_tui() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let mut server = ControlServer::bind(&path).await.expect("control server");
        let invocation = invocation("session-7", "Review auth");
        let client_path = path.clone();
        let client = tokio::spawn(async move { invoke(&client_path, &invocation).await });

        let request = server.requests.recv().await.expect("incoming invocation");
        assert_eq!(request.invocation.agent, "reviewer");
        assert_eq!(request.invocation.session_id, "session-7");
        assert_eq!(request.invocation.task, "Review auth");
        request.respond(AgentResponse {
            success: true,
            result: "No defects".to_owned(),
        });

        let response = client.await.expect("client task").expect("agent response");
        assert!(response.success);
        assert_eq!(response.result, "No defects");
        server.shutdown(&path);
    }

    #[tokio::test]
    async fn concurrent_invocations_keep_independent_response_channels() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let mut server = ControlServer::bind(&path).await.expect("control server");
        let first_path = path.clone();
        let first =
            tokio::spawn(
                async move { invoke(&first_path, &invocation("session-7", "hardware")).await },
            );
        let second_path = path.clone();
        let second = tokio::spawn(async move {
            invoke(&second_path, &invocation("session-7", "operating system")).await
        });

        for _ in 0..2 {
            let request = server.requests.recv().await.expect("concurrent invocation");
            let result = format!("{} complete", request.invocation.task);
            request.respond(AgentResponse {
                success: true,
                result,
            });
        }

        let first = first
            .await
            .expect("first client task")
            .expect("first result");
        let second = second
            .await
            .expect("second client task")
            .expect("second result");
        assert_eq!(first.result, "hardware complete");
        assert_eq!(second.result, "operating system complete");
        server.shutdown(&path);
    }

    #[tokio::test]
    async fn binding_does_not_replace_a_live_tui_socket() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let server = ControlServer::bind(&path).await.expect("control server");

        let error = ControlServer::bind(&path)
            .await
            .err()
            .expect("second server must fail");
        assert!(matches!(error, ControlError::AlreadyRunning(_)));
        server.shutdown(&path);
    }

    #[tokio::test]
    async fn shared_service_routes_multiple_tuis_by_session() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let service_path = directory.path().join("service.sock");
        let first_path = directory.path().join("first.sock");
        let second_path = directory.path().join("second.sock");
        let mut first_server = ControlServer::bind(&first_path).await.expect("first TUI");
        let mut second_server = ControlServer::bind(&second_path).await.expect("second TUI");

        let task_path = service_path.clone();
        let service_task = tokio::spawn(async move { run_service_at(&task_path).await });
        wait_for_service(&service_path).await;
        register_at(&service_path, "first-session", &first_path)
            .await
            .expect("register first TUI");
        register_at(&service_path, "second-session", &second_path)
            .await
            .expect("register second TUI");

        let first_service_path = service_path.clone();
        let first_invocation = tokio::spawn(async move {
            invoke_at(
                &first_service_path,
                &invocation("first-session", "first task"),
            )
            .await
        });
        let second_service_path = service_path.clone();
        let second_invocation = tokio::spawn(async move {
            invoke_at(
                &second_service_path,
                &invocation("second-session", "second task"),
            )
            .await
        });

        let first_request = first_server.requests.recv().await.expect("first request");
        assert_eq!(first_request.invocation.task, "first task");
        first_request.respond(AgentResponse {
            success: true,
            result: "first result".to_owned(),
        });
        let second_request = second_server.requests.recv().await.expect("second request");
        assert_eq!(second_request.invocation.task, "second task");
        second_request.respond(AgentResponse {
            success: true,
            result: "second result".to_owned(),
        });

        assert_eq!(
            first_invocation
                .await
                .expect("first invocation task")
                .expect("first invocation")
                .result,
            "first result"
        );
        assert_eq!(
            second_invocation
                .await
                .expect("second invocation task")
                .expect("second invocation")
                .result,
            "second result"
        );

        expect_ok(&service_path, &ServiceRequest::Shutdown)
            .await
            .expect("shutdown service");
        service_task
            .await
            .expect("service task")
            .expect("service result");
        first_server.shutdown(&first_path);
        second_server.shutdown(&second_path);
    }

    fn invocation(session_id: &str, task: &str) -> AgentInvocation {
        AgentInvocation {
            agent: "reviewer".to_owned(),
            session_id: session_id.to_owned(),
            task: task.to_owned(),
        }
    }

    async fn wait_for_service(path: &std::path::Path) {
        for _ in 0..20 {
            if expect_ok(path, &ServiceRequest::Ping).await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("service did not start");
    }
}
