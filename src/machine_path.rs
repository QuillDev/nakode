//! Machine-local PATH authority. Reads and saves never execute the configured command.
use nakode_sdk::v1 as api;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, OnceLock, RwLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};
use tonic::{Request, Response, Status};

const LIMIT: usize = 64 * 1024;
static EFFECTIVE: RwLock<Option<String>> = RwLock::new(None);
static SERVICE: OnceLock<MachinePathService> = OnceLock::new();

/// Lowest-priority runtime overlay; session Environment and explicit tool env win.
pub(crate) fn environment() -> HashMap<String, String> {
    EFFECTIVE
        .read()
        .ok()
        .and_then(|value| value.clone())
        .map(|value| HashMap::from([("PATH".to_owned(), value)]))
        .unwrap_or_default()
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default)]
struct Saved {
    command: String,
    revision: u64,
    last_good_path: Option<String>,
    resolved_command: Option<String>,
    resolved_at: Option<String>,
}
struct State {
    saved: Saved,
    source: String,
    effective: Option<String>,
    error: Option<String>,
    load_failed: bool,
    receipts: HashMap<String, (String, api::MachinePathState)>,
}
#[derive(Clone)]
pub(crate) struct MachinePathService {
    file: PathBuf,
    inherited: Option<String>,
    state: Arc<Mutex<State>>,
}
impl MachinePathService {
    async fn load(file: PathBuf) -> Self {
        let (saved, error) = match tokio::fs::read(&file).await {
            Ok(bytes) => match serde_json::from_slice::<Saved>(&bytes) {
                Ok(saved) => (saved, None),
                Err(error) => (
                    Saved::default(),
                    Some(format!("Cannot read saved PATH: {error}")),
                ),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (Saved::default(), None),
            Err(error) => (
                Saved::default(),
                Some(format!("Cannot read saved PATH: {error}")),
            ),
        };
        let inherited = std::env::var("PATH").ok();
        let last_good = saved
            .last_good_path
            .clone()
            .filter(|value| valid_path(value));
        let effective = if saved.command.is_empty() {
            inherited.clone()
        } else {
            last_good.clone().or_else(|| inherited.clone())
        };
        Self {
            file,
            inherited,
            state: Arc::new(Mutex::new(State {
                source: if !saved.command.is_empty() && last_good.is_some() {
                    "last_good"
                } else {
                    "inherited"
                }
                .to_owned(),
                effective,
                load_failed: error.is_some(),
                error,
                saved,
                receipts: HashMap::new(),
            })),
        }
    }
    async fn persist(&self, saved: &Saved) -> Result<(), Status> {
        let parent = self
            .file
            .parent()
            .ok_or_else(|| Status::internal("PATH settings directory missing"))?;
        tokio::fs::create_dir_all(parent).await.map_err(internal)?;
        let temporary = self
            .file
            .with_extension(format!("{}.tmp", uuid::Uuid::now_v7()));
        let bytes = serde_json::to_vec(saved).map_err(internal)?;
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary).await.map_err(internal)?;
        let result = async {
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            tokio::fs::rename(&temporary, &self.file).await
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result.map_err(internal)
    }
    fn view(state: &State) -> api::MachinePathState {
        api::MachinePathState {
            command: state.saved.command.clone(),
            revision: state.saved.revision,
            effective_path: state.effective.clone(),
            source: state.source.clone(),
            resolved_command: state.saved.resolved_command.clone(),
            resolved_at: state.saved.resolved_at.clone(),
            error: state.error.clone(),
        }
    }
    async fn resolve(&self, state: &mut State, timeout: Duration) {
        if state.saved.command.is_empty() {
            return;
        }
        match resolve_command(&state.saved.command, self.inherited.as_deref(), timeout).await {
            Ok(path) => {
                let mut saved = state.saved.clone();
                saved.last_good_path = Some(path.clone());
                saved.resolved_command = Some(saved.command.clone());
                saved.resolved_at = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                        .to_string(),
                );
                match self.persist(&saved).await {
                    Ok(()) => {
                        state.saved = saved;
                        state.effective = Some(path);
                        "resolved".clone_into(&mut state.source);
                        state.error = None;
                    }
                    Err(error) => state.error = Some(error.message().to_owned()),
                }
            }
            Err(error) => state.error = Some(error),
        }
    }
    async fn change(
        &self,
        key: String,
        revision: u64,
        command: Option<String>,
    ) -> Result<api::MachinePathState, Status> {
        if key.is_empty() || key.len() > 128 {
            return Err(Status::invalid_argument(
                "idempotency key required (up to 128 bytes)",
            ));
        }
        if command
            .as_ref()
            .is_some_and(|value| value.len() > 16 * 1024 || value.contains('\0'))
        {
            return Err(Status::invalid_argument(
                "command must be at most 16 KiB without NUL",
            ));
        }
        let identity = format!("{revision}:{command:?}");
        let mut state = self.state.lock().await;
        if let Some((previous, result)) = state.receipts.get(&key) {
            return if previous == &identity {
                Ok(result.clone())
            } else {
                Err(Status::already_exists(
                    "idempotency key reused for different input",
                ))
            };
        }
        if state.load_failed {
            return Err(Status::failed_precondition(
                "Repair unreadable machine PATH settings before writing",
            ));
        }
        if state.saved.revision != revision {
            return Err(Status::aborted(
                "PATH settings changed; reload before retrying",
            ));
        }
        if let Some(command) = command {
            let mut saved = state.saved.clone();
            saved.command = command;
            saved.revision = saved
                .revision
                .checked_add(1)
                .ok_or_else(|| Status::internal("PATH revision exhausted"))?;
            self.persist(&saved).await?;
            state.saved = saved;
            if state.saved.command.is_empty() {
                state.effective.clone_from(&self.inherited);
                "inherited".clone_into(&mut state.source);
                state.error = None;
            }
        } else {
            if state.saved.command.is_empty() {
                return Err(Status::failed_precondition(
                    "Save a PATH command before Sync",
                ));
            }
            self.resolve(&mut state, Duration::from_secs(10)).await;
        }
        // Never mutate the service process's global environment.
        if SERVICE
            .get()
            .is_some_and(|service| Arc::ptr_eq(&service.state, &self.state))
            && let Ok(mut effective) = EFFECTIVE.write()
        {
            *effective = if state.saved.command.is_empty() {
                None
            } else {
                state.effective.clone()
            };
        }
        let view = Self::view(&state);
        if state.receipts.len() >= 128 {
            state.receipts.clear();
        }
        state.receipts.insert(key, (identity, view.clone()));
        Ok(view)
    }
    pub(crate) fn into_server(
        self,
    ) -> api::machine_path_service_server::MachinePathServiceServer<Self> {
        api::machine_path_service_server::MachinePathServiceServer::new(self)
    }
    pub(crate) fn authenticated(
        self,
        key: String,
    ) -> tonic::service::interceptor::InterceptedService<
        api::machine_path_service_server::MachinePathServiceServer<Self>,
        nakode_server::grpc::ApiKeyInterceptor,
    > {
        tonic::service::interceptor::InterceptedService::new(
            self.into_server(),
            nakode_server::grpc::ApiKeyInterceptor::new(key),
        )
    }
}
fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("Machine PATH persistence failed: {error}"))
}
fn valid_path(value: &str) -> bool {
    !value.trim().is_empty() && !value.contains(['\n', '\r', '\0']) && value.len() <= LIMIT
}

#[cfg(unix)]
// Kill descendants even if the parent exits leaving pipes open or the request is cancelled.
struct Group(Option<u32>);
#[cfg(unix)]
impl Drop for Group {
    fn drop(&mut self) {
        if let Some(id) = self.0.and_then(|id| i32::try_from(id).ok()) {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(id),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

#[cfg(unix)]
async fn resolve_command(
    command: &str,
    inherited: Option<&str>,
    timeout: Duration,
) -> Result<String, String> {
    use std::{os::unix::process::CommandExt, process::Stdio};
    let mut process = tokio::process::Command::new("/bin/sh");
    process
        .args(["-c", command])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    process.current_dir(std::env::var_os("HOME").map_or_else(|| PathBuf::from("/"), PathBuf::from));
    if let Some(path) = inherited {
        process.env("PATH", path);
    } else {
        process.env_remove("PATH");
    }
    process.as_std_mut().process_group(0);
    let mut child = process
        .spawn()
        .map_err(|error| format!("PATH command could not start: {error}"))?;
    let _group = Group(child.id());
    let stdout = child.stdout.take().ok_or("PATH stdout unavailable")?;
    let stderr = child.stderr.take().ok_or("PATH stderr unavailable")?;
    let result = tokio::time::timeout(timeout, async {
        let (stdout, stderr, status) = tokio::try_join!(bounded(stdout), bounded(stderr), async {
            child.wait().await.map_err(|error| error.to_string())
        })?;
        if !status.success() {
            return Err(format!(
                "PATH command exited with {status}: {}",
                String::from_utf8_lossy(&stderr)
            ));
        }
        let text = String::from_utf8(stdout).map_err(|_| "PATH must be UTF-8".to_owned())?;
        let value = text
            .strip_suffix("\r\n")
            .or_else(|| text.strip_suffix('\n'))
            .unwrap_or(&text);
        if !valid_path(value) {
            return Err(
                "PATH must be a nonempty single line (one trailing newline is allowed)".to_owned(),
            );
        }
        Ok(value.to_owned())
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => Err("PATH command timed out after the resolution deadline".to_owned()),
    }
}
#[cfg(not(unix))]
async fn resolve_command(
    _command: &str,
    _inherited: Option<&str>,
    _timeout: Duration,
) -> Result<String, String> {
    Err("Machine PATH command resolution is currently supported on Unix services only".to_owned())
}
async fn bounded(stream: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    stream
        .take((LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    if bytes.len() > LIMIT {
        return Err("PATH command output exceeded 64 KiB".to_owned());
    }
    Ok(bytes)
}
pub(crate) async fn initialize() -> Result<(), String> {
    let service = MachinePathService::load(
        crate::config::nakode_home()
            .map_err(|error| error.to_string())?
            .join("machine-path.json"),
    )
    .await;
    {
        let mut state = service.state.lock().await;
        if !state.load_failed {
            service.resolve(&mut state, Duration::from_secs(10)).await;
        }
        if !state.saved.command.is_empty()
            && let Ok(mut effective) = EFFECTIVE.write()
        {
            effective.clone_from(&state.effective);
        }
    }
    SERVICE
        .set(service)
        .map_err(|_| "Machine PATH already initialized".to_owned())
}
pub(crate) fn service() -> MachinePathService {
    SERVICE
        .get()
        .expect("PATH initialized before listeners")
        .clone()
}
#[tonic::async_trait]
impl api::machine_path_service_server::MachinePathService for MachinePathService {
    async fn get_machine_path(
        &self,
        _: Request<api::GetMachinePathRequest>,
    ) -> Result<Response<api::MachinePathState>, Status> {
        Ok(Response::new(Self::view(&*self.state.lock().await)))
    }
    async fn save_machine_path(
        &self,
        request: Request<api::SaveMachinePathRequest>,
    ) -> Result<Response<api::MachinePathState>, Status> {
        let input = request.into_inner();
        Ok(Response::new(
            self.change(
                input.idempotency_key,
                input.expected_revision,
                Some(input.command),
            )
            .await?,
        ))
    }
    async fn sync_machine_path(
        &self,
        request: Request<api::SyncMachinePathRequest>,
    ) -> Result<Response<api::MachinePathState>, Status> {
        let input = request.into_inner();
        Ok(Response::new(
            self.change(input.idempotency_key, input.expected_revision, None)
                .await?,
        ))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use api::machine_path_service_server::MachinePathService as _;
    fn root() -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".tmp")
            .join(format!("machine-path-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }
    #[tokio::test]
    async fn validation_timeout_and_output_bounds() {
        let timeout = Duration::from_secs(2);
        assert_eq!(
            resolve_command("printf '/usr/bin:/bin\\n'", Some("/bin"), timeout)
                .await
                .unwrap(),
            "/usr/bin:/bin"
        );
        for command in [
            "printf ''",
            "printf 'a\\nb\\n'",
            "printf 'a\\n\\n'",
            "printf 'a\\000b'",
            "exit 3",
        ] {
            assert!(
                resolve_command(command, Some("/bin"), timeout)
                    .await
                    .is_err(),
                "{command}"
            );
        }
        assert!(
            resolve_command("sleep 5", Some("/bin"), Duration::from_millis(50))
                .await
                .unwrap_err()
                .contains("timed out")
        );
        assert!(
            resolve_command(
                "(sleep 5) & printf /bin",
                Some("/bin"),
                Duration::from_millis(50)
            )
            .await
            .is_err()
        );
        assert!(
            resolve_command(
                "while :; do printf '1234567890'; done",
                Some("/bin"),
                timeout
            )
            .await
            .unwrap_err()
            .contains("64 KiB")
        );
    }
    #[tokio::test]
    async fn save_read_target_isolation_sync_and_startup_fallback() {
        let root = root();
        let service = MachinePathService::load(root.join("one.json")).await;
        let other = MachinePathService::load(root.join("two.json")).await;
        let marker = root.join("executed");
        let command = format!(
            "printf x > '{}'; printf '/custom/bin:/bin\\n'",
            marker.display()
        );
        let saved = service
            .change("save".into(), 0, Some(command.clone()))
            .await
            .unwrap();
        assert_eq!(saved.command, command);
        assert!(!marker.exists());
        let read = service
            .get_machine_path(Request::new(api::GetMachinePathRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(read.command, command);
        assert!(!marker.exists());
        assert!(other.state.lock().await.saved.command.is_empty());
        assert!(
            service
                .change("conflict".into(), 0, Some("exit 1".into()))
                .await
                .is_err()
        );
        let synced = service.change("sync".into(), 1, None).await.unwrap();
        assert!(marker.exists());
        assert_eq!(synced.effective_path.as_deref(), Some("/custom/bin:/bin"));
        assert!(synced.error.is_none());
        assert_eq!(
            service.change("sync".into(), 1, None).await.unwrap(),
            synced
        );
        service
            .change("bad-save".into(), 1, Some("exit 1".into()))
            .await
            .unwrap();
        let failed = service.change("bad-sync".into(), 2, None).await.unwrap();
        assert!(failed.error.is_some());
        assert_eq!(failed.effective_path, synced.effective_path);
        let reloaded = MachinePathService::load(root.join("one.json")).await;
        let mut state = reloaded.state.lock().await;
        reloaded.resolve(&mut state, Duration::from_secs(1)).await;
        assert_eq!(state.effective, synced.effective_path);
        assert_eq!(state.source, "last_good");
        assert!(state.error.is_some());
        drop(state);
        let disabled = reloaded
            .change("disable".into(), 2, Some(String::new()))
            .await
            .unwrap();
        assert_eq!(disabled.source, "inherited");
    }
    #[tokio::test]
    async fn public_service_requires_owner_key_and_get_never_executes() {
        let root = root();
        let service = MachinePathService::load(root.join("settings.json")).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.authenticated("owner-key".into()))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let mut client = api::machine_path_service_client::MachinePathServiceClient::connect(
            format!("http://{address}"),
        )
        .await
        .unwrap();
        let denied = client
            .get_machine_path(api::GetMachinePathRequest {})
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::Unauthenticated);
        let marker = root.join("must-not-exist");
        let mut save = Request::new(api::SaveMachinePathRequest {
            command: format!("printf touched > '{}'; printf /bin", marker.display()),
            expected_revision: 0,
            idempotency_key: "save".into(),
        });
        save.metadata_mut()
            .insert("authorization", "Bearer owner-key".parse().unwrap());
        let saved = client.save_machine_path(save).await.unwrap().into_inner();
        assert_eq!(saved.revision, 1);
        let mut read = Request::new(api::GetMachinePathRequest {});
        read.metadata_mut()
            .insert("authorization", "Bearer owner-key".parse().unwrap());
        assert_eq!(
            client
                .get_machine_path(read)
                .await
                .unwrap()
                .into_inner()
                .command,
            saved.command
        );
        assert!(!marker.exists());
        let _ = stop.send(());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn corrupt_settings_fail_closed_without_overwrite() {
        let file = root().join("bad.json");
        tokio::fs::write(&file, b"bad").await.unwrap();
        let service = MachinePathService::load(file.clone()).await;
        assert!(service.state.lock().await.error.is_some());
        assert!(
            service
                .change("save".into(), 0, Some("printf /bin".into()))
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read(file).await.unwrap(), b"bad");
    }
}
