use std::{
    env,
    path::Path,
    process::Stdio,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_CALL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryBackend {
    #[default]
    Disabled,
    Mnemosyne,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryScope {
    Project,
    Global,
}

impl MemoryScope {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Global => "global",
        }
    }
}

impl MemoryBackend {
    pub const ALL: [Self; 2] = [Self::Disabled, Self::Mnemosyne];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::Mnemosyne => "Mnemosyne",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryConfig {
    pub backend: MemoryBackend,
    pub executable: String,
    pub global_bank: String,
    pub data_directory: String,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            backend: MemoryBackend::Disabled,
            executable: "mnemosyne".to_owned(),
            global_bank: "nakode-global".to_owned(),
            data_directory: String::new(),
        }
    }
}

impl MemoryConfig {
    #[must_use]
    pub fn configured(&self) -> bool {
        self.backend == MemoryBackend::Mnemosyne
            && !self.executable.trim().is_empty()
            && valid_bank_name(self.global_bank.trim())
    }

    #[must_use]
    pub fn available(&self) -> bool {
        self.configured() && executable_available(self.executable.trim())
    }
}

#[must_use]
pub fn project_bank(workspace: &Path) -> String {
    let canonical = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.to_path_buf());
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in canonical.to_string_lossy().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let name = workspace
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace")
        .chars()
        .take(39)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("nakode-{name}-{hash:016x}")
}

fn memory_data_directory(configured: &str) -> Option<std::path::PathBuf> {
    let configured = configured.trim();
    if !configured.is_empty() {
        return Some(configured.into());
    }
    crate::config::nakode_home().ok()
}

fn valid_bank_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 64
        && name
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '-' | '_'))
}

#[must_use]
pub fn executable_available(executable: &str) -> bool {
    if executable.is_empty() {
        return false;
    }
    let path = Path::new(executable);
    if path.components().count() > 1 {
        return executable_file(path);
    }
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| executable_in_directory(&directory, executable))
    })
}

fn executable_in_directory(directory: &Path, executable: &str) -> bool {
    let candidate = directory.join(executable);
    if executable_file(&candidate) {
        return true;
    }
    #[cfg(windows)]
    {
        return env::var_os("PATHEXT").is_some_and(|extensions| {
            extensions.to_string_lossy().split(';').any(|extension| {
                executable_file(&directory.join(format!("{executable}{extension}")))
            })
        });
    }
    #[cfg(not(windows))]
    false
}

#[cfg(unix)]
fn executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn executable_file(path: &Path) -> bool {
    path.is_file()
}

pub type SharedMemoryService = Arc<MemoryService>;

pub struct MemoryService {
    config: Arc<RwLock<MemoryConfig>>,
    project_bank: String,
    project_process: Mutex<Option<McpProcess>>,
    global_process: Mutex<Option<McpProcess>>,
    operational: AtomicBool,
}

impl MemoryService {
    #[must_use]
    pub fn new(config: Arc<RwLock<MemoryConfig>>, project_bank: String) -> Self {
        Self {
            config,
            project_bank,
            project_process: Mutex::new(None),
            global_process: Mutex::new(None),
            operational: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub fn available(&self) -> bool {
        self.operational.load(Ordering::Relaxed)
            && self.config.read().is_ok_and(|config| config.available())
    }

    /// Stops the managed Mnemosyne process, if one is running.
    pub async fn reset(&self) {
        self.operational.store(true, Ordering::Relaxed);
        if let Some(mut process) = self.project_process.lock().await.take() {
            process.stop().await;
        }
        if let Some(mut process) = self.global_process.lock().await.take() {
            process.stop().await;
        }
    }

    /// Calls one supported Mnemosyne MCP tool.
    ///
    /// # Errors
    /// Returns an error when configuration is unavailable, the child process cannot be
    /// started, MCP negotiation fails, the operation times out, or Mnemosyne reports an error.
    pub async fn call(
        &self,
        scope: MemoryScope,
        tool: &str,
        arguments: Value,
        cancellation: &CancellationToken,
    ) -> Result<String, MemoryError> {
        let config = self
            .config
            .read()
            .map_err(|_| MemoryError::Configuration("memory configuration lock poisoned".into()))?
            .clone();
        if !config.available() {
            return Err(MemoryError::Unavailable);
        }

        let bank = match scope {
            MemoryScope::Project => self.project_bank.clone(),
            MemoryScope::Global => config.global_bank.clone(),
        };
        let process_slot = match scope {
            MemoryScope::Project => &self.project_process,
            MemoryScope::Global => &self.global_process,
        };
        let mut process = process_slot.lock().await;
        let needs_restart = process
            .as_ref()
            .is_some_and(|running| running.config != config || running.bank != bank);
        if needs_restart && let Some(mut running) = process.take() {
            running.stop().await;
        }
        if process.is_none() {
            let started = tokio::select! {
                () = cancellation.cancelled() => Err(MemoryError::Cancelled),
                result = tokio::time::timeout(MCP_CALL_TIMEOUT, McpProcess::start(config, bank)) => {
                    result.map_err(|_| MemoryError::Timeout).and_then(|result| result)
                }
            };
            match started {
                Ok(started) => *process = Some(started),
                Err(error) => {
                    if error.provider_failure() {
                        self.operational.store(false, Ordering::Relaxed);
                    }
                    return Err(error);
                }
            }
        }

        let Some(running) = process.as_mut() else {
            return Err(MemoryError::Protocol(
                "Mnemosyne MCP process was not initialized".into(),
            ));
        };
        let operation = running.call_tool(tool, arguments);
        let result = tokio::select! {
            () = cancellation.cancelled() => Err(MemoryError::Cancelled),
            result = tokio::time::timeout(MCP_CALL_TIMEOUT, operation) => {
                result.map_err(|_| MemoryError::Timeout).and_then(|result| result)
            }
        };
        if result.as_ref().is_err_and(MemoryError::provider_failure) {
            self.operational.store(false, Ordering::Relaxed);
        }
        if result.is_err()
            && let Some(mut running) = process.take()
        {
            running.stop().await;
        }
        result
    }
}

struct McpProcess {
    config: MemoryConfig,
    bank: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpProcess {
    async fn start(config: MemoryConfig, bank: String) -> Result<Self, MemoryError> {
        let mut command = Command::new(config.executable.trim());
        command
            .args(["mcp", "--bank", bank.trim()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(data_directory) = memory_data_directory(&config.data_directory) {
            command.env("MNEMOSYNE_DATA_DIR", data_directory);
        }
        let mut child = command.spawn().map_err(MemoryError::Spawn)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(MemoryError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(MemoryError::MissingPipe("stdout"))?;
        let mut process = Self {
            config,
            bank,
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        };
        let response = process
            .request(
                "initialize",
                json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "nakode", "version": env!("CARGO_PKG_VERSION")}
                }),
            )
            .await?;
        if response
            .get("protocolVersion")
            .and_then(Value::as_str)
            .is_none()
        {
            return Err(MemoryError::Protocol(
                "Mnemosyne returned an invalid initialize response".into(),
            ));
        }
        process
            .notify("notifications/initialized", json!({}))
            .await?;
        Ok(process)
    }

    async fn call_tool(&mut self, tool: &str, arguments: Value) -> Result<String, MemoryError> {
        let result = self
            .request("tools/call", json!({"name": tool, "arguments": arguments}))
            .await?;
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            return Err(MemoryError::Tool(content_text(&result)));
        }
        Ok(content_text(&result))
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, MemoryError> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await?;
        loop {
            let message = self.read_message().await?;
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(MemoryError::Protocol(format_mcp_error(error)));
            }
            return message.get("result").cloned().ok_or_else(|| {
                MemoryError::Protocol("MCP response omitted both result and error".into())
            });
        }
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), MemoryError> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await
    }

    async fn write_message(&mut self, message: &Value) -> Result<(), MemoryError> {
        let mut encoded = serde_json::to_vec(message).map_err(MemoryError::Json)?;
        encoded.push(b'\n');
        self.stdin
            .write_all(&encoded)
            .await
            .map_err(MemoryError::Io)?;
        self.stdin.flush().await.map_err(MemoryError::Io)
    }

    async fn read_message(&mut self) -> Result<Value, MemoryError> {
        let mut line = String::new();
        loop {
            line.clear();
            let count = self
                .stdout
                .read_line(&mut line)
                .await
                .map_err(MemoryError::Io)?;
            if count == 0 {
                let status = self.child.try_wait().ok().flatten();
                return Err(MemoryError::Protocol(format!(
                    "Mnemosyne MCP process closed stdout{}",
                    status.map_or_else(String::new, |status| format!(" ({status})"))
                )));
            }
            if line.trim().is_empty() {
                continue;
            }
            return serde_json::from_str(line.trim()).map_err(MemoryError::Json);
        }
    }

    async fn stop(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

fn content_text(result: &Value) -> String {
    let parts = result
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        result.to_string()
    } else {
        parts.join("\n")
    }
}

fn format_mcp_error(error: &Value) -> String {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown MCP error");
    code.map_or_else(|| message.to_owned(), |code| format!("{message} ({code})"))
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("memory is disabled, incomplete, or the Mnemosyne executable is unavailable")]
    Unavailable,
    #[error("invalid memory configuration: {0}")]
    Configuration(String),
    #[error("failed to start Mnemosyne: {0}")]
    Spawn(std::io::Error),
    #[error("Mnemosyne child process did not expose {0}")]
    MissingPipe(&'static str),
    #[error("Mnemosyne I/O failed: {0}")]
    Io(std::io::Error),
    #[error("Mnemosyne returned invalid JSON: {0}")]
    Json(serde_json::Error),
    #[error("Mnemosyne MCP protocol error: {0}")]
    Protocol(String),
    #[error("Mnemosyne memory operation failed: {0}")]
    Tool(String),
    #[error("Mnemosyne memory operation timed out")]
    Timeout,
    #[error("Mnemosyne memory operation was cancelled")]
    Cancelled,
}

impl MemoryError {
    fn provider_failure(&self) -> bool {
        !matches!(self, Self::Tool(_) | Self::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryBackend, MemoryConfig, MemoryScope, memory_data_directory, project_bank};

    #[test]
    fn project_memory_banks_are_stable_and_workspace_isolated() {
        let root = tempfile::tempdir().expect("workspace root");
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::create_dir_all(&first).expect("first workspace");
        std::fs::create_dir_all(&second).expect("second workspace");

        assert_eq!(project_bank(&first), project_bank(&first));
        assert_ne!(project_bank(&first), project_bank(&second));
    }

    #[test]
    fn disabled_memory_is_not_configured() {
        assert!(!MemoryConfig::default().configured());
    }

    #[test]
    fn explicit_memory_data_directory_is_preserved() {
        assert_eq!(
            memory_data_directory(" /tmp/nakode-memory "),
            Some(std::path::PathBuf::from("/tmp/nakode-memory"))
        );
    }

    #[test]
    fn default_memory_data_directory_is_under_nakode_home() {
        let directory = memory_data_directory("").expect("home directory");
        let configured_home = std::env::var_os("NAKODE_HOME").map(std::path::PathBuf::from);
        if let Some(configured_home) = configured_home {
            assert_eq!(directory, configured_home);
        } else {
            assert_eq!(
                directory.file_name().and_then(|name| name.to_str()),
                Some(".nakode")
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn supervised_stdio_client_negotiates_and_calls_mnemosyne() {
        use std::{
            fs,
            os::unix::fs::PermissionsExt as _,
            sync::{Arc, RwLock},
        };

        let directory = tempfile::tempdir().expect("tempdir");
        let executable = directory.path().join("mnemosyne");
        fs::write(
            &executable,
            r#"#!/bin/sh
printf '%s\n' "$3" >> "$(dirname "$0")/banks"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fake","version":"1"}}}'
      ;;
    *'"method":"tools/call"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"memory response"}]}}'
      ;;
  esac
done
"#,
        )
        .expect("write fake executable");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
            .expect("make fake executable runnable");
        let config = MemoryConfig {
            backend: MemoryBackend::Mnemosyne,
            executable: executable.to_string_lossy().into_owned(),
            global_bank: "test-global".into(),
            data_directory: directory.path().join("data").to_string_lossy().into_owned(),
        };
        let service =
            super::MemoryService::new(Arc::new(RwLock::new(config)), "test-project".into());
        let result = service
            .call(
                MemoryScope::Project,
                "mnemosyne_recall",
                serde_json::json!({"query": "decision"}),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
            .expect("MCP call succeeds");
        assert_eq!(result, "memory response");
        let global = service
            .call(
                MemoryScope::Global,
                "mnemosyne_recall",
                serde_json::json!({"query": "preference"}),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
            .expect("global MCP call succeeds");
        assert_eq!(global, "memory response");
        let banks = fs::read_to_string(directory.path().join("banks")).expect("bank log");
        assert!(banks.lines().any(|bank| bank == "test-project"));
        assert!(banks.lines().any(|bank| bank == "test-global"));
    }

    #[cfg(unix)]
    #[test]
    fn memory_tools_are_gated_by_selection_and_executable_availability() {
        use std::{
            fs,
            os::unix::fs::PermissionsExt as _,
            sync::{Arc, RwLock},
        };

        let directory = tempfile::tempdir().expect("tempdir");
        let executable = directory.path().join("mnemosyne");
        fs::write(&executable, "#!/bin/sh\n").expect("write executable");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
            .expect("make executable runnable");
        let config = Arc::new(RwLock::new(MemoryConfig::default()));
        let service = Arc::new(super::MemoryService::new(
            Arc::clone(&config),
            "test-project".into(),
        ));
        let registry = crate::tools::ToolRegistry::base().with_memory(service);
        assert!(
            registry
                .definitions()
                .iter()
                .all(|definition| !definition.name.starts_with("memory_"))
        );

        *config.write().expect("config lock") = MemoryConfig {
            backend: MemoryBackend::Mnemosyne,
            executable: executable.to_string_lossy().into_owned(),
            global_bank: "test-global".into(),
            data_directory: String::new(),
        };
        let definitions = registry.definitions();
        let search = definitions
            .iter()
            .find(|definition| definition.name == "memory_search")
            .expect("search definition");
        assert_eq!(search.parameters["properties"]["scope"]["default"], "all");
        let store = definitions
            .iter()
            .find(|definition| definition.name == "memory_store")
            .expect("store definition");
        assert!(
            store.parameters["required"]
                .as_array()
                .is_some_and(|required| required.contains(&serde_json::json!("scope")))
        );
        let names = definitions
            .into_iter()
            .map(|definition| definition.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"memory_search"));
        assert!(names.contains(&"memory_store"));

        config.write().expect("config lock").global_bank.clear();
        assert!(
            registry
                .definitions()
                .iter()
                .all(|definition| !definition.name.starts_with("memory_"))
        );
    }

    #[test]
    fn mnemosyne_requires_an_executable_and_global_bank() {
        let mut config = MemoryConfig {
            backend: MemoryBackend::Mnemosyne,
            ..MemoryConfig::default()
        };
        assert!(config.configured());
        config.global_bank.clear();
        assert!(!config.configured());
        config.global_bank = "invalid bank".into();
        assert!(!config.configured());
        config.global_bank = "user-memory".into();
        assert!(config.configured());
        config.executable.clear();
        assert!(!config.configured());
    }
}
