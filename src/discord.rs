#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::result_large_err,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

//! Discord frontend adapter.
//!
//! This module deliberately talks to Nakode only through [`nakode_sdk::NakodeClient`].
//! It owns Discord configuration, gateway handling, rendering, and attachment
//! conversion; the native server remains authoritative for sessions, prompts,
//! interactions, and transcript snapshots.

use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use directories::ProjectDirs;
use futures_util::{StreamExt, TryStreamExt, future::BoxFuture, future::FutureExt};
use nakode_sdk::{HydratedSession, NakodeClient, SdkError, v1 as api};
use reqwest::Client as HttpClient;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serenity::{
    all::{
        ChannelId, Context, CreateAllowedMentions, CreateMessage, CreateThread, EditMessage,
        EventHandler, GatewayIntents, Message, MessageId, Ready, UserId,
    },
    async_trait,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::{
    config::{Config, DiscordAction},
    control_service::{
        ServicePaths, TransportAction, TransportController, TransportStatus, TransportSupervisor,
    },
};

const CONFIG_VERSION: u32 = 2;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES: usize = 30 * 1024 * 1024;
const DISCORD_MESSAGE_LIMIT: usize = 2_000;
const DISCORD_CHUNK_SIZE: usize = DISCORD_MESSAGE_LIMIT - 100;
const SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(500);
const RECONCILE_RETRY_DELAY: Duration = Duration::from_secs(2);
const BRIDGE_RPC_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_INBOUND_INFLIGHT: usize = 16;
const MAX_ACTIVE_MULTIPART_ASSEMBLIES: usize = 32;
const MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION: usize = 1;
const MAX_NONCE_SEARCH_PAGES: usize = 64;
const MULTIPART_TTL: Duration = Duration::from_secs(30 * 60);
const TRANSPORT_NAME: &str = "discord";
const REACTION_ACCEPTED: &str = "🔄";
const REACTION_LIVE: &str = "🟡";
const REACTION_COMPLETED: &str = "✅";
const REACTION_FAILED: &str = "⚠️";
const REACTION_BUSY: &str = "❌";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn default_config_version() -> u32 {
    CONFIG_VERSION
}

/// Persisted system configuration for the Discord orchestrator bridge.
///
/// The bot token is intentionally absent and remains in the private token file managed by
/// [`DiscordConfigStore`]. Thread/session mappings are authoritative Nakode state and are never
/// duplicated in this file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    pub enabled: bool,
    pub chat_channel_id: Option<String>,
    pub agent_channel_id: Option<String>,
    pub primary_user_id: Option<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            enabled: false,
            chat_channel_id: None,
            agent_channel_id: None,
            primary_user_id: None,
        }
    }
}

impl DiscordConfig {
    /// Validates persisted configuration without contacting Discord or the Nakode server.
    pub fn validate(&self) -> Result<(), DiscordError> {
        if self.version != CONFIG_VERSION {
            return Err(DiscordError::InvalidConfig(format!(
                "unsupported Discord configuration version {}; rerun `nakode transport discord setup`",
                self.version
            )));
        }
        for (field, value) in [
            ("chat_channel_id", self.chat_channel_id.as_deref()),
            ("agent_channel_id", self.agent_channel_id.as_deref()),
            ("primary_user_id", self.primary_user_id.as_deref()),
        ] {
            if let Some(value) = value {
                validate_snowflake(field, value)?;
            }
        }
        if self.enabled {
            let chat = self.chat_channel_id.as_deref().ok_or_else(|| {
                DiscordError::InvalidConfig(
                    "chat_channel_id is required when Discord is enabled".to_owned(),
                )
            })?;
            let agent = self.agent_channel_id.as_deref().ok_or_else(|| {
                DiscordError::InvalidConfig(
                    "agent_channel_id is required when Discord is enabled".to_owned(),
                )
            })?;
            if self.primary_user_id.is_none() {
                return Err(DiscordError::InvalidConfig(
                    "primary_user_id is required when Discord is enabled".to_owned(),
                ));
            }
            if chat == agent {
                return Err(DiscordError::InvalidConfig(
                    "Chat and Agent orchestrators require different parent channels".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn parent_channel(&self, kind: api::OrchestratorKind) -> Option<ChannelId> {
        let value = match kind {
            api::OrchestratorKind::Chat => self.chat_channel_id.as_deref(),
            api::OrchestratorKind::Agent => self.agent_channel_id.as_deref(),
            api::OrchestratorKind::Unspecified => None,
        }?;
        value.parse::<u64>().ok().map(ChannelId::new)
    }

    fn is_primary_user(&self, user_id: UserId) -> bool {
        self.primary_user_id
            .as_deref()
            .is_some_and(|configured| configured == user_id.get().to_string())
    }
}

fn validate_snowflake(field: &str, value: &str) -> Result<u64, DiscordError> {
    let value = value.trim();
    let parsed = value.parse::<u64>().map_err(|_| DiscordError::InvalidId {
        field: field.to_owned(),
    })?;
    if parsed == 0 {
        return Err(DiscordError::InvalidId {
            field: field.to_owned(),
        });
    }
    Ok(parsed)
}

/// Errors returned by Discord configuration and runtime operations.
#[derive(Debug, Error)]
pub enum DiscordError {
    #[error("could not determine Nakode's application-data directory")]
    MissingDataDirectory,
    #[error("Discord configuration I/O error at {path}: {source}")]
    Io { path: String, source: io::Error },
    #[error("invalid Discord configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid Discord {field} snowflake")]
    InvalidId { field: String },
    #[error("invalid Discord configuration TOML")]
    Toml(
        #[source]
        #[from]
        toml::de::Error,
    ),
    #[error("could not serialize Discord configuration: {0}")]
    TomlSerialize(#[from] toml::ser::Error),
    #[error("Discord bot token is not configured")]
    MissingToken,
    #[error("Discord bot token is too large (maximum {MAX_TOKEN_BYTES} bytes)")]
    TokenTooLarge,
    #[error("Discord gateway error: {0}")]
    Gateway(#[from] serenity::Error),
    #[error("Nakode SDK error: {0}")]
    Sdk(#[from] SdkError),
    #[error("Discord attachment download failed")]
    Http(#[from] reqwest::Error),
    #[error("Discord attachment {name:?} exceeds the {MAX_ATTACHMENT_BYTES} byte limit")]
    AttachmentTooLarge { name: String },
    #[error(
        "combined Discord prompt attachments exceed the {MAX_TOTAL_ATTACHMENT_BYTES} byte limit"
    )]
    CombinedAttachmentsTooLarge,
    #[error("Discord attachment {name:?} is not a supported HTTPS image")]
    UnsupportedAttachment { name: String },
    #[error("Discord durable ingress store failed")]
    IngressStore(#[source] rusqlite::Error),
    #[error("Discord durable ingress payload is invalid")]
    IngressPayload(#[source] serde_json::Error),
    #[error("Discord delivery cursor is outside the bounded hydrated transcript")]
    DeliveryCursorUnavailable,
    #[error("the workspace service is not running; run `nakode start` first")]
    ServiceNotRunning,
    #[error("Discord transport control failed: {0}")]
    Control(#[from] crate::control_service::ControlError),
    #[error("Discord setup input failed: {0}")]
    SetupInput(#[source] io::Error),
}

/// Private system configuration plus workspace-scoped transport state.
/// Credentials and channel/user snowflakes are installation-level. Durable ingress/recovery files
/// remain workspace-hashed so independent Nakode authorities never consume each other's work.
#[derive(Clone, Debug)]
pub struct DiscordConfigStore {
    configuration_directory: PathBuf,
    directory: PathBuf,
}

impl DiscordConfigStore {
    /// Opens the private store associated with one canonical workspace.
    pub fn for_workspace(workspace: &Path) -> Result<Self, DiscordError> {
        let root = if let Some(configured) = std::env::var_os("NAKODE_DISCORD_DIR") {
            PathBuf::from(configured)
        } else {
            ProjectDirs::from("dev", "nakode", "Nakode")
                .map(|project| project.data_local_dir().join("discord"))
                .ok_or(DiscordError::MissingDataDirectory)?
        };
        Self::from_root(workspace, &root)
    }

    fn from_root(workspace: &Path, root: &Path) -> Result<Self, DiscordError> {
        let workspace = workspace
            .canonicalize()
            .map_err(|source| io_error(workspace, source))?;
        let digest = Sha256::digest(workspace.to_string_lossy().as_bytes());
        let mut key = String::with_capacity(32);
        for byte in &digest[..16] {
            key.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
            key.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
        }
        prepare_private_directory(root)?;
        let workspace_root = root.join("workspaces");
        prepare_private_directory(&workspace_root)?;
        let directory = workspace_root.join(key);
        prepare_private_directory(&directory)?;
        Ok(Self {
            configuration_directory: root.to_path_buf(),
            directory,
        })
    }

    #[must_use]
    pub fn config_path(&self) -> PathBuf {
        self.configuration_directory.join("discord.toml")
    }

    #[must_use]
    pub fn token_path(&self) -> PathBuf {
        self.configuration_directory.join("discord.token")
    }

    /// Loads the public configuration. Missing configuration is equivalent to
    /// the disabled default.
    pub fn load(&self) -> Result<DiscordConfig, DiscordError> {
        let path = self.config_path();
        if !path.exists() {
            return Ok(DiscordConfig::default());
        }
        let source = std::fs::read_to_string(&path).map_err(|source| io_error(&path, source))?;
        let config: DiscordConfig = toml::from_str(&source)?;
        config.validate()?;
        Ok(config)
    }

    /// Atomically saves public configuration without the token.
    pub fn save(&self, config: &DiscordConfig) -> Result<(), DiscordError> {
        config.validate()?;
        let encoded = toml::to_string_pretty(config)?;
        atomic_write(&self.config_path(), format!("{encoded}\n").as_bytes())
    }

    /// Returns the token without ever including it in a debug representation
    /// or status response.
    pub fn read_token(&self) -> Result<String, DiscordError> {
        let path = self.token_path();
        if !path.exists() {
            return Err(DiscordError::MissingToken);
        }
        let source = std::fs::read_to_string(&path).map_err(|source| io_error(&path, source))?;
        if source.len() > MAX_TOKEN_BYTES {
            return Err(DiscordError::TokenTooLarge);
        }
        let token = source.trim().to_owned();
        if token.is_empty() {
            return Err(DiscordError::MissingToken);
        }
        Ok(token)
    }

    #[must_use]
    pub fn token_configured(&self) -> bool {
        self.token_path().is_file()
    }

    /// Replaces the token using a private, atomically renamed file.
    pub fn save_token(&self, token: &str) -> Result<(), DiscordError> {
        let token = token.trim();
        if token.is_empty() {
            return Err(DiscordError::MissingToken);
        }
        if token.len() > MAX_TOKEN_BYTES {
            return Err(DiscordError::TokenTooLarge);
        }
        atomic_write(&self.token_path(), format!("{token}\n").as_bytes())
    }

    /// Removes the token while retaining non-secret configuration.
    pub fn delete_token(&self) -> Result<(), DiscordError> {
        let path = self.token_path();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(io_error(&path, source)),
        }
    }
}

fn io_error(path: &Path, source: io::Error) -> DiscordError {
    DiscordError::Io {
        path: path.display().to_string(),
        source,
    }
}

fn prepare_private_directory(directory: &Path) -> Result<(), DiscordError> {
    std::fs::create_dir_all(directory).map_err(|source| io_error(directory, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
            .map_err(|source| io_error(directory, source))?;
    }
    Ok(())
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), DiscordError> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, contents).map_err(|source| io_error(&temporary, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(source) =
            std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
        {
            let _ = std::fs::remove_file(&temporary);
            return Err(io_error(&temporary, source));
        }
    }
    if let Err(source) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(io_error(path, source));
    }
    Ok(())
}

/// Handles a `nakode transport discord ...` command.
pub async fn run_command(config: &Config, action: DiscordAction) -> Result<(), DiscordError> {
    let store = DiscordConfigStore::for_workspace(&config.workspace)?;
    let paths = crate::control_service::ServicePaths::of(config)?;
    match action {
        DiscordAction::Setup {
            chat_channel_id,
            agent_channel_id,
            primary_user_id,
        } => {
            setup(&store, chat_channel_id, agent_channel_id, primary_user_id)?;
            start_or_reload(&paths, "Discord configuration applied").await?;
        }
        DiscordAction::Status { json } => status(&store, &paths, json).await?,
        DiscordAction::Enable => {
            set_enabled(&store, true)?;
            report_live_action(&paths, TransportAction::Start, "Discord frontend started").await?;
        }
        DiscordAction::Disable => {
            set_enabled(&store, false)?;
            report_live_action(&paths, TransportAction::Stop, "Discord frontend stopped").await?;
        }
        DiscordAction::Start => {
            report_required_action(&paths, TransportAction::Start, "Discord frontend started")
                .await?;
        }
        DiscordAction::Stop => {
            report_required_action(&paths, TransportAction::Stop, "Discord frontend stopped")
                .await?;
        }
        DiscordAction::Restart => {
            report_required_action(
                &paths,
                TransportAction::Restart,
                "Discord frontend restarted",
            )
            .await?;
        }
    }
    Ok(())
}

fn setup(
    store: &DiscordConfigStore,
    chat_channel_id: Option<String>,
    agent_channel_id: Option<String>,
    primary_user_id: Option<String>,
) -> Result<(), DiscordError> {
    let existing = store.load()?;
    let token = rpassword::prompt_password("Discord bot token (input hidden): ")
        .map_err(DiscordError::SetupInput)?;
    let chat_channel_id = match chat_channel_id {
        Some(channel_id) => channel_id,
        None => prompt_line(
            "Chat Orchestrator parent-channel ID",
            existing.chat_channel_id.as_deref().unwrap_or_default(),
            true,
        )?,
    };
    let agent_channel_id = match agent_channel_id {
        Some(channel_id) => channel_id,
        None => prompt_line(
            "Agent Orchestrator parent-channel ID",
            existing.agent_channel_id.as_deref().unwrap_or_default(),
            true,
        )?,
    };
    let primary_user_id = match primary_user_id {
        Some(user_id) => user_id,
        None => prompt_line(
            "Primary Discord user ID",
            existing.primary_user_id.as_deref().unwrap_or_default(),
            true,
        )?,
    };

    let next = DiscordConfig {
        version: CONFIG_VERSION,
        enabled: true,
        chat_channel_id: Some(chat_channel_id.trim().to_owned()),
        agent_channel_id: Some(agent_channel_id.trim().to_owned()),
        primary_user_id: Some(primary_user_id.trim().to_owned()),
    };
    next.validate()?;
    store.save_token(&token)?;
    store.save(&next)?;
    println!("Discord orchestrator bridge configured and enabled for automatic service startup.");
    Ok(())
}

async fn report_required_action(
    paths: &ServicePaths,
    action: TransportAction,
    message: &str,
) -> Result<(), DiscordError> {
    let status = match crate::control_service::transport_action(paths, "discord", action).await {
        Ok(status) => status,
        Err(error) if service_unavailable(&error) => return Err(DiscordError::ServiceNotRunning),
        Err(error) => return Err(error.into()),
    };
    println!(
        "{message}; native service remains running (transport running: {}).",
        status.running
    );
    Ok(())
}

async fn report_live_action(
    paths: &ServicePaths,
    action: TransportAction,
    message: &str,
) -> Result<(), DiscordError> {
    match crate::control_service::transport_action(paths, "discord", action).await {
        Ok(status) => {
            println!("{message} (transport running: {}).", status.running);
            Ok(())
        }
        Err(error) if service_unavailable(&error) => {
            println!(
                "Configuration saved. The workspace service is not running, so no live Discord transport was changed."
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn service_unavailable(error: &crate::control_service::ControlError) -> bool {
    matches!(
        error,
        crate::control_service::ControlError::Io { source, .. }
            if matches!(
                source.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            )
    )
}

async fn status(
    store: &DiscordConfigStore,
    paths: &ServicePaths,
    json: bool,
) -> Result<(), DiscordError> {
    let config = store.load()?;
    let token_configured = store.token_configured();
    let runtime =
        match crate::control_service::transport_action(paths, "discord", TransportAction::Status)
            .await
        {
            Ok(status) => Some(status),
            Err(error) if service_unavailable(&error) => None,
            Err(error) => return Err(error.into()),
        };
    if json {
        #[derive(Serialize)]
        struct Status<'a> {
            enabled: bool,
            token_configured: bool,
            chat_channel_id: Option<&'a str>,
            agent_channel_id: Option<&'a str>,
            primary_user_id: Option<&'a str>,
            config_path: String,
            runtime: Option<&'a TransportStatus>,
        }
        let status = Status {
            enabled: config.enabled,
            token_configured,
            chat_channel_id: config.chat_channel_id.as_deref(),
            agent_channel_id: config.agent_channel_id.as_deref(),
            primary_user_id: config.primary_user_id.as_deref(),
            config_path: store.config_path().display().to_string(),
            runtime: runtime.as_ref(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&status).expect("status is serializable")
        );
    } else {
        println!("Discord enabled: {}", config.enabled);
        println!("Discord token configured: {token_configured}");
        match &runtime {
            Some(status) => println!(
                "Discord runtime: {}",
                if status.running { "running" } else { "stopped" }
            ),
            None => println!("Discord runtime: workspace service not running"),
        }
        println!(
            "Chat Orchestrator channel: {}",
            config
                .chat_channel_id
                .as_deref()
                .unwrap_or("not configured")
        );
        println!(
            "Agent Orchestrator channel: {}",
            config
                .agent_channel_id
                .as_deref()
                .unwrap_or("not configured")
        );
        println!(
            "Primary Discord user: {}",
            config
                .primary_user_id
                .as_deref()
                .unwrap_or("not configured")
        );
        println!("Configuration: {}", store.config_path().display());
    }
    Ok(())
}

async fn start_or_reload(paths: &ServicePaths, message: &str) -> Result<(), DiscordError> {
    match crate::control_service::transport_action(paths, "discord", TransportAction::Status).await
    {
        Ok(status) => {
            let action = if status.running {
                TransportAction::Restart
            } else {
                TransportAction::Start
            };
            let status = crate::control_service::transport_action(paths, "discord", action).await?;
            println!("{message} (transport running: {}).", status.running);
            Ok(())
        }
        Err(error) if service_unavailable(&error) => {
            println!(
                "Configuration saved. The workspace service is not running; start it and run `nakode transport discord start` to activate Discord."
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn set_enabled(store: &DiscordConfigStore, enabled: bool) -> Result<(), DiscordError> {
    let mut config = store.load()?;
    config.enabled = enabled;
    config.validate()?;
    if enabled {
        let _ = store.read_token()?;
    }
    store.save(&config)?;
    println!(
        "Discord frontend {} for automatic service startup.",
        if enabled { "enabled" } else { "disabled" }
    );
    Ok(())
}

fn prompt_line(prompt: &str, default: &str, required: bool) -> Result<String, DiscordError> {
    loop {
        if default.is_empty() {
            print!("{prompt}: ");
        } else {
            print!("{prompt} [{default}]: ");
        }
        io::stdout().flush().map_err(DiscordError::SetupInput)?;
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .map_err(DiscordError::SetupInput)?;
        let value = line.trim();
        let value = if value.is_empty() { default } else { value };
        if !required || !value.is_empty() {
            return Ok(value.to_owned());
        }
        println!("A value is required.");
    }
}

/// Creates the workspace's registered frontend transports.
///
/// The supervisor is deliberately generic: future transports can implement
/// [`TransportController`] and register alongside Discord without changing the
/// native service lifecycle.
pub(crate) fn transport_supervisor(workspace: &Path, endpoint: PathBuf) -> TransportSupervisor {
    match DiscordTransport::new(workspace, endpoint) {
        Ok(transport) => TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::new(transport) as Arc<dyn TransportController>,
        )]),
        Err(error) => {
            eprintln!(
                "nakode discord: could not open configuration ({})",
                sanitized_bridge_error(&error)
            );
            TransportSupervisor::default()
        }
    }
}

struct DiscordTransport {
    workspace: PathBuf,
    endpoint: PathBuf,
    store: DiscordConfigStore,
    runtime: Arc<tokio::sync::Mutex<DiscordRuntime>>,
    operation: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct DiscordRuntime {
    generation: u64,
    task: Option<JoinHandle<()>>,
    shutdown: Option<tokio::sync::watch::Sender<bool>>,
    error: Option<String>,
}

impl DiscordTransport {
    fn new(workspace: &Path, endpoint: PathBuf) -> Result<Self, DiscordError> {
        Ok(Self {
            workspace: workspace.to_owned(),
            endpoint,
            store: DiscordConfigStore::for_workspace(workspace)?,
            runtime: Arc::new(tokio::sync::Mutex::new(DiscordRuntime::default())),
            operation: tokio::sync::Mutex::new(()),
        })
    }

    async fn start_inner(&self, only_if_enabled: bool) -> Result<TransportStatus, DiscordError> {
        let config = self.store.load()?;
        {
            let runtime = self.runtime.lock().await;
            if runtime
                .task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
            {
                return Ok(status_for(&config, &runtime));
            }
        }
        if only_if_enabled && !config.enabled {
            return Ok(self.status_for_config(&config).await);
        }
        let token = self.store.read_token()?;
        let mut runtime = self.runtime.lock().await;
        if runtime
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return Ok(status_for(&config, &runtime));
        }
        runtime.task.take();
        runtime.shutdown.take();
        runtime.generation = runtime.generation.wrapping_add(1);
        runtime.error = None;
        let generation = runtime.generation;
        let task_runtime = Arc::clone(&self.runtime);
        let workspace = self.workspace.clone();
        let endpoint = self.endpoint.clone();
        let store = self.store.clone();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let task_config = config.clone();
        let task =
            tokio::spawn(async move {
                let error =
                    match run_gateway(task_config, token, workspace, endpoint, store, shutdown_rx)
                        .await
                    {
                        Ok(()) => None,
                        Err(error) => {
                            let sanitized = sanitized_bridge_error(&error).to_owned();
                            eprintln!("nakode discord: {sanitized}");
                            Some(sanitized)
                        }
                    };
                let mut runtime = task_runtime.lock().await;
                if runtime.generation == generation {
                    runtime.task = None;
                    runtime.shutdown = None;
                    runtime.error = error;
                }
            });
        runtime.task = Some(task);
        runtime.shutdown = Some(shutdown);
        Ok(status_for(&config, &runtime))
    }

    async fn stop_inner(&self) -> Result<TransportStatus, DiscordError> {
        let (handle, shutdown) = {
            let mut runtime = self.runtime.lock().await;
            runtime.generation = runtime.generation.wrapping_add(1);
            runtime.error = None;
            (runtime.task.take(), runtime.shutdown.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(true);
        }
        if let Some(mut handle) = handle
            && tokio::time::timeout(Duration::from_secs(5), &mut handle)
                .await
                .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        self.status_inner().await
    }

    async fn restart_inner(&self) -> Result<TransportStatus, DiscordError> {
        let _operation = self.operation.lock().await;
        let (handle, shutdown) = {
            let mut runtime = self.runtime.lock().await;
            runtime.generation = runtime.generation.wrapping_add(1);
            runtime.error = None;
            (runtime.task.take(), runtime.shutdown.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(true);
        }
        if let Some(mut handle) = handle
            && tokio::time::timeout(Duration::from_secs(5), &mut handle)
                .await
                .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        self.start_locked(false).await
    }

    async fn start_locked(&self, only_if_enabled: bool) -> Result<TransportStatus, DiscordError> {
        self.start_inner(only_if_enabled).await
    }

    async fn status_for_config(&self, config: &DiscordConfig) -> TransportStatus {
        let runtime = self.runtime.lock().await;
        status_for(config, &runtime)
    }

    async fn status_inner(&self) -> Result<TransportStatus, DiscordError> {
        let config = self.store.load()?;
        Ok(self.status_for_config(&config).await)
    }
}

fn status_for(config: &DiscordConfig, runtime: &DiscordRuntime) -> TransportStatus {
    TransportStatus {
        name: "discord".to_owned(),
        enabled: config.enabled,
        running: runtime
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished()),
        error: runtime.error.clone(),
    }
}

impl TransportController for DiscordTransport {
    fn autostart(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            let _operation = self.operation.lock().await;
            self.start_locked(true)
                .await
                .map_err(|error| error.to_string())
        }
        .boxed()
    }

    fn start(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            let _operation = self.operation.lock().await;
            self.start_locked(false)
                .await
                .map_err(|error| error.to_string())
        }
        .boxed()
    }

    fn stop(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            let _operation = self.operation.lock().await;
            self.stop_inner().await.map_err(|error| error.to_string())
        }
        .boxed()
    }

    fn restart(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.restart_inner()
                .await
                .map_err(|error| error.to_string())
        }
        .boxed()
    }

    fn status(&self) -> BoxFuture<'_, Result<TransportStatus, String>> {
        async move { self.status_inner().await.map_err(|error| error.to_string()) }.boxed()
    }
}

#[derive(Clone, Debug)]
struct ExternalMessage {
    id: MessageId,
    nonce: Option<String>,
    thread_id: Option<ChannelId>,
}

#[async_trait]
trait DiscordApi: Send + Sync {
    async fn send_message(
        &self,
        channel_id: ChannelId,
        content: &str,
        nonce: Option<&str>,
    ) -> Result<ExternalMessage, serenity::Error>;
    async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        content: &str,
    ) -> Result<(), serenity::Error>;
    async fn create_thread(
        &self,
        parent_channel_id: ChannelId,
        starter_message_id: MessageId,
        title: &str,
    ) -> Result<ChannelId, serenity::Error>;
    async fn set_thread_archived(
        &self,
        thread_id: ChannelId,
        archived: bool,
    ) -> Result<(), serenity::Error>;
    async fn messages_page(
        &self,
        channel_id: ChannelId,
        before: Option<MessageId>,
    ) -> Result<Vec<ExternalMessage>, serenity::Error>;
    async fn react(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error>;
    async fn remove_own_reaction(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error>;
}

fn disabled_mentions() -> CreateAllowedMentions {
    CreateAllowedMentions::new()
        .all_users(false)
        .all_roles(false)
        .everyone(false)
        .replied_user(false)
}

struct SerenityDiscordApi {
    http: Arc<serenity::http::Http>,
}

#[async_trait]
impl DiscordApi for SerenityDiscordApi {
    async fn send_message(
        &self,
        channel_id: ChannelId,
        content: &str,
        nonce: Option<&str>,
    ) -> Result<ExternalMessage, serenity::Error> {
        let mut request = CreateMessage::new()
            .content(content)
            .allowed_mentions(disabled_mentions());
        if let Some(nonce) = nonce {
            request = request
                .nonce(serenity::all::Nonce::String(nonce.to_owned()))
                .enforce_nonce(true);
        }
        let message = channel_id.send_message(&self.http, request).await?;
        Ok(external_message(&message))
    }

    async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        content: &str,
    ) -> Result<(), serenity::Error> {
        channel_id
            .edit_message(
                &self.http,
                message_id,
                EditMessage::new()
                    .content(content)
                    .allowed_mentions(disabled_mentions()),
            )
            .await?;
        Ok(())
    }

    async fn create_thread(
        &self,
        parent_channel_id: ChannelId,
        starter_message_id: MessageId,
        title: &str,
    ) -> Result<ChannelId, serenity::Error> {
        Ok(parent_channel_id
            .create_thread_from_message(&self.http, starter_message_id, CreateThread::new(title))
            .await?
            .id)
    }

    async fn set_thread_archived(
        &self,
        thread_id: ChannelId,
        archived: bool,
    ) -> Result<(), serenity::Error> {
        thread_id
            .edit_thread(
                &self.http,
                serenity::all::EditThread::new().archived(archived),
            )
            .await?;
        Ok(())
    }

    async fn messages_page(
        &self,
        channel_id: ChannelId,
        before: Option<MessageId>,
    ) -> Result<Vec<ExternalMessage>, serenity::Error> {
        let mut request = serenity::all::GetMessages::new().limit(100);
        if let Some(before) = before {
            request = request.before(before);
        }
        Ok(channel_id
            .messages(&self.http, request)
            .await?
            .iter()
            .map(external_message)
            .collect())
    }

    async fn react(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error> {
        channel_id
            .create_reaction(
                &self.http,
                message_id,
                serenity::all::ReactionType::Unicode(emoji.to_owned()),
            )
            .await
    }

    async fn remove_own_reaction(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error> {
        channel_id
            .delete_reaction(
                &self.http,
                message_id,
                None,
                serenity::all::ReactionType::Unicode(emoji.to_owned()),
            )
            .await
    }
}

fn external_message(message: &Message) -> ExternalMessage {
    let nonce = match &message.nonce {
        Some(serenity::all::Nonce::String(value)) => Some(value.clone()),
        Some(serenity::all::Nonce::Number(value)) => Some(value.to_string()),
        None => None,
    };
    ExternalMessage {
        id: message.id,
        nonce,
        thread_id: message.thread.as_ref().map(|thread| thread.id),
    }
}

struct MultipartAssembler {
    root: PathBuf,
    groups: tokio::sync::Mutex<HashMap<String, MultipartGroup>>,
    completed_groups: tokio::sync::Mutex<HashMap<String, Instant>>,
}

struct MultipartGroup {
    directory: PathBuf,
    session_id: String,
    total: u32,
    received: HashMap<u32, (String, String)>,
    updated: Instant,
}

struct MultipartPart<'a> {
    group: &'a str,
    index: u32,
    total: u32,
    body: &'a str,
}

enum MultipartOutcome {
    Waiting,
    Duplicate,
    Complete {
        group: String,
        text: String,
        event_id: String,
        source_message_id: String,
    },
}

impl MultipartAssembler {
    fn new(root: PathBuf) -> Result<Self, DiscordError> {
        if root.exists() {
            std::fs::remove_dir_all(&root).map_err(|source| io_error(&root, source))?;
        }
        prepare_private_directory(&root)?;
        Ok(Self {
            root,
            groups: tokio::sync::Mutex::new(HashMap::new()),
            completed_groups: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    async fn accept(
        &self,
        session_id: &str,
        message_id: MessageId,
        part: MultipartPart<'_>,
    ) -> Result<MultipartOutcome, DiscordError> {
        let key = hex_digest(format!("{session_id}:{}", part.group).as_bytes());
        {
            let mut completed = self.completed_groups.lock().await;
            completed.retain(|_, completed_at| completed_at.elapsed() <= MULTIPART_TTL);
            if completed.contains_key(&key) {
                return Ok(MultipartOutcome::Duplicate);
            }
        }
        let mut groups = self.groups.lock().await;
        let expired = groups
            .iter()
            .filter(|(_, group)| group.updated.elapsed() > MULTIPART_TTL)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            if let Some(group) = groups.remove(&key) {
                let _ = std::fs::remove_dir_all(group.directory);
            }
        }
        if !groups.contains_key(&key) {
            let session_groups = groups
                .values()
                .filter(|group| group.session_id == session_id)
                .count();
            if session_groups >= MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION {
                return Err(DiscordError::InvalidConfig(
                    "too many active multipart prompts for this session; finish or wait for an existing group to expire"
                        .to_owned(),
                ));
            }
            if groups.len() >= MAX_ACTIVE_MULTIPART_ASSEMBLIES {
                return Err(DiscordError::InvalidConfig(
                    "too many active multipart prompts; finish or wait for an existing group to expire"
                        .to_owned(),
                ));
            }
        }
        let directory = self.root.join(&key[..32]);
        let group = groups.entry(key.clone()).or_insert_with(|| MultipartGroup {
            directory: directory.clone(),
            session_id: session_id.to_owned(),
            total: part.total,
            received: HashMap::new(),
            updated: Instant::now(),
        });
        if group.total != part.total {
            return Err(DiscordError::InvalidConfig(
                "multipart total changed within one group".to_owned(),
            ));
        }
        prepare_private_directory(&group.directory)?;
        let body_hash = hex_digest(part.body.as_bytes());
        if let Some((existing_hash, _)) = group.received.get(&part.index) {
            if existing_hash != &body_hash {
                return Err(DiscordError::InvalidConfig(
                    "multipart part was replayed with different content".to_owned(),
                ));
            }
        } else {
            atomic_write(
                &group.directory.join(format!("{:010}.part", part.index)),
                part.body.as_bytes(),
            )?;
            group
                .received
                .insert(part.index, (body_hash, message_id.get().to_string()));
        }
        group.updated = Instant::now();
        if group.received.len() != usize::try_from(group.total).unwrap_or(usize::MAX) {
            return Ok(MultipartOutcome::Waiting);
        }

        let mut text = String::new();
        let mut event_material = format!("{session_id}:{}:{}", part.group, group.total);
        for index in 1..=group.total {
            let source = std::fs::read_to_string(group.directory.join(format!("{index:010}.part")))
                .map_err(|source| io_error(&group.directory, source))?;
            text.push_str(&source);
            let (_, message_id) = group.received.get(&index).ok_or_else(|| {
                DiscordError::InvalidConfig("multipart prompt is missing a part".to_owned())
            })?;
            event_material.push(':');
            event_material.push_str(message_id);
        }
        let source_message_id = group
            .received
            .get(&group.total)
            .map(|(_, message_id)| message_id.clone())
            .ok_or_else(|| {
                DiscordError::InvalidConfig("multipart prompt is missing its final part".to_owned())
            })?;
        Ok(MultipartOutcome::Complete {
            group: part.group.to_owned(),
            text,
            event_id: format!("multipart:{}", &hex_digest(event_material.as_bytes())[..32]),
            source_message_id,
        })
    }

    async fn finish(&self, session_id: &str, group: &str) {
        let key = hex_digest(format!("{session_id}:{group}").as_bytes());
        if let Some(group) = self.groups.lock().await.remove(&key) {
            let _ = std::fs::remove_dir_all(group.directory);
        }
        self.completed_groups
            .lock()
            .await
            .insert(key, Instant::now());
    }
}

fn parse_multipart(value: &str) -> Option<Result<MultipartPart<'_>, DiscordError>> {
    let (header, body) = value.split_once('\n').unwrap_or((value, ""));
    let command = header.strip_prefix("!nakode multipart ")?;
    let mut fields = command.split_whitespace();
    let group = fields.next().unwrap_or_default();
    let range = fields.next().unwrap_or_default();
    if fields.next().is_some()
        || group.is_empty()
        || group.len() > 32
        || !group
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Some(Err(DiscordError::InvalidConfig(
            "multipart syntax is `!nakode multipart <group> <part>/<total>`".to_owned(),
        )));
    }
    let Some((index, total)) = range.split_once('/') else {
        return Some(Err(DiscordError::InvalidConfig(
            "multipart part and total must use `<part>/<total>`".to_owned(),
        )));
    };
    let parsed = index
        .parse::<u32>()
        .ok()
        .zip(total.parse::<u32>().ok())
        .filter(|(index, total)| *index > 0 && *total > 0 && index <= total);
    Some(parsed.map_or_else(
        || {
            Err(DiscordError::InvalidConfig(
                "multipart part numbers must be positive and no greater than the total".to_owned(),
            ))
        },
        |(index, total)| {
            Ok(MultipartPart {
                group,
                index,
                total,
                body,
            })
        },
    ))
}

const INGRESS_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct IngressAttachment {
    filename: String,
    url: String,
    content_type: Option<String>,
    size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct IngressRecord {
    version: u32,
    session_id: String,
    thread_id: String,
    message_id: String,
    author_id: String,
    received_at_ms: u64,
    content: String,
    attachments: Vec<IngressAttachment>,
    multipart_group: Option<String>,
    forced_busy: bool,
}

impl IngressRecord {
    fn from_message(session_id: String, message: &Message, forced_busy: bool) -> Self {
        let multipart_group = parse_multipart(&message.content)
            .and_then(Result::ok)
            .map(|part| part.group.to_owned());
        Self {
            version: INGRESS_SCHEMA_VERSION,
            session_id,
            thread_id: message.channel_id.get().to_string(),
            message_id: message.id.get().to_string(),
            author_id: message.author.id.get().to_string(),
            received_at_ms: unix_time_ms(),
            content: message.content.clone(),
            attachments: message
                .attachments
                .iter()
                .map(|attachment| IngressAttachment {
                    filename: attachment.filename.clone(),
                    url: attachment.url.clone(),
                    content_type: attachment.content_type.clone(),
                    size: u64::from(attachment.size),
                })
                .collect(),
            multipart_group,
            forced_busy,
        }
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn unix_time_ms_i64() -> i64 {
    i64::try_from(unix_time_ms()).unwrap_or(i64::MAX)
}

struct IngressSpool {
    connection: std::sync::Mutex<Connection>,
}

impl IngressSpool {
    fn open(path: &Path) -> Result<Self, DiscordError> {
        if let Some(parent) = path.parent() {
            prepare_private_directory(parent)?;
        }
        let connection = Connection::open(path).map_err(DiscordError::IngressStore)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(DiscordError::IngressStore)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS discord_ingress (
                   sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                   external_event_id TEXT NOT NULL UNIQUE,
                   session_id TEXT NOT NULL,
                   multipart_group TEXT,
                   payload_json BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS discord_ingress_tombstones (
                   external_event_id TEXT PRIMARY KEY,
                   recorded_at_ms INTEGER NOT NULL
                 );",
            )
            .map_err(DiscordError::IngressStore)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(|source| io_error(path, source))?;
        }
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
    }

    /// Durably admits a gateway event. `None` means this identity already reached a terminal local
    /// or authoritative disposition and must not be reconsidered after a reconnect or reopen.
    fn enqueue(&self, proposed: &IngressRecord) -> Result<Option<IngressRecord>, DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An immediate transaction serializes admission across Nakode processes before either the
        // per-session ordering check or overload decision is observed. The unique event key then
        // makes the first complete durable decision authoritative for duplicate gateway delivery.
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(DiscordError::IngressStore)?;
        let tombstoned = transaction
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM discord_ingress_tombstones WHERE external_event_id = ?1
                 )",
                [&proposed.message_id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        if tombstoned {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(None);
        }
        if let Some(payload) = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [&proposed.message_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?
        {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return serde_json::from_slice(&payload)
                .map(Some)
                .map_err(DiscordError::IngressPayload);
        }

        let mut record = proposed.clone();
        let same_session_pending = transaction
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM discord_ingress
                   WHERE session_id = ?1
                     AND (?2 IS NULL OR multipart_group IS NULL OR multipart_group != ?2)
                 )",
                params![record.session_id, record.multipart_group],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        let pending_count = transaction
            .query_row("SELECT COUNT(*) FROM discord_ingress", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(DiscordError::IngressStore)?;
        if same_session_pending
            || (record.multipart_group.is_none()
                && pending_count >= i64::try_from(MAX_INBOUND_INFLIGHT).unwrap_or(i64::MAX))
        {
            record.forced_busy = true;
        }
        if record.forced_busy {
            // Busy records only need durable identity and route metadata. Do not retain prompt text,
            // expiring attachment URLs, or grouping for work that is guaranteed never to execute.
            record.content.clear();
            record.attachments.clear();
            record.multipart_group = None;
        }
        let payload = serde_json::to_vec(&record).map_err(DiscordError::IngressPayload)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress
                 (external_event_id, session_id, multipart_group, payload_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    record.message_id,
                    record.session_id,
                    record.multipart_group,
                    payload
                ],
            )
            .map_err(DiscordError::IngressStore)?;
        let authoritative = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [&record.message_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)?;
        serde_json::from_slice(&authoritative)
            .map(Some)
            .map_err(DiscordError::IngressPayload)
    }

    fn next_after(&self, sequence: i64) -> Result<Option<(i64, IngressRecord)>, DiscordError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let row = connection
            .query_row(
                "SELECT sequence, payload_json FROM discord_ingress
                 WHERE sequence > ?1 ORDER BY sequence LIMIT 1",
                [sequence],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?;
        row.map(|(sequence, payload)| {
            serde_json::from_slice(&payload)
                .map(|record| (sequence, record))
                .map_err(DiscordError::IngressPayload)
        })
        .transpose()
    }

    fn remove_event(&self, external_event_id: &str) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?2 FROM discord_ingress WHERE external_event_id = ?1",
                params![external_event_id, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE external_event_id = ?1",
                [external_event_id],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    fn remove_multipart_group(&self, session_id: &str, group: &str) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?3 FROM discord_ingress
                 WHERE session_id = ?1 AND multipart_group = ?2",
                params![session_id, group, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE session_id = ?1 AND multipart_group = ?2",
                params![session_id, group],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    /// Quarantines one corrupt payload without retaining user content or allowing its event
    /// identity to become a future prompt after a reconnect.
    fn discard_next_after(&self, sequence: i64) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?2 FROM discord_ingress
                 WHERE sequence = (
                   SELECT MIN(sequence) FROM discord_ingress WHERE sequence > ?1
                 )",
                params![sequence, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE sequence = (
                   SELECT MIN(sequence) FROM discord_ingress WHERE sequence > ?1
                 )",
                [sequence],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    #[cfg(test)]
    fn len(&self) -> Result<u64, DiscordError> {
        let count = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .query_row("SELECT COUNT(*) FROM discord_ingress", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(DiscordError::IngressStore)?;
        Ok(u64::try_from(count).unwrap_or_default())
    }
}

struct BotState {
    client: NakodeClient,
    workspace_id: String,
    workspace_path: String,
    http_client: HttpClient,
    config: DiscordConfig,
    bridges: tokio::sync::RwLock<HashMap<String, api::SessionBridge>>,
    thread_routes: tokio::sync::RwLock<HashMap<u64, String>>,
    workers: tokio::sync::Mutex<HashMap<String, JoinHandle<()>>>,
    thread_creation: tokio::sync::Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    reconciler: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    ingress_replayer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    ingress_inflight: tokio::sync::Mutex<HashSet<String>>,
    ingress_notify: tokio::sync::Notify,
    ingress: IngressSpool,
    bot_user_id: std::sync::OnceLock<UserId>,
    inbound_slots: tokio::sync::Semaphore,
    multipart: MultipartAssembler,
    recovery_root: PathBuf,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl BotState {
    async fn stop_tasks(&self) {
        if let Some(handle) = self.reconciler.lock().await.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(handle) = self.ingress_replayer.lock().await.take() {
            handle.abort();
            let _ = handle.await;
        }
        let handles = self
            .workers
            .lock()
            .await
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn current_bridge(&self, session_id: &str) -> Option<api::SessionBridge> {
        self.bridges.read().await.get(session_id).cloned()
    }

    async fn thread_creation_lock(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.thread_creation.lock().await;
        if let Some(lock) = locks.get(session_id).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(session_id.to_owned(), Arc::downgrade(&lock));
        lock
    }
}

struct Handler {
    state: Arc<BotState>,
}

async fn run_gateway(
    config: DiscordConfig,
    token: String,
    workspace: PathBuf,
    endpoint: PathBuf,
    store: DiscordConfigStore,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), DiscordError> {
    let Some(client) = connect_api(endpoint, &mut shutdown).await? else {
        return Ok(());
    };
    let server_info = client.get_server_info().await?;
    if !server_info
        .capabilities
        .iter()
        .any(|capability| capability == "OrchestratorThreadBridge")
    {
        return Err(DiscordError::InvalidConfig(
            "the running Nakode service does not support orchestrator thread bridges".to_owned(),
        ));
    }
    let workspace_path = workspace.to_string_lossy().into_owned();
    let workspace_state = client.get_workspace(workspace_path, None).await?;
    let initial_bridges = workspace_state
        .session_bridges
        .into_iter()
        .map(|bridge| (bridge.session_id.clone(), bridge))
        .collect();
    let http_client = HttpClient::builder()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 || !is_approved_discord_cdn_url(attempt.url()) {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .build()?;
    let recovery_root = store.directory.join("delivery-recovery");
    if recovery_root.exists() {
        std::fs::remove_dir_all(&recovery_root)
            .map_err(|source| io_error(&recovery_root, source))?;
    }
    prepare_private_directory(&recovery_root)?;
    let state = Arc::new(BotState {
        client,
        workspace_id: workspace_state.workspace_id,
        workspace_path: workspace_state.workspace_path,
        http_client,
        config,
        bridges: tokio::sync::RwLock::new(initial_bridges),
        thread_routes: tokio::sync::RwLock::new(HashMap::new()),
        workers: tokio::sync::Mutex::new(HashMap::new()),
        thread_creation: tokio::sync::Mutex::new(HashMap::new()),
        reconciler: tokio::sync::Mutex::new(None),
        ingress_replayer: tokio::sync::Mutex::new(None),
        ingress_inflight: tokio::sync::Mutex::new(HashSet::new()),
        ingress_notify: tokio::sync::Notify::new(),
        ingress: IngressSpool::open(&store.directory.join("discord-ingress.sqlite"))?,
        bot_user_id: std::sync::OnceLock::new(),
        inbound_slots: tokio::sync::Semaphore::new(MAX_INBOUND_INFLIGHT),
        multipart: MultipartAssembler::new(store.directory.join("assemblies"))?,
        recovery_root,
        shutdown,
    });
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
    let mut gateway_shutdown = state.shutdown.clone();
    let mut reconnect_attempt = 0;
    let retry_identity = stable_retry_identity(&state.workspace_id);
    loop {
        if *gateway_shutdown.borrow() {
            state.stop_tasks().await;
            return Ok(());
        }
        let handler = Handler {
            state: Arc::clone(&state),
        };
        let mut discord = match serenity::Client::builder(token.clone(), intents)
            .event_handler(handler)
            .await
        {
            Ok(discord) => discord,
            Err(error) => {
                let error = DiscordError::Gateway(error);
                eprintln!(
                    "nakode discord: gateway reconnecting ({})",
                    sanitized_bridge_error(&error)
                );
                if !wait_for_reconnect(&mut gateway_shutdown, reconnect_attempt, retry_identity)
                    .await
                {
                    state.stop_tasks().await;
                    return Ok(());
                }
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                continue;
            }
        };
        let shard_manager = Arc::clone(&discord.shard_manager);
        let gateway_result = tokio::select! {
            result = discord.start() => Some(result),
            _ = gateway_shutdown.changed() => {
                shard_manager.shutdown_all().await;
                None
            }
        };
        state.stop_tasks().await;
        let Some(gateway_result) = gateway_result else {
            return Ok(());
        };
        if let Err(error) = gateway_result {
            let error = DiscordError::Gateway(error);
            eprintln!(
                "nakode discord: gateway reconnecting ({})",
                sanitized_bridge_error(&error)
            );
        } else {
            eprintln!("nakode discord: gateway stream ended; reconnecting");
        }
        if !wait_for_reconnect(&mut gateway_shutdown, reconnect_attempt, retry_identity).await {
            return Ok(());
        }
        reconnect_attempt = reconnect_attempt.saturating_add(1);
    }
}

async fn start_ingress_replayer(discord: Arc<dyn DiscordApi>, state: Arc<BotState>) {
    let mut slot = state.ingress_replayer.lock().await;
    if slot.as_ref().is_some_and(|task| !task.is_finished()) {
        state.ingress_notify.notify_one();
        return;
    }
    if let Some(finished) = slot.take() {
        let _ = finished.await;
    }
    let task_state = Arc::clone(&state);
    *slot = Some(tokio::spawn(async move {
        replay_ingress_loop(discord, task_state).await;
    }));
}

async fn replay_ingress_loop(discord: Arc<dyn DiscordApi>, state: Arc<BotState>) {
    let mut sequence = 0i64;
    let mut shutdown = state.shutdown.clone();
    loop {
        if *shutdown.borrow() {
            return;
        }
        match state.ingress.next_after(sequence) {
            Ok(Some((next_sequence, record))) => {
                sequence = next_sequence;
                if !claim_ingress(&state, &record.message_id).await {
                    continue;
                }
                let permit = if record.forced_busy {
                    None
                } else {
                    Some(tokio::select! {
                        _ = shutdown.changed() => return,
                        permit = state.inbound_slots.acquire() => match permit {
                            Ok(permit) => permit,
                            Err(_) => return,
                        },
                    })
                };
                let outcome = process_ingress_record(&*discord, &state, &record).await;
                drop(permit);
                settle_ingress(&state, &record, outcome).await;
                state
                    .ingress_inflight
                    .lock()
                    .await
                    .remove(&record.message_id);
            }
            Ok(None) => {
                sequence = 0;
                tokio::select! {
                    _ = shutdown.changed() => return,
                    () = state.ingress_notify.notified() => {},
                    () = tokio::time::sleep(RECONCILE_RETRY_DELAY) => {},
                }
            }
            Err(error) => {
                let corrupt_payload = matches!(&error, DiscordError::IngressPayload(_));
                eprintln!(
                    "nakode discord: durable ingress replay deferred ({})",
                    sanitized_bridge_error(&error)
                );
                if corrupt_payload {
                    if let Err(discard_error) = state.ingress.discard_next_after(sequence) {
                        eprintln!(
                            "nakode discord: corrupt ingress quarantine deferred ({})",
                            sanitized_bridge_error(&discard_error)
                        );
                    } else {
                        // The invalid row's identity is now a terminal tombstone. Continue without
                        // allowing one corrupt payload to block all later sessions.
                        continue;
                    }
                }
                tokio::select! {
                    _ = shutdown.changed() => return,
                    () = tokio::time::sleep(RECONCILE_RETRY_DELAY) => {},
                }
            }
        }
    }
}

async fn claim_ingress(state: &BotState, message_id: &str) -> bool {
    state
        .ingress_inflight
        .lock()
        .await
        .insert(message_id.to_owned())
}

async fn settle_ingress(state: &BotState, record: &IngressRecord, outcome: IngressProcessOutcome) {
    let result = match outcome {
        IngressProcessOutcome::Terminal => state.ingress.remove_event(&record.message_id),
        IngressProcessOutcome::TerminalMultipart(group) => {
            state.multipart.finish(&record.session_id, &group).await;
            state
                .ingress
                .remove_multipart_group(&record.session_id, &group)
        }
        IngressProcessOutcome::Deferred | IngressProcessOutcome::WaitingMultipart => Ok(()),
    };
    if let Err(error) = result {
        eprintln!(
            "nakode discord: durable ingress cleanup deferred for session {} ({})",
            short_identity(&record.session_id),
            sanitized_bridge_error(&error)
        );
    }
}

async fn start_reconciler(discord: Arc<dyn DiscordApi>, state: Arc<BotState>) {
    let mut slot = state.reconciler.lock().await;
    if slot.as_ref().is_some_and(|task| !task.is_finished()) {
        return;
    }
    if let Some(finished) = slot.take() {
        let _ = finished.await;
    }
    let task_state = Arc::clone(&state);
    *slot = Some(tokio::spawn(async move {
        reconcile_loop(discord, task_state).await;
    }));
}

async fn reconcile_loop(discord: Arc<dyn DiscordApi>, state: Arc<BotState>) {
    let mut updates = state.client.watch_workspace(state.workspace_id.clone());
    let mut shutdown = state.shutdown.clone();
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watch_attempt = 0;
    let retry_identity = stable_retry_identity(&state.workspace_id);
    loop {
        let authoritative_update = tokio::select! {
            _ = shutdown.changed() => return,
            _ = interval.tick() => None,
            update = updates.next() => {
                match update {
                    Some(Ok(workspace)) => {
                        watch_attempt = 0;
                        Some(workspace.session_bridges)
                    }
                    Some(Err(error)) => {
                        eprintln!("nakode discord: workspace bridge watch reconnecting ({})", sanitized_sdk_error(&error));
                        if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                            return;
                        }
                        watch_attempt = watch_attempt.saturating_add(1);
                        updates = state.client.watch_workspace(state.workspace_id.clone());
                        continue;
                    }
                    None => {
                        eprintln!("nakode discord: workspace bridge watch reconnecting (stream ended)");
                        if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                            return;
                        }
                        watch_attempt = watch_attempt.saturating_add(1);
                        updates = state.client.watch_workspace(state.workspace_id.clone());
                        continue;
                    }
                }
            }
        };
        if *shutdown.borrow() {
            return;
        }
        // Periodic Discord reconciliation must not reinstall the last watch snapshot: successful
        // bridge RPCs optimistically update this map before the corresponding watch event arrives.
        // Replaying an older snapshot on every timer tick would transiently erase those checkpoints.
        let snapshot = {
            let mut bridges = state.bridges.write().await;
            reconciliation_snapshot(&mut bridges, authoritative_update)
        };
        reconcile_snapshot(Arc::clone(&discord), Arc::clone(&state), &snapshot).await;
    }
}

fn reconciliation_snapshot(
    bridges: &mut HashMap<String, api::SessionBridge>,
    authoritative_update: Option<Vec<api::SessionBridge>>,
) -> Vec<api::SessionBridge> {
    if let Some(latest) = authoritative_update {
        *bridges = latest
            .into_iter()
            .map(|bridge| (bridge.session_id.clone(), bridge))
            .collect();
    }
    bridges.values().cloned().collect()
}

async fn reconcile_snapshot(
    discord: Arc<dyn DiscordApi>,
    state: Arc<BotState>,
    bridges: &[api::SessionBridge],
) {
    let desired = bridges
        .iter()
        .filter(|bridge| bridge.lifecycle == api::BridgeLifecycle::Open as i32)
        .map(|bridge| bridge.session_id.clone())
        .collect::<HashSet<_>>();
    let stale_session_ids = state
        .workers
        .lock()
        .await
        .keys()
        .filter(|session_id| !desired.contains(*session_id))
        .cloned()
        .collect::<Vec<_>>();
    for session_id in stale_session_ids {
        stop_worker(&state, &session_id).await;
    }

    for bridge in bridges {
        if let Err(error) = reconcile_bridge(Arc::clone(&discord), Arc::clone(&state), bridge).await
        {
            eprintln!(
                "nakode discord: bridge reconciliation deferred for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_bridge_error(&error)
            );
        }
    }
}

async fn reconcile_bridge(
    discord: Arc<dyn DiscordApi>,
    state: Arc<BotState>,
    bridge: &api::SessionBridge,
) -> Result<(), DiscordError> {
    let kind = api::OrchestratorKind::try_from(bridge.kind).map_err(|_| {
        DiscordError::InvalidConfig(
            "session bridge has an unspecified orchestrator kind".to_owned(),
        )
    })?;
    if kind == api::OrchestratorKind::Unspecified {
        return Err(DiscordError::InvalidConfig(
            "session bridge has an unspecified orchestrator kind".to_owned(),
        ));
    }
    let expected_parent = state.config.parent_channel(kind).ok_or_else(|| {
        DiscordError::InvalidConfig("orchestrator parent channel is not configured".to_owned())
    })?;
    if bridge
        .transport
        .as_deref()
        .is_some_and(|value| value != TRANSPORT_NAME)
    {
        return Ok(());
    }

    let mapped_thread = bridge
        .external_thread_id
        .as_deref()
        .map(|value| validate_snowflake("external_thread_id", value).map(ChannelId::new))
        .transpose()?;
    let mapped_parent = bridge
        .external_parent_id
        .as_deref()
        .map(|value| validate_snowflake("external_parent_id", value).map(ChannelId::new))
        .transpose()?;

    if bridge.lifecycle == api::BridgeLifecycle::Archived as i32 {
        stop_worker(&state, &bridge.session_id).await;
        if let Some(thread_id) = mapped_thread {
            state.thread_routes.write().await.remove(&thread_id.get());
            match set_archived_with_retry(&*discord, thread_id, true).await {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {
                    clear_thread_binding(&state.client, bridge, thread_id).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        return Ok(());
    }
    if bridge.lifecycle != api::BridgeLifecycle::Open as i32 {
        return Ok(());
    }

    if let Some(thread_id) = mapped_thread {
        if mapped_parent != Some(expected_parent) {
            let _ = set_archived_with_retry(&*discord, thread_id, true).await;
            clear_thread_binding(&state.client, bridge, thread_id).await?;
            state.thread_routes.write().await.remove(&thread_id.get());
            stop_worker(&state, &bridge.session_id).await;
            return Ok(());
        }
        match set_archived_with_retry(&*discord, thread_id, false).await {
            Ok(()) => {
                register_open_thread(&state, bridge, thread_id).await;
                start_worker(discord, state, bridge.session_id.clone()).await;
            }
            Err(error) if is_not_found(&error) => {
                clear_thread_binding(&state.client, bridge, thread_id).await?;
                state.thread_routes.write().await.remove(&thread_id.get());
                stop_worker(&state, &bridge.session_id).await;
            }
            Err(error) => return Err(error.into()),
        }
        return Ok(());
    }

    let creation_lock = state.thread_creation_lock(&bridge.session_id).await;
    let _creation_guard = creation_lock.lock().await;
    // Snapshot reconciliation and the periodic tick may race. The winner publishes the binding into
    // this process before releasing the per-session lock; every loser adopts it without creating a
    // second starter message or thread.
    if let Some(current) = state.current_bridge(&bridge.session_id).await
        && let Some(thread_id) = current
            .external_thread_id
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .map(ChannelId::new)
    {
        if current.lifecycle == api::BridgeLifecycle::Open as i32
            && current.transport.as_deref() == Some(TRANSPORT_NAME)
            && current.external_parent_id.as_deref() == Some(&expected_parent.get().to_string())
        {
            register_open_thread(&state, &current, thread_id).await;
            start_worker(discord, state, current.session_id).await;
        }
        return Ok(());
    }

    let thread_id = create_or_recover_thread(&*discord, expected_parent, bridge).await?;
    let binding = state
        .client
        .bind_session_bridge_thread(api::BindSessionBridgeThreadRequest {
            mutation: None,
            session_id: bridge.session_id.clone(),
            transport: TRANSPORT_NAME.to_owned(),
            external_parent_id: expected_parent.get().to_string(),
            external_thread_id: thread_id.get().to_string(),
        })
        .await;
    if let Err(error) = binding {
        // A separately reconnecting gateway/process may have won after our preflight. Adopt only the
        // authoritative Nakode binding and archive the unclaimed Discord thread best-effort.
        if let Ok(workspace) = state
            .client
            .get_workspace(state.workspace_path.clone(), None)
            .await
            && let Some(authoritative) = workspace
                .session_bridges
                .into_iter()
                .find(|candidate| candidate.session_id == bridge.session_id)
            && authoritative.transport.as_deref() == Some(TRANSPORT_NAME)
            && let Some(authoritative_thread) = authoritative
                .external_thread_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .map(ChannelId::new)
        {
            if authoritative_thread != thread_id {
                let _ = set_archived_with_retry(&*discord, thread_id, true).await;
            }
            state
                .bridges
                .write()
                .await
                .insert(authoritative.session_id.clone(), authoritative.clone());
            if authoritative.lifecycle == api::BridgeLifecycle::Open as i32 {
                register_open_thread(&state, &authoritative, authoritative_thread).await;
                start_worker(discord, state, authoritative.session_id).await;
            }
            return Ok(());
        }
        return Err(error.into());
    }
    let mut bound = bridge.clone();
    bound.transport = Some(TRANSPORT_NAME.to_owned());
    bound.external_parent_id = Some(expected_parent.get().to_string());
    bound.external_thread_id = Some(thread_id.get().to_string());
    state
        .bridges
        .write()
        .await
        .insert(bound.session_id.clone(), bound.clone());
    register_open_thread(&state, &bound, thread_id).await;
    start_worker(discord, state, bridge.session_id.clone()).await;
    Ok(())
}

async fn register_open_thread(state: &BotState, bridge: &api::SessionBridge, thread_id: ChannelId) {
    state
        .thread_routes
        .write()
        .await
        .insert(thread_id.get(), bridge.session_id.clone());
}

async fn clear_thread_binding(
    client: &NakodeClient,
    bridge: &api::SessionBridge,
    thread_id: ChannelId,
) -> Result<(), DiscordError> {
    client
        .clear_session_bridge_thread(api::ClearSessionBridgeThreadRequest {
            mutation: None,
            session_id: bridge.session_id.clone(),
            transport: TRANSPORT_NAME.to_owned(),
            external_thread_id: thread_id.get().to_string(),
        })
        .await?;
    Ok(())
}

async fn create_or_recover_thread(
    discord: &dyn DiscordApi,
    parent_channel_id: ChannelId,
    bridge: &api::SessionBridge,
) -> Result<ChannelId, DiscordError> {
    let nonce = starter_nonce(&bridge.session_id);
    let starter_text = format!(
        "{} **{}**",
        orchestrator_label(bridge.kind),
        sanitize_mentions(&bridge.display_title)
    );
    let starter = match find_message_by_nonce(discord, parent_channel_id, &nonce).await? {
        Some(message) => message,
        None => {
            discord
                .send_message(parent_channel_id, &starter_text, Some(&nonce))
                .await?
        }
    };
    if let Some(thread_id) = starter.thread_id {
        return Ok(thread_id);
    }
    match discord
        .create_thread(
            parent_channel_id,
            starter.id,
            &thread_title(bridge.kind, &bridge.display_title),
        )
        .await
    {
        Ok(thread_id) => Ok(thread_id),
        Err(error) => {
            if let Some(recovered) =
                find_message_by_nonce(discord, parent_channel_id, &nonce).await?
                && let Some(thread_id) = recovered.thread_id
            {
                return Ok(thread_id);
            }
            Err(error.into())
        }
    }
}

async fn find_message_by_nonce(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    nonce: &str,
) -> Result<Option<ExternalMessage>, serenity::Error> {
    let mut before = None;
    for _ in 0..MAX_NONCE_SEARCH_PAGES {
        let messages = discord.messages_page(channel_id, before).await?;
        if let Some(found) = messages
            .iter()
            .find(|message| message.nonce.as_deref() == Some(nonce))
        {
            return Ok(Some(found.clone()));
        }
        if messages.len() < 100 {
            return Ok(None);
        }
        let next = messages.last().map(|message| message.id);
        if next.is_none() || next == before {
            return Ok(None);
        }
        before = next;
    }
    // Fail closed rather than create a duplicate when the deterministic nonce could be older than
    // the bounded recovery window. A later reconciliation can retry after history is pruned.
    Err(serenity::Error::Other(
        "discord bounded nonce history search exhausted",
    ))
}

async fn set_archived_with_retry(
    discord: &dyn DiscordApi,
    thread_id: ChannelId,
    archived: bool,
) -> Result<(), serenity::Error> {
    let mut last_error = None;
    for attempt in 0..3 {
        match discord.set_thread_archived(thread_id, archived).await {
            Ok(()) => return Ok(()),
            Err(error) if is_not_found(&error) => return Err(error),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(retry_delay(attempt, thread_id.get())).await;
    }
    Err(last_error.expect("at least one Discord attempt was made"))
}

async fn start_worker(discord: Arc<dyn DiscordApi>, state: Arc<BotState>, session_id: String) {
    let mut workers = state.workers.lock().await;
    if workers
        .get(&session_id)
        .is_some_and(|worker| !worker.is_finished())
    {
        return;
    }
    if let Some(finished) = workers.remove(&session_id) {
        let _ = finished.await;
    }
    let key = session_id.clone();
    let worker_state = Arc::clone(&state);
    workers.insert(
        key.clone(),
        tokio::spawn(async move {
            watch_session_bridge(discord, worker_state, session_id).await;
        }),
    );
}

async fn stop_worker(state: &BotState, session_id: &str) {
    if let Some(handle) = state.workers.lock().await.remove(session_id) {
        handle.abort();
        let _ = handle.await;
    }
}

async fn watch_session_bridge(
    discord: Arc<dyn DiscordApi>,
    state: Arc<BotState>,
    session_id: String,
) {
    let mut updates = state
        .client
        .watch_hydrated_session(session_id.clone(), 1_024);
    let mut shutdown = state.shutdown.clone();
    let mut terminal_reactions = HashSet::new();
    let mut watch_attempt = 0;
    let retry_identity = stable_retry_identity(&session_id);
    loop {
        let update = tokio::select! {
            _ = shutdown.changed() => return,
            update = updates.next() => update,
        };
        let mut hydrated = match update {
            Some(Ok(hydrated)) => {
                watch_attempt = 0;
                hydrated
            }
            Some(Err(error)) => {
                eprintln!(
                    "nakode discord: session bridge watch reconnecting for {} ({})",
                    short_identity(&session_id),
                    sanitized_sdk_error(&error)
                );
                if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                    return;
                }
                watch_attempt = watch_attempt.saturating_add(1);
                updates = state
                    .client
                    .watch_hydrated_session(session_id.clone(), 1_024);
                continue;
            }
            None => {
                if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                    return;
                }
                watch_attempt = watch_attempt.saturating_add(1);
                updates = state
                    .client
                    .watch_hydrated_session(session_id.clone(), 1_024);
                continue;
            }
        };
        tokio::select! {
            _ = shutdown.changed() => return,
            () = tokio::time::sleep(SNAPSHOT_DEBOUNCE) => {}
        }
        while let Ok(Some(next)) = updates.try_next().await {
            hydrated = next;
        }
        let Some(bridge) = state.current_bridge(&session_id).await else {
            return;
        };
        if bridge.lifecycle != api::BridgeLifecycle::Open as i32 {
            return;
        }
        let Some(thread_id) = bridge
            .external_thread_id
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .map(ChannelId::new)
        else {
            continue;
        };

        if let Err(error) = project_session_update(
            &*discord,
            &state,
            thread_id,
            &bridge,
            &hydrated,
            &mut terminal_reactions,
        )
        .await
        {
            eprintln!(
                "nakode discord: outbound projection deferred for session {} ({})",
                short_identity(&session_id),
                sanitized_bridge_error(&error)
            );
            tokio::time::sleep(RECONCILE_RETRY_DELAY).await;
        }
    }
}

async fn project_session_update(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    hydrated: &HydratedSession,
    terminal_reactions: &mut HashSet<String>,
) -> Result<(), DiscordError> {
    if let Some(turn) = &hydrated.state.active_turn
        && let Some(body) = assistant_body_for_turn(hydrated, &turn.id, false)
    {
        project_live(discord, state, thread_id, bridge, &turn.id, &body).await?;
    }

    let mut cursor = bridge.last_delivered_turn_id.as_deref();
    let answers = completed_answers(hydrated);
    let transcript_has_earlier = hydrated
        .state
        .transcript
        .as_ref()
        .is_some_and(|transcript| transcript.has_earlier);
    let mut recovered = false;
    let start = match cursor {
        None => 0,
        Some(turn_id) => match answers.iter().position(|answer| answer.turn_id == turn_id) {
            Some(index) => index + 1,
            None if transcript_has_earlier => {
                recover_answers_after_cursor(
                    discord,
                    state,
                    thread_id,
                    bridge,
                    turn_id,
                    hydrated
                        .state
                        .active_turn
                        .as_ref()
                        .map(|turn| turn.id.as_str()),
                )
                .await?;
                recovered = true;
                0
            }
            None if answers.is_empty() => 0,
            None => return Err(DiscordError::DeliveryCursorUnavailable),
        },
    };
    if !recovered {
        for answer in answers.iter().skip(start) {
            if cursor == Some(answer.turn_id.as_str()) {
                continue;
            }
            deliver_final(discord, state, thread_id, bridge, answer).await?;
            cursor = Some(&answer.turn_id);
        }
    }

    if let Some(turn) = &hydrated.state.last_turn
        && matches!(
            api::TurnStatus::try_from(turn.status),
            Ok(api::TurnStatus::Failed | api::TurnStatus::Interrupted)
        )
        && terminal_reactions.insert(turn.id.clone())
    {
        react_source(discord, thread_id, bridge, REACTION_FAILED).await?;
        if let Some(message_id) = bridge
            .live_external_message_id
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .map(MessageId::new)
        {
            discord
                .react(thread_id, message_id, REACTION_FAILED)
                .await?;
        }
        set_live_message(state, bridge, None, None).await?;
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
struct RecoveryEntry {
    id: String,
    turn_id: String,
    body: String,
    body_start_byte: u64,
    body_total_bytes: u64,
}

struct RecoverySpool {
    directory: PathBuf,
    entries: usize,
}

impl RecoverySpool {
    fn new(root: &Path, session_id: &str) -> Result<Self, DiscordError> {
        prepare_private_directory(root)?;
        let directory = root.join(&hex_digest(session_id.as_bytes())[..32]);
        if directory.exists() {
            std::fs::remove_dir_all(&directory).map_err(|source| io_error(&directory, source))?;
        }
        prepare_private_directory(&directory)?;
        Ok(Self {
            directory,
            entries: 0,
        })
    }

    fn push(&mut self, entry: &api::TranscriptEntry, turn_id: &str) -> Result<(), DiscordError> {
        let marker = self
            .directory
            .join(format!("turn-{}", hex_digest(turn_id.as_bytes())));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
        {
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
            Err(source) => return Err(io_error(&marker, source)),
        }
        let stored = RecoveryEntry {
            id: entry.id.clone(),
            turn_id: turn_id.to_owned(),
            body: entry.body.clone(),
            body_start_byte: entry.body_start_byte,
            body_total_bytes: entry.body_total_bytes,
        };
        let encoded = serde_json::to_vec(&stored).map_err(|_| {
            DiscordError::InvalidConfig("could not spool transcript recovery metadata".to_owned())
        })?;
        let path = self
            .directory
            .join(format!("entry-{:020}.json", self.entries));
        atomic_write(&path, &encoded)?;
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    fn oldest_first(&self) -> impl Iterator<Item = Result<RecoveryEntry, DiscordError>> + '_ {
        (0..self.entries).rev().map(|index| {
            let path = self.directory.join(format!("entry-{index:020}.json"));
            let encoded = std::fs::read(&path).map_err(|source| io_error(&path, source))?;
            serde_json::from_slice(&encoded).map_err(|_| {
                DiscordError::InvalidConfig(
                    "invalid private transcript recovery metadata".to_owned(),
                )
            })
        })
    }
}

impl Drop for RecoverySpool {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct CompletedAnswer {
    turn_id: String,
    body: String,
}

fn completed_answers(hydrated: &HydratedSession) -> Vec<CompletedAnswer> {
    let active_turn = hydrated
        .state
        .active_turn
        .as_ref()
        .map(|turn| turn.id.as_str());
    let mut answers = Vec::<CompletedAnswer>::new();
    let Some(transcript) = &hydrated.state.transcript else {
        return answers;
    };
    for entry in &transcript.entries {
        if entry.kind != api::TranscriptEntryKind::Assistant as i32
            || entry.status != api::TranscriptEntryStatus::Complete as i32
        {
            continue;
        }
        let Some(turn_id) = entry.owner_turn_id.as_deref() else {
            continue;
        };
        if active_turn == Some(turn_id) {
            continue;
        }
        if let Some(existing) = answers.iter_mut().find(|answer| answer.turn_id == turn_id) {
            existing.body.clone_from(&entry.body);
        } else {
            answers.push(CompletedAnswer {
                turn_id: turn_id.to_owned(),
                body: entry.body.clone(),
            });
        }
    }
    if answers.is_empty()
        && let Some(turn) = &hydrated.state.last_turn
        && turn.status == api::TurnStatus::Completed as i32
        && let Some(entry) = transcript
            .entries
            .iter()
            .rev()
            .find(|entry| entry.kind == api::TranscriptEntryKind::Assistant as i32)
    {
        answers.push(CompletedAnswer {
            turn_id: turn.id.clone(),
            body: entry.body.clone(),
        });
    }
    answers
}

async fn recover_answers_after_cursor(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    cursor: &str,
    active_turn: Option<&str>,
) -> Result<(), DiscordError> {
    let mut spool = RecoverySpool::new(&state.recovery_root, &bridge.session_id)?;
    let mut before_entry_id = None;
    let mut found_cursor = false;
    loop {
        let page = state
            .client
            .get_transcript_page(api::GetTranscriptPageRequest {
                owner_kind: api::TranscriptOwnerKind::Session as i32,
                owner_id: bridge.session_id.clone(),
                before_entry_id: before_entry_id.clone(),
                limit: 256,
            })
            .await?;
        let next_before = page.entries.first().map(|entry| entry.id.clone());
        for entry in page.entries.iter().rev() {
            if entry.kind != api::TranscriptEntryKind::Assistant as i32
                || entry.status != api::TranscriptEntryStatus::Complete as i32
            {
                continue;
            }
            let Some(turn_id) = entry.owner_turn_id.as_deref() else {
                continue;
            };
            if turn_id == cursor {
                found_cursor = true;
                break;
            }
            if active_turn != Some(turn_id) {
                spool.push(entry, turn_id)?;
            }
        }
        if found_cursor {
            break;
        }
        if !page.has_earlier || page.entries.is_empty() || next_before == before_entry_id {
            return Err(DiscordError::DeliveryCursorUnavailable);
        }
        before_entry_id = next_before;
    }

    for stored in spool.oldest_first() {
        let stored = stored?;
        let mut entry = api::TranscriptEntry {
            id: stored.id,
            body: stored.body,
            body_start_byte: stored.body_start_byte,
            body_total_bytes: stored.body_total_bytes,
            ..api::TranscriptEntry::default()
        };
        state
            .client
            .hydrate_transcript_entry(
                api::TranscriptOwnerKind::Session,
                &bridge.session_id,
                &mut entry,
            )
            .await?;
        deliver_final(
            discord,
            state,
            thread_id,
            bridge,
            &CompletedAnswer {
                turn_id: stored.turn_id,
                body: entry.body,
            },
        )
        .await?;
    }
    Ok(())
}

fn assistant_body_for_turn(
    hydrated: &HydratedSession,
    turn_id: &str,
    complete_only: bool,
) -> Option<String> {
    hydrated
        .state
        .transcript
        .as_ref()?
        .entries
        .iter()
        .rev()
        .find(|entry| {
            entry.kind == api::TranscriptEntryKind::Assistant as i32
                && entry.owner_turn_id.as_deref() == Some(turn_id)
                && (!complete_only || entry.status == api::TranscriptEntryStatus::Complete as i32)
        })
        .map(|entry| entry.body.clone())
}

async fn project_live(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    turn_id: &str,
    body: &str,
) -> Result<(), DiscordError> {
    let safe_body = visible_discord_content(body);
    let preview = DiscordChunks::new(&safe_body)
        .next()
        .unwrap_or_else(|| "…".to_owned());
    let existing = if bridge.live_turn_id.as_deref() == Some(turn_id) {
        bridge
            .live_external_message_id
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .map(MessageId::new)
    } else {
        None
    };
    let message_id = if let Some(message_id) = existing {
        match discord.edit_message(thread_id, message_id, &preview).await {
            Ok(()) => message_id,
            Err(error) if is_not_found(&error) => {
                set_live_message(state, bridge, None, None).await?;
                create_or_recover_live_message(discord, thread_id, bridge, turn_id, &preview)
                    .await?
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        create_or_recover_live_message(discord, thread_id, bridge, turn_id, &preview).await?
    };
    set_live_message(
        state,
        bridge,
        Some(turn_id.to_owned()),
        Some(message_id.get().to_string()),
    )
    .await?;
    discord.react(thread_id, message_id, REACTION_LIVE).await?;
    Ok(())
}

async fn create_or_recover_live_message(
    discord: &dyn DiscordApi,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    turn_id: &str,
    preview: &str,
) -> Result<MessageId, DiscordError> {
    let nonce = live_nonce(&bridge.session_id, turn_id);
    if let Some(message) = find_message_by_nonce(discord, thread_id, &nonce).await? {
        match discord.edit_message(thread_id, message.id, preview).await {
            Ok(()) => return Ok(message.id),
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(discord
        .send_message(thread_id, preview, Some(&nonce))
        .await?
        .id)
}

async fn set_live_message(
    state: &BotState,
    bridge: &api::SessionBridge,
    turn_id: Option<String>,
    external_message_id: Option<String>,
) -> Result<(), DiscordError> {
    state
        .client
        .set_bridge_live_message(api::SetBridgeLiveMessageRequest {
            mutation: None,
            session_id: bridge.session_id.clone(),
            turn_id: turn_id.clone(),
            external_message_id: external_message_id.clone(),
        })
        .await?;
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        current.live_turn_id = turn_id;
        current.live_external_message_id = external_message_id;
    }
    Ok(())
}

async fn deliver_final(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    projected_bridge: &api::SessionBridge,
    answer: &CompletedAnswer,
) -> Result<(), DiscordError> {
    let current_bridge = state
        .current_bridge(&projected_bridge.session_id)
        .await
        .unwrap_or_else(|| projected_bridge.clone());
    let bridge = &current_bridge;
    if bridge.last_delivered_turn_id.as_deref() == Some(answer.turn_id.as_str()) {
        return Ok(());
    }
    let safe_body = visible_discord_content(&answer.body);
    let body_sha256 = hex_digest(safe_body.as_bytes());
    // Count with the same streaming chunker used below, without retaining all chunk text or
    // per-part metadata in memory or in the authoritative projection.
    let part_count = u64::try_from(DiscordChunks::new(&safe_body).count()).map_err(|_| {
        DiscordError::InvalidConfig("final answer has too many Discord chunks".to_owned())
    })?;

    let pending = bridge.delivery.as_ref().filter(|delivery| {
        delivery.turn_id == answer.turn_id
            && delivery.body_sha256 == body_sha256
            && delivery.part_count == part_count
    });
    if bridge.delivery.is_some() && pending.is_none() {
        return Err(DiscordError::InvalidConfig(
            "a different final delivery is already pending".to_owned(),
        ));
    }
    if pending.is_none() {
        state
            .client
            .prepare_bridge_delivery(api::PrepareBridgeDeliveryRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: answer.turn_id.clone(),
                body_sha256: body_sha256.clone(),
                part_count,
            })
            .await?;
    }
    let completed_parts = pending.map_or(0, |delivery| delivery.completed_parts);
    if completed_parts > part_count {
        return Err(DiscordError::InvalidConfig(
            "invalid final delivery progress".to_owned(),
        ));
    }

    for (index, chunk) in DiscordChunks::new(&safe_body).enumerate() {
        let part_index = u64::try_from(index).map_err(|_| {
            DiscordError::InvalidConfig("final answer has too many Discord chunks".to_owned())
        })?;
        if part_index < completed_parts {
            continue;
        }
        let nonce = final_nonce(&bridge.session_id, &answer.turn_id, index);
        let message_id = if index == 0
            && bridge.live_turn_id.as_deref() == Some(answer.turn_id.as_str())
            && bridge.live_external_message_id.is_some()
        {
            let live_message_id = bridge
                .live_external_message_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .map(MessageId::new)
                .ok_or_else(|| {
                    DiscordError::InvalidConfig("invalid live Discord message identity".to_owned())
                })?;
            match discord
                .edit_message(thread_id, live_message_id, &chunk)
                .await
            {
                Ok(()) => live_message_id,
                Err(error) if is_not_found(&error) => {
                    set_live_message(state, bridge, None, None).await?;
                    send_or_recover_final_part(discord, thread_id, &nonce, &chunk).await?
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            send_or_recover_final_part(discord, thread_id, &nonce, &chunk).await?
        };
        // Reactions precede the durable part checkpoint. If a Discord mutation or the checkpoint
        // response fails, the deterministic nonce makes the whole part safe to retry.
        if let Err(error) = discord
            .remove_own_reaction(thread_id, message_id, REACTION_LIVE)
            .await
            && !is_not_found(&error)
        {
            return Err(error.into());
        }
        discord
            .react(thread_id, message_id, REACTION_COMPLETED)
            .await?;
        state
            .client
            .complete_bridge_delivery_part(api::CompleteBridgeDeliveryPartRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: answer.turn_id.clone(),
                part_index,
                external_message_id: message_id.get().to_string(),
            })
            .await?;
    }
    react_source(discord, thread_id, bridge, REACTION_COMPLETED).await?;
    state
        .client
        .finalize_bridge_delivery(api::FinalizeBridgeDeliveryRequest {
            mutation: None,
            session_id: bridge.session_id.clone(),
            turn_id: answer.turn_id.clone(),
        })
        .await?;
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        current.last_delivered_turn_id = Some(answer.turn_id.clone());
        current.delivery = None;
        current.live_turn_id = None;
        current.live_external_message_id = None;
        current.active_source_message_id = None;
    }
    Ok(())
}

async fn send_or_recover_final_part(
    discord: &dyn DiscordApi,
    thread_id: ChannelId,
    nonce: &str,
    chunk: &str,
) -> Result<MessageId, DiscordError> {
    if let Some(message) = find_message_by_nonce(discord, thread_id, nonce).await? {
        match discord.edit_message(thread_id, message.id, chunk).await {
            Ok(()) => return Ok(message.id),
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(discord
        .send_message(thread_id, chunk, Some(nonce))
        .await?
        .id)
}

async fn react_source(
    discord: &dyn DiscordApi,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    reaction: &str,
) -> Result<(), DiscordError> {
    let Some(message_id) = bridge
        .active_source_message_id
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .map(MessageId::new)
    else {
        return Ok(());
    };
    discord
        .remove_own_reaction(thread_id, message_id, REACTION_ACCEPTED)
        .await?;
    discord.react(thread_id, message_id, reaction).await?;
    Ok(())
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let _ = self.state.bot_user_id.set(ready.user.id);
        let discord: Arc<dyn DiscordApi> = Arc::new(SerenityDiscordApi {
            http: Arc::clone(&ctx.http),
        });
        start_reconciler(Arc::clone(&discord), Arc::clone(&self.state)).await;
        start_ingress_replayer(discord, Arc::clone(&self.state)).await;
    }

    async fn message(&self, ctx: Context, message: Message) {
        if message.author.bot
            || message.webhook_id.is_some()
            || !self.state.config.is_primary_user(message.author.id)
        {
            return;
        }
        let session_id = self
            .state
            .thread_routes
            .read()
            .await
            .get(&message.channel_id.get())
            .cloned();
        let Some(session_id) = session_id else {
            return;
        };
        let Some(bridge) = self.state.current_bridge(&session_id).await else {
            return;
        };
        if bridge.lifecycle != api::BridgeLifecycle::Open as i32
            || bridge.transport.as_deref() != Some(TRANSPORT_NAME)
            || bridge.external_thread_id.as_deref() != Some(&message.channel_id.get().to_string())
        {
            return;
        }
        let discord = SerenityDiscordApi {
            http: Arc::clone(&ctx.http),
        };
        let slot = self.state.inbound_slots.try_acquire().ok();
        let proposed = IngressRecord::from_message(session_id, &message, slot.is_none());
        let record = match self.state.ingress.enqueue(&proposed) {
            Ok(Some(record)) => record,
            Ok(None) => return,
            Err(error) => {
                let _ = discord
                    .react(message.channel_id, message.id, REACTION_FAILED)
                    .await;
                eprintln!(
                    "nakode discord: durable ingress checkpoint failed for session {} ({})",
                    short_identity(&bridge.session_id),
                    sanitized_bridge_error(&error)
                );
                return;
            }
        };
        if record.forced_busy || slot.is_none() {
            mark_message_busy(&discord, message.channel_id, message.id).await;
            self.state.ingress_notify.notify_one();
            return;
        }
        if !claim_ingress(&self.state, &record.message_id).await {
            return;
        }
        let outcome = process_ingress_record(&discord, &self.state, &record).await;
        settle_ingress(&self.state, &record, outcome).await;
        self.state
            .ingress_inflight
            .lock()
            .await
            .remove(&record.message_id);
        self.state.ingress_notify.notify_one();
    }
}

enum IngressProcessOutcome {
    Terminal,
    TerminalMultipart(String),
    Deferred,
    WaitingMultipart,
}

async fn process_ingress_record(
    discord: &dyn DiscordApi,
    state: &BotState,
    record: &IngressRecord,
) -> IngressProcessOutcome {
    if record.version != INGRESS_SCHEMA_VERSION
        || state.config.primary_user_id.as_deref() != Some(record.author_id.as_str())
    {
        return IngressProcessOutcome::Terminal;
    }
    let Some(bridge) = state.current_bridge(&record.session_id).await else {
        return IngressProcessOutcome::Terminal;
    };
    if bridge.lifecycle != api::BridgeLifecycle::Open as i32
        || bridge.transport.as_deref() != Some(TRANSPORT_NAME)
        || bridge.external_thread_id.as_deref() != Some(record.thread_id.as_str())
    {
        // A stale local ingress item cannot reopen, rebind, or otherwise mutate a closed session.
        return IngressProcessOutcome::Terminal;
    }
    if record.forced_busy {
        return consume_record_as_busy(discord, state, &bridge, record, REACTION_BUSY).await;
    }
    if record.multipart_group.is_some()
        && unix_time_ms().saturating_sub(record.received_at_ms)
            > u64::try_from(MULTIPART_TTL.as_millis()).unwrap_or(u64::MAX)
    {
        return consume_record_as_busy(discord, state, &bridge, record, REACTION_FAILED).await;
    }
    if let Some(parsed) = parse_multipart(&record.content) {
        handle_multipart_record(discord, state, &bridge, record, parsed).await
    } else {
        handle_prompt_record(discord, state, &bridge, record).await
    }
}

async fn consume_record_as_busy(
    discord: &dyn DiscordApi,
    state: &BotState,
    bridge: &api::SessionBridge,
    record: &IngressRecord,
    terminal_reaction: &str,
) -> IngressProcessOutcome {
    let request = api::ContinueSessionFromBridgeRequest {
        mutation: None,
        session_id: bridge.session_id.clone(),
        transport: TRANSPORT_NAME.to_owned(),
        external_thread_id: record.thread_id.clone(),
        external_event_id: record.message_id.clone(),
        source_message_id: record.message_id.clone(),
        prompt: Some(api::PromptInput {
            text: String::new(),
            attachments: Vec::new(),
        }),
        consume_as_busy: true,
    };
    let result = tokio::time::timeout(
        BRIDGE_RPC_TIMEOUT,
        state.client.continue_session_from_bridge(request),
    )
    .await;
    match result {
        Ok(Ok(response))
            if matches!(
                api::BridgeContinuationDisposition::try_from(response.disposition),
                Ok(api::BridgeContinuationDisposition::Busy
                    | api::BridgeContinuationDisposition::Duplicate)
            ) =>
        {
            let channel_id = record.thread_id.parse::<u64>().ok().map(ChannelId::new);
            let message_id = record.message_id.parse::<u64>().ok().map(MessageId::new);
            if let (Some(channel_id), Some(message_id)) = (channel_id, message_id) {
                let _ = discord
                    .remove_own_reaction(channel_id, message_id, REACTION_FAILED)
                    .await;
                let _ = discord
                    .react(channel_id, message_id, terminal_reaction)
                    .await;
                if terminal_reaction == REACTION_BUSY {
                    let nonce = busy_nonce(message_id);
                    let _ = discord
                        .send_message(
                            channel_id,
                            "❌ This session is busy or closed. Wait for the active turn to finish, then send a new message.",
                            Some(&nonce),
                        )
                        .await;
                } else if terminal_reaction == REACTION_FAILED {
                    let nonce = failed_nonce(message_id);
                    let _ = discord
                        .send_message(
                            channel_id,
                            "⚠️ This message could not be accepted safely. Check its multipart syntax or attachments, then send a new message.",
                            Some(&nonce),
                        )
                        .await;
                }
            }
            IngressProcessOutcome::Terminal
        }
        Ok(Err(error)) => {
            eprintln!(
                "nakode discord: durable inbound rejection deferred for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_sdk_error(&error)
            );
            IngressProcessOutcome::Deferred
        }
        Ok(Ok(_)) => {
            eprintln!(
                "nakode discord: durable inbound rejection returned an invalid disposition for session {}",
                short_identity(&bridge.session_id)
            );
            IngressProcessOutcome::Deferred
        }
        Err(_) => {
            eprintln!(
                "nakode discord: durable inbound rejection timed out for session {}",
                short_identity(&bridge.session_id)
            );
            IngressProcessOutcome::Deferred
        }
    }
}

async fn handle_prompt_record(
    discord: &dyn DiscordApi,
    state: &BotState,
    bridge: &api::SessionBridge,
    record: &IngressRecord,
) -> IngressProcessOutcome {
    let prompt = match prompt_from_record(state, record).await {
        Ok(prompt) => prompt,
        Err(error) => {
            if let (Some(channel_id), Some(message_id)) = (
                record.thread_id.parse::<u64>().ok().map(ChannelId::new),
                record.message_id.parse::<u64>().ok().map(MessageId::new),
            ) {
                let _ = discord.react(channel_id, message_id, REACTION_FAILED).await;
            }
            eprintln!(
                "nakode discord: rejected inbound attachment for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_bridge_error(&error)
            );
            return consume_record_as_busy(discord, state, bridge, record, REACTION_FAILED).await;
        }
    };
    submit_bridge_record(
        discord,
        state,
        bridge,
        record,
        record.message_id.clone(),
        record.message_id.clone(),
        prompt,
    )
    .await
}

async fn handle_multipart_record(
    discord: &dyn DiscordApi,
    state: &BotState,
    bridge: &api::SessionBridge,
    record: &IngressRecord,
    parsed: Result<MultipartPart<'_>, DiscordError>,
) -> IngressProcessOutcome {
    let channel_id = record.thread_id.parse::<u64>().ok().map(ChannelId::new);
    let message_id = record.message_id.parse::<u64>().ok().map(MessageId::new);
    let part = match parsed {
        Ok(part) if record.attachments.is_empty() => part,
        Ok(_) => {
            if let (Some(channel_id), Some(message_id)) = (channel_id, message_id) {
                let _ = discord.react(channel_id, message_id, REACTION_FAILED).await;
            }
            return consume_record_as_busy(discord, state, bridge, record, REACTION_FAILED).await;
        }
        Err(error) => {
            if let (Some(channel_id), Some(message_id)) = (channel_id, message_id) {
                let _ = discord.react(channel_id, message_id, REACTION_FAILED).await;
            }
            eprintln!(
                "nakode discord: rejected multipart prompt for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_bridge_error(&error)
            );
            return consume_record_as_busy(discord, state, bridge, record, REACTION_FAILED).await;
        }
    };
    let Some(message_id) = message_id else {
        return IngressProcessOutcome::Terminal;
    };
    match state
        .multipart
        .accept(&bridge.session_id, message_id, part)
        .await
    {
        Ok(MultipartOutcome::Waiting) => {
            if let Some(channel_id) = channel_id {
                let _ = discord
                    .react(channel_id, message_id, REACTION_ACCEPTED)
                    .await;
            }
            IngressProcessOutcome::WaitingMultipart
        }
        Ok(MultipartOutcome::Duplicate) => {
            if let Some(channel_id) = channel_id {
                let _ = discord
                    .react(channel_id, message_id, REACTION_ACCEPTED)
                    .await;
            }
            IngressProcessOutcome::Terminal
        }
        Ok(MultipartOutcome::Complete {
            group,
            text,
            event_id,
            source_message_id,
        }) => {
            let outcome = submit_bridge_record(
                discord,
                state,
                bridge,
                record,
                event_id,
                source_message_id,
                api::PromptInput {
                    text,
                    attachments: Vec::new(),
                },
            )
            .await;
            match outcome {
                IngressProcessOutcome::Terminal => IngressProcessOutcome::TerminalMultipart(group),
                other => other,
            }
        }
        Err(error) => {
            if let Some(channel_id) = channel_id {
                let _ = discord.react(channel_id, message_id, REACTION_FAILED).await;
            }
            eprintln!(
                "nakode discord: multipart assembly failed for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_bridge_error(&error)
            );
            consume_record_as_busy(discord, state, bridge, record, REACTION_FAILED).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn submit_bridge_record(
    discord: &dyn DiscordApi,
    state: &BotState,
    bridge: &api::SessionBridge,
    record: &IngressRecord,
    external_event_id: String,
    source_message_id: String,
    prompt: api::PromptInput,
) -> IngressProcessOutcome {
    let Some(channel_id) = record.thread_id.parse::<u64>().ok().map(ChannelId::new) else {
        return IngressProcessOutcome::Terminal;
    };
    let reaction_message_id = source_message_id
        .parse::<u64>()
        .ok()
        .map(MessageId::new)
        .or_else(|| record.message_id.parse::<u64>().ok().map(MessageId::new));
    let Some(reaction_message_id) = reaction_message_id else {
        return IngressProcessOutcome::Terminal;
    };
    let accepted_source_message_id = source_message_id.clone();
    let request = api::ContinueSessionFromBridgeRequest {
        mutation: None,
        session_id: bridge.session_id.clone(),
        transport: TRANSPORT_NAME.to_owned(),
        external_thread_id: record.thread_id.clone(),
        external_event_id,
        source_message_id,
        prompt: Some(prompt),
        consume_as_busy: false,
    };
    // Feedback follows the durable local ingress checkpoint and precedes the serialized Nakode
    // claim. A readiness race replaces this optimistic reaction with the documented busy state.
    let _ = discord
        .remove_own_reaction(channel_id, reaction_message_id, REACTION_FAILED)
        .await;
    let _ = discord
        .react(channel_id, reaction_message_id, REACTION_ACCEPTED)
        .await;
    let response = tokio::time::timeout(
        BRIDGE_RPC_TIMEOUT,
        state.client.continue_session_from_bridge(request),
    )
    .await;
    match response {
        Ok(Ok(response)) => {
            match api::BridgeContinuationDisposition::try_from(response.disposition)
                .unwrap_or(api::BridgeContinuationDisposition::Unspecified)
            {
                api::BridgeContinuationDisposition::Accepted => {
                    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
                        current.active_source_message_id = Some(accepted_source_message_id);
                    }
                    IngressProcessOutcome::Terminal
                }
                api::BridgeContinuationDisposition::Duplicate => {
                    let still_active = state
                        .current_bridge(&bridge.session_id)
                        .await
                        .and_then(|current| current.active_source_message_id)
                        .as_deref()
                        == Some(accepted_source_message_id.as_str());
                    if !still_active {
                        let _ = discord
                            .remove_own_reaction(channel_id, reaction_message_id, REACTION_ACCEPTED)
                            .await;
                    }
                    IngressProcessOutcome::Terminal
                }
                api::BridgeContinuationDisposition::Busy => {
                    mark_message_busy(discord, channel_id, reaction_message_id).await;
                    IngressProcessOutcome::Terminal
                }
                api::BridgeContinuationDisposition::Unspecified => {
                    mark_ingress_deferred(discord, channel_id, reaction_message_id).await;
                    IngressProcessOutcome::Deferred
                }
            }
        }
        Ok(Err(error)) => {
            mark_ingress_deferred(discord, channel_id, reaction_message_id).await;
            eprintln!(
                "nakode discord: inbound continuation deferred for session {} ({})",
                short_identity(&bridge.session_id),
                sanitized_sdk_error(&error)
            );
            IngressProcessOutcome::Deferred
        }
        Err(_) => {
            mark_ingress_deferred(discord, channel_id, reaction_message_id).await;
            eprintln!(
                "nakode discord: inbound continuation timed out for session {}",
                short_identity(&bridge.session_id)
            );
            IngressProcessOutcome::Deferred
        }
    }
}

async fn mark_ingress_deferred(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    message_id: MessageId,
) {
    let _ = discord
        .remove_own_reaction(channel_id, message_id, REACTION_ACCEPTED)
        .await;
    let _ = discord.react(channel_id, message_id, REACTION_FAILED).await;
}

async fn mark_message_busy(discord: &dyn DiscordApi, channel_id: ChannelId, message_id: MessageId) {
    let _ = discord
        .remove_own_reaction(channel_id, message_id, REACTION_ACCEPTED)
        .await;
    let _ = discord.react(channel_id, message_id, REACTION_BUSY).await;
    let nonce = busy_nonce(message_id);
    let _ = discord
        .send_message(
            channel_id,
            "❌ This session is busy or closed. Wait for the active turn to finish, then send a new message.",
            Some(&nonce),
        )
        .await;
}

async fn prompt_from_record(
    state: &BotState,
    record: &IngressRecord,
) -> Result<api::PromptInput, DiscordError> {
    let mut attachments = Vec::with_capacity(record.attachments.len());
    let mut total_attachment_bytes = 0usize;
    for attachment in &record.attachments {
        let converted = download_image(&state.http_client, attachment).await?;
        let bytes = match converted.source.as_ref() {
            Some(api::prompt_attachment::Source::InlineImage(image)) => image.data.len(),
            _ => 0,
        };
        total_attachment_bytes = total_attachment_bytes.saturating_add(bytes);
        if total_attachment_bytes > MAX_TOTAL_ATTACHMENT_BYTES {
            return Err(DiscordError::CombinedAttachmentsTooLarge);
        }
        attachments.push(converted);
    }
    let text = if record.content.trim().is_empty() && !attachments.is_empty() {
        "Please inspect the attached image(s).".to_owned()
    } else {
        record.content.clone()
    };
    if text.trim().is_empty() {
        return Err(DiscordError::InvalidConfig(
            "a prompt needs text or a supported image attachment".to_owned(),
        ));
    }
    Ok(api::PromptInput { text, attachments })
}

async fn connect_api(
    endpoint: PathBuf,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<NakodeClient>, DiscordError> {
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let attempt = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(None);
                }
                continue;
            }
            result = tokio::time::timeout(
                Duration::from_secs(2),
                NakodeClient::connect_unix(endpoint.clone()),
            ) => result,
        };
        match attempt {
            Ok(Ok(client)) => return Ok(Some(client)),
            Ok(Err(_)) | Err(_) => {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(None);
                        }
                    }
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

fn orchestrator_label(kind: i32) -> &'static str {
    match api::OrchestratorKind::try_from(kind) {
        Ok(api::OrchestratorKind::Chat) => "💬 Chat",
        Ok(api::OrchestratorKind::Agent) => "🛠️ Agent",
        _ => "Nakode",
    }
}

fn thread_title(kind: i32, display_title: &str) -> String {
    let title = format!("{} · {}", orchestrator_label(kind), display_title.trim());
    let bounded = title.chars().take(100).collect::<String>();
    if bounded.chars().count() < 2 {
        "Nakode session".to_owned()
    } else {
        bounded
    }
}

fn starter_nonce(session_id: &str) -> String {
    format!("nk-s-{}", &hex_digest(session_id.as_bytes())[..20])
}

fn live_nonce(session_id: &str, turn_id: &str) -> String {
    format!(
        "nk-l-{}",
        &hex_digest(format!("{session_id}:{turn_id}").as_bytes())[..20]
    )
}

fn final_nonce(session_id: &str, turn_id: &str, index: usize) -> String {
    format!(
        "nk-f-{}",
        &hex_digest(format!("{session_id}:{turn_id}:{index}").as_bytes())[..20]
    )
}

fn busy_nonce(message_id: MessageId) -> String {
    format!("nk-b-{:x}", message_id.get())
}

fn failed_nonce(message_id: MessageId) -> String {
    format!("nk-e-{:x}", message_id.get())
}

fn hex_digest(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn stable_retry_identity(value: &str) -> u64 {
    value.bytes().fold(1_469_598_103_934_665_603, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(1_099_511_628_211)
    })
}

async fn wait_for_reconnect(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    attempt: u32,
    identity: u64,
) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        _ = shutdown.changed() => false,
        () = tokio::time::sleep(retry_delay(attempt, identity)) => !*shutdown.borrow(),
    }
}

fn retry_delay(attempt: u32, identity: u64) -> Duration {
    let base = 200_u64.saturating_mul(1_u64 << attempt.min(4));
    Duration::from_millis(base + identity % 97)
}

fn is_not_found(error: &serenity::Error) -> bool {
    matches!(error, serenity::Error::Http(error) if error.status_code().is_some_and(|status| status.as_u16() == 404))
}

fn short_identity(identity: &str) -> &str {
    identity.get(..identity.len().min(12)).unwrap_or(identity)
}

fn sanitized_sdk_error(error: &SdkError) -> &'static str {
    match error {
        SdkError::InvalidEndpoint(_) => "invalid endpoint",
        SdkError::Status(status) => match status.code() {
            tonic::Code::Unavailable => "service unavailable",
            tonic::Code::DeadlineExceeded => "service timeout",
            tonic::Code::PermissionDenied => "permission denied",
            tonic::Code::Unimplemented => "capability unavailable",
            _ => "service rejected request",
        },
        SdkError::MissingState(_) => "missing authoritative state",
        SdkError::InvalidProjection(_) => "invalid authoritative projection",
    }
}

fn sanitized_bridge_error(error: &DiscordError) -> &'static str {
    match error {
        DiscordError::Sdk(error) => sanitized_sdk_error(error),
        DiscordError::Gateway(error) if is_not_found(error) => "Discord resource missing",
        DiscordError::Gateway(_) => "Discord request failed",
        DiscordError::Http(_) => "attachment request failed",
        DiscordError::AttachmentTooLarge { .. } => "attachment too large",
        DiscordError::CombinedAttachmentsTooLarge => "combined attachments too large",
        DiscordError::UnsupportedAttachment { .. } => "unsupported attachment",
        DiscordError::InvalidConfig(_) | DiscordError::InvalidId { .. } => "invalid bridge state",
        _ => "bridge operation failed",
    }
}

fn sanitize_mentions(text: &str) -> String {
    text.replace("@everyone", "@\u{200b}everyone")
        .replace("@here", "@\u{200b}here")
}

fn visible_discord_content(text: &str) -> String {
    let sanitized = sanitize_mentions(text);
    if sanitized.trim().is_empty() {
        "…".to_owned()
    } else {
        sanitized
    }
}

struct DiscordChunks<'a> {
    remaining: &'a str,
    open_fence: Option<String>,
    emitted_empty: bool,
}

impl<'a> DiscordChunks<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            remaining: text,
            open_fence: None,
            emitted_empty: false,
        }
    }
}

impl Iterator for DiscordChunks<'_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            if self.emitted_empty {
                return None;
            }
            self.emitted_empty = true;
            return Some("…".to_owned());
        }
        self.emitted_empty = true;
        let prefix = self
            .open_fence
            .as_ref()
            .map_or_else(String::new, |language| format!("```{language}\n"));
        // Reserve a closing fence even when this chunk begins outside one: it may open a fence.
        let closing_reserve = 4;
        let available = DISCORD_CHUNK_SIZE
            .saturating_sub(prefix.encode_utf16().count())
            .saturating_sub(closing_reserve)
            .max(1);
        let byte_limit = byte_index_after_utf16_units(self.remaining, available);
        let split_at = if byte_limit == self.remaining.len() {
            byte_limit
        } else {
            let preferred = preferred_split(&self.remaining[..byte_limit]).unwrap_or(byte_limit);
            avoid_splitting_backtick_run(self.remaining, preferred)
        };
        let raw = &self.remaining[..split_at];
        let next_fence = fence_after(self.open_fence.clone(), raw);
        let suffix = if next_fence.is_some() { "\n```" } else { "" };
        let chunk = format!("{prefix}{raw}{suffix}");
        self.remaining = &self.remaining[split_at..];
        self.open_fence = next_fence;
        Some(chunk)
    }
}

/// Splits text into ordered Discord messages while preserving UTF-8 boundaries and keeping fenced
/// code blocks renderable across continuations. No source character is discarded; synthetic fence
/// closers/reopeners are the only additions.
#[cfg(test)]
fn split_discord_content(text: &str) -> Vec<String> {
    DiscordChunks::new(text).collect()
}

fn byte_index_after_utf16_units(value: &str, maximum: usize) -> usize {
    let mut units = 0usize;
    for (index, character) in value.char_indices() {
        let next = units.saturating_add(character.len_utf16());
        if next > maximum {
            return index;
        }
        units = next;
    }
    value.len()
}

fn avoid_splitting_backtick_run(value: &str, split_at: usize) -> usize {
    let bytes = value.as_bytes();
    let mut start = split_at;
    while start > 0 && bytes[start - 1] == b'`' {
        start -= 1;
    }
    let mut end = split_at;
    while end < bytes.len() && bytes[end] == b'`' {
        end += 1;
    }
    if end.saturating_sub(start) >= 3 && start > 0 {
        start
    } else {
        split_at
    }
}

fn preferred_split(candidate: &str) -> Option<usize> {
    let minimum = candidate.encode_utf16().count() / 2;
    candidate
        .char_indices()
        .filter_map(|(index, character)| {
            (character == '\n' || character.is_whitespace()).then_some(index + character.len_utf8())
        })
        .rfind(|index| candidate[..*index].encode_utf16().count() >= minimum)
}

fn fence_after(mut open: Option<String>, value: &str) -> Option<String> {
    let mut cursor = 0;
    while let Some(relative) = value[cursor..].find("```") {
        let marker_end = cursor + relative + 3;
        if open.is_some() {
            open = None;
        } else {
            let language = value[marker_end..]
                .split_once('\n')
                .map_or("", |(line, _)| line)
                .trim();
            open = Some(
                language
                    .chars()
                    .take(32)
                    .filter(|character| !character.is_control())
                    .collect(),
            );
        }
        cursor = marker_end;
    }
    open
}

fn is_approved_discord_cdn_url(url: &reqwest::Url) -> bool {
    url.scheme() == "https"
        && matches!(
            url.host_str(),
            Some("cdn.discordapp.com" | "media.discordapp.net")
        )
}

async fn download_image(
    client: &HttpClient,
    attachment: &IngressAttachment,
) -> Result<api::PromptAttachment, DiscordError> {
    if attachment.size > MAX_ATTACHMENT_BYTES as u64 {
        return Err(DiscordError::AttachmentTooLarge {
            name: attachment.filename.clone(),
        });
    }
    let url =
        reqwest::Url::parse(&attachment.url).map_err(|_| DiscordError::UnsupportedAttachment {
            name: attachment.filename.clone(),
        })?;
    if !is_approved_discord_cdn_url(&url) {
        return Err(DiscordError::UnsupportedAttachment {
            name: attachment.filename.clone(),
        });
    }
    let media_type = attachment
        .content_type
        .as_deref()
        .filter(|content_type| content_type.starts_with("image/"))
        .map(|content_type| {
            content_type
                .split(';')
                .next()
                .unwrap_or(content_type)
                .to_owned()
        })
        .ok_or_else(|| DiscordError::UnsupportedAttachment {
            name: attachment.filename.clone(),
        })?;
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(DiscordError::UnsupportedAttachment {
            name: attachment.filename.clone(),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ATTACHMENT_BYTES as u64)
    {
        return Err(DiscordError::AttachmentTooLarge {
            name: attachment.filename.clone(),
        });
    }
    let mut data = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.try_next().await? {
        if data.len().saturating_add(chunk.len()) > MAX_ATTACHMENT_BYTES {
            return Err(DiscordError::AttachmentTooLarge {
                name: attachment.filename.clone(),
            });
        }
        data.extend_from_slice(&chunk);
    }
    Ok(api::PromptAttachment {
        label: attachment.filename.clone(),
        source: Some(api::prompt_attachment::Source::InlineImage(
            api::InlineImage { media_type, data },
        )),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Barrier, Mutex},
        time::Duration,
    };

    use serenity::all::{ChannelId, MessageId, UserId};

    use super::{
        CONFIG_VERSION, DiscordApi, DiscordConfig, DiscordConfigStore, DiscordError,
        ExternalMessage, IngressAttachment, IngressRecord, IngressSpool,
        MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION, MultipartAssembler, MultipartOutcome,
        RecoverySpool, busy_nonce, connect_api, create_or_recover_thread, failed_nonce,
        final_nonce, find_message_by_nonce, is_approved_discord_cdn_url, parse_multipart,
        reconciliation_snapshot, sanitize_mentions, send_or_recover_final_part,
        split_discord_content, starter_nonce, thread_title, validate_snowflake,
        visible_discord_content,
    };
    use nakode_sdk::v1 as api;

    fn enabled_config() -> DiscordConfig {
        DiscordConfig {
            version: CONFIG_VERSION,
            enabled: true,
            chat_channel_id: Some("43".to_owned()),
            agent_channel_id: Some("44".to_owned()),
            primary_user_id: Some("42".to_owned()),
        }
    }

    fn bridge() -> api::SessionBridge {
        api::SessionBridge {
            session_id: "session-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            kind: api::OrchestratorKind::Chat as i32,
            lifecycle: api::BridgeLifecycle::Open as i32,
            display_title: "Investigate Unicode 🦀".to_owned(),
            revision: 1,
            transport: None,
            external_parent_id: None,
            external_thread_id: None,
            last_delivered_turn_id: None,
            delivery: None,
            live_turn_id: None,
            live_external_message_id: None,
            active_source_message_id: None,
        }
    }

    fn ingress_record(
        session_id: &str,
        message_id: &str,
        multipart_group: Option<&str>,
        forced_busy: bool,
    ) -> IngressRecord {
        IngressRecord {
            version: super::INGRESS_SCHEMA_VERSION,
            session_id: session_id.to_owned(),
            thread_id: "92".to_owned(),
            message_id: message_id.to_owned(),
            author_id: "42".to_owned(),
            received_at_ms: super::unix_time_ms(),
            content: multipart_group.map_or_else(
                || "continue".to_owned(),
                |group| format!("!nakode multipart {group} 1/2\npart"),
            ),
            attachments: Vec::new(),
            multipart_group: multipart_group.map(str::to_owned),
            forced_busy,
        }
    }

    #[test]
    fn periodic_reconciliation_preserves_optimistic_state_until_a_watch_update() {
        let mut optimistic = bridge();
        optimistic.live_turn_id = Some("turn-1".to_owned());
        optimistic.live_external_message_id = Some("200".to_owned());
        let mut bridges = HashMap::from([(optimistic.session_id.clone(), optimistic)]);

        let periodic = reconciliation_snapshot(&mut bridges, None);
        assert_eq!(periodic[0].live_turn_id.as_deref(), Some("turn-1"));
        assert_eq!(
            bridges["session-1"].live_external_message_id.as_deref(),
            Some("200")
        );

        let mut authoritative = bridge();
        authoritative.revision = 2;
        let watched = reconciliation_snapshot(&mut bridges, Some(vec![authoritative]));
        assert!(watched[0].live_turn_id.is_none());
        assert_eq!(bridges["session-1"].revision, 2);
    }

    #[tokio::test]
    async fn initial_api_connect_stops_cooperatively() {
        let directory = tempfile::tempdir().expect("tempdir");
        let endpoint = directory.path().join("missing-nakode.sock");
        let (shutdown_sender, mut shutdown) = tokio::sync::watch::channel(false);
        let connection = tokio::spawn(async move { connect_api(endpoint, &mut shutdown).await });

        tokio::task::yield_now().await;
        shutdown_sender.send(true).expect("request shutdown");
        let result = tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection loop stops before the transport abort deadline")
            .expect("connection task")
            .expect("cooperative shutdown is not an error");
        assert!(result.is_none());
    }

    #[test]
    fn durable_ingress_preserves_forced_busy_and_same_session_order_across_restart() {
        let directory = tempfile::tempdir().expect("ingress root");
        let path = directory.path().join("ingress.sqlite");
        {
            let spool = IngressSpool::open(&path).expect("open ingress");
            let first = spool
                .enqueue(&ingress_record("session-1", "100", None, false))
                .expect("first event")
                .expect("first event is pending");
            assert!(!first.forced_busy);
            let duplicate = spool
                .enqueue(&ingress_record("session-1", "100", None, true))
                .expect("duplicate event")
                .expect("duplicate remains pending");
            assert!(!duplicate.forced_busy, "the first durable decision wins");
            let later = spool
                .enqueue(&ingress_record("session-1", "101", None, false))
                .expect("later same-session event")
                .expect("later event is pending");
            assert!(
                later.forced_busy,
                "a later turn cannot overtake an unresolved one"
            );
            let concurrent = spool
                .enqueue(&ingress_record("session-2", "102", None, false))
                .expect("concurrent session event")
                .expect("concurrent event is pending");
            assert!(!concurrent.forced_busy);

            let mut overloaded = ingress_record("session-3", "103", Some("private"), true);
            overloaded.attachments.push(IngressAttachment {
                filename: "secret.png".to_owned(),
                url: "https://cdn.discordapp.com/attachments/secret".to_owned(),
                content_type: Some("image/png".to_owned()),
                size: 6,
            });
            let overloaded = spool
                .enqueue(&overloaded)
                .expect("overloaded multipart event")
                .expect("overloaded event is durably consumed");
            assert!(overloaded.forced_busy);
            assert!(overloaded.content.is_empty());
            assert!(overloaded.attachments.is_empty());
            assert!(overloaded.multipart_group.is_none());
        }

        let restored = IngressSpool::open(&path).expect("restore ingress");
        let restored_busy = restored
            .enqueue(&ingress_record("session-1", "101", None, false))
            .expect("replay pending busy identity")
            .expect("pending identity remains present");
        assert!(
            restored_busy.forced_busy,
            "the durable admission decision wins"
        );
        assert!(restored_busy.content.is_empty());
        let (_, first) = restored
            .next_after(0)
            .expect("read ingress")
            .expect("first ingress");
        assert_eq!(first.message_id, "100");
        let (_, later) = restored
            .next_after(1)
            .expect("read later ingress")
            .expect("later ingress");
        assert_eq!(later.message_id, "101");
        assert!(later.forced_busy);
        assert!(later.content.is_empty());
        assert!(later.attachments.is_empty());

        restored.remove_event("100").expect("settle first event");
        assert!(
            restored
                .enqueue(&ingress_record("session-1", "100", None, false))
                .expect("replay settled identity")
                .is_none(),
            "a locally terminal event cannot become a prompt after reopen"
        );
        drop(restored);
        let reopened = IngressSpool::open(&path).expect("reopen ingress");
        assert!(
            reopened
                .enqueue(&ingress_record("session-1", "100", None, false))
                .expect("replay tombstone after restart")
                .is_none()
        );
    }

    #[test]
    fn independent_ingress_connections_serialize_same_session_admission() {
        let directory = tempfile::tempdir().expect("ingress root");
        let path = directory.path().join("ingress.sqlite");
        let first_spool = Arc::new(IngressSpool::open(&path).expect("first connection"));
        let second_spool = Arc::new(IngressSpool::open(&path).expect("second connection"));
        let barrier = Arc::new(Barrier::new(3));

        let first = {
            let spool = Arc::clone(&first_spool);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                spool
                    .enqueue(&ingress_record("session-race", "race-1", None, false))
                    .expect("first admission")
                    .expect("first remains pending")
            })
        };
        let second = {
            let spool = Arc::clone(&second_spool);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                spool
                    .enqueue(&ingress_record("session-race", "race-2", None, false))
                    .expect("second admission")
                    .expect("second remains pending")
            })
        };
        barrier.wait();

        let records = [
            first.join().expect("first thread"),
            second.join().expect("second thread"),
        ];
        assert_eq!(
            records.iter().filter(|record| !record.forced_busy).count(),
            1,
            "only one same-session event may remain executable across connections"
        );
        let rejected = records
            .iter()
            .find(|record| record.forced_busy)
            .expect("one event is durably busy");
        assert!(rejected.content.is_empty());
        assert!(rejected.attachments.is_empty());
    }

    #[test]
    fn corrupt_ingress_is_quarantined_without_blocking_later_sessions() {
        let directory = tempfile::tempdir().expect("ingress root");
        let spool =
            IngressSpool::open(&directory.path().join("ingress.sqlite")).expect("open ingress");
        spool
            .enqueue(&ingress_record("session-1", "300", None, false))
            .expect("first enqueue")
            .expect("first pending");
        spool
            .enqueue(&ingress_record("session-2", "301", None, false))
            .expect("second enqueue")
            .expect("second pending");
        spool
            .connection
            .lock()
            .expect("ingress connection")
            .execute(
                "UPDATE discord_ingress SET payload_json = x'00' WHERE external_event_id = '300'",
                [],
            )
            .expect("corrupt payload");

        assert!(matches!(
            spool.next_after(0),
            Err(DiscordError::IngressPayload(_))
        ));
        spool
            .discard_next_after(0)
            .expect("quarantine corrupt payload");
        let (_, next) = spool
            .next_after(0)
            .expect("read after quarantine")
            .expect("later event remains");
        assert_eq!(next.message_id, "301");
        assert!(
            spool
                .enqueue(&ingress_record("session-1", "300", None, false))
                .expect("replay corrupt identity")
                .is_none(),
            "quarantined identities fail closed"
        );
    }

    #[test]
    fn durable_ingress_cleans_a_completed_multipart_group_as_one_turn() {
        let directory = tempfile::tempdir().expect("ingress root");
        let path = directory.path().join("ingress.sqlite");
        let spool = IngressSpool::open(&path).expect("open ingress");
        for message_id in ["200", "201"] {
            let record = spool
                .enqueue(&ingress_record(
                    "session-1",
                    message_id,
                    Some("long-turn"),
                    false,
                ))
                .expect("multipart event")
                .expect("multipart event is pending");
            assert!(
                !record.forced_busy,
                "parts in one explicit group remain assemblable"
            );
        }
        spool
            .enqueue(&ingress_record("session-2", "202", None, true))
            .expect("other event")
            .expect("other event is pending");
        spool
            .remove_multipart_group("session-1", "long-turn")
            .expect("remove group");
        assert_eq!(spool.len().expect("ingress count"), 1);
        drop(spool);
        let restored = IngressSpool::open(&path).expect("restore ingress");
        for message_id in ["200", "201"] {
            assert!(
                restored
                    .enqueue(&ingress_record(
                        "session-1",
                        message_id,
                        Some("long-turn"),
                        false,
                    ))
                    .expect("replay completed multipart")
                    .is_none(),
                "every grouped part identity remains terminal after restart"
            );
        }
    }

    #[test]
    fn cursor_recovery_spools_unbounded_history_in_oldest_first_order() {
        let directory = tempfile::tempdir().expect("recovery root");
        let mut spool = RecoverySpool::new(directory.path(), "session-1").expect("spool");
        for (id, turn) in [
            ("entry-3", "turn-3"),
            ("entry-2", "turn-2"),
            ("entry-1", "turn-1"),
        ] {
            spool
                .push(
                    &api::TranscriptEntry {
                        id: id.to_owned(),
                        body: format!("body for {turn}"),
                        body_total_bytes: u64::try_from(format!("body for {turn}").len())
                            .expect("body size"),
                        ..api::TranscriptEntry::default()
                    },
                    turn,
                )
                .expect("spool entry");
        }
        // A later duplicate projection for the same turn does not create another delivery.
        spool
            .push(
                &api::TranscriptEntry {
                    id: "entry-2-duplicate".to_owned(),
                    ..api::TranscriptEntry::default()
                },
                "turn-2",
            )
            .expect("duplicate turn");
        let turns = spool
            .oldest_first()
            .map(|entry| entry.expect("stored entry").turn_id)
            .collect::<Vec<_>>();
        assert_eq!(turns, ["turn-1", "turn-2", "turn-3"]);
    }

    #[test]
    fn disabled_default_is_valid_without_ids_or_token() {
        let config = DiscordConfig::default();
        assert_eq!(config.version, CONFIG_VERSION);
        assert!(!config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn enabled_config_requires_distinct_parents_and_one_primary_user() {
        let mut config = DiscordConfig {
            enabled: true,
            ..DiscordConfig::default()
        };
        assert!(config.validate().is_err());
        config.chat_channel_id = Some("43".to_owned());
        config.agent_channel_id = Some("43".to_owned());
        config.primary_user_id = Some("42".to_owned());
        assert!(config.validate().is_err());
        config.agent_channel_id = Some("44".to_owned());
        assert!(config.validate().is_ok());
        config.primary_user_id = Some("not-a-snowflake".to_owned());
        assert!(config.validate().is_err());
    }

    #[test]
    fn authorization_and_parent_selection_use_only_stable_snowflakes() {
        let config = enabled_config();
        assert!(config.is_primary_user(UserId::new(42)));
        assert!(!config.is_primary_user(UserId::new(99)));
        assert_eq!(
            config.parent_channel(api::OrchestratorKind::Chat),
            Some(ChannelId::new(43))
        );
        assert_eq!(
            config.parent_channel(api::OrchestratorKind::Agent),
            Some(ChannelId::new(44))
        );
    }

    #[test]
    fn config_store_never_serializes_the_token() {
        let directory = tempfile::tempdir().expect("workspace");
        let store =
            DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
                .expect("store");
        let config = enabled_config();
        store.save(&config).expect("save config");
        store.save_token("secret-token").expect("save token");
        assert_eq!(store.load().expect("load config"), config);
        assert_eq!(store.read_token().expect("read token"), "secret-token");
        let source = std::fs::read_to_string(store.config_path()).expect("config source");
        assert!(!source.contains("secret-token"));
        assert!(!format!("{config:?}").contains("secret-token"));
        let invalid = validate_snowflake("chat_channel_id", "accidentally-pasted-secret")
            .expect_err("invalid snowflake");
        assert!(!invalid.to_string().contains("accidentally-pasted-secret"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(store.token_path())
                    .expect("token metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn system_configuration_is_shared_while_transport_state_is_workspace_scoped() {
        let directory = tempfile::tempdir().expect("root");
        let first_workspace = directory.path().join("workspace-a");
        let second_workspace = directory.path().join("workspace-b");
        std::fs::create_dir_all(&first_workspace).expect("first workspace");
        std::fs::create_dir_all(&second_workspace).expect("second workspace");
        let data = directory.path().join("discord-data");
        let first = DiscordConfigStore::from_root(&first_workspace, &data).expect("first store");
        let second = DiscordConfigStore::from_root(&second_workspace, &data).expect("second store");

        first.save(&enabled_config()).expect("save shared config");
        first
            .save_token("shared-secret")
            .expect("save shared token");
        assert_eq!(second.load().expect("load shared config"), enabled_config());
        assert_eq!(
            second.read_token().expect("read shared token"),
            "shared-secret"
        );
        assert_eq!(first.config_path(), second.config_path());
        assert_eq!(first.token_path(), second.token_path());
        assert_ne!(first.directory, second.directory);
    }

    #[test]
    fn malformed_or_legacy_config_errors_never_echo_possible_secret_values() {
        let directory = tempfile::tempdir().expect("workspace");
        let store =
            DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
                .expect("store");
        std::fs::write(
            store.config_path(),
            "version = 1\nenabled = true\nbot_token = \"must-never-escape\"\n",
        )
        .expect("legacy config");
        let error = store.load().expect_err("legacy config must not activate");
        assert_eq!(error.to_string(), "invalid Discord configuration TOML");
        assert!(!error.to_string().contains("must-never-escape"));
    }

    #[test]
    fn discord_chunks_preserve_unicode_and_order_without_truncation() {
        let body = format!("{}\n{}", "🦀".repeat(1_500), "終".repeat(1_500));
        let chunks = split_discord_content(&body);
        assert!(chunks.len() >= 2);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 1_900)
        );
        assert_eq!(chunks.concat(), body);
    }

    #[tokio::test]
    async fn explicit_multipart_prompts_assemble_in_part_order_without_combining_groups() {
        let directory = tempfile::tempdir().expect("tempdir");
        let assembler =
            MultipartAssembler::new(directory.path().join("assemblies")).expect("assembler");
        let second = parse_multipart("!nakode multipart long-turn 2/3\n世界")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            assembler
                .accept("session-1", MessageId::new(12), second)
                .await
                .expect("second"),
            MultipartOutcome::Waiting
        ));
        let first = parse_multipart("!nakode multipart long-turn 1/3\nHello ")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            assembler
                .accept("session-1", MessageId::new(11), first)
                .await
                .expect("first"),
            MultipartOutcome::Waiting
        ));
        let third = parse_multipart("!nakode multipart long-turn 3/3\n!")
            .expect("multipart")
            .expect("valid");
        let complete = assembler
            .accept("session-1", MessageId::new(13), third)
            .await
            .expect("third");
        match complete {
            MultipartOutcome::Complete {
                group,
                text,
                source_message_id,
                ..
            } => {
                assert_eq!(group, "long-turn");
                assert_eq!(text, "Hello 世界!");
                assert_eq!(source_message_id, "13");
                assembler.finish("session-1", &group).await;
            }
            MultipartOutcome::Waiting | MultipartOutcome::Duplicate => {
                panic!("expected complete prompt")
            }
        }
        let replay = parse_multipart("!nakode multipart long-turn 3/3\n!")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            assembler
                .accept("session-1", MessageId::new(13), replay)
                .await
                .expect("replay"),
            MultipartOutcome::Duplicate
        ));
        let other = parse_multipart("!nakode multipart another-turn 1/2\nSeparate")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            assembler
                .accept("session-1", MessageId::new(21), other)
                .await
                .expect("other group"),
            MultipartOutcome::Waiting
        ));
    }

    #[tokio::test]
    async fn multipart_state_rebuilds_from_durable_record_contents_after_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("assemblies");
        let before_restart = MultipartAssembler::new(root.clone()).expect("first assembler");
        let first = parse_multipart("!nakode multipart restartable 1/2\nHello ")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            before_restart
                .accept("session-1", MessageId::new(31), first)
                .await
                .expect("first"),
            MultipartOutcome::Waiting
        ));
        drop(before_restart);

        // Startup intentionally clears the derived assembly files. Replaying the durable ingress
        // payloads reconstructs them, so an expiring Discord source message is not needed again.
        let after_restart = MultipartAssembler::new(root).expect("restarted assembler");
        let replayed_first = parse_multipart("!nakode multipart restartable 1/2\nHello ")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            after_restart
                .accept("session-1", MessageId::new(31), replayed_first)
                .await
                .expect("replayed first"),
            MultipartOutcome::Waiting
        ));
        let second = parse_multipart("!nakode multipart restartable 2/2\nworld")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            after_restart
                .accept("session-1", MessageId::new(32), second)
                .await
                .expect("second"),
            MultipartOutcome::Complete { text, .. } if text == "Hello world"
        ));
    }

    #[tokio::test]
    async fn multipart_admission_allows_only_one_group_per_session_without_starving_others() {
        let directory = tempfile::tempdir().expect("tempdir");
        let assembler =
            MultipartAssembler::new(directory.path().join("assemblies")).expect("assembler");
        for index in 0..MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION {
            let content = format!("!nakode multipart group-{index} 1/2\npart");
            let part = parse_multipart(&content)
                .expect("multipart")
                .expect("valid");
            assert!(matches!(
                assembler
                    .accept(
                        "saturated-session",
                        MessageId::new(100 + u64::try_from(index).expect("message id")),
                        part,
                    )
                    .await
                    .expect("within cap"),
                MultipartOutcome::Waiting
            ));
        }
        let extra = parse_multipart("!nakode multipart one-too-many 1/2\npart")
            .expect("multipart")
            .expect("valid");
        assert!(
            assembler
                .accept("saturated-session", MessageId::new(200), extra)
                .await
                .is_err()
        );
        let other = parse_multipart("!nakode multipart independent 1/2\npart")
            .expect("multipart")
            .expect("valid");
        assert!(matches!(
            assembler
                .accept("other-session", MessageId::new(201), other)
                .await
                .expect("other session remains admissible"),
            MultipartOutcome::Waiting
        ));
    }

    #[test]
    fn multipart_prompts_require_an_explicit_bounded_group_header() {
        assert!(parse_multipart("ordinary message").is_none());
        assert!(
            parse_multipart("!nakode multipart ../bad 1/2\ntext")
                .expect("recognized")
                .is_err()
        );
        assert!(
            parse_multipart("!nakode multipart okay 0/2\ntext")
                .expect("recognized")
                .is_err()
        );
        assert!(
            parse_multipart("!nakode multipart okay 3/2\ntext")
                .expect("recognized")
                .is_err()
        );
    }

    #[test]
    fn discord_chunks_close_and_reopen_markdown_code_fences() {
        let body = format!("```rust\n{}\n```\nDone 🦀", "let value = 1;\n".repeat(300));
        let chunks = split_discord_content(&body);
        assert!(chunks.len() >= 2);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 1_900)
        );
        assert!(chunks[0].ends_with("\n```"));
        assert!(chunks[1].starts_with("```rust\n"));
        assert!(
            chunks
                .last()
                .is_some_and(|chunk| chunk.ends_with("Done 🦀"))
        );
    }

    #[test]
    fn discord_chunks_respect_utf16_and_do_not_split_fence_markers() {
        let boundary = "😀".repeat(945);
        let body = format!("{boundary}```typescript\n{}\n```尾", "x".repeat(4_000));
        let chunks = split_discord_content(&body);
        assert!(chunks.len() >= 3);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 1_900)
        );
        assert!(
            chunks
                .iter()
                .all(|chunk| { !chunk.ends_with('`') || chunk.ends_with("```") })
        );
        assert!(chunks.last().is_some_and(|chunk| chunk.ends_with('尾')));
    }

    #[test]
    fn deterministic_discord_nonces_fit_the_platform_contract() {
        let starter = starter_nonce("session-1");
        let first = final_nonce("session-1", "turn-1", 0);
        let retry = final_nonce("session-1", "turn-1", 0);
        let second = final_nonce("session-1", "turn-1", 1);
        let busy = busy_nonce(MessageId::new(42));
        let failed = failed_nonce(MessageId::new(42));
        assert!(starter.len() <= 25);
        assert!(first.len() <= 25);
        assert!(busy.len() <= 25);
        assert!(failed.len() <= 25);
        assert_ne!(busy, failed);
        assert_eq!(first, retry);
        assert_ne!(first, second);
    }

    #[test]
    fn attachment_hosts_and_redirect_targets_are_restricted_to_discord_cdns() {
        for approved in [
            "https://cdn.discordapp.com/attachments/1/2/image.png",
            "https://media.discordapp.net/attachments/1/2/image.png",
        ] {
            assert!(is_approved_discord_cdn_url(
                &reqwest::Url::parse(approved).expect("approved URL")
            ));
        }
        for rejected in [
            "http://cdn.discordapp.com/attachments/1/2/image.png",
            "https://evil.discordapp.com/attachments/1/2/image.png",
            "https://example.com/image.png",
        ] {
            assert!(!is_approved_discord_cdn_url(
                &reqwest::Url::parse(rejected).expect("rejected URL")
            ));
        }
    }

    #[test]
    fn mentions_are_neutralized_without_changing_other_markdown() {
        assert_eq!(
            sanitize_mentions("**ok** @everyone and @here"),
            "**ok** @\u{200b}everyone and @\u{200b}here"
        );
        assert_eq!(visible_discord_content(" \n\t"), "…");
    }

    #[test]
    fn thread_titles_are_readable_bounded_and_kind_specific() {
        let chat = thread_title(api::OrchestratorKind::Chat as i32, &"x".repeat(500));
        let agent = thread_title(api::OrchestratorKind::Agent as i32, "Review auth");
        assert!(chat.chars().count() <= 100);
        assert!(chat.contains("Chat"));
        assert!(agent.contains("Agent"));
    }

    #[derive(Default)]
    struct FakeDiscord {
        messages: Mutex<Vec<ExternalMessage>>,
        sends: Mutex<Vec<(u64, String, Option<String>)>>,
        creates: Mutex<Vec<(u64, u64, String)>>,
        archives: Mutex<Vec<(u64, bool)>>,
        edits: Mutex<Vec<(u64, u64, String)>>,
        fail_next_send_after_record: Mutex<bool>,
        next_message: Mutex<u64>,
        next_thread: Mutex<u64>,
    }

    impl FakeDiscord {
        fn with_message(message: ExternalMessage) -> Self {
            Self {
                messages: Mutex::new(vec![message]),
                next_message: Mutex::new(100),
                next_thread: Mutex::new(200),
                ..Self::default()
            }
        }
    }

    #[serenity::async_trait]
    impl DiscordApi for FakeDiscord {
        async fn send_message(
            &self,
            channel_id: ChannelId,
            content: &str,
            nonce: Option<&str>,
        ) -> Result<ExternalMessage, serenity::Error> {
            if let Some(nonce) = nonce
                && let Some(existing) = self
                    .messages
                    .lock()
                    .expect("messages")
                    .iter()
                    .find(|message| message.nonce.as_deref() == Some(nonce))
                    .cloned()
            {
                return Ok(existing);
            }
            self.sends.lock().expect("sends").push((
                channel_id.get(),
                content.to_owned(),
                nonce.map(str::to_owned),
            ));
            let mut next = self.next_message.lock().expect("message id");
            *next += 1;
            let message = ExternalMessage {
                id: MessageId::new(*next),
                nonce: nonce.map(str::to_owned),
                thread_id: None,
            };
            self.messages
                .lock()
                .expect("messages")
                .push(message.clone());
            if std::mem::take(
                &mut *self
                    .fail_next_send_after_record
                    .lock()
                    .expect("send failure"),
            ) {
                return Err(serenity::Error::Other("simulated lost send response"));
            }
            Ok(message)
        }

        async fn edit_message(
            &self,
            channel_id: ChannelId,
            message_id: MessageId,
            content: &str,
        ) -> Result<(), serenity::Error> {
            self.edits.lock().expect("edits").push((
                channel_id.get(),
                message_id.get(),
                content.to_owned(),
            ));
            Ok(())
        }

        async fn create_thread(
            &self,
            parent_channel_id: ChannelId,
            starter_message_id: MessageId,
            title: &str,
        ) -> Result<ChannelId, serenity::Error> {
            if let Some(thread_id) = self
                .messages
                .lock()
                .expect("messages")
                .iter()
                .find(|message| message.id == starter_message_id)
                .and_then(|message| message.thread_id)
            {
                return Ok(thread_id);
            }
            self.creates.lock().expect("creates").push((
                parent_channel_id.get(),
                starter_message_id.get(),
                title.to_owned(),
            ));
            let mut next = self.next_thread.lock().expect("thread id");
            *next += 1;
            let thread_id = ChannelId::new(*next);
            if let Some(message) = self
                .messages
                .lock()
                .expect("messages")
                .iter_mut()
                .find(|message| message.id == starter_message_id)
            {
                message.thread_id = Some(thread_id);
            }
            Ok(thread_id)
        }

        async fn set_thread_archived(
            &self,
            thread_id: ChannelId,
            archived: bool,
        ) -> Result<(), serenity::Error> {
            self.archives
                .lock()
                .expect("archives")
                .push((thread_id.get(), archived));
            Ok(())
        }

        async fn messages_page(
            &self,
            _channel_id: ChannelId,
            before: Option<MessageId>,
        ) -> Result<Vec<ExternalMessage>, serenity::Error> {
            Ok(self
                .messages
                .lock()
                .expect("messages")
                .iter()
                .filter(|message| before.is_none_or(|before| message.id < before))
                .rev()
                .take(100)
                .cloned()
                .collect())
        }

        async fn react(
            &self,
            _channel_id: ChannelId,
            _message_id: MessageId,
            _emoji: &str,
        ) -> Result<(), serenity::Error> {
            Ok(())
        }

        async fn remove_own_reaction(
            &self,
            _channel_id: ChannelId,
            _message_id: MessageId,
            _emoji: &str,
        ) -> Result<(), serenity::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn lazy_thread_creation_recovers_existing_starter_mapping() {
        let bridge = bridge();
        let nonce = starter_nonce(&bridge.session_id);
        let fake = FakeDiscord::with_message(ExternalMessage {
            id: MessageId::new(91),
            nonce: Some(nonce),
            thread_id: Some(ChannelId::new(92)),
        });
        let thread = create_or_recover_thread(&fake, ChannelId::new(43), &bridge)
            .await
            .expect("recover thread");
        assert_eq!(thread, ChannelId::new(92));
        assert!(fake.sends.lock().expect("sends").is_empty());
        assert!(fake.creates.lock().expect("creates").is_empty());
    }

    #[tokio::test]
    async fn lazy_thread_creation_uses_one_nonce_starter_and_one_thread() {
        let fake = Arc::new(FakeDiscord {
            next_message: Mutex::new(100),
            next_thread: Mutex::new(200),
            ..FakeDiscord::default()
        });
        let bridge = bridge();
        let thread = create_or_recover_thread(&*fake, ChannelId::new(43), &bridge)
            .await
            .expect("create thread");
        assert_eq!(thread, ChannelId::new(201));
        let sends = fake.sends.lock().expect("sends");
        assert_eq!(sends.len(), 1);
        assert_eq!(
            sends[0].2.as_deref(),
            Some(starter_nonce(&bridge.session_id).as_str())
        );
    }

    #[tokio::test]
    async fn concurrent_lazy_creation_adopts_one_nonce_starter_and_thread() {
        let fake = Arc::new(FakeDiscord {
            next_message: Mutex::new(100),
            next_thread: Mutex::new(200),
            ..FakeDiscord::default()
        });
        let bridge = bridge();
        let first = create_or_recover_thread(&*fake, ChannelId::new(43), &bridge);
        let second = create_or_recover_thread(&*fake, ChannelId::new(43), &bridge);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.expect("first"), ChannelId::new(201));
        assert_eq!(second.expect("second"), ChannelId::new(201));
        assert_eq!(fake.sends.lock().expect("sends").len(), 1);
        assert_eq!(fake.creates.lock().expect("creates").len(), 1);
    }

    #[tokio::test]
    async fn final_part_recovers_a_send_that_succeeded_before_its_response_was_lost() {
        let fake = FakeDiscord {
            fail_next_send_after_record: Mutex::new(true),
            next_message: Mutex::new(100),
            ..FakeDiscord::default()
        };
        let nonce = final_nonce("session-1", "turn-1", 0);
        let first =
            send_or_recover_final_part(&fake, ChannelId::new(92), &nonce, "the final answer").await;
        assert!(first.is_err(), "the simulated response is lost");
        assert_eq!(fake.sends.lock().expect("sends").len(), 1);

        let recovered =
            send_or_recover_final_part(&fake, ChannelId::new(92), &nonce, "the final answer")
                .await
                .expect("recover accepted send by nonce");
        assert_eq!(recovered, MessageId::new(101));
        assert_eq!(fake.sends.lock().expect("sends").len(), 1);
        assert_eq!(
            fake.edits.lock().expect("edits").as_slice(),
            &[(92, 101, "the final answer".to_owned())]
        );
    }

    #[tokio::test]
    async fn nonce_recovery_fails_closed_after_bounded_history_search() {
        let nonce = final_nonce("session-1", "outside-window", 0);
        let messages = (1..=6_401)
            .map(|id| ExternalMessage {
                id: MessageId::new(id),
                nonce: (id == 1).then(|| nonce.clone()),
                thread_id: None,
            })
            .collect();
        let fake = FakeDiscord {
            messages: Mutex::new(messages),
            ..FakeDiscord::default()
        };
        let error = find_message_by_nonce(&fake, ChannelId::new(92), &nonce)
            .await
            .expect_err("search cap must fail closed before a duplicate send");
        assert!(error.to_string().contains("bounded nonce history"));
    }

    #[tokio::test]
    async fn nonce_recovery_pages_to_thread_origin_without_buffering_history() {
        let nonce = final_nonce("session-1", "old-turn", 0);
        let messages = (1..=1_205)
            .map(|id| ExternalMessage {
                id: MessageId::new(id),
                nonce: (id == 1).then(|| nonce.clone()),
                thread_id: None,
            })
            .collect();
        let fake = FakeDiscord {
            messages: Mutex::new(messages),
            ..FakeDiscord::default()
        };
        let found = find_message_by_nonce(&fake, ChannelId::new(92), &nonce)
            .await
            .expect("history search")
            .expect("old nonce");
        assert_eq!(found.id, MessageId::new(1));
    }
}
