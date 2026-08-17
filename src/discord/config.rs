//! Discord configuration, credential storage, CLI control, and redacted management API.

use std::{
    collections::VecDeque,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use directories::ProjectDirs;
use nakode_protocol::{
    DiscordIntegrationInput, DiscordIntegrationView, DiscordRuntimeState, ErrorCode,
    IdempotencyKey, ServiceError,
};
use nakode_sdk::v1 as api;
use nakode_server::grpc::{DiscordManagement, DiscordManagementMutation};
use serde::{Deserialize, Serialize};
use serenity::{
    all::{ChannelId, UserId},
    async_trait,
};
use sha2::{Digest, Sha256};

use super::{
    CONFIG_VERSION, DiscordError, DiscordTransport, HEX_DIGITS, MAX_MANAGEMENT_REPLAYS,
    MAX_TOKEN_BYTES, TRANSPORT_NAME, atomic_write, io_error, prepare_private_directory,
    sanitized_bridge_error,
};
use crate::{
    config::{Config, DiscordAction},
    control_service::{
        ServicePaths, TransportAction, TransportController, TransportStatus, TransportSupervisor,
    },
};

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
    /// Monotonic transport-generation signal shared by every live workspace service.
    #[serde(default)]
    pub runtime_generation: u64,
    pub enabled: bool,
    pub chat_channel_id: Option<String>,
    pub agent_channel_id: Option<String>,
    pub primary_user_id: Option<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            runtime_generation: 0,
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
        if let (Some(chat), Some(agent)) = (
            self.chat_channel_id.as_deref(),
            self.agent_channel_id.as_deref(),
        ) && chat == agent
        {
            return Err(DiscordError::InvalidConfig(
                "Chat and Agent orchestrators require different parent channels".to_owned(),
            ));
        }
        if self.enabled {
            self.chat_channel_id.as_deref().ok_or_else(|| {
                DiscordError::InvalidConfig(
                    "chat_channel_id is required when Discord is enabled".to_owned(),
                )
            })?;
            self.agent_channel_id.as_deref().ok_or_else(|| {
                DiscordError::InvalidConfig(
                    "agent_channel_id is required when Discord is enabled".to_owned(),
                )
            })?;
            if self.primary_user_id.is_none() {
                return Err(DiscordError::InvalidConfig(
                    "primary_user_id is required when Discord is enabled".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn parent_channel(&self, kind: api::OrchestratorKind) -> Option<ChannelId> {
        let value = match kind {
            api::OrchestratorKind::Chat => self.chat_channel_id.as_deref(),
            api::OrchestratorKind::Agent => self.agent_channel_id.as_deref(),
            api::OrchestratorKind::Unspecified => None,
        }?;
        value.parse::<u64>().ok().map(ChannelId::new)
    }

    pub(super) fn is_primary_user(&self, user_id: UserId) -> bool {
        self.primary_user_id
            .as_deref()
            .is_some_and(|configured| configured == user_id.get().to_string())
    }
}

pub(super) fn validate_snowflake(field: &str, value: &str) -> Result<u64, DiscordError> {
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

/// Private system configuration plus workspace-scoped transport state.
/// Credentials and channel/user snowflakes are installation-level. Durable ingress/recovery files
/// remain workspace-hashed so independent Nakode authorities never consume each other's work.
#[derive(Clone, Debug)]
pub struct DiscordConfigStore {
    pub(super) configuration_directory: PathBuf,
    pub(super) directory: PathBuf,
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

    pub(super) fn from_root(workspace: &Path, root: &Path) -> Result<Self, DiscordError> {
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

    #[must_use]
    fn lock_path(&self) -> PathBuf {
        self.configuration_directory.join("discord.lock")
    }

    /// Runs a configuration mutation under the installation-wide advisory lock. Every workspace
    /// service shares this path, so read-modify-write operations cannot lose updates to another
    /// service's public IDs or enabled state.
    pub(super) fn with_configuration_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, DiscordError>,
    ) -> Result<T, DiscordError> {
        let path = self.lock_path();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(|source| io_error(&path, source))?;
        }
        fs2::FileExt::lock_exclusive(&lock).map_err(|source| io_error(&path, source))?;
        let outcome = operation();
        let unlock = fs2::FileExt::unlock(&lock).map_err(|source| io_error(&path, source));
        let value = outcome?;
        unlock?;
        Ok(value)
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
        self.with_configuration_lock(|| self.save_unlocked(config))
    }

    fn save_unlocked(&self, config: &DiscordConfig) -> Result<(), DiscordError> {
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
        let token = source.trim();
        if token.len() > MAX_TOKEN_BYTES {
            return Err(DiscordError::TokenTooLarge);
        }
        if token.is_empty() {
            return Err(DiscordError::MissingToken);
        }
        Ok(token.to_owned())
    }

    #[must_use]
    pub fn token_configured(&self) -> bool {
        self.read_token().is_ok()
    }

    /// Replaces the token using a private, atomically renamed file.
    pub fn save_token(&self, token: &str) -> Result<(), DiscordError> {
        self.with_configuration_lock(|| self.save_token_unlocked(token))
    }

    fn save_token_unlocked(&self, token: &str) -> Result<(), DiscordError> {
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
        self.with_configuration_lock(|| self.delete_token_unlocked())
    }

    fn delete_token_unlocked(&self) -> Result<(), DiscordError> {
        let path = self.token_path();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(io_error(&path, source)),
        }
    }
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
        runtime_generation: existing.runtime_generation.saturating_add(1),
        enabled: true,
        chat_channel_id: Some(chat_channel_id.trim().to_owned()),
        agent_channel_id: Some(agent_channel_id.trim().to_owned()),
        primary_user_id: Some(primary_user_id.trim().to_owned()),
    };
    next.validate()?;
    store.with_configuration_lock(|| {
        store.save_token_unlocked(&token)?;
        store.save_unlocked(&next)
    })?;
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
    store.with_configuration_lock(|| {
        let mut config = store.load()?;
        config.enabled = enabled;
        config.validate()?;
        if enabled {
            let _ = store.read_token()?;
        }
        store.save_unlocked(&config)
    })?;
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

/// Creates the root-owned Discord management authority injected into the public gRPC facade.
pub(crate) fn management_service(
    workspace: &Path,
    transports: TransportSupervisor,
) -> Arc<dyn DiscordManagement> {
    Arc::new(DiscordManagementService {
        store: DiscordConfigStore::for_workspace(workspace).map_err(|error| {
            eprintln!(
                "nakode discord: could not open management configuration ({})",
                sanitized_bridge_error(&error)
            );
        }),
        transports,
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    })
}

#[derive(Clone)]
pub(super) struct DiscordManagementService {
    pub(super) store: Result<DiscordConfigStore, ()>,
    pub(super) transports: TransportSupervisor,
    pub(super) operation: Arc<tokio::sync::Mutex<DiscordManagementState>>,
}

#[derive(Default)]
pub(super) struct DiscordManagementState {
    replays: VecDeque<DiscordManagementReplay>,
}

struct DiscordManagementReplay {
    key: String,
    fingerprint: [u8; 32],
    view: DiscordIntegrationView,
}

async fn configuration_io<T>(
    operation: &'static str,
    task: impl FnOnce() -> Result<T, DiscordError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(task).await {
        Ok(result) => result.map_err(|error| management_error(error, operation)),
        Err(_) => Err(ServiceError {
            code: ErrorCode::Internal,
            message: format!("Could not {operation} Discord configuration"),
            retryable: true,
        }),
    }
}

impl DiscordManagementService {
    fn store(&self) -> Result<&DiscordConfigStore, ServiceError> {
        self.store.as_ref().map_err(|()| ServiceError {
            code: ErrorCode::Internal,
            message: "Discord configuration storage is unavailable".to_owned(),
            retryable: true,
        })
    }

    async fn view(&self) -> Result<DiscordIntegrationView, ServiceError> {
        let store = self.store()?.clone();
        let (config, token_configured) = configuration_io("read", move || {
            store.with_configuration_lock(|| {
                let config = store.load()?;
                Ok((config, store.token_configured()))
            })
        })
        .await?;
        let transport = self
            .transports
            .control(TRANSPORT_NAME, TransportAction::Status)
            .await;
        Ok(discord_management_view(
            &config,
            token_configured,
            &transport,
        ))
    }

    async fn save(
        &self,
        input: DiscordIntegrationInput,
    ) -> Result<DiscordIntegrationView, ServiceError> {
        let store = self.store()?.clone();
        let (config, token_configured) = configuration_io("save", move || {
            store.with_configuration_lock(|| {
                let current = store.load()?;
                let config = DiscordConfig {
                    version: CONFIG_VERSION,
                    runtime_generation: current.runtime_generation.saturating_add(1),
                    enabled: current.enabled,
                    chat_channel_id: optional_trimmed(&input.chat_channel_id),
                    agent_channel_id: optional_trimmed(&input.agent_channel_id),
                    primary_user_id: optional_trimmed(&input.primary_user_id),
                };
                config.validate()?;
                if config.enabled && input.bot_token.is_none() && !store.token_configured() {
                    return Err(DiscordError::MissingToken);
                }

                // Credential first is deliberate: if the second atomic rename fails, a retry that
                // omits the credential still preserves the replacement that already landed. The
                // transport is touched only after both durable writes succeed.
                if let Some(token) = input.bot_token {
                    store.save_token_unlocked(&token.0)?;
                }
                store.save_unlocked(&config)?;
                Ok((config, store.token_configured()))
            })
        })
        .await?;
        let transport = self
            .transports
            .control(TRANSPORT_NAME, TransportAction::Restart)
            .await;
        Ok(discord_management_view(
            &config,
            token_configured,
            &transport,
        ))
    }

    async fn set_enabled(&self, enabled: bool) -> Result<DiscordIntegrationView, ServiceError> {
        let store = self.store()?.clone();
        let (config, token_configured) = configuration_io("save", move || {
            store.with_configuration_lock(|| {
                let mut config = store.load()?;
                config.enabled = enabled;
                config.runtime_generation = config.runtime_generation.saturating_add(1);
                config.validate()?;
                if enabled && !store.token_configured() {
                    return Err(DiscordError::MissingToken);
                }
                store.save_unlocked(&config)?;
                Ok((config, store.token_configured()))
            })
        })
        .await?;
        let action = if enabled {
            TransportAction::Start
        } else {
            TransportAction::Restart
        };
        let transport = self.transports.control(TRANSPORT_NAME, action).await;
        Ok(discord_management_view(
            &config,
            token_configured,
            &transport,
        ))
    }

    async fn restart(&self) -> Result<DiscordIntegrationView, ServiceError> {
        let store = self.store()?.clone();
        let (config, token_configured) = configuration_io("restart", move || {
            store.with_configuration_lock(|| {
                let mut config = store.load()?;
                let token_configured = store.token_configured();
                if config.enabled && token_configured {
                    config.runtime_generation = config.runtime_generation.saturating_add(1);
                    store.save_unlocked(&config)?;
                }
                Ok((config, token_configured))
            })
        })
        .await?;
        if !config.enabled {
            return Err(ServiceError {
                code: ErrorCode::InvalidRequest,
                message: "Enable the Discord integration before restarting its transport"
                    .to_owned(),
                retryable: false,
            });
        }
        if !token_configured {
            return Err(ServiceError {
                code: ErrorCode::InvalidRequest,
                message: "A Discord bot token must be saved before restarting the integration"
                    .to_owned(),
                retryable: false,
            });
        }
        let transport = self
            .transports
            .control(TRANSPORT_NAME, TransportAction::Restart)
            .await;
        Ok(discord_management_view(
            &config,
            token_configured,
            &transport,
        ))
    }
    async fn mutate_serialized(
        &self,
        idempotency_key: IdempotencyKey,
        mutation: DiscordManagementMutation,
    ) -> Result<DiscordIntegrationView, ServiceError> {
        let mut state = self.operation.lock().await;
        let fingerprint = management_mutation_fingerprint(&mutation);
        if let Some(replay) = state
            .replays
            .iter()
            .find(|replay| replay.key == idempotency_key.as_str())
        {
            if replay.fingerprint == fingerprint {
                return Ok(replay.view.clone());
            }
            return Err(ServiceError {
                code: ErrorCode::Conflict,
                message: "Discord management idempotency key was reused for a different mutation"
                    .to_owned(),
                retryable: false,
            });
        }

        let view = match mutation {
            DiscordManagementMutation::Save(input) => self.save(input).await?,
            DiscordManagementMutation::SetEnabled(enabled) => self.set_enabled(enabled).await?,
            DiscordManagementMutation::Restart => self.restart().await?,
        };
        state.replays.push_back(DiscordManagementReplay {
            key: idempotency_key.to_string(),
            fingerprint,
            view: view.clone(),
        });
        while state.replays.len() > MAX_MANAGEMENT_REPLAYS {
            state.replays.pop_front();
        }
        Ok(view)
    }
}

#[async_trait]
impl DiscordManagement for DiscordManagementService {
    async fn get(&self) -> Result<DiscordIntegrationView, ServiceError> {
        let _operation = self.operation.lock().await;
        self.view().await
    }

    async fn mutate(
        &self,
        idempotency_key: IdempotencyKey,
        mutation: DiscordManagementMutation,
    ) -> Result<DiscordIntegrationView, ServiceError> {
        // The operation owns its task independently of the request future. If a gRPC client
        // disconnects while private file I/O or transport restart is in flight, serialization and
        // replay insertion still complete before a retry can execute the same business operation.
        let service = self.clone();
        tokio::spawn(async move { service.mutate_serialized(idempotency_key, mutation).await })
            .await
            .map_err(|_| ServiceError {
                code: ErrorCode::Internal,
                message: "Could not complete Discord management operation".to_owned(),
                retryable: true,
            })?
    }
}

fn optional_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn management_mutation_fingerprint(mutation: &DiscordManagementMutation) -> [u8; 32] {
    let mut digest = Sha256::new();
    match mutation {
        DiscordManagementMutation::Save(input) => {
            digest.update(b"save\0");
            for value in [
                &input.chat_channel_id,
                &input.agent_channel_id,
                &input.primary_user_id,
            ] {
                digest.update(value.len().to_le_bytes());
                digest.update(value.as_bytes());
            }
            match &input.bot_token {
                Some(token) => {
                    digest.update([1]);
                    digest.update(token.0.len().to_le_bytes());
                    digest.update(token.0.as_bytes());
                }
                None => digest.update([0]),
            }
        }
        DiscordManagementMutation::SetEnabled(enabled) => {
            digest.update(b"enabled\0");
            digest.update([u8::from(*enabled)]);
        }
        DiscordManagementMutation::Restart => digest.update(b"restart\0"),
    }
    digest.finalize().into()
}

fn management_error(error: DiscordError, operation: &'static str) -> ServiceError {
    match error {
        DiscordError::InvalidConfig(message) => ServiceError {
            code: ErrorCode::InvalidRequest,
            message: format!("Invalid Discord configuration: {message}"),
            retryable: false,
        },
        DiscordError::InvalidId { field } => ServiceError {
            code: ErrorCode::InvalidRequest,
            message: format!("Invalid Discord {field} snowflake"),
            retryable: false,
        },
        DiscordError::MissingToken => ServiceError {
            code: ErrorCode::InvalidRequest,
            message: "Discord bot token is missing or blank".to_owned(),
            retryable: false,
        },
        DiscordError::TokenTooLarge => ServiceError {
            code: ErrorCode::InvalidRequest,
            message: format!("Discord bot token exceeds the {MAX_TOKEN_BYTES}-byte limit"),
            retryable: false,
        },
        _ => ServiceError {
            code: ErrorCode::Internal,
            message: format!("Could not {operation} Discord configuration"),
            retryable: true,
        },
    }
}

fn discord_management_view(
    config: &DiscordConfig,
    token_configured: bool,
    transport: &Result<TransportStatus, String>,
) -> DiscordIntegrationView {
    let configuration_complete = config.chat_channel_id.is_some()
        && config.agent_channel_id.is_some()
        && config.primary_user_id.is_some();
    let (running, failed) = match transport {
        Ok(status) => (status.running, status.error.is_some()),
        Err(_) => (false, true),
    };
    let runtime_state = if running {
        DiscordRuntimeState::Running
    } else if failed {
        DiscordRuntimeState::Failed
    } else if config.enabled {
        DiscordRuntimeState::Stopped
    } else {
        DiscordRuntimeState::Disabled
    };
    let runtime_error = failed.then(|| {
        "Discord transport is unavailable; check the sanitized Nakode service logs".to_owned()
    });
    DiscordIntegrationView {
        enabled: config.enabled,
        configuration_complete,
        token_configured,
        chat_channel_id: config.chat_channel_id.clone(),
        agent_channel_id: config.agent_channel_id.clone(),
        primary_user_id: config.primary_user_id.clone(),
        runtime_state,
        runtime_error,
    }
}
