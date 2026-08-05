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
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use directories::ProjectDirs;
use futures_util::{StreamExt, TryStreamExt, future::BoxFuture, future::FutureExt};
use nakode_sdk::{HydratedSession, NakodeClient, SdkError, v1 as api};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serenity::{
    all::{
        ChannelId, Context, CreateMessage, CreateThread, EditMessage, EventHandler, GatewayIntents,
        GuildId, Message, MessageId, Ready, UserId,
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

const CONFIG_VERSION: u32 = 1;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES: usize = 30 * 1024 * 1024;
const DISCORD_MESSAGE_LIMIT: usize = 2_000;
const DISCORD_CHUNK_SIZE: usize = DISCORD_MESSAGE_LIMIT - 100;
const SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(500);
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn default_config_version() -> u32 {
    CONFIG_VERSION
}

/// Persisted Discord frontend settings. The bot token is intentionally not a
/// field here; it lives in the private token file managed by
/// [`DiscordConfigStore`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    pub enabled: bool,
    /// Discord user snowflakes allowed to submit prompts or resolve
    /// interactions. An empty list is invalid for an enabled bot.
    pub allowed_users: Vec<String>,
    /// Optional global guild allow-list. Channel bindings also carry an
    /// optional guild constraint for deployments that reuse channel IDs in
    /// configuration tooling.
    pub allowed_guilds: Vec<String>,
    pub bindings: Vec<DiscordBinding>,
    /// Discord threads that were created for Nakode sessions.
    #[serde(default)]
    pub thread_bindings: Vec<DiscordThreadBinding>,
}

/// Persists the relationship between one Discord thread and one Nakode
/// session. The parent channel is retained so thread messages can be
/// authorized without trusting arbitrary Discord channels.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordThreadBinding {
    pub thread_id: String,
    pub channel_id: String,
    #[serde(default)]
    pub guild_id: Option<String>,
    pub session_id: String,
}

/// Maps one Discord channel to an optional legacy Nakode session. A missing
/// session id makes the channel a mention-only session entry point; each
/// mention creates a new Discord thread and Nakode session.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordBinding {
    pub channel_id: String,
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            enabled: false,
            allowed_users: Vec::new(),
            allowed_guilds: Vec::new(),
            bindings: Vec::new(),
            thread_bindings: Vec::new(),
        }
    }
}

impl DiscordConfig {
    /// Validates persisted configuration without contacting Discord or the
    /// Nakode server.
    pub fn validate(&self) -> Result<(), DiscordError> {
        if self.version != CONFIG_VERSION {
            return Err(DiscordError::InvalidConfig(format!(
                "unsupported Discord configuration version {}",
                self.version
            )));
        }
        validate_id_list("allowed user", &self.allowed_users)?;
        validate_id_list("allowed guild", &self.allowed_guilds)?;
        let mut channels = HashSet::new();
        for binding in &self.bindings {
            validate_snowflake("channel_id", &binding.channel_id)?;
            if !channels.insert(binding.channel_id.clone()) {
                return Err(DiscordError::InvalidConfig(format!(
                    "channel {} is bound more than once",
                    binding.channel_id
                )));
            }
            if let Some(guild_id) = &binding.guild_id {
                validate_snowflake("guild_id", guild_id)?;
            }
            if binding
                .session_id
                .as_deref()
                .is_some_and(|session_id| session_id.trim().is_empty())
            {
                return Err(DiscordError::InvalidConfig(
                    "session_id cannot be blank when present".to_owned(),
                ));
            }
        }
        let mut threads = HashSet::new();
        for binding in &self.thread_bindings {
            validate_snowflake("thread_id", &binding.thread_id)?;
            if self
                .bindings
                .iter()
                .any(|root| root.channel_id == binding.thread_id)
            {
                return Err(DiscordError::InvalidConfig(format!(
                    "thread {} conflicts with a configured parent channel",
                    binding.thread_id
                )));
            }
            if !threads.insert(binding.thread_id.clone()) {
                return Err(DiscordError::InvalidConfig(format!(
                    "thread {} is bound more than once",
                    binding.thread_id
                )));
            }
            validate_snowflake("channel_id", &binding.channel_id)?;
            if let Some(guild_id) = &binding.guild_id {
                validate_snowflake("guild_id", guild_id)?;
            }
            if binding.session_id.trim().is_empty() {
                return Err(DiscordError::InvalidConfig(
                    "thread session_id cannot be blank".to_owned(),
                ));
            }
        }
        if self.enabled {
            if self.allowed_users.is_empty() {
                return Err(DiscordError::InvalidConfig(
                    "at least one allowed Discord user is required when the bot is enabled"
                        .to_owned(),
                ));
            }
            if self.bindings.is_empty() {
                return Err(DiscordError::InvalidConfig(
                    "at least one Discord channel binding is required when the bot is enabled"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn is_authorized(
        &self,
        user_id: UserId,
        guild_id: Option<GuildId>,
        channel_id: ChannelId,
    ) -> Option<DiscordBinding> {
        let user_id = user_id.get().to_string();
        if !self.allowed_users.iter().any(|allowed| allowed == &user_id) {
            return None;
        }
        if !self.allowed_guilds.is_empty()
            && !guild_id.is_some_and(|guild| {
                self.allowed_guilds
                    .iter()
                    .any(|allowed| allowed == &guild.get().to_string())
            })
        {
            return None;
        }
        let channel_id = channel_id.get().to_string();
        self.bindings
            .iter()
            .find(|binding| {
                binding.channel_id == channel_id
                    && binding.guild_id.as_deref().is_none_or(|guild| {
                        guild_id.is_some_and(|actual| actual.get().to_string() == guild)
                    })
            })
            .cloned()
    }
}

fn validate_id_list(kind: &str, values: &[String]) -> Result<(), DiscordError> {
    let mut seen = HashSet::new();
    for value in values {
        validate_snowflake(kind, value)?;
        if !seen.insert(value) {
            return Err(DiscordError::InvalidConfig(format!(
                "duplicate {kind} id {value}"
            )));
        }
    }
    Ok(())
}

fn validate_snowflake(field: &str, value: &str) -> Result<u64, DiscordError> {
    let value = value.trim();
    let parsed = value.parse::<u64>().map_err(|_| DiscordError::InvalidId {
        field: field.to_owned(),
        value: value.to_owned(),
    })?;
    if parsed == 0 {
        return Err(DiscordError::InvalidId {
            field: field.to_owned(),
            value: value.to_owned(),
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
    #[error("invalid Discord {field} snowflake {value:?}")]
    InvalidId { field: String, value: String },
    #[error("invalid Discord configuration TOML: {0}")]
    Toml(#[from] toml::de::Error),
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
    #[error("Discord attachment download failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Discord attachment {name:?} exceeds the {MAX_ATTACHMENT_BYTES} byte limit")]
    AttachmentTooLarge { name: String },
    #[error(
        "combined Discord prompt attachments exceed the {MAX_TOTAL_ATTACHMENT_BYTES} byte limit"
    )]
    CombinedAttachmentsTooLarge,
    #[error("Discord attachment {name:?} is not a supported HTTPS image")]
    UnsupportedAttachment { name: String },
    #[error("the workspace service is not running; run `nakode start` first")]
    ServiceNotRunning,
    #[error("Discord transport control failed: {0}")]
    Control(#[from] crate::control_service::ControlError),
    #[error("Discord setup input failed: {0}")]
    SetupInput(#[source] io::Error),
}

/// Private per-workspace storage for Discord configuration and its token.
/// This is separate from Nakode's provider/session SQLite database.
#[derive(Clone, Debug)]
pub struct DiscordConfigStore {
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
        Ok(Self { directory })
    }

    #[must_use]
    pub fn config_path(&self) -> PathBuf {
        self.directory.join("discord.toml")
    }

    #[must_use]
    pub fn token_path(&self) -> PathBuf {
        self.directory.join("discord.token")
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

/// Handles a `nakode service discord ...` command.
pub async fn run_command(config: &Config, action: DiscordAction) -> Result<(), DiscordError> {
    let store = DiscordConfigStore::for_workspace(&config.workspace)?;
    let paths = crate::control_service::ServicePaths::of(config)?;
    match action {
        DiscordAction::Setup {
            channel_id,
            guild_id,
            session_id,
            allowed_users,
        } => {
            setup(&store, channel_id, guild_id, session_id, allowed_users)?;
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
        DiscordAction::Bind {
            channel_id,
            guild_id,
            session_id,
        } => {
            bind(&store, channel_id, guild_id, session_id)?;
            let discord_config = store.load()?;
            if discord_config.enabled {
                reload_if_running(&paths, "Discord configuration reloaded").await?;
            }
        }
        DiscordAction::Unbind { channel_id } => {
            unbind(&store, &channel_id)?;
            let discord_config = store.load()?;
            if discord_config.enabled {
                reload_if_running(&paths, "Discord configuration reloaded").await?;
            } else {
                report_live_action(&paths, TransportAction::Stop, "Discord frontend stopped")
                    .await?;
            }
        }
    }
    Ok(())
}

fn setup(
    store: &DiscordConfigStore,
    channel_id: Option<String>,
    guild_id: Option<String>,
    session_id: Option<String>,
    allowed_users: Option<String>,
) -> Result<(), DiscordError> {
    let existing = store.load()?;
    let existing_binding = existing.bindings.first();
    let token = rpassword::prompt_password("Discord bot token (input hidden): ")
        .map_err(DiscordError::SetupInput)?;
    let channel_id = match channel_id {
        Some(channel_id) => channel_id,
        None => prompt_line(
            "Discord channel ID",
            existing_binding.map_or("", |binding| binding.channel_id.as_str()),
            true,
        )?,
    };
    let guild_default = existing_binding
        .and_then(|binding| binding.guild_id.as_deref())
        .unwrap_or("");
    let guild_id = match guild_id {
        Some(guild_id) => optional_string(&guild_id),
        None => optional_string(&prompt_line(
            "Discord guild ID (optional)",
            guild_default,
            false,
        )?),
    };
    let users_default = existing.allowed_users.join(",");
    let users = match allowed_users {
        Some(users) => users,
        None => prompt_line(
            "Authorized Discord user IDs (comma-separated)",
            &users_default,
            true,
        )?,
    };
    let allowed_users = parse_id_list(&users, "allowed user")?;
    let session_default = existing_binding
        .and_then(|binding| binding.session_id.as_deref())
        .unwrap_or("");
    let session_id = match session_id {
        Some(session_id) => optional_string(&session_id),
        None => optional_string(&prompt_line(
            "Nakode session ID (optional)",
            session_default,
            false,
        )?),
    };

    let mut next = existing;
    next.enabled = true;
    next.allowed_users = allowed_users;
    next.allowed_guilds = guild_id.clone().into_iter().collect();
    upsert_binding(
        &mut next,
        DiscordBinding {
            channel_id,
            guild_id,
            session_id,
        },
    );
    next.validate()?;
    store.save_token(&token)?;
    store.save(&next)?;
    println!("Discord frontend configured and enabled for automatic service startup.");
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
            allowed_users: &'a [String],
            allowed_guilds: &'a [String],
            bindings: &'a [DiscordBinding],
            thread_bindings: &'a [DiscordThreadBinding],
            config_path: String,
            runtime: Option<&'a TransportStatus>,
        }
        let status = Status {
            enabled: config.enabled,
            token_configured,
            allowed_users: &config.allowed_users,
            allowed_guilds: &config.allowed_guilds,
            bindings: &config.bindings,
            thread_bindings: &config.thread_bindings,
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
        println!("Allowed users: {}", config.allowed_users.len());
        println!("Allowed guilds: {}", config.allowed_guilds.len());
        println!("Channel bindings: {}", config.bindings.len());
        println!("Thread bindings: {}", config.thread_bindings.len());
        for binding in &config.bindings {
            println!(
                "  channel={} guild={} session={}",
                binding.channel_id,
                binding.guild_id.as_deref().unwrap_or("any"),
                binding
                    .session_id
                    .as_deref()
                    .unwrap_or("mention creates a thread"),
            );
        }
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
                "Configuration saved. The workspace service is not running; start it and run `nakode service discord start` to activate Discord."
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn reload_if_running(paths: &ServicePaths, message: &str) -> Result<(), DiscordError> {
    match crate::control_service::transport_action(paths, "discord", TransportAction::Status).await
    {
        Ok(status) if status.running => {
            let status = crate::control_service::transport_action(
                paths,
                "discord",
                TransportAction::Restart,
            )
            .await?;
            println!("{message} (transport running: {}).", status.running);
            Ok(())
        }
        Ok(_) => {
            println!(
                "Configuration saved. Discord remains stopped; use `nakode service discord start` to activate it."
            );
            Ok(())
        }
        Err(error) if service_unavailable(&error) => {
            println!(
                "Configuration saved. The workspace service is not running; start it and run `nakode service discord start` to activate Discord."
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

fn bind(
    store: &DiscordConfigStore,
    channel_id: String,
    guild_id: Option<String>,
    session_id: Option<String>,
) -> Result<(), DiscordError> {
    let mut config = store.load()?;
    let binding = DiscordBinding {
        channel_id,
        guild_id: guild_id.map(|value| value.trim().to_owned()),
        session_id: session_id.and_then(|value| optional_string(&value)),
    };
    upsert_binding(&mut config, binding);
    config.validate()?;
    store.save(&config)?;
    println!("Discord channel binding saved.");
    Ok(())
}

fn unbind(store: &DiscordConfigStore, channel_id: &str) -> Result<(), DiscordError> {
    validate_snowflake("channel_id", channel_id)?;
    let mut config = store.load()?;
    let before = config.bindings.len();
    config
        .bindings
        .retain(|binding| binding.channel_id != channel_id);
    if before == config.bindings.len() {
        return Err(DiscordError::InvalidConfig(format!(
            "channel {channel_id} is not bound"
        )));
    }
    if config.bindings.is_empty() {
        config.enabled = false;
    }
    config.validate()?;
    store.save(&config)?;
    println!("Discord channel binding removed.");
    Ok(())
}

fn upsert_binding(config: &mut DiscordConfig, binding: DiscordBinding) {
    config
        .bindings
        .retain(|existing| existing.channel_id != binding.channel_id);
    config.bindings.push(binding);
}

fn parse_id_list(source: &str, kind: &str) -> Result<Vec<String>, DiscordError> {
    let values = source
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(DiscordError::InvalidConfig(format!(
            "at least one {kind} id is required"
        )));
    }
    validate_id_list(kind, &values)?;
    Ok(values)
}

fn optional_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
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
            eprintln!("nakode discord: could not open configuration: {error}");
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
                            eprintln!("nakode discord: {error}");
                            Some(error.to_string())
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
        if let Some(handle) = handle {
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
        if let Some(handle) = handle {
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
struct RuntimeBinding {
    channel_id: ChannelId,
    /// The configured parent channel used to authorize messages in a thread.
    /// For a legacy channel binding this is the same as `channel_id`.
    parent_channel_id: ChannelId,
    guild_id: Option<GuildId>,
    /// Thread/session mappings restored after reconnect or restart should not
    /// replay the current assistant entry. Newly created sessions should.
    skip_initial_snapshot: bool,
    /// A mention-only root binding has no session. Thread and legacy bindings
    /// always have one.
    session_id: Option<String>,
}

struct BotState {
    client: NakodeClient,
    workspace_id: String,
    http_client: HttpClient,
    store: DiscordConfigStore,
    config: tokio::sync::RwLock<DiscordConfig>,
    bindings: Vec<RuntimeBinding>,
    thread_bindings: tokio::sync::RwLock<HashMap<u64, RuntimeBinding>>,
    watchers: tokio::sync::Mutex<HashMap<u64, JoinHandle<()>>>,
    bot_user_id: std::sync::OnceLock<UserId>,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl BotState {
    async fn stop_watchers(&self) {
        let handles = {
            let mut watchers = self.watchers.lock().await;
            watchers
                .drain()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>()
        };
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn watch_bindings(&self) -> Vec<RuntimeBinding> {
        let mut bindings = self
            .bindings
            .iter()
            .filter(|binding| binding.session_id.is_some())
            .cloned()
            .collect::<Vec<_>>();
        bindings.extend(
            self.thread_bindings
                .read()
                .await
                .values()
                .filter(|binding| binding.session_id.is_some())
                .cloned(),
        );
        bindings
    }

    async fn register_thread(&self, binding: RuntimeBinding) -> Result<(), DiscordError> {
        let session_id = binding.session_id.clone().ok_or_else(|| {
            DiscordError::InvalidConfig("a Discord thread must have a session".to_owned())
        })?;
        let persisted = DiscordThreadBinding {
            thread_id: binding.channel_id.get().to_string(),
            channel_id: binding.parent_channel_id.get().to_string(),
            guild_id: binding.guild_id.map(|guild| guild.get().to_string()),
            session_id,
        };
        {
            let mut config = self.config.write().await;
            config
                .thread_bindings
                .retain(|existing| existing.thread_id != persisted.thread_id);
            config.thread_bindings.push(persisted);
            self.store.save(&config)?;
        }
        self.thread_bindings
            .write()
            .await
            .insert(binding.channel_id.get(), binding);
        Ok(())
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
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), DiscordError> {
    let client = connect_api(endpoint).await?;
    let workspace_path = workspace.to_string_lossy().into_owned();
    let workspace_state = client.get_workspace(workspace_path.clone(), None).await?;
    let mut bindings = Vec::with_capacity(config.bindings.len());
    for binding in &config.bindings {
        let channel_id = validate_snowflake("channel_id", &binding.channel_id)?;
        let guild_id = binding
            .guild_id
            .as_deref()
            .map(|guild_id| validate_snowflake("guild_id", guild_id))
            .transpose()?;
        let session_id = if let Some(session_id) = &binding.session_id {
            let session_id = client.open_session(session_id.clone()).await?;
            let session = client.get_session(session_id.clone()).await?;
            if session.workspace_id != workspace_state.workspace_id {
                return Err(DiscordError::InvalidConfig(format!(
                    "session {session_id} does not belong to workspace {workspace_path}"
                )));
            }
            Some(session_id)
        } else {
            None
        };
        bindings.push(RuntimeBinding {
            channel_id: ChannelId::new(channel_id),
            parent_channel_id: ChannelId::new(channel_id),
            guild_id: guild_id.map(GuildId::new),
            skip_initial_snapshot: true,
            session_id,
        });
    }
    if bindings.is_empty() {
        return Err(DiscordError::InvalidConfig(
            "no usable Discord channel bindings were found".to_owned(),
        ));
    }

    let mut thread_bindings = HashMap::new();
    for binding in &config.thread_bindings {
        let thread_id = validate_snowflake("thread_id", &binding.thread_id)?;
        let parent_channel_id = validate_snowflake("channel_id", &binding.channel_id)?;
        let guild_id = binding
            .guild_id
            .as_deref()
            .map(|guild_id| validate_snowflake("guild_id", guild_id))
            .transpose()?;
        let parent_is_configured = config.bindings.iter().any(|root| {
            root.channel_id == binding.channel_id
                && root
                    .guild_id
                    .as_deref()
                    .is_none_or(|root_guild| binding.guild_id.as_deref() == Some(root_guild))
        });
        if !parent_is_configured {
            continue;
        }
        let session_id = match client.open_session(binding.session_id.clone()).await {
            Ok(session_id) => session_id,
            Err(error) => {
                eprintln!(
                    "nakode discord: ignoring thread {} with an unavailable session: {error}",
                    binding.thread_id
                );
                continue;
            }
        };
        let session = match client.get_session(session_id.clone()).await {
            Ok(session) => session,
            Err(error) => {
                eprintln!(
                    "nakode discord: ignoring thread {} with an unavailable session: {error}",
                    binding.thread_id
                );
                continue;
            }
        };
        if session.workspace_id != workspace_state.workspace_id {
            eprintln!(
                "nakode discord: ignoring thread {} because its session belongs to another workspace",
                binding.thread_id
            );
            continue;
        }
        thread_bindings.insert(
            thread_id,
            RuntimeBinding {
                channel_id: ChannelId::new(thread_id),
                parent_channel_id: ChannelId::new(parent_channel_id),
                guild_id: guild_id.map(GuildId::new),
                skip_initial_snapshot: true,
                session_id: Some(session_id),
            },
        );
    }

    let http_client = HttpClient::builder()
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;
    let state = Arc::new(BotState {
        client,
        workspace_id: workspace_state.workspace_id,
        http_client,
        store,
        config: tokio::sync::RwLock::new(config),
        bindings,
        thread_bindings: tokio::sync::RwLock::new(thread_bindings),
        watchers: tokio::sync::Mutex::new(HashMap::new()),
        bot_user_id: std::sync::OnceLock::new(),
        shutdown,
    });
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = Handler {
        state: Arc::clone(&state),
    };
    let mut discord = serenity::Client::builder(token, intents)
        .event_handler(handler)
        .await?;
    let gateway_result = discord.start().await;
    state.stop_watchers().await;
    gateway_result?;
    Ok(())
}

async fn start_watcher(
    http: Arc<serenity::http::Http>,
    state: Arc<BotState>,
    binding: RuntimeBinding,
) {
    if binding.session_id.is_none() {
        return;
    }
    let key = binding.channel_id.get();
    let mut watchers = state.watchers.lock().await;
    if watchers.contains_key(&key) {
        return;
    }
    let task_state = Arc::clone(&state);
    let task_binding = binding.clone();
    let shutdown = state.shutdown.clone();
    let task = tokio::spawn(async move {
        watch_binding(http, task_state.clone(), task_binding, shutdown).await;
        task_state.watchers.lock().await.remove(&key);
    });
    watchers.insert(key, task);
}

struct ResolvedRoute {
    binding: DiscordBinding,
    is_thread: bool,
}

fn binding_for_runtime(
    config: &DiscordConfig,
    runtime: RuntimeBinding,
    user_id: UserId,
    guild_id: Option<GuildId>,
) -> Option<DiscordBinding> {
    let mut binding = config.is_authorized(user_id, guild_id, runtime.parent_channel_id)?;
    binding.channel_id = runtime.channel_id.get().to_string();
    binding.guild_id = runtime.guild_id.map(|guild| guild.get().to_string());
    binding.session_id = runtime.session_id;
    Some(binding)
}

async fn resolve_route(state: &BotState, message: &Message) -> Option<ResolvedRoute> {
    let thread_binding = state
        .thread_bindings
        .read()
        .await
        .get(&message.channel_id.get())
        .cloned();
    let config = state.config.read().await;
    if let Some(runtime) = thread_binding {
        let binding = binding_for_runtime(&config, runtime, message.author.id, message.guild_id)?;
        return Some(ResolvedRoute {
            binding,
            is_thread: true,
        });
    }

    let runtime = state.bindings.iter().find(|runtime| {
        runtime.channel_id == message.channel_id
            && runtime
                .guild_id
                .is_none_or(|guild| message.guild_id == Some(guild))
    })?;
    let binding = binding_for_runtime(
        &config,
        runtime.clone(),
        message.author.id,
        message.guild_id,
    )?;
    Some(ResolvedRoute {
        binding,
        is_thread: false,
    })
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let _ = self.state.bot_user_id.set(ready.user.id);
        for binding in self.state.watch_bindings().await {
            start_watcher(Arc::clone(&ctx.http), Arc::clone(&self.state), binding).await;
        }
    }

    async fn message(&self, ctx: Context, message: Message) {
        if message.author.bot {
            return;
        }
        let Some(route) = resolve_route(&self.state, &message).await else {
            return;
        };
        let mention = self.state.bot_user_id.get().and_then(|bot_id| {
            if message.mentions.iter().any(|user| user.id == *bot_id) {
                strip_bot_mention(&message.content, *bot_id)
            } else {
                None
            }
        });
        if !route.is_thread && mention.is_some() {
            if let Err(error) = handle_new_session(
                &ctx,
                &self.state,
                &message,
                mention.as_deref().unwrap_or_default(),
            )
            .await
            {
                let _ = send_text(&ctx.http, message.channel_id, &format!("Nakode: {error}")).await;
            }
            return;
        }
        if route.binding.session_id.is_none() {
            return;
        }
        let content = mention.unwrap_or_else(|| message.content.clone());
        if let Some(command) = parse_command(&content) {
            if let Err(error) = handle_command(&ctx, &self.state, &route.binding, &command).await {
                let _ = send_text(&ctx.http, message.channel_id, &format!("Nakode: {error}")).await;
            }
            return;
        }
        if let Err(error) = handle_prompt(&self.state, &route.binding, &message, &content).await {
            let _ = send_text(&ctx.http, message.channel_id, &format!("Nakode: {error}")).await;
        }
    }
}

async fn handle_new_session(
    ctx: &Context,
    state: &Arc<BotState>,
    message: &Message,
    prompt: &str,
) -> Result<(), DiscordError> {
    let guild_id = message.guild_id.ok_or_else(|| {
        DiscordError::InvalidConfig(
            "mention-driven Nakode sessions require a Discord guild channel".to_owned(),
        )
    })?;
    let title = session_title(prompt);
    let session_id = state
        .client
        .create_session(state.workspace_id.clone(), Some(title.clone()))
        .await?;
    let thread = message
        .channel_id
        .create_thread_from_message(&ctx.http, message.id, CreateThread::new(title))
        .await?;
    let binding = RuntimeBinding {
        channel_id: thread.id,
        parent_channel_id: thread.parent_id.unwrap_or(message.channel_id),
        guild_id: Some(guild_id),
        skip_initial_snapshot: false,
        session_id: Some(session_id),
    };
    state.register_thread(binding.clone()).await?;
    start_watcher(Arc::clone(&ctx.http), Arc::clone(state), binding.clone()).await;

    let discord_binding = DiscordBinding {
        channel_id: binding.channel_id.get().to_string(),
        guild_id: Some(guild_id.get().to_string()),
        session_id: binding.session_id.clone(),
    };
    if prompt.trim().is_empty() && message.attachments.is_empty() {
        send_text(
            &ctx.http,
            binding.channel_id,
            "Nakode session started. Send a prompt in this thread.",
        )
        .await?;
    } else {
        handle_prompt(state, &discord_binding, message, prompt).await?;
    }
    Ok(())
}

fn session_title(prompt: &str) -> String {
    let compact = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    let title = if compact.is_empty() {
        "Nakode session".to_owned()
    } else {
        format!("Nakode: {compact}")
    };
    let truncated = title.chars().take(100).collect::<String>();
    if truncated.chars().count() >= 2 {
        truncated
    } else {
        "Nakode session".to_owned()
    }
}

fn strip_bot_mention(content: &str, bot_id: UserId) -> Option<String> {
    let plain = format!("<@{}>", bot_id.get());
    let nicknamed = format!("<@!{}>", bot_id.get());
    if !content.contains(&plain) && !content.contains(&nicknamed) {
        return None;
    }
    Some(
        content
            .replace(&plain, "")
            .replace(&nicknamed, "")
            .trim()
            .to_owned(),
    )
}

async fn watch_binding(
    http: Arc<serenity::http::Http>,
    state: Arc<BotState>,
    binding: RuntimeBinding,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let Some(session_id) = binding.session_id.clone() else {
        return;
    };
    let mut updates = state.client.watch_hydrated_session(session_id, 64);
    let mut baseline = binding.skip_initial_snapshot;
    let mut last_assistant: Option<(String, String)> = None;
    let mut response_messages = Vec::new();
    let mut announced_interactions = HashSet::new();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let update = tokio::select! {
            _ = shutdown.changed() => return,
            update = updates.next() => update,
        };
        let Some(update) = update else {
            return;
        };
        let Ok(mut hydrated) = update else {
            continue;
        };
        tokio::select! {
            _ = shutdown.changed() => return,
            () = tokio::time::sleep(SNAPSHOT_DEBOUNCE) => {}
        }
        while let Ok(Some(next)) = updates.try_next().await {
            if *shutdown.borrow() {
                return;
            }
            hydrated = next;
        }
        announce_interactions(
            &http,
            binding.channel_id,
            &hydrated,
            &mut announced_interactions,
        )
        .await;
        if baseline {
            baseline = false;
            if let Some((assistant_id, body)) = latest_assistant(&hydrated) {
                last_assistant = Some((assistant_id, body));
            }
            continue;
        }
        let Some((assistant_id, body)) = latest_assistant(&hydrated) else {
            continue;
        };
        if last_assistant
            .as_ref()
            .is_none_or(|(last_id, _)| last_id != &assistant_id)
        {
            response_messages.clear();
            last_assistant = Some((assistant_id, String::new()));
        }
        let Some((_, previous_body)) = &mut last_assistant else {
            continue;
        };
        if *previous_body == body && !response_messages.is_empty() {
            continue;
        }
        previous_body.clone_from(&body);
        if render_response(&http, binding.channel_id, &body, &mut response_messages)
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn connect_api(endpoint: PathBuf) -> Result<NakodeClient, DiscordError> {
    loop {
        match tokio::time::timeout(
            Duration::from_secs(2),
            NakodeClient::connect_unix(endpoint.clone()),
        )
        .await
        {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

fn latest_assistant(hydrated: &HydratedSession) -> Option<(String, String)> {
    hydrated
        .state
        .transcript
        .as_ref()?
        .entries
        .iter()
        .rev()
        .find(|entry| entry.kind == api::TranscriptEntryKind::Assistant as i32)
        .map(|entry| (entry.id.clone(), entry.body.clone()))
}

async fn announce_interactions(
    http: &serenity::http::Http,
    channel_id: ChannelId,
    hydrated: &HydratedSession,
    announced: &mut HashSet<String>,
) {
    for interaction in hydrated
        .state
        .interactions
        .iter()
        .filter(|interaction| interaction.status == api::InteractionStatus::Pending as i32)
    {
        if !announced.insert(interaction.id.clone()) {
            continue;
        }
        let options = interaction
            .options
            .iter()
            .map(|option| format!("`{}` {}", option.id, option.label))
            .collect::<Vec<_>>();
        let options = if options.is_empty() {
            String::new()
        } else {
            format!("\nOptions:\n{}", options.join("\n"))
        };
        let message = format!(
            "Nakode needs input: **{}**\n{}{}\nInteraction ID: `{}`\nUse `!nakode approve {}` or `!nakode decline {}`.",
            interaction.title,
            interaction.detail,
            options,
            interaction.id,
            interaction.id,
            interaction.id,
        );
        let _ = send_text(http, channel_id, &message).await;
    }
}

async fn render_response(
    http: &serenity::http::Http,
    channel_id: ChannelId,
    body: &str,
    message_ids: &mut Vec<MessageId>,
) -> Result<(), serenity::Error> {
    let safe = sanitize_mentions(body);
    let chunks = split_discord_content(&safe);
    for (index, chunk) in chunks.iter().enumerate() {
        if let Some(message_id) = message_ids.get(index).copied() {
            channel_id
                .edit_message(http, message_id, EditMessage::new().content(chunk))
                .await?;
        } else {
            let message = channel_id
                .send_message(http, CreateMessage::new().content(chunk))
                .await?;
            message_ids.push(message.id);
        }
    }
    while message_ids.len() > chunks.len() {
        if let Some(message_id) = message_ids.pop() {
            let _ = channel_id.delete_message(http, message_id).await;
        }
    }
    Ok(())
}

async fn send_text(
    http: &serenity::http::Http,
    channel_id: ChannelId,
    text: &str,
) -> Result<(), serenity::Error> {
    let safe = sanitize_mentions(text);
    for chunk in split_discord_content(&safe) {
        channel_id
            .send_message(http, CreateMessage::new().content(chunk))
            .await?;
    }
    Ok(())
}

fn split_discord_content(text: &str) -> Vec<String> {
    let text = if text.is_empty() { "…" } else { text };
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if current.chars().count() >= DISCORD_CHUNK_SIZE {
            chunks.push(std::mem::take(&mut current));
        }
        current.push(character);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() {
        chunks.push("…".to_owned());
    }
    chunks
}

fn sanitize_mentions(text: &str) -> String {
    text.replace("@everyone", "@\u{200b}everyone")
        .replace("@here", "@\u{200b}here")
}

fn parse_command(content: &str) -> Option<Vec<&str>> {
    let content = content.trim();
    let command = content
        .strip_prefix("!nakode")
        .or_else(|| content.strip_prefix("/nakode"))?;
    if !command.is_empty() && !command.chars().next().is_some_and(char::is_whitespace) {
        return None;
    }
    Some(command.split_whitespace().collect())
}

async fn handle_command(
    ctx: &Context,
    state: &BotState,
    binding: &DiscordBinding,
    command: &[&str],
) -> Result<(), DiscordError> {
    let action = command
        .first()
        .copied()
        .unwrap_or("help")
        .to_ascii_lowercase();
    match action.as_str() {
        "help" => {
            send_text(
                &ctx.http,
                channel_id(binding),
                "Commands: `!nakode help`, `!nakode cancel`, `!nakode approve <interaction-id>`, `!nakode approve-session <interaction-id>`, `!nakode decline <interaction-id>`, and `!nakode answer <interaction-id> <option-id>`.",
            )
            .await?;
        }
        "cancel" => {
            state
                .client
                .cancel_session_work(binding_session(binding), None)
                .await?;
            send_text(
                &ctx.http,
                channel_id(binding),
                "Nakode work cancellation requested.",
            )
            .await?;
        }
        "approve" | "approve-session" | "decline" | "answer" => {
            let interaction_id = command.get(1).copied().ok_or_else(|| {
                DiscordError::InvalidConfig(format!("{action} requires an interaction id"))
            })?;
            let session = state.client.get_session(binding_session(binding)).await?;
            let interaction = session
                .interactions
                .iter()
                .find(|interaction| interaction.id == interaction_id)
                .ok_or_else(|| {
                    DiscordError::InvalidConfig("interaction was not found".to_owned())
                })?;
            if interaction.status != api::InteractionStatus::Pending as i32 {
                return Err(DiscordError::InvalidConfig(
                    "interaction is no longer pending".to_owned(),
                ));
            }
            let (resolution, option_ids) = match action.as_str() {
                "approve" => (
                    api::InteractionResolutionKind::ApproveOnce,
                    command
                        .iter()
                        .skip(2)
                        .map(|value| (*value).to_owned())
                        .collect(),
                ),
                "approve-session" => (
                    api::InteractionResolutionKind::ApproveForSession,
                    command
                        .iter()
                        .skip(2)
                        .map(|value| (*value).to_owned())
                        .collect(),
                ),
                "decline" => (api::InteractionResolutionKind::Decline, Vec::new()),
                "answer" => {
                    let options = command
                        .iter()
                        .skip(2)
                        .map(|value| (*value).to_owned())
                        .collect::<Vec<_>>();
                    if options.is_empty() {
                        return Err(DiscordError::InvalidConfig(
                            "answer requires at least one option id".to_owned(),
                        ));
                    }
                    (api::InteractionResolutionKind::Answer, options)
                }
                _ => unreachable!(),
            };
            state
                .client
                .resolve_interaction(
                    interaction_id,
                    resolution,
                    option_ids,
                    Some(interaction.revision),
                )
                .await?;
            send_text(
                &ctx.http,
                channel_id(binding),
                "Nakode interaction resolved.",
            )
            .await?;
        }
        _ => {
            send_text(
                &ctx.http,
                channel_id(binding),
                "Unknown Nakode command. Use `!nakode help`.",
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_prompt(
    state: &BotState,
    binding: &DiscordBinding,
    message: &Message,
    content: &str,
) -> Result<(), DiscordError> {
    let mut attachments = Vec::with_capacity(message.attachments.len());
    let mut total_attachment_bytes = 0usize;
    for attachment in &message.attachments {
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
    let text = if content.trim().is_empty() && !attachments.is_empty() {
        "Please inspect the attached image(s).".to_owned()
    } else {
        content.to_owned()
    };
    if text.trim().is_empty() {
        return Err(DiscordError::InvalidConfig(
            "a prompt needs text or a supported image attachment".to_owned(),
        ));
    }
    state
        .client
        .send_prompt(
            binding_session(binding),
            api::PromptInput { text, attachments },
            None,
        )
        .await?;
    Ok(())
}

async fn download_image(
    client: &HttpClient,
    attachment: &serenity::all::Attachment,
) -> Result<api::PromptAttachment, DiscordError> {
    if usize::try_from(attachment.size)
        .ok()
        .is_some_and(|size| size > MAX_ATTACHMENT_BYTES)
    {
        return Err(DiscordError::AttachmentTooLarge {
            name: attachment.filename.clone(),
        });
    }
    let url =
        reqwest::Url::parse(&attachment.url).map_err(|_| DiscordError::UnsupportedAttachment {
            name: attachment.filename.clone(),
        })?;
    let host = url.host_str().unwrap_or_default();
    if url.scheme() != "https"
        || !(host == "discordapp.com"
            || host.ends_with(".discordapp.com")
            || host == "discordapp.net"
            || host.ends_with(".discordapp.net"))
    {
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

fn channel_id(binding: &DiscordBinding) -> ChannelId {
    ChannelId::new(binding.channel_id.parse().expect("validated channel id"))
}

fn binding_session(binding: &DiscordBinding) -> &str {
    binding
        .session_id
        .as_deref()
        .expect("runtime command bindings carry a resolved session")
}

impl fmt::Display for DiscordBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "channel {}", self.channel_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiscordBinding, DiscordConfig, DiscordConfigStore, DiscordThreadBinding, RuntimeBinding,
        binding_for_runtime, parse_command, session_title, split_discord_content,
        strip_bot_mention,
    };

    #[test]
    fn disabled_default_has_current_config_version() {
        let config = DiscordConfig::default();
        assert_eq!(config.version, 1);
        assert!(!config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn enabled_config_requires_operator_and_binding() {
        let mut config = DiscordConfig {
            enabled: true,
            ..DiscordConfig::default()
        };
        assert!(config.validate().is_err());
        config.allowed_users.push("42".to_owned());
        assert!(config.validate().is_err());
        config.bindings.push(DiscordBinding {
            channel_id: "43".to_owned(),
            guild_id: None,
            session_id: None,
        });
        assert!(config.validate().is_ok());
    }

    #[test]
    fn authorization_requires_the_allow_lists_and_channel_binding() {
        let mut config = DiscordConfig {
            allowed_users: vec!["42".to_owned()],
            allowed_guilds: vec!["44".to_owned()],
            bindings: vec![DiscordBinding {
                channel_id: "43".to_owned(),
                guild_id: Some("44".to_owned()),
                session_id: None,
            }],
            ..DiscordConfig::default()
        };
        assert!(
            config
                .is_authorized(
                    serenity::all::UserId::new(42),
                    Some(serenity::all::GuildId::new(44)),
                    serenity::all::ChannelId::new(43),
                )
                .is_some()
        );
        assert!(
            config
                .is_authorized(
                    serenity::all::UserId::new(99),
                    Some(serenity::all::GuildId::new(44)),
                    serenity::all::ChannelId::new(43),
                )
                .is_none()
        );
        config.allowed_guilds.clear();
        assert!(
            config
                .is_authorized(
                    serenity::all::UserId::new(42),
                    Some(serenity::all::GuildId::new(45)),
                    serenity::all::ChannelId::new(43),
                )
                .is_none()
        );
    }
    #[test]
    fn thread_runtime_binding_routes_through_its_authorized_parent() {
        let config = DiscordConfig {
            allowed_users: vec!["42".to_owned()],
            allowed_guilds: vec!["44".to_owned()],
            bindings: vec![DiscordBinding {
                channel_id: "43".to_owned(),
                guild_id: Some("44".to_owned()),
                session_id: None,
            }],
            ..DiscordConfig::default()
        };
        let runtime = RuntimeBinding {
            channel_id: serenity::all::ChannelId::new(45),
            parent_channel_id: serenity::all::ChannelId::new(43),
            guild_id: Some(serenity::all::GuildId::new(44)),
            skip_initial_snapshot: true,
            session_id: Some("session-1".to_owned()),
        };
        let routed = binding_for_runtime(
            &config,
            runtime.clone(),
            serenity::all::UserId::new(42),
            Some(serenity::all::GuildId::new(44)),
        )
        .expect("authorized thread route");
        assert_eq!(routed.channel_id, "45");
        assert_eq!(routed.session_id.as_deref(), Some("session-1"));
        assert!(
            binding_for_runtime(
                &config,
                runtime,
                serenity::all::UserId::new(99),
                Some(serenity::all::GuildId::new(44)),
            )
            .is_none()
        );
    }

    #[test]
    fn command_parser_only_accepts_the_nakode_prefix() {
        assert_eq!(parse_command("!nakode cancel"), Some(vec!["cancel"]));
        assert_eq!(
            parse_command("/nakode approve i-1"),
            Some(vec!["approve", "i-1"])
        );
        assert_eq!(parse_command("!nakodecancel"), None);
        assert_eq!(parse_command("hello"), None);
    }

    #[test]
    fn bot_mentions_are_detected_and_removed_in_both_discord_forms() {
        let bot = serenity::all::UserId::new(42);
        assert_eq!(
            strip_bot_mention("<@42> inspect this", bot),
            Some("inspect this".to_owned())
        );
        assert_eq!(
            strip_bot_mention("please <@!42> review", bot),
            Some("please  review".to_owned())
        );
        assert_eq!(strip_bot_mention("<@99> not nako", bot), None);
    }

    #[test]
    fn session_titles_are_valid_thread_names_and_are_bounded() {
        assert_eq!(session_title(""), "Nakode session");
        assert_eq!(
            session_title("  investigate   this  "),
            "Nakode: investigate this"
        );
        let bounded = session_title(&"x".repeat(500));
        assert!(bounded.chars().count() <= 100);
        let payload = serde_json::to_value(serenity::all::CreateThread::new(bounded))
            .expect("thread request payload");
        assert!(
            payload["name"]
                .as_str()
                .is_some_and(|name| !name.is_empty())
        );
    }

    #[test]
    fn thread_bindings_are_validated_and_round_tripped() {
        let mut config = DiscordConfig {
            enabled: true,
            allowed_users: vec!["42".to_owned()],
            bindings: vec![DiscordBinding {
                channel_id: "43".to_owned(),
                guild_id: Some("44".to_owned()),
                session_id: None,
            }],
            thread_bindings: vec![DiscordThreadBinding {
                thread_id: "45".to_owned(),
                channel_id: "43".to_owned(),
                guild_id: Some("44".to_owned()),
                session_id: "session-1".to_owned(),
            }],
            ..DiscordConfig::default()
        };
        assert!(config.validate().is_ok());
        config.thread_bindings[0].thread_id = "43".to_owned();
        assert!(config.validate().is_err());
    }
    #[test]
    fn discord_messages_are_split_under_the_platform_limit() {
        let chunks = split_discord_content(&"x".repeat(4_001));
        assert!(chunks.len() >= 3);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 1_900));
    }

    #[test]
    fn config_store_round_trips_without_storing_a_token_in_toml() {
        let directory = tempfile::tempdir().expect("workspace");
        let store =
            DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
                .expect("store");
        let mut config = DiscordConfig::default();
        config.allowed_users.push("42".to_owned());
        config.bindings.push(DiscordBinding {
            channel_id: "43".to_owned(),
            guild_id: Some("44".to_owned()),
            session_id: Some("session-1".to_owned()),
        });
        config.thread_bindings.push(DiscordThreadBinding {
            thread_id: "45".to_owned(),
            channel_id: "43".to_owned(),
            guild_id: Some("44".to_owned()),
            session_id: "session-2".to_owned(),
        });
        store.save(&config).expect("save config");
        store.save_token("secret-token").expect("save token");
        let loaded = store.load().expect("load config");
        assert_eq!(loaded, config);
        assert_eq!(store.read_token().expect("read token"), "secret-token");
        let source = std::fs::read_to_string(store.config_path()).expect("config source");
        assert!(!source.contains("secret-token"));
    }
}
