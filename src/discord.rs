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
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, TryStreamExt, future::BoxFuture, future::FutureExt};
use nakode_sdk::{HydratedSession, NakodeClient, SdkError, Watch, v1 as api};
use reqwest::Client as HttpClient;
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

#[cfg(test)]
use crate::control_service::TransportSupervisor;
use crate::control_service::{TransportController, TransportStatus};

mod config;

mod ingress;

mod projection;

use projection::{
    ProjectionItem, ProjectionKind, RecoverySpool, completed_projections,
    projection_clears_stale_source, projection_from_entry, same_projection,
};

use ingress::IngressSpool;
#[cfg(test)]
use ingress::prune_ingress_tombstones;

use config::validate_snowflake;
pub use config::{DiscordConfig, DiscordConfigStore, run_command};
#[cfg(test)]
use config::{DiscordManagementService, DiscordManagementState};
pub(crate) use config::{management_service, transport_supervisor};

const CONFIG_VERSION: u32 = 2;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES: usize = 30 * 1024 * 1024;
const DISCORD_MESSAGE_LIMIT: usize = 2_000;
const DISCORD_CHUNK_SIZE: usize = DISCORD_MESSAGE_LIMIT - 100;
const SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(500);
const RECONCILE_RETRY_DELAY: Duration = Duration::from_secs(2);
const BRIDGE_RPC_TIMEOUT: Duration = Duration::from_secs(3);
const DISCORD_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_IDENTIFY_INTERVAL: Duration = Duration::from_secs(5);
const GATEWAY_IDENTIFY_POLL: Duration = Duration::from_millis(100);
const CHILD_JOIN_GRACE: Duration = Duration::from_secs(1);
const MAX_INBOUND_INFLIGHT: usize = 16;
// When an overloaded gateway event arrives before this workspace can prove thread ownership, keep
// only a second bounded tier of stripped route metadata. The owning workspace can then return Busy
// without a non-owning same-token gateway reacting in somebody else's thread.
const MAX_PENDING_ROUTE_REJECTIONS: usize = 16;
const MAX_ACTIVE_MULTIPART_ASSEMBLIES: usize = 32;
const MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION: usize = 1;
const MAX_MULTIPART_PARTS: u32 = 256;
const MAX_MULTIPART_BYTES: usize = 512 * 1024;
const MAX_NONCE_SEARCH_PAGES: usize = 64;
const MAX_INGRESS_TOMBSTONES: usize = 16 * 1_024;
const INGRESS_TOMBSTONE_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MULTIPART_TTL: Duration = Duration::from_secs(30 * 60);
const TRANSPORT_NAME: &str = "discord";
const MAX_MANAGEMENT_REPLAYS: usize = 128;
const REACTION_ACCEPTED: &str = "🔄";
const REACTION_LIVE: &str = "🟡";
const REACTION_COMPLETED: &str = "✅";
const REACTION_FAILED: &str = "⚠️";
const REACTION_BUSY: &str = "❌";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn cached_thread_route_is_current(
    config: &DiscordConfig,
    bridges: &HashMap<String, api::SessionBridge>,
    thread_id: u64,
    session_id: &str,
) -> bool {
    bridges
        .get(session_id)
        .and_then(|bridge| valid_open_thread_route(config, bridge))
        .is_some_and(|(current_thread_id, current_session_id)| {
            current_thread_id == thread_id && current_session_id == session_id
        })
}

fn valid_open_thread_route(
    config: &DiscordConfig,
    bridge: &api::SessionBridge,
) -> Option<(u64, String)> {
    if bridge.lifecycle != api::BridgeLifecycle::Open as i32
        || bridge.transport.as_deref() != Some(TRANSPORT_NAME)
    {
        return None;
    }
    let kind = api::OrchestratorKind::try_from(bridge.kind).ok()?;
    let expected_parent = config.parent_channel(kind)?.get().to_string();
    if bridge.external_parent_id.as_deref() != Some(expected_parent.as_str()) {
        return None;
    }
    let thread_id = bridge.external_thread_id.as_deref()?.parse::<u64>().ok()?;
    Some((thread_id, bridge.session_id.clone()))
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
    #[error("Discord multipart prompt exceeds the {MAX_MULTIPART_PARTS}-part limit")]
    MultipartTooManyParts,
    #[error("Discord multipart prompt exceeds the {MAX_MULTIPART_BYTES}-byte limit")]
    MultipartTooLarge,
    #[error("Discord attachment {name:?} is not a supported HTTPS image")]
    UnsupportedAttachment { name: String },
    #[error("Discord durable ingress store failed")]
    IngressStore(#[source] rusqlite::Error),
    #[error("Discord durable ingress worker stopped")]
    IngressWorker,
    #[error("Discord durable ingress payload is invalid")]
    IngressPayload(#[source] serde_json::Error),
    #[error("Discord delivery cursor is outside the bounded hydrated transcript")]
    DeliveryCursorUnavailable,
    #[error("Discord transcript projection cursor changed concurrently")]
    ProjectionCursorConflict,
    #[error("Nakode bridge RPC exceeded its bounded deadline")]
    BridgeRpcTimeout,
    #[error("Discord gateway shutdown exceeded its bounded deadline")]
    GatewayShutdownTimeout,
    #[error("the workspace service is not running; run `nakode start` first")]
    ServiceNotRunning,
    #[error("Discord transport control failed: {0}")]
    Control(#[from] crate::control_service::ControlError),
    #[error("Discord setup input failed: {0}")]
    SetupInput(#[source] io::Error),
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

    async fn start_inner(&self, _only_if_enabled: bool) -> Result<TransportStatus, DiscordError> {
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
        let task = tokio::spawn(async move {
            // Every workspace service owns only its authoritative workspace projection and ingress
            // state. Discord gateway events fan out to each bot session; this runtime admits only
            // exact typed thread mappings owned by this workspace.
            let error =
                match run_managed_gateway(task_config, workspace, endpoint, store, shutdown_rx)
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
        running: config.enabled
            && runtime
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
    async fn parent_channel_id(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<ChannelId>, serenity::Error>;
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

async fn discord_http<T>(
    operation: impl std::future::Future<Output = Result<T, serenity::Error>>,
) -> Result<T, serenity::Error> {
    tokio::time::timeout(DISCORD_HTTP_TIMEOUT, operation)
        .await
        .map_err(|_| serenity::Error::Other("discord HTTP operation timed out"))?
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
        let message = discord_http(channel_id.send_message(&self.http, request)).await?;
        Ok(external_message(&message))
    }

    async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        content: &str,
    ) -> Result<(), serenity::Error> {
        discord_http(
            channel_id.edit_message(
                &self.http,
                message_id,
                EditMessage::new()
                    .content(content)
                    .allowed_mentions(disabled_mentions()),
            ),
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
        Ok(discord_http(parent_channel_id.create_thread_from_message(
            &self.http,
            starter_message_id,
            CreateThread::new(title),
        ))
        .await?
        .id)
    }

    async fn set_thread_archived(
        &self,
        thread_id: ChannelId,
        archived: bool,
    ) -> Result<(), serenity::Error> {
        discord_http(thread_id.edit_thread(
            &self.http,
            serenity::all::EditThread::new().archived(archived),
        ))
        .await?;
        Ok(())
    }

    async fn parent_channel_id(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<ChannelId>, serenity::Error> {
        Ok(
            match discord_http(thread_id.to_channel(&self.http)).await? {
                serenity::all::Channel::Guild(channel) => channel.parent_id,
                _ => None,
            },
        )
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
        Ok(discord_http(channel_id.messages(&self.http, request))
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
        discord_http(channel_id.create_reaction(
            &self.http,
            message_id,
            serenity::all::ReactionType::Unicode(emoji.to_owned()),
        ))
        .await
    }

    async fn remove_own_reaction(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error> {
        discord_http(channel_id.delete_reaction(
            &self.http,
            message_id,
            None,
            serenity::all::ReactionType::Unicode(emoji.to_owned()),
        ))
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
    received_bytes: usize,
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
            received_bytes: 0,
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
            let received_bytes = group.received_bytes.saturating_add(part.body.len());
            if received_bytes > MAX_MULTIPART_BYTES {
                return Err(DiscordError::MultipartTooLarge);
            }
            atomic_write(
                &group.directory.join(format!("{:010}.part", part.index)),
                part.body.as_bytes(),
            )?;
            group
                .received
                .insert(part.index, (body_hash, message_id.get().to_string()));
            group.received_bytes = received_bytes;
        }
        group.updated = Instant::now();
        if group.received.len() != usize::try_from(group.total).unwrap_or(usize::MAX) {
            return Ok(MultipartOutcome::Waiting);
        }

        let mut text = String::with_capacity(group.received_bytes);
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
        .filter(|(index, total)| {
            *index > 0 && *total > 0 && index <= total && *total <= MAX_MULTIPART_PARTS
        });
    Some(parsed.map_or_else(
        || {
            Err(
                if total
                    .parse::<u32>()
                    .is_ok_and(|total| total > MAX_MULTIPART_PARTS)
                {
                    DiscordError::MultipartTooManyParts
                } else {
                    DiscordError::InvalidConfig(
                        "multipart part numbers must be positive and no greater than the total"
                            .to_owned(),
                    )
                },
            )
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
    /// The durable ingress tombstone already owns this capacity rejection; no replay row exists.
    #[serde(default)]
    local_terminal: bool,
    #[serde(default)]
    route_pending: bool,
}

impl IngressRecord {
    fn from_message(session_id: Option<String>, message: &Message, forced_busy: bool) -> Self {
        let multipart_group = parse_multipart(&message.content)
            .and_then(Result::ok)
            .map(|part| part.group.to_owned());
        let route_pending = session_id.is_none();
        Self {
            version: INGRESS_SCHEMA_VERSION,
            session_id: session_id.unwrap_or_default(),
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
            local_terminal: false,
            route_pending,
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

struct BotState {
    client: NakodeClient,
    workspace_id: String,
    workspace_path: String,
    http_client: HttpClient,
    config: DiscordConfig,
    bridges: tokio::sync::RwLock<HashMap<String, api::SessionBridge>>,
    thread_routes: tokio::sync::RwLock<HashMap<u64, String>>,
    workers: tokio::sync::Mutex<HashMap<String, JoinHandle<()>>>,
    child_tasks: Arc<TrackedChildTasks>,
    thread_creation: tokio::sync::Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    reconciler: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    ingress_replayer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    ingress_inflight: tokio::sync::Mutex<HashSet<String>>,
    ingress_notify: tokio::sync::Notify,
    ingress: Arc<IngressSpool>,
    bot_user_id: std::sync::OnceLock<UserId>,
    inbound_slots: tokio::sync::Semaphore,
    multipart: MultipartAssembler,
    recovery_root: PathBuf,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

#[derive(Default)]
struct TrackedChildTasks {
    handles: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}

impl TrackedChildTasks {
    fn track(&self, handle: &JoinHandle<()>) {
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        handles.retain(|handle| !handle.is_finished());
        handles.push(handle.abort_handle());
    }

    fn abort_all(&self) {
        for handle in self
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            handle.abort();
        }
    }
}

async fn finish_child_tasks(handles: Vec<JoinHandle<()>>, cooperative_grace: Option<Duration>) {
    let deadline = cooperative_grace.map(|grace| tokio::time::Instant::now() + grace);
    for mut handle in handles {
        if let Some(deadline) = deadline
            && tokio::time::timeout_at(deadline, &mut handle).await.is_ok()
        {
            continue;
        }
        handle.abort();
        let _ = handle.await;
    }
}

impl BotState {
    fn track_child(&self, handle: &JoinHandle<()>) {
        self.child_tasks.track(handle);
    }

    async fn stop_tasks(&self) {
        let mut handles = Vec::new();
        if let Some(handle) = self.reconciler.lock().await.take() {
            handles.push(handle);
        }
        if let Some(handle) = self.ingress_replayer.lock().await.take() {
            handles.push(handle);
        }
        handles.extend(self.workers.lock().await.drain().map(|(_, handle)| handle));
        // A configured shutdown signal gives children one shared bounded window to observe their
        // cancellation receiver and join normally. Initialization/error exits abort immediately.
        let cooperative_grace = (*self.shutdown.borrow()).then_some(CHILD_JOIN_GRACE);
        finish_child_tasks(handles, cooperative_grace).await;
    }

    async fn current_bridge(&self, session_id: &str) -> Option<api::SessionBridge> {
        self.bridges.read().await.get(session_id).cloned()
    }

    async fn resolve_thread_route(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<String>, DiscordError> {
        if let Some(session_id) = self
            .thread_routes
            .read()
            .await
            .get(&thread_id.get())
            .cloned()
        {
            let still_valid = {
                let bridges = self.bridges.read().await;
                cached_thread_route_is_current(&self.config, &bridges, thread_id.get(), &session_id)
            };
            if still_valid {
                return Ok(Some(session_id));
            }
            self.thread_routes.write().await.remove(&thread_id.get());
        }
        let workspace = tokio::time::timeout(
            BRIDGE_RPC_TIMEOUT,
            self.client.get_workspace(self.workspace_path.clone(), None),
        )
        .await
        .map_err(|_| DiscordError::BridgeRpcTimeout)??;
        let latest = workspace
            .session_bridges
            .into_iter()
            .map(|bridge| (bridge.session_id.clone(), bridge))
            .collect::<HashMap<_, _>>();
        let routes = latest
            .values()
            .filter_map(|bridge| valid_open_thread_route(&self.config, bridge))
            .collect::<HashMap<_, _>>();
        let resolved = routes.get(&thread_id.get()).cloned();
        *self.bridges.write().await = latest;
        *self.thread_routes.write().await = routes;
        Ok(resolved)
    }

    async fn ingress_enqueue(
        &self,
        proposed: IngressRecord,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.enqueue(&proposed)
        })
        .await
    }

    async fn ingress_next_after(
        &self,
        sequence: i64,
    ) -> Result<Option<(i64, IngressRecord)>, DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.next_after(sequence)
        })
        .await
    }

    async fn ingress_bind_route(
        &self,
        external_event_id: String,
        session_id: String,
        forced_busy: bool,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.bind_route(&external_event_id, &session_id, forced_busy)
        })
        .await
    }

    async fn ingress_force_busy(
        &self,
        external_event_id: String,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.force_busy(&external_event_id)
        })
        .await
    }

    async fn ingress_remove_event(&self, external_event_id: String) -> Result<(), DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.remove_event(&external_event_id)
        })
        .await
    }

    async fn ingress_remove_multipart_group(
        &self,
        session_id: String,
        group: String,
    ) -> Result<(), DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.remove_multipart_group(&session_id, &group)
        })
        .await
    }

    async fn ingress_discard_next_after(&self, sequence: i64) -> Result<(), DiscordError> {
        ingress_io(Arc::clone(&self.ingress), move |ingress| {
            ingress.discard_next_after(sequence)
        })
        .await
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

async fn bridge_rpc<T>(
    operation: impl std::future::Future<Output = Result<T, SdkError>>,
) -> Result<T, DiscordError> {
    tokio::time::timeout(BRIDGE_RPC_TIMEOUT, operation)
        .await
        .map_err(|_| DiscordError::BridgeRpcTimeout)?
        .map_err(DiscordError::Sdk)
}

async fn ingress_io<T>(
    ingress: Arc<IngressSpool>,
    operation: impl FnOnce(&IngressSpool) -> Result<T, DiscordError> + Send + 'static,
) -> Result<T, DiscordError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(&ingress))
        .await
        .map_err(|_| DiscordError::IngressWorker)?
}

struct ChildAbortGuard {
    tasks: Arc<TrackedChildTasks>,
}

impl Drop for ChildAbortGuard {
    fn drop(&mut self) {
        // `DiscordTransport::stop_inner` may abort the top-level gateway after its bounded grace
        // period. Abort handles are synchronous, so no separately spawned worker can outlive that
        // forced parent cancellation even when it is stalled in an RPC.
        self.tasks.abort_all();
    }
}

struct Handler {
    state: Arc<BotState>,
    gateway_ready: tokio::sync::watch::Sender<bool>,
}

enum ManagedGatewayEvent {
    Gateway(Result<(), DiscordError>),
    Configuration(Result<bool, DiscordError>),
}

async fn next_managed_gateway_event<G, C>(
    gateway: std::pin::Pin<&mut G>,
    configuration_changed: std::pin::Pin<&mut C>,
) -> ManagedGatewayEvent
where
    G: std::future::Future<Output = Result<(), DiscordError>> + ?Sized,
    C: std::future::Future<Output = Result<bool, DiscordError>> + ?Sized,
{
    tokio::select! {
        // A simultaneous config/token mutation supersedes an obsolete gateway completion. This
        // deterministic priority prevents a replacement request from being lost as a terminal exit.
        biased;
        changed = configuration_changed => ManagedGatewayEvent::Configuration(changed),
        result = gateway => ManagedGatewayEvent::Gateway(result),
    }
}

async fn await_managed_gateway_shutdown<G>(
    gateway: std::pin::Pin<&mut G>,
    deadline: Duration,
) -> Result<(), DiscordError>
where
    G: std::future::Future<Output = Result<(), DiscordError>> + ?Sized,
{
    tokio::time::timeout(deadline, gateway)
        .await
        .map_err(|_| DiscordError::GatewayShutdownTimeout)?
}

async fn run_managed_gateway(
    mut config: DiscordConfig,
    workspace: PathBuf,
    endpoint: PathBuf,
    store: DiscordConfigStore,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), DiscordError> {
    loop {
        if !config.enabled {
            if !wait_for_runtime_configuration_change(&store, &config, None, shutdown.clone())
                .await?
            {
                return Ok(());
            }
            config = store.load()?;
            continue;
        }

        let change = {
            let token = store.read_token()?;
            let (runtime_shutdown_tx, runtime_shutdown_rx) = tokio::sync::watch::channel(false);
            let gateway = run_gateway(
                config.clone(),
                token.clone(),
                workspace.clone(),
                endpoint.clone(),
                store.clone(),
                runtime_shutdown_rx,
            );
            tokio::pin!(gateway);
            let configuration_changed = wait_for_runtime_configuration_change(
                &store,
                &config,
                Some(token.as_str()),
                shutdown.clone(),
            );
            tokio::pin!(configuration_changed);
            let change =
                match next_managed_gateway_event(gateway.as_mut(), configuration_changed.as_mut())
                    .await
                {
                    ManagedGatewayEvent::Gateway(result) => return result,
                    ManagedGatewayEvent::Configuration(changed) => changed,
                };
            // Never drop a running gateway future as a restart mechanism. Signal its structured
            // shutdown path, await shard shutdown plus tracked child joins, and only then replace it.
            let _ = runtime_shutdown_tx.send(true);
            await_managed_gateway_shutdown(gateway.as_mut(), Duration::from_secs(5)).await?;
            change
        };
        if !change? {
            return Ok(());
        }
        config = store.load()?;
    }
}

async fn wait_for_runtime_configuration_change(
    store: &DiscordConfigStore,
    current_config: &DiscordConfig,
    current_token: Option<&str>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<bool, DiscordError> {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first interval tick is immediate; this closes the save/restart race around gateway
    // construction without logging or projecting credential material.
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(false);
                }
            }
            _ = interval.tick() => {
                let latest = store.load()?;
                if latest != *current_config {
                    return Ok(true);
                }
                if let Some(token) = current_token
                    && store.read_token()? != token
                {
                    return Ok(true);
                }
            }
        }
    }
}

fn discord_session_start_wait(
    remaining: u64,
    reset_after_ms: u64,
    total: u64,
    max_concurrency: u64,
) -> Result<Option<Duration>, DiscordError> {
    if max_concurrency == 0 {
        return Err(DiscordError::InvalidConfig(
            "Discord reported an invalid zero gateway identify concurrency".to_owned(),
        ));
    }
    if remaining > 0 {
        return Ok(None);
    }
    Ok(Some(
        Duration::from_millis(reset_after_ms.max(1_000))
            .saturating_add(retry_delay(0, total ^ max_concurrency)),
    ))
}

async fn await_discord_session_start_budget(
    client: &serenity::Client,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, DiscordError> {
    loop {
        if *shutdown.borrow() {
            return Ok(false);
        }
        let gateway = tokio::time::timeout(BRIDGE_RPC_TIMEOUT, client.http.get_bot_gateway())
            .await
            .map_err(|_| DiscordError::BridgeRpcTimeout)?
            .map_err(DiscordError::Gateway)?;
        let limit = gateway.session_start_limit;
        let Some(wait) = discord_session_start_wait(
            limit.remaining,
            limit.reset_after,
            limit.total,
            limit.max_concurrency,
        )?
        else {
            return Ok(true);
        };

        // Do not consume an exhausted identify budget. Discord supplies the reset duration; add a
        // stable jitter so installations sharing a token do not all retry at once.
        eprintln!(
            "nakode discord: gateway session-start budget exhausted; identify deferred until Discord's reset window"
        );
        tokio::select! {
            _ = shutdown.changed() => return Ok(false),
            () = tokio::time::sleep(wait) => {}
        }
    }
}

struct GatewayIdentifyLease {
    lock: std::fs::File,
    timestamp_path: PathBuf,
}

impl Drop for GatewayIdentifyLease {
    fn drop(&mut self) {
        // Refresh while still holding the lock so the next process waits from the actual
        // Identify/Ready boundary (or this failed attempt), not merely from preflight admission.
        let _ = atomic_write(&self.timestamp_path, unix_time_ms().to_string().as_bytes());
        let _ = fs2::FileExt::unlock(&self.lock);
    }
}

async fn acquire_gateway_identify_lease(
    store: &DiscordConfigStore,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<GatewayIdentifyLease>, DiscordError> {
    let lock_path = store.configuration_directory.join("gateway-identify.lock");
    let timestamp_path = store
        .configuration_directory
        .join("gateway-identify-last-ms");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| io_error(&lock_path, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|source| io_error(&lock_path, source))?;
    }
    loop {
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                tokio::select! {
                    changed = shutdown.changed() => {
                        let _ = changed;
                        return Ok(None);
                    }
                    () = tokio::time::sleep(GATEWAY_IDENTIFY_POLL) => {}
                }
                if *shutdown.borrow() {
                    return Ok(None);
                }
            }
            Err(source) => return Err(io_error(&lock_path, source)),
        }
    }

    let last_identify_ms = match std::fs::read_to_string(&timestamp_path) {
        Ok(value) => value.trim().parse::<u64>().map_err(|_| {
            DiscordError::InvalidConfig("invalid gateway identify checkpoint".to_owned())
        })?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => return Err(io_error(&timestamp_path, source)),
    };
    let elapsed_ms = unix_time_ms().saturating_sub(last_identify_ms);
    let interval_ms = u64::try_from(GATEWAY_IDENTIFY_INTERVAL.as_millis()).unwrap_or(u64::MAX);
    let remaining = Duration::from_millis(interval_ms.saturating_sub(elapsed_ms));
    if !remaining.is_zero() {
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                return Ok(None);
            }
            () = tokio::time::sleep(remaining) => {}
        }
        if *shutdown.borrow() {
            return Ok(None);
        }
    }
    atomic_write(&timestamp_path, unix_time_ms().to_string().as_bytes())?;
    Ok(Some(GatewayIdentifyLease {
        lock,
        timestamp_path,
    }))
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
    let server_info = bridge_rpc(client.get_server_info()).await?;
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
    let workspace_state = bridge_rpc(client.get_workspace(workspace_path, None)).await?;
    let initial_bridges = workspace_state
        .session_bridges
        .into_iter()
        .map(|bridge| (bridge.session_id.clone(), bridge))
        .collect::<HashMap<_, _>>();
    // Prehydrate inbound routes before the gateway can dispatch its first Message event. Ready
    // reconciliation remains authoritative for later changes, but startup never has an empty-map
    // window for already-bound open threads.
    let initial_routes = initial_bridges
        .values()
        .filter_map(|bridge| valid_open_thread_route(&config, bridge))
        .collect();
    let http_client = HttpClient::builder()
        .connect_timeout(BRIDGE_RPC_TIMEOUT)
        .timeout(DISCORD_HTTP_TIMEOUT)
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
        thread_routes: tokio::sync::RwLock::new(initial_routes),
        workers: tokio::sync::Mutex::new(HashMap::new()),
        child_tasks: Arc::new(TrackedChildTasks::default()),
        thread_creation: tokio::sync::Mutex::new(HashMap::new()),
        reconciler: tokio::sync::Mutex::new(None),
        ingress_replayer: tokio::sync::Mutex::new(None),
        ingress_inflight: tokio::sync::Mutex::new(HashSet::new()),
        ingress_notify: tokio::sync::Notify::new(),
        ingress: Arc::new(IngressSpool::open(
            &store.directory.join("discord-ingress.sqlite"),
        )?),
        bot_user_id: std::sync::OnceLock::new(),
        inbound_slots: tokio::sync::Semaphore::new(MAX_INBOUND_INFLIGHT),
        multipart: MultipartAssembler::new(store.directory.join("assemblies"))?,
        recovery_root,
        shutdown,
    });
    let _child_abort_guard = ChildAbortGuard {
        tasks: Arc::clone(&state.child_tasks),
    };
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
    let mut gateway_shutdown = state.shutdown.clone();
    let mut reconnect_attempt = 0;
    let retry_identity = stable_retry_identity(&state.workspace_id);
    loop {
        if *gateway_shutdown.borrow() {
            state.stop_tasks().await;
            return Ok(());
        }
        let (gateway_ready, mut gateway_ready_rx) = tokio::sync::watch::channel(false);
        let handler = Handler {
            state: Arc::clone(&state),
            gateway_ready,
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
        let Some(identify_lease) =
            acquire_gateway_identify_lease(&store, &mut gateway_shutdown).await?
        else {
            state.stop_tasks().await;
            return Ok(());
        };
        if !await_discord_session_start_budget(&discord, &mut gateway_shutdown).await? {
            state.stop_tasks().await;
            return Ok(());
        }
        let mut gateway = Box::pin(discord.start());
        let mut identify_lease = Some(identify_lease);
        let mut ready_signal_open = true;
        let gateway_result = loop {
            tokio::select! {
                result = &mut gateway => break Some(result),
                _ = gateway_shutdown.changed() => {
                    shard_manager.shutdown_all().await;
                    break None;
                }
                changed = gateway_ready_rx.changed(), if identify_lease.is_some() && ready_signal_open => {
                    if changed.is_ok() && *gateway_ready_rx.borrow() {
                        // Keep the installation-wide process lock through Serenity's actual
                        // Identify/Ready boundary. A checkpoint alone leaves a scheduling window
                        // where a delayed first process can identify after a later contender.
                        identify_lease.take();
                    } else if changed.is_err() {
                        // Without an authoritative Ready signal, conservatively retain the lease
                        // until this gateway future terminates rather than admitting a contender.
                        ready_signal_open = false;
                    }
                }
            }
        };
        drop(identify_lease);
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
    let handle = tokio::spawn(async move {
        replay_ingress_loop(discord, task_state).await;
    });
    state.track_child(&handle);
    *slot = Some(handle);
}

#[derive(Debug, Eq, PartialEq)]
enum PendingRouteResolution {
    Routed(String),
    Terminal,
    Deferred,
}

#[async_trait]
trait PendingRouteAuthority: Send + Sync {
    fn discord_config(&self) -> &DiscordConfig;
    async fn resolve_authoritative_thread_route(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<String>, DiscordError>;
    async fn authoritative_bridge(&self, session_id: &str) -> Option<api::SessionBridge>;
}

#[async_trait]
impl PendingRouteAuthority for BotState {
    fn discord_config(&self) -> &DiscordConfig {
        &self.config
    }

    async fn resolve_authoritative_thread_route(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<String>, DiscordError> {
        BotState::resolve_thread_route(self, thread_id).await
    }

    async fn authoritative_bridge(&self, session_id: &str) -> Option<api::SessionBridge> {
        BotState::current_bridge(self, session_id).await
    }
}

async fn resolve_pending_route(
    discord: &dyn DiscordApi,
    authority: &dyn PendingRouteAuthority,
    record: &IngressRecord,
) -> PendingRouteResolution {
    let Ok(thread_snowflake) = record.thread_id.parse::<u64>() else {
        return PendingRouteResolution::Terminal;
    };
    let thread_id = ChannelId::new(thread_snowflake);
    let parent = match tokio::time::timeout(
        BRIDGE_RPC_TIMEOUT,
        discord.parent_channel_id(thread_id),
    )
    .await
    {
        Ok(Ok(Some(parent))) => parent,
        Ok(Ok(None)) => return PendingRouteResolution::Terminal,
        Ok(Err(error)) if is_not_found(&error) => return PendingRouteResolution::Terminal,
        Ok(Err(error)) => {
            eprintln!(
                "nakode discord: pending route parent lookup deferred ({})",
                sanitized_bridge_error(&DiscordError::Gateway(error))
            );
            return PendingRouteResolution::Deferred;
        }
        Err(_) => return PendingRouteResolution::Deferred,
    };
    let parent_is_configured = [
        authority.discord_config().chat_channel_id.as_deref(),
        authority.discord_config().agent_channel_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|configured| configured == parent.get().to_string());
    if !parent_is_configured {
        return PendingRouteResolution::Terminal;
    }

    let session_id = match authority
        .resolve_authoritative_thread_route(thread_id)
        .await
    {
        Ok(Some(session_id)) => session_id,
        Ok(None) => return PendingRouteResolution::Terminal,
        Err(error) => {
            eprintln!(
                "nakode discord: pending authoritative route lookup deferred ({})",
                sanitized_bridge_error(&error)
            );
            return PendingRouteResolution::Deferred;
        }
    };
    let Some(bridge) = authority.authoritative_bridge(&session_id).await else {
        return PendingRouteResolution::Deferred;
    };
    if valid_open_thread_route(authority.discord_config(), &bridge)
        != Some((thread_snowflake, session_id.clone()))
    {
        return PendingRouteResolution::Terminal;
    }
    PendingRouteResolution::Routed(session_id)
}

async fn replay_ingress_loop(discord: Arc<dyn DiscordApi>, state: Arc<BotState>) {
    let mut sequence = 0i64;
    let mut shutdown = state.shutdown.clone();
    loop {
        if *shutdown.borrow() {
            return;
        }
        match state.ingress_next_after(sequence).await {
            Ok(Some((next_sequence, mut record))) => {
                sequence = next_sequence;
                let mut permit = None;
                if record.route_pending {
                    let session_id =
                        match resolve_pending_route(&*discord, state.as_ref(), &record).await {
                            PendingRouteResolution::Routed(session_id) => session_id,
                            PendingRouteResolution::Terminal => {
                                if let Err(error) =
                                    state.ingress_remove_event(record.message_id.clone()).await
                                {
                                    eprintln!(
                                        "nakode discord: rejected route cleanup deferred ({})",
                                        sanitized_bridge_error(&error)
                                    );
                                }
                                continue;
                            }
                            PendingRouteResolution::Deferred => continue,
                        };
                    permit = state.inbound_slots.try_acquire().ok();
                    record = match state
                        .ingress_bind_route(record.message_id.clone(), session_id, permit.is_none())
                        .await
                    {
                        Ok(Some(record)) => record,
                        Ok(None) => continue,
                        Err(error) => {
                            eprintln!(
                                "nakode discord: pending route checkpoint deferred ({})",
                                sanitized_bridge_error(&error)
                            );
                            continue;
                        }
                    };
                }
                if record.forced_busy {
                    permit = None;
                }
                if !record.forced_busy && permit.is_none() {
                    permit = state.inbound_slots.try_acquire().ok();
                    if permit.is_none() {
                        record = match state.ingress_force_busy(record.message_id.clone()).await {
                            Ok(Some(record)) => record,
                            Ok(None) => continue,
                            Err(error) => {
                                eprintln!(
                                    "nakode discord: busy ingress checkpoint deferred ({})",
                                    sanitized_bridge_error(&error)
                                );
                                continue;
                            }
                        };
                    }
                }
                if !claim_ingress(&state, &record.message_id).await {
                    continue;
                }
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
                    if let Err(discard_error) = state.ingress_discard_next_after(sequence).await {
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
        IngressProcessOutcome::Terminal => {
            state.ingress_remove_event(record.message_id.clone()).await
        }
        IngressProcessOutcome::TerminalMultipart(group) => {
            state.multipart.finish(&record.session_id, &group).await;
            state
                .ingress_remove_multipart_group(record.session_id.clone(), group)
                .await
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
    let handle = tokio::spawn(async move {
        reconcile_loop(discord, task_state).await;
    });
    state.track_child(&handle);
    *slot = Some(handle);
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
                    clear_thread_binding(&state, bridge, thread_id).await?;
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
            clear_thread_binding(&state, bridge, thread_id).await?;
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
                clear_thread_binding(&state, bridge, thread_id).await?;
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
    let binding = bridge_rpc(state.client.bind_session_bridge_thread(
        api::BindSessionBridgeThreadRequest {
            mutation: None,
            session_id: bridge.session_id.clone(),
            transport: TRANSPORT_NAME.to_owned(),
            external_parent_id: expected_parent.get().to_string(),
            external_thread_id: thread_id.get().to_string(),
        },
    ))
    .await;
    if let Err(error) = binding {
        // A separately reconnecting gateway/process may have won after our preflight. Adopt only the
        // authoritative Nakode binding and archive the unclaimed Discord thread best-effort.
        if let Ok(workspace) = bridge_rpc(
            state
                .client
                .get_workspace(state.workspace_path.clone(), None),
        )
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
        return Err(error);
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

fn clear_local_thread_binding(
    bridge: &mut api::SessionBridge,
    thread_id: ChannelId,
    expected_revision: u64,
) -> bool {
    if bridge.revision != expected_revision
        || bridge.transport.as_deref() != Some(TRANSPORT_NAME)
        || bridge.external_thread_id.as_deref() != Some(&thread_id.get().to_string())
    {
        return false;
    }
    bridge.transport = None;
    bridge.external_parent_id = None;
    bridge.external_thread_id = None;
    bridge.live_turn_id = None;
    bridge.live_external_message_id = None;
    bridge.active_source_message_id = None;
    if let Some(delivery) = &mut bridge.delivery {
        delivery.completed_parts = 0;
        delivery.last_external_message_id = None;
    }
    bridge.revision = bridge.revision.saturating_add(1);
    true
}

async fn clear_thread_binding(
    state: &BotState,
    bridge: &api::SessionBridge,
    thread_id: ChannelId,
) -> Result<(), DiscordError> {
    bridge_rpc(
        state
            .client
            .clear_session_bridge_thread(api::ClearSessionBridgeThreadRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                transport: TRANSPORT_NAME.to_owned(),
                external_thread_id: thread_id.get().to_string(),
            }),
    )
    .await?;
    // The RPC compare-clears the authoritative thread. Only mirror that clear into the cache when
    // no newer snapshot (including a same-thread rebind) arrived while the RPC was in flight.
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        clear_local_thread_binding(current, thread_id, bridge.revision);
    }
    Ok(())
}

async fn create_or_recover_thread(
    discord: &dyn DiscordApi,
    parent_channel_id: ChannelId,
    bridge: &api::SessionBridge,
) -> Result<ChannelId, DiscordError> {
    let nonce = starter_nonce(parent_channel_id, &bridge.session_id);
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
    let handle = tokio::spawn(async move {
        watch_session_bridge(discord, worker_state, session_id).await;
    });
    state.track_child(&handle);
    workers.insert(key, handle);
}

async fn stop_worker(state: &BotState, session_id: &str) {
    if let Some(handle) = state.workers.lock().await.remove(session_id) {
        handle.abort();
        let _ = handle.await;
    }
}

enum ReadyUpdateDrain {
    Open,
    Interrupted(SdkError),
    Ended,
}

/// Coalesces only updates that are already buffered. Awaiting `try_next()` here would wait for a
/// future snapshot and permanently postpone projection whenever the watch remains healthy but idle.
fn drain_ready_hydrated_updates(
    updates: &mut Watch<HydratedSession>,
    hydrated: &mut HydratedSession,
) -> ReadyUpdateDrain {
    loop {
        match updates.next().now_or_never() {
            None => return ReadyUpdateDrain::Open,
            Some(Some(Ok(next))) => *hydrated = next,
            Some(Some(Err(error))) => return ReadyUpdateDrain::Interrupted(error),
            Some(None) => return ReadyUpdateDrain::Ended,
        }
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
    let mut terminal_reaction = None;
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
                // `watch_hydrated_session` reconnects internally. Keep consuming this stream rather
                // than spawning a second listener and orphaning the first reconnect loop.
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
        let ready_drain = drain_ready_hydrated_updates(&mut updates, &mut hydrated);
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
            &mut terminal_reaction,
        )
        .await
        {
            eprintln!(
                "nakode discord: outbound projection deferred for session {} ({})",
                short_identity(&session_id),
                sanitized_bridge_error(&error)
            );
            tokio::select! {
                _ = shutdown.changed() => return,
                () = tokio::time::sleep(RECONCILE_RETRY_DELAY) => {}
            }
        }
        match ready_drain {
            ReadyUpdateDrain::Open => {}
            ReadyUpdateDrain::Interrupted(error) => {
                eprintln!(
                    "nakode discord: session bridge watch reconnecting for {} ({})",
                    short_identity(&session_id),
                    sanitized_sdk_error(&error)
                );
                if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                    return;
                }
                watch_attempt = watch_attempt.saturating_add(1);
            }
            ReadyUpdateDrain::Ended => {
                if !wait_for_reconnect(&mut shutdown, watch_attempt, retry_identity).await {
                    return;
                }
                watch_attempt = watch_attempt.saturating_add(1);
                updates = state
                    .client
                    .watch_hydrated_session(session_id.clone(), 1_024);
            }
        }
    }
}

async fn project_session_update(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    hydrated: &HydratedSession,
    terminal_reaction: &mut Option<String>,
) -> Result<(), DiscordError> {
    let active_turn_id = hydrated
        .state
        .active_turn
        .as_ref()
        .map(|turn| turn.id.as_str());
    let active_owner_turn_id = hydrated
        .state
        .active_turn
        .as_ref()
        .map(|turn| turn.id.as_str());
    let entries = hydrated
        .state
        .transcript
        .as_ref()
        .map(|transcript| transcript.entries.as_slice())
        .unwrap_or_default();
    let projections = completed_projections(entries, active_turn_id);
    let transcript_has_earlier = hydrated
        .state
        .transcript
        .as_ref()
        .is_some_and(|transcript| transcript.has_earlier);
    let mut cursor = bridge.last_projected.clone();
    let cursor_position = cursor
        .as_ref()
        .and_then(|projected| projections.iter().position(|item| item.matches(projected)));
    let needs_recovery = transcript_has_earlier
        && match cursor.as_ref() {
            None => !projections.is_empty() || active_turn_id.is_some(),
            Some(_) => cursor_position.is_none(),
        };
    if needs_recovery {
        recover_projections_after_cursor(
            discord,
            state,
            thread_id,
            bridge,
            cursor.as_ref(),
            active_turn_id,
            active_owner_turn_id,
        )
        .await?;
    } else {
        let start = cursor_position.map_or(0, |index| index + 1);
        if cursor.is_some() && cursor_position.is_none() && !projections.is_empty() {
            return Err(DiscordError::DeliveryCursorUnavailable);
        }
        for projection in projections.iter().skip(start) {
            deliver_projection(
                discord,
                state,
                thread_id,
                bridge,
                projection,
                cursor.as_ref(),
                active_owner_turn_id,
            )
            .await?;
            cursor = Some(projection.cursor());
        }
    }

    // Every completed user projection—including a durable zero-message cursor advance for a
    // Discord-origin prompt—is finalized before any live assistant preview.
    if let Some(turn) = &hydrated.state.active_turn
        && let Some(body) = assistant_body_for_turn(hydrated, &turn.id, false)
    {
        let current_bridge = state
            .current_bridge(&bridge.session_id)
            .await
            .unwrap_or_else(|| bridge.clone());
        project_live(discord, state, thread_id, &current_bridge, &turn.id, &body).await?;
    }

    if let Some(turn) = &hydrated.state.last_turn
        && matches!(
            api::TurnStatus::try_from(turn.status),
            Ok(api::TurnStatus::Failed | api::TurnStatus::Interrupted)
        )
        && terminal_reaction.as_deref() != Some(turn.id.as_str())
    {
        let current_bridge = state
            .current_bridge(&bridge.session_id)
            .await
            .unwrap_or_else(|| bridge.clone());
        project_terminal_failure(discord, state, thread_id, &current_bridge).await?;
        *terminal_reaction = Some(turn.id.clone());
    }
    Ok(())
}

async fn recover_projections_after_cursor(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
    cursor: Option<&api::BridgeProjection>,
    active_turn: Option<&str>,
    active_owner_turn: Option<&str>,
) -> Result<(), DiscordError> {
    let mut spool = RecoverySpool::new(&state.recovery_root, &bridge.session_id)?;
    let mut before_entry_id = None;
    let mut found_cursor = false;
    loop {
        let page = bridge_rpc(
            state
                .client
                .get_transcript_page(api::GetTranscriptPageRequest {
                    owner_kind: api::TranscriptOwnerKind::Session as i32,
                    owner_id: bridge.session_id.clone(),
                    before_entry_id: before_entry_id.clone(),
                    limit: 256,
                }),
        )
        .await?;
        // Transcript pages are chronological (oldest to newest). Walk from the page's oldest ID,
        // but spool newest to oldest so one final reverse spans every fetched page correctly.
        let next_before = page.entries.first().map(|entry| entry.id.clone());
        for entry in page.entries.iter().rev() {
            let Some(projection) = projection_from_entry(entry, active_turn) else {
                continue;
            };
            if cursor.is_some_and(|cursor| projection.matches(cursor)) {
                found_cursor = true;
                break;
            }
            spool.push(entry)?;
        }
        if found_cursor {
            break;
        }
        if !page.has_earlier || page.entries.is_empty() || next_before == before_entry_id {
            if cursor.is_some() && !found_cursor {
                return Err(DiscordError::DeliveryCursorUnavailable);
            }
            break;
        }
        before_entry_id = next_before;
    }

    let mut expected = cursor.cloned();
    for stored in spool.oldest_first() {
        let stored = stored?;
        let mut entry = api::TranscriptEntry {
            id: stored.id,
            kind: stored.kind,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some(stored.turn_id),
            source_transport: stored.source_transport,
            body: stored.body,
            body_start_byte: stored.body_start_byte,
            body_total_bytes: stored.body_total_bytes,
            ..api::TranscriptEntry::default()
        };
        bridge_rpc(state.client.hydrate_transcript_entry(
            api::TranscriptOwnerKind::Session,
            &bridge.session_id,
            &mut entry,
        ))
        .await?;
        let Some(projection) = projection_from_entry(&entry, active_turn) else {
            continue;
        };
        deliver_projection(
            discord,
            state,
            thread_id,
            bridge,
            &projection,
            expected.as_ref(),
            active_owner_turn,
        )
        .await?;
        expected = Some(projection.cursor());
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
    let nonce = live_nonce(thread_id, &bridge.session_id, turn_id);
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

async fn project_terminal_failure(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    bridge: &api::SessionBridge,
) -> Result<(), DiscordError> {
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
    clear_terminal_bridge_state(state, bridge, bridge.active_source_message_id.as_deref()).await
}

async fn clear_terminal_bridge_state(
    state: &BotState,
    bridge: &api::SessionBridge,
    expected_active_source_message_id: Option<&str>,
) -> Result<(), DiscordError> {
    bridge_rpc(
        state
            .client
            .set_bridge_live_message(api::SetBridgeLiveMessageRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: None,
                external_message_id: None,
                clear_active_source_message_id: expected_active_source_message_id
                    .map(str::to_owned),
            }),
    )
    .await?;
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        current.live_turn_id = None;
        current.live_external_message_id = None;
        if expected_active_source_message_id.is_some()
            && current.active_source_message_id.as_deref() == expected_active_source_message_id
        {
            current.active_source_message_id = None;
        }
    }
    Ok(())
}

async fn set_live_message(
    state: &BotState,
    bridge: &api::SessionBridge,
    turn_id: Option<String>,
    external_message_id: Option<String>,
) -> Result<(), DiscordError> {
    bridge_rpc(
        state
            .client
            .set_bridge_live_message(api::SetBridgeLiveMessageRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: turn_id.clone(),
                external_message_id: external_message_id.clone(),
                clear_active_source_message_id: None,
            }),
    )
    .await?;
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        current.live_turn_id = turn_id;
        current.live_external_message_id = external_message_id;
    }
    Ok(())
}

async fn deliver_projection(
    discord: &dyn DiscordApi,
    state: &BotState,
    thread_id: ChannelId,
    projected_bridge: &api::SessionBridge,
    projection: &ProjectionItem,
    expected_previous: Option<&api::BridgeProjection>,
    active_owner_turn: Option<&str>,
) -> Result<(), DiscordError> {
    let current_bridge = state
        .current_bridge(&projected_bridge.session_id)
        .await
        .unwrap_or_else(|| projected_bridge.clone());
    let bridge = &current_bridge;
    let target = projection.cursor();
    if same_projection(bridge.last_projected.as_ref(), Some(&target)) {
        return Ok(());
    }
    if !same_projection(bridge.last_projected.as_ref(), expected_previous) {
        return Err(DiscordError::ProjectionCursorConflict);
    }
    if projection_clears_stale_source(
        projection,
        active_owner_turn,
        bridge.active_source_message_id.as_deref(),
    ) && let Some(source_message_id) = bridge.active_source_message_id.as_deref()
    {
        // Only the actively-running source-neutral owner turn may clear stale reaction ownership.
        // Historical recovery and the accepted-before-provider-start window must not sever a newer
        // Discord continuation from its source message.
        clear_terminal_bridge_state(state, bridge, Some(source_message_id)).await?;
    }
    let safe_body = if projection.suppressed {
        String::new()
    } else {
        visible_discord_content(&projection.body)
    };
    let body_sha256 = hex_digest(safe_body.as_bytes());
    // A trusted Discord-origin user prompt advances the authoritative typed cursor with zero
    // message parts. The server validates that suppression against durable turn provenance.
    // Other projections use the same streaming chunker below without retaining all chunk text or
    // per-part metadata in memory or in the authoritative projection.
    let part_count = if projection.suppressed {
        0
    } else {
        u64::try_from(DiscordChunks::new(&safe_body).count()).map_err(|_| {
            DiscordError::InvalidConfig(
                "transcript projection has too many Discord chunks".to_owned(),
            )
        })?
    };

    let pending = bridge.delivery.as_ref().filter(|delivery| {
        delivery.projection_kind == projection.kind.api_value()
            && delivery.turn_id == projection.turn_id
            && same_projection(delivery.previous_projection.as_ref(), expected_previous)
            && delivery.body_sha256 == body_sha256
            && delivery.part_count == part_count
    });
    if bridge.delivery.is_some() && pending.is_none() {
        return Err(DiscordError::ProjectionCursorConflict);
    }
    if pending.is_none() {
        bridge_rpc(
            state
                .client
                .prepare_bridge_delivery(api::PrepareBridgeDeliveryRequest {
                    mutation: None,
                    session_id: bridge.session_id.clone(),
                    turn_id: projection.turn_id.clone(),
                    body_sha256: body_sha256.clone(),
                    part_count,
                    projection_kind: projection.kind.api_value(),
                    expected_last_projected: expected_previous.cloned(),
                }),
        )
        .await?;
    }
    let completed_parts = pending.map_or(0, |delivery| delivery.completed_parts);
    if completed_parts > part_count {
        return Err(DiscordError::InvalidConfig(
            "invalid transcript delivery progress".to_owned(),
        ));
    }

    let mut chunks = DiscordChunks::new(&safe_body);
    let projected_chunks = std::iter::from_fn(|| {
        if projection.suppressed {
            None
        } else {
            chunks.next()
        }
    });
    for (index, chunk) in projected_chunks.enumerate() {
        let part_index = u64::try_from(index).map_err(|_| {
            DiscordError::InvalidConfig(
                "transcript projection has too many Discord chunks".to_owned(),
            )
        })?;
        if part_index < completed_parts {
            continue;
        }
        let nonce = projection_nonce(
            thread_id,
            &bridge.session_id,
            projection.kind,
            &projection.turn_id,
            index,
        );
        let can_reuse_live = projection.kind == ProjectionKind::Assistant
            && index == 0
            && bridge.live_turn_id.as_deref() == Some(projection.turn_id.as_str())
            && bridge.live_external_message_id.is_some();
        let message_id = if can_reuse_live {
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
        // response fails, the deterministic kind-aware nonce makes the whole part safe to retry.
        if projection.kind == ProjectionKind::Assistant {
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
        } else {
            discord
                .react(thread_id, message_id, REACTION_ACCEPTED)
                .await?;
        }
        bridge_rpc(state.client.complete_bridge_delivery_part(
            api::CompleteBridgeDeliveryPartRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: projection.turn_id.clone(),
                part_index,
                external_message_id: message_id.get().to_string(),
                projection_kind: projection.kind.api_value(),
            },
        ))
        .await?;
    }
    if projection.kind == ProjectionKind::Assistant {
        react_source(discord, thread_id, bridge, REACTION_COMPLETED).await?;
    }
    bridge_rpc(
        state
            .client
            .finalize_bridge_delivery(api::FinalizeBridgeDeliveryRequest {
                mutation: None,
                session_id: bridge.session_id.clone(),
                turn_id: projection.turn_id.clone(),
                projection_kind: projection.kind.api_value(),
            }),
    )
    .await?;
    if let Some(current) = state.bridges.write().await.get_mut(&bridge.session_id) {
        current.last_projected = Some(target);
        current.delivery = None;
        if projection.kind == ProjectionKind::Assistant {
            current.live_turn_id = None;
            current.live_external_message_id = None;
            current.active_source_message_id = None;
        }
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
    feedback_step(
        discord
            .remove_own_reaction(thread_id, message_id, REACTION_ACCEPTED)
            .await,
    )?;
    feedback_step(discord.react(thread_id, message_id, reaction).await)
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let _ = self.gateway_ready.send(true);
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
        let cached_session = self
            .state
            .thread_routes
            .read()
            .await
            .get(&message.channel_id.get())
            .cloned();
        let session_id = if let Some(session_id) = cached_session {
            let valid = self
                .state
                .current_bridge(&session_id)
                .await
                .is_some_and(|bridge| {
                    valid_open_thread_route(&self.state.config, &bridge)
                        == Some((message.channel_id.get(), session_id.clone()))
                });
            valid.then_some(session_id)
        } else {
            None
        };
        let discord = SerenityDiscordApi {
            http: Arc::clone(&ctx.http),
        };
        let route_resolved = session_id.is_some();
        let slot = session_id
            .as_ref()
            .and_then(|_| self.state.inbound_slots.try_acquire().ok());
        let proposed =
            IngressRecord::from_message(session_id, &message, route_resolved && slot.is_none());
        let record = match self.state.ingress_enqueue(proposed).await {
            Ok(Some(record)) => record,
            Ok(None) => return,
            Err(error) => {
                let _ = tokio::time::timeout(
                    BRIDGE_RPC_TIMEOUT,
                    discord.react(message.channel_id, message.id, REACTION_FAILED),
                )
                .await;
                eprintln!(
                    "nakode discord: durable ingress checkpoint failed ({})",
                    sanitized_bridge_error(&error)
                );
                return;
            }
        };
        if record.local_terminal {
            drop(slot);
            if record.route_pending {
                eprintln!(
                    "nakode discord: unresolved inbound event dropped at the bounded ownership-check limit"
                );
            } else {
                let _ = tokio::time::timeout(
                    BRIDGE_RPC_TIMEOUT,
                    mark_message_busy(&discord, message.channel_id, message.id),
                )
                .await;
            }
            return;
        }
        if record.route_pending {
            drop(slot);
            self.state.ingress_notify.notify_one();
            return;
        }
        if record.forced_busy || slot.is_none() {
            let _ = tokio::time::timeout(
                BRIDGE_RPC_TIMEOUT,
                mark_message_busy(&discord, message.channel_id, message.id),
            )
            .await;
        } else {
            // The durable record and exact cached route exist before optimistic feedback. Nakode's
            // atomic readiness check may still replace this with Busy, but attachment downloads and
            // replay scheduling never delay the documented continuing state.
            let _ = tokio::time::timeout(
                BRIDGE_RPC_TIMEOUT,
                discord.react(message.channel_id, message.id, REACTION_ACCEPTED),
            )
            .await;
        }
        drop(slot);
        // The tracked replayer is the sole owner of continuation RPCs and attachment work. Event
        // callbacks only durably admit, apply an immediate busy reaction, and wake that worker.
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
            let feedback = if let (Some(channel_id), Some(message_id)) = (channel_id, message_id) {
                let disposition =
                    api::BridgeContinuationDisposition::try_from(response.disposition)
                        .unwrap_or(api::BridgeContinuationDisposition::Unspecified);
                if disposition == api::BridgeContinuationDisposition::Duplicate {
                    match response
                        .replayed_disposition
                        .and_then(|value| api::BridgeContinuationDisposition::try_from(value).ok())
                    {
                        Some(api::BridgeContinuationDisposition::Accepted) => {
                            match response.replayed_source_active {
                                Some(true) => {
                                    mark_message_accepted(discord, channel_id, message_id).await
                                }
                                Some(false) => {
                                    remove_accepted_feedback(discord, channel_id, message_id).await
                                }
                                None => mark_message_failed(discord, channel_id, message_id).await,
                            }
                        }
                        Some(api::BridgeContinuationDisposition::Busy) => {
                            if terminal_reaction == REACTION_BUSY {
                                mark_message_busy(discord, channel_id, message_id).await
                            } else {
                                mark_message_failed(discord, channel_id, message_id).await
                            }
                        }
                        _ => mark_message_failed(discord, channel_id, message_id).await,
                    }
                } else if terminal_reaction == REACTION_BUSY {
                    mark_message_busy(discord, channel_id, message_id).await
                } else {
                    mark_message_failed(discord, channel_id, message_id).await
                }
            } else {
                Ok(())
            };
            terminal_feedback_outcome(feedback, &bridge.session_id)
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
    if let (Some(channel_id), Some(message_id)) = (
        record.thread_id.parse::<u64>().ok().map(ChannelId::new),
        record.message_id.parse::<u64>().ok().map(MessageId::new),
    ) {
        let _ = tokio::time::timeout(
            BRIDGE_RPC_TIMEOUT,
            discord.react(channel_id, message_id, REACTION_ACCEPTED),
        )
        .await;
    }
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
    let multipart_group = part.group.to_owned();
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
            let feedback = if let Some(channel_id) = channel_id {
                mark_message_accepted(discord, channel_id, message_id).await
            } else {
                Ok(())
            };
            terminal_feedback_outcome(feedback, &bridge.session_id)
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
            let outcome =
                consume_record_as_busy(discord, state, bridge, record, REACTION_FAILED).await;
            if matches!(outcome, IngressProcessOutcome::Terminal) {
                state
                    .multipart
                    .finish(&bridge.session_id, &multipart_group)
                    .await;
                IngressProcessOutcome::TerminalMultipart(multipart_group)
            } else {
                outcome
            }
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
    let accepted_feedback = mark_message_accepted(discord, channel_id, reaction_message_id).await;
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
                    terminal_feedback_outcome(accepted_feedback, &bridge.session_id)
                }
                api::BridgeContinuationDisposition::Duplicate => {
                    let replayed = response
                        .replayed_disposition
                        .and_then(|value| api::BridgeContinuationDisposition::try_from(value).ok())
                        .unwrap_or(api::BridgeContinuationDisposition::Unspecified);
                    match replayed {
                        api::BridgeContinuationDisposition::Accepted => {
                            match response.replayed_source_active {
                                Some(true) => {
                                    if let Some(current) =
                                        state.bridges.write().await.get_mut(&bridge.session_id)
                                    {
                                        current.active_source_message_id =
                                            Some(accepted_source_message_id.clone());
                                    }
                                    terminal_feedback_outcome(accepted_feedback, &bridge.session_id)
                                }
                                Some(false) => terminal_feedback_outcome(
                                    remove_accepted_feedback(
                                        discord,
                                        channel_id,
                                        reaction_message_id,
                                    )
                                    .await,
                                    &bridge.session_id,
                                ),
                                None => terminal_feedback_outcome(
                                    mark_message_failed(discord, channel_id, reaction_message_id)
                                        .await,
                                    &bridge.session_id,
                                ),
                            }
                        }
                        api::BridgeContinuationDisposition::Busy => terminal_feedback_outcome(
                            mark_message_busy(discord, channel_id, reaction_message_id).await,
                            &bridge.session_id,
                        ),
                        api::BridgeContinuationDisposition::Unspecified
                        | api::BridgeContinuationDisposition::Duplicate => {
                            // Legacy identity-only rows cannot safely reconstruct Accepted versus
                            // Busy. Terminate visibly instead of retrying forever or guessing.
                            terminal_feedback_outcome(
                                mark_message_failed(discord, channel_id, reaction_message_id).await,
                                &bridge.session_id,
                            )
                        }
                    }
                }
                api::BridgeContinuationDisposition::Busy => terminal_feedback_outcome(
                    mark_message_busy(discord, channel_id, reaction_message_id).await,
                    &bridge.session_id,
                ),
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

fn feedback_step(result: Result<(), serenity::Error>) -> Result<(), DiscordError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn remove_accepted_feedback(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    message_id: MessageId,
) -> Result<(), DiscordError> {
    feedback_step(
        discord
            .remove_own_reaction(channel_id, message_id, REACTION_ACCEPTED)
            .await,
    )
}

fn terminal_feedback_outcome(
    feedback: Result<(), DiscordError>,
    session_id: &str,
) -> IngressProcessOutcome {
    match feedback {
        Ok(()) => IngressProcessOutcome::Terminal,
        Err(error) => {
            eprintln!(
                "nakode discord: terminal inbound feedback deferred for session {} ({})",
                short_identity(session_id),
                sanitized_bridge_error(&error)
            );
            IngressProcessOutcome::Deferred
        }
    }
}

async fn mark_message_accepted(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    message_id: MessageId,
) -> Result<(), DiscordError> {
    feedback_step(
        discord
            .remove_own_reaction(channel_id, message_id, REACTION_FAILED)
            .await,
    )?;
    feedback_step(
        discord
            .react(channel_id, message_id, REACTION_ACCEPTED)
            .await,
    )
}

async fn mark_message_failed(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    message_id: MessageId,
) -> Result<(), DiscordError> {
    feedback_step(
        discord
            .remove_own_reaction(channel_id, message_id, REACTION_ACCEPTED)
            .await,
    )?;
    feedback_step(discord.react(channel_id, message_id, REACTION_FAILED).await)?;
    let nonce = failed_nonce(message_id);
    send_or_recover_final_part(
        discord,
        channel_id,
        &nonce,
        "⚠️ This message could not be accepted safely. Check that the session is ready, then send a new message.",
    )
    .await
    .map(|_| ())
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

async fn mark_message_busy(
    discord: &dyn DiscordApi,
    channel_id: ChannelId,
    message_id: MessageId,
) -> Result<(), DiscordError> {
    feedback_step(
        discord
            .remove_own_reaction(channel_id, message_id, REACTION_ACCEPTED)
            .await,
    )?;
    feedback_step(discord.react(channel_id, message_id, REACTION_BUSY).await)?;
    let nonce = busy_nonce(message_id);
    send_or_recover_final_part(
        discord,
        channel_id,
        &nonce,
        "❌ This session is busy or closed. Wait for the active turn to finish, then send a new message.",
    )
    .await
    .map(|_| ())
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

fn starter_nonce(parent_channel_id: ChannelId, session_id: &str) -> String {
    format!(
        "nk-s-{}",
        &hex_digest(format!("{}:{session_id}", parent_channel_id.get()).as_bytes())[..20]
    )
}

fn live_nonce(thread_id: ChannelId, session_id: &str, turn_id: &str) -> String {
    format!(
        "nk-l-{}",
        &hex_digest(format!("{}:{session_id}:{turn_id}", thread_id.get()).as_bytes())[..20]
    )
}

fn projection_nonce(
    thread_id: ChannelId,
    session_id: &str,
    kind: ProjectionKind,
    turn_id: &str,
    index: usize,
) -> String {
    format!(
        "nk-p-{}",
        &hex_digest(
            format!(
                "{}:{session_id}:{}:{turn_id}:{index}",
                thread_id.get(),
                kind.nonce_label()
            )
            .as_bytes()
        )[..20]
    )
}

#[cfg(test)]
fn final_nonce(thread_id: ChannelId, session_id: &str, turn_id: &str, index: usize) -> String {
    projection_nonce(
        thread_id,
        session_id,
        ProjectionKind::Assistant,
        turn_id,
        index,
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
        DiscordError::MultipartTooManyParts => "multipart prompt has too many parts",
        DiscordError::MultipartTooLarge => "multipart prompt too large",
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
mod tests;
