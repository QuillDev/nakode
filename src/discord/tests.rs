use std::{
    collections::HashMap,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use futures_util::FutureExt;
use nakode_protocol::{
    CredentialInput, DiscordIntegrationInput, DiscordRuntimeState, ErrorCode, IdempotencyKey,
};
use nakode_server::grpc::{DiscordManagement, DiscordManagementMutation};
use serenity::{
    all::{ChannelId, MessageId, UserId},
    gateway::GatewayError,
};

use super::{
    CONFIG_VERSION, ChildAbortGuard, DiscordApi, DiscordConfig, DiscordConfigStore, DiscordError,
    DiscordManagementService, DiscordManagementState, DiscordRuntime, DiscordTransport,
    ExternalMessage, IngressAttachment, IngressProcessOutcome, IngressRecord, IngressSpool,
    MAX_ACTIVE_MULTIPART_ASSEMBLIES_PER_SESSION, ManagedGatewayEvent, MultipartAssembler,
    MultipartOutcome, PendingRouteAuthority, PendingRouteResolution, ProjectionKind, REACTION_BUSY,
    RUNTIME_ERROR_INVALID_INTENTS, RUNTIME_ERROR_INVALID_TOKEN,
    RUNTIME_ERROR_MESSAGE_CONTENT_INTENT, ReadyUpdateDrain, RecoverySpool, TrackedChildTasks,
    TransportController, TransportStatus, TransportSupervisor, acquire_gateway_identify_lease,
    await_managed_gateway_shutdown, busy_nonce, cached_thread_route_is_current,
    clear_local_thread_binding, completed_projections, connect_api, create_or_recover_thread,
    discord_session_start_wait, drain_ready_hydrated_updates, failed_nonce, final_nonce,
    find_message_by_nonce, finish_child_tasks, ingress_io, is_approved_discord_cdn_url,
    is_terminal_gateway_error, mark_message_busy, next_managed_gateway_event, parse_multipart,
    projection_clears_stale_source, projection_nonce, reconciliation_snapshot,
    resolve_pending_route, sanitize_mentions, sanitized_bridge_error, send_or_recover_final_part,
    split_discord_content, starter_nonce, terminal_feedback_outcome, terminal_http_gateway_error,
    thread_title, valid_open_thread_route, validate_snowflake, visible_discord_content,
    wait_for_runtime_configuration_change,
};
use nakode_sdk::{HydratedSession, Watch, v1 as api};

fn enabled_config() -> DiscordConfig {
    DiscordConfig {
        version: CONFIG_VERSION,
        runtime_generation: 0,
        enabled: true,
        chat_channel_id: Some("43".to_owned()),
        agent_channel_id: Some("44".to_owned()),
        primary_user_id: Some("42".to_owned()),
    }
}

#[derive(Default)]
struct FakeManagementTransport {
    running: Arc<AtomicBool>,
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    restarts: Arc<AtomicUsize>,
}

impl FakeManagementTransport {
    fn status(&self) -> TransportStatus {
        TransportStatus {
            name: "discord".to_owned(),
            enabled: self.running.load(Ordering::SeqCst),
            running: self.running.load(Ordering::SeqCst),
            error: None,
        }
    }
}

impl TransportController for FakeManagementTransport {
    fn autostart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.status()) }.boxed()
    }

    fn start(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.running.store(true, Ordering::SeqCst);
            Ok(self.status())
        }
        .boxed()
    }

    fn stop(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.stops.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(self.status())
        }
        .boxed()
    }

    fn restart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.restarts.fetch_add(1, Ordering::SeqCst);
            self.running.store(true, Ordering::SeqCst);
            Ok(self.status())
        }
        .boxed()
    }

    fn status(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(FakeManagementTransport::status(self)) }.boxed()
    }
}

struct BlockingRestartTransport {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    restarts: AtomicUsize,
}

impl BlockingRestartTransport {
    fn new() -> Self {
        Self {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            restarts: AtomicUsize::new(0),
        }
    }
}

impl TransportController for BlockingRestartTransport {
    fn autostart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn start(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn stop(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn restart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.restarts.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Ok(TransportStatus {
                name: "discord".to_owned(),
                enabled: false,
                running: false,
                error: None,
            })
        }
        .boxed()
    }

    fn status(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }
}

struct FailingRestartTransport {
    store: DiscordConfigStore,
    restarts: AtomicUsize,
}

impl TransportController for FailingRestartTransport {
    fn autostart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn start(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn stop(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }

    fn restart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move {
            self.restarts.fetch_add(1, Ordering::SeqCst);
            let config = self
                .store
                .load()
                .map_err(|error| format!("config was not durable before restart: {error}"))?;
            if !config.enabled {
                return Err("enabled state was not durable before restart".to_owned());
            }
            let token = self
                .store
                .read_token()
                .map_err(|error| format!("token was not durable before restart: {error}"))?;
            if token != "ordering-secret" {
                return Err("replacement token was not durable before restart".to_owned());
            }
            Err("raw gateway metadata and ordering-secret must never surface".to_owned())
        }
        .boxed()
    }

    fn status(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Err("unused".to_owned()) }.boxed()
    }
}

struct StatusFailureTransport {
    error: &'static str,
}

impl StatusFailureTransport {
    fn failed_status(&self) -> TransportStatus {
        TransportStatus {
            name: "discord".to_owned(),
            enabled: true,
            running: false,
            error: Some(self.error.to_owned()),
        }
    }
}

impl TransportController for StatusFailureTransport {
    fn autostart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.failed_status()) }.boxed()
    }

    fn start(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.failed_status()) }.boxed()
    }

    fn stop(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.failed_status()) }.boxed()
    }

    fn restart(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.failed_status()) }.boxed()
    }

    fn status(&self) -> futures_util::future::BoxFuture<'_, Result<TransportStatus, String>> {
        async move { Ok(self.failed_status()) }.boxed()
    }
}

fn management_input(token: Option<&str>) -> DiscordIntegrationInput {
    DiscordIntegrationInput {
        chat_channel_id: "43".to_owned(),
        agent_channel_id: "44".to_owned(),
        primary_user_id: "42".to_owned(),
        bot_token: token.map(|token| CredentialInput(token.to_owned())),
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
        last_projected: None,
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
        local_terminal: false,
        route_pending: false,
    }
}

struct FakeRouteAuthority {
    config: DiscordConfig,
    route: Mutex<Option<String>>,
    bridges: Mutex<HashMap<String, api::SessionBridge>>,
    fail_next_route: AtomicBool,
    route_calls: AtomicUsize,
}

impl FakeRouteAuthority {
    fn owner(bridge: api::SessionBridge) -> Self {
        Self {
            config: enabled_config(),
            route: Mutex::new(Some(bridge.session_id.clone())),
            bridges: Mutex::new(HashMap::from([(bridge.session_id.clone(), bridge)])),
            fail_next_route: AtomicBool::new(false),
            route_calls: AtomicUsize::new(0),
        }
    }
}

#[serenity::async_trait]
impl PendingRouteAuthority for FakeRouteAuthority {
    fn discord_config(&self) -> &DiscordConfig {
        &self.config
    }

    async fn resolve_authoritative_thread_route(
        &self,
        _thread_id: ChannelId,
    ) -> Result<Option<String>, DiscordError> {
        self.route_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_route.swap(false, Ordering::SeqCst) {
            return Err(DiscordError::InvalidConfig(
                "simulated authoritative route outage".to_owned(),
            ));
        }
        Ok(self.route.lock().expect("route").clone())
    }

    async fn authoritative_bridge(&self, session_id: &str) -> Option<api::SessionBridge> {
        self.bridges
            .lock()
            .expect("bridges")
            .get(session_id)
            .cloned()
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

#[test]
fn discord_gateway_identify_budget_honors_remaining_reset_and_concurrency() {
    assert_eq!(
        discord_session_start_wait(1, 60_000, 1_000, 2).expect("available budget"),
        None
    );
    let wait = discord_session_start_wait(0, 60_000, 1_000, 2)
        .expect("exhausted budget is valid")
        .expect("wait required");
    assert!(wait >= Duration::from_secs(60));
    assert!(wait < Duration::from_secs(61));
    assert!(discord_session_start_wait(1, 1, 1, 0).is_err());
}

#[test]
fn terminal_gateway_configuration_errors_fail_with_actionable_sanitized_status() {
    for (gateway_error, expected) in [
        (
            GatewayError::InvalidAuthentication,
            RUNTIME_ERROR_INVALID_TOKEN,
        ),
        (
            GatewayError::InvalidGatewayIntents,
            RUNTIME_ERROR_INVALID_INTENTS,
        ),
        (
            GatewayError::DisallowedGatewayIntents,
            RUNTIME_ERROR_MESSAGE_CONTENT_INTENT,
        ),
    ] {
        let error = serenity::Error::Gateway(gateway_error);
        assert!(is_terminal_gateway_error(&error));
        assert_eq!(
            sanitized_bridge_error(&DiscordError::Gateway(error)),
            expected
        );
    }

    assert_eq!(
        sanitized_bridge_error(&DiscordError::MissingToken),
        RUNTIME_ERROR_INVALID_TOKEN
    );
    assert_eq!(
        sanitized_bridge_error(&DiscordError::TokenTooLarge),
        RUNTIME_ERROR_INVALID_TOKEN
    );

    assert_eq!(
        terminal_http_gateway_error(Some(serenity::http::StatusCode::UNAUTHORIZED)),
        Some(RUNTIME_ERROR_INVALID_TOKEN)
    );
    assert_eq!(
        terminal_http_gateway_error(Some(serenity::http::StatusCode::FORBIDDEN)),
        None
    );
    assert_eq!(terminal_http_gateway_error(None), None);

    let retryable = serenity::Error::Gateway(GatewayError::HeartbeatFailed);
    assert!(!is_terminal_gateway_error(&retryable));
    assert_eq!(
        sanitized_bridge_error(&DiscordError::Gateway(retryable)),
        "Discord request failed"
    );
}

#[test]
fn ready_watch_debounce_drain_never_waits_for_a_future_snapshot() {
    let mut current = HydratedSession {
        state: api::SessionState {
            id: "initial".to_owned(),
            ..api::SessionState::default()
        },
        artifacts: HashMap::new(),
    };
    let mut idle: Watch<HydratedSession> = Box::pin(futures_util::stream::pending());
    assert!(matches!(
        drain_ready_hydrated_updates(&mut idle, &mut current),
        ReadyUpdateDrain::Open
    ));
    assert_eq!(current.state.id, "initial");

    let buffered = HydratedSession {
        state: api::SessionState {
            id: "latest".to_owned(),
            ..api::SessionState::default()
        },
        artifacts: HashMap::new(),
    };
    let mut ended: Watch<HydratedSession> =
        Box::pin(futures_util::stream::iter(vec![Ok(buffered)]));
    assert!(matches!(
        drain_ready_hydrated_updates(&mut ended, &mut current),
        ReadyUpdateDrain::Ended
    ));
    assert_eq!(current.state.id, "latest");
}

#[test]
fn cached_thread_routes_require_the_current_open_authoritative_bridge() {
    let config = enabled_config();
    let mut current = bridge();
    current.transport = Some("discord".to_owned());
    current.external_parent_id = Some("43".to_owned());
    current.external_thread_id = Some("92".to_owned());
    let mut bridges = HashMap::from([(current.session_id.clone(), current.clone())]);

    assert!(cached_thread_route_is_current(
        &config,
        &bridges,
        92,
        "session-1"
    ));
    bridges.remove("session-1");
    assert!(!cached_thread_route_is_current(
        &config,
        &bridges,
        92,
        "session-1"
    ));

    current.external_thread_id = Some("93".to_owned());
    bridges.insert(current.session_id.clone(), current.clone());
    assert!(!cached_thread_route_is_current(
        &config,
        &bridges,
        92,
        "session-1"
    ));
    current.lifecycle = api::BridgeLifecycle::Archived as i32;
    bridges.insert(current.session_id.clone(), current);
    assert!(!cached_thread_route_is_current(
        &config,
        &bridges,
        93,
        "session-1"
    ));
}

#[test]
fn successful_deleted_thread_clear_is_applied_optimistically_and_compare_guarded() {
    let mut current = bridge();
    current.revision = 7;
    current.transport = Some("discord".to_owned());
    current.external_parent_id = Some("43".to_owned());
    current.external_thread_id = Some("92".to_owned());
    current.live_turn_id = Some("turn-1".to_owned());
    current.live_external_message_id = Some("200".to_owned());
    current.active_source_message_id = Some("201".to_owned());

    assert!(!clear_local_thread_binding(
        &mut current,
        ChannelId::new(93),
        7
    ));
    assert_eq!(current.external_thread_id.as_deref(), Some("92"));

    current.revision = 8;
    assert!(!clear_local_thread_binding(
        &mut current,
        ChannelId::new(92),
        7
    ));
    assert_eq!(current.external_thread_id.as_deref(), Some("92"));
    assert_eq!(current.live_external_message_id.as_deref(), Some("200"));

    assert!(clear_local_thread_binding(
        &mut current,
        ChannelId::new(92),
        8
    ));
    assert!(current.transport.is_none());
    assert!(current.external_parent_id.is_none());
    assert!(current.external_thread_id.is_none());
    assert!(current.live_turn_id.is_none());
    assert!(current.live_external_message_id.is_none());
    assert!(current.active_source_message_id.is_none());
    assert_eq!(current.revision, 9);
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
fn durable_ingress_bounds_same_session_backpressure_and_preserves_tombstones() {
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
            later.forced_busy && later.local_terminal,
            "a later turn is durably rejected instead of queued behind unresolved work"
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
    assert!(
        restored
            .enqueue(&ingress_record("session-1", "101", None, false))
            .expect("replay locally rejected identity")
            .is_none(),
        "a capacity rejection is tombstoned without occupying the replay queue"
    );
    let (_, first) = restored
        .next_after(0)
        .expect("read ingress")
        .expect("first ingress");
    assert_eq!(first.message_id, "100");
    let (_, concurrent) = restored
        .next_after(1)
        .expect("read concurrent ingress")
        .expect("concurrent ingress");
    assert_eq!(concurrent.message_id, "102");
    assert!(!concurrent.forced_busy);

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
fn ordinary_ingress_queue_is_hard_bounded_with_durable_busy_tombstones() {
    let directory = tempfile::tempdir().expect("ingress root");
    let spool = IngressSpool::open(&directory.path().join("ingress.sqlite")).expect("open ingress");
    for index in 0..super::MAX_INBOUND_INFLIGHT {
        spool
            .enqueue(&ingress_record(
                &format!("session-{index}"),
                &format!("message-{index}"),
                None,
                false,
            ))
            .expect("bounded admission")
            .expect("pending event");
    }
    let overflow = ingress_record("session-overflow", "message-overflow", None, false);
    let rejected = spool
        .enqueue(&overflow)
        .expect("durable overload decision")
        .expect("local busy disposition");
    assert!(rejected.forced_busy);
    assert!(rejected.local_terminal);
    assert!(rejected.content.is_empty());

    let mut unresolved = ingress_record("", "message-unresolved-overflow", None, false);
    unresolved.route_pending = true;
    let unresolved = spool
        .enqueue(&unresolved)
        .expect("durable unresolved overload decision")
        .expect("bounded ownership-check record");
    assert!(unresolved.forced_busy && !unresolved.local_terminal);
    assert!(unresolved.route_pending);
    assert!(unresolved.content.is_empty());

    for index in 1..super::MAX_PENDING_ROUTE_REJECTIONS {
        let mut pending = ingress_record(
            "",
            &format!("message-unresolved-overflow-{index}"),
            None,
            false,
        );
        pending.route_pending = true;
        let rejection = spool
            .enqueue(&pending)
            .expect("bounded unresolved rejection")
            .expect("route metadata retained");
        assert!(rejection.forced_busy && !rejection.local_terminal);
        assert!(rejection.content.is_empty());
    }
    let mut hard_overflow = ingress_record("", "message-unresolved-hard-overflow", None, false);
    hard_overflow.route_pending = true;
    let hard_overflow = spool
        .enqueue(&hard_overflow)
        .expect("hard unresolved bound")
        .expect("silent terminal decision");
    assert!(hard_overflow.forced_busy && hard_overflow.local_terminal);
    assert!(hard_overflow.route_pending);

    let connection = spool
        .connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pending = connection
        .query_row("SELECT COUNT(*) FROM discord_ingress", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("pending count");
    assert_eq!(
        pending,
        i64::try_from(super::MAX_INBOUND_INFLIGHT + super::MAX_PENDING_ROUTE_REJECTIONS)
            .expect("bounded capacity")
    );
    drop(connection);
    assert!(
        spool
            .enqueue(&overflow)
            .expect("duplicate overload identity")
            .is_none(),
        "a reconnect cannot turn a local capacity rejection into a queued prompt"
    );
}

#[test]
fn ingress_tombstones_have_bounded_count_and_age_retention() {
    let directory = tempfile::tempdir().expect("ingress root");
    let spool = IngressSpool::open(&directory.path().join("ingress.sqlite")).expect("open ingress");
    let mut connection = spool
        .connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let transaction = connection.transaction().expect("tombstone transaction");
    let expired = super::unix_time_ms_i64().saturating_sub(
        i64::try_from(super::INGRESS_TOMBSTONE_RETENTION.as_millis())
            .unwrap_or(i64::MAX)
            .saturating_add(1),
    );
    transaction
        .execute(
            "INSERT INTO discord_ingress_tombstones (external_event_id, recorded_at_ms)
             VALUES ('expired', ?1)",
            [expired],
        )
        .expect("expired tombstone");
    for index in 0..super::MAX_INGRESS_TOMBSTONES + 3 {
        transaction
            .execute(
                "INSERT INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms) VALUES (?1, ?2)",
                rusqlite::params![format!("recent-{index}"), super::unix_time_ms_i64()],
            )
            .expect("recent tombstone");
    }
    super::prune_ingress_tombstones(&transaction).expect("bounded pruning");
    let remaining = transaction
        .query_row(
            "SELECT COUNT(*) FROM discord_ingress_tombstones",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("remaining count");
    let expired_remains = transaction
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM discord_ingress_tombstones WHERE external_event_id = 'expired'
             )",
            [],
            |row| row.get::<_, bool>(0),
        )
        .expect("expired lookup");
    assert_eq!(
        usize::try_from(remaining).expect("non-negative tombstone count"),
        super::MAX_INGRESS_TOMBSTONES
    );
    assert!(!expired_remains);
    transaction.commit().expect("commit tombstone pruning");
}

#[test]
fn unresolved_route_is_durable_then_atomically_bound_without_duplicate_admission() {
    let directory = tempfile::tempdir().expect("ingress root");
    let spool = IngressSpool::open(&directory.path().join("ingress.sqlite")).expect("open ingress");
    let mut pending = ingress_record("", "route-100", None, false);
    pending.route_pending = true;
    pending.thread_id = "9001".to_owned();
    pending.content = "retain me until authoritative routing recovers".to_owned();

    let admitted = spool
        .enqueue(&pending)
        .expect("durable unresolved admission")
        .expect("unresolved event pending");
    assert!(admitted.route_pending);
    assert!(admitted.session_id.is_empty());
    assert_eq!(admitted.content, pending.content);
    let duplicate = spool
        .enqueue(&pending)
        .expect("duplicate unresolved event")
        .expect("same durable event");
    assert_eq!(duplicate, admitted);

    let routed = spool
        .bind_route("route-100", "session-1", false)
        .expect("route checkpoint")
        .expect("routed event");
    assert!(!routed.route_pending);
    assert_eq!(routed.session_id, "session-1");
    assert_eq!(routed.content, pending.content);
    let duplicate_route = spool
        .bind_route("route-100", "session-1", false)
        .expect("idempotent route checkpoint")
        .expect("still pending");
    assert_eq!(duplicate_route, routed);
    assert!(
        spool.bind_route("route-100", "session-2", false).is_err(),
        "an admitted event cannot be cross-wired to another session"
    );

    spool
        .remove_event("route-100")
        .expect("terminal route event");
    assert!(
        spool
            .enqueue(&pending)
            .expect("duplicate after terminal handling")
            .is_none(),
        "gateway replay cannot resurrect a settled unresolved event"
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

#[tokio::test(flavor = "current_thread")]
async fn durable_ingress_lock_contention_does_not_block_the_async_runtime() {
    let directory = tempfile::tempdir().expect("ingress root");
    let path = directory.path().join("ingress.sqlite");
    let spool = Arc::new(IngressSpool::open(&path).expect("ingress connection"));
    let blocker = rusqlite::Connection::open(&path).expect("blocking connection");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold the ingress write lock");

    let pending = tokio::spawn(ingress_io(Arc::clone(&spool), move |ingress| {
        ingress.enqueue(&ingress_record(
            "session-runtime",
            "runtime-event",
            None,
            false,
        ))
    }));
    tokio::task::yield_now().await;
    let started = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "SQLite's busy timeout must run on the blocking pool, not a Tokio worker"
    );
    assert!(!pending.is_finished(), "the database write is still locked");

    blocker.execute_batch("COMMIT").expect("release write lock");
    let admitted = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .expect("blocking operation resumes")
        .expect("blocking worker joins")
        .expect("ingress write succeeds")
        .expect("event is admitted");
    assert_eq!(admitted.message_id, "runtime-event");
}

#[test]
fn corrupt_ingress_is_quarantined_without_blocking_later_sessions() {
    let directory = tempfile::tempdir().expect("ingress root");
    let spool = IngressSpool::open(&directory.path().join("ingress.sqlite")).expect("open ingress");
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
fn completed_projection_allowlist_orders_user_before_assistant_and_marks_echo_suppression() {
    let markdown = "Dashboard says 🦀\n```rust\nlet answer = \"✅\";\n```";
    let entries = vec![
        api::TranscriptEntry {
            id: "user".to_owned(),
            kind: api::TranscriptEntryKind::User as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("turn-1".to_owned()),
            body: markdown.to_owned(),
            ..api::TranscriptEntry::default()
        },
        api::TranscriptEntry {
            id: "reasoning".to_owned(),
            kind: api::TranscriptEntryKind::Reasoning as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("turn-1".to_owned()),
            body: "never project internal reasoning".to_owned(),
            ..api::TranscriptEntry::default()
        },
        api::TranscriptEntry {
            id: "assistant".to_owned(),
            kind: api::TranscriptEntryKind::Assistant as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("turn-1".to_owned()),
            body: "Final ✅".to_owned(),
            ..api::TranscriptEntry::default()
        },
    ];

    let projected = completed_projections(&entries, None);
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].kind, ProjectionKind::User);
    assert_eq!(projected[0].body, markdown);
    assert!(!projected[0].suppressed);
    assert_eq!(projected[1].kind, ProjectionKind::Assistant);
    assert_eq!(projected[1].body, "Final ✅");
    assert!(!projected[1].suppressed);
    assert_ne!(
        projection_nonce(
            ChannelId::new(92),
            "session-1",
            ProjectionKind::User,
            "turn-1",
            0,
        ),
        projection_nonce(
            ChannelId::new(92),
            "session-1",
            ProjectionKind::Assistant,
            "turn-1",
            0,
        ),
        "user and assistant parts for one provider turn cannot collide"
    );

    let mut discord_entries = entries;
    discord_entries[0].source_transport = Some("discord".to_owned());
    let projected = completed_projections(&discord_entries, None);
    assert_eq!(projected.len(), 2);
    assert!(projected[0].suppressed);
    assert_eq!(projected[0].kind, ProjectionKind::User);
    assert_eq!(
        projected[0].body, markdown,
        "source filtering must not mutate the user-visible transcript"
    );
    assert_eq!(projected[1].kind, ProjectionKind::Assistant);

    let while_active = completed_projections(&discord_entries, Some("turn-1"));
    assert_eq!(while_active.len(), 1);
    assert!(while_active[0].suppressed);
    assert_eq!(while_active[0].kind, ProjectionKind::User);
}

#[test]
fn historical_dashboard_recovery_cannot_clear_a_newer_discord_source_owner() {
    let dashboard = completed_projections(
        &[api::TranscriptEntry {
            id: "historical-user".to_owned(),
            kind: api::TranscriptEntryKind::User as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("historical-turn".to_owned()),
            body: "old dashboard prompt".to_owned(),
            ..api::TranscriptEntry::default()
        }],
        None,
    )
    .pop()
    .expect("dashboard projection");
    assert!(!projection_clears_stale_source(
        &dashboard,
        Some("newer-discord-turn"),
        Some("source-message")
    ));
    assert!(!projection_clears_stale_source(
        &dashboard,
        None,
        Some("newer-source-before-provider-start")
    ));
    assert!(
        projection_clears_stale_source(
            &dashboard,
            Some("historical-turn"),
            Some("stale-source-message")
        ),
        "cleanup is allowed only when this dashboard turn is actively running"
    );

    let discord = completed_projections(
        &[api::TranscriptEntry {
            id: "discord-user".to_owned(),
            kind: api::TranscriptEntryKind::User as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("newer-discord-turn".to_owned()),
            source_transport: Some("discord".to_owned()),
            body: "Discord prompt".to_owned(),
            ..api::TranscriptEntry::default()
        }],
        None,
    )
    .pop()
    .expect("discord projection");
    assert!(!projection_clears_stale_source(
        &discord,
        Some("newer-discord-turn"),
        Some("source-message")
    ));
}

#[test]
fn recovery_spool_preserves_trusted_suppression_across_duplicate_user_entries() {
    let directory = tempfile::tempdir().expect("recovery root");
    let mut spool = RecoverySpool::new(directory.path(), "session-1").expect("spool");
    let base = api::TranscriptEntry {
        id: "user-newest".to_owned(),
        kind: api::TranscriptEntryKind::User as i32,
        status: api::TranscriptEntryStatus::Complete as i32,
        owner_turn_id: Some("turn-discord".to_owned()),
        body: "continued from Discord".to_owned(),
        ..api::TranscriptEntry::default()
    };
    spool.push(&base).expect("newest duplicate");
    spool
        .push(&api::TranscriptEntry {
            id: "user-older".to_owned(),
            source_transport: Some("discord".to_owned()),
            ..base
        })
        .expect("trusted older duplicate");
    let stored = spool
        .oldest_first()
        .next()
        .expect("stored projection")
        .expect("valid projection");
    assert_eq!(stored.source_transport.as_deref(), Some("discord"));
    assert_eq!(spool.oldest_first().count(), 1);
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
            .push(&api::TranscriptEntry {
                id: id.to_owned(),
                kind: api::TranscriptEntryKind::Assistant as i32,
                status: api::TranscriptEntryStatus::Complete as i32,
                owner_turn_id: Some(turn.to_owned()),
                body: format!("body for {turn}"),
                body_total_bytes: u64::try_from(format!("body for {turn}").len())
                    .expect("body size"),
                ..api::TranscriptEntry::default()
            })
            .expect("spool entry");
    }
    // A later duplicate projection for the same turn does not create another delivery.
    spool
        .push(&api::TranscriptEntry {
            id: "entry-2-duplicate".to_owned(),
            kind: api::TranscriptEntryKind::Assistant as i32,
            status: api::TranscriptEntryStatus::Complete as i32,
            owner_turn_id: Some("turn-2".to_owned()),
            ..api::TranscriptEntry::default()
        })
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
fn prehydrated_routes_require_open_matching_snowflake_bindings() {
    let config = enabled_config();
    let mut chat = bridge();
    chat.transport = Some("discord".to_owned());
    chat.external_parent_id = Some("43".to_owned());
    chat.external_thread_id = Some("9001".to_owned());
    assert_eq!(
        valid_open_thread_route(&config, &chat),
        Some((9_001, "session-1".to_owned()))
    );

    let mut agent = chat.clone();
    agent.session_id = "session-2".to_owned();
    agent.kind = api::OrchestratorKind::Agent as i32;
    agent.external_parent_id = Some("44".to_owned());
    agent.external_thread_id = Some("9002".to_owned());
    assert_eq!(
        valid_open_thread_route(&config, &agent),
        Some((9_002, "session-2".to_owned()))
    );

    agent.external_parent_id = Some("43".to_owned());
    assert!(valid_open_thread_route(&config, &agent).is_none());
    chat.lifecycle = api::BridgeLifecycle::Archived as i32;
    assert!(valid_open_thread_route(&config, &chat).is_none());
}

#[test]
fn sibling_workspaces_keep_independent_gateway_ingress_authorities() {
    let directory = tempfile::tempdir().expect("installation");
    let workspace_a = directory.path().join("workspace-a");
    let workspace_b = directory.path().join("workspace-b");
    std::fs::create_dir_all(&workspace_a).expect("workspace a");
    std::fs::create_dir_all(&workspace_b).expect("workspace b");
    let config_root = directory.path().join("shared-discord");
    let store_a = DiscordConfigStore::from_root(&workspace_a, &config_root).expect("store a");
    let store_b = DiscordConfigStore::from_root(&workspace_b, &config_root).expect("store b");
    let ingress_a = IngressSpool::open(&store_a.directory.join("discord-ingress.sqlite"))
        .expect("workspace a ingress");
    let ingress_b = IngressSpool::open(&store_b.directory.join("discord-ingress.sqlite"))
        .expect("workspace b ingress");

    let first = ingress_a
        .enqueue(&ingress_record(
            "session-a",
            "same-gateway-event",
            None,
            false,
        ))
        .expect("workspace a event")
        .expect("workspace a pending");
    let second = ingress_b
        .enqueue(&ingress_record(
            "session-b",
            "same-gateway-event",
            None,
            false,
        ))
        .expect("workspace b event")
        .expect("workspace b pending");
    assert_eq!(first.session_id, "session-a");
    assert_eq!(second.session_id, "session-b");
    assert_ne!(store_a.directory, store_b.directory);
}

#[tokio::test]
async fn gateway_configuration_watcher_observes_shared_generation_changes() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let initial = enabled_config();
    let next_generation = initial.runtime_generation.saturating_add(1);
    store.save(&initial).expect("initial config");
    store
        .save_token("fixture-token-not-a-live-credential")
        .expect("initial token");
    let token = store.read_token().expect("load token");
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let watched_store = store.clone();
    let watcher = tokio::spawn(async move {
        wait_for_runtime_configuration_change(&watched_store, &initial, Some(&token), shutdown_rx)
            .await
    });

    let mut changed = enabled_config();
    changed.runtime_generation = next_generation;
    store.save(&changed).expect("shared restart generation");
    let observed = tokio::time::timeout(Duration::from_secs(2), watcher)
        .await
        .expect("watcher deadline")
        .expect("watcher task")
        .expect("watcher read");
    assert!(observed);
    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn disabled_autostart_retains_a_config_watcher_until_service_stop() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    store
        .save(&DiscordConfig::default())
        .expect("disabled config");
    let transport = DiscordTransport {
        workspace: directory.path().to_owned(),
        endpoint: directory.path().join("missing.sock"),
        store,
        runtime: Arc::new(tokio::sync::Mutex::new(DiscordRuntime::default())),
        operation: tokio::sync::Mutex::new(()),
    };

    let status = transport.autostart().await.expect("disabled autostart");
    assert!(!status.enabled);
    assert!(!status.running);
    assert!(
        transport
            .runtime
            .lock()
            .await
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished()),
        "the hidden manager keeps polling shared configuration while disabled"
    );

    transport.stop().await.expect("service stop");
    assert!(transport.runtime.lock().await.task.is_none());
}

#[tokio::test]
async fn disabled_gateway_watcher_observes_shared_enable_without_reading_a_token() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let initial = DiscordConfig::default();
    store.save(&initial).expect("disabled config");
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let watched_store = store.clone();
    let watcher = tokio::spawn(async move {
        wait_for_runtime_configuration_change(&watched_store, &initial, None, shutdown_rx).await
    });

    let changed = enabled_config();
    store.save(&changed).expect("shared enable");
    let observed = tokio::time::timeout(Duration::from_secs(2), watcher)
        .await
        .expect("watcher deadline")
        .expect("watcher task")
        .expect("watcher read");
    assert!(observed);
}

#[tokio::test]
async fn managed_gateway_tie_prioritizes_authoritative_configuration_change() {
    let gateway = std::future::ready(Ok(()));
    let configuration = std::future::ready(Ok(true));
    tokio::pin!(gateway);
    tokio::pin!(configuration);
    let event = next_managed_gateway_event(gateway.as_mut(), configuration.as_mut()).await;
    assert!(matches!(
        event,
        ManagedGatewayEvent::Configuration(Ok(true))
    ));
}

#[tokio::test]
async fn managed_gateway_shutdown_timeout_is_bounded_and_explicit() {
    let gateway = std::future::pending::<Result<(), DiscordError>>();
    tokio::pin!(gateway);
    let error = await_managed_gateway_shutdown(gateway.as_mut(), Duration::from_millis(10))
        .await
        .expect_err("stalled gateway exceeds bounded shutdown");
    assert!(matches!(error, DiscordError::GatewayShutdownTimeout));
}

#[tokio::test]
async fn managed_gateway_shutdown_propagates_gateway_completion() {
    let gateway = std::future::ready(Ok(()));
    tokio::pin!(gateway);
    await_managed_gateway_shutdown(gateway.as_mut(), Duration::from_secs(1))
        .await
        .expect("completed gateway joins normally");
}

#[tokio::test]
async fn installation_wide_gateway_identify_throttle_is_shutdown_cancellable_while_contended() {
    let directory = tempfile::tempdir().expect("installation root");
    let workspace_a = directory.path().join("workspace-a");
    let workspace_b = directory.path().join("workspace-b");
    std::fs::create_dir_all(&workspace_a).expect("workspace a");
    std::fs::create_dir_all(&workspace_b).expect("workspace b");
    let configuration_root = directory.path().join("discord-data");
    let owner_store =
        DiscordConfigStore::from_root(&workspace_a, &configuration_root).expect("owner store");
    let waited_store =
        DiscordConfigStore::from_root(&workspace_b, &configuration_root).expect("waiter store");
    assert_ne!(owner_store.directory, waited_store.directory);
    assert_eq!(
        owner_store.configuration_directory,
        waited_store.configuration_directory
    );
    let lock_path = owner_store
        .configuration_directory
        .join("gateway-identify.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("identify lock");
    fs2::FileExt::lock_exclusive(&lock).expect("hold identify slot");
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let waiter = tokio::spawn(async move {
        acquire_gateway_identify_lease(&waited_store, &mut shutdown_rx).await
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    shutdown_tx.send(true).expect("shutdown signal");
    let admitted = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("bounded identify cancellation")
        .expect("waiter task")
        .expect("identify result");
    assert!(admitted.is_none());
    fs2::FileExt::unlock(&lock).expect("release identify slot");
}

#[tokio::test]
async fn gateway_identify_lease_remains_exclusive_until_ready_owner_releases_it() {
    let directory = tempfile::tempdir().expect("installation root");
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let store = DiscordConfigStore::from_root(&workspace, &directory.path().join("discord-data"))
        .expect("store");
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let lease = acquire_gateway_identify_lease(&store, &mut shutdown_rx)
        .await
        .expect("identify admission")
        .expect("active lease");
    let lock_path = store.configuration_directory.join("gateway-identify.lock");
    let contender = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("contender lock file");
    let error = fs2::FileExt::try_lock_exclusive(&contender)
        .expect_err("lease remains held across admission return");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    drop(lease);
    fs2::FileExt::try_lock_exclusive(&contender).expect("lease released on drop");
    fs2::FileExt::unlock(&contender).expect("release contender");
}

#[tokio::test]
async fn tracked_children_join_cooperatively_after_shutdown_signal() {
    let observed = Arc::new(AtomicBool::new(false));
    let child_observed = Arc::clone(&observed);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let child = tokio::spawn(async move {
        let _ = shutdown_rx.changed().await;
        child_observed.store(true, Ordering::SeqCst);
    });
    shutdown_tx.send(true).expect("shutdown signal");
    finish_child_tasks(vec![child], Some(Duration::from_secs(1))).await;
    assert!(observed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stalled_tracked_children_are_aborted_after_one_shared_deadline() {
    let retained = Arc::new(());
    let child_retained = Arc::clone(&retained);
    let child = tokio::spawn(async move {
        std::future::pending::<()>().await;
        drop(child_retained);
    });
    tokio::task::yield_now().await;
    finish_child_tasks(vec![child], Some(Duration::from_millis(10))).await;
    assert_eq!(Arc::strong_count(&retained), 1, "stalled child was dropped");
}

#[tokio::test]
async fn forced_child_guard_aborts_and_joins_a_stalled_child() {
    let tasks = Arc::new(TrackedChildTasks::default());
    let child = tokio::spawn(std::future::pending::<()>());
    tasks.track(&child);
    let guard = ChildAbortGuard {
        tasks: Arc::clone(&tasks),
    };
    drop(guard);
    let result = tokio::time::timeout(Duration::from_secs(1), child)
        .await
        .expect("aborted child joins within the bounded shutdown window")
        .expect_err("child was aborted");
    assert!(result.is_cancelled());
}

#[test]
fn token_configured_requires_a_nonblank_bounded_credential() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    assert!(!store.token_configured());
    std::fs::write(store.token_path(), "  \n").expect("blank token fixture");
    assert!(!store.token_configured());
    std::fs::write(store.token_path(), "x".repeat(super::MAX_TOKEN_BYTES + 1))
        .expect("oversized token fixture");
    assert!(!store.token_configured());
    store
        .save_token("  valid-fixture-token  ")
        .expect("valid token");
    assert!(store.token_configured());
    assert_eq!(
        store.read_token().expect("trimmed token"),
        "valid-fixture-token"
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

#[tokio::test]
async fn management_mutation_survives_request_cancellation_and_replays_once() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let transport = Arc::new(BlockingRestartTransport::new());
    let manager = DiscordManagementService {
        store: Ok(store),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };
    let key = IdempotencyKey::new("cancelled-save");
    let cancelled_manager = manager.clone();
    let cancelled_key = key.clone();
    let caller = tokio::spawn(async move {
        cancelled_manager
            .mutate(
                cancelled_key,
                DiscordManagementMutation::Save(management_input(None)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), transport.started.notified())
        .await
        .expect("transport mutation started");
    caller.abort();
    let cancelled = caller.await.expect_err("request task cancelled");
    assert!(cancelled.is_cancelled());

    transport.release.notify_one();
    let replay = tokio::time::timeout(
        Duration::from_secs(1),
        manager.mutate(key, DiscordManagementMutation::Save(management_input(None))),
    )
    .await
    .expect("request-independent mutation finishes")
    .expect("same-key retry replays the completed mutation");
    assert!(!replay.enabled);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn management_save_is_redacted_preserves_blank_token_and_replays_once() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let transport = Arc::new(FakeManagementTransport::default());
    let manager = DiscordManagementService {
        store: Ok(store.clone()),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };
    let input = management_input(Some("write-only-secret"));

    let first = manager
        .mutate(
            IdempotencyKey::new("save-once"),
            DiscordManagementMutation::Save(input.clone()),
        )
        .await
        .expect("save configuration");
    assert!(first.token_configured);
    assert!(!first.enabled);
    assert_eq!(first.chat_channel_id.as_deref(), Some("43"));
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.read_token().expect("stored token"),
        "write-only-secret"
    );
    let persisted = std::fs::read_to_string(store.config_path()).expect("public config");
    assert!(!persisted.contains("write-only-secret"));
    assert!(!format!("{first:?}").contains("write-only-secret"));

    let replay = manager
        .mutate(
            IdempotencyKey::new("save-once"),
            DiscordManagementMutation::Save(input),
        )
        .await
        .expect("idempotent replay");
    assert_eq!(replay, first);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);

    let mut conflicting = management_input(None);
    conflicting.primary_user_id = "45".to_owned();
    let conflict = manager
        .mutate(
            IdempotencyKey::new("save-once"),
            DiscordManagementMutation::Save(conflicting),
        )
        .await
        .expect_err("different mutation must conflict");
    assert_eq!(conflict.code, ErrorCode::Conflict);

    let enabled = manager
        .mutate(
            IdempotencyKey::new("enable-with-preserved-token"),
            DiscordManagementMutation::SetEnabled(true),
        )
        .await
        .expect("enable with stored credential");
    assert!(enabled.enabled);
    assert!(enabled.token_configured);
    assert_eq!(transport.starts.load(Ordering::SeqCst), 1);

    let preserved = manager
        .mutate(
            IdempotencyKey::new("save-with-preserved-token"),
            DiscordManagementMutation::Save(management_input(None)),
        )
        .await
        .expect("blank token preserves stored credential");
    assert!(preserved.enabled);
    assert!(preserved.token_configured);
    assert_eq!(
        store.read_token().expect("preserved token"),
        "write-only-secret"
    );
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 2);

    let replaced = manager
        .mutate(
            IdempotencyKey::new("replace-token"),
            DiscordManagementMutation::Save(management_input(Some("replacement-secret"))),
        )
        .await
        .expect("replace token");
    assert!(replaced.token_configured);
    assert_eq!(
        store.read_token().expect("replacement token"),
        "replacement-secret"
    );
    assert!(!format!("{replaced:?}").contains("replacement-secret"));
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 3);

    let disabled = manager
        .mutate(
            IdempotencyKey::new("disable"),
            DiscordManagementMutation::SetEnabled(false),
        )
        .await
        .expect("disable transport");
    assert!(!disabled.enabled);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 4);
    let reenabled = manager
        .mutate(
            IdempotencyKey::new("enable"),
            DiscordManagementMutation::SetEnabled(true),
        )
        .await
        .expect("enable transport");
    assert!(reenabled.enabled);
    assert_eq!(transport.starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn management_rejects_invalid_ids_and_enable_without_a_token_before_side_effects() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let transport = Arc::new(FakeManagementTransport::default());
    let manager = DiscordManagementService {
        store: Ok(store.clone()),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };
    let mut invalid = management_input(Some("must-not-land"));
    invalid.agent_channel_id = invalid.chat_channel_id.clone();

    let error = manager
        .mutate(
            IdempotencyKey::new("invalid-parents"),
            DiscordManagementMutation::Save(invalid),
        )
        .await
        .expect_err("duplicate parent ids");
    assert_eq!(error.code, ErrorCode::InvalidRequest);
    assert!(!store.token_configured());
    assert!(!store.config_path().exists());

    let configured = manager
        .mutate(
            IdempotencyKey::new("configure-without-token"),
            DiscordManagementMutation::Save(management_input(None)),
        )
        .await
        .expect("disabled configuration may be saved without a token");
    assert!(!configured.enabled);
    assert!(!configured.token_configured);

    let missing = manager
        .mutate(
            IdempotencyKey::new("missing-token"),
            DiscordManagementMutation::SetEnabled(true),
        )
        .await
        .expect_err("enable requires token");
    assert_eq!(missing.code, ErrorCode::InvalidRequest);
    assert!(!store.load().expect("public config").enabled);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn management_storage_failures_are_sanitized_retryable_and_not_replayed() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    std::fs::create_dir_all(store.config_path()).expect("blocking config destination");
    let transport = Arc::new(FakeManagementTransport::default());
    let manager = DiscordManagementService {
        store: Ok(store.clone()),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };

    let failure = manager
        .mutate(
            IdempotencyKey::new("retry-after-storage-repair"),
            DiscordManagementMutation::Save(management_input(None)),
        )
        .await
        .expect_err("directory cannot be replaced by the public config file");
    assert_eq!(failure.code, ErrorCode::Internal);
    assert!(failure.retryable);
    assert_eq!(failure.message, "Could not save Discord configuration");
    assert!(
        !failure
            .message
            .contains(&directory.path().display().to_string())
    );
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 0);

    std::fs::remove_dir(store.config_path()).expect("repair config destination");
    let recovered = manager
        .mutate(
            IdempotencyKey::new("retry-after-storage-repair"),
            DiscordManagementMutation::Save(management_input(None)),
        )
        .await
        .expect("failed mutations do not poison idempotent retry");
    assert!(!recovered.enabled);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);

    let unavailable = DiscordManagementService {
        store: Err(()),
        transports: TransportSupervisor::default(),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    }
    .get()
    .await
    .expect_err("unavailable root store");
    assert_eq!(unavailable.code, ErrorCode::Internal);
    assert!(unavailable.retryable);
    assert_eq!(
        unavailable.message,
        "Discord configuration storage is unavailable"
    );
}

#[tokio::test]
async fn management_exposes_only_allowlisted_actionable_gateway_failures() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    store
        .save(&enabled_config())
        .expect("enabled configuration");
    store
        .save_token("must-remain-write-only")
        .expect("private token");

    for (transport_error, expected) in [
        (
            RUNTIME_ERROR_MESSAGE_CONTENT_INTENT,
            RUNTIME_ERROR_MESSAGE_CONTENT_INTENT,
        ),
        (RUNTIME_ERROR_INVALID_TOKEN, RUNTIME_ERROR_INVALID_TOKEN),
        (RUNTIME_ERROR_INVALID_INTENTS, RUNTIME_ERROR_INVALID_INTENTS),
        (
            "raw Discord gateway metadata must not escape",
            "Discord transport is unavailable; check the sanitized Nakode service logs",
        ),
    ] {
        let transport = Arc::new(StatusFailureTransport {
            error: transport_error,
        });
        let manager = DiscordManagementService {
            store: Ok(store.clone()),
            transports: TransportSupervisor::new([(
                "discord".to_owned(),
                transport as Arc<dyn TransportController>,
            )]),
            operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
        };
        let view = manager.get().await.expect("redacted failed status");
        assert_eq!(view.runtime_state, DiscordRuntimeState::Failed);
        assert_eq!(view.runtime_error.as_deref(), Some(expected));
        let debug = format!("{view:?}");
        assert!(!debug.contains("must-remain-write-only"));
        assert!(!debug.contains("raw Discord gateway metadata"));
    }
}

#[tokio::test]
async fn management_persists_before_restart_and_sanitizes_transport_failure() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    store
        .save(&enabled_config())
        .expect("enable public configuration before replacement");
    let transport = Arc::new(FailingRestartTransport {
        store: store.clone(),
        restarts: AtomicUsize::new(0),
    });
    let manager = DiscordManagementService {
        store: Ok(store),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };

    let failed = manager
        .mutate(
            IdempotencyKey::new("durable-before-restart"),
            DiscordManagementMutation::Save(management_input(Some("ordering-secret"))),
        )
        .await
        .expect("transport failures are projected as redacted runtime state");
    assert_eq!(failed.runtime_state, DiscordRuntimeState::Failed);
    assert_eq!(
        failed.runtime_error.as_deref(),
        Some("Discord transport is unavailable; check the sanitized Nakode service logs")
    );
    let debug = format!("{failed:?}");
    assert!(!debug.contains("ordering-secret"));
    assert!(!debug.contains("raw gateway metadata"));
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);

    let replay = manager
        .mutate(
            IdempotencyKey::new("durable-before-restart"),
            DiscordManagementMutation::Save(management_input(Some("ordering-secret"))),
        )
        .await
        .expect("failed runtime projection replays without another restart");
    assert_eq!(replay, failed);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn management_restart_rejects_disabled_or_tokenless_config_without_transport_calls() {
    let directory = tempfile::tempdir().expect("workspace");
    let store =
        DiscordConfigStore::from_root(directory.path(), &directory.path().join("discord-data"))
            .expect("store");
    let transport = Arc::new(FakeManagementTransport::default());
    let manager = DiscordManagementService {
        store: Ok(store.clone()),
        transports: TransportSupervisor::new([(
            "discord".to_owned(),
            Arc::clone(&transport) as Arc<dyn TransportController>,
        )]),
        operation: Arc::new(tokio::sync::Mutex::new(DiscordManagementState::default())),
    };

    let disabled = manager
        .mutate(
            IdempotencyKey::new("restart-disabled"),
            DiscordManagementMutation::Restart,
        )
        .await
        .expect_err("disabled restart");
    assert_eq!(disabled.code, ErrorCode::InvalidRequest);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 0);

    store
        .save(&DiscordConfig {
            version: CONFIG_VERSION,
            runtime_generation: 0,
            enabled: true,
            chat_channel_id: Some("43".to_owned()),
            agent_channel_id: Some("44".to_owned()),
            primary_user_id: Some("42".to_owned()),
        })
        .expect("enabled public config");
    let tokenless = manager
        .mutate(
            IdempotencyKey::new("restart-tokenless"),
            DiscordManagementMutation::Restart,
        )
        .await
        .expect_err("tokenless restart");
    assert_eq!(tokenless.code, ErrorCode::InvalidRequest);
    assert_eq!(transport.restarts.load(Ordering::SeqCst), 0);
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
fn installation_configuration_lock_serializes_different_workspace_stores() {
    let directory = tempfile::tempdir().expect("root");
    let first_workspace = directory.path().join("workspace-a");
    let second_workspace = directory.path().join("workspace-b");
    std::fs::create_dir_all(&first_workspace).expect("first workspace");
    std::fs::create_dir_all(&second_workspace).expect("second workspace");
    let data = directory.path().join("discord-data");
    let first = DiscordConfigStore::from_root(&first_workspace, &data).expect("first store");
    let second = DiscordConfigStore::from_root(&second_workspace, &data).expect("second store");
    let (locked_sender, locked_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let holder = std::thread::spawn(move || {
        first
            .with_configuration_lock(|| {
                locked_sender.send(()).expect("announce held lock");
                release_receiver.recv().expect("release held lock");
                Ok(())
            })
            .expect("hold installation lock");
    });
    locked_receiver.recv().expect("lock is held");

    let (writer_sender, writer_receiver) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        second
            .with_configuration_lock(|| {
                writer_sender.send(()).expect("announce writer lock");
                Ok(())
            })
            .expect("second writer lock");
    });
    assert!(
        writer_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "a writer from another workspace service must wait for the shared lock"
    );
    release_sender.send(()).expect("release first writer");
    holder.join().expect("lock holder");
    writer_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("second writer acquires after release");
    writer.join().expect("second writer");
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

#[tokio::test]
async fn multipart_limits_count_unique_parts_and_allow_cleanup_after_oversize() {
    let directory = tempfile::tempdir().expect("tempdir");
    let assembler =
        MultipartAssembler::new(directory.path().join("assemblies")).expect("assembler");

    let too_many = format!(
        "!nakode multipart excessive-parts 1/{}\npart",
        super::MAX_MULTIPART_PARTS + 1
    );
    let Err(error) = parse_multipart(&too_many).expect("multipart") else {
        panic!("part count is bounded");
    };
    assert!(matches!(error, DiscordError::MultipartTooManyParts));

    let first_body = "🦀".repeat(super::MAX_MULTIPART_BYTES / 8);
    let first_wire = format!("!nakode multipart excessive-bytes 1/2\n{first_body}");
    let first = parse_multipart(&first_wire)
        .expect("multipart")
        .expect("valid first part");
    assert!(matches!(
        assembler
            .accept("session-bytes", MessageId::new(2), first)
            .await
            .expect("first part"),
        MultipartOutcome::Waiting
    ));
    let duplicate = parse_multipart(&first_wire)
        .expect("multipart")
        .expect("valid duplicate");
    assert!(matches!(
        assembler
            .accept("session-bytes", MessageId::new(2), duplicate)
            .await
            .expect("duplicate does not add bytes"),
        MultipartOutcome::Waiting
    ));

    let second_body = format!("{}x", "終".repeat(super::MAX_MULTIPART_BYTES / 6));
    let second_wire = format!("!nakode multipart excessive-bytes 2/2\n{second_body}");
    assert!(matches!(
        assembler
            .accept(
                "session-bytes",
                MessageId::new(3),
                parse_multipart(&second_wire)
                    .expect("multipart")
                    .expect("valid second part"),
            )
            .await
            .expect("a duplicate part was not counted twice"),
        MultipartOutcome::Complete { text, .. }
            if text.len() == super::MAX_MULTIPART_BYTES
    ));
    assembler.finish("session-bytes", "excessive-bytes").await;

    let full_body = "x".repeat(super::MAX_MULTIPART_BYTES);
    let full_wire = format!("!nakode multipart actually-oversize 1/2\n{full_body}");
    assert!(matches!(
        assembler
            .accept(
                "session-bytes",
                MessageId::new(4),
                parse_multipart(&full_wire)
                    .expect("multipart")
                    .expect("valid full part"),
            )
            .await
            .expect("the exact byte cap is accepted"),
        MultipartOutcome::Waiting
    ));
    let Err(error) = assembler
        .accept(
            "session-bytes",
            MessageId::new(5),
            parse_multipart("!nakode multipart actually-oversize 2/2\ny")
                .expect("multipart")
                .expect("valid overflow part"),
        )
        .await
    else {
        panic!("aggregate body bytes are bounded");
    };
    assert!(matches!(error, DiscordError::MultipartTooLarge));

    assembler.finish("session-bytes", "actually-oversize").await;
    let replacement = parse_multipart("!nakode multipart replacement 1/2\nok")
        .expect("multipart")
        .expect("valid replacement");
    assert!(matches!(
        assembler
            .accept("session-bytes", MessageId::new(6), replacement)
            .await
            .expect("cleanup releases per-session admission"),
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
    let starter = starter_nonce(ChannelId::new(43), "session-1");
    let first = final_nonce(ChannelId::new(92), "session-1", "turn-1", 0);
    let retry = final_nonce(ChannelId::new(92), "session-1", "turn-1", 0);
    let second = final_nonce(ChannelId::new(92), "session-1", "turn-1", 1);
    let busy = busy_nonce(MessageId::new(42));
    let failed = failed_nonce(MessageId::new(42));
    assert!(starter.len() <= 25);
    assert!(first.len() <= 25);
    assert!(busy.len() <= 25);
    assert!(failed.len() <= 25);
    assert_ne!(busy, failed);
    assert_eq!(first, retry);
    assert_ne!(first, second);
    assert_ne!(
        starter,
        starter_nonce(ChannelId::new(44), "session-1"),
        "a replacement parent must not reuse Discord's author-wide nonce"
    );
    assert_ne!(
        first,
        final_nonce(ChannelId::new(93), "session-1", "turn-1", 0),
        "a replacement thread must not recover a message from the deleted thread"
    );
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
    parents: Mutex<HashMap<u64, u64>>,
    fail_next_parent_lookup: Mutex<bool>,
    edits: Mutex<Vec<(u64, u64, String)>>,
    reactions: Mutex<Vec<(u64, u64, String)>>,
    removals: Mutex<Vec<(u64, u64, String)>>,
    fail_next_reaction: Mutex<bool>,
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

    async fn parent_channel_id(
        &self,
        thread_id: ChannelId,
    ) -> Result<Option<ChannelId>, serenity::Error> {
        if std::mem::take(&mut *self.fail_next_parent_lookup.lock().expect("parent failure")) {
            return Err(serenity::Error::Other("simulated parent lookup outage"));
        }
        Ok(self
            .parents
            .lock()
            .expect("parents")
            .get(&thread_id.get())
            .copied()
            .map(ChannelId::new))
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
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error> {
        if std::mem::take(&mut *self.fail_next_reaction.lock().expect("reaction failure")) {
            return Err(serenity::Error::Other("simulated reaction failure"));
        }
        self.reactions.lock().expect("reactions").push((
            channel_id.get(),
            message_id.get(),
            emoji.to_owned(),
        ));
        Ok(())
    }

    async fn remove_own_reaction(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &str,
    ) -> Result<(), serenity::Error> {
        self.removals.lock().expect("removals").push((
            channel_id.get(),
            message_id.get(),
            emoji.to_owned(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn pending_route_retries_parent_and_authority_outages_then_routes_only_the_owner() {
    let mut owned_bridge = bridge();
    owned_bridge.transport = Some("discord".to_owned());
    owned_bridge.external_parent_id = Some("43".to_owned());
    owned_bridge.external_thread_id = Some("92".to_owned());
    let authority = FakeRouteAuthority::owner(owned_bridge);
    authority.fail_next_route.store(true, Ordering::SeqCst);
    let fake = FakeDiscord {
        parents: Mutex::new(HashMap::from([(92, 43)])),
        fail_next_parent_lookup: Mutex::new(true),
        ..FakeDiscord::default()
    };
    let mut record = ingress_record("", "500", None, false);
    record.route_pending = true;

    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Deferred,
        "a transient Discord parent lookup outage must retain the unresolved ingress row"
    );
    assert_eq!(authority.route_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Deferred,
        "a transient authoritative route outage must also retain the row"
    );
    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Routed("session-1".to_owned())
    );
    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Routed("session-1".to_owned()),
        "duplicate gateway delivery resolves to the same stable owner"
    );
}

#[tokio::test]
async fn pending_route_rejects_wrong_parent_and_non_owner_without_mutating_authority() {
    let mut owned_bridge = bridge();
    owned_bridge.transport = Some("discord".to_owned());
    owned_bridge.external_parent_id = Some("43".to_owned());
    owned_bridge.external_thread_id = Some("92".to_owned());
    let authority = FakeRouteAuthority::owner(owned_bridge);
    let mut record = ingress_record("", "501", None, false);
    record.route_pending = true;

    let wrong_parent = FakeDiscord {
        parents: Mutex::new(HashMap::from([(92, 999)])),
        ..FakeDiscord::default()
    };
    assert_eq!(
        resolve_pending_route(&wrong_parent, &authority, &record).await,
        PendingRouteResolution::Terminal
    );
    assert_eq!(authority.route_calls.load(Ordering::SeqCst), 0);

    *authority.route.lock().expect("route") = None;
    let configured_parent = FakeDiscord {
        parents: Mutex::new(HashMap::from([(92, 43)])),
        ..FakeDiscord::default()
    };
    assert_eq!(
        resolve_pending_route(&configured_parent, &authority, &record).await,
        PendingRouteResolution::Terminal,
        "a sibling workspace receiving the same gateway event is not an owner"
    );
    assert_eq!(authority.route_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_route_defers_warmup_and_follows_authoritative_rebinding() {
    let mut stale_bridge = bridge();
    stale_bridge.transport = Some("discord".to_owned());
    stale_bridge.external_parent_id = Some("43".to_owned());
    stale_bridge.external_thread_id = Some("91".to_owned());
    let authority = FakeRouteAuthority::owner(stale_bridge);
    let fake = FakeDiscord {
        parents: Mutex::new(HashMap::from([(92, 43)])),
        ..FakeDiscord::default()
    };
    let mut record = ingress_record("", "502", None, false);
    record.route_pending = true;

    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Terminal,
        "a stale or deleted-thread binding cannot claim a different thread"
    );

    authority.bridges.lock().expect("bridges").clear();
    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Deferred,
        "route lookup may lead its replacement bridge snapshot during warm-up"
    );

    let mut rebound = bridge();
    rebound.session_id = "session-2".to_owned();
    rebound.transport = Some("discord".to_owned());
    rebound.external_parent_id = Some("43".to_owned());
    rebound.external_thread_id = Some("92".to_owned());
    *authority.route.lock().expect("route") = Some("session-2".to_owned());
    authority
        .bridges
        .lock()
        .expect("bridges")
        .insert("session-2".to_owned(), rebound);
    assert_eq!(
        resolve_pending_route(&fake, &authority, &record).await,
        PendingRouteResolution::Routed("session-2".to_owned()),
        "the authoritative cross-session rebind wins over stale local identity"
    );
}

#[tokio::test]
async fn lazy_thread_creation_recovers_existing_starter_mapping() {
    let bridge = bridge();
    let nonce = starter_nonce(ChannelId::new(43), &bridge.session_id);
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
        Some(starter_nonce(ChannelId::new(43), &bridge.session_id).as_str())
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
async fn terminal_busy_feedback_retries_transient_reaction_loss_without_duplicate_messages() {
    let fake = FakeDiscord {
        fail_next_reaction: Mutex::new(true),
        next_message: Mutex::new(100),
        ..FakeDiscord::default()
    };
    let channel_id = ChannelId::new(92);
    let message_id = MessageId::new(93);

    let first = mark_message_busy(&fake, channel_id, message_id).await;
    assert!(
        first.is_err(),
        "transient reaction loss must defer settlement"
    );
    assert!(fake.sends.lock().expect("sends").is_empty());
    assert!(matches!(
        terminal_feedback_outcome(first, "session-1"),
        IngressProcessOutcome::Deferred
    ));

    mark_message_busy(&fake, channel_id, message_id)
        .await
        .expect("retry terminal feedback");
    mark_message_busy(&fake, channel_id, message_id)
        .await
        .expect("idempotent duplicate terminal feedback");

    assert_eq!(fake.sends.lock().expect("sends").len(), 1);
    assert_eq!(
        fake.reactions.lock().expect("reactions").as_slice(),
        &[
            (92, 93, REACTION_BUSY.to_owned()),
            (92, 93, REACTION_BUSY.to_owned())
        ]
    );
    assert_eq!(fake.edits.lock().expect("edits").len(), 1);
}

#[tokio::test]
async fn final_part_recovers_a_send_that_succeeded_before_its_response_was_lost() {
    let fake = FakeDiscord {
        fail_next_send_after_record: Mutex::new(true),
        next_message: Mutex::new(100),
        ..FakeDiscord::default()
    };
    let nonce = final_nonce(ChannelId::new(92), "session-1", "turn-1", 0);
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
    let nonce = final_nonce(ChannelId::new(92), "session-1", "outside-window", 0);
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
    let nonce = final_nonce(ChannelId::new(92), "session-1", "old-turn", 0);
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
