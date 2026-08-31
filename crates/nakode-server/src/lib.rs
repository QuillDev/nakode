//! Authoritative request and publication broker for Nakode's public API.
//!
//! Transport adapters submit semantic commands and queries through
//! [`ServerEndpoint`]. The application server remains the sole owner of
//! canonical state, persistence, policy, and execution.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use nakode_protocol::{
    ClientId, Command, CommandAccepted, Cursor, ErrorCode, IdempotencyKey, Query, QueryResult,
    RequestId, ServerEpoch, ServiceCapabilities, ServiceError, Snapshot, SubscriptionId,
    SubscriptionScope, SubscriptionView, ViewEvent,
};
use thiserror::Error;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

pub mod grpc;

pub(crate) const DEFAULT_PUBLICATION_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("Nakode server event sequence is exhausted")]
pub struct PublishError;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
// This public broker request envelope intentionally keeps `Command` value-shaped. Boxing only the
// command arm would push allocation and ownership changes through every transport and server.
pub enum ServerRequest {
    Command {
        timing: RequestTiming,
        client_id: ClientId,
        request_id: RequestId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
        respond: oneshot::Sender<Result<CommandAccepted, ServiceError>>,
    },
    Query {
        timing: RequestTiming,
        client_id: ClientId,
        request_id: RequestId,
        query: Query,
        respond: oneshot::Sender<Result<Snapshot<QueryResult>, ServiceError>>,
    },
    Subscribe {
        timing: RequestTiming,
        client_id: ClientId,
        request_id: RequestId,
        subscription_id: SubscriptionId,
        scope: SubscriptionScope,
        respond: oneshot::Sender<Result<Snapshot<SubscriptionView>, ServiceError>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestLane {
    Control,
    Query,
    Hydration,
    Subscription,
}

impl RequestLane {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Query => "query",
            Self::Hydration => "hydration",
            Self::Subscription => "subscription",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerTiming {
    pub lane: RequestLane,
    pub lane_sequence: u64,
    pub admission: Duration,
    pub queue: Duration,
    pub service: Duration,
    pub total: Duration,
}

#[derive(Clone, Debug)]
pub struct TimedServerResponse<T> {
    pub result: Result<T, ServiceError>,
    pub timing: ServerTiming,
}

#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct RequestTiming {
    lane: RequestLane,
    lane_sequence: u64,
    dequeued_at: Arc<OnceLock<Instant>>,
}

impl RequestTiming {
    /// Creates timing state for a request constructed outside [`ServerEndpoint`].
    #[doc(hidden)]
    #[must_use]
    pub fn untracked(lane: RequestLane) -> Self {
        Self {
            lane,
            lane_sequence: 0,
            dequeued_at: Arc::new(OnceLock::new()),
        }
    }
}

impl ServerRequest {
    /// Records when the authoritative runtime dequeues this request.
    #[doc(hidden)]
    pub fn mark_dequeued(&self) {
        let timing = match self {
            Self::Command { timing, .. }
            | Self::Query { timing, .. }
            | Self::Subscribe { timing, .. } => timing,
        };
        let _ = timing.dequeued_at.set(Instant::now());
    }
}

#[derive(Clone, Debug)]
pub struct PublishedEvent {
    pub cursor: Cursor,
    pub scopes: Vec<SubscriptionScope>,
    pub event: ViewEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryLane {
    Control,
    Ordinary,
    Hydration,
}

fn query_lane(query: &Query) -> QueryLane {
    match query {
        Query::Bootstrap {
            session_id: None, ..
        } => QueryLane::Control,
        Query::Bootstrap {
            session_id: Some(_),
            ..
        }
        | Query::GetSession { .. }
        | Query::GetRun { .. }
        | Query::GetTranscriptPage { .. }
        | Query::GetRunTranscriptPage { .. }
        | Query::GetTranscriptBodyWindow { .. }
        | Query::ListRuns { .. }
        | Query::GetRunTextWindow { .. }
        | Query::GetArtifact { .. } => QueryLane::Hydration,
        _ => QueryLane::Ordinary,
    }
}

#[derive(Clone)]
pub struct ServerEndpoint {
    inner: Arc<Inner>,
}

pub struct ServerRequests {
    control: mpsc::Receiver<ServerRequest>,
    queries: mpsc::Receiver<ServerRequest>,
    hydration: mpsc::Receiver<ServerRequest>,
    subscriptions: mpsc::Receiver<ServerRequest>,
    next_lower_lane: usize,
    consecutive_control: usize,
}

struct Inner {
    epoch: ServerEpoch,
    capabilities: ServiceCapabilities,
    server_version: String,
    build_revision: Option<String>,
    control_requests: mpsc::Sender<ServerRequest>,
    query_requests: mpsc::Sender<ServerRequest>,
    hydration_requests: mpsc::Sender<ServerRequest>,
    subscription_requests: mpsc::Sender<ServerRequest>,
    publications: broadcast::Sender<PublishedEvent>,
    sequence: AtomicU64,
    next_subscription_id: AtomicU64,
    control_sequence: AtomicU64,
    query_sequence: AtomicU64,
    hydration_sequence: AtomicU64,
    subscription_sequence: AtomicU64,
    subscription_refreshes: ScopeSnapshotLoads<Result<Snapshot<SubscriptionView>, ServiceError>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SubscriptionCacheKey {
    Workspace(String),
    Session(String),
    Run(String),
}

impl From<&SubscriptionScope> for SubscriptionCacheKey {
    fn from(scope: &SubscriptionScope) -> Self {
        match scope {
            SubscriptionScope::Workspace { workspace_id } => {
                Self::Workspace(workspace_id.to_string())
            }
            SubscriptionScope::Session { session_id } => Self::Session(session_id.to_string()),
            SubscriptionScope::Run { run_id } => Self::Run(run_id.to_string()),
        }
    }
}

type InFlightSnapshot<Value> = Arc<Mutex<Option<(Cursor, Value)>>>;
type SharedInFlightSnapshot<Value> = Weak<Mutex<Option<(Cursor, Value)>>>;

/// Coalesces overlapping refreshes without retaining completed resource snapshots.
struct ScopeSnapshotLoads<Value> {
    entries: Mutex<HashMap<SubscriptionCacheKey, SharedInFlightSnapshot<Value>>>,
}

impl<Value> Default for ScopeSnapshotLoads<Value> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<Value: Clone> ScopeSnapshotLoads<Value> {
    async fn get_or_load<Load, Loaded>(
        &self,
        key: SubscriptionCacheKey,
        cursor: Cursor,
        load: Load,
    ) -> Value
    where
        Load: FnOnce() -> Loaded,
        Loaded: Future<Output = Value>,
    {
        let entry: InFlightSnapshot<Value> = {
            let mut entries = self.entries.lock().await;
            entries.retain(|_, entry| entry.strong_count() > 0);
            if let Some(entry) = entries.get(&key).and_then(Weak::upgrade) {
                entry
            } else {
                let entry = Arc::new(Mutex::new(None));
                entries.insert(key, Arc::downgrade(&entry));
                entry
            }
        };
        let mut cached = entry.lock().await;
        if let Some((cached_cursor, value)) = cached.as_ref()
            && cached_cursor == &cursor
        {
            return value.clone();
        }
        let value = load().await;
        *cached = Some((cursor, value.clone()));
        value
    }
}

impl ServerEndpoint {
    /// Creates a bounded server endpoint.
    ///
    /// `request_capacity` is the aggregate admission budget split across four reserved traffic
    /// lanes. Values below four are raised to four so every lane can make progress.
    #[must_use]
    pub fn channel(
        server_version: impl Into<String>,
        capabilities: ServiceCapabilities,
        request_capacity: usize,
    ) -> (Self, ServerRequests) {
        Self::channel_with_build_revision(server_version, None, capabilities, request_capacity)
    }

    #[must_use]
    pub fn channel_with_build_revision(
        server_version: impl Into<String>,
        build_revision: Option<String>,
        capabilities: ServiceCapabilities,
        request_capacity: usize,
    ) -> (Self, ServerRequests) {
        // Reserve admission independently for interactive control/catalogue work, ordinary reads,
        // history hydration, and replacement-snapshot refreshes. The serialized runtime still owns
        // execution order, but a full history lane can no longer prevent a command from entering it.
        let request_capacity = request_capacity.max(4);
        let lane_capacity = (request_capacity / 4).max(1);
        let (control_requests, control) = mpsc::channel(lane_capacity);
        let (query_requests, queries) = mpsc::channel(lane_capacity);
        let (hydration_requests, hydration) = mpsc::channel(lane_capacity);
        let (subscription_requests, subscriptions) = mpsc::channel(
            request_capacity
                .saturating_sub(lane_capacity.saturating_mul(3))
                .max(1),
        );
        let (publications, _) = broadcast::channel(DEFAULT_PUBLICATION_CAPACITY);
        (
            Self {
                inner: Arc::new(Inner {
                    epoch: ServerEpoch::from(uuid::Uuid::now_v7().to_string()),
                    capabilities,
                    server_version: server_version.into(),
                    build_revision,
                    control_requests,
                    query_requests,
                    hydration_requests,
                    subscription_requests,
                    publications,
                    sequence: AtomicU64::new(0),
                    next_subscription_id: AtomicU64::new(1),
                    control_sequence: AtomicU64::new(0),
                    query_sequence: AtomicU64::new(0),
                    hydration_sequence: AtomicU64::new(0),
                    subscription_sequence: AtomicU64::new(0),
                    subscription_refreshes: ScopeSnapshotLoads::default(),
                }),
            },
            ServerRequests {
                control,
                queries,
                hydration,
                subscriptions,
                next_lower_lane: 0,
                consecutive_control: 0,
            },
        )
    }

    #[must_use]
    pub fn epoch(&self) -> &ServerEpoch {
        &self.inner.epoch
    }

    #[must_use]
    pub fn capabilities(&self) -> &ServiceCapabilities {
        &self.inner.capabilities
    }

    #[must_use]
    pub fn server_version(&self) -> &str {
        &self.inner.server_version
    }

    #[must_use]
    pub fn build_revision(&self) -> Option<&str> {
        self.inner.build_revision.as_deref()
    }

    fn next_lane_sequence(&self, lane: RequestLane) -> u64 {
        let counter = match lane {
            RequestLane::Control => &self.inner.control_sequence,
            RequestLane::Query => &self.inner.query_sequence,
            RequestLane::Hydration => &self.inner.hydration_sequence,
            RequestLane::Subscription => &self.inner.subscription_sequence,
        };
        counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    async fn submit<T>(
        &self,
        sender: &mpsc::Sender<ServerRequest>,
        lane: RequestLane,
        make_request: impl FnOnce(
            RequestTiming,
            oneshot::Sender<Result<T, ServiceError>>,
        ) -> ServerRequest,
    ) -> TimedServerResponse<T> {
        let started_at = Instant::now();
        let lane_sequence = self.next_lane_sequence(lane);
        let Ok(permit) = sender.reserve().await else {
            return TimedServerResponse {
                result: Err(server_unavailable()),
                timing: ServerTiming {
                    lane,
                    lane_sequence,
                    admission: started_at.elapsed(),
                    queue: Duration::ZERO,
                    service: Duration::ZERO,
                    total: started_at.elapsed(),
                },
            };
        };
        let admitted_at = Instant::now();
        let pending = RequestTiming {
            lane,
            lane_sequence,
            dequeued_at: Arc::new(OnceLock::new()),
        };
        let (respond, receive) = oneshot::channel();
        permit.send(make_request(pending.clone(), respond));
        let result = receive.await.unwrap_or_else(|_| Err(server_unavailable()));
        let response_ready_at = Instant::now();
        let dequeued_at = pending.dequeued_at.get().copied().unwrap_or(admitted_at);
        TimedServerResponse {
            result,
            timing: ServerTiming {
                lane: pending.lane,
                lane_sequence: pending.lane_sequence,
                admission: admitted_at.saturating_duration_since(started_at),
                queue: dequeued_at.saturating_duration_since(admitted_at),
                service: response_ready_at.saturating_duration_since(dequeued_at),
                total: response_ready_at.saturating_duration_since(started_at),
            },
        }
    }

    pub async fn execute_command_timed(
        &self,
        client_id: ClientId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
    ) -> TimedServerResponse<CommandAccepted> {
        self.submit(
            &self.inner.control_requests,
            RequestLane::Control,
            |timing, respond| ServerRequest::Command {
                timing,
                client_id,
                request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
                idempotency_key,
                expected_revision,
                replay_only,
                command,
                respond,
            },
        )
        .await
    }

    /// Executes one semantic mutation through the authoritative request loop.
    ///
    /// # Errors
    ///
    /// Returns a semantic service error if the request is rejected or the runtime is unavailable.
    pub async fn execute_command(
        &self,
        client_id: ClientId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
    ) -> Result<CommandAccepted, ServiceError> {
        self.execute_command_timed(
            client_id,
            idempotency_key,
            expected_revision,
            replay_only,
            command,
        )
        .await
        .result
    }

    pub async fn execute_query_timed(
        &self,
        client_id: ClientId,
        query: Query,
    ) -> TimedServerResponse<Snapshot<QueryResult>> {
        let (lane, sender) = match query_lane(&query) {
            QueryLane::Control => (RequestLane::Control, &self.inner.control_requests),
            QueryLane::Ordinary => (RequestLane::Query, &self.inner.query_requests),
            QueryLane::Hydration => (RequestLane::Hydration, &self.inner.hydration_requests),
        };
        self.submit(sender, lane, |timing, respond| ServerRequest::Query {
            timing,
            client_id,
            request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
            query,
            respond,
        })
        .await
    }

    /// Executes one semantic read through the authoritative request loop.
    ///
    /// # Errors
    ///
    /// Returns a semantic service error if the request is rejected or the runtime is unavailable.
    pub async fn execute_query(
        &self,
        client_id: ClientId,
        query: Query,
    ) -> Result<Snapshot<QueryResult>, ServiceError> {
        self.execute_query_timed(client_id, query).await.result
    }

    pub async fn execute_subscription_timed(
        &self,
        client_id: ClientId,
        scope: SubscriptionScope,
    ) -> TimedServerResponse<Snapshot<SubscriptionView>> {
        self.submit(
            &self.inner.subscription_requests,
            RequestLane::Subscription,
            |timing, respond| ServerRequest::Subscribe {
                timing,
                client_id,
                request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
                subscription_id: self.next_subscription_id(),
                scope,
                respond,
            },
        )
        .await
    }

    /// Returns the authoritative snapshot for a watch scope.
    ///
    /// # Errors
    ///
    /// Returns a semantic service error if the request is rejected or the runtime is unavailable.
    pub async fn execute_subscription(
        &self,
        client_id: ClientId,
        scope: SubscriptionScope,
    ) -> Result<Snapshot<SubscriptionView>, ServiceError> {
        let key = SubscriptionCacheKey::from(&scope);
        let cursor = self.cursor();
        self.inner
            .subscription_refreshes
            .get_or_load(key, cursor, || async move {
                self.execute_subscription_timed(client_id, scope)
                    .await
                    .result
            })
            .await
    }

    /// Observes internal semantic publications. Public transports use these
    /// only as invalidation signals and then fetch a complete replacement.
    #[must_use]
    pub fn subscribe_publications(&self) -> broadcast::Receiver<PublishedEvent> {
        self.inner.publications.subscribe()
    }

    #[must_use]
    pub fn cursor(&self) -> Cursor {
        Cursor {
            server_epoch: self.inner.epoch.clone(),
            sequence: self.inner.sequence.load(Ordering::Acquire),
        }
    }

    /// Publishes an internal invalidation event without waiting for clients.
    ///
    /// # Errors
    /// Returns an error if the server epoch exhausts its event sequence.
    pub fn publish(
        &self,
        scopes: Vec<SubscriptionScope>,
        event: ViewEvent,
    ) -> Result<Cursor, PublishError> {
        let sequence = self
            .inner
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| PublishError)?
            + 1;
        let publication = PublishedEvent {
            cursor: Cursor {
                server_epoch: self.inner.epoch.clone(),
                sequence,
            },
            scopes,
            event,
        };
        let _ = self.inner.publications.send(publication.clone());
        Ok(publication.cursor)
    }

    fn next_subscription_id(&self) -> SubscriptionId {
        SubscriptionId::new(
            self.inner
                .next_subscription_id
                .fetch_add(1, Ordering::Relaxed)
                .to_string(),
        )
    }
}

impl ServerRequests {
    // Control gets prompt service, but every eight continuously queued control requests yield one
    // turn to a round-robin lower lane so transcript and workspace progress remain measurable.
    const CONTROL_BURST: usize = 8;
    const LOWER_LANES: usize = 3;

    fn dequeued(request: ServerRequest) -> ServerRequest {
        request.mark_dequeued();
        request
    }

    fn try_lower(&mut self) -> Option<ServerRequest> {
        for offset in 0..Self::LOWER_LANES {
            let lane = (self.next_lower_lane + offset) % Self::LOWER_LANES;
            let request = match lane {
                0 => self.queries.try_recv().ok(),
                1 => self.hydration.try_recv().ok(),
                2 => self.subscriptions.try_recv().ok(),
                _ => unreachable!(),
            };
            if let Some(request) = request {
                self.next_lower_lane = (lane + 1) % Self::LOWER_LANES;
                self.consecutive_control = 0;
                return Some(Self::dequeued(request));
            }
        }
        None
    }

    fn closed_and_empty(&self) -> bool {
        self.control.is_closed()
            && self.control.is_empty()
            && self.queries.is_closed()
            && self.queries.is_empty()
            && self.hydration.is_closed()
            && self.hydration.is_empty()
            && self.subscriptions.is_closed()
            && self.subscriptions.is_empty()
    }

    pub async fn recv(&mut self) -> Option<ServerRequest> {
        loop {
            if self.consecutive_control < Self::CONTROL_BURST
                && let Ok(request) = self.control.try_recv()
            {
                self.consecutive_control += 1;
                return Some(Self::dequeued(request));
            }
            if let Some(request) = self.try_lower() {
                return Some(request);
            }
            if let Ok(request) = self.control.try_recv() {
                self.consecutive_control = self.consecutive_control.saturating_add(1);
                return Some(Self::dequeued(request));
            }
            if self.closed_and_empty() {
                return None;
            }

            tokio::select! {
                request = self.control.recv(), if !self.control.is_closed() => {
                    if let Some(request) = request {
                        self.consecutive_control = self.consecutive_control.saturating_add(1);
                        return Some(Self::dequeued(request));
                    }
                }
                request = self.queries.recv(), if !self.queries.is_closed() => {
                    if let Some(request) = request {
                        self.next_lower_lane = 1;
                        self.consecutive_control = 0;
                        return Some(Self::dequeued(request));
                    }
                }
                request = self.hydration.recv(), if !self.hydration.is_closed() => {
                    if let Some(request) = request {
                        self.next_lower_lane = 2;
                        self.consecutive_control = 0;
                        return Some(Self::dequeued(request));
                    }
                }
                request = self.subscriptions.recv(), if !self.subscriptions.is_closed() => {
                    if let Some(request) = request {
                        self.next_lower_lane = 0;
                        self.consecutive_control = 0;
                        return Some(Self::dequeued(request));
                    }
                }
                // Every sender can close after the pre-select closure check but before branch
                // evaluation. With no enabled receiver branch, the request stream is terminal.
                else => return None,
            }
        }
    }
}

fn server_unavailable() -> ServiceError {
    ServiceError {
        code: ErrorCode::Internal,
        message: "the Nakode server runtime is unavailable".to_owned(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::future::join_all;

    use super::*;

    #[tokio::test]
    async fn same_scope_and_cursor_share_one_snapshot_load() {
        let cache = ScopeSnapshotLoads::<Result<u64, ()>>::default();
        let loads = AtomicUsize::new(0);
        let cursor = Cursor {
            server_epoch: ServerEpoch::from("epoch"),
            sequence: 7,
        };

        let values = join_all((0..32).map(|_| {
            cache.get_or_load(
                SubscriptionCacheKey::Workspace("workspace".to_owned()),
                cursor.clone(),
                || async {
                    tokio::task::yield_now().await;
                    loads.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, ()>(41)
                },
            )
        }))
        .await;

        assert!(values.into_iter().all(|value| value == Ok(41)));
        assert_eq!(loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn same_scope_failure_is_shared_by_every_inflight_waiter() {
        let cache = ScopeSnapshotLoads::<Result<u64, &'static str>>::default();
        let loads = AtomicUsize::new(0);
        let cursor = Cursor {
            server_epoch: ServerEpoch::from("epoch"),
            sequence: 7,
        };

        let values = join_all((0..32).map(|_| {
            cache.get_or_load(
                SubscriptionCacheKey::Workspace("workspace".to_owned()),
                cursor.clone(),
                || async {
                    tokio::task::yield_now().await;
                    loads.fetch_add(1, Ordering::Relaxed);
                    Err("unavailable")
                },
            )
        }))
        .await;

        assert!(values.into_iter().all(|value| value == Err("unavailable")));
        assert_eq!(loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn completed_refreshes_are_not_retained_for_sequential_calls() {
        let loads = AtomicUsize::new(0);
        let cache = ScopeSnapshotLoads::<Result<u64, ()>>::default();
        let key = SubscriptionCacheKey::Workspace("workspace".to_owned());
        let cursor = Cursor {
            server_epoch: ServerEpoch::from("epoch"),
            sequence: 7,
        };

        for _ in 0..2 {
            cache
                .get_or_load(key.clone(), cursor.clone(), || async {
                    loads.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, ()>(41)
                })
                .await
                .expect("snapshot load");
        }

        assert_eq!(loads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn newer_cursor_waits_for_and_then_replaces_an_inflight_snapshot() {
        let cache = ScopeSnapshotLoads::<Result<u64, ()>>::default();
        let loads = AtomicUsize::new(0);
        let key = SubscriptionCacheKey::Session("session".to_owned());
        let epoch = ServerEpoch::from("epoch");
        let first = cache.get_or_load(
            key.clone(),
            Cursor {
                server_epoch: epoch.clone(),
                sequence: 1,
            },
            || async {
                tokio::task::yield_now().await;
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(1)
            },
        );
        let second = cache.get_or_load(
            key,
            Cursor {
                server_epoch: epoch,
                sequence: 2,
            },
            || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(2)
            },
        );

        let (first, second) = tokio::join!(first, second);

        assert_eq!(first, Ok(1));
        assert_eq!(second, Ok(2));
        assert_eq!(loads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn completed_scope_loads_do_not_accumulate_cache_entries() {
        let cache = ScopeSnapshotLoads::<Result<u64, ()>>::default();
        let epoch = ServerEpoch::from("epoch");

        for sequence in 0..1_000 {
            cache
                .get_or_load(
                    SubscriptionCacheKey::Run(format!("run-{sequence}")),
                    Cursor {
                        server_epoch: epoch.clone(),
                        sequence,
                    },
                    || async { Ok::<_, ()>(sequence) },
                )
                .await
                .expect("snapshot load");
        }

        let entries = cache.entries.lock().await;
        assert_eq!(entries.len(), 1);
        assert!(entries.values().all(|entry| entry.strong_count() == 0));
    }

    #[test]
    fn query_routing_keeps_catalogue_lightweight_and_session_projection_out_of_control() {
        assert_eq!(
            query_lane(&Query::Bootstrap {
                workspace: "/workspace".to_owned(),
                session_id: None,
            }),
            QueryLane::Control
        );
        assert_eq!(
            query_lane(&Query::Bootstrap {
                workspace: "/workspace".to_owned(),
                session_id: Some(nakode_protocol::SessionId::from("session")),
            }),
            QueryLane::Hydration
        );
        assert_eq!(
            query_lane(&Query::GetSession {
                session_id: nakode_protocol::SessionId::from("session"),
            }),
            QueryLane::Hydration
        );
        assert_eq!(
            query_lane(&Query::GetRun {
                run_id: nakode_protocol::RunId::from("run"),
            }),
            QueryLane::Hydration
        );
        assert_eq!(
            query_lane(&Query::GetInvocationSummary),
            QueryLane::Ordinary
        );
    }

    #[tokio::test]
    async fn request_receiver_terminates_after_every_lane_closes() {
        let (endpoint, mut requests) =
            ServerEndpoint::channel("closure-test", ServiceCapabilities::default(), 4);
        drop(endpoint);

        assert!(requests.recv().await.is_none());
    }

    #[tokio::test]
    async fn receiver_records_queue_time_before_handing_request_to_consumer() {
        let (endpoint, mut requests) =
            ServerEndpoint::channel("timing-test", ServiceCapabilities::default(), 4);
        let query_endpoint = endpoint.clone();
        let query = tokio::spawn(async move {
            query_endpoint
                .execute_query_timed(ClientId::from("reader"), transcript_window_query(1))
                .await
        });
        wait_for_queued(&endpoint.inner.hydration_requests, 1).await;
        tokio::time::sleep(Duration::from_millis(5)).await;

        let request = requests.recv().await.expect("queued request");
        let ServerRequest::Query { respond, .. } = request else {
            panic!("expected query request");
        };
        let _ = respond.send(Err(server_unavailable()));
        let timed = query.await.expect("query task");

        assert!(timed.timing.queue >= Duration::from_millis(5));
        assert!(timed.timing.total >= timed.timing.queue);
    }

    #[tokio::test]
    async fn concurrent_timed_subscriptions_keep_independent_lane_sequences() {
        let (endpoint, mut requests) = ServerEndpoint::channel(
            "subscription-timing-test",
            ServiceCapabilities::default(),
            8,
        );
        let first_endpoint = endpoint.clone();
        let first = tokio::spawn(async move {
            first_endpoint
                .execute_subscription_timed(
                    ClientId::from("watcher-1"),
                    SubscriptionScope::Workspace {
                        workspace_id: nakode_protocol::WorkspaceId::from("workspace"),
                    },
                )
                .await
        });
        let second_endpoint = endpoint.clone();
        let second = tokio::spawn(async move {
            second_endpoint
                .execute_subscription_timed(
                    ClientId::from("watcher-2"),
                    SubscriptionScope::Workspace {
                        workspace_id: nakode_protocol::WorkspaceId::from("workspace"),
                    },
                )
                .await
        });
        wait_for_queued(&endpoint.inner.subscription_requests, 2).await;

        for _ in 0..2 {
            let request = requests.recv().await.expect("queued subscription");
            let ServerRequest::Subscribe { respond, .. } = request else {
                panic!("expected subscription request");
            };
            let _ = respond.send(Err(server_unavailable()));
        }
        let first = first.await.expect("first subscription task");
        let second = second.await.expect("second subscription task");

        assert_ne!(first.timing.lane_sequence, second.timing.lane_sequence);
        assert_eq!(first.timing.lane, RequestLane::Subscription);
        assert_eq!(second.timing.lane, RequestLane::Subscription);
    }

    #[tokio::test]
    async fn command_overtakes_eight_concurrent_transcript_window_reads() {
        let (endpoint, mut requests) =
            ServerEndpoint::channel("pressure-test", ServiceCapabilities::default(), 64);
        let mut reads = Vec::new();
        for index in 0..8 {
            let endpoint = endpoint.clone();
            reads.push(tokio::spawn(async move {
                endpoint
                    .execute_query(
                        ClientId::from(format!("parent-{index}")),
                        transcript_window_query(index),
                    )
                    .await
            }));
        }
        wait_for_queued(&endpoint.inner.hydration_requests, 8).await;

        let command_endpoint = endpoint.clone();
        let command = tokio::spawn(async move {
            command_endpoint
                .execute_command(
                    ClientId::from("orchestrator"),
                    IdempotencyKey::from("pressure-command"),
                    None,
                    false,
                    reload_provider_command(),
                )
                .await
        });
        wait_for_queued(&endpoint.inner.control_requests, 1).await;

        let first = requests.recv().await.expect("queued request");
        assert!(
            matches!(first, ServerRequest::Command { .. }),
            "interactive command waited behind transcript hydration traffic"
        );

        command.abort();
        for read in reads {
            read.abort();
        }
    }

    #[tokio::test]
    async fn transcript_reads_make_progress_through_a_sustained_control_backlog() {
        let (endpoint, mut requests) =
            ServerEndpoint::channel("fairness-test", ServiceCapabilities::default(), 64);
        let mut commands = Vec::new();
        for index in 0..9 {
            let endpoint = endpoint.clone();
            commands.push(tokio::spawn(async move {
                endpoint
                    .execute_command(
                        ClientId::from(format!("client-{index}")),
                        IdempotencyKey::from(format!("command-{index}")),
                        None,
                        false,
                        reload_provider_command(),
                    )
                    .await
            }));
        }
        let hydration_endpoint = endpoint.clone();
        let hydration = tokio::spawn(async move {
            hydration_endpoint
                .execute_query(ClientId::from("hydrator"), transcript_window_query(10))
                .await
        });
        wait_for_queued(&endpoint.inner.control_requests, 9).await;
        wait_for_queued(&endpoint.inner.hydration_requests, 1).await;

        for position in 0..=ServerRequests::CONTROL_BURST {
            let request = requests.recv().await.expect("queued request");
            if position < ServerRequests::CONTROL_BURST {
                assert!(matches!(request, ServerRequest::Command { .. }));
            } else {
                assert!(
                    matches!(request, ServerRequest::Query { .. }),
                    "control traffic starved the bounded hydration lane"
                );
            }
        }

        hydration.abort();
        for command in commands {
            command.abort();
        }
    }

    async fn wait_for_queued(sender: &mpsc::Sender<ServerRequest>, expected: usize) {
        for _ in 0..1_000 {
            if sender.max_capacity().saturating_sub(sender.capacity()) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected {expected} queued requests");
    }

    fn transcript_window_query(index: usize) -> Query {
        Query::GetTranscriptBodyWindow {
            owner: nakode_protocol::TranscriptOwner::Session {
                session_id: nakode_protocol::SessionId::from(format!("session-{index}")),
            },
            entry_id: nakode_protocol::EntryId::from(format!("entry-{index}")),
            before_byte: None,
            limit_bytes: 64 * 1_024,
        }
    }

    fn reload_provider_command() -> Command {
        Command::ReloadProvider {
            provider_id: nakode_protocol::ProviderId::from("fixture"),
        }
    }
}
