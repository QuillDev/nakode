//! Authoritative request and publication broker for Nakode's public API.
//!
//! Transport adapters submit semantic commands and queries through
//! [`ServerEndpoint`]. The application server remains the sole owner of
//! canonical state, persistence, policy, and execution.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
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
        client_id: ClientId,
        request_id: RequestId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
        respond: oneshot::Sender<Result<CommandAccepted, ServiceError>>,
    },
    Query {
        client_id: ClientId,
        request_id: RequestId,
        query: Query,
        respond: oneshot::Sender<Result<Snapshot<QueryResult>, ServiceError>>,
    },
    Subscribe {
        client_id: ClientId,
        request_id: RequestId,
        subscription_id: SubscriptionId,
        scope: SubscriptionScope,
        respond: oneshot::Sender<Result<Snapshot<SubscriptionView>, ServiceError>>,
    },
}

#[derive(Clone, Debug)]
pub struct PublishedEvent {
    pub cursor: Cursor,
    pub scopes: Vec<SubscriptionScope>,
    pub event: ViewEvent,
}

#[derive(Clone)]
pub struct ServerEndpoint {
    inner: Arc<Inner>,
}

pub struct ServerRequests {
    receiver: mpsc::Receiver<ServerRequest>,
}

struct Inner {
    epoch: ServerEpoch,
    capabilities: ServiceCapabilities,
    server_version: String,
    requests: mpsc::Sender<ServerRequest>,
    publications: broadcast::Sender<PublishedEvent>,
    sequence: AtomicU64,
    next_subscription_id: AtomicU64,
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
    #[must_use]
    pub fn channel(
        server_version: impl Into<String>,
        capabilities: ServiceCapabilities,
        request_capacity: usize,
    ) -> (Self, ServerRequests) {
        let (requests, receiver) = mpsc::channel(request_capacity.max(1));
        let (publications, _) = broadcast::channel(DEFAULT_PUBLICATION_CAPACITY);
        (
            Self {
                inner: Arc::new(Inner {
                    epoch: ServerEpoch::from(uuid::Uuid::now_v7().to_string()),
                    capabilities,
                    server_version: server_version.into(),
                    requests,
                    publications,
                    sequence: AtomicU64::new(0),
                    next_subscription_id: AtomicU64::new(1),
                    subscription_refreshes: ScopeSnapshotLoads::default(),
                }),
            },
            ServerRequests { receiver },
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

    /// Executes one semantic mutation through the authoritative request loop.
    ///
    /// # Errors
    /// Returns a semantic server error or reports an unavailable request loop.
    pub async fn execute_command(
        &self,
        client_id: ClientId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
    ) -> Result<CommandAccepted, ServiceError> {
        let (respond, receive) = oneshot::channel();
        self.inner
            .requests
            .send(ServerRequest::Command {
                client_id,
                request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
                idempotency_key,
                expected_revision,
                replay_only,
                command,
                respond,
            })
            .await
            .map_err(|_| server_unavailable())?;
        receive.await.map_err(|_| server_unavailable())?
    }

    /// Executes one semantic read through the authoritative request loop.
    ///
    /// # Errors
    /// Returns a semantic server error or reports an unavailable request loop.
    pub async fn execute_query(
        &self,
        client_id: ClientId,
        query: Query,
    ) -> Result<Snapshot<QueryResult>, ServiceError> {
        let (respond, receive) = oneshot::channel();
        self.inner
            .requests
            .send(ServerRequest::Query {
                client_id,
                request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
                query,
                respond,
            })
            .await
            .map_err(|_| server_unavailable())?;
        receive.await.map_err(|_| server_unavailable())?
    }

    /// Returns the authoritative snapshot for a watch scope.
    ///
    /// # Errors
    /// Returns a semantic server error or reports an unavailable request loop.
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
                let (respond, receive) = oneshot::channel();
                self.inner
                    .requests
                    .send(ServerRequest::Subscribe {
                        client_id,
                        request_id: RequestId::new(uuid::Uuid::now_v7().to_string()),
                        subscription_id: self.next_subscription_id(),
                        scope,
                        respond,
                    })
                    .await
                    .map_err(|_| server_unavailable())?;
                receive.await.map_err(|_| server_unavailable())?
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
    pub async fn recv(&mut self) -> Option<ServerRequest> {
        self.receiver.recv().await
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
}
