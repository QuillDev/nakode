//! Authoritative request and publication broker for Nakode's public API.
//!
//! Transport adapters submit semantic commands and queries through
//! [`ServerEndpoint`]. The application server remains the sole owner of
//! canonical state, persistence, policy, and execution.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use nakode_protocol::{
    ClientId, Command, CommandAccepted, Cursor, ErrorCode, IdempotencyKey, Query, QueryResult,
    RequestId, ServerEpoch, ServiceCapabilities, ServiceError, Snapshot, SubscriptionId,
    SubscriptionScope, SubscriptionView, ViewEvent,
};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

pub mod grpc;

const DEFAULT_PUBLICATION_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("Nakode server event sequence is exhausted")]
pub struct PublishError;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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
