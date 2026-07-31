//! Persistent, bounded transport for the frontend-neutral Nakode protocol.
//!
//! This crate owns connection framing, handshake/version negotiation,
//! subscription bookkeeping, replay history, and slow-client isolation. Domain
//! state and command execution remain behind the [`ServerRequest`] channel.

use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use nakode_protocol::{
    ClientFrame, ClientId, Command, CommandAccepted, Cursor, ErrorCode, IdempotencyKey,
    PROTOCOL_VERSION, Query, QueryResult, RequestId, ServerEpoch, ServerFrame, ServiceCapabilities,
    ServiceError, Snapshot, SubscriptionId, SubscriptionScope, SubscriptionView, ViewEvent,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, broadcast, mpsc, oneshot},
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_EVENT_HISTORY: usize = 512;
const DEFAULT_CONNECTION_EVENTS: usize = 256;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Nakode transport I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Nakode frame codec error: {0}")]
    Codec(#[from] LinesCodecError),
    #[error("invalid Nakode protocol frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("outbound Nakode frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("Nakode server event sequence is exhausted")]
pub struct PublishError;

#[derive(Debug)]
pub enum ServerRequest {
    Command {
        client_id: ClientId,
        request_id: RequestId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
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
    ResumeSubscription {
        client_id: ClientId,
        request_id: RequestId,
        subscription_id: SubscriptionId,
        scope: SubscriptionScope,
        after: Cursor,
        respond: oneshot::Sender<ResumeReply>,
    },
    Detached {
        client_id: ClientId,
    },
}

#[derive(Clone, Debug)]
pub struct PublishedEvent {
    pub cursor: Cursor,
    pub scopes: Vec<SubscriptionScope>,
    pub event: ViewEvent,
}

#[derive(Debug)]
pub enum ResumeReply {
    Resumed {
        through: Cursor,
        events: Vec<PublishedEvent>,
    },
    ResyncRequired {
        oldest_available: Cursor,
        current: Cursor,
    },
    Rejected(ServiceError),
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
    journal: Mutex<EventJournal>,
    next_subscription_id: AtomicU64,
}

struct EventJournal {
    history: VecDeque<PublishedEvent>,
    capacity: usize,
    sequence: u64,
}

impl ServerEndpoint {
    #[must_use]
    pub fn channel(
        server_version: impl Into<String>,
        capabilities: ServiceCapabilities,
        request_capacity: usize,
    ) -> (Self, ServerRequests) {
        let (requests, receiver) = mpsc::channel(request_capacity.max(1));
        let (publications, _) = broadcast::channel(DEFAULT_CONNECTION_EVENTS);
        (
            Self {
                inner: Arc::new(Inner {
                    epoch: ServerEpoch::from(uuid::Uuid::now_v7().to_string()),
                    capabilities,
                    server_version: server_version.into(),
                    requests,
                    publications,
                    journal: Mutex::new(EventJournal {
                        history: VecDeque::with_capacity(DEFAULT_EVENT_HISTORY),
                        capacity: DEFAULT_EVENT_HISTORY,
                        sequence: 0,
                    }),
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
    pub async fn cursor(&self) -> Cursor {
        let journal = self.inner.journal.lock().await;
        Cursor {
            server_epoch: self.inner.epoch.clone(),
            sequence: journal.sequence,
        }
    }

    /// Publishes one semantic event without waiting for any connected client.
    ///
    /// # Errors
    /// Returns an error if the server epoch exhausts its 64-bit event sequence.
    pub async fn publish(
        &self,
        scopes: Vec<SubscriptionScope>,
        event: ViewEvent,
    ) -> Result<Cursor, PublishError> {
        let publication = {
            let mut journal = self.inner.journal.lock().await;
            journal.sequence = journal.sequence.checked_add(1).ok_or(PublishError)?;
            let publication = PublishedEvent {
                cursor: Cursor {
                    server_epoch: self.inner.epoch.clone(),
                    sequence: journal.sequence,
                },
                scopes,
                event,
            };
            journal.history.push_back(publication.clone());
            if journal.history.len() > journal.capacity {
                journal.history.pop_front();
            }
            publication
        };
        let _ = self.inner.publications.send(publication.clone());
        Ok(publication.cursor)
    }

    /// Returns retained events strictly after a cursor for one scope.
    ///
    /// # Errors
    /// Returns the oldest resumable and current cursors when the epoch differs
    /// or the requested sequence is outside retained history.
    pub async fn replay(
        &self,
        scope: &SubscriptionScope,
        after: &Cursor,
    ) -> Result<(Cursor, Vec<PublishedEvent>), (Cursor, Cursor)> {
        let journal = self.inner.journal.lock().await;
        let current = Cursor {
            server_epoch: self.inner.epoch.clone(),
            sequence: journal.sequence,
        };
        let oldest = journal.history.front().map_or_else(
            || current.clone(),
            |event| Cursor {
                server_epoch: self.inner.epoch.clone(),
                sequence: event.cursor.sequence.saturating_sub(1),
            },
        );
        if after.server_epoch != self.inner.epoch
            || after.sequence > current.sequence
            || after.sequence < oldest.sequence
        {
            return Err((oldest, current));
        }
        Ok((
            current,
            journal
                .history
                .iter()
                .filter(|event| {
                    event.cursor.sequence > after.sequence && event.scopes.contains(scope)
                })
                .cloned()
                .collect(),
        ))
    }

    /// Serves one persistent frontend connection until detach or disconnect.
    ///
    /// # Errors
    /// Returns framing, decoding, or socket write failures. A normal client
    /// disconnect is not an error.
    pub async fn serve_stream<Stream>(&self, stream: Stream) -> Result<(), TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let Some(hello) = framed.next().await else {
            return Ok(());
        };
        let hello = serde_json::from_str::<ClientFrame>(&hello?)?;
        let ClientFrame::Hello {
            supported,
            client_id,
            ..
        } = hello
        else {
            send_frame(
                &mut framed,
                &ServerFrame::Fatal {
                    error: service_error(
                        ErrorCode::InvalidRequest,
                        "the first frame must be hello",
                        false,
                    ),
                },
            )
            .await?;
            return Ok(());
        };
        if !supported.supports(PROTOCOL_VERSION) {
            send_frame(
                &mut framed,
                &ServerFrame::Fatal {
                    error: service_error(
                        ErrorCode::UnsupportedVersion,
                        &format!(
                            "server protocol {PROTOCOL_VERSION} is outside client range {}-{}",
                            supported.minimum, supported.maximum
                        ),
                        false,
                    ),
                },
            )
            .await?;
            return Ok(());
        }
        send_frame(
            &mut framed,
            &ServerFrame::Welcome {
                version: PROTOCOL_VERSION,
                server_epoch: self.inner.epoch.clone(),
                server_version: self.inner.server_version.clone(),
                capabilities: self.inner.capabilities.clone(),
            },
        )
        .await?;

        let mut publications = self.inner.publications.subscribe();
        let mut subscriptions = HashMap::<SubscriptionId, SubscriptionScope>::new();
        loop {
            tokio::select! {
                incoming = framed.next() => {
                    let Some(incoming) = incoming else {
                        self.notify_detached(client_id).await;
                        return Ok(());
                    };
                    let frame = serde_json::from_str::<ClientFrame>(&incoming?)?;
                    if self.handle_frame(&mut framed, &client_id, frame, &mut subscriptions).await? {
                        return Ok(());
                    }
                }
                publication = publications.recv(), if !subscriptions.is_empty() => {
                    match publication {
                        Ok(publication) => {
                            for (subscription_id, scope) in &subscriptions {
                                if publication.scopes.contains(scope) {
                                    send_frame(&mut framed, &ServerFrame::Event {
                                        subscription_id: subscription_id.clone(),
                                        cursor: publication.cursor.clone(),
                                        event: publication.event.clone(),
                                    }).await?;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let current = self.cursor().await;
                            let oldest_available = self.oldest_cursor().await;
                            for subscription_id in subscriptions.keys() {
                                send_frame(&mut framed, &ServerFrame::SubscriptionLagged {
                                    subscription_id: subscription_id.clone(),
                                    oldest_available: oldest_available.clone(),
                                    current: current.clone(),
                                }).await?;
                            }
                            subscriptions.clear();
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
        }
    }

    async fn handle_frame<Stream>(
        &self,
        framed: &mut Framed<Stream, LinesCodec>,
        client_id: &ClientId,
        frame: ClientFrame,
        subscriptions: &mut HashMap<SubscriptionId, SubscriptionScope>,
    ) -> Result<bool, TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        match frame {
            ClientFrame::Hello { .. } => {
                send_frame(
                    framed,
                    &ServerFrame::Fatal {
                        error: service_error(
                            ErrorCode::InvalidRequest,
                            "hello was already completed",
                            false,
                        ),
                    },
                )
                .await?;
            }
            ClientFrame::Command {
                request_id,
                idempotency_key,
                expected_revision,
                command,
            } => {
                if self
                    .handle_command(
                        framed,
                        client_id,
                        request_id,
                        idempotency_key,
                        expected_revision,
                        command,
                    )
                    .await?
                {
                    return Ok(true);
                }
            }
            ClientFrame::Query { request_id, query } => {
                if self
                    .handle_query(framed, client_id, request_id, query)
                    .await?
                {
                    return Ok(true);
                }
            }
            ClientFrame::Subscribe { request_id, scope } => {
                if self
                    .handle_subscribe(framed, client_id, request_id, scope, subscriptions)
                    .await?
                {
                    return Ok(true);
                }
            }
            ClientFrame::ResumeSubscription {
                request_id,
                scope,
                after,
            } => {
                if self
                    .handle_resume(framed, client_id, request_id, scope, after, subscriptions)
                    .await?
                {
                    return Ok(true);
                }
            }
            ClientFrame::Unsubscribe { subscription_id } => {
                subscriptions.remove(&subscription_id);
            }
            ClientFrame::Ping { nonce } => {
                send_frame(framed, &ServerFrame::Pong { nonce }).await?;
            }
            ClientFrame::Detach => {
                self.notify_detached(client_id.clone()).await;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn handle_command<Stream>(
        &self,
        framed: &mut Framed<Stream, LinesCodec>,
        client_id: &ClientId,
        request_id: RequestId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        command: Command,
    ) -> Result<bool, TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let (respond, receive) = oneshot::channel();
        let response_id = request_id.clone();
        if self
            .inner
            .requests
            .send(ServerRequest::Command {
                client_id: client_id.clone(),
                request_id,
                idempotency_key,
                expected_revision,
                command,
                respond,
            })
            .await
            .is_err()
        {
            send_unavailable(framed).await?;
            return Ok(true);
        }
        let result = receive.await.unwrap_or_else(|_| Err(unavailable_error()));
        send_frame(
            framed,
            &ServerFrame::CommandResult {
                request_id: response_id,
                result,
            },
        )
        .await?;
        Ok(false)
    }

    async fn handle_query<Stream>(
        &self,
        framed: &mut Framed<Stream, LinesCodec>,
        client_id: &ClientId,
        request_id: RequestId,
        query: Query,
    ) -> Result<bool, TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let (respond, receive) = oneshot::channel();
        let response_id = request_id.clone();
        if self
            .inner
            .requests
            .send(ServerRequest::Query {
                client_id: client_id.clone(),
                request_id,
                query,
                respond,
            })
            .await
            .is_err()
        {
            send_unavailable(framed).await?;
            return Ok(true);
        }
        let result = receive.await.unwrap_or_else(|_| Err(unavailable_error()));
        send_frame(
            framed,
            &ServerFrame::QueryResult {
                request_id: response_id,
                result,
            },
        )
        .await?;
        Ok(false)
    }

    async fn handle_subscribe<Stream>(
        &self,
        framed: &mut Framed<Stream, LinesCodec>,
        client_id: &ClientId,
        request_id: RequestId,
        scope: SubscriptionScope,
        subscriptions: &mut HashMap<SubscriptionId, SubscriptionScope>,
    ) -> Result<bool, TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let subscription_id = self.next_subscription_id();
        let (respond, receive) = oneshot::channel();
        let response_id = request_id.clone();
        if self
            .inner
            .requests
            .send(ServerRequest::Subscribe {
                client_id: client_id.clone(),
                request_id,
                subscription_id: subscription_id.clone(),
                scope: scope.clone(),
                respond,
            })
            .await
            .is_err()
        {
            send_unavailable(framed).await?;
            return Ok(true);
        }
        match receive.await.unwrap_or_else(|_| Err(unavailable_error())) {
            Ok(snapshot) => {
                subscriptions.insert(subscription_id.clone(), scope);
                send_frame(
                    framed,
                    &ServerFrame::Subscribed {
                        request_id: response_id,
                        subscription_id,
                        snapshot,
                    },
                )
                .await?;
            }
            Err(error) => send_frame(framed, &ServerFrame::Fatal { error }).await?,
        }
        Ok(false)
    }

    async fn handle_resume<Stream>(
        &self,
        framed: &mut Framed<Stream, LinesCodec>,
        client_id: &ClientId,
        request_id: RequestId,
        scope: SubscriptionScope,
        after: Cursor,
        subscriptions: &mut HashMap<SubscriptionId, SubscriptionScope>,
    ) -> Result<bool, TransportError>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let subscription_id = self.next_subscription_id();
        let (respond, receive) = oneshot::channel();
        let response_id = request_id.clone();
        if self
            .inner
            .requests
            .send(ServerRequest::ResumeSubscription {
                client_id: client_id.clone(),
                request_id,
                subscription_id: subscription_id.clone(),
                scope: scope.clone(),
                after: after.clone(),
                respond,
            })
            .await
            .is_err()
        {
            send_unavailable(framed).await?;
            return Ok(true);
        }
        match receive
            .await
            .unwrap_or_else(|_| ResumeReply::Rejected(unavailable_error()))
        {
            ResumeReply::Resumed { through, events } => {
                subscriptions.insert(subscription_id.clone(), scope);
                send_frame(
                    framed,
                    &ServerFrame::SubscriptionResumed {
                        request_id: response_id,
                        subscription_id: subscription_id.clone(),
                        from: after,
                        through,
                    },
                )
                .await?;
                for event in events {
                    send_frame(
                        framed,
                        &ServerFrame::Event {
                            subscription_id: subscription_id.clone(),
                            cursor: event.cursor,
                            event: event.event,
                        },
                    )
                    .await?;
                }
            }
            ResumeReply::ResyncRequired {
                oldest_available,
                current,
            } => {
                send_frame(
                    framed,
                    &ServerFrame::ResyncRequired {
                        request_id: response_id,
                        oldest_available,
                        current,
                    },
                )
                .await?;
            }
            ResumeReply::Rejected(error) => {
                send_frame(framed, &ServerFrame::Fatal { error }).await?;
            }
        }
        Ok(false)
    }

    async fn notify_detached(&self, client_id: ClientId) {
        let _ = self
            .inner
            .requests
            .send(ServerRequest::Detached { client_id })
            .await;
    }

    fn next_subscription_id(&self) -> SubscriptionId {
        SubscriptionId::from(
            self.inner
                .next_subscription_id
                .fetch_add(1, Ordering::Relaxed)
                .to_string(),
        )
    }

    async fn oldest_cursor(&self) -> Cursor {
        let journal = self.inner.journal.lock().await;
        journal.history.front().map_or_else(
            || Cursor {
                server_epoch: self.inner.epoch.clone(),
                sequence: journal.sequence,
            },
            |event| Cursor {
                server_epoch: self.inner.epoch.clone(),
                sequence: event.cursor.sequence.saturating_sub(1),
            },
        )
    }
}

impl ServerRequests {
    pub async fn recv(&mut self) -> Option<ServerRequest> {
        self.receiver.recv().await
    }
}

async fn send_frame<Stream>(
    framed: &mut Framed<Stream, LinesCodec>,
    frame: &ServerFrame,
) -> Result<(), TransportError>
where
    Stream: AsyncRead + AsyncWrite + Unpin,
{
    let encoded = serde_json::to_string(frame)?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: encoded.len(),
            maximum: MAX_FRAME_BYTES,
        });
    }
    framed.send(encoded).await?;
    Ok(())
}

async fn send_unavailable<Stream>(
    framed: &mut Framed<Stream, LinesCodec>,
) -> Result<(), TransportError>
where
    Stream: AsyncRead + AsyncWrite + Unpin,
{
    send_frame(
        framed,
        &ServerFrame::Fatal {
            error: unavailable_error(),
        },
    )
    .await
}

fn unavailable_error() -> ServiceError {
    service_error(
        ErrorCode::Internal,
        "the Nakode server runtime is unavailable",
        true,
    )
}

fn service_error(code: ErrorCode, message: &str, retryable: bool) -> ServiceError {
    ServiceError {
        code,
        message: message.to_owned(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nakode_client::NakodeClient;
    use nakode_protocol::{
        ClientDescriptor, ClientId, ServerFrame, ServiceCapabilities, ServiceCapability,
    };

    use super::{ServerEndpoint, ServerRequest};

    #[tokio::test]
    async fn disconnect_only_notifies_detach_and_does_not_stop_the_server() {
        let capabilities = ServiceCapabilities {
            supported: BTreeSet::from([
                ServiceCapability::Subscriptions,
                ServiceCapability::MultipleClients,
            ]),
        };
        let (endpoint, mut requests) = ServerEndpoint::channel("test", capabilities, 8);
        let (client_stream, server_stream) = tokio::io::duplex(16 * 1024);
        let serving = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move { endpoint.serve_stream(server_stream).await })
        };

        let mut client = NakodeClient::from_stream(client_stream, ClientId::from("client-1"));
        let welcome = client
            .hello(ClientDescriptor {
                name: "test".to_owned(),
                version: "1".to_owned(),
                frontend: "plain".to_owned(),
            })
            .await
            .expect("welcome");
        assert!(matches!(welcome, ServerFrame::Welcome { .. }));
        client
            .send(&nakode_protocol::ClientFrame::Detach)
            .await
            .expect("detach");

        assert!(matches!(
            requests.recv().await,
            Some(ServerRequest::Detached { client_id }) if client_id.as_str() == "client-1"
        ));
        serving
            .await
            .expect("serve task")
            .expect("clean disconnect");
        assert_eq!(endpoint.cursor().await.sequence, 0);
    }
}
