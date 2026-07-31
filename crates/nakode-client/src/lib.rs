//! Reusable client connection for any Nakode frontend.
//!
//! The client transports only [`nakode_protocol`] frames. It does not depend on
//! the Nakode server implementation or on a particular UI toolkit.

use std::io;

use futures_util::{SinkExt, StreamExt};
use nakode_protocol::{
    ClientDescriptor, ClientFrame, ClientId, Command, Cursor, IdempotencyKey, Query, RequestId,
    ServerFrame, SubscriptionScope, VersionRange,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

pub use nakode_protocol as protocol;

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Nakode transport error: {0}")]
    Io(#[from] io::Error),
    #[error("Nakode frame codec error: {0}")]
    Codec(#[from] LinesCodecError),
    #[error("invalid Nakode protocol frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Nakode server closed the connection")]
    Disconnected,
    #[error("expected Nakode welcome frame, received {0:?}")]
    UnexpectedHandshake(Box<ServerFrame>),
    #[error("Nakode server selected unsupported protocol version {0}")]
    UnsupportedVersion(u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandTicket {
    pub request_id: RequestId,
    pub idempotency_key: IdempotencyKey,
}

pub struct NakodeClient<Stream> {
    framed: Framed<Stream, LinesCodec>,
    client_id: ClientId,
}

impl<Stream> NakodeClient<Stream>
where
    Stream: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub fn from_stream(stream: Stream, client_id: ClientId) -> Self {
        Self {
            framed: Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES)),
            client_id,
        }
    }

    #[must_use]
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Negotiates the protocol before any commands, queries, or subscriptions.
    ///
    /// # Errors
    /// Returns a transport, decoding, or unexpected-frame error.
    pub async fn hello(
        &mut self,
        descriptor: ClientDescriptor,
    ) -> Result<ServerFrame, ClientError> {
        self.send(&ClientFrame::Hello {
            supported: VersionRange::current(),
            client_id: self.client_id.clone(),
            client: descriptor,
        })
        .await?;
        let response = self.receive().await?;
        match response {
            response @ ServerFrame::Welcome { version, .. }
                if VersionRange::current().supports(version) =>
            {
                Ok(response)
            }
            ServerFrame::Welcome { version, .. } => Err(ClientError::UnsupportedVersion(version)),
            response => Err(ClientError::UnexpectedHandshake(Box::new(response))),
        }
    }

    /// Sends a semantic command with separate correlation and idempotency IDs.
    ///
    /// # Errors
    /// Returns a transport or encoding error.
    pub async fn command(
        &mut self,
        command: Command,
        expected_revision: Option<u64>,
    ) -> Result<CommandTicket, ClientError> {
        self.command_with_key(
            command,
            expected_revision,
            IdempotencyKey::new(uuid::Uuid::now_v7().to_string()),
        )
        .await
    }

    /// Sends a semantic command using a caller-stable idempotency key.
    ///
    /// Reuse the same key when retrying an operation after reconnecting. A new
    /// request ID is generated for correlation on every attempt.
    ///
    /// # Errors
    /// Returns a transport or encoding error.
    pub async fn command_with_key(
        &mut self,
        command: Command,
        expected_revision: Option<u64>,
        idempotency_key: IdempotencyKey,
    ) -> Result<CommandTicket, ClientError> {
        let request_id = RequestId::new(uuid::Uuid::now_v7().to_string());
        self.send(&ClientFrame::Command {
            request_id: request_id.clone(),
            idempotency_key: idempotency_key.clone(),
            expected_revision,
            command,
        })
        .await?;
        Ok(CommandTicket {
            request_id,
            idempotency_key,
        })
    }

    /// Sends a frontend-neutral query.
    ///
    /// # Errors
    /// Returns a transport or encoding error.
    pub async fn query(&mut self, query: Query) -> Result<RequestId, ClientError> {
        let request_id = RequestId::new(uuid::Uuid::now_v7().to_string());
        self.send(&ClientFrame::Query {
            request_id: request_id.clone(),
            query,
        })
        .await?;
        Ok(request_id)
    }

    /// Starts or resumes an ordered subscription.
    ///
    /// # Errors
    /// Returns a transport or encoding error.
    pub async fn subscribe(&mut self, scope: SubscriptionScope) -> Result<RequestId, ClientError> {
        let request_id = RequestId::new(uuid::Uuid::now_v7().to_string());
        self.send(&ClientFrame::Subscribe {
            request_id: request_id.clone(),
            scope,
        })
        .await?;
        Ok(request_id)
    }

    /// Resumes a disposable projection from its last applied event cursor.
    ///
    /// # Errors
    /// Returns a transport or encoding error.
    pub async fn resume_subscription(
        &mut self,
        scope: SubscriptionScope,
        after: Cursor,
    ) -> Result<RequestId, ClientError> {
        let request_id = RequestId::new(uuid::Uuid::now_v7().to_string());
        self.send(&ClientFrame::ResumeSubscription {
            request_id: request_id.clone(),
            scope,
            after,
        })
        .await?;
        Ok(request_id)
    }

    /// Sends one protocol frame.
    ///
    /// # Errors
    /// Returns a transport or JSON-encoding error.
    pub async fn send(&mut self, frame: &ClientFrame) -> Result<(), ClientError> {
        self.framed.send(serde_json::to_string(frame)?).await?;
        Ok(())
    }

    /// Receives one protocol frame.
    ///
    /// # Errors
    /// Returns a transport, decoding, or disconnect error.
    pub async fn receive(&mut self) -> Result<ServerFrame, ClientError> {
        let line = self
            .framed
            .next()
            .await
            .ok_or(ClientError::Disconnected)??;
        Ok(serde_json::from_str(&line)?)
    }
}

#[cfg(unix)]
impl NakodeClient<tokio::net::UnixStream> {
    /// Connects to a local Nakode server socket.
    ///
    /// # Errors
    /// Returns a socket error when the server is unavailable.
    pub async fn connect_local(
        path: impl AsRef<std::path::Path>,
        client_id: ClientId,
    ) -> Result<Self, ClientError> {
        let stream = tokio::net::UnixStream::connect(path).await?;
        Ok(Self::from_stream(stream, client_id))
    }
}

#[cfg(test)]
mod tests {
    use nakode_protocol::{
        ClientDescriptor, ClientId, PROTOCOL_VERSION, ServerEpoch, ServerFrame, ServiceCapabilities,
    };
    use tokio_util::codec::{Framed, LinesCodec};

    use super::{MAX_FRAME_BYTES, NakodeClient};
    use futures_util::{SinkExt, StreamExt};

    #[tokio::test]
    async fn standalone_frontend_negotiates_without_server_types() {
        let (client_stream, server_stream) = tokio::io::duplex(16 * 1024);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                server_stream,
                LinesCodec::new_with_max_length(MAX_FRAME_BYTES),
            );
            let line = framed
                .next()
                .await
                .expect("hello")
                .expect("valid hello frame");
            let _: nakode_protocol::ClientFrame =
                serde_json::from_str(&line).expect("decode hello");
            let response = ServerFrame::Welcome {
                version: PROTOCOL_VERSION,
                server_epoch: ServerEpoch::from("epoch-1"),
                server_version: "test".to_owned(),
                capabilities: ServiceCapabilities::default(),
            };
            framed
                .send(serde_json::to_string(&response).expect("encode welcome"))
                .await
                .expect("send welcome");
        });

        let mut client = NakodeClient::from_stream(client_stream, ClientId::from("plain-client"));
        let response = client
            .hello(ClientDescriptor {
                name: "Plain frontend".to_owned(),
                version: "1".to_owned(),
                frontend: "text".to_owned(),
            })
            .await
            .expect("handshake");
        assert!(matches!(
            response,
            ServerFrame::Welcome {
                version: PROTOCOL_VERSION,
                ..
            }
        ));
        server.await.expect("server task");
    }
}
