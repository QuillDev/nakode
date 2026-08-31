//! High-level, renderer-facing Nakode SDK.
//!
//! Frontends use this crate instead of generated transport stubs. It creates
//! idempotency keys, reconnects the transport, resumes authoritative watches,
//! and exposes product-level methods returning render-ready API state.

use std::{
    collections::{HashMap, HashSet},
    fmt::Write as _,
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::Duration,
};

use futures_util::{Stream, StreamExt};
use nakode_api::v1::{
    self as api, activation_service_client::ActivationServiceClient,
    nakode_service_client::NakodeServiceClient,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    metadata::AsciiMetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tower::service_fn;

pub use nakode_api::v1;

const RETRY_DELAY: Duration = Duration::from_millis(100);
const WATCH_BUFFER: usize = 32;

#[derive(Debug, Error)]
pub enum SdkError {
    #[error("invalid Nakode API endpoint: {0}")]
    InvalidEndpoint(#[from] tonic::transport::Error),
    #[error("Nakode API request failed: {0}")]
    Status(#[from] tonic::Status),
    #[error("Nakode API returned no {0} state")]
    MissingState(&'static str),
    #[error("invalid Nakode API projection: {0}")]
    InvalidProjection(String),
}

pub type Watch<T> = Pin<Box<dyn Stream<Item = Result<T, SdkError>> + Send + 'static>>;

/// Receiver-backed watch whose reconnecting producer is cancelled with the consumer. Without this
/// ownership edge, dropping a frontend watch could leave its gRPC stream and reconnect loop alive
/// until the server happened to emit another item.
struct ManagedWatch<T> {
    receiver: ReceiverStream<Result<T, SdkError>>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> Unpin for ManagedWatch<T> {}

impl<T> Stream for ManagedWatch<T> {
    type Item = Result<T, SdkError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(context)
    }
}

impl<T> Drop for ManagedWatch<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn managed_watch<T: Send + 'static>(
    receiver: tokio::sync::mpsc::Receiver<Result<T, SdkError>>,
    task: tokio::task::JoinHandle<()>,
) -> Watch<T> {
    Box::pin(ManagedWatch {
        receiver: ReceiverStream::new(receiver),
        task,
    })
}

/// One logical-session catalogue plus whether it is safe to treat omission as authoritative.
#[derive(Clone, Debug)]
pub struct LogicalSessionInventory {
    pub sessions: Vec<api::SessionSummary>,
    pub complete: bool,
}

/// A fully materialized session suitable for direct rendering.
#[derive(Clone, Debug)]
pub struct HydratedSession {
    pub state: api::SessionState,
    pub artifacts: HashMap<String, api::Artifact>,
}

/// Client-owned attachment inputs that must be re-established before a persisted logical session is
/// accepted after a service generation changes.
#[derive(Clone, Debug, Default)]
pub struct SessionAttachment {
    pub tools: Option<api::SessionToolConfiguration>,
    pub mcp_grant: Option<api::McpSessionGrant>,
    pub profile_id: Option<String>,
    /// Optional account affinity used when restoring a legacy/unbound session.
    pub account_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct ClientApiKey {
    authorization: Option<AsciiMetadataValue>,
}

impl Interceptor for ClientApiKey {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(value) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

type ApiTransport = InterceptedService<Channel, ClientApiKey>;

/// Cloneable high-level client. A clone shares the reconnecting HTTP/2
/// channel but each request and watch has independent generated client state.
#[derive(Clone)]
pub struct NakodeClient {
    transport: NakodeServiceClient<ApiTransport>,
}

/// Attempt-qualified activation watch cursor. Revisions are monotonic only within one activation
/// attempt, so clients must retain both fields when a later update may begin at a lower revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivationCursor {
    pub attempt_id: String,
    pub revision: u64,
}

/// Cloneable client for installation-scoped update activation status and mutations.
#[derive(Clone)]
pub struct ActivationClient {
    transport: ActivationServiceClient<Channel>,
}

impl ActivationClient {
    /// Connects to the activation endpoint selected by `nakode endpoint`. While an older service
    /// owns work this is the helper socket; after cutover it is the ordinary service API socket.
    ///
    /// # Errors
    /// Returns when the endpoint is invalid or cannot be reached.
    #[cfg(unix)]
    pub async fn connect_unix(path: impl Into<PathBuf>) -> Result<Self, SdkError> {
        let path = path.into();
        let channel = Endpoint::from_static("http://nakode.local")
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map(hyper_util::rt::TokioIo::new)
                }
            }))
            .await?;
        Ok(Self::from_channel(channel))
    }

    #[cfg(not(unix))]
    pub async fn connect_unix(_path: impl Into<PathBuf>) -> Result<Self, SdkError> {
        Err(SdkError::InvalidEndpoint(
            tonic::transport::Error::from_source(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix sockets are unavailable",
            )),
        ))
    }

    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            transport: configured_activation_transport(channel),
        }
    }

    /// Returns the durable authoritative activation snapshot.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_status(&self) -> Result<api::ActivationStatus, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_activation_status(api::GetActivationStatusRequest {})
            .await?
            .into_inner())
    }

    /// Watches complete replacement activation snapshots from one endpoint generation.
    /// Callers rediscover the endpoint after the terminal `activated` snapshot or an unavailable
    /// transport, because helper-to-service handoff intentionally changes the socket path.
    ///
    /// # Errors
    /// Returns when the initial watch cannot be established.
    pub async fn watch_status(
        &self,
        after_revision: Option<u64>,
    ) -> Result<Watch<api::ActivationStatus>, SdkError> {
        self.watch_status_after(after_revision.map(|revision| ActivationCursor {
            attempt_id: String::new(),
            revision,
        }))
        .await
    }

    /// Watches one endpoint generation after an attempt-qualified cursor. Prefer this over
    /// [`Self::watch_status`] when the caller has already observed a complete activation snapshot.
    ///
    /// # Errors
    /// Returns when the initial watch cannot be established.
    pub async fn watch_status_after(
        &self,
        after: Option<ActivationCursor>,
    ) -> Result<Watch<api::ActivationStatus>, SdkError> {
        let request = match after {
            Some(cursor) => api::WatchActivationStatusRequest {
                after_revision: Some(cursor.revision),
                after_attempt_id: cursor.attempt_id,
            },
            None => api::WatchActivationStatusRequest {
                after_revision: None,
                after_attempt_id: String::new(),
            },
        };
        let stream = self
            .transport
            .clone()
            .watch_activation_status(request)
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|result| result.map_err(Into::into))))
    }

    /// Watches activation status across helper-to-service endpoint handoff. The resolver must perform
    /// fresh installation-scoped endpoint discovery on every call. A unary snapshot is reconciled
    /// before each stream so a new attempt with a lower numeric revision is never hidden.
    pub fn watch_status_with_rediscovery<Discover, Discovery>(
        discover: Discover,
        after: Option<ActivationCursor>,
    ) -> Watch<api::ActivationStatus>
    where
        Discover: Fn() -> Discovery + Send + Sync + 'static,
        Discovery: Future<Output = Result<Self, SdkError>> + Send + 'static,
    {
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            let mut cursor = after;
            let mut reconnect_reported = false;
            loop {
                let client = match discover().await {
                    Ok(client) => client,
                    Err(error) => {
                        if !report_sdk_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                };
                let status = match client.get_status().await {
                    Ok(status) => status,
                    Err(error) => {
                        if !report_sdk_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                };
                reconnect_reported = false;
                if activation_cursor_changed(cursor.as_ref(), &status) {
                    cursor = Some(activation_cursor(&status));
                    if sender.send(Ok(status.clone())).await.is_err() {
                        return;
                    }
                }
                let mut stream = match client
                    .watch_status_after(Some(activation_cursor(&status)))
                    .await
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        if !report_sdk_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                };
                while let Some(update) = stream.next().await {
                    match update {
                        Ok(status) => {
                            reconnect_reported = false;
                            if activation_cursor_changed(cursor.as_ref(), &status) {
                                cursor = Some(activation_cursor(&status));
                                if sender.send(Ok(status)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            if !report_sdk_watch_error(&sender, &mut reconnect_reported, error)
                                .await
                            {
                                return;
                            }
                            break;
                        }
                    }
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        managed_watch(receiver, task)
    }

    /// Requests an immediate safe quiescence check. The SDK supplies an idempotency key when the
    /// caller does not need to replay a prior logical request.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn recheck(
        &self,
        idempotency_key: Option<String>,
    ) -> Result<api::ActivationStatus, SdkError> {
        Ok(self
            .transport
            .clone()
            .force_activation_recheck(api::ActivationMutationRequest {
                idempotency_key: idempotency_key
                    .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
            })
            .await?
            .into_inner())
    }

    /// Destructively activates only if the running service atomically observes the exact confirmed
    /// blocker identity/revision set, activation attempt, and activation revision.
    ///
    /// # Errors
    /// Returns a transport or capability/fence status error.
    pub async fn force_activate(
        &self,
        expected_attempt_id: String,
        expected_activation_revision: u64,
        expected_blockers: Vec<api::ActivationBlocker>,
        idempotency_key: Option<String>,
    ) -> Result<api::ActivationStatus, SdkError> {
        Ok(self
            .transport
            .clone()
            .force_activate(api::ForceActivateRequest {
                idempotency_key: idempotency_key
                    .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                expected_activation_revision,
                expected_blockers,
                expected_attempt_id,
            })
            .await?
            .into_inner())
    }
}

macro_rules! typed_mutation {
    ($method:ident, $request:ty) => {
        /// Executes this typed product mutation with an SDK-owned idempotency key.
        /// A caller-supplied key is preserved; callers repeating a completed SDK invocation must
        /// supply the same key themselves. Automatic transport retries within one invocation reuse
        /// the exact request.
        ///
        /// # Errors
        /// Returns a transport or server status error.
        pub async fn $method(
            &self,
            mut request: $request,
        ) -> Result<api::MutationResult, SdkError> {
            if request.mutation.is_none() {
                request.mutation = Some(mutation(None));
            }
            retry_transport(request, |request| {
                let mut transport = self.transport.clone();
                async move { transport.$method(request).await }
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
        }
    };
}

macro_rules! send_mutation {
    ($client:expr, $method:ident, $request:expr) => {{
        let request = $request;
        retry_transport(request, |request| {
            let mut transport = $client.transport.clone();
            async move { transport.$method(request).await }
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(SdkError::from)
    }};
}

impl NakodeClient {
    /// Connects to the native server's generated API over its private Unix
    /// socket. The channel reconnects by reopening this path.
    ///
    /// # Errors
    /// Returns when the endpoint is invalid or cannot be reached.
    #[cfg(unix)]
    pub async fn connect_unix(path: impl Into<PathBuf>) -> Result<Self, SdkError> {
        let path = path.into();
        let channel = Endpoint::from_static("http://nakode.local")
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map(hyper_util::rt::TokioIo::new)
                }
            }))
            .await?;
        Ok(Self {
            transport: configured_transport(channel, ClientApiKey::default()),
        })
    }

    /// Connects to an authenticated TLS Nakode endpoint.
    ///
    /// # Errors
    /// Returns when TLS setup, endpoint connection, or API-key metadata is invalid.
    pub async fn connect_remote(
        endpoint: impl AsRef<str>,
        ca_certificate_pem: impl AsRef<[u8]>,
        tls_server_name: impl Into<String>,
        api_key: impl AsRef<str>,
    ) -> Result<Self, SdkError> {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_certificate_pem))
            .domain_name(tls_server_name.into());
        let channel = Endpoint::from_shared(endpoint.as_ref().to_owned())?
            .tls_config(tls)?
            .connect()
            .await?;
        let authorization = format!("Bearer {}", api_key.as_ref())
            .parse()
            .map_err(|_| SdkError::InvalidProjection("invalid remote API key".to_owned()))?;
        Ok(Self {
            transport: configured_transport(
                channel,
                ClientApiKey {
                    authorization: Some(authorization),
                },
            ),
        })
    }

    #[cfg(not(unix))]
    pub async fn connect_unix(_path: impl Into<PathBuf>) -> Result<Self, SdkError> {
        Err(SdkError::InvalidEndpoint(
            tonic::transport::Error::from_source(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix sockets are unavailable",
            )),
        ))
    }

    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            transport: configured_transport(channel, ClientApiKey::default()),
        }
    }

    /// Validates and describes one executable path on the server machine without creating a session.
    ///
    /// # Errors
    /// Returns a transport or server validation error.
    pub async fn inspect_workspace_path(
        &self,
        path: impl Into<String>,
        expected_git_repository: Option<String>,
    ) -> Result<api::WorkspacePathInspection, SdkError> {
        Ok(self
            .transport
            .clone()
            .inspect_workspace_path(api::InspectWorkspacePathRequest {
                path: path.into(),
                expected_git_repository,
            })
            .await?
            .into_inner())
    }

    /// Returns authoritative workspace state.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-state error.
    pub async fn get_workspace(
        &self,
        workspace: impl Into<String>,
        session_id: Option<String>,
    ) -> Result<api::WorkspaceState, SdkError> {
        let response = self
            .transport
            .clone()
            .get_workspace(api::GetWorkspaceRequest {
                workspace: workspace.into(),
                session_id,
            })
            .await?
            .into_inner();
        response.state.ok_or(SdkError::MissingState("workspace"))
    }

    /// Returns redacted account metadata for one provider from the authoritative workspace snapshot.
    /// No credential payload is present in this projection.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_provider_accounts(
        &self,
        workspace: impl Into<String>,
        provider_id: impl Into<String>,
    ) -> Result<Vec<api::ProviderAccount>, SdkError> {
        let provider_id = provider_id.into();
        let state = self.get_workspace(workspace, None).await?;
        Ok(state
            .providers
            .into_iter()
            .find(|provider| provider.id == provider_id)
            .map(|provider| provider.accounts)
            .unwrap_or_default())
    }

    /// Resolves the logical session a frontend should render. The workspace is a session access
    /// root, not a service selector: the installation authority opens the requested session, reuses
    /// the most recent session rooted there, or creates one with that working directory.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-state error.
    pub async fn open_workspace_session(
        &self,
        workspace: impl Into<String>,
        requested_session: Option<String>,
    ) -> Result<(api::WorkspaceState, String), SdkError> {
        let requested_workspace = workspace.into();
        let canonical_workspace = std::fs::canonicalize(&requested_workspace)
            .unwrap_or_else(|_| std::path::PathBuf::from(&requested_workspace))
            .to_string_lossy()
            .into_owned();
        let state = self
            .get_workspace(canonical_workspace.clone(), None)
            .await?;
        let session_id = if let Some(session_id) = requested_session {
            self.open_session(session_id).await?
        } else if let Some(session) = state
            .sessions
            .iter()
            .find(|session| session.working_directory == canonical_workspace)
        {
            self.open_session(session.id.clone()).await?
        } else {
            self.create_session_in_directory(state.workspace_id.clone(), None, canonical_workspace)
                .await?
        };
        Ok((state, session_id))
    }

    /// Creates a logical session and returns its server-assigned identifier.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-identifier error.
    pub async fn create_session(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
    ) -> Result<String, SdkError> {
        self.create_session_with_configuration(workspace_id, title, None, None, None)
            .await
    }

    /// Creates a logical session with an explicit provider-account affinity. The account is
    /// validated by the server; omission is the only mode that permits automatic routing.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_with_account(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        account_id: impl Into<String>,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            create_session,
            api::CreateSessionRequest {
                mutation: Some(mutation(None)),
                workspace_id: workspace_id.into(),
                title,
                model_id: None,
                options: None,
                tools: None,
                mcp_grant: None,
                initial_instructions: None,
                bridge: None,
                working_directory: None,
                profile_id: None,
                account_id: Some(account_id.into()),
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// The logical workspace remains the owner and service partition.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_in_directory(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        working_directory: impl Into<String>,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            create_session,
            api::CreateSessionRequest {
                mutation: Some(mutation(None)),
                workspace_id: workspace_id.into(),
                title,
                model_id: None,
                options: None,
                tools: None,
                initial_instructions: None,
                mcp_grant: None,
                bridge: None,
                working_directory: Some(working_directory.into()),
                profile_id: None,
                account_id: None,
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// Creates a logical session with an atomic external-thread bridge intent.
    ///
    /// Frontends use this when the session must never be published without its Chat/Agent
    /// classification. External transport credentials and thread identities are deliberately not
    /// accepted here.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_with_bridge(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        bridge: api::SessionBridgeIntent,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            create_session,
            api::CreateSessionRequest {
                mutation: Some(mutation(None)),
                workspace_id: workspace_id.into(),
                title,
                model_id: None,
                options: None,
                tools: None,
                initial_instructions: None,
                mcp_grant: None,
                bridge: Some(bridge),
                working_directory: None,
                profile_id: None,
                account_id: None,
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// Creates a logical session with an optional provider-qualified initial model and its options.
    /// The selection is validated and committed atomically with creation, before a first prompt can
    /// run. Omitting it inherits Nakode's configured provider/model defaults.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_with_model(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        model_id: Option<String>,
        options: Option<api::ModelOptions>,
    ) -> Result<String, SdkError> {
        self.create_session_with_configuration(workspace_id, title, model_id, options, None)
            .await
    }

    /// Creates a logical session with model/options and client-owned tools committed atomically.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_with_configuration(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        model_id: Option<String>,
        options: Option<api::ModelOptions>,
        tools: Option<api::SessionToolConfiguration>,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            create_session,
            api::CreateSessionRequest {
                mutation: Some(mutation(None)),
                workspace_id: workspace_id.into(),
                title,
                model_id,
                options,
                tools,
                initial_instructions: None,
                mcp_grant: None,
                bridge: None,
                working_directory: None,
                profile_id: None,
                account_id: None,
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// Creates a logical session with model/options, client-owned tools, and optional provider
    /// system instructions committed atomically.
    ///
    /// # Errors
    /// Returns a transport, server validation, or missing-identifier error.
    pub async fn create_session_with_initial_instructions(
        &self,
        workspace_id: impl Into<String>,
        title: Option<String>,
        model_id: Option<String>,
        options: Option<api::ModelOptions>,
        tools: Option<api::SessionToolConfiguration>,
        initial_instructions: Option<String>,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            create_session,
            api::CreateSessionRequest {
                mutation: Some(mutation(None)),
                workspace_id: workspace_id.into(),
                title,
                model_id,
                options,
                tools,
                mcp_grant: None,
                initial_instructions,
                bridge: None,
                working_directory: None,
                profile_id: None,
                account_id: None,
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// Opens or reattaches to a logical session with an explicit account affinity.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session_with_account(
        &self,
        session_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Result<String, SdkError> {
        self.open_session_with_attachment(
            session_id,
            SessionAttachment {
                account_id: Some(account_id.into()),
                ..SessionAttachment::default()
            },
        )
        .await
    }

    /// Opens a persisted logical session in the server and returns its full ID.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session(&self, session_id: impl Into<String>) -> Result<String, SdkError> {
        self.open_session_with_tools_for_profile(session_id, None, None)
            .await
    }

    /// Opens or reattaches to a logical session with its client-owned tools established first.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session_with_tools(
        &self,
        session_id: impl Into<String>,
        tools: Option<api::SessionToolConfiguration>,
    ) -> Result<String, SdkError> {
        self.open_session_with_tools_for_profile(session_id, tools, None)
            .await
    }

    /// Opens or reattaches to a logical session under a current client skill profile.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session_with_tools_for_profile(
        &self,
        session_id: impl Into<String>,
        tools: Option<api::SessionToolConfiguration>,
        profile_id: Option<String>,
    ) -> Result<String, SdkError> {
        self.open_session_with_attachment(
            session_id,
            SessionAttachment {
                tools,
                mcp_grant: None,
                profile_id,
                account_id: None,
            },
        )
        .await
    }

    /// Opens or reattaches to a logical session after atomically restoring every client-owned
    /// attachment input. Callers should retain this value for generation-changing reconnects.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session_with_attachment(
        &self,
        session_id: impl Into<String>,
        attachment: SessionAttachment,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            open_session,
            api::OpenSessionRequest {
                mutation: Some(mutation(None)),
                session_id: session_id.into(),
                tools: attachment.tools,
                mcp_grant: attachment.mcp_grant,
                profile_id: attachment.profile_id,
                account_id: attachment.account_id,
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("opened session identifier"))
    }

    /// Lists logical sessions for a workspace.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_sessions(
        &self,
        workspace_id: impl Into<String>,
        limit: u32,
    ) -> Result<Vec<api::SessionSummary>, SdkError> {
        Ok(self
            .list_session_inventory(workspace_id, limit)
            .await?
            .sessions)
    }

    /// Lists logical sessions and preserves the server's explicit completeness boundary.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_session_inventory(
        &self,
        workspace_id: impl Into<String>,
        limit: u32,
    ) -> Result<LogicalSessionInventory, SdkError> {
        let response = self
            .transport
            .clone()
            .list_sessions(api::ListSessionsRequest {
                workspace_id: workspace_id.into(),
                limit,
            })
            .await?
            .into_inner();
        Ok(LogicalSessionInventory {
            sessions: response.sessions,
            complete: response.complete,
        })
    }

    /// Returns authoritative state for one session.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-state error.
    pub async fn get_session(
        &self,
        session_id: impl Into<String>,
    ) -> Result<api::SessionState, SdkError> {
        let response = self
            .transport
            .clone()
            .get_session(api::GetSessionRequest {
                session_id: session_id.into(),
            })
            .await?
            .into_inner();
        response.state.ok_or(SdkError::MissingState("session"))
    }

    /// Returns a bounded, fully materialized session projection. Paging, body
    /// reconstruction, run history, and artifact transfer are SDK concerns.
    ///
    /// # Errors
    /// Returns a transport, server status, or inconsistent-projection error.
    pub async fn get_hydrated_session(
        &self,
        session_id: impl Into<String>,
        limit: usize,
    ) -> Result<HydratedSession, SdkError> {
        let state = self.get_session(session_id).await?;
        self.hydrate_session_with_refresh(state, limit).await
    }

    /// Sends work using the server-owned start-versus-queue policy.
    ///
    /// `SendPrompt` is append-only owner intent. The server evaluates session lifecycle at execution
    /// time and does not use `expected_revision` as a fence; the argument remains for source and wire
    /// compatibility with clients that already supplied an observed revision. The SDK-generated
    /// idempotency key is stable across automatic transport retries of this invocation.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn send_prompt(
        &self,
        session_id: impl Into<String>,
        prompt: api::PromptInput,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            send_prompt,
            api::SendPromptRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                prompt: Some(prompt),
            }
        )
    }

    /// Adds work to the server-owned session queue.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn enqueue_prompt(
        &self,
        session_id: impl Into<String>,
        prompt: api::PromptInput,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            enqueue_prompt,
            api::EnqueuePromptRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                prompt: Some(prompt),
            }
        )
    }

    /// Atomically redirects active work to one queued prompt.
    ///
    /// Steering-capable providers accept it in the current turn. Interruption-only providers stop
    /// that turn and start the selected prompt next.
    ///
    /// # Errors
    /// Returns a transport or server status error. A rejected conversion leaves the prompt queued.
    pub async fn steer_queued_prompt(
        &self,
        session_id: impl Into<String>,
        prompt_id: impl Into<String>,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            steer_queued_prompt,
            api::SteerQueuedPromptRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                prompt_id: prompt_id.into(),
            }
        )
    }

    /// Steers the active native turn.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn steer_turn(
        &self,
        turn_id: impl Into<String>,
        text: impl Into<String>,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            steer_turn,
            api::SteerTurnRequest {
                mutation: Some(mutation(expected_revision)),
                turn_id: turn_id.into(),
                text: text.into(),
            }
        )
    }

    /// Cancels server-owned work current for a logical session when the command executes.
    ///
    /// Omitting `expected_revision` makes this a priority lifecycle operation: newer provider or
    /// delegated-run progress does not invalidate it, and a successor turn that became current before
    /// execution is included in the cancellation scope.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn cancel_session_work(
        &self,
        session_id: impl Into<String>,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            cancel_session_work,
            api::CancelSessionWorkRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
            }
        )
    }

    /// Resolves a pending approval or question.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn resolve_interaction(
        &self,
        interaction_id: impl Into<String>,
        resolution: api::InteractionResolutionKind,
        option_ids: Vec<String>,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            resolve_interaction,
            api::ResolveInteractionRequest {
                mutation: Some(mutation(expected_revision)),
                interaction_id: interaction_id.into(),
                resolution: resolution as i32,
                option_ids,
                answers: Vec::new(),
            }
        )
    }

    /// Installs a client-owned tool surface before the session's first prompt.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn configure_session_tools(
        &self,
        session_id: impl Into<String>,
        tools: Vec<api::ExternalToolDefinition>,
        replace_builtin_tools: bool,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            configure_session_tools,
            api::ConfigureSessionToolsRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                tools,
                replace_builtin_tools,
            }
        )
    }

    /// Changes one idle session's model-facing tool surface for its next turn.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn set_session_code_mode(
        &self,
        session_id: impl Into<String>,
        enabled: bool,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            set_session_code_mode,
            api::SetSessionCodeModeRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                enabled,
            }
        )
    }

    /// Resolves one server-owned external tool request.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn submit_external_tool_result(
        &self,
        session_id: impl Into<String>,
        call_id: impl Into<String>,
        output: impl Into<String>,
        failed: bool,
        expected_revision: Option<u64>,
    ) -> Result<api::MutationResult, SdkError> {
        send_mutation!(
            self,
            submit_external_tool_result,
            api::SubmitExternalToolResultRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                call_id: call_id.into(),
                output: output.into(),
                failed,
            }
        )
    }

    /// Starts a bounded delegated run and returns its identifier.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-identifier error.
    pub async fn delegate(
        &self,
        session_id: impl Into<String>,
        agent_slug: impl Into<String>,
        task: impl Into<String>,
        expected_revision: Option<u64>,
    ) -> Result<String, SdkError> {
        self.delegate_attributed(
            session_id,
            agent_slug,
            task,
            None::<String>,
            expected_revision,
        )
        .await
    }

    /// Starts a delegated run attributed to another run in the same logical session.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-identifier error.
    pub async fn delegate_attributed(
        &self,
        session_id: impl Into<String>,
        agent_slug: impl Into<String>,
        task: impl Into<String>,
        parent_run_id: Option<impl Into<String>>,
        expected_revision: Option<u64>,
    ) -> Result<String, SdkError> {
        let result = send_mutation!(
            self,
            delegate,
            api::DelegateRequest {
                mutation: Some(mutation(expected_revision)),
                session_id: session_id.into(),
                agent_slug: agent_slug.into(),
                task: task.into(),
                parent_run_id: parent_run_id.map(Into::into),
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("delegated run identifier"))
    }

    /// Returns authoritative state for one orchestration run.
    ///
    /// # Errors
    /// Returns a transport, server status, or missing-state error.
    pub async fn get_run(&self, run_id: impl Into<String>) -> Result<api::RunState, SdkError> {
        let response = self
            .transport
            .clone()
            .get_run(api::GetRunRequest {
                run_id: run_id.into(),
            })
            .await?
            .into_inner();
        response.state.ok_or(SdkError::MissingState("run"))
    }

    /// Fetches one server-owned artifact.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_artifact(
        &self,
        artifact_id: impl Into<String>,
    ) -> Result<api::Artifact, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_artifact(api::GetArtifactRequest {
                artifact_id: artifact_id.into(),
            })
            .await?
            .into_inner())
    }

    /// Reads Nakode's single configured Soul.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_soul(
        &self,
        request: api::GetSoulRequest,
    ) -> Result<api::SoulDocument, SdkError> {
        Ok(self.transport.clone().get_soul(request).await?.into_inner())
    }

    typed_mutation!(reload_workspace, api::ReloadWorkspaceRequest);
    typed_mutation!(
        set_session_bridge_lifecycle,
        api::SetSessionBridgeLifecycleRequest
    );
    typed_mutation!(
        set_workspace_bridge_lifecycle,
        api::SetWorkspaceBridgeLifecycleRequest
    );
    typed_mutation!(
        bind_session_bridge_thread,
        api::BindSessionBridgeThreadRequest
    );
    typed_mutation!(
        clear_session_bridge_thread,
        api::ClearSessionBridgeThreadRequest
    );
    typed_mutation!(prepare_bridge_delivery, api::PrepareBridgeDeliveryRequest);
    typed_mutation!(
        complete_bridge_delivery_part,
        api::CompleteBridgeDeliveryPartRequest
    );
    typed_mutation!(finalize_bridge_delivery, api::FinalizeBridgeDeliveryRequest);
    typed_mutation!(set_bridge_live_message, api::SetBridgeLiveMessageRequest);
    /// Atomically attempts an idle-only continuation from an authoritative external thread binding.
    /// Busy and duplicate events are successful typed dispositions rather than queued work.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn continue_session_from_bridge(
        &self,
        mut request: api::ContinueSessionFromBridgeRequest,
    ) -> Result<api::ContinueSessionFromBridgeResponse, SdkError> {
        if request.mutation.is_none() {
            request.mutation = Some(bridge_continuation_mutation(&request));
        }
        retry_transport(request, |request| {
            let mut transport = self.transport.clone();
            async move { transport.continue_session_from_bridge(request).await }
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(Into::into)
    }
    typed_mutation!(save_soul, api::SaveSoulRequest);
    /// Removes one exact queued prompt against authoritative session state.
    ///
    /// A caller-supplied idempotency key is preserved across transport retries. Any supplied
    /// `expected_revision` is omitted because queue progress, activation, and concurrent appends do
    /// not invalidate removal of a stable prompt identity. A target already absent is success.
    ///
    /// # Errors
    /// Returns a transport or server status error, or a domain refusal while the target is reserved
    /// for redirection.
    pub async fn remove_queued_prompt(
        &self,
        request: api::RemoveQueuedPromptRequest,
    ) -> Result<api::MutationResult, SdkError> {
        let request = authoritative_remove_request(request);
        retry_transport(request, |request| {
            let mut transport = self.transport.clone();
            async move { transport.remove_queued_prompt(request).await }
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(Into::into)
    }
    typed_mutation!(cancel_turn, api::CancelTurnRequest);
    typed_mutation!(compact_context, api::CompactContextRequest);
    typed_mutation!(run_shell, api::RunShellRequest);
    typed_mutation!(select_model, api::SelectModelRequest);
    typed_mutation!(
        set_provider_model_filter,
        api::SetProviderModelFilterRequest
    );
    typed_mutation!(set_skill_enabled, api::SetSkillEnabledRequest);
    typed_mutation!(prune_skill, api::PruneSkillRequest);
    typed_mutation!(set_provider_enabled, api::SetProviderEnabledRequest);
    typed_mutation!(
        begin_provider_authentication,
        api::BeginProviderAuthenticationRequest
    );
    typed_mutation!(set_provider_credential, api::SetProviderCredentialRequest);
    typed_mutation!(
        clear_provider_credential,
        api::ClearProviderCredentialRequest
    );
    typed_mutation!(reload_provider, api::ReloadProviderRequest);
    typed_mutation!(add_provider_account, api::AddProviderAccountRequest);
    typed_mutation!(
        begin_provider_account_authentication,
        api::BeginProviderAccountAuthenticationRequest
    );
    typed_mutation!(
        set_provider_account_credential,
        api::SetProviderAccountCredentialRequest
    );
    typed_mutation!(
        clear_provider_account_credential,
        api::ClearProviderAccountCredentialRequest
    );
    typed_mutation!(reload_provider_account, api::ReloadProviderAccountRequest);
    typed_mutation!(
        set_provider_account_label,
        api::SetProviderAccountLabelRequest
    );
    typed_mutation!(
        set_provider_account_enabled,
        api::SetProviderAccountEnabledRequest
    );
    typed_mutation!(
        set_provider_account_default,
        api::SetProviderAccountDefaultRequest
    );
    typed_mutation!(remove_provider_account, api::RemoveProviderAccountRequest);
    typed_mutation!(save_mcp_server, api::SaveMcpServerRequest);
    typed_mutation!(delete_mcp_server, api::DeleteMcpServerRequest);
    typed_mutation!(set_mcp_server_enabled, api::SetMcpServerEnabledRequest);
    typed_mutation!(refresh_mcp_server, api::RefreshMcpServerRequest);
    typed_mutation!(
        set_mcp_server_credential,
        api::SetMcpServerCredentialRequest
    );
    typed_mutation!(
        clear_mcp_server_credential,
        api::ClearMcpServerCredentialRequest
    );
    typed_mutation!(set_mcp_server_grants, api::SetMcpServerGrantsRequest);
    typed_mutation!(save_agent, api::SaveAgentRequest);
    typed_mutation!(delete_agent, api::DeleteAgentRequest);
    typed_mutation!(delete_session, api::DeleteSessionRequest);
    typed_mutation!(update_settings, api::UpdateSettingsRequest);
    typed_mutation!(check_agent_browser, api::CheckAgentBrowserRequest);
    typed_mutation!(cancel_run, api::CancelRunRequest);
    typed_mutation!(continue_run, api::ContinueRunRequest);

    /// Lists a server-paged run window.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_runs(
        &self,
        request: api::ListRunsRequest,
    ) -> Result<api::ListRunsResponse, SdkError> {
        Ok(self
            .transport
            .clone()
            .list_runs(request)
            .await?
            .into_inner())
    }

    /// Fetches a typed transcript page.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_transcript_page(
        &self,
        request: api::GetTranscriptPageRequest,
    ) -> Result<api::TranscriptPage, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_transcript_page(request)
            .await?
            .into_inner())
    }

    /// Fetches a bounded transcript body window.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_transcript_body_window(
        &self,
        request: api::GetTranscriptBodyWindowRequest,
    ) -> Result<api::TranscriptBodyWindow, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_transcript_body_window(request)
            .await?
            .into_inner())
    }

    /// Fetches a bounded orchestration text window.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_run_text_window(
        &self,
        request: api::GetRunTextWindowRequest,
    ) -> Result<api::RunTextWindow, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_run_text_window(request)
            .await?
            .into_inner())
    }

    /// Returns privacy-preserving server diagnostics.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_diagnostics(
        &self,
        request: api::GetDiagnosticsRequest,
    ) -> Result<api::DiagnosticsReport, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_diagnostics(request)
            .await?
            .into_inner())
    }

    /// Returns local invocation consent and aggregate usage, including current zero-use items.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_invocation_summary(&self) -> Result<api::InvocationSummary, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_invocation_summary(api::GetInvocationSummaryRequest {})
            .await?
            .into_inner())
    }

    /// Returns bounded server-bucketed invocation history.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_invocation_timeline(
        &self,
        request: api::GetInvocationTimelineRequest,
    ) -> Result<api::InvocationTimeline, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_invocation_timeline(request)
            .await?
            .into_inner())
    }

    /// Returns the full profile-scoped manageable skill catalogue, including unavailable rows.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_skills(
        &self,
        workspace_id: impl Into<String>,
        profile_id: impl Into<String>,
    ) -> Result<api::SkillCatalogue, SdkError> {
        self.list_skills_with_refresh(workspace_id, profile_id, false)
            .await
    }

    /// Returns the full profile catalogue after optionally rediscovering installed skills.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn list_skills_with_refresh(
        &self,
        workspace_id: impl Into<String>,
        profile_id: impl Into<String>,
        refresh: bool,
    ) -> Result<api::SkillCatalogue, SdkError> {
        Ok(self
            .transport
            .clone()
            .list_skills(api::ListSkillsRequest {
                workspace_id: workspace_id.into(),
                profile_id: profile_id.into(),
                refresh,
            })
            .await?
            .into_inner())
    }

    /// Returns Nakode's redacted, workspace-scoped MCP management snapshot.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_mcp_management(
        &self,
        workspace_id: impl Into<String>,
    ) -> Result<api::McpManagement, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_mcp_management(api::GetMcpManagementRequest {
                workspace_id: workspace_id.into(),
            })
            .await?
            .into_inner())
    }

    /// Returns API version and capability metadata.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_server_info(&self) -> Result<api::ServerInfo, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_server_info(())
            .await?
            .into_inner())
    }

    pub fn watch_workspace(&self, workspace_id: impl Into<String>) -> Watch<api::WorkspaceState> {
        let client = self.clone();
        let workspace_id = workspace_id.into();
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            let mut after = None;
            let mut reconnect_reported = false;
            loop {
                let request = api::WatchWorkspaceRequest {
                    workspace_id: workspace_id.clone(),
                    after: after.clone(),
                };
                match client.transport.clone().watch_workspace(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        while let Some(update) = stream.next().await {
                            match update {
                                Ok(snapshot) => {
                                    reconnect_reported = false;
                                    after = snapshot.cursor;
                                    let Some(state) = snapshot.state else {
                                        if sender
                                            .send(Err(SdkError::MissingState("workspace")))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                        continue;
                                    };
                                    if sender.send(Ok(state)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(error) => {
                                    if !report_watch_error(&sender, &mut reconnect_reported, error)
                                        .await
                                    {
                                        return;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        if !report_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                    }
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        managed_watch(receiver, task)
    }

    /// Watches one already-attached logical session. This reconnects transport streams but does not
    /// mutate server attachment state; use [`Self::watch_attached_session`] across service cutovers.
    pub fn watch_session(&self, session_id: impl Into<String>) -> Watch<api::SessionState> {
        self.watch_session_with_attachment(session_id.into(), None)
    }

    /// Watches one logical session and re-establishes its exact client-owned attachment if a new
    /// service generation reports the persisted ID as not yet open. Reattachment must return the
    /// same logical ID before any replacement snapshot is published.
    pub fn watch_attached_session(
        &self,
        session_id: impl Into<String>,
        attachment: SessionAttachment,
    ) -> Watch<api::SessionState> {
        self.watch_session_with_attachment(session_id.into(), Some(attachment))
    }

    fn watch_session_with_attachment(
        &self,
        session_id: String,
        attachment: Option<SessionAttachment>,
    ) -> Watch<api::SessionState> {
        let client = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            let mut after = None;
            let mut reconnect_reported = false;
            loop {
                let request = api::WatchSessionRequest {
                    session_id: session_id.clone(),
                    after: after.clone(),
                };
                match client.transport.clone().watch_session(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        while let Some(update) = stream.next().await {
                            match update {
                                Ok(snapshot) => {
                                    reconnect_reported = false;
                                    after = snapshot.cursor;
                                    let Some(state) = snapshot.state else {
                                        if sender
                                            .send(Err(SdkError::MissingState("session")))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                        continue;
                                    };
                                    if state.id != session_id {
                                        let _ = sender
                                            .send(Err(SdkError::InvalidProjection(format!(
                                                "session watch for {session_id} returned logical ID {}",
                                                state.id
                                            ))))
                                            .await;
                                        return;
                                    }
                                    if sender.send(Ok(state)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(error)
                                    if error.code() == tonic::Code::NotFound
                                        && attachment.is_some() =>
                                {
                                    if !reattach_watched_session(
                                        &client,
                                        &session_id,
                                        attachment.clone().unwrap_or_default(),
                                        &sender,
                                        &mut reconnect_reported,
                                        &mut after,
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                    break;
                                }
                                Err(error) => {
                                    if !report_watch_error(&sender, &mut reconnect_reported, error)
                                        .await
                                    {
                                        return;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) if error.code() == tonic::Code::NotFound && attachment.is_some() => {
                        if !reattach_watched_session(
                            &client,
                            &session_id,
                            attachment.clone().unwrap_or_default(),
                            &sender,
                            &mut reconnect_reported,
                            &mut after,
                        )
                        .await
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        if !report_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                    }
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        managed_watch(receiver, task)
    }

    /// Watches a fully hydrated authoritative session. Each item replaces the
    /// previous render model; clients do not reduce patches or manage pages.
    #[must_use]
    pub fn watch_hydrated_session(
        &self,
        session_id: impl Into<String>,
        limit: usize,
    ) -> Watch<HydratedSession> {
        let client = self.clone();
        let mut source = self.watch_session(session_id);
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            while let Some(update) = source.next().await {
                let hydrated = match update {
                    Ok(state) => client.hydrate_session_with_refresh(state, limit).await,
                    Err(error) => Err(error),
                };
                if sender.send(hydrated).await.is_err() {
                    return;
                }
            }
        });
        managed_watch(receiver, task)
    }

    /// Watches a fully hydrated authoritative session while restoring the retained attachment after
    /// a service-generation handoff. Each replacement snapshot keeps the same logical session ID.
    #[must_use]
    pub fn watch_attached_hydrated_session(
        &self,
        session_id: impl Into<String>,
        limit: usize,
        attachment: SessionAttachment,
    ) -> Watch<HydratedSession> {
        let client = self.clone();
        let mut source = self.watch_attached_session(session_id, attachment);
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            while let Some(update) = source.next().await {
                let hydrated = match update {
                    Ok(state) => client.hydrate_session_with_refresh(state, limit).await,
                    Err(error) => Err(error),
                };
                if sender.send(hydrated).await.is_err() {
                    return;
                }
            }
        });
        managed_watch(receiver, task)
    }

    pub fn watch_run(&self, run_id: impl Into<String>) -> Watch<api::RunState> {
        let client = self.clone();
        let run_id = run_id.into();
        let (sender, receiver) = tokio::sync::mpsc::channel(WATCH_BUFFER);
        let task = tokio::spawn(async move {
            let mut after = None;
            let mut reconnect_reported = false;
            loop {
                let request = api::WatchRunRequest {
                    run_id: run_id.clone(),
                    after: after.clone(),
                };
                match client.transport.clone().watch_run(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        while let Some(update) = stream.next().await {
                            match update {
                                Ok(snapshot) => {
                                    reconnect_reported = false;
                                    after = snapshot.cursor;
                                    let Some(state) = snapshot.state else {
                                        if sender
                                            .send(Err(SdkError::MissingState("run")))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                        continue;
                                    };
                                    if sender.send(Ok(state)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(error) => {
                                    if !report_watch_error(&sender, &mut reconnect_reported, error)
                                        .await
                                    {
                                        return;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        if !report_watch_error(&sender, &mut reconnect_reported, error).await {
                            return;
                        }
                    }
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        managed_watch(receiver, task)
    }

    async fn hydrate_session_with_refresh(
        &self,
        state: api::SessionState,
        limit: usize,
    ) -> Result<HydratedSession, SdkError> {
        let session_id = state.id.clone();
        match self.hydrate_session(state, limit).await {
            Err(SdkError::InvalidProjection(_)) => {
                let fresh = self.get_session(session_id).await?;
                self.hydrate_session(fresh, limit).await
            }
            result => result,
        }
    }

    async fn hydrate_session(
        &self,
        mut state: api::SessionState,
        limit: usize,
    ) -> Result<HydratedSession, SdkError> {
        let limit = limit.max(1);
        let session_id = state.id.clone();
        if state.runs.len() > limit {
            let remove_count = state.runs.len() - limit;
            state.runs.drain(..remove_count);
            state.runs_has_earlier = true;
        }
        let transcript = state
            .transcript
            .take()
            .ok_or(SdkError::MissingState("session transcript"))?;
        state.transcript = Some(
            self.hydrate_transcript(
                api::TranscriptOwnerKind::Session,
                &session_id,
                transcript,
                limit,
            )
            .await?,
        );

        while state.runs_has_earlier && state.runs.len() < limit {
            let Some(before_run_id) = state.runs.first().map(|run| run.id.clone()) else {
                break;
            };
            let page = self
                .list_runs(api::ListRunsRequest {
                    session_id: session_id.clone(),
                    before_run_id: Some(before_run_id),
                    limit: bounded_limit(limit.saturating_sub(state.runs.len())),
                })
                .await?;
            let previous_len = state.runs.len();
            prepend_runs(&mut state.runs, page.runs, limit);
            state.runs_has_earlier = page.has_earlier;
            if state.runs.len() == previous_len {
                break;
            }
        }

        let session_transcript = state
            .transcript
            .clone()
            .ok_or(SdkError::MissingState("session transcript"))?;
        let session_entries = session_transcript
            .entries
            .iter()
            .chain(session_transcript.current_owner_entry.iter())
            .map(|entry| (entry.id.clone(), entry.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(active_agent_session) = state.active_agent_session.as_mut() {
            active_agent_session.transcript = Some(session_transcript);
        }

        for run in &mut state.runs {
            self.hydrate_run(run, limit, &session_entries).await?;
        }

        let artifact_ids = session_artifact_ids(&state);
        let mut artifacts = HashMap::with_capacity(artifact_ids.len());
        for artifact_id in artifact_ids {
            let artifact = self.get_artifact(artifact_id.clone()).await?;
            if artifact.id != artifact_id || artifact.byte_length != artifact.data.len() as u64 {
                return Err(SdkError::InvalidProjection(format!(
                    "artifact {artifact_id} metadata is inconsistent"
                )));
            }
            artifacts.insert(artifact_id, artifact);
        }
        Ok(HydratedSession { state, artifacts })
    }

    async fn hydrate_run(
        &self,
        run: &mut api::RunState,
        limit: usize,
        session_entries: &HashMap<String, api::TranscriptEntry>,
    ) -> Result<(), SdkError> {
        run.objective = self
            .hydrate_run_text(
                &run.id,
                api::RunTextField::Objective,
                std::mem::take(&mut run.objective),
                run.objective_start_byte,
                run.objective_total_bytes,
            )
            .await?;
        run.objective_start_byte = 0;
        run.latest_activity = self
            .hydrate_run_text(
                &run.id,
                api::RunTextField::LatestActivity,
                std::mem::take(&mut run.latest_activity),
                run.latest_activity_start_byte,
                run.latest_activity_total_bytes,
            )
            .await?;
        run.latest_activity_start_byte = 0;
        if let Some(outcome) = run.outcome.as_mut() {
            outcome.body = self
                .hydrate_run_text(
                    &run.id,
                    api::RunTextField::Outcome,
                    std::mem::take(&mut outcome.body),
                    run.outcome_start_byte,
                    run.outcome_total_bytes,
                )
                .await?;
            run.outcome_start_byte = 0;
        }
        if let Some(result) = run.result.as_mut() {
            *result = self
                .hydrate_run_text(
                    &run.id,
                    api::RunTextField::Result,
                    std::mem::take(result),
                    run.result_start_byte,
                    run.result_total_bytes,
                )
                .await?;
            run.result_start_byte = 0;
        }
        if let Some(originating_owner) = run.originating_owner_entry.as_mut()
            && let Some(hydrated) = session_entries.get(&originating_owner.id)
        {
            *originating_owner = hydrated.clone();
        }
        let transcript = run
            .transcript
            .take()
            .ok_or(SdkError::MissingState("run transcript"))?;
        run.transcript = Some(
            self.hydrate_transcript(api::TranscriptOwnerKind::Run, &run.id, transcript, limit)
                .await?,
        );
        Ok(())
    }

    async fn hydrate_run_text(
        &self,
        run_id: &str,
        field: api::RunTextField,
        mut text: String,
        mut start_byte: u64,
        total_bytes: u64,
    ) -> Result<String, SdkError> {
        while start_byte > 0 {
            let expected_end = start_byte;
            let window = self
                .get_run_text_window(api::GetRunTextWindowRequest {
                    run_id: run_id.to_owned(),
                    field: field as i32,
                    before_byte: Some(expected_end),
                    limit_bytes: 256 * 1024,
                })
                .await?;
            let returned_end = window
                .start_byte
                .saturating_add(u64::try_from(window.text.len()).unwrap_or(u64::MAX));
            if window.run_id != run_id
                || window.field != field as i32
                || window.total_bytes != total_bytes
                || returned_end != expected_end
                || window.start_byte >= expected_end
                || window.has_earlier != (window.start_byte > 0)
            {
                return Err(SdkError::InvalidProjection(format!(
                    "run text for {run_id} field {} is not contiguous",
                    field as i32
                )));
            }
            text.insert_str(0, &window.text);
            start_byte = window.start_byte;
        }
        if u64::try_from(text.len()).unwrap_or(u64::MAX) != total_bytes {
            return Err(SdkError::InvalidProjection(format!(
                "run text for {run_id} field {} ended early",
                field as i32
            )));
        }
        Ok(text)
    }

    async fn hydrate_transcript(
        &self,
        owner_kind: api::TranscriptOwnerKind,
        owner_id: &str,
        mut transcript: api::TranscriptPage,
        limit: usize,
    ) -> Result<api::TranscriptPage, SdkError> {
        let initial_entry_ids = transcript
            .entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>();
        let current_owner_turn_id = transcript
            .current_owner_entry
            .as_ref()
            .and_then(|entry| entry.owner_turn_id.clone());
        let mut client_omitted_owner_tool_calls = 0_u64;
        if transcript.entries.len() > limit {
            let remove_count = transcript.entries.len() - limit;
            if let Some(owner_turn_id) = current_owner_turn_id.as_ref() {
                client_omitted_owner_tool_calls = u64::try_from(
                    transcript.entries[..remove_count]
                        .iter()
                        .filter(|entry| {
                            entry.owner_turn_id.as_ref() == Some(owner_turn_id)
                                && matches!(
                                    api::TranscriptEntryKind::try_from(entry.kind),
                                    Ok(api::TranscriptEntryKind::Tool
                                        | api::TranscriptEntryKind::Diff)
                                )
                        })
                        .count(),
                )
                .unwrap_or(u64::MAX);
            }
            transcript.entries.drain(..remove_count);
            transcript.has_earlier = true;
        }
        while transcript.has_earlier && transcript.entries.len() < limit {
            let Some(before_entry_id) = transcript.entries.first().map(|entry| entry.id.clone())
            else {
                break;
            };
            let page = self
                .get_transcript_page(api::GetTranscriptPageRequest {
                    owner_kind: owner_kind as i32,
                    owner_id: owner_id.to_owned(),
                    before_entry_id: Some(before_entry_id),
                    limit: bounded_limit(limit.saturating_sub(transcript.entries.len())),
                })
                .await?;
            let previous_len = transcript.entries.len();
            let has_earlier = page.has_earlier;
            prepend_entries(&mut transcript.entries, page.entries, limit);
            transcript.has_earlier = has_earlier;
            if transcript.entries.len() == previous_len {
                break;
            }
        }
        for entry in &mut transcript.entries {
            self.hydrate_transcript_entry(owner_kind, owner_id, entry)
                .await?;
        }
        if let Some(current_owner) = transcript.current_owner_entry.as_mut() {
            self.hydrate_transcript_entry(owner_kind, owner_id, current_owner)
                .await?;
        }
        if let Some(owner_turn_id) = current_owner_turn_id {
            let restored_tool_calls = transcript
                .entries
                .iter()
                .filter(|entry| {
                    !initial_entry_ids.contains(&entry.id)
                        && entry.owner_turn_id.as_ref() == Some(&owner_turn_id)
                        && matches!(
                            api::TranscriptEntryKind::try_from(entry.kind),
                            Ok(api::TranscriptEntryKind::Tool | api::TranscriptEntryKind::Diff)
                        )
                })
                .count();
            transcript.current_owner_omitted_tool_calls = transcript
                .current_owner_omitted_tool_calls
                .saturating_add(client_omitted_owner_tool_calls)
                .saturating_sub(u64::try_from(restored_tool_calls).unwrap_or(u64::MAX));
        }
        if !transcript.has_earlier {
            transcript.current_owner_omitted_tool_calls = 0;
        }
        Ok(transcript)
    }

    /// Hydrates the complete body for one typed transcript entry without hydrating other history.
    ///
    /// This supports clients that page old history through bounded storage and process one entry at a
    /// time rather than constructing an unbounded replacement transcript in memory.
    ///
    /// # Errors
    /// Returns a transport error or an inconsistent-window projection error.
    pub async fn hydrate_transcript_entry(
        &self,
        owner_kind: api::TranscriptOwnerKind,
        owner_id: &str,
        entry: &mut api::TranscriptEntry,
    ) -> Result<(), SdkError> {
        while entry.body_start_byte > 0 {
            let expected_end = entry.body_start_byte;
            let window = self
                .get_transcript_body_window(api::GetTranscriptBodyWindowRequest {
                    owner_kind: owner_kind as i32,
                    owner_id: owner_id.to_owned(),
                    entry_id: entry.id.clone(),
                    before_byte: Some(expected_end),
                    limit_bytes: 256 * 1024,
                })
                .await?;
            let returned_end = window.start_byte.saturating_add(window.body.len() as u64);
            if window.entry_id != entry.id
                || window.total_bytes != entry.body_total_bytes
                || returned_end != expected_end
                || window.start_byte >= expected_end
            {
                return Err(SdkError::InvalidProjection(format!(
                    "transcript body for {} is not contiguous",
                    entry.id
                )));
            }
            entry.body.insert_str(0, &window.body);
            entry.body_start_byte = window.start_byte;
            if !window.has_earlier && entry.body_start_byte != 0 {
                return Err(SdkError::InvalidProjection(format!(
                    "transcript body for {} ended early",
                    entry.id
                )));
            }
        }
        if u64::try_from(entry.body.len()).unwrap_or(u64::MAX) != entry.body_total_bytes {
            return Err(SdkError::InvalidProjection(format!(
                "transcript body for {} ended early",
                entry.id
            )));
        }
        Ok(())
    }
}

fn configured_transport(
    channel: Channel,
    interceptor: ClientApiKey,
) -> NakodeServiceClient<ApiTransport> {
    NakodeServiceClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
        .max_encoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
}

fn configured_activation_transport(channel: Channel) -> ActivationServiceClient<Channel> {
    ActivationServiceClient::new(channel)
        .max_decoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
        .max_encoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
}

fn bridge_continuation_mutation(
    request: &api::ContinueSessionFromBridgeRequest,
) -> api::MutationOptions {
    let mut digest = Sha256::new();
    for identity_part in [
        request.session_id.as_bytes(),
        request.transport.as_bytes(),
        request.external_thread_id.as_bytes(),
        request.external_event_id.as_bytes(),
    ] {
        digest.update(identity_part.len().to_be_bytes());
        digest.update(identity_part);
    }
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    api::MutationOptions {
        idempotency_key: format!("bridge-continuation-{encoded}"),
        expected_revision: None,
    }
}

fn authoritative_remove_request(
    mut request: api::RemoveQueuedPromptRequest,
) -> api::RemoveQueuedPromptRequest {
    let options = request.mutation.get_or_insert_with(|| mutation(None));
    options.expected_revision = None;
    request
}

fn mutation(expected_revision: Option<u64>) -> api::MutationOptions {
    api::MutationOptions {
        idempotency_key: uuid::Uuid::now_v7().to_string(),
        expected_revision,
    }
}

fn retryable_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

fn activation_cursor(status: &api::ActivationStatus) -> ActivationCursor {
    ActivationCursor {
        attempt_id: status.attempt_id.clone(),
        revision: status.revision,
    }
}

fn activation_cursor_changed(
    previous: Option<&ActivationCursor>,
    status: &api::ActivationStatus,
) -> bool {
    previous.is_none_or(|previous| {
        previous.attempt_id != status.attempt_id || previous.revision != status.revision
    })
}

async fn reattach_session(
    client: &NakodeClient,
    expected_session_id: &str,
    attachment: SessionAttachment,
) -> Result<(), SdkError> {
    let reopened = client
        .open_session_with_attachment(expected_session_id.to_owned(), attachment)
        .await?;
    if reopened == expected_session_id {
        Ok(())
    } else {
        Err(SdkError::InvalidProjection(format!(
            "Nakode reopened logical session {reopened}, expected {expected_session_id}"
        )))
    }
}

async fn reattach_watched_session(
    client: &NakodeClient,
    expected_session_id: &str,
    attachment: SessionAttachment,
    sender: &tokio::sync::mpsc::Sender<Result<api::SessionState, SdkError>>,
    reconnect_reported: &mut bool,
    after: &mut Option<api::Cursor>,
) -> bool {
    match reattach_session(client, expected_session_id, attachment).await {
        Ok(()) => {
            *reconnect_reported = false;
            *after = None;
            true
        }
        Err(error @ SdkError::InvalidProjection(_)) => {
            let _ = sender.send(Err(error)).await;
            false
        }
        Err(error) => report_sdk_watch_error(sender, reconnect_reported, error).await,
    }
}

async fn report_sdk_watch_error<T>(
    sender: &tokio::sync::mpsc::Sender<Result<T, SdkError>>,
    already_reported: &mut bool,
    error: SdkError,
) -> bool {
    if *already_reported {
        return true;
    }
    *already_reported = true;
    sender.send(Err(error)).await.is_ok()
}

async fn report_watch_error<T>(
    sender: &tokio::sync::mpsc::Sender<Result<T, SdkError>>,
    already_reported: &mut bool,
    error: tonic::Status,
) -> bool {
    if *already_reported {
        return true;
    }
    *already_reported = true;
    sender.send(Err(error.into())).await.is_ok()
}

async fn retry_transport<Request, Response, Call, CallFuture>(
    request: Request,
    mut call: Call,
) -> Result<Response, tonic::Status>
where
    Request: Clone,
    Call: FnMut(Request) -> CallFuture,
    CallFuture: Future<Output = Result<Response, tonic::Status>>,
{
    loop {
        match call(request.clone()).await {
            Ok(response) => return Ok(response),
            Err(error) if retryable_status(&error) => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn bounded_limit(remaining: usize) -> u32 {
    u32::try_from(remaining.max(1)).unwrap_or(u32::MAX)
}

fn prepend_entries(
    current: &mut Vec<api::TranscriptEntry>,
    older: Vec<api::TranscriptEntry>,
    limit: usize,
) {
    let mut combined = older;
    for entry in current.drain(..) {
        if let Some(position) = combined
            .iter()
            .position(|candidate| candidate.id == entry.id)
        {
            combined[position] = entry;
        } else {
            combined.push(entry);
        }
    }
    if combined.len() > limit {
        combined.drain(..combined.len() - limit);
    }
    *current = combined;
}

fn prepend_runs(current: &mut Vec<api::RunState>, older: Vec<api::RunState>, limit: usize) {
    let mut combined = older;
    for run in current.drain(..) {
        if let Some(position) = combined.iter().position(|candidate| candidate.id == run.id) {
            combined[position] = run;
        } else {
            combined.push(run);
        }
    }
    if combined.len() > limit {
        combined.drain(..combined.len() - limit);
    }
    *current = combined;
}

fn session_artifact_ids(session: &api::SessionState) -> HashSet<String> {
    let session_entries = session.transcript.iter().flat_map(|transcript| {
        transcript
            .entries
            .iter()
            .chain(transcript.current_owner_entry.iter())
    });
    let active_agent_entries = session
        .active_agent_session
        .iter()
        .flat_map(|agent_session| {
            agent_session.transcript.iter().flat_map(|transcript| {
                transcript
                    .entries
                    .iter()
                    .chain(transcript.current_owner_entry.iter())
            })
        });
    let run_entries = session.runs.iter().flat_map(|run| {
        run.transcript
            .iter()
            .flat_map(|transcript| {
                transcript
                    .entries
                    .iter()
                    .chain(transcript.current_owner_entry.iter())
            })
            .chain(run.originating_owner_entry.iter())
    });
    session_entries
        .chain(active_agent_entries)
        .chain(run_entries)
        .flat_map(|entry| entry.artifact_ids.iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use futures_util::{Stream, StreamExt};
    use nakode_protocol as protocol;
    use nakode_server::{ServerEndpoint, ServerRequest, ServerRequests};
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::{Request as TonicRequest, Response, Status};

    use super::{
        ActivationClient, ActivationCursor, NakodeClient, SdkError, SessionAttachment,
        activation_cursor_changed, api, authoritative_remove_request, bridge_continuation_mutation,
        managed_watch, retry_transport,
    };

    #[test]
    fn queued_prompt_removal_preserves_identity_and_omits_the_revision_fence() {
        let request = authoritative_remove_request(api::RemoveQueuedPromptRequest {
            mutation: Some(api::MutationOptions {
                idempotency_key: "remove-one-exact-prompt".to_owned(),
                expected_revision: Some(41),
            }),
            session_id: "session-1".to_owned(),
            prompt_id: "prompt-2".to_owned(),
        });
        let options = request.mutation.expect("normalized mutation options");

        assert_eq!(options.idempotency_key, "remove-one-exact-prompt");
        assert_eq!(options.expected_revision, None);
        assert_eq!(request.prompt_id, "prompt-2");
    }

    fn bridge_request(
        session_id: &str,
        thread_id: &str,
        event_id: &str,
    ) -> api::ContinueSessionFromBridgeRequest {
        api::ContinueSessionFromBridgeRequest {
            mutation: None,
            session_id: session_id.to_owned(),
            transport: "thread-transport".to_owned(),
            external_thread_id: thread_id.to_owned(),
            external_event_id: event_id.to_owned(),
            source_message_id: event_id.to_owned(),
            prompt: None,
            consume_as_busy: false,
        }
    }

    #[derive(Clone)]
    struct Request {
        idempotency_key: String,
    }

    #[derive(Clone, Copy)]
    enum SessionServerMode {
        ReattachSame,
        ReattachDifferent,
        SnapshotDifferent,
    }

    #[derive(Clone, Debug, Default)]
    struct CapturedAttachment {
        session_id: String,
        tools: Option<protocol::SessionToolConfiguration>,
        mcp_grant: Option<protocol::McpSessionGrant>,
        profile_id: Option<String>,
        account_id: Option<String>,
    }

    struct TestUnixServer {
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        server: tokio::task::JoinHandle<()>,
        actor: Option<tokio::task::JoinHandle<()>>,
    }

    impl TestUnixServer {
        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if tokio::time::timeout(Duration::from_secs(1), &mut self.server)
                .await
                .is_err()
            {
                self.server.abort();
                let _ = self.server.await;
            }
            if let Some(actor) = self.actor.take() {
                actor.abort();
                let _ = actor.await;
            }
        }
    }

    fn session_view(id: &str) -> protocol::SessionView {
        protocol::SessionView {
            id: protocol::SessionId::from(id),
            revision: 1,
            workspace_id: protocol::WorkspaceId::from("workspace-a"),
            working_directory: "/tmp/workspace-a".to_owned(),
            title: "Attached session".to_owned(),
            code_mode: false,
            status_message: "Ready".to_owned(),
            diagnostic_count: 0,
            activity: protocol::SessionActivity::Idle,
            selected_provider_id: None,
            selected_model_id: None,
            selected_model_options: protocol::ModelOptions::default(),
            active_agent_session: None,
            active_turn: None,
            last_turn: None,
            next_turn_configuration_pending: false,
            next_turn_transition: None,
            context_usage: None,
            transcript: protocol::TranscriptPage {
                entries: Vec::new(),
                has_earlier: false,
                stream_active: false,
                stream_label: String::new(),
                current_owner_entry: None,
                current_owner_omitted_tool_calls: 0,
            },
            recoverable_prompt: None,
            queue: Vec::new(),
            interactions: Vec::new(),
            todos: Vec::new(),
            runs: Vec::new(),
            runs_total: Some(0),
            runs_has_earlier: false,
            notices: Vec::new(),
            external_tool_calls: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            last_owner_activity_at_ms: 0,
            selected_account_id: None,
            routing_diagnostic: None,
            failure: None,
        }
    }

    fn session_server_snapshot(
        endpoint: &ServerEndpoint,
        scope: protocol::SubscriptionScope,
        attached: bool,
        mode: SessionServerMode,
    ) -> Result<protocol::Snapshot<protocol::SubscriptionView>, protocol::ServiceError> {
        let protocol::SubscriptionScope::Session { session_id } = scope else {
            return Err(protocol::ServiceError {
                code: protocol::ErrorCode::InvalidRequest,
                message: "session subscription required".to_owned(),
                retryable: false,
            });
        };
        if !attached && !matches!(mode, SessionServerMode::SnapshotDifferent) {
            return Err(protocol::ServiceError {
                code: protocol::ErrorCode::NotFound,
                message: "session is persisted but not attached".to_owned(),
                retryable: false,
            });
        }
        let projected_id = if matches!(mode, SessionServerMode::SnapshotDifferent) {
            "different-session"
        } else {
            session_id.as_str()
        };
        Ok(protocol::Snapshot {
            cursor: endpoint.cursor(),
            value: protocol::SubscriptionView::Session(Box::new(session_view(projected_id))),
        })
    }

    async fn run_session_actor(
        actor_endpoint: ServerEndpoint,
        mut requests: ServerRequests,
        mode: SessionServerMode,
        captured: Arc<Mutex<Option<CapturedAttachment>>>,
    ) {
        let mut attached = false;
        while let Some(request) = requests.recv().await {
            match request {
                ServerRequest::Subscribe { scope, respond, .. } => {
                    let _ = respond.send(session_server_snapshot(
                        &actor_endpoint,
                        scope,
                        attached,
                        mode,
                    ));
                }
                ServerRequest::Command {
                    command:
                        protocol::Command::OpenSession {
                            session_id,
                            tools,
                            mcp_grant,
                            profile_id,
                            account_id,
                            ..
                        },
                    respond,
                    ..
                } => {
                    *captured.lock().expect("captured attachment") = Some(CapturedAttachment {
                        session_id: session_id.to_string(),
                        tools,
                        mcp_grant,
                        profile_id,
                        account_id,
                    });
                    attached = true;
                    let resource_id = if matches!(mode, SessionServerMode::ReattachDifferent) {
                        "different-session".to_owned()
                    } else {
                        session_id.to_string()
                    };
                    let _ = respond.send(Ok(protocol::CommandAccepted {
                        resource_id: Some(resource_id),
                        revision: Some(1),
                        bridge_continuation: None,
                        replayed_bridge_continuation: None,
                        replayed_bridge_source_active: None,
                    }));
                }
                ServerRequest::Command { respond, .. } => {
                    let _ = respond.send(Err(protocol::ServiceError {
                        code: protocol::ErrorCode::InvalidRequest,
                        message: "unexpected command".to_owned(),
                        retryable: false,
                    }));
                }
                ServerRequest::Query { respond, .. } => {
                    let _ = respond.send(Err(protocol::ServiceError {
                        code: protocol::ErrorCode::InvalidRequest,
                        message: "unexpected query".to_owned(),
                        retryable: false,
                    }));
                }
            }
        }
    }

    fn spawn_session_server(
        path: &Path,
        mode: SessionServerMode,
        captured: Arc<Mutex<Option<CapturedAttachment>>>,
    ) -> TestUnixServer {
        let listener = UnixListener::bind(path).expect("bind fake Nakode API");
        let (endpoint, requests) = ServerEndpoint::channel_with_build_revision(
            "sdk-test",
            Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
            protocol::ServiceCapabilities::default(),
            8,
        );
        let actor = tokio::spawn(run_session_actor(
            endpoint.clone(),
            requests,
            mode,
            captured,
        ));
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let incoming = UnixListenerStream::new(listener);
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(nakode_server::grpc::GrpcService::new(endpoint).into_server())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await;
        });
        TestUnixServer {
            shutdown: Some(shutdown),
            server,
            actor: Some(actor),
        }
    }

    #[derive(Clone)]
    enum ActivationWatchBehavior {
        Empty,
        Pending(Arc<AtomicUsize>),
    }

    struct DropAwareActivationStream {
        dropped: Arc<AtomicUsize>,
    }

    impl Stream for DropAwareActivationStream {
        type Item = Result<api::ActivationStatus, Status>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for DropAwareActivationStream {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct FakeActivationService {
        status: api::ActivationStatus,
        behavior: ActivationWatchBehavior,
        requests: Arc<Mutex<Vec<api::WatchActivationStatusRequest>>>,
    }

    #[tonic::async_trait]
    impl api::activation_service_server::ActivationService for FakeActivationService {
        async fn get_activation_status(
            &self,
            _request: TonicRequest<api::GetActivationStatusRequest>,
        ) -> Result<Response<api::ActivationStatus>, Status> {
            Ok(Response::new(self.status.clone()))
        }

        type WatchActivationStatusStream =
            Pin<Box<dyn Stream<Item = Result<api::ActivationStatus, Status>> + Send + 'static>>;

        async fn watch_activation_status(
            &self,
            request: TonicRequest<api::WatchActivationStatusRequest>,
        ) -> Result<Response<Self::WatchActivationStatusStream>, Status> {
            self.requests
                .lock()
                .expect("activation requests")
                .push(request.into_inner());
            let stream: Self::WatchActivationStatusStream = match &self.behavior {
                ActivationWatchBehavior::Empty => Box::pin(futures_util::stream::empty()),
                ActivationWatchBehavior::Pending(dropped) => Box::pin(DropAwareActivationStream {
                    dropped: Arc::clone(dropped),
                }),
            };
            Ok(Response::new(stream))
        }

        async fn force_activation_recheck(
            &self,
            _request: TonicRequest<api::ActivationMutationRequest>,
        ) -> Result<Response<api::ActivationStatus>, Status> {
            Err(Status::unimplemented("not used by SDK transport tests"))
        }

        async fn force_activate(
            &self,
            _request: TonicRequest<api::ForceActivateRequest>,
        ) -> Result<Response<api::ActivationStatus>, Status> {
            Err(Status::unimplemented("not used by SDK transport tests"))
        }
    }

    fn spawn_activation_server(path: &Path, service: FakeActivationService) -> TestUnixServer {
        let listener = UnixListener::bind(path).expect("bind fake activation API");
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let incoming = UnixListenerStream::new(listener);
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(api::activation_service_server::ActivationServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await;
        });
        TestUnixServer {
            shutdown: Some(shutdown),
            server,
            actor: None,
        }
    }

    fn activation_status(
        attempt_id: &str,
        revision: u64,
        phase: api::ActivationPhase,
    ) -> api::ActivationStatus {
        api::ActivationStatus {
            attempt_id: attempt_id.to_owned(),
            revision,
            phase: phase as i32,
            ..api::ActivationStatus::default()
        }
    }

    #[tokio::test]
    async fn server_info_preserves_the_live_build_revision() {
        let directory = tempfile::tempdir().expect("SDK transport directory");
        let socket = directory.path().join("nakode.sock");
        let server = spawn_session_server(
            &socket,
            SessionServerMode::ReattachSame,
            Arc::new(Mutex::new(None)),
        );
        let client = NakodeClient::connect_unix(&socket)
            .await
            .expect("connect fake Nakode API");

        let info = client
            .get_server_info()
            .await
            .expect("server info must cross the SDK transport");
        assert_eq!(
            info.build_revision.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert!(info.rpc_lanes.iter().any(|assignment| {
            assignment.service == "NakodeService"
                && assignment.method == "GetTranscriptBodyWindow"
                && assignment.rule == i32::from(api::rpc_lane_definition::Rule::Hydration)
        }));

        server.stop().await;
    }

    #[tokio::test]
    async fn attached_watch_reopens_same_id_and_forwards_every_attachment() {
        let directory = tempfile::tempdir().expect("SDK transport directory");
        let socket = directory.path().join("nakode.sock");
        let captured = Arc::new(Mutex::new(None));
        let server = spawn_session_server(
            &socket,
            SessionServerMode::ReattachSame,
            Arc::clone(&captured),
        );
        let client = NakodeClient::connect_unix(&socket)
            .await
            .expect("connect fake Nakode API");
        let attachment = SessionAttachment {
            tools: Some(api::SessionToolConfiguration {
                tools: vec![api::ExternalToolDefinition {
                    name: "ticket_lookup".to_owned(),
                    description: "Look up one ticket".to_owned(),
                    input_schema_json: r#"{"type":"object"}"#.to_owned(),
                }],
                replace_builtin_tools: true,
                code_mode: false,
                allowed_builtin_tools: vec!["read".to_owned()],
            }),
            mcp_grant: Some(api::McpSessionGrant {
                surface: api::McpSessionSurface::CodingAgent as i32,
                server_ids: vec!["linear".to_owned()],
            }),
            profile_id: Some("profile-a".to_owned()),
            account_id: None,
        };
        let mut watch = client.watch_attached_session("session-a", attachment);
        let state = tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .expect("reattached snapshot deadline")
            .expect("reattached snapshot")
            .expect("valid reattached snapshot");
        assert_eq!(state.id, "session-a");

        let captured = captured
            .lock()
            .expect("captured attachment")
            .clone()
            .expect("open-session attachment");
        assert_eq!(captured.session_id, "session-a");
        assert_eq!(captured.profile_id.as_deref(), Some("profile-a"));
        assert_eq!(captured.account_id, None);
        let tools = captured.tools.expect("forwarded tools");
        assert!(tools.replace_builtin_tools);
        assert_eq!(tools.allowed_builtin_tools, Some(vec!["read".to_owned()]));
        assert_eq!(tools.tools.len(), 1);
        assert_eq!(tools.tools[0].name, "ticket_lookup");
        let grant = captured.mcp_grant.expect("forwarded MCP grant");
        assert_eq!(
            grant.surface,
            Some(protocol::McpSessionSurface::CodingAgent)
        );
        assert_eq!(grant.server_ids, ["linear"]);

        drop(watch);
        server.stop().await;
    }

    #[tokio::test]
    async fn attached_watch_rejects_identity_changes_from_reopen_and_snapshot() {
        for (name, mode) in [
            ("reopen", SessionServerMode::ReattachDifferent),
            ("snapshot", SessionServerMode::SnapshotDifferent),
        ] {
            let directory = tempfile::tempdir().expect("SDK transport directory");
            let socket = directory.path().join(format!("nakode-{name}.sock"));
            let server = spawn_session_server(&socket, mode, Arc::new(Mutex::new(None)));
            let client = NakodeClient::connect_unix(&socket)
                .await
                .expect("connect fake Nakode API");
            let mut watch =
                client.watch_attached_session("session-a", SessionAttachment::default());
            let error = tokio::time::timeout(Duration::from_secs(3), watch.next())
                .await
                .expect("projection error deadline")
                .expect("projection error item")
                .expect_err("identity change must be terminal");
            assert!(
                matches!(error, SdkError::InvalidProjection(_)),
                "{name}: {error}"
            );
            assert!(
                tokio::time::timeout(Duration::from_secs(1), watch.next())
                    .await
                    .expect("terminated producer deadline")
                    .is_none(),
                "{name}: producer continued after identity violation"
            );
            server.stop().await;
        }
    }

    #[tokio::test]
    async fn activation_watch_rediscovery_hands_off_sockets_and_sends_attempt_cursor() {
        let directory = tempfile::tempdir().expect("activation transport directory");
        let helper_socket = directory.path().join("helper.sock");
        let service_socket = directory.path().join("service.sock");
        let helper_requests = Arc::new(Mutex::new(Vec::new()));
        let service_requests = Arc::new(Mutex::new(Vec::new()));
        let helper = spawn_activation_server(
            &helper_socket,
            FakeActivationService {
                status: activation_status("attempt-a", 2, api::ActivationPhase::Blocked),
                behavior: ActivationWatchBehavior::Empty,
                requests: Arc::clone(&helper_requests),
            },
        );
        let service = spawn_activation_server(
            &service_socket,
            FakeActivationService {
                status: activation_status("attempt-a", 3, api::ActivationPhase::Activated),
                behavior: ActivationWatchBehavior::Empty,
                requests: Arc::clone(&service_requests),
            },
        );
        let discoveries = Arc::new(AtomicUsize::new(0));
        let mut watch = ActivationClient::watch_status_with_rediscovery(
            {
                let discoveries = Arc::clone(&discoveries);
                let helper_socket = helper_socket.clone();
                let service_socket = service_socket.clone();
                move || {
                    let path = if discoveries.fetch_add(1, Ordering::SeqCst) == 0 {
                        helper_socket.clone()
                    } else {
                        service_socket.clone()
                    };
                    async move { ActivationClient::connect_unix(path).await }
                }
            },
            None,
        );
        let blocked = tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .expect("helper snapshot deadline")
            .expect("helper snapshot")
            .expect("helper status");
        assert_eq!(blocked.phase, api::ActivationPhase::Blocked as i32);
        let activated = tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .expect("service snapshot deadline")
            .expect("service snapshot")
            .expect("service status");
        assert_eq!(activated.phase, api::ActivationPhase::Activated as i32);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !helper_requests.lock().expect("helper requests").is_empty()
                    && !service_requests
                        .lock()
                        .expect("service requests")
                        .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("attempt-qualified watches reached both endpoints");
        let helper_request = helper_requests.lock().expect("helper requests")[0].clone();
        assert_eq!(helper_request.after_attempt_id, "attempt-a");
        assert_eq!(helper_request.after_revision, Some(2));
        let service_request = service_requests.lock().expect("service requests")[0].clone();
        assert_eq!(service_request.after_attempt_id, "attempt-a");
        assert_eq!(service_request.after_revision, Some(3));

        drop(watch);
        helper.stop().await;
        service.stop().await;
    }

    #[tokio::test]
    async fn dropping_rediscovering_activation_watch_cancels_real_transport_stream() {
        let directory = tempfile::tempdir().expect("activation transport directory");
        let socket = directory.path().join("activation.sock");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let dropped = Arc::new(AtomicUsize::new(0));
        let server = spawn_activation_server(
            &socket,
            FakeActivationService {
                status: activation_status("attempt-a", 2, api::ActivationPhase::Blocked),
                behavior: ActivationWatchBehavior::Pending(Arc::clone(&dropped)),
                requests: Arc::clone(&requests),
            },
        );
        let mut watch = ActivationClient::watch_status_with_rediscovery(
            {
                let socket = PathBuf::from(&socket);
                move || {
                    let socket = socket.clone();
                    async move { ActivationClient::connect_unix(socket).await }
                }
            },
            None,
        );
        let _ = tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .expect("initial activation status deadline")
            .expect("initial activation status")
            .expect("valid activation status");
        tokio::time::timeout(Duration::from_secs(1), async {
            while requests.lock().expect("activation requests").is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("transport watch established");
        drop(watch);
        tokio::time::timeout(Duration::from_secs(2), async {
            while dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server transport stream dropped with SDK consumer");
        server.stop().await;
    }

    #[tokio::test]
    async fn dropping_a_watch_cancels_its_reconnecting_producer() {
        struct DropSignal(Arc<AtomicUsize>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<api::WorkspaceState, SdkError>>(1);
        let _sender = sender;
        let dropped = Arc::new(AtomicUsize::new(0));
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let task_dropped = Arc::clone(&dropped);
        let task = tokio::spawn(async move {
            let _signal = DropSignal(task_dropped);
            let _ = started_sender.send(());
            futures_util::future::pending::<()>().await;
        });
        let watch = managed_watch(receiver, task);
        started_receiver.await.expect("producer started");
        drop(watch);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer is cancelled with the consumer");
    }

    #[test]
    fn activation_cursor_treats_a_new_attempt_with_a_lower_revision_as_new() {
        let previous = ActivationCursor {
            attempt_id: "attempt-a".to_owned(),
            revision: 50,
        };
        let mut next = api::ActivationStatus {
            attempt_id: "attempt-b".to_owned(),
            revision: 1,
            ..Default::default()
        };
        assert!(activation_cursor_changed(Some(&previous), &next));
        next.attempt_id = previous.attempt_id.clone();
        assert!(activation_cursor_changed(Some(&previous), &next));
        next.revision = previous.revision;
        assert!(!activation_cursor_changed(Some(&previous), &next));
    }

    #[test]
    fn bridge_continuation_key_is_stable_across_sdk_invocations() {
        let request = bridge_request("session-a", "thread-a", "event-a");
        let first = bridge_continuation_mutation(&request).idempotency_key;
        let second = bridge_continuation_mutation(&request).idempotency_key;
        assert_eq!(first, second);
        assert_ne!(
            first,
            bridge_continuation_mutation(&bridge_request("session-a", "thread-a", "event-b"))
                .idempotency_key
        );
        assert_ne!(
            first,
            bridge_continuation_mutation(&bridge_request("session-b", "thread-a", "event-a"))
                .idempotency_key
        );
        let mut other_transport = request.clone();
        other_transport.transport = "other-transport".to_owned();
        assert_ne!(
            first,
            bridge_continuation_mutation(&other_transport).idempotency_key
        );
        assert_ne!(
            first,
            bridge_continuation_mutation(&bridge_request("session-a", "thread-b", "event-a"))
                .idempotency_key
        );
    }

    #[test]
    fn transient_transport_retry_reuses_the_exact_request() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime")
            .block_on(async {
                let attempts = Arc::new(AtomicUsize::new(0));
                let keys = Arc::new(Mutex::new(Vec::new()));
                let result = retry_transport(
                    Request {
                        idempotency_key: "stable-key".to_owned(),
                    },
                    {
                        let attempts = Arc::clone(&attempts);
                        let keys = Arc::clone(&keys);
                        move |request: Request| {
                            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                            keys.lock()
                                .expect("recorded keys")
                                .push(request.idempotency_key.clone());
                            async move {
                                if attempt == 0 {
                                    Err(tonic::Status::unavailable("transport interrupted"))
                                } else {
                                    Ok(request.idempotency_key)
                                }
                            }
                        }
                    },
                )
                .await
                .expect("retry succeeds");
                assert_eq!(result, "stable-key");
                assert_eq!(
                    *keys.lock().expect("recorded keys"),
                    ["stable-key", "stable-key"]
                );
            });
    }

    #[test]
    fn semantic_failure_is_not_retried() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime")
            .block_on(async {
                let attempts = Arc::new(AtomicUsize::new(0));
                let error = retry_transport(
                    Request {
                        idempotency_key: "key".into(),
                    },
                    {
                        let attempts = Arc::clone(&attempts);
                        move |_request| {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            async { Err::<(), _>(tonic::Status::failed_precondition("provider")) }
                        }
                    },
                )
                .await
                .expect_err("semantic failure is returned");
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert_eq!(attempts.load(Ordering::SeqCst), 1);
            });
    }
}
