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
use nakode_api::v1::{self as api, nakode_service_client::NakodeServiceClient};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
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

/// Cloneable high-level client. A clone shares the reconnecting HTTP/2
/// channel but each request and watch has independent generated client state.
#[derive(Clone)]
pub struct NakodeClient {
    transport: NakodeServiceClient<Channel>,
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

macro_rules! typed_response_mutation {
    ($method:ident, $request:ty, $response:ty) => {
        /// Executes this typed product mutation with an SDK-owned idempotency key and returns the
        /// resulting redacted management view.
        ///
        /// # Errors
        /// Returns a transport or server status error.
        pub async fn $method(&self, mut request: $request) -> Result<$response, SdkError> {
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
            transport: configured_transport(channel),
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
            transport: configured_transport(channel),
        }
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

    /// Creates a logical session rooted at an explicit filesystem/provider working directory.
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
            }
        )?;
        result
            .resource_id
            .ok_or(SdkError::MissingState("created session identifier"))
    }

    /// Opens a persisted logical session in the server and returns its full ID.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn open_session(&self, session_id: impl Into<String>) -> Result<String, SdkError> {
        self.open_session_with_tools(session_id, None).await
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
        let result = send_mutation!(
            self,
            open_session,
            api::OpenSessionRequest {
                mutation: Some(mutation(None)),
                session_id: session_id.into(),
                tools,
                mcp_grant: None,
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
        self.hydrate_session(state, limit).await
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
    typed_mutation!(remove_queued_prompt, api::RemoveQueuedPromptRequest);
    typed_mutation!(cancel_turn, api::CancelTurnRequest);
    typed_mutation!(compact_context, api::CompactContextRequest);
    typed_mutation!(run_shell, api::RunShellRequest);
    typed_mutation!(select_model, api::SelectModelRequest);
    typed_mutation!(
        set_provider_model_filter,
        api::SetProviderModelFilterRequest
    );
    typed_mutation!(set_skill_enabled, api::SetSkillEnabledRequest);
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
    typed_response_mutation!(
        save_discord_integration,
        api::SaveDiscordIntegrationRequest,
        api::DiscordIntegration
    );
    typed_response_mutation!(
        set_discord_integration_enabled,
        api::SetDiscordIntegrationEnabledRequest,
        api::DiscordIntegration
    );
    typed_response_mutation!(
        restart_discord_integration,
        api::RestartDiscordIntegrationRequest,
        api::DiscordIntegration
    );
    typed_mutation!(save_agent, api::SaveAgentRequest);
    typed_mutation!(delete_agent, api::DeleteAgentRequest);
    typed_mutation!(delete_session, api::DeleteSessionRequest);
    typed_mutation!(update_settings, api::UpdateSettingsRequest);
    typed_mutation!(check_agent_browser, api::CheckAgentBrowserRequest);
    typed_mutation!(cancel_run, api::CancelRunRequest);

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
        Ok(self
            .transport
            .clone()
            .list_skills(api::ListSkillsRequest {
                workspace_id: workspace_id.into(),
                profile_id: profile_id.into(),
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

    /// Returns redacted installation Discord configuration and this workspace service's runtime
    /// transport state. The bot token is represented only by `token_configured`.
    ///
    /// # Errors
    /// Returns a transport or server status error.
    pub async fn get_discord_integration(&self) -> Result<api::DiscordIntegration, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_discord_integration(api::GetDiscordIntegrationRequest {})
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

    pub fn watch_session(&self, session_id: impl Into<String>) -> Watch<api::SessionState> {
        let client = self.clone();
        let session_id = session_id.into();
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
                    Ok(state) => client.hydrate_session(state, limit).await,
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

    async fn hydrate_session(
        &self,
        mut state: api::SessionState,
        limit: usize,
    ) -> Result<HydratedSession, SdkError> {
        let limit = limit.max(1);
        let session_id = state.id.clone();
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
            state.runs_has_earlier = page.has_earlier || state.runs.len() == limit;
            if state.runs.len() == previous_len {
                break;
            }
        }

        for run in &mut state.runs {
            let transcript = run
                .transcript
                .take()
                .ok_or(SdkError::MissingState("run transcript"))?;
            run.transcript = Some(
                self.hydrate_transcript(api::TranscriptOwnerKind::Run, &run.id, transcript, limit)
                    .await?,
            );
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

    async fn hydrate_transcript(
        &self,
        owner_kind: api::TranscriptOwnerKind,
        owner_id: &str,
        mut transcript: api::TranscriptPage,
        limit: usize,
    ) -> Result<api::TranscriptPage, SdkError> {
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
            transcript.has_earlier = has_earlier || transcript.entries.len() == limit;
            if transcript.entries.len() == previous_len {
                break;
            }
        }
        for entry in &mut transcript.entries {
            self.hydrate_transcript_entry(owner_kind, owner_id, entry)
                .await?;
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
        Ok(())
    }
}

fn configured_transport(channel: Channel) -> NakodeServiceClient<Channel> {
    NakodeServiceClient::new(channel)
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
    let session_entries = session
        .transcript
        .iter()
        .flat_map(|transcript| &transcript.entries);
    let run_entries = session.runs.iter().flat_map(|run| {
        run.transcript
            .iter()
            .flat_map(|transcript| &transcript.entries)
    });
    session_entries
        .chain(run_entries)
        .flat_map(|entry| entry.artifact_ids.iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{SdkError, api, bridge_continuation_mutation, managed_watch, retry_transport};

    fn bridge_request(
        session_id: &str,
        thread_id: &str,
        event_id: &str,
    ) -> api::ContinueSessionFromBridgeRequest {
        api::ContinueSessionFromBridgeRequest {
            mutation: None,
            session_id: session_id.to_owned(),
            transport: "discord".to_owned(),
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
