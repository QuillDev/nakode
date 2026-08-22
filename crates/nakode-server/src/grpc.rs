//! gRPC adapter for the authoritative Nakode server request boundary.
//!
//! Conversion lives here so provider/runtime protocol types never leak into
//! the public generated contract. The adapter never owns domain state.

use std::{pin::Pin, sync::Arc};

use futures_util::Stream;
use nakode_api::v1 as api;
use nakode_protocol as protocol;
use tokio_stream::wrappers::ReceiverStream;

use crate::{PublishedEvent, ServerEndpoint};

/// Root-owned Discord management mutation injected into the dependency-neutral gRPC facade.
/// Credential debug output remains redacted through [`protocol::CredentialInput`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiscordManagementMutation {
    Save(protocol::DiscordIntegrationInput),
    SetEnabled(bool),
    Restart,
}

/// Dependency-inverted authority for installation configuration and the current service's Discord
/// transport. The root application implements this because it owns private credential storage and
/// transport supervision; `nakode-server` never depends on either implementation.
#[tonic::async_trait]
pub trait DiscordManagement: Send + Sync {
    async fn get(&self) -> Result<protocol::DiscordIntegrationView, protocol::ServiceError>;

    async fn mutate(
        &self,
        idempotency_key: protocol::IdempotencyKey,
        mutation: DiscordManagementMutation,
    ) -> Result<protocol::DiscordIntegrationView, protocol::ServiceError>;
}

/// Public gRPC facade over one authoritative [`ServerEndpoint`].
#[derive(Clone)]
pub struct GrpcService {
    endpoint: ServerEndpoint,
    client_id: protocol::ClientId,
    discord_management: Option<Arc<dyn DiscordManagement>>,
}

impl GrpcService {
    #[must_use]
    pub fn new(endpoint: ServerEndpoint) -> Self {
        Self {
            endpoint,
            client_id: protocol::ClientId::new(format!("grpc-{}", uuid::Uuid::now_v7())),
            discord_management: None,
        }
    }

    /// Installs the root-owned Discord configuration and transport authority.
    #[must_use]
    pub fn with_discord_management(mut self, management: Arc<dyn DiscordManagement>) -> Self {
        self.discord_management = Some(management);
        self
    }

    #[must_use]
    pub fn into_server(self) -> api::nakode_service_server::NakodeServiceServer<Self> {
        api::nakode_service_server::NakodeServiceServer::new(self)
            .max_decoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
            .max_encoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
    }

    async fn execute_mutation(
        &self,
        options: Option<api::MutationOptions>,
        command: protocol::Command,
    ) -> Result<protocol::CommandAccepted, tonic::Status> {
        let options =
            options.ok_or_else(|| tonic::Status::invalid_argument("mutation is required"))?;
        if options.idempotency_key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "mutation.idempotency_key is required",
            ));
        }
        self.endpoint
            .execute_command(
                self.client_id.clone(),
                protocol::IdempotencyKey::new(options.idempotency_key),
                options.expected_revision,
                false,
                command,
            )
            .await
            .map_err(status)
    }

    async fn mutate(
        &self,
        options: Option<api::MutationOptions>,
        command: protocol::Command,
    ) -> Result<tonic::Response<api::MutationResult>, tonic::Status> {
        let result = self.execute_mutation(options, command).await?;
        Ok(tonic::Response::new(api::MutationResult {
            resource_id: result.resource_id,
            revision: result.revision,
        }))
    }

    async fn query(
        &self,
        query: protocol::Query,
    ) -> Result<protocol::Snapshot<protocol::QueryResult>, tonic::Status> {
        self.endpoint
            .execute_query(self.client_id.clone(), query)
            .await
            .map_err(status)
    }

    async fn subscription(
        &self,
        scope: protocol::SubscriptionScope,
    ) -> Result<protocol::Snapshot<protocol::SubscriptionView>, tonic::Status> {
        self.endpoint
            .execute_subscription(self.client_id.clone(), scope)
            .await
            .map_err(status)
    }

    fn discord_management(&self) -> Result<Arc<dyn DiscordManagement>, tonic::Status> {
        self.discord_management.clone().ok_or_else(|| {
            tonic::Status::unimplemented("Discord management is unavailable in this service")
        })
    }

    fn discord_mutation_key(
        options: Option<api::MutationOptions>,
    ) -> Result<protocol::IdempotencyKey, tonic::Status> {
        let options =
            options.ok_or_else(|| tonic::Status::invalid_argument("mutation is required"))?;
        if options.idempotency_key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "mutation.idempotency_key is required",
            ));
        }
        if options.expected_revision.is_some() {
            return Err(tonic::Status::invalid_argument(
                "Discord management mutations do not accept expected_revision",
            ));
        }
        Ok(protocol::IdempotencyKey::new(options.idempotency_key))
    }
}

type ApiStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

fn status(error: protocol::ServiceError) -> tonic::Status {
    let code = match error.code {
        protocol::ErrorCode::InvalidRequest | protocol::ErrorCode::UnsupportedVersion => {
            tonic::Code::InvalidArgument
        }
        protocol::ErrorCode::NotFound => tonic::Code::NotFound,
        protocol::ErrorCode::Conflict | protocol::ErrorCode::ResyncRequired => tonic::Code::Aborted,
        protocol::ErrorCode::PermissionDenied => tonic::Code::PermissionDenied,
        protocol::ErrorCode::ProviderUnavailable => tonic::Code::FailedPrecondition,
        protocol::ErrorCode::CapabilityUnsupported => tonic::Code::Unimplemented,
        protocol::ErrorCode::Internal => tonic::Code::Internal,
    };
    tonic::Status::new(code, error.message)
}

fn discord_integration(value: protocol::DiscordIntegrationView) -> api::DiscordIntegration {
    let runtime_state = match value.runtime_state {
        protocol::DiscordRuntimeState::Disabled => api::discord_integration::RuntimeState::Disabled,
        protocol::DiscordRuntimeState::Stopped => api::discord_integration::RuntimeState::Stopped,
        protocol::DiscordRuntimeState::Running => api::discord_integration::RuntimeState::Running,
        protocol::DiscordRuntimeState::Failed => api::discord_integration::RuntimeState::Failed,
    };
    api::DiscordIntegration {
        enabled: value.enabled,
        configuration_complete: value.configuration_complete,
        token_configured: value.token_configured,
        chat_channel_id: value.chat_channel_id,
        agent_channel_id: value.agent_channel_id,
        primary_user_id: value.primary_user_id,
        runtime_state: runtime_state.into(),
        runtime_error: value.runtime_error,
    }
}

fn advertise_capability(
    value: protocol::ServiceCapability,
    discord_management_available: bool,
) -> bool {
    value != protocol::ServiceCapability::DiscordManagement || discord_management_available
}

fn prompt(value: Option<api::PromptInput>) -> Result<protocol::PromptInput, tonic::Status> {
    let value = value.ok_or_else(|| tonic::Status::invalid_argument("prompt is required"))?;
    Ok(protocol::PromptInput {
        text: value.text,
        attachments: value
            .attachments
            .into_iter()
            .map(protocol_attachment)
            .collect::<Result<_, _>>()?,
    })
}

fn protocol_attachment(
    value: api::PromptAttachment,
) -> Result<protocol::PromptAttachment, tonic::Status> {
    use api::prompt_attachment::Source;
    match value
        .source
        .ok_or_else(|| tonic::Status::invalid_argument("attachment source is required"))?
    {
        Source::ArtifactId(id) => Ok(protocol::PromptAttachment::Artifact {
            artifact_id: protocol::ArtifactId::from(id),
            label: value.label,
        }),
        Source::LocalFile(path) => Ok(protocol::PromptAttachment::LocalFile {
            label: value.label,
            path,
        }),
        Source::InlineImage(image) => Ok(protocol::PromptAttachment::InlineImage {
            label: value.label,
            media_type: image.media_type,
            data: image.data,
        }),
    }
}

fn model_options(value: Option<api::ModelOptions>) -> protocol::ModelOptions {
    value.map_or_else(protocol::ModelOptions::default, |value| {
        protocol::ModelOptions {
            reasoning_effort: value.reasoning_effort,
            fast_mode: value.fast_mode,
        }
    })
}

fn session_tools(
    value: Option<api::SessionToolConfiguration>,
) -> Option<protocol::SessionToolConfiguration> {
    value.map(|value| protocol::SessionToolConfiguration {
        tools: value
            .tools
            .into_iter()
            .map(|tool| protocol::ExternalToolDefinition {
                name: tool.name,
                description: tool.description,
                input_schema_json: tool.input_schema_json,
            })
            .collect(),
        replace_builtin_tools: value.replace_builtin_tools,
        allowed_builtin_tools: (!value.allowed_builtin_tools.is_empty())
            .then_some(value.allowed_builtin_tools),
    })
}

fn bridge_lifecycle(value: i32) -> Result<protocol::BridgeLifecycle, tonic::Status> {
    match api::BridgeLifecycle::try_from(value).unwrap_or(api::BridgeLifecycle::Unspecified) {
        api::BridgeLifecycle::Open => Ok(protocol::BridgeLifecycle::Open),
        api::BridgeLifecycle::Archived => Ok(protocol::BridgeLifecycle::Archived),
        api::BridgeLifecycle::Unspecified => Err(tonic::Status::invalid_argument(
            "bridge lifecycle is required",
        )),
    }
}

fn orchestrator_kind(value: i32) -> Result<protocol::OrchestratorKind, tonic::Status> {
    match api::OrchestratorKind::try_from(value).unwrap_or(api::OrchestratorKind::Unspecified) {
        api::OrchestratorKind::Chat => Ok(protocol::OrchestratorKind::Chat),
        api::OrchestratorKind::Agent => Ok(protocol::OrchestratorKind::Agent),
        api::OrchestratorKind::Unspecified => Err(tonic::Status::invalid_argument(
            "orchestrator kind is required",
        )),
    }
}

fn bridge_intent(
    value: Option<api::SessionBridgeIntent>,
) -> Result<Option<protocol::SessionBridgeIntent>, tonic::Status> {
    value
        .map(|value| {
            Ok(protocol::SessionBridgeIntent {
                kind: orchestrator_kind(value.kind)?,
                lifecycle: bridge_lifecycle(value.lifecycle)?,
                display_title: value.display_title,
            })
        })
        .transpose()
}

fn mcp_grant(
    value: Option<api::McpSessionGrant>,
) -> Result<Option<protocol::McpSessionGrant>, tonic::Status> {
    value
        .map(|value| {
            let surface = match api::McpSessionSurface::try_from(value.surface)
                .unwrap_or(api::McpSessionSurface::Unspecified)
            {
                api::McpSessionSurface::Chat => Some(protocol::McpSessionSurface::Chat),
                api::McpSessionSurface::CodingAgent => {
                    Some(protocol::McpSessionSurface::CodingAgent)
                }
                api::McpSessionSurface::Unspecified => None,
            };
            Ok(protocol::McpSessionGrant {
                surface,
                server_ids: value.server_ids,
            })
        })
        .transpose()
}

fn mcp_grants(value: Option<api::McpGrantPolicy>) -> protocol::McpGrantPolicy {
    value.map_or_else(protocol::McpGrantPolicy::default, |value| {
        protocol::McpGrantPolicy {
            chat: value.chat,
            coding_agent: value.coding_agent,
            archetype_slugs: value.archetype_slugs,
        }
    })
}

fn mcp_server_input(
    value: Option<api::McpServerInput>,
) -> Result<protocol::McpServerInput, tonic::Status> {
    let value = value.ok_or_else(|| tonic::Status::invalid_argument("MCP server is required"))?;
    Ok(protocol::McpServerInput {
        id: value.id,
        display_name: value.display_name,
        endpoint: value.endpoint,
        transport: value.transport,
        enabled: value.enabled,
        auth_kind: value.auth_kind,
        credential_required: value.credential_required,
        protocol_version: value.protocol_version,
        provenance_url: value.provenance_url,
        provenance_version: value.provenance_version,
        provenance_commit: value.provenance_commit,
        provenance_sha256: value.provenance_sha256,
        license_evidence: value.license_evidence,
        timeout_ms: value.timeout_ms,
        max_response_bytes: value.max_response_bytes,
        artifact_semantics: value.artifact_semantics,
        template_id: value.template_id,
    })
}

fn model_target(value: Option<api::ModelTarget>) -> Result<protocol::ModelTarget, tonic::Status> {
    use api::model_target::Target;
    match value
        .and_then(|value| value.target)
        .ok_or_else(|| tonic::Status::invalid_argument("model target is required"))?
    {
        Target::ProviderDefault(id) => Ok(protocol::ModelTarget::ProviderDefault {
            provider_id: protocol::ProviderId::from(id),
        }),
        Target::SessionId(id) => Ok(protocol::ModelTarget::Session {
            session_id: protocol::SessionId::from(id),
        }),
        Target::AgentSessionId(id) => Ok(protocol::ModelTarget::AgentSession {
            agent_session_id: protocol::AgentSessionId::from(id),
        }),
        Target::Vision(true) => Ok(protocol::ModelTarget::Vision),
        Target::Vision(false) => Err(tonic::Status::invalid_argument(
            "vision target must be true",
        )),
    }
}

fn agent_input(
    value: Option<api::AgentDefinitionInput>,
) -> Result<protocol::AgentDefinitionInput, tonic::Status> {
    let value = value.ok_or_else(|| tonic::Status::invalid_argument("definition is required"))?;
    Ok(protocol::AgentDefinitionInput {
        slug: value.slug,
        description: value.description,
        system_prompt: value.system_prompt,
        first_message: value.first_message,
        model: value.model_id.map(protocol::ModelId::from),
        fallback_models: value
            .fallback_models
            .into_iter()
            .map(protocol::ModelId::from)
            .collect(),
        fast_mode: value.fast_mode,
        // Trimmed to `None`: an empty string is a client that sent the field without a level in it,
        // which means the model's default, not a level named "".
        reasoning_effort: value
            .reasoning_effort
            .map(|effort| effort.trim().to_owned())
            .filter(|effort| !effort.is_empty()),
        ownership: value.ownership,
        enabled: value.enabled.unwrap_or(true),
        allowed_capabilities: value.allowed_capabilities,
        denied_capabilities: value.denied_capabilities,
        allowed_tools: value.allowed_tools,
        denied_tools: value.denied_tools,
        tool_profile: value.tool_profile,
        task_shape: value.task_shape,
        output_contract: value.output_contract,
        timeout_seconds: value.timeout_seconds,
        poll_interval_ms: value.poll_interval_ms,
        max_turns: value.max_turns,
        max_concurrency: value.max_concurrency,
        fallback_policy: value.fallback_policy,
        can_delegate: value.can_delegate,
        max_delegation_depth: value.max_delegation_depth,
        require_parent_attribution: value.require_parent_attribution.unwrap_or(true),
    })
}

fn settings_patch(
    value: Option<api::SettingsPatch>,
) -> Result<protocol::SettingsPatch, tonic::Status> {
    use api::settings_patch::Patch;
    match value
        .and_then(|value| value.patch)
        .ok_or_else(|| tonic::Status::invalid_argument("settings patch is required"))?
    {
        Patch::Web(value) => Ok(protocol::SettingsPatch::Web {
            backend: value.backend,
            credential: value.credential.map(protocol::CredentialInput),
        }),
        Patch::Memory(value) => Ok(protocol::SettingsPatch::Memory {
            backend: value.backend,
            executable: value.executable,
            global_bank: value.global_bank,
            data_directory: value.data_directory,
        }),
        Patch::Vision(value) => Ok(protocol::SettingsPatch::Vision {
            model_id: if value.clear_model {
                None
            } else {
                value.model_id.map(protocol::ModelId::from)
            },
        }),
        Patch::TerminalImages(value) => {
            Ok(protocol::SettingsPatch::TerminalImages { mode: value.mode })
        }
        Patch::InvocationTelemetry(value) => Ok(protocol::SettingsPatch::InvocationTelemetry {
            enabled: value.enabled,
        }),
    }
}

macro_rules! command_rpc {
    ($name:ident, $request:ty, $input:ident, $command:expr) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            request: tonic::Request<$request>,
        ) -> Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<tonic::Response<api::MutationResult>, tonic::Status>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let mut $input = request.into_inner();
                let options = $input.mutation.take();
                self.mutate(options, $command).await
            })
        }
    };
}

macro_rules! try_command_rpc {
    ($name:ident, $request:ty, $input:ident, $command:expr) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            request: tonic::Request<$request>,
        ) -> Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<tonic::Response<api::MutationResult>, tonic::Status>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let mut $input = request.into_inner();
                let options = $input.mutation.take();
                let command = $command?;
                self.mutate(options, command).await
            })
        }
    };
}

#[tonic::async_trait]
impl api::nakode_service_server::NakodeService for GrpcService {
    async fn get_workspace(
        &self,
        request: tonic::Request<api::GetWorkspaceRequest>,
    ) -> Result<tonic::Response<api::WorkspaceSnapshot>, tonic::Status> {
        let request = request.into_inner();
        let result = self
            .query(protocol::Query::Bootstrap {
                workspace: request.workspace,
                session_id: request.session_id.map(protocol::SessionId::from),
            })
            .await?;
        let protocol::QueryResult::Bootstrap(value) = result.value else {
            return Err(tonic::Status::internal("unexpected workspace response"));
        };
        Ok(tonic::Response::new(api::WorkspaceSnapshot {
            cursor: Some(cursor(&result.cursor)),
            state: Some(workspace(*value)),
        }))
    }

    type WatchWorkspaceStream = ApiStream<api::WorkspaceSnapshot>;
    async fn watch_workspace(
        &self,
        request: tonic::Request<api::WatchWorkspaceRequest>,
    ) -> Result<tonic::Response<Self::WatchWorkspaceStream>, tonic::Status> {
        let scope = protocol::SubscriptionScope::Workspace {
            workspace_id: protocol::WorkspaceId::from(request.into_inner().workspace_id),
        };
        let publications = self.endpoint.subscribe_publications();
        let initial = self.subscription(scope.clone()).await?;
        let protocol::SubscriptionView::Workspace(value) = initial.value else {
            return Err(tonic::Status::internal("unexpected workspace subscription"));
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        sender
            .send(Ok(api::WorkspaceSnapshot {
                cursor: Some(cursor(&initial.cursor)),
                state: Some(workspace(*value)),
            }))
            .await
            .map_err(|_| tonic::Status::cancelled("watch closed"))?;
        spawn_workspace_watch(self.clone(), scope, publications, sender);
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            receiver,
        ))))
    }

    command_rpc!(
        reload_workspace,
        api::ReloadWorkspaceRequest,
        input,
        protocol::Command::ReloadWorkspace {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            session_id: protocol::SessionId::from(input.session_id)
        }
    );

    async fn get_soul(
        &self,
        request: tonic::Request<api::GetSoulRequest>,
    ) -> Result<tonic::Response<api::SoulDocument>, tonic::Status> {
        let request = request.into_inner();
        let result = self
            .query(protocol::Query::GetSoul {
                workspace_id: protocol::WorkspaceId::from(request.workspace_id),
            })
            .await?;
        let protocol::QueryResult::SoulDocument(value) = result.value else {
            return Err(tonic::Status::internal("unexpected Soul response"));
        };
        let source = match value.source.as_str() {
            "file" => api::soul_document::Source::File,
            "missing" => api::soul_document::Source::Missing,
            _ => api::soul_document::Source::Unspecified,
        };
        Ok(tonic::Response::new(api::SoulDocument {
            workspace_id: value.workspace_id.to_string(),
            content: value.content,
            path: value.path,
            source: source.into(),
            exists: value.exists,
            digest: value.digest,
        }))
    }

    command_rpc!(
        save_soul,
        api::SaveSoulRequest,
        input,
        protocol::Command::SaveSoul {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            content: input.content,
            expected_digest: input.expected_digest,
        }
    );
    async fn get_mcp_management(
        &self,
        request: tonic::Request<api::GetMcpManagementRequest>,
    ) -> Result<tonic::Response<api::McpManagement>, tonic::Status> {
        let result = self
            .query(protocol::Query::GetMcpManagement {
                workspace_id: protocol::WorkspaceId::from(request.into_inner().workspace_id),
            })
            .await?;
        let protocol::QueryResult::McpManagement(value) = result.value else {
            return Err(tonic::Status::internal(
                "unexpected MCP management response",
            ));
        };
        Ok(tonic::Response::new(mcp_management(value)))
    }

    command_rpc!(
        save_mcp_server,
        api::SaveMcpServerRequest,
        input,
        protocol::Command::SaveMcpServer {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server: mcp_server_input(input.server)?,
            grants: mcp_grants(input.grants),
        }
    );
    command_rpc!(
        delete_mcp_server,
        api::DeleteMcpServerRequest,
        input,
        protocol::Command::DeleteMcpServer {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id
        }
    );
    command_rpc!(
        set_mcp_server_enabled,
        api::SetMcpServerEnabledRequest,
        input,
        protocol::Command::SetMcpServerEnabled {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id,
            enabled: input.enabled
        }
    );
    command_rpc!(
        refresh_mcp_server,
        api::RefreshMcpServerRequest,
        input,
        protocol::Command::RefreshMcpServer {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id
        }
    );
    command_rpc!(
        set_mcp_server_credential,
        api::SetMcpServerCredentialRequest,
        input,
        protocol::Command::SetMcpServerCredential {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id,
            kind: input.kind,
            credential: protocol::CredentialInput(input.credential)
        }
    );
    command_rpc!(
        clear_mcp_server_credential,
        api::ClearMcpServerCredentialRequest,
        input,
        protocol::Command::ClearMcpServerCredential {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id
        }
    );
    command_rpc!(
        set_mcp_server_grants,
        api::SetMcpServerGrantsRequest,
        input,
        protocol::Command::SetMcpServerGrants {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            server_id: input.server_id,
            grants: mcp_grants(input.grants)
        }
    );

    async fn get_discord_integration(
        &self,
        _request: tonic::Request<api::GetDiscordIntegrationRequest>,
    ) -> Result<tonic::Response<api::DiscordIntegration>, tonic::Status> {
        let value = self.discord_management()?.get().await.map_err(status)?;
        Ok(tonic::Response::new(discord_integration(value)))
    }

    async fn save_discord_integration(
        &self,
        request: tonic::Request<api::SaveDiscordIntegrationRequest>,
    ) -> Result<tonic::Response<api::DiscordIntegration>, tonic::Status> {
        let input = request.into_inner();
        let key = Self::discord_mutation_key(input.mutation)?;
        let value = self
            .discord_management()?
            .mutate(
                key,
                DiscordManagementMutation::Save(protocol::DiscordIntegrationInput {
                    chat_channel_id: input.chat_channel_id,
                    agent_channel_id: input.agent_channel_id,
                    primary_user_id: input.primary_user_id,
                    bot_token: input.bot_token.map(protocol::CredentialInput),
                }),
            )
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(discord_integration(value)))
    }

    async fn set_discord_integration_enabled(
        &self,
        request: tonic::Request<api::SetDiscordIntegrationEnabledRequest>,
    ) -> Result<tonic::Response<api::DiscordIntegration>, tonic::Status> {
        let input = request.into_inner();
        let key = Self::discord_mutation_key(input.mutation)?;
        let value = self
            .discord_management()?
            .mutate(key, DiscordManagementMutation::SetEnabled(input.enabled))
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(discord_integration(value)))
    }

    async fn restart_discord_integration(
        &self,
        request: tonic::Request<api::RestartDiscordIntegrationRequest>,
    ) -> Result<tonic::Response<api::DiscordIntegration>, tonic::Status> {
        let input = request.into_inner();
        let key = Self::discord_mutation_key(input.mutation)?;
        let value = self
            .discord_management()?
            .mutate(key, DiscordManagementMutation::Restart)
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(discord_integration(value)))
    }

    command_rpc!(
        create_session,
        api::CreateSessionRequest,
        input,
        protocol::Command::CreateSession {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            working_directory: input.working_directory,
            title: input.title,
            model_id: input.model_id.map(protocol::ModelId::from),
            options: model_options(input.options),
            tools: session_tools(input.tools),
            initial_instructions: input.initial_instructions,
            bridge: bridge_intent(input.bridge)?,
            mcp_grant: mcp_grant(input.mcp_grant)?,
            profile_id: input.profile_id,
            disabled_skill_ids: Vec::new(),
        }
    );
    command_rpc!(
        open_session,
        api::OpenSessionRequest,
        input,
        protocol::Command::OpenSession {
            session_id: protocol::SessionId::from(input.session_id),
            tools: session_tools(input.tools),
            mcp_grant: mcp_grant(input.mcp_grant)?,
        }
    );

    command_rpc!(
        set_session_bridge_lifecycle,
        api::SetSessionBridgeLifecycleRequest,
        input,
        protocol::Command::SetSessionBridgeLifecycle {
            session_id: protocol::SessionId::from(input.session_id),
            lifecycle: bridge_lifecycle(input.lifecycle)?,
        }
    );
    command_rpc!(
        set_workspace_bridge_lifecycle,
        api::SetWorkspaceBridgeLifecycleRequest,
        input,
        protocol::Command::SetWorkspaceBridgeLifecycle {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            lifecycle: bridge_lifecycle(input.lifecycle)?,
        }
    );
    command_rpc!(
        bind_session_bridge_thread,
        api::BindSessionBridgeThreadRequest,
        input,
        protocol::Command::BindSessionBridgeThread {
            session_id: protocol::SessionId::from(input.session_id),
            transport: input.transport,
            external_parent_id: input.external_parent_id,
            external_thread_id: input.external_thread_id,
        }
    );
    command_rpc!(
        clear_session_bridge_thread,
        api::ClearSessionBridgeThreadRequest,
        input,
        protocol::Command::ClearSessionBridgeThread {
            session_id: protocol::SessionId::from(input.session_id),
            transport: input.transport,
            external_thread_id: input.external_thread_id,
        }
    );
    try_command_rpc!(
        prepare_bridge_delivery,
        api::PrepareBridgeDeliveryRequest,
        input,
        (|| -> Result<protocol::Command, tonic::Status> {
            Ok(protocol::Command::PrepareBridgeDelivery {
                session_id: protocol::SessionId::from(input.session_id),
                projection_kind: bridge_projection_kind(input.projection_kind)?,
                turn_id: protocol::TurnId::from(input.turn_id),
                expected_last_projected: input
                    .expected_last_projected
                    .map(bridge_projection)
                    .transpose()?,
                body_sha256: input.body_sha256,
                part_count: input.part_count,
            })
        })()
    );
    try_command_rpc!(
        complete_bridge_delivery_part,
        api::CompleteBridgeDeliveryPartRequest,
        input,
        bridge_projection_kind(input.projection_kind).map(|projection_kind| {
            protocol::Command::CompleteBridgeDeliveryPart {
                session_id: protocol::SessionId::from(input.session_id),
                projection_kind,
                turn_id: protocol::TurnId::from(input.turn_id),
                part_index: input.part_index,
                external_message_id: input.external_message_id,
            }
        })
    );
    try_command_rpc!(
        finalize_bridge_delivery,
        api::FinalizeBridgeDeliveryRequest,
        input,
        bridge_projection_kind(input.projection_kind).map(|projection_kind| {
            protocol::Command::FinalizeBridgeDelivery {
                session_id: protocol::SessionId::from(input.session_id),
                projection_kind,
                turn_id: protocol::TurnId::from(input.turn_id),
            }
        })
    );
    command_rpc!(
        set_bridge_live_message,
        api::SetBridgeLiveMessageRequest,
        input,
        protocol::Command::SetBridgeLiveMessage {
            session_id: protocol::SessionId::from(input.session_id),
            turn_id: input.turn_id.map(protocol::TurnId::from),
            external_message_id: input.external_message_id,
            clear_active_source_message_id: input.clear_active_source_message_id,
        }
    );
    async fn continue_session_from_bridge(
        &self,
        request: tonic::Request<api::ContinueSessionFromBridgeRequest>,
    ) -> Result<tonic::Response<api::ContinueSessionFromBridgeResponse>, tonic::Status> {
        let mut input = request.into_inner();
        let options = input.mutation.take();
        let prompt = prompt(input.prompt)?;
        let result = self
            .execute_mutation(
                options,
                protocol::Command::ContinueSessionFromBridge {
                    session_id: protocol::SessionId::from(input.session_id),
                    transport: input.transport,
                    external_thread_id: input.external_thread_id,
                    external_event_id: input.external_event_id,
                    source_message_id: input.source_message_id,
                    prompt,
                    consume_as_busy: input.consume_as_busy,
                },
            )
            .await?;
        let wire_disposition = |disposition| match disposition {
            protocol::BridgeContinuationDisposition::Accepted => {
                api::BridgeContinuationDisposition::Accepted
            }
            protocol::BridgeContinuationDisposition::Duplicate => {
                api::BridgeContinuationDisposition::Duplicate
            }
            protocol::BridgeContinuationDisposition::Busy => {
                api::BridgeContinuationDisposition::Busy
            }
        };
        let disposition = result
            .bridge_continuation
            .map(wire_disposition)
            .ok_or_else(|| {
                tonic::Status::internal("bridge continuation result omitted its disposition")
            })?;
        let replayed_disposition = result
            .replayed_bridge_continuation
            .map(wire_disposition)
            .map(|disposition| disposition as i32);
        Ok(tonic::Response::new(
            api::ContinueSessionFromBridgeResponse {
                mutation: Some(api::MutationResult {
                    resource_id: result.resource_id,
                    revision: result.revision,
                }),
                disposition: disposition as i32,
                replayed_disposition,
                replayed_source_active: result.replayed_bridge_source_active,
            },
        ))
    }

    async fn list_sessions(
        &self,
        request: tonic::Request<api::ListSessionsRequest>,
    ) -> Result<tonic::Response<api::ListSessionsResponse>, tonic::Status> {
        let request = request.into_inner();
        let result = self
            .query(protocol::Query::ListSessions {
                workspace_id: protocol::WorkspaceId::from(request.workspace_id),
                limit: request.limit,
            })
            .await?;
        let protocol::QueryResult::Sessions(inventory) = result.value else {
            return Err(tonic::Status::internal("unexpected sessions response"));
        };
        Ok(tonic::Response::new(api::ListSessionsResponse {
            sessions: inventory
                .sessions
                .into_iter()
                .map(session_summary)
                .collect(),
            complete: inventory.complete,
        }))
    }

    async fn get_session(
        &self,
        request: tonic::Request<api::GetSessionRequest>,
    ) -> Result<tonic::Response<api::SessionSnapshot>, tonic::Status> {
        let result = self
            .query(protocol::Query::GetSession {
                session_id: protocol::SessionId::from(request.into_inner().session_id),
            })
            .await?;
        let protocol::QueryResult::Session(value) = result.value else {
            return Err(tonic::Status::internal("unexpected session response"));
        };
        Ok(tonic::Response::new(api::SessionSnapshot {
            cursor: Some(cursor(&result.cursor)),
            state: Some(session(*value)),
        }))
    }

    type WatchSessionStream = ApiStream<api::SessionSnapshot>;
    async fn watch_session(
        &self,
        request: tonic::Request<api::WatchSessionRequest>,
    ) -> Result<tonic::Response<Self::WatchSessionStream>, tonic::Status> {
        let scope = protocol::SubscriptionScope::Session {
            session_id: protocol::SessionId::from(request.into_inner().session_id),
        };
        let publications = self.endpoint.subscribe_publications();
        let initial = self.subscription(scope.clone()).await?;
        let protocol::SubscriptionView::Session(value) = initial.value else {
            return Err(tonic::Status::internal("unexpected session subscription"));
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        sender
            .send(Ok(api::SessionSnapshot {
                cursor: Some(cursor(&initial.cursor)),
                state: Some(session(*value)),
            }))
            .await
            .map_err(|_| tonic::Status::cancelled("watch closed"))?;
        spawn_session_watch(self.clone(), scope, publications, sender);
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            receiver,
        ))))
    }

    try_command_rpc!(
        send_prompt,
        api::SendPromptRequest,
        input,
        prompt(input.prompt).map(|prompt| protocol::Command::SendPrompt {
            session_id: protocol::SessionId::from(input.session_id),
            prompt
        })
    );
    try_command_rpc!(
        enqueue_prompt,
        api::EnqueuePromptRequest,
        input,
        prompt(input.prompt).map(|prompt| protocol::Command::EnqueuePrompt {
            session_id: protocol::SessionId::from(input.session_id),
            prompt
        })
    );
    command_rpc!(
        remove_queued_prompt,
        api::RemoveQueuedPromptRequest,
        input,
        protocol::Command::RemoveQueuedPrompt {
            session_id: protocol::SessionId::from(input.session_id),
            prompt_id: protocol::PromptId::from(input.prompt_id)
        }
    );
    command_rpc!(
        steer_queued_prompt,
        api::SteerQueuedPromptRequest,
        input,
        protocol::Command::SteerQueuedPrompt {
            session_id: protocol::SessionId::from(input.session_id),
            prompt_id: protocol::PromptId::from(input.prompt_id)
        }
    );
    command_rpc!(
        steer_turn,
        api::SteerTurnRequest,
        input,
        protocol::Command::SteerTurn {
            turn_id: protocol::TurnId::from(input.turn_id),
            text: input.text
        }
    );
    command_rpc!(
        cancel_turn,
        api::CancelTurnRequest,
        input,
        protocol::Command::CancelTurn {
            turn_id: protocol::TurnId::from(input.turn_id)
        }
    );
    command_rpc!(
        cancel_session_work,
        api::CancelSessionWorkRequest,
        input,
        protocol::Command::CancelSessionWork {
            session_id: protocol::SessionId::from(input.session_id)
        }
    );
    command_rpc!(
        compact_context,
        api::CompactContextRequest,
        input,
        protocol::Command::CompactContext {
            agent_session_id: protocol::AgentSessionId::from(input.agent_session_id)
        }
    );

    async fn resolve_interaction(
        &self,
        request: tonic::Request<api::ResolveInteractionRequest>,
    ) -> Result<tonic::Response<api::MutationResult>, tonic::Status> {
        let mut input = request.into_inner();
        let options = input.mutation.take();
        let resolution = match api::InteractionResolutionKind::try_from(input.resolution)
            .map_err(|_| tonic::Status::invalid_argument("invalid interaction resolution"))?
        {
            api::InteractionResolutionKind::ApproveOnce => {
                protocol::InteractionResolution::ApproveOnce
            }
            api::InteractionResolutionKind::ApproveForSession => {
                protocol::InteractionResolution::ApproveForSession
            }
            api::InteractionResolutionKind::Decline => protocol::InteractionResolution::Decline,
            api::InteractionResolutionKind::Answer => {
                if input.answers.is_empty() {
                    protocol::InteractionResolution::Answer {
                        option_ids: input.option_ids,
                    }
                } else {
                    if !input.option_ids.is_empty() {
                        return Err(tonic::Status::invalid_argument(
                            "use either legacy option_ids or structured answers, not both",
                        ));
                    }
                    protocol::InteractionResolution::AnswerQuestions {
                        answers: input
                            .answers
                            .into_iter()
                            .map(|answer| protocol::QuestionResponse {
                                question_id: answer.question_id,
                                option_ids: answer.option_ids,
                                text: answer.text,
                            })
                            .collect(),
                    }
                }
            }
            api::InteractionResolutionKind::Unspecified => {
                return Err(tonic::Status::invalid_argument(
                    "interaction resolution is required",
                ));
            }
        };
        self.mutate(
            options,
            protocol::Command::ResolveInteraction {
                interaction_id: protocol::InteractionId::from(input.interaction_id),
                resolution,
            },
        )
        .await
    }

    async fn configure_session_tools(
        &self,
        request: tonic::Request<api::ConfigureSessionToolsRequest>,
    ) -> Result<tonic::Response<api::MutationResult>, tonic::Status> {
        let mut input = request.into_inner();
        let options = input.mutation.take();
        self.mutate(
            options,
            protocol::Command::ConfigureSessionTools {
                session_id: protocol::SessionId::from(input.session_id),
                tools: input
                    .tools
                    .into_iter()
                    .map(|tool| protocol::ExternalToolDefinition {
                        name: tool.name,
                        description: tool.description,
                        input_schema_json: tool.input_schema_json,
                    })
                    .collect(),
                replace_builtin_tools: input.replace_builtin_tools,
            },
        )
        .await
    }

    command_rpc!(
        submit_external_tool_result,
        api::SubmitExternalToolResultRequest,
        input,
        protocol::Command::SubmitExternalToolResult {
            session_id: protocol::SessionId::from(input.session_id),
            call_id: input.call_id,
            output: input.output,
            failed: input.failed
        }
    );

    command_rpc!(
        run_shell,
        api::RunShellRequest,
        input,
        protocol::Command::RunShell {
            session_id: protocol::SessionId::from(input.session_id),
            command: input.command
        }
    );
    try_command_rpc!(
        select_model,
        api::SelectModelRequest,
        input,
        model_target(input.target).map(|target| protocol::Command::SelectModel {
            target,
            model_id: protocol::ModelId::from(input.model_id),
            options: model_options(input.options)
        })
    );
    command_rpc!(
        set_provider_enabled,
        api::SetProviderEnabledRequest,
        input,
        protocol::Command::SetProviderEnabled {
            provider_id: protocol::ProviderId::from(input.provider_id),
            enabled: input.enabled
        }
    );
    command_rpc!(
        set_provider_model_filter,
        api::SetProviderModelFilterRequest,
        input,
        protocol::Command::SetProviderModelFilter {
            provider_id: protocol::ProviderId::from(input.provider_id),
            enabled: input.enabled,
            selected_model_ids: input
                .selected_model_ids
                .into_iter()
                .map(protocol::ModelId::from)
                .collect()
        }
    );
    command_rpc!(
        set_skill_enabled,
        api::SetSkillEnabledRequest,
        input,
        protocol::Command::SetSkillEnabled {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            profile_id: input.profile_id,
            skill_id: input.skill_id,
            enabled: input.enabled,
        }
    );
    async fn list_skills(
        &self,
        request: tonic::Request<api::ListSkillsRequest>,
    ) -> Result<tonic::Response<api::SkillCatalogue>, tonic::Status> {
        let input = request.into_inner();
        let result = self
            .query(protocol::Query::ListSkills {
                workspace_id: protocol::WorkspaceId::from(input.workspace_id),
                profile_id: input.profile_id,
            })
            .await?;
        let protocol::QueryResult::Skills(value) = result.value else {
            return Err(tonic::Status::internal(
                "unexpected skill catalogue response",
            ));
        };
        Ok(tonic::Response::new(api::SkillCatalogue {
            skills: value
                .skills
                .into_iter()
                .map(|skill| api::Skill {
                    id: skill.id,
                    name: skill.name,
                    description: skill.description,
                    enabled: skill.enabled,
                    available: skill.available,
                })
                .collect(),
        }))
    }
    command_rpc!(
        begin_provider_authentication,
        api::BeginProviderAuthenticationRequest,
        input,
        protocol::Command::BeginProviderAuthentication {
            provider_id: protocol::ProviderId::from(input.provider_id)
        }
    );
    command_rpc!(
        set_provider_credential,
        api::SetProviderCredentialRequest,
        input,
        protocol::Command::SetProviderCredential {
            provider_id: protocol::ProviderId::from(input.provider_id),
            kind: input.kind,
            credential: protocol::CredentialInput(input.credential)
        }
    );
    command_rpc!(
        clear_provider_credential,
        api::ClearProviderCredentialRequest,
        input,
        protocol::Command::ClearProviderCredential {
            provider_id: protocol::ProviderId::from(input.provider_id)
        }
    );
    command_rpc!(
        reload_provider,
        api::ReloadProviderRequest,
        input,
        protocol::Command::ReloadProvider {
            provider_id: protocol::ProviderId::from(input.provider_id)
        }
    );
    try_command_rpc!(
        save_agent,
        api::SaveAgentRequest,
        input,
        agent_input(input.definition).map(|definition| protocol::Command::SaveAgent {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            definition,
            previous_slug: input.previous_slug
        })
    );
    command_rpc!(
        delete_agent,
        api::DeleteAgentRequest,
        input,
        protocol::Command::DeleteAgent {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            slug: input.slug
        }
    );
    command_rpc!(
        delete_session,
        api::DeleteSessionRequest,
        input,
        protocol::Command::DeleteSession {
            session_id: protocol::SessionId::from(input.session_id)
        }
    );
    try_command_rpc!(
        update_settings,
        api::UpdateSettingsRequest,
        input,
        settings_patch(input.patch).map(|patch| protocol::Command::UpdateSettings { patch })
    );
    command_rpc!(
        check_agent_browser,
        api::CheckAgentBrowserRequest,
        input,
        protocol::Command::CheckAgentBrowser {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id)
        }
    );
    command_rpc!(
        delegate,
        api::DelegateRequest,
        input,
        protocol::Command::Delegate {
            session_id: protocol::SessionId::from(input.session_id),
            agent_slug: input.agent_slug,
            task: input.task,
            parent_run_id: input.parent_run_id.map(protocol::RunId::from)
        }
    );

    async fn list_runs(
        &self,
        request: tonic::Request<api::ListRunsRequest>,
    ) -> Result<tonic::Response<api::ListRunsResponse>, tonic::Status> {
        let request = request.into_inner();
        let result = self
            .query(protocol::Query::ListRuns {
                session_id: protocol::SessionId::from(request.session_id),
                before: request.before_run_id.map(protocol::RunId::from),
                limit: request.limit,
            })
            .await?;
        let protocol::QueryResult::Runs(values) = result.value else {
            return Err(tonic::Status::internal("unexpected runs response"));
        };
        Ok(tonic::Response::new(api::ListRunsResponse {
            runs: values.runs.into_iter().map(run).collect(),
            has_earlier: values.has_earlier,
        }))
    }

    async fn get_run(
        &self,
        request: tonic::Request<api::GetRunRequest>,
    ) -> Result<tonic::Response<api::RunSnapshot>, tonic::Status> {
        let result = self
            .query(protocol::Query::GetRun {
                run_id: protocol::RunId::from(request.into_inner().run_id),
            })
            .await?;
        let protocol::QueryResult::Run(value) = result.value else {
            return Err(tonic::Status::internal("unexpected run response"));
        };
        Ok(tonic::Response::new(api::RunSnapshot {
            cursor: Some(cursor(&result.cursor)),
            state: Some(run(*value)),
        }))
    }

    type WatchRunStream = ApiStream<api::RunSnapshot>;
    async fn watch_run(
        &self,
        request: tonic::Request<api::WatchRunRequest>,
    ) -> Result<tonic::Response<Self::WatchRunStream>, tonic::Status> {
        let scope = protocol::SubscriptionScope::Run {
            run_id: protocol::RunId::from(request.into_inner().run_id),
        };
        let publications = self.endpoint.subscribe_publications();
        let initial = self.subscription(scope.clone()).await?;
        let protocol::SubscriptionView::Run(value) = initial.value else {
            return Err(tonic::Status::internal("unexpected run subscription"));
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        sender
            .send(Ok(api::RunSnapshot {
                cursor: Some(cursor(&initial.cursor)),
                state: Some(run(*value)),
            }))
            .await
            .map_err(|_| tonic::Status::cancelled("watch closed"))?;
        spawn_run_watch(self.clone(), scope, publications, sender);
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            receiver,
        ))))
    }

    command_rpc!(
        cancel_run,
        api::CancelRunRequest,
        input,
        protocol::Command::CancelRun {
            run_id: protocol::RunId::from(input.run_id)
        }
    );

    command_rpc!(
        continue_run,
        api::ContinueRunRequest,
        input,
        protocol::Command::ContinueRun {
            run_id: protocol::RunId::from(input.run_id),
            additional_turns: input.additional_turns,
        }
    );

    async fn get_transcript_page(
        &self,
        request: tonic::Request<api::GetTranscriptPageRequest>,
    ) -> Result<tonic::Response<api::TranscriptPage>, tonic::Status> {
        let request = request.into_inner();
        let query = match api::TranscriptOwnerKind::try_from(request.owner_kind)
            .map_err(|_| tonic::Status::invalid_argument("invalid transcript owner"))?
        {
            api::TranscriptOwnerKind::Session => protocol::Query::GetTranscriptPage {
                session_id: protocol::SessionId::from(request.owner_id),
                before: request.before_entry_id.map(protocol::EntryId::from),
                limit: request.limit,
            },
            api::TranscriptOwnerKind::Run => protocol::Query::GetRunTranscriptPage {
                run_id: protocol::RunId::from(request.owner_id),
                before: request.before_entry_id.map(protocol::EntryId::from),
                limit: request.limit,
            },
            api::TranscriptOwnerKind::Unspecified => {
                return Err(tonic::Status::invalid_argument(
                    "transcript owner is required",
                ));
            }
        };
        let result = self.query(query).await?;
        let protocol::QueryResult::Transcript(value) = result.value else {
            return Err(tonic::Status::internal("unexpected transcript response"));
        };
        Ok(tonic::Response::new(transcript(*value)))
    }

    async fn get_transcript_body_window(
        &self,
        request: tonic::Request<api::GetTranscriptBodyWindowRequest>,
    ) -> Result<tonic::Response<api::TranscriptBodyWindow>, tonic::Status> {
        let request = request.into_inner();
        let owner = match api::TranscriptOwnerKind::try_from(request.owner_kind)
            .map_err(|_| tonic::Status::invalid_argument("invalid transcript owner"))?
        {
            api::TranscriptOwnerKind::Session => protocol::TranscriptOwner::Session {
                session_id: protocol::SessionId::from(request.owner_id),
            },
            api::TranscriptOwnerKind::Run => protocol::TranscriptOwner::Run {
                run_id: protocol::RunId::from(request.owner_id),
            },
            api::TranscriptOwnerKind::Unspecified => {
                return Err(tonic::Status::invalid_argument(
                    "transcript owner is required",
                ));
            }
        };
        let result = self
            .query(protocol::Query::GetTranscriptBodyWindow {
                owner,
                entry_id: protocol::EntryId::from(request.entry_id),
                before_byte: request.before_byte,
                limit_bytes: request.limit_bytes,
            })
            .await?;
        let protocol::QueryResult::TranscriptBody(value) = result.value else {
            return Err(tonic::Status::internal(
                "unexpected transcript body response",
            ));
        };
        Ok(tonic::Response::new(transcript_body(value)))
    }

    async fn get_run_text_window(
        &self,
        request: tonic::Request<api::GetRunTextWindowRequest>,
    ) -> Result<tonic::Response<api::RunTextWindow>, tonic::Status> {
        let request = request.into_inner();
        let field = match api::RunTextField::try_from(request.field)
            .map_err(|_| tonic::Status::invalid_argument("invalid run text field"))?
        {
            api::RunTextField::Objective => protocol::RunTextField::Objective,
            api::RunTextField::LatestActivity => protocol::RunTextField::LatestActivity,
            api::RunTextField::Outcome => protocol::RunTextField::Outcome,
            api::RunTextField::Result => protocol::RunTextField::Result,
            api::RunTextField::Unspecified => {
                return Err(tonic::Status::invalid_argument(
                    "run text field is required",
                ));
            }
        };
        let result = self
            .query(protocol::Query::GetRunTextWindow {
                run_id: protocol::RunId::from(request.run_id),
                field,
                before_byte: request.before_byte,
                limit_bytes: request.limit_bytes,
            })
            .await?;
        let protocol::QueryResult::RunText(value) = result.value else {
            return Err(tonic::Status::internal("unexpected run text response"));
        };
        Ok(tonic::Response::new(run_text(value)))
    }

    async fn get_artifact(
        &self,
        request: tonic::Request<api::GetArtifactRequest>,
    ) -> Result<tonic::Response<api::Artifact>, tonic::Status> {
        let result = self
            .query(protocol::Query::GetArtifact {
                artifact_id: protocol::ArtifactId::from(request.into_inner().artifact_id),
            })
            .await?;
        let protocol::QueryResult::Artifact(value) = result.value else {
            return Err(tonic::Status::internal("unexpected artifact response"));
        };
        Ok(tonic::Response::new(artifact(value)))
    }

    async fn get_diagnostics(
        &self,
        request: tonic::Request<api::GetDiagnosticsRequest>,
    ) -> Result<tonic::Response<api::DiagnosticsReport>, tonic::Status> {
        let request = request.into_inner();
        let days = u16::try_from(request.days)
            .map_err(|_| tonic::Status::invalid_argument("days exceeds 65535"))?;
        let result = self
            .query(protocol::Query::GetDiagnostics {
                days,
                session_limit: request.session_limit,
                provider_id: request.provider_id.map(protocol::ProviderId::from),
            })
            .await?;
        let protocol::QueryResult::Diagnostics(value) = result.value else {
            return Err(tonic::Status::internal("unexpected diagnostics response"));
        };
        Ok(tonic::Response::new(diagnostics(*value)))
    }

    async fn get_invocation_summary(
        &self,
        _request: tonic::Request<api::GetInvocationSummaryRequest>,
    ) -> Result<tonic::Response<api::InvocationSummary>, tonic::Status> {
        let result = self.query(protocol::Query::GetInvocationSummary).await?;
        let protocol::QueryResult::InvocationSummary(value) = result.value else {
            return Err(tonic::Status::internal(
                "unexpected invocation summary response",
            ));
        };
        Ok(tonic::Response::new(invocation_summary(*value)))
    }

    async fn get_invocation_timeline(
        &self,
        request: tonic::Request<api::GetInvocationTimelineRequest>,
    ) -> Result<tonic::Response<api::InvocationTimeline>, tonic::Status> {
        let request = request.into_inner();
        let result = self
            .query(protocol::Query::GetInvocationTimeline {
                start_at_ms: request.start_at_ms,
                end_at_ms: request.end_at_ms,
                bucket_width_ms: request.bucket_width_ms,
            })
            .await?;
        let protocol::QueryResult::InvocationTimeline(value) = result.value else {
            return Err(tonic::Status::internal(
                "unexpected invocation timeline response",
            ));
        };
        Ok(tonic::Response::new(invocation_timeline(*value)))
    }

    async fn get_server_info(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<api::ServerInfo>, tonic::Status> {
        Ok(tonic::Response::new(api::ServerInfo {
            server_version: self.endpoint.server_version().to_owned(),
            api_version: "nakode.v1".to_owned(),
            capabilities: self
                .endpoint
                .capabilities()
                .supported
                .iter()
                .filter(|value| advertise_capability(**value, self.discord_management.is_some()))
                .map(|value| format!("{value:?}"))
                .collect(),
        }))
    }
}

fn spawn_workspace_watch(
    service: GrpcService,
    scope: protocol::SubscriptionScope,
    mut publications: tokio::sync::broadcast::Receiver<PublishedEvent>,
    sender: tokio::sync::mpsc::Sender<Result<api::WorkspaceSnapshot, tonic::Status>>,
) {
    tokio::spawn(async move {
        loop {
            match publications.recv().await {
                Ok(publication) if publication.scopes.contains(&scope) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Workspace(value) = snapshot.value
                            else {
                                return Err(tonic::Status::internal(
                                    "unexpected workspace subscription",
                                ));
                            };
                            Ok(api::WorkspaceSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(workspace(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Workspace(value) = snapshot.value
                            else {
                                return Err(tonic::Status::internal(
                                    "unexpected workspace subscription",
                                ));
                            };
                            Ok(api::WorkspaceSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(workspace(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn spawn_session_watch(
    service: GrpcService,
    scope: protocol::SubscriptionScope,
    mut publications: tokio::sync::broadcast::Receiver<PublishedEvent>,
    sender: tokio::sync::mpsc::Sender<Result<api::SessionSnapshot, tonic::Status>>,
) {
    tokio::spawn(async move {
        loop {
            match publications.recv().await {
                Ok(publication) if publication.scopes.contains(&scope) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Session(value) = snapshot.value else {
                                return Err(tonic::Status::internal(
                                    "unexpected session subscription",
                                ));
                            };
                            Ok(api::SessionSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(session(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Session(value) = snapshot.value else {
                                return Err(tonic::Status::internal(
                                    "unexpected session subscription",
                                ));
                            };
                            Ok(api::SessionSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(session(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn spawn_run_watch(
    service: GrpcService,
    scope: protocol::SubscriptionScope,
    mut publications: tokio::sync::broadcast::Receiver<PublishedEvent>,
    sender: tokio::sync::mpsc::Sender<Result<api::RunSnapshot, tonic::Status>>,
) {
    tokio::spawn(async move {
        loop {
            match publications.recv().await {
                Ok(publication) if publication.scopes.contains(&scope) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Run(value) = snapshot.value else {
                                return Err(tonic::Status::internal("unexpected run subscription"));
                            };
                            Ok(api::RunSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(run(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let update = service
                        .subscription(scope.clone())
                        .await
                        .and_then(|snapshot| {
                            let protocol::SubscriptionView::Run(value) = snapshot.value else {
                                return Err(tonic::Status::internal("unexpected run subscription"));
                            };
                            Ok(api::RunSnapshot {
                                cursor: Some(cursor(&snapshot.cursor)),
                                state: Some(run(*value)),
                            })
                        });
                    if sender.send(update).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

pub(crate) fn cursor(value: &protocol::Cursor) -> api::Cursor {
    api::Cursor {
        server_epoch: value.server_epoch.to_string(),
        sequence: value.sequence,
    }
}

pub(crate) fn mcp_management(value: protocol::McpManagementView) -> api::McpManagement {
    api::McpManagement {
        workspace_id: value.workspace_id.to_string(),
        servers: value
            .servers
            .into_iter()
            .map(|server| api::McpServer {
                id: server.id,
                workspace_id: server.workspace_id.to_string(),
                display_name: server.display_name,
                endpoint: server.endpoint,
                transport: server.transport,
                enabled: server.enabled,
                health: server.health,
                credential_required: server.credential_required,
                credential_configured: server.credential_configured,
                credential_kind: server.credential_kind,
                protocol_version: server.protocol_version,
                server_name: server.server_name,
                server_version: server.server_version,
                provenance_url: server.provenance_url,
                provenance_version: server.provenance_version,
                provenance_commit: server.provenance_commit,
                provenance_sha256: server.provenance_sha256,
                license_evidence: server.license_evidence,
                last_error: server.last_error,
                last_connected_at_ms: server.last_connected_at_ms,
                updated_at_ms: server.updated_at_ms,
                timeout_ms: server.timeout_ms,
                max_response_bytes: server.max_response_bytes,
                artifact_semantics: server.artifact_semantics,
                template_id: server.template_id,
                tools: server
                    .tools
                    .into_iter()
                    .map(|tool| api::McpTool {
                        remote_name: tool.remote_name,
                        exposed_name: tool.exposed_name,
                        description: tool.description,
                        input_schema_json: tool.input_schema_json,
                        app_only: tool.app_only,
                    })
                    .collect(),
                grants: Some(api::McpGrantPolicy {
                    chat: server.grants.chat,
                    coding_agent: server.grants.coding_agent,
                    archetype_slugs: server.grants.archetype_slugs,
                }),
            })
            .collect(),
        templates: value
            .templates
            .into_iter()
            .map(|template| api::McpTemplate {
                id: template.id,
                display_name: template.display_name,
                description: template.description,
                endpoint: template.endpoint,
                provenance_url: template.provenance_url,
                provenance_version: template.provenance_version,
                provenance_commit: template.provenance_commit,
                provenance_sha256: template.provenance_sha256,
                license_evidence: template.license_evidence,
                artifact_semantics: template.artifact_semantics,
                credential_required: template.credential_required,
            })
            .collect(),
    }
}

pub(crate) fn workspace(value: protocol::BootstrapView) -> api::WorkspaceState {
    api::WorkspaceState {
        workspace_id: value.workspace_id.to_string(),
        workspace_path: value.workspace_path,
        providers: value.providers.into_iter().map(provider).collect(),
        models: value.models.into_iter().map(model).collect(),
        agents: value.agents.into_iter().map(agent).collect(),
        skills: value
            .skills
            .into_iter()
            .map(|skill| api::Skill {
                id: skill.name.clone(),
                name: skill.name,
                description: skill.description,
                enabled: true,
                available: true,
            })
            .collect(),
        settings: Some(settings(value.settings)),
        sessions: value.sessions.into_iter().map(session_summary).collect(),
        active_session: value.active_session.map(session),
        session_bridges: value
            .session_bridges
            .into_iter()
            .map(session_bridge)
            .collect(),
    }
}

fn session_bridge(value: protocol::SessionBridgeView) -> api::SessionBridge {
    api::SessionBridge {
        session_id: value.session_id.to_string(),
        workspace_id: value.workspace_id.to_string(),
        kind: match value.kind {
            protocol::OrchestratorKind::Chat => api::OrchestratorKind::Chat as i32,
            protocol::OrchestratorKind::Agent => api::OrchestratorKind::Agent as i32,
        },
        lifecycle: match value.lifecycle {
            protocol::BridgeLifecycle::Open => api::BridgeLifecycle::Open as i32,
            protocol::BridgeLifecycle::Archived => api::BridgeLifecycle::Archived as i32,
        },
        display_title: value.display_title,
        revision: value.revision,
        transport: value.transport,
        external_parent_id: value.external_parent_id,
        external_thread_id: value.external_thread_id,
        last_projected: value.last_projected.as_ref().map(api_bridge_projection),
        delivery: value.delivery.map(|delivery| api::BridgeDelivery {
            projection_kind: api_bridge_projection_kind(delivery.projection.kind),
            turn_id: delivery.projection.turn_id.to_string(),
            previous_projection: delivery
                .previous_projection
                .as_ref()
                .map(api_bridge_projection),
            body_sha256: delivery.body_sha256,
            part_count: delivery.part_count,
            completed_parts: delivery.completed_parts,
            last_external_message_id: delivery.last_external_message_id,
        }),
        live_turn_id: value.live_turn_id.map(|id| id.to_string()),
        live_external_message_id: value.live_external_message_id,
        active_source_message_id: value.active_source_message_id,
    }
}

fn bridge_projection_kind(value: i32) -> Result<protocol::BridgeProjectionKind, tonic::Status> {
    match api::BridgeProjectionKind::try_from(value) {
        Ok(api::BridgeProjectionKind::User) => Ok(protocol::BridgeProjectionKind::User),
        Ok(api::BridgeProjectionKind::Assistant) => Ok(protocol::BridgeProjectionKind::Assistant),
        Ok(api::BridgeProjectionKind::Unspecified) | Err(_) => Err(
            tonic::Status::invalid_argument("bridge projection kind must be user or assistant"),
        ),
    }
}

fn bridge_projection(
    value: api::BridgeProjection,
) -> Result<protocol::BridgeProjectionView, tonic::Status> {
    Ok(protocol::BridgeProjectionView {
        kind: bridge_projection_kind(value.kind)?,
        turn_id: protocol::TurnId::from(value.turn_id),
    })
}

fn api_bridge_projection_kind(value: protocol::BridgeProjectionKind) -> i32 {
    match value {
        protocol::BridgeProjectionKind::User => api::BridgeProjectionKind::User as i32,
        protocol::BridgeProjectionKind::Assistant => api::BridgeProjectionKind::Assistant as i32,
    }
}

fn api_bridge_projection(value: &protocol::BridgeProjectionView) -> api::BridgeProjection {
    api::BridgeProjection {
        kind: api_bridge_projection_kind(value.kind),
        turn_id: value.turn_id.to_string(),
    }
}

fn provider(value: protocol::ProviderView) -> api::Provider {
    let supported_builtin_tools = value.supported_builtin_tools;
    let available_builtin_tools = value.available_builtin_tools;
    api::Provider {
        id: value.id.to_string(),
        display_name: value.display_name,
        enabled: value.enabled,
        credential_configured: value.credential_configured,
        credential_kind: value.credential_kind,
        connection: Some(connection(value.connection)),
        capabilities: Some(capabilities(value.capabilities)),
        authentication: value.authentication.map(authentication),
        model_filter_enabled: value.model_filter_enabled,
        selected_model_ids: value
            .selected_model_ids
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        model_candidates: value.model_candidates.into_iter().map(model).collect(),
        builtin_tool_availability_known: available_builtin_tools.is_some(),
        available_builtin_tools: available_builtin_tools.unwrap_or_default(),
        builtin_tool_support_known: supported_builtin_tools.is_some(),
        supported_builtin_tools: supported_builtin_tools.unwrap_or_default(),
    }
}

fn capabilities(value: protocol::ProviderCapabilities) -> api::ProviderCapabilities {
    api::ProviderCapabilities {
        supported: value
            .supported
            .into_iter()
            .map(|capability| match capability {
                protocol::ProviderCapability::Resume => api::ProviderCapability::Resume as i32,
                protocol::ProviderCapability::Steering => api::ProviderCapability::Steering as i32,
                protocol::ProviderCapability::Interruption => {
                    api::ProviderCapability::Interruption as i32
                }
                protocol::ProviderCapability::ModelCatalog => {
                    api::ProviderCapability::ModelCatalog as i32
                }
                protocol::ProviderCapability::ModelsRequireSession => {
                    api::ProviderCapability::ModelsRequireSession as i32
                }
                protocol::ProviderCapability::SessionModelConfiguration => {
                    api::ProviderCapability::SessionModelConfiguration as i32
                }
                protocol::ProviderCapability::ContextCompaction => {
                    api::ProviderCapability::ContextCompaction as i32
                }
                protocol::ProviderCapability::Approvals => {
                    api::ProviderCapability::Approvals as i32
                }
                protocol::ProviderCapability::NativeTools => {
                    api::ProviderCapability::NativeTools as i32
                }
                protocol::ProviderCapability::Mcp => api::ProviderCapability::Mcp as i32,
                protocol::ProviderCapability::CloseSession => {
                    api::ProviderCapability::CloseSession as i32
                }
                protocol::ProviderCapability::ExternalTools => {
                    api::ProviderCapability::ExternalTools as i32
                }
            })
            .collect(),
    }
}

fn connection(value: protocol::ConnectionView) -> api::Connection {
    let (state, message) = match value {
        protocol::ConnectionView::Disabled => (api::ConnectionState::Disabled, None),
        protocol::ConnectionView::Starting => (api::ConnectionState::Starting, None),
        protocol::ConnectionView::Ready => (api::ConnectionState::Ready, None),
        protocol::ConnectionView::Failed { message } => {
            (api::ConnectionState::Failed, Some(message))
        }
        protocol::ConnectionView::Disconnected { message } => {
            (api::ConnectionState::Disconnected, Some(message))
        }
    };
    api::Connection {
        state: state as i32,
        message,
    }
}

fn authentication(value: protocol::ProviderAuthenticationView) -> api::ProviderAuthentication {
    use api::provider_authentication::Kind;
    match value {
        protocol::ProviderAuthenticationView::Starting => api::ProviderAuthentication {
            kind: Kind::Starting as i32,
            ..Default::default()
        },
        protocol::ProviderAuthenticationView::ApiKeyRequired {
            dashboard_url,
            credential_kind,
        } => api::ProviderAuthentication {
            kind: Kind::ApiKeyRequired as i32,
            dashboard_url: Some(dashboard_url),
            credential_kind: Some(credential_kind),
            ..Default::default()
        },
        protocol::ProviderAuthenticationView::Challenge {
            verification_url,
            user_code,
        } => api::ProviderAuthentication {
            kind: Kind::Challenge as i32,
            verification_url: Some(verification_url),
            user_code: Some(user_code),
            ..Default::default()
        },
    }
}

fn model(value: protocol::ModelView) -> api::Model {
    api::Model {
        id: value.id.to_string(),
        provider_id: value.provider_id.to_string(),
        model_slug: value.model_slug,
        display_name: value.display_name,
        is_default: value.is_default,
        reasoning_effort: value.reasoning_effort,
        fast_mode: value.fast_mode,
        configuration: Some(api::ModelConfiguration {
            reasoning_efforts: value.configuration.reasoning_efforts,
            fast_mode_configurable: value.configuration.fast_mode_configurable,
            vision_eligible: value.configuration.vision_eligible,
            accepts_image_input: value.configuration.accepts_image_input,
        }),
    }
}

fn agent(value: protocol::AgentDefinitionView) -> api::AgentDefinition {
    api::AgentDefinition {
        slug: value.slug,
        description: value.description,
        system_prompt: value.system_prompt,
        first_message: value.first_message,
        model_id: value.model_id.map(|id| id.to_string()),
        fallback_models: value
            .fallback_models
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        fast_mode: value.fast_mode,
        reasoning_effort: value.reasoning_effort,
        ownership: value.ownership,
        enabled: Some(value.enabled),
        allowed_capabilities: value.allowed_capabilities,
        denied_capabilities: value.denied_capabilities,
        allowed_tools: value.allowed_tools,
        denied_tools: value.denied_tools,
        tool_profile: value.tool_profile,
        task_shape: value.task_shape,
        output_contract: value.output_contract,
        timeout_seconds: value.timeout_seconds,
        poll_interval_ms: value.poll_interval_ms,
        max_turns: value.max_turns,
        max_concurrency: value.max_concurrency,
        fallback_policy: value.fallback_policy,
        can_delegate: value.can_delegate,
        max_delegation_depth: value.max_delegation_depth,
        require_parent_attribution: Some(value.require_parent_attribution),
        effective_builtin_tools: value.effective_builtin_tools.clone().unwrap_or_default(),
        effective_builtin_tools_uses_runtime_default: value.effective_builtin_tools.is_none(),
        effective_capabilities: value.effective_capabilities.clone().unwrap_or_default(),
        effective_capabilities_use_runtime_default: value.effective_capabilities.is_none(),
        policy_warnings: value.policy_warnings,
        dashboard_tools_injected: value.dashboard_tools_injected,
        policy_projection_version: value.policy_projection_version,
    }
}

fn settings(value: protocol::SettingsView) -> api::Settings {
    api::Settings {
        web: Some(api::WebSettings {
            backend: value.web.backend,
            credential_configured: value.web.credential_configured,
            agent_browser: Some(agent_browser(value.web.agent_browser)),
        }),
        memory: Some(api::MemorySettings {
            backend: value.memory.backend,
            executable: value.memory.executable,
            global_bank: value.memory.global_bank,
            data_directory: value.memory.data_directory,
            configured: value.memory.configured,
            available: value.memory.available,
        }),
        vision: Some(api::VisionSettings {
            model_id: value.vision.model_id.map(|id| id.to_string()),
            availability: match value.vision.availability {
                protocol::VisionAvailabilityView::Unknown => api::VisionAvailability::Unspecified,
                protocol::VisionAvailabilityView::Disabled => api::VisionAvailability::Disabled,
                protocol::VisionAvailabilityView::Ready => api::VisionAvailability::Ready,
                protocol::VisionAvailabilityView::ModelUnavailable => {
                    api::VisionAvailability::ModelUnavailable
                }
                protocol::VisionAvailabilityView::ModelUnsupported => {
                    api::VisionAvailability::ModelUnsupported
                }
                protocol::VisionAvailabilityView::ProviderUnavailable => {
                    api::VisionAvailability::ProviderUnavailable
                }
                protocol::VisionAvailabilityView::ServiceUnavailable => {
                    api::VisionAvailability::ServiceUnavailable
                }
            } as i32,
            diagnostic: value.vision.diagnostic,
        }),
        terminal_images: match value.terminal_images {
            protocol::TerminalImageModeView::Auto => api::TerminalImageMode::Auto as i32,
            protocol::TerminalImageModeView::On => api::TerminalImageMode::On as i32,
            protocol::TerminalImageModeView::Off => api::TerminalImageMode::Off as i32,
        },
        invocation_telemetry_enabled: value.invocation_telemetry_enabled,
    }
}

fn agent_browser(value: protocol::AgentBrowserView) -> api::AgentBrowser {
    use api::agent_browser::State;
    match value {
        protocol::AgentBrowserView::Checking => api::AgentBrowser {
            state: State::Checking as i32,
            version: None,
        },
        protocol::AgentBrowserView::Available { version } => api::AgentBrowser {
            state: State::Available as i32,
            version: Some(version),
        },
        protocol::AgentBrowserView::Unavailable => api::AgentBrowser {
            state: State::Unavailable as i32,
            version: None,
        },
    }
}

pub(crate) fn session_summary(value: protocol::SessionSummary) -> api::SessionSummary {
    api::SessionSummary {
        id: value.id.to_string(),
        workspace_id: value.workspace_id.to_string(),
        title: value.title,
        working_directory: value.working_directory,
        active_provider_id: value.active_provider_id.map(|id| id.to_string()),
        active_model_id: value.active_model_id.map(|id| id.to_string()),
        updated_at_ms: value.updated_at_ms,
        created_at_ms: value.created_at_ms,
        owned_provider_sessions: value
            .owned_provider_sessions
            .into_iter()
            .map(|resource| api::OwnedProviderSession {
                provider_id: resource.provider_id.to_string(),
                native_session_id: resource.native_session_id,
            })
            .collect(),
        running: value.running,
    }
}

pub(crate) fn session(value: protocol::SessionView) -> api::SessionState {
    api::SessionState {
        id: value.id.to_string(),
        revision: value.revision,
        workspace_id: value.workspace_id.to_string(),
        working_directory: value.working_directory,
        title: value.title,
        status_message: value.status_message,
        diagnostic_count: value.diagnostic_count,
        activity: session_activity(value.activity),
        selected_provider_id: value.selected_provider_id.map(|id| id.to_string()),
        selected_model_id: value.selected_model_id.map(|id| id.to_string()),
        selected_model_options: Some(projected_model_options(value.selected_model_options)),
        active_agent_session: value.active_agent_session.map(agent_session),
        active_turn: value.active_turn.map(turn),
        last_turn: value.last_turn.map(turn),
        next_turn_configuration_pending: value.next_turn_configuration_pending,
        next_turn_transition: value.next_turn_transition,
        context_usage: value.context_usage.map(context_usage),
        transcript: Some(transcript(value.transcript)),
        recoverable_prompt: value.recoverable_prompt.map(recoverable_prompt),
        queue: value.queue.into_iter().map(queue_item).collect(),
        interactions: value.interactions.into_iter().map(interaction).collect(),
        todos: value.todos.into_iter().map(todo_phase).collect(),
        runs: value.runs.into_iter().map(run).collect(),
        runs_total: value.runs_total,
        runs_has_earlier: value.runs_has_earlier,
        notices: value.notices.into_iter().map(notice).collect(),
        external_tool_calls: value
            .external_tool_calls
            .into_iter()
            .map(|call| api::ExternalToolCall {
                id: call.id,
                name: call.name,
                arguments_json: call.arguments_json,
            })
            .collect(),
        created_at_ms: value.created_at_ms,
        updated_at_ms: value.updated_at_ms,
    }
}

fn projected_model_options(value: protocol::ModelOptions) -> api::ModelOptions {
    api::ModelOptions {
        reasoning_effort: value.reasoning_effort,
        fast_mode: value.fast_mode,
    }
}

fn session_activity(value: protocol::SessionActivity) -> i32 {
    (match value {
        protocol::SessionActivity::Idle => api::SessionActivity::Idle,
        protocol::SessionActivity::CreatingAgentSession => {
            api::SessionActivity::CreatingAgentSession
        }
        protocol::SessionActivity::StartingTurn => api::SessionActivity::StartingTurn,
        protocol::SessionActivity::RunningTurn => api::SessionActivity::RunningTurn,
        protocol::SessionActivity::CompactingContext => api::SessionActivity::CompactingContext,
        protocol::SessionActivity::RunningDelegates => api::SessionActivity::RunningDelegates,
        protocol::SessionActivity::RunningShell => api::SessionActivity::RunningShell,
    }) as i32
}

fn agent_session(value: protocol::AgentSessionView) -> api::AgentSession {
    api::AgentSession {
        id: value.id.to_string(),
        provider_id: value.provider_id.to_string(),
        model_id: value.model_id.map(|id| id.to_string()),
        role: value.role,
        capabilities: Some(capabilities(value.capabilities)),
        connection: Some(connection(value.connection)),
        native_session_id: value.native_session_id,
        transcript: Some(transcript(value.transcript)),
        usage: Some(token_usage(&value.usage)),
    }
}

fn token_usage(value: &protocol::TokenUsageView) -> api::TokenUsage {
    api::TokenUsage {
        input_tokens: value.input_tokens,
        output_tokens: value.output_tokens,
        cached_input_tokens: value.cached_input_tokens,
        cache_write_tokens: value.cache_write_tokens,
    }
}

fn turn(value: protocol::TurnView) -> api::Turn {
    let status = match value.status {
        protocol::TurnStatus::Starting => api::TurnStatus::Starting,
        protocol::TurnStatus::Running => api::TurnStatus::Running,
        protocol::TurnStatus::Cancelling => api::TurnStatus::Cancelling,
        protocol::TurnStatus::Completed => api::TurnStatus::Completed,
        protocol::TurnStatus::Interrupted => api::TurnStatus::Interrupted,
        protocol::TurnStatus::Failed => api::TurnStatus::Failed,
    };
    api::Turn {
        id: value.id.to_string(),
        agent_session_id: value.agent_session_id.to_string(),
        model_id: value.model_id.map(|id| id.to_string()),
        status: status as i32,
        resolved_model_options: Some(projected_model_options(value.resolved_model_options)),
    }
}

fn context_usage(value: protocol::ContextUsageView) -> api::ContextUsage {
    api::ContextUsage {
        estimated_tokens: value.estimated_tokens,
        context_window: value.context_window,
        compacting: value.compacting,
    }
}

pub(crate) fn transcript(value: protocol::TranscriptPage) -> api::TranscriptPage {
    api::TranscriptPage {
        entries: value.entries.into_iter().map(transcript_entry).collect(),
        has_earlier: value.has_earlier,
        stream_active: value.stream_active,
        stream_label: value.stream_label,
        current_owner_entry: value.current_owner_entry.map(transcript_entry),
        current_owner_omitted_tool_calls: value.current_owner_omitted_tool_calls,
    }
}

fn transcript_entry(value: protocol::TranscriptEntryView) -> api::TranscriptEntry {
    let kind = match value.kind {
        protocol::TranscriptEntryKind::System => api::TranscriptEntryKind::System,
        protocol::TranscriptEntryKind::User => api::TranscriptEntryKind::User,
        protocol::TranscriptEntryKind::Assistant => api::TranscriptEntryKind::Assistant,
        protocol::TranscriptEntryKind::Steering => api::TranscriptEntryKind::Steering,
        protocol::TranscriptEntryKind::Reasoning => api::TranscriptEntryKind::Reasoning,
        protocol::TranscriptEntryKind::Tool => api::TranscriptEntryKind::Tool,
        protocol::TranscriptEntryKind::Diff => api::TranscriptEntryKind::Diff,
        protocol::TranscriptEntryKind::Warning => api::TranscriptEntryKind::Warning,
        protocol::TranscriptEntryKind::Error => api::TranscriptEntryKind::Error,
    };
    let status = match value.status {
        protocol::TranscriptEntryStatus::Running => api::TranscriptEntryStatus::Running,
        protocol::TranscriptEntryStatus::Complete => api::TranscriptEntryStatus::Complete,
        protocol::TranscriptEntryStatus::Failed => api::TranscriptEntryStatus::Failed,
        protocol::TranscriptEntryStatus::Interrupted => api::TranscriptEntryStatus::Interrupted,
    };
    api::TranscriptEntry {
        id: value.id.to_string(),
        kind: kind as i32,
        title: value.title,
        body: value.body,
        body_start_byte: value.body_start_byte,
        body_total_bytes: value.body_total_bytes,
        status: status as i32,
        artifact_ids: value
            .artifacts
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        provider_id: value.provider_id,
        model_id: value.model_id.map(|id| id.to_string()),
        source_transport: value.source_transport,
        tool_audit_json: value.tool_audit_json,
        created_at_ms: value.created_at_ms,
        owner_turn_id: value.owner_turn_id.map(|id| id.to_string()),
        resolved_reasoning_effort: value.resolved_reasoning_effort,
        resolved_fast_mode: value.resolved_fast_mode,
    }
}

fn queue_item(value: protocol::QueueItemView) -> api::QueueItem {
    api::QueueItem {
        id: value.id.to_string(),
        summary: value.summary,
        attachment_count: value.attachment_count,
        text: value.text,
        redirecting: value.redirecting,
    }
}
fn recoverable_prompt(value: protocol::RecoverablePromptView) -> api::RecoverablePrompt {
    api::RecoverablePrompt {
        id: value.id.to_string(),
        text: value.text,
        attachments: value
            .attachments
            .into_iter()
            .map(prompt_attachment)
            .collect(),
    }
}

fn prompt_attachment(value: protocol::PromptAttachment) -> api::PromptAttachment {
    use api::prompt_attachment::Source;
    match value {
        protocol::PromptAttachment::Artifact { artifact_id, label } => api::PromptAttachment {
            label,
            source: Some(Source::ArtifactId(artifact_id.to_string())),
        },
        protocol::PromptAttachment::LocalFile { label, path } => api::PromptAttachment {
            label,
            source: Some(Source::LocalFile(path)),
        },
        protocol::PromptAttachment::InlineImage {
            label,
            media_type,
            data,
        } => api::PromptAttachment {
            label,
            source: Some(Source::InlineImage(api::InlineImage { media_type, data })),
        },
    }
}

fn interaction(value: protocol::InteractionView) -> api::Interaction {
    let kind = match value.kind {
        protocol::InteractionKind::Approval => api::InteractionKind::Approval,
        protocol::InteractionKind::Question => api::InteractionKind::Question,
    };
    let status = match value.status {
        protocol::InteractionStatus::Pending => api::InteractionStatus::Pending,
        protocol::InteractionStatus::Resolved => api::InteractionStatus::Resolved,
        protocol::InteractionStatus::Declined => api::InteractionStatus::Declined,
        protocol::InteractionStatus::Cancelled => api::InteractionStatus::Cancelled,
    };
    api::Interaction {
        id: value.id.to_string(),
        revision: value.revision,
        kind: kind as i32,
        status: status as i32,
        title: value.title,
        detail: value.detail,
        options: value
            .options
            .into_iter()
            .map(|option| api::InteractionOption {
                id: option.id,
                label: option.label,
                description: option.description,
                recommended: option.recommended,
            })
            .collect(),
        multiple: value.multiple,
        questions: value
            .questions
            .into_iter()
            .map(|question| api::InteractionQuestion {
                id: question.id,
                title: question.title,
                detail: question.detail,
                options: question
                    .options
                    .into_iter()
                    .map(|option| api::InteractionOption {
                        id: option.id,
                        label: option.label,
                        description: option.description,
                        recommended: option.recommended,
                    })
                    .collect(),
                multiple: question.multiple,
            })
            .collect(),
    }
}

fn todo_phase(value: protocol::TodoPhaseView) -> api::TodoPhase {
    api::TodoPhase {
        name: value.name,
        tasks: value
            .tasks
            .into_iter()
            .map(|item| api::TodoItem {
                content: item.content,
                status: match item.status {
                    protocol::TodoStatusView::Pending => api::TodoStatus::Pending,
                    protocol::TodoStatusView::InProgress => api::TodoStatus::InProgress,
                    protocol::TodoStatusView::Completed => api::TodoStatus::Completed,
                    protocol::TodoStatusView::Abandoned => api::TodoStatus::Abandoned,
                } as i32,
            })
            .collect(),
    }
}

fn notice(value: protocol::NoticeView) -> api::Notice {
    api::Notice {
        id: value.id,
        level: match value.level {
            protocol::NoticeLevel::Info => api::NoticeLevel::Info,
            protocol::NoticeLevel::Warning => api::NoticeLevel::Warning,
            protocol::NoticeLevel::Error => api::NoticeLevel::Error,
        } as i32,
        message: value.message,
    }
}

pub(crate) fn run(value: protocol::RunView) -> api::RunState {
    api::RunState {
        id: value.id.to_string(),
        parent_run_id: value.parent_run_id.map(|id| id.to_string()),
        agent_slug: value.agent_slug,
        archetype_purpose: value.archetype_purpose,
        provider_id: value.provider_id.to_string(),
        model_id: value.model_id.map(|id| id.to_string()),
        reasoning_effort: value.reasoning_effort,
        fast_mode: value.fast_mode,
        started_at_ms: value.started_at_ms,
        ended_at_ms: value.ended_at_ms,
        duration_ms: value.duration_ms,
        termination_kind: value.termination_kind,
        termination_detail: value.termination_detail,
        objective_mismatch_handoff: value.objective_mismatch_handoff,
        policy: Some(api::RunPolicy {
            allowed_capabilities: value.policy.allowed_capabilities,
            denied_capabilities: value.policy.denied_capabilities,
            allowed_tools: value.policy.allowed_tools,
            denied_tools: value.policy.denied_tools,
            provider: value.policy.provider,
            policy_available: value.policy.policy_available,
            provider_tools_restricted: value.policy.provider_tools_restricted,
            provider_allowed_tools: value.policy.provider_allowed_tools,
            unsupported_canonical_tools: value.policy.unsupported_canonical_tools,
            tool_profile: value.policy.tool_profile,
            task_shape: value.policy.task_shape,
            output_contract: value.policy.output_contract,
            timeout_seconds: value.policy.timeout_seconds,
            max_turns: value.policy.max_turns,
            can_delegate: value.policy.can_delegate,
            max_delegation_depth: value.policy.max_delegation_depth,
            remaining_delegation_depth: value.policy.remaining_delegation_depth,
            require_parent_attribution: value.policy.require_parent_attribution,
            truncated_fields: value.policy.truncated_fields,
        }),
        tool_denials: value
            .tool_denials
            .into_iter()
            .map(|denial| api::RunToolDenial {
                entry_id: denial.entry_id,
                tool: denial.tool,
                reason: denial.reason,
                tool_start_byte: denial.tool_start_byte,
                tool_total_bytes: denial.tool_total_bytes,
                reason_start_byte: denial.reason_start_byte,
                reason_total_bytes: denial.reason_total_bytes,
            })
            .collect(),
        tool_denials_retained_total: value.tool_denials_retained_total,
        salvage: value.salvage.map(run_salvage),
        continued_from_run_id: value.continued_from_run_id.map(|id| id.to_string()),
        continued_by_run_id: value.continued_by_run_id.map(|id| id.to_string()),
        continuation_depth: value.continuation_depth,
        additional_turns: value.additional_turns,
        inherited_evidence: value
            .inherited_evidence
            .into_iter()
            .map(salvaged_evidence)
            .collect(),
        native_session_id: value.native_session_id,
        usage: Some(token_usage(&value.usage)),
        objective: value.objective,
        objective_start_byte: value.objective_start_byte,
        objective_total_bytes: value.objective_total_bytes,
        status: match value.status {
            protocol::RunStatus::Starting => api::RunStatus::Starting,
            protocol::RunStatus::Working => api::RunStatus::Working,
            protocol::RunStatus::Completed => api::RunStatus::Completed,
            protocol::RunStatus::Partial => api::RunStatus::Partial,
            protocol::RunStatus::Interrupted => api::RunStatus::Interrupted,
            protocol::RunStatus::Failed => api::RunStatus::Failed,
        } as i32,
        latest_activity: value.latest_activity,
        latest_activity_start_byte: value.latest_activity_start_byte,
        latest_activity_total_bytes: value.latest_activity_total_bytes,
        outcome: value.outcome.map(run_outcome),
        outcome_start_byte: value.outcome_start_byte,
        outcome_total_bytes: value.outcome_total_bytes,
        result: value.result,
        result_start_byte: value.result_start_byte,
        result_total_bytes: value.result_total_bytes,
        transcript: Some(transcript(value.transcript)),
    }
}

fn salvaged_evidence(value: protocol::SalvagedEvidenceView) -> api::SalvagedEvidence {
    api::SalvagedEvidence {
        entry_id: value.entry_id,
        title: value.title,
        body: value.body,
        truncated: value.truncated,
    }
}

fn run_salvage(value: protocol::RunSalvageView) -> api::RunSalvage {
    api::RunSalvage {
        terminal_reason: value.terminal_reason,
        original_objective: value.original_objective,
        completed_work: value.completed_work,
        verified_evidence: value
            .verified_evidence
            .into_iter()
            .map(salvaged_evidence)
            .collect(),
        last_successful_evidence: value.last_successful_evidence.map(salvaged_evidence),
        unresolved_questions: value.unresolved_questions,
        continuation: Some(api::ContinuationProposition {
            verified_findings: value.continuation.verified_findings,
            unresolved_boundary: value.continuation.unresolved_boundary,
            why_it_matters: value.continuation.why_it_matters,
            recommended_archetype: value.continuation.recommended_archetype,
            follow_up_objective: value.continuation.follow_up_objective,
            inherited_evidence: value.continuation.inherited_evidence,
            can_proceed_independently: value.continuation.can_proceed_independently,
        }),
        can_resume: value.can_resume,
        redacted: value.redacted,
        truncated: value.truncated,
    }
}

fn run_outcome(value: protocol::RunOutcome) -> api::RunOutcome {
    use api::run_outcome::Kind;
    match value {
        protocol::RunOutcome::Completed { body } => api::RunOutcome {
            kind: Kind::Completed as i32,
            body,
        },
        protocol::RunOutcome::Partial { body } => api::RunOutcome {
            kind: Kind::Partial as i32,
            body,
        },
        protocol::RunOutcome::Failed { reason } => api::RunOutcome {
            kind: Kind::Failed as i32,
            body: reason,
        },
        protocol::RunOutcome::Interrupted { reason } => api::RunOutcome {
            kind: Kind::Interrupted as i32,
            body: reason,
        },
    }
}

pub(crate) fn transcript_body(value: protocol::TranscriptBodyWindow) -> api::TranscriptBodyWindow {
    api::TranscriptBodyWindow {
        entry_id: value.entry_id.to_string(),
        body: value.body,
        start_byte: value.start_byte,
        total_bytes: value.total_bytes,
        has_earlier: value.has_earlier,
    }
}

pub(crate) fn run_text(value: protocol::RunTextWindow) -> api::RunTextWindow {
    api::RunTextWindow {
        run_id: value.run_id.to_string(),
        field: match value.field {
            protocol::RunTextField::Objective => api::RunTextField::Objective,
            protocol::RunTextField::LatestActivity => api::RunTextField::LatestActivity,
            protocol::RunTextField::Outcome => api::RunTextField::Outcome,
            protocol::RunTextField::Result => api::RunTextField::Result,
        } as i32,
        text: value.text,
        start_byte: value.start_byte,
        total_bytes: value.total_bytes,
        has_earlier: value.has_earlier,
    }
}

pub(crate) fn artifact(value: protocol::ArtifactView) -> api::Artifact {
    api::Artifact {
        id: value.id.to_string(),
        label: value.label,
        media_type: value.media_type,
        byte_length: value.byte_length,
        data: value.data,
    }
}

fn invocation_summary(value: protocol::InvocationSummary) -> api::InvocationSummary {
    api::InvocationSummary {
        enabled: value.enabled,
        items: value
            .items
            .into_iter()
            .map(|item| api::InvocationUsage {
                kind: match item.kind {
                    protocol::InvocationKind::Archetype => api::InvocationKind::Archetype as i32,
                    protocol::InvocationKind::Skill => api::InvocationKind::Skill as i32,
                },
                identity: item.identity,
                display_label: item.display_label,
                currently_installed: item.currently_installed,
                invocation_count: item.invocation_count,
                first_used_at_ms: item.first_used_at_ms,
                last_used_at_ms: item.last_used_at_ms,
            })
            .collect(),
    }
}

fn invocation_timeline(value: protocol::InvocationTimeline) -> api::InvocationTimeline {
    api::InvocationTimeline {
        start_at_ms: value.start_at_ms,
        end_at_ms: value.end_at_ms,
        bucket_width_ms: value.bucket_width_ms,
        buckets: value
            .buckets
            .into_iter()
            .map(|bucket| api::InvocationBucket {
                start_at_ms: bucket.start_at_ms,
                archetype_count: bucket.archetype_count,
                skill_count: bucket.skill_count,
            })
            .collect(),
    }
}

pub(crate) fn diagnostics(value: protocol::DiagnosticsReport) -> api::DiagnosticsReport {
    api::DiagnosticsReport {
        generated_at_ms: value.generated_at_ms,
        period_days: u32::from(value.period_days),
        provider_filter: value.provider_filter.map(|id| id.to_string()),
        sessions_scanned: value.sessions_scanned,
        sessions_with_activity: value.sessions_with_activity,
        totals: Some(diagnostics_totals(&value.totals)),
        daily: value
            .daily
            .into_iter()
            .map(|item| api::DiagnosticsDailyUsage {
                date_utc: item.date_utc,
                provider_id: item.provider_id.to_string(),
                totals: Some(diagnostics_totals(&item.totals)),
            })
            .collect(),
        tools: value
            .tools
            .into_iter()
            .map(|item| api::DiagnosticsToolUsage {
                provider_id: item.provider_id.to_string(),
                tool: item.tool,
                calls: item.calls,
                failures: item.failures,
                full_output_bytes: item.full_output_bytes,
                model_output_bytes: item.model_output_bytes,
                duration_ms: item.duration_ms,
            })
            .collect(),
        sessions: value
            .sessions
            .into_iter()
            .map(|item| api::DiagnosticsSessionUsage {
                session_id: item.session_id.to_string(),
                provider_id: item.provider_id.to_string(),
                model: item.model,
                latest_activity_ms: item.latest_activity_ms,
                totals: Some(diagnostics_totals(&item.totals)),
            })
            .collect(),
        notes: value.notes,
    }
}

fn diagnostics_totals(value: &protocol::DiagnosticsUsageTotals) -> api::DiagnosticsUsageTotals {
    api::DiagnosticsUsageTotals {
        inference_rounds: value.inference_rounds,
        compaction_rounds: value.compaction_rounds,
        failed_rounds: value.failed_rounds,
        retry_count: value.retry_count,
        estimated_input_tokens: value.estimated_input_tokens,
        reported_input_tokens: value.reported_input_tokens,
        reported_cached_input_tokens: value.reported_cached_input_tokens,
        reported_cache_write_tokens: value.reported_cache_write_tokens,
        reported_output_tokens: value.reported_output_tokens,
        request_bytes: value.request_bytes,
        response_bytes: value.response_bytes,
        inference_duration_ms: value.inference_duration_ms,
        requested_tool_calls: value.requested_tool_calls,
        executed_tool_calls: value.executed_tool_calls,
        failed_tool_calls: value.failed_tool_calls,
        full_tool_output_bytes: value.full_tool_output_bytes,
        model_tool_output_bytes: value.model_tool_output_bytes,
        tool_duration_ms: value.tool_duration_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_builtin_availability_preserves_known_empty_and_legacy_absence() {
        let view = |available_builtin_tools| protocol::ProviderView {
            id: protocol::ProviderId::from("openai-codex"),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential_configured: true,
            credential_kind: None,
            connection: protocol::ConnectionView::Ready,
            capabilities: protocol::ProviderCapabilities::default(),
            authentication: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
            model_candidates: Vec::new(),
            supported_builtin_tools: Some(Vec::new()),
            available_builtin_tools,
        };

        let known = provider(view(Some(Vec::new())));
        assert!(known.builtin_tool_availability_known);
        assert!(known.available_builtin_tools.is_empty());

        let legacy = provider(view(None));
        assert!(!legacy.builtin_tool_availability_known);
        assert!(legacy.available_builtin_tools.is_empty());
    }

    #[test]
    fn discord_management_capability_is_advertised_only_when_injected() {
        assert!(!advertise_capability(
            protocol::ServiceCapability::DiscordManagement,
            false,
        ));
        assert!(advertise_capability(
            protocol::ServiceCapability::DiscordManagement,
            true,
        ));
        assert!(advertise_capability(
            protocol::ServiceCapability::OrchestratorThreadBridge,
            false,
        ));
    }

    #[test]
    fn discord_mutations_require_an_idempotency_key_without_a_revision() {
        let missing = GrpcService::discord_mutation_key(None).expect_err("missing options");
        assert_eq!(missing.code(), tonic::Code::InvalidArgument);

        let blank = GrpcService::discord_mutation_key(Some(api::MutationOptions {
            idempotency_key: String::new(),
            expected_revision: None,
        }))
        .expect_err("blank idempotency key");
        assert_eq!(blank.code(), tonic::Code::InvalidArgument);

        let revision = GrpcService::discord_mutation_key(Some(api::MutationOptions {
            idempotency_key: "discord-save".to_owned(),
            expected_revision: Some(4),
        }))
        .expect_err("installation state has no workspace revision");
        assert_eq!(revision.code(), tonic::Code::InvalidArgument);

        let key = GrpcService::discord_mutation_key(Some(api::MutationOptions {
            idempotency_key: "discord-save".to_owned(),
            expected_revision: None,
        }))
        .expect("valid key");
        assert_eq!(key.as_str(), "discord-save");
    }

    #[test]
    fn queue_redirect_reservation_projects_to_grpc() {
        let projected = queue_item(protocol::QueueItemView {
            id: protocol::PromptId::from("prompt-1"),
            summary: "reserved follow-up".to_owned(),
            text: "run next".to_owned(),
            attachment_count: 1,
            redirecting: true,
        });
        assert_eq!(projected.id, "prompt-1");
        assert_eq!(projected.text, "run next");
        assert_eq!(projected.attachment_count, 1);
        assert!(projected.redirecting);
    }

    #[test]
    fn interpreted_agent_policy_projects_exact_and_ambiguous_boundaries() {
        let base = protocol::AgentDefinitionView {
            slug: "reviewer".to_owned(),
            description: "Reviews changes".to_owned(),
            system_prompt: String::new(),
            first_message: String::new(),
            model_id: None,
            fallback_models: Vec::new(),
            fast_mode: false,
            reasoning_effort: None,
            ownership: "owner_defined".to_owned(),
            enabled: true,
            allowed_capabilities: vec!["filesystem_read".to_owned()],
            denied_capabilities: Vec::new(),
            allowed_tools: vec!["read".to_owned()],
            denied_tools: Vec::new(),
            tool_profile: "read_only".to_owned(),
            task_shape: String::new(),
            output_contract: String::new(),
            timeout_seconds: None,
            poll_interval_ms: None,
            max_turns: None,
            max_concurrency: 1,
            fallback_policy: "configured_only".to_owned(),
            can_delegate: false,
            max_delegation_depth: 0,
            require_parent_attribution: true,
            effective_builtin_tools: Some(vec!["read".to_owned()]),
            effective_capabilities: Some(vec!["filesystem_read".to_owned()]),
            policy_warnings: vec!["authoritative warning".to_owned()],
            dashboard_tools_injected: false,
            policy_projection_version: 1,
        };

        let exact = agent(base.clone());
        assert_eq!(exact.effective_builtin_tools, vec!["read"]);
        assert!(!exact.effective_builtin_tools_uses_runtime_default);
        assert_eq!(exact.effective_capabilities, vec!["filesystem_read"]);
        assert!(!exact.effective_capabilities_use_runtime_default);
        assert_eq!(exact.policy_warnings, vec!["authoritative warning"]);
        assert!(!exact.dashboard_tools_injected);
        assert_eq!(exact.policy_projection_version, 1);

        let ambiguous = agent(protocol::AgentDefinitionView {
            effective_builtin_tools: None,
            effective_capabilities: None,
            ..base
        });
        assert!(ambiguous.effective_builtin_tools.is_empty());
        assert!(ambiguous.effective_builtin_tools_uses_runtime_default);
        assert!(ambiguous.effective_capabilities.is_empty());
        assert!(ambiguous.effective_capabilities_use_runtime_default);
    }

    #[test]
    fn selected_session_options_are_projected_to_grpc() {
        let projected = projected_model_options(protocol::ModelOptions {
            reasoning_effort: Some("high".to_owned()),
            fast_mode: true,
        });

        assert_eq!(projected.reasoning_effort.as_deref(), Some("high"));
        assert!(projected.fast_mode);
    }
}
