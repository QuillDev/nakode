//! gRPC adapter for the authoritative Nakode server request boundary.
//!
//! Conversion lives here so provider/runtime protocol types never leak into
//! the public generated contract. The adapter never owns domain state.

use std::pin::Pin;

use futures_util::Stream;
use nakode_api::v1 as api;
use nakode_protocol as protocol;
use tokio_stream::wrappers::ReceiverStream;

use crate::{PublishedEvent, ServerEndpoint};

/// Public gRPC facade over one authoritative [`ServerEndpoint`].
#[derive(Clone)]
pub struct GrpcService {
    endpoint: ServerEndpoint,
    client_id: protocol::ClientId,
}

impl GrpcService {
    #[must_use]
    pub fn new(endpoint: ServerEndpoint) -> Self {
        Self {
            endpoint,
            client_id: protocol::ClientId::new(format!("grpc-{}", uuid::Uuid::now_v7())),
        }
    }

    #[must_use]
    pub fn into_server(self) -> api::nakode_service_server::NakodeServiceServer<Self> {
        api::nakode_service_server::NakodeServiceServer::new(self)
            .max_decoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
            .max_encoding_message_size(nakode_api::MAX_API_MESSAGE_BYTES)
    }

    async fn mutate(
        &self,
        options: Option<api::MutationOptions>,
        command: protocol::Command,
    ) -> Result<tonic::Response<api::MutationResult>, tonic::Status> {
        let options =
            options.ok_or_else(|| tonic::Status::invalid_argument("mutation is required"))?;
        if options.idempotency_key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "mutation.idempotency_key is required",
            ));
        }
        let result = self
            .endpoint
            .execute_command(
                self.client_id.clone(),
                protocol::IdempotencyKey::new(options.idempotency_key),
                options.expected_revision,
                false,
                command,
            )
            .await
            .map_err(status)?;
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
    command_rpc!(
        create_session,
        api::CreateSessionRequest,
        input,
        protocol::Command::CreateSession {
            workspace_id: protocol::WorkspaceId::from(input.workspace_id),
            title: input.title,
            model_id: input.model_id.map(protocol::ModelId::from),
            options: model_options(input.options),
            tools: session_tools(input.tools)
        }
    );
    command_rpc!(
        open_session,
        api::OpenSessionRequest,
        input,
        protocol::Command::OpenSession {
            session_id: protocol::SessionId::from(input.session_id),
            tools: session_tools(input.tools)
        }
    );

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
        let protocol::QueryResult::Sessions(values) = result.value else {
            return Err(tonic::Status::internal("unexpected sessions response"));
        };
        Ok(tonic::Response::new(api::ListSessionsResponse {
            sessions: values.into_iter().map(session_summary).collect(),
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
        Ok(tonic::Response::new(transcript(value)))
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
                name: skill.name,
                description: skill.description,
            })
            .collect(),
        settings: Some(settings(value.settings)),
        sessions: value.sessions.into_iter().map(session_summary).collect(),
        active_session: value.active_session.map(session),
    }
}

fn provider(value: protocol::ProviderView) -> api::Provider {
    api::Provider {
        id: value.id.to_string(),
        display_name: value.display_name,
        enabled: value.enabled,
        credential_configured: value.credential_configured,
        credential_kind: value.credential_kind,
        connection: Some(connection(value.connection)),
        capabilities: Some(capabilities(value.capabilities)),
        authentication: value.authentication.map(authentication),
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
        }),
        terminal_images: match value.terminal_images {
            protocol::TerminalImageModeView::Auto => api::TerminalImageMode::Auto as i32,
            protocol::TerminalImageModeView::On => api::TerminalImageMode::On as i32,
            protocol::TerminalImageModeView::Off => api::TerminalImageMode::Off as i32,
        },
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
        active_provider_id: value.active_provider_id.map(|id| id.to_string()),
        active_model_id: value.active_model_id.map(|id| id.to_string()),
        updated_at_ms: value.updated_at_ms,
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
        title: value.title,
        status_message: value.status_message,
        diagnostic_count: value.diagnostic_count,
        activity: session_activity(value.activity),
        selected_provider_id: value.selected_provider_id.map(|id| id.to_string()),
        selected_model_id: value.selected_model_id.map(|id| id.to_string()),
        selected_model_options: Some(projected_model_options(value.selected_model_options)),
        active_agent_session: value.active_agent_session.map(agent_session),
        active_turn: value.active_turn.map(turn),
        context_usage: value.context_usage.map(context_usage),
        transcript: Some(transcript(value.transcript)),
        recoverable_prompt: value.recoverable_prompt.map(recoverable_prompt),
        queue: value.queue.into_iter().map(queue_item).collect(),
        interactions: value.interactions.into_iter().map(interaction).collect(),
        todos: value.todos.into_iter().map(todo_phase).collect(),
        runs: value.runs.into_iter().map(run).collect(),
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
        tool_audit_json: value.tool_audit_json,
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
        agent_slug: value.agent_slug,
        provider_id: value.provider_id.to_string(),
        model_id: value.model_id.map(|id| id.to_string()),
        native_session_id: value.native_session_id,
        usage: Some(token_usage(&value.usage)),
        objective: value.objective,
        objective_start_byte: value.objective_start_byte,
        objective_total_bytes: value.objective_total_bytes,
        status: match value.status {
            protocol::RunStatus::Starting => api::RunStatus::Starting,
            protocol::RunStatus::Working => api::RunStatus::Working,
            protocol::RunStatus::Completed => api::RunStatus::Completed,
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

fn run_outcome(value: protocol::RunOutcome) -> api::RunOutcome {
    use api::run_outcome::Kind;
    match value {
        protocol::RunOutcome::Completed { body } => api::RunOutcome {
            kind: Kind::Completed as i32,
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
