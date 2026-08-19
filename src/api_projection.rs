//! Pure presentation adapter from generated API snapshots to the existing
//! Ratatui view model. It performs no reduction, persistence, or policy.

use std::collections::BTreeSet;

use nakode_protocol as view;
use nakode_sdk::v1 as api;

/// Presentation-layer actions emitted by controls. This is deliberately not
/// the server's internal command enum: dispatch below calls distinct public
/// SDK methods for every action.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
// Actions remain value-shaped so controls and the SDK adapter share one explicit semantic request;
// boxing only agent saves would complicate this short-lived presentation boundary.
pub(crate) enum TuiAction {
    CreateSession {
        workspace_id: view::WorkspaceId,
        title: Option<String>,
    },
    OpenSession {
        session_id: view::SessionId,
    },
    SendPrompt {
        session_id: view::SessionId,
        prompt: view::PromptInput,
    },
    EnqueuePrompt {
        session_id: view::SessionId,
        prompt: view::PromptInput,
    },
    RemoveQueuedPrompt {
        session_id: view::SessionId,
        prompt_id: view::PromptId,
    },
    SteerTurn {
        turn_id: view::TurnId,
        text: String,
    },
    CancelSessionWork {
        session_id: view::SessionId,
    },
    CompactContext {
        agent_session_id: view::AgentSessionId,
    },
    SelectModel {
        target: view::ModelTarget,
        model_id: view::ModelId,
        options: view::ModelOptions,
    },
    ResolveInteraction {
        interaction_id: view::InteractionId,
        resolution: view::InteractionResolution,
    },
    CancelRun {
        run_id: view::RunId,
    },
    RunShell {
        session_id: view::SessionId,
        command: String,
    },
    SetProviderEnabled {
        provider_id: view::ProviderId,
        enabled: bool,
    },
    SetProviderModelFilter {
        provider_id: view::ProviderId,
        enabled: bool,
        selected_model_ids: Vec<view::ModelId>,
    },
    BeginProviderAuthentication {
        provider_id: view::ProviderId,
    },
    SetProviderCredential {
        provider_id: view::ProviderId,
        kind: String,
        credential: view::CredentialInput,
    },
    ClearProviderCredential {
        provider_id: view::ProviderId,
    },
    SaveAgent {
        workspace_id: view::WorkspaceId,
        definition: view::AgentDefinitionInput,
        previous_slug: Option<String>,
    },
    DeleteAgent {
        workspace_id: view::WorkspaceId,
        slug: String,
    },
    UpdateSettings {
        patch: view::SettingsPatch,
    },
    CheckAgentBrowser {
        workspace_id: view::WorkspaceId,
    },
    ReloadWorkspace {
        workspace_id: view::WorkspaceId,
        session_id: view::SessionId,
    },
}

pub(crate) async fn execute_command(
    client: &nakode_sdk::NakodeClient,
    command: TuiAction,
) -> Result<api::MutationResult, nakode_sdk::SdkError> {
    match command {
        TuiAction::CreateSession {
            workspace_id,
            title,
        } => {
            let resource_id = client
                .create_session(workspace_id.to_string(), title)
                .await?;
            Ok(api::MutationResult {
                resource_id: Some(resource_id),
                revision: None,
            })
        }
        TuiAction::OpenSession { session_id } => {
            let session_id = client.open_session(session_id.to_string()).await?;
            Ok(api::MutationResult {
                resource_id: Some(session_id),
                revision: None,
            })
        }
        TuiAction::SendPrompt { session_id, prompt } => {
            client
                .send_prompt(session_id.to_string(), api_prompt(prompt), None)
                .await
        }
        TuiAction::EnqueuePrompt { session_id, prompt } => {
            client
                .enqueue_prompt(session_id.to_string(), api_prompt(prompt), None)
                .await
        }
        TuiAction::RemoveQueuedPrompt {
            session_id,
            prompt_id,
        } => {
            client
                .remove_queued_prompt(api::RemoveQueuedPromptRequest {
                    mutation: None,
                    session_id: session_id.to_string(),
                    prompt_id: prompt_id.to_string(),
                })
                .await
        }
        TuiAction::SteerTurn { turn_id, text } => {
            client.steer_turn(turn_id.to_string(), text, None).await
        }
        TuiAction::CancelSessionWork { session_id } => {
            client
                .cancel_session_work(session_id.to_string(), None)
                .await
        }
        TuiAction::CompactContext { agent_session_id } => {
            client
                .compact_context(api::CompactContextRequest {
                    mutation: None,
                    agent_session_id: agent_session_id.to_string(),
                })
                .await
        }
        other => execute_management_command(client, other).await,
    }
}

async fn execute_management_command(
    client: &nakode_sdk::NakodeClient,
    command: TuiAction,
) -> Result<api::MutationResult, nakode_sdk::SdkError> {
    match command {
        TuiAction::SelectModel {
            target,
            model_id,
            options,
        } => {
            client
                .select_model(api::SelectModelRequest {
                    mutation: None,
                    target: Some(api_model_target(target)),
                    model_id: model_id.to_string(),
                    options: Some(api::ModelOptions {
                        reasoning_effort: options.reasoning_effort,
                        fast_mode: options.fast_mode,
                    }),
                })
                .await
        }
        TuiAction::ResolveInteraction {
            interaction_id,
            resolution,
        } => {
            let (resolution, option_ids) = api_resolution(resolution);
            client
                .resolve_interaction(interaction_id.to_string(), resolution, option_ids, None)
                .await
        }
        TuiAction::CancelRun { run_id } => {
            client
                .cancel_run(api::CancelRunRequest {
                    mutation: None,
                    run_id: run_id.to_string(),
                })
                .await
        }
        TuiAction::RunShell {
            session_id,
            command,
        } => {
            client
                .run_shell(api::RunShellRequest {
                    mutation: None,
                    session_id: session_id.to_string(),
                    command,
                })
                .await
        }
        TuiAction::SetProviderEnabled {
            provider_id,
            enabled,
        } => {
            client
                .set_provider_enabled(api::SetProviderEnabledRequest {
                    mutation: None,
                    provider_id: provider_id.to_string(),
                    enabled,
                })
                .await
        }
        action @ TuiAction::SetProviderModelFilter { .. } => {
            execute_provider_model_filter(client, action).await
        }
        TuiAction::BeginProviderAuthentication { provider_id } => {
            client
                .begin_provider_authentication(api::BeginProviderAuthenticationRequest {
                    mutation: None,
                    provider_id: provider_id.to_string(),
                })
                .await
        }
        TuiAction::SetProviderCredential {
            provider_id,
            kind,
            credential,
        } => {
            client
                .set_provider_credential(api::SetProviderCredentialRequest {
                    mutation: None,
                    provider_id: provider_id.to_string(),
                    kind,
                    credential: credential.0,
                })
                .await
        }
        TuiAction::ClearProviderCredential { provider_id } => {
            client
                .clear_provider_credential(api::ClearProviderCredentialRequest {
                    mutation: None,
                    provider_id: provider_id.to_string(),
                })
                .await
        }
        other => execute_catalog_command(client, other).await,
    }
}

async fn execute_provider_model_filter(
    client: &nakode_sdk::NakodeClient,
    action: TuiAction,
) -> Result<api::MutationResult, nakode_sdk::SdkError> {
    let TuiAction::SetProviderModelFilter {
        provider_id,
        enabled,
        selected_model_ids,
    } = action
    else {
        unreachable!("called only for provider model filters")
    };
    client
        .set_provider_model_filter(api::SetProviderModelFilterRequest {
            mutation: None,
            provider_id: provider_id.to_string(),
            enabled,
            selected_model_ids: selected_model_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
        })
        .await
}

async fn execute_catalog_command(
    client: &nakode_sdk::NakodeClient,
    command: TuiAction,
) -> Result<api::MutationResult, nakode_sdk::SdkError> {
    match command {
        TuiAction::SaveAgent {
            workspace_id,
            definition,
            previous_slug,
        } => {
            client
                .save_agent(api::SaveAgentRequest {
                    mutation: None,
                    workspace_id: workspace_id.to_string(),
                    definition: Some(api::AgentDefinitionInput {
                        slug: definition.slug,
                        description: definition.description,
                        system_prompt: definition.system_prompt,
                        first_message: definition.first_message,
                        model_id: definition.model.map(|id| id.to_string()),
                        fallback_models: definition
                            .fallback_models
                            .into_iter()
                            .map(|id| id.to_string())
                            .collect(),
                        fast_mode: definition.fast_mode,
                        reasoning_effort: definition.reasoning_effort,
                        ownership: definition.ownership,
                        enabled: Some(definition.enabled),
                        allowed_capabilities: definition.allowed_capabilities,
                        denied_capabilities: definition.denied_capabilities,
                        allowed_tools: definition.allowed_tools,
                        denied_tools: definition.denied_tools,
                        tool_profile: definition.tool_profile,
                        task_shape: definition.task_shape,
                        output_contract: definition.output_contract,
                        timeout_seconds: definition.timeout_seconds,
                        poll_interval_ms: definition.poll_interval_ms,
                        max_turns: definition.max_turns,
                        max_concurrency: definition.max_concurrency,
                        fallback_policy: definition.fallback_policy,
                        can_delegate: definition.can_delegate,
                        max_delegation_depth: definition.max_delegation_depth,
                        require_parent_attribution: Some(definition.require_parent_attribution),
                    }),
                    previous_slug,
                })
                .await
        }
        TuiAction::DeleteAgent { workspace_id, slug } => {
            client
                .delete_agent(api::DeleteAgentRequest {
                    mutation: None,
                    workspace_id: workspace_id.to_string(),
                    slug,
                })
                .await
        }
        TuiAction::UpdateSettings { patch } => {
            client
                .update_settings(api::UpdateSettingsRequest {
                    mutation: None,
                    patch: Some(api_settings_patch(patch)),
                })
                .await
        }
        TuiAction::CheckAgentBrowser { workspace_id } => {
            client
                .check_agent_browser(api::CheckAgentBrowserRequest {
                    mutation: None,
                    workspace_id: workspace_id.to_string(),
                })
                .await
        }
        TuiAction::ReloadWorkspace {
            workspace_id,
            session_id,
        } => {
            client
                .reload_workspace(api::ReloadWorkspaceRequest {
                    mutation: None,
                    workspace_id: workspace_id.to_string(),
                    session_id: session_id.to_string(),
                })
                .await
        }
        _ => unreachable!("TUI action was routed to the wrong SDK dispatch group"),
    }
}

fn api_prompt(value: view::PromptInput) -> api::PromptInput {
    api::PromptInput {
        text: value.text,
        attachments: value
            .attachments
            .into_iter()
            .map(|attachment| {
                use api::prompt_attachment::Source;
                match attachment {
                    view::PromptAttachment::Artifact { artifact_id, label } => {
                        api::PromptAttachment {
                            label,
                            source: Some(Source::ArtifactId(artifact_id.to_string())),
                        }
                    }
                    view::PromptAttachment::LocalFile { label, path } => api::PromptAttachment {
                        label,
                        source: Some(Source::LocalFile(path)),
                    },
                    view::PromptAttachment::InlineImage {
                        label,
                        media_type,
                        data,
                    } => api::PromptAttachment {
                        label,
                        source: Some(Source::InlineImage(api::InlineImage { media_type, data })),
                    },
                }
            })
            .collect(),
    }
}

fn api_model_target(value: view::ModelTarget) -> api::ModelTarget {
    use api::model_target::Target;
    api::ModelTarget {
        target: Some(match value {
            view::ModelTarget::ProviderDefault { provider_id } => {
                Target::ProviderDefault(provider_id.to_string())
            }
            view::ModelTarget::Session { session_id } => Target::SessionId(session_id.to_string()),
            view::ModelTarget::AgentSession { agent_session_id } => {
                Target::AgentSessionId(agent_session_id.to_string())
            }
            view::ModelTarget::Vision => Target::Vision(true),
        }),
    }
}

fn api_resolution(
    value: view::InteractionResolution,
) -> (api::InteractionResolutionKind, Vec<String>) {
    match value {
        view::InteractionResolution::ApproveOnce => {
            (api::InteractionResolutionKind::ApproveOnce, Vec::new())
        }
        view::InteractionResolution::ApproveForSession => (
            api::InteractionResolutionKind::ApproveForSession,
            Vec::new(),
        ),
        view::InteractionResolution::Decline => {
            (api::InteractionResolutionKind::Decline, Vec::new())
        }
        view::InteractionResolution::Answer { option_ids } => {
            (api::InteractionResolutionKind::Answer, option_ids)
        }
        view::InteractionResolution::AnswerQuestions { .. } => {
            unreachable!("the native TUI does not construct structured question answers")
        }
    }
}

fn api_settings_patch(value: view::SettingsPatch) -> api::SettingsPatch {
    use api::settings_patch::Patch;
    api::SettingsPatch {
        patch: Some(match value {
            view::SettingsPatch::Web {
                backend,
                credential,
            } => Patch::Web(api::WebSettingsPatch {
                backend,
                credential: credential.map(|value| value.0),
            }),
            view::SettingsPatch::Memory {
                backend,
                executable,
                global_bank,
                data_directory,
            } => Patch::Memory(api::MemorySettingsPatch {
                backend,
                executable,
                global_bank,
                data_directory,
            }),
            view::SettingsPatch::Vision { model_id } => Patch::Vision(api::VisionSettingsPatch {
                clear_model: model_id.is_none(),
                model_id: model_id.map(|id| id.to_string()),
            }),
            view::SettingsPatch::TerminalImages { mode } => {
                Patch::TerminalImages(api::TerminalImagesSettingsPatch { mode })
            }
        }),
    }
}

pub(crate) fn workspace(value: api::WorkspaceState) -> Result<view::BootstrapView, String> {
    Ok(view::BootstrapView {
        workspace_id: view::WorkspaceId::from(value.workspace_id),
        workspace_path: value.workspace_path,
        providers: value
            .providers
            .into_iter()
            .map(provider)
            .collect::<Result<_, _>>()?,
        models: value.models.into_iter().map(model).collect(),
        agents: value.agents.into_iter().map(agent).collect(),
        skills: value
            .skills
            .into_iter()
            .map(|skill| view::SkillView {
                name: skill.name,
                description: skill.description,
            })
            .collect(),
        settings: settings(required(value.settings, "workspace settings")?)?,
        sessions: value.sessions.into_iter().map(session_summary).collect(),
        active_session: value.active_session.map(session).transpose()?,
        session_bridges: value
            .session_bridges
            .into_iter()
            .map(session_bridge)
            .collect::<Result<_, _>>()?,
    })
}

fn session_bridge(value: api::SessionBridge) -> Result<view::SessionBridgeView, String> {
    Ok(view::SessionBridgeView {
        session_id: view::SessionId::from(value.session_id),
        workspace_id: view::WorkspaceId::from(value.workspace_id),
        kind: match api::OrchestratorKind::try_from(value.kind).map_err(invalid_enum)? {
            api::OrchestratorKind::Chat => view::OrchestratorKind::Chat,
            api::OrchestratorKind::Agent => view::OrchestratorKind::Agent,
            api::OrchestratorKind::Unspecified => {
                return Err("unspecified orchestrator kind".to_owned());
            }
        },
        lifecycle: match api::BridgeLifecycle::try_from(value.lifecycle).map_err(invalid_enum)? {
            api::BridgeLifecycle::Open => view::BridgeLifecycle::Open,
            api::BridgeLifecycle::Archived => view::BridgeLifecycle::Archived,
            api::BridgeLifecycle::Unspecified => {
                return Err("unspecified bridge lifecycle".to_owned());
            }
        },
        display_title: value.display_title,
        revision: value.revision,
        transport: value.transport,
        external_parent_id: value.external_parent_id,
        external_thread_id: value.external_thread_id,
        last_projected: value.last_projected.map(bridge_projection).transpose()?,
        delivery: value
            .delivery
            .map(|delivery| -> Result<view::BridgeDeliveryView, String> {
                Ok(view::BridgeDeliveryView {
                    projection: view::BridgeProjectionView {
                        kind: bridge_projection_kind(delivery.projection_kind)?,
                        turn_id: view::TurnId::from(delivery.turn_id),
                    },
                    previous_projection: delivery
                        .previous_projection
                        .map(bridge_projection)
                        .transpose()?,
                    body_sha256: delivery.body_sha256,
                    part_count: delivery.part_count,
                    completed_parts: delivery.completed_parts,
                    last_external_message_id: delivery.last_external_message_id,
                })
            })
            .transpose()?,
        live_turn_id: value.live_turn_id.map(view::TurnId::from),
        live_external_message_id: value.live_external_message_id,
        active_source_message_id: value.active_source_message_id,
    })
}

fn bridge_projection(value: api::BridgeProjection) -> Result<view::BridgeProjectionView, String> {
    Ok(view::BridgeProjectionView {
        kind: bridge_projection_kind(value.kind)?,
        turn_id: view::TurnId::from(value.turn_id),
    })
}

fn bridge_projection_kind(value: i32) -> Result<view::BridgeProjectionKind, String> {
    match api::BridgeProjectionKind::try_from(value).map_err(invalid_enum)? {
        api::BridgeProjectionKind::User => Ok(view::BridgeProjectionKind::User),
        api::BridgeProjectionKind::Assistant => Ok(view::BridgeProjectionKind::Assistant),
        api::BridgeProjectionKind::Unspecified => {
            Err("unspecified bridge projection kind".to_owned())
        }
    }
}

pub(crate) fn session(value: api::SessionState) -> Result<view::SessionView, String> {
    Ok(view::SessionView {
        id: view::SessionId::from(value.id),
        revision: value.revision,
        workspace_id: view::WorkspaceId::from(value.workspace_id),
        title: value.title,
        status_message: value.status_message,
        diagnostic_count: value.diagnostic_count,
        activity: session_activity(value.activity)?,
        selected_provider_id: value.selected_provider_id.map(view::ProviderId::from),
        selected_model_id: value.selected_model_id.map(view::ModelId::from),
        selected_model_options: value.selected_model_options.map_or_else(
            view::ModelOptions::default,
            |options| view::ModelOptions {
                reasoning_effort: options.reasoning_effort,
                fast_mode: options.fast_mode,
            },
        ),
        active_agent_session: value.active_agent_session.map(agent_session).transpose()?,
        active_turn: value.active_turn.map(turn).transpose()?,
        last_turn: value.last_turn.map(turn).transpose()?,
        next_turn_configuration_pending: value.next_turn_configuration_pending,
        next_turn_transition: value.next_turn_transition,
        context_usage: value.context_usage.map(|usage| view::ContextUsageView {
            estimated_tokens: usage.estimated_tokens,
            context_window: usage.context_window,
            compacting: usage.compacting,
        }),
        transcript: transcript(required(value.transcript, "session transcript")?)?,
        recoverable_prompt: value
            .recoverable_prompt
            .map(recoverable_prompt)
            .transpose()?,
        queue: value
            .queue
            .into_iter()
            .map(|item| view::QueueItemView {
                id: view::PromptId::from(item.id),
                summary: item.summary,
                text: item.text,
                attachment_count: item.attachment_count,
                redirecting: item.redirecting,
            })
            .collect(),
        interactions: value
            .interactions
            .into_iter()
            .map(interaction)
            .collect::<Result<_, _>>()?,
        todos: value
            .todos
            .into_iter()
            .map(todo_phase)
            .collect::<Result<_, _>>()?,
        runs: value.runs.into_iter().map(run).collect::<Result<_, _>>()?,
        runs_total: value.runs_total,
        runs_has_earlier: value.runs_has_earlier,
        notices: value
            .notices
            .into_iter()
            .map(notice)
            .collect::<Result<_, _>>()?,
        external_tool_calls: value
            .external_tool_calls
            .into_iter()
            .map(|call| view::ExternalToolCallView {
                id: call.id,
                name: call.name,
                arguments_json: call.arguments_json,
            })
            .collect(),
        created_at_ms: value.created_at_ms,
        updated_at_ms: value.updated_at_ms,
    })
}

pub(crate) fn artifact(value: api::Artifact) -> view::ArtifactView {
    view::ArtifactView {
        id: view::ArtifactId::from(value.id),
        label: value.label,
        media_type: value.media_type,
        byte_length: value.byte_length,
        data: value.data,
    }
}

pub(crate) fn diagnostics(value: api::DiagnosticsReport) -> view::DiagnosticsReport {
    view::DiagnosticsReport {
        generated_at_ms: value.generated_at_ms,
        period_days: u16::try_from(value.period_days).unwrap_or(u16::MAX),
        provider_filter: value.provider_filter.map(view::ProviderId::from),
        sessions_scanned: value.sessions_scanned,
        sessions_with_activity: value.sessions_with_activity,
        totals: diagnostics_totals(value.totals.unwrap_or_default()),
        daily: value
            .daily
            .into_iter()
            .map(|item| view::DiagnosticsDailyUsage {
                date_utc: item.date_utc,
                provider_id: view::ProviderId::from(item.provider_id),
                totals: diagnostics_totals(item.totals.unwrap_or_default()),
            })
            .collect(),
        tools: value
            .tools
            .into_iter()
            .map(|item| view::DiagnosticsToolUsage {
                provider_id: view::ProviderId::from(item.provider_id),
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
            .map(|item| view::DiagnosticsSessionUsage {
                session_id: view::SessionId::from(item.session_id),
                provider_id: view::ProviderId::from(item.provider_id),
                model: item.model,
                latest_activity_ms: item.latest_activity_ms,
                totals: diagnostics_totals(item.totals.unwrap_or_default()),
            })
            .collect(),
        notes: value.notes,
    }
}

fn diagnostics_totals(value: api::DiagnosticsUsageTotals) -> view::DiagnosticsUsageTotals {
    view::DiagnosticsUsageTotals {
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

fn provider(value: api::Provider) -> Result<view::ProviderView, String> {
    Ok(view::ProviderView {
        id: view::ProviderId::from(value.id),
        display_name: value.display_name,
        enabled: value.enabled,
        credential_configured: value.credential_configured,
        credential_kind: value.credential_kind,
        connection: connection(required(value.connection, "provider connection")?)?,
        capabilities: capabilities(value.capabilities.unwrap_or_default())?,
        authentication: value.authentication.map(authentication).transpose()?,
        model_filter_enabled: value.model_filter_enabled,
        selected_model_ids: value
            .selected_model_ids
            .into_iter()
            .map(view::ModelId::from)
            .collect(),
        model_candidates: value.model_candidates.into_iter().map(model).collect(),
    })
}

fn capabilities(value: api::ProviderCapabilities) -> Result<view::ProviderCapabilities, String> {
    let supported = value
        .supported
        .into_iter()
        .map(
            |raw| match api::ProviderCapability::try_from(raw).map_err(invalid_enum)? {
                api::ProviderCapability::Resume => Ok(view::ProviderCapability::Resume),
                api::ProviderCapability::Steering => Ok(view::ProviderCapability::Steering),
                api::ProviderCapability::Interruption => Ok(view::ProviderCapability::Interruption),
                api::ProviderCapability::ModelCatalog => Ok(view::ProviderCapability::ModelCatalog),
                api::ProviderCapability::ModelsRequireSession => {
                    Ok(view::ProviderCapability::ModelsRequireSession)
                }
                api::ProviderCapability::SessionModelConfiguration => {
                    Ok(view::ProviderCapability::SessionModelConfiguration)
                }
                api::ProviderCapability::ContextCompaction => {
                    Ok(view::ProviderCapability::ContextCompaction)
                }
                api::ProviderCapability::Approvals => Ok(view::ProviderCapability::Approvals),
                api::ProviderCapability::NativeTools => Ok(view::ProviderCapability::NativeTools),
                api::ProviderCapability::Mcp => Ok(view::ProviderCapability::Mcp),
                api::ProviderCapability::CloseSession => Ok(view::ProviderCapability::CloseSession),
                api::ProviderCapability::ExternalTools => {
                    Ok(view::ProviderCapability::ExternalTools)
                }
                api::ProviderCapability::Unspecified => {
                    Err("unspecified provider capability".into())
                }
            },
        )
        .collect::<Result<BTreeSet<_>, String>>()?;
    Ok(view::ProviderCapabilities { supported })
}

fn connection(value: api::Connection) -> Result<view::ConnectionView, String> {
    match api::ConnectionState::try_from(value.state).map_err(invalid_enum)? {
        api::ConnectionState::Disabled => Ok(view::ConnectionView::Disabled),
        api::ConnectionState::Starting => Ok(view::ConnectionView::Starting),
        api::ConnectionState::Ready => Ok(view::ConnectionView::Ready),
        api::ConnectionState::Failed => Ok(view::ConnectionView::Failed {
            message: value.message.unwrap_or_default(),
        }),
        api::ConnectionState::Disconnected => Ok(view::ConnectionView::Disconnected {
            message: value.message.unwrap_or_default(),
        }),
        api::ConnectionState::Unspecified => Err("unspecified connection state".into()),
    }
}

fn authentication(
    value: api::ProviderAuthentication,
) -> Result<view::ProviderAuthenticationView, String> {
    use api::provider_authentication::Kind;
    match Kind::try_from(value.kind).map_err(invalid_enum)? {
        Kind::Starting => Ok(view::ProviderAuthenticationView::Starting),
        Kind::ApiKeyRequired => Ok(view::ProviderAuthenticationView::ApiKeyRequired {
            dashboard_url: value.dashboard_url.unwrap_or_default(),
            credential_kind: value.credential_kind.unwrap_or_default(),
        }),
        Kind::Challenge => Ok(view::ProviderAuthenticationView::Challenge {
            verification_url: value.verification_url.unwrap_or_default(),
            user_code: value.user_code.unwrap_or_default(),
        }),
        Kind::Unspecified => Err("unspecified provider authentication".into()),
    }
}

fn model(value: api::Model) -> view::ModelView {
    let configuration = value.configuration.unwrap_or_default();
    view::ModelView {
        id: view::ModelId::from(value.id),
        provider_id: view::ProviderId::from(value.provider_id),
        model_slug: value.model_slug,
        display_name: value.display_name,
        is_default: value.is_default,
        reasoning_effort: value.reasoning_effort,
        fast_mode: value.fast_mode,
        configuration: view::ModelConfigurationView {
            reasoning_efforts: configuration.reasoning_efforts,
            fast_mode_configurable: configuration.fast_mode_configurable,
            vision_eligible: configuration.vision_eligible,
            accepts_image_input: configuration.accepts_image_input,
        },
    }
}

fn agent(value: api::AgentDefinition) -> view::AgentDefinitionView {
    view::AgentDefinitionView {
        slug: value.slug,
        description: value.description,
        system_prompt: value.system_prompt,
        first_message: value.first_message,
        model_id: value.model_id.map(view::ModelId::from),
        fallback_models: value
            .fallback_models
            .into_iter()
            .map(view::ModelId::from)
            .collect(),
        fast_mode: value.fast_mode,
        reasoning_effort: value.reasoning_effort,
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
        effective_builtin_tools: (!value.effective_builtin_tools_uses_runtime_default)
            .then_some(value.effective_builtin_tools),
        effective_capabilities: (!value.effective_capabilities_use_runtime_default)
            .then_some(value.effective_capabilities),
        policy_warnings: value.policy_warnings,
        dashboard_tools_injected: value.dashboard_tools_injected,
        policy_projection_version: value.policy_projection_version,
    }
}

fn settings(value: api::Settings) -> Result<view::SettingsView, String> {
    let web = required(value.web, "web settings")?;
    let memory = required(value.memory, "memory settings")?;
    let vision = required(value.vision, "vision settings")?;
    Ok(view::SettingsView {
        web: view::WebSettingsView {
            backend: web.backend,
            credential_configured: web.credential_configured,
            agent_browser: agent_browser(required(web.agent_browser, "agent browser")?)?,
        },
        memory: view::MemorySettingsView {
            backend: memory.backend,
            executable: memory.executable,
            global_bank: memory.global_bank,
            data_directory: memory.data_directory,
            configured: memory.configured,
            available: memory.available,
        },
        vision: view::VisionSettingsView {
            model_id: vision.model_id.map(view::ModelId::from),
        },
        terminal_images: match api::TerminalImageMode::try_from(value.terminal_images)
            .map_err(invalid_enum)?
        {
            api::TerminalImageMode::Auto => view::TerminalImageModeView::Auto,
            api::TerminalImageMode::On => view::TerminalImageModeView::On,
            api::TerminalImageMode::Off => view::TerminalImageModeView::Off,
            api::TerminalImageMode::Unspecified => {
                return Err("unspecified terminal image mode".into());
            }
        },
    })
}

fn agent_browser(value: api::AgentBrowser) -> Result<view::AgentBrowserView, String> {
    use api::agent_browser::State;
    match State::try_from(value.state).map_err(invalid_enum)? {
        State::Checking => Ok(view::AgentBrowserView::Checking),
        State::Available => Ok(view::AgentBrowserView::Available {
            version: value.version.unwrap_or_default(),
        }),
        State::Unavailable => Ok(view::AgentBrowserView::Unavailable),
        State::Unspecified => Err("unspecified agent browser state".into()),
    }
}

fn session_summary(value: api::SessionSummary) -> view::SessionSummary {
    view::SessionSummary {
        id: view::SessionId::from(value.id),
        workspace_id: view::WorkspaceId::from(value.workspace_id),
        title: value.title,
        active_provider_id: value.active_provider_id.map(view::ProviderId::from),
        active_model_id: value.active_model_id.map(view::ModelId::from),
        updated_at_ms: value.updated_at_ms,
        created_at_ms: value.created_at_ms,
        owned_provider_sessions: value
            .owned_provider_sessions
            .into_iter()
            .map(|resource| view::OwnedProviderSessionView {
                provider_id: view::ProviderId::from(resource.provider_id),
                native_session_id: resource.native_session_id,
            })
            .collect(),
        running: value.running,
    }
}

fn session_activity(raw: i32) -> Result<view::SessionActivity, String> {
    match api::SessionActivity::try_from(raw).map_err(invalid_enum)? {
        api::SessionActivity::Idle => Ok(view::SessionActivity::Idle),
        api::SessionActivity::CreatingAgentSession => {
            Ok(view::SessionActivity::CreatingAgentSession)
        }
        api::SessionActivity::StartingTurn => Ok(view::SessionActivity::StartingTurn),
        api::SessionActivity::RunningTurn => Ok(view::SessionActivity::RunningTurn),
        api::SessionActivity::CompactingContext => Ok(view::SessionActivity::CompactingContext),
        api::SessionActivity::RunningDelegates => Ok(view::SessionActivity::RunningDelegates),
        api::SessionActivity::RunningShell => Ok(view::SessionActivity::RunningShell),
        api::SessionActivity::Unspecified => Err("unspecified session activity".into()),
    }
}

fn agent_session(value: api::AgentSession) -> Result<view::AgentSessionView, String> {
    Ok(view::AgentSessionView {
        id: view::AgentSessionId::from(value.id),
        provider_id: view::ProviderId::from(value.provider_id),
        model_id: value.model_id.map(view::ModelId::from),
        role: value.role,
        capabilities: capabilities(value.capabilities.unwrap_or_default())?,
        connection: connection(required(value.connection, "agent session connection")?)?,
        native_session_id: value.native_session_id,
        transcript: transcript(value.transcript.unwrap_or_default())?,
        usage: token_usage(value.usage.unwrap_or_default()),
    })
}

fn token_usage(value: api::TokenUsage) -> view::TokenUsageView {
    view::TokenUsageView {
        input_tokens: value.input_tokens,
        output_tokens: value.output_tokens,
        cached_input_tokens: value.cached_input_tokens,
        cache_write_tokens: value.cache_write_tokens,
    }
}

fn turn(value: api::Turn) -> Result<view::TurnView, String> {
    Ok(view::TurnView {
        id: view::TurnId::from(value.id),
        agent_session_id: view::AgentSessionId::from(value.agent_session_id),
        model_id: value.model_id.map(view::ModelId::from),
        resolved_model_options: value.resolved_model_options.map_or_else(
            view::ModelOptions::default,
            |options| view::ModelOptions {
                reasoning_effort: options.reasoning_effort,
                fast_mode: options.fast_mode,
            },
        ),
        status: match api::TurnStatus::try_from(value.status).map_err(invalid_enum)? {
            api::TurnStatus::Starting => view::TurnStatus::Starting,
            api::TurnStatus::Running => view::TurnStatus::Running,
            api::TurnStatus::Cancelling => view::TurnStatus::Cancelling,
            api::TurnStatus::Completed => view::TurnStatus::Completed,
            api::TurnStatus::Interrupted => view::TurnStatus::Interrupted,
            api::TurnStatus::Failed => view::TurnStatus::Failed,
            api::TurnStatus::Unspecified => return Err("unspecified turn status".into()),
        },
    })
}

fn transcript(value: api::TranscriptPage) -> Result<view::TranscriptPage, String> {
    Ok(view::TranscriptPage {
        entries: value
            .entries
            .into_iter()
            .map(transcript_entry)
            .collect::<Result<_, _>>()?,
        has_earlier: value.has_earlier,
        stream_active: value.stream_active,
        stream_label: value.stream_label,
    })
}

fn transcript_entry(value: api::TranscriptEntry) -> Result<view::TranscriptEntryView, String> {
    Ok(view::TranscriptEntryView {
        id: view::EntryId::from(value.id),
        kind: match api::TranscriptEntryKind::try_from(value.kind).map_err(invalid_enum)? {
            api::TranscriptEntryKind::System => view::TranscriptEntryKind::System,
            api::TranscriptEntryKind::User => view::TranscriptEntryKind::User,
            api::TranscriptEntryKind::Assistant => view::TranscriptEntryKind::Assistant,
            api::TranscriptEntryKind::Steering => view::TranscriptEntryKind::Steering,
            api::TranscriptEntryKind::Reasoning => view::TranscriptEntryKind::Reasoning,
            api::TranscriptEntryKind::Tool => view::TranscriptEntryKind::Tool,
            api::TranscriptEntryKind::Diff => view::TranscriptEntryKind::Diff,
            api::TranscriptEntryKind::Warning => view::TranscriptEntryKind::Warning,
            api::TranscriptEntryKind::Error => view::TranscriptEntryKind::Error,
            api::TranscriptEntryKind::Unspecified => {
                return Err("unspecified transcript kind".into());
            }
        },
        title: value.title,
        body: value.body,
        body_start_byte: value.body_start_byte,
        body_total_bytes: value.body_total_bytes,
        status: match api::TranscriptEntryStatus::try_from(value.status).map_err(invalid_enum)? {
            api::TranscriptEntryStatus::Running => view::TranscriptEntryStatus::Running,
            api::TranscriptEntryStatus::Complete => view::TranscriptEntryStatus::Complete,
            api::TranscriptEntryStatus::Failed => view::TranscriptEntryStatus::Failed,
            api::TranscriptEntryStatus::Interrupted => view::TranscriptEntryStatus::Interrupted,
            api::TranscriptEntryStatus::Unspecified => {
                return Err("unspecified transcript status".into());
            }
        },
        artifacts: value
            .artifact_ids
            .into_iter()
            .map(view::ArtifactId::from)
            .collect(),
        provider_id: value.provider_id,
        model_id: value.model_id.map(view::ModelId::from),
        owner_turn_id: None,
        resolved_reasoning_effort: None,
        resolved_fast_mode: None,
        source_transport: value.source_transport,
        tool_audit_json: value.tool_audit_json,
        created_at_ms: value.created_at_ms,
    })
}

fn recoverable_prompt(
    value: api::RecoverablePrompt,
) -> Result<view::RecoverablePromptView, String> {
    Ok(view::RecoverablePromptView {
        id: view::PromptId::from(value.id),
        text: value.text,
        attachments: value
            .attachments
            .into_iter()
            .map(prompt_attachment)
            .collect::<Result<_, _>>()?,
    })
}

fn prompt_attachment(value: api::PromptAttachment) -> Result<view::PromptAttachment, String> {
    use api::prompt_attachment::Source;
    match required(value.source, "prompt attachment source")? {
        Source::ArtifactId(id) => Ok(view::PromptAttachment::Artifact {
            artifact_id: view::ArtifactId::from(id),
            label: value.label,
        }),
        Source::LocalFile(path) => Ok(view::PromptAttachment::LocalFile {
            label: value.label,
            path,
        }),
        Source::InlineImage(image) => Ok(view::PromptAttachment::InlineImage {
            label: value.label,
            media_type: image.media_type,
            data: image.data,
        }),
    }
}

fn interaction(value: api::Interaction) -> Result<view::InteractionView, String> {
    Ok(view::InteractionView {
        id: view::InteractionId::from(value.id),
        revision: value.revision,
        kind: match api::InteractionKind::try_from(value.kind).map_err(invalid_enum)? {
            api::InteractionKind::Approval => view::InteractionKind::Approval,
            api::InteractionKind::Question => view::InteractionKind::Question,
            api::InteractionKind::Unspecified => return Err("unspecified interaction kind".into()),
        },
        status: match api::InteractionStatus::try_from(value.status).map_err(invalid_enum)? {
            api::InteractionStatus::Pending => view::InteractionStatus::Pending,
            api::InteractionStatus::Resolved => view::InteractionStatus::Resolved,
            api::InteractionStatus::Declined => view::InteractionStatus::Declined,
            api::InteractionStatus::Cancelled => view::InteractionStatus::Cancelled,
            api::InteractionStatus::Unspecified => {
                return Err("unspecified interaction status".into());
            }
        },
        title: value.title,
        detail: value.detail,
        options: value
            .options
            .into_iter()
            .map(|option| view::InteractionOptionView {
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
            .map(|question| view::InteractionQuestionView {
                id: question.id,
                title: question.title,
                detail: question.detail,
                options: question
                    .options
                    .into_iter()
                    .map(|option| view::InteractionOptionView {
                        id: option.id,
                        label: option.label,
                        description: option.description,
                        recommended: option.recommended,
                    })
                    .collect(),
                multiple: question.multiple,
            })
            .collect(),
    })
}

fn todo_phase(value: api::TodoPhase) -> Result<view::TodoPhaseView, String> {
    Ok(view::TodoPhaseView {
        name: value.name,
        tasks: value
            .tasks
            .into_iter()
            .map(|item| {
                Ok(view::TodoItemView {
                    content: item.content,
                    status: match api::TodoStatus::try_from(item.status).map_err(invalid_enum)? {
                        api::TodoStatus::Pending => view::TodoStatusView::Pending,
                        api::TodoStatus::InProgress => view::TodoStatusView::InProgress,
                        api::TodoStatus::Completed => view::TodoStatusView::Completed,
                        api::TodoStatus::Abandoned => view::TodoStatusView::Abandoned,
                        api::TodoStatus::Unspecified => {
                            return Err("unspecified todo status".into());
                        }
                    },
                })
            })
            .collect::<Result<_, String>>()?,
    })
}

pub(crate) fn run(value: api::RunState) -> Result<view::RunView, String> {
    Ok(view::RunView {
        id: view::RunId::from(value.id),
        parent_run_id: value.parent_run_id.map(view::RunId::from),
        agent_slug: value.agent_slug,
        archetype_purpose: value.archetype_purpose,
        provider_id: view::ProviderId::from(value.provider_id),
        model_id: value.model_id.map(view::ModelId::from),
        reasoning_effort: value.reasoning_effort,
        fast_mode: value.fast_mode,
        started_at_ms: value.started_at_ms,
        ended_at_ms: value.ended_at_ms,
        duration_ms: value.duration_ms,
        termination_kind: value.termination_kind,
        termination_detail: value.termination_detail,
        objective_mismatch_handoff: value.objective_mismatch_handoff,
        policy: value
            .policy
            .map_or_else(view::RunPolicyView::default, |policy| view::RunPolicyView {
                allowed_capabilities: policy.allowed_capabilities,
                denied_capabilities: policy.denied_capabilities,
                allowed_tools: policy.allowed_tools,
                denied_tools: policy.denied_tools,
                provider: policy.provider,
                policy_available: policy.policy_available,
                provider_tools_restricted: policy.provider_tools_restricted,
                provider_allowed_tools: policy.provider_allowed_tools,
                unsupported_canonical_tools: policy.unsupported_canonical_tools,
                tool_profile: policy.tool_profile,
                task_shape: policy.task_shape,
                output_contract: policy.output_contract,
                timeout_seconds: policy.timeout_seconds,
                max_turns: policy.max_turns,
                can_delegate: policy.can_delegate,
                max_delegation_depth: policy.max_delegation_depth,
                remaining_delegation_depth: policy.remaining_delegation_depth,
                require_parent_attribution: policy.require_parent_attribution,
                truncated_fields: policy.truncated_fields,
            }),
        tool_denials: value
            .tool_denials
            .into_iter()
            .map(|denial| view::RunToolDenialView {
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
        native_session_id: value.native_session_id,
        usage: token_usage(value.usage.unwrap_or_default()),
        objective: value.objective,
        objective_start_byte: value.objective_start_byte,
        objective_total_bytes: value.objective_total_bytes,
        status: match api::RunStatus::try_from(value.status).map_err(invalid_enum)? {
            api::RunStatus::Starting => view::RunStatus::Starting,
            api::RunStatus::Working => view::RunStatus::Working,
            api::RunStatus::Completed => view::RunStatus::Completed,
            api::RunStatus::Interrupted => view::RunStatus::Interrupted,
            api::RunStatus::Failed => view::RunStatus::Failed,
            api::RunStatus::Unspecified => return Err("unspecified run status".into()),
        },
        latest_activity: value.latest_activity,
        latest_activity_start_byte: value.latest_activity_start_byte,
        latest_activity_total_bytes: value.latest_activity_total_bytes,
        outcome: value.outcome.map(run_outcome).transpose()?,
        outcome_start_byte: value.outcome_start_byte,
        outcome_total_bytes: value.outcome_total_bytes,
        result: value.result,
        result_start_byte: value.result_start_byte,
        result_total_bytes: value.result_total_bytes,
        transcript: transcript(required(value.transcript, "run transcript")?)?,
    })
}

fn run_outcome(value: api::RunOutcome) -> Result<view::RunOutcome, String> {
    use api::run_outcome::Kind;
    match Kind::try_from(value.kind).map_err(invalid_enum)? {
        Kind::Completed => Ok(view::RunOutcome::Completed { body: value.body }),
        Kind::Failed => Ok(view::RunOutcome::Failed { reason: value.body }),
        Kind::Interrupted => Ok(view::RunOutcome::Interrupted { reason: value.body }),
        Kind::Unspecified => Err("unspecified run outcome".into()),
    }
}

fn notice(value: api::Notice) -> Result<view::NoticeView, String> {
    Ok(view::NoticeView {
        id: value.id,
        level: match api::NoticeLevel::try_from(value.level).map_err(invalid_enum)? {
            api::NoticeLevel::Info => view::NoticeLevel::Info,
            api::NoticeLevel::Warning => view::NoticeLevel::Warning,
            api::NoticeLevel::Error => view::NoticeLevel::Error,
            api::NoticeLevel::Unspecified => return Err("unspecified notice level".into()),
        },
        message: value.message,
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("API snapshot omitted {name}"))
}

fn invalid_enum(error: impl std::fmt::Display) -> String {
    format!("API snapshot contains an invalid enum: {error}")
}
