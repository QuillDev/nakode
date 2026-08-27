use std::{collections::BTreeSet, path::Component};

use nakode_protocol::{
    AgentBrowserView, AgentDefinitionView, AgentSessionId, AgentSessionView, ArtifactId,
    ArtifactView, BootstrapView, ConnectionView, ContextUsageView, ContinuationPropositionView,
    EntryId, ExternalToolCallView, InteractionId, InteractionKind, InteractionOptionView,
    InteractionStatus, InteractionView, MAX_ARTIFACT_BYTES, MAX_RUN_POLICY_ITEMS,
    MAX_RUN_POLICY_TEXT_BYTES, MAX_RUN_TEXT_BYTES, MAX_RUN_TOOL_DENIAL_TEXT_BYTES,
    MAX_RUN_TOOL_DENIALS, MAX_SESSION_RUNS, MAX_SESSION_RUNS_BYTES,
    MAX_TRANSCRIPT_ENTRY_BODY_BYTES, MAX_TRANSCRIPT_PAGE_BODY_BYTES, MAX_TRANSCRIPT_PAGE_ENTRIES,
    MAX_TRANSCRIPT_SNAPSHOT_BODY_BYTES, MAX_TRANSCRIPT_SNAPSHOT_ENTRIES, MemorySettingsView,
    ModelConfigurationView, ModelId, ModelOptions as ProtocolModelOptions, ModelView, NoticeLevel,
    NoticeView, PromptAttachment as ProtocolPromptAttachment, PromptId, ProviderAuthenticationView,
    ProviderCapabilities, ProviderCapability, ProviderId, ProviderView, QueueItemView,
    RecoverablePromptView, RunId, RunOutcome, RunPage, RunPolicyView, RunSalvageView, RunStatus,
    RunTextField, RunTextWindow, RunToolDenialView, RunView, SalvagedEvidenceView, SessionActivity,
    SessionId, SessionSummary, SessionView, SettingsView, SkillView, TerminalImageModeView,
    TodoItemView, TodoPhaseView, TodoStatusView, TranscriptBodyWindow, TranscriptEntryKind,
    TranscriptEntryStatus, TranscriptEntryView, TranscriptPage, TurnId, TurnStatus, TurnView,
    VisionAvailabilityView, VisionSettingsView, WebSettingsView, WorkspaceId,
};

use super::{
    AgentBrowserStatus, ConnectionState, DomainState, ProviderAuthenticationState, QuestionPrompt,
    SubagentRun, SubagentStatus,
};
use crate::{
    agent::{AgentDefinition, AgentToolProfile},
    backend::{
        BackendCapabilities, CLAUDE_PROVIDER, CODEX_PROVIDER, CURSOR_PROVIDER, CapabilitySupport,
        GLM_PROVIDER, KIMI_PROVIDER, ModelInfo, TodoStatus,
    },
    domain_transcript::{DomainTranscript, EntryKind, EntryStatus, TranscriptEntry},
    memory::MemoryBackend,
    session::{ProviderAccountRecord, ProviderRecord, SessionRecord},
    settings::TerminalImageMode,
    web::WebBackend,
};

const ID_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0xf3dc_a1f0_948e_5fa3_8f0e_5c26_c07c_42be);

#[must_use]
pub fn workspace_id(workspace: &str) -> WorkspaceId {
    WorkspaceId::from(scoped_id("workspace", workspace))
}

#[must_use]
#[allow(clippy::too_many_lines)]
// Bootstrap is the exhaustive authoritative snapshot projection. Keeping the field mapping together
// makes omissions visible when the protocol evolves and avoids partial competing projections.
pub fn bootstrap(
    state: &DomainState,
    revision: u64,
    providers: &[ProviderRecord],
    sessions: &[SessionRecord],
) -> BootstrapView {
    let workspace_id = workspace_id(&state.workspace);
    let active_session = session_view(state, revision, &workspace_id, sessions);
    let mut session_summaries = sessions
        .iter()
        .map(|session| session_summary(session, &workspace_id))
        .collect::<Vec<_>>();
    if !session_summaries
        .iter()
        .any(|session| session.id.as_str() == state.nakode_session_id)
    {
        session_summaries.insert(
            0,
            SessionSummary {
                id: SessionId::from(state.nakode_session_id.clone()),
                workspace_id: workspace_id.clone(),
                working_directory: state.working_directory.clone(),
                title: active_session.title.clone(),
                active_provider_id: provider_id(&state.backend_provider),
                active_model_id: state.selected_model.clone().map(ModelId::from),
                updated_at_ms: 0,
                created_at_ms: 0,
                last_owner_activity_at_ms: 0,
                owned_provider_sessions: owned_provider_sessions(state),
                running: state.is_busy(),
                selected_account_id: state.provider_account_id.clone(),
                routing_diagnostic: state.provider_account_routing.clone(),
            },
        );
    }

    BootstrapView {
        workspace_id: workspace_id.clone(),
        workspace_path: state.workspace.clone(),
        session_bridges: Vec::new(),
        providers: providers
            .iter()
            .map(|provider| provider_view(state, provider))
            .collect(),
        models: state
            .models
            .iter()
            .filter(|model| {
                providers
                    .iter()
                    .find(|p| p.provider == model.provider)
                    .is_none_or(|p| model_filter_passes(state, p, model))
            })
            .map(|model| model_view(state, model))
            .collect(),
        agents: state
            .agents
            .definitions()
            .iter()
            .map(|definition| AgentDefinitionView {
                slug: definition.slug.clone(),
                description: definition.description.clone(),
                system_prompt: definition.system_prompt.clone(),
                first_message: definition.first_message.clone(),
                model_id: definition.model.clone().map(ModelId::from),
                fallback_models: definition
                    .fallback_models
                    .iter()
                    .cloned()
                    .map(ModelId::from)
                    .collect(),
                fast_mode: definition.fast_mode,
                reasoning_effort: definition.reasoning_effort.clone(),
                ownership: match definition.ownership {
                    crate::agent::AgentOwnership::BuiltIn => "built_in",
                    crate::agent::AgentOwnership::OwnerDefined => "owner_defined",
                }
                .to_owned(),
                enabled: definition.enabled,
                allowed_capabilities: definition.allowed_capabilities.clone(),
                denied_capabilities: definition.denied_capabilities.clone(),
                allowed_tools: definition.allowed_tools.clone(),
                denied_tools: definition.denied_tools.clone(),
                tool_profile: match definition.tool_profile {
                    crate::agent::AgentToolProfile::None => "none",
                    crate::agent::AgentToolProfile::ReadOnly => "read_only",
                    crate::agent::AgentToolProfile::CommandRunner => "command_runner",
                    crate::agent::AgentToolProfile::BoundedWatcher => "bounded_watcher",
                    crate::agent::AgentToolProfile::Custom => "custom",
                }
                .to_owned(),
                task_shape: definition.task_shape.clone(),
                output_contract: definition.output_contract.clone(),
                timeout_seconds: definition.timeout_seconds,
                poll_interval_ms: definition.poll_interval_ms,
                max_turns: definition.max_turns,
                max_concurrency: definition.max_concurrency,
                fallback_policy: match definition.fallback_policy {
                    crate::agent::AgentFallbackPolicy::Prohibited => "prohibited",
                    crate::agent::AgentFallbackPolicy::ConfiguredOnly => "configured_only",
                }
                .to_owned(),
                can_delegate: definition.can_delegate,
                max_delegation_depth: definition.max_delegation_depth,
                require_parent_attribution: definition.require_parent_attribution,
                effective_builtin_tools: definition.builtin_tool_allowlist(),
                effective_capabilities: definition.effective_capabilities(),
                policy_warnings: definition.policy_warnings(),
                dashboard_tools_injected: false,
                policy_projection_version: 1,
            })
            .collect(),
        skills: state
            .skills
            .definitions()
            .iter()
            .map(|skill| SkillView {
                name: skill.name.clone(),
                description: skill.description.clone(),
            })
            .collect(),
        settings: settings_view(state, providers),
        sessions: session_summaries,
        active_session: Some(active_session),
    }
}

/// The levels and flags a model takes. The ONE place that rule lives — every frontend, and the
/// delegated-run path that has to refuse a level a model cannot take, reads this.
pub(crate) fn model_configuration(
    model: &ModelInfo,
    vision_add_on_enabled: bool,
) -> ModelConfigurationView {
    let mut configuration = ModelConfigurationView {
        reasoning_efforts: model.capabilities.reasoning_efforts.clone(),
        accepts_image_input: vision_add_on_enabled
            || matches!(
                model.provider.as_str(),
                CODEX_PROVIDER | CLAUDE_PROVIDER | CURSOR_PROVIDER | KIMI_PROVIDER | GLM_PROVIDER
            ),
        ..ModelConfigurationView::default()
    };
    if model.provider == CODEX_PROVIDER {
        configuration.fast_mode_configurable = true;
        configuration.vision_eligible = true;
        return configuration;
    }
    let model_id = model.id.to_ascii_lowercase();
    if model.provider == CURSOR_PROVIDER
        && (model_id.starts_with("composer-") || model_id.starts_with("grok-4.5"))
    {
        configuration.fast_mode_configurable = true;
    }
    configuration
}

fn session_view(
    state: &DomainState,
    revision: u64,
    workspace_id: &WorkspaceId,
    sessions: &[SessionRecord],
) -> SessionView {
    let session_id = SessionId::from(state.nakode_session_id.clone());
    let provider = provider_id(&state.backend_provider);
    let agent_session = agent_session_view(state, &session_id, provider.as_ref());
    let active_turn = turn_view(state, &session_id, agent_session.as_ref());
    let last_turn = last_turn_view(state, &session_id, agent_session.as_ref());
    let (runs, runs_has_earlier) = run_views(state);

    let selected_options = state.selected_model_options();
    let persisted = sessions
        .iter()
        .find(|session| session.id == state.nakode_session_id);
    let created_at_ms = persisted.map_or(0, |session| {
        unix_seconds_to_milliseconds(session.created_at)
    });
    let updated_at_ms = persisted.map_or(0, |session| {
        unix_seconds_to_milliseconds(session.updated_at)
    });
    let last_owner_activity_at_ms = persisted
        .and_then(|session| session.last_owner_activity_at)
        .map_or(0, unix_seconds_to_milliseconds);
    SessionView {
        id: session_id.clone(),
        revision,
        workspace_id: workspace_id.clone(),
        working_directory: state.working_directory.clone(),
        title: session_title(state, sessions),
        code_mode: state.code_mode(),
        status_message: state.status_message.clone(),
        diagnostic_count: u64::try_from(state.diagnostic_count).unwrap_or(u64::MAX),
        activity: activity(state),
        selected_provider_id: provider,
        selected_account_id: state.provider_account_id.clone(),
        routing_diagnostic: state.provider_account_routing.clone(),
        selected_model_id: state.selected_model.clone().map(ModelId::from),
        selected_model_options: ProtocolModelOptions {
            reasoning_effort: selected_options.reasoning_effort,
            fast_mode: selected_options.fast_mode,
        },
        active_agent_session: agent_session,
        active_turn,
        last_turn,
        next_turn_configuration_pending: next_turn_configuration_pending(state),
        next_turn_transition: next_turn_transition(state),
        context_usage: context_usage_view(state),
        transcript: transcript_page(&state.transcript),
        recoverable_prompt: recoverable_prompt_view(state),
        queue: queue_views(state),
        interactions: interactions(state, revision),
        todos: todo_views(state),
        runs,
        runs_total: Some(u64::try_from(state.subagents.len()).unwrap_or(u64::MAX)),
        runs_has_earlier,
        notices: notice_views(state, revision),
        external_tool_calls: state
            .external_tool_calls
            .iter()
            .map(|call| ExternalToolCallView {
                id: call.id.clone(),
                name: call.name.clone(),
                arguments_json: call.arguments_json.clone(),
            })
            .collect(),
        created_at_ms,
        updated_at_ms,
        last_owner_activity_at_ms,
    }
}

fn recoverable_prompt_view(state: &DomainState) -> Option<RecoverablePromptView> {
    let prompt = state.recoverable_prompt()?;
    let entry_key = format!("user:{}", prompt.id);
    let entry = state
        .transcript
        .entries()
        .iter()
        .find(|entry| entry.key.as_deref() == Some(entry_key.as_str()));
    let mut image_artifacts = entry.into_iter().flat_map(|entry| {
        state
            .transcript
            .image_artifacts(entry)
            .enumerate()
            .map(move |(index, _)| transcript_artifact_id(&entry.id, index))
    });
    let attachments = prompt
        .attachments
        .iter()
        .map(|attachment| {
            if attachment.image.is_some() {
                return image_artifacts.next().map(|artifact_id| {
                    ProtocolPromptAttachment::Artifact {
                        artifact_id,
                        label: attachment.label.clone(),
                    }
                });
            }
            let path = attachment.path.as_deref().filter(|path| {
                !path.as_os_str().is_empty()
                    && !path.is_absolute()
                    && path.components().all(|component| {
                        !matches!(
                            component,
                            Component::ParentDir | Component::RootDir | Component::Prefix(_)
                        )
                    })
            })?;
            Some(ProtocolPromptAttachment::LocalFile {
                label: attachment.label.clone(),
                path: path.to_str()?.to_owned(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    if image_artifacts.next().is_some() {
        return None;
    }
    Some(RecoverablePromptView {
        id: PromptId::from(prompt.id.clone()),
        text: prompt.text.clone(),
        attachments,
    })
}

fn session_title(state: &DomainState, sessions: &[SessionRecord]) -> String {
    sessions
        .iter()
        .find(|session| session.id == state.nakode_session_id)
        .map_or_else(
            || {
                state
                    .transcript
                    .entries()
                    .iter()
                    .find(|entry| entry.kind == EntryKind::User)
                    .map_or_else(|| "New session".to_owned(), |entry| first_line(&entry.body))
            },
            |session| session.title.clone(),
        )
}

fn agent_session_view(
    state: &DomainState,
    session_id: &SessionId,
    provider_id: Option<&ProviderId>,
) -> Option<AgentSessionView> {
    let provider_id = provider_id?;
    Some(AgentSessionView {
        id: AgentSessionId::from(scoped_id(
            "agent-session",
            &format!("{}:{}:primary", session_id.as_str(), provider_id.as_str()),
        )),
        provider_id: provider_id.clone(),
        account_id: state.provider_account_id.clone(),
        model_id: state.selected_model.clone().map(ModelId::from),
        role: "primary".to_owned(),
        capabilities: capabilities_view(&state.backend_capabilities),
        connection: connection_view(&state.connection),
        native_session_id: state.provider_session_id.clone(),
        transcript: transcript_page(&state.transcript),
        usage: token_usage_view(state.provider_usage),
    })
}

fn turn_view(
    state: &DomainState,
    session_id: &SessionId,
    agent_session: Option<&AgentSessionView>,
) -> Option<TurnView> {
    let agent_session = agent_session?;
    if let Some(turn) = state.active_turn.as_ref() {
        return Some(TurnView {
            id: TurnId::from(scoped_id(
                "turn",
                &format!("{}:{}", session_id.as_str(), turn.id),
            )),
            agent_session_id: agent_session.id.clone(),
            model_id: turn.model.clone().map(ModelId::from),
            resolved_model_options: ProtocolModelOptions {
                reasoning_effort: turn.options.reasoning_effort.clone(),
                fast_mode: turn.options.fast_mode,
            },
            status: if turn.cancelling {
                TurnStatus::Cancelling
            } else {
                TurnStatus::Running
            },
        });
    }
    let starting = state
        .starting_turn
        .as_ref()
        .or(state.pending_session_prompt.as_ref())?;
    Some(TurnView {
        id: TurnId::from(scoped_id(
            "turn-starting",
            &format!("{}:{}", session_id.as_str(), starting.id),
        )),
        agent_session_id: agent_session.id.clone(),
        model_id: starting.resolved_model.clone().map(ModelId::from),
        resolved_model_options: ProtocolModelOptions {
            reasoning_effort: starting.options.reasoning_effort.clone(),
            fast_mode: starting.options.fast_mode,
        },
        status: TurnStatus::Starting,
    })
}

fn last_turn_view(
    state: &DomainState,
    session_id: &SessionId,
    agent_session: Option<&AgentSessionView>,
) -> Option<TurnView> {
    let turn = state.last_turn.as_ref()?;
    let agent_session = agent_session?;
    Some(TurnView {
        id: TurnId::from(scoped_id(
            "turn",
            &format!("{}:{}", session_id.as_str(), turn.id),
        )),
        agent_session_id: agent_session.id.clone(),
        model_id: turn.model.clone().map(ModelId::from),
        resolved_model_options: ProtocolModelOptions {
            reasoning_effort: turn.options.reasoning_effort.clone(),
            fast_mode: turn.options.fast_mode,
        },
        status: match turn.outcome {
            crate::backend::TurnOutcome::Completed => TurnStatus::Completed,
            crate::backend::TurnOutcome::Interrupted => TurnStatus::Interrupted,
            crate::backend::TurnOutcome::Failed => TurnStatus::Failed,
        },
    })
}

fn next_turn_configuration_pending(state: &DomainState) -> bool {
    let current = state.active_turn.as_ref().map_or_else(
        || {
            state
                .starting_turn
                .as_ref()
                .or(state.pending_session_prompt.as_ref())
                .map(|turn| (turn.resolved_model.as_ref(), &turn.options))
        },
        |turn| Some((turn.model.as_ref(), &turn.options)),
    );
    let Some((model, options)) = current else {
        return false;
    };
    model != state.selected_model.as_ref() || *options != state.selected_model_options()
}

fn next_turn_transition(state: &DomainState) -> Option<String> {
    let selected_provider = state
        .selected_model
        .as_deref()
        .and_then(|model| model.split_once('/').map(|(provider, _)| provider));
    let changes_provider =
        selected_provider.is_some_and(|provider| provider != state.backend_provider);
    if next_turn_configuration_pending(state) {
        return Some(if changes_provider {
            "Active owner work keeps its captured configuration; the selected configuration starts with the next owner turn in a fresh provider-native session with a continuity handoff."
                .to_owned()
        } else {
            "Active owner work keeps its captured configuration; the selected configuration starts with the next owner turn."
                .to_owned()
        });
    }
    if state.active_turn.is_none()
        && state.starting_turn.is_none()
        && state.pending_session_prompt.is_none()
    {
        return Some(if changes_provider && state.provider_session_id.is_some() {
            "Selected configuration applies to the next owner turn in a fresh provider-native session with a continuity handoff."
                .to_owned()
        } else {
            "Selected configuration applies to the next owner turn.".to_owned()
        });
    }
    None
}

fn context_usage_view(state: &DomainState) -> Option<ContextUsageView> {
    state.context_usage.map(|usage| ContextUsageView {
        estimated_tokens: u64::try_from(usage.estimated_tokens).unwrap_or(u64::MAX),
        context_window: usage
            .context_window
            .map(|window| u64::try_from(window).unwrap_or(u64::MAX)),
        compacting: state.context_compaction.is_some(),
    })
}

pub(super) fn queue_views(state: &DomainState) -> Vec<QueueItemView> {
    let native_steer = state.pending_steer.as_ref().and_then(|pending| {
        pending
            .queued_origin
            .as_ref()
            .map(|origin| origin.prompt_id.as_str())
    });
    let fallback = state
        .pending_redirect
        .as_ref()
        .map(|pending| pending.prompt_id.as_str());
    state
        .queue
        .iter()
        .map(|prompt| QueueItemView {
            id: PromptId::from(prompt.id.clone()),
            summary: first_line(&prompt.text),
            text: prompt.text.clone(),
            attachment_count: u32::try_from(prompt.attachments.len()).unwrap_or(u32::MAX),
            redirecting: native_steer == Some(prompt.id.as_str())
                || fallback == Some(prompt.id.as_str()),
        })
        .collect()
}

fn todo_views(state: &DomainState) -> Vec<TodoPhaseView> {
    state
        .todo_phases
        .iter()
        .map(|phase| TodoPhaseView {
            name: phase.name.clone(),
            tasks: phase
                .tasks
                .iter()
                .map(|task| TodoItemView {
                    content: task.content.clone(),
                    status: todo_status(task.status),
                })
                .collect(),
        })
        .collect()
}

fn run_views(state: &DomainState) -> (Vec<RunView>, bool) {
    let page = projected_run_page(state, None, MAX_SESSION_RUNS)
        .expect("an unbounded recent run page always exists");
    (page.runs, page.has_earlier)
}

pub(crate) fn run_page(
    state: &DomainState,
    before: Option<&RunId>,
    limit: usize,
) -> Option<RunPage> {
    projected_run_page(state, before, limit.min(MAX_SESSION_RUNS))
}

pub(crate) fn run_view(state: &DomainState, run_id: &RunId) -> Option<RunView> {
    let run = state
        .subagents
        .iter()
        .find(|run| run.id == run_id.as_str())?;
    Some(project_run(state, run, MAX_TRANSCRIPT_PAGE_BODY_BYTES))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunTextWindowError {
    RunNotFound,
    FieldUnavailable,
    LimitOutOfBounds { actual: usize, maximum: usize },
    CursorOutOfBounds { actual: usize, total: usize },
    CursorNotUtf8Boundary { actual: usize },
    LimitTooSmallForCharacter { minimum: usize },
}

pub(crate) fn run_text_window(
    state: &DomainState,
    run_id: &RunId,
    field: RunTextField,
    before_byte: Option<u64>,
    limit_bytes: usize,
) -> Result<RunTextWindow, RunTextWindowError> {
    if limit_bytes == 0 || limit_bytes > MAX_RUN_TEXT_BYTES {
        return Err(RunTextWindowError::LimitOutOfBounds {
            actual: limit_bytes,
            maximum: MAX_RUN_TEXT_BYTES,
        });
    }
    let run = state
        .subagents
        .iter()
        .find(|run| run.id == run_id.as_str())
        .ok_or(RunTextWindowError::RunNotFound)?;
    let text = run_text(state, run, field).ok_or(RunTextWindowError::FieldUnavailable)?;
    let end = before_byte.map_or(Ok(text.len()), |before| {
        usize::try_from(before).map_err(|_| RunTextWindowError::CursorOutOfBounds {
            actual: usize::MAX,
            total: text.len(),
        })
    })?;
    if end > text.len() {
        return Err(RunTextWindowError::CursorOutOfBounds {
            actual: end,
            total: text.len(),
        });
    }
    if !text.is_char_boundary(end) {
        return Err(RunTextWindowError::CursorNotUtf8Boundary { actual: end });
    }
    let mut start = end.saturating_sub(limit_bytes);
    while start < end && !text.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    if start == end && end > 0 {
        let minimum = text[..end].chars().next_back().map_or(1, char::len_utf8);
        return Err(RunTextWindowError::LimitTooSmallForCharacter { minimum });
    }
    Ok(RunTextWindow {
        run_id: run_id.clone(),
        field,
        text: text[start..end].to_owned(),
        start_byte: u64::try_from(start).unwrap_or(u64::MAX),
        total_bytes: u64::try_from(text.len()).unwrap_or(u64::MAX),
        has_earlier: start > 0,
    })
}

fn run_text<'a>(
    state: &'a DomainState,
    run: &'a SubagentRun,
    field: RunTextField,
) -> Option<&'a str> {
    match field {
        RunTextField::Objective => Some(&run.objective),
        RunTextField::LatestActivity => Some(&run.latest_activity),
        RunTextField::Outcome => run_outcome_text(state, run),
        RunTextField::Result
            if matches!(
                run_status(run.status),
                RunStatus::Completed | RunStatus::Partial
            ) =>
        {
            run_outcome_text(state, run)
        }
        RunTextField::Result => None,
    }
}

fn run_outcome_text<'a>(state: &'a DomainState, run: &'a SubagentRun) -> Option<&'a str> {
    match run_status(run.status) {
        RunStatus::Starting | RunStatus::Working => None,
        RunStatus::Completed => {
            latest_run_transcript_body(state, run, |entry| entry.kind == EntryKind::Assistant)
                .or(Some(&run.latest_activity))
        }
        RunStatus::Partial => latest_run_transcript_body(state, run, |entry| {
            entry.kind == EntryKind::Assistant
                || (entry.kind == EntryKind::System && entry.title == "SALVAGED PARTIAL RESULT")
        })
        .or(Some(&run.latest_activity)),
        RunStatus::Failed => {
            latest_run_transcript_body(state, run, |entry| entry.kind == EntryKind::Error)
                .or(Some(&run.latest_activity))
        }
        RunStatus::Interrupted => latest_run_transcript_body(state, run, |entry| {
            entry.status == EntryStatus::Interrupted
                && matches!(
                    entry.kind,
                    EntryKind::System | EntryKind::Warning | EntryKind::Error
                )
        })
        .or(Some(&run.latest_activity)),
    }
}

fn latest_run_transcript_body<'a>(
    state: &'a DomainState,
    run: &SubagentRun,
    predicate: impl Fn(&TranscriptEntry) -> bool,
) -> Option<&'a str> {
    state
        .subagent_chats
        .get(&run.id)?
        .transcript
        .entries()
        .iter()
        .rev()
        .find(|entry| predicate(entry))
        .map(|entry| entry.body.as_str())
}

fn projected_run_page(
    state: &DomainState,
    before: Option<&RunId>,
    limit: usize,
) -> Option<RunPage> {
    let end = before.map_or(state.subagents.len(), |before| {
        state
            .subagents
            .iter()
            .position(|run| run.id == before.as_str())
            .unwrap_or(usize::MAX)
    });
    if end == usize::MAX {
        return None;
    }

    let mut runs = Vec::with_capacity(limit.min(end));
    let mut remaining_bytes = MAX_SESSION_RUNS_BYTES;
    for run in state.subagents[..end].iter().rev().take(limit) {
        let projection = project_run(state, run, MAX_TRANSCRIPT_PAGE_BODY_BYTES / 16);
        let encoded_bytes =
            serde_json::to_vec(&projection).map_or(remaining_bytes, |value| value.len());
        if encoded_bytes > remaining_bytes && !runs.is_empty() {
            break;
        }
        remaining_bytes = remaining_bytes.saturating_sub(encoded_bytes);
        runs.push(projection);
    }
    runs.reverse();
    Some(RunPage {
        has_earlier: runs.len() < end,
        runs,
    })
}

fn originating_owner_entry(state: &DomainState, run: &SubagentRun) -> Option<TranscriptEntryView> {
    run.observability
        .originating_owner_entry_id
        .as_deref()
        .and_then(|entry_id| {
            state
                .transcript
                .entries()
                .iter()
                .find(|entry| entry.id == entry_id)
        })
        .map(|entry| {
            let body = bounded_text(&entry.body).value;
            transcript_entry_view(&state.transcript, entry, &body, false)
        })
}

fn project_run(state: &DomainState, run: &SubagentRun, body_budget: usize) -> RunView {
    let transcript = state
        .subagent_chats
        .get(&run.id)
        .map_or_else(empty_transcript_page, |chat| {
            projected_transcript_page(
                &chat.transcript,
                None,
                MAX_TRANSCRIPT_PAGE_ENTRIES,
                body_budget,
            )
            .expect("an unbounded recent transcript page always exists")
        });
    let status = run_status(run.status);
    let objective = bounded_text(&run.objective);
    let latest_activity = bounded_text(&run.latest_activity);
    let (outcome, outcome_window) = run_outcome_projection(
        status,
        &latest_activity,
        &transcript,
        canonical_run_outcome_window(state, run, status),
    );
    let result = match status {
        RunStatus::Completed | RunStatus::Partial => Some(outcome_window.clone()),
        _ => None,
    };
    let (policy, reasoning_effort, fast_mode) = run_policy(run);
    let (tool_denials, tool_denials_retained_total) = projected_tool_denials(state, run);
    let ended_at_ms = run.observability.ended_at_ms;
    let originating_owner_entry = originating_owner_entry(state, run);
    RunView {
        id: RunId::from(run.id.clone()),
        parent_run_id: run.observability.parent_run_id.clone().map(RunId::from),
        agent_slug: run.agent.clone(),
        archetype_purpose: run.observability.archetype_purpose.clone(),
        provider_id: ProviderId::from(run.provider.clone()),
        model_id: run.model.clone().map(ModelId::from),
        reasoning_effort,
        fast_mode,
        started_at_ms: run.observability.started_at_ms,
        ended_at_ms,
        duration_ms: ended_at_ms.map(|ended| ended.saturating_sub(run.observability.started_at_ms)),
        termination_kind: run.observability.termination_kind.clone(),
        termination_detail: run.observability.termination_detail.clone(),
        objective_mismatch_handoff: run.observability.objective_mismatch_handoff.clone(),
        policy,
        tool_denials,
        salvage: run.observability.salvage.as_ref().map(project_salvage),
        continued_from_run_id: run
            .observability
            .continued_from_run_id
            .clone()
            .map(RunId::from),
        continued_by_run_id: run
            .observability
            .continued_by_run_id
            .clone()
            .map(RunId::from),
        continuation_depth: run.observability.continuation_depth,
        additional_turns: run.observability.additional_turns,
        inherited_evidence: run
            .observability
            .inherited_evidence
            .iter()
            .map(project_salvaged_evidence)
            .collect(),
        tool_denials_retained_total,
        native_session_id: run.provider_session_id.clone(),
        usage: token_usage_view(run.usage),
        objective: objective.value,
        objective_start_byte: objective.start_byte,
        objective_total_bytes: objective.total_bytes,
        status,
        latest_activity: latest_activity.value,
        latest_activity_start_byte: latest_activity.start_byte,
        latest_activity_total_bytes: latest_activity.total_bytes,
        outcome,
        outcome_start_byte: outcome_window.start_byte,
        outcome_total_bytes: outcome_window.total_bytes,
        result: result.as_ref().map(|window| window.value.clone()),
        result_start_byte: result.as_ref().map_or(0, |window| window.start_byte),
        result_total_bytes: result.as_ref().map_or(0, |window| window.total_bytes),
        invocation_turn_id: run.observability.invocation_turn_id.clone(),
        invocation_call_id: run.observability.invocation_call_id.clone(),
        originating_owner_entry,
        transcript,
    }
}

fn project_salvaged_evidence(evidence: &crate::session::SalvagedEvidence) -> SalvagedEvidenceView {
    SalvagedEvidenceView {
        entry_id: evidence.entry_id.clone(),
        title: bounded_text(&evidence.title).value,
        body: bounded_text(&evidence.body).value,
        truncated: evidence.truncated || evidence.body.len() > MAX_RUN_TEXT_BYTES,
    }
}

fn project_salvage(salvage: &crate::session::SubagentSalvage) -> RunSalvageView {
    RunSalvageView {
        terminal_reason: salvage.terminal_reason.clone(),
        original_objective: bounded_text(&salvage.original_objective).value,
        completed_work: salvage
            .completed_work
            .iter()
            .map(|value| bounded_text(value).value)
            .collect(),
        verified_evidence: salvage
            .verified_evidence
            .iter()
            .map(project_salvaged_evidence)
            .collect(),
        last_successful_evidence: salvage
            .last_successful_evidence
            .as_ref()
            .map(project_salvaged_evidence),
        unresolved_questions: salvage
            .unresolved_questions
            .iter()
            .map(|value| bounded_text(value).value)
            .collect(),
        continuation: ContinuationPropositionView {
            verified_findings: salvage
                .continuation
                .verified_findings
                .iter()
                .map(|value| bounded_text(value).value)
                .collect(),
            unresolved_boundary: bounded_text(&salvage.continuation.unresolved_boundary).value,
            why_it_matters: bounded_text(&salvage.continuation.why_it_matters).value,
            recommended_archetype: salvage.continuation.recommended_archetype.clone(),
            follow_up_objective: bounded_text(&salvage.continuation.follow_up_objective).value,
            inherited_evidence: salvage.continuation.inherited_evidence.clone(),
            can_proceed_independently: salvage.continuation.can_proceed_independently,
        },
        can_resume: salvage.can_resume,
        redacted: salvage.redacted,
        truncated: salvage.truncated,
    }
}

fn run_policy(run: &SubagentRun) -> (RunPolicyView, Option<String>, bool) {
    let parsed_definition = serde_json::from_str::<AgentDefinition>(&run.observability.policy_json);
    let policy_available = parsed_definition
        .as_ref()
        .is_ok_and(|_| run.observability.policy_json.trim() != "{}");
    let definition = parsed_definition.unwrap_or_else(|_| AgentDefinition {
        slug: run.agent.clone(),
        description: run.observability.archetype_purpose.clone(),
        ..AgentDefinition::default()
    });
    let tool_profile = match definition.tool_profile {
        AgentToolProfile::None => "none",
        AgentToolProfile::ReadOnly => "read_only",
        AgentToolProfile::CommandRunner => "command_runner",
        AgentToolProfile::BoundedWatcher => "bounded_watcher",
        AgentToolProfile::Custom => "custom",
    }
    .to_owned();
    let mut truncated_fields = Vec::new();
    let reasoning_effort = definition
        .reasoning_effort
        .as_deref()
        .map(|value| bounded_policy_text(value, "reasoning_effort", &mut truncated_fields));
    let fast_mode = definition.fast_mode;
    let effective_canonical_tools = definition.builtin_tool_allowlist();
    let allowed_capabilities = bounded_policy_list(
        definition.allowed_capabilities,
        "allowed_capabilities",
        &mut truncated_fields,
    );
    let denied_capabilities = bounded_policy_list(
        definition.denied_capabilities,
        "denied_capabilities",
        &mut truncated_fields,
    );
    let allowed_tools = bounded_policy_list(
        definition.allowed_tools,
        "allowed_tools",
        &mut truncated_fields,
    );
    let denied_tools = bounded_policy_list(
        definition.denied_tools,
        "denied_tools",
        &mut truncated_fields,
    );
    let provider_projection =
        crate::backend::project_provider_tools(&run.provider, effective_canonical_tools.as_deref());
    let provider = bounded_policy_text(&run.provider, "provider", &mut truncated_fields);
    let provider_tools_restricted = policy_available
        && provider_projection.enforced
        && provider_projection.allowed_tools.is_some();
    let provider_allowed_tools = bounded_policy_list(
        provider_projection.allowed_tools.unwrap_or_default(),
        "provider_allowed_tools",
        &mut truncated_fields,
    );
    let unsupported_canonical_tools = bounded_policy_list(
        provider_projection.unsupported_canonical_tools,
        "unsupported_canonical_tools",
        &mut truncated_fields,
    );
    let task_shape =
        bounded_policy_text(&definition.task_shape, "task_shape", &mut truncated_fields);
    let output_contract = bounded_policy_text(
        &definition.output_contract,
        "output_contract",
        &mut truncated_fields,
    );
    (
        RunPolicyView {
            allowed_capabilities,
            denied_capabilities,
            allowed_tools,
            denied_tools,
            provider,
            policy_available,
            provider_tools_restricted,
            provider_allowed_tools,
            unsupported_canonical_tools,
            tool_profile,
            task_shape,
            output_contract,
            timeout_seconds: definition.timeout_seconds,
            max_turns: definition.max_turns,
            can_delegate: definition.can_delegate,
            max_delegation_depth: definition.max_delegation_depth,
            remaining_delegation_depth: run.observability.remaining_delegation_depth,
            require_parent_attribution: definition.require_parent_attribution,
            truncated_fields,
        },
        reasoning_effort,
        fast_mode,
    )
}

fn bounded_policy_list(
    mut values: Vec<String>,
    name: &str,
    truncated_fields: &mut Vec<String>,
) -> Vec<String> {
    let mut truncated = values.len() > MAX_RUN_POLICY_ITEMS;
    values.truncate(MAX_RUN_POLICY_ITEMS);
    for value in &mut values {
        let bounded = bounded_text_to(value, MAX_RUN_POLICY_TEXT_BYTES);
        truncated |= bounded.start_byte > 0;
        *value = bounded.value;
    }
    if truncated {
        truncated_fields.push(name.to_owned());
    }
    values
}

fn bounded_policy_text(value: &str, name: &str, truncated_fields: &mut Vec<String>) -> String {
    let bounded = bounded_text_to(value, MAX_RUN_POLICY_TEXT_BYTES);
    if bounded.start_byte > 0 {
        truncated_fields.push(name.to_owned());
    }
    bounded.value
}

fn projected_tool_denials(state: &DomainState, run: &SubagentRun) -> (Vec<RunToolDenialView>, u32) {
    let Some(chat) = state.subagent_chats.get(&run.id) else {
        return (Vec::new(), 0);
    };
    let mut denials = chat
        .transcript
        .entries()
        .iter()
        .filter_map(|entry| {
            let audit =
                serde_json::from_str::<serde_json::Value>(entry.tool_audit_json.as_deref()?)
                    .ok()?;
            if audit.get("denied").and_then(serde_json::Value::as_bool) != Some(true) {
                return None;
            }
            let tool = bounded_text_to(
                audit
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&entry.title),
                MAX_RUN_TOOL_DENIAL_TEXT_BYTES,
            );
            let reason = bounded_text_to(
                audit
                    .get("denialReason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&entry.body),
                MAX_RUN_TOOL_DENIAL_TEXT_BYTES,
            );
            Some(RunToolDenialView {
                entry_id: entry.id.clone(),
                tool: tool.value,
                tool_start_byte: tool.start_byte,
                tool_total_bytes: tool.total_bytes,
                reason: reason.value,
                reason_start_byte: reason.start_byte,
                reason_total_bytes: reason.total_bytes,
            })
        })
        .collect::<Vec<_>>();
    let total = u32::try_from(denials.len()).unwrap_or(u32::MAX);
    if denials.len() > MAX_RUN_TOOL_DENIALS {
        denials.drain(..denials.len() - MAX_RUN_TOOL_DENIALS);
    }
    (denials, total)
}

#[cfg(test)]
fn run_outcome(
    status: RunStatus,
    latest_activity: &str,
    transcript: &TranscriptPage,
) -> Option<RunOutcome> {
    run_outcome_projection(status, &bounded_text(latest_activity), transcript, None).0
}

fn run_outcome_projection(
    status: RunStatus,
    latest_activity: &BoundedText,
    transcript: &TranscriptPage,
    canonical_window: Option<BoundedText>,
) -> (Option<RunOutcome>, BoundedText) {
    match status {
        RunStatus::Starting | RunStatus::Working => (None, BoundedText::default()),
        RunStatus::Completed => {
            let window = canonical_window.unwrap_or_else(|| {
                latest_run_entry(transcript, |entry| {
                    entry.kind == TranscriptEntryKind::Assistant
                })
                .map_or_else(
                    || latest_activity.clone(),
                    BoundedText::from_transcript_entry,
                )
            });
            (
                Some(RunOutcome::Completed {
                    body: window.value.clone(),
                }),
                window,
            )
        }
        RunStatus::Partial => {
            let window = canonical_window.unwrap_or_else(|| {
                latest_run_entry(transcript, |entry| {
                    entry.kind == TranscriptEntryKind::Assistant
                        || (entry.kind == TranscriptEntryKind::System
                            && entry.title == "SALVAGED PARTIAL RESULT")
                })
                .map_or_else(
                    || latest_activity.clone(),
                    BoundedText::from_transcript_entry,
                )
            });
            (
                Some(RunOutcome::Partial {
                    body: window.value.clone(),
                }),
                window,
            )
        }
        RunStatus::Failed => {
            let window = canonical_window.unwrap_or_else(|| {
                latest_run_entry(transcript, |entry| entry.kind == TranscriptEntryKind::Error)
                    .map_or_else(
                        || latest_activity.clone(),
                        BoundedText::from_transcript_entry,
                    )
            });
            (
                Some(RunOutcome::Failed {
                    reason: window.value.clone(),
                }),
                window,
            )
        }
        RunStatus::Interrupted => {
            let window = canonical_window.unwrap_or_else(|| {
                latest_run_entry(transcript, |entry| {
                    entry.status == TranscriptEntryStatus::Interrupted
                        && matches!(
                            entry.kind,
                            TranscriptEntryKind::System
                                | TranscriptEntryKind::Warning
                                | TranscriptEntryKind::Error
                        )
                })
                .map_or_else(
                    || latest_activity.clone(),
                    BoundedText::from_transcript_entry,
                )
            });
            (
                Some(RunOutcome::Interrupted {
                    reason: window.value.clone(),
                }),
                window,
            )
        }
    }
}

fn canonical_run_outcome_window(
    state: &DomainState,
    run: &SubagentRun,
    status: RunStatus,
) -> Option<BoundedText> {
    let transcript = &state.subagent_chats.get(&run.id)?.transcript;
    transcript
        .entries()
        .iter()
        .rev()
        .find(|entry| match status {
            RunStatus::Completed => entry.kind == EntryKind::Assistant,
            RunStatus::Partial => {
                entry.kind == EntryKind::Assistant
                    || (entry.kind == EntryKind::System && entry.title == "SALVAGED PARTIAL RESULT")
            }
            RunStatus::Failed => entry.kind == EntryKind::Error,
            RunStatus::Interrupted => {
                entry.status == EntryStatus::Interrupted
                    && matches!(
                        entry.kind,
                        EntryKind::System | EntryKind::Warning | EntryKind::Error
                    )
            }
            RunStatus::Starting | RunStatus::Working => false,
        })
        .map(|entry| bounded_text(&entry.body))
}

fn latest_run_entry(
    transcript: &TranscriptPage,
    predicate: impl Fn(&TranscriptEntryView) -> bool,
) -> Option<&TranscriptEntryView> {
    transcript
        .entries
        .iter()
        .rev()
        .find(|entry| predicate(entry))
}

#[derive(Clone, Debug, Default)]
struct BoundedText {
    value: String,
    start_byte: u64,
    total_bytes: u64,
}

impl BoundedText {
    fn from_transcript_entry(entry: &TranscriptEntryView) -> Self {
        Self {
            value: entry.body.clone(),
            start_byte: entry.body_start_byte,
            total_bytes: entry.body_total_bytes,
        }
    }
}

fn bounded_text(value: &str) -> BoundedText {
    bounded_text_to(value, MAX_RUN_TEXT_BYTES)
}

fn bounded_text_to(value: &str, limit: usize) -> BoundedText {
    let tail = utf8_tail(value, limit);
    BoundedText {
        value: tail.to_owned(),
        start_byte: u64::try_from(value.len().saturating_sub(tail.len())).unwrap_or(u64::MAX),
        total_bytes: u64::try_from(value.len()).unwrap_or(u64::MAX),
    }
}

fn notice_views(state: &DomainState, revision: u64) -> Vec<NoticeView> {
    (!state.status_message.is_empty())
        .then(|| NoticeView {
            id: format!("status:{revision}"),
            level: NoticeLevel::Info,
            message: state.status_message.clone(),
        })
        .into_iter()
        .collect()
}

/// Whether `model` survives its provider's discovery filter in the ordinary catalogue.
///
/// An enabled filter whose selection matches **no currently discovered model** — an empty
/// selection, or one whose every entry went stale — fails open rather than hiding the provider's
/// entire catalogue: that state comes from a never-configured toggle or a discovery refresh that
/// renamed everything, and silently projecting zero models breaks pickers and exact-ID
/// reconciliation for no possible intent. Selections that still match at least one discovered
/// model filter normally, so a deliberate hide-everything-but-x keeps working.
fn model_filter_passes(state: &DomainState, provider: &ProviderRecord, model: &ModelInfo) -> bool {
    if !provider.model_filter_enabled {
        return true;
    }
    if provider
        .selected_model_ids
        .iter()
        .any(|id| id == &model.qualified_id())
    {
        return true;
    }
    !state.models.iter().any(|candidate| {
        candidate.provider == model.provider
            && provider
                .selected_model_ids
                .iter()
                .any(|id| id == &candidate.qualified_id())
    })
}

fn model_view(state: &DomainState, model: &ModelInfo) -> ModelView {
    let qualified = model.qualified_id();
    let options = state.model_options_for_qualified(&qualified);
    ModelView {
        id: ModelId::from(qualified),
        provider_id: ProviderId::from(model.provider.clone()),
        model_slug: model.id.clone(),
        display_name: model.display_name(),
        is_default: model.is_default,
        reasoning_effort: options.reasoning_effort,
        fast_mode: options.fast_mode,
        configuration: model_configuration(model, state.vision_config.is_enabled()),
    }
}

fn supported_builtin_tools(provider: &str) -> Vec<String> {
    let canonical = crate::agent::CANONICAL_AGENT_TOOLS
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    let projection = crate::backend::project_provider_tools(provider, Some(&canonical));
    canonical
        .into_iter()
        .filter(|name| !projection.unsupported_canonical_tools.contains(name))
        .collect()
}

fn provider_account_view(
    state: &DomainState,
    account: &ProviderAccountRecord,
) -> nakode_protocol::ProviderAccountView {
    nakode_protocol::ProviderAccountView {
        account_id: account.account_id.clone(),
        label: account.label.clone(),
        enabled: account.enabled,
        is_default: account.is_default,
        identity: account.identity.clone(),
        credential_configured: account.credential.is_some(),
        credential_kind: account
            .credential
            .as_ref()
            .map(|credential| credential.kind.clone()),
        created_at_ms: u64::try_from(account.created_at)
            .unwrap_or_default()
            .saturating_mul(1_000),
        updated_at_ms: u64::try_from(account.updated_at)
            .unwrap_or_default()
            .saturating_mul(1_000),
        routing_mode: account.routing_mode,
        health: state
            .provider_account_health
            .get(&(account.provider.clone(), account.account_id.clone()))
            .cloned()
            .or_else(|| {
                Some(nakode_protocol::ProviderAccountHealthView {
                    state: if account.credential.is_some() {
                        nakode_protocol::ProviderAccountHealthState::Unknown
                    } else {
                        nakode_protocol::ProviderAccountHealthState::AuthenticationRequired
                    },
                    safe_reason: account
                        .credential
                        .is_none()
                        .then(|| "authentication is required".to_owned()),
                    cooldown_until_ms: None,
                })
            }),
        authentication: state
            .provider_account_authentication
            .get(&(account.provider.clone(), account.account_id.clone()))
            .map(authentication_view)
            .or_else(|| {
                (account.credential.is_none())
                    .then(|| crate::backend::api_key_provider_setup(&account.provider))
                    .flatten()
                    .map(|setup| ProviderAuthenticationView::ApiKeyRequired {
                        dashboard_url: setup.dashboard_url.to_owned(),
                        credential_kind: setup.credential_kind.to_owned(),
                    })
            }),
    }
}

fn provider_view(state: &DomainState, provider: &ProviderRecord) -> ProviderView {
    let connection = state.provider_connection(&provider.provider).map_or_else(
        || {
            if provider.enabled {
                ConnectionView::Starting
            } else {
                ConnectionView::Disabled
            }
        },
        connection_view,
    );
    let authentication = state
        .provider_authentication
        .get(&provider.provider)
        .map(authentication_view)
        .or_else(|| {
            (provider.credential.is_none())
                .then(|| crate::backend::api_key_provider_setup(&provider.provider))
                .flatten()
                .map(|setup| ProviderAuthenticationView::ApiKeyRequired {
                    dashboard_url: setup.dashboard_url.to_owned(),
                    credential_kind: setup.credential_kind.to_owned(),
                })
        });
    ProviderView {
        id: ProviderId::from(provider.provider.clone()),
        display_name: provider.display_name.clone(),
        enabled: provider.enabled,
        credential_configured: provider.credential.is_some(),
        credential_kind: provider
            .credential
            .as_ref()
            .map(|credential| credential.kind.clone()),
        connection,
        capabilities: state
            .provider_capabilities(&provider.provider)
            .map_or_else(ProviderCapabilities::default, capabilities_view),
        authentication,
        model_filter_enabled: provider.model_filter_enabled,
        selected_model_ids: provider
            .selected_model_ids
            .iter()
            .cloned()
            .map(ModelId::from)
            .collect(),
        model_candidates: state
            .models
            .iter()
            .filter(|model| model.provider == provider.provider)
            .map(|model| model_view(state, model))
            .collect(),
        supported_builtin_tools: Some(supported_builtin_tools(&provider.provider)),
        available_builtin_tools: state
            .available_builtin_tools(&provider.provider)
            .map(<[String]>::to_vec),
        accounts: provider
            .accounts
            .iter()
            .map(|account| provider_account_view(state, account))
            .collect(),
    }
}

fn vision_settings_view(state: &DomainState, providers: &[ProviderRecord]) -> VisionSettingsView {
    let Some(configured_id) = state.vision_config.model.as_deref() else {
        return VisionSettingsView {
            model_id: None,
            availability: VisionAvailabilityView::Disabled,
            diagnostic: "The callable vision add-on is disabled.".to_owned(),
        };
    };
    let model_id = Some(ModelId::from(configured_id));
    let Some(model) = state
        .models
        .iter()
        .find(|model| model.qualified_id() == configured_id)
    else {
        return VisionSettingsView {
            model_id,
            availability: VisionAvailabilityView::ModelUnavailable,
            diagnostic: "The selected vision model is no longer in the live provider catalogue. Choose an available vision model.".to_owned(),
        };
    };
    if !model_configuration(model, true).vision_eligible {
        return VisionSettingsView {
            model_id,
            availability: VisionAvailabilityView::ModelUnsupported,
            diagnostic: "The selected model does not support the callable vision service. Choose a vision-eligible model.".to_owned(),
        };
    }
    let provider = providers
        .iter()
        .find(|provider| provider.provider == model.provider);
    let provider_ready = provider.is_some_and(|provider| {
        provider.enabled
            && state
                .provider_connection(&provider.provider)
                .is_some_and(|connection| matches!(connection, ConnectionState::Ready { .. }))
    });
    if !provider_ready {
        return VisionSettingsView {
            model_id,
            availability: VisionAvailabilityView::ProviderUnavailable,
            diagnostic:
                "The selected model's provider is disconnected. Connect or reload that provider."
                    .to_owned(),
        };
    }
    if state
        .available_builtin_tools(&model.provider)
        .is_some_and(|tools| tools.iter().any(|tool| tool == "vision"))
    {
        VisionSettingsView {
            model_id,
            availability: VisionAvailabilityView::Ready,
            diagnostic: "The callable vision service is ready.".to_owned(),
        }
    } else {
        VisionSettingsView {
            model_id,
            availability: VisionAvailabilityView::ServiceUnavailable,
            diagnostic: "The selected model has no live provider-backed callable vision service. Reload its provider or choose another vision model.".to_owned(),
        }
    }
}

fn settings_view(state: &DomainState, providers: &[ProviderRecord]) -> SettingsView {
    SettingsView {
        web: WebSettingsView {
            backend: match state.web_config.backend {
                WebBackend::Disabled => "disabled",
                WebBackend::AgentBrowser => "agent-browser",
                WebBackend::Firecrawl => "firecrawl",
            }
            .to_owned(),
            credential_configured: !state.web_config.firecrawl_api_key.trim().is_empty(),
            agent_browser: match &state.agent_browser_status {
                AgentBrowserStatus::Checking => AgentBrowserView::Checking,
                AgentBrowserStatus::Available(version) => AgentBrowserView::Available {
                    version: version.clone(),
                },
                AgentBrowserStatus::Unavailable => AgentBrowserView::Unavailable,
            },
        },
        memory: MemorySettingsView {
            backend: match state.memory_config.backend {
                MemoryBackend::Disabled => "disabled",
                MemoryBackend::Mnemosyne => "mnemosyne",
            }
            .to_owned(),
            executable: state.memory_config.executable.clone(),
            global_bank: state.memory_config.global_bank.clone(),
            data_directory: state.memory_config.data_directory.clone(),
            configured: state.memory_config.configured(),
            available: state.memory_config.available(),
        },
        vision: vision_settings_view(state, providers),
        terminal_images: match state.terminal_image_mode {
            TerminalImageMode::Auto => TerminalImageModeView::Auto,
            TerminalImageMode::On => TerminalImageModeView::On,
            TerminalImageMode::Off => TerminalImageModeView::Off,
        },
        invocation_telemetry_enabled: state.invocation_telemetry_enabled(),
    }
}

fn interactions(state: &DomainState, revision: u64) -> Vec<InteractionView> {
    let approvals = state.approvals.iter().map(|approval| InteractionView {
        id: approval_interaction_id(&state.nakode_session_id, &approval.id),
        revision,
        kind: InteractionKind::Approval,
        status: InteractionStatus::Pending,
        title: approval.title.clone(),
        detail: approval.detail.clone(),
        options: vec![
            interaction_option("approve_once", "Approve once"),
            interaction_option("approve_session", "Approve for session"),
            interaction_option("decline", "Decline"),
        ],
        multiple: false,
        questions: Vec::new(),
    });
    let mut groups: Vec<(&str, Vec<&QuestionPrompt>)> = Vec::new();
    for question in &state.questions {
        if let Some((_, questions)) = groups
            .iter_mut()
            .find(|(group_id, _)| *group_id == question.request.group_id)
        {
            questions.push(question);
        } else {
            groups.push((&question.request.group_id, vec![question]));
        }
    }
    let questions = groups.into_iter().map(|(group_id, mut prompts)| {
        prompts.sort_by_key(|prompt| prompt.request.order);
        let items = prompts
            .iter()
            .map(|question| nakode_protocol::InteractionQuestionView {
                id: question.request.logical_id.clone(),
                title: question.request.title.clone(),
                detail: question.request.question.clone(),
                options: question_options(question),
                multiple: question.request.multi,
            })
            .collect::<Vec<_>>();
        let first = prompts[0];
        InteractionView {
            id: question_interaction_id(&state.nakode_session_id, group_id),
            revision,
            kind: InteractionKind::Question,
            status: InteractionStatus::Pending,
            // Preserve the old scalar shape so old clients can still answer a one-item ask losslessly.
            title: first.request.title.clone(),
            detail: first.request.question.clone(),
            options: question_options(first),
            multiple: first.request.multi,
            questions: items,
        }
    });
    approvals.chain(questions).collect()
}

fn question_options(question: &QuestionPrompt) -> Vec<InteractionOptionView> {
    question
        .request
        .options
        .iter()
        .enumerate()
        .map(|(index, option)| InteractionOptionView {
            id: index.to_string(),
            label: option.label.clone(),
            description: option.description.clone(),
            recommended: question.request.recommended == Some(index),
        })
        .collect()
}

pub(super) fn approval_interaction_id(
    session_id: &str,
    provider_id: &serde_json::Value,
) -> InteractionId {
    InteractionId::from(scoped_id(
        "interaction",
        &format!(
            "{session_id}:approval:{}",
            serde_json::to_string(provider_id).unwrap_or_default()
        ),
    ))
}

pub(super) fn question_interaction_id(session_id: &str, provider_id: &str) -> InteractionId {
    InteractionId::from(scoped_id(
        "interaction",
        &format!("{session_id}:question:{provider_id}"),
    ))
}

fn interaction_option(id: &str, label: &str) -> InteractionOptionView {
    InteractionOptionView {
        id: id.to_owned(),
        label: label.to_owned(),
        description: None,
        recommended: false,
    }
}

fn transcript_page(transcript: &DomainTranscript) -> TranscriptPage {
    projected_transcript_page(
        transcript,
        None,
        MAX_TRANSCRIPT_SNAPSHOT_ENTRIES,
        MAX_TRANSCRIPT_SNAPSHOT_BODY_BYTES,
    )
    .expect("an unbounded recent transcript page always exists")
}

pub(crate) fn session_transcript_page(
    state: &DomainState,
    before: Option<&EntryId>,
    limit: usize,
) -> Option<TranscriptPage> {
    projected_transcript_page(
        &state.transcript,
        before,
        limit.min(MAX_TRANSCRIPT_PAGE_ENTRIES),
        MAX_TRANSCRIPT_PAGE_BODY_BYTES,
    )
}

pub(crate) fn run_transcript_page(
    state: &DomainState,
    run_id: &RunId,
    before: Option<&EntryId>,
    limit: usize,
) -> Option<TranscriptPage> {
    let transcript = &state.subagent_chats.get(run_id.as_str())?.transcript;
    projected_transcript_page(
        transcript,
        before,
        limit.min(MAX_TRANSCRIPT_PAGE_ENTRIES),
        MAX_TRANSCRIPT_PAGE_BODY_BYTES,
    )
}

pub(crate) fn transcript_entry_body<'a>(
    state: &'a DomainState,
    run_id: Option<&RunId>,
    entry_id: &EntryId,
) -> Option<&'a str> {
    let transcript = match run_id {
        Some(run_id) => &state.subagent_chats.get(run_id.as_str())?.transcript,
        None => &state.transcript,
    };
    transcript
        .entries()
        .iter()
        .find(|entry| entry.id == entry_id.as_str())
        .map(|entry| entry.body.as_str())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TranscriptBodyWindowError {
    EntryNotFound,
    LimitOutOfBounds { actual: usize, maximum: usize },
    CursorOutOfBounds { actual: usize, total: usize },
    CursorNotUtf8Boundary { actual: usize },
    LimitTooSmallForCharacter { minimum: usize },
}

pub(crate) fn transcript_body_window(
    state: &DomainState,
    run_id: Option<&RunId>,
    entry_id: &EntryId,
    before_byte: Option<u64>,
    limit_bytes: usize,
) -> Result<TranscriptBodyWindow, TranscriptBodyWindowError> {
    if limit_bytes == 0 || limit_bytes > MAX_TRANSCRIPT_ENTRY_BODY_BYTES {
        return Err(TranscriptBodyWindowError::LimitOutOfBounds {
            actual: limit_bytes,
            maximum: MAX_TRANSCRIPT_ENTRY_BODY_BYTES,
        });
    }
    let body = transcript_entry_body(state, run_id, entry_id)
        .ok_or(TranscriptBodyWindowError::EntryNotFound)?;
    let end = before_byte.map_or(Ok(body.len()), |before| {
        usize::try_from(before).map_err(|_| TranscriptBodyWindowError::CursorOutOfBounds {
            actual: usize::MAX,
            total: body.len(),
        })
    })?;
    if end > body.len() {
        return Err(TranscriptBodyWindowError::CursorOutOfBounds {
            actual: end,
            total: body.len(),
        });
    }
    if !body.is_char_boundary(end) {
        return Err(TranscriptBodyWindowError::CursorNotUtf8Boundary { actual: end });
    }

    let mut start = end.saturating_sub(limit_bytes);
    while start < end && !body.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    if start == end && end > 0 {
        let minimum = body[..end].chars().next_back().map_or(1, char::len_utf8);
        return Err(TranscriptBodyWindowError::LimitTooSmallForCharacter { minimum });
    }
    Ok(TranscriptBodyWindow {
        entry_id: entry_id.clone(),
        body: body[start..end].to_owned(),
        start_byte: u64::try_from(start).unwrap_or(u64::MAX),
        total_bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
        has_earlier: start > 0,
    })
}

fn projected_transcript_page(
    transcript: &DomainTranscript,
    before: Option<&EntryId>,
    limit: usize,
    body_budget: usize,
) -> Option<TranscriptPage> {
    let entries = transcript
        .entries()
        .iter()
        .filter(|entry| {
            !entry
                .key
                .as_deref()
                .is_some_and(|key| key.starts_with("subagent:"))
        })
        .collect::<Vec<_>>();
    let end = before.map_or(entries.len(), |before| {
        entries
            .iter()
            .position(|entry| entry.id == before.as_str())
            .unwrap_or(usize::MAX)
    });
    if end == usize::MAX {
        return None;
    }

    let current_owner = if before.is_none() {
        entries
            .iter()
            .rev()
            .find(|entry| entry.kind == EntryKind::User)
            .copied()
    } else {
        None
    };
    let row_window_start = end.saturating_sub(limit);
    let current_owner_outside_row_window = current_owner.is_some_and(|owner| {
        entries[..end]
            .iter()
            .position(|entry| entry.id == owner.id)
            .is_some_and(|index| index < row_window_start)
    });
    let reserved_owner_body = current_owner
        .filter(|_| current_owner_outside_row_window)
        .map(|entry| {
            utf8_tail(
                &entry.body,
                body_budget.min(MAX_TRANSCRIPT_ENTRY_BODY_BYTES),
            )
        });
    let mut remaining_body_bytes =
        body_budget.saturating_sub(reserved_owner_body.map_or(0, str::len));
    let mut projected = Vec::with_capacity(limit.min(entries.len()));
    for entry in entries[..end].iter().rev().take(limit) {
        // Tool audit envelopes share the same IPC/memory budget as transcript bodies. Providers bound
        // each payload field before it reaches here; the page keeps an envelope whole so a client is
        // never handed valid-looking partial JSON.
        let audit_bytes = entry.tool_audit_json.as_ref().map_or(0, String::len);
        let include_audit =
            delegated_invocation_entry(entry) || audit_bytes <= remaining_body_bytes;
        if include_audit {
            remaining_body_bytes = remaining_body_bytes.saturating_sub(audit_bytes);
        }
        let body_limit = remaining_body_bytes.min(MAX_TRANSCRIPT_ENTRY_BODY_BYTES);
        let body = utf8_tail(&entry.body, body_limit);
        remaining_body_bytes = remaining_body_bytes.saturating_sub(body.len());
        projected.push(transcript_entry_view(
            transcript,
            entry,
            body,
            include_audit,
        ));
    }
    projected.reverse();
    let omitted_entries = projected.len() < end;
    let projected_ids = projected
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let current_owner_entry =
        projected_current_owner(transcript, current_owner, &projected, reserved_owner_body);
    let current_owner_omitted_tool_calls = current_owner
        .and_then(|entry| entry.owner_turn_id.as_deref())
        .map_or(0, |turn_id| {
            entries
                .iter()
                .filter(|entry| {
                    matches!(entry.kind, EntryKind::Tool | EntryKind::Diff)
                        && entry.owner_turn_id.as_deref() == Some(turn_id)
                        && !projected_ids.contains(entry.id.as_str())
                })
                .count()
        });
    Some(TranscriptPage {
        entries: projected,
        has_earlier: transcript.has_earlier_entries() || omitted_entries,
        stream_active: before.is_none() && transcript.stream_active(),
        stream_label: transcript.stream_label().to_owned(),
        current_owner_entry,
        current_owner_omitted_tool_calls: u64::try_from(current_owner_omitted_tool_calls)
            .unwrap_or(u64::MAX),
    })
}

fn projected_current_owner(
    transcript: &DomainTranscript,
    current_owner: Option<&TranscriptEntry>,
    projected: &[TranscriptEntryView],
    reserved_owner_body: Option<&str>,
) -> Option<TranscriptEntryView> {
    current_owner.map(|entry| {
        projected
            .iter()
            .find(|projected| projected.id.as_str() == entry.id)
            .cloned()
            .map_or_else(
                || {
                    transcript_entry_view(
                        transcript,
                        entry,
                        reserved_owner_body.unwrap_or(""),
                        false,
                    )
                },
                |mut projected| {
                    projected.body.clear();
                    projected.body_start_byte = projected.body_total_bytes;
                    projected
                },
            )
    })
}

fn delegated_invocation_entry(entry: &TranscriptEntry) -> bool {
    entry.kind == EntryKind::Tool
        && (["nakode_agent", "mcp__nakode__delegate"]
            .iter()
            .any(|name| entry.title.starts_with(name))
            || entry.tool_audit_json.as_deref().is_some_and(|audit| {
                ["nakode_agent", "mcp__nakode__delegate"]
                    .iter()
                    .any(|name| audit.contains(&format!("\"name\":\"{name}\"")))
            }))
}

fn tool_audit_identity(entry: &TranscriptEntry, field: &str) -> Option<String> {
    entry
        .tool_audit_json
        .as_deref()
        .and_then(|audit| serde_json::from_str::<serde_json::Value>(audit).ok())
        .and_then(|audit| {
            audit
                .get(field)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
}

fn parent_tool_entry_id(transcript: &DomainTranscript, entry: &TranscriptEntry) -> Option<EntryId> {
    let parent_item_id = tool_audit_identity(entry, "parentItemId")?;
    transcript
        .entries()
        .iter()
        .find(|candidate| {
            candidate.kind == EntryKind::Tool
                && candidate.id == parent_item_id
                && candidate.tool_audit_json.as_deref().is_some_and(|audit| {
                    serde_json::from_str::<serde_json::Value>(audit).is_ok_and(|audit| {
                        audit.get("name").and_then(serde_json::Value::as_str) == Some("codemode")
                    })
                })
        })
        .map(|parent| EntryId::from(parent.id.clone()))
}

fn transcript_entry_view(
    transcript: &DomainTranscript,
    entry: &TranscriptEntry,
    body: &str,
    include_audit: bool,
) -> TranscriptEntryView {
    TranscriptEntryView {
        id: EntryId::from(entry.id.clone()),
        kind: entry_kind(entry.kind),
        title: entry.title.clone(),
        body: body.to_owned(),
        body_start_byte: u64::try_from(entry.body.len().saturating_sub(body.len()))
            .unwrap_or(u64::MAX),
        body_total_bytes: u64::try_from(entry.body.len()).unwrap_or(u64::MAX),
        status: entry_status(entry.status),
        artifacts: transcript
            .image_artifacts(entry)
            .enumerate()
            .map(|(index, _)| transcript_artifact_id(&entry.id, index))
            .collect(),
        created_at_ms: entry.created_at_ms,
        provider_id: entry.provider_id.clone(),
        model_id: entry.model_id.clone().map(ModelId::from),
        owner_turn_id: entry.owner_turn_id.clone().map(TurnId::from),
        resolved_reasoning_effort: entry.reasoning_effort.clone(),
        resolved_fast_mode: entry.fast_mode,
        source_transport: entry.source_transport.clone(),
        source_prompt_id: (entry.kind == EntryKind::User)
            .then(|| {
                entry
                    .key
                    .as_deref()
                    .and_then(|key| key.strip_prefix("user:"))
                    .map(str::to_owned)
            })
            .flatten(),
        tool_audit_json: include_audit
            .then(|| entry.tool_audit_json.clone())
            .flatten(),
        parent_tool_entry_id: parent_tool_entry_id(transcript, entry),
    }
}

fn utf8_tail(value: &str, maximum_bytes: usize) -> &str {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut start = value.len().saturating_sub(maximum_bytes);
    while start < value.len() && !value.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    &value[start..]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactTooLarge {
    pub(crate) actual: usize,
    pub(crate) maximum: usize,
}

pub(crate) fn artifact_view(
    state: &DomainState,
    artifact_id: &ArtifactId,
) -> Result<Option<ArtifactView>, ArtifactTooLarge> {
    let transcripts = std::iter::once(&state.transcript)
        .chain(state.subagent_chats.values().map(|chat| &chat.transcript));
    for transcript in transcripts {
        if let Some(artifact) = transcript_artifact_view(transcript, artifact_id)? {
            return Ok(Some(artifact));
        }
    }
    Ok(None)
}

fn transcript_artifact_view(
    transcript: &DomainTranscript,
    artifact_id: &ArtifactId,
) -> Result<Option<ArtifactView>, ArtifactTooLarge> {
    for entry in transcript.entries() {
        for (index, (label, image)) in transcript.image_artifacts(entry).enumerate() {
            let id = transcript_artifact_id(&entry.id, index);
            if id != *artifact_id {
                continue;
            }
            if image.data.len() > MAX_ARTIFACT_BYTES {
                return Err(ArtifactTooLarge {
                    actual: image.data.len(),
                    maximum: MAX_ARTIFACT_BYTES,
                });
            }
            return Ok(Some(ArtifactView {
                id,
                label: label.to_owned(),
                media_type: image.mime_type.clone(),
                byte_length: u64::try_from(image.data.len()).unwrap_or(u64::MAX),
                data: image.data.clone(),
            }));
        }
    }
    Ok(None)
}

fn transcript_artifact_id(entry_id: &str, index: usize) -> ArtifactId {
    ArtifactId::from(scoped_id("artifact", &format!("{entry_id}:{index}")))
}

fn empty_transcript_page() -> TranscriptPage {
    TranscriptPage {
        entries: Vec::new(),
        has_earlier: false,
        stream_active: false,
        stream_label: String::new(),
        current_owner_entry: None,
        current_owner_omitted_tool_calls: 0,
    }
}

fn unix_seconds_to_milliseconds(seconds: i64) -> i64 {
    seconds.saturating_mul(1_000)
}

pub(crate) fn active_session_summary(
    state: &DomainState,
    sessions: &[SessionRecord],
) -> Option<SessionSummary> {
    if !state.subagents.is_empty() {
        // Workspace summaries apply both a run-count and encoded-byte budget. Rebuilding that page
        // would defeat this lightweight detector, so subagent owners conservatively take the full
        // workspace projection path.
        return None;
    }
    let workspace_id = workspace_id(&state.workspace);
    let persisted = sessions
        .iter()
        .find(|session| session.id == state.nakode_session_id);
    Some(SessionSummary {
        id: SessionId::from(state.nakode_session_id.clone()),
        workspace_id,
        working_directory: state.working_directory.clone(),
        title: session_title(state, sessions),
        active_provider_id: provider_id(&state.backend_provider),
        active_model_id: state.selected_model.clone().map(ModelId::from),
        updated_at_ms: persisted.map_or(0, |session| {
            unix_seconds_to_milliseconds(session.updated_at)
        }),
        created_at_ms: persisted.map_or(0, |session| {
            unix_seconds_to_milliseconds(session.created_at)
        }),
        last_owner_activity_at_ms: persisted
            .and_then(|session| session.last_owner_activity_at)
            .map_or(0, unix_seconds_to_milliseconds),
        owned_provider_sessions: owned_provider_sessions(state),
        running: activity(state) != SessionActivity::Idle,
        selected_account_id: state.provider_account_id.clone(),
        routing_diagnostic: state.provider_account_routing.clone(),
    })
}

fn session_summary(session: &SessionRecord, workspace_id: &WorkspaceId) -> SessionSummary {
    SessionSummary {
        id: SessionId::from(session.id.clone()),
        workspace_id: workspace_id.clone(),
        working_directory: session.working_directory.clone(),
        title: session.title.clone(),
        active_provider_id: provider_id(&session.provider),
        active_model_id: session.model.clone().map(ModelId::from),
        updated_at_ms: unix_seconds_to_milliseconds(session.updated_at),
        created_at_ms: unix_seconds_to_milliseconds(session.created_at),
        last_owner_activity_at_ms: session
            .last_owner_activity_at
            .map_or(0, unix_seconds_to_milliseconds),
        owned_provider_sessions: owned_provider_session(
            &session.provider,
            Some(&session.provider_session_id),
        )
        .into_iter()
        .chain(session.owned_provider_sessions.iter().filter_map(
            |(provider, native_session_id)| {
                owned_provider_session(provider, Some(native_session_id))
            },
        ))
        .collect(),
        running: false,
        selected_account_id: session.account_id.clone(),
        routing_diagnostic: session.account_id.as_ref().map(|account_id| {
            nakode_protocol::ProviderAccountRoutingDiagnosticView {
                account_id: Some(account_id.clone()),
                account_label: None,
                reason: "persisted session affinity".to_owned(),
                cooldown_until_ms: None,
            }
        }),
    }
}

fn owned_provider_sessions(state: &DomainState) -> Vec<nakode_protocol::OwnedProviderSessionView> {
    owned_provider_session(
        &state.backend_provider,
        state.provider_session_id.as_deref(),
    )
    .into_iter()
    .chain(state.subagents.iter().filter_map(|run| {
        owned_provider_session(&run.provider, run.provider_session_id.as_deref())
    }))
    .collect()
}

fn owned_provider_session(
    provider: &str,
    native_session_id: Option<&str>,
) -> Option<nakode_protocol::OwnedProviderSessionView> {
    let provider_id = provider_id(provider)?;
    let native_session_id = native_session_id?.trim();
    if native_session_id.is_empty() {
        return None;
    }
    Some(nakode_protocol::OwnedProviderSessionView {
        provider_id,
        native_session_id: native_session_id.to_owned(),
    })
}

fn token_usage_view(usage: crate::backend::BackendTokenUsage) -> nakode_protocol::TokenUsageView {
    nakode_protocol::TokenUsageView {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        cache_write_tokens: usage.cache_write_tokens,
    }
}

fn capabilities_view(capabilities: &BackendCapabilities) -> ProviderCapabilities {
    let supported = [
        (ProviderCapability::Resume, capabilities.resume),
        (ProviderCapability::Steering, capabilities.steering),
        (ProviderCapability::Interruption, capabilities.interruption),
        (ProviderCapability::ModelCatalog, capabilities.model_catalog),
        (
            ProviderCapability::ModelsRequireSession,
            capabilities.models_require_session,
        ),
        (
            ProviderCapability::SessionModelConfiguration,
            capabilities.session_model_config,
        ),
        (
            ProviderCapability::ContextCompaction,
            capabilities.context_compaction,
        ),
        (ProviderCapability::Approvals, capabilities.approvals),
        (ProviderCapability::NativeTools, capabilities.native_tools),
        (ProviderCapability::Mcp, capabilities.mcp),
        (ProviderCapability::CloseSession, capabilities.close_session),
        (
            ProviderCapability::ExternalTools,
            capabilities.external_tools,
        ),
    ]
    .into_iter()
    .filter_map(|(capability, support)| {
        (support == CapabilitySupport::Supported).then_some(capability)
    })
    .collect::<BTreeSet<_>>();
    ProviderCapabilities { supported }
}

fn connection_view(connection: &ConnectionState) -> ConnectionView {
    match connection {
        ConnectionState::Starting => ConnectionView::Starting,
        ConnectionState::Ready { .. } => ConnectionView::Ready,
        ConnectionState::Failed(message) => ConnectionView::Failed {
            message: message.clone(),
        },
        ConnectionState::Disconnected(message) => ConnectionView::Disconnected {
            message: message.clone(),
        },
    }
}

fn authentication_view(authentication: &ProviderAuthenticationState) -> ProviderAuthenticationView {
    match authentication {
        ProviderAuthenticationState::Starting => ProviderAuthenticationView::Starting,
        #[cfg(test)]
        ProviderAuthenticationState::ApiKeyRequired {
            dashboard_url,
            credential_kind,
        } => ProviderAuthenticationView::ApiKeyRequired {
            dashboard_url: dashboard_url.clone(),
            credential_kind: credential_kind.clone(),
        },
        ProviderAuthenticationState::Challenge {
            verification_url,
            user_code,
        } => ProviderAuthenticationView::Challenge {
            verification_url: verification_url.clone(),
            user_code: user_code.clone(),
        },
    }
}

fn activity(state: &DomainState) -> SessionActivity {
    if state.context_compaction.is_some() {
        SessionActivity::CompactingContext
    } else if state.active_turn.is_some() {
        SessionActivity::RunningTurn
    } else if state.starting_turn.is_some() {
        SessionActivity::StartingTurn
    } else if state.creating_session.is_some() {
        SessionActivity::CreatingAgentSession
    } else if state.has_running_subagents() {
        SessionActivity::RunningDelegates
    } else if !state.active_shell_ids().is_empty() {
        SessionActivity::RunningShell
    } else {
        SessionActivity::Idle
    }
}

fn entry_kind(kind: EntryKind) -> TranscriptEntryKind {
    match kind {
        EntryKind::System => TranscriptEntryKind::System,
        EntryKind::User => TranscriptEntryKind::User,
        EntryKind::Assistant => TranscriptEntryKind::Assistant,
        EntryKind::Steering => TranscriptEntryKind::Steering,
        EntryKind::Reasoning => TranscriptEntryKind::Reasoning,
        EntryKind::Tool => TranscriptEntryKind::Tool,
        EntryKind::Diff => TranscriptEntryKind::Diff,
        EntryKind::Warning => TranscriptEntryKind::Warning,
        EntryKind::Error => TranscriptEntryKind::Error,
    }
}

fn entry_status(status: EntryStatus) -> TranscriptEntryStatus {
    match status {
        EntryStatus::Running => TranscriptEntryStatus::Running,
        EntryStatus::Complete => TranscriptEntryStatus::Complete,
        EntryStatus::Failed => TranscriptEntryStatus::Failed,
        EntryStatus::Interrupted => TranscriptEntryStatus::Interrupted,
    }
}

const fn todo_status(status: TodoStatus) -> TodoStatusView {
    match status {
        TodoStatus::Pending => TodoStatusView::Pending,
        TodoStatus::InProgress => TodoStatusView::InProgress,
        TodoStatus::Completed => TodoStatusView::Completed,
        TodoStatus::Abandoned => TodoStatusView::Abandoned,
    }
}

const fn run_status(status: SubagentStatus) -> RunStatus {
    match status {
        SubagentStatus::Starting => RunStatus::Starting,
        SubagentStatus::Working => RunStatus::Working,
        SubagentStatus::Completed => RunStatus::Completed,
        SubagentStatus::Partial => RunStatus::Partial,
        SubagentStatus::Interrupted => RunStatus::Interrupted,
        SubagentStatus::Failed => RunStatus::Failed,
    }
}

fn provider_id(provider: &str) -> Option<ProviderId> {
    (!provider.is_empty()).then(|| ProviderId::from(provider.to_owned()))
}

fn first_line(value: &str) -> String {
    let line = value.lines().next().unwrap_or_default().trim();
    if line.is_empty() {
        "New session".to_owned()
    } else {
        line.to_owned()
    }
}

fn scoped_id(kind: &str, value: &str) -> String {
    uuid::Uuid::new_v5(&ID_NAMESPACE, format!("{kind}:{value}").as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use nakode_protocol::{
        EntryId, MAX_API_MESSAGE_BYTES, MAX_ARTIFACT_BYTES, MAX_RUN_POLICY_ITEMS,
        MAX_RUN_POLICY_TEXT_BYTES, MAX_RUN_TEXT_BYTES, MAX_RUN_TOOL_DENIAL_TEXT_BYTES,
        MAX_RUN_TOOL_DENIALS, MAX_SESSION_RUNS, MAX_TRANSCRIPT_ENTRY_BODY_BYTES,
        MAX_TRANSCRIPT_SNAPSHOT_BODY_BYTES, MAX_TRANSCRIPT_SNAPSHOT_ENTRIES, RunId, RunOutcome,
        RunStatus, TranscriptEntryKind, TranscriptEntryStatus, TranscriptEntryView, TranscriptPage,
        VisionAvailabilityView,
    };

    use super::{
        artifact_view, bootstrap, capabilities_view, model_configuration, run_outcome,
        transcript_page, vision_settings_view,
    };
    use crate::{
        backend::{
            BackendCapabilities, BackendEvent, BackendIdentity, CLAUDE_PROVIDER, CODEX_PROVIDER,
            CURSOR_PROVIDER, CapabilitySupport, ModelInfo, PromptImage,
        },
        domain_transcript::{DomainTranscript, EntryKind, EntryStatus, TranscriptEntry},
        session::{ProviderRecord, SubagentObservability, SubagentRecord},
        state::{AppState, ReasoningSummaryTracker, SubagentChat, SubagentRun, SubagentStatus},
    };

    #[test]
    fn nested_runtime_audit_projects_the_code_mode_parent_entry() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        let entry = |id: &str,
                     call_id: &str,
                     name: &str,
                     parent_call_id: Option<&str>,
                     parent_item_id: Option<&str>,
                     turn: &str| {
            TranscriptEntry {
                id: id.to_owned(),
                key: Some(format!("tool:{call_id}")),
                kind: EntryKind::Tool,
                title: name.to_owned(),
                body: String::new(),
                status: EntryStatus::Complete,
                created_at_ms: None,
                provider_id: None,
                model_id: None,
                owner_turn_id: Some(turn.to_owned()),
                reasoning_effort: None,
                fast_mode: None,
                source_transport: None,
                tool_audit_json: Some(
                    serde_json::json!({
                        "callId": call_id,
                        "parentCallId": parent_call_id,
                        "parentItemId": parent_item_id,
                        "name": name,
                    })
                    .to_string(),
                ),
            }
        };
        state.transcript.restore(entry(
            "earlier-parent",
            "outer",
            "codemode",
            None,
            None,
            "turn-1",
        ));
        state.transcript.restore(entry(
            "parent-entry",
            "outer",
            "codemode",
            None,
            None,
            "turn-2",
        ));
        state.transcript.restore(entry(
            "child-entry",
            "outer/1",
            "read",
            Some("outer"),
            Some("parent-entry"),
            "turn-2",
        ));
        state.transcript.restore(entry(
            "direct-entry",
            "direct",
            "bash",
            None,
            None,
            "turn-2",
        ));

        let page = transcript_page(&state.transcript);
        let child = page
            .entries
            .iter()
            .find(|entry| entry.id.as_str() == "child-entry")
            .unwrap();
        let direct = page
            .entries
            .iter()
            .find(|entry| entry.id.as_str() == "direct-entry")
            .unwrap();
        assert_eq!(
            child.parent_tool_entry_id.as_ref().map(EntryId::as_str),
            Some("parent-entry")
        );
        assert_eq!(direct.parent_tool_entry_id, None);
    }

    #[test]
    fn provider_configuration_reports_external_tool_support() {
        let capabilities = BackendCapabilities {
            external_tools: CapabilitySupport::Supported,
            ..BackendCapabilities::default()
        };

        assert!(
            capabilities_view(&capabilities)
                .supports(nakode_protocol::ProviderCapability::ExternalTools)
        );
        assert!(
            !capabilities_view(&BackendCapabilities::default())
                .supports(nakode_protocol::ProviderCapability::ExternalTools)
        );
    }

    #[test]
    fn vision_readiness_reconciles_luna_with_model_and_live_service_authority() {
        let provider = ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
            accounts: Vec::new(),
        };
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        state.install_vision_config(crate::vision::VisionConfig {
            model: Some("openai-codex/gpt-5.6-luna".to_owned()),
        });

        assert_eq!(
            vision_settings_view(&state, std::slice::from_ref(&provider)).availability,
            VisionAvailabilityView::ModelUnavailable
        );

        state.install_vision_config(crate::vision::VisionConfig {
            model: Some(format!("{CURSOR_PROVIDER}/composer-2")),
        });
        state.install_cached_models(vec![model(CURSOR_PROVIDER, "composer-2")]);
        let cursor_provider = ProviderRecord {
            provider: CURSOR_PROVIDER.to_owned(),
            display_name: "Cursor".to_owned(),
            enabled: true,
            credential: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
            accounts: Vec::new(),
        };
        assert_eq!(
            vision_settings_view(&state, &[cursor_provider]).availability,
            VisionAvailabilityView::ModelUnsupported
        );

        state.install_vision_config(crate::vision::VisionConfig {
            model: Some("openai-codex/gpt-5.6-luna".to_owned()),
        });
        state.install_cached_models(vec![model(CODEX_PROVIDER, "gpt-5.6-luna")]);
        assert_eq!(
            vision_settings_view(&state, std::slice::from_ref(&provider)).availability,
            VisionAvailabilityView::ProviderUnavailable
        );

        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: Some("live".to_owned()),
                capabilities: BackendCapabilities::default(),
            }),
        );
        assert_eq!(
            vision_settings_view(&state, std::slice::from_ref(&provider)).availability,
            VisionAvailabilityView::ServiceUnavailable
        );

        state.install_available_builtin_tools(std::collections::HashMap::from([(
            CODEX_PROVIDER.to_owned(),
            vec!["vision".to_owned()],
        )]));
        let ready = vision_settings_view(&state, &[provider]);
        assert_eq!(ready.availability, VisionAvailabilityView::Ready);
        assert!(ready.diagnostic.contains("ready"));
    }

    #[test]
    fn model_configuration_is_derived_before_reaching_frontends() {
        let openai = model_configuration(&model(CODEX_PROVIDER, "gpt-5.6"), false);
        assert_eq!(
            openai
                .reasoning_efforts
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["none", "low", "medium", "high", "xhigh", "max"]
        );
        assert!(openai.fast_mode_configurable);
        assert!(openai.vision_eligible);
        assert!(openai.accepts_image_input);

        let mut claude = model(CLAUDE_PROVIDER, "opus");
        claude.capabilities.reasoning_efforts = ["low", "medium", "high"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            model_configuration(&claude, false).reasoning_efforts,
            ["low", "medium", "high"]
        );
        assert!(model_configuration(&claude, false).accepts_image_input);

        let cursor = model_configuration(&model(CURSOR_PROVIDER, "composer-2"), false);
        assert!(cursor.reasoning_efforts.is_empty());
        assert!(cursor.fast_mode_configurable);
        assert!(!cursor.vision_eligible);

        let cursor_basic = model_configuration(&model(CURSOR_PROVIDER, "basic"), false);
        assert!(cursor_basic.accepts_image_input);
        assert!(!cursor_basic.fast_mode_configurable);
        assert!(!cursor_basic.vision_eligible);

        let devin_without_add_on = model_configuration(&model("devin-acp", "swe"), false);
        assert!(!devin_without_add_on.accepts_image_input);
        assert_eq!(
            devin_without_add_on,
            nakode_protocol::ModelConfigurationView::default()
        );
        assert!(model_configuration(&model("devin-acp", "swe"), true).accepts_image_input);
        assert!(!model_configuration(&model("devin-acp", "swe"), true).vision_eligible);

        assert_eq!(
            model_configuration(&model("other-provider", "model"), false),
            nakode_protocol::ModelConfigurationView::default()
        );
    }

    #[test]
    fn provider_filters_only_the_ordinary_catalogue_and_preserves_candidates_and_stale_ids() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.models = vec![
            model(CODEX_PROVIDER, "visible"),
            model(CODEX_PROVIDER, "hidden"),
            model(CLAUDE_PROVIDER, "all-models"),
        ];
        let providers = vec![
            ProviderRecord {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                enabled: true,
                credential: None,
                model_filter_enabled: true,
                selected_model_ids: vec![
                    format!("{CODEX_PROVIDER}/visible"),
                    format!("{CODEX_PROVIDER}/stale"),
                ],
                accounts: Vec::new(),
            },
            ProviderRecord {
                provider: CLAUDE_PROVIDER.to_owned(),
                display_name: "Claude".to_owned(),
                enabled: true,
                credential: None,
                model_filter_enabled: false,
                selected_model_ids: Vec::new(),
                accounts: Vec::new(),
            },
        ];

        let view = bootstrap(&state, 1, &providers, &[]);
        assert_eq!(
            view.models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            ["openai-codex/visible", "claude-agent/all-models"]
        );
        let codex = view
            .providers
            .iter()
            .find(|provider| provider.id.as_str() == CODEX_PROVIDER)
            .expect("Codex provider");
        assert_eq!(codex.model_candidates.len(), 2);
        assert!(
            codex
                .selected_model_ids
                .iter()
                .any(|id| id.as_str() == "openai-codex/stale")
        );
    }

    #[test]
    fn provider_filter_with_no_live_selection_fails_open_instead_of_hiding_the_provider() {
        // Regression: an enabled filter whose selection is empty (or whose every entry went stale)
        // must not project an empty catalogue — that state is a never-configured toggle or a
        // discovery rename, not intent, and it breaks pickers and exact-ID reconciliation.
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.models = vec![
            model(CODEX_PROVIDER, "visible"),
            model(CODEX_PROVIDER, "also-visible"),
        ];
        for selected_model_ids in [Vec::new(), vec![format!("{CODEX_PROVIDER}/renamed-away")]] {
            let providers = vec![ProviderRecord {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                enabled: true,
                credential: None,
                model_filter_enabled: true,
                selected_model_ids,
                accounts: Vec::new(),
            }];
            let view = bootstrap(&state, 1, &providers, &[]);
            assert_eq!(
                view.models
                    .iter()
                    .map(|model| model.id.as_str())
                    .collect::<Vec<_>>(),
                ["openai-codex/visible", "openai-codex/also-visible"]
            );
            let codex = view
                .providers
                .iter()
                .find(|provider| provider.id.as_str() == CODEX_PROVIDER)
                .expect("Codex provider");
            assert!(codex.model_filter_enabled);
            assert_eq!(codex.model_candidates.len(), 2);
        }
    }

    #[test]
    fn bootstrap_contains_stable_semantic_ids_and_no_secret_settings() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.web_config.firecrawl_api_key = "must-not-leak".to_owned();
        state
            .transcript
            .push(EntryKind::User, "YOU", "hello", EntryStatus::Complete);

        let first = bootstrap(&state, 7, &[], &[]);
        let second = bootstrap(&state, 7, &[], &[]);
        assert_eq!(first.workspace_id, second.workspace_id);
        assert_eq!(
            first.active_session.as_ref().map(|session| &session.id),
            second.active_session.as_ref().map(|session| &session.id)
        );
        assert_eq!(
            first
                .active_session
                .as_ref()
                .expect("active session")
                .transcript
                .entries[0]
                .id,
            second
                .active_session
                .as_ref()
                .expect("active session")
                .transcript
                .entries[0]
                .id
        );
        let encoded = serde_json::to_string(&first).expect("serialize bootstrap");
        assert!(!encoded.contains("must-not-leak"));
    }

    #[test]
    fn session_transcript_omits_legacy_inline_subagent_rows() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.transcript.upsert(
            "subagent:run-1",
            EntryKind::Assistant,
            "reviewer",
            "legacy duplicate",
            EntryStatus::Complete,
        );
        state.transcript.upsert(
            "assistant:primary",
            EntryKind::Assistant,
            "Nakode",
            "primary response",
            EntryStatus::Complete,
        );

        let view = bootstrap(&state, 1, &[], &[]);
        let transcript = &view.active_session.expect("active session").transcript;
        assert_eq!(transcript.entries.len(), 1);
        assert_eq!(transcript.entries[0].body, "primary response");
    }

    #[test]
    fn session_snapshot_and_history_pages_are_explicitly_bounded() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 3_000);
        for index in 0..2_000 {
            state.transcript.push(
                EntryKind::Assistant,
                "Nakode",
                format!("entry-{index:04}-{}", "x".repeat(1_024)),
                EntryStatus::Complete,
            );
        }
        state.transcript.push(
            EntryKind::Assistant,
            "Nakode",
            "z".repeat(MAX_TRANSCRIPT_ENTRY_BODY_BYTES * 2),
            EntryStatus::Running,
        );

        let snapshot = bootstrap(&state, 3, &[], &[]);
        let session = snapshot.active_session.expect("active session");
        assert!(session.transcript.has_earlier);
        assert!(session.transcript.entries.len() <= MAX_TRANSCRIPT_SNAPSHOT_ENTRIES);
        assert!(
            session
                .transcript
                .entries
                .iter()
                .map(|entry| entry.body.len())
                .sum::<usize>()
                <= MAX_TRANSCRIPT_SNAPSHOT_BODY_BYTES
        );
        let last = session.transcript.entries.last().expect("latest entry");
        assert!(last.body_start_byte > 0);
        assert_eq!(
            last.body_total_bytes,
            u64::try_from(MAX_TRANSCRIPT_ENTRY_BODY_BYTES * 2).expect("body length")
        );
        assert!(
            serde_json::to_vec(&session).expect("encode session").len() < MAX_API_MESSAGE_BYTES
        );

        let before = session.transcript.entries[0].id.clone();
        let earlier = super::session_transcript_page(&state, Some(&before), 32)
            .expect("page from canonical transcript");
        assert!(!earlier.entries.is_empty());
        assert!(earlier.entries.iter().all(|entry| {
            !session
                .transcript
                .entries
                .iter()
                .any(|visible| visible.id == entry.id)
        }));
    }

    #[test]
    fn current_owner_metadata_is_additive_when_the_owner_row_is_retained() {
        let mut transcript = DomainTranscript::new(10);
        transcript.upsert(
            "owner",
            EntryKind::User,
            "YOU",
            "Inspect one file",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("owner", "turn-retained", None, false);
        transcript.upsert(
            "read",
            EntryKind::Tool,
            "read",
            "README.md",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("read", "turn-retained", None, false);

        let page = super::projected_transcript_page(&transcript, None, 10, 1_024)
            .expect("untruncated current page");
        let owner = page.current_owner_entry.as_ref().expect("current owner");
        assert!(page.entries.iter().any(|entry| entry.id == owner.id));
        assert_eq!(page.current_owner_omitted_tool_calls, 0);
    }

    #[test]
    fn current_owner_metadata_preserves_omitted_prompt_and_counts_only_its_omitted_tools() {
        let mut transcript = DomainTranscript::new(100);
        transcript.upsert(
            "owner",
            EntryKind::User,
            "YOU",
            "Inspect the transcript window",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("owner", "turn-7", None, false);
        for (key, kind, status) in [
            ("read-1", EntryKind::Tool, EntryStatus::Complete),
            ("assistant", EntryKind::Assistant, EntryStatus::Complete),
            ("grep-2", EntryKind::Tool, EntryStatus::Failed),
            ("warning", EntryKind::Warning, EntryStatus::Complete),
            ("read-3", EntryKind::Diff, EntryStatus::Complete),
            ("read-4", EntryKind::Tool, EntryStatus::Running),
        ] {
            transcript.upsert(key, kind, key, key, status);
            transcript.set_turn_attribution(key, "turn-7", None, false);
        }

        let page = super::projected_transcript_page(&transcript, None, 3, 1_024)
            .expect("bounded current page");
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>(),
            ["warning", "read-3", "read-4"]
        );
        let owner = page
            .current_owner_entry
            .as_ref()
            .expect("omitted owner message");
        assert_eq!(owner.body, "Inspect the transcript window");
        assert!(owner.created_at_ms.is_some());
        assert_eq!(page.current_owner_omitted_tool_calls, 2);

        let history = super::projected_transcript_page(&transcript, Some(&owner.id), 3, 1_024)
            .expect("historical page");
        assert_eq!(history.current_owner_entry, None);
        assert_eq!(history.current_owner_omitted_tool_calls, 0);
    }

    #[test]
    fn current_owner_body_is_not_double_charged_against_the_page_budget() {
        let mut transcript = DomainTranscript::new(100);
        transcript.upsert(
            "owner",
            EntryKind::User,
            "YOU",
            "Keep this turn visible",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("owner", "turn-budget", None, false);
        transcript.upsert(
            "omitted-tool",
            EntryKind::Tool,
            "read",
            "tool body",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("omitted-tool", "turn-budget", None, false);
        transcript.upsert(
            "latest-message",
            EntryKind::Assistant,
            "Nakode",
            "1234567890",
            EntryStatus::Complete,
        );
        transcript.set_turn_attribution("latest-message", "turn-budget", None, false);

        let page = super::projected_transcript_page(&transcript, None, 10, 30)
            .expect("body-budgeted current page");
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>(),
            ["YOU", "read", "Nakode"]
        );
        assert!(!page.has_earlier);
        let projected_body_bytes = page
            .entries
            .iter()
            .map(|entry| entry.body.len())
            .sum::<usize>()
            + page
                .current_owner_entry
                .as_ref()
                .map_or(0, |entry| entry.body.len());
        assert!(projected_body_bytes <= 30);
        assert_eq!(page.current_owner_omitted_tool_calls, 0);
        assert_eq!(
            page.current_owner_entry.as_ref().map(|entry| (
                entry.body.as_str(),
                entry.body_start_byte,
                entry.body_total_bytes
            )),
            Some(("", 22, 22))
        );
    }

    #[test]
    fn over_budget_tool_audit_is_omitted_without_dropping_its_transcript_row() {
        let mut transcript = DomainTranscript::new(100);
        transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "read · README.md",
            "",
            EntryStatus::Complete,
        );
        transcript.set_tool_audit("tool-1", Some("x".repeat(64)));

        let page = super::projected_transcript_page(&transcript, None, 10, 16)
            .expect("bounded transcript page");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].title, "read · README.md");
        assert_eq!(page.entries[0].tool_audit_json, None);

        let page = super::projected_transcript_page(&transcript, None, 10, 128)
            .expect("roomy transcript page");
        assert_eq!(
            page.entries[0].tool_audit_json.as_deref(),
            Some("x".repeat(64).as_str())
        );
    }

    #[test]
    fn delegated_invocation_audit_identity_survives_a_zero_body_budget() {
        let mut transcript = DomainTranscript::new(100);
        transcript.upsert(
            "delegate-1",
            EntryKind::Tool,
            "nakode_agent · reviewer",
            "",
            EntryStatus::Complete,
        );
        transcript.set_tool_audit(
            "delegate-1",
            Some(r#"{"callId":"call-1","name":"nakode_agent"}"#.to_owned()),
        );

        let page = super::projected_transcript_page(&transcript, None, 10, 0)
            .expect("delegated audit projection");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(
            page.entries[0].tool_audit_json.as_deref(),
            Some(r#"{"callId":"call-1","name":"nakode_agent"}"#)
        );
    }

    #[test]
    fn current_owner_budget_exhaustion_still_emits_newer_cursor_rows() {
        let mut transcript = DomainTranscript::new(100);
        transcript.upsert(
            "owner-1",
            EntryKind::User,
            "YOU",
            "o".repeat(64),
            EntryStatus::Complete,
        );
        transcript.upsert(
            "assistant-1",
            EntryKind::Assistant,
            "Assistant",
            "newer response",
            EntryStatus::Complete,
        );

        let page = super::projected_transcript_page(&transcript, None, 1, 8)
            .expect("cursor-bearing transcript page");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].title, "Assistant");
        assert_eq!(page.entries[0].body, "");
        assert_eq!(page.entries[0].body_start_byte, 14);
    }

    fn subagent_records(parent_session_id: &str, count: usize) -> Vec<SubagentRecord> {
        (0..count)
            .rev()
            .map(|index| SubagentRecord {
                parent_session_id: parent_session_id.to_owned(),
                id: format!("run-{index:03}"),
                agent: "reviewer".to_owned(),
                provider: CODEX_PROVIDER.to_owned(),
                model: None,
                provider_session_id: None,
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
                objective: format!("Review {index}"),
                status: SubagentStatus::Completed,
                latest_activity: "Completed".to_owned(),
                transcript: Vec::new(),
                observability: SubagentObservability {
                    // Exercise the session-wide recursive semantics: descendants count and compete
                    // at the same latest-64 boundary as direct runs.
                    parent_run_id: (index % 2 == 1).then(|| format!("run-{:03}", index - 1)),
                    ..SubagentObservability::default()
                },
                transcript_has_earlier: false,
            })
            .collect()
    }

    #[test]
    fn session_run_window_and_authoritative_total_are_independent_at_every_cap_boundary() {
        for count in [0, 7, MAX_SESSION_RUNS, MAX_SESSION_RUNS + 17] {
            let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
            let parent_session_id = state.nakode_session_id.clone();
            let _ = state.install_subagents(subagent_records(&parent_session_id, count));

            let session = bootstrap(&state, 2, &[], &[])
                .active_session
                .expect("active session");
            assert_eq!(
                session.runs_total,
                Some(u64::try_from(count).expect("test count"))
            );
            assert_eq!(session.runs.len(), count.min(MAX_SESSION_RUNS));
            assert_eq!(session.runs_has_earlier, count > MAX_SESSION_RUNS);

            let first_retained = count.saturating_sub(MAX_SESSION_RUNS);
            assert_eq!(
                session.runs.first().map(|run| run.id.as_str()),
                (count > 0)
                    .then(|| format!("run-{first_retained:03}"))
                    .as_deref()
            );
            assert_eq!(
                session.runs.last().map(|run| run.id.as_str()),
                count
                    .checked_sub(1)
                    .map(|index| format!("run-{index:03}"))
                    .as_deref()
            );
        }
    }

    #[test]
    fn omitted_runs_are_discoverable_through_complete_cursor_pagination() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        let parent_session_id = state.nakode_session_id.clone();
        let _ = state.install_subagents(subagent_records(&parent_session_id, 150));

        let session = bootstrap(&state, 2, &[], &[])
            .active_session
            .expect("active session");
        assert_eq!(session.runs.len(), MAX_SESSION_RUNS);
        assert_eq!(session.runs_total, Some(150));
        assert!(session.runs_has_earlier);

        let mut before = None;
        let mut run_ids = Vec::new();
        loop {
            let page = super::run_page(&state, before.as_ref(), 37).expect("run page");
            assert!(page.runs.len() <= 37);
            if page.runs.is_empty() {
                break;
            }
            before = page.runs.first().map(|run| run.id.clone());
            run_ids.extend(page.runs.iter().map(|run| run.id.clone()));
            if !page.has_earlier {
                break;
            }
        }
        run_ids.sort();
        run_ids.dedup();
        assert_eq!(run_ids.len(), 150);
        assert_eq!(run_ids.first(), Some(&RunId::from("run-000")));
        assert_eq!(run_ids.last(), Some(&RunId::from("run-149")));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn run_metadata_and_outcomes_expose_their_bounded_text_windows() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        let body = "r".repeat(MAX_TRANSCRIPT_ENTRY_BODY_BYTES * 2);
        let policy_json = serde_json::json!({
            "slug": "reviewer",
            "description": "Bounded reviewer",
            "tool_profile": "read_only",
            "denied_capabilities": ["filesystem_write"],
            "allowed_tools": (0..75).map(|index| format!("tool-{index}")).collect::<Vec<_>>(),
            "denied_tools": ["write"],
            "output_contract": "p".repeat(MAX_RUN_POLICY_TEXT_BYTES * 2),
            "reasoning_effort": "e".repeat(MAX_RUN_POLICY_TEXT_BYTES * 2),
            "timeout_seconds": 300,
            "max_turns": 5,
            "require_parent_attribution": true,
        })
        .to_string();
        let _ = state.install_subagents(vec![SubagentRecord {
            parent_session_id: state.nakode_session_id.clone(),
            id: "run-large".to_owned(),
            agent: "reviewer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "o".repeat(MAX_RUN_TEXT_BYTES * 2),
            status: SubagentStatus::Completed,
            latest_activity: "a".repeat(MAX_RUN_TEXT_BYTES * 2),
            transcript: (0..75)
                .map(|index| TranscriptEntry {
                    id: format!("run-entry-{index:02}"),
                    key: Some(format!("assistant-{index}")),
                    kind: EntryKind::Assistant,
                    title: "reviewer".to_owned(),
                    body: body.clone(),
                    status: EntryStatus::Complete,
                    created_at_ms: None,
                    provider_id: None,
                    model_id: None,
                    owner_turn_id: None,
                    reasoning_effort: None,
                    fast_mode: None,
                    source_transport: None,
                    tool_audit_json: Some(
                        serde_json::json!({
                            "denied": true,
                            "name": "write",
                            "denialReason": "d".repeat(MAX_RUN_TOOL_DENIAL_TEXT_BYTES * 2),
                        })
                        .to_string(),
                    ),
                })
                .collect(),
            observability: SubagentObservability {
                parent_run_id: Some("root-run".to_owned()),
                archetype_purpose: "Bounded reviewer".to_owned(),
                policy_json,
                started_at_ms: 100,
                ended_at_ms: Some(150),
                termination_kind: Some("completed".to_owned()),
                ..SubagentObservability::default()
            },
            transcript_has_earlier: false,
        }]);

        let run = super::run_view(&state, &RunId::from("run-large")).expect("run projection");
        assert_eq!(run.parent_run_id, Some(RunId::from("root-run")));
        assert_eq!(run.archetype_purpose, "Bounded reviewer");
        assert_eq!(run.duration_ms, Some(50));
        assert_eq!(run.termination_kind.as_deref(), Some("completed"));
        assert_eq!(run.policy.tool_profile, "read_only");
        assert_eq!(run.policy.provider, CODEX_PROVIDER);
        assert!(run.policy.policy_available);
        assert!(run.policy.provider_tools_restricted);
        assert_eq!(
            run.policy.provider_allowed_tools,
            ["read_skill", "read_skill_component"]
        );
        assert_eq!(
            run.policy.unsupported_canonical_tools.len(),
            MAX_RUN_POLICY_ITEMS
        );
        assert_eq!(run.policy.denied_tools, ["write"]);
        assert_eq!(run.policy.allowed_tools.len(), MAX_RUN_POLICY_ITEMS);
        assert_eq!(run.policy.output_contract.len(), MAX_RUN_POLICY_TEXT_BYTES);
        assert_eq!(
            run.reasoning_effort.as_ref().map(String::len),
            Some(MAX_RUN_POLICY_TEXT_BYTES)
        );
        assert_eq!(
            run.policy.truncated_fields,
            [
                "reasoning_effort",
                "allowed_tools",
                "unsupported_canonical_tools",
                "output_contract"
            ]
        );
        assert_eq!(run.tool_denials.len(), MAX_RUN_TOOL_DENIALS);
        assert_eq!(run.tool_denials_retained_total, 75);
        assert_eq!(run.tool_denials[0].entry_id, "run-entry-25");
        assert_eq!(run.tool_denials[0].tool, "write");
        assert_eq!(
            run.tool_denials[0].reason.len(),
            MAX_RUN_TOOL_DENIAL_TEXT_BYTES
        );
        assert!(run.tool_denials[0].reason_start_byte > 0);
        assert_eq!(
            run.tool_denials[0].reason_total_bytes,
            u64::try_from(MAX_RUN_TOOL_DENIAL_TEXT_BYTES * 2).unwrap()
        );
        assert!(run.objective_start_byte > 0);
        assert_eq!(run.objective.len(), MAX_RUN_TEXT_BYTES);
        assert!(run.latest_activity_start_byte > 0);
        assert!(run.outcome_start_byte > 0);
        assert!(run.result_start_byte > 0);
        assert!(serde_json::to_vec(&run).expect("encode run").len() < MAX_API_MESSAGE_BYTES);
    }

    #[test]
    fn transcript_images_project_stable_reconnectable_artifacts() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.transcript.set_labeled_images(
            "user:image",
            vec![(
                "clipboard.png".to_owned(),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: vec![0, 1, 2, 255],
                },
            )],
        );
        state.transcript.upsert(
            "user:image",
            EntryKind::User,
            "YOU",
            "[clipboard.png]",
            EntryStatus::Complete,
        );

        let first = bootstrap(&state, 7, &[], &[]);
        let second = bootstrap(&state, 8, &[], &[]);
        let first_id = first
            .active_session
            .as_ref()
            .expect("active session")
            .transcript
            .entries[0]
            .artifacts[0]
            .clone();
        let second_id = second
            .active_session
            .as_ref()
            .expect("active session")
            .transcript
            .entries[0]
            .artifacts[0]
            .clone();
        assert_eq!(first_id, second_id);

        let artifact = artifact_view(&state, &first_id)
            .expect("artifact is within the protocol limit")
            .expect("artifact exists");
        assert_eq!(artifact.label, "clipboard.png");
        assert_eq!(artifact.media_type, "image/png");
        assert_eq!(artifact.byte_length, 4);
        assert_eq!(artifact.data, [0, 1, 2, 255]);
    }

    #[test]
    fn run_images_are_queryable_and_oversized_artifacts_fail_explicitly() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        let mut transcript = DomainTranscript::new(100);
        transcript.set_labeled_images(
            "run:image",
            vec![(
                "review.png".to_owned(),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: vec![9, 8, 7],
                },
            )],
        );
        transcript.upsert(
            "run:image",
            EntryKind::User,
            "TASK",
            "[review.png]",
            EntryStatus::Complete,
        );
        state.subagents.push(SubagentRun {
            id: "run-1".to_owned(),
            agent: "reviewer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            usage: crate::backend::BackendTokenUsage::default(),
            objective: "Review".to_owned(),
            status: SubagentStatus::Working,
            latest_activity: "Working".to_owned(),
            observability: SubagentObservability::default(),
        });
        state.subagent_chats.insert(
            "run-1".to_owned(),
            SubagentChat {
                transcript,
                reasoning_summaries: ReasoningSummaryTracker::default(),
            },
        );

        let view = bootstrap(&state, 3, &[], &[]);
        let artifact_id = view.active_session.as_ref().expect("active session").runs[0]
            .transcript
            .entries[0]
            .artifacts[0]
            .clone();
        assert_eq!(
            artifact_view(&state, &artifact_id)
                .expect("bounded artifact")
                .expect("run artifact")
                .data,
            [9, 8, 7]
        );

        let chat = state
            .subagent_chats
            .get_mut("run-1")
            .expect("run transcript");
        chat.transcript.set_labeled_images(
            "run:image",
            vec![(
                "review.png".to_owned(),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: vec![0; MAX_ARTIFACT_BYTES.saturating_add(1)],
                },
            )],
        );
        let error = artifact_view(&state, &artifact_id).expect_err("artifact exceeds limit");
        assert_eq!(error.actual, MAX_ARTIFACT_BYTES.saturating_add(1));
        assert_eq!(error.maximum, MAX_ARTIFACT_BYTES);
    }

    #[test]
    fn completed_run_outcome_uses_the_final_assistant_body() {
        let transcript = transcript([
            entry(
                TranscriptEntryKind::Assistant,
                "Earlier response",
                TranscriptEntryStatus::Complete,
            ),
            entry(
                TranscriptEntryKind::Assistant,
                "Final response",
                TranscriptEntryStatus::Complete,
            ),
        ]);

        assert_eq!(
            run_outcome(RunStatus::Completed, "Completed", &transcript),
            Some(RunOutcome::Completed {
                body: "Final response".to_owned(),
            })
        );
    }

    #[test]
    fn failed_run_outcome_uses_the_exact_error_reason() {
        let transcript = transcript([
            entry(
                TranscriptEntryKind::Assistant,
                "Partial response",
                TranscriptEntryStatus::Complete,
            ),
            entry(
                TranscriptEntryKind::Error,
                "Provider authentication expired.",
                TranscriptEntryStatus::Failed,
            ),
        ]);

        assert_eq!(
            run_outcome(RunStatus::Failed, "Failed", &transcript),
            Some(RunOutcome::Failed {
                reason: "Provider authentication expired.".to_owned(),
            })
        );
    }

    #[test]
    fn interrupted_run_outcome_uses_the_exact_interruption_reason() {
        let transcript = transcript([entry(
            TranscriptEntryKind::System,
            "Interrupted by a client.",
            TranscriptEntryStatus::Interrupted,
        )]);

        assert_eq!(
            run_outcome(RunStatus::Interrupted, "Interrupted", &transcript),
            Some(RunOutcome::Interrupted {
                reason: "Interrupted by a client.".to_owned(),
            })
        );
    }

    #[test]
    fn active_run_has_no_terminal_outcome() {
        assert_eq!(
            run_outcome(RunStatus::Working, "Working", &transcript([])),
            None
        );
    }

    fn transcript<const N: usize>(entries: [TranscriptEntryView; N]) -> TranscriptPage {
        TranscriptPage {
            entries: entries.into(),
            has_earlier: false,
            stream_active: false,
            stream_label: "reviewer".to_owned(),
            current_owner_entry: None,
            current_owner_omitted_tool_calls: 0,
        }
    }

    fn entry(
        kind: TranscriptEntryKind,
        body: &str,
        status: TranscriptEntryStatus,
    ) -> TranscriptEntryView {
        TranscriptEntryView {
            id: EntryId::from(format!("entry-{body}")),
            kind,
            title: String::new(),
            body: body.to_owned(),
            body_start_byte: 0,
            body_total_bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
            status,
            artifacts: Vec::new(),
            created_at_ms: None,
            provider_id: None,
            model_id: None,
            owner_turn_id: None,
            resolved_reasoning_effort: None,
            resolved_fast_mode: None,
            source_transport: None,
            source_prompt_id: None,
            tool_audit_json: None,
            parent_tool_entry_id: None,
        }
    }

    fn model(provider: &str, id: &str) -> ModelInfo {
        ModelInfo {
            provider: provider.to_owned(),
            id: id.to_owned(),
            is_default: false,
            capabilities: crate::backend::ModelCapabilities {
                reasoning_efforts: if provider == CODEX_PROVIDER {
                    ["none", "low", "medium", "high", "xhigh", "max"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect()
                } else {
                    Vec::new()
                },
            },
        }
    }
}
