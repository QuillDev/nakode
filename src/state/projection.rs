use std::{collections::BTreeSet, path::Component};

use nakode_protocol::{
    AgentBrowserView, AgentDefinitionView, AgentSessionId, AgentSessionView, ArtifactId,
    ArtifactView, BootstrapView, ConnectionView, ContextUsageView, EntryId, ExternalToolCallView,
    InteractionId, InteractionKind, InteractionOptionView, InteractionStatus, InteractionView,
    MAX_ARTIFACT_BYTES, MAX_RUN_POLICY_ITEMS, MAX_RUN_POLICY_TEXT_BYTES, MAX_RUN_TEXT_BYTES,
    MAX_RUN_TOOL_DENIAL_TEXT_BYTES, MAX_RUN_TOOL_DENIALS, MAX_SESSION_RUNS, MAX_SESSION_RUNS_BYTES,
    MAX_TRANSCRIPT_ENTRY_BODY_BYTES, MAX_TRANSCRIPT_PAGE_BODY_BYTES, MAX_TRANSCRIPT_PAGE_ENTRIES,
    MemorySettingsView, ModelConfigurationView, ModelId, ModelOptions as ProtocolModelOptions,
    ModelView, NoticeLevel, NoticeView, PromptAttachment as ProtocolPromptAttachment, PromptId,
    ProviderAuthenticationView, ProviderCapabilities, ProviderCapability, ProviderId, ProviderView,
    QueueItemView, RecoverablePromptView, RunId, RunOutcome, RunPage, RunPolicyView, RunStatus,
    RunTextField, RunTextWindow, RunToolDenialView, RunView, SessionActivity, SessionId,
    SessionSummary, SessionView, SettingsView, SkillView, TerminalImageModeView, TodoItemView,
    TodoPhaseView, TodoStatusView, TranscriptBodyWindow, TranscriptEntryKind,
    TranscriptEntryStatus, TranscriptEntryView, TranscriptPage, TurnId, TurnStatus, TurnView,
    VisionSettingsView, WebSettingsView, WorkspaceId,
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
    session::{ProviderRecord, SessionRecord},
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
                title: active_session.title.clone(),
                active_provider_id: provider_id(&state.backend_provider),
                active_model_id: state.selected_model.clone().map(ModelId::from),
                updated_at_ms: 0,
                created_at_ms: 0,
                owned_provider_sessions: owned_provider_sessions(state),
                running: state.is_busy(),
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
            .map(|model| {
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
            })
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
        settings: settings_view(state),
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
    SessionView {
        id: session_id.clone(),
        revision,
        workspace_id: workspace_id.clone(),
        title: session_title(state, sessions),
        status_message: state.status_message.clone(),
        diagnostic_count: u64::try_from(state.diagnostic_count).unwrap_or(u64::MAX),
        activity: activity(state),
        selected_provider_id: provider,
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
        RunTextField::Result if run_status(run.status) == RunStatus::Completed => {
            latest_run_transcript_body(state, run, |entry| entry.kind == EntryKind::Assistant)
                .or(Some(&run.latest_activity))
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
    let result_entry = transcript
        .entries
        .iter()
        .rev()
        .find(|entry| entry.kind == TranscriptEntryKind::Assistant);
    let result = (status == RunStatus::Completed).then(|| {
        result_entry.map_or_else(
            || latest_activity.clone(),
            BoundedText::from_transcript_entry,
        )
    });
    let (outcome, outcome_window) = run_outcome_projection(status, &latest_activity, &transcript);
    let (policy, reasoning_effort, fast_mode) = run_policy(run);
    let (tool_denials, tool_denials_retained_total) = projected_tool_denials(state, run);
    let ended_at_ms = run.observability.ended_at_ms;
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
        transcript,
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
    run_outcome_projection(status, &bounded_text(latest_activity), transcript).0
}

fn run_outcome_projection(
    status: RunStatus,
    latest_activity: &BoundedText,
    transcript: &TranscriptPage,
) -> (Option<RunOutcome>, BoundedText) {
    match status {
        RunStatus::Starting | RunStatus::Working => (None, BoundedText::default()),
        RunStatus::Completed => {
            let window = latest_run_entry(transcript, |entry| {
                entry.kind == TranscriptEntryKind::Assistant
            })
            .map_or_else(
                || latest_activity.clone(),
                BoundedText::from_transcript_entry,
            );
            (
                Some(RunOutcome::Completed {
                    body: window.value.clone(),
                }),
                window,
            )
        }
        RunStatus::Failed => {
            let window =
                latest_run_entry(transcript, |entry| entry.kind == TranscriptEntryKind::Error)
                    .map_or_else(
                        || latest_activity.clone(),
                        BoundedText::from_transcript_entry,
                    );
            (
                Some(RunOutcome::Failed {
                    reason: window.value.clone(),
                }),
                window,
            )
        }
        RunStatus::Interrupted => {
            let window = latest_run_entry(transcript, |entry| {
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
            );
            (
                Some(RunOutcome::Interrupted {
                    reason: window.value.clone(),
                }),
                window,
            )
        }
    }
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
    }
}

fn settings_view(state: &DomainState) -> SettingsView {
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
        vision: VisionSettingsView {
            model_id: state.vision_config.model.clone().map(ModelId::from),
        },
        terminal_images: match state.terminal_image_mode {
            TerminalImageMode::Auto => TerminalImageModeView::Auto,
            TerminalImageMode::On => TerminalImageModeView::On,
            TerminalImageMode::Off => TerminalImageModeView::Off,
        },
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
        MAX_TRANSCRIPT_PAGE_ENTRIES,
        MAX_TRANSCRIPT_PAGE_BODY_BYTES,
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

    let mut remaining_body_bytes = body_budget;
    let mut projected = Vec::with_capacity(limit.min(entries.len()));
    let mut truncated_body = false;
    for entry in entries[..end].iter().rev().take(limit) {
        // Tool audit envelopes share the same IPC/memory budget as transcript bodies. Providers bound
        // each payload field before it reaches here; the page keeps an envelope whole so a client is
        // never handed valid-looking partial JSON.
        let audit_bytes = entry.tool_audit_json.as_ref().map_or(0, String::len);
        let include_audit = audit_bytes <= remaining_body_bytes;
        if include_audit {
            remaining_body_bytes = remaining_body_bytes.saturating_sub(audit_bytes);
        }
        let body_limit = remaining_body_bytes.min(MAX_TRANSCRIPT_ENTRY_BODY_BYTES);
        if body_limit == 0 && !entry.body.is_empty() {
            break;
        }
        let body = utf8_tail(&entry.body, body_limit);
        truncated_body |= body.len() < entry.body.len();
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
    Some(TranscriptPage {
        entries: projected,
        has_earlier: transcript.has_earlier_entries() || omitted_entries || truncated_body,
        stream_active: before.is_none() && transcript.stream_active(),
        stream_label: transcript.stream_label().to_owned(),
    })
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
        tool_audit_json: include_audit
            .then(|| entry.tool_audit_json.clone())
            .flatten(),
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
    }
}

fn unix_seconds_to_milliseconds(seconds: i64) -> i64 {
    seconds.saturating_mul(1_000)
}

fn session_summary(session: &SessionRecord, workspace_id: &WorkspaceId) -> SessionSummary {
    SessionSummary {
        id: SessionId::from(session.id.clone()),
        workspace_id: workspace_id.clone(),
        title: session.title.clone(),
        active_provider_id: provider_id(&session.provider),
        active_model_id: session.model.clone().map(ModelId::from),
        updated_at_ms: unix_seconds_to_milliseconds(session.updated_at),
        created_at_ms: unix_seconds_to_milliseconds(session.created_at),
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
        MAX_TRANSCRIPT_PAGE_BODY_BYTES, RunId, RunOutcome, RunStatus, TranscriptEntryKind,
        TranscriptEntryStatus, TranscriptEntryView, TranscriptPage,
    };

    use super::{artifact_view, bootstrap, capabilities_view, model_configuration, run_outcome};
    use crate::{
        backend::{
            BackendCapabilities, CLAUDE_PROVIDER, CODEX_PROVIDER, CURSOR_PROVIDER,
            CapabilitySupport, ModelInfo, PromptImage,
        },
        domain_transcript::{DomainTranscript, EntryKind, EntryStatus, TranscriptEntry},
        session::{SubagentObservability, SubagentRecord},
        state::{AppState, ReasoningSummaryTracker, SubagentChat, SubagentRun, SubagentStatus},
    };

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
        assert!(session.transcript.entries.len() <= nakode_protocol::MAX_TRANSCRIPT_PAGE_ENTRIES);
        assert!(
            session
                .transcript
                .entries
                .iter()
                .map(|entry| entry.body.len())
                .sum::<usize>()
                <= MAX_TRANSCRIPT_PAGE_BODY_BYTES
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
    fn omitted_runs_are_discoverable_through_complete_cursor_pagination() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        let parent_session_id = state.nakode_session_id.clone();
        state.install_subagents(
            (0..150)
                .map(|index| SubagentRecord {
                    parent_session_id: parent_session_id.clone(),
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
                    observability: SubagentObservability::default(),
                })
                .collect(),
        );

        let session = bootstrap(&state, 2, &[], &[])
            .active_session
            .expect("active session");
        assert_eq!(session.runs.len(), MAX_SESSION_RUNS);
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
        state.install_subagents(vec![SubagentRecord {
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
        assert!(run.policy.provider_allowed_tools.is_empty());
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
            tool_audit_json: None,
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
