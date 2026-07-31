use std::collections::BTreeSet;

use nakode_protocol::{
    AgentBrowserView, AgentDefinitionView, AgentSessionId, AgentSessionView, BootstrapView,
    ConnectionView, ContextUsageView, EntryId, InteractionId, InteractionKind,
    InteractionOptionView, InteractionStatus, InteractionView, MemorySettingsView, ModelId,
    ModelView, NoticeLevel, NoticeView, PromptId, ProviderAuthenticationView, ProviderCapabilities,
    ProviderCapability, ProviderId, ProviderView, QueueItemView, RunId, RunStatus, RunView,
    SessionActivity, SessionId, SessionSummary, SessionView, SettingsView, SkillView,
    TerminalImageModeView, TodoItemView, TodoPhaseView, TodoStatusView, TranscriptEntryKind,
    TranscriptEntryStatus, TranscriptEntryView, TranscriptPage, TurnId, TurnStatus, TurnView,
    VisionSettingsView, WebSettingsView, WorkspaceId,
};

use super::{
    AgentBrowserStatus, AppState, ConnectionState, ProviderAuthenticationState, SubagentStatus,
};
use crate::{
    backend::{BackendCapabilities, CapabilitySupport, TodoStatus},
    memory::MemoryBackend,
    session::{ProviderRecord, SessionRecord},
    terminal_image::TerminalImageMode,
    transcript::{EntryKind, EntryStatus, Transcript},
    web::WebBackend,
};

const ID_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0xf3dc_a1f0_948e_5fa3_8f0e_5c26_c07c_42be);

#[must_use]
pub fn workspace_id(workspace: &str) -> WorkspaceId {
    WorkspaceId::from(scoped_id("workspace", workspace))
}

#[must_use]
pub fn bootstrap(
    state: &AppState,
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
            },
        );
    }

    BootstrapView {
        workspace_id: workspace_id.clone(),
        workspace_path: state.workspace.clone(),
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

fn session_view(
    state: &AppState,
    revision: u64,
    workspace_id: &WorkspaceId,
    sessions: &[SessionRecord],
) -> SessionView {
    let session_id = SessionId::from(state.nakode_session_id.clone());
    let provider = provider_id(&state.backend_provider);
    let agent_session = agent_session_view(state, &session_id, provider.as_ref());
    let active_turn = turn_view(state, &session_id, agent_session.as_ref());

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
        active_agent_session: agent_session,
        active_turn,
        context_usage: context_usage_view(state),
        transcript: transcript_page(&state.transcript),
        queue: queue_views(state),
        interactions: interactions(state, revision),
        todos: todo_views(state),
        runs: run_views(state),
        notices: notice_views(state, revision),
    }
}

fn session_title(state: &AppState, sessions: &[SessionRecord]) -> String {
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
    state: &AppState,
    session_id: &SessionId,
    provider_id: Option<&ProviderId>,
) -> Option<AgentSessionView> {
    let provider_id = provider_id?;
    let opaque = state.provider_session_id.as_deref().unwrap_or("pending");
    Some(AgentSessionView {
        id: AgentSessionId::from(scoped_id(
            "agent-session",
            &format!("{}:{}:{opaque}", session_id.as_str(), provider_id.as_str()),
        )),
        provider_id: provider_id.clone(),
        model_id: state.selected_model.clone().map(ModelId::from),
        role: "primary".to_owned(),
        capabilities: capabilities_view(&state.backend_capabilities),
        connection: connection_view(&state.connection),
    })
}

fn turn_view(
    state: &AppState,
    session_id: &SessionId,
    agent_session: Option<&AgentSessionView>,
) -> Option<TurnView> {
    let turn = state.active_turn.as_ref()?;
    let agent_session = agent_session?;
    Some(TurnView {
        id: TurnId::from(scoped_id(
            "turn",
            &format!("{}:{}", session_id.as_str(), turn.id),
        )),
        agent_session_id: agent_session.id.clone(),
        model_id: turn.model.clone().map(|model| qualify_model(state, &model)),
        status: if turn.cancelling {
            TurnStatus::Cancelling
        } else {
            TurnStatus::Running
        },
    })
}

fn context_usage_view(state: &AppState) -> Option<ContextUsageView> {
    state.context_usage.map(|usage| ContextUsageView {
        estimated_tokens: u64::try_from(usage.estimated_tokens).unwrap_or(u64::MAX),
        context_window: usage
            .context_window
            .map(|window| u64::try_from(window).unwrap_or(u64::MAX)),
        compacting: state.context_compaction.is_some(),
    })
}

fn queue_views(state: &AppState) -> Vec<QueueItemView> {
    state
        .queue
        .iter()
        .map(|prompt| QueueItemView {
            id: PromptId::from(prompt.id.clone()),
            summary: first_line(&prompt.text),
            attachment_count: u32::try_from(prompt.attachments.len()).unwrap_or(u32::MAX),
        })
        .collect()
}

fn todo_views(state: &AppState) -> Vec<TodoPhaseView> {
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

fn run_views(state: &AppState) -> Vec<RunView> {
    state
        .subagents
        .iter()
        .map(|run| {
            let transcript = state
                .subagent_chats
                .get(&run.id)
                .map_or_else(empty_transcript_page, |chat| {
                    transcript_page(&chat.transcript)
                });
            let result = transcript
                .entries
                .iter()
                .rev()
                .find(|entry| entry.kind == TranscriptEntryKind::Assistant)
                .map(|entry| entry.body.clone());
            RunView {
                id: RunId::from(run.id.clone()),
                agent_slug: run.agent.clone(),
                provider_id: ProviderId::from(run.provider.clone()),
                objective: run.objective.clone(),
                status: run_status(run.status),
                latest_activity: run.latest_activity.clone(),
                result,
                transcript,
            }
        })
        .collect()
}

fn notice_views(state: &AppState, revision: u64) -> Vec<NoticeView> {
    (!state.status_message.is_empty())
        .then(|| NoticeView {
            id: format!("status:{revision}"),
            level: NoticeLevel::Info,
            message: state.status_message.clone(),
        })
        .into_iter()
        .collect()
}

fn provider_view(state: &AppState, provider: &ProviderRecord) -> ProviderView {
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

fn settings_view(state: &AppState) -> SettingsView {
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

fn interactions(state: &AppState, revision: u64) -> Vec<InteractionView> {
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
    });
    let questions = state.questions.iter().map(|question| InteractionView {
        id: question_interaction_id(&state.nakode_session_id, &question.request.id),
        revision,
        kind: InteractionKind::Question,
        status: InteractionStatus::Pending,
        title: question.request.title.clone(),
        detail: question.request.question.clone(),
        options: question
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
            .collect(),
        multiple: question.request.multi,
    });
    approvals.chain(questions).collect()
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

fn transcript_page(transcript: &Transcript) -> TranscriptPage {
    TranscriptPage {
        entries: transcript
            .entries()
            .iter()
            .map(|entry| TranscriptEntryView {
                id: EntryId::from(entry.id.clone()),
                kind: entry_kind(entry.kind),
                title: entry.title.clone(),
                body: entry.body.clone(),
                status: entry_status(entry.status),
                artifacts: Vec::new(),
            })
            .collect(),
        has_earlier: transcript.has_earlier_entries(),
        stream_active: transcript.stream_active(),
        stream_label: transcript.stream_label().to_owned(),
    }
}

fn empty_transcript_page() -> TranscriptPage {
    TranscriptPage {
        entries: Vec::new(),
        has_earlier: false,
        stream_active: false,
        stream_label: String::new(),
    }
}

fn session_summary(session: &SessionRecord, workspace_id: &WorkspaceId) -> SessionSummary {
    SessionSummary {
        id: SessionId::from(session.id.clone()),
        workspace_id: workspace_id.clone(),
        title: session.title.clone(),
        active_provider_id: provider_id(&session.provider),
        active_model_id: session.model.clone().map(ModelId::from),
        updated_at_ms: session.updated_at.saturating_mul(1_000),
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

fn activity(state: &AppState) -> SessionActivity {
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

fn qualify_model(state: &AppState, model: &str) -> ModelId {
    ModelId::from(if model.contains('/') {
        model.to_owned()
    } else {
        format!("{}/{model}", state.backend_provider)
    })
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
    use super::bootstrap;
    use crate::state::AppState;

    #[test]
    fn bootstrap_contains_stable_semantic_ids_and_no_secret_settings() {
        let mut state = AppState::new_unconfigured("/tmp/workspace", None, 100);
        state.web_config.firecrawl_api_key = "must-not-leak".to_owned();
        state.transcript.push(
            crate::transcript::EntryKind::User,
            "YOU",
            "hello",
            crate::transcript::EntryStatus::Complete,
        );

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
}
