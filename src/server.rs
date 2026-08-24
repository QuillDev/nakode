//! Native server command/query core.
//!
//! The transport talks only to this type. It owns canonical state and returns
//! server effects for the runtime supervisor to execute; clients never receive
//! provider commands, persistence handles, or process objects.

pub(crate) mod runtime;

use std::{
    collections::{HashMap, VecDeque},
    fmt::Write as _,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use nakode_protocol::{
    AgentDefinitionInput, AgentSessionId, BridgeContinuationDisposition, BridgeLifecycle,
    BridgeProjectionKind, BridgeProjectionView, Command, CommandAccepted, CredentialInput, EntryId,
    ErrorCode, IdempotencyKey, MAX_ARTIFACT_BYTES, MAX_TRANSCRIPT_DELTA_BYTES, McpGrantPolicy,
    McpServerInput, McpSessionGrant, ModelTarget, PromptInput, ProviderId, Query, QueryResult,
    RunId, RunMetadataView, RunTextField, RunView, ServiceCapability, ServiceError,
    SessionBridgeIntent, SessionId, SessionMetadataView, SessionView, Snapshot, SoulDocumentView,
    SubscriptionScope, SubscriptionView, TranscriptEntryStatus, TranscriptEntryView,
    TranscriptOwner, TranscriptPage, TranscriptWindowView, TurnId, ViewEvent, WorkspaceId,
};
use nakode_server::{ServerEndpoint, ServerRequest};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::{
    agent::{AgentCatalog, AgentDefinition, AgentFallbackPolicy, AgentOwnership, AgentToolProfile},
    backend::PromptAttachment,
    memory::MemoryConfig,
    service::ServiceEngine,
    session::{
        BridgeDeliveryRecord, BridgeInboundTurnOriginRecord, BridgePendingInboundRecord,
        BridgeProjectionRecord, ProviderRecord, SessionBridgeRecord, SessionRecord,
    },
    skill::SkillCatalog,
    soul::{SoulError, SoulSource, SoulStore},
    state::{DomainCommandError, Effect},
};

const IDEMPOTENCY_CAPACITY: usize = 1_024;
const RECENT_INBOUND_CACHE_CAPACITY: usize = 256;

type DomainCommandOutcome = Result<(CommandAccepted, Vec<Effect>), DomainCommandError>;
type McpArchetypeGrants = HashMap<String, std::collections::HashSet<String>>;
type McpInstalledGrant = (
    Vec<nakode_protocol::ExternalToolDefinition>,
    McpArchetypeGrants,
);

pub(crate) struct DispatchOutcome {
    pub(crate) effects: Vec<Effect>,
    pub(crate) effect_session: Option<SessionId>,
    pub(crate) changed: bool,
    command_response: Option<PendingCommandResponse>,
}

struct PendingCommandResponse {
    respond: oneshot::Sender<Result<CommandAccepted, ServiceError>>,
    result: Result<CommandAccepted, ServiceError>,
}

#[derive(Clone)]
struct CachedCommand {
    digest: [u8; 32],
    result: Result<CommandAccepted, ServiceError>,
}

#[derive(Clone)]
struct PublishedSessionProjection {
    view: SessionView,
    transcript_lengths: HashMap<EntryId, usize>,
    run_transcript_lengths: HashMap<RunId, HashMap<EntryId, usize>>,
}

struct PendingPublication {
    scopes: Vec<SubscriptionScope>,
    event: ViewEvent,
}

#[derive(Clone)]
pub(crate) struct BridgeStateCheckpoint {
    session_bridges: Vec<SessionBridgeRecord>,
    inbound_event_dispositions: HashMap<(SessionId, String), BridgeContinuationDisposition>,
    command_cache: HashMap<IdempotencyKey, CachedCommand>,
    command_order: VecDeque<IdempotencyKey>,
    published_workspace: Option<nakode_protocol::BootstrapView>,
}

#[derive(Clone)]
pub struct ServerCore {
    sessions_by_id: HashMap<SessionId, ServiceEngine>,
    default_session: SessionId,
    session_template: crate::state::DomainState,
    mcp_servers: Vec<crate::mcp::McpServerRecord>,
    providers: Vec<ProviderRecord>,
    sessions: Vec<SessionRecord>,
    session_inventory_complete: bool,
    session_bridges: Vec<SessionBridgeRecord>,
    inbound_event_dispositions: HashMap<(SessionId, String), BridgeContinuationDisposition>,
    command_cache: HashMap<IdempotencyKey, CachedCommand>,
    command_order: VecDeque<IdempotencyKey>,
    published_workspace: Option<nakode_protocol::BootstrapView>,
    published_sessions: HashMap<SessionId, PublishedSessionProjection>,
    soul_store: Option<SoulStore>,
}

impl ServerCore {
    #[must_use]
    pub fn new(
        engine: ServiceEngine,
        providers: Vec<ProviderRecord>,
        sessions: Vec<SessionRecord>,
    ) -> Self {
        let default_session = SessionId::from(engine.state().nakode_session_id.clone());
        let session_template = engine.state().clone();
        let sessions_by_id = HashMap::from([(default_session.clone(), engine)]);
        let mut core = Self {
            sessions_by_id,
            default_session,
            session_template,
            mcp_servers: Vec::new(),
            providers,
            sessions,
            session_inventory_complete: true,
            session_bridges: Vec::new(),
            inbound_event_dispositions: HashMap::new(),
            command_cache: HashMap::new(),
            command_order: VecDeque::new(),
            published_workspace: None,
            published_sessions: HashMap::new(),
            soul_store: SoulStore::user_default().ok(),
        };
        core.published_workspace = Some(core.workspace_bootstrap());
        if let Some(projection) = core.published_session(&core.default_session) {
            core.published_sessions
                .insert(core.default_session.clone(), projection);
        }
        core
    }

    pub(crate) fn set_session_inventory_complete(&mut self, complete: bool) {
        self.session_inventory_complete = complete;
    }

    pub(crate) fn install_soul_store(&mut self, store: SoulStore) {
        self.soul_store = Some(store);
    }

    #[must_use]
    /// Returns the runtime for the server's initial session.
    ///
    /// # Panics
    /// Panics only if the internal default-session invariant is broken.
    pub fn engine(&self) -> &ServiceEngine {
        self.sessions_by_id
            .get(&self.default_session)
            .expect("the default session runtime always exists")
    }

    /// Returns the mutable runtime for the server's initial session.
    ///
    /// # Panics
    /// Panics only if the internal default-session invariant is broken.
    pub fn engine_mut(&mut self) -> &mut ServiceEngine {
        self.sessions_by_id
            .get_mut(&self.default_session)
            .expect("the default session runtime always exists")
    }

    #[must_use]
    /// Consumes the server and returns the runtime for its initial session.
    ///
    /// # Panics
    /// Panics only if the internal default-session invariant is broken.
    pub fn into_engine(self) -> ServiceEngine {
        self.sessions_by_id
            .into_iter()
            .find_map(|(session_id, engine)| (session_id == self.default_session).then_some(engine))
            .expect("the default session runtime always exists")
    }

    pub(crate) fn install_session_bridges(&mut self, bridges: Vec<SessionBridgeRecord>) {
        self.session_bridges = bridges;
        self.published_workspace = Some(self.workspace_bootstrap());
    }

    pub(crate) fn install_skill_authority(
        &mut self,
        catalogue: &SkillCatalog,
        preferences: &HashMap<String, Vec<crate::skill::SkillPreference>>,
    ) {
        let installed_ids = catalogue.stable_ids();
        self.engine_mut()
            .state_mut()
            .install_skill_snapshot(catalogue.clone(), Some(&installed_ids));
        self.session_template
            .install_skill_snapshot(catalogue.clone(), Some(&installed_ids));
        for engine in self.sessions_by_id.values_mut() {
            let Some(profile_id) = engine.state().skill_profile_id().map(str::to_owned) else {
                continue;
            };
            let effective = catalogue.enabled_for(
                preferences
                    .get(&profile_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                &profile_id,
            );
            let ids = effective.stable_ids();
            engine
                .state_mut()
                .install_skill_snapshot(effective, Some(&ids));
        }
    }

    pub(crate) fn install_profile_skill_catalogue(
        &mut self,
        profile_id: &str,
        catalogue: &SkillCatalog,
    ) {
        for engine in self.sessions_by_id.values_mut() {
            if engine.state().skill_profile_id() == Some(profile_id) {
                engine
                    .state_mut()
                    .install_skill_snapshot(catalogue.clone(), Some(&catalogue.stable_ids()));
            }
        }
    }

    pub(crate) fn install_available_builtin_tools(
        &mut self,
        availability: &HashMap<String, Vec<String>>,
    ) {
        self.session_template
            .install_available_builtin_tools(availability.clone());
        for engine in self.sessions_by_id.values_mut() {
            engine
                .state_mut()
                .install_available_builtin_tools(availability.clone());
        }
    }

    pub(crate) fn remember_durable_bridge_inbound_event(
        &mut self,
        session_id: &SessionId,
        external_event_id: &str,
        disposition: BridgeContinuationDisposition,
    ) {
        self.remember_bridge_inbound_event(session_id, external_event_id, disposition);
    }

    fn remember_bridge_inbound_event(
        &mut self,
        session_id: &SessionId,
        external_event_id: &str,
        disposition: BridgeContinuationDisposition,
    ) {
        let evicted = self
            .session_bridges
            .iter_mut()
            .find(|bridge| bridge.session_id == session_id.as_str())
            .and_then(|bridge| remember_inbound_event(bridge, external_event_id));
        if let Some(evicted) = evicted {
            self.inbound_event_dispositions
                .remove(&(session_id.clone(), evicted));
        }
        self.inbound_event_dispositions.insert(
            (session_id.clone(), external_event_id.to_owned()),
            disposition,
        );
    }

    #[must_use]
    pub(crate) fn bridge_state_checkpoint(&self) -> BridgeStateCheckpoint {
        BridgeStateCheckpoint {
            session_bridges: self.session_bridges.clone(),
            inbound_event_dispositions: self.inbound_event_dispositions.clone(),
            command_cache: self.command_cache.clone(),
            command_order: self.command_order.clone(),
            published_workspace: self.published_workspace.clone(),
        }
    }

    pub(crate) fn restore_bridge_state(&mut self, checkpoint: BridgeStateCheckpoint) {
        self.session_bridges = checkpoint.session_bridges;
        self.inbound_event_dispositions = checkpoint.inbound_event_dispositions;
        self.command_cache = checkpoint.command_cache;
        self.command_order = checkpoint.command_order;
        self.published_workspace = checkpoint.published_workspace;
    }

    pub(crate) fn replace_session_bridges(&mut self, bridges: Vec<SessionBridgeRecord>) {
        self.session_bridges = bridges;
    }

    pub(crate) fn install_mcp_servers(&mut self, servers: Vec<crate::mcp::McpServerRecord>) {
        self.mcp_servers = servers;
    }

    #[must_use]
    pub(crate) fn mcp_servers(&self) -> &[crate::mcp::McpServerRecord] {
        &self.mcp_servers
    }

    pub(crate) fn replace_mcp_server(&mut self, server: crate::mcp::McpServerRecord) {
        if let Some(existing) = self
            .mcp_servers
            .iter_mut()
            .find(|item| item.id == server.id)
        {
            *existing = server;
        } else {
            self.mcp_servers.push(server);
            self.mcp_servers
                .sort_by(|left, right| left.display_name.cmp(&right.display_name));
        }
    }

    pub(crate) fn remove_mcp_server(&mut self, server_id: &str) {
        self.mcp_servers.retain(|server| server.id != server_id);
    }

    #[must_use]
    pub(crate) fn mcp_management(&self) -> nakode_protocol::McpManagementView {
        nakode_protocol::McpManagementView {
            workspace_id: crate::state::projection::workspace_id(&self.engine().state().workspace),
            servers: self
                .mcp_servers
                .iter()
                .map(crate::mcp::McpServerRecord::view)
                .collect(),
            templates: vec![crate::mcp::excalidraw_template()],
        }
    }

    pub(crate) fn default_session_id(&self) -> &SessionId {
        &self.default_session
    }

    pub(crate) fn engine_for(&self, session_id: &SessionId) -> Option<&ServiceEngine> {
        self.sessions_by_id.get(session_id)
    }

    pub(crate) fn engine_for_mut(&mut self, session_id: &SessionId) -> Option<&mut ServiceEngine> {
        self.sessions_by_id.get_mut(session_id)
    }

    /// Handles one transport request and returns effects for server-owned
    /// supervisors to execute.
    pub(crate) fn handle(
        &mut self,
        endpoint: &ServerEndpoint,
        request: ServerRequest,
    ) -> DispatchOutcome {
        match request {
            ServerRequest::Command {
                idempotency_key,
                expected_revision,
                replay_only,
                command,
                respond,
                ..
            } => {
                let (result, effects, effect_session, changed) = self.execute_idempotent(
                    idempotency_key,
                    expected_revision,
                    replay_only,
                    command,
                );
                DispatchOutcome {
                    effects,
                    effect_session,
                    changed,
                    command_response: Some(PendingCommandResponse { respond, result }),
                }
            }
            ServerRequest::Query { query, respond, .. } => {
                let cursor = endpoint.cursor();
                let refresh = if matches!(&query, Query::Bootstrap { .. }) {
                    self.reload_global_agent_catalogue()
                } else {
                    Ok(())
                };
                let result = if let Err(error) = refresh {
                    Err(error)
                } else if matches!(&query, Query::GetArtifact { .. })
                    && !endpoint
                        .capabilities()
                        .supports(ServiceCapability::ArtifactTransfer)
                {
                    Err(service_error(
                        ErrorCode::CapabilityUnsupported,
                        "artifact transfer is not supported by this Nakode server",
                        false,
                    ))
                } else {
                    self.query(query).map(|value| Snapshot { cursor, value })
                };
                let _ = respond.send(result);
                DispatchOutcome::unchanged()
            }
            ServerRequest::Subscribe { scope, respond, .. } => {
                let cursor = endpoint.cursor();
                let result = self
                    .subscription_view(&scope)
                    .map(|value| Snapshot { cursor, value });
                let _ = respond.send(result);
                DispatchOutcome::unchanged()
            }
        }
    }

    pub(crate) fn commit_and_publish_session(
        &mut self,
        endpoint: &ServerEndpoint,
        session_id: &SessionId,
    ) {
        self.commit_and_publish_session_inner(endpoint, session_id, false);
    }

    pub(crate) fn commit_and_publish_backend_session(
        &mut self,
        endpoint: &ServerEndpoint,
        session_id: &SessionId,
    ) {
        self.commit_and_publish_session_inner(endpoint, session_id, true);
    }

    fn commit_and_publish_session_inner(
        &mut self,
        endpoint: &ServerEndpoint,
        session_id: &SessionId,
        skip_unchanged_workspace: bool,
    ) {
        let workspace_configuration_changed =
            self.sessions_by_id.get(session_id).is_some_and(|engine| {
                !self
                    .session_template
                    .workspace_configuration_matches(engine.state())
            });
        let other_session_views = if workspace_configuration_changed {
            self.sessions_by_id
                .keys()
                .filter(|candidate_id| *candidate_id != session_id)
                .filter_map(|candidate_id| {
                    self.published_session(candidate_id)
                        .map(|projection| (candidate_id.clone(), projection.view))
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if let Some(engine) = self.sessions_by_id.get_mut(session_id) {
            engine.note_state_change();
        }
        if workspace_configuration_changed {
            self.synchronize_workspace_state_from(session_id);
        }
        let mut changed_sessions = other_session_views
            .into_iter()
            .filter_map(|(candidate_id, previous)| {
                self.published_session(&candidate_id)
                    .is_some_and(|current| current.view != previous)
                    .then_some(candidate_id)
            })
            .collect::<Vec<_>>();
        changed_sessions.sort();
        for changed_session in &changed_sessions {
            if let Some(engine) = self.sessions_by_id.get_mut(changed_session) {
                engine.note_state_change();
            }
        }
        let workspace_projection_changed = !skip_unchanged_workspace
            || workspace_configuration_changed
            || self.workspace_session_summary_changed(session_id)
            || self.workspace_bridges_changed();
        if workspace_projection_changed {
            self.publish_state(endpoint, session_id);
        } else {
            self.publish_session_state(endpoint, session_id);
        }
        for changed_session in changed_sessions {
            self.publish_session_state(endpoint, &changed_session);
        }
    }

    fn workspace_session_summary_changed(&self, session_id: &SessionId) -> bool {
        let Some(engine) = self.sessions_by_id.get(session_id) else {
            return false;
        };
        let Some(current) =
            crate::state::projection::active_session_summary(engine.state(), &self.sessions)
        else {
            return true;
        };
        self.published_workspace.as_ref().is_none_or(|workspace| {
            workspace
                .sessions
                .iter()
                .find(|summary| summary.id == current.id)
                != Some(&current)
        })
    }

    fn workspace_bridges_changed(&self) -> bool {
        let current = self
            .session_bridges
            .iter()
            .map(session_bridge_view)
            .collect::<Vec<_>>();
        self.published_workspace
            .as_ref()
            .is_none_or(|workspace| workspace.session_bridges != current)
    }

    pub(crate) fn commit_and_publish_session_delta(
        &mut self,
        endpoint: &ServerEndpoint,
        session_id: &SessionId,
    ) {
        if let Some(engine) = self.sessions_by_id.get_mut(session_id) {
            engine.note_state_change();
        }
        self.publish_session_state(endpoint, session_id);
    }

    pub(crate) fn replace_provider_records(&mut self, providers: Vec<ProviderRecord>) {
        self.providers = providers;
    }

    pub(crate) fn provider_records(&self) -> &[ProviderRecord] {
        &self.providers
    }

    pub(crate) fn configured_vision_model_provider(&self) -> Option<&str> {
        self.session_template.configured_vision_model_provider()
    }

    pub(crate) fn replace_session_records(&mut self, sessions: Vec<SessionRecord>) {
        self.sessions = sessions;
    }

    fn execute_idempotent(
        &mut self,
        key: IdempotencyKey,
        expected_revision: Option<u64>,
        replay_only: bool,
        command: Command,
    ) -> (
        Result<CommandAccepted, ServiceError>,
        Vec<Effect>,
        Option<SessionId>,
        bool,
    ) {
        let digest = command_digest(&command);
        if let Some(cached) = self.command_cache.get(&key) {
            let result = if cached.digest == digest {
                cached.result.clone()
            } else {
                Err(service_error(
                    ErrorCode::Conflict,
                    "the idempotency key was already used for a different command",
                    false,
                ))
            };
            return (result, Vec::new(), None, false);
        }
        if replay_only {
            return (
                Err(service_error(
                    ErrorCode::Conflict,
                    "the command result is no longer available; the mutation was not executed",
                    false,
                )),
                Vec::new(),
                None,
                false,
            );
        }
        let effect_session = self.command_session(&command);
        let command_revision = effect_session
            .as_ref()
            .and_then(|session_id| self.engine_for(session_id))
            .map_or_else(|| self.engine().revision(), ServiceEngine::revision);
        // Prompt submission is append-only owner intent. Its idempotency key, not a snapshot fence,
        // distinguishes a retry from another deliberate send; lifecycle and queue placement are
        // therefore evaluated against authoritative state when this command executes.
        let append_prompt = matches!(
            &command,
            Command::SendPrompt { .. } | Command::EnqueuePrompt { .. }
        );
        let revision_fenced = !append_prompt;
        let (mut result, effects) = if revision_fenced
            && expected_revision.is_some_and(|revision| revision != command_revision)
        {
            (
                Err(service_error(
                    ErrorCode::Conflict,
                    "the expected revision is stale",
                    true,
                )),
                Vec::new(),
            )
        } else {
            let prompt_id = append_prompt.then(|| key.as_str().to_owned());
            self.execute_command(command, prompt_id.as_deref())
        };
        let effect_session = effect_session.or_else(|| {
            result
                .as_ref()
                .ok()
                .and_then(|accepted| accepted.resource_id.as_ref())
                .map(|resource_id| SessionId::from(resource_id.clone()))
                .filter(|session_id| self.sessions_by_id.contains_key(session_id))
        });
        if let Ok(accepted) = &mut result {
            let revision = effect_session
                .as_ref()
                .and_then(|session_id| self.engine_for(session_id))
                .map_or_else(|| self.engine().revision(), ServiceEngine::revision);
            accepted.revision = Some(revision.saturating_add(1));
        }
        self.command_cache.insert(
            key.clone(),
            CachedCommand {
                digest,
                result: result.clone(),
            },
        );
        self.command_order.push_back(key);
        if self.command_order.len() > IDEMPOTENCY_CAPACITY
            && let Some(expired) = self.command_order.pop_front()
        {
            self.command_cache.remove(&expired);
        }
        let changed = result.is_ok();
        (result, effects, effect_session, changed)
    }

    fn execute_command(
        &mut self,
        command: Command,
        prompt_id: Option<&str>,
    ) -> (Result<CommandAccepted, ServiceError>, Vec<Effect>) {
        match self.try_execute_command_with_prompt_id(command, prompt_id) {
            Ok((accepted, effects)) => (Ok(accepted), effects),
            Err(error) => (Err(domain_error(error)), Vec::new()),
        }
    }

    #[cfg(test)]
    fn try_execute_command(&mut self, command: Command) -> DomainCommandOutcome {
        self.try_execute_command_with_prompt_id(command, None)
    }

    #[allow(clippy::too_many_lines)]
    fn try_execute_command_with_prompt_id(
        &mut self,
        command: Command,
        prompt_id: Option<&str>,
    ) -> DomainCommandOutcome {
        match command {
            Command::CreateSession {
                workspace_id,
                working_directory,
                title,
                model_id,
                options,
                tools,
                initial_instructions,
                bridge,
                mcp_grant,
                profile_id,
                disabled_skill_ids,
            } => self.create_session_command_with_mcp_and_skills(
                &workspace_id,
                working_directory.as_deref(),
                title.as_deref(),
                model_id.as_ref(),
                &options,
                tools,
                initial_instructions.as_deref(),
                bridge,
                mcp_grant.as_ref(),
                profile_id,
                &disabled_skill_ids,
            ),
            Command::OpenSession {
                session_id,
                tools,
                mcp_grant,
                profile_id,
                enabled_skill_ids,
            } => self.open_session_command_with_mcp_and_profile(
                &session_id,
                tools,
                mcp_grant.as_ref(),
                profile_id,
                &enabled_skill_ids,
            ),
            Command::SetSessionBridgeLifecycle {
                session_id,
                lifecycle,
            } => self.set_session_bridge_lifecycle_command(&session_id, lifecycle),
            Command::SetWorkspaceBridgeLifecycle {
                workspace_id,
                lifecycle,
            } => self.set_workspace_bridge_lifecycle_command(&workspace_id, lifecycle),
            Command::BindSessionBridgeThread {
                session_id,
                transport,
                external_parent_id,
                external_thread_id,
            } => self.bind_session_bridge_thread_command(
                &session_id,
                &transport,
                &external_parent_id,
                &external_thread_id,
            ),
            Command::ClearSessionBridgeThread {
                session_id,
                transport,
                external_thread_id,
            } => self.clear_session_bridge_thread_command(
                &session_id,
                &transport,
                &external_thread_id,
            ),
            Command::PrepareBridgeDelivery {
                session_id,
                projection_kind,
                turn_id,
                expected_last_projected,
                body_sha256,
                part_count,
            } => self.prepare_bridge_delivery_command(
                &session_id,
                projection_kind,
                &turn_id,
                expected_last_projected.as_ref(),
                &body_sha256,
                part_count,
            ),
            Command::CompleteBridgeDeliveryPart {
                session_id,
                projection_kind,
                turn_id,
                part_index,
                external_message_id,
            } => self.complete_bridge_delivery_part_command(
                &session_id,
                projection_kind,
                &turn_id,
                part_index,
                &external_message_id,
            ),
            Command::FinalizeBridgeDelivery {
                session_id,
                projection_kind,
                turn_id,
                clear_active_source_message_id,
            } => self.finalize_bridge_delivery_command_with_source(
                &session_id,
                projection_kind,
                &turn_id,
                clear_active_source_message_id.as_deref(),
            ),
            Command::SetBridgeLiveMessage {
                session_id,
                turn_id,
                external_message_id,
                clear_active_source_message_id,
            } => self.set_bridge_live_message_command(
                &session_id,
                turn_id.as_ref(),
                external_message_id.as_deref(),
                clear_active_source_message_id.as_deref(),
            ),
            Command::ContinueSessionFromBridge {
                session_id,
                transport,
                external_thread_id,
                external_event_id,
                source_message_id,
                prompt,
                consume_as_busy,
            } => self.continue_session_from_bridge_command(
                &session_id,
                &transport,
                &external_thread_id,
                &external_event_id,
                &source_message_id,
                prompt,
                consume_as_busy,
            ),
            Command::SendPrompt { session_id, prompt } => {
                let enqueue = self
                    .engine_for(&session_id)
                    .is_some_and(|engine| engine.state().is_busy());
                self.prompt_command(&session_id, prompt, enqueue, prompt_id)
            }
            Command::EnqueuePrompt { session_id, prompt } => {
                self.prompt_command(&session_id, prompt, true, prompt_id)
            }
            Command::RemoveQueuedPrompt {
                session_id,
                prompt_id,
            } => self.remove_queued_prompt_command(&session_id, prompt_id.as_str()),
            Command::SteerQueuedPrompt {
                session_id,
                prompt_id,
            } => self.steer_queued_prompt_command(&session_id, prompt_id.as_str()),
            Command::SteerTurn { turn_id, text } => self.steer_turn_command(&turn_id, &text),
            Command::CancelTurn { turn_id } => self.cancel_turn_command(&turn_id),
            Command::CancelSessionWork { session_id } => {
                self.cancel_session_work_command(&session_id)
            }
            Command::DeleteSession { session_id } => self.delete_session_command(&session_id),
            Command::CompactContext { agent_session_id } => {
                self.compact_context_command(&agent_session_id)
            }
            Command::SelectModel {
                target,
                model_id,
                options,
            } => self.select_model_command(&target, &model_id, &options),
            Command::ResolveInteraction {
                interaction_id,
                resolution,
            } => self.resolve_interaction_command(&interaction_id, &resolution),
            Command::ConfigureSessionTools {
                session_id,
                tools,
                replace_builtin_tools,
            } => {
                self.ensure_session(&session_id)?;
                let effects = self
                    .session_engine_mut(&session_id)?
                    .state_mut()
                    .configure_external_tools(tools, replace_builtin_tools)?;
                Ok(Self::accepted(Some(session_id.to_string()), effects))
            }
            Command::SubmitExternalToolResult {
                session_id,
                call_id,
                output,
                failed,
            } => {
                self.ensure_session(&session_id)?;
                let effects = self
                    .session_engine_mut(&session_id)?
                    .state_mut()
                    .submit_external_tool_result(&call_id, output, failed)?;
                Ok(Self::accepted(Some(session_id.to_string()), effects))
            }
            Command::Delegate {
                session_id,
                agent_slug,
                task,
                parent_run_id,
            } => self.delegate_command(&session_id, &agent_slug, &task, parent_run_id.as_ref()),
            Command::CancelRun { run_id } => self.cancel_run_command(&run_id),
            Command::ContinueRun {
                run_id,
                additional_turns,
            } => self.continue_run_command(&run_id, additional_turns),
            Command::RunShell {
                session_id,
                command,
            } => self.run_shell_command(&session_id, command),
            Command::SetSkillEnabled { .. } => Err(DomainCommandError::Invalid(
                "skill availability is served by the native persistence runtime".to_owned(),
            )),
            Command::SetProviderModelFilter {
                provider_id,
                enabled,
                selected_model_ids,
            } => self.set_provider_model_filter_command(&provider_id, enabled, selected_model_ids),
            Command::SetProviderEnabled {
                provider_id,
                enabled,
            } => self.set_provider_enabled_command(&provider_id, enabled),
            Command::BeginProviderAuthentication { provider_id } => {
                self.begin_provider_authentication_command(&provider_id)
            }
            Command::SetProviderCredential {
                provider_id,
                kind,
                credential,
            } => self.set_provider_credential_command(&provider_id, kind, &credential),
            Command::ClearProviderCredential { provider_id } => {
                self.clear_provider_credential_command(&provider_id)
            }
            Command::ReloadProvider { provider_id } => self.reload_provider_command(&provider_id),
            Command::SaveMcpServer {
                workspace_id,
                server,
                grants,
            } => self.save_mcp_server_command(&workspace_id, server, grants),
            Command::DeleteMcpServer {
                workspace_id,
                server_id,
            } => self.delete_mcp_server_command(&workspace_id, &server_id),
            Command::SetMcpServerEnabled {
                workspace_id,
                server_id,
                enabled,
            } => self.set_mcp_server_enabled_command(&workspace_id, &server_id, enabled),
            Command::RefreshMcpServer {
                workspace_id,
                server_id,
            } => self.refresh_mcp_server_command(&workspace_id, &server_id),
            Command::SetMcpServerCredential {
                workspace_id,
                server_id,
                kind,
                credential,
            } => {
                self.set_mcp_server_credential_command(&workspace_id, &server_id, kind, credential)
            }
            Command::ClearMcpServerCredential {
                workspace_id,
                server_id,
            } => self.clear_mcp_server_credential_command(&workspace_id, &server_id),
            Command::SetMcpServerGrants {
                workspace_id,
                server_id,
                grants,
            } => self.set_mcp_server_grants_command(&workspace_id, &server_id, grants),
            Command::SaveAgent {
                workspace_id,
                definition,
                previous_slug,
            } => self.save_agent_command(&workspace_id, definition, previous_slug),
            Command::DeleteAgent { workspace_id, slug } => {
                self.delete_agent_command(&workspace_id, slug)
            }
            Command::UpdateSettings { patch } => self.update_settings_command(&patch),
            Command::CheckAgentBrowser { workspace_id } => {
                self.check_agent_browser_command(&workspace_id)
            }
            Command::ReloadWorkspace {
                workspace_id,
                session_id,
            } => self.reload_workspace_command(&workspace_id, &session_id),
            Command::SaveSoul {
                workspace_id,
                content,
                expected_digest,
            } => self.save_soul_command(&workspace_id, &content, expected_digest.as_deref()),
        }
    }

    #[cfg(test)]
    fn create_session_command(
        &mut self,
        workspace_id: &WorkspaceId,
        model_id: Option<&nakode_protocol::ModelId>,
        options: &nakode_protocol::ModelOptions,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
    ) -> DomainCommandOutcome {
        self.create_session_command_with_mcp(
            workspace_id,
            None,
            None,
            model_id,
            options,
            tools,
            None,
            None,
            None,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn create_session_command_with_mcp(
        &mut self,
        workspace_id: &WorkspaceId,
        working_directory: Option<&str>,
        title: Option<&str>,
        model_id: Option<&nakode_protocol::ModelId>,
        options: &nakode_protocol::ModelOptions,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        initial_instructions: Option<&str>,
        bridge: Option<SessionBridgeIntent>,
        mcp_grant: Option<&McpSessionGrant>,
    ) -> DomainCommandOutcome {
        self.create_session_command_with_mcp_and_skills(
            workspace_id,
            working_directory,
            title,
            model_id,
            options,
            tools,
            initial_instructions,
            bridge,
            mcp_grant,
            None,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    // Session creation mirrors the versioned public command. Keeping every typed option explicit
    // prevents bridge/MCP/default policy from being hidden in a frontend-owned bag of values.
    fn create_session_command_with_mcp_and_skills(
        &mut self,
        workspace_id: &WorkspaceId,
        working_directory: Option<&str>,
        title: Option<&str>,
        model_id: Option<&nakode_protocol::ModelId>,
        options: &nakode_protocol::ModelOptions,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        initial_instructions: Option<&str>,
        bridge: Option<SessionBridgeIntent>,
        mcp_grant: Option<&McpSessionGrant>,
        profile_id: Option<String>,
        disabled_skill_ids: &[String],
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let working_directory =
            canonical_working_directory(working_directory, &self.session_template.workspace)?;
        if model_id.is_none() && (options.reasoning_effort.is_some() || options.fast_mode) {
            return Err(DomainCommandError::Invalid(
                "initial model options require model_id".to_owned(),
            ));
        }
        self.refresh_session_template_addenda()?;
        let skills = SkillCatalog::load(Path::new(&working_directory))
            .map(|catalogue| catalogue.without_ids(disabled_skill_ids))
            .map_err(|error| {
                DomainCommandError::Invalid(format!(
                    "failed to load skills for {working_directory}: {error}"
                ))
            })?;
        let mut engine = ServiceEngine::new(self.session_template.clone());
        engine.state_mut().set_working_directory(working_directory);
        engine.state_mut().set_skill_profile(profile_id);
        engine.state_mut().install_skill_snapshot(skills, None);
        engine
            .state_mut()
            .set_initial_client_instructions(initial_instructions)?;
        let mut effects = engine.state_mut().create_logical_session()?;
        let session_id = SessionId::from(engine.state().nakode_session_id.clone());
        if let Some(model_id) = model_id {
            effects.extend(engine.state_mut().select_model_intent(
                &ModelTarget::Session {
                    session_id: session_id.clone(),
                },
                model_id,
                options,
            )?);
        }
        if let Some(tools) = tools {
            let provider = engine.state().active_provider_id().to_owned();
            let tools = engine
                .state()
                .reconcile_available_builtin_tools(&provider, tools);
            effects.extend(engine.state_mut().configure_session_tools(
                tools.tools,
                tools.replace_builtin_tools,
                tools.allowed_builtin_tools,
            )?);
        }
        if let Some(grant) = mcp_grant {
            let (mcp_tools, archetype_grants) = self.mcp_tools_for_grant(grant)?;
            effects.extend(engine.state_mut().configure_mcp_tools(mcp_tools)?);
            engine
                .state_mut()
                .configure_mcp_archetype_grants(archetype_grants);
        }
        if let Some(bridge) = bridge {
            let display_title = validated_bridge_title(&bridge.display_title, title)?;
            let record = SessionBridgeRecord {
                session_id: session_id.to_string(),
                workspace: engine.state().workspace.clone(),
                kind: bridge.kind,
                lifecycle: bridge.lifecycle,
                display_title,
                revision: 1,
                transport: None,
                external_parent_id: None,
                external_thread_id: None,
                last_projected: None,
                delivery: None,
                live_turn_id: None,
                live_external_message_id: None,
                active_source_message_id: None,
                recent_inbound_event_ids: Vec::new(),
                pending_inbound: None,
                inbound_turn_origins: Vec::new(),
                updated_at_ms: unix_timestamp_ms(),
            };
            effects.insert(0, Effect::PersistSessionBridge(record.clone()));
            self.session_bridges.push(record);
        }
        self.sessions_by_id.insert(session_id.clone(), engine);
        Ok(Self::accepted(Some(session_id.to_string()), effects))
    }

    fn set_session_bridge_lifecycle_command(
        &mut self,
        session_id: &SessionId,
        lifecycle: BridgeLifecycle,
    ) -> DomainCommandOutcome {
        let changed = {
            let bridge = self.session_bridge_mut(session_id)?;
            if bridge.lifecycle == lifecycle {
                false
            } else {
                bridge.lifecycle = lifecycle;
                bump_bridge_revision(bridge);
                true
            }
        };
        let mut effects = if changed {
            vec![Effect::PersistSessionBridge(
                self.session_bridge(session_id)?.clone(),
            )]
        } else {
            Vec::new()
        };
        if lifecycle == BridgeLifecycle::Open {
            effects.extend(self.resume_pending_bridge_prompt(session_id)?);
        }
        Ok(Self::accepted(Some(session_id.to_string()), effects))
    }

    fn set_workspace_bridge_lifecycle_command(
        &mut self,
        workspace_id: &WorkspaceId,
        lifecycle: BridgeLifecycle,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let workspace = self.engine().state().workspace.clone();
        let mut effects = Vec::new();
        let mut reopened = Vec::new();
        for bridge in self
            .session_bridges
            .iter_mut()
            .filter(|bridge| bridge.workspace == workspace)
        {
            if bridge.lifecycle != lifecycle {
                bridge.lifecycle = lifecycle;
                bump_bridge_revision(bridge);
                effects.push(Effect::PersistSessionBridge(bridge.clone()));
                if lifecycle == BridgeLifecycle::Open && bridge.pending_inbound.is_some() {
                    reopened.push(SessionId::from(bridge.session_id.clone()));
                }
            }
        }
        for session_id in reopened {
            effects.extend(self.resume_pending_bridge_prompt(&session_id)?);
        }
        Ok(Self::accepted(Some(workspace_id.to_string()), effects))
    }

    fn bind_session_bridge_thread_command(
        &mut self,
        session_id: &SessionId,
        transport: &str,
        external_parent_id: &str,
        external_thread_id: &str,
    ) -> DomainCommandOutcome {
        validate_external_identity("transport", transport, 32)?;
        validate_external_identity("external parent id", external_parent_id, 128)?;
        validate_external_identity("external thread id", external_thread_id, 128)?;
        if self.session_bridges.iter().any(|candidate| {
            candidate.session_id != session_id.as_str()
                && candidate.transport.as_deref() == Some(transport)
                && candidate.external_thread_id.as_deref() == Some(external_thread_id)
        }) {
            return Err(DomainCommandError::Conflict(
                "external thread is already paired with another session".to_owned(),
            ));
        }
        let bridge = self.session_bridge_mut(session_id)?;
        match (
            bridge.transport.as_deref(),
            bridge.external_parent_id.as_deref(),
            bridge.external_thread_id.as_deref(),
        ) {
            (Some(current_transport), Some(current_parent), Some(current_thread))
                if current_transport == transport
                    && current_parent == external_parent_id
                    && current_thread == external_thread_id =>
            {
                return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
            }
            (None, None, None) => {}
            _ => {
                return Err(DomainCommandError::Conflict(
                    "session bridge is already paired with a different external thread".to_owned(),
                ));
            }
        }
        bridge.transport = Some(transport.to_owned());
        bridge.external_parent_id = Some(external_parent_id.to_owned());
        bridge.external_thread_id = Some(external_thread_id.to_owned());
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    fn clear_session_bridge_thread_command(
        &mut self,
        session_id: &SessionId,
        transport: &str,
        external_thread_id: &str,
    ) -> DomainCommandOutcome {
        let bridge = self.session_bridge_mut(session_id)?;
        if bridge.transport.as_deref() != Some(transport)
            || bridge.external_thread_id.as_deref() != Some(external_thread_id)
        {
            return Err(DomainCommandError::Conflict(
                "external thread no longer matches this session bridge".to_owned(),
            ));
        }
        bridge.transport = None;
        bridge.external_parent_id = None;
        bridge.external_thread_id = None;
        bridge.live_turn_id = None;
        bridge.live_external_message_id = None;
        bridge.active_source_message_id = None;
        if let Some(delivery) = &mut bridge.delivery {
            delivery.completed_parts = 0;
            delivery.last_external_message_id = None;
        }
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    fn prepare_bridge_delivery_command(
        &mut self,
        session_id: &SessionId,
        projection_kind: BridgeProjectionKind,
        turn_id: &TurnId,
        expected_last_projected: Option<&BridgeProjectionView>,
        body_sha256: &str,
        part_count: u64,
    ) -> DomainCommandOutcome {
        validate_delivery_plan(body_sha256, part_count)?;
        let target = BridgeProjectionRecord {
            kind: projection_kind,
            turn_id: turn_id.to_string(),
        };
        let expected = expected_last_projected.map(|projection| BridgeProjectionRecord {
            kind: projection.kind,
            turn_id: projection.turn_id.to_string(),
        });
        let bridge = self.session_bridge_mut(session_id)?;
        if bridge.last_projected.as_ref() == Some(&target) {
            return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
        }
        let zero_part_suppression_allowed = part_count == 0
            && projection_kind == BridgeProjectionKind::User
            && bridge.inbound_turn_origins.iter().any(|origin| {
                origin.turn_id == turn_id.as_str()
                    && origin.transport == bridge.transport.as_deref().unwrap_or_default()
            });
        if part_count == 0 && !zero_part_suppression_allowed {
            return Err(DomainCommandError::Invalid(
                "zero-part projection requires trusted inbound user provenance".to_owned(),
            ));
        }
        let delivery = BridgeDeliveryRecord {
            projection_kind,
            turn_id: turn_id.to_string(),
            previous_projection: expected.clone(),
            body_sha256: body_sha256.to_owned(),
            part_count,
            completed_parts: 0,
            last_external_message_id: None,
        };
        if let Some(current) = &bridge.delivery {
            if current.projection_kind == delivery.projection_kind
                && current.turn_id == delivery.turn_id
                && current.previous_projection == delivery.previous_projection
                && current.body_sha256 == delivery.body_sha256
                && current.part_count == delivery.part_count
            {
                return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
            }
            return Err(DomainCommandError::Conflict(
                "another transcript delivery is already pending for this session".to_owned(),
            ));
        }
        if bridge.last_projected != expected {
            return Err(DomainCommandError::Conflict(
                "bridge transcript projection cursor advanced; refresh before delivering"
                    .to_owned(),
            ));
        }
        bridge.delivery = Some(delivery);
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    fn complete_bridge_delivery_part_command(
        &mut self,
        session_id: &SessionId,
        projection_kind: BridgeProjectionKind,
        turn_id: &TurnId,
        part_index: u64,
        external_message_id: &str,
    ) -> DomainCommandOutcome {
        validate_external_identity("external message id", external_message_id, 128)?;
        let bridge = self.session_bridge_mut(session_id)?;
        let delivery = bridge.delivery.as_mut().ok_or_else(|| {
            DomainCommandError::Conflict("no transcript delivery is pending".to_owned())
        })?;
        if delivery.projection_kind != projection_kind || delivery.turn_id != turn_id.as_str() {
            return Err(DomainCommandError::Conflict(
                "pending transcript delivery belongs to another projection".to_owned(),
            ));
        }
        if part_index >= delivery.part_count {
            return Err(DomainCommandError::Invalid(
                "delivery part index exceeds the prepared part count".to_owned(),
            ));
        }
        if part_index < delivery.completed_parts {
            // The constant-size checkpoint can authenticate the most recently completed part. This
            // catches a lost-response retry that somehow resolved the deterministic nonce to a
            // different external-transport message instead of silently accepting a duplicate send.
            if part_index.saturating_add(1) == delivery.completed_parts
                && delivery.last_external_message_id.as_deref() != Some(external_message_id)
            {
                return Err(DomainCommandError::Conflict(
                    "completed delivery part has a different external message identity".to_owned(),
                ));
            }
            return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
        }
        if part_index != delivery.completed_parts {
            return Err(DomainCommandError::Conflict(
                "delivery parts must be checkpointed in order".to_owned(),
            ));
        }
        delivery.completed_parts = delivery.completed_parts.saturating_add(1);
        delivery.last_external_message_id = Some(external_message_id.to_owned());
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    #[cfg(test)]
    fn finalize_bridge_delivery_command(
        &mut self,
        session_id: &SessionId,
        projection_kind: BridgeProjectionKind,
        turn_id: &TurnId,
    ) -> DomainCommandOutcome {
        self.finalize_bridge_delivery_command_with_source(
            session_id,
            projection_kind,
            turn_id,
            None,
        )
    }

    fn finalize_bridge_delivery_command_with_source(
        &mut self,
        session_id: &SessionId,
        projection_kind: BridgeProjectionKind,
        turn_id: &TurnId,
        clear_active_source_message_id: Option<&str>,
    ) -> DomainCommandOutcome {
        if let Some(message_id) = clear_active_source_message_id {
            validate_external_identity("active source message id", message_id, 128)?;
        }
        let target = BridgeProjectionRecord {
            kind: projection_kind,
            turn_id: turn_id.to_string(),
        };
        let bridge = self.session_bridge_mut(session_id)?;
        if bridge.last_projected.as_ref() == Some(&target) {
            return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
        }
        let delivery = bridge.delivery.as_ref().ok_or_else(|| {
            DomainCommandError::Conflict("no transcript delivery is pending".to_owned())
        })?;
        if delivery.projection_kind != projection_kind || delivery.turn_id != turn_id.as_str() {
            return Err(DomainCommandError::Conflict(
                "pending transcript delivery belongs to another projection".to_owned(),
            ));
        }
        if bridge.last_projected != delivery.previous_projection {
            return Err(DomainCommandError::Conflict(
                "bridge transcript projection cursor changed during delivery".to_owned(),
            ));
        }
        if delivery.completed_parts != delivery.part_count {
            return Err(DomainCommandError::Conflict(
                "transcript delivery still has unsent parts".to_owned(),
            ));
        }
        bridge.last_projected = Some(target);
        bridge.delivery = None;
        if matches!(
            projection_kind,
            BridgeProjectionKind::User | BridgeProjectionKind::Assistant
        ) {
            bridge
                .inbound_turn_origins
                .retain(|origin| origin.turn_id != turn_id.as_str());
        }
        if projection_kind == BridgeProjectionKind::Assistant {
            if bridge.live_turn_id.as_deref() == Some(turn_id.as_str()) {
                bridge.live_turn_id = None;
                bridge.live_external_message_id = None;
            }
            if clear_active_source_message_id.is_some()
                && bridge.active_source_message_id.as_deref() == clear_active_source_message_id
            {
                bridge.active_source_message_id = None;
            }
        }
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    fn set_bridge_live_message_command(
        &mut self,
        session_id: &SessionId,
        turn_id: Option<&TurnId>,
        external_message_id: Option<&str>,
        clear_active_source_message_id: Option<&str>,
    ) -> DomainCommandOutcome {
        if turn_id.is_some() != external_message_id.is_some() {
            return Err(DomainCommandError::Invalid(
                "live turn and external message identities must be set or cleared together"
                    .to_owned(),
            ));
        }
        if let Some(message_id) = external_message_id {
            validate_external_identity("external message id", message_id, 128)?;
        }
        if let Some(message_id) = clear_active_source_message_id {
            validate_external_identity("active source message id", message_id, 128)?;
        }
        let bridge = self.session_bridge_mut(session_id)?;
        if let Some(expected) = clear_active_source_message_id
            && bridge
                .active_source_message_id
                .as_deref()
                .is_some_and(|current| current != expected)
        {
            return Err(DomainCommandError::Conflict(
                "bridge source-message owner changed before terminal cleanup".to_owned(),
            ));
        }
        let next_turn = turn_id.map(ToString::to_string);
        let next_message = external_message_id.map(ToOwned::to_owned);
        let clear_source = clear_active_source_message_id.is_some()
            && bridge.active_source_message_id.as_deref() == clear_active_source_message_id;
        if bridge.live_turn_id == next_turn
            && bridge.live_external_message_id == next_message
            && !clear_source
        {
            return Ok(Self::accepted(Some(session_id.to_string()), Vec::new()));
        }
        bridge.live_turn_id = next_turn;
        bridge.live_external_message_id = next_message;
        if clear_source {
            bridge.active_source_message_id = None;
        }
        bump_bridge_revision(bridge);
        let effect = Effect::PersistSessionBridge(bridge.clone());
        Ok(Self::accepted(Some(session_id.to_string()), vec![effect]))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    // These fields are the complete authenticated external-event identity plus the typed prompt;
    // preserving them as distinct arguments makes accidental route or dedup-key swaps visible. The
    // route check, replay disposition, readiness decision, and durable checkpoint stay together so
    // no accepted or busy branch can bypass the same atomic command boundary.
    fn continue_session_from_bridge_command(
        &mut self,
        session_id: &SessionId,
        transport: &str,
        external_thread_id: &str,
        external_event_id: &str,
        source_message_id: &str,
        prompt: PromptInput,
        consume_as_busy: bool,
    ) -> DomainCommandOutcome {
        validate_external_identity("transport", transport, 32)?;
        validate_external_identity("external thread id", external_thread_id, 128)?;
        validate_external_identity("external event id", external_event_id, 128)?;
        validate_external_identity("source message id", source_message_id, 128)?;
        let bridge = self.session_bridge(session_id)?;
        if bridge.lifecycle != BridgeLifecycle::Open
            || bridge.transport.as_deref() != Some(transport)
            || bridge.external_thread_id.as_deref() != Some(external_thread_id)
        {
            return Err(DomainCommandError::Conflict(
                "message does not belong to an open bound session bridge".to_owned(),
            ));
        }
        if bridge
            .recent_inbound_event_ids
            .iter()
            .any(|event_id| event_id == external_event_id)
        {
            let replayed = self
                .inbound_event_dispositions
                .get(&(session_id.clone(), external_event_id.to_owned()))
                .copied()
                .filter(|disposition| {
                    matches!(
                        disposition,
                        BridgeContinuationDisposition::Accepted
                            | BridgeContinuationDisposition::Busy
                    )
                });
            let replayed_source_active = (replayed
                == Some(BridgeContinuationDisposition::Accepted))
            .then(|| bridge.active_source_message_id.as_deref() == Some(source_message_id));
            let (mut accepted, effects) = Self::accepted_bridge_continuation(
                session_id,
                BridgeContinuationDisposition::Duplicate,
                Vec::new(),
            );
            accepted.replayed_bridge_continuation = replayed;
            accepted.replayed_bridge_source_active = replayed_source_active;
            return Ok((accepted, effects));
        }
        if consume_as_busy {
            self.remember_bridge_inbound_event(
                session_id,
                external_event_id,
                BridgeContinuationDisposition::Busy,
            );
            let bridge = self.session_bridge_mut(session_id)?;
            bump_bridge_revision(bridge);
            let effect = Effect::PersistSessionBridge(bridge.clone());
            return Ok(Self::accepted_bridge_continuation(
                session_id,
                BridgeContinuationDisposition::Busy,
                vec![effect],
            ));
        }

        self.reload_agent_catalogue_for_session(session_id)?;
        let (text, attachments) = self.convert_prompt(session_id, prompt)?;
        let client_prompt_id = bridge_prompt_id(external_event_id);
        let pending = BridgePendingInboundRecord {
            external_event_id: external_event_id.to_owned(),
            source_message_id: source_message_id.to_owned(),
            client_prompt_id: client_prompt_id.clone(),
            text: text.clone(),
            attachments: attachments.clone(),
        };
        match self
            .session_engine_mut(session_id)?
            .state_mut()
            .submit_prompt_with_id_and_source(
                client_prompt_id,
                text,
                attachments,
                Some(transport.to_owned()),
            ) {
            Ok(mut effects) => {
                self.remember_bridge_inbound_event(
                    session_id,
                    external_event_id,
                    BridgeContinuationDisposition::Accepted,
                );
                let bridge = self.session_bridge_mut(session_id)?;
                bridge.active_source_message_id = Some(source_message_id.to_owned());
                bridge.pending_inbound = Some(pending);
                bump_bridge_revision(bridge);
                effects.insert(0, Effect::PersistSessionBridge(bridge.clone()));
                Ok(Self::accepted_bridge_continuation(
                    session_id,
                    BridgeContinuationDisposition::Accepted,
                    effects,
                ))
            }
            Err(DomainCommandError::Conflict(_)) => {
                // A busy/not-ready message is consumed durably rather than queued. Replayed gateway
                // events can therefore never turn into a later prompt after the active turn ends.
                self.remember_bridge_inbound_event(
                    session_id,
                    external_event_id,
                    BridgeContinuationDisposition::Busy,
                );
                let bridge = self.session_bridge_mut(session_id)?;
                bump_bridge_revision(bridge);
                let effect = Effect::PersistSessionBridge(bridge.clone());
                Ok(Self::accepted_bridge_continuation(
                    session_id,
                    BridgeContinuationDisposition::Busy,
                    vec![effect],
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Replays one durable accepted bridge prompt after restart using its original provider client
    /// id. Busy/not-ready states leave the inbox item in place for a later backend event.
    pub(crate) fn resume_pending_bridge_prompt(
        &mut self,
        session_id: &SessionId,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        let (pending, source_transport) = {
            let bridge = self.session_bridge(session_id)?;
            if bridge.lifecycle != BridgeLifecycle::Open {
                return Ok(Vec::new());
            }
            let Some(pending) = bridge.pending_inbound.clone() else {
                return Ok(Vec::new());
            };
            (pending, bridge.transport.clone())
        };
        self.reload_agent_catalogue_for_session(session_id)?;
        match self
            .session_engine_mut(session_id)?
            .state_mut()
            .submit_prompt_with_id_and_source(
                pending.client_prompt_id,
                pending.text,
                pending.attachments,
                source_transport,
            ) {
            Ok(effects) => Ok(effects),
            Err(DomainCommandError::Conflict(_)) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// Resolves a provider acceptance to the durable client prompt id. Some provider protocols
    /// return a new provider turn id even though Nakode supplied a stable client-message id.
    #[must_use]
    pub(crate) fn bridge_prompt_acknowledgement_id(
        &self,
        session_id: &SessionId,
        provider_turn_id: &str,
        accepted: bool,
    ) -> Option<String> {
        let pending = self
            .session_bridge(session_id)
            .ok()?
            .pending_inbound
            .as_ref()?;
        if pending.client_prompt_id == provider_turn_id
            || (accepted
                && self
                    .sessions_by_id
                    .get(session_id)
                    .and_then(|engine| engine.state().starting_prompt_id())
                    == Some(pending.client_prompt_id.as_str()))
        {
            Some(pending.client_prompt_id.clone())
        } else {
            None
        }
    }

    pub(crate) fn reapply_bridge_turn_origins(&mut self, session_id: &SessionId) {
        let origins = self
            .session_bridges
            .iter()
            .find(|bridge| bridge.session_id == session_id.as_str())
            .map(|bridge| bridge.inbound_turn_origins.clone())
            .unwrap_or_default();
        if let Some(engine) = self.sessions_by_id.get_mut(session_id) {
            for origin in origins {
                engine
                    .state_mut()
                    .set_user_source_transport_for_turn(&origin.turn_id, &origin.transport);
            }
        }
    }

    /// Persists trusted source provenance once the provider assigns the logical turn identity.
    /// The transcript is the source of truth during the live process; this cursor-pruned bridge
    /// record restores the association after history rebuilds. A non-transport turn also clears
    /// stale inbound reaction ownership left by a failed prior transport turn.
    pub(crate) fn record_bridge_turn_origin(
        &mut self,
        session_id: &SessionId,
        provider_turn_id: &str,
    ) -> Option<Effect> {
        let source_transport = self
            .sessions_by_id
            .get(session_id)?
            .state()
            .user_source_transport_for_turn(provider_turn_id)
            .map(ToOwned::to_owned);
        let bridge = self
            .session_bridges
            .iter_mut()
            .find(|bridge| bridge.session_id == session_id.as_str())?;
        let mut changed = false;
        if let Some(source_transport) = source_transport.as_deref()
            && !bridge.inbound_turn_origins.iter().any(|origin| {
                origin.turn_id == provider_turn_id && origin.transport == source_transport
            })
        {
            bridge
                .inbound_turn_origins
                .push(BridgeInboundTurnOriginRecord {
                    turn_id: provider_turn_id.to_owned(),
                    transport: source_transport.to_owned(),
                });
            changed = true;
        }
        if source_transport.as_deref() != bridge.transport.as_deref()
            && bridge.active_source_message_id.take().is_some()
        {
            changed = true;
        }
        if !changed {
            return None;
        }
        bump_bridge_revision(bridge);
        Some(Effect::PersistSessionBridge(bridge.clone()))
    }

    /// Clears the durable inbox only after a backend acknowledges the stable prompt client id.
    pub(crate) fn acknowledge_bridge_prompt(
        &mut self,
        session_id: &SessionId,
        client_prompt_id: &str,
    ) -> Option<Effect> {
        let bridge = self
            .session_bridges
            .iter_mut()
            .find(|bridge| bridge.session_id == session_id.as_str())?;
        if bridge
            .pending_inbound
            .as_ref()
            .is_none_or(|pending| pending.client_prompt_id != client_prompt_id)
        {
            return None;
        }
        bridge.pending_inbound = None;
        bump_bridge_revision(bridge);
        Some(Effect::PersistSessionBridge(bridge.clone()))
    }

    #[cfg(test)]
    fn open_session_command(
        &mut self,
        session_id: &SessionId,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
    ) -> DomainCommandOutcome {
        self.open_session_command_with_mcp(session_id, tools, None)
    }

    #[cfg(test)]
    fn open_session_command_with_mcp(
        &mut self,
        session_id: &SessionId,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        mcp_grant: Option<&McpSessionGrant>,
    ) -> DomainCommandOutcome {
        self.open_session_command_with_mcp_and_profile(session_id, tools, mcp_grant, None, &[])
    }

    #[allow(clippy::too_many_lines)]
    fn open_session_command_with_mcp_and_profile(
        &mut self,
        session_id: &SessionId,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        mcp_grant: Option<&McpSessionGrant>,
        profile_id: Option<String>,
        enabled_skill_ids: &[String],
    ) -> DomainCommandOutcome {
        let loaded = self
            .sessions_by_id
            .keys()
            .filter(|loaded| loaded.as_str().starts_with(session_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        match loaded.as_slice() {
            [loaded] => {
                let (loaded_workspace, loaded_working_directory) = {
                    let state = self.session_engine_mut(loaded)?.state();
                    (state.workspace.clone(), state.working_directory.clone())
                };
                canonical_working_directory(Some(&loaded_working_directory), &loaded_workspace)?;
                if *loaded == self.default_session
                    && !self
                        .sessions
                        .iter()
                        .any(|record| record.id == loaded.as_str())
                {
                    return Err(DomainCommandError::NotFound(session_id.to_string()));
                }
                if let Some(tools) = tools {
                    let provider = self
                        .session_engine_mut(loaded)?
                        .state()
                        .active_provider_id()
                        .to_owned();
                    let tools = self
                        .session_engine_mut(loaded)?
                        .state()
                        .reconcile_available_builtin_tools(&provider, tools);
                    self.session_engine_mut(loaded)?
                        .state_mut()
                        .configure_or_validate_external_tools(
                            &tools.tools,
                            tools.replace_builtin_tools,
                            tools.allowed_builtin_tools.as_deref(),
                        )?;
                }
                if let Some(profile_id) = profile_id {
                    let state = self.session_engine_mut(loaded)?.state_mut();
                    let skills = state.skill_catalogue().only_ids(enabled_skill_ids);
                    state.set_skill_profile(Some(profile_id));
                    state.install_skill_snapshot(skills, Some(enabled_skill_ids));
                }
                // Opening an attached session is a reattachment, not a tool reconfiguration. The
                // runtime already owns its installed MCP tools, so a caller's current grant must
                // not make an otherwise valid reattach fail or mutate the active session.
                return Ok(Self::accepted(Some(loaded.to_string()), Vec::new()));
            }
            [_, ..] => {
                return Err(DomainCommandError::Conflict(format!(
                    "session prefix {session_id} is ambiguous"
                )));
            }
            [] => {}
        }
        let matches = self
            .sessions
            .iter()
            .filter(|session| session.id.starts_with(session_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let session = match matches.as_slice() {
            [session] => session.clone(),
            [] => return Err(DomainCommandError::NotFound(session_id.to_string())),
            _ => {
                return Err(DomainCommandError::Conflict(format!(
                    "session prefix {session_id} is ambiguous"
                )));
            }
        };
        let working_directory =
            canonical_working_directory(Some(&session.working_directory), &session.workspace)?;
        self.refresh_session_template_addenda()?;
        let authoritative_ids = profile_id
            .as_ref()
            .map(|_| enabled_skill_ids)
            .or(session.enabled_skill_ids.as_deref());
        let skills = authoritative_ids.map_or_else(
            || self.session_template.skill_catalogue(),
            |ids| self.session_template.skill_catalogue().only_ids(ids),
        );
        let mut engine = ServiceEngine::new(self.session_template.clone());
        engine.state_mut().set_working_directory(working_directory);
        engine.state_mut().set_skill_profile(profile_id);
        engine
            .state_mut()
            .install_skill_snapshot(skills, authoritative_ids);
        // The workspace template may still carry the bootstrap provider/session identity. Reset that
        // clone before installing client-owned tools; restoration begins only after validation.
        let _discarded_template_effects = engine.state_mut().create_logical_session()?;
        if let Some(tools) = tools {
            let tools = engine
                .state()
                .reconcile_available_builtin_tools(&session.provider, tools);
            Self::validate_provider_tool_projection(&session.provider, &tools)?;
            engine.state_mut().configure_session_tools(
                tools.tools,
                tools.replace_builtin_tools,
                tools.allowed_builtin_tools,
            )?;
        }
        if let Some(grant) = mcp_grant {
            let (mcp_tools, archetype_grants) = self.mcp_tools_for_grant(grant)?;
            engine.state_mut().configure_mcp_tools(mcp_tools)?;
            engine
                .state_mut()
                .configure_mcp_archetype_grants(archetype_grants);
        }
        let mut effects = engine.state_mut().begin_resume(session.clone());
        Self::prepend_resume_hydration_effects(&session, &engine, &mut effects);
        let loaded_id = SessionId::from(session.id.clone());
        self.sessions_by_id.insert(loaded_id.clone(), engine);
        Ok(Self::accepted(Some(session.id), effects))
    }

    fn prepend_resume_hydration_effects(
        session: &SessionRecord,
        engine: &ServiceEngine,
        effects: &mut Vec<Effect>,
    ) {
        if effects.is_empty() {
            return;
        }
        // Hydrate persisted children as soon as an accepted resume begins so clients can inspect
        // terminal evidence without waiting for the provider handshake. A rejected resume must
        // not install child state into an engine that has no logical session identity.
        effects.insert(0, Effect::LoadSubagents(session.id.clone()));
        Self::persist_legacy_skill_snapshot(session, engine, effects);
    }

    fn persist_legacy_skill_snapshot(
        session: &SessionRecord,
        engine: &ServiceEngine,
        effects: &mut Vec<Effect>,
    ) {
        if session.enabled_skill_ids.is_none() {
            effects.insert(
                0,
                Effect::PersistSessionSkillSnapshot {
                    session_id: session.id.clone(),
                    enabled_skill_ids: engine.state().enabled_skill_ids(),
                },
            );
        }
    }

    fn validate_provider_tool_projection(
        provider: &str,
        tools: &nakode_protocol::SessionToolConfiguration,
    ) -> Result<(), DomainCommandError> {
        let Some(allowed) = tools.allowed_builtin_tools.as_deref() else {
            return Ok(());
        };
        let projection = crate::backend::project_provider_tools(provider, Some(allowed));
        if projection.unsupported_canonical_tools.is_empty() {
            Ok(())
        } else {
            Err(DomainCommandError::Invalid(format!(
                "provider {provider} cannot project allowed builtin tools: {}",
                projection.unsupported_canonical_tools.join(", ")
            )))
        }
    }

    fn select_model_command(
        &mut self,
        target: &ModelTarget,
        model_id: &nakode_protocol::ModelId,
        options: &nakode_protocol::ModelOptions,
    ) -> DomainCommandOutcome {
        let session_id = match target {
            ModelTarget::Session { session_id } => {
                self.ensure_session(session_id)?;
                session_id.clone()
            }
            ModelTarget::AgentSession { agent_session_id } => {
                self.session_for_agent_session(agent_session_id)?
            }
            ModelTarget::ProviderDefault { .. } | ModelTarget::Vision => {
                self.default_session.clone()
            }
        };
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .select_model_intent(target, model_id, options)?;
        Ok(Self::accepted(Some(model_id.to_string()), effects))
    }

    fn update_settings_command(
        &mut self,
        patch: &nakode_protocol::SettingsPatch,
    ) -> DomainCommandOutcome {
        let session_id = self.default_session.clone();
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .update_settings_intent(patch)?;
        Ok(Self::accepted(None, effects))
    }

    fn prompt_command(
        &mut self,
        session_id: &SessionId,
        prompt: PromptInput,
        enqueue: bool,
        prompt_id: Option<&str>,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        self.reload_agent_catalogue_for_session(session_id)?;
        let (text, attachments) = self.convert_prompt(session_id, prompt)?;
        let accepted_prompt_id = prompt_id.map_or_else(
            || format!("nakode-msg-{}", uuid::Uuid::now_v7()),
            str::to_owned,
        );
        let state = self.session_engine_mut(session_id)?.state_mut();
        let effects = if enqueue {
            state.enqueue_prompt_with_id(accepted_prompt_id.clone(), text, attachments)?
        } else {
            state.submit_prompt_with_id(accepted_prompt_id.clone(), text, attachments)?
        };
        Ok(Self::accepted(Some(accepted_prompt_id), effects))
    }

    fn remove_queued_prompt_command(
        &mut self,
        session_id: &SessionId,
        prompt_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self
            .session_engine_mut(session_id)?
            .state_mut()
            .remove_queued_prompt(prompt_id)?;
        Ok(Self::accepted(None, effects))
    }

    fn steer_queued_prompt_command(
        &mut self,
        session_id: &SessionId,
        prompt_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self
            .session_engine_mut(session_id)?
            .state_mut()
            .steer_queued_prompt(prompt_id)?;
        Ok(Self::accepted(None, effects))
    }

    fn steer_turn_command(
        &mut self,
        turn_id: &nakode_protocol::TurnId,
        text: &str,
    ) -> DomainCommandOutcome {
        let (session_id, provider_turn_id) = self.provider_turn_id(turn_id)?;
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .steer_turn(&provider_turn_id, text)?;
        Ok(Self::accepted(None, effects))
    }

    fn cancel_turn_command(&mut self, turn_id: &nakode_protocol::TurnId) -> DomainCommandOutcome {
        let (session_id, provider_turn_id) = self.provider_turn_id(turn_id)?;
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .cancel_turn(&provider_turn_id)?;
        Ok(Self::accepted(None, effects))
    }

    fn cancel_session_work_command(&mut self, session_id: &SessionId) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self
            .session_engine_mut(session_id)?
            .state_mut()
            .cancel_session_work()?;
        Ok(Self::accepted(Some(session_id.to_string()), effects))
    }

    /// Resolves the canonical identity used by both live teardown and durable deletion.
    ///
    /// Exact IDs win, matching repository lookup semantics. Otherwise every loaded engine, persisted
    /// record, and retained bridge participates in unique-prefix resolution so no layer can delete a
    /// different logical session from the one the runtime releases.
    fn canonical_delete_session_id(
        &self,
        requested: &SessionId,
    ) -> Result<SessionId, DomainCommandError> {
        let requested_id = requested.as_str();
        let has_exact = self.sessions_by_id.contains_key(requested)
            || self.sessions.iter().any(|record| record.id == requested_id)
            || self
                .session_bridges
                .iter()
                .any(|bridge| bridge.session_id == requested_id);
        if has_exact {
            return Ok(requested.clone());
        }
        let mut matches = Vec::new();
        for candidate in self
            .sessions_by_id
            .keys()
            .map(SessionId::as_str)
            .chain(self.sessions.iter().map(|record| record.id.as_str()))
            .chain(
                self.session_bridges
                    .iter()
                    .map(|bridge| bridge.session_id.as_str()),
            )
            .filter(|candidate| candidate.starts_with(requested_id))
        {
            if !matches.iter().any(|existing| existing == candidate) {
                matches.push(candidate.to_owned());
            }
        }
        match matches.as_slice() {
            [canonical] => Ok(SessionId::from(canonical.clone())),
            [_, ..] => Err(DomainCommandError::Conflict(format!(
                "session prefix {requested} is ambiguous"
            ))),
            [] => Ok(requested.clone()),
        }
    }

    /// Deletes one logical session and releases all runtime state attached to it.
    ///
    /// Unlike other session commands, deletion does not require an attached engine; unattached and
    /// already-missing IDs are accepted so retries can converge. Attached sessions are evicted here,
    /// but live work remains a conflict that callers must first stop with `CancelSessionWork`.
    /// A unique prefix is resolved before mutation so runtime teardown and durable deletion use the
    /// same canonical identity. Deleting a closed default session installs its successor first.
    fn delete_session_command(&mut self, session_id: &SessionId) -> DomainCommandOutcome {
        let session_id = self.canonical_delete_session_id(session_id)?;
        let lifecycle = self.sessions_by_id.get(&session_id).map(|engine| {
            let state = engine.state();
            (
                state.is_busy() && state.provider_is_live(),
                state.provider_is_live() && state.provider_session_id.is_some(),
            )
        });
        let mut effects = Vec::new();
        if let Some((working, owns_live_provider_session)) = lifecycle {
            if working {
                return Err(DomainCommandError::Conflict(format!(
                    "session {session_id} has work in flight; cancel it before deleting it"
                )));
            }
            if session_id == self.default_session && owns_live_provider_session {
                return Err(DomainCommandError::Conflict(format!(
                    "session {session_id} is this workspace's active initial session and cannot be deleted"
                )));
            }
            effects.push(Effect::ReleaseSessionBackends(session_id.to_string()));
            if session_id == self.default_session {
                self.replace_default_session()?;
            }
            self.release_session(&session_id);
        }
        if let Some(bridge) = self
            .session_bridges
            .iter_mut()
            .find(|bridge| bridge.session_id == session_id.as_str())
            && bridge.lifecycle != BridgeLifecycle::Archived
        {
            bridge.lifecycle = BridgeLifecycle::Archived;
            bump_bridge_revision(bridge);
        }
        // Durable deletion archives the bridge and removes session-owned rows in one repository
        // transaction. Keeping those writes together prevents a failed delete from leaving the
        // canonical bridge archived while the live session is restored for retry.
        effects.push(Effect::DeleteSession(session_id.to_string()));
        Ok(Self::accepted(Some(session_id.to_string()), effects))
    }

    /// Installs the successor for a closed initial session before its engine is removed.
    fn replace_default_session(&mut self) -> Result<(), DomainCommandError> {
        self.refresh_session_template_addenda()?;
        let mut successor = ServiceEngine::new(self.session_template.clone());
        // `new` clones workspace configuration only in production, but tests and legacy callers may
        // provide a template carrying an old native id. `create_logical_session` clears it; any
        // unsubscribe effect belongs to that template snapshot, not to the fresh successor.
        let _discarded_template_effects = successor.state_mut().create_logical_session()?;
        let successor_id = SessionId::from(successor.state().nakode_session_id.clone());
        self.sessions_by_id.insert(successor_id.clone(), successor);
        self.default_session = successor_id;
        Ok(())
    }

    /// Drops one session's in-memory engine and the projection kept for its subscribers.
    ///
    /// The only eviction path there is. `sessions_by_id` and `published_sessions` were insert-only,
    /// so both grew with every session for the life of the process, each retaining a whole
    /// `DomainState` — transcript, entries, subagents and all. Deletion is what frees them.
    fn release_session(&mut self, session_id: &SessionId) {
        debug_assert_ne!(
            session_id, &self.default_session,
            "the default session runtime always exists"
        );
        self.sessions_by_id.remove(session_id);
        self.published_sessions.remove(session_id);
    }

    fn compact_context_command(
        &mut self,
        agent_session_id: &AgentSessionId,
    ) -> DomainCommandOutcome {
        let session_id = self.session_for_agent_session(agent_session_id)?;
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .compact_context()?;
        Ok(Self::accepted(None, effects))
    }

    fn resolve_interaction_command(
        &mut self,
        interaction_id: &nakode_protocol::InteractionId,
        resolution: &nakode_protocol::InteractionResolution,
    ) -> DomainCommandOutcome {
        let session_id = self.session_for_interaction(interaction_id)?;
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .resolve_interaction(interaction_id, resolution)?;
        Ok(Self::accepted(None, effects))
    }

    #[allow(clippy::too_many_arguments)]
    // This internal boundary keeps session, request, parent, and invocation identities explicit;
    // collapsing them into UI-specific state would obscure the public server ownership contract.
    pub(crate) fn delegate_agent_attributed(
        &mut self,
        session_id: &SessionId,
        agent_slug: &str,
        task: &str,
        parent_run_id: Option<&str>,
        request_id: u64,
        invocation_turn_id: Option<&str>,
        invocation_call_id: Option<&str>,
    ) -> Result<(String, Vec<Effect>), DomainCommandError> {
        self.ensure_session(session_id)?;
        if task.trim().is_empty() {
            return Err(DomainCommandError::Invalid(
                "agent delegation requires a non-empty task".to_owned(),
            ));
        }
        self.reload_agent_catalogue_for_session(session_id)?;
        self.session_engine_mut(session_id)?
            .state_mut()
            .delegate_agent_attributed_for_request(
                agent_slug,
                task,
                parent_run_id,
                request_id,
                invocation_turn_id,
                invocation_call_id,
            )
    }

    fn delegate_command(
        &mut self,
        session_id: &SessionId,
        agent_slug: &str,
        task: &str,
        parent_run_id: Option<&RunId>,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        if task.trim().is_empty() {
            return Err(DomainCommandError::Invalid(
                "agent delegation requires a non-empty task".to_owned(),
            ));
        }
        // Agent archetypes are global files shared by independently running workspace services.
        // Re-read at the invocation boundary so a service that was already running observes edits
        // made through another workspace without requiring a restart.
        self.reload_agent_catalogue_for_session(session_id)?;
        let (run_id, effects) = self
            .session_engine_mut(session_id)?
            .state_mut()
            .delegate_agent_attributed(
                agent_slug,
                task,
                parent_run_id.map(nakode_protocol::RunId::as_str),
            )?;
        Ok(Self::accepted(Some(run_id), effects))
    }

    pub(crate) fn cancel_attributed_run(
        &mut self,
        session_id: &SessionId,
        run_id: &str,
    ) -> Result<Vec<Effect>, DomainCommandError> {
        self.ensure_session(session_id)?;
        self.session_engine_mut(session_id)?
            .state_mut()
            .cancel_run(run_id)
    }

    fn cancel_run_command(&mut self, run_id: &RunId) -> DomainCommandOutcome {
        let session_id = self.session_for_run(run_id)?;
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .cancel_run(run_id.as_str())?;
        Ok(Self::accepted(Some(run_id.to_string()), effects))
    }

    fn continue_run_command(
        &mut self,
        run_id: &RunId,
        additional_turns: u32,
    ) -> DomainCommandOutcome {
        let session_id = self.session_for_run(run_id)?;
        let (successor_run_id, effects) = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .continue_subagent(run_id.as_str(), additional_turns)?;
        Ok(Self::accepted(Some(successor_run_id), effects))
    }

    fn run_shell_command(
        &mut self,
        session_id: &SessionId,
        command: String,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self
            .session_engine_mut(session_id)?
            .state_mut()
            .run_shell_command(command)?;
        Ok(Self::accepted(None, effects))
    }

    fn mcp_tools_for_grant(
        &self,
        grant: &McpSessionGrant,
    ) -> Result<McpInstalledGrant, DomainCommandError> {
        crate::mcp::validate_grant(grant, &self.mcp_servers)
            .map(|servers| {
                let mut archetype_grants = HashMap::new();
                let tools = servers
                    .into_iter()
                    .flat_map(|server| {
                        server
                            .tools
                            .into_iter()
                            .filter(|tool| !tool.app_only)
                            .map(move |tool| (tool, server.grants.archetype_slugs.clone()))
                    })
                    .map(|(tool, slugs)| {
                        archetype_grants
                            .insert(tool.exposed_name.clone(), slugs.into_iter().collect());
                        tool.external_definition()
                    })
                    .collect();
                (tools, archetype_grants)
            })
            .map_err(|error| DomainCommandError::Invalid(error.to_string()))
    }

    fn save_mcp_server_command(
        &self,
        workspace_id: &WorkspaceId,
        server: McpServerInput,
        grants: McpGrantPolicy,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let server = crate::mcp::input_record(workspace_id, server, grants);
        crate::mcp::validate_server(&server)
            .map_err(|error| DomainCommandError::Invalid(error.to_string()))?;
        Ok(Self::accepted(
            Some(server.id.clone()),
            vec![Effect::SaveMcpServer(server)],
        ))
    }

    fn delete_mcp_server_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        self.ensure_mcp_server(server_id)?;
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::DeleteMcpServer {
                workspace: workspace_id.to_string(),
                server_id: server_id.to_owned(),
            }],
        ))
    }

    fn set_mcp_server_enabled_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
        enabled: bool,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let mut server = self.mcp_server(server_id)?.clone();
        server.enabled = enabled;
        (if enabled { "saved" } else { "disabled" }).clone_into(&mut server.health);
        server.updated_at_ms = crate::mcp::unix_time_ms();
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::SaveMcpServer(server)],
        ))
    }

    fn refresh_mcp_server_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let server = self.mcp_server(server_id)?.clone();
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::RefreshMcpServer(server)],
        ))
    }

    fn set_mcp_server_credential_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
        kind: String,
        credential: CredentialInput,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let server = self.mcp_server(server_id)?;
        if kind != server.auth_kind {
            return Err(DomainCommandError::Invalid(format!(
                "MCP server {server_id:?} requires credential kind {:?}",
                server.auth_kind
            )));
        }
        if credential.0.trim().is_empty() {
            return Err(DomainCommandError::Invalid(
                "MCP credential cannot be empty".to_owned(),
            ));
        }
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::SaveMcpCredential {
                workspace: workspace_id.to_string(),
                server_id: server_id.to_owned(),
                kind,
                secret: credential.0,
            }],
        ))
    }

    fn clear_mcp_server_credential_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        self.ensure_mcp_server(server_id)?;
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::ClearMcpCredential {
                workspace: workspace_id.to_string(),
                server_id: server_id.to_owned(),
            }],
        ))
    }

    fn set_mcp_server_grants_command(
        &self,
        workspace_id: &WorkspaceId,
        server_id: &str,
        grants: McpGrantPolicy,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let mut server = self.mcp_server(server_id)?.clone();
        server.grants = grants;
        server.updated_at_ms = crate::mcp::unix_time_ms();
        Ok(Self::accepted(
            Some(server_id.to_owned()),
            vec![Effect::SaveMcpServer(server)],
        ))
    }

    fn mcp_server(
        &self,
        server_id: &str,
    ) -> Result<&crate::mcp::McpServerRecord, DomainCommandError> {
        self.mcp_servers
            .iter()
            .find(|server| server.id == server_id)
            .ok_or_else(|| DomainCommandError::NotFound(format!("MCP server {server_id}")))
    }

    fn ensure_mcp_server(&self, server_id: &str) -> Result<(), DomainCommandError> {
        self.mcp_server(server_id).map(|_| ())
    }

    fn set_provider_model_filter_command(
        &self,
        provider_id: &ProviderId,
        enabled: bool,
        selected_model_ids: Vec<nakode_protocol::ModelId>,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        let prefix = format!("{provider_id}/");
        let mut selected_model_ids = selected_model_ids
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        if let Some(id) = selected_model_ids
            .iter()
            .find(|id| !id.starts_with(&prefix) || id.len() == prefix.len())
        {
            return Err(DomainCommandError::Invalid(format!(
                "model filter entry {id:?} must be an exact {provider_id}/model ID"
            )));
        }
        selected_model_ids.sort();
        selected_model_ids.dedup();
        Ok(Self::accepted(
            Some(provider_id.to_string()),
            vec![Effect::SetProviderModelFilter {
                provider: provider_id.to_string(),
                enabled,
                selected_model_ids,
            }],
        ))
    }

    fn set_provider_enabled_command(
        &self,
        provider_id: &ProviderId,
        enabled: bool,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(Self::accepted(
            Some(provider_id.to_string()),
            vec![Effect::SetProviderEnabled {
                provider: provider_id.to_string(),
                enabled,
            }],
        ))
    }

    fn begin_provider_authentication_command(
        &mut self,
        provider_id: &ProviderId,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        let display_name = self
            .providers
            .iter()
            .find(|provider| provider.provider == provider_id.as_str())
            .map_or_else(
                || provider_id.to_string(),
                |provider| provider.display_name.clone(),
            );
        let session_id = self.default_session.clone();
        let effects = self
            .session_engine_mut(&session_id)?
            .state_mut()
            .begin_provider_authentication(provider_id.as_str(), &display_name);
        Ok(Self::accepted(Some(provider_id.to_string()), effects))
    }

    fn set_provider_credential_command(
        &self,
        provider_id: &ProviderId,
        kind: String,
        credential: &CredentialInput,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(Self::accepted(
            Some(provider_id.to_string()),
            vec![Effect::SaveProviderCredential {
                provider: provider_id.to_string(),
                kind,
                metadata: serde_json::json!({ "api_key": credential.0.clone() }),
            }],
        ))
    }

    fn clear_provider_credential_command(&self, provider_id: &ProviderId) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(Self::accepted(
            Some(provider_id.to_string()),
            vec![Effect::ClearProviderCredential(provider_id.to_string())],
        ))
    }

    fn reload_provider_command(&self, provider_id: &ProviderId) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        let enabled = self
            .providers
            .iter()
            .find(|provider| provider.provider == provider_id.as_str())
            .is_some_and(|provider| provider.enabled);
        let effect = if enabled {
            Effect::ReloadProvider(provider_id.to_string())
        } else {
            Effect::SetProviderEnabled {
                provider: provider_id.to_string(),
                enabled: true,
            }
        };
        Ok(Self::accepted(Some(provider_id.to_string()), vec![effect]))
    }

    fn save_agent_command(
        &self,
        workspace_id: &WorkspaceId,
        definition: AgentDefinitionInput,
        previous_slug: Option<String>,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let slug = definition.slug.clone();
        let definition = AgentDefinition {
            id: String::new(),
            slug: definition.slug,
            description: definition.description,
            system_prompt: definition.system_prompt,
            first_message: definition.first_message,
            model: definition.model.map(|model| model.to_string()),
            fallback_models: definition
                .fallback_models
                .into_iter()
                .map(|model| model.to_string())
                .collect(),
            fast_mode: definition.fast_mode,
            reasoning_effort: definition.reasoning_effort,
            ownership: match definition.ownership.as_str() {
                "" | "owner_defined" => AgentOwnership::OwnerDefined,
                "built_in" => AgentOwnership::BuiltIn,
                other => {
                    return Err(DomainCommandError::Invalid(format!(
                        "unknown agent ownership {other:?}"
                    )));
                }
            },
            enabled: definition.enabled,
            allowed_capabilities: definition.allowed_capabilities,
            denied_capabilities: definition.denied_capabilities,
            allowed_tools: definition.allowed_tools,
            denied_tools: definition.denied_tools,
            tool_profile: match definition.tool_profile.as_str() {
                "" | "custom" => AgentToolProfile::Custom,
                "none" => AgentToolProfile::None,
                "read_only" => AgentToolProfile::ReadOnly,
                "command_runner" => AgentToolProfile::CommandRunner,
                "bounded_watcher" => AgentToolProfile::BoundedWatcher,
                other => {
                    return Err(DomainCommandError::Invalid(format!(
                        "unknown agent tool profile {other:?}"
                    )));
                }
            },
            task_shape: definition.task_shape,
            output_contract: definition.output_contract,
            timeout_seconds: definition.timeout_seconds,
            poll_interval_ms: definition.poll_interval_ms,
            max_turns: definition.max_turns,
            max_concurrency: if definition.max_concurrency == 0 {
                4
            } else {
                definition.max_concurrency
            },
            fallback_policy: match definition.fallback_policy.as_str() {
                "" | "configured_only" => AgentFallbackPolicy::ConfiguredOnly,
                "prohibited" => AgentFallbackPolicy::Prohibited,
                other => {
                    return Err(DomainCommandError::Invalid(format!(
                        "unknown agent fallback policy {other:?}"
                    )));
                }
            },
            can_delegate: definition.can_delegate,
            max_delegation_depth: definition.max_delegation_depth,
            require_parent_attribution: definition.require_parent_attribution,
        };
        self.engine()
            .state()
            .validate_agent_definition(&definition, previous_slug.as_deref())?;
        Ok(Self::accepted(
            Some(slug),
            vec![Effect::SaveAgent {
                definition,
                previous_slug,
            }],
        ))
    }

    fn delete_agent_command(
        &self,
        workspace_id: &WorkspaceId,
        slug: String,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        self.engine().state().validate_agent_deletion(&slug)?;
        Ok(Self::accepted(
            Some(slug.clone()),
            vec![Effect::DeleteAgent(slug)],
        ))
    }

    fn check_agent_browser_command(&self, workspace_id: &WorkspaceId) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        Ok(Self::accepted(
            Some(workspace_id.to_string()),
            vec![Effect::CheckAgentBrowser],
        ))
    }

    fn save_soul_command(
        &self,
        workspace_id: &WorkspaceId,
        content: &str,
        expected_digest: Option<&str>,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let store = self.soul_store.as_ref().ok_or_else(|| {
            DomainCommandError::Invalid("Nakode Soul storage is unavailable".to_owned())
        })?;
        store
            .save(content, expected_digest)
            .map_err(|error| soul_domain_error(&error))?;
        Ok(Self::accepted(Some(workspace_id.to_string()), Vec::new()))
    }

    fn refresh_session_template_addenda(&mut self) -> Result<(), DomainCommandError> {
        self.session_template
            .reload_prompt_addenda()
            .map_err(|error| {
                DomainCommandError::Invalid(format!(
                    "could not load prompt addenda for the new session: {error}"
                ))
            })
    }

    fn reload_workspace_command(
        &self,
        workspace_id: &WorkspaceId,
        session_id: &SessionId,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        self.ensure_session(session_id)?;
        Ok(Self::accepted(
            Some(workspace_id.to_string()),
            vec![Effect::ReloadConfiguration],
        ))
    }

    fn accepted(
        resource_id: Option<String>,
        effects: Vec<Effect>,
    ) -> (CommandAccepted, Vec<Effect>) {
        (
            CommandAccepted {
                resource_id,
                revision: None,
                bridge_continuation: None,
                replayed_bridge_continuation: None,
                replayed_bridge_source_active: None,
            },
            effects,
        )
    }

    fn accepted_bridge_continuation(
        session_id: &SessionId,
        disposition: BridgeContinuationDisposition,
        effects: Vec<Effect>,
    ) -> (CommandAccepted, Vec<Effect>) {
        (
            CommandAccepted {
                resource_id: Some(session_id.to_string()),
                revision: None,
                bridge_continuation: Some(disposition),
                replayed_bridge_continuation: None,
                replayed_bridge_source_active: None,
            },
            effects,
        )
    }

    fn reload_agent_catalogue_for_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), DomainCommandError> {
        let directory = self
            .session_engine_mut(session_id)?
            .state()
            .agent_directory()
            .to_path_buf();
        let agents = AgentCatalog::load(&directory)
            .map_err(|error| DomainCommandError::Invalid(error.to_string()))?;
        self.session_engine_mut(session_id)?
            .state_mut()
            .install_agents(agents);
        Ok(())
    }

    pub(crate) fn install_memory_config(&mut self, config: &MemoryConfig) {
        self.session_template.install_memory_config(config.clone());
        for engine in self.sessions_by_id.values_mut() {
            engine.state_mut().install_memory_config(config.clone());
        }
    }

    fn reload_global_agent_catalogue(&mut self) -> Result<(), ServiceError> {
        let directory = self.engine().state().agent_directory().to_path_buf();
        let agents = AgentCatalog::load(&directory).map_err(|error| {
            domain_error(DomainCommandError::Invalid(format!(
                "could not reload global agent catalogue: {error}"
            )))
        })?;
        self.session_template.install_agents(agents.clone());
        for engine in self.sessions_by_id.values_mut() {
            engine.state_mut().install_agents(agents.clone());
        }
        Ok(())
    }

    fn query(&self, query: Query) -> Result<QueryResult, ServiceError> {
        let bootstrap = || self.workspace_bootstrap();
        match query {
            Query::InspectWorkspacePath {
                path,
                expected_git_repository,
            } => Ok(QueryResult::WorkspacePathInspection(
                inspect_workspace_path(&path, expected_git_repository.as_deref())?,
            )),
            Query::Bootstrap {
                workspace: _,
                session_id,
            } => {
                let mut view = bootstrap();
                if let Some(session_id) = session_id {
                    view.active_session = Some(self.session_view(&session_id)?);
                }
                Ok(QueryResult::Bootstrap(Box::new(view)))
            }
            Query::GetSoul { workspace_id } => self.query_soul(workspace_id),
            Query::GetMcpManagement { workspace_id } => {
                self.ensure_workspace(&workspace_id).map_err(domain_error)?;
                Ok(QueryResult::McpManagement(self.mcp_management()))
            }
            Query::ListSessions {
                workspace_id,
                limit,
            } => {
                self.ensure_workspace(&workspace_id).map_err(domain_error)?;
                let mut sessions = bootstrap().sessions;
                let limit = usize::try_from(limit).unwrap_or(usize::MAX).min(500);
                let complete = self.session_inventory_complete && sessions.len() <= limit;
                sessions.truncate(limit);
                Ok(QueryResult::Sessions(nakode_protocol::SessionInventory {
                    sessions,
                    complete,
                }))
            }
            Query::GetSession { session_id } => Ok(QueryResult::Session(Box::new(
                self.session_view(&session_id)?,
            ))),
            Query::GetTranscriptPage {
                session_id,
                before,
                limit,
            } => self.query_session_transcript_page(&session_id, before.as_ref(), limit),
            Query::GetRunTranscriptPage {
                run_id,
                before,
                limit,
            } => self.query_run_transcript_page(&run_id, before.as_ref(), limit),
            Query::GetTranscriptBodyWindow {
                owner,
                entry_id,
                before_byte,
                limit_bytes,
            } => self.query_transcript_body_window(&owner, &entry_id, before_byte, limit_bytes),
            Query::GetRun { run_id } => {
                let run = self.run_view(&run_id)?;
                Ok(QueryResult::Run(Box::new(run)))
            }
            Query::ListRuns {
                session_id,
                before,
                limit,
            } => {
                let state = self
                    .engine_for(&session_id)
                    .ok_or_else(|| not_found("session", session_id.as_str()))?
                    .state();
                let page = crate::state::projection::run_page(
                    state,
                    before.as_ref(),
                    usize::try_from(limit).unwrap_or(usize::MAX),
                )
                .ok_or_else(|| not_found("run", before.as_ref().map_or("", RunId::as_str)))?;
                Ok(QueryResult::Runs(page))
            }
            Query::GetRunTextWindow {
                run_id,
                field,
                before_byte,
                limit_bytes,
            } => self.query_run_text_window(&run_id, field, before_byte, limit_bytes),
            Query::GetArtifact { artifact_id } => self.query_artifact(&artifact_id),
            Query::GetDiagnostics { .. }
            | Query::ListSkills { .. }
            | Query::GetInvocationSummary
            | Query::GetInvocationTimeline { .. } => Err(service_error(
                ErrorCode::Internal,
                "telemetry queries are served by the native runtime",
                false,
            )),
        }
    }

    fn query_soul(&self, workspace_id: WorkspaceId) -> Result<QueryResult, ServiceError> {
        self.ensure_workspace(&workspace_id).map_err(domain_error)?;
        let store = self.soul_store.as_ref().ok_or_else(|| {
            service_error(
                ErrorCode::Internal,
                "Nakode Soul storage is unavailable",
                false,
            )
        })?;
        let soul = store.read().map_err(|error| soul_service_error(&error))?;
        Ok(QueryResult::SoulDocument(SoulDocumentView {
            workspace_id,
            content: soul.content.unwrap_or_default(),
            path: soul.path.to_string_lossy().into_owned(),
            source: match soul.source {
                SoulSource::File => "file",
                SoulSource::Missing => "missing",
            }
            .to_owned(),
            exists: soul.exists,
            digest: soul.digest,
        }))
    }

    fn query_session_transcript_page(
        &self,
        session_id: &SessionId,
        before: Option<&EntryId>,
        limit: u32,
    ) -> Result<QueryResult, ServiceError> {
        let state = self
            .engine_for(session_id)
            .ok_or_else(|| not_found("session", session_id.as_str()))?
            .state();
        let page = crate::state::projection::session_transcript_page(
            state,
            before,
            usize::try_from(limit).unwrap_or(usize::MAX),
        )
        .ok_or_else(|| {
            not_found(
                "transcript entry",
                before.map_or("", nakode_protocol::EntryId::as_str),
            )
        })?;
        Ok(QueryResult::Transcript(Box::new(page)))
    }

    fn query_run_transcript_page(
        &self,
        run_id: &RunId,
        before: Option<&EntryId>,
        limit: u32,
    ) -> Result<QueryResult, ServiceError> {
        let session_id = self.session_for_run(run_id).map_err(domain_error)?;
        let state = self
            .engine_for(&session_id)
            .ok_or_else(|| not_found("session", session_id.as_str()))?
            .state();
        let page = crate::state::projection::run_transcript_page(
            state,
            run_id,
            before,
            usize::try_from(limit).unwrap_or(usize::MAX),
        )
        .ok_or_else(|| {
            not_found(
                "transcript entry",
                before.map_or("", nakode_protocol::EntryId::as_str),
            )
        })?;
        Ok(QueryResult::Transcript(Box::new(page)))
    }

    fn query_transcript_body_window(
        &self,
        owner: &TranscriptOwner,
        entry_id: &EntryId,
        before_byte: Option<u64>,
        limit_bytes: u32,
    ) -> Result<QueryResult, ServiceError> {
        let (state, run_id) = match owner {
            TranscriptOwner::Session { session_id } => (
                self.engine_for(session_id)
                    .ok_or_else(|| not_found("session", session_id.as_str()))?
                    .state(),
                None,
            ),
            TranscriptOwner::Run { run_id } => {
                let session_id = self.session_for_run(run_id).map_err(domain_error)?;
                (
                    self.engine_for(&session_id)
                        .ok_or_else(|| not_found("session", session_id.as_str()))?
                        .state(),
                    Some(run_id),
                )
            }
        };
        let window = crate::state::projection::transcript_body_window(
            state,
            run_id,
            entry_id,
            before_byte,
            usize::try_from(limit_bytes).unwrap_or(usize::MAX),
        )
        .map_err(|error| transcript_body_window_error(error, entry_id))?;
        Ok(QueryResult::TranscriptBody(window))
    }

    fn query_artifact(
        &self,
        artifact_id: &nakode_protocol::ArtifactId,
    ) -> Result<QueryResult, ServiceError> {
        for engine in self.sessions_by_id.values() {
            match crate::state::projection::artifact_view(engine.state(), artifact_id) {
                Ok(Some(artifact)) => return Ok(QueryResult::Artifact(artifact)),
                Ok(None) => {}
                Err(error) => {
                    return Err(service_error(
                        ErrorCode::InvalidRequest,
                        &format!(
                            "artifact {:?} is {} bytes; maximum transferable size is {} bytes",
                            artifact_id.as_str(),
                            error.actual,
                            error.maximum
                        ),
                        false,
                    ));
                }
            }
        }
        Err(not_found("artifact", artifact_id.as_str()))
    }

    fn query_run_text_window(
        &self,
        run_id: &RunId,
        field: RunTextField,
        before_byte: Option<u64>,
        limit_bytes: u32,
    ) -> Result<QueryResult, ServiceError> {
        let session_id = self.session_for_run(run_id).map_err(domain_error)?;
        let state = self
            .engine_for(&session_id)
            .ok_or_else(|| not_found("session", session_id.as_str()))?
            .state();
        let window = crate::state::projection::run_text_window(
            state,
            run_id,
            field,
            before_byte,
            usize::try_from(limit_bytes).unwrap_or(usize::MAX),
        )
        .map_err(|error| run_text_window_error(error, run_id, field))?;
        Ok(QueryResult::RunText(window))
    }

    fn subscription_view(
        &self,
        scope: &SubscriptionScope,
    ) -> Result<SubscriptionView, ServiceError> {
        match scope {
            SubscriptionScope::Workspace { workspace_id } => {
                self.ensure_workspace(workspace_id).map_err(domain_error)?;
                Ok(SubscriptionView::Workspace(Box::new(
                    self.workspace_bootstrap(),
                )))
            }
            SubscriptionScope::Session { session_id } => Ok(SubscriptionView::Session(Box::new(
                self.session_view(session_id)?,
            ))),
            SubscriptionScope::Run { run_id } => {
                Ok(SubscriptionView::Run(Box::new(self.run_view(run_id)?)))
            }
        }
    }

    fn publish_state(&mut self, endpoint: &ServerEndpoint, session_id: &SessionId) {
        let workspace = self.workspace_bootstrap();
        let publications =
            self.workspace_publications(self.published_workspace.as_ref(), &workspace);
        self.published_workspace = Some(workspace);

        for publication in publications {
            let _ = endpoint.publish(publication.scopes, publication.event);
        }
        self.publish_session_state(endpoint, session_id);
    }

    fn publish_session_state(&mut self, endpoint: &ServerEndpoint, session_id: &SessionId) {
        let mut publications = Vec::new();
        if let Some(session) = self.published_session(session_id) {
            publications.extend(
                self.session_publications(self.published_sessions.get(session_id), &session),
            );
            self.published_sessions.insert(session_id.clone(), session);
        }

        for publication in publications {
            let _ = endpoint.publish(publication.scopes, publication.event);
        }
    }

    fn workspace_publications(
        &self,
        previous: Option<&nakode_protocol::BootstrapView>,
        current: &nakode_protocol::BootstrapView,
    ) -> Vec<PendingPublication> {
        let scope = vec![SubscriptionScope::Workspace {
            workspace_id: current.workspace_id.clone(),
        }];
        let Some(previous) = previous else {
            return vec![PendingPublication {
                scopes: scope,
                event: ViewEvent::BootstrapChanged {
                    snapshot: Box::new(current.clone()),
                },
            }];
        };
        if workspace_metadata(previous) != workspace_metadata(current) {
            return vec![PendingPublication {
                scopes: scope,
                event: ViewEvent::BootstrapChanged {
                    snapshot: Box::new(current.clone()),
                },
            }];
        }

        let mut publications = Vec::new();
        for provider in &current.providers {
            if previous
                .providers
                .iter()
                .find(|candidate| candidate.id == provider.id)
                != Some(provider)
            {
                publications.push(PendingPublication {
                    scopes: scope.clone(),
                    event: ViewEvent::ProviderChanged {
                        provider: provider.clone(),
                    },
                });
            }
        }
        for provider in &previous.providers {
            if !current
                .providers
                .iter()
                .any(|candidate| candidate.id == provider.id)
            {
                publications.push(PendingPublication {
                    scopes: scope.clone(),
                    event: ViewEvent::ProviderRemoved {
                        provider_id: provider.id.clone(),
                    },
                });
            }
        }
        for session in &current.sessions {
            if previous
                .sessions
                .iter()
                .find(|candidate| candidate.id == session.id)
                != Some(session)
            {
                let revision = self
                    .engine_for(&session.id)
                    .map_or(0, ServiceEngine::revision);
                publications.push(PendingPublication {
                    scopes: scope.clone(),
                    event: ViewEvent::SessionUpserted {
                        revision,
                        session: session.clone(),
                    },
                });
            }
        }
        for session in &previous.sessions {
            if !current
                .sessions
                .iter()
                .any(|candidate| candidate.id == session.id)
            {
                publications.push(PendingPublication {
                    scopes: scope.clone(),
                    event: ViewEvent::SessionRemoved {
                        session_id: session.id.clone(),
                    },
                });
            }
        }
        publications
    }

    fn session_publications(
        &self,
        previous: Option<&PublishedSessionProjection>,
        current: &PublishedSessionProjection,
    ) -> Vec<PendingPublication> {
        let session_id = current.view.id.clone();
        let revision = current.view.revision;
        let session_scope = vec![SubscriptionScope::Session {
            session_id: session_id.clone(),
        }];
        let Some(previous) = previous else {
            let mut scopes = session_scope;
            scopes.extend(current.view.runs.iter().map(|run| SubscriptionScope::Run {
                run_id: run.id.clone(),
            }));
            return vec![PendingPublication {
                scopes,
                event: ViewEvent::SessionChanged {
                    session: Box::new(current.view.clone()),
                },
            }];
        };

        let Some(engine) = self.engine_for(&session_id) else {
            return Vec::new();
        };
        let mut publications = Vec::new();
        let metadata = session_metadata(&current.view);
        if session_metadata(&previous.view) != metadata {
            publications.push(PendingPublication {
                scopes: session_scope.clone(),
                event: ViewEvent::SessionMetadataChanged {
                    session_id: session_id.clone(),
                    revision,
                    metadata: Box::new(metadata.clone()),
                },
            });
        }
        publications.extend(
            transcript_events(
                engine.state(),
                &TranscriptTarget::Session {
                    session_id: session_id.clone(),
                    revision,
                },
                &previous.view.transcript,
                &previous.transcript_lengths,
                &current.view.transcript,
                &current.transcript_lengths,
            )
            .into_iter()
            .map(|event| PendingPublication {
                scopes: session_scope.clone(),
                event,
            }),
        );
        if previous.view.queue != current.view.queue {
            publications.push(PendingPublication {
                scopes: session_scope.clone(),
                event: ViewEvent::QueueChanged {
                    session_id: session_id.clone(),
                    revision,
                    queue: current.view.queue.clone(),
                },
            });
        }
        if previous.view.interactions != current.view.interactions {
            publications.push(PendingPublication {
                scopes: session_scope.clone(),
                event: ViewEvent::InteractionsChanged {
                    session_id: session_id.clone(),
                    revision,
                    interactions: current.view.interactions.clone(),
                },
            });
        }
        if previous.view.todos != current.view.todos {
            publications.push(PendingPublication {
                scopes: session_scope.clone(),
                event: ViewEvent::TodosChanged {
                    session_id: session_id.clone(),
                    revision,
                    phases: current.view.todos.clone(),
                },
            });
        }

        publications.extend(Self::run_publications(engine, previous, current));
        if publications.is_empty() {
            publications.push(PendingPublication {
                scopes: session_scope,
                event: ViewEvent::SessionMetadataChanged {
                    session_id,
                    revision,
                    metadata: Box::new(metadata),
                },
            });
        }
        publications
    }

    fn run_publications(
        engine: &ServiceEngine,
        previous: &PublishedSessionProjection,
        current: &PublishedSessionProjection,
    ) -> Vec<PendingPublication> {
        let session_id = &current.view.id;
        let revision = current.view.revision;
        let mut publications = current
            .view
            .runs
            .iter()
            .flat_map(|run| Self::one_run_publications(engine, previous, current, run))
            .collect::<Vec<_>>();
        let session_scope = vec![SubscriptionScope::Session {
            session_id: session_id.clone(),
        }];
        for run in &previous.view.runs {
            if !current
                .view
                .runs
                .iter()
                .any(|candidate| candidate.id == run.id)
            {
                publications.push(PendingPublication {
                    scopes: session_scope.clone(),
                    event: ViewEvent::RunRemoved {
                        session_id: session_id.clone(),
                        revision,
                        run_id: run.id.clone(),
                    },
                });
            }
        }
        let previous_run_ids = run_ids(&previous.view);
        let current_run_ids = run_ids(&current.view);
        if previous_run_ids != current_run_ids
            || previous.view.runs_has_earlier != current.view.runs_has_earlier
        {
            publications.push(PendingPublication {
                scopes: session_scope,
                event: ViewEvent::RunWindowChanged {
                    session_id: session_id.clone(),
                    revision,
                    run_ids: current_run_ids,
                    has_earlier: current.view.runs_has_earlier,
                },
            });
        }
        publications
    }

    fn one_run_publications(
        engine: &ServiceEngine,
        previous: &PublishedSessionProjection,
        current: &PublishedSessionProjection,
        run: &RunView,
    ) -> Vec<PendingPublication> {
        let session_id = &current.view.id;
        let revision = current.view.revision;
        let run_scope = vec![
            SubscriptionScope::Session {
                session_id: session_id.clone(),
            },
            SubscriptionScope::Run {
                run_id: run.id.clone(),
            },
        ];
        let Some(previous_run) = previous
            .view
            .runs
            .iter()
            .find(|candidate| candidate.id == run.id)
        else {
            return vec![PendingPublication {
                scopes: run_scope,
                event: ViewEvent::RunChanged {
                    session_id: session_id.clone(),
                    revision,
                    run: Box::new(run.clone()),
                },
            }];
        };
        let mut publications = Vec::new();
        let metadata = run_metadata(run);
        if run_metadata(previous_run) != metadata {
            publications.push(PendingPublication {
                scopes: run_scope.clone(),
                event: ViewEvent::RunMetadataChanged {
                    session_id: session_id.clone(),
                    revision,
                    run: Box::new(metadata),
                },
            });
        }
        let previous_lengths = previous
            .run_transcript_lengths
            .get(&run.id)
            .cloned()
            .unwrap_or_default();
        let current_lengths = current
            .run_transcript_lengths
            .get(&run.id)
            .cloned()
            .unwrap_or_default();
        publications.extend(
            transcript_events(
                engine.state(),
                &TranscriptTarget::Run {
                    session_id: session_id.clone(),
                    revision,
                    run_id: run.id.clone(),
                },
                &previous_run.transcript,
                &previous_lengths,
                &run.transcript,
                &current_lengths,
            )
            .into_iter()
            .map(|event| PendingPublication {
                scopes: run_scope.clone(),
                event,
            }),
        );
        publications
    }

    fn published_session(&self, session_id: &SessionId) -> Option<PublishedSessionProjection> {
        let engine = self.engine_for(session_id)?;
        let view = self.session_view(session_id).ok()?;
        let session_transcript_lengths = transcript_lengths(engine.state(), None, &view.transcript);
        let run_transcript_lengths = view
            .runs
            .iter()
            .map(|run| {
                (
                    run.id.clone(),
                    transcript_lengths(engine.state(), Some(&run.id), &run.transcript),
                )
            })
            .collect();
        Some(PublishedSessionProjection {
            view,
            transcript_lengths: session_transcript_lengths,
            run_transcript_lengths,
        })
    }

    pub(crate) fn live_work_sessions(&self) -> Vec<(String, u64)> {
        // In-memory turns, queues, shells, and delegates exist only in attached engines. Persisted
        // records outside this map are closed snapshots and cannot own process-local live work.
        self.sessions_by_id
            .iter()
            .filter_map(|(session_id, engine)| {
                let session = engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session?;
                (!matches!(session.activity, nakode_protocol::SessionActivity::Idle)
                    || !session.queue.is_empty())
                .then(|| (session_id.to_string(), session.revision))
            })
            .collect()
    }

    fn workspace_bootstrap(&self) -> nakode_protocol::BootstrapView {
        let mut bootstrap = self
            .engine()
            .bootstrap_view(&self.providers, &self.sessions);
        let initial_session_is_persisted = self
            .sessions
            .iter()
            .any(|record| record.id == self.default_session.as_str());
        if !initial_session_is_persisted {
            bootstrap
                .sessions
                .retain(|summary| summary.id != self.default_session);
        }
        for (session_id, engine) in &self.sessions_by_id {
            // A fresh initial engine is the workspace control-plane host, not a logical conversation.
            // It only becomes discoverable if provider work persists it. This also keeps successor
            // engines from recreating an epoch-dated `New session` duplicate after deletion.
            if *session_id == self.default_session && !initial_session_is_persisted {
                continue;
            }
            let summary = if let Some(summary) =
                crate::state::projection::active_session_summary(engine.state(), &self.sessions)
            {
                summary
            } else {
                let Some(session) = engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session
                else {
                    continue;
                };
                let mut owned_provider_sessions = session
                    .active_agent_session
                    .as_ref()
                    .and_then(|agent| {
                        agent.native_session_id.as_ref().map(|native_session_id| {
                            nakode_protocol::OwnedProviderSessionView {
                                provider_id: agent.provider_id.clone(),
                                native_session_id: native_session_id.clone(),
                            }
                        })
                    })
                    .into_iter()
                    .collect::<Vec<_>>();
                owned_provider_sessions.extend(session.runs.iter().filter_map(|run| {
                    run.native_session_id.as_ref().map(|native_session_id| {
                        nakode_protocol::OwnedProviderSessionView {
                            provider_id: run.provider_id.clone(),
                            native_session_id: native_session_id.clone(),
                        }
                    })
                }));
                nakode_protocol::SessionSummary {
                    id: session.id,
                    workspace_id: session.workspace_id,
                    working_directory: session.working_directory,
                    title: session.title,
                    active_provider_id: session.selected_provider_id,
                    active_model_id: session.selected_model_id,
                    updated_at_ms: session.updated_at_ms,
                    created_at_ms: session.created_at_ms,
                    last_owner_activity_at_ms: session.last_owner_activity_at_ms,
                    owned_provider_sessions,
                    running: !matches!(session.activity, nakode_protocol::SessionActivity::Idle),
                }
            };
            let position = bootstrap
                .sessions
                .iter()
                .position(|candidate| candidate.id == summary.id);
            if let Some(position) = position {
                bootstrap.sessions[position] = summary;
            } else {
                // Match projection::bootstrap's treatment of a live session that has not reached
                // persistence yet: current process-owned work precedes the persisted recency list.
                bootstrap.sessions.insert(0, summary);
            }
        }
        bootstrap.session_bridges = self
            .session_bridges
            .iter()
            .map(session_bridge_view)
            .collect();
        bootstrap.active_session = None;
        bootstrap
    }

    fn session_view(
        &self,
        session_id: &SessionId,
    ) -> Result<nakode_protocol::SessionView, ServiceError> {
        self.engine_for(session_id)
            .and_then(|engine| {
                engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session
            })
            .ok_or_else(|| not_found("session", session_id.as_str()))
    }

    fn run_view(&self, run_id: &RunId) -> Result<nakode_protocol::RunView, ServiceError> {
        self.sessions_by_id
            .values()
            .find_map(|engine| crate::state::projection::run_view(engine.state(), run_id))
            .ok_or_else(|| not_found("run", run_id.as_str()))
    }

    fn synchronize_workspace_state_from(&mut self, session_id: &SessionId) {
        let Some(source) = self
            .sessions_by_id
            .get(session_id)
            .map(|engine| engine.state().clone())
        else {
            return;
        };
        self.session_template
            .synchronize_workspace_configuration(&source);
        for (candidate_id, engine) in &mut self.sessions_by_id {
            if candidate_id != session_id {
                engine
                    .state_mut()
                    .synchronize_workspace_configuration(&source);
            }
        }
    }

    pub(crate) fn session_for_run_id(&self, run_id: &str) -> Option<SessionId> {
        self.session_for_run(&RunId::from(run_id.to_owned())).ok()
    }

    fn ensure_workspace(
        &self,
        workspace_id: &nakode_protocol::WorkspaceId,
    ) -> Result<(), DomainCommandError> {
        let expected = crate::state::projection::workspace_id(&self.engine().state().workspace);
        if *workspace_id == expected {
            Ok(())
        } else {
            Err(DomainCommandError::NotFound(workspace_id.to_string()))
        }
    }

    fn ensure_session(
        &self,
        session_id: &nakode_protocol::SessionId,
    ) -> Result<(), DomainCommandError> {
        if self.sessions_by_id.contains_key(session_id) {
            Ok(())
        } else {
            Err(DomainCommandError::NotFound(session_id.to_string()))
        }
    }

    fn session_bridge(
        &self,
        session_id: &SessionId,
    ) -> Result<&SessionBridgeRecord, DomainCommandError> {
        self.session_bridges
            .iter()
            .find(|bridge| bridge.session_id == session_id.as_str())
            .ok_or_else(|| DomainCommandError::NotFound(session_id.to_string()))
    }

    fn session_bridge_mut(
        &mut self,
        session_id: &SessionId,
    ) -> Result<&mut SessionBridgeRecord, DomainCommandError> {
        self.session_bridges
            .iter_mut()
            .find(|bridge| bridge.session_id == session_id.as_str())
            .ok_or_else(|| DomainCommandError::NotFound(session_id.to_string()))
    }

    fn session_engine_mut(
        &mut self,
        session_id: &SessionId,
    ) -> Result<&mut ServiceEngine, DomainCommandError> {
        self.sessions_by_id
            .get_mut(session_id)
            .ok_or_else(|| DomainCommandError::NotFound(session_id.to_string()))
    }

    fn ensure_provider(&self, provider_id: &ProviderId) -> Result<(), DomainCommandError> {
        if self
            .providers
            .iter()
            .any(|provider| provider.provider == provider_id.as_str())
        {
            Ok(())
        } else {
            Err(DomainCommandError::NotFound(provider_id.to_string()))
        }
    }

    fn session_for_agent_session(
        &self,
        agent_session_id: &AgentSessionId,
    ) -> Result<SessionId, DomainCommandError> {
        self.sessions_by_id
            .iter()
            .find_map(|(session_id, engine)| {
                engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session
                    .and_then(|session| session.active_agent_session)
                    .is_some_and(|session| session.id == *agent_session_id)
                    .then(|| session_id.clone())
            })
            .ok_or_else(|| DomainCommandError::NotFound(agent_session_id.to_string()))
    }

    fn provider_turn_id(
        &self,
        turn_id: &nakode_protocol::TurnId,
    ) -> Result<(SessionId, String), DomainCommandError> {
        self.sessions_by_id
            .iter()
            .find_map(|(session_id, engine)| {
                engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session
                    .and_then(|session| session.active_turn)
                    .is_some_and(|turn| turn.id == *turn_id)
                    .then(|| {
                        engine
                            .state()
                            .active_turn
                            .as_ref()
                            .map(|turn| (session_id.clone(), turn.id.clone()))
                    })
                    .flatten()
            })
            .ok_or_else(|| DomainCommandError::NotFound(turn_id.to_string()))
    }

    fn session_for_interaction(
        &self,
        interaction_id: &nakode_protocol::InteractionId,
    ) -> Result<SessionId, DomainCommandError> {
        self.sessions_by_id
            .iter()
            .find_map(|(session_id, engine)| {
                engine
                    .bootstrap_view(&self.providers, &self.sessions)
                    .active_session
                    .into_iter()
                    .flat_map(|session| session.interactions)
                    .any(|interaction| interaction.id == *interaction_id)
                    .then(|| session_id.clone())
            })
            .ok_or_else(|| DomainCommandError::NotFound(interaction_id.to_string()))
    }

    fn session_for_run(&self, run_id: &RunId) -> Result<SessionId, DomainCommandError> {
        self.sessions_by_id
            .iter()
            .find_map(|(session_id, engine)| {
                engine
                    .state()
                    .subagents
                    .iter()
                    .any(|run| run.id == run_id.as_str())
                    .then(|| session_id.clone())
            })
            .ok_or_else(|| DomainCommandError::NotFound(run_id.to_string()))
    }

    fn command_session(&self, command: &Command) -> Option<SessionId> {
        match command {
            Command::SendPrompt { session_id, .. }
            | Command::ContinueSessionFromBridge { session_id, .. }
            | Command::SetSessionBridgeLifecycle { session_id, .. }
            | Command::BindSessionBridgeThread { session_id, .. }
            | Command::ClearSessionBridgeThread { session_id, .. }
            | Command::PrepareBridgeDelivery { session_id, .. }
            | Command::CompleteBridgeDeliveryPart { session_id, .. }
            | Command::FinalizeBridgeDelivery { session_id, .. }
            | Command::SetBridgeLiveMessage { session_id, .. }
            | Command::EnqueuePrompt { session_id, .. }
            | Command::RemoveQueuedPrompt { session_id, .. }
            | Command::SteerQueuedPrompt { session_id, .. }
            | Command::CancelSessionWork { session_id }
            | Command::Delegate { session_id, .. }
            | Command::RunShell { session_id, .. }
            | Command::ReloadWorkspace { session_id, .. }
            | Command::ConfigureSessionTools { session_id, .. }
            | Command::SubmitExternalToolResult { session_id, .. }
            | Command::SelectModel {
                target: ModelTarget::Session { session_id },
                ..
            } => Some(session_id.clone()),
            Command::OpenSession { session_id, .. } => self
                .sessions_by_id
                .keys()
                .find(|loaded| loaded.as_str().starts_with(session_id.as_str()))
                .cloned(),
            Command::SteerTurn { turn_id, .. } | Command::CancelTurn { turn_id } => self
                .provider_turn_id(turn_id)
                .ok()
                .map(|(session_id, _)| session_id),
            Command::CompactContext { agent_session_id } => {
                self.session_for_agent_session(agent_session_id).ok()
            }
            Command::SelectModel {
                target: ModelTarget::AgentSession { agent_session_id },
                ..
            } => self.session_for_agent_session(agent_session_id).ok(),
            Command::ResolveInteraction { interaction_id, .. } => {
                self.session_for_interaction(interaction_id).ok()
            }
            Command::CancelRun { run_id } | Command::ContinueRun { run_id, .. } => {
                self.session_for_run(run_id).ok()
            }
            Command::CreateSession { .. }
            | Command::SetWorkspaceBridgeLifecycle { .. }
            | Command::SelectModel { .. }
            | Command::SetProviderModelFilter { .. }
            | Command::SetSkillEnabled { .. }
            | Command::SetProviderEnabled { .. }
            | Command::BeginProviderAuthentication { .. }
            | Command::SetProviderCredential { .. }
            | Command::ClearProviderCredential { .. }
            | Command::ReloadProvider { .. }
            | Command::SaveMcpServer { .. }
            | Command::DeleteMcpServer { .. }
            | Command::SetMcpServerEnabled { .. }
            | Command::RefreshMcpServer { .. }
            | Command::SetMcpServerCredential { .. }
            | Command::ClearMcpServerCredential { .. }
            | Command::SetMcpServerGrants { .. }
            | Command::SaveAgent { .. }
            | Command::SaveSoul { .. }
            | Command::DeleteAgent { .. }
            | Command::UpdateSettings { .. }
            | Command::CheckAgentBrowser { .. } => Some(self.default_session.clone()),
            // Run deletion effects through whichever control-plane engine is default AFTER command
            // acceptance. This matters when deleting a closed initial session rotates that role.
            Command::DeleteSession { .. } => None,
        }
    }

    fn convert_prompt(
        &self,
        session_id: &SessionId,
        prompt: nakode_protocol::PromptInput,
    ) -> Result<(String, Vec<PromptAttachment>), DomainCommandError> {
        let state = self
            .engine_for(session_id)
            .ok_or_else(|| DomainCommandError::NotFound(session_id.to_string()))?
            .state();
        let attachments = prompt
            .attachments
            .into_iter()
            .map(|attachment| match attachment {
                nakode_protocol::PromptAttachment::Artifact { artifact_id, label } => {
                    let artifact = crate::state::projection::artifact_view(state, &artifact_id)
                        .map_err(|error| {
                            DomainCommandError::Invalid(format!(
                                "artifact {artifact_id:?} is {} bytes; maximum is {} bytes",
                                error.actual, error.maximum
                            ))
                        })?
                        .ok_or_else(|| DomainCommandError::NotFound(artifact_id.to_string()))?;
                    Ok(PromptAttachment {
                        label,
                        path: None,
                        image: Some(crate::backend::PromptImage {
                            mime_type: artifact.media_type,
                            data: artifact.data,
                        }),
                    })
                }
                nakode_protocol::PromptAttachment::LocalFile { label, path } => {
                    let path = validated_relative_path(&path)?;
                    Ok(PromptAttachment {
                        label,
                        path: Some(path),
                        image: None,
                    })
                }
                nakode_protocol::PromptAttachment::InlineImage {
                    label,
                    media_type,
                    data,
                } => {
                    if data.len() > MAX_ARTIFACT_BYTES {
                        return Err(DomainCommandError::Invalid(format!(
                            "inline image {label:?} is {} bytes; maximum is {MAX_ARTIFACT_BYTES} bytes",
                            data.len()
                        )));
                    }
                    Ok(PromptAttachment {
                        label,
                        path: None,
                        image: Some(crate::backend::PromptImage {
                            mime_type: media_type,
                            data,
                        }),
                    })
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((prompt.text, attachments))
    }
}

fn inspect_workspace_path(
    requested: &str,
    expected_git_repository: Option<&str>,
) -> Result<nakode_protocol::WorkspacePathInspectionView, ServiceError> {
    let canonical_path =
        canonical_working_directory(Some(requested), requested).map_err(domain_error)?;
    let git = |arguments: &[&str]| -> Option<String> {
        let output = std::process::Command::new("git")
            .args(["-C", canonical_path.as_str()])
            .args(arguments)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    let git_repository = git(&["config", "--get", "remote.origin.url"])
        .filter(|value| !value.is_empty())
        .map(|value| sanitized_repository_identity(&value));
    if let Some(expected) = expected_git_repository {
        let expected = sanitized_repository_identity(expected);
        let actual = git_repository.as_deref().ok_or_else(|| {
            service_error(
                ErrorCode::Conflict,
                "workspace path has no configured origin repository",
                false,
            )
        })?;
        if actual != expected {
            let message =
                format!("workspace repository mismatch: expected {expected}, found {actual}");
            return Err(service_error(ErrorCode::Conflict, &message, false));
        }
    }
    let branch =
        git(&["symbolic-ref", "--quiet", "--short", "HEAD"]).filter(|value| !value.is_empty());
    let revision = git(&["rev-parse", "HEAD"]).filter(|value| !value.is_empty());
    let dirty = git(&["status", "--porcelain=v1", "--untracked-files=normal"])
        .is_some_and(|value| !value.is_empty());
    Ok(nakode_protocol::WorkspacePathInspectionView {
        canonical_path,
        git_repository,
        branch,
        revision,
        dirty,
    })
}

fn sanitized_repository_identity(value: &str) -> String {
    const UNRECOGNIZED: &str = "[unrecognized repository]";

    let value = value.trim();
    if value.chars().any(char::is_whitespace) || value.contains(['?', '#']) {
        return UNRECOGNIZED.to_owned();
    }

    if value.contains("://") {
        if let Ok(parsed) = reqwest::Url::parse(value)
            && matches!(parsed.scheme(), "http" | "https" | "ssh" | "git")
            && let Some(host) = parsed.host_str()
            && let Some(path) = canonical_repository_path(parsed.path())
        {
            return canonical_repository_identity(host, parsed.port(), &path);
        }
        return UNRECOGNIZED.to_owned();
    }

    if let Some((authority, path)) = value.split_once(':')
        && !authority.contains('/')
        && !path
            .split('/')
            .next()
            .is_some_and(|component| component.contains('@') || component.parse::<u16>().is_ok())
        && let Some(path) = canonical_repository_path(path)
    {
        let host = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        if !host.is_empty() {
            return canonical_repository_identity(host, None, &path);
        }
    }

    if let Some((authority, path)) = value.split_once('/')
        && let Some(path) = canonical_repository_path(path)
    {
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let (host, port) = authority
            .rsplit_once(':')
            .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host, Some(port))))
            .unwrap_or((authority, None));
        if !host.is_empty() {
            return canonical_repository_identity(host, port, &path);
        }
    }

    UNRECOGNIZED.to_owned()
}

fn canonical_repository_identity(host: &str, port: Option<u16>, path: &str) -> String {
    let host = host.to_ascii_lowercase();
    let path = if host == "github.com" {
        path.to_ascii_lowercase()
    } else {
        path.to_owned()
    };
    match port {
        Some(port) => format!("{host}:{port}/{path}"),
        None => format!("{host}/{path}"),
    }
}

fn canonical_repository_path(value: &str) -> Option<String> {
    let mut path = value.trim_matches('/');
    if let Some(without_suffix) = path.strip_suffix(".git") {
        path = without_suffix.trim_end_matches('/');
    }
    if path.is_empty()
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return None;
    }
    Some(path.to_owned())
}

fn canonical_working_directory(
    requested: Option<&str>,
    workspace: &str,
) -> Result<String, DomainCommandError> {
    let path = match requested {
        None => Path::new(workspace),
        Some(value) if value.trim().is_empty() => {
            return Err(DomainCommandError::Invalid(
                "working_directory must not be empty when supplied".to_owned(),
            ));
        }
        Some(value) => Path::new(value),
    };
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        DomainCommandError::Invalid(format!(
            "working_directory {} is unavailable: {error}",
            path.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(DomainCommandError::Invalid(format!(
            "working_directory {} is not a directory",
            canonical.display()
        )));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn validated_bridge_title(
    requested: &str,
    fallback: Option<&str>,
) -> Result<String, DomainCommandError> {
    let candidate = if requested.trim().is_empty() {
        fallback.unwrap_or("Nakode session")
    } else {
        requested
    };
    let title = candidate.lines().next().unwrap_or_default().trim();
    if title.chars().any(char::is_control) {
        return Err(DomainCommandError::Invalid(
            "bridge display title contains control characters".to_owned(),
        ));
    }
    let title = if title.is_empty() {
        "Nakode session".to_owned()
    } else {
        title.chars().take(100).collect()
    };
    Ok(title)
}

fn validate_external_identity(
    field: &str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), DomainCommandError> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || value.chars().any(char::is_control)
        || value.trim() != value
    {
        return Err(DomainCommandError::Invalid(format!(
            "{field} must be a non-empty normalized value of at most {maximum_bytes} bytes"
        )));
    }
    Ok(())
}

fn validate_delivery_plan(body_sha256: &str, _part_count: u64) -> Result<(), DomainCommandError> {
    if body_sha256.len() != 64
        || !body_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DomainCommandError::Invalid(
            "bridge delivery body_sha256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn bridge_prompt_id(external_event_id: &str) -> String {
    let digest = Sha256::digest(external_event_id.as_bytes());
    let mut id = String::from("bridge-");
    for byte in &digest[..16] {
        write!(&mut id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    id
}

fn remember_inbound_event(
    bridge: &mut SessionBridgeRecord,
    external_event_id: &str,
) -> Option<String> {
    if bridge
        .recent_inbound_event_ids
        .iter()
        .any(|existing| existing == external_event_id)
    {
        return None;
    }
    let evicted = (bridge.recent_inbound_event_ids.len() == RECENT_INBOUND_CACHE_CAPACITY)
        .then(|| bridge.recent_inbound_event_ids.remove(0));
    bridge
        .recent_inbound_event_ids
        .push(external_event_id.to_owned());
    evicted
}

fn bump_bridge_revision(bridge: &mut SessionBridgeRecord) {
    bridge.revision = bridge.revision.saturating_add(1);
    bridge.updated_at_ms = unix_timestamp_ms();
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

fn session_bridge_view(bridge: &SessionBridgeRecord) -> nakode_protocol::SessionBridgeView {
    nakode_protocol::SessionBridgeView {
        session_id: SessionId::from(bridge.session_id.clone()),
        workspace_id: crate::state::projection::workspace_id(&bridge.workspace),
        kind: bridge.kind,
        lifecycle: bridge.lifecycle,
        display_title: bridge.display_title.clone(),
        revision: bridge.revision,
        transport: bridge.transport.clone(),
        external_parent_id: bridge.external_parent_id.clone(),
        external_thread_id: bridge.external_thread_id.clone(),
        last_projected: bridge.last_projected.as_ref().map(bridge_projection_view),
        delivery: bridge
            .delivery
            .as_ref()
            .map(|delivery| nakode_protocol::BridgeDeliveryView {
                projection: BridgeProjectionView {
                    kind: delivery.projection_kind,
                    turn_id: TurnId::from(delivery.turn_id.clone()),
                },
                previous_projection: delivery
                    .previous_projection
                    .as_ref()
                    .map(bridge_projection_view),
                body_sha256: delivery.body_sha256.clone(),
                part_count: delivery.part_count,
                completed_parts: delivery.completed_parts,
                last_external_message_id: delivery.last_external_message_id.clone(),
            }),
        live_turn_id: bridge.live_turn_id.clone().map(TurnId::from),
        live_external_message_id: bridge.live_external_message_id.clone(),
        active_source_message_id: bridge.active_source_message_id.clone(),
    }
}

fn bridge_projection_view(projection: &BridgeProjectionRecord) -> BridgeProjectionView {
    BridgeProjectionView {
        kind: projection.kind,
        turn_id: TurnId::from(projection.turn_id.clone()),
    }
}

#[derive(Clone)]
enum TranscriptTarget {
    Session {
        session_id: SessionId,
        revision: u64,
    },
    Run {
        session_id: SessionId,
        revision: u64,
        run_id: RunId,
    },
}

impl TranscriptTarget {
    fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::Session { .. } => None,
            Self::Run { run_id, .. } => Some(run_id),
        }
    }

    fn created(&self, entry: TranscriptEntryView) -> ViewEvent {
        match self {
            Self::Session {
                session_id,
                revision,
            } => ViewEvent::TranscriptEntryCreated {
                session_id: session_id.clone(),
                revision: *revision,
                entry,
            },
            Self::Run {
                session_id,
                revision,
                run_id,
            } => ViewEvent::RunTranscriptEntryCreated {
                session_id: session_id.clone(),
                revision: *revision,
                run_id: run_id.clone(),
                entry,
            },
        }
    }

    fn patched(&self, entry: TranscriptEntryView) -> ViewEvent {
        match self {
            Self::Session {
                session_id,
                revision,
            } => ViewEvent::TranscriptEntryPatched {
                session_id: session_id.clone(),
                revision: *revision,
                entry,
            },
            Self::Run {
                session_id,
                revision,
                run_id,
            } => ViewEvent::RunTranscriptEntryPatched {
                session_id: session_id.clone(),
                revision: *revision,
                run_id: run_id.clone(),
                entry,
            },
        }
    }

    fn delta(
        &self,
        entry_id: EntryId,
        append_at_byte: u64,
        delta: String,
        status: TranscriptEntryStatus,
    ) -> ViewEvent {
        match self {
            Self::Session {
                session_id,
                revision,
            } => ViewEvent::TranscriptEntryDelta {
                session_id: session_id.clone(),
                revision: *revision,
                entry_id,
                append_at_byte,
                delta,
                status,
            },
            Self::Run {
                session_id,
                revision,
                run_id,
            } => ViewEvent::RunTranscriptEntryDelta {
                session_id: session_id.clone(),
                revision: *revision,
                run_id: run_id.clone(),
                entry_id,
                append_at_byte,
                delta,
                status,
            },
        }
    }

    fn window(&self, window: TranscriptWindowView) -> ViewEvent {
        match self {
            Self::Session {
                session_id,
                revision,
            } => ViewEvent::TranscriptWindowChanged {
                session_id: session_id.clone(),
                revision: *revision,
                window,
            },
            Self::Run {
                session_id,
                revision,
                run_id,
            } => ViewEvent::RunTranscriptWindowChanged {
                session_id: session_id.clone(),
                revision: *revision,
                run_id: run_id.clone(),
                window,
            },
        }
    }
}

fn workspace_metadata(view: &nakode_protocol::BootstrapView) -> nakode_protocol::BootstrapView {
    let mut metadata = view.clone();
    metadata.providers.clear();
    metadata.sessions.clear();
    metadata.active_session = None;
    metadata
}

fn session_metadata(view: &SessionView) -> SessionMetadataView {
    SessionMetadataView {
        workspace_id: view.workspace_id.clone(),
        title: view.title.clone(),
        status_message: view.status_message.clone(),
        diagnostic_count: view.diagnostic_count,
        activity: view.activity,
        selected_provider_id: view.selected_provider_id.clone(),
        selected_model_id: view.selected_model_id.clone(),
        selected_model_options: view.selected_model_options.clone(),
        active_agent_session: view.active_agent_session.clone(),
        active_turn: view.active_turn.clone(),
        last_turn: view.last_turn.clone(),
        next_turn_configuration_pending: view.next_turn_configuration_pending,
        next_turn_transition: view.next_turn_transition.clone(),
        context_usage: view.context_usage,
        recoverable_prompt: view.recoverable_prompt.clone(),
        notices: view.notices.clone(),
    }
}

fn run_metadata(view: &RunView) -> RunMetadataView {
    RunMetadataView {
        id: view.id.clone(),
        agent_slug: view.agent_slug.clone(),
        provider_id: view.provider_id.clone(),
        objective: view.objective.clone(),
        objective_start_byte: view.objective_start_byte,
        objective_total_bytes: view.objective_total_bytes,
        status: view.status,
        latest_activity: view.latest_activity.clone(),
        latest_activity_start_byte: view.latest_activity_start_byte,
        latest_activity_total_bytes: view.latest_activity_total_bytes,
        outcome: view.outcome.clone(),
        outcome_start_byte: view.outcome_start_byte,
        outcome_total_bytes: view.outcome_total_bytes,
        result: view.result.clone(),
        result_start_byte: view.result_start_byte,
        result_total_bytes: view.result_total_bytes,
    }
}

fn run_ids(view: &SessionView) -> Vec<RunId> {
    view.runs.iter().map(|run| run.id.clone()).collect()
}

fn transcript_lengths(
    state: &crate::state::DomainState,
    run_id: Option<&RunId>,
    page: &TranscriptPage,
) -> HashMap<EntryId, usize> {
    page.entries
        .iter()
        .filter_map(|entry| {
            crate::state::projection::transcript_entry_body(state, run_id, &entry.id)
                .map(|body| (entry.id.clone(), body.len()))
        })
        .collect()
}

fn transcript_events(
    state: &crate::state::DomainState,
    target: &TranscriptTarget,
    previous: &TranscriptPage,
    previous_lengths: &HashMap<EntryId, usize>,
    current: &TranscriptPage,
    current_lengths: &HashMap<EntryId, usize>,
) -> Vec<ViewEvent> {
    let mut events = Vec::new();
    for entry in &current.entries {
        let Some(previous_entry) = previous
            .entries
            .iter()
            .find(|candidate| candidate.id == entry.id)
        else {
            events.push(target.created(entry.clone()));
            continue;
        };
        if previous_entry == entry {
            continue;
        }
        let delta = appended_transcript_delta(
            state,
            target.run_id(),
            previous_entry,
            previous_lengths.get(&entry.id).copied(),
            current_lengths.get(&entry.id).copied(),
        );
        if let Some(delta) = delta
            && previous_entry.kind == entry.kind
            && previous_entry.title == entry.title
            && previous_entry.artifacts == entry.artifacts
            && !delta.is_empty()
        {
            let mut append_at_byte = previous_lengths
                .get(&entry.id)
                .copied()
                .and_then(|length| u64::try_from(length).ok())
                .unwrap_or_default();
            for chunk in utf8_chunks(delta, MAX_TRANSCRIPT_DELTA_BYTES) {
                events.push(target.delta(
                    entry.id.clone(),
                    append_at_byte,
                    chunk.to_owned(),
                    entry.status,
                ));
                append_at_byte =
                    append_at_byte.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            }
        } else {
            events.push(target.patched(entry.clone()));
        }
    }

    let previous_window = transcript_window(previous);
    let current_window = transcript_window(current);
    if previous_window != current_window {
        events.push(target.window(current_window));
    }
    events
}

fn appended_transcript_delta<'a>(
    state: &'a crate::state::DomainState,
    run_id: Option<&RunId>,
    previous: &TranscriptEntryView,
    previous_length: Option<usize>,
    current_length: Option<usize>,
) -> Option<&'a str> {
    let previous_length = previous_length?;
    let current_length = current_length?;
    if current_length < previous_length || previous.body.len() > previous_length {
        return None;
    }
    let body = crate::state::projection::transcript_entry_body(state, run_id, &previous.id)?;
    let visible_start = previous_length.saturating_sub(previous.body.len());
    if body.get(visible_start..previous_length) != Some(previous.body.as_str()) {
        return None;
    }
    body.get(previous_length..current_length)
}

fn transcript_window(page: &TranscriptPage) -> TranscriptWindowView {
    TranscriptWindowView {
        entry_ids: page.entries.iter().map(|entry| entry.id.clone()).collect(),
        has_earlier: page.has_earlier,
        stream_active: page.stream_active,
        stream_label: page.stream_label.clone(),
    }
}

fn utf8_chunks(value: &str, maximum_bytes: usize) -> Vec<&str> {
    if value.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < value.len() {
        let mut end = start.saturating_add(maximum_bytes).min(value.len());
        while end > start && !value.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        if end == start {
            end = value[start..]
                .char_indices()
                .nth(1)
                .map_or(value.len(), |(offset, _)| start.saturating_add(offset));
        }
        chunks.push(&value[start..end]);
        start = end;
    }
    chunks
}

impl DispatchOutcome {
    const fn unchanged() -> Self {
        Self {
            effects: Vec::new(),
            effect_session: None,
            changed: false,
            command_response: None,
        }
    }

    pub(crate) fn bridge_continuation(&self) -> Option<BridgeContinuationDisposition> {
        self.command_response
            .as_ref()?
            .result
            .as_ref()
            .ok()?
            .bridge_continuation
    }

    pub(crate) fn respond_with_error(mut self, error: ServiceError) {
        if let Some(response) = self.command_response.take() {
            let _ = response.respond.send(Err(error));
        }
    }

    pub(crate) fn respond(self) {
        if let Some(response) = self.command_response {
            let _ = response.respond.send(response.result);
        }
    }
}

fn validated_relative_path(path: &str) -> Result<PathBuf, DomainCommandError> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(DomainCommandError::Invalid(
            "attachment paths must be workspace-relative".to_owned(),
        ));
    }
    Ok(path.to_path_buf())
}

fn command_digest(command: &Command) -> [u8; 32] {
    let encoded = serde_json::to_vec(command).unwrap_or_default();
    Sha256::digest(encoded).into()
}

fn soul_domain_error(error: &SoulError) -> DomainCommandError {
    match error {
        SoulError::Conflict { .. } | SoulError::Appeared => {
            DomainCommandError::Conflict(error.to_string())
        }
        SoulError::MissingDirectory | SoulError::Read { .. } | SoulError::Write { .. } => {
            DomainCommandError::Invalid(error.to_string())
        }
    }
}

fn soul_service_error(error: &SoulError) -> ServiceError {
    let code = match error {
        SoulError::Conflict { .. } | SoulError::Appeared => ErrorCode::Conflict,
        SoulError::MissingDirectory | SoulError::Read { .. } | SoulError::Write { .. } => {
            ErrorCode::Internal
        }
    };
    service_error(code, &error.to_string(), false)
}

fn domain_error(error: DomainCommandError) -> ServiceError {
    let (code, message) = match error {
        DomainCommandError::Invalid(message) => (
            ErrorCode::InvalidRequest,
            format!("invalid command: {message}"),
        ),
        DomainCommandError::Conflict(message) => (
            ErrorCode::Conflict,
            format!("command conflicts with current state: {message}"),
        ),
        DomainCommandError::NotFound(message) => (
            ErrorCode::NotFound,
            format!("resource was not found: {message}"),
        ),
        DomainCommandError::Unsupported(message) => (
            ErrorCode::CapabilityUnsupported,
            format!("capability is unsupported: {message}"),
        ),
    };
    service_error(code, &message, false)
}

fn transcript_body_window_error(
    error: crate::state::projection::TranscriptBodyWindowError,
    entry_id: &EntryId,
) -> ServiceError {
    use crate::state::projection::TranscriptBodyWindowError;

    match error {
        TranscriptBodyWindowError::EntryNotFound => {
            not_found("transcript entry", entry_id.as_str())
        }
        TranscriptBodyWindowError::LimitOutOfBounds { actual, maximum } => service_error(
            ErrorCode::InvalidRequest,
            &format!("body window limit must be between 1 and {maximum} bytes; received {actual}"),
            false,
        ),
        TranscriptBodyWindowError::CursorOutOfBounds { actual, total } => service_error(
            ErrorCode::InvalidRequest,
            &format!("body window cursor {actual} exceeds entry length {total}"),
            false,
        ),
        TranscriptBodyWindowError::CursorNotUtf8Boundary { actual } => service_error(
            ErrorCode::InvalidRequest,
            &format!("body window cursor {actual} is not a UTF-8 character boundary"),
            false,
        ),
        TranscriptBodyWindowError::LimitTooSmallForCharacter { minimum } => service_error(
            ErrorCode::InvalidRequest,
            &format!(
                "body window limit is too small for the preceding UTF-8 character; minimum is {minimum} bytes"
            ),
            false,
        ),
    }
}

fn run_text_window_error(
    error: crate::state::projection::RunTextWindowError,
    run_id: &RunId,
    field: RunTextField,
) -> ServiceError {
    use crate::state::projection::RunTextWindowError;

    match error {
        RunTextWindowError::RunNotFound => not_found("run", run_id.as_str()),
        RunTextWindowError::FieldUnavailable => service_error(
            ErrorCode::NotFound,
            &format!(
                "run text field {field:?} is not available for {:?}",
                run_id.as_str()
            ),
            false,
        ),
        RunTextWindowError::LimitOutOfBounds { actual, maximum } => service_error(
            ErrorCode::InvalidRequest,
            &format!(
                "run text window limit must be between 1 and {maximum} bytes; received {actual}"
            ),
            false,
        ),
        RunTextWindowError::CursorOutOfBounds { actual, total } => service_error(
            ErrorCode::InvalidRequest,
            &format!("run text window cursor {actual} exceeds field length {total}"),
            false,
        ),
        RunTextWindowError::CursorNotUtf8Boundary { actual } => service_error(
            ErrorCode::InvalidRequest,
            &format!("run text window cursor {actual} is not a UTF-8 character boundary"),
            false,
        ),
        RunTextWindowError::LimitTooSmallForCharacter { minimum } => service_error(
            ErrorCode::InvalidRequest,
            &format!(
                "run text window limit is too small for the preceding UTF-8 character; minimum is {minimum} bytes"
            ),
            false,
        ),
    }
}

fn not_found(kind: &str, id: &str) -> ServiceError {
    service_error(
        ErrorCode::NotFound,
        &format!("{kind} {id:?} was not found"),
        false,
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
    use std::{
        collections::{BTreeSet, HashMap},
        fs,
        process::Command as ProcessCommand,
    };

    use nakode_protocol::{
        AgentDefinitionInput, BridgeContinuationDisposition, BridgeLifecycle, BridgeProjectionKind,
        BridgeProjectionView, ClientId, Command, ErrorCode, ExternalToolDefinition, IdempotencyKey,
        MAX_API_MESSAGE_BYTES, MAX_ARTIFACT_BYTES, MAX_RUN_TEXT_BYTES, MAX_TRANSCRIPT_DELTA_BYTES,
        ModelId, ModelOptions, ModelTarget, OrchestratorKind,
        PromptAttachment as ProtocolPromptAttachment, PromptInput, ProviderAuthenticationView,
        ProviderId, Query, QueryResult, RunId, RunTextField, ServiceCapabilities,
        ServiceCapability, SessionBridgeIntent, SessionId, SessionToolConfiguration,
        SubscriptionScope, SubscriptionView, TranscriptOwner, TurnId, ViewEvent, WorkspaceId,
    };
    use nakode_server::{PublishedEvent, ServerEndpoint, ServerRequest};
    use tokio::sync::broadcast;

    use super::{
        IDEMPOTENCY_CAPACITY, ServerCore, inspect_workspace_path, sanitized_repository_identity,
        unix_timestamp_ms,
    };
    use crate::{
        agent::{AgentCatalog, AgentDefinition},
        backend::{
            BackendCapabilities, BackendCommand, BackendEvent, BackendIdentity, BackendOperation,
            CLAUDE_PROVIDER, CODEX_PROVIDER, CapabilitySupport, ModelCapabilities, ModelInfo,
            PromptImage, TurnOutcome,
        },
        domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
        personality::PromptAddenda,
        service::ServiceEngine,
        session::{
            BridgeInboundTurnOriginRecord, BridgeProjectionRecord, ProviderRecord,
            SessionBridgeRecord, SessionRecord, SubagentObservability, SubagentRecord,
        },
        soul::{SoulSource, SoulStore},
        state::{AppState, DomainCommandError},
    };

    fn install_available_tools(core: &mut ServerCore, provider: &str, tools: &[&str]) {
        core.install_available_builtin_tools(&HashMap::from([(
            provider.to_owned(),
            tools.iter().map(|name| (*name).to_owned()).collect(),
        )]));
    }

    fn dashboard_tools(name: &str, replace_builtin_tools: bool) -> SessionToolConfiguration {
        SessionToolConfiguration {
            tools: vec![ExternalToolDefinition {
                name: name.to_owned(),
                description: format!("Run {name}"),
                input_schema_json: r#"{"type":"object","properties":{}}"#.to_owned(),
            }],
            replace_builtin_tools,
            allowed_builtin_tools: None,
        }
    }

    fn drain_publications(
        receiver: &mut broadcast::Receiver<PublishedEvent>,
    ) -> Vec<PublishedEvent> {
        let mut events = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(event) => events.push(event),
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    panic!("test publication receiver lagged")
                }
            }
        }
        events
    }

    fn project_workspace() -> &'static str {
        std::fs::create_dir_all("/tmp/project").expect("test workspace");
        "/tmp/project"
    }

    fn ready_codex_server() -> (ServerCore, SessionId) {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        state.provider_session_id = Some("thread-1".to_owned());
        let session_id = SessionId::from(state.nakode_session_id.clone());
        (
            ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new()),
            session_id,
        )
    }

    fn ready_external_tools_server() -> (ServerCore, SessionId) {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    external_tools: CapabilitySupport::Supported,
                    resume: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.provider_session_id = Some("thread-1".to_owned());
        let session_id = SessionId::from(state.nakode_session_id.clone());
        (
            ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new()),
            session_id,
        )
    }

    fn create_default_session(
        core: &mut ServerCore,
        workspace_id: &WorkspaceId,
    ) -> super::DomainCommandOutcome {
        core.create_session_command(workspace_id, None, &ModelOptions::default(), None)
    }

    #[test]
    fn repository_identity_removes_credentials_and_normalizes_transport_spelling() {
        assert_eq!(
            sanitized_repository_identity("https://token:secret@example.invalid/team/repo.git"),
            "example.invalid/team/repo"
        );
        assert_eq!(
            sanitized_repository_identity("git@example.invalid:team/repo.git"),
            "example.invalid/team/repo"
        );
        assert_eq!(
            sanitized_repository_identity("ssh://git@example.invalid/team/repo.git"),
            "example.invalid/team/repo"
        );
        assert_eq!(
            sanitized_repository_identity("https://github.com/QuillDev/Nakode.git"),
            "github.com/quilldev/nakode"
        );
        assert_eq!(
            sanitized_repository_identity("github.com/quilldev/nakode"),
            "github.com/quilldev/nakode"
        );
        assert_eq!(
            sanitized_repository_identity("token:secret@example.invalid/team/Repo.git/"),
            "example.invalid/team/Repo"
        );
        assert_eq!(
            sanitized_repository_identity("example.invalid:8443/team/Repo.git/"),
            "example.invalid:8443/team/Repo"
        );
        assert_eq!(
            sanitized_repository_identity("https://example.invalid/team/Repo.git/"),
            "example.invalid/team/Repo"
        );
        assert_eq!(
            sanitized_repository_identity("file:///tmp/repo.git"),
            "[unrecognized repository]"
        );
        assert_eq!(
            sanitized_repository_identity("https://example.invalid/team/repo.git?token=secret"),
            "[unrecognized repository]"
        );
    }

    #[test]
    fn workspace_path_inspection_reports_git_identity_and_dirty_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        for arguments in [
            vec!["init", "-b", "main"],
            vec!["config", "user.email", "nakode@example.invalid"],
            vec!["config", "user.name", "Nakode Test"],
            vec![
                "remote",
                "add",
                "origin",
                "ssh://example.invalid/team/repo.git",
            ],
        ] {
            assert!(
                ProcessCommand::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(arguments)
                    .status()
                    .expect("run git")
                    .success()
            );
        }
        fs::write(directory.path().join("tracked.txt"), "initial\n").expect("write fixture");
        for arguments in [vec!["add", "tracked.txt"], vec!["commit", "-m", "initial"]] {
            assert!(
                ProcessCommand::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(arguments)
                    .status()
                    .expect("run git")
                    .success()
            );
        }

        let clean = inspect_workspace_path(
            directory.path().to_str().unwrap(),
            Some("ssh://example.invalid/team/repo.git"),
        )
        .expect("inspect clean repository");
        assert_eq!(
            clean.canonical_path,
            directory.path().canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(
            clean.git_repository.as_deref(),
            Some("example.invalid/team/repo")
        );
        assert_eq!(clean.branch.as_deref(), Some("main"));
        assert_eq!(clean.revision.as_deref().map(str::len), Some(40));
        assert!(!clean.dirty);

        fs::write(directory.path().join("untracked.txt"), "dirty\n").expect("dirty fixture");
        assert!(
            inspect_workspace_path(directory.path().to_str().unwrap(), None)
                .expect("inspect dirty repository")
                .dirty
        );
        let mismatch = inspect_workspace_path(
            directory.path().to_str().unwrap(),
            Some("ssh://example.invalid/other.git"),
        )
        .expect_err("reject repository mismatch");
        assert_eq!(mismatch.code, ErrorCode::Conflict);
    }

    #[test]
    fn workspace_path_inspection_rejects_missing_or_non_directory_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let file = directory.path().join("file.txt");
        fs::write(&file, "not a directory").expect("write fixture");
        for path in [file, directory.path().join("missing")] {
            let error = inspect_workspace_path(path.to_str().unwrap(), None)
                .expect_err("invalid inspection path");
            assert_eq!(error.code, ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn rootless_and_explicit_sessions_share_logical_workspace_but_keep_distinct_cwds() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let explicit = tempfile::tempdir().expect("explicit cwd");
        let explicit_canonical = explicit.path().canonicalize().expect("canonical cwd");

        let (rootless, _) = core
            .create_session_command(&workspace_id, None, &ModelOptions::default(), None)
            .expect("rootless session");
        let (rooted, _) = core
            .create_session_command_with_mcp(
                &workspace_id,
                Some(explicit.path().to_str().expect("utf8 path")),
                None,
                None,
                &ModelOptions::default(),
                None,
                None,
                None,
                None,
            )
            .expect("explicitly rooted session");
        let rootless_id = SessionId::from(rootless.resource_id.expect("rootless id"));
        let rooted_id = SessionId::from(rooted.resource_id.expect("rooted id"));

        assert_eq!(
            core.engine_for(&rootless_id)
                .expect("rootless engine")
                .state()
                .working_directory,
            std::fs::canonicalize(project_workspace())
                .expect("canonical workspace")
                .to_string_lossy()
        );
        assert_eq!(
            core.engine_for(&rooted_id)
                .expect("rooted engine")
                .state()
                .working_directory,
            explicit_canonical.to_string_lossy()
        );
        let projected = core.workspace_bootstrap();
        let rooted_summary = projected
            .sessions
            .iter()
            .find(|session| session.id == rooted_id)
            .expect("rooted summary");
        assert_eq!(rooted_summary.workspace_id, workspace_id);
        assert_eq!(
            rooted_summary.working_directory,
            explicit_canonical.to_string_lossy()
        );
    }

    #[test]
    fn session_creation_rejects_missing_and_non_directory_cwds() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let directory = tempfile::tempdir().expect("cwd fixture");
        let file = directory.path().join("file");
        std::fs::write(&file, "not a directory").expect("write fixture");
        for invalid in [directory.path().join("missing"), file] {
            let result = core.create_session_command_with_mcp(
                &workspace_id,
                Some(invalid.to_str().expect("utf8 path")),
                None,
                None,
                &ModelOptions::default(),
                None,
                None,
                None,
                None,
            );
            assert!(
                result.is_err(),
                "accepted invalid cwd {}",
                invalid.display()
            );
        }
    }

    #[test]
    fn open_restores_persisted_cwd_and_fails_after_it_is_deleted() {
        let (mut core, _) = ready_external_tools_server();
        let directory = tempfile::tempdir().expect("persisted cwd");
        let canonical = directory.path().canonicalize().expect("canonical cwd");
        let restored_id = SessionId::from("restored-cwd-session");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-restored-cwd".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: canonical.to_string_lossy().into_owned(),
            title: "Restored cwd".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);

        let (_, effects) = core
            .open_session_command(&restored_id, None)
            .expect("persisted cwd restores");
        assert!(effects.iter().any(
            |effect| matches!(effect, crate::state::Effect::LoadSubagents(parent) if parent == restored_id.as_str())
        ));
        assert_eq!(
            core.engine_for(&restored_id)
                .expect("restored engine")
                .state()
                .working_directory,
            canonical.to_string_lossy()
        );
        drop(directory);
        let error = core
            .open_session_command(&restored_id, None)
            .expect_err("deleted cwd must not fall back to workspace");
        assert!(error.to_string().contains("working_directory"));
    }

    #[test]
    fn rejected_restored_open_does_not_load_subagents() {
        let (mut core, _) = ready_codex_server();
        let directory = tempfile::tempdir().expect("persisted cwd");
        let restored_id = SessionId::from("resume-unsupported-session");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-resume-unsupported".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: directory
                .path()
                .canonicalize()
                .expect("canonical cwd")
                .to_string_lossy()
                .into_owned(),
            title: "Resume unsupported".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);

        let (_, effects) = core
            .open_session_command(&restored_id, None)
            .expect("open reports the resume rejection in session state");

        assert!(effects.is_empty());
        assert!(
            core.engine_for(&restored_id)
                .expect("restored engine")
                .state()
                .status_message
                .contains("does not support session resume")
        );
    }

    #[test]
    fn bridge_intent_is_created_atomically_and_projected_with_the_session() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let (accepted, effects) = core
            .create_session_command_with_mcp(
                &workspace_id,
                None,
                Some("Dashboard title"),
                None,
                &ModelOptions::default(),
                None,
                None,
                Some(SessionBridgeIntent {
                    kind: OrchestratorKind::Chat,
                    lifecycle: BridgeLifecycle::Open,
                    display_title: String::new(),
                }),
                None,
            )
            .expect("bridged session");
        let session_id = SessionId::from(accepted.resource_id.expect("session id"));
        assert!(matches!(
            effects.first(),
            Some(crate::state::Effect::PersistSessionBridge(bridge))
                if bridge.session_id == session_id.as_str()
                    && bridge.display_title == "Dashboard title"
        ));
        let projected = core.workspace_bootstrap();
        let bridge = projected
            .session_bridges
            .iter()
            .find(|bridge| bridge.session_id == session_id)
            .expect("bridge projection");
        assert_eq!(bridge.kind, OrchestratorKind::Chat);
        assert_eq!(bridge.lifecycle, BridgeLifecycle::Open);
        assert!(bridge.external_thread_id.is_none());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bridge_binding_lifecycle_and_final_delivery_are_idempotent() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let (accepted, _) = core
            .create_session_command_with_mcp(
                &workspace_id,
                None,
                Some("Agent review"),
                None,
                &ModelOptions::default(),
                None,
                None,
                Some(SessionBridgeIntent {
                    kind: OrchestratorKind::Agent,
                    lifecycle: BridgeLifecycle::Open,
                    display_title: "Agent review".to_owned(),
                }),
                None,
            )
            .expect("bridged session");
        let session_id = SessionId::from(accepted.resource_id.expect("session id"));
        let (_, bound) = core
            .bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "101")
            .expect("bind");
        assert_eq!(bound.len(), 1);
        let (_, duplicate) = core
            .bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "101")
            .expect("same binding is idempotent");
        assert!(duplicate.is_empty());
        assert!(
            core.bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "102")
                .is_err()
        );
        let (other, _) = core
            .create_session_command_with_mcp(
                &workspace_id,
                None,
                Some("Other chat"),
                None,
                &ModelOptions::default(),
                None,
                None,
                Some(SessionBridgeIntent {
                    kind: OrchestratorKind::Chat,
                    lifecycle: BridgeLifecycle::Open,
                    display_title: "Other chat".to_owned(),
                }),
                None,
            )
            .expect("second bridged session");
        let other_session_id = SessionId::from(other.resource_id.expect("other session id"));
        assert!(
            core.bind_session_bridge_thread_command(
                &other_session_id,
                "thread-transport",
                "100",
                "101",
            )
            .is_err(),
            "one external thread cannot be cross-wired to concurrent logical sessions"
        );
        assert!(
            core.session_bridge(&other_session_id)
                .expect("other bridge")
                .external_thread_id
                .is_none()
        );

        let turn_id = TurnId::from("turn-1");
        let body_sha256 = "a".repeat(64);
        core.prepare_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            None,
            &body_sha256,
            2,
        )
        .expect("prepare");
        let (_, duplicate_prepare) = core
            .prepare_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
                None,
                &body_sha256,
                2,
            )
            .expect("lost prepare response is idempotent");
        assert!(duplicate_prepare.is_empty());
        let prepared = core
            .session_bridge(&session_id)
            .expect("bridge")
            .delivery
            .as_ref()
            .expect("prepared delivery");
        assert_eq!(prepared.completed_parts, 0);
        assert!(prepared.last_external_message_id.is_none());
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            0,
            "200",
        )
        .expect("first part");
        let (_, duplicate_first_part) = core
            .complete_bridge_delivery_part_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
                0,
                "200",
            )
            .expect("lost-response retry is idempotent");
        assert!(duplicate_first_part.is_empty());
        assert!(
            core.complete_bridge_delivery_part_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
                0,
                "different"
            )
            .is_err(),
            "the last completed part cannot be acknowledged with a different message"
        );
        let delivery = core
            .session_bridge(&session_id)
            .expect("bridge")
            .delivery
            .as_ref()
            .expect("pending delivery");
        assert_eq!(delivery.completed_parts, 1);
        assert_eq!(delivery.last_external_message_id.as_deref(), Some("200"));
        assert_eq!(delivery.part_count, 2);
        assert!(
            core.complete_bridge_delivery_part_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
                2,
                "202"
            )
            .is_err()
        );
        assert!(
            core.finalize_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
            )
            .is_err()
        );

        core.clear_session_bridge_thread_command(&session_id, "thread-transport", "101")
            .expect("clear missing thread binding");
        let reset = core
            .session_bridge(&session_id)
            .expect("bridge")
            .delivery
            .as_ref()
            .expect("delivery remains prepared for replacement thread");
        assert_eq!(reset.completed_parts, 0);
        assert!(reset.last_external_message_id.is_none());
        core.bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "102")
            .expect("bind replacement thread");
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            0,
            "300",
        )
        .expect("first part is resent to replacement thread");
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            1,
            "301",
        )
        .expect("second part");
        {
            let bridge = core.session_bridge_mut(&session_id).expect("bridge");
            bridge.live_turn_id = Some("turn-2".to_owned());
            bridge.live_external_message_id = Some("newer-live-message".to_owned());
            bridge.active_source_message_id = Some("newer-source-message".to_owned());
        }
        core.finalize_bridge_delivery_command_with_source(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            Some("turn-1-source-message"),
        )
        .expect("finalize");
        {
            let bridge = core.session_bridge(&session_id).expect("bridge");
            assert_eq!(bridge.live_turn_id.as_deref(), Some("turn-2"));
            assert_eq!(
                bridge.live_external_message_id.as_deref(),
                Some("newer-live-message")
            );
            assert_eq!(
                bridge.active_source_message_id.as_deref(),
                Some("newer-source-message")
            );
        }
        let (_, duplicate_finalize) = core
            .finalize_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
            )
            .expect("finalization is idempotent");
        assert!(duplicate_finalize.is_empty());
        let bridge = core.session_bridge(&session_id).expect("bridge");
        assert_eq!(
            bridge.last_projected.as_ref(),
            Some(&BridgeProjectionRecord {
                kind: BridgeProjectionKind::Assistant,
                turn_id: "turn-1".to_owned(),
            })
        );
        assert!(bridge.delivery.is_none());

        core.set_session_bridge_lifecycle_command(&session_id, BridgeLifecycle::Archived)
            .expect("archive");
        assert_eq!(
            core.session_bridge(&session_id).expect("bridge").lifecycle,
            BridgeLifecycle::Archived
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bridge_projection_cursor_enforces_user_before_assistant_and_rejects_stale_workers() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let (accepted, _) = core
            .create_session_command_with_mcp(
                &workspace_id,
                None,
                Some("Ordered projection"),
                None,
                &ModelOptions::default(),
                None,
                None,
                Some(SessionBridgeIntent {
                    kind: OrchestratorKind::Chat,
                    lifecycle: BridgeLifecycle::Open,
                    display_title: "Ordered projection".to_owned(),
                }),
                None,
            )
            .expect("bridged session");
        let session_id = SessionId::from(accepted.resource_id.expect("session id"));
        let turn_id = TurnId::from("turn-ordered");
        let body_sha256 = "b".repeat(64);

        core.prepare_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::User,
            &turn_id,
            None,
            &body_sha256,
            1,
        )
        .expect("prepare user");
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::User,
            &turn_id,
            0,
            "user-message",
        )
        .expect("checkpoint user");
        core.finalize_bridge_delivery_command(&session_id, BridgeProjectionKind::User, &turn_id)
            .expect("finalize user");

        assert!(
            core.prepare_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::Assistant,
                &turn_id,
                None,
                &body_sha256,
                1,
            )
            .is_err(),
            "a stale worker cannot prepare against the pre-user cursor"
        );
        let user_cursor = BridgeProjectionView {
            kind: BridgeProjectionKind::User,
            turn_id: turn_id.clone(),
        };
        core.prepare_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            Some(&user_cursor),
            &body_sha256,
            1,
        )
        .expect("prepare assistant after user");
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
            0,
            "assistant-message",
        )
        .expect("checkpoint assistant");
        core.finalize_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &turn_id,
        )
        .expect("finalize assistant");

        assert!(
            core.prepare_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::User,
                &TurnId::from("later-turn"),
                Some(&user_cursor),
                &body_sha256,
                1,
            )
            .is_err(),
            "a stale predecessor cannot regress the assistant cursor"
        );
        assert_eq!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .last_projected,
            Some(BridgeProjectionRecord {
                kind: BridgeProjectionKind::Assistant,
                turn_id: turn_id.to_string(),
            })
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bridge_continuation_rejects_busy_work_without_queueing_and_deduplicates_events() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let (accepted, _) = core
            .create_session_command_with_mcp(
                &workspace_id,
                None,
                Some("Chat"),
                None,
                &ModelOptions::default(),
                None,
                None,
                Some(SessionBridgeIntent {
                    kind: OrchestratorKind::Chat,
                    lifecycle: BridgeLifecycle::Open,
                    display_title: "Chat".to_owned(),
                }),
                None,
            )
            .expect("bridged session");
        let session_id = SessionId::from(accepted.resource_id.expect("session id"));
        core.bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "101")
            .expect("bind");
        let prompt = PromptInput {
            text: "continue".to_owned(),
            attachments: Vec::new(),
        };
        let (overload_result, overload_effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-overload",
                "message-overload",
                PromptInput {
                    text: String::new(),
                    attachments: Vec::new(),
                },
                true,
            )
            .expect("transport overload is durably consumed while ready");
        assert_eq!(
            overload_result.bridge_continuation,
            Some(BridgeContinuationDisposition::Busy)
        );
        assert!(matches!(
            overload_effects.as_slice(),
            [crate::state::Effect::PersistSessionBridge(_)]
        ));
        assert!(
            !core
                .engine_for(&session_id)
                .expect("session")
                .state()
                .is_busy()
        );
        let (continued, effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-1",
                "message-1",
                prompt.clone(),
                false,
            )
            .expect("idle continuation");
        assert_eq!(
            continued.bridge_continuation,
            Some(BridgeContinuationDisposition::Accepted)
        );
        assert!(matches!(
            effects.first(),
            Some(crate::state::Effect::PersistSessionBridge(_))
        ));
        let (duplicate_result, duplicate) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-1",
                "message-1",
                prompt.clone(),
                false,
            )
            .expect("duplicate event");
        assert_eq!(
            duplicate_result.bridge_continuation,
            Some(BridgeContinuationDisposition::Duplicate)
        );
        assert_eq!(
            duplicate_result.replayed_bridge_continuation,
            Some(BridgeContinuationDisposition::Accepted)
        );
        assert_eq!(duplicate_result.replayed_bridge_source_active, Some(true));
        assert!(duplicate.is_empty());
        let (busy_result, busy_effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-2",
                "message-2",
                prompt.clone(),
                false,
            )
            .expect("busy event is durably consumed");
        assert_eq!(
            busy_result.bridge_continuation,
            Some(BridgeContinuationDisposition::Busy)
        );
        assert!(matches!(
            busy_effects.as_slice(),
            [crate::state::Effect::PersistSessionBridge(_)]
        ));
        let (busy_duplicate, duplicate_effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-2",
                "message-2",
                prompt.clone(),
                false,
            )
            .expect("busy duplicate event");
        assert_eq!(
            busy_duplicate.bridge_continuation,
            Some(BridgeContinuationDisposition::Duplicate)
        );
        assert_eq!(
            busy_duplicate.replayed_bridge_continuation,
            Some(BridgeContinuationDisposition::Busy)
        );
        assert_eq!(busy_duplicate.replayed_bridge_source_active, None);
        assert!(duplicate_effects.is_empty());
        let session = core.session_view(&session_id).expect("session");
        assert!(
            session.queue.is_empty(),
            "busy bridge input must never queue"
        );
        assert_eq!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .recent_inbound_event_ids,
            ["event-overload", "event-1", "event-2"]
        );
        for index in 0..130 {
            let event_id = format!("later-busy-{index}");
            let (result, _) = core
                .continue_session_from_bridge_command(
                    &session_id,
                    "thread-transport",
                    "101",
                    &event_id,
                    &format!("later-message-{index}"),
                    prompt.clone(),
                    false,
                )
                .expect("later busy event");
            assert_eq!(
                result.bridge_continuation,
                Some(BridgeContinuationDisposition::Busy)
            );
        }
        let (old_duplicate, effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-1",
                "message-1",
                prompt,
                false,
            )
            .expect("old event remains consumed");
        assert_eq!(
            old_duplicate.bridge_continuation,
            Some(BridgeContinuationDisposition::Duplicate)
        );
        assert!(effects.is_empty());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn accepted_bridge_continuation_replays_stable_prompt_until_backend_acknowledges_it() {
        let (mut core, session_id) = ready_codex_server();
        let workspace = core
            .engine_for(&session_id)
            .expect("session")
            .state()
            .workspace
            .clone();
        core.session_bridges.push(SessionBridgeRecord {
            session_id: session_id.to_string(),
            workspace,
            kind: OrchestratorKind::Chat,
            lifecycle: BridgeLifecycle::Open,
            display_title: "Chat".to_owned(),
            revision: 1,
            transport: None,
            external_parent_id: None,
            external_thread_id: None,
            last_projected: None,
            delivery: None,
            live_turn_id: None,
            live_external_message_id: None,
            active_source_message_id: None,
            recent_inbound_event_ids: Vec::new(),
            pending_inbound: None,
            inbound_turn_origins: Vec::new(),
            updated_at_ms: unix_timestamp_ms(),
        });
        core.bind_session_bridge_thread_command(&session_id, "thread-transport", "100", "101")
            .expect("bind");

        let (_, initial_effects) = core
            .continue_session_from_bridge_command(
                &session_id,
                "thread-transport",
                "101",
                "event-durable",
                "message-durable",
                PromptInput {
                    text: "survive a crash window".to_owned(),
                    attachments: Vec::new(),
                },
                false,
            )
            .expect("accepted");
        let initial_client_id = initial_effects
            .iter()
            .find_map(|effect| match effect {
                crate::state::Effect::Backend(BackendCommand::StartTurn { client_id, .. }) => {
                    Some(client_id.clone())
                }
                _ => None,
            })
            .expect("turn start effect");
        assert_eq!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .pending_inbound
                .as_ref()
                .map(|pending| pending.client_prompt_id.as_str()),
            Some(initial_client_id.as_str())
        );
        assert_eq!(
            core.bridge_prompt_acknowledgement_id(&session_id, "provider-turn-42", true)
                .as_deref(),
            Some(initial_client_id.as_str()),
            "provider acceptance correlates through the stable client prompt id"
        );
        assert_eq!(
            core.bridge_prompt_acknowledgement_id(&session_id, "unrelated-turn", false),
            None,
            "an unrelated terminal event must not consume the durable inbox"
        );

        core.engine_for_mut(&session_id)
            .expect("session")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::RequestFailed {
                    operation: BackendOperation::StartTurn,
                    code: -1,
                    message: "process stopped before acknowledgement".to_owned(),
                },
            );
        let replay = core
            .resume_pending_bridge_prompt(&session_id)
            .expect("durable replay");
        assert!(replay.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartTurn { client_id, .. })
                if client_id == &initial_client_id
        )));

        core.engine_for_mut(&session_id)
            .expect("session")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnStarted {
                    turn_id: "provider-turn-42".to_owned(),
                },
            );
        assert!(
            core.record_bridge_turn_origin(&session_id, "provider-turn-42")
                .is_some(),
            "provider turn provenance is durably checkpointed"
        );
        let transcript = core.session_view(&session_id).expect("session").transcript;
        let user = transcript
            .entries
            .iter()
            .find(|entry| entry.kind == nakode_protocol::TranscriptEntryKind::User)
            .expect("bridge user entry");
        assert_eq!(
            user.owner_turn_id.as_ref().map(TurnId::as_str),
            Some("provider-turn-42")
        );
        assert_eq!(user.source_transport.as_deref(), Some("thread-transport"));
        assert_eq!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .inbound_turn_origins,
            [BridgeInboundTurnOriginRecord {
                turn_id: "provider-turn-42".to_owned(),
                transport: "thread-transport".to_owned(),
            }]
        );

        let provider_turn = TurnId::from("provider-turn-42");
        let empty_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        core.prepare_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::User,
            &provider_turn,
            None,
            empty_sha256,
            0,
        )
        .expect("trusted external transport user is suppressed with a zero-message checkpoint");
        core.finalize_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::User,
            &provider_turn,
        )
        .expect("advance suppressed user cursor");
        assert!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .inbound_turn_origins
                .is_empty(),
            "provenance is pruned only after the typed cursor passes its user projection"
        );
        let (_, duplicate_suppression) = core
            .prepare_bridge_delivery_command(
                &session_id,
                BridgeProjectionKind::User,
                &provider_turn,
                None,
                empty_sha256,
                0,
            )
            .expect("lost suppression response is idempotent after provenance pruning");
        assert!(duplicate_suppression.is_empty());
        let user_cursor = BridgeProjectionView {
            kind: BridgeProjectionKind::User,
            turn_id: provider_turn.clone(),
        };
        core.prepare_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &provider_turn,
            Some(&user_cursor),
            empty_sha256,
            1,
        )
        .expect("assistant follows the suppressed user cursor");
        core.complete_bridge_delivery_part_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &provider_turn,
            0,
            "assistant-message",
        )
        .expect("assistant checkpoint");
        core.finalize_bridge_delivery_command(
            &session_id,
            BridgeProjectionKind::Assistant,
            &provider_turn,
        )
        .expect("assistant finalization");
        assert!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .inbound_turn_origins
                .is_empty(),
            "assistant finalization keeps already-pruned provenance empty"
        );

        assert!(
            core.acknowledge_bridge_prompt(&session_id, &initial_client_id)
                .is_some()
        );
        assert!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .pending_inbound
                .is_none()
        );

        {
            let bridge = core.session_bridge_mut(&session_id).expect("bridge");
            bridge.active_source_message_id = Some("stale-thread-transport-source".to_owned());
        }
        assert!(
            core.record_bridge_turn_origin(&session_id, "dashboard-origin-turn")
                .is_some(),
            "a source-neutral provider turn clears stale external transport reaction ownership"
        );
        assert!(
            core.session_bridge(&session_id)
                .expect("bridge")
                .active_source_message_id
                .is_none()
        );

        {
            let bridge = core.session_bridge_mut(&session_id).expect("bridge");
            bridge.active_source_message_id = Some("failed-source".to_owned());
            bridge.live_turn_id = Some("failed-turn".to_owned());
            bridge.live_external_message_id = Some("failed-live".to_owned());
        }
        assert!(
            core.set_bridge_live_message_command(&session_id, None, None, Some("newer-source"),)
                .is_err(),
            "terminal cleanup cannot clear a newer source-message owner"
        );
        core.set_bridge_live_message_command(&session_id, None, None, Some("failed-source"))
            .expect("compare-and-clear failed terminal state");
        let bridge = core.session_bridge(&session_id).expect("bridge");
        assert!(bridge.active_source_message_id.is_none());
        assert!(bridge.live_turn_id.is_none());
        assert!(bridge.live_external_message_id.is_none());
        core.set_bridge_live_message_command(&session_id, None, None, Some("failed-source"))
            .expect("lost terminal cleanup response is idempotent");
    }

    fn shell_command(session_id: &SessionId, command: &str) -> Command {
        Command::RunShell {
            session_id: session_id.clone(),
            command: command.to_owned(),
        }
    }

    fn read_run_text_field(core: &ServerCore, run_id: &RunId, field: RunTextField) -> String {
        let mut before_byte = None;
        let mut chunks = Vec::new();
        loop {
            let QueryResult::RunText(window) = core
                .query(Query::GetRunTextWindow {
                    run_id: run_id.clone(),
                    field,
                    before_byte,
                    limit_bytes: u32::try_from(MAX_RUN_TEXT_BYTES).expect("limit fits u32"),
                })
                .expect("run text query")
            else {
                panic!("run text result");
            };
            before_byte = Some(window.start_byte);
            chunks.push(window.text);
            if !window.has_earlier {
                break;
            }
        }
        chunks.reverse();
        chunks.concat()
    }

    #[test]
    fn workspace_discovery_excludes_the_uncreated_initial_engine() {
        let (mut core, initial_id) = ready_codex_server();
        let workspace = core.workspace_bootstrap();

        assert!(
            workspace
                .sessions
                .iter()
                .all(|session| session.id != initial_id)
        );
        assert!(matches!(
            core.open_session_command(&initial_id, None),
            Err(DomainCommandError::NotFound(_))
        ));

        let (created, _) = core
            .create_session_command(
                &workspace.workspace_id,
                None,
                &ModelOptions::default(),
                None,
            )
            .expect("explicit logical session");
        let created_id = SessionId::from(created.resource_id.expect("created session id"));
        let discovered = core.workspace_bootstrap();
        assert_eq!(discovered.sessions.len(), 1);
        assert_eq!(discovered.sessions[0].id, created_id);
        assert_eq!(discovered.sessions[0].updated_at_ms, 0);
    }

    #[test]
    fn session_inventory_is_authoritative_only_when_startup_and_request_are_complete() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        for _ in 0..2 {
            core.create_session_command(&workspace_id, None, &ModelOptions::default(), None)
                .expect("logical session");
        }

        let QueryResult::Sessions(complete) = core
            .query(Query::ListSessions {
                workspace_id: workspace_id.clone(),
                limit: 2,
            })
            .expect("complete inventory")
        else {
            panic!("session inventory result");
        };
        assert!(complete.complete);
        assert_eq!(complete.sessions.len(), 2);

        let QueryResult::Sessions(truncated) = core
            .query(Query::ListSessions {
                workspace_id: workspace_id.clone(),
                limit: 1,
            })
            .expect("bounded inventory")
        else {
            panic!("session inventory result");
        };
        assert!(!truncated.complete);
        assert_eq!(truncated.sessions.len(), 1);

        core.set_session_inventory_complete(false);
        let QueryResult::Sessions(startup_partial) = core
            .query(Query::ListSessions {
                workspace_id,
                limit: 500,
            })
            .expect("startup-partial inventory")
        else {
            panic!("session inventory result");
        };
        assert!(!startup_partial.complete);
        assert_eq!(startup_partial.sessions.len(), 2);
    }

    #[test]
    fn fresh_session_tools_are_installed_before_the_first_provider_effect() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let tools = dashboard_tools("DashboardRead", true);
        let (created, creation_effects) = core
            .create_session_command(
                &workspace_id,
                None,
                &ModelOptions::default(),
                Some(tools.clone()),
            )
            .expect("session with initial tools");
        assert!(
            creation_effects.iter().all(|effect| !matches!(
                effect,
                crate::state::Effect::Backend(
                    BackendCommand::StartSession { .. } | BackendCommand::ResumeSession { .. }
                )
            )),
            "creation must not start or restore provider inference: {creation_effects:#?}"
        );
        let session_id = SessionId::from(created.resource_id.expect("logical session id"));

        let (_, effects) = core
            .try_execute_command(Command::SendPrompt {
                session_id,
                prompt: PromptInput {
                    text: "Read the dashboard".to_owned(),
                    attachments: Vec::new(),
                },
            })
            .expect("first prompt");
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::Backend(BackendCommand::StartSession {
                    external_tools,
                    replace_builtin_tools: true,
                    ..
                }) if external_tools == &tools.tools
            )),
            "{effects:#?}"
        );
    }

    #[test]
    fn fresh_session_builtin_allowlist_is_carried_to_provider_start() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let tools = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec!["read".to_owned(), "grep".to_owned()]),
        };
        let (created, _) = core
            .create_session_command(&workspace_id, None, &ModelOptions::default(), Some(tools))
            .expect("builtin allowlist without external tools");
        let (_, effects) = core
            .try_execute_command(Command::SendPrompt {
                session_id: SessionId::from(created.resource_id.expect("session id")),
                prompt: PromptInput {
                    text: "inspect".to_owned(),
                    attachments: Vec::new(),
                },
            })
            .expect("first prompt");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartSession {
                allowed_builtin_tools: Some(allowed),
                ..
            }) if allowed == &["read".to_owned(), "grep".to_owned()]
        )));
    }

    #[test]
    fn disabled_memory_only_allowlist_remains_a_stable_authorization_boundary() {
        let (mut core, _) = ready_external_tools_server();
        install_available_tools(&mut core, CODEX_PROVIDER, &["read"]);
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let tools = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec![
                "memory_search".to_owned(),
                "memory_store".to_owned(),
            ]),
        };
        let (created, _) = core
            .create_session_command(&workspace_id, None, &ModelOptions::default(), Some(tools))
            .expect("disabled memory does not invalidate stable authorization");
        let (_, effects) = core
            .try_execute_command(Command::SendPrompt {
                session_id: SessionId::from(created.resource_id.expect("session id")),
                prompt: PromptInput {
                    text: "inspect".to_owned(),
                    attachments: Vec::new(),
                },
            })
            .expect("first prompt");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartSession {
                external_tools,
                replace_builtin_tools: false,
                allowed_builtin_tools: Some(allowed),
                ..
            }) if external_tools.is_empty()
                && allowed == &["memory_search".to_owned(), "memory_store".to_owned()]
        )));
    }

    #[test]
    fn attached_session_tool_validation_includes_builtin_allowlist() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let configured = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec!["read".to_owned()]),
        };
        let (created, _) = core
            .create_session_command(
                &workspace_id,
                None,
                &ModelOptions::default(),
                Some(configured.clone()),
            )
            .expect("create");
        let id = SessionId::from(created.resource_id.expect("session id"));
        assert!(
            core.open_session_command(
                &id,
                Some(SessionToolConfiguration {
                    allowed_builtin_tools: Some(vec!["grep".to_owned()]),
                    ..configured
                })
            )
            .is_err()
        );
    }

    #[test]
    fn availability_updates_preserve_client_authorization_boundaries() {
        let (mut core, loaded_id) = ready_external_tools_server();
        install_available_tools(
            &mut core,
            CODEX_PROVIDER,
            &["read", "memory_search", "memory_store", "browser", "vision"],
        );
        let requested = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec!["memory_search".to_owned()]),
        };
        assert_eq!(
            core.engine_for(&loaded_id)
                .expect("loaded session")
                .state()
                .reconcile_available_builtin_tools(CODEX_PROVIDER, requested.clone())
                .allowed_builtin_tools,
            requested.allowed_builtin_tools
        );

        install_available_tools(&mut core, CODEX_PROVIDER, &["read"]);
        let normalized = core
            .engine_for(&loaded_id)
            .expect("loaded session")
            .state()
            .reconcile_available_builtin_tools(CODEX_PROVIDER, requested.clone());
        assert_eq!(normalized, requested);

        let workspace_id = core.workspace_bootstrap().workspace_id;
        core.create_session_command(
            &workspace_id,
            None,
            &ModelOptions::default(),
            Some(SessionToolConfiguration {
                tools: Vec::new(),
                replace_builtin_tools: false,
                allowed_builtin_tools: Some(vec!["memory_store".to_owned()]),
            }),
        )
        .expect("disabled add-on does not invalidate stable client authorization");
    }

    #[test]
    fn disabled_tools_remain_authorized_during_persisted_session_resume() {
        let (mut core, _) = ready_external_tools_server();
        install_available_tools(&mut core, CODEX_PROVIDER, &["read"]);
        let restored_id = SessionId::from("restored-tools-session");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-restored".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "Old dashboard prompt".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);
        let mut tools = dashboard_tools("DashboardRead", false);
        tools.allowed_builtin_tools =
            Some(vec!["memory_search".to_owned(), "memory_store".to_owned()]);

        let (_, effects) = core
            .open_session_command(&restored_id, Some(tools.clone()))
            .expect("atomic restored open");
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::PersistSessionSkillSnapshot {
                    session_id,
                    ..
                } if session_id == restored_id.as_str()
            )),
            "legacy rows must be bound to an explicit snapshot on first resume: {effects:#?}"
        );
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::Backend(BackendCommand::ResumeSession {
                    provider_session_id,
                    external_tools,
                    replace_builtin_tools: false,
                    allowed_builtin_tools: Some(allowed),
                    ..
                }) if provider_session_id == "thread-restored"
                    && external_tools == &tools.tools
                    && allowed == &["memory_search".to_owned(), "memory_store".to_owned()]
            )),
            "{effects:#?}"
        );
    }

    #[test]
    fn enabled_but_runtime_unavailable_memory_remains_authorized() {
        let (mut core, _) = ready_external_tools_server();
        install_available_tools(&mut core, CODEX_PROVIDER, &["read"]);
        let restored_id = SessionId::from("restored-unavailable-memory");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-unavailable-memory".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "Memory backend unavailable".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: Some(Vec::new()),
            owned_provider_sessions: Vec::new(),
        }]);
        let tools = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec![
                "memory_search".to_owned(),
                "memory_store".to_owned(),
            ]),
        };

        let (_, effects) = core
            .open_session_command(&restored_id, Some(tools))
            .expect("runtime readiness does not rewrite memory authorization");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::ResumeSession {
                enabled_skill_ids,
                replace_builtin_tools: false,
                allowed_builtin_tools: Some(allowed),
                ..
            }) if enabled_skill_ids.is_empty()
                && allowed == &["memory_search".to_owned(), "memory_store".to_owned()]
        )));
    }

    #[test]
    fn persisted_session_surfaces_unsupported_provider_tool_error() {
        let (mut core, _) = ready_external_tools_server();
        core.session_template.handle_provider_backend(
            CLAUDE_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CLAUDE_PROVIDER.to_owned(),
                display_name: "Claude".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    resume: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        install_available_tools(&mut core, CLAUDE_PROVIDER, &["read", "ask"]);
        let restored_id = SessionId::from("restored-claude-session");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CLAUDE_PROVIDER.to_owned(),
            provider_session_id: "claude-thread".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "Historical Claude session".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);
        let tools = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: Some(vec![
                "memory_search".to_owned(),
                "memory_store".to_owned(),
            ]),
        };

        let error = core
            .open_session_command(&restored_id, Some(tools))
            .expect_err("unsupported enabled tools must return a provider-specific error");
        assert!(error.to_string().contains("provider claude-agent"));
        assert!(error.to_string().contains("memory_search, memory_store"));
    }

    #[test]
    fn attached_session_tool_reattach_is_idempotent_and_rejects_a_different_table() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let tools = dashboard_tools("ReadAssociatedTicket", false);
        let (created, _) = core
            .create_session_command(
                &workspace_id,
                None,
                &ModelOptions::default(),
                Some(tools.clone()),
            )
            .expect("configured coding session");
        let session_id = SessionId::from(created.resource_id.expect("logical session id"));

        let (_, effects) = core
            .open_session_command(&session_id, Some(tools))
            .expect("identical reattach");
        assert!(effects.is_empty());
        let error = core
            .open_session_command(
                &session_id,
                Some(dashboard_tools("UpdateAssociatedTicket", false)),
            )
            .expect_err("different table must fail closed");
        assert!(error.to_string().contains("different tool table"));
    }

    #[test]
    fn attached_session_reattach_compares_persisted_authorization() {
        let (mut core, _) = ready_external_tools_server();
        install_available_tools(
            &mut core,
            CODEX_PROVIDER,
            &["read", "memory_search", "memory_store"],
        );
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let mut tools = dashboard_tools("ReadAssociatedTicket", false);
        tools.allowed_builtin_tools =
            Some(vec!["memory_search".to_owned(), "memory_store".to_owned()]);
        let (created, _) = core
            .create_session_command(
                &workspace_id,
                None,
                &ModelOptions::default(),
                Some(tools.clone()),
            )
            .expect("session created while memory is available");
        let session_id = SessionId::from(created.resource_id.expect("logical session id"));

        install_available_tools(&mut core, CODEX_PROVIDER, &["read"]);
        let (_, effects) = core
            .open_session_command(&session_id, Some(tools))
            .expect("enabled-state change does not mutate attached-session authorization");
        assert!(effects.is_empty());
    }

    #[test]
    fn attached_session_ignores_a_new_mcp_grant() {
        let (mut core, _) = ready_external_tools_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let (created, _) = core
            .create_session_command(&workspace_id, None, &ModelOptions::default(), None)
            .expect("coding session");
        let session_id = SessionId::from(created.resource_id.expect("logical session id"));
        let changed_grant = nakode_protocol::McpSessionGrant {
            surface: Some(nakode_protocol::McpSessionSurface::CodingAgent),
            server_ids: vec!["not-installed-on-this-session".to_owned()],
        };

        let (_, effects) = core
            .open_session_command_with_mcp(&session_id, None, Some(&changed_grant))
            .expect("attached sessions retain their installed MCP configuration");

        assert!(effects.is_empty());
    }

    #[test]
    fn invalid_initial_tools_publish_and_restore_nothing() {
        let (mut core, _) = ready_external_tools_server();
        let restored_id = SessionId::from("invalid-restored-tools-session");
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let session_count = core.sessions_by_id.len();
        let invalid = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: false,
            allowed_builtin_tools: None,
        };
        core.create_session_command(
            &workspace_id,
            None,
            &ModelOptions::default(),
            Some(invalid.clone()),
        )
        .expect_err("empty initial table");
        assert_eq!(core.sessions_by_id.len(), session_count);

        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-old".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "Old".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 1,
            updated_at: 2,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);
        let loaded_before_open = core.sessions_by_id.len();
        core.open_session_command(&restored_id, Some(invalid))
            .expect_err("invalid restored table");
        assert_eq!(core.sessions_by_id.len(), loaded_before_open);
        assert!(
            core.engine_for(&restored_id).is_none(),
            "invalid restore must not publish a runtime"
        );
    }

    #[test]
    fn persisted_initial_engine_is_discoverable_after_restart_reconciliation() {
        let (mut core, initial_id) = ready_codex_server();
        core.replace_session_records(vec![SessionRecord {
            id: initial_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-1".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "Direct terminal session".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 10,
            updated_at: 12,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);

        let discovered = core.workspace_bootstrap();
        assert_eq!(discovered.sessions.len(), 1);
        assert_eq!(discovered.sessions[0].id, initial_id);
        assert_eq!(discovered.sessions[0].title, "Direct terminal session");
        assert_eq!(discovered.sessions[0].created_at_ms, 10_000);
        assert_eq!(discovered.sessions[0].updated_at_ms, 12_000);
        let restored = core
            .session_view(&initial_id)
            .expect("persisted session state remains projectable");
        assert_eq!(restored.created_at_ms, 10_000);
        assert_eq!(restored.updated_at_ms, 12_000);
        core.open_session_command(&initial_id, None)
            .expect("persisted initial session remains resumable");
    }

    #[test]
    fn creating_a_session_applies_the_requested_model_atomically() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    model_catalog: CapabilitySupport::Supported,
                    session_model_config: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Models(vec![
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-a".to_owned(),
                    is_default: true,
                    capabilities: ModelCapabilities {
                        reasoning_efforts: vec!["high".to_owned()],
                    },
                },
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-b".to_owned(),
                    is_default: false,
                    capabilities: ModelCapabilities {
                        reasoning_efforts: vec!["high".to_owned()],
                    },
                },
            ]),
        );
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let workspace_id = core.workspace_bootstrap().workspace_id;

        let (accepted, effects) = core
            .create_session_command(
                &workspace_id,
                Some(&ModelId::from("openai-codex/model-b")),
                &ModelOptions {
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: false,
                },
                None,
            )
            .expect("configured logical session");
        let session_id = SessionId::from(accepted.resource_id.expect("created session id"));
        let created_state = core
            .engine_for(&session_id)
            .expect("created session runtime")
            .state();
        assert_eq!(
            created_state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
        assert_eq!(
            created_state
                .selected_model_options()
                .reasoning_effort
                .as_deref(),
            Some("high")
        );
        assert!(
            effects.is_empty(),
            "creation must not start a transient provider session"
        );

        core.session_template.selected_model = Some("openai-codex/model-a".to_owned());
        core.open_session_command(&session_id, None)
            .expect("existing logical session resumes");
        let resumed_state = core
            .engine_for(&session_id)
            .expect("resumed session runtime")
            .state();
        assert_eq!(
            resumed_state.selected_model.as_deref(),
            Some("openai-codex/model-b"),
            "current defaults must not replace persisted session configuration on resume"
        );
        assert_eq!(
            resumed_state
                .selected_model_options()
                .reasoning_effort
                .as_deref(),
            Some("high"),
            "resume must preserve the persisted effort"
        );
    }

    #[test]
    fn invalid_initial_model_options_do_not_publish_a_session() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let session_count = core.sessions_by_id.len();

        let error = core
            .create_session_command(
                &workspace_id,
                Some(&ModelId::from("openai-codex/removed-model")),
                &ModelOptions {
                    reasoning_effort: Some("impossible".to_owned()),
                    fast_mode: false,
                },
                None,
            )
            .expect_err("stale initial model must be rejected");

        let message = error.to_string();
        assert!(message.contains("removed-model"), "{message}");
        assert_eq!(core.sessions_by_id.len(), session_count);
    }

    #[test]
    fn initial_options_without_a_model_are_rejected_without_publishing_a_session() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let session_count = core.sessions_by_id.len();

        let error = core
            .create_session_command(
                &workspace_id,
                None,
                &ModelOptions {
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: false,
                },
                None,
            )
            .expect_err("orphan initial options must be rejected");

        assert!(error.to_string().contains("require model_id"));
        assert_eq!(core.sessions_by_id.len(), session_count);
    }

    #[test]
    fn creating_a_session_without_a_selection_keeps_the_server_default() {
        let (mut core, _) = ready_codex_server();
        let workspace_id = core.workspace_bootstrap().workspace_id;

        let (accepted, _) = core
            .create_session_command(&workspace_id, None, &ModelOptions::default(), None)
            .expect("default logical session");
        let session_id = SessionId::from(accepted.resource_id.expect("created session id"));
        assert_eq!(
            core.engine_for(&session_id)
                .expect("created session runtime")
                .state()
                .selected_model,
            None
        );
    }

    fn owner_prompt_text(prompt: &str) -> &str {
        const OWNER_MARKER: &str = "\n\n## Owner message\n\n";
        prompt
            .split_once(OWNER_MARKER)
            .map_or(prompt, |(_, owner)| owner)
    }

    fn assert_queued_turn_starts(
        core: &mut ServerCore,
        session_id: &SessionId,
        completed_turn: &str,
        next_turn: &str,
        expected_prompt: &str,
        expected_id: &str,
    ) {
        let effects = core
            .engine_for_mut(session_id)
            .expect("session engine")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnCompleted {
                    turn_id: completed_turn.to_owned(),
                    outcome: crate::backend::TurnOutcome::Completed,
                    error: None,
                },
            );
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(
                    effect,
                    crate::state::Effect::Backend(BackendCommand::StartTurn { .. })
                ))
                .count(),
            1,
            "each terminal turn starts exactly one successor"
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartTurn { client_id, prompt, .. })
                if client_id == expected_id && owner_prompt_text(prompt).starts_with(expected_prompt)
        )));
        core.engine_for_mut(session_id)
            .expect("session engine")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnAccepted {
                    turn_id: next_turn.to_owned(),
                },
            );
    }

    fn assert_source_prompt_ids(core: &ServerCore, session_id: &SessionId, expected: &[&str]) {
        let view = core.session_view(session_id).expect("session view");
        let actual = view
            .transcript
            .entries
            .iter()
            .filter_map(|entry| entry.source_prompt_id.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn busy_server_with_stale_revision(turn_id: &str) -> (ServerCore, SessionId, u64) {
        let (mut core, session_id) = ready_codex_server();
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnStarted {
                    turn_id: turn_id.to_owned(),
                },
            );
        let observed_revision = core
            .engine_for(&session_id)
            .expect("session engine")
            .revision();
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .note_state_change();
        (core, session_id, observed_revision)
    }

    #[test]
    fn idle_prompt_receipt_identity_follows_the_active_turn_and_replays_without_duplication() {
        let (mut core, session_id) = ready_codex_server();
        let prompt = Command::SendPrompt {
            session_id: session_id.clone(),
            prompt: PromptInput {
                text: "start once".to_owned(),
                attachments: Vec::new(),
            },
        };
        let key = IdempotencyKey::from("window-idle-start");
        let (accepted, effects, effect_session, changed) =
            core.execute_idempotent(key.clone(), None, false, prompt.clone());
        let accepted = accepted.expect("idle prompt starts");
        assert_eq!(accepted.resource_id.as_deref(), Some("window-idle-start"));
        assert_eq!(effect_session.as_ref(), Some(&session_id));
        assert!(changed);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartTurn { client_id, prompt, .. })
                if client_id == "window-idle-start" && owner_prompt_text(prompt).starts_with("start once")
        )));

        let (retry, retry_effects, retry_session, retry_changed) =
            core.execute_idempotent(key.clone(), None, false, prompt);
        assert_eq!(
            retry.expect("retry replays receipt").resource_id,
            accepted.resource_id
        );
        assert!(retry_effects.is_empty());
        assert_eq!(retry_session, None);
        assert!(!retry_changed);

        let (conflict, conflict_effects, _, conflict_changed) = core.execute_idempotent(
            key,
            None,
            false,
            Command::SendPrompt {
                session_id,
                prompt: PromptInput {
                    text: "different body".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );
        assert!(matches!(conflict, Err(error) if error.code == ErrorCode::Conflict));
        assert!(conflict_effects.is_empty());
        assert!(!conflict_changed);
    }

    #[test]
    fn prompt_identity_prevents_reexecution_after_idempotency_cache_eviction() {
        let (mut core, session_id) = ready_codex_server();
        let key = IdempotencyKey::from("evicted-prompt-operation");
        let command = Command::SendPrompt {
            session_id: session_id.clone(),
            prompt: PromptInput {
                text: "run this once".to_owned(),
                attachments: Vec::new(),
            },
        };
        core.execute_idempotent(key.clone(), None, false, command.clone())
            .0
            .expect("first prompt submission");
        core.command_cache.remove(&key);
        core.command_order.retain(|candidate| candidate != &key);

        let (retry, effects, _, changed) =
            core.execute_idempotent(key.clone(), None, false, command);
        assert_eq!(
            retry
                .expect("stable transcript identity replays acceptance")
                .resource_id,
            Some(key.to_string())
        );
        assert!(effects.is_empty(), "the provider turn must not run twice");
        assert!(changed, "the replay publishes a fresh receipt revision");

        core.command_cache.remove(&key);
        core.command_order.retain(|candidate| candidate != &key);
        let conflict = core.execute_idempotent(
            key,
            None,
            false,
            Command::SendPrompt {
                session_id,
                prompt: PromptInput {
                    text: "different content".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );
        assert!(matches!(conflict.0, Err(error) if error.code == ErrorCode::Conflict));
        assert!(conflict.1.is_empty());
    }

    #[test]
    fn queued_prompt_identity_prevents_reexecution_after_idempotency_cache_eviction() {
        let (mut core, session_id, observed_revision) =
            busy_server_with_stale_revision("active-turn");
        let key = IdempotencyKey::from("evicted-queued-prompt-operation");
        let command = Command::EnqueuePrompt {
            session_id: session_id.clone(),
            prompt: PromptInput {
                text: "queue this once".to_owned(),
                attachments: Vec::new(),
            },
        };
        let accepted = core
            .execute_idempotent(key.clone(), Some(observed_revision), false, command.clone())
            .0
            .expect("first queued prompt submission");
        assert_eq!(accepted.resource_id.as_deref(), Some(key.as_str()));
        assert_eq!(
            core.session_view(&session_id).expect("session").queue.len(),
            1
        );
        core.command_cache.remove(&key);
        core.command_order.retain(|candidate| candidate != &key);

        let (retry, effects, _, changed) =
            core.execute_idempotent(key.clone(), Some(observed_revision), false, command);
        assert_eq!(
            retry
                .expect("stable queue identity replays acceptance")
                .resource_id,
            Some(key.to_string())
        );
        assert!(effects.is_empty());
        assert!(changed, "the replay publishes a fresh receipt revision");
        let queue = core.session_view(&session_id).expect("session").queue;
        assert_eq!(queue.len(), 1, "retry must not append a second follow-up");
        assert_eq!(queue[0].id.as_str(), key.as_str());
    }

    #[test]
    fn local_file_prompt_identity_converges_after_idempotency_cache_eviction() {
        let (mut core, session_id) = ready_codex_server();
        let key = IdempotencyKey::from("evicted-local-file-prompt");
        let command = Command::SendPrompt {
            session_id: session_id.clone(),
            prompt: PromptInput {
                text: "inspect this file once".to_owned(),
                attachments: vec![ProtocolPromptAttachment::LocalFile {
                    label: "context".to_owned(),
                    path: "src/context.rs".to_owned(),
                }],
            },
        };
        core.execute_idempotent(key.clone(), None, false, command.clone())
            .0
            .expect("first local-file prompt submission");
        core.command_cache.remove(&key);
        core.command_order.retain(|candidate| candidate != &key);

        let (retry, effects, _, _) = core.execute_idempotent(key.clone(), None, false, command);
        assert_eq!(
            retry
                .expect("identical local-file prompt replays acceptance")
                .resource_id,
            Some(key.to_string())
        );
        assert!(effects.is_empty(), "the provider turn must not run twice");

        core.command_cache.remove(&key);
        core.command_order.retain(|candidate| candidate != &key);
        let conflict = core.execute_idempotent(
            key,
            None,
            false,
            Command::SendPrompt {
                session_id,
                prompt: PromptInput {
                    text: "inspect this file once".to_owned(),
                    attachments: vec![ProtocolPromptAttachment::LocalFile {
                        label: "context".to_owned(),
                        path: "src/different.rs".to_owned(),
                    }],
                },
            },
        );
        assert!(matches!(conflict.0, Err(error) if error.code == ErrorCode::Conflict));
        assert!(conflict.1.is_empty());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn stale_prompt_send_queues_once_in_command_order() {
        let (mut core, session_id, observed_revision) =
            busy_server_with_stale_revision("active-turn");

        let repeated = Command::SendPrompt {
            session_id: session_id.clone(),
            prompt: PromptInput {
                text: "repeat me".to_owned(),
                attachments: Vec::new(),
            },
        };
        let first_key = IdempotencyKey::from("window-a-repeat");
        let (first, effects, _, changed) = core.execute_idempotent(
            first_key.clone(),
            Some(observed_revision),
            false,
            repeated.clone(),
        );
        let first = first.expect("stale prompt append is accepted");
        assert_eq!(first.resource_id.as_deref(), Some("window-a-repeat"));
        assert!(
            effects.is_empty(),
            "a busy send queues without interrupting"
        );
        assert!(changed);

        let (retry, retry_effects, retry_session, retry_changed) =
            core.execute_idempotent(first_key, Some(observed_revision), false, repeated.clone());
        let retry = retry.expect("an ambiguous transport retry replays acceptance");
        assert_eq!(retry.resource_id, first.resource_id);
        assert!(retry_effects.is_empty());
        assert_eq!(retry_session, None);
        assert!(!retry_changed, "a replay must not append a second copy");

        core.execute_idempotent(
            IdempotencyKey::from("window-b-repeat"),
            Some(observed_revision),
            false,
            repeated,
        )
        .0
        .expect("a distinct concurrent send remains distinct");
        core.execute_idempotent(
            IdempotencyKey::from("window-c-next"),
            Some(observed_revision),
            false,
            Command::SendPrompt {
                session_id: session_id.clone(),
                prompt: PromptInput {
                    text: "after repeats".to_owned(),
                    attachments: Vec::new(),
                },
            },
        )
        .0
        .expect("later concurrent send is accepted");

        let queued = &core.session_view(&session_id).expect("session view").queue;
        assert_eq!(
            queued
                .iter()
                .map(|prompt| prompt.text.as_str())
                .collect::<Vec<_>>(),
            ["repeat me", "repeat me", "after repeats"],
            "distinct sends preserve duplicates and serialized arrival order"
        );
        let queued_ids = queued
            .iter()
            .map(|prompt| prompt.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            queued_ids,
            ["window-a-repeat", "window-b-repeat", "window-c-next"],
            "queue identity is the stable mutation identity returned by the receipt"
        );

        for (completed_turn, next_turn, expected_prompt, expected_id) in [
            ("active-turn", "queued-turn-1", "repeat me", queued_ids[0]),
            ("queued-turn-1", "queued-turn-2", "repeat me", queued_ids[1]),
            (
                "queued-turn-2",
                "queued-turn-3",
                "after repeats",
                queued_ids[2],
            ),
        ] {
            assert_queued_turn_starts(
                &mut core,
                &session_id,
                completed_turn,
                next_turn,
                expected_prompt,
                expected_id,
            );
        }
        assert!(
            core.session_view(&session_id)
                .expect("session view")
                .queue
                .is_empty()
        );
        assert_source_prompt_ids(
            &core,
            &session_id,
            &["window-a-repeat", "window-b-repeat", "window-c-next"],
        );
    }

    #[test]
    fn busy_prompt_queue_rejects_oversized_caller_identity_without_mutation() {
        let (mut core, session_id, _) = busy_server_with_stale_revision("active-turn");
        let oversized_id = "x".repeat(129);
        let (result, effects, _effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from(oversized_id),
            None,
            false,
            Command::EnqueuePrompt {
                session_id: session_id.clone(),
                prompt: PromptInput {
                    text: "must not queue".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );

        let error = result.expect_err("oversized prompt identity must be rejected");
        assert_eq!(error.code, ErrorCode::InvalidRequest);
        assert!(error.message.contains("1 to 128 bytes"));
        assert!(effects.is_empty());
        assert!(!changed);
        assert!(
            core.session_view(&session_id)
                .expect("session view")
                .queue
                .is_empty()
        );
    }

    #[test]
    fn explicit_enqueued_prompt_retains_mutation_identity_after_promotion() {
        let (mut core, session_id, _) = busy_server_with_stale_revision("active-turn");
        core.execute_idempotent(
            IdempotencyKey::from("explicit-queued-submission"),
            None,
            false,
            Command::EnqueuePrompt {
                session_id: session_id.clone(),
                prompt: PromptInput {
                    text: "queued owner message".to_owned(),
                    attachments: Vec::new(),
                },
            },
        )
        .0
        .expect("queue accepted");
        assert_eq!(
            core.session_view(&session_id).expect("session view").queue[0]
                .id
                .as_str(),
            "explicit-queued-submission"
        );

        assert_queued_turn_starts(
            &mut core,
            &session_id,
            "active-turn",
            "queued-turn",
            "queued owner message",
            "explicit-queued-submission",
        );
        let view = core.session_view(&session_id).expect("session view");
        assert!(view.transcript.entries.iter().any(|entry| {
            entry.source_prompt_id.as_deref() == Some("explicit-queued-submission")
                && entry.body == "queued owner message"
        }));
    }

    #[test]
    fn immediate_prompt_entry_uses_the_idempotency_key_as_source_identity() {
        let (mut core, session_id) = ready_codex_server();
        core.execute_idempotent(
            IdempotencyKey::from("chat-submission-1"),
            None,
            false,
            Command::SendPrompt {
                session_id: session_id.clone(),
                prompt: PromptInput {
                    text: "one owner message".to_owned(),
                    attachments: Vec::new(),
                },
            },
        )
        .0
        .expect("prompt accepted");

        let view = core.session_view(&session_id).expect("session view");
        let owner = view
            .transcript
            .entries
            .iter()
            .find(|entry| entry.source_prompt_id.as_deref() == Some("chat-submission-1"))
            .expect("owner entry with caller identity");
        assert_eq!(owner.body, "one owner message");
    }

    #[test]
    fn stale_prompt_send_does_not_weaken_other_revision_fences() {
        let (mut core, session_id) = ready_codex_server();
        let observed_revision = core
            .engine_for(&session_id)
            .expect("session engine")
            .revision();
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .note_state_change();

        let (result, effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("stale-unsafe-mutation"),
            Some(observed_revision),
            false,
            shell_command(&session_id, "pwd"),
        );

        assert!(matches!(
            result,
            Err(error)
                if error.code == ErrorCode::Conflict
                    && error.message == "the expected revision is stale"
        ));
        assert!(effects.is_empty());
        assert!(!changed);
    }

    #[test]
    fn stale_prompt_send_uses_authoritative_state_when_the_active_turn_becomes_terminal() {
        let (mut core, session_id) = ready_codex_server();
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnStarted {
                    turn_id: "terminal-race-turn".to_owned(),
                },
            );
        let observed_revision = core
            .engine_for(&session_id)
            .expect("session engine")
            .revision();
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnCompleted {
                    turn_id: "terminal-race-turn".to_owned(),
                    outcome: crate::backend::TurnOutcome::Interrupted,
                    error: None,
                },
            );
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .note_state_change();

        let (result, effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("send-after-terminal-race"),
            Some(observed_revision),
            false,
            Command::SendPrompt {
                session_id: session_id.clone(),
                prompt: PromptInput {
                    text: "run against current state".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );

        result.expect("terminal progress does not stale owner intent");
        assert!(changed);
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::Backend(BackendCommand::StartTurn { prompt, .. })
                    if owner_prompt_text(prompt).starts_with("run against current state")
            )),
            "execution-time idle send did not start a turn: {effects:?}"
        );
        assert!(
            core.session_view(&session_id)
                .expect("session view")
                .queue
                .is_empty(),
            "an idle-at-execution send starts instead of being stranded in the queue"
        );
    }

    #[test]
    fn stale_prompt_send_still_refuses_a_closed_session() {
        let (mut core, _) = ready_codex_server();
        let closed_session = SessionId::from("closed-session");

        let (result, effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("send-to-closed-session"),
            Some(1),
            false,
            Command::SendPrompt {
                session_id: closed_session,
                prompt: PromptInput {
                    text: "do not drop me".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );

        assert!(matches!(result, Err(error) if error.code == ErrorCode::NotFound));
        assert!(effects.is_empty());
        assert!(!changed);
    }

    #[test]
    fn replay_only_command_returns_the_cached_result_without_effects() {
        let (mut core, session_id) = ready_codex_server();
        let key = IdempotencyKey::from("safe-retry");
        let command = shell_command(&session_id, "pwd");

        let (first, first_effects, _, first_changed) =
            core.execute_idempotent(key.clone(), None, false, command.clone());
        assert!(first_changed);
        assert!(!first_effects.is_empty());

        let (replayed, replayed_effects, effect_session, replayed_changed) =
            core.execute_idempotent(key, None, true, command);
        assert_eq!(replayed, first);
        assert!(replayed_effects.is_empty());
        assert_eq!(effect_session, None);
        assert!(!replayed_changed);
    }

    #[test]
    fn replay_only_command_never_executes_after_cache_eviction() {
        let (mut core, session_id) = ready_codex_server();
        let key = IdempotencyKey::from("evicted-retry");
        let command = shell_command(&session_id, "pwd");
        core.execute_idempotent(key.clone(), None, false, command.clone())
            .0
            .expect("first attempt is accepted");
        for index in 0..IDEMPOTENCY_CAPACITY {
            core.execute_idempotent(
                IdempotencyKey::new(format!("other-{index}")),
                None,
                false,
                shell_command(&session_id, "true"),
            )
            .0
            .expect("filler command is accepted");
        }

        let (result, effects, effect_session, changed) =
            core.execute_idempotent(key, None, true, command);
        let error = result.expect_err("an evicted retry must not execute");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("was not executed"));
        assert!(effects.is_empty());
        assert_eq!(effect_session, None);
        assert!(!changed);
    }

    fn prompt_with_image_and_file() -> PromptInput {
        PromptInput {
            text: "inspect the image".to_owned(),
            attachments: vec![
                ProtocolPromptAttachment::InlineImage {
                    label: "screen.png".to_owned(),
                    media_type: "image/png".to_owned(),
                    data: vec![1, 2, 3, 4],
                },
                ProtocolPromptAttachment::LocalFile {
                    label: "notes.md".to_owned(),
                    path: "docs/notes.md".to_owned(),
                },
            ],
        }
    }

    #[test]
    fn provider_authentication_command_publishes_starting_state_before_effect() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let provider = ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
        };
        let mut core = ServerCore::new(ServiceEngine::new(state), vec![provider], Vec::new());

        let (_, effects) = core
            .begin_provider_authentication_command(&ProviderId::from(CODEX_PROVIDER))
            .expect("authentication intent");

        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::AuthenticateProvider(provider)]
                if provider == CODEX_PROVIDER
        ));
        let bootstrap = core.workspace_bootstrap();
        assert!(matches!(
            bootstrap.providers[0].authentication,
            Some(ProviderAuthenticationView::Starting)
        ));
        let session_id = core.default_session_id().clone();
        assert_eq!(
            core.session_view(&session_id)
                .expect("active session")
                .status_message,
            "Starting Codex authentication…"
        );
    }

    #[test]
    fn provider_reload_targets_one_enabled_provider() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let provider = ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: true,
            credential: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
        };
        let core = ServerCore::new(ServiceEngine::new(state), vec![provider], Vec::new());

        let (_, effects) = core
            .reload_provider_command(&ProviderId::from(CODEX_PROVIDER))
            .expect("provider reload");

        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::ReloadProvider(provider)] if provider == CODEX_PROVIDER
        ));
    }

    #[test]
    fn provider_reload_reconnects_a_disabled_provider() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let provider = ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: false,
            credential: None,
            model_filter_enabled: false,
            selected_model_ids: Vec::new(),
        };
        let core = ServerCore::new(ServiceEngine::new(state), vec![provider], Vec::new());

        let (_, effects) = core
            .reload_provider_command(&ProviderId::from(CODEX_PROVIDER))
            .expect("provider reconnect");

        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::SetProviderEnabled { provider, enabled: true }]
                if provider == CODEX_PROVIDER
        ));
    }

    #[test]
    fn invalid_delegation_is_rejected_without_creating_a_run() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let empty = core
            .delegate_command(&session_id, "missing", "   ", None)
            .expect_err("blank delegation must be rejected");
        assert!(empty.to_string().contains("non-empty task"));

        let unknown = core
            .delegate_command(&session_id, "missing", "Inspect authentication", None)
            .expect_err("unknown agent must be rejected");
        assert!(unknown.to_string().contains("predefined agent"));
        assert!(
            core.engine_for(&session_id)
                .expect("session runtime")
                .state()
                .subagents
                .is_empty()
        );
    }

    #[test]
    fn delegation_reloads_the_global_agent_catalogue_at_invocation() {
        let directory = tempfile::tempdir().expect("global agents");
        std::fs::write(
            directory.path().join("reviewer.toml"),
            r#"slug = "reviewer"
description = "Review changes"
system_prompt = "Review carefully"
first_message = "Starting review"
model = "openai-codex/gpt-5"
"#,
        )
        .expect("agent definition");
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        state.set_agent_directory(directory.path().to_path_buf());
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (accepted, effects) = core
            .delegate_command(&session_id, "reviewer", "Inspect authentication", None)
            .expect("fresh global agent is available");

        assert!(accepted.resource_id.is_some());
        assert!(!effects.is_empty());
    }

    #[test]
    fn global_agent_catalogue_reload_updates_workspace_projection() {
        let directory = tempfile::tempdir().expect("global agents");
        std::fs::write(
            directory.path().join("reviewer.toml"),
            r#"slug = "reviewer"
description = "Review changes"
system_prompt = "Review carefully"
first_message = "Starting review"
"#,
        )
        .expect("agent definition");
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        state.set_agent_directory(directory.path().to_path_buf());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        core.reload_global_agent_catalogue()
            .expect("global catalogue reload");

        assert_eq!(core.workspace_bootstrap().agents[0].slug, "reviewer");
    }

    #[test]
    fn invalid_agent_mutations_are_rejected_before_persistence() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        state.install_agents(AgentCatalog::from_definitions(vec![AgentDefinition {
            slug: "reviewer".to_owned(),
            description: "Review changes".to_owned(),
            system_prompt: "Review carefully".to_owned(),
            first_message: "Starting review".to_owned(),
            model: None,
            fallback_models: Vec::new(),
            fast_mode: false,
            reasoning_effort: None,
            ..AgentDefinition::default()
        }]));
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let malformed = core
            .save_agent_command(
                &workspace_id,
                AgentDefinitionInput {
                    slug: "Not Valid".to_owned(),
                    description: String::new(),
                    system_prompt: String::new(),
                    first_message: String::new(),
                    model: None,
                    fallback_models: Vec::new(),
                    fast_mode: false,
                    reasoning_effort: None,
                    ..AgentDefinitionInput::default()
                },
                None,
            )
            .expect_err("malformed agent must be rejected");
        assert!(matches!(
            malformed,
            crate::state::DomainCommandError::Invalid(_)
        ));

        let duplicate = core
            .save_agent_command(
                &workspace_id,
                AgentDefinitionInput {
                    slug: "reviewer".to_owned(),
                    description: "Duplicate".to_owned(),
                    system_prompt: "Duplicate".to_owned(),
                    first_message: "Duplicate".to_owned(),
                    model: None,
                    fallback_models: Vec::new(),
                    fast_mode: false,
                    reasoning_effort: None,
                    ..AgentDefinitionInput::default()
                },
                None,
            )
            .expect_err("duplicate agent must be rejected");
        assert!(matches!(
            duplicate,
            crate::state::DomainCommandError::Conflict(_)
        ));
    }

    #[test]
    fn loaded_session_runtimes_keep_independent_domain_state() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (accepted, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(
            accepted
                .resource_id
                .expect("created session has a resource id"),
        );
        core.engine_for_mut(&first_id)
            .expect("first runtime")
            .state_mut()
            .set_status("first");
        core.engine_for_mut(&second_id)
            .expect("second runtime")
            .state_mut()
            .set_status("second");

        let QueryResult::Session(first) = core
            .query(Query::GetSession {
                session_id: first_id.clone(),
            })
            .expect("query first")
        else {
            panic!("expected first session");
        };
        let QueryResult::Session(second) = core
            .query(Query::GetSession {
                session_id: second_id.clone(),
            })
            .expect("query second")
        else {
            panic!("expected second session");
        };
        assert_eq!(first.id, first_id);
        assert_eq!(first.status_message, "first");
        assert_eq!(second.id, second_id);
        assert_eq!(second.status_message, "second");
        assert_eq!(
            core.subscription_view(&SubscriptionScope::Session {
                session_id: first_id,
            })
            .expect("first subscription"),
            SubscriptionView::Session(first),
        );
        assert_eq!(
            core.subscription_view(&SubscriptionScope::Session {
                session_id: second_id,
            })
            .expect("second subscription"),
            SubscriptionView::Session(second),
        );
    }

    #[tokio::test]
    async fn workspace_changes_advance_and_publish_every_changed_session() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(created.resource_id.expect("created session has an id"));
        let (endpoint, _requests) =
            ServerEndpoint::channel("test", ServiceCapabilities::default(), 16);
        core.publish_state(&endpoint, &second_id);
        let mut publications = endpoint.subscribe_publications();
        let previous_revision = core
            .engine_for(&second_id)
            .expect("second session")
            .revision();

        core.engine_for_mut(&first_id)
            .expect("first session")
            .state_mut()
            .provider_starting(CODEX_PROVIDER, "Codex");
        core.commit_and_publish_session(&endpoint, &first_id);

        let current_revision = core
            .engine_for(&second_id)
            .expect("second session")
            .revision();
        assert_eq!(current_revision, previous_revision + 1);
        let events = drain_publications(&mut publications);
        assert!(events.iter().any(|event| matches!(
            &event.event,
            ViewEvent::SessionMetadataChanged {
                session_id,
                revision,
                ..
            } if session_id == &second_id && *revision == current_revision
        )));
    }

    #[tokio::test]
    async fn ordinary_turn_updates_do_not_advance_or_publish_other_sessions() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(created.resource_id.expect("created session has an id"));
        let (endpoint, _requests) =
            ServerEndpoint::channel("test", ServiceCapabilities::default(), 16);
        core.publish_state(&endpoint, &second_id);
        core.commit_and_publish_backend_session(&endpoint, &second_id);
        let expected_summary = crate::state::projection::active_session_summary(
            core.engine_for(&second_id).expect("second session").state(),
            &core.sessions,
        )
        .expect("session without subagents has a lightweight summary");
        let workspace = core.workspace_bootstrap();
        assert_eq!(
            workspace
                .sessions
                .iter()
                .find(|summary| summary.id == second_id)
                .expect("published second-session summary"),
            &expected_summary,
        );
        let mut publications = endpoint.subscribe_publications();
        let previous_revision = core
            .engine_for(&first_id)
            .expect("first session")
            .revision();

        core.engine_for_mut(&second_id)
            .expect("second session")
            .state_mut()
            .set_status("session-local backend progress");
        core.commit_and_publish_backend_session(&endpoint, &second_id);

        assert_eq!(
            core.engine_for(&first_id)
                .expect("first session")
                .revision(),
            previous_revision
        );
        let events = drain_publications(&mut publications);
        assert!(
            events
                .iter()
                .all(|event| !event.scopes.contains(&SubscriptionScope::Session {
                    session_id: first_id.clone(),
                })),
            "{events:#?}"
        );
        assert!(
            events.iter().all(
                |event| !event.scopes.contains(&SubscriptionScope::Workspace {
                    workspace_id: workspace_id.clone(),
                })
            ),
            "session-local backend updates must not invalidate the workspace: {events:#?}"
        );
    }

    #[test]
    fn artifact_queries_search_every_loaded_session_and_enforce_the_bound() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(
            created
                .resource_id
                .expect("created session has a resource id"),
        );
        let second = core
            .engine_for_mut(&second_id)
            .expect("second session")
            .state_mut();
        second.transcript.set_labeled_images(
            "user:image",
            vec![(
                "architecture.png".to_owned(),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: vec![1, 3, 3, 7],
                },
            )],
        );
        second.transcript.upsert(
            "user:image",
            EntryKind::User,
            "YOU",
            "[architecture.png]",
            EntryStatus::Complete,
        );

        let QueryResult::Session(session) = core
            .query(Query::GetSession {
                session_id: second_id,
            })
            .expect("second session projection")
        else {
            panic!("expected session");
        };
        let artifact_id = session
            .transcript
            .entries
            .iter()
            .find_map(|entry| entry.artifacts.first())
            .cloned()
            .expect("projected image artifact");
        let QueryResult::Artifact(artifact) = core
            .query(Query::GetArtifact {
                artifact_id: artifact_id.clone(),
            })
            .expect("artifact query")
        else {
            panic!("expected artifact");
        };
        assert_eq!(artifact.id, artifact_id);
        assert_eq!(artifact.label, "architecture.png");
        assert_eq!(artifact.media_type, "image/png");
        assert_eq!(artifact.byte_length, 4);
        assert_eq!(artifact.data, [1, 3, 3, 7]);

        core.engine_for_mut(&session.id)
            .expect("second session")
            .state_mut()
            .transcript
            .set_labeled_images(
                "user:image",
                vec![(
                    "architecture.png".to_owned(),
                    PromptImage {
                        mime_type: "image/png".to_owned(),
                        data: vec![0; MAX_ARTIFACT_BYTES.saturating_add(1)],
                    },
                )],
            );
        let error = core
            .query(Query::GetArtifact {
                artifact_id: artifact_id.clone(),
            })
            .expect_err("oversized artifact must be rejected before framing");
        assert_eq!(error.code, ErrorCode::InvalidRequest);
        assert!(error.message.contains("maximum transferable size"));
        let error = core
            .convert_prompt(
                &session.id,
                PromptInput {
                    text: "retry".to_owned(),
                    attachments: vec![ProtocolPromptAttachment::Artifact {
                        artifact_id,
                        label: "architecture.png".to_owned(),
                    }],
                },
            )
            .expect_err("oversized artifact retry must be rejected");
        assert!(matches!(
            error,
            crate::state::DomainCommandError::Invalid(message)
                if message.contains("maximum")
        ));
    }

    #[test]
    fn oversized_inline_images_are_rejected_before_domain_state_changes() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let error = core
            .convert_prompt(
                &session_id,
                PromptInput {
                    text: "inspect".to_owned(),
                    attachments: vec![nakode_protocol::PromptAttachment::InlineImage {
                        label: "huge.png".to_owned(),
                        media_type: "image/png".to_owned(),
                        data: vec![0; MAX_ARTIFACT_BYTES.saturating_add(1)],
                    }],
                },
            )
            .expect_err("oversized inline image");
        assert!(matches!(
            error,
            crate::state::DomainCommandError::Invalid(message)
                if message.contains("maximum")
        ));
    }

    #[test]
    fn failed_prompt_recovery_round_trips_images_as_session_artifacts() {
        let (mut core, session_id) = ready_codex_server();

        let (_, effects) = core
            .prompt_command(&session_id, prompt_with_image_and_file(), false, None)
            .expect("initial prompt");
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::Backend(BackendCommand::StartTurn { .. })
            )),
            "{effects:#?}"
        );
        core.engine_for_mut(&session_id)
            .expect("session")
            .state_mut()
            .handle_backend(BackendEvent::RequestFailed {
                operation: BackendOperation::StartTurn,
                code: -32602,
                message: "rejected".to_owned(),
            });

        let QueryResult::Session(session) = core
            .query(Query::GetSession {
                session_id: session_id.clone(),
            })
            .expect("recovery projection")
        else {
            panic!("session result");
        };
        let recovery = session
            .recoverable_prompt
            .clone()
            .expect("semantic prompt recovery");
        assert!(!recovery.id.as_str().is_empty());
        assert_eq!(recovery.text, "inspect the image");
        let ProtocolPromptAttachment::Artifact { artifact_id, label } = &recovery.attachments[0]
        else {
            panic!("failed image is represented by an artifact reference");
        };
        assert_eq!(label, "screen.png");
        assert!(matches!(
            &recovery.attachments[1],
            ProtocolPromptAttachment::LocalFile { label, path }
                if label == "notes.md" && path == "docs/notes.md"
        ));
        let QueryResult::Artifact(artifact) = core
            .query(Query::GetArtifact {
                artifact_id: artifact_id.clone(),
            })
            .expect("recovery artifact")
        else {
            panic!("artifact result");
        };
        assert_eq!(artifact.data, [1, 2, 3, 4]);

        let (_, effects) = core
            .prompt_command(
                &session_id,
                PromptInput {
                    text: recovery.text,
                    attachments: recovery.attachments,
                },
                false,
                None,
            )
            .expect("artifact-backed retry");
        let Some(attachments) = effects.iter().find_map(|effect| match effect {
            crate::state::Effect::Backend(BackendCommand::StartTurn { attachments, .. }) => {
                Some(attachments)
            }
            _ => None,
        }) else {
            panic!("retry starts a provider-neutral turn");
        };
        assert!(matches!(
            &attachments[0],
            crate::backend::PromptAttachment {
                label,
                path: None,
                image: Some(image),
            } if label == "screen.png"
                && image.mime_type == "image/png"
                && image.data == [1, 2, 3, 4]
        ));
        assert!(matches!(
            &attachments[1],
            crate::backend::PromptAttachment {
                label,
                path: Some(path),
                image: None,
            } if label == "notes.md" && path == std::path::Path::new("docs/notes.md")
        ));
        let QueryResult::Session(session) = core
            .query(Query::GetSession { session_id })
            .expect("post-retry projection")
        else {
            panic!("session result");
        };
        assert!(session.recoverable_prompt.is_none());
    }

    #[test]
    fn artifact_prompt_references_are_scoped_to_the_addressed_session() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(created.resource_id.expect("second session id"));
        let state = core
            .engine_for_mut(&second_id)
            .expect("second session")
            .state_mut();
        state.transcript.set_labeled_images(
            "user:image",
            vec![(
                "other.png".to_owned(),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: vec![9],
                },
            )],
        );
        state.transcript.upsert(
            "user:image",
            EntryKind::User,
            "YOU",
            "[other.png]",
            EntryStatus::Complete,
        );
        let QueryResult::Session(second) = core
            .query(Query::GetSession {
                session_id: second_id,
            })
            .expect("second session")
        else {
            panic!("session result");
        };
        let artifact_id = second
            .transcript
            .entries
            .iter()
            .find_map(|entry| entry.artifacts.first())
            .cloned()
            .expect("second session artifact");

        let error = core
            .convert_prompt(
                &first_id,
                PromptInput {
                    text: "cross-session retry".to_owned(),
                    attachments: vec![ProtocolPromptAttachment::Artifact {
                        artifact_id,
                        label: "other.png".to_owned(),
                    }],
                },
            )
            .expect_err("cross-session artifacts are not addressable");
        assert!(matches!(
            error,
            crate::state::DomainCommandError::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn artifact_queries_fail_with_a_capability_error_when_not_advertised() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (endpoint, _requests) =
            ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
        let (respond, result) = tokio::sync::oneshot::channel();

        let outcome = core.handle(
            &endpoint,
            ServerRequest::Query {
                client_id: ClientId::from("plain"),
                request_id: nakode_protocol::RequestId::from("artifact-query"),
                query: Query::GetArtifact {
                    artifact_id: nakode_protocol::ArtifactId::from("artifact-1"),
                },
                respond,
            },
        );
        assert!(!outcome.changed);
        let error = result
            .await
            .expect("query response")
            .expect_err("capability is not advertised");
        assert_eq!(error.code, ErrorCode::CapabilityUnsupported);
        assert!(error.message.contains("artifact transfer"));
    }

    #[test]
    fn command_receipts_use_the_target_sessions_revision() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(
            created
                .resource_id
                .expect("created session has a resource id"),
        );
        for _ in 0..4 {
            core.engine_for_mut(&first_id)
                .expect("first session")
                .note_state_change();
        }
        core.engine_for_mut(&second_id)
            .expect("second session")
            .note_state_change();

        let (result, _, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("second-session-revision"),
            Some(2),
            false,
            Command::RunShell {
                session_id: second_id.clone(),
                command: "pwd".to_owned(),
            },
        );
        let accepted = result.expect("the second session revision is current");
        assert_eq!(effect_session, Some(second_id));
        assert!(changed);
        assert_eq!(accepted.revision, Some(3));
    }

    #[test]
    fn list_runs_pages_every_omitted_run_with_an_exclusive_cursor() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let _ = state.install_subagents(
            (0..130)
                .map(|index| SubagentRecord {
                    parent_session_id: session_id.to_string(),
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
                    status: crate::state::SubagentStatus::Completed,
                    latest_activity: "Completed".to_owned(),
                    transcript: Vec::new(),
                    observability: SubagentObservability::default(),
                    transcript_has_earlier: false,
                })
                .collect(),
        );
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let mut before = None;
        let mut ids = Vec::new();
        loop {
            let QueryResult::Runs(page) = core
                .query(Query::ListRuns {
                    session_id: session_id.clone(),
                    before: before.clone(),
                    limit: 31,
                })
                .expect("list runs")
            else {
                panic!("run page");
            };
            if page.runs.is_empty() {
                break;
            }
            before = page.runs.first().map(|run| run.id.clone());
            ids.extend(page.runs.into_iter().map(|run| run.id));
            if !page.has_earlier {
                break;
            }
        }
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 130);
        assert_eq!(ids.first(), Some(&RunId::from("run-000")));
        assert_eq!(ids.last(), Some(&RunId::from("run-129")));
    }

    #[test]
    fn run_text_windows_recover_every_truncated_metadata_field() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let objective = "objective-é".repeat(MAX_RUN_TEXT_BYTES / 4);
        let latest_activity = "activity-🦀".repeat(MAX_RUN_TEXT_BYTES / 4);
        let result = "result-λ".repeat(MAX_RUN_TEXT_BYTES / 4);
        let _ = state.install_subagents(vec![SubagentRecord {
            parent_session_id: session_id.to_string(),
            id: "run-long-text".to_owned(),
            agent: "reviewer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: objective.clone(),
            status: crate::state::SubagentStatus::Completed,
            latest_activity: latest_activity.clone(),
            transcript: vec![TranscriptEntry {
                id: "final-result".to_owned(),
                key: None,
                kind: EntryKind::Assistant,
                title: "reviewer".to_owned(),
                body: result.clone(),
                status: EntryStatus::Complete,
                created_at_ms: None,
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                source_transport: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        }]);
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let run_id = RunId::from("run-long-text");

        assert_eq!(
            read_run_text_field(&core, &run_id, RunTextField::Objective),
            objective
        );
        assert_eq!(
            read_run_text_field(&core, &run_id, RunTextField::LatestActivity),
            latest_activity
        );
        assert_eq!(
            read_run_text_field(&core, &run_id, RunTextField::Result),
            result
        );
        assert_eq!(
            read_run_text_field(&core, &run_id, RunTextField::Outcome),
            result
        );
    }

    #[test]
    fn run_transcript_pages_recover_every_older_entry() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 500);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let _ = state.install_subagents(vec![SubagentRecord {
            parent_session_id: session_id.to_string(),
            id: "run-history".to_owned(),
            agent: "reviewer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Review history".to_owned(),
            status: crate::state::SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: (0..260)
                .map(|index| TranscriptEntry {
                    id: format!("run-entry-{index:03}"),
                    key: Some(format!("assistant:{index:03}")),
                    kind: EntryKind::Assistant,
                    title: "reviewer".to_owned(),
                    body: format!("entry body {index:03}"),
                    status: EntryStatus::Complete,
                    created_at_ms: None,
                    provider_id: None,
                    model_id: None,
                    owner_turn_id: None,
                    reasoning_effort: None,
                    fast_mode: None,
                    source_transport: None,
                    tool_audit_json: None,
                })
                .collect(),
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        }]);
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let run_id = RunId::from("run-history");
        let mut before = None;
        let mut entries = Vec::new();
        loop {
            let QueryResult::Transcript(page) = core
                .query(Query::GetRunTranscriptPage {
                    run_id: run_id.clone(),
                    before: before.clone(),
                    limit: 37,
                })
                .expect("run transcript page")
            else {
                panic!("transcript result");
            };
            let page = *page;
            assert!(page.entries.len() <= 37);
            if page.entries.is_empty() {
                break;
            }
            before = page.entries.first().map(|entry| entry.id.clone());
            entries.extend(page.entries.into_iter().map(|entry| (entry.id, entry.body)));
            if !page.has_earlier {
                break;
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        assert_eq!(entries.len(), 260);
        let first_entry_id = entries
            .iter()
            .find_map(|(id, body)| (body == "entry body 000").then(|| id.clone()))
            .expect("oldest run entry");
        assert!(entries.iter().any(|(_, body)| body == "entry body 259"));

        let QueryResult::TranscriptBody(window) = core
            .query(Query::GetTranscriptBodyWindow {
                owner: TranscriptOwner::Run { run_id },
                entry_id: first_entry_id,
                before_byte: None,
                limit_bytes: 32,
            })
            .expect("run body window")
        else {
            panic!("transcript body result");
        };
        assert_eq!(window.body, "entry body 000");
        assert_eq!(window.start_byte, 0);
        assert!(!window.has_earlier);
    }

    #[test]
    fn transcript_body_windows_page_backward_on_utf8_boundaries() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 100);
        let body = "αβ🙂gamma終";
        state.transcript.upsert(
            "assistant:utf8",
            EntryKind::Assistant,
            "Nakode",
            body,
            EntryStatus::Complete,
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let QueryResult::Session(session) = core
            .query(Query::GetSession {
                session_id: session_id.clone(),
            })
            .expect("session")
        else {
            panic!("session result");
        };
        let entry_id = session.transcript.entries[0].id.clone();

        let mut before_byte = None;
        let mut windows = Vec::new();
        loop {
            let QueryResult::TranscriptBody(window) = core
                .query(Query::GetTranscriptBodyWindow {
                    owner: TranscriptOwner::Session {
                        session_id: session_id.clone(),
                    },
                    entry_id: entry_id.clone(),
                    before_byte,
                    limit_bytes: 7,
                })
                .expect("UTF-8 body window")
            else {
                panic!("transcript body result");
            };
            assert!(window.body.len() <= 7);
            assert!(
                body.is_char_boundary(usize::try_from(window.start_byte).expect("body offset"))
            );
            before_byte = Some(window.start_byte);
            windows.push(window.body);
            if !window.has_earlier {
                break;
            }
        }
        windows.reverse();
        assert_eq!(windows.concat(), body);

        let invalid_boundary = core
            .query(Query::GetTranscriptBodyWindow {
                owner: TranscriptOwner::Session {
                    session_id: session_id.clone(),
                },
                entry_id: entry_id.clone(),
                before_byte: Some(1),
                limit_bytes: 7,
            })
            .expect_err("cursor must be a UTF-8 boundary");
        assert_eq!(invalid_boundary.code, ErrorCode::InvalidRequest);
        assert!(
            invalid_boundary
                .message
                .contains("UTF-8 character boundary")
        );

        let past_end = core
            .query(Query::GetTranscriptBodyWindow {
                owner: TranscriptOwner::Session { session_id },
                entry_id,
                before_byte: Some(u64::try_from(body.len()).unwrap_or(u64::MAX) + 1),
                limit_bytes: 7,
            })
            .expect_err("cursor must not exceed the body");
        assert_eq!(past_end.code, ErrorCode::InvalidRequest);
        assert!(past_end.message.contains("exceeds entry length"));
    }

    #[tokio::test]
    async fn large_streams_publish_bounded_deltas_instead_of_growing_snapshots() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 5_000);
        state
            .transcript
            .append_delta("stream", EntryKind::Assistant, "Nakode", "seed");
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (endpoint, _requests) =
            ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
        let scope = SubscriptionScope::Session {
            session_id: session_id.clone(),
        };
        let mut publications = endpoint.subscribe_publications();
        let delta = "x".repeat(2 * 1024 * 1024);
        core.engine_for_mut(&session_id)
            .expect("session")
            .state_mut()
            .transcript
            .append_delta("stream", EntryKind::Assistant, "Nakode", &delta);
        core.commit_and_publish_session_delta(&endpoint, &session_id);

        let all_events = drain_publications(&mut publications);
        let workspace_scope = SubscriptionScope::Workspace {
            workspace_id: crate::state::projection::workspace_id("/tmp/project"),
        };
        assert!(
            all_events
                .iter()
                .all(|event| !event.scopes.contains(&workspace_scope)),
            "streaming transcript deltas must not rebuild or invalidate the workspace projection"
        );
        let events = all_events
            .into_iter()
            .filter(|event| event.scopes.contains(&scope))
            .collect::<Vec<_>>();
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| !matches!(
            event.event,
            ViewEvent::SessionChanged { .. } | ViewEvent::BootstrapChanged { .. }
        )));
        let delta_events = events
            .iter()
            .filter_map(|event| match &event.event {
                ViewEvent::TranscriptEntryDelta { delta, .. } => Some(delta),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(delta_events.len() > 1);
        assert_eq!(
            delta_events.iter().map(|delta| delta.len()).sum::<usize>(),
            delta.len()
        );
        assert!(
            delta_events
                .iter()
                .all(|delta| delta.len() <= MAX_TRANSCRIPT_DELTA_BYTES)
        );
        let encoded_bytes = events
            .iter()
            .map(|event| {
                serde_json::to_vec(&event.event)
                    .expect("encode event")
                    .len()
            })
            .sum::<usize>();
        assert!(encoded_bytes < delta.len() + events.len() * 1_024);
        assert!(events.iter().all(|event| {
            serde_json::to_vec(&event.event)
                .expect("encode event")
                .len()
                < MAX_API_MESSAGE_BYTES
        }));
    }

    #[tokio::test]
    async fn run_subscriptions_receive_bounded_run_deltas_directly() {
        let mut state = AppState::new_unconfigured("/tmp/project", None, 5_000);
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let _ = state.install_subagents(vec![SubagentRecord {
            parent_session_id: session_id.to_string(),
            id: "run-stream".to_owned(),
            agent: "reviewer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Review".to_owned(),
            status: crate::state::SubagentStatus::Completed,
            latest_activity: "Working".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "run-entry".to_owned(),
                key: Some("stream".to_owned()),
                kind: EntryKind::Assistant,
                title: "reviewer".to_owned(),
                body: "seed".to_owned(),
                status: EntryStatus::Running,
                created_at_ms: None,
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                source_transport: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        }]);
        state.client.subagent_modal = Some("run-stream".to_owned());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (endpoint, _requests) =
            ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
        let scope = SubscriptionScope::Run {
            run_id: RunId::from("run-stream"),
        };
        let mut publications = endpoint.subscribe_publications();
        let delta = "r".repeat(512 * 1024);
        let state = core
            .engine_for_mut(&session_id)
            .expect("session")
            .state_mut();
        state.client.subagent_modal = Some("run-stream".to_owned());
        state
            .selected_subagent_transcript_mut()
            .expect("selected run transcript")
            .0
            .append_delta("stream", EntryKind::Assistant, "reviewer", &delta);
        core.commit_and_publish_session(&endpoint, &session_id);

        let events = drain_publications(&mut publications)
            .into_iter()
            .filter(|event| event.scopes.contains(&scope))
            .collect::<Vec<_>>();
        assert!(
            events
                .iter()
                .any(|event| matches!(event.event, ViewEvent::RunTranscriptEntryDelta { .. }))
        );
        assert!(events.iter().all(|event| !matches!(
            event.event,
            ViewEvent::RunChanged { .. } | ViewEvent::SessionChanged { .. }
        )));
        assert!(events.iter().all(|event| {
            serde_json::to_vec(&event.event)
                .expect("encode run event")
                .len()
                < MAX_API_MESSAGE_BYTES
        }));
    }

    #[test]
    fn logical_session_selects_a_model_before_a_native_session_exists() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    model_catalog: CapabilitySupport::Supported,
                    session_model_config: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Models(vec![
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-a".to_owned(),
                    is_default: true,
                    capabilities: crate::codex::model_capabilities(),
                },
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-b".to_owned(),
                    is_default: false,
                    capabilities: crate::codex::model_capabilities(),
                },
            ]),
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        assert!(state.provider_session_id.is_none());
        assert!(state.client.model_picker.is_none());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("select-logical-session-model"),
            None,
            false,
            Command::SelectModel {
                target: ModelTarget::Session {
                    session_id: session_id.clone(),
                },
                model_id: ModelId::from("openai-codex/model-b"),
                options: ModelOptions::default(),
            },
        );
        result.expect("logical-session model selection is accepted");
        assert!(effects.is_empty());
        assert_eq!(effect_session, Some(session_id.clone()));
        assert!(changed);
        let state = core
            .engine_for(&session_id)
            .expect("session runtime")
            .state();
        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
        assert!(state.provider_session_id.is_none());
        assert!(
            state.client.model_picker.is_none(),
            "server model selection must not drive a client picker"
        );

        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("start-with-logical-session-model"),
            None,
            false,
            Command::SendPrompt {
                session_id,
                prompt: PromptInput {
                    text: "Use the selected model".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );
        result.expect("prompt is accepted");
        let start_model = effects.iter().find_map(|effect| match effect {
            crate::state::Effect::Backend(BackendCommand::StartSession { model, .. }) => {
                model.as_deref()
            }
            _ => None,
        });
        assert_eq!(start_model, Some("model-b"), "{effects:#?}");
    }

    #[test]
    fn provider_default_selection_does_not_override_the_current_session() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    model_catalog: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Models(vec![
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-a".to_owned(),
                    is_default: true,
                    capabilities: crate::codex::model_capabilities(),
                },
                ModelInfo {
                    provider: CODEX_PROVIDER.to_owned(),
                    id: "model-b".to_owned(),
                    is_default: false,
                    capabilities: crate::codex::model_capabilities(),
                },
            ]),
        );
        let selected_before = state.selected_model.clone();
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("select-provider-default"),
            None,
            false,
            Command::SelectModel {
                target: ModelTarget::ProviderDefault {
                    provider_id: CODEX_PROVIDER.into(),
                },
                model_id: ModelId::from("openai-codex/model-b"),
                options: ModelOptions::default(),
            },
        );
        result.expect("provider default selection is accepted");
        assert_eq!(effect_session, Some(session_id.clone()));
        assert!(changed);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::SetDefaultModel { provider, model }
                if provider == CODEX_PROVIDER && model == "model-b"
        )));
        let state = core
            .engine_for(&session_id)
            .expect("session runtime")
            .state();
        assert_eq!(state.selected_model, selected_before);
        assert!(state.client.model_picker.is_none());
        assert!(state.models.iter().any(|model| model.id == "model-b"
            && model.provider == CODEX_PROVIDER
            && model.is_default));
    }

    /// Stages one non-default session and returns its id.
    ///
    /// Non-default deliberately: the default session's engine may not be evicted, so a lifecycle
    /// assertion made on it would pass for the wrong reason.
    fn attached_session(core: &mut ServerCore) -> SessionId {
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let accepted = create_default_session(core, &workspace_id).expect("a session is created");
        SessionId::from(
            accepted
                .0
                .resource_id
                .expect("a created session names itself"),
        )
    }

    /// A session whose backend has gone can be deleted, without any close first.
    ///
    /// This is the defect: `DeleteSession` used to answer an attached session with "close it before
    /// deleting it", and there is no `CloseSession` in the protocol to answer that with. Nothing
    /// evicted an engine either, so a session stayed attached for the life of the process and the
    /// refusal was permanent. A dead session was therefore undeletable forever.
    #[test]
    fn a_dead_session_that_was_never_closed_is_deletable() {
        let (mut core, _) = ready_codex_server();
        let dead = attached_session(&mut core);
        core.engine_for_mut(&dead)
            .expect("session runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "provider exited".to_owned(),
                },
            );

        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-dead"),
            None,
            false,
            Command::DeleteSession {
                session_id: dead.clone(),
            },
        );

        let accepted = result.expect("a dead session is deletable without a close first");
        assert_eq!(accepted.resource_id.as_deref(), Some(dead.as_str()));
        // The backend is released BEFORE the history it was writing to is removed.
        assert!(
            matches!(
                effects.as_slice(),
                [
                    crate::state::Effect::ReleaseSessionBackends(released),
                    crate::state::Effect::DeleteSession(deleted),
                ] if released == dead.as_str() && deleted == dead.as_str()
            ),
            "expected release-then-delete, got: {effects:?}"
        );
        assert!(
            core.engine_for(&dead).is_none(),
            "deleting a session must evict its engine"
        );
    }

    #[test]
    fn deleting_an_attached_session_by_unique_prefix_uses_its_canonical_identity_everywhere() {
        let (mut core, _) = ready_codex_server();
        let attached = attached_session(&mut core);
        core.engine_for_mut(&attached)
            .expect("session runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "provider exited".to_owned(),
                },
            );
        let prefix_len = (1..attached.as_str().len())
            .find(|length| {
                let prefix = &attached.as_str()[..*length];
                core.sessions_by_id
                    .keys()
                    .filter(|candidate| candidate.as_str().starts_with(prefix))
                    .count()
                    == 1
            })
            .expect("attached session has a unique proper prefix");
        let prefix = SessionId::from(attached.as_str()[..prefix_len].to_owned());

        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-dead-prefix"),
            None,
            false,
            Command::DeleteSession { session_id: prefix },
        );

        let accepted = result.expect("a uniquely prefixed dead session is deletable");
        assert_eq!(accepted.resource_id.as_deref(), Some(attached.as_str()));
        assert!(matches!(
            effects.as_slice(),
            [
                crate::state::Effect::ReleaseSessionBackends(released),
                crate::state::Effect::DeleteSession(deleted),
            ] if released == attached.as_str() && deleted == attached.as_str()
        ));
        assert!(core.engine_for(&attached).is_none());
    }

    /// A legacy session with orphaned busy display state behind a dead backend is deletable too.
    ///
    /// Live disconnect handling now settles executable delegated runs. This guard remains for older
    /// or otherwise inconsistent in-memory projections that have no execution left to cancel.
    #[test]
    fn a_session_stuck_busy_behind_a_dead_backend_is_deletable() {
        let (mut core, _) = ready_codex_server();
        let dead = attached_session(&mut core);
        {
            let state = core
                .engine_for_mut(&dead)
                .expect("session runtime")
                .state_mut();
            state.subagents.push(crate::state::SubagentRun {
                id: "run-1".to_owned(),
                agent: "reviewer".to_owned(),
                provider: CODEX_PROVIDER.to_owned(),
                model: None,
                provider_session_id: None,
                usage: crate::backend::BackendTokenUsage::default(),
                objective: "review the diff".to_owned(),
                status: crate::session::SubagentStatus::Working,
                latest_activity: String::new(),
                observability: SubagentObservability::default(),
            });
            state.handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "provider exited".to_owned(),
                },
            );
            assert!(
                state.is_busy(),
                "the stuck-busy state this test exists for did not reproduce"
            );
        }

        let (result, _, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-stuck"),
            None,
            false,
            Command::DeleteSession {
                session_id: dead.clone(),
            },
        );

        result.expect("a session with no backend behind it has no work to cancel");
        assert!(core.engine_for(&dead).is_none());
    }

    /// A session actually working is still refused, and cancelling is still its verb.
    ///
    /// The proto documents this refusal. Deleting mid-inference would drop the history a live provider
    /// child is still writing to, so this command must not become a way to reach `CancelSessionWork`.
    #[test]
    fn deleting_a_working_session_is_refused() {
        let (mut core, _) = ready_codex_server();
        let working = attached_session(&mut core);
        core.engine_for_mut(&working)
            .expect("session runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::TurnStarted {
                    turn_id: "provider-turn".to_owned(),
                },
            );

        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-busy"),
            None,
            false,
            Command::DeleteSession {
                session_id: working.clone(),
            },
        );

        let error = result.expect_err("a working session is not deletable");
        assert!(
            error.message.contains("work in flight"),
            "expected the in-flight refusal, got: {}",
            error.message
        );
        assert!(effects.is_empty(), "a refused delete performs no effect");
        assert!(
            core.engine_for(&working).is_some(),
            "a refused delete must not evict anything"
        );
    }

    /// The workspace's initial role remains protected while it owns a live native session.
    #[test]
    fn deleting_an_active_initial_session_is_refused_with_its_reason() {
        let (mut core, default_session) = ready_codex_server();

        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-default"),
            None,
            false,
            Command::DeleteSession {
                session_id: default_session.clone(),
            },
        );

        let error = result.expect_err("the active initial session is not deletable");
        assert!(
            error.message.contains("active initial session"),
            "expected the initial-session refusal, got: {}",
            error.message
        );
        assert!(effects.is_empty());
        assert_eq!(core.default_session_id(), &default_session);
        assert!(core.engine_for(&default_session).is_some());
    }

    /// Once its provider resource is closed, an initial session is ordinary persisted history.
    #[test]
    fn deleting_a_closed_initial_session_installs_an_empty_successor() {
        let (mut core, initial_session) = ready_codex_server();
        core.engine_for_mut(&initial_session)
            .expect("initial runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "workspace session closed".to_owned(),
                },
            );

        let (result, effects, effect_session, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-closed-initial"),
            None,
            false,
            Command::DeleteSession {
                session_id: initial_session.clone(),
            },
        );

        result.expect("a closed initial session is deletable");
        assert!(matches!(
            effects.as_slice(),
            [
                crate::state::Effect::ReleaseSessionBackends(released),
                crate::state::Effect::DeleteSession(deleted),
            ] if released == initial_session.as_str() && deleted == initial_session.as_str()
        ));
        assert_eq!(
            effect_session, None,
            "effects must run on the post-command default"
        );
        let successor = core.default_session_id().clone();
        assert_ne!(successor, initial_session);
        assert!(core.engine_for(&initial_session).is_none());
        assert!(core.engine_for(&successor).is_some());
        assert!(
            core.workspace_bootstrap().sessions.is_empty(),
            "the unpersisted successor is control-plane state, not an epoch-dated conversation"
        );
    }

    /// A stale persisted row for the former initial id cannot restore its live-role protection.
    #[test]
    fn stale_initial_records_and_repeated_deletion_are_deterministic() {
        let (mut core, former_initial) = ready_codex_server();
        core.engine_for_mut(&former_initial)
            .expect("initial runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "closed".to_owned(),
                },
            );
        core.delete_session_command(&former_initial)
            .expect("first delete rotates the role");
        let successor = core.default_session_id().clone();
        core.replace_session_records(vec![SessionRecord {
            id: former_initial.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "stale-thread".to_owned(),
            workspace: "/tmp/project".to_owned(),
            working_directory: "/tmp/project".to_owned(),
            title: "New session".to_owned(),
            model: None,
            model_options: crate::backend::ModelOptions::default(),
            last_turn: None,
            owner_turns: Vec::new(),
            created_at: 0,
            updated_at: 0,
            last_owner_activity_at: None,
            enabled_skill_ids: None,
            owned_provider_sessions: Vec::new(),
        }]);

        for attempt in 0..2 {
            let (_, effects) = core
                .delete_session_command(&former_initial)
                .unwrap_or_else(|error| panic!("repeat {attempt} was refused: {error}"));
            assert!(matches!(
                effects.as_slice(),
                [crate::state::Effect::DeleteSession(id)] if id == former_initial.as_str()
            ));
            assert_eq!(core.default_session_id(), &successor);
        }
    }

    /// Session creation continues through the successor after the old initial session is gone.
    #[test]
    fn a_session_can_be_created_after_the_closed_initial_session_is_deleted() {
        let (mut core, initial) = ready_codex_server();
        core.engine_for_mut(&initial)
            .expect("initial runtime")
            .state_mut()
            .handle_provider_backend(
                CODEX_PROVIDER,
                BackendEvent::Disconnected {
                    reason: "closed".to_owned(),
                },
            );
        core.delete_session_command(&initial)
            .expect("closed initial deletion");
        let workspace_id = core.workspace_bootstrap().workspace_id;

        let (accepted, _) = create_default_session(&mut core, &workspace_id)
            .expect("session creation recovers through the successor");
        let created = SessionId::from(accepted.resource_id.expect("created id"));
        assert_ne!(created, initial);
        assert_ne!(&created, core.default_session_id());
        assert!(core.engine_for(&created).is_some());
    }

    /// Session lifecycle does not accumulate engines.
    ///
    /// `sessions_by_id` and `published_sessions` were insert-only, so every session ever created or
    /// opened retained a whole `DomainState` for the life of the process. Repeated create/delete now
    /// returns to its starting size instead of growing once per cycle.
    #[test]
    fn repeated_session_lifecycles_do_not_retain_engines() {
        let (mut core, _) = ready_codex_server();
        let engines = core.sessions_by_id.len();
        let published = core.published_sessions.len();

        for cycle in 0..8 {
            let session_id = attached_session(&mut core);
            let (result, _, _, _) = core.execute_idempotent(
                IdempotencyKey::from(format!("cycle-{cycle}")),
                None,
                false,
                Command::DeleteSession {
                    session_id: session_id.clone(),
                },
            );
            result.expect("each cycle deletes its own session");
        }

        assert_eq!(
            core.sessions_by_id.len(),
            engines,
            "engines accumulated across create/delete cycles"
        );
        assert_eq!(
            core.published_sessions.len(),
            published,
            "session projections accumulated across create/delete cycles"
        );
    }

    /// Deleting the same session twice is safe and says the same thing.
    ///
    /// The second call finds nothing attached, which is the already-deleted path. It still accepts, so
    /// a caller retrying after a partial failure — the engine evicted but the row left behind — can
    /// finish the job rather than being told the session is open.
    #[test]
    fn deleting_a_session_twice_is_accepted_both_times() {
        let (mut core, _) = ready_codex_server();
        let session_id = attached_session(&mut core);

        for attempt in 0..2 {
            let (result, effects, _, _) = core.execute_idempotent(
                IdempotencyKey::from(format!("delete-again-{attempt}")),
                None,
                false,
                Command::DeleteSession {
                    session_id: session_id.clone(),
                },
            );
            result.expect("a repeated delete is accepted");
            assert!(
                effects
                    .iter()
                    .any(|effect| matches!(effect, crate::state::Effect::DeleteSession(id) if id == session_id.as_str())),
                "every attempt must still reach persistence: {effects:?}"
            );
        }
        assert!(core.engine_for(&session_id).is_none());
    }

    /// An unattached session deletes with the persistence effect alone.
    #[test]
    fn deleting_an_unattached_session_reaches_persistence() {
        let (mut core, _) = ready_codex_server();

        // An id nobody has open is the deletable case, and its effect is the persistence one.
        let closed = SessionId::from("019fcf35-780e-7d21-aa88-c10db392bf63".to_owned());
        let (result, effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("delete-closed"),
            None,
            false,
            Command::DeleteSession {
                session_id: closed.clone(),
            },
        );
        let accepted = result.expect("an unattached session is deletable");
        assert_eq!(accepted.resource_id.as_deref(), Some(closed.as_str()));
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::DeleteSession(id)] if id == closed.as_str()
        ));
    }

    #[test]
    fn cancelling_session_work_uses_one_server_owned_policy_command() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    interruption: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::SessionCreated {
                provider_session_id: "provider-session".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::TurnStarted {
                turn_id: "provider-turn".to_owned(),
            },
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("cancel-session-work"),
            None,
            false,
            Command::CancelSessionWork {
                session_id: session_id.clone(),
            },
        );
        let accepted = result.expect("session cancellation is accepted");
        assert_eq!(accepted.resource_id.as_deref(), Some(session_id.as_str()));
        assert_eq!(effect_session, Some(session_id.clone()));
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::Backend(BackendCommand::InterruptTurn {
                provider_session_id,
                turn_id,
            })] if provider_session_id == "provider-session" && turn_id == "provider-turn"
        ));
        assert!(
            core.engine_for(&session_id)
                .expect("session runtime")
                .state()
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.cancelling)
        );
    }

    #[test]
    fn cancelling_idle_session_work_is_idempotently_accepted() {
        let (mut core, session_id) = ready_codex_server();

        for attempt in 0..2 {
            let (result, effects, effect_session, _) = core.execute_idempotent(
                IdempotencyKey::from(format!("cancel-idle-{attempt}")),
                None,
                false,
                Command::CancelSessionWork {
                    session_id: session_id.clone(),
                },
            );
            let accepted = result.expect("idle cancellation is a successful no-op");
            assert_eq!(accepted.resource_id.as_deref(), Some(session_id.as_str()));
            assert_eq!(effect_session, Some(session_id.clone()));
            assert!(effects.is_empty());
        }
    }

    #[test]
    fn current_session_cancellation_overrides_intervening_revision_progress() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    interruption: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::SessionCreated {
                provider_session_id: "provider-session".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::TurnStarted {
                turn_id: "provider-turn".to_owned(),
            },
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let observed_revision = core
            .engine_for(&session_id)
            .expect("session engine")
            .revision();

        // Primary and delegated backend events share this revision boundary. Advancing it after the
        // client observation deterministically models the race without making cancellation timing
        // dependent on an asynchronous provider.
        core.engine_for_mut(&session_id)
            .expect("session engine")
            .note_state_change();

        let (fenced_result, fenced_effects, _, fenced_changed) = core.execute_idempotent(
            IdempotencyKey::from("revision-fenced-cancel"),
            Some(observed_revision),
            false,
            Command::CancelSessionWork {
                session_id: session_id.clone(),
            },
        );
        assert!(matches!(
            fenced_result,
            Err(error)
                if error.code == ErrorCode::Conflict
                    && error.message == "the expected revision is stale"
        ));
        assert!(fenced_effects.is_empty());
        assert!(!fenced_changed);

        let (result, effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("priority-current-work-cancel"),
            None,
            false,
            Command::CancelSessionWork { session_id },
        );
        result.expect("unfenced current-work cancellation is accepted");
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::Backend(BackendCommand::InterruptTurn { turn_id, .. })]
                if turn_id == "provider-turn"
        ));
    }

    #[test]
    fn current_session_cancellation_targets_a_successor_current_at_execution() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    interruption: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::SessionCreated {
                provider_session_id: "provider-session".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::TurnStarted {
                turn_id: "turn-1".to_owned(),
            },
        );
        state
            .enqueue_prompt("successor".to_owned(), Vec::new())
            .expect("queue successor");
        let completion_effects = state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::TurnCompleted {
                turn_id: "turn-1".to_owned(),
                outcome: TurnOutcome::Completed,
                error: None,
            },
        );
        assert!(completion_effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartTurn { .. })
        )));
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::TurnStarted {
                turn_id: "turn-2".to_owned(),
            },
        );
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (result, effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("cancel-current-successor"),
            None,
            false,
            Command::CancelSessionWork { session_id },
        );
        result.expect("current successor cancellation is accepted");
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::Backend(BackendCommand::InterruptTurn { turn_id, .. })]
                if turn_id == "turn-2"
        ));
    }

    #[test]
    fn cancelling_session_work_interrupts_manual_context_compaction() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities {
                    interruption: CapabilitySupport::Supported,
                    context_compaction: CapabilitySupport::Supported,
                    ..BackendCapabilities::default()
                },
            }),
        );
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::SessionCreated {
                provider_session_id: "provider-session".to_owned(),
                model: "model-a".to_owned(),
            },
        );
        state.compact_context().expect("manual compaction starts");
        let compaction_turn_id = state
            .context_compaction
            .as_ref()
            .expect("active compaction")
            .turn_id
            .clone();
        let session_id = SessionId::from(state.nakode_session_id.clone());
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("cancel-session-compaction"),
            None,
            false,
            Command::CancelSessionWork {
                session_id: session_id.clone(),
            },
        );

        result.expect("context compaction cancellation is accepted");
        assert_eq!(effect_session, Some(session_id));
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::Backend(BackendCommand::InterruptTurn {
                provider_session_id,
                turn_id,
            })] if provider_session_id == "provider-session" && turn_id == &compaction_turn_id
        ));
    }

    #[test]
    fn browser_availability_check_is_a_server_owned_workspace_effect() {
        let (mut core, session_id) = ready_codex_server();
        let workspace_id = crate::state::projection::workspace_id(&core.engine().state().workspace);

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("check-agent-browser"),
            None,
            false,
            Command::CheckAgentBrowser {
                workspace_id: workspace_id.clone(),
            },
        );

        let accepted = result.expect("browser check is accepted");
        assert_eq!(accepted.resource_id.as_deref(), Some(workspace_id.as_str()));
        assert_eq!(effect_session, Some(session_id));
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::CheckAgentBrowser]
        ));
    }

    #[test]
    fn workspace_reload_effect_targets_the_selected_session() {
        let (mut core, default_session_id) = ready_codex_server();
        let workspace_id = crate::state::projection::workspace_id(&core.engine().state().workspace);
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create selected session");
        let selected_session_id = SessionId::from(created.resource_id.expect("created session id"));
        assert_ne!(selected_session_id, default_session_id);
        core.engine_for_mut(&selected_session_id)
            .expect("selected session")
            .state_mut()
            .provider_session_id = Some("selected-provider-session".to_owned());

        let (result, effects, effect_session, changed) = core.execute_idempotent(
            IdempotencyKey::from("reload-selected-session"),
            None,
            false,
            Command::ReloadWorkspace {
                workspace_id,
                session_id: selected_session_id.clone(),
            },
        );

        result.expect("workspace reload is accepted");
        assert_eq!(effect_session, Some(selected_session_id));
        assert!(changed);
        assert!(matches!(
            effects.as_slice(),
            [crate::state::Effect::ReloadConfiguration]
        ));
    }

    #[test]
    fn same_provider_work_is_accepted_for_independent_sessions() {
        let mut state =
            AppState::new_for_backend(project_workspace(), None, 100, CODEX_PROVIDER, "Codex");
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Ready(BackendIdentity {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                version: None,
                capabilities: BackendCapabilities::default(),
            }),
        );
        let first_id = SessionId::from(state.nakode_session_id.clone());
        let workspace_id = crate::state::projection::workspace_id(&state.workspace);
        let mut core = ServerCore::new(ServiceEngine::new(state), Vec::new(), Vec::new());
        let (created, _) =
            create_default_session(&mut core, &workspace_id).expect("create second session");
        let second_id = SessionId::from(
            created
                .resource_id
                .expect("created session has a resource id"),
        );

        let (first_result, first_effects, _, _) = core.execute_idempotent(
            IdempotencyKey::from("first-prompt"),
            None,
            false,
            Command::SendPrompt {
                session_id: first_id.clone(),
                prompt: PromptInput {
                    text: "first".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );
        first_result.expect("first prompt is accepted");
        assert!(first_effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartSession { .. })
        )));

        let (second_result, second_effects, _, changed) = core.execute_idempotent(
            IdempotencyKey::from("second-prompt"),
            None,
            false,
            Command::SendPrompt {
                session_id: second_id.clone(),
                prompt: PromptInput {
                    text: "second".to_owned(),
                    attachments: Vec::new(),
                },
            },
        );
        second_result.expect("second prompt is accepted concurrently");
        assert!(changed);
        assert!(second_effects.iter().any(|effect| matches!(
            effect,
            crate::state::Effect::Backend(BackendCommand::StartSession { .. })
        )));
        assert!(
            core.engine_for(&first_id)
                .expect("first runtime")
                .state()
                .is_busy()
        );
        assert!(
            core.engine_for(&second_id)
                .expect("second runtime")
                .state()
                .is_busy()
        );
    }

    #[test]
    fn soul_save_updates_new_logical_sessions_but_preserves_existing_snapshots() {
        let (mut core, _) = ready_codex_server();
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("SOUL.md");
        std::fs::write(&path, "Original Soul").expect("original soul");
        core.install_soul_store(SoulStore::new(&path));
        core.session_template.install_prompt_addenda(
            PromptAddenda::load(None, Some(&path)).expect("original addenda"),
        );
        let workspace_id = core.workspace_bootstrap().workspace_id;

        let (first, _) = create_default_session(&mut core, &workspace_id).expect("first session");
        let first_id = SessionId::from(first.resource_id.expect("first id"));
        let initial = core
            .query(Query::GetSoul {
                workspace_id: workspace_id.clone(),
            })
            .expect("initial soul");
        let QueryResult::SoulDocument(initial) = initial else {
            panic!("expected Soul query");
        };
        core.save_soul_command(&workspace_id, "Changed Soul", initial.digest.as_deref())
            .expect("save changed soul");
        let (second, _) = create_default_session(&mut core, &workspace_id).expect("second session");
        let second_id = SessionId::from(second.resource_id.expect("second id"));

        let instructions_for = |core: &mut ServerCore, session_id: SessionId, key: &str| {
            let (result, effects, _, _) = core.execute_idempotent(
                IdempotencyKey::from(key),
                None,
                false,
                Command::SendPrompt {
                    session_id,
                    prompt: PromptInput {
                        text: "hello".to_owned(),
                        attachments: Vec::new(),
                    },
                },
            );
            result.expect("prompt accepted");
            effects
                .into_iter()
                .find_map(|effect| match effect {
                    crate::state::Effect::Backend(BackendCommand::StartSession {
                        instructions,
                        ..
                    }) => instructions,
                    _ => None,
                })
                .expect("start instructions")
        };

        let first_instructions = instructions_for(&mut core, first_id, "first-snapshot");
        let second_instructions = instructions_for(&mut core, second_id, "second-snapshot");
        assert!(first_instructions.contains("[Soul]\nOriginal Soul"));
        assert!(!first_instructions.contains("Changed Soul"));
        assert!(second_instructions.contains("[Soul]\nChanged Soul"));
        assert!(!second_instructions.contains("Original Soul"));
    }

    #[test]
    fn soul_command_round_trips_and_rejects_stale_or_cross_workspace_access() {
        let (mut core, _) = ready_codex_server();
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("SOUL.md");
        core.install_soul_store(SoulStore::new(&path));
        let workspace_id = core.workspace_bootstrap().workspace_id;

        let initial = core
            .query(Query::GetSoul {
                workspace_id: workspace_id.clone(),
            })
            .expect("query missing");
        let QueryResult::SoulDocument(initial) = initial else {
            panic!("expected Soul query");
        };
        assert_eq!(initial.source, "missing");

        let (_, effects) = core
            .save_soul_command(&workspace_id, "singleton", None)
            .expect("create");
        assert!(effects.is_empty());
        let saved = core
            .query(Query::GetSoul {
                workspace_id: workspace_id.clone(),
            })
            .expect("query file");
        let QueryResult::SoulDocument(saved) = saved else {
            panic!("expected Soul query");
        };
        assert_eq!(saved.source, "file");
        assert_eq!(saved.content, "singleton");
        assert_eq!(saved.path, path.to_string_lossy());
        assert!(
            core.save_soul_command(&workspace_id, "clobber", None)
                .is_err()
        );
        assert!(
            core.query(Query::GetSoul {
                workspace_id: WorkspaceId::from("another-workspace"),
            })
            .is_err()
        );
        assert_eq!(
            SoulSource::File,
            SoulStore::new(path).read().expect("persisted").source
        );
    }

    #[tokio::test]
    async fn public_query_boundary_bootstraps_through_the_real_server_core() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let engine = ServiceEngine::new(state);
        let mut core = ServerCore::new(engine, Vec::new(), Vec::new());
        let (endpoint, mut requests) = ServerEndpoint::channel(
            "test",
            ServiceCapabilities {
                supported: BTreeSet::from([
                    ServiceCapability::Subscriptions,
                    ServiceCapability::MultipleClients,
                    ServiceCapability::ArtifactTransfer,
                    ServiceCapability::ExternalTools,
                ]),
            },
            16,
        );
        let runtime = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                while let Some(request) = requests.recv().await {
                    let _ = core.handle(&endpoint, request);
                }
            })
        };

        let response = endpoint
            .execute_query(
                ClientId::from("plain"),
                Query::Bootstrap {
                    workspace: "/tmp/project".to_owned(),
                    session_id: None,
                },
            )
            .await
            .expect("bootstrap query");
        assert!(matches!(response.value, QueryResult::Bootstrap(_)));
        runtime.abort();
    }
}
