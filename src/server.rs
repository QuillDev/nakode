//! Native server command/query core.
//!
//! The transport talks only to this type. It owns canonical state and returns
//! server effects for the runtime supervisor to execute; clients never receive
//! provider commands, persistence handles, or process objects.

pub(crate) mod runtime;

use std::{
    collections::{HashMap, VecDeque},
    path::{Component, Path, PathBuf},
};

use nakode_protocol::{
    AgentDefinitionInput, AgentSessionId, Command, CommandAccepted, CredentialInput, EntryId,
    ErrorCode, IdempotencyKey, MAX_ARTIFACT_BYTES, MAX_TRANSCRIPT_DELTA_BYTES, McpGrantPolicy,
    McpServerInput, McpSessionGrant, ModelTarget, PromptInput, ProviderId, Query, QueryResult,
    RunId, RunMetadataView, RunTextField, RunView, ServiceCapability, ServiceError, SessionId,
    SessionMetadataView, SessionView, Snapshot, SoulDocumentView, SubscriptionScope,
    SubscriptionView, TranscriptEntryStatus, TranscriptEntryView, TranscriptOwner, TranscriptPage,
    TranscriptWindowView, ViewEvent, WorkspaceId,
};
use nakode_server::{ServerEndpoint, ServerRequest};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::{
    agent::{AgentCatalog, AgentDefinition, AgentFallbackPolicy, AgentOwnership, AgentToolProfile},
    backend::PromptAttachment,
    service::ServiceEngine,
    session::{ProviderRecord, SessionRecord},
    soul::{SoulError, SoulSource, SoulStore},
    state::{DomainCommandError, Effect},
};

const IDEMPOTENCY_CAPACITY: usize = 1_024;

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

pub struct ServerCore {
    sessions_by_id: HashMap<SessionId, ServiceEngine>,
    default_session: SessionId,
    session_template: crate::state::DomainState,
    mcp_servers: Vec<crate::mcp::McpServerRecord>,
    providers: Vec<ProviderRecord>,
    sessions: Vec<SessionRecord>,
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
            workspace_id: WorkspaceId::from(self.engine().state().workspace.clone()),
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
        let other_session_views = self
            .sessions_by_id
            .keys()
            .filter(|candidate_id| *candidate_id != session_id)
            .filter_map(|candidate_id| {
                self.published_session(candidate_id)
                    .map(|projection| (candidate_id.clone(), projection.view))
            })
            .collect::<Vec<_>>();
        if let Some(engine) = self.sessions_by_id.get_mut(session_id) {
            engine.note_state_change();
        }
        self.synchronize_workspace_state_from(session_id);
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
        self.publish_state(endpoint, session_id);
        for changed_session in changed_sessions {
            self.publish_state(endpoint, &changed_session);
        }
    }

    pub(crate) fn replace_provider_records(&mut self, providers: Vec<ProviderRecord>) {
        self.providers = providers;
    }

    pub(crate) fn provider_records(&self) -> &[ProviderRecord] {
        &self.providers
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
        let (mut result, effects) =
            if expected_revision.is_some_and(|revision| revision != command_revision) {
                (
                    Err(service_error(
                        ErrorCode::Conflict,
                        "the expected revision is stale",
                        true,
                    )),
                    Vec::new(),
                )
            } else {
                self.execute_command(command)
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
    ) -> (Result<CommandAccepted, ServiceError>, Vec<Effect>) {
        match self.try_execute_command(command) {
            Ok((accepted, effects)) => (Ok(accepted), effects),
            Err(error) => (Err(domain_error(error)), Vec::new()),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn try_execute_command(&mut self, command: Command) -> DomainCommandOutcome {
        match command {
            Command::CreateSession {
                workspace_id,
                model_id,
                options,
                tools,
                mcp_grant,
                ..
            } => self.create_session_command_with_mcp(
                &workspace_id,
                model_id.as_ref(),
                &options,
                tools,
                mcp_grant.as_ref(),
            ),
            Command::OpenSession {
                session_id,
                tools,
                mcp_grant,
            } => self.open_session_command_with_mcp(&session_id, tools, mcp_grant.as_ref()),
            Command::SendPrompt { session_id, prompt } => {
                let enqueue = self
                    .engine_for(&session_id)
                    .is_some_and(|engine| engine.state().is_busy());
                self.prompt_command(&session_id, prompt, enqueue)
            }
            Command::EnqueuePrompt { session_id, prompt } => {
                self.prompt_command(&session_id, prompt, true)
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
            Command::RunShell {
                session_id,
                command,
            } => self.run_shell_command(&session_id, command),
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
        self.create_session_command_with_mcp(workspace_id, model_id, options, tools, None)
    }

    fn create_session_command_with_mcp(
        &mut self,
        workspace_id: &WorkspaceId,
        model_id: Option<&nakode_protocol::ModelId>,
        options: &nakode_protocol::ModelOptions,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        mcp_grant: Option<&McpSessionGrant>,
    ) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        if model_id.is_none() && (options.reasoning_effort.is_some() || options.fast_mode) {
            return Err(DomainCommandError::Invalid(
                "initial model options require model_id".to_owned(),
            ));
        }
        self.refresh_session_template_addenda()?;
        let mut engine = ServiceEngine::new(self.session_template.clone());
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
            effects.extend(
                engine
                    .state_mut()
                    .configure_external_tools(tools.tools, tools.replace_builtin_tools)?,
            );
        }
        if let Some(grant) = mcp_grant {
            let (mcp_tools, archetype_grants) = self.mcp_tools_for_grant(grant)?;
            effects.extend(engine.state_mut().configure_mcp_tools(mcp_tools)?);
            engine
                .state_mut()
                .configure_mcp_archetype_grants(archetype_grants);
        }
        self.sessions_by_id.insert(session_id.clone(), engine);
        Ok(Self::accepted(Some(session_id.to_string()), effects))
    }

    #[cfg(test)]
    fn open_session_command(
        &mut self,
        session_id: &SessionId,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
    ) -> DomainCommandOutcome {
        self.open_session_command_with_mcp(session_id, tools, None)
    }

    fn open_session_command_with_mcp(
        &mut self,
        session_id: &SessionId,
        tools: Option<nakode_protocol::SessionToolConfiguration>,
        mcp_grant: Option<&McpSessionGrant>,
    ) -> DomainCommandOutcome {
        let loaded = self
            .sessions_by_id
            .keys()
            .filter(|loaded| loaded.as_str().starts_with(session_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        match loaded.as_slice() {
            [loaded] => {
                if *loaded == self.default_session
                    && !self
                        .sessions
                        .iter()
                        .any(|record| record.id == loaded.as_str())
                {
                    return Err(DomainCommandError::NotFound(session_id.to_string()));
                }
                if let Some(tools) = tools {
                    self.session_engine_mut(loaded)?
                        .state_mut()
                        .configure_or_validate_external_tools(
                            &tools.tools,
                            tools.replace_builtin_tools,
                        )?;
                }
                if mcp_grant.is_some() {
                    return Err(DomainCommandError::Invalid(
                        "an attached session cannot change its MCP grant".to_owned(),
                    ));
                }
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
        self.refresh_session_template_addenda()?;
        let mut engine = ServiceEngine::new(self.session_template.clone());
        // The workspace template may still carry the bootstrap provider/session identity. Reset that
        // clone before installing client-owned tools; restoration begins only after validation.
        let _discarded_template_effects = engine.state_mut().create_logical_session()?;
        if let Some(tools) = tools {
            engine
                .state_mut()
                .configure_external_tools(tools.tools, tools.replace_builtin_tools)?;
        }
        if let Some(grant) = mcp_grant {
            let (mcp_tools, archetype_grants) = self.mcp_tools_for_grant(grant)?;
            engine.state_mut().configure_mcp_tools(mcp_tools)?;
            engine
                .state_mut()
                .configure_mcp_archetype_grants(archetype_grants);
        }
        let effects = engine.state_mut().begin_resume(session.clone());
        let loaded_id = SessionId::from(session.id.clone());
        self.sessions_by_id.insert(loaded_id.clone(), engine);
        Ok(Self::accepted(Some(session.id), effects))
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
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        self.reload_agent_catalogue_for_session(session_id)?;
        let (text, attachments) = self.convert_prompt(session_id, prompt)?;
        let effects = if enqueue {
            self.session_engine_mut(session_id)?
                .state_mut()
                .enqueue_prompt(text, attachments)?
        } else {
            self.session_engine_mut(session_id)?
                .state_mut()
                .submit_prompt(text, attachments)?
        };
        Ok(Self::accepted(None, effects))
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

    /// Removes a logical session and everything persisted beneath it.
    ///
    /// Unlike every other session command this one does NOT `ensure_session`: requiring an attached
    /// engine would mean loading a conversation into memory in order to throw it away.
    ///
    /// **Deletion is authoritative, so an attached session is CLOSED here rather than refused.** This
    /// command used to answer an attached session with "close it before deleting it", which named an
    /// operation that does not exist: there is no `CloseSession` in the protocol, and nothing else
    /// evicts an engine. Every session ever created or opened therefore stayed attached for the life
    /// of the process and became permanently undeletable — the dead sessions that piled up in the
    /// store. Doing the close internally is what makes the caller's request satisfiable at all.
    ///
    /// A session with live work is refused, which the proto documents and which
    /// `CancelSessionWork` is the verb for — deleting mid-inference would drop the history a live
    /// provider child is still writing. That test reads liveness as well as busyness on purpose: a
    /// backend that died mid-turn can leave `is_busy` true forever (see `provider_is_live`), and the
    /// old code refused those too, with a cancel that could never land.
    ///
    /// The initial-session identity is a LIVE control-plane role, not a historical property. While its
    /// native provider session is live it remains protected. Once that resource is gone, deletion first
    /// installs a fresh, unpersisted control-plane engine as the default and only then releases the old
    /// engine. Every default-engine lookup therefore has a valid successor, while the former initial
    /// session becomes ordinary persisted history.
    fn delete_session_command(&mut self, session_id: &SessionId) -> DomainCommandOutcome {
        let lifecycle = self.sessions_by_id.get(session_id).map(|engine| {
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
            if *session_id == self.default_session && owns_live_provider_session {
                return Err(DomainCommandError::Conflict(format!(
                    "session {session_id} is this workspace's active initial session and cannot be deleted"
                )));
            }
            effects.push(Effect::ReleaseSessionBackends(session_id.to_string()));
            if *session_id == self.default_session {
                self.replace_default_session()?;
            }
            self.release_session(session_id);
        }
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

    pub(crate) fn delegate_agent_attributed(
        &mut self,
        session_id: &SessionId,
        agent_slug: &str,
        task: &str,
        parent_run_id: Option<&str>,
        request_id: u64,
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
            .delegate_agent_attributed_for_request(agent_slug, task, parent_run_id, request_id)
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
            Query::Bootstrap {
                workspace,
                session_id,
            } => {
                if workspace != self.engine().state().workspace {
                    return Err(not_found("workspace", &workspace));
                }
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
                sessions.truncate(usize::try_from(limit).unwrap_or(usize::MAX).min(500));
                Ok(QueryResult::Sessions(sessions))
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
            Query::GetDiagnostics { .. } => Err(service_error(
                ErrorCode::Internal,
                "diagnostics is served by the native runtime",
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
        Ok(QueryResult::Transcript(page))
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
        Ok(QueryResult::Transcript(page))
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
        let mut publications =
            self.workspace_publications(self.published_workspace.as_ref(), &workspace);
        self.published_workspace = Some(workspace);

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

    pub(crate) fn live_work_session_ids(&self) -> Vec<String> {
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
                .then(|| session_id.to_string())
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
            let Some(session) = engine
                .bootstrap_view(&self.providers, &self.sessions)
                .active_session
            else {
                continue;
            };
            let position = bootstrap
                .sessions
                .iter()
                .position(|summary| summary.id == session.id);
            let updated_at_ms = position.map_or(0, |index| bootstrap.sessions[index].updated_at_ms);
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
            let summary = nakode_protocol::SessionSummary {
                id: session.id,
                workspace_id: session.workspace_id,
                title: session.title,
                active_provider_id: session.selected_provider_id,
                active_model_id: session.selected_model_id,
                updated_at_ms,
                owned_provider_sessions,
                running: !matches!(session.activity, nakode_protocol::SessionActivity::Idle),
            };
            if let Some(position) = position {
                bootstrap.sessions[position] = summary;
            } else {
                bootstrap.sessions.push(summary);
            }
        }
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
            Command::CancelRun { run_id } => self.session_for_run(run_id).ok(),
            Command::CreateSession { .. }
            | Command::SelectModel { .. }
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

    fn respond(self) {
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
    use std::collections::BTreeSet;

    use nakode_protocol::{
        AgentDefinitionInput, ClientId, Command, ErrorCode, ExternalToolDefinition, IdempotencyKey,
        MAX_API_MESSAGE_BYTES, MAX_ARTIFACT_BYTES, MAX_RUN_TEXT_BYTES, MAX_TRANSCRIPT_DELTA_BYTES,
        ModelId, ModelOptions, ModelTarget, PromptAttachment as ProtocolPromptAttachment,
        PromptInput, ProviderAuthenticationView, ProviderId, Query, QueryResult, RunId,
        RunTextField, ServiceCapabilities, ServiceCapability, SessionId, SessionToolConfiguration,
        SubscriptionScope, SubscriptionView, TranscriptOwner, ViewEvent, WorkspaceId,
    };
    use nakode_server::{PublishedEvent, ServerEndpoint, ServerRequest};
    use tokio::sync::broadcast;

    use super::{IDEMPOTENCY_CAPACITY, ServerCore};
    use crate::{
        agent::{AgentCatalog, AgentDefinition},
        backend::{
            BackendCapabilities, BackendCommand, BackendEvent, BackendIdentity, BackendOperation,
            CODEX_PROVIDER, CapabilitySupport, ModelCapabilities, ModelInfo, PromptImage,
            TurnOutcome,
        },
        domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
        personality::PromptAddenda,
        service::ServiceEngine,
        session::{ProviderRecord, SessionRecord, SubagentObservability, SubagentRecord},
        soul::{SoulSource, SoulStore},
        state::{AppState, DomainCommandError},
    };

    fn dashboard_tools(name: &str, replace_builtin_tools: bool) -> SessionToolConfiguration {
        SessionToolConfiguration {
            tools: vec![ExternalToolDefinition {
                name: name.to_owned(),
                description: format!("Run {name}"),
                input_schema_json: r#"{"type":"object","properties":{}}"#.to_owned(),
            }],
            replace_builtin_tools,
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

    fn ready_codex_server() -> (ServerCore, SessionId) {
        let mut state =
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
    fn persisted_session_tools_are_installed_before_provider_resume() {
        let (mut core, _) = ready_external_tools_server();
        let restored_id = SessionId::from("restored-tools-session");
        core.replace_session_records(vec![SessionRecord {
            id: restored_id.to_string(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-restored".to_owned(),
            workspace: "/tmp/project".to_owned(),
            title: "Old dashboard prompt".to_owned(),
            model: None,
            created_at: 10,
            updated_at: 12,
            owned_provider_sessions: Vec::new(),
        }]);
        let tools = dashboard_tools("DashboardRead", true);

        let (_, effects) = core
            .open_session_command(&restored_id, Some(tools.clone()))
            .expect("atomic restored open");
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                crate::state::Effect::Backend(BackendCommand::ResumeSession {
                    provider_session_id,
                    external_tools,
                    replace_builtin_tools: true,
                    ..
                }) if provider_session_id == "thread-restored" && external_tools == &tools.tools
            )),
            "{effects:#?}"
        );
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
    fn invalid_initial_tools_publish_and_restore_nothing() {
        let (mut core, _) = ready_external_tools_server();
        let restored_id = SessionId::from("invalid-restored-tools-session");
        let workspace_id = core.workspace_bootstrap().workspace_id;
        let session_count = core.sessions_by_id.len();
        let invalid = SessionToolConfiguration {
            tools: Vec::new(),
            replace_builtin_tools: true,
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
            title: "Old".to_owned(),
            model: None,
            created_at: 1,
            updated_at: 2,
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
            title: "Direct terminal session".to_owned(),
            model: None,
            created_at: 10,
            updated_at: 12,
            owned_provider_sessions: Vec::new(),
        }]);

        let discovered = core.workspace_bootstrap();
        assert_eq!(discovered.sessions.len(), 1);
        assert_eq!(discovered.sessions[0].id, initial_id);
        assert_eq!(discovered.sessions[0].title, "Direct terminal session");
        assert_eq!(discovered.sessions[0].updated_at_ms, 12_000);
        core.open_session_command(&initial_id, None)
            .expect("persisted initial session remains resumable");
    }

    #[test]
    fn creating_a_session_applies_the_requested_model_atomically() {
        let mut state =
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
        let mut publications = endpoint.subscribe_publications();
        let previous_revision = core
            .engine_for(&second_id)
            .expect("second session")
            .revision();

        core.engine_for_mut(&first_id)
            .expect("first session")
            .state_mut()
            .transcript
            .append_delta("turn-stream", EntryKind::Assistant, "Nakode", "working");
        core.commit_and_publish_session(&endpoint, &first_id);

        assert_eq!(
            core.engine_for(&second_id)
                .expect("second session")
                .revision(),
            previous_revision
        );
        let events = drain_publications(&mut publications);
        assert!(
            events
                .iter()
                .all(|event| !event.scopes.contains(&SubscriptionScope::Session {
                    session_id: second_id.clone(),
                })),
            "{events:#?}"
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
            .prompt_command(&session_id, prompt_with_image_and_file(), false)
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
        state.install_subagents(
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
        state.install_subagents(vec![SubagentRecord {
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
                provider_id: None,
                model_id: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
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
        state.install_subagents(vec![SubagentRecord {
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
                    provider_id: None,
                    model_id: None,
                    tool_audit_json: None,
                })
                .collect(),
            observability: SubagentObservability::default(),
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
        core.commit_and_publish_session(&endpoint, &session_id);

        let events = drain_publications(&mut publications)
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
        state.install_subagents(vec![SubagentRecord {
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
                provider_id: None,
                model_id: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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

    /// A session stuck busy behind a dead backend is deletable too.
    ///
    /// `handle_disconnected` drops the turn but never marks a running subagent stopped, so `is_busy`
    /// stays true for good. Refusing this one for "work in flight" asked the caller to cancel work
    /// that nothing was doing, which is the second way a dead session became permanently stuck.
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
            title: "New session".to_owned(),
            model: None,
            created_at: 0,
            updated_at: 0,
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
    fn current_session_cancellation_overrides_intervening_revision_progress() {
        let mut state =
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
            AppState::new_for_backend("/tmp/project", None, 100, CODEX_PROVIDER, "Codex");
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
