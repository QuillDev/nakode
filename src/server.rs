//! Native server command/query core.
//!
//! The transport talks only to this type. It owns canonical state and returns
//! server effects for the runtime supervisor to execute; clients never receive
//! provider commands, persistence handles, or process objects.

use std::{
    collections::{HashMap, VecDeque},
    path::{Component, Path, PathBuf},
};

use nakode_protocol::{
    AgentDefinitionInput, AgentSessionId, Command, CommandAccepted, CredentialInput, ErrorCode,
    IdempotencyKey, ModelTarget, PromptInput, ProviderId, Query, QueryResult, RunId, ServiceError,
    SessionId, Snapshot, SubscriptionScope, SubscriptionView, ViewEvent, WorkspaceId,
};
use nakode_server::{ResumeReply, ServerEndpoint, ServerRequest};
use sha2::{Digest, Sha256};

use crate::{
    agent::AgentDefinition,
    backend::PromptAttachment,
    service::ServiceEngine,
    session::{ProviderRecord, SessionRecord},
    state::{AgentRequest, DomainCommandError, Effect},
};

const IDEMPOTENCY_CAPACITY: usize = 1_024;

type DomainCommandOutcome = Result<(CommandAccepted, Vec<Effect>), DomainCommandError>;

#[derive(Clone)]
struct CachedCommand {
    digest: [u8; 32],
    result: Result<CommandAccepted, ServiceError>,
}

pub struct ServerCore {
    engine: ServiceEngine,
    providers: Vec<ProviderRecord>,
    sessions: Vec<SessionRecord>,
    command_cache: HashMap<IdempotencyKey, CachedCommand>,
    command_order: VecDeque<IdempotencyKey>,
    next_agent_request: u64,
}

impl ServerCore {
    #[must_use]
    pub fn new(
        engine: ServiceEngine,
        providers: Vec<ProviderRecord>,
        sessions: Vec<SessionRecord>,
    ) -> Self {
        Self {
            engine,
            providers,
            sessions,
            command_cache: HashMap::new(),
            command_order: VecDeque::new(),
            next_agent_request: 1,
        }
    }

    #[must_use]
    pub const fn engine(&self) -> &ServiceEngine {
        &self.engine
    }

    pub const fn engine_mut(&mut self) -> &mut ServiceEngine {
        &mut self.engine
    }

    #[must_use]
    pub fn into_engine(self) -> ServiceEngine {
        self.engine
    }

    /// Handles one transport request and returns effects for server-owned
    /// supervisors to execute.
    pub async fn handle(
        &mut self,
        endpoint: &ServerEndpoint,
        request: ServerRequest,
    ) -> Vec<Effect> {
        match request {
            ServerRequest::Command {
                idempotency_key,
                expected_revision,
                command,
                respond,
                ..
            } => {
                let (result, effects, changed) =
                    self.execute_idempotent(idempotency_key, expected_revision, command);
                if changed {
                    self.engine.note_state_change();
                    self.publish_state(endpoint).await;
                }
                let _ = respond.send(result);
                effects
            }
            ServerRequest::Query { query, respond, .. } => {
                let cursor = endpoint.cursor().await;
                let result = self.query(query).map(|value| Snapshot { cursor, value });
                let _ = respond.send(result);
                Vec::new()
            }
            ServerRequest::Subscribe { scope, respond, .. } => {
                let cursor = endpoint.cursor().await;
                let result = self
                    .subscription_view(&scope)
                    .map(|value| Snapshot { cursor, value });
                let _ = respond.send(result);
                Vec::new()
            }
            ServerRequest::ResumeSubscription {
                scope,
                after,
                respond,
                ..
            } => {
                let reply = match endpoint.replay(&scope, &after).await {
                    Ok((through, events)) => ResumeReply::Resumed { through, events },
                    Err((oldest_available, current)) => ResumeReply::ResyncRequired {
                        oldest_available,
                        current,
                    },
                };
                let _ = respond.send(reply);
                Vec::new()
            }
            ServerRequest::Detached { .. } => Vec::new(),
        }
    }

    fn execute_idempotent(
        &mut self,
        key: IdempotencyKey,
        expected_revision: Option<u64>,
        command: Command,
    ) -> (Result<CommandAccepted, ServiceError>, Vec<Effect>, bool) {
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
            return (result, Vec::new(), false);
        }
        let (result, effects) =
            if expected_revision.is_some_and(|revision| revision != self.engine.revision()) {
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
        (result, effects, changed)
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

    fn try_execute_command(&mut self, command: Command) -> DomainCommandOutcome {
        match command {
            Command::CreateSession { workspace_id, .. } => {
                self.create_session_command(&workspace_id)
            }
            Command::SubmitPrompt { session_id, prompt } => {
                self.prompt_command(&session_id, prompt, false)
            }
            Command::EnqueuePrompt { session_id, prompt } => {
                self.prompt_command(&session_id, prompt, true)
            }
            Command::RemoveQueuedPrompt {
                session_id,
                prompt_id,
            } => self.remove_queued_prompt_command(&session_id, prompt_id.as_str()),
            Command::SteerTurn { turn_id, text } => self.steer_turn_command(&turn_id, &text),
            Command::CancelTurn { turn_id } => self.cancel_turn_command(&turn_id),
            Command::CompactContext { agent_session_id } => {
                self.compact_context_command(&agent_session_id)
            }
            Command::SelectModel { target, .. } => Err(unsupported_model_target(&target)),
            Command::ResolveInteraction {
                interaction_id,
                resolution,
            } => self.resolve_interaction_command(&interaction_id, &resolution),
            Command::Delegate {
                session_id,
                agent_slug,
                task,
            } => self.delegate_command(&session_id, agent_slug, task),
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
            Command::SaveAgent {
                workspace_id,
                definition,
                previous_slug,
            } => self.save_agent_command(&workspace_id, definition, previous_slug),
            Command::DeleteAgent { workspace_id, slug } => {
                self.delete_agent_command(&workspace_id, slug)
            }
            Command::UpdateSettings { .. } => Err(DomainCommandError::Unsupported(
                "settings commands are not connected to the native server yet".to_owned(),
            )),
            Command::ReloadWorkspace { workspace_id } => {
                self.reload_workspace_command(&workspace_id)
            }
        }
    }

    fn create_session_command(&mut self, workspace_id: &WorkspaceId) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        let effects = self.engine.state_mut().create_logical_session()?;
        let session_id = self.engine.state().nakode_session_id.clone();
        Ok(self.accepted(Some(session_id), effects))
    }

    fn prompt_command(
        &mut self,
        session_id: &SessionId,
        prompt: PromptInput,
        enqueue: bool,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let (text, attachments) = Self::convert_prompt(prompt)?;
        let effects = if enqueue {
            self.engine.state_mut().enqueue_prompt(text, attachments)?
        } else {
            self.engine.state_mut().submit_prompt(text, attachments)?
        };
        Ok(self.accepted(None, effects))
    }

    fn remove_queued_prompt_command(
        &mut self,
        session_id: &SessionId,
        prompt_id: &str,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self.engine.state_mut().remove_queued_prompt(prompt_id)?;
        Ok(self.accepted(None, effects))
    }

    fn steer_turn_command(
        &mut self,
        turn_id: &nakode_protocol::TurnId,
        text: &str,
    ) -> DomainCommandOutcome {
        let provider_turn_id = self.provider_turn_id(turn_id)?;
        let effects = self
            .engine
            .state_mut()
            .steer_turn(&provider_turn_id, text)?;
        Ok(self.accepted(None, effects))
    }

    fn cancel_turn_command(&mut self, turn_id: &nakode_protocol::TurnId) -> DomainCommandOutcome {
        let provider_turn_id = self.provider_turn_id(turn_id)?;
        let effects = self.engine.state_mut().cancel_turn(&provider_turn_id)?;
        Ok(self.accepted(None, effects))
    }

    fn compact_context_command(
        &mut self,
        agent_session_id: &AgentSessionId,
    ) -> DomainCommandOutcome {
        self.ensure_agent_session(agent_session_id)?;
        let effects = self.engine.state_mut().compact_context()?;
        Ok(self.accepted(None, effects))
    }

    fn resolve_interaction_command(
        &mut self,
        interaction_id: &nakode_protocol::InteractionId,
        resolution: &nakode_protocol::InteractionResolution,
    ) -> DomainCommandOutcome {
        let effects = self
            .engine
            .state_mut()
            .resolve_interaction(interaction_id, resolution)?;
        Ok(self.accepted(None, effects))
    }

    fn delegate_command(
        &mut self,
        session_id: &SessionId,
        agent_slug: String,
        task: String,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let request_id = self.next_agent_request;
        self.next_agent_request = self.next_agent_request.wrapping_add(1);
        let before = self.engine.state().subagents.len();
        let effects = self.engine.state_mut().invoke_agent(&AgentRequest {
            id: request_id,
            agent: agent_slug,
            task,
        });
        let run_id = self
            .engine
            .state()
            .subagents
            .get(before)
            .map(|run| run.id.clone());
        Ok(self.accepted(run_id, effects))
    }

    fn cancel_run_command(&mut self, run_id: &RunId) -> DomainCommandOutcome {
        let effects = self.engine.state_mut().cancel_run(run_id.as_str())?;
        Ok(self.accepted(Some(run_id.to_string()), effects))
    }

    fn run_shell_command(
        &mut self,
        session_id: &SessionId,
        command: String,
    ) -> DomainCommandOutcome {
        self.ensure_session(session_id)?;
        let effects = self.engine.state_mut().run_shell_command(command)?;
        Ok(self.accepted(None, effects))
    }

    fn set_provider_enabled_command(
        &self,
        provider_id: &ProviderId,
        enabled: bool,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(self.accepted(
            Some(provider_id.to_string()),
            vec![Effect::SetProviderEnabled {
                provider: provider_id.to_string(),
                enabled,
            }],
        ))
    }

    fn begin_provider_authentication_command(
        &self,
        provider_id: &ProviderId,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(self.accepted(
            Some(provider_id.to_string()),
            vec![Effect::AuthenticateProvider(provider_id.to_string())],
        ))
    }

    fn set_provider_credential_command(
        &self,
        provider_id: &ProviderId,
        kind: String,
        credential: &CredentialInput,
    ) -> DomainCommandOutcome {
        self.ensure_provider(provider_id)?;
        Ok(self.accepted(
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
        Ok(self.accepted(
            Some(provider_id.to_string()),
            vec![Effect::ClearProviderCredential(provider_id.to_string())],
        ))
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
        };
        Ok(self.accepted(
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
        Ok(self.accepted(Some(slug.clone()), vec![Effect::DeleteAgent(slug)]))
    }

    fn reload_workspace_command(&self, workspace_id: &WorkspaceId) -> DomainCommandOutcome {
        self.ensure_workspace(workspace_id)?;
        Ok(self.accepted(
            Some(workspace_id.to_string()),
            vec![Effect::ReloadConfiguration],
        ))
    }

    fn accepted(
        &self,
        resource_id: Option<String>,
        effects: Vec<Effect>,
    ) -> (CommandAccepted, Vec<Effect>) {
        (
            CommandAccepted {
                resource_id,
                revision: Some(self.engine.revision().saturating_add(1)),
            },
            effects,
        )
    }

    fn query(&self, query: Query) -> Result<QueryResult, ServiceError> {
        let bootstrap = || self.engine.bootstrap_view(&self.providers, &self.sessions);
        match query {
            Query::Bootstrap {
                workspace,
                session_id,
            } => {
                if workspace != self.engine.state().workspace {
                    return Err(not_found("workspace", &workspace));
                }
                if let Some(session_id) = session_id {
                    self.ensure_session(&session_id).map_err(domain_error)?;
                }
                Ok(QueryResult::Bootstrap(Box::new(bootstrap())))
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
            Query::GetSession { session_id } => {
                self.ensure_session(&session_id).map_err(domain_error)?;
                let session = bootstrap()
                    .active_session
                    .ok_or_else(|| not_found("session", session_id.as_str()))?;
                Ok(QueryResult::Session(Box::new(session)))
            }
            Query::GetTranscriptPage {
                session_id,
                before,
                limit,
            } => {
                self.ensure_session(&session_id).map_err(domain_error)?;
                let mut page = bootstrap()
                    .active_session
                    .ok_or_else(|| not_found("session", session_id.as_str()))?
                    .transcript;
                if let Some(before) = before {
                    let end = page
                        .entries
                        .iter()
                        .position(|entry| entry.id == before)
                        .ok_or_else(|| not_found("transcript entry", before.as_str()))?;
                    page.entries.truncate(end);
                }
                let limit = usize::try_from(limit).unwrap_or(usize::MAX).min(500);
                if page.entries.len() > limit {
                    let remove = page.entries.len() - limit;
                    page.entries.drain(..remove);
                    page.has_earlier = true;
                }
                Ok(QueryResult::Transcript(page))
            }
            Query::GetRun { run_id } => {
                let run = bootstrap()
                    .active_session
                    .into_iter()
                    .flat_map(|session| session.runs)
                    .find(|run| run.id == run_id)
                    .ok_or_else(|| not_found("run", run_id.as_str()))?;
                Ok(QueryResult::Run(Box::new(run)))
            }
            Query::GetArtifact { artifact_id } => Err(not_found("artifact", artifact_id.as_str())),
        }
    }

    fn subscription_view(
        &self,
        scope: &SubscriptionScope,
    ) -> Result<SubscriptionView, ServiceError> {
        let bootstrap = self.engine.bootstrap_view(&self.providers, &self.sessions);
        match scope {
            SubscriptionScope::Workspace { workspace_id } => {
                self.ensure_workspace(workspace_id).map_err(domain_error)?;
                Ok(SubscriptionView::Workspace(Box::new(bootstrap)))
            }
            SubscriptionScope::Session { session_id } => {
                self.ensure_session(session_id).map_err(domain_error)?;
                bootstrap
                    .active_session
                    .map(|session| SubscriptionView::Session(Box::new(session)))
                    .ok_or_else(|| not_found("session", session_id.as_str()))
            }
            SubscriptionScope::Run { run_id } => bootstrap
                .active_session
                .into_iter()
                .flat_map(|session| session.runs)
                .find(|run| run.id == *run_id)
                .map(|run| SubscriptionView::Run(Box::new(run)))
                .ok_or_else(|| not_found("run", run_id.as_str())),
        }
    }

    async fn publish_state(&self, endpoint: &ServerEndpoint) {
        let snapshot = self.engine.bootstrap_view(&self.providers, &self.sessions);
        let mut scopes = vec![SubscriptionScope::Workspace {
            workspace_id: snapshot.workspace_id.clone(),
        }];
        if let Some(session) = &snapshot.active_session {
            scopes.push(SubscriptionScope::Session {
                session_id: session.id.clone(),
            });
        }
        let _ = endpoint
            .publish(
                scopes,
                ViewEvent::BootstrapChanged {
                    snapshot: Box::new(snapshot),
                },
            )
            .await;
    }

    fn ensure_workspace(
        &self,
        workspace_id: &nakode_protocol::WorkspaceId,
    ) -> Result<(), DomainCommandError> {
        let expected = crate::state::projection::workspace_id(&self.engine.state().workspace);
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
        if session_id.as_str() == self.engine.state().nakode_session_id {
            Ok(())
        } else {
            Err(DomainCommandError::NotFound(session_id.to_string()))
        }
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

    fn ensure_agent_session(
        &self,
        agent_session_id: &AgentSessionId,
    ) -> Result<(), DomainCommandError> {
        let active = self
            .engine
            .bootstrap_view(&self.providers, &self.sessions)
            .active_session
            .and_then(|session| session.active_agent_session);
        if active
            .as_ref()
            .is_some_and(|session| session.id == *agent_session_id)
        {
            Ok(())
        } else {
            Err(DomainCommandError::NotFound(agent_session_id.to_string()))
        }
    }

    fn provider_turn_id(
        &self,
        turn_id: &nakode_protocol::TurnId,
    ) -> Result<String, DomainCommandError> {
        let view = self.engine.bootstrap_view(&self.providers, &self.sessions);
        let matches = view
            .active_session
            .and_then(|session| session.active_turn)
            .is_some_and(|turn| turn.id == *turn_id);
        if !matches {
            return Err(DomainCommandError::NotFound(turn_id.to_string()));
        }
        self.engine
            .state()
            .active_turn
            .as_ref()
            .map(|turn| turn.id.clone())
            .ok_or_else(|| DomainCommandError::NotFound(turn_id.to_string()))
    }

    fn convert_prompt(
        prompt: nakode_protocol::PromptInput,
    ) -> Result<(String, Vec<PromptAttachment>), DomainCommandError> {
        let attachments = prompt
            .attachments
            .into_iter()
            .map(|attachment| match attachment {
                nakode_protocol::PromptAttachment::Artifact { artifact_id } => {
                    Err(DomainCommandError::Unsupported(format!(
                        "artifact transfer is not available for {artifact_id}"
                    )))
                }
                nakode_protocol::PromptAttachment::LocalFile { label, path } => {
                    let path = validated_relative_path(&path)?;
                    Ok(PromptAttachment {
                        label,
                        path: Some(path),
                        image: None,
                    })
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((prompt.text, attachments))
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

fn unsupported_model_target(target: &ModelTarget) -> DomainCommandError {
    DomainCommandError::Unsupported(format!(
        "model selection for {target:?} is not connected to the native server yet"
    ))
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

    use nakode_client::NakodeClient;
    use nakode_protocol::{
        ClientDescriptor, ClientId, Query, QueryResult, ServerFrame, ServiceCapabilities,
        ServiceCapability, Snapshot,
    };
    use nakode_server::ServerEndpoint;

    use super::ServerCore;
    use crate::{service::ServiceEngine, state::AppState};

    #[tokio::test]
    async fn plain_frontend_bootstraps_through_the_real_server_core() {
        let state = AppState::new_unconfigured("/tmp/project", None, 100);
        let engine = ServiceEngine::new(state);
        let mut core = ServerCore::new(engine, Vec::new(), Vec::new());
        let (endpoint, mut requests) = ServerEndpoint::channel(
            "test",
            ServiceCapabilities {
                supported: BTreeSet::from([
                    ServiceCapability::Subscriptions,
                    ServiceCapability::MultipleClients,
                ]),
            },
            16,
        );
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let connection = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move { endpoint.serve_stream(server_stream).await })
        };
        let runtime = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                while let Some(request) = requests.recv().await {
                    let _ = core.handle(&endpoint, request).await;
                }
            })
        };

        let mut client = NakodeClient::from_stream(client_stream, ClientId::from("plain"));
        client
            .hello(ClientDescriptor {
                name: "Plain frontend".to_owned(),
                version: "1".to_owned(),
                frontend: "text".to_owned(),
            })
            .await
            .expect("hello");
        let request_id = client
            .query(Query::Bootstrap {
                workspace: "/tmp/project".to_owned(),
                session_id: None,
            })
            .await
            .expect("query");
        let response = client.receive().await.expect("bootstrap response");
        assert!(matches!(
            response,
            ServerFrame::QueryResult {
                request_id: response_id,
                result: Ok(Snapshot {
                    value: QueryResult::Bootstrap(_),
                    ..
                }),
            } if response_id == request_id
        ));
        client
            .send(&nakode_protocol::ClientFrame::Detach)
            .await
            .expect("detach");
        connection.await.expect("connection").expect("serve");
        runtime.abort();
    }
}
