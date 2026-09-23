use super::{BackendEvent, Effect, NativeServerRuntime, ServerCore};
use crate::followups::{InboxRequest, InboxStore, refuse};
use nakode_protocol::{
    Command, PromptAttachment, PromptInput, Query, QueryResult, ServiceError, SessionId, Snapshot,
};

impl NativeServerRuntime {
    /// The transport's client ID is audit attribution, not an ownership or permission grant.
    pub(super) fn handle_followup_request(
        &mut self,
        request: nakode_server::ServerRequest,
    ) -> Option<nakode_server::ServerRequest> {
        // Durable batching is explicit-only until its producer identities, UI and uncertainty
        // recovery are complete. Never divert SendPrompt/EnqueuePrompt from the visible queue.
        match request {
            nakode_server::ServerRequest::Query {
                query:
                    Query::ListFollowups {
                        session_id,
                        after_sequence,
                        limit,
                        view,
                    },
                respond,
                ..
            } => {
                let result = InboxStore::open(&self.effects.persistence.database)
                    .and_then(|store| store.list_view(&session_id, after_sequence, limit, &view))
                    .map(|view| Snapshot {
                        cursor: self.endpoint.cursor(),
                        value: QueryResult::Followups(view),
                    });
                let _ = respond.send(result);
                None
            }
            nakode_server::ServerRequest::Command {
                command:
                    command @ (Command::EnqueueFollowup { .. }
                    | Command::RelayAgentFollowup { .. }
                    | Command::SetFollowupPaused { .. }
                    | Command::RemoveFollowup { .. }),
                idempotency_key,
                client_id,
                expected_revision,
                replay_only,
                respond,
                ..
            } => {
                let session = self.core.command_session(&command);
                let result = if expected_revision.is_some() {
                    Err(refuse(
                        "follow-up admission uses durable identities, not volatile session revision fences",
                    ))
                } else {
                    self.admit_followup(
                        &command,
                        idempotency_key.as_str(),
                        client_id.as_str(),
                        replay_only,
                    )
                };
                if result.is_ok()
                    && let Some(session) = session
                {
                    self.followup_polling_enabled = true;
                    self.core
                        .commit_and_publish_session(&self.endpoint, &session);
                }
                let _ = respond
                    .send(result.map(|resource| ServerCore::accepted(resource, Vec::new()).0));
                None
            }
            request => Some(request),
        }
    }

    fn admit_followup(
        &self,
        command: &Command,
        key: &str,
        sender: &str,
        replay_only: bool,
    ) -> Result<Option<String>, ServiceError> {
        let session = self
            .core
            .command_session(command)
            .ok_or_else(|| refuse("follow-up session is missing"))?;
        // Ordinary mutations address an explicitly opened logical session, as SendPrompt does.
        // Read-only retained inbox queries never activate a provider. Authentication belongs to
        // the service transport; a client ID is attribution, never a profile/ownership credential.
        if !matches!(command, Command::RemoveFollowup { .. }) {
            self.core
                .ensure_session(&session)
                .map_err(super::super::domain_error)?;
        }
        InboxStore::open(&self.effects.persistence.database)?.execute_authenticated(
            InboxRequest {
                command,
                key,
                sender,
                replay_only,
                now_ms: super::super::unix_timestamp_ms(),
            },
            |prompt| self.freeze_followup_attachments(&session, prompt),
            || self.authenticate_relay(command),
        )
    }

    fn authenticate_relay(&self, command: &Command) -> Result<(), ServiceError> {
        let Command::RelayAgentFollowup {
            session_id,
            source_session_id,
            source_call_id,
            prompt,
            ..
        } = command
        else {
            return Err(refuse("not a relay command"));
        };
        let call = self
            .core
            .engine_for(source_session_id)
            .and_then(|engine| {
                engine
                    .state()
                    .external_tool_calls
                    .iter()
                    .find(|call| call.id == *source_call_id)
            })
            .ok_or_else(|| refuse("source relay call is not pending"))?;
        if call.name != "SendAgentMessage" {
            return Err(refuse("source call is not an agent-message operation"));
        }
        let arguments: serde_json::Value = serde_json::from_str(&call.arguments_json)
            .map_err(|_| refuse("source call arguments are invalid"))?;
        if arguments
            .get("sessionId")
            .and_then(serde_json::Value::as_str)
            != Some(session_id.as_str())
            || arguments.get("message").and_then(serde_json::Value::as_str)
                != Some(prompt.text.as_str())
        {
            return Err(refuse(
                "relay target or message differs from the pending source call",
            ));
        }
        let references = arguments
            .get("imageReferences")
            .and_then(serde_json::Value::as_array);
        let selected = references
            .into_iter()
            .flatten()
            .map(|reference| {
                let id = reference
                    .as_str()
                    .ok_or_else(|| refuse("invalid source image reference"))?;
                Ok(PromptAttachment::Artifact {
                    artifact_id: id.into(),
                    label: "Coordinator image".to_owned(),
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        let expected = self.freeze_followup_attachments(
            source_session_id,
            &PromptInput {
                text: prompt.text.clone(),
                attachments: selected,
            },
        )?;
        if expected.attachments != prompt.attachments {
            return Err(refuse(
                "relay attachments differ from the source session's selected images",
            ));
        }
        Ok(())
    }

    fn freeze_followup_attachments(
        &self,
        session: &SessionId,
        prompt: &PromptInput,
    ) -> Result<PromptInput, ServiceError> {
        if prompt.attachments.is_empty() {
            return Ok(prompt.clone());
        }
        // Validate every attachment through the canonical converter before accepting it. In
        // particular, invalid file paths must not become a poison head for the durable FIFO.
        let (text, attachments) = self
            .core
            .convert_prompt(session, prompt.clone())
            .map_err(super::super::domain_error)?;
        let attachments = attachments
            .into_iter()
            .map(|item| {
                if let Some(image) = item.image {
                    Ok(PromptAttachment::InlineImage {
                        label: item.label,
                        media_type: image.mime_type,
                        data: image.data,
                    })
                } else if let Some(path) = item.path {
                    Ok(PromptAttachment::LocalFile {
                        label: item.label,
                        path: path.to_string_lossy().into_owned(),
                    })
                } else {
                    Err(refuse(
                        "follow-up attachment could not be preserved; nothing was enqueued",
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PromptInput { text, attachments })
    }

    /// A fixed timer, never a sliding debounce, gives idle bursts one cutoff and prevents an
    /// unending producer stream from postponing execution. Legacy queue members keep their order.
    pub(super) async fn dispatch_followups(&mut self) {
        if !self.accepting_work {
            return;
        }
        let Ok(mut store) = InboxStore::open(&self.effects.persistence.database) else {
            return;
        };
        self.observe_child_questions();
        match store.admit_child_events() {
            Ok(true) => self.followup_polling_enabled = true,
            Ok(false) => {}
            Err(error) => {
                self.core.engine_mut().state_mut().status_message =
                    format!("Child notification admission held: {}", error.message);
                return;
            }
        }
        if !self.followup_polling_enabled {
            return;
        }
        let Ok(sessions) = store.candidates(self.followup_cursor.as_ref()) else {
            return;
        };
        // An exhausted page resets to None, so busy and previously visited sessions are revisited.
        self.followup_cursor = sessions.last().cloned();
        for session in sessions {
            let ready = self.core.engine_for(&session).is_some_and(|engine| {
                let state = engine.state();
                state.connection.is_ready() && !state.is_busy() && state.queue.is_empty()
            });
            if !ready {
                continue;
            }
            let Ok(Some(batch)) = store.claim(&session) else {
                continue;
            };
            let checkpoint = self.core.clone();
            let mut effects =
                match self
                    .core
                    .prompt_command(&session, batch.prompt, false, Some(&batch.id))
                {
                    Ok((_, effects)) => effects,
                    Err(error) => {
                        self.core = checkpoint;
                        let _ = store.block(&batch.id, &super::super::domain_error(error).message);
                        continue;
                    }
                };
            if let Some(engine) = self.core.engine_for_mut(&session) {
                engine
                    .state_mut()
                    .set_prompt_coordination(&batch.id, &batch.coordination_json);
            }
            // This ledger, not the legacy owner-prompt replay bit, owns batch delivery recovery.
            for effect in &mut effects {
                if let Effect::PersistAcceptedOwnerPrompt { prompt, .. } = effect {
                    prompt.dispatch_pending = false;
                }
            }
            if let Err(error) = super::persist_owner_prompt_effects(
                &mut self.core,
                &session,
                self.effects.persistence.sessions.as_ref(),
                &mut effects,
            ) {
                self.core = checkpoint;
                let _ = store.block(&batch.id, &error.to_string());
                continue;
            }
            if store.fence_dispatch(&session, &batch.id).is_err() {
                self.core = checkpoint;
                continue;
            }
            self.register_effect_owners(&session, &effects);
            if let Some(engine) = self.core.engine_for_mut(&session) {
                self.effects
                    .execute(
                        &session,
                        engine.state_mut(),
                        effects,
                        super::EffectOrigin::PrimarySession,
                    )
                    .await;
            }
            self.core
                .commit_and_publish_session(&self.endpoint, &session);
        }
    }

    fn observe_child_questions(&mut self) {
        let Ok(reports) =
            crate::child_reports::ReportStore::open(&self.effects.persistence.database)
        else {
            return;
        };
        // Retry observation while the original question is pending; never manufacture or answer
        // an interaction. Primary logical sessions only: native run questions are excluded.
        let mut question_failures = Vec::new();
        for (session, engine) in &self.core.sessions_by_id {
            for question in &engine.state().questions {
                if let Err(error) = reports.record_question(
                    session.as_str(),
                    &question.request,
                    super::super::unix_timestamp_ms(),
                ) && !question_failures
                    .iter()
                    .any(|(failed_session, _)| failed_session == session)
                {
                    question_failures.push((session.clone(), error.message));
                }
            }
        }
        for (session, message) in question_failures {
            let message = format!("Child notification held: {message}");
            if let Some(engine) = self.core.engine_for_mut(&session)
                && engine.state().status_message != message
            {
                engine.state_mut().status_message = message;
                self.core
                    .commit_and_publish_session(&self.endpoint, &session);
            }
        }
    }

    /// Called before the reducer consumes `starting_prompt_id`. Only acceptance or an echoed stable
    /// client identity can settle the batch; arbitrary provider progress is not acknowledgement.
    pub(super) fn acknowledge_followup_event(
        &self,
        source: &super::BackendSource,
        event: &BackendEvent,
    ) {
        if !self.followup_polling_enabled {
            return;
        }
        let super::BackendSource::Primary { session_id, .. } = source else {
            return;
        };
        if !matches!(
            event,
            BackendEvent::TurnAccepted { .. }
                | BackendEvent::TurnStarted { .. }
                | BackendEvent::TurnCompleted { .. }
        ) {
            return;
        }
        let Ok(store) = InboxStore::open(&self.effects.persistence.database) else {
            return;
        };
        match event {
            BackendEvent::TurnAccepted { turn_id } => {
                if let Some(starting) = self
                    .core
                    .engine_for(session_id)
                    .and_then(|engine| engine.state().starting_prompt_id())
                {
                    let _ = store.observe_accepted(session_id, starting, turn_id);
                }
            }
            BackendEvent::TurnStarted { turn_id } | BackendEvent::TurnCompleted { turn_id, .. } => {
                // Failure leaves the durable fence intact; later correlated progress can retry
                // acknowledgement, but never inference dispatch.
                let _ = store.acknowledge(session_id, turn_id);
            }
            _ => {}
        }
    }

    pub(super) fn pause_followups_for_stop(&self, session: &SessionId) -> Result<(), ServiceError> {
        InboxStore::open(&self.effects.persistence.database)?.pause(session)
    }
}
