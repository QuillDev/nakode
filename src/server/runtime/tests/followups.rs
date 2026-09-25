use super::*;
mod relationships;
use crate::followups::InboxStore;
use nakode_protocol::{CommandAccepted, FollowupInbox, PromptInput, ServiceError};

#[tokio::test]
async fn ordinary_runtime_skips_unused_inbox_polling_and_explicit_admission_enables_it() {
    let mut harness = Harness::new().await;
    assert!(!harness.runtime.followup_polling_enabled);
    assert!(
        !InboxStore::open(&harness.runtime.effects.persistence.database)
            .unwrap()
            .has_messages()
            .unwrap()
    );
    harness.runtime.dispatch_followups().await;
    assert!(harness.commands.try_recv().is_err());

    harness.enqueue("enable-explicit-inbox").await;
    assert!(harness.runtime.followup_polling_enabled);
    // The startup detector must also recognize persisted work, including after consumption.
    assert!(
        InboxStore::open(&harness.runtime.effects.persistence.database)
            .unwrap()
            .has_messages()
            .unwrap()
    );
    harness.runtime.dispatch_followups().await;
    assert!(harness.commands.try_recv().is_ok());
}

#[tokio::test]
async fn invalid_file_paths_are_refused_before_they_can_block_the_inbox() {
    let mut harness = Harness::new().await;
    for (index, path) in ["../outside", "/absolute/path"].into_iter().enumerate() {
        let key = format!("invalid-file-{index}");
        let command = Command::EnqueueFollowup {
            session_id: harness.session.clone(),
            message_id: key.clone(),
            prompt: PromptInput {
                text: "Read this file".to_owned(),
                attachments: vec![nakode_protocol::PromptAttachment::LocalFile {
                    label: "file".to_owned(),
                    path: path.to_owned(),
                }],
            },
        };
        let error = harness
            .command(&key, None, false, command)
            .await
            .unwrap_err();
        assert!(error.message.contains("workspace-relative"));
    }
    let inbox = InboxStore::open(&harness.runtime.effects.persistence.database)
        .unwrap()
        .list(&harness.session, 0, 32)
        .unwrap();
    assert!(inbox.items.is_empty());
    harness.enqueue("valid-later").await;
    harness.runtime.dispatch_followups().await;
    assert!(harness.commands.try_recv().is_ok());
}

struct Harness {
    runtime: NativeServerRuntime,
    session: SessionId,
    commands: mpsc::Receiver<BackendCommand>,
    _control: mpsc::Receiver<BackendCommand>,
    _directory: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
        std::fs::create_dir_all(&root).unwrap();
        let directory = tempfile::tempdir_in(root).unwrap();
        let workspace = directory.path();
        let (persistence, _) = test_persistence(workspace);
        let mut state = DomainState::new_for_backend(
            workspace.to_string_lossy(),
            None,
            100,
            CODEX_PROVIDER,
            "Codex",
        );
        let record = persistence
            .sessions
            .create_with_id(
                &state.nakode_session_id,
                CODEX_PROVIDER,
                "native",
                &state.workspace,
                &state.workspace,
                "Inbox agent",
                None,
                &BackendModelOptions::default(),
                None,
            )
            .unwrap();
        state.session_persisted(&record);
        state.provider_session_id = Some(record.provider_session_id.clone());
        state.provider_account_id = Some("test-account".to_owned());
        state.connection = crate::state::ConnectionState::Ready {
            server: "test".to_owned(),
        };
        state.backend_capabilities.interruption = crate::backend::CapabilitySupport::Supported;
        let session = SessionId::from(record.id.clone());
        persistence
            .sessions
            .save_session_bridge(&SessionBridgeRecord {
                session_id: session.to_string(),
                workspace: state.workspace.clone(),
                kind: OrchestratorKind::Agent,
                lifecycle: BridgeLifecycle::Open,
                display_title: "Inbox agent".to_owned(),
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
                updated_at_ms: 1,
            })
            .unwrap();
        let effects = EffectExecutor::new(empty_registry(workspace).await, persistence);
        let (mut runtime, _) = NativeServerRuntime::from_parts(
            ServiceEngine::new(state),
            Vec::new(),
            vec![record],
            effects,
            mpsc::channel(1).1,
        );
        let (control, control_rx, events) = fake_backend();
        runtime
            .effects
            .backends
            .insert_provider_control(CODEX_PROVIDER.to_owned(), control);
        drop(events);
        let (backend, commands, events) = fake_backend();
        runtime.effects.backends.insert_session(
            session.clone(),
            CODEX_PROVIDER.to_owned(),
            "test-account".to_owned(),
            backend,
        );
        drop(events);
        Self {
            runtime,
            session,
            commands,
            _control: control_rx,
            _directory: directory,
        }
    }

    async fn command(
        &mut self,
        key: &str,
        revision: Option<u64>,
        replay: bool,
        command: Command,
    ) -> Result<CommandAccepted, ServiceError> {
        let endpoint = self.runtime.endpoint.clone();
        let request = endpoint.execute_command(
            ClientId::from("owner"),
            IdempotencyKey::from(key),
            revision,
            replay,
            command,
        );
        let receive = async {
            let request = self.runtime.requests.recv().await.unwrap();
            self.runtime.handle_request(request).await;
        };
        let (result, ()) = tokio::join!(request, receive);
        result
    }

    async fn enqueue(&mut self, id: &str) {
        self.command(
            id,
            None,
            false,
            Command::EnqueueFollowup {
                session_id: self.session.clone(),
                message_id: id.to_owned(),
                prompt: PromptInput {
                    text: format!("Requirement {id}"),
                    attachments: Vec::new(),
                },
            },
        )
        .await
        .unwrap();
    }

    fn state(&self) -> &DomainState {
        self.runtime.core.engine_for(&self.session).unwrap().state()
    }

    fn stored_session(&self) -> crate::session::SessionRecord {
        self.runtime
            .effects
            .persistence
            .sessions
            .find(self.session.as_str())
            .unwrap()
            .unwrap()
    }

    fn inbox(&self) -> FollowupInbox {
        InboxStore::open(&self.runtime.effects.persistence.database)
            .unwrap()
            .list(&self.session, 0, 64)
            .unwrap()
    }

    async fn event(&mut self, event: BackendEvent) {
        self.runtime
            .handle_backend_event(
                BackendSource::Primary {
                    session_id: self.session.clone(),
                    provider: CODEX_PROVIDER.to_owned(),
                    account_id: "test-account".to_owned(),
                },
                event,
            )
            .await;
    }

    fn start(&mut self) -> (String, String) {
        let BackendCommand::StartTurn {
            client_id, prompt, ..
        } = self.commands.try_recv().expect("one provider start")
        else {
            panic!("expected StartTurn")
        };
        assert!(self.commands.try_recv().is_err(), "no second start");
        (client_id, prompt)
    }
}

#[tokio::test]
async fn ordinary_agent_followups_keep_visible_fifo_and_replay_identity() {
    let mut h = Harness::new().await;
    assert_eq!(
        h.runtime.core.session_bridge(&h.session).unwrap().kind,
        OrchestratorKind::Agent
    );
    let send = |id: &SessionId, text: &str| Command::SendPrompt {
        session_id: id.clone(),
        prompt: PromptInput {
            text: text.to_owned(),
            attachments: Vec::new(),
        },
    };
    h.command("initial", None, false, send(&h.session, "First task"))
        .await
        .unwrap();
    let (id, prompt) = h.start();
    assert_eq!(id, "initial");
    assert!(prompt.starts_with("First task\n\n[Nakode Current Agent Catalogue]"));
    h.event(BackendEvent::TurnStarted {
        turn_id: "first-turn".to_owned(),
    })
    .await;
    let followup = send(&h.session, "Same text is deliberate");
    h.command("second", None, false, followup.clone())
        .await
        .unwrap();
    // Transport retry is not another message; deliberate repeated text IS another message.
    h.command("second", None, false, followup).await.unwrap();
    h.command(
        "third",
        None,
        false,
        Command::EnqueuePrompt {
            session_id: h.session.clone(),
            prompt: PromptInput {
                text: "Same text is deliberate".to_owned(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    let state = h.state();
    assert_eq!(
        state
            .queue
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["second", "third"]
    );
    let stored = h.stored_session();
    assert_eq!(
        stored.queued_prompts,
        state.queue.iter().cloned().collect::<Vec<_>>()
    );
    let mut retained =
        DomainState::new_for_backend(&state.workspace, None, 100, CODEX_PROVIDER, "Codex");
    retained.install_retained_session(&stored, Vec::new());
    assert_eq!(
        retained.queue, state.queue,
        "retained reads expose accepted work without provider activation"
    );
    assert!(retained.provider_session_id.is_none());
    assert!(
        h.inbox().items.is_empty(),
        "ordinary messages never enter the opt-in ledger"
    );
    h.event(BackendEvent::TurnCompleted {
        turn_id: "first-turn".to_owned(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    let (id, prompt) = h.start();
    assert_eq!(id, "second");
    assert!(prompt.starts_with("Same text is deliberate\n\n[Nakode Current Agent Catalogue]"));
    h.event(BackendEvent::TurnStarted {
        turn_id: "second-turn".to_owned(),
    })
    .await;
    h.event(BackendEvent::TurnCompleted {
        turn_id: "second-turn".to_owned(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    let (id, prompt) = h.start();
    assert_eq!(id, "third");
    assert!(prompt.starts_with("Same text is deliberate\n\n[Nakode Current Agent Catalogue]"));
    assert!(h.state().queue.is_empty());
    assert!(h.stored_session().queued_prompts.is_empty());
    assert!(h.inbox().items.is_empty());
}

#[tokio::test]
async fn visible_queue_admission_and_removal_roll_back_on_storage_failure() {
    let mut h = Harness::new().await;
    let send = |session: &SessionId, text: &str| Command::SendPrompt {
        session_id: session.clone(),
        prompt: PromptInput {
            text: text.to_owned(),
            attachments: Vec::new(),
        },
    };
    h.command("active", None, false, send(&h.session, "Active task"))
        .await
        .unwrap();
    h.start();
    h.event(BackendEvent::TurnStarted {
        turn_id: "active-turn".to_owned(),
    })
    .await;
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_queue_insert BEFORE INSERT ON session_prompt_queues BEGIN SELECT RAISE(ABORT, 'queue failure'); END;").unwrap();
    let queued = send(&h.session, "Keep this accepted message");
    assert!(
        h.command("queued", None, false, queued.clone())
            .await
            .is_err()
    );
    assert!(h.state().queue.is_empty());
    assert!(h.commands.try_recv().is_err());
    connection
        .execute_batch("DROP TRIGGER fail_queue_insert;")
        .unwrap();
    h.command("queued", None, false, queued).await.unwrap();
    assert_eq!(h.stored_session().queued_prompts.len(), 1);
    connection.execute_batch("CREATE TRIGGER fail_queue_delete BEFORE DELETE ON session_prompt_queues BEGIN SELECT RAISE(ABORT, 'queue failure'); END;").unwrap();
    let remove = Command::RemoveQueuedPrompt {
        session_id: h.session.clone(),
        prompt_id: "queued".into(),
    };
    assert!(
        h.command("remove", None, false, remove.clone())
            .await
            .is_err()
    );
    assert_eq!(h.state().queue.len(), 1);
    connection
        .execute_batch("DROP TRIGGER fail_queue_delete;")
        .unwrap();
    h.command("remove", None, false, remove).await.unwrap();
    assert!(h.stored_session().queued_prompts.is_empty());
}

#[tokio::test]
async fn native_queue_steering_is_fenced_before_dispatch_and_after_failed_ack_checkpoint() {
    let mut h = Harness::new().await;
    h.runtime
        .core
        .engine_for_mut(&h.session)
        .unwrap()
        .state_mut()
        .backend_capabilities
        .steering = crate::backend::CapabilitySupport::Supported;
    let send = |session: &SessionId, text: &str| Command::SendPrompt {
        session_id: session.clone(),
        prompt: PromptInput {
            text: text.to_owned(),
            attachments: Vec::new(),
        },
    };
    h.command("active", None, false, send(&h.session, "Active task"))
        .await
        .unwrap();
    h.start();
    h.event(BackendEvent::TurnStarted {
        turn_id: "active-turn".to_owned(),
    })
    .await;
    h.command("guidance", None, false, send(&h.session, "Guidance"))
        .await
        .unwrap();
    let steer = Command::SteerQueuedPrompt {
        session_id: h.session.clone(),
        prompt_id: "guidance".into(),
    };
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_steer_intent BEFORE INSERT ON session_prompt_queues BEGIN SELECT RAISE(ABORT, 'queue failure'); END;").unwrap();
    assert!(
        h.command("steer", None, false, steer.clone())
            .await
            .is_err()
    );
    assert!(!h.state().queue[0].delivery_uncertain);
    assert!(!h.stored_session().queued_prompts[0].delivery_uncertain);
    assert!(
        h.commands.try_recv().is_err(),
        "no dispatch without durable intent"
    );
    connection
        .execute_batch("DROP TRIGGER fail_steer_intent;")
        .unwrap();
    h.command("steer", None, false, steer).await.unwrap();
    assert!(matches!(
        h.commands.try_recv().unwrap(),
        BackendCommand::SteerTurn { .. }
    ));
    let stored = h.stored_session();
    assert!(
        stored.queued_prompts[0].delivery_uncertain,
        "fence precedes native dispatch"
    );
    connection.execute_batch("CREATE TRIGGER fail_steer_ack BEFORE DELETE ON session_prompt_queues BEGIN SELECT RAISE(ABORT, 'queue failure'); END;").unwrap();
    h.event(BackendEvent::SteerAccepted {
        turn_id: "active-turn".to_owned(),
    })
    .await;
    assert!(h.state().queue.is_empty());
    let retained = h.stored_session();
    assert!(retained.queued_prompts[0].delivery_uncertain);
    let mut recovered =
        DomainState::new_for_backend(&retained.workspace, None, 100, CODEX_PROVIDER, "Codex");
    recovered.backend_capabilities.resume = crate::backend::CapabilitySupport::Supported;
    recovered.connection = crate::state::ConnectionState::Ready {
        server: "fixture".to_owned(),
    };
    recovered.begin_resume(retained);
    let effects = recovered.handle_backend(BackendEvent::SessionResumed {
        provider_session_id: "native".to_owned(),
        model: String::new(),
        history: Vec::new(),
    });
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Backend(BackendCommand::StartTurn { .. })))
    );
    assert!(
        recovered
            .submit_prompt("New work".to_owned(), Vec::new())
            .is_err()
    );
    assert_eq!(recovered.queue.len(), 1);
    recovered.remove_queued_prompt("guidance").unwrap();
    assert!(
        recovered.queue.is_empty(),
        "explicit removal remains the recovery control"
    );
}

#[tokio::test]
async fn idle_burst_starts_once_and_arrivals_wait_for_the_next_turn_boundary() {
    let mut h = Harness::new().await;
    for index in 0..8 {
        h.enqueue(&format!("m{index}")).await;
    }
    h.runtime.dispatch_followups().await;
    let (batch, prompt) = h.start();
    assert_eq!(prompt.matches("Requirement m").count(), 8);
    h.event(BackendEvent::TurnAccepted {
        turn_id: "provider-one".to_owned(),
    })
    .await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "provider-one".to_owned(),
    })
    .await;
    assert!(h.inbox().items.iter().all(|item| item.state == "consumed"));
    h.enqueue("later").await;
    for _ in 0..4 {
        h.runtime.dispatch_followups().await;
    }
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox().pending_count, 1);
    h.event(BackendEvent::TurnCompleted {
        turn_id: "provider-one".to_owned(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    // One scan may hit the end of the cursor page; the following scan wraps to the busy session.
    for _ in 0..2 {
        h.runtime.dispatch_followups().await;
    }
    let (next, prompt) = h.start();
    assert_ne!(batch, next);
    assert!(prompt.contains("Requirement later"));
    assert!(!prompt.contains("Requirement m0"));
}

#[tokio::test]
async fn stop_rejections_and_replays_cannot_pause_after_a_later_resume() {
    let mut h = Harness::new().await;
    h.enqueue("pending").await;
    let stop = Command::CancelSessionWork {
        session_id: h.session.clone(),
    };
    assert!(
        h.command("stale", Some(u64::MAX), false, stop.clone())
            .await
            .is_err()
    );
    assert!(!h.inbox().paused);
    assert!(
        h.command("missing", None, true, stop.clone())
            .await
            .is_err()
    );
    assert!(!h.inbox().paused);
    h.command("stop", None, false, stop.clone()).await.unwrap();
    assert!(h.inbox().paused);
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
    h.command(
        "resume",
        None,
        false,
        Command::SetFollowupPaused {
            session_id: h.session.clone(),
            paused: false,
        },
    )
    .await
    .unwrap();
    h.command("stop", None, false, stop).await.unwrap();
    assert!(!h.inbox().paused);
    for _ in 0..2 {
        h.runtime.dispatch_followups().await;
    }
    h.start();
}

#[tokio::test]
async fn legacy_queue_keeps_its_inputs_and_inbox_waits_without_rewriting_it() {
    let mut h = Harness::new().await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "existing-turn".to_owned(),
    })
    .await;
    h.runtime
        .core
        .engine_for_mut(&h.session)
        .unwrap()
        .state_mut()
        .enqueue_prompt("Legacy message".to_owned(), Vec::new())
        .unwrap();
    h.enqueue("new-inbox-message").await;
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox().pending_count, 1);
    let queue = &h.state().queue;
    assert_eq!(queue.len(), 1);
}

#[tokio::test]
async fn uncertain_dispatch_is_not_legacy_replayable() {
    let mut h = Harness::new().await;
    h.enqueue("one").await;
    h.runtime.dispatch_followups().await;
    let (batch, _) = h.start();
    let prompts = h.stored_session().owner_prompts;
    let prompt = prompts
        .iter()
        .find(|prompt| prompt.prompt_id == batch)
        .unwrap();
    assert!(!prompt.dispatch_pending, "the inbox alone owns recovery");
    assert!(
        InboxStore::open(&h.runtime.effects.persistence.database)
            .unwrap()
            .claim(&h.session)
            .unwrap()
            .is_none()
    );
    assert!(h.inbox().blocked_reason.is_some());
}

fn linked_child(h: &Harness) -> String {
    let child = h
        .runtime
        .effects
        .persistence
        .sessions
        .create(
            CODEX_PROVIDER,
            "durable-child",
            &h.state().workspace,
            "Durable child",
            None,
        )
        .unwrap();
    crate::child_reports::ReportStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .execute(
            &Command::LinkChildSession {
                parent_session_id: h.session.clone(),
                child_session_id: child.id.clone().into(),
            },
            "link-child",
            false,
            None,
            1,
        )
        .unwrap();
    child.id
}

#[tokio::test]
async fn durable_child_terminal_event_wakes_idle_parent_once_without_restarting_child() {
    let mut h = Harness::new().await;
    let child = linked_child(&h);
    h.runtime
        .effects
        .persistence
        .sessions
        .update_last_turn(
            &child,
            &crate::session::PersistedTurnConfiguration {
                completion: None,
                id: "child-completion".into(),
                model: Some("test-model".into()),
                options: BackendModelOptions::default(),
                outcome: crate::backend::TurnOutcome::Completed,
            },
        )
        .unwrap();
    h.runtime.dispatch_followups().await;
    let (batch, prompt) = h.start();
    assert!(prompt.contains("durable_child_evidence"));
    assert!(prompt.contains(&child));
    assert!(prompt.contains("child-completion"));
    assert_eq!(h.inbox().items.len(), 1);
    h.event(BackendEvent::TurnStarted {
        turn_id: batch.clone(),
    })
    .await;
    h.event(BackendEvent::TurnCompleted {
        turn_id: batch,
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    h.runtime.dispatch_followups().await;
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox().items[0].state, "consumed");
    assert!(
        h.runtime.core.engine_for(&SessionId::from(child)).is_none(),
        "notification never restores a child"
    );
}

#[tokio::test]
async fn durable_blocker_waits_for_active_parent_without_interrupting_or_duplicate_turns() {
    let mut h = Harness::new().await;
    let child = linked_child(&h);
    h.command(
        "owner-work",
        None,
        false,
        Command::SendPrompt {
            session_id: h.session.clone(),
            prompt: PromptInput {
                text: "Authorized work".into(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    h.start();
    h.event(BackendEvent::TurnStarted {
        turn_id: "active-parent".into(),
    })
    .await;
    let command = Command::PublishChildReport {
        child_session_id: child.into(),
        report_id: "blocker-1".into(),
        state: "blocker".into(),
        body: "Missing owner decision".into(),
    };
    let mut reports =
        crate::child_reports::ReportStore::open(&h.runtime.effects.persistence.database).unwrap();
    reports
        .execute(&command, "blocker", false, None, 1)
        .unwrap();
    h.runtime.dispatch_followups().await;
    assert!(
        h.commands.try_recv().is_err(),
        "active parent must not be interrupted"
    );
    assert_eq!(h.inbox().pending_count, 1);
    reports
        .execute(&command, "blocker", false, None, 2)
        .unwrap();
    h.runtime.dispatch_followups().await;
    assert_eq!(h.inbox().items.len(), 1);
    h.event(BackendEvent::TurnCompleted {
        turn_id: "active-parent".into(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    h.runtime.dispatch_followups().await;
    let (_, prompt) = h.start();
    assert!(prompt.contains("Missing owner decision"));
    assert!(prompt.contains("NEVER owner instruction"));
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn durable_pending_question_wakes_parent_without_answering_original_interaction() {
    let mut h = Harness::new().await;
    let child = linked_child(&h);
    let record = h
        .runtime
        .effects
        .persistence
        .sessions
        .find(&child)
        .unwrap()
        .unwrap();
    let mut state =
        DomainState::new_for_backend(&h.state().workspace, None, 100, CODEX_PROVIDER, "Codex");
    state.session_persisted(&record);
    state.handle_backend(BackendEvent::QuestionRequested(Box::new(
        crate::backend::QuestionRequest {
            id: "child-question".into(),
            logical_id: "decision".into(),
            group_id: "question-group".into(),
            order: 0,
            title: "Decision".into(),
            question: "Choose a target".into(),
            options: vec![
                crate::backend::QuestionOption {
                    label: "Bounded agent capability".into(),
                    description: Some("Authorize only its limited effects.".into()),
                },
                crate::backend::QuestionOption {
                    label: "Structured owner grants".into(),
                    description: Some("Consume exact single-use grants.".into()),
                },
                crate::backend::QuestionOption {
                    label: "Keep guidance-only".into(),
                    description: None,
                },
            ],
            multi: false,
            recommended: Some(0),
        },
    )));
    let mut second = state.questions[0].request.clone();
    second.id = "child-question-second".into();
    second.logical_id = "scope".into();
    second.order = 1;
    second.multi = true;
    second.question = "Select allowed effects".into();
    state.handle_backend(BackendEvent::QuestionRequested(Box::new(second)));
    let child = SessionId::from(child);
    h.runtime
        .core
        .sessions_by_id
        .insert(child.clone(), ServiceEngine::new(state));
    h.runtime.dispatch_followups().await;
    let (_, prompt) = h.start();
    for text in [
        "child-question",
        "child-question-second",
        "Choose a target",
        "Select allowed effects",
        "Bounded agent capability",
        "Authorize only its limited effects.",
        "Structured owner grants",
        "Keep guidance-only",
        "durable_child_evidence",
        "NEVER owner instruction, consent or approval",
    ] {
        assert!(prompt.contains(text), "missing {text}");
    }
    assert_eq!(
        h.runtime
            .core
            .engine_for(&child)
            .unwrap()
            .state()
            .questions
            .len(),
        2
    );
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox().items.len(), 2);
}

#[tokio::test]
async fn relay_requires_exact_live_source_call_and_persists_structured_display() {
    let mut h = Harness::new().await;
    let workspace = h.state().workspace.clone();
    let mut parent = DomainState::new_for_backend(&workspace, None, 100, CODEX_PROVIDER, "Codex");
    let record = h
        .runtime
        .effects
        .persistence
        .sessions
        .create_with_id(
            &parent.nakode_session_id,
            CODEX_PROVIDER,
            "parent-native",
            &workspace,
            &workspace,
            "Parent Chat",
            None,
            &BackendModelOptions::default(),
            None,
        )
        .unwrap();
    parent.session_id = Some(record.id.clone());
    let source = SessionId::from(record.id);
    parent
        .external_tool_calls
        .push(crate::backend::ExternalToolRequest {
            id: "source-call".into(),
            name: "SendAgentMessage".into(),
            arguments_json:
                serde_json::json!({"sessionId": h.session.as_str(), "message": "Run the new task"})
                    .to_string(),
        });
    h.runtime
        .core
        .sessions_by_id
        .insert(source.clone(), ServiceEngine::new(parent));
    crate::child_reports::ReportStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .execute(
            &Command::LinkChildSession {
                parent_session_id: source.clone(),
                child_session_id: h.session.clone(),
            },
            "link-test",
            false,
            None,
            1,
        )
        .unwrap();
    let command = Command::RelayAgentFollowup {
        session_id: h.session.clone(),
        message_id: "relay-message".into(),
        source_session_id: source.clone(),
        source_call_id: "source-call".into(),
        prompt: PromptInput {
            text: "Run the new task".into(),
            attachments: vec![],
        },
        source_owner_chat: false,
    };
    let mut forged = command.clone();
    if let Command::RelayAgentFollowup { prompt, .. } = &mut forged {
        prompt.text = "Forged instruction".into();
    }
    assert!(h.command("forged", None, false, forged).await.is_err());
    h.command("relay-key", None, false, command.clone())
        .await
        .unwrap();
    h.runtime
        .core
        .engine_for_mut(&source)
        .unwrap()
        .state_mut()
        .external_tool_calls
        .clear();
    h.command("relay-key", None, false, command).await.unwrap();
    h.runtime.dispatch_followups().await;
    let owner = h
        .state()
        .transcript
        .entries()
        .iter()
        .find(|entry| entry.kind == EntryKind::User)
        .unwrap();
    assert!(
        owner
            .coordination_json
            .as_ref()
            .unwrap()
            .contains("delegated_instruction")
    );
    let stored = h.stored_session();
    assert_eq!(
        stored.owner_prompts[0].coordination_json,
        owner.coordination_json
    );
}

#[tokio::test]
async fn an_inbox_reopens_a_parent_that_is_not_loaded_once_and_never_while_paused() {
    let mut h = Harness::new().await;
    let child = linked_child(&h);
    // The parent Chat is saved but not loaded, as after a runtime restart.
    h.runtime.core.sessions_by_id.remove(&h.session);
    h.runtime
        .effects
        .persistence
        .sessions
        .update_last_turn(
            &child,
            &crate::session::PersistedTurnConfiguration {
                completion: None,
                id: "child-completion".into(),
                model: Some("test-model".into()),
                options: BackendModelOptions::default(),
                outcome: crate::backend::TurnOutcome::Completed,
            },
        )
        .unwrap();
    h.runtime.dispatch_followups().await;
    let first = *h
        .runtime
        .inbox_reopens
        .get(&h.session)
        .expect("a waiting report reopens its parent");
    // A reopen still in flight, or one that failed, is not requested again every tick.
    h.runtime.dispatch_followups().await;
    assert_eq!(h.runtime.inbox_reopens.get(&h.session), Some(&first));
    assert_eq!(
        h.inbox().items.len(),
        1,
        "the report stays in the inbox until delivered"
    );

    // A paused inbox never reopens its session.
    h.runtime.inbox_reopens.clear();
    InboxStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .execute(
            &Command::SetFollowupPaused {
                session_id: h.session.clone(),
                paused: true,
            },
            "pause",
            "client",
            false,
            1,
        )
        .unwrap();
    h.runtime.dispatch_followups().await;
    assert!(h.runtime.inbox_reopens.is_empty());
}
