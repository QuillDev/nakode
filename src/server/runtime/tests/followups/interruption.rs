use super::*;

async fn running() -> Harness {
    let mut h = Harness::new().await;
    h.event(BackendEvent::Ready(crate::backend::BackendIdentity {
        provider: CODEX_PROVIDER.to_owned(),
        display_name: "Codex".to_owned(),
        version: None,
        capabilities: h.state().backend_capabilities.clone(),
    }))
    .await;
    h.command(
        "owner-task",
        None,
        false,
        Command::SendPrompt {
            session_id: h.session.clone(),
            prompt: PromptInput {
                text: "Original task, never replay".to_owned(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    h.start();
    h.event(BackendEvent::TurnStarted {
        turn_id: "original".to_owned(),
    })
    .await;
    h
}

async fn finish(h: &mut Harness) {
    h.event(BackendEvent::TurnCompleted {
        turn_id: "original".to_owned(),
        outcome: crate::backend::TurnOutcome::Interrupted,
        error: None,
    })
    .await;
}

async fn interrupt(h: &mut Harness, manual: bool) {
    let command = if manual {
        Command::CancelSessionWork {
            session_id: h.session.clone(),
            parent_fence: None,
        }
    } else {
        Command::InterruptSessionWork {
            session_id: h.session.clone(),
        }
    };
    h.command("stop", None, false, command).await.unwrap();
    assert!(matches!(
        h.commands.try_recv().unwrap(),
        BackendCommand::InterruptTurn { .. }
    ));
}

async fn unrelated_inbox(h: &mut Harness) -> (SessionId, FollowupInbox) {
    let mut other =
        DomainState::new_for_backend(&h.state().workspace, None, 100, CODEX_PROVIDER, "Codex");
    other.synchronize_workspace_configuration(h.state());
    let record = h
        .runtime
        .effects
        .persistence
        .sessions
        .create_with_id(
            &other.nakode_session_id,
            CODEX_PROVIDER,
            "other",
            &other.workspace,
            &other.workspace,
            "Other agent",
            None,
            &BackendModelOptions::default(),
            None,
        )
        .unwrap();
    other.session_persisted(&record);
    let other_id = SessionId::from(record.id);
    h.runtime
        .core
        .sessions_by_id
        .insert(other_id.clone(), ServiceEngine::new(other));
    h.command(
        "other-message",
        None,
        false,
        Command::EnqueueFollowup {
            session_id: other_id.clone(),
            message_id: "other-message".to_owned(),
            prompt: PromptInput {
                text: "Other scope".to_owned(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    h.command(
        "other-pause",
        None,
        false,
        Command::SetFollowupPaused {
            session_id: other_id.clone(),
            paused: true,
        },
    )
    .await
    .unwrap();
    let inbox = InboxStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .list(&other_id, 0, 64)
        .unwrap();
    (other_id, inbox)
}

#[tokio::test]
async fn tool_interrupt_waits_for_idle_then_dispatches_ordered_followups_once() {
    let mut h = Box::pin(running()).await;
    h.enqueue("first").await;
    h.enqueue("second").await;
    // A second logical session shares this runtime/store, but not cancellation or inbox state.
    let (other_id, other_before) = Box::pin(unrelated_inbox(&mut h)).await;
    interrupt(&mut h, false).await;
    let other_after = InboxStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .list(&other_id, 0, 64)
        .unwrap();
    assert_eq!(other_before, other_after);
    assert!(!h.inbox().paused);
    for _ in 0..3 {
        h.runtime.dispatch_followups().await;
    }
    assert!(h.commands.try_recv().is_err(), "cancellation is not idle");
    assert_eq!(h.inbox().pending_count, 2);
    h.enqueue("during-cancel").await;
    finish(&mut h).await;
    for _ in 0..3 {
        h.runtime.dispatch_followups().await;
    }
    let (_, prompt) = h.start();
    let first = prompt.find("Requirement first").unwrap();
    let second = prompt.find("Requirement second").unwrap();
    let third = prompt.find("Requirement during-cancel").unwrap();
    assert!(first < second && second < third);
    assert!(!prompt.contains("Original task"));
    h.event(BackendEvent::TurnAccepted {
        turn_id: "followup".to_owned(),
    })
    .await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "followup".to_owned(),
    })
    .await;
    // A transport retry must not interrupt the new turn or change its inbox.
    h.command(
        "stop",
        None,
        false,
        Command::InterruptSessionWork {
            session_id: h.session.clone(),
        },
    )
    .await
    .unwrap();
    assert!(h.inbox().items.iter().all(|item| item.state == "consumed"));
    h.event(BackendEvent::TurnCompleted {
        turn_id: "followup".to_owned(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    for _ in 0..3 {
        h.runtime.dispatch_followups().await;
    }
    assert!(
        h.commands.try_recv().is_err(),
        "no restart or duplicate dispatch"
    );
}

#[tokio::test]
async fn manual_stop_and_prepaused_tool_interrupt_keep_pending_inbox_paused() {
    for manual in [true, false] {
        let mut h = Box::pin(running()).await;
        h.enqueue("pending").await;
        if !manual {
            h.command(
                "manual-pause",
                None,
                false,
                Command::SetFollowupPaused {
                    session_id: h.session.clone(),
                    paused: true,
                },
            )
            .await
            .unwrap();
        }
        interrupt(&mut h, manual).await;
        finish(&mut h).await;
        for _ in 0..3 {
            h.runtime.dispatch_followups().await;
        }
        assert!(h.inbox().paused);
        assert_eq!(h.inbox().pending_count, 1);
        assert!(h.commands.try_recv().is_err());
    }
}

#[tokio::test]
async fn tool_interrupt_after_completion_does_not_restart_and_rejection_is_isolated() {
    let mut h = Box::pin(running()).await;
    h.enqueue("pending").await;
    let before = h.inbox();
    let error = h
        .command(
            "unknown-session",
            None,
            false,
            Command::InterruptSessionWork {
                session_id: SessionId::from("unrelated"),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(h.inbox(), before);
    assert!(h.commands.try_recv().is_err());
    finish(&mut h).await;
    h.command(
        "late-stop",
        None,
        false,
        Command::InterruptSessionWork {
            session_id: h.session.clone(),
        },
    )
    .await
    .unwrap();
    assert!(!h.inbox().paused);
    for _ in 0..3 {
        h.runtime.dispatch_followups().await;
    }
    let (_, prompt) = h.start();
    assert!(prompt.contains("Requirement pending"));
    assert!(!prompt.contains("Original task"));
}

async fn steer(h: &mut Harness, key: &str) -> Result<CommandAccepted, ServiceError> {
    let session_id = h.session.clone();
    h.command(key, None, false, Command::SteerFollowups { session_id })
        .await
}

#[tokio::test]
async fn steer_now_interrupts_once_keeps_inbox_enabled_and_delivers_pending_in_order() {
    let mut h = Box::pin(running()).await;
    h.enqueue("first").await;
    h.enqueue("second").await;
    steer(&mut h, "steer-1").await.unwrap();
    assert!(matches!(
        h.commands.try_recv().unwrap(),
        BackendCommand::InterruptTurn { .. }
    ));
    assert!(!h.inbox().paused);
    // A double click while the turn is still being interrupted is accepted without a second
    // provider interrupt.
    steer(&mut h, "steer-2").await.unwrap();
    assert!(h.commands.try_recv().is_err());
    h.enqueue("during-cancel").await;
    finish(&mut h).await;
    h.runtime.dispatch_followups().await;
    let (_, prompt) = h.start();
    let first = prompt.find("Requirement first").unwrap();
    let second = prompt.find("Requirement second").unwrap();
    let third = prompt.find("Requirement during-cancel").unwrap();
    assert!(first < second && second < third);
    assert!(!prompt.contains("Original task"));
    h.event(BackendEvent::TurnAccepted {
        turn_id: "followup".to_owned(),
    })
    .await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "followup".to_owned(),
    })
    .await;
    // A late click finds nothing pending and must not interrupt the delivering turn.
    let error = steer(&mut h, "steer-3").await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(h.commands.try_recv().is_err());
    // A transport retry of the accepted first click replays its outcome, not a new interrupt.
    steer(&mut h, "steer-1").await.unwrap();
    assert!(h.commands.try_recv().is_err());
    assert!(h.inbox().items.iter().all(|item| item.state == "consumed"));
    assert!(!h.inbox().paused);
}

#[tokio::test]
async fn steer_now_refuses_without_interrupting_when_nothing_can_deliver() {
    let mut h = Box::pin(running()).await;
    let error = steer(&mut h, "empty").await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(h.commands.try_recv().is_err());

    h.enqueue("pending").await;
    h.command(
        "manual-pause",
        None,
        false,
        Command::SetFollowupPaused {
            session_id: h.session.clone(),
            paused: true,
        },
    )
    .await
    .unwrap();
    let error = steer(&mut h, "paused").await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(h.commands.try_recv().is_err());
    assert!(h.inbox().paused);
    assert_eq!(h.inbox().pending_count, 1);
    // A retry of the refused request under its key replays the refusal after Resume.
    h.command(
        "manual-resume",
        None,
        false,
        Command::SetFollowupPaused {
            session_id: h.session.clone(),
            paused: false,
        },
    )
    .await
    .unwrap();
    let retried = steer(&mut h, "paused").await.unwrap_err();
    assert_eq!(retried, error);
    assert!(h.commands.try_recv().is_err());
    assert!(
        h.state()
            .active_turn
            .as_ref()
            .is_some_and(|turn| !turn.cancelling)
    );
}

#[tokio::test]
async fn steer_now_on_idle_session_waits_for_dispatch_and_never_interrupts_a_delivering_batch() {
    let mut h = Box::pin(running()).await;
    h.event(BackendEvent::TurnCompleted {
        turn_id: "original".to_owned(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    h.command(
        "pause",
        None,
        false,
        Command::SetFollowupPaused {
            session_id: h.session.clone(),
            paused: true,
        },
    )
    .await
    .unwrap();
    h.enqueue("idle").await;
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
    // Idle: nothing to interrupt; the next tick delivers.
    steer(&mut h, "idle-steer").await.unwrap();
    assert!(h.commands.try_recv().is_err());
    h.runtime.dispatch_followups().await;
    let (_, prompt) = h.start();
    assert!(prompt.contains("Requirement idle"));
    // The batch is fenced but its turn has not started; a new arrival must not let a steer
    // interrupt the delivery in flight.
    h.enqueue("arrival").await;
    let error = steer(&mut h, "race-steer").await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox().pending_count, 1);
}

#[tokio::test]
async fn steer_now_cancellation_failure_keeps_every_message_pending() {
    let mut h = Box::pin(running()).await;
    h.enqueue("kept").await;
    h.runtime
        .core
        .engine_for_mut(&h.session)
        .unwrap()
        .state_mut()
        .backend_capabilities
        .interruption = crate::backend::CapabilitySupport::Unsupported;
    let before = h.inbox();
    let error = steer(&mut h, "unsupported").await.unwrap_err();
    assert_ne!(error.code, ErrorCode::NotFound);
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.inbox(), before);
    assert!(!h.inbox().paused);
}
