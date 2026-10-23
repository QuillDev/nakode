use super::*;
use crate::child_reports::ReportStore;

async fn admit_report(h: &mut Harness) -> String {
    let sessions = &h.runtime.effects.persistence.sessions;
    let child = sessions
        .create(CODEX_PROVIDER, "child", &h.state().workspace, "Child", None)
        .unwrap();
    let mut reports = ReportStore::open(&h.runtime.effects.persistence.database).unwrap();
    for (key, command) in [
        (
            "link",
            Command::LinkChildSession {
                parent_session_id: h.session.clone(),
                child_session_id: child.id.clone().into(),
            },
        ),
        (
            "report",
            Command::PublishChildReport {
                child_session_id: child.id.clone().into(),
                report_id: "done".into(),
                state: "completed".into(),
                body: "Finished child work".into(),
            },
        ),
    ] {
        reports.execute(&command, key, false, None, 1).unwrap();
    }
    h.runtime.dispatch_followups().await;
    child.id
}

/// Exercise the running actor's timer, not a test-only dispatch after completion. Evidence
/// admitted during active work must wake even if its source is deleted on either side of idle.
#[tokio::test]
async fn source_deleted_at_either_side_of_turn_completion_cannot_strand_automatic_delivery() {
    for deletion_before_completion in [true, false] {
        let mut h = Harness::new().await;
        h.command(
            "owner",
            None,
            false,
            Command::SendPrompt {
                session_id: h.session.clone(),
                prompt: PromptInput {
                    text: "Owner work".into(),
                    attachments: Vec::new(),
                },
            },
        )
        .await
        .unwrap();
        h.start();
        h.event(BackendEvent::TurnStarted {
            turn_id: "owner-turn".into(),
        })
        .await;
        let child = admit_report(&mut h).await;
        if !deletion_before_completion {
            h.event(BackendEvent::TurnCompleted {
                turn_id: "owner-turn".into(),
                outcome: crate::backend::TurnOutcome::Completed,
                error: None,
            })
            .await;
        }
        h.command(
            "delete",
            None,
            false,
            Command::DeleteSession {
                session_id: child.clone().into(),
                parent_fence: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(h.inbox().pending_count, 1);
        assert!(h.inbox().blocked_reason.is_none());

        let (events, receiver) = mpsc::channel(16);
        h.runtime.effects.backends.events = receiver;
        let source = BackendSource::Primary {
            session_id: h.session.clone(),
            provider: CODEX_PROVIDER.into(),
            account_id: "test-account".into(),
        };
        let database = h.runtime.effects.persistence.database.clone();
        let actor = tokio::spawn(h.runtime.run());
        if deletion_before_completion {
            assert!(
                tokio::time::timeout(Duration::from_millis(550), h.commands.recv())
                    .await
                    .is_err(),
                "active work must not be interrupted"
            );
            events
                .send((
                    source.clone(),
                    BackendEvent::TurnCompleted {
                        turn_id: "owner-turn".into(),
                        outcome: crate::backend::TurnOutcome::Completed,
                        error: None,
                    },
                ))
                .await
                .unwrap();
        }
        verify_delivery(
            &mut h.commands,
            &database,
            &h.session,
            &events,
            source,
            &child,
        )
        .await;
        actor.abort();
        let _ = actor.await;
    }
}

#[tokio::test]
async fn manual_owner_turn_does_not_consume_or_strand_deleted_source_evidence() {
    let mut h = Harness::new().await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "first-owner".into(),
    })
    .await;
    let child = admit_report(&mut h).await;
    h.command(
        "delete",
        None,
        false,
        Command::DeleteSession {
            session_id: child.into(),
            parent_fence: None,
        },
    )
    .await
    .unwrap();
    h.event(BackendEvent::TurnCompleted {
        turn_id: "first-owner".into(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    // An owner request wins this idle boundary before the timer claims a batch.
    h.command(
        "manual",
        None,
        false,
        Command::SendPrompt {
            session_id: h.session.clone(),
            prompt: PromptInput {
                text: "Manual owner work".into(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    let (_, prompt) = h.start();
    assert!(prompt.contains("Manual owner work"));
    assert!(!prompt.contains("Finished child work"));
    h.event(BackendEvent::TurnAccepted {
        turn_id: "manual-owner".into(),
    })
    .await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "manual-owner".into(),
    })
    .await;
    h.runtime.dispatch_followups().await;
    assert!(h.commands.try_recv().is_err());
    assert_eq!(
        h.inbox().pending_count,
        1,
        "owner input and reads are not an inbox acknowledgement"
    );
    assert_eq!(h.inbox().items[0].state, "pending");
    h.event(BackendEvent::TurnCompleted {
        turn_id: "manual-owner".into(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    })
    .await;
    for _ in 0..2 {
        h.runtime.dispatch_followups().await;
    }
    let (_, prompt) = h.start();
    assert!(prompt.contains("Finished child work"));
    assert_eq!(h.inbox().items[0].state, "dispatching");
}

async fn verify_delivery(
    commands: &mut mpsc::Receiver<BackendCommand>,
    database: &Path,
    session: &SessionId,
    events: &mpsc::Sender<(BackendSource, BackendEvent)>,
    source: BackendSource,
    child: &str,
) {
    let command = tokio::time::timeout(Duration::from_secs(5), commands.recv())
        .await
        .expect("eligible report must automatically reach provider input")
        .unwrap();
    let BackendCommand::StartTurn {
        client_id, prompt, ..
    } = command
    else {
        panic!("expected continuation input");
    };
    let batch = client_id;
    assert!(prompt.contains("Finished child work"));
    assert!(prompt.contains(child));
    assert!(prompt.contains("durable_child_evidence"));
    let store = InboxStore::open(database).unwrap();
    assert_eq!(
        store.list(session, 0, 64).unwrap().items[0].state,
        "dispatching"
    );
    events
        .send((
            source.clone(),
            BackendEvent::TurnAccepted {
                turn_id: "report-turn".into(),
            },
        ))
        .await
        .unwrap();
    events
        .send((
            source.clone(),
            BackendEvent::TurnStarted {
                turn_id: "report-turn".into(),
            },
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inbox = store.list(session, 0, 64).unwrap();
            if inbox.items[0].state == "consumed" {
                assert_eq!(inbox.items[0].batch_id.as_deref(), Some(batch.as_str()));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    events
        .send((
            source,
            BackendEvent::TurnCompleted {
                turn_id: "report-turn".into(),
                outcome: crate::backend::TurnOutcome::Completed,
                error: None,
            },
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(550), commands.recv())
            .await
            .is_err(),
        "delivered input must not be duplicated"
    );
}
