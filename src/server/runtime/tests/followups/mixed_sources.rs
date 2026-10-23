use super::*;
use crate::child_reports::ReportStore;

fn linked_report_source(h: &Harness) -> String {
    let child = h
        .runtime
        .effects
        .persistence
        .sessions
        .create(
            CODEX_PROVIDER,
            "report source",
            &h.state().workspace,
            "Source",
            None,
        )
        .unwrap();
    ReportStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .execute(
            &Command::LinkChildSession {
                parent_session_id: h.session.clone(),
                child_session_id: child.id.clone().into(),
            },
            &format!("link:{}", child.id),
            false,
            None,
            1,
        )
        .unwrap();
    child.id
}

fn publish(h: &Harness, child: &str, report: &str, state: &str) {
    ReportStore::open(&h.runtime.effects.persistence.database)
        .unwrap()
        .execute(
            &Command::PublishChildReport {
                child_session_id: child.into(),
                report_id: report.into(),
                state: state.into(),
                body: report.into(),
            },
            &format!("report:{child}:{report}"),
            false,
            None,
            2,
        )
        .unwrap();
}

#[tokio::test]
async fn live_source_only_reports_automatically_reach_model_input() {
    Box::pin(exercise_report_sources(false)).await;
}

#[tokio::test]
async fn deleted_source_head_cannot_strand_live_question_and_completion_reports() {
    Box::pin(exercise_report_sources(true)).await;
}

async fn report_fixture(deleted_head: bool) -> (Harness, Vec<&'static str>) {
    let mut h = Harness::new().await;
    h.event(BackendEvent::TurnStarted {
        turn_id: "owner".into(),
    })
    .await;
    let mut expected = Vec::new();
    if deleted_head {
        let child = linked_report_source(&h);
        publish(&h, &child, "deleted-completion", "completed");
        h.runtime.dispatch_followups().await;
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
        expected.push("deleted-completion");
    }
    let live = linked_report_source(&h);
    publish(&h, &live, "live-question", "question");
    publish(&h, &live, "live-completion", "completed");
    expected.extend(["live-question", "live-completion"]);
    h.runtime.dispatch_followups().await;
    assert!(
        h.runtime
            .effects
            .persistence
            .sessions
            .find(&live)
            .unwrap()
            .is_some()
    );
    let inbox = h.inbox();
    assert_eq!(inbox.items.len(), expected.len());
    assert!(
        inbox
            .items
            .iter()
            .all(|item| item.state == "pending" && item.batch_id.is_none())
    );
    (h, expected)
}

async fn exercise_report_sources(deleted_head: bool) {
    let (mut h, expected) = Box::pin(report_fixture(deleted_head)).await;
    let database = h.runtime.effects.persistence.database.clone();
    let store = InboxStore::open(&database).unwrap();
    let source = BackendSource::Primary {
        session_id: h.session.clone(),
        provider: CODEX_PROVIDER.into(),
        account_id: "test-account".into(),
    };
    let (events, receiver) = mpsc::channel(16);
    h.runtime.effects.backends.events = receiver;
    let actor = tokio::spawn(h.runtime.run());
    assert!(
        tokio::time::timeout(Duration::from_millis(550), h.commands.recv())
            .await
            .is_err()
    );
    events
        .send((source.clone(), completed("owner")))
        .await
        .unwrap();
    let batch = receive_batch(&mut h.commands, &expected, &store, &h.session).await;
    // Exercise the same ledger acknowledgment used by runtime turn events, without changing
    // the actor's active-turn state with a fabricated provider lifecycle.
    store.acknowledge(&h.session, "unrelated-turn").unwrap();
    let inbox = store.list(&h.session, 0, 64).unwrap();
    assert!(
        inbox
            .items
            .iter()
            .all(|item| item.state == "dispatching" && item.batch_id.as_deref() == Some(&batch))
    );
    for event in [
        BackendEvent::TurnAccepted {
            turn_id: "reports".into(),
        },
        BackendEvent::TurnStarted {
            turn_id: "reports".into(),
        },
    ] {
        events.send((source.clone(), event)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inbox = store.list(&h.session, 0, 64).unwrap();
            if inbox.items.iter().all(|item| item.state == "consumed") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    events.send((source, completed("reports"))).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(550), h.commands.recv())
            .await
            .is_err(),
        "no duplicate input after consumption"
    );
    actor.abort();
    let _ = actor.await;
}

fn completed(turn: &str) -> BackendEvent {
    BackendEvent::TurnCompleted {
        turn_id: turn.into(),
        outcome: crate::backend::TurnOutcome::Completed,
        error: None,
    }
}

async fn receive_batch(
    commands: &mut mpsc::Receiver<BackendCommand>,
    expected: &[&str],
    store: &InboxStore,
    session: &SessionId,
) -> String {
    let command = tokio::time::timeout(Duration::from_secs(5), commands.recv())
        .await
        .unwrap_or_else(|error| {
            let inbox = store.list(session, 0, 64).unwrap();
            let states: Vec<_> = inbox
                .items
                .iter()
                .map(|item| (&item.state, &item.batch_id))
                .collect();
            panic!(
                "automatic delivery timed out: pending={}, states={states:?}, error={error}",
                inbox.pending_count
            );
        })
        .unwrap();
    let BackendCommand::StartTurn {
        client_id, prompt, ..
    } = command
    else {
        panic!("expected provider continuation");
    };
    let mut offset = 0;
    for report in expected {
        let position = prompt[offset..]
            .find(report)
            .expect("ordered original report in batch");
        offset += position + report.len();
    }
    assert!(prompt.contains("durable_child_evidence"));
    client_id
}
