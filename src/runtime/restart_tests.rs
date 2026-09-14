use super::*;
use crate::session::SessionRepository;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

struct RestartProvider {
    calls: AtomicUsize,
    checkpointed: Notify,
}

impl InferenceProvider for RestartProvider {
    fn infer(
        &self,
        _request: InferenceRequest,
        _events: mpsc::Sender<InferenceEvent>,
        _cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if round == 0 {
                return Ok(InferenceOutput {
                    text: "Recorded progress before restart".to_owned(),
                    tool_calls: vec![ToolCall {
                        id: "write-once".to_owned(),
                        parent_call_id: None,
                        name: "write".to_owned(),
                        arguments: serde_json::json!({"path":"effect.txt","content":"first effect"}),
                    }],
                    ..InferenceOutput::default()
                });
            }
            self.checkpointed.notify_one();
            std::future::pending().await
        })
    }
}

// Abort only this isolated turn task, reopen the SQLite checkpoint, and create a new runtime.
// The same test covers Chat's primary native session and a Ticket Agent's native session.
#[tokio::test]
async fn restart_checkpoint_retains_chat_and_ticket_agent_progress_without_replay() {
    for owner in ["chat-orchestrator", "ticket-agent"] {
        let root = tempfile::tempdir().expect("isolated workspace");
        let database = root.path().join("sessions.sqlite3");
        let repository = crate::session::SqliteSessionRepository::open(&database).expect("schema");
        let store = RuntimeSessionStore::new(database, "fixture");
        let provider = Arc::new(RestartProvider {
            calls: AtomicUsize::new(0),
            checkpointed: Notify::new(),
        });
        let runtime = AgentRuntime::new(root.path().to_path_buf(), provider.clone())
            .with_session_store(store.clone());
        let mut session = RuntimeSession::new(
            "fixture-model".to_owned(),
            "retained instructions".to_owned(),
        )
        .with_provider("fixture")
        .with_owner(Some(owner.to_owned()), None);
        let id = session.id.clone();
        repository
            .create_with_id(
                owner,
                "fixture",
                &id,
                &root.path().to_string_lossy(),
                &root.path().to_string_lossy(),
                "Retained owner title",
                None,
                &crate::backend::ModelOptions::default(),
                None,
            )
            .expect("logical identity");
        let (events, _receiver) = mpsc::channel(128);
        let task = tokio::spawn(async move {
            runtime
                .run_turn(
                    &mut session,
                    "interrupted-turn",
                    "Write once".to_owned(),
                    Vec::new(),
                    &events,
                    CancellationToken::new(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), provider.checkpointed.notified())
            .await
            .expect("tool result checkpoint");
        task.abort();
        assert!(
            task.await
                .expect_err("abrupt task interruption")
                .is_cancelled()
        );
        assert_checkpointed_turn(
            &repository,
            owner,
            "interrupted-turn",
            crate::backend::TurnOutcome::Interrupted,
        );
        let mut restored = store.load(&id).expect("read").expect("durable identity");
        assert_eq!(restored.id, id);
        assert_eq!(restored.owner_session_id.as_deref(), Some(owner));
        assert_eq!(restored.instructions, "retained instructions");
        assert_eq!(restored.pending_turn.as_deref(), Some("interrupted-turn"));
        assert!(
            restored
                .normalized_history()
                .iter()
                .any(|item| item.item.body.contains("Recorded progress"))
        );
        assert!(
            restored
                .normalized_history()
                .iter()
                .any(|item| item.item.title == "Turn interrupted")
        );
        assert!(restored.history.iter().any(|item| matches!(item, ConversationItem::ToolResult { call_id, failed: false, .. } if call_id == "write-once")));
        assert_eq!(
            std::fs::read_to_string(root.path().join("effect.txt")).expect("effect"),
            "first effect"
        );
        // Explicit restoration settles metadata only. Neither it nor repeated read-only hydration executes tools.
        std::fs::write(root.path().join("effect.txt"), "owner changed it").expect("owner edit");
        restored.recover_interrupted_turn();
        store.save(&restored).expect("explicit recovery checkpoint");
        assert_eq!(restored.interrupted_turns.len(), 1);
        assert_eq!(restored.interrupted_turns[0].id, "interrupted-turn");
        assert!(restored.pending_turn.is_none());
        assert_eq!(
            std::fs::read_to_string(root.path().join("effect.txt")).expect("retained effect"),
            "owner changed it"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_safe_continuation(root.path(), &store, &mut restored).await;
        assert_checkpointed_turn(
            &repository,
            owner,
            "new-owner-turn",
            crate::backend::TurnOutcome::Completed,
        );
    }
}

fn assert_checkpointed_turn(
    repository: &dyn SessionRepository,
    owner: &str,
    turn: &str,
    outcome: crate::backend::TurnOutcome,
) {
    let logical = repository
        .find(owner)
        .expect("read projection")
        .expect("logical session");
    let last = logical.last_turn.expect("checkpoint projects current turn");
    assert_eq!(last.id, turn);
    assert_eq!(last.outcome, outcome);
    assert_eq!(last.model.as_deref(), Some("fixture/fixture-model"));
}

struct ContinuationProvider;
impl InferenceProvider for ContinuationProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        _events: mpsc::Sender<InferenceEvent>,
        _cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move {
            assert!(request.history.iter().any(|item| matches!(item, ConversationItem::ToolResult { call_id, .. } if call_id == "write-once")));
            Ok(InferenceOutput {
                text: "Explicit continuation".to_owned(),
                ..InferenceOutput::default()
            })
        })
    }
}

async fn assert_safe_continuation(
    root: &std::path::Path,
    store: &RuntimeSessionStore,
    session: &mut RuntimeSession,
) {
    let runtime = AgentRuntime::new(root.to_path_buf(), Arc::new(ContinuationProvider))
        .with_session_store(store.clone());
    let (events, _receiver) = mpsc::channel(128);
    let count = session.history.len();
    let replay = runtime
        .run_turn(
            session,
            "interrupted-turn",
            "Write once".to_owned(),
            Vec::new(),
            &events,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(replay, Err(TurnError::Interrupted)));
    assert_eq!(
        session.history.len(),
        count,
        "checkpoint/acknowledgement crash window cannot duplicate an owner prompt"
    );
    runtime
        .run_turn(
            session,
            "new-owner-turn",
            "Continue from retained evidence".to_owned(),
            Vec::new(),
            &events,
            CancellationToken::new(),
        )
        .await
        .expect("explicit continuation");
    assert_eq!(
        std::fs::read_to_string(root.join("effect.txt")).expect("no replay"),
        "owner changed it"
    );
    let mut completed = store
        .load(&session.id)
        .expect("read completed checkpoint")
        .expect("identity");
    let retained_count = completed.history.len();
    runtime
        .run_turn(
            &mut completed,
            "new-owner-turn",
            "Continue from retained evidence".to_owned(),
            Vec::new(),
            &events,
            CancellationToken::new(),
        )
        .await
        .expect("completed redelivery");
    assert_eq!(
        completed.history.len(),
        retained_count,
        "completed turn is not replayed after acknowledgement loss"
    );
    let history = session.normalized_history();
    let interruption = history
        .iter()
        .position(|item| item.item.title == "Turn interrupted")
        .expect("notice");
    let continued = history
        .iter()
        .position(|item| item.item.body == "Explicit continuation")
        .expect("answer");
    assert!(
        interruption < continued,
        "interruption remains at its historical boundary"
    );
}

#[tokio::test]
async fn native_turn_task_shutdown_joins_and_drop_cancels_owned_work() {
    let cancellation = CancellationToken::new();
    let stopped = Arc::new(AtomicUsize::new(0));
    let turn_cancellation = cancellation.clone();
    let marker = stopped.clone();
    let task = NativeTurnTask::new(tokio::spawn(async move {
        turn_cancellation.cancelled().await;
        marker.fetch_add(1, Ordering::SeqCst);
    }));
    task.stop(&cancellation).await;
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let task = NativeTurnTask::new(tokio::spawn(async move {
        let _sender = sender;
        std::future::pending::<()>().await;
    }));
    drop(task);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("task aborted")
            .is_err()
    );
}

#[test]
fn interrupted_tool_outcome_is_unknown_and_never_replayed() {
    let mut session = RuntimeSession::new("fixture".to_owned(), String::new());
    session.pending_turn = Some("turn".to_owned());
    session.history.push(ConversationItem::Assistant {
        text: "Starting operation".to_owned(),
        reasoning: String::new(),
        tool_calls: vec![ToolCall {
            id: "uncertain".to_owned(),
            parent_call_id: None,
            name: "CreateTicket".to_owned(),
            arguments: serde_json::json!({}),
        }],
        provider_id: None,
        model_id: None,
        signature: None,
        provider_state: Vec::new(),
    });
    session.recover_interrupted_turn();
    session.recover_interrupted_turn();
    assert_eq!(session.history.len(), 2);
    assert!(
        matches!(&session.history[1], ConversationItem::ToolResult { output, failed: true, .. } if output.contains("may have happened") && output.contains("not automatically replayed"))
    );
}
