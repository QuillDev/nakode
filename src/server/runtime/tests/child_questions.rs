use super::*;

mod reliability;
use nakode_protocol::{ChildQuestionSnapshot, InteractionResolution, QuestionResponse};
use nakode_server::ServerEndpoint;

struct QuestionHarness {
    runtime: NativeServerRuntime,
    handle: super::super::NativeServerHandle,
    parent: SessionId,
    children: Vec<SessionId>,
    commands: Vec<mpsc::Receiver<BackendCommand>>,
    _control_commands: mpsc::Receiver<BackendCommand>,
    _directory: tempfile::TempDir,
}

async fn question_harness() -> QuestionHarness {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
    std::fs::create_dir_all(&root).unwrap();
    let directory = tempfile::tempdir_in(root).unwrap();
    let workspace = directory.path();
    let (persistence, _) = test_persistence(workspace);
    let mut states = Vec::new();
    for title in ["Parent", "Ticket child", "General child"] {
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
                &format!("native-{title}"),
                &state.workspace,
                &state.workspace,
                title,
                None,
                &BackendModelOptions::default(),
                None,
            )
            .unwrap();
        state.session_persisted(&record);
        state.provider_session_id = Some(record.provider_session_id);
        state.connection = crate::state::ConnectionState::Ready {
            server: "test".to_owned(),
        };
        state.provider_account_id = Some("test-account".to_owned());
        state.backend_capabilities.interruption = crate::backend::CapabilitySupport::Supported;
        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: format!("turn-{title}"),
        });
        states.push(state);
    }
    let parent_state = states.remove(0);
    let parent = SessionId::from(parent_state.nakode_session_id.clone());
    let effects = EffectExecutor::new(empty_registry(workspace).await, persistence);
    let (mut runtime, handle) = NativeServerRuntime::from_parts(
        ServiceEngine::new(parent_state),
        Vec::new(),
        Vec::new(),
        effects,
        mpsc::channel(1).1,
    );
    let (control, control_commands, control_events) = fake_backend();
    runtime
        .effects
        .backends
        .insert_provider_control(CODEX_PROVIDER.to_owned(), control);
    let mut children = Vec::new();
    let mut commands = Vec::new();
    // No provider events are needed by this harness. Close the fake streams so shutdown can
    // join their forwarding tasks, just as real backends do when they process Shutdown.
    drop(control_events);
    for mut state in states {
        let id = SessionId::from(state.nakode_session_id.clone());
        inject_questions(&mut state, &id);
        runtime
            .core
            .sessions_by_id
            .insert(id.clone(), ServiceEngine::new(state));
        let (backend, receiver, sender) = fake_backend();
        runtime.effects.backends.insert_session(
            id.clone(),
            CODEX_PROVIDER.to_owned(),
            "test-account".to_owned(),
            backend,
        );
        link_child(&runtime, &parent, &id);
        children.push(id);
        commands.push(receiver);
        drop(sender);
    }
    QuestionHarness {
        runtime,
        handle,
        parent,
        children,
        commands,
        _control_commands: control_commands,
        _directory: directory,
    }
}

fn link_child(runtime: &NativeServerRuntime, parent: &SessionId, child: &SessionId) {
    let mut store =
        crate::child_reports::ReportStore::open(&runtime.effects.persistence.database).unwrap();
    store
        .execute(
            &Command::LinkChildSession {
                parent_session_id: parent.clone(),
                child_session_id: child.clone(),
            },
            &format!("link-{child}"),
            false,
            None,
            0,
        )
        .unwrap();
}

fn inject_questions(state: &mut DomainState, id: &SessionId) {
    for (logical_id, order) in [("targets", 0), ("reason", 1)] {
        state.handle_backend(BackendEvent::QuestionRequested(Box::new(
            crate::backend::QuestionRequest {
                id: format!("{id}-{logical_id}"),
                logical_id: logical_id.to_owned(),
                group_id: "shared-ask".to_owned(),
                order,
                title: logical_id.to_owned(),
                question: format!("Choose {logical_id}"),
                options: vec![
                    crate::backend::QuestionOption {
                        label: "One".to_owned(),
                        description: Some("First target".to_owned()),
                    },
                    crate::backend::QuestionOption {
                        label: "Two".to_owned(),
                        description: None,
                    },
                ],
                multi: order == 0,
                recommended: Some(0),
            },
        )));
    }
}

async fn pending(endpoint: &ServerEndpoint, parent: &SessionId) -> ChildQuestionSnapshot {
    let view = endpoint
        .execute_query(
            ClientId::from("parent-view"),
            Query::ListChildQuestions {
                parent_session_id: parent.clone(),
            },
        )
        .await
        .unwrap();
    let QueryResult::ChildQuestions(view) = view.value else {
        panic!("child question snapshot")
    };
    view
}

fn answers() -> Vec<QuestionResponse> {
    vec![
        QuestionResponse {
            question_id: "targets".to_owned(),
            option_ids: vec!["0".to_owned(), "1".to_owned()],
            text: None,
        },
        QuestionResponse {
            question_id: "reason".to_owned(),
            option_ids: Vec::new(),
            text: Some("Owner already specified this".to_owned()),
        },
    ]
}

fn expect_answers(receiver: &mut mpsc::Receiver<BackendCommand>, child: &SessionId) {
    let expected = [
        (
            "targets",
            crate::backend::QuestionAnswer::Options(vec!["One".to_owned(), "Two".to_owned()]),
        ),
        (
            "reason",
            crate::backend::QuestionAnswer::Text("Owner already specified this".to_owned()),
        ),
    ];
    for (question, expected_answer) in expected {
        let BackendCommand::ResolveQuestion { id, answer } = receiver
            .try_recv()
            .expect("exact original waiter receives answer")
        else {
            panic!("unexpected backend command");
        };
        assert_eq!(id, format!("{child}-{question}"));
        assert_eq!(answer, expected_answer);
    }
    assert!(receiver.try_recv().is_err(), "no duplicate resume command");
}

#[tokio::test]
async fn both_layers_share_original_questions_and_parent_work_continues() {
    let mut harness = question_harness().await;
    let endpoint = harness.handle.endpoint().clone();
    let task = tokio::spawn(harness.runtime.run());
    let before = pending(&endpoint, &harness.parent).await;
    assert_eq!(before.children.len(), 2);
    for (index, id) in harness.children.iter().enumerate() {
        let item = before
            .children
            .iter()
            .find(|item| &item.child_session_id == id)
            .unwrap();
        let ask = &item.interactions[0];
        assert_eq!(ask.questions.len(), 2);
        assert!(ask.questions[0].multiple);
        assert!(ask.questions[0].options[0].recommended);
        let original = endpoint
            .execute_query(
                ClientId::from("child-view"),
                Query::GetSession {
                    session_id: id.clone(),
                },
            )
            .await
            .unwrap();
        let QueryResult::Session(original) = original.value else {
            panic!("session")
        };
        assert_eq!(original.interactions, item.interactions);
        let command = if index == 0 {
            Command::AnswerChildQuestions {
                parent_session_id: harness.parent.clone(),
                child_session_id: id.clone(),
                interaction_id: ask.id.clone(),
                answers: answers(),
            }
        } else {
            Command::ResolveInteraction {
                interaction_id: ask.id.clone(),
                resolution: InteractionResolution::AnswerQuestions { answers: answers() },
            }
        };
        endpoint
            .execute_command(
                ClientId::from("answer"),
                IdempotencyKey::from(format!("answer-{index}")),
                None,
                false,
                command,
            )
            .await
            .unwrap();
        expect_answers(&mut harness.commands[index], id);
    }
    assert!(
        pending(&endpoint, &harness.parent)
            .await
            .children
            .iter()
            .all(|item| item.interactions.is_empty())
    );
    let parent = endpoint
        .execute_query(
            ClientId::from("parent"),
            Query::GetSession {
                session_id: harness.parent.clone(),
            },
        )
        .await
        .unwrap();
    let QueryResult::Session(parent) = parent.value else {
        panic!("parent")
    };
    assert!(parent.active_turn.is_some());
    assert!(parent.interactions.is_empty());
    assert!(parent.queue.is_empty());
    harness.handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn competing_answers_have_one_winner_and_wrong_child_never_receives_an_answer() {
    let mut harness = question_harness().await;
    let endpoint = harness.handle.endpoint().clone();
    let task = tokio::spawn(harness.runtime.run());
    let view = pending(&endpoint, &harness.parent).await;
    let child = &harness.children[0];
    let ask = &view
        .children
        .iter()
        .find(|item| &item.child_session_id == child)
        .unwrap()
        .interactions[0];
    let mut parent_answer = Command::AnswerChildQuestions {
        parent_session_id: harness.parent.clone(),
        child_session_id: harness.children[1].clone(),
        interaction_id: ask.id.clone(),
        answers: answers(),
    };
    assert!(
        endpoint
            .execute_command(
                ClientId::from("parent"),
                IdempotencyKey::from("wrong-child"),
                None,
                false,
                parent_answer.clone()
            )
            .await
            .is_err()
    );
    if let Command::AnswerChildQuestions {
        child_session_id, ..
    } = &mut parent_answer
    {
        *child_session_id = child.clone();
    }
    let child_answer = Command::ResolveInteraction {
        interaction_id: ask.id.clone(),
        resolution: InteractionResolution::AnswerQuestions { answers: answers() },
    };
    let (left, right) = tokio::join!(
        endpoint.execute_command(
            ClientId::from("parent"),
            IdempotencyKey::from("parent-answer"),
            None,
            false,
            parent_answer.clone()
        ),
        endpoint.execute_command(
            ClientId::from("child"),
            IdempotencyKey::from("child-answer"),
            None,
            false,
            child_answer
        ),
    );
    assert_ne!(left.is_ok(), right.is_ok());
    assert!(
        endpoint
            .execute_command(
                ClientId::from("parent"),
                IdempotencyKey::from("late-answer"),
                None,
                false,
                parent_answer
            )
            .await
            .is_err()
    );
    expect_answers(&mut harness.commands[0], child);
    assert!(harness.commands[1].try_recv().is_err());
    harness.handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn retained_children_never_resurrect_answerable_questions_after_runtime_loss() {
    let mut harness = question_harness().await;
    let store =
        crate::child_reports::ReportStore::open(&harness.runtime.effects.persistence.database)
            .unwrap();
    let before =
        crate::child_questions::snapshot(&store, &harness.runtime.core, &harness.parent).unwrap();
    let child = &harness.children[0];
    let ask = &before
        .children
        .iter()
        .find(|item| &item.child_session_id == child)
        .unwrap()
        .interactions[0];
    harness.runtime.core.sessions_by_id.remove(child);
    let recovered_store =
        crate::child_reports::ReportStore::open(&harness.runtime.effects.persistence.database)
            .unwrap();
    let after =
        crate::child_questions::snapshot(&recovered_store, &harness.runtime.core, &harness.parent)
            .unwrap();
    let retained = after
        .children
        .iter()
        .find(|item| &item.child_session_id == child)
        .unwrap();
    assert_eq!(
        retained.availability,
        nakode_protocol::ChildQuestionAvailability::Unavailable
    );
    assert!(retained.interactions.is_empty());
    assert!(
        crate::child_questions::answer_command(
            &recovered_store,
            &harness.runtime.core,
            &harness.parent,
            child,
            &ask.id,
            answers()
        )
        .is_err()
    );
    assert_eq!(
        after
            .children
            .iter()
            .filter(|item| !item.interactions.is_empty())
            .count(),
        1
    );
}
