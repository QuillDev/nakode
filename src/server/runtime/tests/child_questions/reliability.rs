use super::*;

fn first_question(harness: &QuestionHarness) -> nakode_protocol::InteractionId {
    let store =
        crate::child_reports::ReportStore::open(&harness.runtime.effects.persistence.database)
            .unwrap();
    let view =
        crate::child_questions::snapshot(&store, &harness.runtime.core, &harness.parent).unwrap();
    view.children
        .iter()
        .find(|child| child.child_session_id == harness.children[0])
        .unwrap()
        .interactions[0]
        .id
        .clone()
}

fn parent_answer(harness: &QuestionHarness) -> Command {
    Command::AnswerChildQuestions {
        parent_session_id: harness.parent.clone(),
        child_session_id: harness.children[0].clone(),
        interaction_id: first_question(harness),
        answers: answers(),
    }
}

#[tokio::test]
async fn same_key_replays_original_parent_command_without_resuming_twice() {
    let mut harness = question_harness().await;
    let command = parent_answer(&harness);
    let endpoint = harness.handle.endpoint().clone();
    let task = tokio::spawn(harness.runtime.run());
    let accepted = endpoint
        .execute_command(
            ClientId::from("parent"),
            IdempotencyKey::from("answer"),
            None,
            false,
            command.clone(),
        )
        .await
        .unwrap();
    expect_answers(&mut harness.commands[0], &harness.children[0]);
    for replay_only in [false, true] {
        let replay = endpoint
            .execute_command(
                ClientId::from("reconnected-parent"),
                IdempotencyKey::from("answer"),
                Some(u64::MAX),
                replay_only,
                command.clone(),
            )
            .await
            .unwrap();
        assert_eq!(replay, accepted);
        assert!(harness.commands[0].try_recv().is_err());
    }
    let mut changed_parent = command.clone();
    if let Command::AnswerChildQuestions {
        parent_session_id, ..
    } = &mut changed_parent
    {
        *parent_session_id = SessionId::from("another-parent");
    }
    let error = endpoint
        .execute_command(
            ClientId::from("parent"),
            IdempotencyKey::from("answer"),
            None,
            false,
            changed_parent,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(error.message.contains("different command"));
    let Command::AnswerChildQuestions {
        interaction_id,
        answers,
        ..
    } = command
    else {
        unreachable!()
    };
    let error = endpoint
        .execute_command(
            ClientId::from("child"),
            IdempotencyKey::from("answer"),
            None,
            false,
            Command::ResolveInteraction {
                interaction_id,
                resolution: InteractionResolution::AnswerQuestions { answers },
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        ErrorCode::Conflict,
        "parent and child commands have distinct receipts"
    );
    assert!(harness.commands[0].try_recv().is_err());
    harness.handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn invalid_answers_and_missing_replay_or_stale_revision_preserve_the_entire_ask() {
    let mut harness = question_harness().await;
    let command = parent_answer(&harness);
    let endpoint = harness.handle.endpoint().clone();
    let task = tokio::spawn(harness.runtime.run());
    let before = pending(&endpoint, &harness.parent).await;
    for (key, revision, replay) in [
        ("missing-receipt", None, true),
        ("stale-revision", Some(u64::MAX), false),
    ] {
        let error = endpoint
            .execute_command(
                ClientId::from("parent"),
                IdempotencyKey::from(key),
                revision,
                replay,
                command.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict);
    }
    let mut partial = answers();
    partial.pop();
    let mut duplicate = answers();
    duplicate[1] = duplicate[0].clone();
    let mut unknown = answers();
    unknown[0].option_ids = vec!["not-an-option".to_owned()];
    let mut mixed = answers();
    mixed[0].text = Some("cannot also select".to_owned());
    for (index, invalid) in [partial, duplicate, unknown, mixed].into_iter().enumerate() {
        let mut invalid_command = command.clone();
        if let Command::AnswerChildQuestions { answers, .. } = &mut invalid_command {
            *answers = invalid;
        }
        let error = endpoint
            .execute_command(
                ClientId::from("parent"),
                IdempotencyKey::from(format!("invalid-{index}")),
                None,
                false,
                invalid_command,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidRequest);
    }
    assert_eq!(pending(&endpoint, &harness.parent).await, before);
    assert!(
        harness
            .commands
            .iter_mut()
            .all(|commands| commands.try_recv().is_err())
    );
    endpoint
        .execute_command(
            ClientId::from("parent"),
            IdempotencyKey::from("valid"),
            None,
            false,
            command,
        )
        .await
        .unwrap();
    expect_answers(&mut harness.commands[0], &harness.children[0]);
    harness.handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn current_profile_binding_is_rechecked_before_parent_read_and_answer() {
    let mut harness = question_harness().await;
    let command = parent_answer(&harness);
    harness
        .runtime
        .effects
        .persistence
        .sessions
        .bind_session_skill_profile(harness.children[0].as_str(), "different-owner")
        .unwrap();
    let endpoint = harness.handle.endpoint().clone();
    let task = tokio::spawn(harness.runtime.run());
    let view = pending(&endpoint, &harness.parent).await;
    assert_eq!(view.children.len(), 1);
    assert_eq!(view.children[0].child_session_id, harness.children[1]);
    let error = endpoint
        .execute_command(
            ClientId::from("parent"),
            IdempotencyKey::from("no-longer-owned"),
            None,
            false,
            command,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        harness
            .commands
            .iter_mut()
            .all(|commands| commands.try_recv().is_err())
    );
    harness.handle.shutdown().await;
    task.await.unwrap();
}

fn assert_late_completion_preserves_questions(engine: &mut ServiceEngine) {
    engine
        .state_mut()
        .handle_backend(BackendEvent::TurnCompleted {
            turn_id: "unrelated-old-turn".to_owned(),
            outcome: crate::backend::TurnOutcome::Completed,
            error: None,
        });
    assert_eq!(
        engine.state().questions.len(),
        2,
        "stale terminal events cannot cancel the live ask"
    );
}

#[tokio::test]
async fn cancelled_and_disconnected_questions_retire_in_both_views_without_answers() {
    for disconnect in [false, true] {
        let mut harness = question_harness().await;
        let command = parent_answer(&harness);
        let child = &harness.children[0];
        let engine = harness.runtime.core.engine_for_mut(child).unwrap();
        assert_late_completion_preserves_questions(engine);
        if disconnect {
            engine
                .state_mut()
                .handle_backend(BackendEvent::Disconnected {
                    reason: "backend lost".to_owned(),
                });
        } else {
            engine.state_mut().cancel_turn("turn-Ticket child").unwrap();
            let Command::AnswerChildQuestions {
                interaction_id,
                answers,
                ..
            } = &command
            else {
                unreachable!()
            };
            assert!(
                engine
                    .state_mut()
                    .resolve_interaction(
                        interaction_id,
                        &InteractionResolution::AnswerQuestions {
                            answers: answers.clone()
                        }
                    )
                    .is_err()
            );
            engine
                .state_mut()
                .handle_backend(BackendEvent::TurnCompleted {
                    turn_id: "turn-Ticket child".to_owned(),
                    outcome: crate::backend::TurnOutcome::Interrupted,
                    error: None,
                });
        }
        assert!(engine.state().questions.is_empty());
        let endpoint = harness.handle.endpoint().clone();
        let task = tokio::spawn(harness.runtime.run());
        let parent = pending(&endpoint, &harness.parent).await;
        let projected_child = parent
            .children
            .iter()
            .find(|item| &item.child_session_id == child)
            .unwrap();
        if disconnect {
            assert_eq!(
                projected_child.availability,
                nakode_protocol::ChildQuestionAvailability::Unavailable
            );
        }
        assert!(projected_child.interactions.is_empty());
        let original = endpoint
            .execute_query(
                ClientId::from("child"),
                Query::GetSession {
                    session_id: child.clone(),
                },
            )
            .await
            .unwrap();
        let QueryResult::Session(original) = original.value else {
            panic!("session")
        };
        assert!(original.interactions.is_empty());
        assert!(
            endpoint
                .execute_command(
                    ClientId::from("parent"),
                    IdempotencyKey::from("stale"),
                    None,
                    false,
                    command
                )
                .await
                .is_err()
        );
        assert!(
            harness
                .commands
                .iter_mut()
                .all(|commands| commands.try_recv().is_err())
        );
        assert_eq!(
            parent
                .children
                .iter()
                .filter(|item| !item.interactions.is_empty())
                .count(),
            1
        );
        harness.handle.shutdown().await;
        task.await.unwrap();
    }
}
