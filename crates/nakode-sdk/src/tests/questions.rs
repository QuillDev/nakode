use super::*;

fn pending(provider: &str, grouped: bool) -> protocol::SessionView {
    let mut state = session_view(provider);
    state.selected_provider_id = Some(protocol::ProviderId::from(provider));
    let options = vec![protocol::InteractionOptionView {
        id: "local".to_owned(),
        label: "Leave committed locally".to_owned(),
        description: Some("Do not retry publication".to_owned()),
        recommended: true,
    }];
    state.interactions = vec![protocol::InteractionView {
        id: protocol::InteractionId::from(format!("{provider}-interaction")),
        revision: state.revision,
        kind: protocol::InteractionKind::Question,
        status: protocol::InteractionStatus::Pending,
        title: "Publication".to_owned(),
        detail: "How should publication proceed?".to_owned(),
        options: options.clone(),
        multiple: false,
        questions: if grouped {
            vec![
                protocol::InteractionQuestionView {
                    id: "publication".to_owned(),
                    title: "Publication".to_owned(),
                    detail: "How should publication proceed?".to_owned(),
                    options,
                    multiple: false,
                },
                protocol::InteractionQuestionView {
                    id: "constraints".to_owned(),
                    title: "Constraints".to_owned(),
                    detail: "Which constraints apply?".to_owned(),
                    options: Vec::new(),
                    multiple: true,
                },
            ]
        } else {
            Vec::new()
        },
    }];
    state
}

#[tokio::test]
async fn active_inventory_preserves_canonical_questions_through_public_sdk() {
    let (client, mut requests, shutdown, server) =
        parent_creation::server(Some(protocol::ServiceCapability::QuestionTextAnswers)).await;
    let states = vec![pending("openai-codex", true), pending("anthropic", false)];
    let expected = states.clone();
    let actor = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let ServerRequest::Query { query, respond, .. } = request else {
                panic!("discovery must not mutate or restore a session");
            };
            let value = match query {
                protocol::Query::ListActiveSessions { .. } => {
                    protocol::QueryResult::ActiveSessions(states.clone())
                }
                protocol::Query::GetSession { session_id } => {
                    protocol::QueryResult::Session(Box::new(
                        states
                            .iter()
                            .find(|state| state.id == session_id)
                            .unwrap()
                            .clone(),
                    ))
                }
                other => panic!("unexpected discovery query: {other:?}"),
            };
            respond
                .send(Ok(protocol::Snapshot {
                    cursor: protocol::Cursor {
                        server_epoch: protocol::ServerEpoch::from("question-test"),
                        sequence: 1,
                    },
                    value,
                }))
                .unwrap();
        }
    });
    let inventory = client.list_active_sessions("workspace-a").await.unwrap();
    assert_eq!(inventory.len(), expected.len());
    for (overview, original) in inventory.iter().zip(&expected) {
        let detail = client.get_session(original.id.as_str()).await.unwrap();
        assert_eq!(overview.interactions, detail.interactions);
        assert_eq!(
            overview.interactions[0].id,
            original.interactions[0].id.as_str()
        );
        assert_eq!(
            overview.interactions[0].detail,
            "How should publication proceed?"
        );
        assert_eq!(
            overview.interactions[0].options[0].label,
            "Leave committed locally"
        );
        assert!(overview.interactions[0].options[0].recommended);
        assert!(overview.transcript.is_none());
        if original.interactions[0].questions.is_empty() {
            assert!(overview.interactions[0].questions.is_empty());
        } else {
            assert_eq!(overview.interactions[0].questions.len(), 2);
            assert_eq!(overview.interactions[0].questions[0].id, "publication");
            assert!(overview.interactions[0].questions[1].multiple);
        }
    }
    shutdown.send(()).unwrap();
    server.await.unwrap();
    actor.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn question_answers_preserve_identity_text_and_stale_errors_through_public_sdk() {
    let (client, mut requests, shutdown, server) =
        parent_creation::server(Some(protocol::ServiceCapability::QuestionTextAnswers)).await;
    let actor = tokio::spawn(async move {
        for accepted in [true, false] {
            let Some(ServerRequest::Command {
                command,
                expected_revision,
                respond,
                ..
            }) = requests.recv().await
            else {
                panic!("expected question resolution");
            };
            assert_eq!(expected_revision, Some(7));
            let protocol::Command::ResolveInteraction {
                interaction_id,
                resolution,
            } = command
            else {
                panic!("answers must use the original resolver, never an ordinary prompt");
            };
            assert_eq!(interaction_id.as_str(), "exact-interaction");
            let protocol::InteractionResolution::AnswerQuestions { answers } = resolution else {
                panic!("expected structured answers");
            };
            assert_eq!(answers[0].question_id, "publication");
            assert_eq!(answers[0].option_ids, ["local"]);
            assert_eq!(answers[1].question_id, "constraints");
            assert_eq!(answers[1].text.as_deref(), Some("Do not retry publication"));
            let result = if accepted {
                Ok(protocol::CommandAccepted {
                    resource_id: None,
                    revision: Some(8),
                    effective_session_tools: None,
                    bridge_continuation: None,
                    replayed_bridge_continuation: None,
                    replayed_bridge_source_active: None,
                })
            } else {
                Err(protocol::ServiceError {
                    code: protocol::ErrorCode::Conflict,
                    message: "interaction is no longer pending".to_owned(),
                    retryable: false,
                })
            };
            respond.send(result).unwrap();
        }
    });
    let answers = vec![
        api::InteractionAnswer {
            question_id: "publication".to_owned(),
            option_ids: vec!["local".to_owned()],
            text: None,
        },
        api::InteractionAnswer {
            question_id: "constraints".to_owned(),
            option_ids: Vec::new(),
            text: Some("Do not retry publication".to_owned()),
        },
    ];
    client
        .resolve_interaction_answers("exact-interaction", answers.clone(), Some(7))
        .await
        .unwrap();
    let error = client
        .resolve_interaction_answers("exact-interaction", answers, Some(7))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SdkError::Status(status) if status.code() == tonic::Code::Aborted && status.message().contains("no longer pending"))
    );
    actor.await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
