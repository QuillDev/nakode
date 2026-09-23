use super::*;

#[tokio::test]
async fn followup_capability_gate_refuses_without_mutation_or_legacy_fallback() {
    let (client, mut requests, shutdown, server) = parent_creation::server(None).await;
    let enqueue = client
        .enqueue_followup(api::EnqueueFollowupRequest {
            session_id: "session".into(),
            message_id: "message".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let pause = client
        .set_followup_paused(api::SetFollowupPausedRequest {
            session_id: "session".into(),
            paused: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    for error in [enqueue, pause] {
        assert!(
            error
                .to_string()
                .contains("does not support DurableFollowupInbox")
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn followup_sdk_preserves_caller_identity_and_original_message() {
    let (client, mut requests, shutdown, server) =
        parent_creation::server(Some(protocol::ServiceCapability::DurableFollowupInbox)).await;
    let request = api::EnqueueFollowupRequest {
        mutation: Some(api::MutationOptions {
            idempotency_key: "http-retry-key".into(),
            ..Default::default()
        }),
        session_id: "exact-session".into(),
        message_id: "exact-message".into(),
        prompt: Some(api::PromptInput {
            text: "Integrate this exact requirement".into(),
            attachments: vec![],
        }),
    };
    let send = client.enqueue_followup(request);
    let receive = async {
        let Some(ServerRequest::Command {
            command,
            idempotency_key,
            respond,
            ..
        }) = requests.recv().await
        else {
            panic!("expected EnqueueFollowup");
        };
        assert_eq!(idempotency_key.as_str(), "http-retry-key");
        let protocol::Command::EnqueueFollowup {
            session_id,
            message_id,
            prompt,
        } = command
        else {
            panic!("unexpected mutation or legacy fallback");
        };
        assert_eq!(session_id.as_str(), "exact-session");
        assert_eq!(message_id, "exact-message");
        assert_eq!(prompt.text, "Integrate this exact requirement");
        respond
            .send(Ok(protocol::CommandAccepted {
                resource_id: Some(message_id),
                revision: Some(1),
                effective_session_tools: None,
                bridge_continuation: None,
                replayed_bridge_continuation: None,
                replayed_bridge_source_active: None,
            }))
            .unwrap();
    };
    let (result, ()) = tokio::join!(send, receive);
    assert_eq!(
        result.unwrap().resource_id.as_deref(),
        Some("exact-message")
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
