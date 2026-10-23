use super::*;

fn request() -> api::OpenSessionRequest {
    api::OpenSessionRequest {
        session_id: "exact-chat".to_owned(),
        profile_id: Some("owner-profile".to_owned()),
        bridge: Some(api::SessionBridgeIntent {
            kind: api::OrchestratorKind::Chat as i32,
            lifecycle: api::BridgeLifecycle::Open as i32,
            display_title: "Hosted Chat".to_owned(),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn bridge_adoption_requires_capability_before_mutation() {
    let (client, mut requests, shutdown, server) = parent_creation::server(None).await;
    assert!(client.open_session_request(request()).await.is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn bridge_adoption_preserves_exact_profile_and_intent_through_public_transport() {
    let (client, mut requests, shutdown, server) =
        parent_creation::server(Some(protocol::ServiceCapability::AttachedSessionBridge)).await;
    let open = client.open_session_request(request());
    let serve = async {
        let Some(ServerRequest::Command {
            command, respond, ..
        }) = requests.recv().await
        else {
            panic!("expected OpenSession");
        };
        let protocol::Command::OpenSession {
            session_id,
            profile_id,
            bridge,
            tools,
            ..
        } = command
        else {
            panic!("wrong command");
        };
        assert_eq!(session_id.as_str(), "exact-chat");
        assert_eq!(profile_id.as_deref(), Some("owner-profile"));
        let bridge = bridge.unwrap();
        assert_eq!(bridge.kind, protocol::OrchestratorKind::Chat);
        assert_eq!(bridge.lifecycle, protocol::BridgeLifecycle::Open);
        assert!(tools.is_none());
        respond
            .send(Ok(protocol::CommandAccepted {
                resource_id: Some("exact-chat".to_owned()),
                revision: Some(1),
                effective_session_tools: None,
                bridge_continuation: None,
                replayed_bridge_continuation: None,
                replayed_bridge_source_active: None,
            }))
            .unwrap();
    };
    let (result, ()) = tokio::join!(open, serve);
    assert_eq!(result.unwrap().resource_id.as_deref(), Some("exact-chat"));
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
