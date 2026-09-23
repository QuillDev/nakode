use super::*;

pub(super) async fn server(
    capability: Option<protocol::ServiceCapability>,
) -> (
    NakodeClient,
    ServerRequests,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let capabilities = protocol::ServiceCapabilities {
        supported: capability.into_iter().collect(),
    };
    let (endpoint, requests) =
        ServerEndpoint::channel_with_build_revision("capability-test", None, capabilities, 8);
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(nakode_server::grpc::GrpcService::new(endpoint).into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (
        NakodeClient::from_channel(channel),
        requests,
        shutdown,
        server,
    )
}

#[tokio::test]
async fn parent_creation_capability_gate_refuses_before_any_mutation() {
    let (client, mut requests, shutdown, server) = server(None).await;
    let error = client
        .create_session_request(api::CreateSessionRequest {
            workspace_id: "workspace".to_owned(),
            parent_session_id: Some("parent".to_owned()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support atomic parent session creation")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn parent_creation_preserves_exact_parent_across_sdk_and_grpc() {
    let (client, mut requests, shutdown, server) =
        server(Some(protocol::ServiceCapability::ParentSessionCreation)).await;
    let request = api::CreateSessionRequest {
        workspace_id: "workspace".to_owned(),
        parent_session_id: Some("exact-parent".to_owned()),
        profile_id: Some("owner-profile".to_owned()),
        title: Some("Durable child".to_owned()),
        ..Default::default()
    };
    let create = client.create_session_request(request);
    let serve = async {
        let Some(ServerRequest::Command {
            command, respond, ..
        }) = requests.recv().await
        else {
            panic!("expected CreateSession");
        };
        let protocol::Command::CreateSession {
            parent_session_id,
            profile_id,
            title,
            ..
        } = command
        else {
            panic!("wrong command");
        };
        assert_eq!(
            parent_session_id.as_ref().map(protocol::SessionId::as_str),
            Some("exact-parent")
        );
        assert_eq!(profile_id.as_deref(), Some("owner-profile"));
        assert_eq!(title.as_deref(), Some("Durable child"));
        respond
            .send(Ok(protocol::CommandAccepted {
                resource_id: Some("durable-child".to_owned()),
                revision: Some(1),
                effective_session_tools: None,
                bridge_continuation: None,
                replayed_bridge_continuation: None,
                replayed_bridge_source_active: None,
            }))
            .unwrap();
    };
    let (result, ()) = tokio::join!(create, serve);
    assert_eq!(result.unwrap(), "durable-child");
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
