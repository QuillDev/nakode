use super::*;
use nakode_protocol::{Query, QueryResult, ServiceCapabilities, ServiceCapability};
use nakode_server::{ServerEndpoint, ServerRequest};

struct Server {
    client: NakodeClient,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    queries: tokio::task::JoinHandle<()>,
}

async fn server(known_machine: bool, supported: bool) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let capabilities = ServiceCapabilities {
        supported: if supported {
            [
                ServiceCapability::ChildMaterials,
                ServiceCapability::ArtifactTransfer,
            ]
            .into()
        } else {
            std::collections::BTreeSet::default()
        },
    };
    let (endpoint, mut requests) =
        ServerEndpoint::channel_with_build_revision("routing-tests", None, capabilities, 8);
    let cursor = endpoint.cursor();
    let service = nakode_server::grpc::GrpcService::new(endpoint)
        .with_server_id("installation")
        .with_execution_routing(
            known_machine.then(|| api::ExecutionMachine {
                authority: "fixture".into(),
                id: "machine".into(),
            }),
            true,
            false,
        )
        .unwrap();
    let queries = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let ServerRequest::Query {
                query: Query::GetSessionRouting { session_id },
                respond,
                ..
            } = request
            else {
                panic!("material reads must refuse before reaching canonical query execution");
            };
            respond
                .send(Ok(nakode_protocol::Snapshot {
                    cursor: cursor.clone(),
                    value: QueryResult::SessionRouting(session_id),
                }))
                .unwrap();
        }
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
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
    Server {
        client: NakodeClient::from_channel(channel),
        shutdown,
        task,
        queries,
    }
}

impl Server {
    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        self.task.await.unwrap();
        self.queries.await.unwrap();
    }
}

#[tokio::test]
async fn unknown_locality_and_missing_material_capability_do_not_break_other_discovery() {
    for (known, supported, expected) in [
        (false, true, "locality is unknown"),
        (true, false, "does not support scoped child materials"),
    ] {
        let server = server(known, supported).await;
        let route = server.client.get_session_routing("child").await.unwrap();
        let caller = route.location.clone().unwrap();
        let error = MaterialClient::bind(&caller, &route, Some(&server.client), None)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(
            server.client.get_server_info().await.unwrap().server_id,
            "installation"
        );
        server.stop().await;
    }
}

#[tokio::test]
async fn stale_epoch_and_invalid_scopes_are_refused_before_material_execution() {
    let server = server(true, true).await;
    let route = server.client.get_session_routing("child").await.unwrap();
    let caller = route.location.clone().unwrap();
    let materials = MaterialClient::bind(&caller, &route, Some(&server.client), None)
        .await
        .unwrap();
    let mut scope = api::MaterialScope {
        parent_session_id: "parent".into(),
        source: Some(api::MaterialSource {
            session_id: "child".into(),
            run_id: None,
        }),
    };
    for limit in [0, 65, u32::MAX] {
        assert!(
            materials
                .list(scope.clone(), None, limit)
                .await
                .unwrap_err()
                .to_string()
                .contains("page size")
        );
    }
    let status = server
        .client
        .transport
        .clone()
        .list_child_materials(api::ListChildMaterialsRequest {
            scope: Some(scope.clone()),
            after_artifact_id: None,
            limit: 1,
            expected_runtime_epoch: "stale".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    scope.source.as_mut().unwrap().session_id = "wrong-session".into();
    assert!(
        materials
            .get(scope, "image".into(), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("selected destination")
    );
    let mut wrong = route;
    wrong.location.as_mut().unwrap().server_id = "other-installation".into();
    assert!(
        MaterialClient::bind(&caller, &wrong, Some(&server.client), None)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("destination changed")
    );
    server.stop().await;
}

#[test]
fn malformed_machine_facts_remain_unknown() {
    for id in [String::new(), " padded ".into(), "x".repeat(201)] {
        let location = api::ExecutionLocation {
            machine: Some(api::ExecutionMachine {
                authority: "fixture".into(),
                id,
            }),
            server_id: "same-installation".into(),
            runtime_epoch: "same-epoch".into(),
        };
        assert_eq!(
            machine_locality(Some(&location), Some(&location)),
            MachineLocality::Unknown
        );
    }
}
