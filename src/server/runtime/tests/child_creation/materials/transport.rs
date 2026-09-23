use super::*;
use nakode_sdk::materials::{MachineLocality, MaterialClient, machine_locality};
use nakode_sdk::{NakodeClient, v1 as api};

async fn drive<T>(
    runtime: &mut NativeServerRuntime,
    future: impl std::future::Future<Output = T>,
) -> T {
    tokio::pin!(future);
    loop {
        tokio::select! {
            value = &mut future => return value,
            request = runtime.requests.recv() => runtime.handle_request(request.unwrap()).await,
        }
    }
}

fn api_scope(parent: &SessionId, child: &SessionId) -> api::MaterialScope {
    api::MaterialScope {
        parent_session_id: parent.to_string(),
        source: Some(api::MaterialSource {
            session_id: child.to_string(),
            run_id: None,
        }),
    }
}

fn machine() -> api::ExecutionMachine {
    api::ExecutionMachine {
        authority: "fixture.enrollment".into(),
        id: "child-machine".into(),
    }
}

#[tokio::test]
async fn child_image_bytes_cross_authenticated_tls_and_wrong_credentials_refuse() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 1);
    let certificate =
        rcgen::generate_simple_self_signed(vec!["nakode.material.test".to_owned()]).unwrap();
    let cert_pem = certificate.cert.pem();
    let tls = crate::control_service::remote_server_tls_config(
        cert_pem.as_bytes(),
        certificate.signing_key.serialize_pem().as_bytes(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = nakode_server::grpc::GrpcService::new(h.runtime.endpoint.clone())
        .with_server_id("child-installation")
        .with_execution_routing(Some(machine()), false, true)
        .unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(service.into_authenticated_server("fixture-key"))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .unwrap();
    });
    let endpoint = format!("https://{address}");
    let bad =
        NakodeClient::connect_remote(&endpoint, &cert_pem, "nakode.material.test", "wrong-key")
            .await
            .unwrap();
    assert!(
        matches!(bad.get_server_info().await.unwrap_err(), nakode_sdk::SdkError::Status(status) if status.code() == tonic::Code::Unauthenticated)
    );
    let client =
        NakodeClient::connect_remote(&endpoint, &cert_pem, "nakode.material.test", "fixture-key")
            .await
            .unwrap();
    let route = drive(
        &mut h.runtime,
        client.get_session_routing(child.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(route.location.as_ref().unwrap().machine, Some(machine()));
    let caller = api::ExecutionLocation {
        machine: Some(api::ExecutionMachine {
            id: "operator-executor".into(),
            ..machine()
        }),
        server_id: "caller-installation".into(),
        runtime_epoch: "caller-epoch".into(),
    };
    assert_eq!(
        machine_locality(Some(&caller), route.location.as_ref()),
        MachineLocality::Remote
    );
    let materials = drive(
        &mut h.runtime,
        MaterialClient::bind(&caller, &route, None, Some(&client)),
    )
    .await
    .unwrap();
    let scope = api_scope(&h.parent, &child);
    let page = drive(&mut h.runtime, materials.list(scope.clone(), None, 1))
        .await
        .unwrap();
    let image = drive(
        &mut h.runtime,
        materials.get(scope, page.items[0].artifact_id.clone(), None),
    )
    .await
    .unwrap();
    let artifact = image.artifact.unwrap();
    assert_eq!(artifact.data, crate::image_handoff::tests::png(64, 32));
    assert_eq!(
        crate::image_handoff::dimensions(&artifact.data).unwrap(),
        (64, 32)
    );
    assert_eq!(
        image.scope.unwrap().source.unwrap().session_id,
        child.as_str()
    );
    let mut stale = route.clone();
    stale.location.as_mut().unwrap().runtime_epoch = "stale".into();
    assert!(
        drive(
            &mut h.runtime,
            MaterialClient::bind(&caller, &stale, None, Some(&client))
        )
        .await
        .is_err()
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn same_machine_material_sdk_uses_unix_service_with_no_proxy_and_no_fallback() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 1);
    let socket = h.directory.path().join("m.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let service = nakode_server::grpc::GrpcService::new(h.runtime.endpoint.clone())
        .with_server_id("child-installation")
        .with_execution_routing(Some(machine()), true, false)
        .unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .unwrap();
    });
    let client = NakodeClient::connect_unix(socket).await.unwrap();
    let route = drive(
        &mut h.runtime,
        client.get_session_routing(child.to_string()),
    )
    .await
    .unwrap();
    let caller = route.location.clone().unwrap();
    assert_eq!(
        machine_locality(Some(&caller), route.location.as_ref()),
        MachineLocality::SameMachine
    );
    // There is no remote client, proxy listener, cloud credential or network discovery in this path.
    let materials = drive(
        &mut h.runtime,
        MaterialClient::bind(&caller, &route, Some(&client), None),
    )
    .await
    .unwrap();
    let scope = api_scope(&h.parent, &child);
    let page = drive(&mut h.runtime, materials.list(scope.clone(), None, 1))
        .await
        .unwrap();
    let image = drive(
        &mut h.runtime,
        materials.get(scope, page.items[0].artifact_id.clone(), None),
    )
    .await
    .unwrap();
    assert_eq!(
        image.artifact.unwrap().data,
        crate::image_handoff::tests::png(64, 32)
    );
    assert!(
        MaterialClient::bind(&caller, &route, None, Some(&client))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("no proxy fallback")
    );
    shutdown.send(()).unwrap();
    server.await.unwrap();
}

#[test]
fn locality_never_uses_installation_ids_hostnames_or_partial_discovery() {
    let mut left = api::ExecutionLocation {
        server_id: "same-installation".into(),
        runtime_epoch: "same-epoch".into(),
        machine: None,
    };
    assert_eq!(
        machine_locality(Some(&left), Some(&left)),
        MachineLocality::Unknown
    );
    left.machine = Some(machine());
    let mut right = left.clone();
    right.machine.as_mut().unwrap().authority = "different-enrollment".into();
    assert_eq!(
        machine_locality(Some(&left), Some(&right)),
        MachineLocality::Unknown
    );
    assert_eq!(
        machine_locality(Some(&left), None),
        MachineLocality::Unknown
    );
}
