//! A transcript-changes watch over real gRPC: the runtime sends only changed entries after its first
//! snapshot, and the SDK still yields complete session states.
use super::{
    NakodeClient, Path, RpcLayer, ServerEndpoint, ServerRequest, StreamExt, TestUnixServer,
    UnixListener, UnixListenerStream, protocol, session_view,
};

fn entry(id: &str, body: &str) -> protocol::TranscriptEntryView {
    protocol::TranscriptEntryView {
        body_sha256: String::new(),
        id: protocol::EntryId::from(id),
        kind: protocol::TranscriptEntryKind::Assistant,
        title: String::new(),
        body: body.to_owned(),
        body_start_byte: 0,
        body_total_bytes: body.len() as u64,
        status: protocol::TranscriptEntryStatus::Complete,
        artifacts: Vec::new(),
        created_at_ms: None,
        provider_id: None,
        model_id: None,
        owner_turn_id: None,
        resolved_reasoning_effort: None,
        resolved_fast_mode: None,
        source_transport: None,
        source_prompt_id: None,
        tool_audit_json: None,
        coordination_json: None,
        parent_tool_entry_id: None,
    }
}

/// The runtime's answer to a changes read since `since`, scripted over three generations.
fn scripted_changes(since: u64) -> protocol::SessionChangesView {
    let (generation, entries, changes) = match since {
        0 => (1, vec![entry("a", "one"), entry("b", "two")], None),
        1 => (
            2,
            vec![entry("b", "two more")],
            Some(protocol::TranscriptChanges { entry_ids: None }),
        ),
        2 => (
            3,
            vec![entry("c", "three")],
            Some(protocol::TranscriptChanges {
                entry_ids: Some(vec![
                    protocol::EntryId::from("b"),
                    protocol::EntryId::from("c"),
                ]),
            }),
        ),
        other => panic!("unexpected changes read since {other}"),
    };
    let mut session = session_view("session-1");
    session.revision = generation;
    session.transcript.entries = entries;
    protocol::SessionChangesView {
        generation,
        changes,
        session,
    }
}

/// Serves scripted changes reads, and returns the endpoint so the test can publish.
fn spawn_changes_server(path: &Path) -> (TestUnixServer, ServerEndpoint) {
    let listener = UnixListener::bind(path).expect("bind changes fixture");
    let (endpoint, mut requests) = ServerEndpoint::channel_with_build_revision(
        "transcript-changes-test",
        None,
        protocol::ServiceCapabilities::default(),
        8,
    );
    let actor_endpoint = endpoint.clone();
    let actor = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let ServerRequest::Subscribe {
                changes_since,
                respond,
                ..
            } = request
            else {
                panic!("the changes fixture only serves watches");
            };
            let since = changes_since.expect("an opted-in watch reads changes");
            let _ = respond.send(Ok(protocol::Snapshot {
                cursor: actor_endpoint.cursor(),
                value: protocol::SubscriptionView::SessionChanges(Box::new(scripted_changes(
                    since,
                ))),
            }));
        }
    });
    let served = endpoint.clone();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(RpcLayer(false))
            .add_service(nakode_server::grpc::GrpcService::new(served).into_server())
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .expect("serve changes fixture");
    });
    let server = TestUnixServer {
        shutdown: Some(shutdown),
        server,
        actor: Some(actor),
    };
    (server, endpoint)
}

fn bodies(state: &super::api::SessionState) -> Vec<(String, String)> {
    state
        .transcript
        .as_ref()
        .expect("transcript")
        .entries
        .iter()
        .map(|entry| (entry.id.clone(), entry.body.clone()))
        .collect()
}

fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(id, body)| ((*id).to_owned(), (*body).to_owned()))
        .collect()
}

#[tokio::test]
async fn a_changes_watch_yields_complete_states() {
    std::fs::create_dir_all("../../.tmp").expect("checkout-root scratch");
    let directory = tempfile::tempdir_in("../../.tmp").expect("isolated transport directory");
    let socket = Path::new("../../.tmp")
        .join(directory.path().file_name().expect("scratch name"))
        .join("changes.sock");
    let (server, endpoint) = spawn_changes_server(&socket);
    let client = NakodeClient::connect_unix(&socket)
        .await
        .expect("connect SDK");
    let mut watch = client.watch_session_changes("session-1");
    let publish = || {
        endpoint
            .publish(
                vec![protocol::SubscriptionScope::Session {
                    session_id: protocol::SessionId::from("session-1"),
                }],
                protocol::ViewEvent::SessionRemoved {
                    session_id: protocol::SessionId::from("session-1"),
                },
            )
            .expect("publish");
    };

    let first = watch.next().await.expect("first").expect("first state");
    assert_eq!(bodies(&first), pairs(&[("a", "one"), ("b", "two")]));

    publish();
    let changed = watch.next().await.expect("second").expect("second state");
    assert_eq!(bodies(&changed), pairs(&[("a", "one"), ("b", "two more")]));

    publish();
    let reordered = watch.next().await.expect("third").expect("third state");
    assert_eq!(
        bodies(&reordered),
        pairs(&[("b", "two more"), ("c", "three")])
    );
    assert_eq!(reordered.revision, 3);

    drop(watch);
    drop(server);
}
