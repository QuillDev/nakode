//! Synthetic transport benchmark: count real SDK/gRPC hydration work, not production latency.
use super::{
    Arc, AtomicUsize, Duration, NakodeClient, Ordering, Path, RpcLayer, ServerEndpoint,
    ServerRequest, TestUnixServer, UnixListener, UnixListenerStream, api, protocol,
};

fn entry(index: usize) -> protocol::TranscriptEntryView {
    protocol::TranscriptEntryView {
        id: protocol::EntryId::from(format!("entry-{index}")),
        kind: protocol::TranscriptEntryKind::Assistant,
        title: String::new(),
        body: "tail".to_owned(),
        body_start_byte: 4,
        body_total_bytes: 8,
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
        parent_tool_entry_id: None,
    }
}

fn spawn_history_server(
    path: &Path,
    pages: Arc<AtomicUsize>,
    bodies: Arc<AtomicUsize>,
) -> TestUnixServer {
    let listener = UnixListener::bind(path).expect("bind isolated history fixture");
    let (endpoint, mut requests) = ServerEndpoint::channel_with_build_revision(
        "hydration-cost-test",
        None,
        protocol::ServiceCapabilities::default(),
        8,
    );
    let actor_endpoint = endpoint.clone();
    let actor = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let ServerRequest::Query { query, respond, .. } = request else {
                panic!("hydration fixture must never receive a command");
            };
            // Explicit artificial service cost; elapsed numbers are not production measurements.
            tokio::time::sleep(Duration::from_millis(1)).await;
            let result = match query {
                protocol::Query::GetTranscriptPage { before, limit, .. } => {
                    pages.fetch_add(1, Ordering::SeqCst);
                    let end: usize = before
                        .expect("page cursor")
                        .as_str()
                        .strip_prefix("entry-")
                        .expect("fixture identity")
                        .parse()
                        .expect("index");
                    let start = end.saturating_sub((limit as usize).min(64));
                    protocol::QueryResult::Transcript(Box::new(protocol::TranscriptPage {
                        entries: (start..end).map(entry).collect(),
                        has_earlier: start > 0,
                        stream_active: false,
                        stream_label: String::new(),
                        current_owner_entry: None,
                        current_owner_omitted_tool_calls: 0,
                    }))
                }
                protocol::Query::GetTranscriptBodyWindow {
                    entry_id,
                    before_byte,
                    ..
                } => {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(before_byte, Some(4));
                    protocol::QueryResult::TranscriptBody(protocol::TranscriptBodyWindow {
                        entry_id,
                        body: "head".to_owned(),
                        start_byte: 0,
                        total_bytes: 8,
                        has_earlier: false,
                    })
                }
                other => panic!("unexpected hydration query: {other:?}"),
            };
            let _ = respond.send(Ok(protocol::Snapshot {
                cursor: actor_endpoint.cursor(),
                value: result,
            }));
        }
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(RpcLayer(false))
            .add_service(nakode_server::grpc::GrpcService::new(endpoint).into_server())
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .expect("serve isolated hydration fixture");
    });
    TestUnixServer {
        shutdown: Some(shutdown),
        server,
        actor: Some(actor),
    }
}

#[tokio::test]
async fn repeated_full_history_hydration_cost_scales_with_history() {
    std::fs::create_dir_all("../../.tmp").expect("checkout-root scratch");
    let directory = tempfile::tempdir_in("../../.tmp").expect("isolated transport directory");
    let socket = Path::new("../../.tmp")
        .join(directory.path().file_name().expect("scratch name"))
        .join("cost.sock");
    let pages = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(AtomicUsize::new(0));
    let server = spawn_history_server(&socket, pages.clone(), bodies.clone());
    let client = NakodeClient::connect_unix(&socket)
        .await
        .expect("connect SDK");
    for count in [16, 128, 512] {
        let snapshot = api::SessionState {
            id: "history-session".to_owned(),
            revision: 1,
            transcript: Some(api::TranscriptPage {
                entries: vec![api::TranscriptEntry {
                    id: format!("entry-{}", count - 1),
                    kind: api::TranscriptEntryKind::Assistant as i32,
                    status: api::TranscriptEntryStatus::Complete as i32,
                    body: "tail".to_owned(),
                    body_start_byte: 4,
                    body_total_bytes: 8,
                    ..Default::default()
                }],
                has_earlier: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        for pass in 1..=2 {
            let before_pages = pages.load(Ordering::SeqCst);
            let before_bodies = bodies.load(Ordering::SeqCst);
            let started = std::time::Instant::now();
            let hydrated = client
                .hydrate_session_with_refresh(snapshot.clone(), usize::MAX)
                .await
                .expect("hydrate full history");
            let elapsed = started.elapsed();
            let page_calls = pages.load(Ordering::SeqCst) - before_pages;
            let body_calls = bodies.load(Ordering::SeqCst) - before_bodies;
            let transcript = hydrated.state.transcript.expect("hydrated transcript");
            assert_eq!(transcript.entries.len(), count);
            assert!(!transcript.has_earlier);
            assert!(
                transcript
                    .entries
                    .iter()
                    .all(|entry| entry.body == "headtail")
            );
            assert_eq!(body_calls, count);
            assert_eq!(page_calls, (count - 1).div_ceil(64));
            eprintln!(
                "synthetic hydration: entries={count} pass={pass} page_rpcs={page_calls} body_rpcs={body_calls} elapsed_ms={} injected_service_ms_per_rpc=1",
                elapsed.as_millis()
            );
        }
    }
    server.stop().await;
}
