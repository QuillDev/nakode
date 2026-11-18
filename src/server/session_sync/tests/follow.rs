//! A real gRPC follower consuming a real publication, including all work up to SDK delivery.
use super::*;
use futures_util::StreamExt;
use nakode_protocol::ServiceCapability;
use nakode_sdk::NakodeClient;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

#[tokio::test]
async fn one_live_entry_costs_no_older_page_or_body_reads() {
    for length in [16, 128, 512, 2048, 4096] {
        Box::pin(measure(length)).await;
    }
}

fn fixture(length: usize) -> AppState {
    let mut state = AppState::new_unconfigured("/synthetic/session-sync", None, 5_000);
    for i in 1..length {
        let key = format!("history-{i}");
        state.transcript.upsert(
            key,
            EntryKind::Assistant,
            "history",
            "old ".repeat(64),
            EntryStatus::Complete,
        );
    }
    state
        .transcript
        .append_delta("stream", EntryKind::Assistant, "answer", "seed");
    state
}

async fn measure(length: usize) {
    let state = fixture(length);
    let id = SessionId::from(state.nakode_session_id.clone());
    let core = Arc::new(Mutex::new(ServerCore::new(
        ServiceEngine::new(state),
        vec![],
        vec![],
    )));
    let (endpoint, mut requests) = ServerEndpoint::channel(
        "follow-test",
        ServiceCapabilities {
            supported: [ServiceCapability::SessionSyncV1].into_iter().collect(),
        },
        8,
    );
    let queries = Arc::new(AtomicUsize::new(0));
    let actor = tokio::spawn({
        let core = core.clone();
        let endpoint = endpoint.clone();
        let queries = queries.clone();
        async move {
            while let Some(request) = requests.recv().await {
                if matches!(request, nakode_server::ServerRequest::Query { .. }) {
                    queries.fetch_add(1, Ordering::Relaxed);
                }
                core.lock().unwrap().handle(&endpoint, request).respond();
            }
        }
    });
    std::fs::create_dir_all(".tmp").unwrap();
    let dir = tempfile::tempdir_in(".tmp").unwrap();
    let path = std::path::Path::new(".tmp")
        .join(dir.path().file_name().unwrap())
        .join("follow.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let service = nakode_server::grpc::GrpcService::new(endpoint.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let client = NakodeClient::connect_unix(path).await.unwrap();
    let mut follow = client.follow_session_versioned(id.to_string());
    let seed = follow.next().await.unwrap().unwrap();
    let (publish, delivered) =
        timed_updates(&core, &endpoint, &id, &mut follow, &client, &queries).await;
    let wire_bytes = replay_bytes(endpoint.clone(), &id, seed.cursor).await;
    println!(
        "sync length={length} wire_bytes={wire_bytes} median_publish_us={publish} median_sdk_delivery_us={delivered} hydration_queries=0"
    );
    if length == 512 {
        check_historical_correction(
            &client,
            &id,
            &core,
            &endpoint,
            &mut follow,
            seed.state.transcript.as_ref().unwrap(),
        )
        .await;
    }
    actor.abort();
    let error = tokio::time::timeout(Duration::from_secs(2), follow.next())
        .await
        .expect("idle follow must notice actor exit")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error,nakode_sdk::SdkError::Status(status) if status.code()==tonic::Code::Unavailable)
    );
    server.abort();
}

async fn replay_bytes(
    endpoint: ServerEndpoint,
    id: &SessionId,
    after: Option<nakode_sdk::v1::SessionSyncCursor>,
) -> usize {
    use nakode_sdk::v1::{
        FollowSessionRequest, nakode_service_server::NakodeService, session_sync_update::Update,
    };
    use prost::Message;
    let service = nakode_server::grpc::GrpcService::new(endpoint);
    let mut stream = service
        .follow_session(tonic::Request::new(FollowSessionRequest {
            session_id: id.to_string(),
            after,
        }))
        .await
        .unwrap()
        .into_inner();
    let mut sizes = Vec::new();
    for _ in 0..9 {
        let update = stream.next().await.unwrap().unwrap();
        sizes.push(update.encoded_len());
        let Some(Update::Batch(batch)) = update.update else {
            panic!("should replay complete retained batches")
        };
        assert_eq!(
            batch.projection.unwrap().transcript.unwrap().entries.len(),
            1
        );
    }
    sizes.sort_unstable();
    sizes[4]
}

async fn check_historical_correction(
    client: &NakodeClient,
    id: &SessionId,
    core: &Mutex<ServerCore>,
    endpoint: &ServerEndpoint,
    follow: &mut nakode_sdk::Watch<nakode_sdk::FollowedSession>,
    before: &nakode_sdk::v1::TranscriptPage,
) {
    let old = client
        .previous_session_page(id.as_str(), before, 256)
        .await
        .unwrap()
        .unwrap();
    {
        let mut core = core.lock().unwrap();
        let target = core
            .engine()
            .state()
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.id == old.entries[0].id)
            .unwrap()
            .key
            .clone()
            .unwrap();
        core.engine_mut().state_mut().transcript.replace_body(
            &target,
            "historical correction",
            EntryStatus::Complete,
        );
        core.commit_and_publish_session_delta(endpoint, id);
    }
    let update = follow.next().await.unwrap().unwrap();
    assert_eq!(
        update.state,
        client.get_session(id.to_string()).await.unwrap()
    );
    let page = update.state.transcript.unwrap();
    assert_ne!(old.prefix_through, page.prefix_before);
    assert!(
        client
            .previous_session_page(id.as_str(), before, 256)
            .await
            .is_err()
    );
    let corrected = client
        .previous_session_page(id.as_str(), &page, 256)
        .await
        .unwrap()
        .unwrap();
    assert!(
        corrected
            .entries
            .iter()
            .any(|e| e.body == "historical correction")
    );
}

async fn timed_updates(
    core: &Mutex<ServerCore>,
    endpoint: &ServerEndpoint,
    id: &SessionId,
    follow: &mut nakode_sdk::Watch<nakode_sdk::FollowedSession>,
    client: &NakodeClient,
    queries: &AtomicUsize,
) -> (u128, u128) {
    let mut samples = Vec::new();
    for _ in 0..9 {
        let start = Instant::now();
        {
            let mut core = core.lock().unwrap();
            core.engine_mut().state_mut().transcript.append_delta(
                "stream",
                EntryKind::Assistant,
                "answer",
                "π",
            );
            core.commit_and_publish_session_delta(endpoint, id);
        }
        let publication = start.elapsed();
        let observed = tokio::time::timeout(Duration::from_secs(5), follow.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        samples.push((publication.as_micros(), start.elapsed().as_micros()));
        assert_eq!(
            queries.load(Ordering::Relaxed),
            0,
            "no historical hydration queries"
        );
        assert_eq!(
            observed.state,
            client.get_session(id.to_string()).await.unwrap(),
            "reduction equals fresh canonical view"
        );
        queries.store(0, Ordering::Relaxed);
    }
    let mut publish = samples.iter().map(|s| s.0).collect::<Vec<_>>();
    publish.sort_unstable();
    let mut delivered = samples.iter().map(|s| s.1).collect::<Vec<_>>();
    delivered.sort_unstable();
    (publish[4], delivered[4])
}
