use super::*;

#[tokio::test]
async fn snapshot_waits_for_publication_without_suppressing_legacy_events() {
    let state = AppState::new_unconfigured("/synthetic/session-sync", None, 5_000);
    let id = SessionId::from(state.nakode_session_id.clone());
    let mut core = ServerCore::new(ServiceEngine::new(state), vec![], vec![]);
    let (endpoint, _requests) = ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
    let mut legacy = endpoint.subscribe_publications();
    core.engine_for_mut(&id)
        .expect("engine")
        .state_mut()
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", "seed");
    let error = core
        .session_sync_snapshot(&id)
        .expect_err("unpublished snapshot cannot establish replay");
    assert_eq!(error.code, nakode_protocol::ErrorCode::Conflict);
    assert!(!core.session_histories.observes(&id));
    core.commit_and_publish_session_delta(&endpoint, &id);
    assert!(
        legacy.try_recv().is_ok(),
        "snapshot must not consume the legacy diff baseline"
    );
    let baseline = core.session_sync_snapshot(&id).expect("published baseline");
    core.engine_for_mut(&id)
        .expect("engine")
        .state_mut()
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", " next");
    core.commit_and_publish_session_delta(&endpoint, &id);
    assert_eq!(
        batches(
            core.session_sync_changes(&baseline.cursor)
                .expect("continuous replay")
        )
        .1
        .len(),
        1
    );
    core.published_sessions.remove(&id);
    assert!(
        core.session_sync_snapshot(&id).is_err(),
        "missing baseline also waits"
    );
}

#[tokio::test]
async fn long_history_is_not_included_in_stream_change_batches() {
    let mut state = AppState::new_unconfigured("/synthetic/session-sync", None, 5_000);
    for i in 0..512 {
        let key = format!("history-{i}");
        state
            .transcript
            .append_delta(&key, EntryKind::Assistant, "Nakode", &"old ".repeat(1024));
        state.transcript.set_status(&key, EntryStatus::Complete);
    }
    state
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", "seed");
    let id = SessionId::from(state.nakode_session_id.clone());
    let mut core = ServerCore::new(ServiceEngine::new(state), vec![], vec![]);
    let (endpoint, _requests) = ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
    let snapshot = core.session_sync_snapshot(&id).expect("bounded baseline");
    core.engine_for_mut(&id)
        .expect("engine")
        .state_mut()
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", " next");
    core.commit_and_publish_session_delta(&endpoint, &id);
    let (_, replay) = batches(
        core.session_sync_changes(&snapshot.cursor)
            .expect("changes"),
    );
    assert_eq!(replay.len(), 1);
    assert!(replay[0].events.iter().any(
        |event| matches!(event, ViewEvent::TranscriptEntryDelta { delta, .. } if delta == " next")
    ));
    assert!(
        replay[0].events.iter().all(|event| match event {
            ViewEvent::TranscriptEntryDelta { delta, .. } => delta == " next",
            // A core publication may also update bounded status/configuration notices.
            ViewEvent::SessionMetadataChanged { .. } => true,
            _ => false,
        }),
        "unexpected history hydration: {:?}",
        replay[0].events
    );
    assert!(
        core.session_histories.sessions[&id].batches[0]
            .encoded
            .len()
            < 4096
    );
    assert_eq!(
        core.engine_for(&id)
            .expect("engine")
            .state()
            .transcript
            .entries()
            .len(),
        513,
        "canonical history remains present"
    );
}
