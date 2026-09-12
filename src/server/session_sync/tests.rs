use super::*;

mod publication;
use crate::{
    domain_transcript::{EntryKind, EntryStatus},
    service::ServiceEngine,
    state::AppState,
};
use nakode_protocol::{EntryId, ServiceCapabilities, TranscriptEntryStatus};
use nakode_server::ServerEndpoint;

fn delta(session_id: &SessionId, revision: u64, text: &str) -> ViewEvent {
    ViewEvent::TranscriptEntryDelta {
        session_id: session_id.clone(),
        revision,
        entry_id: EntryId::from("stream"),
        append_at_byte: 4,
        delta: text.to_owned(),
        status: TranscriptEntryStatus::Running,
    }
}

fn batches(result: SessionChanges) -> (SessionSyncCursor, Vec<SessionChangeBatch>) {
    let SessionChanges::Changes { cursor, batches } = result else {
        panic!("expected complete replay");
    };
    (cursor, batches)
}

#[test]
fn complete_batches_advance_without_freezing_streaming_entries() {
    let mut history = SessionHistories::default();
    let id = SessionId::from("session");
    let initial = history.establish(&id, 10);
    let now = Instant::now();
    let first = delta(&id, 11, "hello");
    let queue = ViewEvent::QueueChanged {
        session_id: id.clone(),
        revision: 11,
        queue: vec![],
    };
    history.record(&id, Some(10), 11, &[&first, &queue], now);
    let (cursor, first_batch) = batches(history.changes(&initial, now));
    assert_eq!(cursor.revision, 11);
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].events, vec![first, queue]);
    let mut terminal = delta(&id, 12, "");
    if let ViewEvent::TranscriptEntryDelta { status, .. } = &mut terminal {
        *status = TranscriptEntryStatus::Complete;
    }
    history.record(&id, Some(11), 12, &[&terminal], now);
    let (next, second_batch) = batches(history.changes(&cursor, now));
    assert_eq!(next.revision, 12);
    assert_eq!(second_batch[0].events, vec![terminal]);
    assert_eq!(second_batch[0].base, cursor);
    assert_eq!(second_batch[0].next, next);
    assert_eq!(batches(history.changes(&initial, now)).1.len(), 2);
    assert!(batches(history.changes(&next, now)).1.is_empty());
}

#[test]
fn incarnation_session_future_and_inside_batch_cursors_reset() {
    let mut history = SessionHistories::default();
    let id = SessionId::from("session");
    let initial = history.establish(&id, 10);
    let now = Instant::now();
    history.record(&id, Some(10), 15, &[&delta(&id, 15, "next")], now);
    for revision in [9, 11, 16] {
        let mut invalid = initial.clone();
        invalid.revision = revision;
        assert!(matches!(
            history.changes(&invalid, now),
            SessionChanges::ResetRequired
        ));
    }
    let mut other_epoch = initial.clone();
    other_epoch.incarnation = "other".to_owned();
    assert!(matches!(
        history.changes(&other_epoch, now),
        SessionChanges::ResetRequired
    ));
    let mut other_session = initial;
    other_session.session_id = SessionId::from("other");
    history.establish(&other_session.session_id, 10);
    assert!(matches!(
        history.changes(&other_session, now),
        SessionChanges::ResetRequired
    ));
}

#[test]
fn stale_duplicate_and_missing_publication_baselines_invalidate_history() {
    let id = SessionId::from("session");
    for (base, next) in [(None, 11), (Some(9), 11), (Some(10), 10), (Some(10), 9)] {
        let mut history = SessionHistories::default();
        let initial = history.establish(&id, 10);
        history.record(&id, base, next, &[&delta(&id, next, "x")], Instant::now());
        assert!(matches!(
            history.changes(&initial, Instant::now()),
            SessionChanges::ResetRequired
        ));
    }
}

#[test]
fn retention_expiry_resets_old_cursors_but_preserves_current_cursor() {
    let mut history = SessionHistories::default();
    let id = SessionId::from("session");
    let initial = history.establish(&id, 10);
    let now = Instant::now();
    history.record(&id, Some(10), 11, &[&delta(&id, 11, "x")], now);
    let (current, _) = batches(history.changes(&initial, now));
    let expired = now + history.max_age;
    assert!(matches!(
        history.changes(&initial, expired),
        SessionChanges::ResetRequired
    ));
    assert!(batches(history.changes(&current, expired)).1.is_empty());
    assert!(history.sessions[&id].batches.is_empty());
}

#[test]
fn global_byte_batch_and_session_limits_force_safe_reset() {
    for mode in 0..3 {
        let mut history = SessionHistories::default();
        match mode {
            0 => history.max_bytes = 1,
            1 => history.max_batches = 0,
            _ => history.max_sessions = 1,
        }
        let id = SessionId::from("first");
        let initial = history.establish(&id, 10);
        history.record(&id, Some(10), 11, &[&delta(&id, 11, "x")], Instant::now());
        if mode == 2 {
            history.establish(&SessionId::from("second"), 1);
        }
        assert!(matches!(
            history.changes(&initial, Instant::now()),
            SessionChanges::ResetRequired
        ));
        assert!(
            history
                .sessions
                .values()
                .flat_map(|h| &h.batches)
                .map(|b| b.encoded.len())
                .sum::<usize>()
                <= history.max_bytes
        );
    }
}

#[test]
fn baseline_reset_removal_and_resnapshot_never_reuse_an_incarnation() {
    let mut history = SessionHistories::default();
    let id = SessionId::from("session");
    let initial = history.establish(&id, 10);
    assert_eq!(initial, history.establish(&id, 10));
    let newer = history.establish(&id, 11);
    assert_ne!(newer.incarnation, initial.incarnation);
    history.remove(&id);
    let recreated = history.establish(&id, 11);
    assert_ne!(newer.incarnation, recreated.incarnation);
    assert!(matches!(
        history.changes(&initial, Instant::now()),
        SessionChanges::ResetRequired
    ));
}

#[tokio::test]
async fn authoritative_stream_and_terminal_publications_replay_after_cursor_advance() {
    let mut state = AppState::new_unconfigured("/synthetic/session-sync", None, 5_000);
    state
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", "seed");
    let id = SessionId::from(state.nakode_session_id.clone());
    let mut core = ServerCore::new(ServiceEngine::new(state), vec![], vec![]);
    let (endpoint, _requests) = ServerEndpoint::channel("test", ServiceCapabilities::default(), 1);
    let initial = core.session_sync_snapshot(&id).expect("snapshot");
    let entry_id = initial
        .session
        .transcript
        .entries
        .last()
        .expect("entry")
        .id
        .clone();
    core.engine_for_mut(&id)
        .expect("engine")
        .state_mut()
        .transcript
        .append_delta("stream", EntryKind::Assistant, "Nakode", " more");
    core.commit_and_publish_session_delta(&endpoint, &id);
    let (cursor, first) = batches(core.session_sync_changes(&initial.cursor).expect("changes"));
    assert!(first.iter().flat_map(|b| &b.events).any(|event| matches!(event,
        ViewEvent::TranscriptEntryDelta { entry_id: changed, delta, .. } if changed == &entry_id && delta == " more")));
    core.engine_for_mut(&id)
        .expect("engine")
        .state_mut()
        .transcript
        .set_status("stream", EntryStatus::Complete);
    core.commit_and_publish_session_delta(&endpoint, &id);
    let (terminal_cursor, terminal) = batches(
        core.session_sync_changes(&cursor)
            .expect("terminal changes"),
    );
    assert!(terminal_cursor.revision > cursor.revision);
    assert!(
        terminal
            .iter()
            .flat_map(|b| &b.events)
            .any(|event| match event {
                ViewEvent::TranscriptEntryPatched { entry, .. } =>
                    entry.id == entry_id && entry.status == TranscriptEntryStatus::Complete,
                ViewEvent::TranscriptEntryDelta {
                    entry_id: changed,
                    status,
                    ..
                } => changed == &entry_id && *status == TranscriptEntryStatus::Complete,
                _ => false,
            })
    );
    assert!(
        batches(
            core.session_sync_changes(&terminal_cursor)
                .expect("unchanged")
        )
        .1
        .is_empty()
    );
}
