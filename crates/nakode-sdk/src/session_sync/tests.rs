use super::*;

fn cursor(revision: u64) -> api::SessionSyncCursor {
    api::SessionSyncCursor {
        session_id: "session".into(),
        incarnation: "epoch".into(),
        revision,
    }
}
fn state(revision: u64, body: &str) -> api::SessionState {
    api::SessionState {
        id: "session".into(),
        revision,
        transcript: Some(api::TranscriptPage {
            entries: vec![api::TranscriptEntry {
                id: "stream".into(),
                body: body.into(),
                body_total_bytes: body.len() as u64,
                status: api::TranscriptEntryStatus::Running.into(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}
#[test]
fn invalid_tail_does_not_publish_partial_control_or_advance_the_cursor() {
    let initial = state(1, "π");
    let mut held = Applied::seed(
        "session",
        api::SessionSyncSnapshot {
            cursor: Some(cursor(1)),
            state: Some(initial.clone()),
        },
    )
    .unwrap();
    let mut terminal = state(2, "π");
    terminal.title = "completed".into();
    terminal.transcript.as_mut().unwrap().entries[0].status =
        api::TranscriptEntryStatus::Complete.into();
    let valid = api::SessionSyncBatch {
        base: Some(cursor(1)),
        next: Some(cursor(2)),
        projection: Some(terminal.clone()),
        order: None,
    };
    let mut malformed = valid.clone();
    malformed.order = Some(api::TranscriptOrder {
        entry_ids: vec!["stream".into(), "missing".into()],
    });
    assert_eq!(held.apply(malformed), None);
    assert_eq!(held.state, initial);
    assert_eq!(held.cursor, cursor(1));
    assert_eq!(held.apply(valid.clone()), Some(true));
    assert_eq!(held.state, terminal);
    assert_eq!(held.versions["stream"], 2);
    assert_eq!(held.apply(valid), Some(false));
}
