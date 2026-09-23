use super::*;

fn remove(session: &SessionId, message: &str) -> Command {
    Command::RemoveFollowup {
        session_id: session.clone(),
        message_id: message.to_owned(),
    }
}

#[test]
fn removal_persists_and_preserves_admission_deduplication() {
    let f = Fixture::new();
    f.enqueue("removed", "Do not deliver");
    f.enqueue("retained", "Deliver this");
    let command = remove(&f.session, "removed");
    for key in ["remove-key", "remove-key", "second-remove-key"] {
        assert_eq!(
            f.store().execute(&command, key, "owner", false, 0).unwrap(),
            Some("removed".to_owned())
        );
    }
    // Both command replay and a new command with the same message identity stay deduplicated.
    f.enqueue("removed", "Do not deliver");
    f.store()
        .execute(
            &input(&f.session, "removed", "Do not deliver"),
            "new-admit-key",
            "owner",
            false,
            0,
        )
        .unwrap();
    assert!(
        f.store()
            .execute(
                &input(&f.session, "removed", "Changed"),
                "changed-key",
                "owner",
                false,
                0
            )
            .is_err()
    );
    let view = f.store().list(&f.session, 0, 32).unwrap();
    assert_eq!(view.pending_count, 1);
    assert_eq!(view.items.len(), 1);
    assert_eq!(view.items[0].message_id, "retained");
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    assert!(!batch.prompt.text.contains("Do not deliver"));
    assert!(batch.prompt.text.contains("Deliver this"));
}

#[test]
fn sequence_is_table_wide_and_removed_gaps_do_not_break_pages() {
    let f = Fixture::new();
    let sessions = SqliteSessionRepository::open(&f.path).unwrap();
    let other = SessionId::from(
        sessions
            .create("codex", "other-native", "/other", "Other", None)
            .unwrap()
            .id,
    );
    f.enqueue("first", "First");
    f.store()
        .execute(
            &input(&other, "other", "Other session"),
            "other-key",
            "owner",
            false,
            0,
        )
        .unwrap();
    f.enqueue("middle", "Middle");
    f.enqueue("last", "Last");
    let before = f.store().list(&f.session, 0, 1).unwrap();
    let first = before.items[0].sequence;
    assert!(before.has_more);
    let middle = f.store().list(&f.session, first, 1).unwrap();
    assert_eq!(middle.items[0].sequence, first + 2);
    let last_cursor = middle.items[0].sequence;
    f.store()
        .execute(
            &remove(&f.session, "middle"),
            "rm-middle",
            "owner",
            false,
            0,
        )
        .unwrap();
    let next = f.store().list(&f.session, first, 1).unwrap();
    assert_eq!(next.items[0].message_id, "last");
    assert!(!next.has_more);
    f.store()
        .execute(&remove(&f.session, "last"), "rm-last", "owner", false, 0)
        .unwrap();
    let empty = f.store().list(&f.session, last_cursor, 1).unwrap();
    assert!(empty.items.is_empty());
    assert!(!empty.has_more);
    assert_eq!(empty.pending_count, 1);
    assert_eq!(
        f.store().list(&other, 0, 32).unwrap().items[0].message_id,
        "other"
    );
    let error = f
        .store()
        .execute(&remove(&other, "first"), "wrong-session", "owner", false, 0)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
}

#[test]
fn claimed_dispatching_and_consumed_messages_cannot_be_recalled() {
    let f = Fixture::new();
    f.enqueue("message", "Retain exact input");
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    let command = remove(&f.session, "message");
    for state in ["claimed", "dispatching", "consumed"] {
        let error = f
            .store()
            .execute(&command, "refused-removal", "owner", false, 0)
            .unwrap_err();
        assert!(error.message.contains(state));
        assert!(error.message.contains("cannot be removed or recalled"));
        match state {
            "claimed" => f.store().fence_dispatch(&f.session, &batch.id).unwrap(),
            "dispatching" => f.store().acknowledge(&f.session, &batch.id).unwrap(),
            _ => {}
        }
    }
    assert_eq!(
        f.store().list(&f.session, 0, 32).unwrap().items[0].text,
        "Retain exact input"
    );
}

#[test]
fn competing_claim_and_remove_have_exactly_one_winner() {
    for _ in 0..16 {
        let f = Fixture::new();
        f.enqueue("message", "Race input");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let claim_barrier = barrier.clone();
        let path = f.path.clone();
        let session = f.session.clone();
        let worker = std::thread::spawn(move || {
            let mut store = InboxStore::open(&path).unwrap();
            claim_barrier.wait();
            store.claim(&session).unwrap()
        });
        let mut store = f.store();
        barrier.wait();
        let removed = store.execute(
            &remove(&f.session, "message"),
            "race-remove",
            "owner",
            false,
            0,
        );
        let claimed = worker.join().unwrap();
        assert_eq!(removed.is_ok(), claimed.is_none());
        let page = f.store().list(&f.session, 0, 32).unwrap();
        assert_eq!(page.items.is_empty(), removed.is_ok());
        assert_eq!(page.pending_count, 0);
    }
}

#[test]
fn removal_does_not_re_admit_child_evidence() {
    let f = Fixture::new();
    // The report identity remains recorded even after payload removal.
    f.enqueue("evidence", "Child evidence");
    let store = f.store();
    store
        .0
        .execute(
            "INSERT INTO child_followup_deliveries(report_sequence, message_sequence)
         SELECT 123, sequence FROM followup_messages WHERE message_id = 'evidence'",
            [],
        )
        .unwrap();
    f.store()
        .execute(
            &remove(&f.session, "evidence"),
            "remove-evidence",
            "owner",
            false,
            0,
        )
        .unwrap();
    let count: i64 = f
        .store()
        .0
        .query_row(
            "SELECT COUNT(*) FROM child_followup_deliveries",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert!(f.store().claim(&f.session).unwrap().is_none());
}

#[test]
fn removal_releases_only_its_private_image_copy() {
    let f = Fixture::new();
    let image = image::DynamicImage::new_rgba8(1, 1);
    let mut bytes = std::io::Cursor::new(Vec::new());
    image.write_to(&mut bytes, image::ImageFormat::Png).unwrap();
    let attachment = PromptAttachment::InlineImage {
        label: "Shared source image".to_owned(),
        media_type: "image/png".to_owned(),
        data: bytes.into_inner(),
    };
    for message in ["remove-image", "keep-image"] {
        let command = Command::EnqueueFollowup {
            session_id: f.session.clone(),
            message_id: message.to_owned(),
            prompt: PromptInput {
                text: String::new(),
                attachments: vec![attachment.clone()],
            },
        };
        f.store()
            .execute(&command, message, "owner", false, 0)
            .unwrap();
    }
    f.store()
        .execute(
            &remove(&f.session, "remove-image"),
            "remove-image-command",
            "owner",
            false,
            0,
        )
        .unwrap();
    let retained = f.store().claim(&f.session).unwrap().unwrap();
    assert_eq!(retained.prompt.attachments, vec![attachment]);
    let removed_payload: (String, i64) = f.store().0.query_row(
        "SELECT prompt_json, payload_bytes FROM followup_messages WHERE message_id = 'remove-image'", [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(removed_payload, ("{}".to_owned(), 0));
}
