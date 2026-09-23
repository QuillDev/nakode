use super::*;
use crate::session::{SessionRepository, SqliteSessionRepository};

struct Fixture {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    session: SessionId,
}
impl Fixture {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
        std::fs::create_dir_all(&root).unwrap();
        let directory = tempfile::tempdir_in(root).unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let record = sessions
            .create("codex", "native", "/workspace", "Agent", None)
            .unwrap();
        Self {
            _directory: directory,
            path,
            session: SessionId::from(record.id),
        }
    }
    fn store(&self) -> InboxStore {
        InboxStore::open(&self.path).unwrap()
    }
    fn enqueue(&self, id: &str, text: &str) {
        self.store()
            .execute(
                &input(&self.session, id, text),
                id,
                "owner-client",
                false,
                1234,
            )
            .unwrap();
    }
}
fn input(session: &SessionId, id: &str, text: &str) -> Command {
    Command::EnqueueFollowup {
        session_id: session.clone(),
        message_id: id.to_owned(),
        prompt: PromptInput {
            text: text.to_owned(),
            attachments: Vec::new(),
        },
    }
}

#[test]
fn metadata_pages_do_not_decode_retained_attachment_payloads() {
    let f = Fixture::new();
    f.enqueue("m1", "Original display text");
    // Corrupt only the provider payload to prove metadata reads are independent.
    f.store()
        .0
        .execute("UPDATE followup_messages SET prompt_json = 'invalid'", [])
        .unwrap();
    let view = f.store().list(&f.session, 0, 32).unwrap();
    assert_eq!(view.items[0].text, "Original display text");
    assert!(view.items[0].attachment_labels.is_empty());
    assert!(f.store().claim(&f.session).is_err());
}

#[test]
fn batch_text_bound_includes_exact_escaped_attribution_headers() {
    let f = Fixture::new();
    for index in 0..32 {
        let id = format!("{index}{}", "\u{0001}".repeat(190));
        let command = input(&f.session, &id, &"x".repeat(3_400));
        f.store()
            .execute(&command, &id, &"\u{0002}".repeat(190), false, 1234)
            .unwrap();
    }
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    assert!(batch.prompt.text.len() <= 128 * 1024);
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert!(view.pending_count > 0);
    assert_eq!(view.items.len(), 32);
}

#[test]
fn burst_claim_is_one_ordered_cutoff_and_later_arrivals_remain_pending() {
    let f = Fixture::new();
    for index in 0..12 {
        f.enqueue(&format!("m{index}"), &format!("Requirement {index}"));
    }
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    assert!(
        batch.prompt.text.find("Requirement 2\n").unwrap()
            < batch.prompt.text.find("Requirement 11\n").unwrap()
    );
    f.enqueue("later", "Arrived after cutoff");
    let recovered = f.store().claim(&f.session).unwrap().unwrap();
    assert_eq!(batch.id, recovered.id);
    assert_eq!(batch.prompt, recovered.prompt);
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.pending_count, 1);
    assert_eq!(
        view.items
            .iter()
            .filter(|item| item.state == "claimed")
            .count(),
        12
    );
    assert_eq!(view.items[0].received_at_ms, 1234);
    assert_eq!(view.items[0].submitted_by, "owner-client");
    assert!(
        view.items
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    f.store().fence_dispatch(&f.session, &batch.id).unwrap();
    assert!(f.store().fence_dispatch(&f.session, &batch.id).is_err());
    assert!(f.store().claim(&f.session).unwrap().is_none());
    f.store().acknowledge(&f.session, &batch.id).unwrap();
    let next = f.store().claim(&f.session).unwrap().unwrap();
    assert_ne!(next.id, batch.id);
    assert!(next.prompt.text.contains("Arrived after cutoff"));
    assert!(!next.prompt.text.contains("Requirement 0"));
}

#[test]
fn restart_never_replays_uncertain_dispatch_and_ack_requires_exact_correlation() {
    let f = Fixture::new();
    f.enqueue("m1", "Do once");
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    f.store().fence_dispatch(&f.session, &batch.id).unwrap();
    f.store().acknowledge(&f.session, "foreign-turn").unwrap();
    assert!(f.store().claim(&f.session).unwrap().is_none());
    assert!(
        f.store()
            .list(&f.session, 0, 64)
            .unwrap()
            .blocked_reason
            .is_some()
    );
    f.store()
        .observe_accepted(&f.session, &batch.id, "provider-turn")
        .unwrap();
    assert_eq!(
        f.store().list(&f.session, 0, 64).unwrap().items[0].state,
        "dispatching"
    );
    f.store()
        .acknowledge(&SessionId::from("other"), "provider-turn")
        .unwrap();
    f.store().acknowledge(&f.session, "provider-turn").unwrap();
    f.store().acknowledge(&f.session, "provider-turn").unwrap();
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.items.len(), 1);
    assert_eq!(view.items[0].state, "consumed");
    assert!(view.unsettled_batch_id.is_none());
    assert!(f.store().claim(&f.session).unwrap().is_none());
}

#[test]
fn concurrent_producers_and_claimers_share_one_durable_batch() {
    let f = Fixture::new();
    std::thread::scope(|scope| {
        for index in 0..16 {
            let f = &f;
            scope.spawn(move || {
                f.enqueue(&format!("m{index}"), "Distinct message with repeated text");
            });
        }
    });
    let ids = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| f.store().claim(&f.session).unwrap().unwrap().id))
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(ids.iter().all(|id| id == &ids[0]));
    let page = f.store().list(&f.session, 0, 7).unwrap();
    assert!(page.has_more);
    assert_eq!(page.items.len(), 7);
    let tail = f
        .store()
        .list(&f.session, page.items.last().unwrap().sequence, 64)
        .unwrap();
    assert_eq!(tail.items.len(), 9);
    assert!(!tail.has_more);
}

#[test]
fn durable_receipts_preserve_identical_messages_and_reject_conflicting_replays() {
    let f = Fixture::new();
    let command = input(&f.session, "id", "Original");
    f.store()
        .execute(&command, "key", "sender", false, 1)
        .unwrap();
    assert_eq!(
        f.store()
            .execute(&command, "key", "reconnected-client", true, 2)
            .unwrap(),
        Some("id".to_owned())
    );
    assert!(
        f.store()
            .execute(
                &input(&f.session, "id", "Different"),
                "key",
                "sender",
                false,
                2
            )
            .is_err()
    );
    assert!(
        f.store()
            .execute(&command, "absent", "sender", true, 2)
            .is_err()
    );
    f.enqueue("id2", "Original");
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.items.len(), 2);
    assert_eq!(view.items[0].received_at_ms, 1);
    assert_eq!(view.items[0].submitted_by, "sender");
}

#[test]
fn bounded_fifo_batches_do_not_drop_or_skip_oversized_requirements() {
    let f = Fixture::new();
    let text = "a".repeat(MAX_TEXT_BYTES);
    f.enqueue("first", &text);
    f.enqueue("second", &text);
    f.enqueue("third", "Small later message");
    assert!(
        f.store()
            .execute(
                &input(&f.session, "too-large", &"x".repeat(MAX_TEXT_BYTES + 1)),
                "large",
                "sender",
                false,
                0
            )
            .is_err()
    );
    let first = f.store().claim(&f.session).unwrap().unwrap();
    assert!(first.prompt.text.len() < 128 * 1024);
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.pending_count, 2);
    assert_eq!(view.items.len(), 3);
    assert_eq!(view.items[1].text, text);
    f.store().fence_dispatch(&f.session, &first.id).unwrap();
    f.store().acknowledge(&f.session, &first.id).unwrap();
    let next = f.store().claim(&f.session).unwrap().unwrap();
    assert!(next.prompt.text.contains("Small later message"));
    assert_eq!(f.store().list(&f.session, 0, 64).unwrap().pending_count, 0);
}

#[test]
fn attachment_bytes_and_labels_survive_restart_and_batch_assembly() {
    let f = Fixture::new();
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 2)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let attachment = PromptAttachment::InlineImage {
        label: "Exact image".to_owned(),
        media_type: "image/png".to_owned(),
        data: bytes.into_inner(),
    };
    let command = Command::EnqueueFollowup {
        session_id: f.session.clone(),
        message_id: "image".to_owned(),
        prompt: PromptInput {
            text: String::new(),
            attachments: vec![attachment.clone()],
        },
    };
    f.store()
        .execute(&command, "image-key", "sender", false, 99)
        .unwrap();
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    assert_eq!(batch.prompt.attachments, vec![attachment]);
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.items[0].attachment_labels, vec!["Exact image"]);
    assert_eq!(view.items[0].text, "");
    assert_eq!(
        f.store().claim(&f.session).unwrap().unwrap().prompt,
        batch.prompt
    );
}

#[test]
fn original_request_receipt_replays_without_accessing_expired_artifacts() {
    let f = Fixture::new();
    let command = input(&f.session, "artifact-message", "Original input");
    let request = InboxRequest {
        command: &command,
        key: "artifact-request",
        sender: "owner",
        replay_only: false,
        now_ms: 42,
    };
    f.store()
        .execute_materialized(request, |_| {
            Ok(PromptInput {
                text: "Frozen canonical payload".to_owned(),
                attachments: Vec::new(),
            })
        })
        .unwrap();
    let replay = f
        .store()
        .execute_materialized(request, |_| {
            panic!("receipt replay must not read volatile artifacts")
        })
        .unwrap();
    assert_eq!(replay.as_deref(), Some("artifact-message"));
    let new_key_replay = f
        .store()
        .execute_materialized(
            InboxRequest {
                key: "new-transport-key",
                sender: "other-client",
                now_ms: 9999,
                ..request
            },
            |_| panic!("same message must not rematerialize an expired artifact"),
        )
        .unwrap();
    assert_eq!(new_key_replay, replay);
    let changed = input(
        &f.session,
        "artifact-message",
        "Changed original requirement",
    );
    assert!(
        f.store()
            .execute_materialized(
                InboxRequest {
                    command: &changed,
                    key: "changed-key",
                    ..request
                },
                |_| panic!("a message identity conflict must refuse before materialization"),
            )
            .is_err()
    );
    let view = f.store().list(&f.session, 0, 64).unwrap();
    assert_eq!(view.items.len(), 1);
    assert_eq!(view.items[0].submitted_by, request.sender);
    assert_eq!(view.items[0].received_at_ms, request.now_ms);
    assert_eq!(
        f.store().list(&f.session, 0, 64).unwrap().items[0].text,
        "Frozen canonical payload"
    );
}

#[test]
fn inbox_capacity_refuses_explicitly_and_retains_every_prior_message() {
    let f = Fixture::new();
    for index in 0..256 {
        f.enqueue(&format!("item-{index}"), "Preserve me");
    }
    let error = f
        .store()
        .execute(
            &input(&f.session, "overflow", "Do not silently lose this"),
            "overflow",
            "owner",
            false,
            0,
        )
        .unwrap_err();
    assert!(error.message.contains("inbox is full"));
    assert_eq!(
        f.store().list(&f.session, 0, 64).unwrap().pending_count,
        256
    );
    assert_eq!(f.store().list(&f.session, 0, 64).unwrap().items.len(), 64);
}

#[test]
fn stop_pause_retains_inputs_and_resume_does_not_reset_uncertain_delivery() {
    let f = Fixture::new();
    f.enqueue("m1", "Retain me");
    f.store().pause(&f.session).unwrap();
    assert!(f.store().claim(&f.session).unwrap().is_none());
    let resume = Command::SetFollowupPaused {
        session_id: f.session.clone(),
        paused: false,
    };
    f.store()
        .execute(&resume, "resume", "owner", false, 0)
        .unwrap();
    let batch = f.store().claim(&f.session).unwrap().unwrap();
    f.store().fence_dispatch(&f.session, &batch.id).unwrap();
    f.store().pause(&f.session).unwrap();
    f.store()
        .execute(&resume, "resume-again", "owner", false, 0)
        .unwrap();
    assert!(f.store().claim(&f.session).unwrap().is_none());
    assert_eq!(
        f.store().list(&f.session, 0, 64).unwrap().items[0].text,
        "Retain me"
    );
    assert!(f.store().list(&SessionId::from("missing"), 0, 1).is_err());
    assert!(f.store().list(&f.session, 0, 65).is_err());
    assert!(f.store().list(&f.session, u64::MAX, 1).is_err());
}
