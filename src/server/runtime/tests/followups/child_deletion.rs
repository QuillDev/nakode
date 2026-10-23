use super::*;
use nakode_protocol::{DeletionParentFence, ErrorCode};

fn create_session(h: &Harness, title: &str) -> String {
    h.runtime
        .effects
        .persistence
        .sessions
        .create(CODEX_PROVIDER, title, &h.state().workspace, title, None)
        .unwrap()
        .id
}

fn database(h: &Harness) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection.execute("PRAGMA foreign_keys = ON", []).unwrap();
    connection
}

fn link(h: &Harness, child: &str, parent: &str) {
    database(h)
        .execute(
            "INSERT INTO session_child_links (child_id, parent_id, child_title) VALUES (?1, ?2, 'child')
             ON CONFLICT(child_id) DO UPDATE SET parent_id = excluded.parent_id",
            rusqlite::params![child, parent],
        )
        .unwrap();
}

fn delete_child(child: &str, parent: Option<&str>) -> Command {
    Command::DeleteSession {
        session_id: SessionId::from(child),
        parent_fence: Some(DeletionParentFence {
            parent_session_id: parent.map(SessionId::from),
        }),
    }
}

fn exists(h: &Harness, id: &str) -> bool {
    h.runtime
        .effects
        .persistence
        .sessions
        .find(id)
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn fenced_deletion_removes_open_and_closed_linked_children() {
    for closed in [false, true] {
        let mut h = Harness::new().await;
        let parent = h.session.to_string();
        let child = create_session(&h, "child");
        link(&h, &child, &parent);
        if closed {
            database(&h).execute("INSERT INTO session_bridges (session_id, workspace, kind, lifecycle, display_title, revision, updated_at_ms) VALUES (?1, ?2, 'agent', 'archived', 'Retained child', 1, 1)", rusqlite::params![child, h.state().workspace]).unwrap();
        }
        h.runtime.refresh_catalogs();
        h.command("linked", None, false, delete_child(&child, Some(&parent)))
            .await
            .unwrap();
        assert!(!exists(&h, &child));
    }
}

#[tokio::test]
async fn fenced_deletion_refuses_other_parents_orphans_and_unknown_sessions() {
    let mut h = Harness::new().await;
    let caller = h.session.to_string();
    let other = create_session(&h, "other chat");
    let elsewhere = create_session(&h, "elsewhere");
    let orphan = create_session(&h, "orphan");
    link(&h, &elsewhere, &other);
    h.runtime.refresh_catalogs();

    for (key, target) in [("elsewhere", &elsewhere), ("orphan", &orphan)] {
        let error = h
            .command(key, None, false, delete_child(target, Some(&caller)))
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(!error.retryable);
        assert!(exists(&h, target));
    }
    // A parentless fence is for cross-runtime children; a runtime-linked child never matches it.
    assert!(
        h.command("relay-linked", None, false, delete_child(&elsewhere, None))
            .await
            .is_err()
    );
    assert!(exists(&h, &elsewhere));
    let unknown = h
        .command(
            "unknown",
            None,
            false,
            delete_child("missing-session", Some(&caller)),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown.code, ErrorCode::NotFound);

    h.command("relay-orphan", None, false, delete_child(&orphan, None))
        .await
        .unwrap();
    assert!(!exists(&h, &orphan));
}

#[tokio::test]
async fn fenced_deletion_refuses_a_reparent_committed_after_the_in_memory_check() {
    let mut h = Harness::new().await;
    let caller = h.session.to_string();
    let other = create_session(&h, "other chat");
    let child = create_session(&h, "child");
    link(&h, &child, &caller);
    h.runtime.refresh_catalogs();
    // Another connection transfers the child without refreshing this runtime's projection, so only
    // the deletion transaction can observe the new parent.
    link(&h, &child, &other);

    let error = h
        .command("raced", None, false, delete_child(&child, Some(&caller)))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(!error.retryable);
    assert!(exists(&h, &child));
}

/// The harness's default session, with a turn in flight that a stop would interrupt.
async fn running_child() -> Harness {
    let mut h = Harness::new().await;
    h.event(BackendEvent::Ready(crate::backend::BackendIdentity {
        provider: CODEX_PROVIDER.to_owned(),
        display_name: "Codex".to_owned(),
        version: None,
        capabilities: h.state().backend_capabilities.clone(),
    }))
    .await;
    h.command(
        "task",
        None,
        false,
        Command::SendPrompt {
            session_id: h.session.clone(),
            prompt: PromptInput {
                text: "Child work".to_owned(),
                attachments: Vec::new(),
            },
        },
    )
    .await
    .unwrap();
    h.start();
    h.event(BackendEvent::TurnStarted {
        turn_id: "child-turn".to_owned(),
    })
    .await;
    while h.commands.try_recv().is_ok() {}
    h
}

fn stop_child(child: &str, parent: Option<&str>) -> Command {
    Command::CancelSessionWork {
        session_id: SessionId::from(child),
        parent_fence: Some(DeletionParentFence {
            parent_session_id: parent.map(SessionId::from),
        }),
    }
}

fn interrupted(h: &mut Harness) -> bool {
    std::iter::from_fn(|| h.commands.try_recv().ok())
        .any(|command| matches!(command, BackendCommand::InterruptTurn { .. }))
}

fn held(h: &Harness, child: &str) -> bool {
    database(h)
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_deletion_holds WHERE child_id = ?1)",
            [child],
            |row| row.get(0),
        )
        .unwrap()
}

fn relink(h: &Harness, child: &str, parent: &str) -> rusqlite::Result<usize> {
    database(h).execute(
        "UPDATE session_child_links SET parent_id = ?2 WHERE child_id = ?1",
        rusqlite::params![child, parent],
    )
}

#[tokio::test]
async fn a_child_transferred_before_the_stop_is_neither_interrupted_nor_held() {
    let mut h = Box::pin(running_child()).await;
    let child = h.session.to_string();
    let caller = create_session(&h, "calling chat");
    let other = create_session(&h, "other chat");
    link(&h, &child, &caller);
    h.runtime.refresh_catalogs();
    // The transfer commits after DeleteAgent authorized the caller but before the stop arrives.
    relink(&h, &child, &other).unwrap();

    let error = h
        .command("stop", None, false, stop_child(&child, Some(&caller)))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(!error.retryable);
    assert!(!interrupted(&mut h));
    assert!(!held(&h, &child));
    assert!(exists(&h, &child));
    assert!(h.state().has_active_execution());
}

#[tokio::test]
async fn a_fenced_stop_rejected_before_any_effect_releases_its_hold() {
    let mut h = Box::pin(running_child()).await;
    let child = h.session.to_string();
    let caller = create_session(&h, "calling chat");
    let other = create_session(&h, "other chat");
    link(&h, &child, &caller);
    h.runtime.refresh_catalogs();

    let stale = h.command(
        "stale",
        Some(u64::MAX),
        false,
        stop_child(&child, Some(&caller)),
    );
    assert!(stale.await.unwrap_err().message.contains("stale"));
    let unexecuted = h.command("lost", None, true, stop_child(&child, Some(&caller)));
    assert!(
        unexecuted
            .await
            .unwrap_err()
            .message
            .contains("not executed")
    );

    assert!(!interrupted(&mut h));
    assert!(!held(&h, &child));
    assert_eq!(relink(&h, &child, &other).unwrap(), 1);
}

#[tokio::test]
async fn a_fenced_stop_holds_the_link_against_every_relink_until_it_expires() {
    let mut h = Box::pin(running_child()).await;
    let child = h.session.to_string();
    let caller = create_session(&h, "calling chat");
    let other = create_session(&h, "other chat");
    link(&h, &child, &caller);
    h.runtime.refresh_catalogs();

    h.command("stop", None, false, stop_child(&child, Some(&caller)))
        .await
        .unwrap();
    assert!(interrupted(&mut h));
    assert!(held(&h, &child));
    assert!(relink(&h, &child, &other).is_err());
    assert!(
        database(&h)
            .execute(
                "DELETE FROM session_child_links WHERE child_id = ?1",
                [&child]
            )
            .is_err()
    );

    // An interrupted caller cannot block claim or transfer forever.
    database(&h)
        .execute("UPDATE session_deletion_holds SET expires_at_ms = 0", [])
        .unwrap();
    relink(&h, &child, &other).unwrap();
}

#[tokio::test]
async fn deletion_releases_its_hold_and_rechecks_the_parent_after_an_expired_hold() {
    let mut h = Harness::new().await;
    let caller = h.session.to_string();
    let other = create_session(&h, "other chat");
    let child = create_session(&h, "child");
    let moved = create_session(&h, "moved child");
    link(&h, &child, &caller);
    link(&h, &moved, &caller);
    h.runtime.refresh_catalogs();

    h.command("linked", None, false, delete_child(&child, Some(&caller)))
        .await
        .unwrap();
    assert!(!exists(&h, &child));
    assert!(!held(&h, &child));

    // The transaction is the last fence: a relink after an expired hold still prevents deletion.
    let sessions = &h.runtime.effects.persistence.sessions;
    sessions.hold_child_deletion(&moved, Some(&caller)).unwrap();
    database(&h)
        .execute("UPDATE session_deletion_holds SET expires_at_ms = 0", [])
        .unwrap();
    relink(&h, &moved, &other).unwrap();
    assert!(matches!(
        sessions.delete_child(&moved, Some(&caller)),
        Err(crate::session::SessionError::ParentFenceMismatch)
    ));
    assert!(exists(&h, &moved));
}

#[tokio::test]
async fn a_cross_runtime_child_hold_refuses_a_claim_until_deletion() {
    let mut h = Harness::new().await;
    let claimer = h.session.to_string();
    let child = create_session(&h, "relay child");
    h.runtime.refresh_catalogs();

    h.runtime
        .effects
        .persistence
        .sessions
        .hold_child_deletion(&child, None)
        .unwrap();
    assert!(
        database(&h)
            .execute(
                "INSERT INTO session_child_links (child_id, parent_id, child_title) VALUES (?1, ?2, 'child')",
                rusqlite::params![child, claimer],
            )
            .is_err()
    );
    h.command("relay", None, false, delete_child(&child, None))
        .await
        .unwrap();
    assert!(!exists(&h, &child));
}
