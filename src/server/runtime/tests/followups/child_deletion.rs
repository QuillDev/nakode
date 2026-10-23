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
