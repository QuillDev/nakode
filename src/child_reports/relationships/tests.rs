use super::*;
use crate::{
    followups::InboxStore,
    session::{SessionRepository, SqliteSessionRepository},
};

struct Fixture {
    directory: tempfile::TempDir,
    child: String,
    old: String,
    new: String,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let ids: Vec<_> = ["child", "old", "new"]
            .into_iter()
            .map(|name| {
                let record = sessions.create("codex", name, "/w", name, None).unwrap();
                sessions
                    .bind_session_skill_profile(&record.id, "profile")
                    .unwrap();
                record.id
            })
            .collect();
        Self {
            directory,
            child: ids[0].clone(),
            old: ids[1].clone(),
            new: ids[2].clone(),
        }
    }
    fn path(&self) -> std::path::PathBuf {
        self.directory.path().join("sessions.db")
    }
    fn store(&self) -> ReportStore {
        ReportStore::open(&self.path()).unwrap()
    }
    fn command(&self, parent: &str, previous: Option<&str>, revision: u64, call: &str) -> Command {
        Command::ReparentChildSession {
            source_session_id: parent.into(),
            source_call_id: call.into(),
            child_session_id: self.child.clone().into(),
            expected_parent_session_id: previous.map(Into::into),
            expected_relationship_revision: revision,
            transfer: previous.is_some(),
        }
    }
    fn apply(&self, command: &Command, key: &str) -> Result<()> {
        self.store().reparent(command, key, false, 100, || Ok(()))
    }
    fn claim(&self) {
        self.apply(&self.command(&self.old, None, 0, "claim"), "claim")
            .unwrap();
    }
    fn parent(&self) -> (Option<String>, u64) {
        let session = SqliteSessionRepository::open(self.path())
            .unwrap()
            .find(&self.child)
            .unwrap()
            .unwrap();
        (session.parent_session_id, session.relationship_revision)
    }
    fn report(&self, id: &str) {
        self.store()
            .publish(&self.child, id, "completed", "Inert evidence", 1)
            .unwrap();
    }
}

#[test]
fn claim_transfer_receipt_restart_and_aba_preserve_session() {
    let f = Fixture::new();
    let repository = SqliteSessionRepository::open(f.path()).unwrap();
    let before = repository.find(&f.child).unwrap().unwrap();
    f.claim();
    let transfer = f.command(&f.new, Some(&f.old), 1, "transfer");
    f.apply(&transfer, "transfer").unwrap();
    assert_eq!(f.parent(), (Some(f.new.clone()), 2));
    f.store()
        .reparent(&transfer, "transfer", true, 200, || {
            panic!("receipt must not authenticate or mutate again")
        })
        .unwrap();
    f.apply(&f.command(&f.old, Some(&f.new), 2, "back"), "back")
        .unwrap();
    assert_eq!(f.parent(), (Some(f.old.clone()), 3));
    assert!(
        f.apply(&f.command(&f.new, Some(&f.old), 1, "stale"), "stale")
            .is_err()
    );
    let mut after = repository.find(&f.child).unwrap().unwrap();
    after.parent_session_id = None;
    after.relationship_revision = 0;
    assert_eq!(
        before, after,
        "identity, history, provider, workspace and timestamps unchanged"
    );
    let count: i64 = f
        .store()
        .0
        .query_row(
            "SELECT COUNT(*) FROM session_relationship_transitions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn claim_never_steals_and_source_authentication_is_required() {
    let f = Fixture::new();
    let command = f.command(&f.old, None, 0, "claim");
    assert!(
        f.store()
            .reparent(&command, "claim", false, 1, || Err(refuse("not pending")))
            .is_err()
    );
    assert_eq!(f.parent(), (None, 0));
    assert!(
        f.store()
            .reparent(&command, "claim", true, 1, || Ok(()))
            .is_err()
    );
    f.claim();
    assert!(
        f.apply(&f.command(&f.new, None, 1, "steal"), "steal")
            .is_err()
    );
    assert!(
        f.apply(&f.command(&f.new, Some(&f.old), 1, "claim"), "claim")
            .is_err()
    );
}

#[test]
fn concurrent_expected_revision_has_one_winner() {
    let f = Fixture::new();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = [&f.old, &f.new]
            .into_iter()
            .map(|parent| {
                let barrier = &barrier;
                let f = &f;
                scope.spawn(move || {
                    let mut store = f.store();
                    barrier.wait();
                    store
                        .reparent(
                            &f.command(parent, None, 0, parent),
                            parent,
                            false,
                            1,
                            || Ok(()),
                        )
                        .is_ok()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.into_iter().filter(|success| *success).count(), 1);
    assert_eq!(f.parent().1, 1);
}

#[test]
fn cross_profile_native_missing_closed_and_nested_targets_refuse() {
    let f = Fixture::new();
    f.store()
        .0
        .execute(
            "UPDATE session_skill_profiles SET profile_id = 'foreign' WHERE session_id = ?1",
            [&f.new],
        )
        .unwrap();
    assert!(
        f.apply(&f.command(&f.new, None, 0, "foreign"), "foreign")
            .is_err()
    );
    let mut native = f.command(&f.old, None, 0, "native");
    if let Command::ReparentChildSession {
        child_session_id, ..
    } = &mut native
    {
        *child_session_id = "native-run-not-a-logical-session".into();
    }
    assert!(f.apply(&native, "native").is_err());
    f.store().link(&f.child, &f.old).unwrap();
    assert!(
        f.apply(&f.command(&f.old, None, 0, "cycle"), "cycle")
            .is_err()
    );
    assert!(
        f.apply(&f.command(&f.child, None, 0, "self"), "self")
            .is_err()
    );
    let closed = Fixture::new();
    closed.store().0.execute("INSERT INTO session_bridges(session_id, workspace, kind, lifecycle, display_title, revision, updated_at_ms) VALUES (?1, '/w', 'chat', 'archived', 'Closed', 1, 1)", [&closed.child]).unwrap();
    assert!(
        closed
            .apply(&closed.command(&closed.old, None, 0, "closed"), "closed")
            .is_err()
    );
}

#[test]
fn pending_claimed_and_uncertain_reports_refuse_then_future_routes_to_new_parent() {
    let f = Fixture::new();
    f.claim();
    f.report("done");
    let mut inbox = InboxStore::open(&f.path()).unwrap();
    inbox.admit_child_events().unwrap();
    let transfer = f.command(&f.new, Some(&f.old), 1, "transfer");
    assert!(f.apply(&transfer, "transfer").is_err());
    let old = f.old.clone().into();
    let batch = inbox.claim(&old).unwrap().unwrap();
    assert!(f.apply(&transfer, "transfer").is_err());
    inbox.fence_dispatch(&old, &batch.id).unwrap();
    assert!(f.apply(&transfer, "transfer").is_err());
    inbox.acknowledge(&old, &batch.id).unwrap();
    f.apply(&transfer, "transfer").unwrap();
    assert!(
        inbox.claim(&old).unwrap().is_none(),
        "old parent not blocked or reawakened"
    );
    f.report("future");
    inbox.admit_child_events().unwrap();
    assert_eq!(inbox.list(&old, 0, 64).unwrap().items.len(), 1);
    let new = f.new.clone().into();
    assert_eq!(inbox.list(&new, 0, 64).unwrap().items.len(), 1);
    assert!(
        inbox
            .claim(&new)
            .unwrap()
            .unwrap()
            .prompt
            .text
            .contains("durable_child_evidence")
    );
    assert!(!inbox.admit_child_events().unwrap());
}

#[test]
fn pending_downward_instruction_refuses_and_explicit_withdrawal_unblocks_transfer() {
    let f = Fixture::new();
    f.claim();
    let mut inbox = InboxStore::open(&f.path()).unwrap();
    relay(&f, &mut inbox);
    let transfer = f.command(&f.new, Some(&f.old), 1, "transfer");
    assert!(f.apply(&transfer, "transfer").is_err());
    inbox
        .execute(
            &Command::RemoveFollowup {
                session_id: f.child.clone().into(),
                message_id: "relay".into(),
            },
            "withdraw",
            "owner",
            false,
            2,
        )
        .unwrap();
    f.apply(&transfer, "transfer").unwrap();
    assert!(inbox.claim(&f.child.clone().into()).unwrap().is_none());
}

#[test]
fn parent_deletion_keeps_orphan_revision_and_claim_never_accepts_original_zero() {
    let f = Fixture::new();
    f.claim();
    f.store()
        .0
        .execute("DELETE FROM sessions WHERE id = ?1", [&f.old])
        .unwrap();
    assert_eq!(f.parent(), (None, 2));
    assert!(
        f.apply(&f.command(&f.new, None, 0, "stale"), "stale")
            .is_err()
    );
    f.apply(&f.command(&f.new, None, 2, "reclaim"), "reclaim")
        .unwrap();
    assert_eq!(f.parent(), (Some(f.new.clone()), 3));
}

fn relay(f: &Fixture, inbox: &mut InboxStore) {
    let child = f.child.clone().into();
    inbox
        .execute_authenticated(
            crate::followups::InboxRequest {
                command: &Command::RelayAgentFollowup {
                    session_id: child,
                    message_id: "relay".into(),
                    source_session_id: f.old.clone().into(),
                    source_call_id: "send".into(),
                    prompt: nakode_protocol::PromptInput {
                        text: "Task".into(),
                        attachments: vec![],
                    },
                },
                key: "relay",
                sender: "host",
                replay_only: false,
                now_ms: 1,
            },
            |prompt| Ok(prompt.clone()),
            || Ok(()),
        )
        .unwrap();
}

#[test]
fn claimed_and_dispatching_downward_instruction_refuse_until_consumed() {
    let f = Fixture::new();
    f.claim();
    let mut inbox = InboxStore::open(&f.path()).unwrap();
    relay(&f, &mut inbox);
    let child = f.child.clone().into();
    let batch = inbox.claim(&child).unwrap().unwrap();
    let transfer = f.command(&f.new, Some(&f.old), 1, "transfer");
    assert!(f.apply(&transfer, "transfer").is_err());
    inbox.fence_dispatch(&child, &batch.id).unwrap();
    assert!(f.apply(&transfer, "transfer").is_err());
    inbox.acknowledge(&child, &batch.id).unwrap();
    f.apply(&transfer, "transfer").unwrap();
    assert!(inbox.claim(&child).unwrap().is_none());
    assert_eq!(f.parent(), (Some(f.new.clone()), 2));
}
