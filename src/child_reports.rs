//! Durable cross-session evidence. This store deliberately does not dispatch inference or treat
//! a report as an owner prompt. Automatic continuation requires a separate origin-aware queue.
use std::{path::Path, time::Duration};

use nakode_protocol::{ChildReport, ChildReportPage, Command, ErrorCode, ServiceError};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

pub mod completion;
pub(crate) mod relationships;
pub(crate) use completion::display as completion_display;

pub(crate) struct ReportStore(Connection);

type Result<T> = std::result::Result<T, ServiceError>;

fn failure(error: &rusqlite::Error) -> ServiceError {
    ServiceError {
        code: ErrorCode::Internal,
        message: format!("child report persistence: {error}"),
        retryable: true,
    }
}

fn refuse(message: &str) -> ServiceError {
    ServiceError {
        code: ErrorCode::Conflict,
        message: message.to_owned(),
        retryable: false,
    }
}

impl ReportStore {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(|error| failure(&error))?;
        connection
            .busy_timeout(Duration::from_secs(2))
            .map_err(|error| failure(&error))?;
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|error| failure(&error))?;
        Ok(Self(connection))
    }

    /// Questions are evidence about the existing child interaction, never an answer or approval.
    /// Native run events cannot call this with a linked logical child identity.
    pub(crate) fn record_question(
        &self,
        child: &str,
        question: &crate::backend::QuestionRequest,
        now_ms: i64,
    ) -> Result<()> {
        let report_id = format!("question:{:x}", Sha256::digest(question.id.as_bytes()));
        let pending: bool = self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1)
             AND NOT EXISTS(SELECT 1 FROM session_child_reports WHERE child_id = ?1 AND report_id = ?2)",
            params![child, report_id], |row| row.get(0),
        ).map_err(|error| failure(&error))?;
        if !pending {
            return Ok(());
        }
        let body = serde_json::json!({
            "schema_version": 1,
            "question_id": question.id,
            "interaction_id": crate::state::projection::question_interaction_id(child, &question.group_id),
            "group_id": question.group_id,
            "order": question.order,
            "question": crate::state::projection::interaction_question(question),
            "status_at_observation": "pending",
            "detail": "Question evidence, not owner consent or an answer. Revalidate the original child interaction before resolving it; other questions may belong to the same group."
        }).to_string();
        if body.len() > 16 * 1024 {
            return Err(refuse(
                "structured child question exceeds 16384-byte report limit; question remains pending",
            ));
        }
        let inserted = self
            .0
            .execute(
                "INSERT INTO session_child_reports(child_id, report_id, state, body, created_at_ms)
             SELECT child_id, ?2, 'question', ?3, ?4 FROM session_child_links WHERE child_id = ?1
               AND (SELECT COUNT(*) FROM session_child_reports WHERE child_id = ?1) < 4096
             ON CONFLICT(child_id, report_id) DO NOTHING",
                params![child, report_id, body, now_ms],
            )
            .map_err(|error| failure(&error))?;
        if inserted == 0 {
            let withheld: bool = self.0.query_row(
                "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1)
                 AND NOT EXISTS(SELECT 1 FROM session_child_reports WHERE child_id = ?1 AND report_id = ?2)",
                params![child, report_id], |row| row.get(0),
            ).map_err(|error| failure(&error))?;
            if withheld {
                return Err(refuse(
                    "child report retention limit reached; question remains pending",
                ));
            }
        }
        Ok(())
    }

    /// Commit the report-domain mutation and its retry receipt together. Revision fencing is not
    /// supported for this append-only domain; callers must not silently lose an explicit fence.
    pub(crate) fn execute(
        &mut self,
        command: &Command,
        key: &str,
        replay_only: bool,
        expected_revision: Option<u64>,
        now_ms: i64,
    ) -> Result<()> {
        if key.is_empty() || key.len() > 200 {
            return Err(refuse("report command key must be 1–200 bytes"));
        }
        let encoded = serde_json::to_vec(command).map_err(|_| refuse("invalid report command"))?;
        let digest = Sha256::digest(encoded).to_vec();
        let tx = self
            .0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| failure(&error))?;
        let saved: Option<Vec<u8>> = tx
            .query_row(
                "SELECT digest FROM session_child_command_receipts WHERE command_key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| failure(&error))?;
        if let Some(saved) = saved {
            return if saved == digest {
                Ok(())
            } else {
                Err(refuse(
                    "idempotency key already used for a different report command",
                ))
            };
        }
        if replay_only {
            return Err(refuse(
                "report command receipt is unavailable; no mutation was executed",
            ));
        }
        if expected_revision.is_some() {
            return Err(ServiceError {
                code: ErrorCode::InvalidRequest,
                message: "report commands do not support revision fences".to_owned(),
                retryable: false,
            });
        }
        let child = match command {
            Command::LinkChildSession {
                parent_session_id,
                child_session_id,
            } => {
                Self::link_in_transaction(
                    &tx,
                    parent_session_id.as_str(),
                    child_session_id.as_str(),
                )?;
                child_session_id.as_str()
            }
            Command::PublishChildReport {
                child_session_id,
                report_id,
                state,
                body,
            } => {
                Self::publish_in_transaction(
                    &tx,
                    child_session_id.as_str(),
                    report_id,
                    state,
                    body,
                    now_ms,
                )?;
                child_session_id.as_str()
            }
            _ => return Err(refuse("not a report command")),
        };
        tx.execute("INSERT INTO session_child_command_receipts(command_key, child_id, digest) VALUES (?1, ?2, ?3)", params![key, child, digest]).map_err(|error| failure(&error))?;
        tx.commit().map_err(|error| failure(&error))
    }

    /// Immutable one-level relationships prevent notification loops by construction. Both sessions
    /// must exist in this runtime under the same bound profile. Unbound legacy sessions additionally
    /// require the same workspace; provider credential account IDs never establish ownership.
    pub(crate) fn link_in_transaction(
        tx: &Transaction<'_>,
        parent: &str,
        child: &str,
    ) -> Result<()> {
        if parent == child {
            return Err(refuse("a session cannot parent itself"));
        }
        let closed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_bridges WHERE session_id IN (?1, ?2) AND lifecycle <> 'open')",
            params![parent, child], |row| row.get(0),
        ).map_err(|error| failure(&error))?;
        if closed {
            return Err(refuse(
                "closed sessions cannot acquire parent relationships",
            ));
        }
        let title: Option<String> = tx
            .query_row(
                "SELECT c.title FROM sessions c JOIN sessions p ON p.id = ?1
             LEFT JOIN session_skill_profiles cp ON cp.session_id = c.id
             LEFT JOIN session_skill_profiles pp ON pp.session_id = p.id
             WHERE c.id = ?2 AND (
               (cp.profile_id IS NOT NULL AND cp.profile_id = pp.profile_id)
               OR (cp.profile_id IS NULL AND pp.profile_id IS NULL AND c.workspace = p.workspace)
             )",
                params![parent, child],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| failure(&error))?;
        let title = title.ok_or_else(|| {
            refuse("parent and child must exist on this runtime under the same profile; unbound sessions require the same workspace")
        })?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT parent_id FROM session_child_links WHERE child_id = ?1",
                [child],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| failure(&error))?;
        if let Some(existing) = existing {
            return if existing == parent {
                Ok(())
            } else {
                Err(refuse("child already has a different parent"))
            };
        }
        let nested: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1 OR parent_id = ?2)", params![parent, child], |row| row.get(0)).map_err(|error| failure(&error))?;
        if nested {
            return Err(refuse("nested logical-session parents are not supported"));
        }
        let count: u32 = tx
            .query_row(
                "SELECT count(*) FROM session_child_links WHERE parent_id = ?1",
                [parent],
                |row| row.get(0),
            )
            .map_err(|error| failure(&error))?;
        if count >= 32 {
            return Err(refuse("parent already has the maximum 32 linked children"));
        }
        tx.execute(
            "INSERT INTO session_child_links(child_id, parent_id, child_title) VALUES (?1, ?2, ?3)",
            params![child, parent, title],
        )
        .map_err(|error| failure(&error))?;
        Ok(())
    }

    fn publish_in_transaction(
        tx: &Transaction<'_>,
        child: &str,
        report_id: &str,
        state: &str,
        body: &str,
        now_ms: i64,
    ) -> Result<()> {
        if report_id.is_empty()
            || report_id.starts_with("turn:")
            || report_id.starts_with("question:")
            || report_id.len() > 200
            || body.is_empty()
            || body.len() > 16 * 1024
            || !matches!(
                state,
                "progress" | "blocker" | "question" | "completed" | "failed" | "cancelled"
            )
        {
            return Err(ServiceError { code: ErrorCode::InvalidRequest, message: "report requires an id (1–200 bytes), body (1–16384 bytes), and a supported state".to_owned(), retryable: false });
        }
        let linked: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1)",
                [child],
                |row| row.get(0),
            )
            .map_err(|error| failure(&error))?;
        if !linked {
            return Err(refuse("child has no existing parent link"));
        }
        let saved: Option<(String, String)> = tx.query_row("SELECT state, body FROM session_child_reports WHERE child_id = ?1 AND report_id = ?2", params![child, report_id], |row| Ok((row.get(0)?, row.get(1)?))).optional().map_err(|error| failure(&error))?;
        if let Some((saved_state, saved_body)) = saved {
            return if saved_state == state && saved_body == body {
                Ok(())
            } else {
                Err(refuse("report id was already used with different content"))
            };
        }
        let count: u32 = tx
            .query_row(
                "SELECT count(*) FROM session_child_reports WHERE child_id = ?1",
                [child],
                |row| row.get(0),
            )
            .map_err(|error| failure(&error))?;
        if count >= 4096 {
            return Err(refuse(
                "child report retention limit reached; existing evidence is preserved",
            ));
        }
        tx.execute("INSERT INTO session_child_reports(child_id, report_id, state, body, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)", params![child, report_id, state, body, now_ms]).map_err(|error| failure(&error))?;
        Ok(())
    }

    #[cfg(test)]
    fn link(&mut self, parent: &str, child: &str) -> Result<()> {
        let command = Command::LinkChildSession {
            parent_session_id: parent.into(),
            child_session_id: child.into(),
        };
        self.execute(&command, &format!("link:{parent}:{child}"), false, None, 0)
    }

    #[cfg(test)]
    fn publish(
        &mut self,
        child: &str,
        report_id: &str,
        state: &str,
        body: &str,
        now_ms: i64,
    ) -> Result<()> {
        let command = Command::PublishChildReport {
            child_session_id: child.into(),
            report_id: report_id.to_owned(),
            state: state.to_owned(),
            body: body.to_owned(),
        };
        self.execute(
            &command,
            &format!("report:{child}:{report_id}"),
            false,
            None,
            now_ms,
        )
    }

    /// Read the durable association without opening children. Current ownership and lifecycle are
    /// rechecked here rather than trusting an old parent-side projection or a caller's child ID.
    pub(crate) fn question_children(&self, parent: &str) -> Result<Vec<(String, String, bool)>> {
        let parent_open: Option<bool> = self.0.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM session_bridges WHERE session_id = s.id AND lifecycle <> 'open') FROM sessions s WHERE s.id = ?1",
            [parent], |row| row.get(0),
        ).optional().map_err(|error| failure(&error))?;
        match parent_open {
            Some(true) => {}
            Some(false) => return Err(refuse("parent session is closed")),
            None => {
                return Err(ServiceError {
                    code: ErrorCode::NotFound,
                    message: "parent session not found".to_owned(),
                    retryable: false,
                });
            }
        }
        let mut statement = self
            .0
            .prepare(
                "SELECT c.id, c.title,
               EXISTS(SELECT 1 FROM session_bridges WHERE session_id = c.id AND lifecycle <> 'open')
             FROM session_child_links l
             JOIN sessions c ON c.id = l.child_id JOIN sessions p ON p.id = l.parent_id
             LEFT JOIN session_skill_profiles cp ON cp.session_id = c.id
             LEFT JOIN session_skill_profiles pp ON pp.session_id = p.id
             WHERE l.parent_id = ?1 AND (
               (cp.profile_id IS NOT NULL AND cp.profile_id = pp.profile_id)
               OR (cp.profile_id IS NULL AND pp.profile_id IS NULL AND c.workspace = p.workspace)
             ) ORDER BY c.id LIMIT 32",
            )
            .map_err(|error| failure(&error))?;
        statement
            .query_map([parent], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(|error| failure(&error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| failure(&error))
    }

    pub(crate) fn list(&self, parent: &str, after: u64, limit: u32) -> Result<ChildReportPage> {
        let exists: bool = self
            .0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
                [parent],
                |row| row.get(0),
            )
            .map_err(|error| failure(&error))?;
        if !exists {
            return Err(ServiceError {
                code: ErrorCode::NotFound,
                message: "parent session not found".to_owned(),
                retryable: false,
            });
        }
        let bounded_limit = limit.clamp(1, 64);
        let limit = bounded_limit as usize;
        let after =
            i64::try_from(after).map_err(|_| refuse("report cursor exceeds the sequence range"))?;
        let mut statement = self.0.prepare(
            "SELECT r.sequence, r.report_id, l.parent_id, l.child_id, l.child_title, r.state, r.body, r.created_at_ms
             FROM session_child_reports r JOIN session_child_links l ON l.child_id = r.child_id
             WHERE l.parent_id = ?1 AND r.sequence > ?2 ORDER BY r.sequence LIMIT ?3"
        ).map_err(|error| failure(&error))?;
        let mut reports = statement
            .query_map(params![parent, after, bounded_limit + 1], |row| {
                Ok(ChildReport {
                    sequence: u64::try_from(row.get::<_, i64>(0)?).unwrap_or_default(),
                    report_id: row.get(1)?,
                    parent_session_id: row.get(2)?,
                    child_session_id: row.get(3)?,
                    child_title: row.get(4)?,
                    state: row.get(5)?,
                    body: row.get(6)?,
                    created_at_ms: u64::try_from(row.get::<_, i64>(7)?).unwrap_or_default(),
                })
            })
            .map_err(|error| failure(&error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| failure(&error))?;
        let has_more = reports.len() > limit;
        reports.truncate(limit);
        Ok(ChildReportPage { reports, has_more })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{SessionRepository, SqliteSessionRepository};

    #[test]
    fn concurrent_reports_page_in_order_and_parent_deletion_removes_delivery_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let parent = sessions.create("codex", "p", "/w", "Parent", None).unwrap();
        let child = sessions.create("codex", "c", "/w", "Child", None).unwrap();
        let mut store = ReportStore::open(&path).unwrap();
        store.link(&parent.id, &child.id).unwrap();
        std::thread::scope(|scope| {
            for index in 0..8 {
                let path = &path;
                let child = &child.id;
                scope.spawn(move || {
                    ReportStore::open(path)
                        .unwrap()
                        .publish(
                            child,
                            &format!("report-{index}"),
                            "progress",
                            "Observed progress",
                            index,
                        )
                        .unwrap();
                });
            }
        });
        let first = store.list(&parent.id, 0, 4).unwrap();
        assert_eq!(first.reports.len(), 4);
        assert!(first.has_more);
        let second = store
            .list(&parent.id, first.reports[3].sequence, 4)
            .unwrap();
        assert_eq!(second.reports.len(), 4);
        assert!(!second.has_more);
        assert!(second.reports[0].sequence > first.reports[3].sequence);
        store
            .0
            .execute("DELETE FROM sessions WHERE id = ?1", [&parent.id])
            .unwrap();
        for table in [
            "session_child_links",
            "session_child_reports",
            "session_child_command_receipts",
        ] {
            let count: i64 = store
                .0
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        }
        assert!(
            store
                .publish(&child.id, "after-delete", "completed", "No destination", 9)
                .is_err()
        );
    }

    #[test]
    fn receipts_refuse_conflicts_and_replay_only_never_creates_work() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let parent = sessions.create("codex", "p", "/w", "Parent", None).unwrap();
        let child = sessions.create("codex", "c", "/w", "Child", None).unwrap();
        let command = Command::LinkChildSession {
            parent_session_id: parent.id.clone().into(),
            child_session_id: child.id.clone().into(),
        };
        let mut store = ReportStore::open(&path).unwrap();
        assert!(store.execute(&command, "receipt", true, None, 0).is_err());
        assert!(
            store
                .execute(&command, "receipt", false, Some(1), 0)
                .is_err()
        );
        store.execute(&command, "receipt", false, None, 0).unwrap();
        drop(store);
        let mut store = ReportStore::open(&path).unwrap();
        store.execute(&command, "receipt", true, None, 0).unwrap();
        let report = Command::PublishChildReport {
            child_session_id: child.id.clone().into(),
            report_id: "progress".into(),
            state: "progress".into(),
            body: "Inert evidence".into(),
        };
        assert!(store.execute(&report, "receipt", false, None, 0).is_err());
        assert!(store.list(&parent.id, 0, 64).unwrap().reports.is_empty());
        assert_eq!(
            store.list("missing", 0, 64).unwrap_err().code,
            ErrorCode::NotFound
        );
        assert!(
            store
                .publish(&child.id, "turn:spoof", "completed", "fake", 0)
                .is_err()
        );
    }

    #[test]
    fn terminal_reports_commit_atomically_and_replay_once() {
        use crate::backend::{ModelOptions, TurnOutcome};
        use crate::session::PersistedTurnConfiguration;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let parent = sessions.create("codex", "p", "/w", "Parent", None).unwrap();
        let child = sessions.create("codex", "c", "/w", "Child", None).unwrap();
        let mut store = ReportStore::open(&path).unwrap();
        store.link(&parent.id, &child.id).unwrap();
        let turn = PersistedTurnConfiguration {
            completion: None,
            id: "terminal".into(),
            model: None,
            options: ModelOptions::default(),
            outcome: TurnOutcome::Interrupted,
        };
        sessions.update_last_turn(&child.id, &turn).unwrap();
        sessions.update_last_turn(&child.id, &turn).unwrap();
        let page = store.list(&parent.id, 0, 64).unwrap();
        assert_eq!(page.reports.len(), 1);
        assert_eq!(page.reports[0].report_id, "turn:terminal");
        assert_eq!(page.reports[0].state, "cancelled");
        store.0.execute_batch("CREATE TRIGGER refuse_child_report BEFORE INSERT ON session_child_reports BEGIN SELECT RAISE(ABORT, 'injected persistence failure'); END;").unwrap();
        let failed_turn = PersistedTurnConfiguration {
            completion: None,
            id: "must-rollback".into(),
            ..turn.clone()
        };
        assert!(sessions.update_last_turn(&child.id, &failed_turn).is_err());
        assert_eq!(
            sessions
                .find(&child.id)
                .unwrap()
                .unwrap()
                .last_turn
                .unwrap()
                .id,
            turn.id
        );
        assert_eq!(store.list(&parent.id, 0, 64).unwrap().reports.len(), 1);
    }

    #[test]
    fn reports_survive_restart_deduplicate_and_keep_attribution() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let parent = sessions
            .create("codex", "parent-native", "/workspace", "Orchestrator", None)
            .unwrap();
        let child = sessions
            .create("codex", "child-native", "/workspace", "Audit routing", None)
            .unwrap();
        let mut store = ReportStore::open(&path).unwrap();
        store.link(&parent.id, &child.id).unwrap();
        store.link(&parent.id, &child.id).unwrap();
        store
            .publish(&child.id, "turn-1", "completed", "Observed tests passed", 1)
            .unwrap();
        drop(store);
        let mut store = ReportStore::open(&path).unwrap();
        store
            .publish(&child.id, "turn-1", "completed", "Observed tests passed", 9)
            .unwrap();
        assert!(
            store
                .publish(&child.id, "turn-1", "completed", "Different", 9)
                .is_err()
        );
        let page = store.list(&parent.id, 0, 64).unwrap();
        assert_eq!(page.reports.len(), 1);
        assert_eq!(page.reports[0].child_title, "Audit routing");
        assert_eq!(page.reports[0].created_at_ms, 1);
        assert!(
            store
                .list(&parent.id, page.reports[0].sequence, 64)
                .unwrap()
                .reports
                .is_empty()
        );
    }

    #[test]
    fn links_refuse_cycles_reparenting_cross_workspace_and_cross_profile() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.db");
        let sessions = SqliteSessionRepository::open(&path).unwrap();
        let a = sessions.create("codex", "a", "/w", "a", None).unwrap();
        let b = sessions.create("codex", "b", "/w", "b", None).unwrap();
        let c = sessions.create("codex", "c", "/other", "c", None).unwrap();
        let d = sessions.create("codex", "d", "/w", "d", None).unwrap();
        let mut store = ReportStore::open(&path).unwrap();
        assert!(store.link(&a.id, &a.id).is_err());
        assert!(store.link(&a.id, &c.id).is_err());
        store.link(&a.id, &b.id).unwrap();
        assert!(store.link(&b.id, &a.id).is_err());
        assert!(store.link(&d.id, &b.id).is_err());
        assert!(store.link(&b.id, &d.id).is_err());
        sessions
            .bind_session_skill_profile(&d.id, "another-owner")
            .unwrap();
        assert!(store.link(&a.id, &d.id).is_err());
        sessions.bind_session_skill_profile(&a.id, "owner").unwrap();
        sessions.bind_session_skill_profile(&c.id, "owner").unwrap();
        store.link(&a.id, &c.id).unwrap();
        assert!(
            store
                .publish(&d.id, "x", "progress", "unlinked", 1)
                .is_err()
        );
    }
}
