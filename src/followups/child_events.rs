//! Runtime-owned admission of durable child evidence. Native runs have no session-child link and
//! cannot enter this path. Admission and the report receipt commit together in the inbox database.
use super::{InboxRequest, InboxStore, Result, admission, failure};
use nakode_protocol::{Command, PromptInput, SessionId};
use rusqlite::{TransactionBehavior, params};
use sha2::{Digest, Sha256};

struct ChildEvent {
    sequence: i64,
    parent: String,
    child: String,
    report: String,
    state: String,
    body: String,
    created: i64,
    title: String,
}

impl ChildEvent {
    fn prompt(&self) -> PromptInput {
        // Serialize untrusted fields as data. Batch composition adds a separate trusted
        // origin marker; neither report prose nor a client-supplied sender grants authority.
        let mut evidence = serde_json::json!({
            "parent_session_id": self.parent,
            "child_session_id": self.child,
            "report_id": self.report,
            "state": self.state,
            "body": self.body,
        });
        let mut text = evidence.to_string();
        if text.len() > super::MAX_TEXT_BYTES {
            // Escaping control characters can enlarge a valid report. Retain the original in
            // report history and notify by identity instead of poisoning the FIFO head.
            evidence["body"] = serde_json::Value::Null;
            evidence["body_retained_in_report_history"] = serde_json::Value::Bool(true);
            text = evidence.to_string();
        }
        PromptInput {
            text,
            attachments: Vec::new(),
        }
    }
}

impl InboxStore {
    /// One bounded page per actor tick. Closed parents retain reports without activation. A full
    /// inbox rolls back admission, so the next tick retries evidence, never an inference effect.
    pub(crate) fn admit_child_events(&mut self) -> Result<bool> {
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let events = {
            let mut statement = tx.prepare(
                "SELECT r.sequence, l.parent_id, r.child_id, r.report_id, r.state, r.body, r.created_at_ms, l.child_title
                 FROM session_child_reports r
                 JOIN session_child_links l ON l.child_id = r.child_id
                 JOIN sessions p ON p.id = l.parent_id
                 JOIN sessions c ON c.id = l.child_id
                 LEFT JOIN session_skill_profiles pp ON pp.session_id = p.id
                 LEFT JOIN session_skill_profiles cp ON cp.session_id = c.id
                 WHERE r.state IN ('completed', 'failed', 'blocker', 'question')
                   AND NOT EXISTS (SELECT 1 FROM child_followup_deliveries d WHERE d.report_sequence = r.sequence)
                   AND (SELECT COUNT(*) FROM followup_messages m LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
                        WHERE m.session_id = p.id AND (b.state IS NULL OR b.state <> 'consumed')) < 256
                   AND (SELECT COALESCE(SUM(m.payload_bytes), 0) FROM followup_messages m
                        LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
                        WHERE m.session_id = p.id AND (b.state IS NULL OR b.state <> 'consumed')) <= ?1
                   AND NOT EXISTS (SELECT 1 FROM session_bridges b WHERE b.session_id = p.id AND b.lifecycle <> 'open')
                   AND ((cp.profile_id IS NOT NULL AND cp.profile_id = pp.profile_id)
                     OR (cp.profile_id IS NULL AND pp.profile_id IS NULL AND c.workspace = p.workspace))
                 ORDER BY r.sequence LIMIT 64"
            ).map_err(failure)?;
            statement
                .query_map(
                    [
                        i64::try_from(super::MAX_PENDING_BYTES - super::MAX_TEXT_BYTES)
                            .expect("fixed inbox bounds"),
                    ],
                    |row| {
                        Ok(ChildEvent {
                            sequence: row.get(0)?,
                            parent: row.get(1)?,
                            child: row.get(2)?,
                            report: row.get(3)?,
                            state: row.get(4)?,
                            body: row.get(5)?,
                            created: row.get(6)?,
                            title: row.get(7)?,
                        })
                    },
                )
                .map_err(failure)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(failure)?
        };
        let mut admitted = false;
        for event in events {
            let message = format!("nakode-child-event:{}", event.sequence);
            let prompt = event.prompt();
            let command = Command::EnqueueFollowup {
                session_id: SessionId::from(event.parent.clone()),
                message_id: message.clone(),
                prompt: prompt.clone(),
            };
            let digest = Sha256::digest(prompt.text.as_bytes());
            let request = InboxRequest {
                command: &command,
                key: &message,
                sender: "nakode:durable-child",
                replay_only: false,
                now_ms: event.created,
            };
            // Capacity failure for one parent must not starve unrelated parents.
            match admission::admit_message(&tx, &event.parent, &message, &prompt, request, &digest)
            {
                Ok(()) => {}
                Err(error) if error.code == nakode_protocol::ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
            super::coordination::save_source(
                &tx,
                &event.parent,
                &message,
                &super::coordination::Source {
                    kind: "durable_child_evidence".to_owned(),
                    session_id: event.child.clone(),
                    title: event.title.chars().take(120).collect(),
                    call_id: None,
                    status: Some(event.state.clone()),
                },
            )?;
            let display = if event.report.starts_with("turn:") {
                crate::child_reports::completion_display(&event.body)
            } else {
                event.body.clone()
            };
            tx.execute("UPDATE followup_messages SET display_text = ?3 WHERE session_id = ?1 AND message_id = ?2", params![event.parent, message, display]).map_err(failure)?;
            tx.execute(
                "INSERT INTO child_followup_deliveries(report_sequence, message_sequence)
                 SELECT ?1, sequence FROM followup_messages WHERE session_id = ?2 AND message_id = ?3",
                params![event.sequence, event.parent, message],
            )
            .map_err(failure)?;
            admitted = true;
        }
        tx.commit().map_err(failure)?;
        Ok(admitted)
    }
}

/// Revalidate the persisted relationship before claiming a batch, not just at admission. Unknown
/// ownership holds evidence for explicit recovery rather than dispatching under an old binding.
pub(super) fn authorized_child_messages(
    connection: &rusqlite::Connection,
    parent: &str,
) -> Result<bool> {
    connection.query_row(
        "SELECT NOT EXISTS (
           SELECT 1 FROM followup_messages m
           JOIN child_followup_deliveries d ON d.message_sequence = m.sequence
           LEFT JOIN session_child_reports r ON r.sequence = d.report_sequence
           LEFT JOIN session_child_links l ON l.child_id = r.child_id AND l.parent_id = m.session_id
           LEFT JOIN sessions p ON p.id = l.parent_id
           LEFT JOIN sessions c ON c.id = l.child_id
           LEFT JOIN session_skill_profiles pp ON pp.session_id = p.id
           LEFT JOIN session_skill_profiles cp ON cp.session_id = c.id
           WHERE m.session_id = ?1 AND (m.batch_id IS NULL OR m.batch_id IN (
             SELECT batch_id FROM followup_batches WHERE state <> 'consumed'))
           AND (l.parent_id IS NULL OR NOT (
             (cp.profile_id IS NOT NULL AND pp.profile_id IS NOT NULL AND cp.profile_id = pp.profile_id)
             OR (cp.profile_id IS NULL AND pp.profile_id IS NULL AND c.workspace = p.workspace)))
         )", params![parent], |row| row.get(0),
    ).map_err(failure)
}
