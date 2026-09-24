//! Atomic adoption of existing logical sessions. No provider, workspace or turn is changed.
use super::{ReportStore, Result, failure, refuse};
use nakode_protocol::Command;
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests;

pub(crate) struct Reparent<'a> {
    pub parent: &'a str,
    pub call: &'a str,
    pub child: &'a str,
    pub previous_parent: Option<&'a str>,
    pub revision: u64,
    pub transfer: bool,
}

impl<'a> Reparent<'a> {
    pub(crate) fn from_command(command: &'a Command) -> Result<Self> {
        let Command::ReparentChildSession {
            source_session_id,
            source_call_id,
            child_session_id,
            expected_parent_session_id,
            expected_relationship_revision,
            transfer,
        } = command
        else {
            return Err(refuse("not a relationship command"));
        };
        Ok(Self {
            parent: source_session_id.as_str(),
            call: source_call_id,
            child: child_session_id.as_str(),
            previous_parent: expected_parent_session_id
                .as_ref()
                .map(nakode_protocol::SessionId::as_str),
            revision: *expected_relationship_revision,
            transfer: *transfer,
        })
    }
}

impl ReportStore {
    /// Receipt and transition share the link/inbox database's IMMEDIATE transaction. Authentication
    /// runs only for a new mutation: an exact retry is a receipt read, never a second adoption.
    pub(crate) fn reparent(
        &mut self,
        command: &Command,
        key: &str,
        replay_only: bool,
        now_ms: i64,
        authenticate: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let request = Reparent::from_command(command)?;
        let revision = i64::try_from(request.revision)
            .ok()
            .filter(|value| *value < i64::MAX)
            .ok_or_else(|| refuse("relationship revision is out of range"))?;
        if key.is_empty() || key.len() > 200 || request.call.is_empty() || request.call.len() > 200
        {
            return Err(refuse(
                "relationship command and source call keys must be 1–200 bytes",
            ));
        }
        let digest = Sha256::digest(
            serde_json::to_vec(command).map_err(|_| refuse("invalid relationship command"))?,
        )
        .to_vec();
        let tx = self
            .0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| failure(&error))?;
        authorize(&tx, &request)?;
        let saved: Option<Vec<u8>> = tx.query_row(
            "SELECT digest FROM session_relationship_transitions WHERE command_key = ?1 OR (parent_id = ?2 AND source_call_id = ?3)",
            params![key, request.parent, request.call], |row| row.get(0),
        ).optional().map_err(|error| failure(&error))?;
        if let Some(saved) = saved {
            return if saved == digest {
                Ok(())
            } else {
                Err(refuse(
                    "relationship receipt already exists with different arguments",
                ))
            };
        }
        if replay_only {
            return Err(refuse(
                "relationship receipt is unavailable; no mutation executed",
            ));
        }
        authenticate()?;
        apply(&tx, &request)?;
        tx.execute(
            "INSERT INTO session_relationship_transitions(command_key, digest, child_id, previous_parent_id, parent_id, previous_revision, revision, source_call_id, changed_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6 + 1, ?7, ?8)",
            params![key, digest, request.child, request.previous_parent, request.parent, revision, request.call, now_ms],
        ).map_err(|error| failure(&error))?;
        tx.commit().map_err(|error| failure(&error))
    }
}

fn authorize(tx: &Transaction<'_>, request: &Reparent<'_>) -> Result<()> {
    let same_owner: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions c JOIN sessions p ON p.id = ?1
         JOIN session_skill_profiles cp ON cp.session_id = c.id
         JOIN session_skill_profiles pp ON pp.session_id = p.id AND pp.profile_id = cp.profile_id
         WHERE c.id = ?2 AND c.id <> p.id
           AND NOT EXISTS(SELECT 1 FROM orchestration_runs WHERE id IN (?1, ?2))
           AND (?3 IS NULL OR EXISTS(SELECT 1 FROM session_skill_profiles op WHERE op.session_id = ?3 AND op.profile_id = cp.profile_id)))",
        params![request.parent, request.child, request.previous_parent], |row| row.get(0),
    ).map_err(|error| failure(&error))?;
    if !same_owner {
        return Err(refuse(
            "claim/transfer requires exact durable sessions on this runtime with the same bound profile; native runs and unbound sessions are not eligible",
        ));
    }
    Ok(())
}

fn apply(tx: &Transaction<'_>, request: &Reparent<'_>) -> Result<()> {
    if request.transfer != request.previous_parent.is_some()
        || request.previous_parent == Some(request.parent)
    {
        return Err(refuse(
            "claim requires an orphan; explicit transfer requires a different expected current parent",
        ));
    }
    let current: Option<String> = tx
        .query_row(
            "SELECT parent_id FROM session_child_links WHERE child_id = ?1",
            [request.child],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| failure(&error))?;
    let revision: i64 = tx.query_row("SELECT COALESCE((SELECT revision FROM session_relationship_revisions WHERE child_id = ?1), 0)", [request.child], |row| row.get(0))
        .map_err(|error| failure(&error))?;
    if current.as_deref() != request.previous_parent
        || u64::try_from(revision).ok() != Some(request.revision)
    {
        return Err(refuse(
            "relationship changed; read authoritative parent and revision before a new explicit attempt",
        ));
    }
    let invalid: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1 OR parent_id = ?2)
         OR EXISTS(SELECT 1 FROM session_bridges WHERE session_id IN (?1, ?2) AND lifecycle <> 'open')
         OR (SELECT COUNT(*) FROM session_child_links WHERE parent_id = ?1) >= 32",
        params![request.parent, request.child], |row| row.get(0),
    ).map_err(|error| failure(&error))?;
    if invalid {
        return Err(refuse(
            "claim/transfer requires open, one-level sessions and fewer than 32 children; cycles and nested parents are not supported",
        ));
    }
    ensure_delivery_settled(tx, request)?;
    if request.transfer {
        tx.execute(
            "UPDATE session_child_links SET parent_id = ?2 WHERE child_id = ?1",
            params![request.child, request.parent],
        )
        .map_err(|error| failure(&error))?;
    } else {
        ReportStore::link_in_transaction(tx, request.parent, request.child)?;
    }
    Ok(())
}

/// Pending, claimed, blocked, and dispatch-uncertain messages are not moved or replayed. Refuse
/// before changing any authority. The owner can drain or explicitly withdraw pending messages;
/// a dispatch fence still requires the existing inbox recovery path. Consumed evidence stays inert.
fn ensure_delivery_settled(tx: &Transaction<'_>, request: &Reparent<'_>) -> Result<()> {
    let unsettled: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM followup_messages m
         LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
         LEFT JOIN followup_sources s ON s.message_sequence = m.sequence
         LEFT JOIN child_followup_deliveries d ON d.message_sequence = m.sequence
         LEFT JOIN session_child_reports r ON r.sequence = d.report_sequence
         WHERE m.sequence NOT IN (SELECT message_sequence FROM followup_removals)
           AND (b.state IS NULL OR b.state <> 'consumed')
           AND (r.child_id = ?1 OR
             (json_extract(s.source_json, '$.kind') = 'durable_child_evidence' AND json_extract(s.source_json, '$.sessionId') = ?1) OR
             (m.session_id = ?1 AND json_extract(s.source_json, '$.kind') = 'delegated_instruction')))",
        [request.child], |row| row.get(0),
    ).map_err(|error| failure(&error))?;
    if unsettled {
        return Err(refuse(
            "relationship has unsettled child reports or delegated instructions; drain or explicitly withdraw pending messages first; uncertain dispatch must not be replayed",
        ));
    }
    Ok(())
}
