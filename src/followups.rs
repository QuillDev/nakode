//! Ordinary owner follow-ups have their own durable ledger. Claims never delete inputs; a
//! pre-dispatch fence prevents automatic replay after an ambiguous provider/process failure.
use std::{path::Path, time::Duration};

use nakode_protocol::{
    Command, ErrorCode, FollowupInbox, FollowupItem, PromptAttachment, PromptInput, ServiceError,
    SessionId,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

mod admission;
mod batches;
#[cfg(test)]
mod tests;

pub(crate) const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
pub(crate) type Result<T> = std::result::Result<T, ServiceError>;
pub(crate) struct InboxStore(Connection);

#[derive(Clone, Copy)]
pub(crate) struct InboxRequest<'a> {
    pub command: &'a Command,
    pub key: &'a str,
    pub sender: &'a str,
    pub replay_only: bool,
    pub now_ms: i64,
}

pub(crate) fn refuse(message: impl Into<String>) -> ServiceError {
    ServiceError {
        code: ErrorCode::Conflict,
        message: message.into(),
        retryable: false,
    }
}
fn failure(error: impl std::fmt::Display) -> ServiceError {
    ServiceError {
        code: ErrorCode::Internal,
        message: format!("follow-up persistence: {error}"),
        retryable: true,
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 200 && value.trim() == value
}

fn authorize(connection: &Connection, session: &str, require_open: bool) -> Result<()> {
    if !valid_id(session) {
        return Err(refuse("invalid follow-up session identity"));
    }
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
            [session],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if !exists {
        return Err(ServiceError {
            code: ErrorCode::NotFound,
            message: "follow-up session not found".to_owned(),
            retryable: false,
        });
    }
    let closed: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_bridges WHERE session_id = ?1 AND lifecycle <> 'open')",
        [session], |row| row.get(0),
    ).map_err(failure)?;
    if require_open && closed {
        return Err(refuse(
            "closed sessions cannot accept or dispatch follow-ups",
        ));
    }
    Ok(())
}

fn validate_prompt(prompt: &PromptInput) -> Result<usize> {
    if prompt.text.len() > MAX_TEXT_BYTES
        || (prompt.text.trim().is_empty() && prompt.attachments.is_empty())
    {
        return Err(refuse(
            "follow-up requires content and at most 64 KiB of text; nothing was enqueued",
        ));
    }
    if prompt.attachments.len() > 8 {
        return Err(refuse("follow-up supports at most eight attachments"));
    }
    let mut bytes = prompt.text.len();
    for attachment in &prompt.attachments {
        match attachment {
            PromptAttachment::InlineImage {
                label,
                media_type,
                data,
            } => {
                if label.len() > 512 {
                    return Err(refuse("attachment label exceeds 512 bytes"));
                }
                crate::image_handoff::validate_image(data, media_type).map_err(refuse)?;
                bytes += data.len();
            }
            // Admission must materialize references through the canonical projector first. Keeping
            // only a volatile artifact ID would lose the accepted attachment on history compaction.
            PromptAttachment::Artifact { .. } => {
                return Err(refuse(
                    "follow-up artifact must be materialized before admission",
                ));
            }
            PromptAttachment::LocalFile { label, path } => {
                if label.len() > 512 || path.len() > 4096 {
                    return Err(refuse("follow-up file metadata exceeds bounds"));
                }
                bytes += label.len() + path.len();
            }
        }
    }
    if bytes > 20 * 1024 * 1024 + MAX_TEXT_BYTES {
        return Err(refuse("follow-up attachments exceed 20 MiB"));
    }
    Ok(bytes)
}

impl InboxStore {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(failure)?;
        connection
            .busy_timeout(Duration::from_secs(2))
            .map_err(failure)?;
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(failure)?;
        Ok(Self(connection))
    }

    pub(crate) fn has_messages(&self) -> Result<bool> {
        self.0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM followup_messages)",
                [],
                |row| row.get(0),
            )
            .map_err(failure)
    }

    #[cfg(test)]
    pub(crate) fn execute(
        &mut self,
        command: &Command,
        key: &str,
        sender: &str,
        replay_only: bool,
        now_ms: i64,
    ) -> Result<Option<String>> {
        self.execute_materialized(
            InboxRequest {
                command,
                key,
                sender,
                replay_only,
                now_ms,
            },
            |prompt| Ok(prompt.clone()),
        )
    }

    /// Receipts hash the original request, not volatile materialization results. A retry can replay
    /// its durable receipt even after the source artifact is no longer in the in-memory transcript.
    pub(crate) fn execute_materialized(
        &mut self,
        request: InboxRequest<'_>,
        materialize: impl FnOnce(&PromptInput) -> Result<PromptInput>,
    ) -> Result<Option<String>> {
        let InboxRequest {
            command,
            key,
            sender,
            replay_only,
            now_ms: _,
        } = request;
        if !valid_id(key) || !valid_id(sender) {
            return Err(refuse(
                "follow-up command/client identity must be 1–200 bytes",
            ));
        }
        let encoded =
            serde_json::to_vec(command).map_err(|_| refuse("invalid follow-up command"))?;
        let digest = Sha256::digest(&encoded).to_vec();
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let saved: Option<(Vec<u8>, Option<String>)> = tx
            .query_row(
                "SELECT digest, resource_id FROM followup_receipts WHERE command_key = ?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(failure)?;
        if let Some((saved, id)) = saved {
            return if saved == digest {
                Ok(id)
            } else {
                Err(refuse(
                    "follow-up idempotency key reused for different content",
                ))
            };
        }
        if replay_only {
            return Err(refuse("follow-up receipt missing; no mutation executed"));
        }
        let (session, id) = match command {
            Command::EnqueueFollowup {
                session_id,
                message_id,
                prompt,
            } => {
                authorize(&tx, session_id.as_str(), true)?;
                if !valid_id(message_id) {
                    return Err(refuse("invalid follow-up message identity"));
                }
                if !admission::message_exists(&tx, session_id.as_str(), message_id, &digest)? {
                    let prompt = materialize(prompt)?;
                    admission::admit_message(
                        &tx,
                        session_id.as_str(),
                        message_id,
                        &prompt,
                        request,
                        &digest,
                    )?;
                }
                (session_id.as_str(), Some(message_id.clone()))
            }
            Command::SetFollowupPaused { session_id, paused } => {
                authorize(&tx, session_id.as_str(), true)?;
                tx.execute(
                    "INSERT INTO followup_inboxes(session_id, paused) VALUES (?1, ?2)
                     ON CONFLICT(session_id) DO UPDATE SET paused = excluded.paused",
                    params![session_id.as_str(), paused],
                )
                .map_err(failure)?;
                if !paused {
                    tx.execute(
                        "UPDATE followup_batches SET blocked_reason = NULL WHERE session_id = ?1 AND state = 'claimed'",
                        [session_id.as_str()],
                    ).map_err(failure)?;
                }
                (session_id.as_str(), None)
            }
            _ => return Err(refuse("not a follow-up command")),
        };
        tx.execute(
            "INSERT INTO followup_receipts(command_key, session_id, digest, resource_id) VALUES (?1, ?2, ?3, ?4)",
            params![key, session, digest, id],
        ).map_err(failure)?;
        tx.commit().map_err(failure)?;
        Ok(id)
    }

    pub(crate) fn list(
        &self,
        session: &SessionId,
        after: u64,
        limit: u32,
    ) -> Result<FollowupInbox> {
        authorize(&self.0, session.as_str(), false)?;
        if !(1..=64).contains(&limit) || after > i64::MAX as u64 {
            return Err(refuse(
                "follow-up page limit must be 1–64; cursor must be nonnegative i64",
            ));
        }
        let mut statement = self
            .0
            .prepare(
                "SELECT m.sequence, m.message_id, m.submitted_by, m.received_at_ms,
                 m.display_text, COALESCE(b.state, 'pending'), m.batch_id, m.attachment_labels_json
             FROM followup_messages m LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
             WHERE m.session_id = ?1 AND m.sequence > ?2 ORDER BY m.sequence LIMIT ?3",
            )
            .map_err(failure)?;
        let mut rows = statement
            .query(params![
                session.as_str(),
                i64::try_from(after).map_err(|_| refuse("invalid follow-up cursor"))?,
                limit + 1
            ])
            .map_err(failure)?;
        let mut items = Vec::new();
        let mut text_bytes = 0;
        let mut has_more = false;
        while let Some(row) = rows.next().map_err(failure)? {
            let text: String = row.get(4).map_err(failure)?;
            if items.len() >= limit as usize || text_bytes + text.len() > 256 * 1024 {
                has_more = true;
                break;
            }
            text_bytes += text.len();
            items.push(FollowupItem {
                sequence: u64::try_from(row.get::<_, i64>(0).map_err(failure)?)
                    .map_err(|_| refuse("invalid stored sequence"))?,
                message_id: row.get(1).map_err(failure)?,
                submitted_by: row.get(2).map_err(failure)?,
                received_at_ms: row.get(3).map_err(failure)?,
                text,
                attachment_labels: serde_json::from_str(&row.get::<_, String>(7).map_err(failure)?)
                    .map_err(|_| refuse("stored follow-up labels are invalid"))?,
                state: row.get(5).map_err(failure)?,
                batch_id: row.get(6).map_err(failure)?,
            });
        }
        let pending_count: i64 = self
            .0
            .query_row(
                "SELECT COUNT(*) FROM followup_messages WHERE session_id = ?1 AND batch_id IS NULL",
                [session.as_str()],
                |row| row.get(0),
            )
            .map_err(failure)?;
        let paused = self
            .0
            .query_row(
                "SELECT paused FROM followup_inboxes WHERE session_id = ?1",
                [session.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(failure)?
            .unwrap_or(false);
        let unresolved: Option<(String, String, Option<String>)> = self
            .0
            .query_row(
                "SELECT batch_id, state, blocked_reason FROM followup_batches
             WHERE session_id = ?1 AND state <> 'consumed'",
                [session.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(failure)?;
        let (unsettled_batch_id, blocked_reason) =
            unresolved.map_or((None, None), |(id, state, reason)| {
                (
                    Some(id),
                    reason.or_else(|| {
                        (state == "dispatching").then(|| {
                            "Provider dispatch is unacknowledged; automatic replay is disabled."
                                .to_owned()
                        })
                    }),
                )
            });
        Ok(FollowupInbox {
            session_id: session.clone(),
            items,
            has_more,
            pending_count: u64::try_from(pending_count)
                .map_err(|_| refuse("invalid follow-up count"))?,
            paused,
            unsettled_batch_id,
            blocked_reason,
        })
    }
}
