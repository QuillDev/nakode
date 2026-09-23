use super::{InboxRequest, MAX_PENDING_BYTES, Result, failure, refuse, validate_prompt};
use nakode_protocol::{PromptAttachment, PromptInput};
use rusqlite::{Connection, OptionalExtension, params};

pub(super) fn message_exists(
    connection: &Connection,
    session: &str,
    message_id: &str,
    digest: &[u8],
) -> Result<bool> {
    let existing: Option<Vec<u8>> = connection.query_row(
        "SELECT request_digest FROM followup_messages WHERE session_id = ?1 AND message_id = ?2",
        params![session, message_id], |row| row.get(0),
    ).optional().map_err(failure)?;
    match existing {
        None => Ok(false),
        Some(existing) if existing == digest => Ok(true),
        Some(_) => Err(refuse(
            "follow-up message identity reused for different content",
        )),
    }
}

pub(super) fn admit_message(
    connection: &Connection,
    session: &str,
    message_id: &str,
    prompt: &PromptInput,
    request: InboxRequest<'_>,
    digest: &[u8],
) -> Result<()> {
    let bytes =
        i64::try_from(validate_prompt(prompt)?).map_err(|_| refuse("follow-up size overflow"))?;
    let json = serde_json::to_string(prompt).map_err(|_| refuse("invalid follow-up payload"))?;
    let (count, pending_bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(m.payload_bytes), 0)
         FROM followup_messages m LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
         WHERE m.session_id = ?1 AND m.sequence NOT IN (SELECT message_sequence FROM followup_removals) AND (b.state IS NULL OR b.state <> 'consumed')",
            [session],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(failure)?;
    if count >= 256
        || pending_bytes + bytes > i64::try_from(MAX_PENDING_BYTES).expect("fixed inbox bound")
    {
        return Err(refuse(
            "follow-up inbox is full (256 items / 64 MiB); nothing was enqueued",
        ));
    }
    let labels: Vec<_> = prompt
        .attachments
        .iter()
        .map(|attachment| match attachment {
            PromptAttachment::Artifact { label, .. }
            | PromptAttachment::InlineImage { label, .. }
            | PromptAttachment::LocalFile { label, .. } => label,
        })
        .collect();
    let labels = serde_json::to_string(&labels).map_err(|_| refuse("invalid attachment labels"))?;
    // Keep metadata independently readable: listing never decodes retained image payloads.
    connection
        .execute(
            "INSERT INTO followup_messages (
             session_id, message_id, submitted_by, received_at_ms, prompt_json,
             display_text, attachment_labels_json, payload_bytes, request_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                session,
                message_id,
                request.sender,
                request.now_ms,
                json,
                prompt.text,
                labels,
                bytes,
                digest
            ],
        )
        .map_err(failure)?;
    Ok(())
}
