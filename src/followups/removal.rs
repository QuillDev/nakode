use super::{Result, authorize, failure, refuse, valid_id};
use nakode_protocol::{ErrorCode, ServiceError, SessionId};
use rusqlite::{OptionalExtension, Transaction, params};

/// The caller holds the same IMMEDIATE write transaction used by batch claim.
/// Keep message identity and receipts so old producer retries cannot resurrect removed input.
pub(super) fn remove_pending(
    tx: &Transaction<'_>,
    session_id: &SessionId,
    message_id: &str,
) -> Result<()> {
    authorize(tx, session_id.as_str(), false)?;
    if !valid_id(message_id) {
        return Err(refuse("invalid follow-up message identity"));
    }
    let message: Option<(i64, Option<String>)> = tx
        .query_row(
            "SELECT m.sequence, b.state FROM followup_messages m
         LEFT JOIN followup_batches b ON b.batch_id = m.batch_id
         WHERE m.session_id = ?1 AND m.message_id = ?2",
            params![session_id.as_str(), message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(failure)?;
    let Some((sequence, state)) = message else {
        return Err(ServiceError {
            code: ErrorCode::NotFound,
            message: "follow-up message not found in this session".to_owned(),
            retryable: false,
        });
    };
    if let Some(state) = state {
        return Err(refuse(format!(
            "Update is already {state}; it cannot be removed or recalled."
        )));
    }
    tx.execute(
        "INSERT OR IGNORE INTO followup_removals(message_sequence) VALUES (?1)",
        [sequence],
    )
    .map_err(failure)?;
    // These bytes belong solely to this ledger row. Shared artifact storage is untouched.
    tx.execute(
        "UPDATE followup_messages SET prompt_json = '{}', display_text = '',
         attachment_labels_json = '[]', payload_bytes = 0 WHERE sequence = ?1",
        [sequence],
    )
    .map_err(failure)?;
    Ok(())
}
