//! Canonical delegated-transcript image associations; bytes never cross a private client boundary.
use rusqlite::{Connection, Transaction, params};

use super::{SessionError, SubagentRecord};
use crate::{domain_transcript::StoredTranscriptImage, media::ImageData};

pub(super) fn migrate(connection: &Connection) -> Result<(), SessionError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS orchestration_transcript_images (
            parent_session_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            entry_key TEXT NOT NULL,
            label TEXT NOT NULL,
            media_type TEXT NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY(parent_session_id, run_id, sequence),
            FOREIGN KEY(parent_session_id, run_id) REFERENCES orchestration_runs(parent_session_id, id) ON DELETE CASCADE
        );",
    )?;
    Ok(())
}

pub(super) fn save(
    transaction: &Transaction<'_>,
    record: &SubagentRecord,
) -> Result<(), SessionError> {
    transaction.execute(
        "DELETE FROM orchestration_transcript_images WHERE parent_session_id = ?1 AND run_id = ?2",
        params![record.parent_session_id, record.id],
    )?;
    for (sequence, image) in record.images.iter().enumerate() {
        transaction.execute(
            "INSERT INTO orchestration_transcript_images (parent_session_id, run_id, sequence, entry_key, label, media_type, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![record.parent_session_id, record.id, i64::try_from(sequence).unwrap_or(i64::MAX), image.entry_key, image.label, image.image.mime_type, image.image.data],
        )?;
    }
    Ok(())
}

pub(super) fn load(
    connection: &Connection,
    parent: &str,
    run: &str,
) -> Result<Vec<StoredTranscriptImage>, SessionError> {
    let mut statement = connection.prepare("SELECT entry_key, label, media_type, data FROM orchestration_transcript_images WHERE parent_session_id = ?1 AND run_id = ?2 ORDER BY sequence")?;
    Ok(statement
        .query_map(params![parent, run], |row| {
            Ok(StoredTranscriptImage {
                entry_key: row.get(0)?,
                label: row.get(1)?,
                image: ImageData {
                    mime_type: row.get(2)?,
                    data: row.get(3)?,
                },
            })
        })?
        .collect::<Result<Vec<_>, _>>()?)
}
