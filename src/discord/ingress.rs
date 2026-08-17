//! Workspace-local durable Discord gateway ingress persistence.

use std::{path::Path, time::Duration};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    DiscordError, INGRESS_TOMBSTONE_RETENTION, IngressRecord, MAX_INBOUND_INFLIGHT,
    MAX_INGRESS_TOMBSTONES, MAX_PENDING_ROUTE_REJECTIONS, io_error, prepare_private_directory,
    unix_time_ms_i64,
};

pub(super) fn prune_ingress_tombstones(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), DiscordError> {
    let retention_ms = i64::try_from(INGRESS_TOMBSTONE_RETENTION.as_millis()).unwrap_or(i64::MAX);
    let cutoff = unix_time_ms_i64().saturating_sub(retention_ms);
    transaction
        .execute(
            "DELETE FROM discord_ingress_tombstones WHERE recorded_at_ms < ?1",
            [cutoff],
        )
        .map_err(DiscordError::IngressStore)?;
    transaction
        .execute(
            "DELETE FROM discord_ingress_tombstones
             WHERE external_event_id IN (
               SELECT external_event_id FROM discord_ingress_tombstones
               ORDER BY recorded_at_ms DESC, external_event_id DESC
               LIMIT -1 OFFSET ?1
             )",
            [i64::try_from(MAX_INGRESS_TOMBSTONES).unwrap_or(i64::MAX)],
        )
        .map_err(DiscordError::IngressStore)?;
    Ok(())
}

pub(super) struct IngressSpool {
    pub(super) connection: std::sync::Mutex<Connection>,
}

impl IngressSpool {
    pub(super) fn open(path: &Path) -> Result<Self, DiscordError> {
        if let Some(parent) = path.parent() {
            prepare_private_directory(parent)?;
        }
        let connection = Connection::open(path).map_err(DiscordError::IngressStore)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(DiscordError::IngressStore)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS discord_ingress (
                   sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                   external_event_id TEXT NOT NULL UNIQUE,
                   session_id TEXT NOT NULL,
                   multipart_group TEXT,
                   payload_json BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS discord_ingress_tombstones (
                   external_event_id TEXT PRIMARY KEY,
                   recorded_at_ms INTEGER NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS discord_ingress_tombstones_recorded
                   ON discord_ingress_tombstones(recorded_at_ms);",
            )
            .map_err(DiscordError::IngressStore)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(|source| io_error(path, source))?;
        }
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
    }

    /// Durably admits a gateway event. `None` means this identity already reached a terminal local
    /// or authoritative disposition and must not be reconsidered after a reconnect or reopen.
    pub(super) fn enqueue(
        &self,
        proposed: &IngressRecord,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An immediate transaction serializes admission across Nakode processes before either the
        // per-session ordering check or overload decision is observed. The unique event key then
        // makes the first complete durable decision authoritative for duplicate gateway delivery.
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(DiscordError::IngressStore)?;
        let tombstoned = transaction
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM discord_ingress_tombstones WHERE external_event_id = ?1
                 )",
                [&proposed.message_id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        if tombstoned {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(None);
        }
        if let Some(payload) = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [&proposed.message_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?
        {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return serde_json::from_slice(&payload)
                .map(Some)
                .map_err(DiscordError::IngressPayload);
        }

        let mut record = proposed.clone();
        let same_session_pending = if record.route_pending {
            false
        } else {
            transaction
                .query_row(
                    "SELECT EXISTS(
                       SELECT 1 FROM discord_ingress
                       WHERE session_id = ?1
                         AND (?2 IS NULL OR multipart_group IS NULL OR multipart_group != ?2)
                     )",
                    params![record.session_id, record.multipart_group],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DiscordError::IngressStore)?
        };
        let pending_count = transaction
            .query_row("SELECT COUNT(*) FROM discord_ingress", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(DiscordError::IngressStore)?;
        let ordinary_capacity_exhausted = pending_count
            >= i64::try_from(MAX_INBOUND_INFLIGHT).unwrap_or(i64::MAX)
            && (record.route_pending || record.multipart_group.is_none());
        let route_rejection_capacity_exhausted = pending_count
            >= i64::try_from(MAX_INBOUND_INFLIGHT.saturating_add(MAX_PENDING_ROUTE_REJECTIONS))
                .unwrap_or(i64::MAX);
        if same_session_pending {
            record.forced_busy = true;
            record.local_terminal = true;
        } else if ordinary_capacity_exhausted {
            record.forced_busy = true;
            // A resolved route can be rejected and reacted locally. An unresolved route gets one
            // bounded, content-free replay row so only its owning workspace applies Busy. At the
            // second hard bound every workspace drops/tombstones silently rather than cross-react.
            record.local_terminal = !record.route_pending || route_rejection_capacity_exhausted;
        }
        if record.forced_busy {
            // Busy records only need durable identity and route metadata. Do not retain prompt text,
            // expiring attachment URLs, or grouping for work that is guaranteed never to execute.
            record.content.clear();
            record.attachments.clear();
            record.multipart_group = None;
        }
        if record.local_terminal {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO discord_ingress_tombstones
                     (external_event_id, recorded_at_ms) VALUES (?1, ?2)",
                    params![record.message_id, unix_time_ms_i64()],
                )
                .map_err(DiscordError::IngressStore)?;
            prune_ingress_tombstones(&transaction)?;
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(Some(record));
        }
        let payload = serde_json::to_vec(&record).map_err(DiscordError::IngressPayload)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress
                 (external_event_id, session_id, multipart_group, payload_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    record.message_id,
                    record.session_id,
                    record.multipart_group,
                    payload
                ],
            )
            .map_err(DiscordError::IngressStore)?;
        let authoritative = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [&record.message_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)?;
        serde_json::from_slice(&authoritative)
            .map(Some)
            .map_err(DiscordError::IngressPayload)
    }

    pub(super) fn bind_route(
        &self,
        external_event_id: &str,
        session_id: &str,
        force_busy: bool,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        if session_id.is_empty() {
            return Err(DiscordError::InvalidConfig(
                "resolved ingress route has no session identity".to_owned(),
            ));
        }
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(DiscordError::IngressStore)?;
        let Some(payload) = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [external_event_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?
        else {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(None);
        };
        let mut record: IngressRecord =
            serde_json::from_slice(&payload).map_err(DiscordError::IngressPayload)?;
        if !record.route_pending {
            if record.session_id != session_id {
                return Err(DiscordError::InvalidConfig(
                    "durable ingress route changed after admission".to_owned(),
                ));
            }
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(Some(record));
        }

        session_id.clone_into(&mut record.session_id);
        record.route_pending = false;
        let same_session_pending = transaction
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM discord_ingress
                   WHERE external_event_id != ?1 AND session_id = ?2
                     AND (?3 IS NULL OR multipart_group IS NULL OR multipart_group != ?3)
                 )",
                params![external_event_id, record.session_id, record.multipart_group],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DiscordError::IngressStore)?;
        if same_session_pending || force_busy {
            record.forced_busy = true;
            record.content.clear();
            record.attachments.clear();
            record.multipart_group = None;
        }
        let encoded = serde_json::to_vec(&record).map_err(DiscordError::IngressPayload)?;
        transaction
            .execute(
                "UPDATE discord_ingress
                 SET session_id = ?2, multipart_group = ?3, payload_json = ?4
                 WHERE external_event_id = ?1",
                params![
                    external_event_id,
                    record.session_id,
                    record.multipart_group,
                    encoded
                ],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)?;
        Ok(Some(record))
    }

    pub(super) fn force_busy(
        &self,
        external_event_id: &str,
    ) -> Result<Option<IngressRecord>, DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(DiscordError::IngressStore)?;
        let Some(payload) = transaction
            .query_row(
                "SELECT payload_json FROM discord_ingress WHERE external_event_id = ?1",
                [external_event_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?
        else {
            transaction.commit().map_err(DiscordError::IngressStore)?;
            return Ok(None);
        };
        let mut record: IngressRecord =
            serde_json::from_slice(&payload).map_err(DiscordError::IngressPayload)?;
        record.forced_busy = true;
        record.content.clear();
        record.attachments.clear();
        record.multipart_group = None;
        let encoded = serde_json::to_vec(&record).map_err(DiscordError::IngressPayload)?;
        transaction
            .execute(
                "UPDATE discord_ingress
                 SET multipart_group = NULL, payload_json = ?2
                 WHERE external_event_id = ?1",
                params![external_event_id, encoded],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction.commit().map_err(DiscordError::IngressStore)?;
        Ok(Some(record))
    }

    pub(super) fn next_after(
        &self,
        sequence: i64,
    ) -> Result<Option<(i64, IngressRecord)>, DiscordError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let row = connection
            .query_row(
                "SELECT sequence, payload_json FROM discord_ingress
                 WHERE sequence > ?1 ORDER BY sequence LIMIT 1",
                [sequence],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(DiscordError::IngressStore)?;
        row.map(|(sequence, payload)| {
            serde_json::from_slice(&payload)
                .map(|record| (sequence, record))
                .map_err(DiscordError::IngressPayload)
        })
        .transpose()
    }

    pub(super) fn remove_event(&self, external_event_id: &str) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?2 FROM discord_ingress WHERE external_event_id = ?1",
                params![external_event_id, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE external_event_id = ?1",
                [external_event_id],
            )
            .map_err(DiscordError::IngressStore)?;
        prune_ingress_tombstones(&transaction)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    pub(super) fn remove_multipart_group(
        &self,
        session_id: &str,
        group: &str,
    ) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?3 FROM discord_ingress
                 WHERE session_id = ?1 AND multipart_group = ?2",
                params![session_id, group, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE session_id = ?1 AND multipart_group = ?2",
                params![session_id, group],
            )
            .map_err(DiscordError::IngressStore)?;
        prune_ingress_tombstones(&transaction)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    /// Quarantines one corrupt payload without retaining user content or allowing its event
    /// identity to become a future prompt after a reconnect.
    pub(super) fn discard_next_after(&self, sequence: i64) -> Result<(), DiscordError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection
            .transaction()
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO discord_ingress_tombstones
                 (external_event_id, recorded_at_ms)
                 SELECT external_event_id, ?2 FROM discord_ingress
                 WHERE sequence = (
                   SELECT MIN(sequence) FROM discord_ingress WHERE sequence > ?1
                 )",
                params![sequence, unix_time_ms_i64()],
            )
            .map_err(DiscordError::IngressStore)?;
        transaction
            .execute(
                "DELETE FROM discord_ingress WHERE sequence = (
                   SELECT MIN(sequence) FROM discord_ingress WHERE sequence > ?1
                 )",
                [sequence],
            )
            .map_err(DiscordError::IngressStore)?;
        prune_ingress_tombstones(&transaction)?;
        transaction.commit().map_err(DiscordError::IngressStore)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> Result<u64, DiscordError> {
        let count = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .query_row("SELECT COUNT(*) FROM discord_ingress", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(DiscordError::IngressStore)?;
        Ok(u64::try_from(count).unwrap_or_default())
    }
}
