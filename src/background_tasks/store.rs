//! Durable registry rows for managed background tasks, kept in Nakode's sessions database.

use std::{path::Path, time::Duration};

use rusqlite::{Connection, OptionalExtension as _, Row, params};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS background_tasks (
   task_id TEXT PRIMARY KEY,
   name TEXT NOT NULL,
   owner_session_id TEXT NOT NULL,
   origin_run_id TEXT,
   origin_turn_id TEXT,
   origin_call_id TEXT,
   command TEXT NOT NULL,
   cwd TEXT NOT NULL,
   machine TEXT NOT NULL,
   state TEXT NOT NULL,
   supervision TEXT NOT NULL,
   process_id INTEGER,
   process_start TEXT,
   exit_code INTEGER,
   exit_signal INTEGER,
   detail TEXT,
   stop_reason TEXT,
   stop_requested_at_ms INTEGER,
   owner_deleted_at_ms INTEGER,
   endpoints_json TEXT NOT NULL DEFAULT '[]',
   log_path TEXT NOT NULL,
   created_at_ms INTEGER NOT NULL,
   started_at_ms INTEGER,
   ended_at_ms INTEGER,
   updated_at_ms INTEGER NOT NULL
 );
 CREATE INDEX IF NOT EXISTS background_tasks_owner
   ON background_tasks(owner_session_id, created_at_ms);
 CREATE INDEX IF NOT EXISTS background_tasks_state
   ON background_tasks(state);
 CREATE TABLE IF NOT EXISTS background_task_receipts (
   task_id TEXT PRIMARY KEY,
   owner_session_id TEXT NOT NULL,
   origin_run_id TEXT
 );
 CREATE TABLE IF NOT EXISTS background_task_processes (
   task_id TEXT NOT NULL,
   process_id INTEGER NOT NULL,
   process_start TEXT NOT NULL,
   PRIMARY KEY (task_id, process_id)
 );";

const COLUMNS: &str =
    "task_id, name, owner_session_id, origin_run_id, origin_turn_id, origin_call_id,
   command, cwd, machine, state, supervision, process_id, process_start, exit_code, exit_signal,
   detail, stop_reason, stop_requested_at_ms, owner_deleted_at_ms, endpoints_json, log_path,
   created_at_ms, started_at_ms, ended_at_ms, updated_at_ms";

/// Lifecycle states. Only `Running`, `Stopping`, `Launching` and `Unknown` may still have live
/// processes; the rest are terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskState {
    Launching,
    Running,
    Stopping,
    Exited,
    Stopped,
    FailedToStart,
    CleanupFailed,
    Unknown,
}

impl TaskState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Launching => "launching",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Exited => "exited",
            Self::Stopped => "stopped",
            Self::FailedToStart => "failed_to_start",
            Self::CleanupFailed => "cleanup_failed",
            Self::Unknown => "unknown",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "launching" => Self::Launching,
            "running" => Self::Running,
            "stopping" => Self::Stopping,
            "exited" => Self::Exited,
            "stopped" => Self::Stopped,
            "failed_to_start" => Self::FailedToStart,
            "cleanup_failed" => Self::CleanupFailed,
            // An unrecognized persisted state is never treated as ended.
            _ => Self::Unknown,
        }
    }

    /// Whether no process of this task can be alive any more.
    ///
    /// `CleanupFailed` is deliberately not ended: processes remained after the last stop.
    pub(crate) fn ended(self) -> bool {
        matches!(self, Self::Exited | Self::Stopped | Self::FailedToStart)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaskRecord {
    pub(crate) task_id: String,
    pub(crate) name: String,
    pub(crate) owner_session_id: String,
    pub(crate) origin_run_id: Option<String>,
    pub(crate) origin_turn_id: Option<String>,
    pub(crate) origin_call_id: Option<String>,
    pub(crate) command: String,
    pub(crate) cwd: String,
    pub(crate) machine: String,
    pub(crate) state: TaskState,
    /// `child` while this runtime process spawned and reaps the leader, `adopted` after restart.
    pub(crate) supervision: String,
    /// Process-group leader id; the group id equals it.
    pub(crate) process_id: Option<i32>,
    /// The leader's OS start time, recorded to distinguish a reused process id.
    pub(crate) process_start: Option<String>,
    pub(crate) exit_code: Option<i32>,
    pub(crate) exit_signal: Option<i32>,
    pub(crate) detail: Option<String>,
    pub(crate) stop_reason: Option<String>,
    pub(crate) stop_requested_at_ms: Option<i64>,
    pub(crate) owner_deleted_at_ms: Option<i64>,
    pub(crate) endpoints: Vec<String>,
    pub(crate) log_path: String,
    pub(crate) created_at_ms: i64,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) ended_at_ms: Option<i64>,
    pub(crate) updated_at_ms: i64,
}

pub(crate) struct TaskStore {
    connection: Connection,
}

impl TaskStore {
    pub(crate) fn open(database: &Path) -> Result<Self, String> {
        let connection = Connection::open(database).map_err(|error| failure(&error))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|error| failure(&error))?;
        connection
            .execute_batch(SCHEMA)
            .map_err(|error| failure(&error))?;
        Ok(Self { connection })
    }

    pub(crate) fn insert(&self, record: &TaskRecord) -> Result<(), String> {
        self.connection
            .execute(
                &format!(
                    "INSERT INTO background_tasks ({COLUMNS}) VALUES
                     (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                      ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)"
                ),
                params![
                    record.task_id,
                    record.name,
                    record.owner_session_id,
                    record.origin_run_id,
                    record.origin_turn_id,
                    record.origin_call_id,
                    record.command,
                    record.cwd,
                    record.machine,
                    record.state.as_str(),
                    record.supervision,
                    record.process_id,
                    record.process_start,
                    record.exit_code,
                    record.exit_signal,
                    record.detail,
                    record.stop_reason,
                    record.stop_requested_at_ms,
                    record.owner_deleted_at_ms,
                    serde_json::to_string(&record.endpoints).unwrap_or_else(|_| "[]".to_owned()),
                    record.log_path,
                    record.created_at_ms,
                    record.started_at_ms,
                    record.ended_at_ms,
                    record.updated_at_ms,
                ],
            )
            .map_err(|error| failure(&error))?;
        Ok(())
    }

    /// Writes every mutable lifecycle column of an existing row.
    pub(crate) fn update(&self, record: &TaskRecord) -> Result<(), String> {
        let changed = self
            .connection
            .execute(
                "UPDATE background_tasks SET
                   state = ?2, supervision = ?3, process_id = ?4, process_start = ?5,
                   exit_code = ?6, exit_signal = ?7, detail = ?8, stop_reason = ?9,
                   stop_requested_at_ms = ?10, owner_deleted_at_ms = ?11, started_at_ms = ?12,
                   ended_at_ms = ?13, updated_at_ms = ?14
                 WHERE task_id = ?1",
                params![
                    record.task_id,
                    record.state.as_str(),
                    record.supervision,
                    record.process_id,
                    record.process_start,
                    record.exit_code,
                    record.exit_signal,
                    record.detail,
                    record.stop_reason,
                    record.stop_requested_at_ms,
                    record.owner_deleted_at_ms,
                    record.started_at_ms,
                    record.ended_at_ms,
                    record.updated_at_ms,
                ],
            )
            .map_err(|error| failure(&error))?;
        if changed == 1 {
            Ok(())
        } else {
            Err(format!(
                "background task {} is missing from the registry",
                record.task_id
            ))
        }
    }

    pub(crate) fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, String> {
        self.connection
            .query_row(
                &format!("SELECT {COLUMNS} FROM background_tasks WHERE task_id = ?1"),
                [task_id],
                read_record,
            )
            .optional()
            .map_err(|error| failure(&error))
    }

    /// Every task, or every task of one owner, oldest first.
    pub(crate) fn list(&self, owner_session_id: Option<&str>) -> Result<Vec<TaskRecord>, String> {
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT {COLUMNS} FROM background_tasks
                 WHERE ?1 IS NULL OR owner_session_id = ?1
                 ORDER BY created_at_ms, task_id"
            ))
            .map_err(|error| failure(&error))?;
        statement
            .query_map([owner_session_id], read_record)
            .map_err(|error| failure(&error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| failure(&error))
    }

    /// Atomically replace an operational record with a minimal acknowledgement receipt.
    pub(crate) fn remove(&self, record: &TaskRecord) -> Result<(), String> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(|error| failure(&error))?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO background_task_receipts VALUES (?1, ?2, ?3)",
                params![
                    record.task_id,
                    record.owner_session_id,
                    record.origin_run_id
                ],
            )
            .map_err(|error| failure(&error))?;
        transaction
            .execute(
                "DELETE FROM background_task_processes WHERE task_id = ?1",
                [&record.task_id],
            )
            .map_err(|error| failure(&error))?;
        transaction
            .execute(
                "DELETE FROM background_tasks WHERE task_id = ?1",
                [&record.task_id],
            )
            .map_err(|error| failure(&error))?;
        transaction.commit().map_err(|error| failure(&error))
    }

    pub(crate) fn receipt(
        &self,
        task_id: &str,
    ) -> Result<Option<(String, Option<String>)>, String> {
        self.connection.query_row(
            "SELECT owner_session_id, origin_run_id FROM background_task_receipts WHERE task_id = ?1",
            [task_id], |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(|error| failure(&error))
    }

    pub(crate) fn tracked(
        &self,
        task_id: &str,
    ) -> Result<std::collections::HashMap<i32, String>, String> {
        let mut statement = self.connection.prepare(
            "SELECT process_id, process_start FROM background_task_processes WHERE task_id = ?1",
        ).map_err(|error| failure(&error))?;
        statement
            .query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|error| failure(&error))?
            .collect::<Result<_, _>>()
            .map_err(|error| failure(&error))
    }

    pub(crate) fn track(
        &self,
        task_id: &str,
        tracked: &std::collections::HashMap<i32, String>,
    ) -> Result<(), String> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(|error| failure(&error))?;
        transaction
            .execute(
                "DELETE FROM background_task_processes WHERE task_id = ?1",
                [task_id],
            )
            .map_err(|error| failure(&error))?;
        for (pid, start) in tracked {
            transaction
                .execute(
                    "INSERT OR REPLACE INTO background_task_processes SELECT ?1, ?2, ?3 WHERE EXISTS (SELECT 1 FROM background_tasks WHERE task_id = ?1)",
                    params![task_id, pid, start],
                )
                .map_err(|error| failure(&error))?;
        }
        transaction.commit().map_err(|error| failure(&error))
    }

    /// Tasks whose processes may still be alive.
    pub(crate) fn list_unended(&self) -> Result<Vec<TaskRecord>, String> {
        Ok(self
            .list(None)?
            .into_iter()
            .filter(|record| !record.state.ended())
            .collect())
    }
}

fn read_record(row: &Row<'_>) -> rusqlite::Result<TaskRecord> {
    let state: String = row.get(9)?;
    let endpoints: String = row.get(19)?;
    Ok(TaskRecord {
        task_id: row.get(0)?,
        name: row.get(1)?,
        owner_session_id: row.get(2)?,
        origin_run_id: row.get(3)?,
        origin_turn_id: row.get(4)?,
        origin_call_id: row.get(5)?,
        command: row.get(6)?,
        cwd: row.get(7)?,
        machine: row.get(8)?,
        state: TaskState::parse(&state),
        supervision: row.get(10)?,
        process_id: row.get(11)?,
        process_start: row.get(12)?,
        exit_code: row.get(13)?,
        exit_signal: row.get(14)?,
        detail: row.get(15)?,
        stop_reason: row.get(16)?,
        stop_requested_at_ms: row.get(17)?,
        owner_deleted_at_ms: row.get(18)?,
        endpoints: serde_json::from_str(&endpoints).unwrap_or_default(),
        log_path: row.get(20)?,
        created_at_ms: row.get(21)?,
        started_at_ms: row.get(22)?,
        ended_at_ms: row.get(23)?,
        updated_at_ms: row.get(24)?,
    })
}

fn failure(error: &rusqlite::Error) -> String {
    format!("background task registry failed: {error}")
}
