use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use directories::ProjectDirs;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

pub use crate::backend::{CODEX_PROVIDER, DEVIN_PROVIDER};
use crate::{
    backend::ModelInfo,
    transcript::{EntryKind, EntryStatus, TranscriptEntry},
};

const PROVIDER_CATALOG_PATH: &str = "config/providers.toml";
const PROVIDER_CATALOG: &str = include_str!("../config/providers.toml");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCatalog {
    providers: Vec<ProviderCatalogEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCatalogEntry {
    slug: String,
    display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRecord {
    pub id: String,
    pub provider: String,
    pub provider_session_id: String,
    pub workspace: String,
    pub title: String,
    pub model: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRecord {
    pub provider: String,
    pub display_name: String,
    pub enabled: bool,
    pub credential: Option<ProviderCredentialRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCredentialRecord {
    pub provider: String,
    pub kind: String,
    pub metadata: serde_json::Value,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubagentStatus {
    Starting,
    Working,
    Completed,
    Interrupted,
    Failed,
}

impl SubagentStatus {
    const fn database_value(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Working => "working",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
        }
    }

    fn from_database(value: &str) -> Result<Self, SessionError> {
        match value {
            "starting" => Ok(Self::Starting),
            "working" => Ok(Self::Working),
            "completed" => Ok(Self::Completed),
            "interrupted" => Ok(Self::Interrupted),
            "failed" => Ok(Self::Failed),
            _ => Err(SessionError::InvalidStoredValue {
                field: "orchestration_runs.status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentRecord {
    pub parent_session_id: String,
    pub id: String,
    pub agent: String,
    pub provider: String,
    pub provider_session_id: Option<String>,
    pub objective: String,
    pub status: SubagentStatus,
    pub latest_activity: String,
    pub transcript: Vec<TranscriptEntry>,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not determine Nakode's application-data directory")]
    MissingDataDirectory,
    #[error("failed to create session database directory {path}: {source}")]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to protect credential-bearing session storage {path}: {source}")]
    ProtectStorage {
        path: String,
        source: std::io::Error,
    },
    #[error("session database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("session {0:?} is ambiguous; use a longer id")]
    Ambiguous(String),
    #[error("invalid persisted value for {field}: {value:?}")]
    InvalidStoredValue { field: &'static str, value: String },
    #[error("provider {0} has no configured credentials")]
    MissingProviderCredential(String),
    #[error("invalid provider catalog {path}: {source}")]
    InvalidProviderCatalog {
        path: &'static str,
        source: toml::de::Error,
    },
}

pub trait SessionRepository: Send + Sync {
    /// Lists the most recently used sessions in a workspace.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_recent(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, SessionError>;
    /// Finds a session by its full id or unambiguous prefix.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or the prefix is ambiguous.
    fn find(&self, id: &str) -> Result<Option<SessionRecord>, SessionError>;
    /// Creates a logical session record.
    ///
    /// # Errors
    /// Returns an error when the record cannot be persisted.
    fn create(
        &self,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        title: &str,
        model: Option<&str>,
    ) -> Result<SessionRecord, SessionError>;
    /// Marks a session as recently used.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn touch(&self, id: &str) -> Result<(), SessionError>;
    /// Updates the model associated with a session.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn update_model(&self, id: &str, model: Option<&str>) -> Result<(), SessionError>;
    /// Lists cached models for a provider.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError>;
    /// Replaces the cached model catalog for a provider.
    ///
    /// # Errors
    /// Returns an error when the transaction cannot be committed.
    fn replace_models(&self, provider: &str, models: &[ModelInfo]) -> Result<(), SessionError>;
    /// Sets the model used by default for new sessions on a provider.
    ///
    /// # Errors
    /// Returns an error when the preference cannot be persisted.
    fn set_default_model(&self, provider: &str, model: &str) -> Result<(), SessionError>;
    /// Lists configured providers.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError>;
    /// Changes whether a provider accepts new work.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError>;
    /// Saves provider credential metadata and its native-store reference.
    ///
    /// # Errors
    /// Returns an error when the credential record cannot be persisted.
    fn save_provider_credential(
        &self,
        provider: &str,
        kind: &str,
        metadata: &serde_json::Value,
    ) -> Result<(), SessionError>;
    /// Removes a provider credential record.
    ///
    /// # Errors
    /// Returns an error when the credential record cannot be removed.
    fn delete_provider_credential(&self, provider: &str) -> Result<(), SessionError>;
    /// Saves the current durable projection of a sub-agent run and its transcript.
    ///
    /// # Errors
    /// Returns an error when the transaction cannot be committed.
    fn save_subagent(&self, record: &SubagentRecord) -> Result<(), SessionError>;
    /// Lists the sub-agent runs associated with a logical parent session.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or contains invalid data.
    fn list_subagents(&self, parent_session_id: &str) -> Result<Vec<SubagentRecord>, SessionError>;
}

pub struct SqliteSessionRepository {
    connection: Mutex<Connection>,
    path: PathBuf,
}

impl SqliteSessionRepository {
    /// Returns Nakode's platform-specific application data directory.
    ///
    /// # Errors
    /// Returns an error when the platform does not expose an application data directory.
    pub fn default_data_directory() -> Result<PathBuf, SessionError> {
        ProjectDirs::from("dev", "nakode", "Nakode")
            .map(|project| project.data_local_dir().to_path_buf())
            .ok_or(SessionError::MissingDataDirectory)
    }

    /// Opens the repository in Nakode's platform-specific data directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the data directory or database cannot be opened.
    pub fn open_default() -> Result<Self, SessionError> {
        let directory = Self::default_data_directory()?;
        let new_database = directory.join("sessions.sqlite3");
        let legacy_database = [
            ProjectDirs::from("dev", "nako-agent", "Nako Agent"),
            ProjectDirs::from("dev", "flock", "Flock"),
        ]
        .into_iter()
        .flatten()
        .map(|legacy| legacy.data_local_dir().join("sessions.sqlite3"))
        .find(|legacy| legacy.exists());
        if !new_database.exists()
            && let Some(legacy_database) = legacy_database
        {
            return Self::open(legacy_database);
        }
        std::fs::create_dir_all(&directory).map_err(|source| SessionError::CreateDirectory {
            path: directory.display().to_string(),
            source,
        })?;
        protect_path(&directory, 0o700)?;
        Self::open(new_database)
    }

    /// Opens or creates a repository at `path` and applies its schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or migrated.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionError> {
        let path = path.as_ref();
        let connection = Connection::open(path)?;
        configure_connection(&connection)?;
        protect_path(path, 0o600)?;
        execute_batch_with_busy_retry(
            &connection,
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS sessions (
               id TEXT PRIMARY KEY,
               provider TEXT NOT NULL,
               provider_session_id TEXT NOT NULL,
               workspace TEXT NOT NULL,
               title TEXT NOT NULL,
               model TEXT,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               UNIQUE(provider, provider_session_id)
             );
             CREATE INDEX IF NOT EXISTS sessions_workspace_updated
               ON sessions(workspace, updated_at DESC);
             CREATE TABLE IF NOT EXISTS provider_models (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               is_default INTEGER NOT NULL,
               cached_at INTEGER NOT NULL,
               PRIMARY KEY(provider, model_id)
             );
             CREATE TABLE IF NOT EXISTS provider_model_preferences (
               provider TEXT PRIMARY KEY,
               model_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS providers (
               provider TEXT PRIMARY KEY,
               display_name TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_credentials (
               provider TEXT PRIMARY KEY REFERENCES providers(provider) ON DELETE CASCADE,
               credential_kind TEXT NOT NULL,
               credential_json TEXT NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS native_runtime_sessions (
               provider TEXT NOT NULL,
               session_id TEXT NOT NULL,
               session_json TEXT NOT NULL,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(provider, session_id)
             );
             CREATE TABLE IF NOT EXISTS orchestration_runs (
               parent_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
               id TEXT NOT NULL,
               agent_slug TEXT NOT NULL,
               provider TEXT NOT NULL,
               provider_session_id TEXT,
               objective TEXT NOT NULL,
               status TEXT NOT NULL,
               latest_activity TEXT NOT NULL,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(parent_session_id, id)
             );
             CREATE INDEX IF NOT EXISTS orchestration_runs_parent_created
               ON orchestration_runs(parent_session_id, created_at, id);
             CREATE TABLE IF NOT EXISTS agent_turns (
               parent_session_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               item_key TEXT,
               kind TEXT NOT NULL,
               title TEXT NOT NULL,
               body TEXT NOT NULL,
               status TEXT NOT NULL,
               PRIMARY KEY(parent_session_id, run_id, sequence),
               FOREIGN KEY(parent_session_id, run_id)
                 REFERENCES orchestration_runs(parent_session_id, id) ON DELETE CASCADE
             );",
        )?;
        let has_legacy_models = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'backend_models'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if has_legacy_models {
            connection.execute_batch(
                "INSERT OR IGNORE INTO provider_models
                   (provider, model_id, is_default, cached_at)
                 SELECT provider, model_id, is_default, cached_at FROM backend_models;
                 DROP TABLE backend_models;",
            )?;
        }
        seed_provider_catalog(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            path: path.to_path_buf(),
        })
    }

    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.path
    }

    fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
        Ok(SessionRecord {
            id: row.get(0)?,
            provider: row.get(1)?,
            provider_session_id: row.get(2)?,
            workspace: row.get(3)?,
            title: row.get(4)?,
            model: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
        })
    }
}

fn configure_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    execute_batch_with_busy_retry(connection, "PRAGMA journal_mode = WAL;")
}

fn execute_batch_with_busy_retry(
    connection: &Connection,
    statements: &str,
) -> rusqlite::Result<()> {
    const ATTEMPTS: usize = 100;
    const RETRY_DELAY: Duration = Duration::from_millis(25);

    for attempt in 0..ATTEMPTS {
        match connection.execute_batch(statements) {
            Ok(()) => return Ok(()),
            Err(error) if is_database_busy(&error) && attempt + 1 < ATTEMPTS => {
                std::thread::sleep(RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the final database initialization attempt always returns")
}

fn is_database_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(details, _)
            if matches!(
                details.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(unix)]
fn protect_path(path: &Path, mode: u32) -> Result<(), SessionError> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|source| {
        SessionError::ProtectStorage {
            path: path.display().to_string(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn protect_path(_path: &Path, _mode: u32) -> Result<(), SessionError> {
    Ok(())
}

fn seed_provider_catalog(connection: &Connection) -> Result<(), SessionError> {
    let provider_catalog =
        toml::from_str::<ProviderCatalog>(PROVIDER_CATALOG).map_err(|source| {
            SessionError::InvalidProviderCatalog {
                path: PROVIDER_CATALOG_PATH,
                source,
            }
        })?;
    for provider in provider_catalog.providers {
        connection.execute(
            "INSERT OR IGNORE INTO providers (provider, display_name, enabled, updated_at)
             VALUES (?1, ?2, 0, ?3)",
            params![provider.slug, provider.display_name, unix_timestamp()],
        )?;
    }
    connection.execute(
        "UPDATE providers SET enabled = 0
         WHERE enabled = 1
           AND provider NOT IN (SELECT provider FROM provider_credentials)",
        [],
    )?;
    Ok(())
}

impl SessionRepository for SqliteSessionRepository {
    fn list_recent(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, provider, provider_session_id, workspace, title, model, created_at, updated_at
             FROM sessions WHERE workspace = ?1 ORDER BY updated_at DESC LIMIT ?2",
        )?;
        let bounded_limit = i64::try_from(limit.min(500)).expect("limit is at most 500");
        let rows = statement.query_map(params![workspace, bounded_limit], Self::row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn find(&self, id: &str) -> Result<Option<SessionRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let exact = connection
            .query_row(
                "SELECT id, provider, provider_session_id, workspace, title, model, created_at, updated_at
                 FROM sessions WHERE id = ?1",
                [id],
                Self::row,
            )
            .optional()?;
        if exact.is_some() {
            return Ok(exact);
        }
        let pattern = format!("{id}%");
        let mut statement = connection.prepare(
            "SELECT id, provider, provider_session_id, workspace, title, model, created_at, updated_at
             FROM sessions WHERE id LIKE ?1 ORDER BY updated_at DESC LIMIT 2",
        )?;
        let matches = statement
            .query_map([pattern], Self::row)?
            .collect::<Result<Vec<_>, _>>()?;
        match matches.as_slice() {
            [] => Ok(None),
            [record] => Ok(Some(record.clone())),
            _ => Err(SessionError::Ambiguous(id.to_owned())),
        }
    }

    fn create(
        &self,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        title: &str,
        model: Option<&str>,
    ) -> Result<SessionRecord, SessionError> {
        let now = unix_timestamp();
        let id = Uuid::now_v7().to_string();
        let title = title.lines().next().unwrap_or("New session").trim();
        let title = if title.is_empty() {
            "New session"
        } else {
            title
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO sessions (id, provider, provider_session_id, workspace, title, model, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
             ON CONFLICT(provider, provider_session_id) DO UPDATE SET updated_at = excluded.updated_at",
            params![id, provider, provider_session_id, workspace, title, model, now],
        )?;
        connection.query_row(
            "SELECT id, provider, provider_session_id, workspace, title, model, created_at, updated_at
             FROM sessions WHERE provider = ?1 AND provider_session_id = ?2",
            params![provider, provider_session_id],
            Self::row,
        ).map_err(Into::into)
    }

    fn touch(&self, id: &str) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
            params![unix_timestamp(), id],
        )?;
        Ok(())
    }

    fn update_model(&self, id: &str, model: Option<&str>) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "UPDATE sessions SET model = ?1, updated_at = ?2 WHERE id = ?3",
            params![model, unix_timestamp(), id],
        )?;
        Ok(())
    }

    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, is_default
             FROM provider_models WHERE provider = ?1
             ORDER BY is_default DESC, model_id COLLATE NOCASE",
        )?;
        let model_provider = provider.to_owned();
        let rows = statement.query_map([provider], |row| {
            Ok(ModelInfo {
                provider: model_provider.clone(),
                id: row.get(0)?,
                is_default: row.get::<_, i64>(1)? != 0,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn replace_models(&self, provider: &str, models: &[ModelInfo]) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let preferred = transaction
            .query_row(
                "SELECT model_id FROM provider_model_preferences WHERE provider = ?1",
                [provider],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .filter(|preferred| models.iter().any(|model| model.id == *preferred));
        transaction.execute(
            "DELETE FROM provider_models WHERE provider = ?1",
            [provider],
        )?;
        let now = unix_timestamp();
        {
            let mut statement = transaction.prepare(
                "INSERT INTO provider_models
                 (provider, model_id, is_default, cached_at)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for model in models {
                statement.execute(params![
                    provider,
                    model.id,
                    i64::from(
                        preferred
                            .as_ref()
                            .map_or(model.is_default, |preferred| preferred == &model.id)
                    ),
                    now,
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn set_default_model(&self, provider: &str, model: &str) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO provider_model_preferences (provider, model_id)
             VALUES (?1, ?2)
             ON CONFLICT(provider) DO UPDATE SET model_id = excluded.model_id",
            params![provider, model],
        )?;
        transaction.execute(
            "UPDATE provider_models
             SET is_default = CASE WHEN model_id = ?1 THEN 1 ELSE 0 END
             WHERE provider = ?2",
            params![model, provider],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT p.provider, p.display_name, p.enabled,
                    c.credential_kind, c.credential_json, c.updated_at
             FROM providers p
             LEFT JOIN provider_credentials c ON c.provider = p.provider
             ORDER BY p.display_name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let credential_kind = row.get::<_, Option<String>>(3)?;
            Ok(ProviderRecord {
                provider: row.get(0)?,
                display_name: row.get(1)?,
                enabled: row.get::<_, i64>(2)? != 0,
                credential: credential_kind
                    .map(|kind| {
                        let metadata_source = row.get::<_, String>(4)?;
                        let metadata = serde_json::from_str(&metadata_source).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                        Ok::<ProviderCredentialRecord, rusqlite::Error>(ProviderCredentialRecord {
                            provider: row.get(0)?,
                            kind,
                            metadata,
                            updated_at: row.get(5)?,
                        })
                    })
                    .transpose()?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        if enabled {
            let has_credential = connection
                .query_row(
                    "SELECT 1 FROM provider_credentials WHERE provider = ?1",
                    params![provider],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !has_credential {
                return Err(SessionError::MissingProviderCredential(provider.to_owned()));
            }
        }
        connection.execute(
            "UPDATE providers SET enabled = ?1, updated_at = ?2 WHERE provider = ?3",
            params![i64::from(enabled), unix_timestamp(), provider],
        )?;
        Ok(())
    }

    fn save_provider_credential(
        &self,
        provider: &str,
        kind: &str,
        metadata: &serde_json::Value,
    ) -> Result<(), SessionError> {
        self.connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "INSERT INTO provider_credentials
                   (provider, credential_kind, credential_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(provider) DO UPDATE SET
                   credential_kind = excluded.credential_kind,
                   credential_json = excluded.credential_json,
                   updated_at = excluded.updated_at",
                params![provider, kind, metadata.to_string(), unix_timestamp()],
            )?;
        Ok(())
    }

    fn delete_provider_credential(&self, provider: &str) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE providers SET enabled = 0, updated_at = ?1 WHERE provider = ?2",
            params![unix_timestamp(), provider],
        )?;
        transaction.execute(
            "DELETE FROM provider_credentials WHERE provider = ?1",
            params![provider],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn save_subagent(&self, record: &SubagentRecord) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let now = unix_timestamp();
        transaction.execute(
            "INSERT INTO orchestration_runs
               (parent_session_id, id, agent_slug, provider, provider_session_id, objective,
                status, latest_activity, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
             ON CONFLICT(parent_session_id, id) DO UPDATE SET
               agent_slug = excluded.agent_slug,
               provider = excluded.provider,
               provider_session_id = excluded.provider_session_id,
               objective = excluded.objective,
               status = excluded.status,
               latest_activity = excluded.latest_activity,
               updated_at = excluded.updated_at",
            params![
                record.parent_session_id,
                record.id,
                record.agent,
                record.provider,
                record.provider_session_id,
                record.objective,
                record.status.database_value(),
                record.latest_activity,
                now,
            ],
        )?;
        transaction.execute(
            "DELETE FROM agent_turns WHERE parent_session_id = ?1 AND run_id = ?2",
            params![record.parent_session_id, record.id],
        )?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO agent_turns
                   (parent_session_id, run_id, sequence, item_key, kind, title, body, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for (sequence, entry) in record.transcript.iter().enumerate() {
                let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
                statement.execute(params![
                    record.parent_session_id,
                    record.id,
                    sequence,
                    entry.key,
                    entry_kind_value(entry.kind),
                    entry.title,
                    entry.body,
                    entry_status_value(entry.status),
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn list_subagents(&self, parent_session_id: &str) -> Result<Vec<SubagentRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, agent_slug, provider, provider_session_id, objective, status,
                    latest_activity
             FROM orchestration_runs
             WHERE parent_session_id = ?1
             ORDER BY created_at, id",
        )?;
        let rows = statement.query_map([parent_session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        let stored_runs = rows.collect::<Result<Vec<_>, _>>()?;
        let mut records = Vec::with_capacity(stored_runs.len());
        for (id, agent, provider, provider_session_id, objective, status, latest_activity) in
            stored_runs
        {
            let transcript = load_subagent_transcript(&connection, parent_session_id, &id)?;
            records.push(SubagentRecord {
                parent_session_id: parent_session_id.to_owned(),
                id,
                agent,
                provider,
                provider_session_id,
                objective,
                status: SubagentStatus::from_database(&status)?,
                latest_activity,
                transcript,
            });
        }
        Ok(records)
    }
}

fn load_subagent_transcript(
    connection: &Connection,
    parent_session_id: &str,
    run_id: &str,
) -> Result<Vec<TranscriptEntry>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT item_key, kind, title, body, status
         FROM agent_turns
         WHERE parent_session_id = ?1 AND run_id = ?2
         ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![parent_session_id, run_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    rows.map(|row| {
        let (key, kind, title, body, status) = row?;
        Ok(TranscriptEntry {
            key,
            kind: entry_kind_from_value(&kind)?,
            title,
            body,
            status: entry_status_from_value(&status)?,
        })
    })
    .collect()
}

const fn entry_kind_value(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::System => "system",
        EntryKind::User => "user",
        EntryKind::Assistant => "assistant",
        EntryKind::Steering => "steering",
        EntryKind::Reasoning => "reasoning",
        EntryKind::Tool => "tool",
        EntryKind::Diff => "diff",
        EntryKind::Warning => "warning",
        EntryKind::Error => "error",
    }
}

fn entry_kind_from_value(value: &str) -> Result<EntryKind, SessionError> {
    match value {
        "system" => Ok(EntryKind::System),
        "user" => Ok(EntryKind::User),
        "assistant" => Ok(EntryKind::Assistant),
        "steering" => Ok(EntryKind::Steering),
        "reasoning" => Ok(EntryKind::Reasoning),
        "tool" => Ok(EntryKind::Tool),
        "diff" => Ok(EntryKind::Diff),
        "warning" => Ok(EntryKind::Warning),
        "error" => Ok(EntryKind::Error),
        _ => Err(SessionError::InvalidStoredValue {
            field: "agent_turns.kind",
            value: value.to_owned(),
        }),
    }
}

const fn entry_status_value(status: EntryStatus) -> &'static str {
    match status {
        EntryStatus::Running => "running",
        EntryStatus::Complete => "complete",
        EntryStatus::Failed => "failed",
        EntryStatus::Interrupted => "interrupted",
    }
}

fn entry_status_from_value(value: &str) -> Result<EntryStatus, SessionError> {
    match value {
        "running" => Ok(EntryStatus::Running),
        "complete" => Ok(EntryStatus::Complete),
        "failed" => Ok(EntryStatus::Failed),
        "interrupted" => Ok(EntryStatus::Interrupted),
        _ => Err(SessionError::InvalidStoredValue {
            field: "agent_turns.status",
            value: value.to_owned(),
        }),
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_and_orders_sessions() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let first = store.create(
            CODEX_PROVIDER,
            "provider-1",
            "/tmp/project",
            "First prompt",
            Some("model"),
        )?;
        let second = store.create(
            CODEX_PROVIDER,
            "provider-2",
            "/tmp/project",
            "Second prompt",
            None,
        )?;
        assert_eq!(store.find(&first.id)?, Some(first));
        let recent = store.list_recent("/tmp/project", 10)?;
        assert_eq!(recent.len(), 2);
        assert!(recent.iter().any(|record| record.id == second.id));

        let models = vec![
            ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-a".to_owned(),
                is_default: true,
            },
            ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
            },
        ];
        store.update_model(&second.id, Some("model-a"))?;
        assert_eq!(
            store.find(&second.id)?.and_then(|record| record.model),
            Some("model-a".to_owned())
        );
        store.replace_models(CODEX_PROVIDER, &models)?;
        assert_eq!(store.list_models(CODEX_PROVIDER)?, models);
        store.set_default_model(CODEX_PROVIDER, "model-b")?;
        let preferred = store.list_models(CODEX_PROVIDER)?;
        assert_eq!(preferred[0].id, "model-b");
        assert!(preferred[0].is_default);
        assert_eq!(preferred[1].id, "model-a");
        assert!(!preferred[1].is_default);
        store.replace_models(CODEX_PROVIDER, &models)?;
        assert_eq!(store.list_models(CODEX_PROVIDER)?, preferred);
        store.replace_models(CODEX_PROVIDER, &[])?;
        assert!(store.list_models(CODEX_PROVIDER)?.is_empty());
        Ok(())
    }

    #[test]
    fn persists_provider_enablement() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("providers.db"))?;
        let providers = store.list_providers()?;
        assert_eq!(providers.len(), 2);
        assert!(providers.iter().all(|provider| !provider.enabled));

        assert!(matches!(
            store.set_provider_enabled(CODEX_PROVIDER, true),
            Err(SessionError::MissingProviderCredential(provider)) if provider == CODEX_PROVIDER
        ));
        let metadata = serde_json::json!({
            "credential_store": "codex_managed",
            "account": "fixture",
        });
        store.save_provider_credential(CODEX_PROVIDER, "chatgpt_device_code", &metadata)?;
        store.set_provider_enabled(CODEX_PROVIDER, true)?;
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(codex.enabled);
        assert_eq!(
            codex.credential.map(|credential| credential.metadata),
            Some(metadata)
        );
        store.delete_provider_credential(CODEX_PROVIDER)?;
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(!codex.enabled);
        assert!(codex.credential.is_none());

        store.set_provider_enabled(DEVIN_PROVIDER, false)?;
        let devin = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == DEVIN_PROVIDER)
            .expect("Devin provider");
        assert!(!devin.enabled);
        Ok(())
    }

    #[test]
    fn persists_subagent_run_and_transcript_projection() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("subagents.db"))?;
        let parent = store.create(
            CODEX_PROVIDER,
            "parent-provider-session",
            "/tmp/project",
            "Parent work",
            Some("model-a"),
        )?;
        let record = SubagentRecord {
            parent_session_id: parent.id.clone(),
            id: "agent-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: Some("child-provider-session".to_owned()),
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![
                TranscriptEntry {
                    key: None,
                    kind: EntryKind::User,
                    title: "PARENT".to_owned(),
                    body: "Delegated task: Map persistence".to_owned(),
                    status: EntryStatus::Complete,
                },
                TranscriptEntry {
                    key: Some("assistant-1".to_owned()),
                    kind: EntryKind::Assistant,
                    title: "ASSISTANT".to_owned(),
                    body: "Persistence report".to_owned(),
                    status: EntryStatus::Complete,
                },
            ],
        };

        store.save_subagent(&record)?;
        assert_eq!(store.list_subagents(&parent.id)?, vec![record.clone()]);

        let mut updated = record;
        updated.latest_activity = "Reviewed".to_owned();
        updated.transcript[1].body = "Updated persistence report".to_owned();
        store.save_subagent(&updated)?;
        assert_eq!(store.list_subagents(&parent.id)?, vec![updated]);
        Ok(())
    }

    #[test]
    fn migrates_legacy_model_metadata_to_slug_only_catalog() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy-models.db");
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TABLE backend_models (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               display_name TEXT NOT NULL,
               description TEXT NOT NULL,
               is_default INTEGER NOT NULL,
               cached_at INTEGER NOT NULL,
               PRIMARY KEY(provider, model_id)
             );
             INSERT INTO backend_models VALUES
               ('openai-codex', 'legacy-model', 'Old name', 'Old description', 1, 1);",
        )?;
        drop(connection);

        let store = SqliteSessionRepository::open(&path)?;
        assert_eq!(
            store.list_models(CODEX_PROVIDER)?,
            vec![ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "legacy-model".to_owned(),
                is_default: true,
            }]
        );
        let connection = store.connection.lock().expect("database mutex");
        let legacy_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'backend_models'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        assert!(!legacy_exists);
        Ok(())
    }

    #[test]
    fn concurrent_repository_initialization_waits_for_schema_lock() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("concurrent.db");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    SqliteSessionRepository::open(path).expect("concurrent repository open");
                })
            })
            .collect::<Vec<_>>();

        barrier.wait();
        for handle in handles {
            handle.join().expect("repository thread");
        }
    }
}
