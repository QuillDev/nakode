use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use directories::ProjectDirs;
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;
use uuid::Uuid;

use crate::backend::ModelInfo;
pub use crate::backend::{CODEX_PROVIDER, DEVIN_PROVIDER};

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
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not determine Flock's application-data directory")]
    MissingDataDirectory,
    #[error("failed to create session database directory {path}: {source}")]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("session database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("session {0:?} is ambiguous; use a longer id")]
    Ambiguous(String),
}

pub trait SessionRepository: Send + Sync {
    fn list_recent(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, SessionError>;
    fn find(&self, id: &str) -> Result<Option<SessionRecord>, SessionError>;
    fn create(
        &self,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        title: &str,
        model: Option<&str>,
    ) -> Result<SessionRecord, SessionError>;
    fn touch(&self, id: &str) -> Result<(), SessionError>;
    fn update_model(&self, id: &str, model: Option<&str>) -> Result<(), SessionError>;
    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError>;
    fn replace_models(&self, provider: &str, models: &[ModelInfo]) -> Result<(), SessionError>;
    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError>;
    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError>;
}

pub struct SqliteSessionRepository {
    connection: Mutex<Connection>,
}

impl SqliteSessionRepository {
    pub fn open_default() -> Result<Self, SessionError> {
        let project =
            ProjectDirs::from("dev", "flock", "Flock").ok_or(SessionError::MissingDataDirectory)?;
        let directory = project.data_local_dir();
        std::fs::create_dir_all(directory).map_err(|source| SessionError::CreateDirectory {
            path: directory.display().to_string(),
            source,
        })?;
        Self::open(directory.join("sessions.sqlite3"))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionError> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
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
             CREATE TABLE IF NOT EXISTS providers (
               provider TEXT PRIMARY KEY,
               display_name TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               updated_at INTEGER NOT NULL
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
        connection.execute(
            "INSERT OR IGNORE INTO providers (provider, display_name, enabled, updated_at)
             VALUES (?1, ?2, 1, ?3)",
            params![CODEX_PROVIDER, "Codex", unix_timestamp()],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO providers (provider, display_name, enabled, updated_at)
             VALUES (?1, ?2, 1, ?3)",
            params![DEVIN_PROVIDER, "Devin", unix_timestamp()],
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
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
        let rows = statement.query_map(params![workspace, limit.min(500) as i64], Self::row)?;
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
                    i64::from(model.is_default),
                    now,
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT provider, display_name, enabled FROM providers ORDER BY display_name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ProviderRecord {
                provider: row.get(0)?,
                display_name: row.get(1)?,
                enabled: row.get::<_, i64>(2)? != 0,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "UPDATE providers SET enabled = ?1, updated_at = ?2 WHERE provider = ?3",
            params![i64::from(enabled), unix_timestamp(), provider],
        )?;
        Ok(())
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
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

        let models = vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-a".to_owned(),
            is_default: true,
        }];
        store.update_model(&second.id, Some("model-a"))?;
        assert_eq!(
            store.find(&second.id)?.and_then(|record| record.model),
            Some("model-a".to_owned())
        );
        store.replace_models(CODEX_PROVIDER, &models)?;
        assert_eq!(store.list_models(CODEX_PROVIDER)?, models);
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
        assert!(providers.iter().all(|provider| provider.enabled));

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
}
