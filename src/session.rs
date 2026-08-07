use std::{
    fmt::Write as _,
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
    backend::{ModelInfo, ModelOptions},
    credential::CredentialMetadata,
    domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
    memory::{MemoryBackend, MemoryConfig},
    settings::TerminalImageMode,
    vision::VisionConfig,
    web::{WebBackend, WebConfig},
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
    /// Unix epoch seconds at initial persistence; converted exactly once at API projection.
    pub created_at: i64,
    /// Unix epoch seconds at the latest persistence touch; converted exactly once at API projection.
    pub updated_at: i64,
    /// Additional provider-native resources owned by delegated runs beneath this session.
    pub owned_provider_sessions: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionPurgeReport {
    pub sessions: usize,
    pub orchestration_runs: usize,
    pub agent_turns: usize,
    pub native_runtime_sessions: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRecord {
    pub provider: String,
    pub display_name: String,
    pub enabled: bool,
    pub credential: Option<CredentialMetadata>,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubagentObservability {
    pub parent_run_id: Option<String>,
    pub archetype_purpose: String,
    /// Exact delegated archetype definition used for this run, serialized at acceptance time.
    pub policy_json: String,
    pub remaining_delegation_depth: u32,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub termination_kind: Option<String>,
    pub termination_detail: Option<String>,
    pub objective_mismatch_handoff: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentRecord {
    pub parent_session_id: String,
    pub id: String,
    pub agent: String,
    pub provider: String,
    pub model: Option<String>,
    pub provider_session_id: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub objective: String,
    pub status: SubagentStatus,
    pub latest_activity: String,
    pub observability: SubagentObservability,
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
    #[error("session {0:?} was not found")]
    SessionNotFound(String),
    #[error("provider {0:?} was not found")]
    ProviderNotFound(String),
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
    ) -> Result<SessionRecord, SessionError> {
        self.create_with_id(
            &Uuid::now_v7().to_string(),
            provider,
            provider_session_id,
            workspace,
            title,
            model,
        )
    }
    /// Creates a logical session using an identity assigned before provider work begins.
    ///
    /// # Errors
    /// Returns an error when the record cannot be persisted.
    fn create_with_id(
        &self,
        id: &str,
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
    /// Deletes a logical session and every record persisted beneath it.
    ///
    /// Takes the session row, its runs and their turns (by cascade), and the native runtime history
    /// keyed by the provider session it resolves to. That last table carries no foreign key back to
    /// `sessions`, so it is deleted explicitly — a cascade cannot reach it, and it is where the bulk of
    /// a session's bytes actually live.
    ///
    /// # Errors
    /// Returns an error when the session is unknown or the transaction cannot be committed.
    fn delete(&self, id: &str) -> Result<(), SessionError>;
    /// Deletes every logical session and every session-scoped persistence row.
    ///
    /// Unlike repeated single-session deletion, this also removes orphaned native runtime histories
    /// left by dead or partially initialized sessions. Provider credentials, provider/model
    /// preferences, and global add-on configuration are outside this boundary and remain untouched.
    ///
    /// # Errors
    /// Returns an error when the atomic purge transaction cannot be committed.
    fn purge_all(&self) -> Result<SessionPurgeReport, SessionError>;
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
    /// Lists model-specific inference options saved through `/models`.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn list_model_options(
        &self,
        provider: &str,
    ) -> Result<Vec<(String, ModelOptions)>, SessionError>;
    /// Saves model-specific inference options selected through `/models`.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_model_options(
        &self,
        provider: &str,
        model: &str,
        options: &ModelOptions,
    ) -> Result<(), SessionError>;
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
    /// Loads optional web-browser add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_web_config(&self) -> Result<WebConfig, SessionError>;
    /// Saves optional web-browser add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_web_config(&self, config: &WebConfig) -> Result<(), SessionError>;
    /// Loads optional semantic-memory add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_memory_config(&self) -> Result<MemoryConfig, SessionError>;
    /// Saves optional semantic-memory add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_memory_config(&self, config: &MemoryConfig) -> Result<(), SessionError>;
    /// Loads optional vision add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_vision_config(&self) -> Result<VisionConfig, SessionError>;
    /// Saves optional vision add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_vision_config(&self, config: &VisionConfig) -> Result<(), SessionError>;
    /// Loads the terminal image-preview preference.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_terminal_image_mode(&self) -> Result<TerminalImageMode, SessionError>;
    /// Saves the terminal image-preview preference.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_terminal_image_mode(&self, mode: TerminalImageMode) -> Result<(), SessionError>;
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
    #[allow(clippy::too_many_lines)]
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
               capabilities TEXT NOT NULL DEFAULT '{}',
               PRIMARY KEY(provider, model_id)
             );
             CREATE TABLE IF NOT EXISTS provider_model_preferences (
               provider TEXT PRIMARY KEY,
               model_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_model_options (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               reasoning_effort TEXT,
               fast_mode INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(provider, model_id)
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
               model TEXT,
               provider_session_id TEXT,
               input_tokens INTEGER NOT NULL DEFAULT 0,
               output_tokens INTEGER NOT NULL DEFAULT 0,
               cached_input_tokens INTEGER NOT NULL DEFAULT 0,
               cache_write_tokens INTEGER NOT NULL DEFAULT 0,
               objective TEXT NOT NULL,
               status TEXT NOT NULL,
               latest_activity TEXT NOT NULL,
               parent_run_id TEXT,
               archetype_purpose TEXT NOT NULL DEFAULT '',
               policy_json TEXT NOT NULL DEFAULT '{}',
               remaining_delegation_depth INTEGER NOT NULL DEFAULT 0,
               started_at_ms INTEGER NOT NULL DEFAULT 0,
               ended_at_ms INTEGER,
               termination_kind TEXT,
               termination_detail TEXT,
               objective_mismatch_handoff TEXT,
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
               entry_id TEXT NOT NULL,
               item_key TEXT,
               kind TEXT NOT NULL,
               title TEXT NOT NULL,
               body TEXT NOT NULL,
               status TEXT NOT NULL,
               provider_id TEXT,
               model_id TEXT,
               tool_audit_json TEXT,
               PRIMARY KEY(parent_session_id, run_id, sequence),
               FOREIGN KEY(parent_session_id, run_id)
                 REFERENCES orchestration_runs(parent_session_id, id) ON DELETE CASCADE
             );",
        )?;
        let orchestration_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(orchestration_runs)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut orchestration_migration = String::from("BEGIN IMMEDIATE;\n");
        if !orchestration_columns.iter().any(|column| column == "model") {
            orchestration_migration
                .push_str("ALTER TABLE orchestration_runs ADD COLUMN model TEXT;\n");
        }
        for (column, definition) in [
            ("input_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("output_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("cached_input_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("cache_write_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("parent_run_id", "TEXT"),
            ("archetype_purpose", "TEXT NOT NULL DEFAULT ''"),
            ("policy_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("remaining_delegation_depth", "INTEGER NOT NULL DEFAULT 0"),
            ("started_at_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("ended_at_ms", "INTEGER"),
            ("termination_kind", "TEXT"),
            ("termination_detail", "TEXT"),
            ("objective_mismatch_handoff", "TEXT"),
        ] {
            if !orchestration_columns
                .iter()
                .any(|existing| existing == column)
            {
                writeln!(
                    orchestration_migration,
                    "ALTER TABLE orchestration_runs ADD COLUMN {column} {definition};"
                )
                .expect("writing to a String cannot fail");
            }
        }
        orchestration_migration.push_str("COMMIT;");
        execute_batch_with_busy_retry(&connection, &orchestration_migration)?;
        let agent_turn_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(agent_turns)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        if !agent_turn_columns.iter().any(|column| column == "entry_id") {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE agent_turns ADD COLUMN entry_id TEXT;
                 UPDATE agent_turns
                 SET entry_id = parent_session_id || ':' || run_id || ':' || sequence
                 WHERE entry_id IS NULL;
                 COMMIT;",
            )?;
        }
        for column in ["provider_id", "model_id", "tool_audit_json"] {
            if !agent_turn_columns.iter().any(|existing| existing == column) {
                execute_batch_with_busy_retry(
                    &connection,
                    &format!("ALTER TABLE agent_turns ADD COLUMN {column} TEXT;"),
                )?;
            }
        }
        let has_model_specific_options = {
            let mut statement = connection.prepare("PRAGMA table_info(provider_model_options)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            columns.iter().any(|column| column == "model_id")
        };
        if !has_model_specific_options {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE provider_model_options RENAME TO provider_model_options_legacy;
                 CREATE TABLE provider_model_options (
                   provider TEXT NOT NULL,
                   model_id TEXT NOT NULL,
                   reasoning_effort TEXT,
                   fast_mode INTEGER NOT NULL DEFAULT 0,
                   PRIMARY KEY(provider, model_id)
                 );
                 INSERT INTO provider_model_options
                   (provider, model_id, reasoning_effort, fast_mode)
                 SELECT provider, '*', reasoning_effort, fast_mode
                 FROM provider_model_options_legacy;
                 DROP TABLE provider_model_options_legacy;
                 COMMIT;",
            )?;
        }
        let has_model_capabilities = {
            let mut statement = connection.prepare("PRAGMA table_info(provider_models)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            columns.iter().any(|column| column == "capabilities")
        };
        if !has_model_capabilities {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE provider_models
                   ADD COLUMN capabilities TEXT NOT NULL DEFAULT '{}';
                 COMMIT;",
            )?;
        }
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
            owned_provider_sessions: Vec::new(),
        })
    }
}

fn load_owned_provider_sessions(
    connection: &Connection,
    parent_session_id: &str,
) -> Result<Vec<(String, String)>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT provider, provider_session_id
         FROM orchestration_runs
         WHERE parent_session_id = ?1 AND provider_session_id IS NOT NULL
         GROUP BY provider, provider_session_id
         ORDER BY MIN(created_at), MIN(id)",
    )?;
    statement
        .query_map([parent_session_id], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
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

fn table_row_count(
    transaction: &rusqlite::Transaction<'_>,
    table: &str,
) -> Result<usize, SessionError> {
    debug_assert!(matches!(
        table,
        "sessions" | "orchestration_runs" | "agent_turns" | "native_runtime_sessions"
    ));
    let count = transaction.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
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
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS addon_web_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           backend TEXT NOT NULL,
           firecrawl_api_key TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS addon_memory_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           backend TEXT NOT NULL,
           executable TEXT NOT NULL,
           global_bank TEXT NOT NULL,
           data_directory TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS addon_vision_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           model TEXT
         );
         CREATE TABLE IF NOT EXISTS terminal_image_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           mode TEXT NOT NULL
         );",
    )?;
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
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for record in &mut records {
            record.owned_provider_sessions = load_owned_provider_sessions(&connection, &record.id)?;
        }
        Ok(records)
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
        if let Some(mut exact) = exact {
            exact.owned_provider_sessions = load_owned_provider_sessions(&connection, &exact.id)?;
            return Ok(Some(exact));
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
            [record] => {
                let mut record = record.clone();
                record.owned_provider_sessions =
                    load_owned_provider_sessions(&connection, &record.id)?;
                Ok(Some(record))
            }
            _ => Err(SessionError::Ambiguous(id.to_owned())),
        }
    }

    fn create_with_id(
        &self,
        id: &str,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        title: &str,
        model: Option<&str>,
    ) -> Result<SessionRecord, SessionError> {
        let now = unix_timestamp();
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
        let updated = connection.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
            params![unix_timestamp(), id],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn delete(&self, id: &str) -> Result<(), SessionError> {
        // Resolved through `find` so deletion accepts exactly the ids every other call does, prefixes
        // included, and so an ambiguous prefix is refused here rather than deleting an arbitrary match.
        let record = self
            .find(id)?
            .ok_or_else(|| SessionError::SessionNotFound(id.to_owned()))?;
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        // The transcript first. It is keyed by the PROVIDER session id and has no foreign key to
        // `sessions`, so nothing removes it on our behalf; dropping the parent row first would leave it
        // unreachable and permanent — which is the orphan this table already accumulates.
        transaction.execute(
            "DELETE FROM native_runtime_sessions WHERE provider = ?1 AND session_id = ?2",
            params![record.provider, record.provider_session_id],
        )?;
        // Then the session itself. `orchestration_runs` and `agent_turns` go with it by cascade, which
        // the connection's `PRAGMA foreign_keys = ON` is what makes true.
        let removed =
            transaction.execute("DELETE FROM sessions WHERE id = ?1", params![record.id])?;
        transaction.commit()?;
        if removed == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn purge_all(&self) -> Result<SessionPurgeReport, SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let report = SessionPurgeReport {
            sessions: table_row_count(&transaction, "sessions")?,
            orchestration_runs: table_row_count(&transaction, "orchestration_runs")?,
            agent_turns: table_row_count(&transaction, "agent_turns")?,
            native_runtime_sessions: table_row_count(&transaction, "native_runtime_sessions")?,
        };
        // Runtime histories intentionally have no parent foreign key and may exist without a complete
        // logical session. Clear the authoritative session-runtime table directly before cascading the
        // logical session hierarchy.
        transaction.execute("DELETE FROM native_runtime_sessions", [])?;
        transaction.execute("DELETE FROM sessions", [])?;
        transaction.commit()?;
        Ok(report)
    }

    fn update_model(&self, id: &str, model: Option<&str>) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE sessions SET model = ?1, updated_at = ?2 WHERE id = ?3",
            params![model, unix_timestamp(), id],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, is_default, capabilities
             FROM provider_models WHERE provider = ?1
             ORDER BY is_default DESC, model_id COLLATE NOCASE",
        )?;
        let model_provider = provider.to_owned();
        let rows = statement.query_map([provider], |row| {
            Ok(ModelInfo {
                provider: model_provider.clone(),
                id: row.get(0)?,
                is_default: row.get::<_, i64>(1)? != 0,
                capabilities: row
                    .get::<_, String>(2)
                    .ok()
                    .and_then(|encoded| serde_json::from_str(&encoded).ok())
                    .unwrap_or_default(),
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
                 (provider, model_id, is_default, cached_at, capabilities)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
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
                    serde_json::to_string(&model.capabilities)
                        .expect("model capabilities serialize"),
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

    fn list_model_options(
        &self,
        provider: &str,
    ) -> Result<Vec<(String, ModelOptions)>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, reasoning_effort, fast_mode
             FROM provider_model_options WHERE provider = ?1
             ORDER BY model_id COLLATE NOCASE",
        )?;
        let rows = statement.query_map([provider], |row| {
            Ok((
                row.get(0)?,
                ModelOptions {
                    reasoning_effort: row.get(1)?,
                    fast_mode: row.get::<_, i64>(2)? != 0,
                },
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn save_model_options(
        &self,
        provider: &str,
        model: &str,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO provider_model_options (provider, model_id, reasoning_effort, fast_mode)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(provider, model_id) DO UPDATE SET
               reasoning_effort = excluded.reasoning_effort,
               fast_mode = excluded.fast_mode",
            params![
                provider,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode)
            ],
        )?;
        Ok(())
    }

    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT p.provider, p.display_name, p.enabled,
                    c.credential_kind, c.updated_at
             FROM providers p
             LEFT JOIN provider_credentials c ON c.provider = p.provider
             ORDER BY p.display_name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let provider = row.get::<_, String>(0)?;
            let credential_kind = row.get::<_, Option<String>>(3)?;
            let credential_updated_at = row.get::<_, Option<i64>>(4)?;
            Ok(ProviderRecord {
                provider: provider.clone(),
                display_name: row.get(1)?,
                enabled: row.get::<_, i64>(2)? != 0,
                credential: credential_kind
                    .zip(credential_updated_at)
                    .map(|(kind, updated_at)| CredentialMetadata {
                        provider,
                        kind,
                        updated_at,
                    }),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE providers SET enabled = ?1, updated_at = ?2 WHERE provider = ?3",
            params![i64::from(enabled), unix_timestamp(), provider],
        )?;
        if updated == 0 {
            return Err(SessionError::ProviderNotFound(provider.to_owned()));
        }
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
               (parent_session_id, id, agent_slug, provider, model, provider_session_id,
                input_tokens, output_tokens, cached_input_tokens, cache_write_tokens, objective,
                status, latest_activity, parent_run_id, archetype_purpose, policy_json,
                remaining_delegation_depth, started_at_ms, ended_at_ms, termination_kind,
                termination_detail, objective_mismatch_handoff, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                     ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?23)
             ON CONFLICT(parent_session_id, id) DO UPDATE SET
               agent_slug = excluded.agent_slug,
               provider = excluded.provider,
               model = excluded.model,
               provider_session_id = excluded.provider_session_id,
               input_tokens = excluded.input_tokens,
               output_tokens = excluded.output_tokens,
               cached_input_tokens = excluded.cached_input_tokens,
               cache_write_tokens = excluded.cache_write_tokens,
               objective = excluded.objective,
               status = excluded.status,
               latest_activity = excluded.latest_activity,
               parent_run_id = excluded.parent_run_id,
               archetype_purpose = excluded.archetype_purpose,
               policy_json = excluded.policy_json,
               remaining_delegation_depth = excluded.remaining_delegation_depth,
               started_at_ms = excluded.started_at_ms,
               ended_at_ms = excluded.ended_at_ms,
               termination_kind = excluded.termination_kind,
               termination_detail = excluded.termination_detail,
               objective_mismatch_handoff = excluded.objective_mismatch_handoff,
               updated_at = excluded.updated_at",
            params![
                record.parent_session_id,
                record.id,
                record.agent,
                record.provider,
                record.model,
                record.provider_session_id,
                i64::try_from(record.input_tokens).unwrap_or(i64::MAX),
                i64::try_from(record.output_tokens).unwrap_or(i64::MAX),
                i64::try_from(record.cached_input_tokens).unwrap_or(i64::MAX),
                i64::try_from(record.cache_write_tokens).unwrap_or(i64::MAX),
                record.objective,
                record.status.database_value(),
                record.latest_activity,
                record.observability.parent_run_id,
                record.observability.archetype_purpose,
                record.observability.policy_json,
                i64::from(record.observability.remaining_delegation_depth),
                i64::try_from(record.observability.started_at_ms).unwrap_or(i64::MAX),
                record
                    .observability
                    .ended_at_ms
                    .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                record.observability.termination_kind,
                record.observability.termination_detail,
                record.observability.objective_mismatch_handoff,
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
                   (parent_session_id, run_id, sequence, entry_id, item_key, kind, title, body, status,
                    provider_id, model_id, tool_audit_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )?;
            for (sequence, entry) in record.transcript.iter().enumerate() {
                let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
                statement.execute(params![
                    record.parent_session_id,
                    record.id,
                    sequence,
                    entry.id,
                    entry.key,
                    entry_kind_value(entry.kind),
                    entry.title,
                    entry.body,
                    entry_status_value(entry.status),
                    entry.provider_id,
                    entry.model_id,
                    entry.tool_audit_json,
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
            "SELECT id, agent_slug, provider, model, provider_session_id,
                    input_tokens, output_tokens, cached_input_tokens, cache_write_tokens,
                    objective, status, latest_activity, parent_run_id, archetype_purpose,
                    policy_json, remaining_delegation_depth, started_at_ms, ended_at_ms,
                    termination_kind, termination_detail, objective_mismatch_handoff
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
                row.get::<_, Option<String>>(4)?,
                u64::try_from(row.get::<_, i64>(5)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(6)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(7)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(8)?).unwrap_or_default(),
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
                u32::try_from(row.get::<_, i64>(15)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(16)?).unwrap_or_default(),
                row.get::<_, Option<i64>>(17)?
                    .map(|value| u64::try_from(value).unwrap_or_default()),
                row.get::<_, Option<String>>(18)?,
                row.get::<_, Option<String>>(19)?,
                row.get::<_, Option<String>>(20)?,
            ))
        })?;
        let stored_runs = rows.collect::<Result<Vec<_>, _>>()?;
        let mut records = Vec::with_capacity(stored_runs.len());
        for (
            id,
            agent,
            provider,
            model,
            provider_session_id,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            cache_write_tokens,
            objective,
            status,
            latest_activity,
            parent_run_id,
            archetype_purpose,
            policy_json,
            remaining_delegation_depth,
            started_at_ms,
            ended_at_ms,
            termination_kind,
            termination_detail,
            objective_mismatch_handoff,
        ) in stored_runs
        {
            let transcript = load_subagent_transcript(&connection, parent_session_id, &id)?;
            records.push(SubagentRecord {
                parent_session_id: parent_session_id.to_owned(),
                id,
                agent,
                provider,
                model,
                provider_session_id,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_write_tokens,
                objective,
                status: SubagentStatus::from_database(&status)?,
                latest_activity,
                observability: SubagentObservability {
                    parent_run_id,
                    archetype_purpose,
                    policy_json,
                    remaining_delegation_depth,
                    started_at_ms,
                    ended_at_ms,
                    termination_kind,
                    termination_detail,
                    objective_mismatch_handoff,
                },
                transcript,
            });
        }
        Ok(records)
    }

    fn load_web_config(&self) -> Result<WebConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let stored = connection
            .query_row(
                "SELECT backend, firecrawl_api_key FROM addon_web_settings WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((backend, firecrawl_api_key)) = stored else {
            return Ok(WebConfig::default());
        };
        let backend = match backend.as_str() {
            "disabled" => WebBackend::Disabled,
            "agent-browser" => WebBackend::AgentBrowser,
            "firecrawl" => WebBackend::Firecrawl,
            _ => {
                return Err(SessionError::InvalidStoredValue {
                    field: "addon_web_settings.backend",
                    value: backend,
                });
            }
        };
        Ok(WebConfig {
            backend,
            firecrawl_api_key,
        })
    }

    fn save_web_config(&self, config: &WebConfig) -> Result<(), SessionError> {
        let backend = match config.backend {
            WebBackend::Disabled => "disabled",
            WebBackend::AgentBrowser => "agent-browser",
            WebBackend::Firecrawl => "firecrawl",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_web_settings (singleton, backend, firecrawl_api_key) VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET backend = excluded.backend, firecrawl_api_key = excluded.firecrawl_api_key",
            params![backend, config.firecrawl_api_key],
        )?;
        Ok(())
    }

    fn load_memory_config(&self) -> Result<MemoryConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let stored = connection
            .query_row(
                "SELECT backend, executable, global_bank, data_directory FROM addon_memory_settings WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((backend, executable, global_bank, data_directory)) = stored else {
            return Ok(MemoryConfig::default());
        };
        let backend = match backend.as_str() {
            "disabled" => MemoryBackend::Disabled,
            "mnemosyne" => MemoryBackend::Mnemosyne,
            _ => {
                return Err(SessionError::InvalidStoredValue {
                    field: "addon_memory_settings.backend",
                    value: backend,
                });
            }
        };
        Ok(MemoryConfig {
            backend,
            executable,
            global_bank,
            data_directory,
        })
    }

    fn save_memory_config(&self, config: &MemoryConfig) -> Result<(), SessionError> {
        let backend = match config.backend {
            MemoryBackend::Disabled => "disabled",
            MemoryBackend::Mnemosyne => "mnemosyne",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_memory_settings (singleton, backend, executable, global_bank, data_directory) VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(singleton) DO UPDATE SET backend = excluded.backend, executable = excluded.executable, global_bank = excluded.global_bank, data_directory = excluded.data_directory",
            params![backend, config.executable, config.global_bank, config.data_directory],
        )?;
        Ok(())
    }

    fn load_vision_config(&self) -> Result<VisionConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let model = connection
            .query_row(
                "SELECT model FROM addon_vision_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        Ok(VisionConfig { model })
    }

    fn save_vision_config(&self, config: &VisionConfig) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_vision_settings (singleton, model) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET model = excluded.model",
            params![config.model],
        )?;
        Ok(())
    }

    fn load_terminal_image_mode(&self) -> Result<TerminalImageMode, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mode = connection
            .query_row(
                "SELECT mode FROM terminal_image_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(match mode.as_deref() {
            Some("on") => TerminalImageMode::On,
            Some("off") => TerminalImageMode::Off,
            _ => TerminalImageMode::Auto,
        })
    }

    fn save_terminal_image_mode(&self, mode: TerminalImageMode) -> Result<(), SessionError> {
        let mode = match mode {
            TerminalImageMode::Auto => "auto",
            TerminalImageMode::On => "on",
            TerminalImageMode::Off => "off",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO terminal_image_settings (singleton, mode) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET mode = excluded.mode",
            [mode],
        )?;
        Ok(())
    }
}

fn load_subagent_transcript(
    connection: &Connection,
    parent_session_id: &str,
    run_id: &str,
) -> Result<Vec<TranscriptEntry>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT entry_id, item_key, kind, title, body, status, provider_id, model_id,
                tool_audit_json
         FROM agent_turns
         WHERE parent_session_id = ?1 AND run_id = ?2
         ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![parent_session_id, run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
        ))
    })?;
    rows.map(|row| {
        let (id, key, kind, title, body, status, provider_id, model_id, tool_audit_json) = row?;
        Ok(TranscriptEntry {
            id,
            key,
            kind: entry_kind_from_value(&kind)?,
            title,
            body,
            status: entry_status_from_value(&status)?,
            provider_id,
            model_id,
            tool_audit_json,
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
    use crate::credential::{Credential, CredentialStore, SecretValue};

    #[test]
    fn terminal_image_mode_defaults_to_auto_and_persists() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_terminal_image_mode()?, TerminalImageMode::Auto);

        store.save_terminal_image_mode(TerminalImageMode::Off)?;
        assert_eq!(store.load_terminal_image_mode()?, TerminalImageMode::Off);
        Ok(())
    }

    #[test]
    fn model_options_are_saved_per_model() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert!(store.list_model_options(CODEX_PROVIDER)?.is_empty());

        let high_fast = ModelOptions {
            reasoning_effort: Some("high".to_owned()),
            fast_mode: true,
        };
        let low_standard = ModelOptions {
            reasoning_effort: Some("low".to_owned()),
            fast_mode: false,
        };
        store.save_model_options(CODEX_PROVIDER, "model-a", &high_fast)?;
        store.save_model_options(CODEX_PROVIDER, "model-b", &low_standard)?;
        assert_eq!(
            store.list_model_options(CODEX_PROVIDER)?,
            vec![
                ("model-a".to_owned(), high_fast),
                ("model-b".to_owned(), low_standard),
            ]
        );
        Ok(())
    }

    #[test]
    fn migrates_provider_wide_model_options_to_a_wildcard_profile() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy-options.db");
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TABLE provider_model_options (
               provider TEXT PRIMARY KEY,
               reasoning_effort TEXT,
               fast_mode INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO provider_model_options VALUES ('openai-codex', 'high', 1);",
        )?;
        drop(connection);

        let store = SqliteSessionRepository::open(&path)?;
        assert_eq!(
            store.list_model_options(CODEX_PROVIDER)?,
            vec![(
                "*".to_owned(),
                ModelOptions {
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: true,
                },
            )]
        );
        Ok(())
    }

    #[test]
    fn vision_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_vision_config()?, VisionConfig::default());

        let configured = VisionConfig {
            model: Some("openai-codex/gpt-5.4".to_owned()),
        };
        store.save_vision_config(&configured)?;
        assert_eq!(store.load_vision_config()?, configured);
        Ok(())
    }

    #[test]
    fn browser_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_web_config()?, WebConfig::default());

        let configured = WebConfig {
            backend: WebBackend::Firecrawl,
            firecrawl_api_key: "secret".to_owned(),
        };
        store.save_web_config(&configured)?;
        assert_eq!(store.load_web_config()?, configured);
        Ok(())
    }

    #[test]
    fn memory_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_memory_config()?, MemoryConfig::default());

        let configured = MemoryConfig {
            backend: MemoryBackend::Mnemosyne,
            executable: "/opt/bin/mnemosyne".to_owned(),
            global_bank: "my-global-memory".to_owned(),
            data_directory: "/tmp/memory".to_owned(),
        };
        store.save_memory_config(&configured)?;
        assert_eq!(store.load_memory_config()?, configured);
        Ok(())
    }

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
                capabilities: crate::backend::ModelCapabilities {
                    reasoning_efforts: vec!["low".to_owned(), "high".to_owned()],
                },
            },
            ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
                capabilities: crate::codex::model_capabilities(),
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
        let path = directory.path().join("providers.db");
        let store = SqliteSessionRepository::open(&path)?;
        let credentials =
            crate::credential::SqliteCredentialStore::open(&path).expect("credential store");
        let providers = store.list_providers()?;
        assert_eq!(providers.len(), 6);
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::CLAUDE_PROVIDER)
        );
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::KIMI_PROVIDER)
        );
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::GLM_PROVIDER)
        );
        assert!(providers.iter().all(|provider| !provider.enabled));

        let metadata = serde_json::json!({
            "credential_store": "codex_managed",
            "account": "fixture",
        });
        crate::credential::CredentialStore::put(
            &credentials,
            CODEX_PROVIDER,
            &crate::credential::Credential {
                kind: "chatgpt_device_code".to_owned(),
                secret: crate::credential::SecretValue::new(metadata),
            },
        )
        .expect("save credential");
        store.set_provider_enabled(CODEX_PROVIDER, true)?;
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(codex.enabled);
        assert_eq!(
            codex.credential.map(|credential| credential.kind),
            Some("chatgpt_device_code".to_owned())
        );
        store.set_provider_enabled(CODEX_PROVIDER, false)?;
        crate::credential::CredentialStore::delete(&credentials, CODEX_PROVIDER)
            .expect("delete credential");
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
    fn rejects_mutations_for_unknown_records() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("unknown.db"))?;

        assert!(matches!(
            store.touch("missing-session"),
            Err(SessionError::SessionNotFound(id)) if id == "missing-session"
        ));
        assert!(matches!(
            store.update_model("missing-session", Some("model")),
            Err(SessionError::SessionNotFound(id)) if id == "missing-session"
        ));
        assert!(matches!(
            store.set_provider_enabled("missing-provider", true),
            Err(SessionError::ProviderNotFound(provider)) if provider == "missing-provider"
        ));
        Ok(())
    }

    /// Deleting a session must take everything under it, including the one table no cascade reaches.
    ///
    /// `native_runtime_sessions` holds the transcripts and has no foreign key to `sessions`, so it is
    /// the table a naive `DELETE FROM sessions` leaves behind — permanently, since nothing else keys off
    /// it. That orphan is the whole reason this test writes a row into it by hand.
    #[test]
    fn deletes_a_session_with_its_runs_and_native_history() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let doomed = store.create(
            CODEX_PROVIDER,
            "doomed-provider-session",
            "/tmp/project",
            "Finished work",
            Some("model-a"),
        )?;
        let kept = store.create(
            CODEX_PROVIDER,
            "kept-provider-session",
            "/tmp/project",
            "Live work",
            Some("model-a"),
        )?;
        store.save_subagent(&SubagentRecord {
            parent_session_id: doomed.id.clone(),
            id: "agent-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: Some("child-provider-session".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "entry-1".to_owned(),
                key: None,
                kind: EntryKind::User,
                title: "PARENT".to_owned(),
                body: "Delegated task".to_owned(),
                status: EntryStatus::Complete,
                provider_id: None,
                model_id: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
        })?;
        let native_rows = |provider_session_id: &str| -> Result<i64, SessionError> {
            let connection = store
                .connection
                .lock()
                .expect("session database mutex poisoned");
            Ok(connection.query_row(
                "SELECT COUNT(*) FROM native_runtime_sessions WHERE provider = ?1 AND session_id = ?2",
                params![CODEX_PROVIDER, provider_session_id],
                |row| row.get::<_, i64>(0),
            )?)
        };
        {
            let connection = store
                .connection
                .lock()
                .expect("session database mutex poisoned");
            for provider_session_id in ["doomed-provider-session", "kept-provider-session"] {
                connection.execute(
                    "INSERT INTO native_runtime_sessions (provider, session_id, session_json, updated_at)
                     VALUES (?1, ?2, ?3, unixepoch())",
                    params![CODEX_PROVIDER, provider_session_id, "{}"],
                )?;
            }
        }
        assert_eq!(native_rows("doomed-provider-session")?, 1);
        assert_eq!(store.list_subagents(&doomed.id)?.len(), 1);

        store.delete(&doomed.id)?;

        assert_eq!(store.find(&doomed.id)?, None);
        // The cascade, which only fires because the connection sets `PRAGMA foreign_keys = ON`.
        assert!(store.list_subagents(&doomed.id)?.is_empty());
        // The table no cascade reaches.
        assert_eq!(native_rows("doomed-provider-session")?, 0);
        // And nothing belonging to the session beside it moved.
        assert_eq!(store.find(&kept.id)?, Some(kept));
        assert_eq!(native_rows("kept-provider-session")?, 1);

        // Deleting what is already gone is an error, not a silent success: a caller retrying a failed
        // cleanup should be told the id means nothing rather than believing it removed something.
        assert!(matches!(
            store.delete(&doomed.id),
            Err(SessionError::SessionNotFound(id)) if id == doomed.id
        ));
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
            model: None,
            provider_session_id: Some("child-provider-session".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![
                TranscriptEntry {
                    id: "entry-1".to_owned(),
                    key: None,
                    kind: EntryKind::User,
                    title: "PARENT".to_owned(),
                    body: "Delegated task: Map persistence".to_owned(),
                    status: EntryStatus::Complete,
                    provider_id: None,
                    model_id: None,
                    tool_audit_json: None,
                },
                TranscriptEntry {
                    id: "entry-2".to_owned(),
                    key: Some("assistant-1".to_owned()),
                    kind: EntryKind::Assistant,
                    title: "ASSISTANT".to_owned(),
                    body: "Persistence report".to_owned(),
                    status: EntryStatus::Complete,
                    provider_id: Some(CODEX_PROVIDER.to_owned()),
                    model_id: Some("openai-codex/gpt-5.4".to_owned()),
                    tool_audit_json: Some(
                        r#"{"version":1,"callId":"call-1","kind":"native"}"#.to_owned(),
                    ),
                },
            ],
            observability: SubagentObservability {
                parent_run_id: Some("agent-root".to_owned()),
                archetype_purpose: "Read-only persistence scout".to_owned(),
                policy_json: r#"{"slug":"explorer","description":"Read-only persistence scout","tool_profile":"read_only","allowed_capabilities":["filesystem_read"],"denied_capabilities":["filesystem_write"],"allowed_tools":["read","grep"],"denied_tools":["write"],"timeout_seconds":300,"max_turns":5,"require_parent_attribution":true}"#.to_owned(),
                remaining_delegation_depth: 0,
                started_at_ms: 1_000,
                ended_at_ms: Some(1_250),
                termination_kind: Some("completed".to_owned()),
                termination_detail: None,
                objective_mismatch_handoff: None,
            },
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
                capabilities: crate::backend::ModelCapabilities::default(),
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

    /// Seeds two logical sessions, a delegated run with one transcript turn, and two native runtime
    /// histories: one owned by a live session and one orphaned by a partially initialized session.
    fn seed_mixed_session_state(store: &SqliteSessionRepository) -> Result<(), SessionError> {
        let first = store.create(
            CODEX_PROVIDER,
            "provider-first",
            "/tmp/first",
            "First",
            None,
        )?;
        store.create(
            DEVIN_PROVIDER,
            "provider-second",
            "/tmp/second",
            "Second",
            None,
        )?;
        store.save_subagent(&SubagentRecord {
            parent_session_id: first.id,
            id: "run-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: Some("provider-child".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Inspect".to_owned(),
            status: SubagentStatus::Interrupted,
            latest_activity: "stale".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "turn-1".to_owned(),
                key: None,
                kind: EntryKind::Assistant,
                title: "ASSISTANT".to_owned(),
                body: "partial".to_owned(),
                status: EntryStatus::Running,
                provider_id: None,
                model_id: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
        })?;
        let connection = store.connection.lock().expect("database mutex");
        for (provider, session_id) in [
            (CODEX_PROVIDER, "provider-first"),
            (DEVIN_PROVIDER, "orphaned-partial-session"),
        ] {
            connection.execute(
                "INSERT INTO native_runtime_sessions
                 (provider, session_id, session_json, updated_at)
                 VALUES (?1, ?2, '{}', unixepoch())",
                params![provider, session_id],
            )?;
        }
        Ok(())
    }

    #[test]
    fn bulk_purge_removes_all_session_state_and_preserves_global_configuration()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("purge.db");
        let store = SqliteSessionRepository::open(&path)?;
        let credentials =
            crate::credential::SqliteCredentialStore::open(&path).expect("credential store");
        credentials
            .put(
                CODEX_PROVIDER,
                &Credential {
                    kind: "oauth".to_owned(),
                    secret: SecretValue::new(serde_json::json!({"access_token": "keep-me"})),
                },
            )
            .expect("save credential");
        store.set_provider_enabled(CODEX_PROVIDER, true)?;
        store.set_default_model(CODEX_PROVIDER, "model-default")?;
        store.save_web_config(&WebConfig {
            backend: WebBackend::Firecrawl,
            firecrawl_api_key: "keep-web-key".to_owned(),
        })?;

        seed_mixed_session_state(&store)?;

        assert_eq!(
            store.purge_all()?,
            SessionPurgeReport {
                sessions: 2,
                orchestration_runs: 1,
                agent_turns: 1,
                native_runtime_sessions: 2,
            }
        );
        assert!(store.list_recent("/tmp/first", 500)?.is_empty());
        assert!(store.list_recent("/tmp/second", 500)?.is_empty());
        assert_eq!(store.purge_all()?, SessionPurgeReport::default());

        let kept_credential = credentials
            .get(CODEX_PROVIDER)
            .expect("load credential")
            .expect("credential survives");
        assert_eq!(kept_credential.secret.expose()["access_token"], "keep-me");
        assert!(
            store
                .list_providers()?
                .into_iter()
                .find(|provider| provider.provider == CODEX_PROVIDER)
                .expect("provider")
                .enabled
        );
        assert_eq!(
            store.load_web_config()?,
            WebConfig {
                backend: WebBackend::Firecrawl,
                firecrawl_api_key: "keep-web-key".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn failed_bulk_purge_rolls_back_every_session_table() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("purge-failure.db"))?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-session",
            "/tmp/project",
            "Keep on failure",
            None,
        )?;
        {
            let connection = store.connection.lock().expect("database mutex");
            connection.execute_batch(
                "INSERT INTO native_runtime_sessions
                   (provider, session_id, session_json, updated_at)
                 VALUES ('openai-codex', 'provider-session', '{}', unixepoch());
                 CREATE TRIGGER refuse_session_purge BEFORE DELETE ON sessions
                 BEGIN SELECT RAISE(FAIL, 'injected purge failure'); END;",
            )?;
        }

        assert!(store.purge_all().is_err());
        assert!(store.find(&session.id)?.is_some());
        let connection = store.connection.lock().expect("database mutex");
        let histories =
            connection.query_row("SELECT COUNT(*) FROM native_runtime_sessions", [], |row| {
                row.get::<_, i64>(0)
            })?;
        assert_eq!(
            histories, 1,
            "the failed transaction restores runtime history"
        );
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
