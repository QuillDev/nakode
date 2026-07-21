use std::{fmt, path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use thiserror::Error;

const MAX_CREDENTIAL_BYTES: usize = 64 * 1024;

/// Secret credential payload that deliberately redacts its `Debug` representation.
#[derive(Clone, PartialEq)]
pub struct SecretValue(Value);

impl SecretValue {
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn expose(&self) -> &Value {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> Value {
        self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Credential {
    pub kind: String,
    pub secret: SecretValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialMetadata {
    pub provider: String,
    pub kind: String,
    pub updated_at: i64,
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("stored credential for {provider} is invalid: {reason}")]
    Invalid { provider: String, reason: String },
    #[error("credential for {provider} exceeds the {maximum} byte storage limit")]
    TooLarge { provider: String, maximum: usize },
}

pub trait CredentialStore: Send + Sync {
    /// Loads the credential for one provider.
    ///
    /// # Errors
    /// Returns an error when storage is unavailable or the stored value is malformed.
    fn get(&self, provider: &str) -> Result<Option<Credential>, CredentialError>;

    /// Atomically inserts or replaces one provider credential.
    ///
    /// # Errors
    /// Returns an error when the value is too large or cannot be persisted.
    fn put(&self, provider: &str, credential: &Credential) -> Result<(), CredentialError>;

    /// Removes one provider credential.
    ///
    /// # Errors
    /// Returns an error when storage cannot be updated.
    fn delete(&self, provider: &str) -> Result<(), CredentialError>;
}

/// Default self-contained credential backend using Nakode's protected `SQLite` database.
pub struct SqliteCredentialStore {
    connection: Mutex<Connection>,
}

impl SqliteCredentialStore {
    /// Opens the credential backend over an initialized Nakode database.
    ///
    /// # Errors
    /// Returns an error when `SQLite` cannot open or configure the database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CredentialError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
}

impl CredentialStore for SqliteCredentialStore {
    fn get(&self, provider: &str) -> Result<Option<Credential>, CredentialError> {
        let stored = self
            .connection
            .lock()
            .expect("credential database mutex poisoned")
            .query_row(
                "SELECT credential_kind, credential_json
                 FROM provider_credentials WHERE provider = ?1",
                [provider],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        stored
            .map(|(kind, source)| {
                if source.len() > MAX_CREDENTIAL_BYTES {
                    return Err(CredentialError::TooLarge {
                        provider: provider.to_owned(),
                        maximum: MAX_CREDENTIAL_BYTES,
                    });
                }
                let secret =
                    serde_json::from_str(&source).map_err(|error| CredentialError::Invalid {
                        provider: provider.to_owned(),
                        reason: error.to_string(),
                    })?;
                Ok(Credential {
                    kind,
                    secret: SecretValue::new(secret),
                })
            })
            .transpose()
    }

    fn put(&self, provider: &str, credential: &Credential) -> Result<(), CredentialError> {
        let serialized = serde_json::to_string(credential.secret.expose()).map_err(|error| {
            CredentialError::Invalid {
                provider: provider.to_owned(),
                reason: error.to_string(),
            }
        })?;
        if serialized.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLarge {
                provider: provider.to_owned(),
                maximum: MAX_CREDENTIAL_BYTES,
            });
        }
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "INSERT INTO provider_credentials
                   (provider, credential_kind, credential_json, updated_at)
                 VALUES (?1, ?2, ?3, unixepoch())
                 ON CONFLICT(provider) DO UPDATE SET
                   credential_kind = excluded.credential_kind,
                   credential_json = excluded.credential_json,
                   updated_at = excluded.updated_at",
                params![provider, credential.kind, serialized],
            )?;
        Ok(())
    }

    fn delete(&self, provider: &str) -> Result<(), CredentialError> {
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "DELETE FROM provider_credentials WHERE provider = ?1",
                [provider],
            )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Credential, CredentialStore, SecretValue, SqliteCredentialStore};
    use crate::session::{CODEX_PROVIDER, SqliteSessionRepository};

    #[test]
    fn sqlite_store_round_trips_replaces_and_deletes_credentials() {
        let directory = tempfile::tempdir().expect("credential directory");
        let path = directory.path().join("credentials.db");
        let _sessions = SqliteSessionRepository::open(&path).expect("initialize database");
        let store = SqliteCredentialStore::open(&path).expect("credential store");
        let first = Credential {
            kind: "oauth".to_owned(),
            secret: SecretValue::new(json!({"access_token":"secret-one"})),
        };
        store.put(CODEX_PROVIDER, &first).expect("save credential");
        assert_eq!(
            store.get(CODEX_PROVIDER).expect("load credential"),
            Some(first)
        );

        let replacement = Credential {
            kind: "oauth".to_owned(),
            secret: SecretValue::new(json!({"access_token":"secret-two"})),
        };
        store
            .put(CODEX_PROVIDER, &replacement)
            .expect("replace credential");
        assert_eq!(
            store.get(CODEX_PROVIDER).expect("load replacement"),
            Some(replacement)
        );

        store.delete(CODEX_PROVIDER).expect("delete credential");
        assert!(store.get(CODEX_PROVIDER).expect("load deletion").is_none());
    }

    #[test]
    fn secret_debug_output_is_redacted() {
        let secret = SecretValue::new(json!({"api_key":"must-not-appear"}));
        let rendered = format!("{secret:?}");
        assert_eq!(rendered, "SecretValue([REDACTED])");
        assert!(!rendered.contains("must-not-appear"));
    }
}
