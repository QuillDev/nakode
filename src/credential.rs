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
    pub account_id: String,
    pub kind: String,
    pub updated_at: i64,
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("stored credential for {provider} is invalid: {reason}")]
    Invalid { provider: String, reason: String },
    #[error("credential account {account_id} was not found for {provider}")]
    AccountNotFound {
        provider: String,
        account_id: String,
    },
    #[error("credential for {provider} exceeds the {maximum} byte storage limit")]
    TooLarge { provider: String, maximum: usize },
}

pub trait CredentialStore: Send + Sync {
    /// Loads the default credential for one provider. Kept for compatible single-account callers.
    ///
    /// # Errors
    /// Returns an error when storage is unavailable or the stored value is malformed.
    fn get(&self, provider: &str) -> Result<Option<Credential>, CredentialError>;

    /// Loads one account credential.
    ///
    /// # Errors
    /// Returns an error when storage is unavailable or the stored value is malformed.
    fn get_account(
        &self,
        provider: &str,
        account_id: &str,
    ) -> Result<Option<Credential>, CredentialError>;

    /// Atomically inserts or replaces the default provider credential, creating a durable default
    /// account when the provider has not been assigned one yet.
    ///
    /// # Errors
    /// Returns an error when the value is too large, the provider is absent, or the value cannot be
    /// persisted.
    fn put(&self, provider: &str, credential: &Credential) -> Result<(), CredentialError>;

    /// Atomically inserts or replaces one account credential.
    ///
    /// # Errors
    /// Returns an error when the value is too large, the account is absent, or the value cannot be
    /// persisted.
    fn put_account(
        &self,
        provider: &str,
        account_id: &str,
        credential: &Credential,
    ) -> Result<(), CredentialError>;

    /// Loads one MCP server credential from Nakode's protected credential authority.
    ///
    /// # Errors
    /// Returns an error when storage is unavailable or the stored value is malformed.
    fn get_mcp(
        &self,
        workspace: &str,
        server_id: &str,
    ) -> Result<Option<Credential>, CredentialError>;

    /// Atomically saves one MCP server credential.
    ///
    /// # Errors
    /// Returns an error when the value is too large or cannot be persisted.
    fn put_mcp(
        &self,
        workspace: &str,
        server_id: &str,
        credential: &Credential,
    ) -> Result<(), CredentialError>;

    /// Removes one MCP server credential.
    ///
    /// # Errors
    /// Returns an error when storage cannot be updated.
    fn delete_mcp(&self, workspace: &str, server_id: &str) -> Result<(), CredentialError>;

    /// Removes the default provider credential.
    ///
    /// # Errors
    /// Returns an error when storage cannot be updated.
    fn delete(&self, provider: &str) -> Result<(), CredentialError>;

    /// Removes one account credential.
    ///
    /// # Errors
    /// Returns an error when storage cannot be updated.
    fn delete_account(&self, provider: &str, account_id: &str) -> Result<(), CredentialError>;
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
                "SELECT c.credential_kind, c.credential_json
                 FROM provider_accounts a
                 JOIN provider_account_credentials c ON c.account_id = a.account_id
                 WHERE a.provider = ?1 AND a.is_default = 1",
                [provider],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        parse_optional_stored_credential(provider, stored)
    }

    fn get_account(
        &self,
        provider: &str,
        account_id: &str,
    ) -> Result<Option<Credential>, CredentialError> {
        let stored = self
            .connection
            .lock()
            .expect("credential database mutex poisoned")
            .query_row(
                "SELECT c.credential_kind, c.credential_json
                 FROM provider_accounts a
                 JOIN provider_account_credentials c ON c.account_id = a.account_id
                 WHERE a.provider = ?1 AND a.account_id = ?2",
                params![provider, account_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        parse_optional_stored_credential(provider, stored)
    }

    fn put(&self, provider: &str, credential: &Credential) -> Result<(), CredentialError> {
        let serialized = serialize_credential(provider, credential)?;
        let mut connection = self
            .connection
            .lock()
            .expect("credential database mutex poisoned");
        let transaction = connection.transaction()?;
        let account_id = transaction
            .query_row(
                "SELECT account_id FROM provider_accounts
                 WHERE provider = ?1 AND is_default = 1",
                [provider],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let account_id = if let Some(account_id) = account_id {
            account_id
        } else if let Some(account_id) = transaction
            .query_row(
                "SELECT account_id FROM provider_accounts
                 WHERE provider = ?1 ORDER BY account_id LIMIT 1",
                [provider],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            transaction.execute(
                "UPDATE provider_accounts SET is_default = 1, enabled = 1, updated_at = unixepoch()
                 WHERE provider = ?1 AND account_id = ?2",
                params![provider, account_id],
            )?;
            account_id
        } else {
            let account_id = format!("legacy-{provider}");
            let inserted = transaction.execute(
                "INSERT INTO provider_accounts
                   (account_id, provider, label, enabled, is_default, routing_mode,
                    created_at, updated_at)
                 SELECT ?1, provider, 'Default', 1, 1, 'explicit_only', unixepoch(), unixepoch()
                 FROM providers WHERE provider = ?2",
                params![account_id, provider],
            )?;
            if inserted == 0 {
                return Err(CredentialError::Database(
                    rusqlite::Error::QueryReturnedNoRows,
                ));
            }
            account_id
        };
        let affected = transaction.execute(
            "INSERT INTO provider_account_credentials
               (account_id, credential_kind, credential_json, updated_at)
             SELECT account_id, ?3, ?4, unixepoch() FROM provider_accounts
             WHERE provider = ?1 AND account_id = ?2
             ON CONFLICT(account_id) DO UPDATE SET
               credential_kind = excluded.credential_kind,
               credential_json = excluded.credential_json,
               updated_at = excluded.updated_at",
            params![provider, account_id, credential.kind, serialized],
        )?;
        if affected == 0 {
            return Err(CredentialError::AccountNotFound {
                provider: provider.to_owned(),
                account_id,
            });
        }
        transaction.commit()?;
        Ok(())
    }

    fn put_account(
        &self,
        provider: &str,
        account_id: &str,
        credential: &Credential,
    ) -> Result<(), CredentialError> {
        let serialized = serialize_credential(provider, credential)?;
        let affected = self
            .connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "INSERT INTO provider_account_credentials
                   (account_id, credential_kind, credential_json, updated_at)
                 SELECT account_id, ?3, ?4, unixepoch() FROM provider_accounts
                 WHERE provider = ?1 AND account_id = ?2
                 ON CONFLICT(account_id) DO UPDATE SET
                   credential_kind = excluded.credential_kind,
                   credential_json = excluded.credential_json,
                   updated_at = excluded.updated_at",
                params![provider, account_id, credential.kind, serialized],
            )?;
        if affected == 0 {
            return Err(CredentialError::AccountNotFound {
                provider: provider.to_owned(),
                account_id: account_id.to_owned(),
            });
        }
        Ok(())
    }

    fn delete(&self, provider: &str) -> Result<(), CredentialError> {
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "DELETE FROM provider_account_credentials WHERE account_id =
                   (SELECT account_id FROM provider_accounts
                    WHERE provider = ?1 AND is_default = 1)",
                [provider],
            )?;
        Ok(())
    }

    fn delete_account(&self, provider: &str, account_id: &str) -> Result<(), CredentialError> {
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "DELETE FROM provider_account_credentials WHERE account_id IN
                   (SELECT account_id FROM provider_accounts
                    WHERE provider = ?1 AND account_id = ?2)",
                params![provider, account_id],
            )?;
        Ok(())
    }

    fn get_mcp(
        &self,
        workspace: &str,
        server_id: &str,
    ) -> Result<Option<Credential>, CredentialError> {
        let stored = self
            .connection
            .lock()
            .expect("credential database mutex poisoned")
            .query_row(
                "SELECT credential_kind, credential_json FROM mcp_credentials
                 WHERE workspace = ?1 AND server_id = ?2",
                rusqlite::params![workspace, server_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        stored
            .map(|(kind, source)| parse_stored_credential(server_id, kind, &source))
            .transpose()
    }

    fn put_mcp(
        &self,
        workspace: &str,
        server_id: &str,
        credential: &Credential,
    ) -> Result<(), CredentialError> {
        let serialized = serialize_credential(server_id, credential)?;
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "INSERT INTO mcp_credentials
                   (workspace, server_id, credential_kind, credential_json, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(workspace, server_id) DO UPDATE SET
                   credential_kind=excluded.credential_kind,
                   credential_json=excluded.credential_json,
                   updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![
                    workspace,
                    server_id,
                    credential.kind,
                    serialized,
                    unix_time_ms()
                ],
            )?;
        Ok(())
    }

    fn delete_mcp(&self, workspace: &str, server_id: &str) -> Result<(), CredentialError> {
        self.connection
            .lock()
            .expect("credential database mutex poisoned")
            .execute(
                "DELETE FROM mcp_credentials WHERE workspace = ?1 AND server_id = ?2",
                rusqlite::params![workspace, server_id],
            )?;
        Ok(())
    }
}

fn parse_optional_stored_credential(
    owner: &str,
    stored: Option<(String, String)>,
) -> Result<Option<Credential>, CredentialError> {
    stored
        .map(|(kind, source)| parse_stored_credential(owner, kind, &source))
        .transpose()
}

fn parse_stored_credential(
    owner: &str,
    kind: String,
    source: &str,
) -> Result<Credential, CredentialError> {
    if source.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge {
            provider: owner.to_owned(),
            maximum: MAX_CREDENTIAL_BYTES,
        });
    }
    let secret = serde_json::from_str(source).map_err(|error| CredentialError::Invalid {
        provider: owner.to_owned(),
        reason: error.to_string(),
    })?;
    Ok(Credential {
        kind,
        secret: SecretValue::new(secret),
    })
}

fn serialize_credential(owner: &str, credential: &Credential) -> Result<String, CredentialError> {
    let serialized = serde_json::to_string(credential.secret.expose()).map_err(|error| {
        CredentialError::Invalid {
            provider: owner.to_owned(),
            reason: error.to_string(),
        }
    })?;
    if serialized.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge {
            provider: owner.to_owned(),
            maximum: MAX_CREDENTIAL_BYTES,
        });
    }
    Ok(serialized)
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Credential, CredentialError, CredentialStore, SecretValue, SqliteCredentialStore};
    use crate::session::{CODEX_PROVIDER, SessionRepository, SqliteSessionRepository};

    #[test]
    fn sqlite_store_round_trips_replaces_and_deletes_credentials() {
        let directory = tempfile::tempdir().expect("credential directory");
        let path = directory.path().join("credentials.db");
        let sessions = SqliteSessionRepository::open(&path).expect("initialize database");
        let store = SqliteCredentialStore::open(&path).expect("credential store");
        let first = Credential {
            kind: "oauth".to_owned(),
            secret: SecretValue::new(json!({"access_token":"secret-one"})),
        };
        store.put(CODEX_PROVIDER, &first).expect("save credential");
        let accounts = sessions
            .list_providers()
            .expect("list providers")
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("provider")
            .accounts;
        assert_eq!(accounts.len(), 1);
        assert!(accounts[0].is_default);
        assert_eq!(accounts[0].label, "Default");
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
    fn put_account_rejects_an_unknown_account() {
        let directory = tempfile::tempdir().expect("credential directory");
        let path = directory.path().join("credentials.db");
        let _sessions = SqliteSessionRepository::open(&path).expect("initialize database");
        let store = SqliteCredentialStore::open(&path).expect("credential store");
        let credential = Credential {
            kind: "api_key".to_owned(),
            secret: SecretValue::new(json!({"api_key":"secret"})),
        };

        let error = store
            .put_account(CODEX_PROVIDER, "missing-account", &credential)
            .expect_err("unknown account must not report a successful save");
        assert!(matches!(
            error,
            CredentialError::AccountNotFound { provider, account_id }
                if provider == CODEX_PROVIDER && account_id == "missing-account"
        ));
    }

    #[test]
    fn secret_debug_output_is_redacted() {
        let secret = SecretValue::new(json!({"api_key":"must-not-appear"}));
        let rendered = format!("{secret:?}");
        assert_eq!(rendered, "SecretValue([REDACTED])");
        assert!(!rendered.contains("must-not-appear"));
    }
}
