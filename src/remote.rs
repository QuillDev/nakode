use std::{fs, net::SocketAddr, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const REMOTE_CONFIG_FILE: &str = "remote.json";
const SERVER_ID_FILE: &str = "server-id";
pub const TLS_SERVER_NAME: &str = "nakode.remote";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RemoteConfig {
    pub enabled: bool,
    pub bind: SocketAddr,
    pub server_id: String,
    pub api_key: String,
    pub certificate_pem: String,
    pub private_key_pem: String,
}

#[derive(Debug, Error)]
pub enum RemoteConfigError {
    #[error("could not resolve Nakode home: {0}")]
    Home(#[from] crate::config::ConfigError),
    #[error("could not read or write remote configuration at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("remote access is not configured; run `nakode remote enable` first")]
    NotConfigured,
    #[error("invalid remote configuration: {0}")]
    Malformed(String),
    #[error("invalid remote configuration JSON: {0}")]
    Invalid(#[from] serde_json::Error),
    #[error("could not generate remote TLS identity: {0}")]
    Certificate(#[from] rcgen::Error),
}

/// Loads the installation's private remote-access configuration, when present.
///
/// # Errors
/// Returns when the Nakode home cannot be resolved or the file cannot be read or decoded.
pub fn load() -> Result<Option<RemoteConfig>, RemoteConfigError> {
    let path = path()?;
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(RemoteConfigError::Io {
                path: path.display().to_string(),
                source,
            });
        }
    };
    let config: RemoteConfig = serde_json::from_str(&content)?;
    validate(&config)?;
    Ok(Some(config))
}

/// Returns the stable identity of this Nakode installation, creating it when absent.
///
/// # Errors
/// Returns when the private installation identity cannot be read or written.
pub fn installation_server_id() -> Result<String, RemoteConfigError> {
    let identity_path = crate::config::nakode_home()?.join(SERVER_ID_FILE);
    match fs::read_to_string(&identity_path) {
        Ok(value) if !value.trim().is_empty() => return Ok(value.trim().to_owned()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(RemoteConfigError::Io {
                path: identity_path.display().to_string(),
                source,
            });
        }
    }
    let server_id = load()?.map_or_else(
        || uuid::Uuid::now_v7().to_string(),
        |config| config.server_id,
    );
    write_private(&identity_path, format!("{server_id}\n").as_bytes())?;
    Ok(server_id)
}

/// Enables remote access with a fresh API key and TLS identity.
///
/// # Errors
/// Returns when configuration or certificate generation fails.
pub fn enable(bind: SocketAddr) -> Result<RemoteConfig, RemoteConfigError> {
    let certified = rcgen::generate_simple_self_signed(vec![TLS_SERVER_NAME.to_owned()])?;
    let config = RemoteConfig {
        enabled: true,
        bind,
        server_id: installation_server_id()?,
        api_key: generate_api_key(),
        certificate_pem: certified.cert.pem(),
        private_key_pem: certified.signing_key.serialize_pem(),
    };
    save(&config)?;
    Ok(config)
}

/// Replaces the configured API key, invalidating the old key after service restart.
///
/// # Errors
/// Returns when remote access is not configured or the private file cannot be updated.
pub fn regenerate_key() -> Result<RemoteConfig, RemoteConfigError> {
    let mut config = load()?.ok_or(RemoteConfigError::NotConfigured)?;
    config.api_key = generate_api_key();
    save(&config)?;
    Ok(config)
}

/// Marks the remote listener disabled without deleting its identity.
///
/// # Errors
/// Returns when the private configuration cannot be read or updated.
pub fn disable() -> Result<Option<RemoteConfig>, RemoteConfigError> {
    let Some(mut config) = load()? else {
        return Ok(None);
    };
    config.enabled = false;
    save(&config)?;
    Ok(Some(config))
}

#[must_use]
pub fn public_connection(config: &RemoteConfig) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "transport": "grpc+tls",
        "bind": config.bind,
        "tls_server_name": TLS_SERVER_NAME,
        "server_id": config.server_id,
        "api_key": config.api_key,
        "ca_certificate_pem": config.certificate_pem,
    })
}

fn validate(config: &RemoteConfig) -> Result<(), RemoteConfigError> {
    let key_valid = config.api_key.len() == 46
        && config.api_key.starts_with("nk_")
        && config
            .api_key
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-'));
    if !key_valid {
        return Err(RemoteConfigError::Malformed(
            "api_key must be one generated 256-bit nk_ credential".to_owned(),
        ));
    }
    if uuid::Uuid::parse_str(&config.server_id).is_err() {
        return Err(RemoteConfigError::Malformed(
            "server_id must be a UUID".to_owned(),
        ));
    }
    if config.certificate_pem.trim().is_empty() || config.private_key_pem.trim().is_empty() {
        return Err(RemoteConfigError::Malformed(
            "TLS certificate and private key must be present".to_owned(),
        ));
    }
    Ok(())
}

fn generate_api_key() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("nk_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn save(config: &RemoteConfig) -> Result<(), RemoteConfigError> {
    let path = path()?;
    write_private(
        &path,
        format!("{}\n", serde_json::to_string_pretty(config)?).as_bytes(),
    )
}

fn write_private(path: &std::path::Path, content: &[u8]) -> Result<(), RemoteConfigError> {
    let parent = path.parent().expect("private state file has a parent");
    fs::create_dir_all(parent).map_err(|source| RemoteConfigError::Io {
        path: parent.display().to_string(),
        source,
    })?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, content).map_err(|source| RemoteConfigError::Io {
        path: temporary.display().to_string(),
        source,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).map_err(|source| {
            RemoteConfigError::Io {
                path: temporary.display().to_string(),
                source,
            }
        })?;
    }
    fs::rename(&temporary, path).map_err(|source| RemoteConfigError::Io {
        path: path.display().to_string(),
        source,
    })?;
    Ok(())
}

fn path() -> Result<PathBuf, RemoteConfigError> {
    Ok(crate::config::nakode_home()?.join(REMOTE_CONFIG_FILE))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{RemoteConfig, generate_api_key, validate};

    #[test]
    fn remote_configuration_rejects_empty_or_malformed_credentials() {
        let config = RemoteConfig {
            enabled: true,
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7342),
            server_id: uuid::Uuid::now_v7().to_string(),
            api_key: String::new(),
            certificate_pem: "certificate".to_owned(),
            private_key_pem: "private key".to_owned(),
        };
        assert!(validate(&config).is_err());
    }

    #[test]
    fn api_keys_are_prefixed_random_and_url_safe() {
        let first = generate_api_key();
        let second = generate_api_key();
        assert!(first.starts_with("nk_"));
        assert_eq!(first.len(), 46);
        assert_ne!(first, second);
        assert!(
            first
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-'))
        );
    }
}
