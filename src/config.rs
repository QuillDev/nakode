use std::path::{Path, PathBuf};

use clap::Parser;
use thiserror::Error;

/// Command-line and environment configuration for Flock.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "flock",
    version,
    about = "A provider-neutral terminal layer for native agent backends"
)]
pub struct Config {
    /// Codex executable used when the Codex provider is enabled.
    #[arg(long, env = "FLOCK_CODEX", default_value = "codex")]
    pub codex: PathBuf,

    /// Devin executable used when the Devin provider is enabled.
    #[arg(long, env = "FLOCK_DEVIN", default_value = "devin")]
    pub devin: PathBuf,

    /// Workspace made available to enabled providers.
    #[arg(long, env = "FLOCK_WORKSPACE", default_value = ".")]
    pub workspace: PathBuf,

    /// Initial provider-qualified model (`provider/model`).
    #[arg(long, env = "FLOCK_MODEL")]
    pub model: Option<String>,

    /// Resume a saved Flock session by id (unique prefixes are accepted).
    #[arg(long, env = "FLOCK_RESUME")]
    pub resume: Option<String>,

    /// Maximum number of logical transcript entries retained in memory.
    #[arg(long, env = "FLOCK_SCROLLBACK", default_value_t = 2_000)]
    pub scrollback: usize,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("workspace does not exist: {0}")]
    MissingWorkspace(PathBuf),
    #[error("workspace is not a directory: {0}")]
    WorkspaceIsNotDirectory(PathBuf),
    #[error("failed to resolve workspace {path}: {source}")]
    ResolveWorkspace {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("model must use the provider/model form: {0}")]
    InvalidModel(String),
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        Self::parse().validated()
    }

    pub fn validated(mut self) -> Result<Self, ConfigError> {
        if !self.workspace.exists() {
            return Err(ConfigError::MissingWorkspace(self.workspace));
        }
        if !self.workspace.is_dir() {
            return Err(ConfigError::WorkspaceIsNotDirectory(self.workspace));
        }

        self.workspace = canonicalize(&self.workspace)?;
        self.scrollback = self.scrollback.max(100);
        self.model = self
            .model
            .take()
            .map(|model| model.trim().to_owned())
            .filter(|model| !model.is_empty());
        if let Some(model) = &self.model
            && model
                .split_once('/')
                .is_none_or(|(provider, model)| provider.is_empty() || model.is_empty())
        {
            return Err(ConfigError::InvalidModel(model.clone()));
        }
        self.resume = self
            .resume
            .take()
            .map(|session| session.trim().to_owned())
            .filter(|session| !session.is_empty());
        Ok(self)
    }
}

fn canonicalize(path: &Path) -> Result<PathBuf, ConfigError> {
    path.canonicalize()
        .map_err(|source| ConfigError::ResolveWorkspace {
            path: path.to_owned(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn backend_flag_is_not_part_of_the_cli() {
        assert!(Config::try_parse_from(["flock", "--backend", "devin"]).is_err());
        assert!(Config::try_parse_from(["flock"]).is_ok());
    }

    #[test]
    fn initial_model_requires_provider_qualification() {
        assert!(Config::try_parse_from(["flock", "--model", "model-only"]).is_ok());
        let invalid = Config::try_parse_from(["flock", "--model", "model-only"])
            .expect("CLI parse")
            .validated();
        assert!(matches!(invalid, Err(super::ConfigError::InvalidModel(_))));
        assert!(
            Config::try_parse_from(["flock", "--model", "openai-codex/gpt-5"])
                .expect("CLI parse")
                .validated()
                .is_ok()
        );
    }
}
