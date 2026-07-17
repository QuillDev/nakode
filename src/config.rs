use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BackendChoice {
    Codex,
    Devin,
}

impl BackendChoice {
    pub fn provider(self) -> &'static str {
        match self {
            Self::Codex => crate::backend::CODEX_PROVIDER,
            Self::Devin => crate::backend::DEVIN_PROVIDER,
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Devin => "Devin",
        }
    }
}

/// Command-line and environment configuration for Flock.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "flock",
    version,
    about = "A provider-neutral terminal layer for native agent backends"
)]
pub struct Config {
    /// Agent backend to launch.
    #[arg(long, env = "FLOCK_BACKEND", value_enum, default_value_t = BackendChoice::Codex)]
    pub backend: BackendChoice,

    /// Codex executable to launch when --backend codex is selected.
    #[arg(long, env = "FLOCK_CODEX", default_value = "codex")]
    pub codex: PathBuf,

    /// Devin executable to launch when --backend devin is selected.
    #[arg(long, env = "FLOCK_DEVIN", default_value = "devin")]
    pub devin: PathBuf,

    /// Workspace made available to the selected backend.
    #[arg(long, env = "FLOCK_WORKSPACE", default_value = ".")]
    pub workspace: PathBuf,

    /// Initial model, when supported by the selected backend.
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

    use super::{BackendChoice, Config};

    #[test]
    fn selects_devin_without_changing_codex_default() {
        let devin =
            Config::try_parse_from(["flock", "--backend", "devin"]).expect("parse Devin backend");
        assert_eq!(devin.backend, BackendChoice::Devin);
        assert_eq!(devin.devin.to_string_lossy(), "devin");

        let default = Config::try_parse_from(["flock"]).expect("parse default backend");
        assert_eq!(default.backend, BackendChoice::Codex);
    }
}
