use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use thiserror::Error;

/// Command-line and environment configuration for Nako Agent.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "nako-agent",
    version,
    about = "A provider-neutral terminal layer for native agent backends"
)]
pub struct Config {
    #[command(subcommand)]
    pub command: Option<NakoAgentCommand>,
    /// Workspace made available to enabled providers.
    #[arg(long, env = "NAKO_AGENT_WORKSPACE", default_value = ".")]
    pub workspace: PathBuf,

    /// Initial provider-qualified model (`provider/model`).
    #[arg(long, env = "NAKO_AGENT_MODEL")]
    pub model: Option<String>,

    /// Resume a saved Nako Agent session by id (unique prefixes are accepted).
    #[arg(long, env = "NAKO_AGENT_RESUME")]
    pub resume: Option<String>,

    /// Maximum number of logical transcript entries retained in memory.
    #[arg(long, env = "NAKO_AGENT_SCROLLBACK", default_value_t = 2_000)]
    pub scrollback: usize,

    /// Directory containing predefined TOML agent definitions.
    #[arg(long, env = "NAKO_AGENT_AGENTS", default_value = ".nako-agent/agents")]
    pub agents: PathBuf,
}

#[derive(Clone, Debug, Subcommand)]
pub enum NakoAgentCommand {
    /// Invoke a predefined agent through the running Nako Agent control service.
    Agent {
        agent_slug: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "Complete your predefined assignment.")]
        task: String,
    },
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
    /// Loads configuration from command-line arguments and the environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied configuration is invalid.
    pub fn load() -> Result<Self, ConfigError> {
        Self::parse().validated()
    }

    /// Validates and normalizes this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace or initial model is invalid.
    pub fn validated(mut self) -> Result<Self, ConfigError> {
        if !self.workspace.exists() {
            return Err(ConfigError::MissingWorkspace(self.workspace));
        }
        if !self.workspace.is_dir() {
            return Err(ConfigError::WorkspaceIsNotDirectory(self.workspace));
        }

        self.workspace = canonicalize(&self.workspace)?;
        if self.agents.is_relative() {
            self.agents = self.workspace.join(&self.agents);
        }
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

    use super::{Config, NakoAgentCommand};

    #[test]
    fn backend_flag_is_not_part_of_the_cli() {
        assert!(Config::try_parse_from(["nako-agent", "--backend", "devin"]).is_err());
        assert!(Config::try_parse_from(["nako-agent"]).is_ok());
    }

    #[test]
    fn initial_model_requires_provider_qualification() {
        assert!(Config::try_parse_from(["nako-agent", "--model", "model-only"]).is_ok());
        let invalid = Config::try_parse_from(["nako-agent", "--model", "model-only"])
            .expect("CLI parse")
            .validated();
        assert!(matches!(invalid, Err(super::ConfigError::InvalidModel(_))));
        assert!(
            Config::try_parse_from(["nako-agent", "--model", "openai-codex/gpt-5"])
                .expect("CLI parse")
                .validated()
                .is_ok()
        );
    }

    #[test]
    fn agent_command_requires_a_slug_and_session_id() {
        let config = Config::try_parse_from([
            "nako-agent",
            "agent",
            "reviewer",
            "--session-id=session-7",
            "--task=Review auth",
        ])
        .expect("agent command");

        assert!(matches!(
            config.command,
            Some(NakoAgentCommand::Agent {
                agent_slug,
                session_id,
                task,
            }) if agent_slug == "reviewer" && session_id == "session-7" && task == "Review auth"
        ));
        assert!(Config::try_parse_from(["nako-agent", "agent", "explorer"]).is_err());
    }
}
