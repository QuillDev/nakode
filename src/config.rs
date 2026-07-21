use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use thiserror::Error;

/// Command-line and environment configuration for Nakode.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "nakode",
    version,
    about = "A provider-neutral terminal layer for native agent backends"
)]
pub struct Config {
    #[command(subcommand)]
    pub command: Option<NakodeCommand>,

    /// Update this installation through its package manager.
    #[arg(long)]
    pub update: bool,

    /// Workspace made available to enabled providers.
    #[arg(long, env = "NAKODE_WORKSPACE", default_value = ".")]
    pub workspace: PathBuf,

    /// Initial provider-qualified model (`provider/model`).
    #[arg(long, env = "NAKODE_MODEL")]
    pub model: Option<String>,

    /// Resume a saved Nakode session by id (unique prefixes are accepted).
    #[arg(long, env = "NAKODE_RESUME")]
    pub resume: Option<String>,

    /// Maximum number of logical transcript entries retained in memory.
    #[arg(long, env = "NAKODE_SCROLLBACK", default_value_t = 2_000)]
    pub scrollback: usize,

    /// Percentage of the model context window that triggers proactive compaction.
    #[arg(
        long,
        env = "NAKODE_COMPACTION_THRESHOLD_PERCENT",
        default_value_t = 85,
        value_parser = clap::value_parser!(u8).range(1..=100)
    )]
    pub compaction_threshold_percent: u8,

    /// Reasoning effort requested from `OpenAI` models.
    #[arg(
        long,
        env = "NAKODE_OPENAI_REASONING_EFFORT",
        value_enum,
        default_value_t = OpenAiReasoningEffort::Medium
    )]
    pub openai_reasoning_effort: OpenAiReasoningEffort,

    /// Directory containing predefined TOML agent definitions.
    #[arg(long, env = "NAKODE_AGENTS", default_value = ".nakode/agents")]
    pub agents: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum OpenAiReasoningEffort {
    None,
    Low,
    #[default]
    Medium,
    High,
    Xhigh,
    Max,
}

impl OpenAiReasoningEffort {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
pub enum NakodeCommand {
    /// Report persisted token, cache, session, and tool telemetry without prompt content.
    Diagnostics {
        /// Number of days of telemetry to include.
        #[arg(long, default_value_t = 7, value_parser = clap::value_parser!(u16).range(1..=3650))]
        days: u16,
        /// Maximum number of highest-input sessions to display.
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u16).range(1..=500))]
        sessions: u16,
        /// Include only one provider slug, such as `openai-codex`.
        #[arg(long)]
        provider: Option<String>,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Update this installation through its package manager.
    Update,
    /// Invoke a predefined agent through the running Nakode control service.
    Agent {
        agent_slug: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "Complete your predefined assignment.")]
        task: String,
    },
    /// Manage the shared user-level control service.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ServiceAction {
    /// Run the control service in the foreground.
    Run,
    /// Stop the running control service. Active TUIs reconnect automatically.
    Shutdown,
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
    #[error("--update cannot be combined with another command")]
    UpdateWithCommand,
}

impl Config {
    /// Loads configuration from command-line arguments and the environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied configuration is invalid.
    pub fn load() -> Result<Self, ConfigError> {
        let mut config = Self::parse();
        config.apply_legacy_environment();
        config.validated()
    }

    fn apply_legacy_environment(&mut self) {
        if std::env::var_os("NAKODE_WORKSPACE").is_none()
            && self.workspace == Path::new(".")
            && let Some(workspace) = std::env::var_os("NAKO_AGENT_WORKSPACE")
        {
            self.workspace = workspace.into();
        }
        if std::env::var_os("NAKODE_MODEL").is_none()
            && self.model.is_none()
            && let Some(model) = std::env::var_os("NAKO_AGENT_MODEL")
        {
            self.model = Some(model.to_string_lossy().into_owned());
        }
        if std::env::var_os("NAKODE_RESUME").is_none()
            && self.resume.is_none()
            && let Some(resume) = std::env::var_os("NAKO_AGENT_RESUME")
        {
            self.resume = Some(resume.to_string_lossy().into_owned());
        }
        if std::env::var_os("NAKODE_SCROLLBACK").is_none()
            && self.scrollback == 2_000
            && let Some(scrollback) = std::env::var_os("NAKO_AGENT_SCROLLBACK")
                .and_then(|value| value.to_string_lossy().parse().ok())
        {
            self.scrollback = scrollback;
        }
        if std::env::var_os("NAKODE_AGENTS").is_none()
            && self.agents == Path::new(".nakode/agents")
            && let Some(agents) = std::env::var_os("NAKO_AGENT_AGENTS")
        {
            self.agents = agents.into();
        }
    }

    /// Validates and normalizes this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace or initial model is invalid.
    pub fn validated(mut self) -> Result<Self, ConfigError> {
        if self.update && self.command.is_some() {
            return Err(ConfigError::UpdateWithCommand);
        }
        if self.update
            || matches!(
                self.command.as_ref(),
                Some(NakodeCommand::Update | NakodeCommand::Diagnostics { .. })
            )
        {
            return Ok(self);
        }
        if !self.workspace.exists() {
            return Err(ConfigError::MissingWorkspace(self.workspace));
        }
        if !self.workspace.is_dir() {
            return Err(ConfigError::WorkspaceIsNotDirectory(self.workspace));
        }

        self.workspace = canonicalize(&self.workspace)?;
        if self.agents.is_relative() {
            let uses_default = self.agents == Path::new(".nakode/agents");
            let configured = self.workspace.join(&self.agents);
            let legacy = self.workspace.join(".nako-agent/agents");
            self.agents = if uses_default && !configured.exists() && legacy.exists() {
                legacy
            } else {
                configured
            };
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

    use super::{Config, NakodeCommand, OpenAiReasoningEffort, ServiceAction};

    #[test]
    fn backend_flag_is_not_part_of_the_cli() {
        assert!(Config::try_parse_from(["nakode", "--backend", "devin"]).is_err());
        assert!(Config::try_parse_from(["nakode"]).is_ok());
    }

    #[test]
    fn compaction_threshold_defaults_to_85_percent_and_is_bounded() {
        let config = Config::try_parse_from(["nakode"]).expect("default config");
        assert_eq!(config.compaction_threshold_percent, 85);
        assert!(Config::try_parse_from(["nakode", "--compaction-threshold-percent", "1"]).is_ok());
        assert!(
            Config::try_parse_from(["nakode", "--compaction-threshold-percent", "100"]).is_ok()
        );
        assert!(Config::try_parse_from(["nakode", "--compaction-threshold-percent", "0"]).is_err());
        assert!(
            Config::try_parse_from(["nakode", "--compaction-threshold-percent", "101"]).is_err()
        );
    }

    #[test]
    fn openai_reasoning_effort_defaults_to_medium_and_accepts_explicit_levels() {
        let default = Config::try_parse_from(["nakode"]).expect("default config");
        assert_eq!(
            default.openai_reasoning_effort,
            OpenAiReasoningEffort::Medium
        );
        let low = Config::try_parse_from(["nakode", "--openai-reasoning-effort", "low"])
            .expect("low effort");
        assert_eq!(low.openai_reasoning_effort, OpenAiReasoningEffort::Low);
        assert!(
            Config::try_parse_from(["nakode", "--openai-reasoning-effort", "extreme"]).is_err()
        );
    }

    #[test]
    fn initial_model_requires_provider_qualification() {
        assert!(Config::try_parse_from(["nakode", "--model", "model-only"]).is_ok());
        let invalid = Config::try_parse_from(["nakode", "--model", "model-only"])
            .expect("CLI parse")
            .validated();
        assert!(matches!(invalid, Err(super::ConfigError::InvalidModel(_))));
        assert!(
            Config::try_parse_from(["nakode", "--model", "openai-codex/gpt-5"])
                .expect("CLI parse")
                .validated()
                .is_ok()
        );
    }

    #[test]
    fn default_agent_directory_falls_back_to_the_legacy_location() {
        let workspace = tempfile::tempdir().expect("workspace");
        let legacy = workspace.path().join(".nako-agent/agents");
        std::fs::create_dir_all(&legacy).expect("legacy agent directory");
        let config = Config::try_parse_from([
            "nakode",
            "--workspace",
            workspace.path().to_str().expect("UTF-8 workspace"),
        ])
        .expect("CLI parse")
        .validated()
        .expect("validated config");
        let legacy = legacy.canonicalize().expect("canonical legacy directory");

        assert_eq!(config.agents, legacy);
    }

    #[test]
    fn agent_command_requires_a_slug_and_session_id() {
        let config = Config::try_parse_from([
            "nakode",
            "agent",
            "reviewer",
            "--session-id=session-7",
            "--task=Review auth",
        ])
        .expect("agent command");

        assert!(matches!(
            config.command,
            Some(NakodeCommand::Agent {
                agent_slug,
                session_id,
                task,
            }) if agent_slug == "reviewer" && session_id == "session-7" && task == "Review auth"
        ));
        assert!(Config::try_parse_from(["nakode", "agent", "explorer"]).is_err());
    }

    #[test]
    fn diagnostics_command_parses_bounded_privacy_preserving_options() {
        let config = Config::try_parse_from([
            "nakode",
            "diagnostics",
            "--days=30",
            "--sessions=40",
            "--provider=openai-codex",
            "--json",
        ])
        .expect("diagnostics command");
        assert!(matches!(
            config.command,
            Some(NakodeCommand::Diagnostics {
                days: 30,
                sessions: 40,
                provider: Some(ref provider),
                json: true,
            }) if provider == "openai-codex"
        ));
        assert!(Config::try_parse_from(["nakode", "diagnostics", "--days=0"]).is_err());
        assert!(Config::try_parse_from(["nakode", "diagnostics", "--sessions=501"]).is_err());
    }

    #[test]
    fn update_command_and_flag_are_supported() {
        let command = Config::try_parse_from(["nakode", "update"]).expect("update command");
        assert!(matches!(command.command, Some(NakodeCommand::Update)));

        let flag = Config::try_parse_from(["nakode", "--update"]).expect("update flag");
        assert!(flag.update);

        let combined = Config::try_parse_from(["nakode", "--update", "service", "run"])
            .expect("syntactically valid command")
            .validated();
        assert!(matches!(
            combined,
            Err(super::ConfigError::UpdateWithCommand)
        ));
    }

    #[test]
    fn service_commands_parse_explicit_actions() {
        let run = Config::try_parse_from(["nakode", "service", "run"]).expect("service run");
        assert!(matches!(
            run.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Run
            })
        ));
        let shutdown =
            Config::try_parse_from(["nakode", "service", "shutdown"]).expect("service shutdown");
        assert!(matches!(
            shutdown.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Shutdown
            })
        ));
    }
}
