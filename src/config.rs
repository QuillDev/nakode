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

    /// Update the managed source checkout and reinstall Nakode.
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

    /// Optional personalities TOML file. Defaults to the user-specific
    /// `personalities.toml` in Nakode's configuration directory.
    #[arg(long, env = "NAKODE_PERSONALITIES")]
    pub personalities: Option<PathBuf>,

    /// Optional Soul Markdown file. Defaults to the user-specific `SOUL.md` in
    /// Nakode's configuration directory; no file is created automatically.
    #[arg(long, env = "NAKODE_SOUL")]
    pub soul: Option<PathBuf>,

    /// Directory containing global predefined TOML agent definitions. Relative paths resolve under
    /// `NAKODE_HOME` (default: `~/.nakode`), never under the workspace.
    #[arg(long, env = "NAKODE_AGENTS", default_value = "agents")]
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
    /// Update the managed source checkout and reinstall Nakode.
    Update,
    /// Invoke a predefined agent through the native Nakode server.
    Agent {
        agent_slug: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "Complete your predefined assignment.")]
        task: String,
    },
    /// Manage the native server for this workspace.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Run the deterministic, headless TUI evaluation protocol.
    #[command(hide = true)]
    TuiEval {
        /// Read JSON Lines actions from this file instead of standard input.
        #[arg(long)]
        scenario: Option<PathBuf>,
        /// Terminal width in cells.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(20..))]
        width: u16,
        /// Terminal height in cells.
        #[arg(long, default_value_t = 28, value_parser = clap::value_parser!(u16).range(8..))]
        height: u16,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ServiceAction {
    /// Run the workspace server in the foreground.
    Run,
    /// Show whether the workspace server is running.
    Status {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Restart the workspace server in the background.
    Restart,
    /// Stop the workspace server. Connected clients reconnect automatically.
    Shutdown,
    /// Manage the Discord frontend attached to this workspace.
    Discord {
        #[command(subcommand)]
        action: DiscordAction,
    },
    /// Print the private gRPC endpoint used by native frontends.
    #[command(hide = true)]
    Endpoint,
}

#[derive(Clone, Debug, Subcommand)]
pub enum DiscordAction {
    /// Interactively configure the Discord bot and its first channel binding.
    Setup {
        /// Discord channel snowflake. If omitted, setup prompts for it.
        #[arg(long)]
        channel_id: Option<String>,
        /// Optional Discord guild snowflake for the channel binding.
        #[arg(long)]
        guild_id: Option<String>,
        /// Optional Nakode session id for legacy direct channel routing. Blank makes the channel
        /// mention-driven, creating a new session and thread for each bot mention.
        #[arg(long)]
        session_id: Option<String>,
        /// Comma-separated Discord user snowflakes allowed to use the bot.
        #[arg(long)]
        allowed_users: Option<String>,
    },
    /// Show Discord configuration without revealing the bot token.
    Status {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Enable Discord autostart. If the workspace service is running, also start Discord now.
    Enable,
    /// Disable Discord autostart. If the workspace service is running, also stop Discord now.
    Disable,
    /// Start Discord in the running workspace service without restarting the native service.
    Start,
    /// Stop Discord in the running workspace service without stopping the native service.
    Stop,
    /// Restart Discord in the running workspace service.
    Restart,
    /// Add or replace a parent-channel binding.
    Bind {
        /// Discord channel snowflake.
        #[arg(long)]
        channel_id: String,
        /// Optional Discord guild snowflake.
        #[arg(long)]
        guild_id: Option<String>,
        /// Optional Nakode session id for legacy direct channel routing. Blank makes the channel
        /// mention-driven, creating a new session and thread for each bot mention.
        #[arg(long)]
        session_id: Option<String>,
    },
    /// Remove a parent-channel binding.
    Unbind {
        /// Discord channel snowflake.
        #[arg(long)]
        channel_id: String,
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
    #[error("neither NAKODE_HOME nor HOME is set; cannot locate Nakode's global agent directory")]
    MissingNakodeHome,
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
            && self.agents == Path::new("agents")
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
        if self.update || matches!(self.command.as_ref(), Some(NakodeCommand::Update)) {
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
            self.agents = nakode_home()?.join(&self.agents);
        }
        if let Some(path) = &self.personalities
            && path.is_relative()
        {
            self.personalities = Some(self.workspace.join(path));
        }
        if let Some(path) = &self.soul
            && path.is_relative()
        {
            self.soul = Some(self.workspace.join(path));
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

fn nakode_home() -> Result<PathBuf, ConfigError> {
    if let Some(home) = std::env::var_os("NAKODE_HOME") {
        return Ok(PathBuf::from(home));
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".nakode"))
        .ok_or(ConfigError::MissingNakodeHome)
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

    use super::{Config, DiscordAction, NakodeCommand, OpenAiReasoningEffort, ServiceAction};

    #[test]
    fn backend_flag_is_not_part_of_the_cli() {
        assert!(Config::try_parse_from(["nakode", "--backend", "devin"]).is_err());
        assert!(Config::try_parse_from(["nakode"]).is_ok());
    }

    #[test]
    fn personality_and_soul_paths_are_configurable() {
        let config = Config::try_parse_from([
            "nakode",
            "--personalities",
            "prompts.toml",
            "--soul",
            "identity.md",
        ])
        .expect("prompt addendum flags");
        assert_eq!(
            config.personalities.as_deref(),
            Some(std::path::Path::new("prompts.toml"))
        );
        assert_eq!(
            config.soul.as_deref(),
            Some(std::path::Path::new("identity.md"))
        );
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
    fn default_agent_directory_is_global_to_nakode_home() {
        let workspace = tempfile::tempdir().expect("workspace");
        let config = Config::try_parse_from([
            "nakode",
            "--workspace",
            workspace.path().to_str().expect("UTF-8 workspace"),
        ])
        .expect("CLI parse")
        .validated()
        .expect("validated config");
        let home = std::env::var_os("NAKODE_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".nakode"))
            })
            .expect("test environment has a home directory");

        assert_eq!(config.agents, home.join("agents"));
        assert!(!config.agents.starts_with(workspace.path()));
    }

    #[test]
    fn relative_agent_override_resolves_under_nakode_home() {
        let workspace = tempfile::tempdir().expect("workspace");
        let config = Config::try_parse_from([
            "nakode",
            "--workspace",
            workspace.path().to_str().expect("UTF-8 workspace"),
            "--agents",
            "shared-agents",
        ])
        .expect("CLI parse")
        .validated()
        .expect("validated config");
        let home = std::env::var_os("NAKODE_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".nakode"))
            })
            .expect("test environment has a home directory");

        assert_eq!(config.agents, home.join("shared-agents"));
    }

    #[test]
    fn different_workspaces_resolve_to_the_same_global_agent_directory() {
        let first = tempfile::tempdir().expect("first workspace");
        let second = tempfile::tempdir().expect("second workspace");
        let resolve = |workspace: &std::path::Path| {
            Config::try_parse_from([
                "nakode",
                "--workspace",
                workspace.to_str().expect("UTF-8 workspace"),
            ])
            .expect("CLI parse")
            .validated()
            .expect("validated config")
            .agents
        };

        assert_eq!(resolve(first.path()), resolve(second.path()));
    }

    #[test]
    fn absolute_agent_override_is_used_unchanged() {
        let workspace = tempfile::tempdir().expect("workspace");
        let agents = tempfile::tempdir().expect("agents");
        let config = Config::try_parse_from([
            "nakode",
            "--workspace",
            workspace.path().to_str().expect("UTF-8 workspace"),
            "--agents",
            agents.path().to_str().expect("UTF-8 agents"),
        ])
        .expect("CLI parse")
        .validated()
        .expect("validated config");

        assert_eq!(config.agents, agents.path());
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
    fn discord_service_commands_parse_nested_configuration_actions() {
        let setup = Config::try_parse_from([
            "nakode",
            "service",
            "discord",
            "setup",
            "--channel-id",
            "123",
            "--guild-id",
            "456",
            "--allowed-users",
            "789,790",
        ])
        .expect("Discord setup command");
        assert!(matches!(
            setup.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Setup {
                        channel_id: Some(channel_id),
                        guild_id: Some(guild_id),
                        allowed_users: Some(allowed_users),
                        ..
                    }
                }
            }) if channel_id == "123" && guild_id == "456" && allowed_users == "789,790"
        ));

        let start = Config::try_parse_from(["nakode", "service", "discord", "start"])
            .expect("Discord start command");
        assert!(matches!(
            start.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Start
                }
            })
        ));
        let stop = Config::try_parse_from(["nakode", "service", "discord", "stop"])
            .expect("Discord stop command");
        assert!(matches!(
            stop.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Stop
                }
            })
        ));
        let restart = Config::try_parse_from(["nakode", "service", "discord", "restart"])
            .expect("Discord restart command");
        assert!(matches!(
            restart.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Restart
                }
            })
        ));

        let status = Config::try_parse_from(["nakode", "service", "discord", "status", "--json"])
            .expect("Discord status command");
        assert!(matches!(
            status.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Status { json: true }
                }
            })
        ));
    }
    #[test]
    fn hidden_service_endpoint_action_parses_for_native_connectors() {
        let config = Config::try_parse_from(["nakode", "service", "endpoint"])
            .expect("hidden endpoint action");

        assert!(matches!(
            config.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Endpoint
            })
        ));
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

        let workspace = tempfile::tempdir().expect("workspace");
        let noncanonical = workspace.path().join(".");
        let config = Config::try_parse_from([
            "nakode",
            "--workspace",
            noncanonical.to_str().expect("UTF-8 workspace"),
            "diagnostics",
        ])
        .expect("diagnostics command")
        .validated()
        .expect("validated diagnostics");
        assert_eq!(
            config.workspace,
            workspace
                .path()
                .canonicalize()
                .expect("canonical workspace")
        );
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
        let status = Config::try_parse_from(["nakode", "service", "status", "--json"])
            .expect("service status");
        assert!(matches!(
            status.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Status { json: true }
            })
        ));
        let restart =
            Config::try_parse_from(["nakode", "service", "restart"]).expect("service restart");
        assert!(matches!(
            restart.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Restart
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
