use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use thiserror::Error;

/// Command-line and environment configuration for Nakode.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "nakode",
    version,
    about = "The Nakode agent service",
    long_about = "Nakode is a provider-neutral agent service. It owns sessions, providers, tools, \
and orchestration, and keeps running with no client attached.\n\n\
Run `nakode start` to bring the service up in the background, or `nakode run` to keep it in the \
foreground. --workspace selects the canonical workspace whose isolated service is addressed.\n\n\
The interactive terminal client is one frontend over that service: run `nakode --tui`."
)]
pub struct Config {
    #[command(subcommand)]
    pub command: Option<NakodeCommand>,

    /// Launch the interactive terminal client for the current directory.
    #[arg(long)]
    pub tui: bool,

    /// Update the managed source checkout and reinstall Nakode.
    #[arg(long)]
    pub update: bool,

    /// Workspace made available to enabled providers and selecting its isolated service.
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
    /// Run the Nakode service in the foreground until Ctrl-C.
    Run,
    /// Start the Nakode service in the background and wait until it accepts connections.
    Start,
    /// Stop the Nakode service. Connected clients reconnect automatically.
    Stop,
    /// Restart the Nakode service in the background.
    Restart,
    /// Report the service's state, endpoint, versions, and log location.
    Status {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Show the service log.
    Logs {
        /// Keep printing new lines as the service writes them.
        #[arg(short = 'f', long)]
        follow: bool,
        /// Number of trailing lines to print before following.
        #[arg(
            short = 'n',
            long,
            default_value_t = 200,
            value_parser = clap::value_parser!(u32).range(1..=1_000_000)
        )]
        lines: u32,
    },
    /// Manage the frontend transports the service hosts.
    Transport {
        #[command(subcommand)]
        action: TransportCommand,
    },
    /// Print the private gRPC endpoint used by native frontends.
    #[command(hide = true)]
    Endpoint,
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
    /// Remove every Nakode session and its persisted session state after confirmation.
    PurgeUnsafe,
    /// Refresh every stale workspace service after installation.
    #[command(hide = true)]
    RestartStale,
    /// Update the managed source checkout and reinstall Nakode.
    Update,
    /// Invoke a predefined agent through the native Nakode server.
    Agent {
        agent_slug: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "Complete your predefined assignment.")]
        task: String,
        /// Attributed parent run for policy-bounded recursive delegation.
        #[arg(long)]
        parent_run_id: Option<String>,
    },
    /// Deprecated. Manage the native server; use the top-level commands.
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
pub enum TransportCommand {
    /// Manage the Discord frontend attached to this workspace.
    Discord {
        #[command(subcommand)]
        action: DiscordAction,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ServiceAction {
    /// Deprecated. Use `nakode run`.
    Run,
    /// Deprecated. Use `nakode status`.
    Status {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Deprecated. Use `nakode restart`.
    Restart,
    /// Deprecated. Use `nakode stop`.
    Shutdown,
    /// Deprecated. Use `nakode transport discord`.
    Discord {
        #[command(subcommand)]
        action: DiscordAction,
    },
    /// Deprecated. Use `nakode endpoint`.
    #[command(hide = true)]
    Endpoint,
}

impl ServiceAction {
    /// Returns the non-deprecated spelling that replaces this `nakode service` action.
    #[must_use]
    pub const fn replacement(&self) -> &'static str {
        match self {
            Self::Run => "nakode run",
            Self::Status { .. } => "nakode status",
            Self::Restart => "nakode restart",
            Self::Shutdown => "nakode stop",
            Self::Discord { .. } => "nakode transport discord",
            Self::Endpoint => "nakode endpoint",
        }
    }

    /// Returns the deprecated `nakode service` spelling this action was invoked as.
    #[must_use]
    pub const fn deprecated_spelling(&self) -> &'static str {
        match self {
            Self::Run => "nakode service run",
            Self::Status { .. } => "nakode service status",
            Self::Restart => "nakode service restart",
            Self::Shutdown => "nakode service shutdown",
            Self::Discord { .. } => "nakode service discord",
            Self::Endpoint => "nakode service endpoint",
        }
    }

    /// Rewrites this deprecated action as the top-level command that replaces it.
    #[must_use]
    pub fn into_command(self) -> NakodeCommand {
        match self {
            Self::Run => NakodeCommand::Run,
            Self::Status { json } => NakodeCommand::Status { json },
            Self::Restart => NakodeCommand::Restart,
            Self::Shutdown => NakodeCommand::Stop,
            Self::Discord { action } => NakodeCommand::Transport {
                action: TransportCommand::Discord { action },
            },
            Self::Endpoint => NakodeCommand::Endpoint,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
pub enum DiscordAction {
    /// Interactively configure the Discord bot and orchestrator parent channels.
    Setup {
        /// Chat Orchestrator parent-channel snowflake. If omitted, setup prompts for it.
        #[arg(long)]
        chat_channel_id: Option<String>,
        /// Agent Orchestrator parent-channel snowflake. If omitted, setup prompts for it.
        #[arg(long)]
        agent_channel_id: Option<String>,
        /// The sole Discord user snowflake authorized to continue bridged sessions.
        #[arg(long)]
        primary_user_id: Option<String>,
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
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid command-line configuration: {0}")]
    Parse(#[from] clap::Error),
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
    #[error("--tui cannot be combined with a command; run `nakode --tui` on its own")]
    TuiWithCommand,
}

impl Config {
    /// Builds the default service configuration for a known canonical workspace.
    ///
    /// Global service maintenance has no single caller workspace from which to inherit command
    /// line options, so it uses the normal CLI defaults for each discovered workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the canonical workspace is invalid or the default configuration
    /// cannot be validated.
    pub fn for_workspace(workspace: PathBuf) -> Result<Self, ConfigError> {
        let arguments = [
            std::ffi::OsString::from("nakode"),
            std::ffi::OsString::from("--workspace"),
            workspace.into_os_string(),
        ];
        let config = Self::try_parse_from(arguments)?;
        config.validated()
    }

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
        if self.update && (self.command.is_some() || self.tui) {
            return Err(ConfigError::UpdateWithCommand);
        }
        if self.tui && self.command.is_some() {
            return Err(ConfigError::TuiWithCommand);
        }
        if self.update
            || matches!(
                self.command.as_ref(),
                Some(
                    NakodeCommand::Update
                        | NakodeCommand::PurgeUnsafe
                        | NakodeCommand::RestartStale,
                )
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

    use super::{
        Config, DiscordAction, NakodeCommand, OpenAiReasoningEffort, ServiceAction,
        TransportCommand,
    };

    /// Parses arguments and states the workspace the command would have run in.
    ///
    /// `Config::load` takes the working directory; tests cannot change the
    /// process directory without racing each other, so they set it here.
    fn parsed_in<'a>(
        arguments: impl IntoIterator<Item = &'a str>,
        workspace: &std::path::Path,
    ) -> Config {
        let mut config = Config::try_parse_from(arguments).expect("CLI parse");
        config.workspace = workspace.to_path_buf();
        config
    }

    #[test]
    fn backend_flag_is_not_part_of_the_cli() {
        assert!(Config::try_parse_from(["nakode", "--backend", "devin"]).is_err());
        assert!(Config::try_parse_from(["nakode"]).is_ok());
    }

    #[test]
    fn bare_invocation_selects_neither_a_client_nor_a_service() {
        let config = Config::try_parse_from(["nakode"]).expect("bare invocation");

        assert!(config.command.is_none());
        assert!(!config.tui);
        assert!(!config.update);
    }

    #[test]
    fn unnamed_workspace_keeps_the_current_directory_default() {
        let config = Config::try_parse_from(["nakode", "status"]).expect("CLI parse");

        assert_eq!(config.workspace, std::path::Path::new("."));
    }

    #[test]
    fn the_interactive_client_is_a_flag_that_cannot_carry_a_command() {
        let tui = Config::try_parse_from(["nakode", "--tui"]).expect("client flag");
        assert!(tui.tui);
        assert!(tui.command.is_none());

        let workspace = tempfile::tempdir().expect("workspace");
        let with_options = parsed_in(
            [
                "nakode",
                "--tui",
                "--model",
                "openai-codex/gpt-5",
                "--scrollback",
                "500",
            ],
            workspace.path(),
        )
        .validated()
        .expect("validated client configuration");
        assert!(with_options.tui);
        assert_eq!(with_options.model.as_deref(), Some("openai-codex/gpt-5"));
        assert_eq!(with_options.scrollback, 500);

        let with_command = Config::try_parse_from(["nakode", "--tui", "status"])
            .expect("syntactically valid command")
            .validated();
        assert!(matches!(
            with_command,
            Err(super::ConfigError::TuiWithCommand)
        ));
    }

    #[test]
    fn service_lifecycle_commands_are_top_level() {
        for (arguments, expected) in [
            (vec!["nakode", "run"], NakodeCommand::Run),
            (vec!["nakode", "start"], NakodeCommand::Start),
            (vec!["nakode", "stop"], NakodeCommand::Stop),
            (vec!["nakode", "restart"], NakodeCommand::Restart),
            (
                vec!["nakode", "status", "--json"],
                NakodeCommand::Status { json: true },
            ),
            (vec!["nakode", "endpoint"], NakodeCommand::Endpoint),
        ] {
            let config = Config::try_parse_from(arguments.clone()).expect("lifecycle command");
            assert_eq!(
                format!("{:?}", config.command.expect("command")),
                format!("{expected:?}"),
                "{arguments:?} did not parse as {expected:?}"
            );
        }
    }

    #[test]
    fn logs_accepts_follow_and_bounded_line_counts() {
        let default = Config::try_parse_from(["nakode", "logs"]).expect("logs command");
        assert!(matches!(
            default.command,
            Some(NakodeCommand::Logs {
                follow: false,
                lines: 200
            })
        ));

        let followed = Config::try_parse_from(["nakode", "logs", "-f", "-n", "20"])
            .expect("short follow and line flags");
        assert!(matches!(
            followed.command,
            Some(NakodeCommand::Logs {
                follow: true,
                lines: 20
            })
        ));
        assert!(
            Config::try_parse_from(["nakode", "logs", "--follow", "--lines", "5"]).is_ok(),
            "long spellings must work as well as short ones"
        );
        assert!(Config::try_parse_from(["nakode", "logs", "--lines", "0"]).is_err());
    }

    #[test]
    fn discord_is_managed_as_a_transport() {
        let config = Config::try_parse_from(["nakode", "transport", "discord", "status", "--json"])
            .expect("transport command");

        assert!(matches!(
            config.command,
            Some(NakodeCommand::Transport {
                action: TransportCommand::Discord {
                    action: DiscordAction::Status { json: true }
                }
            })
        ));
    }

    #[test]
    fn deprecated_service_actions_stay_visible_and_name_their_replacements() {
        use clap::CommandFactory;

        let help = Config::command()
            .find_subcommand_mut("service")
            .expect("service remains a documented command")
            .render_long_help()
            .to_string();
        assert!(help.contains("Use `nakode stop`"));
        assert!(help.contains("Use `nakode transport discord`"));

        for (action, deprecated, replacement) in [
            (ServiceAction::Run, "nakode service run", "nakode run"),
            (
                ServiceAction::Status { json: false },
                "nakode service status",
                "nakode status",
            ),
            (
                ServiceAction::Restart,
                "nakode service restart",
                "nakode restart",
            ),
            (
                ServiceAction::Shutdown,
                "nakode service shutdown",
                "nakode stop",
            ),
            (
                ServiceAction::Endpoint,
                "nakode service endpoint",
                "nakode endpoint",
            ),
        ] {
            assert_eq!(action.deprecated_spelling(), deprecated);
            assert_eq!(action.replacement(), replacement);
        }
    }

    #[test]
    fn deprecated_service_actions_rewrite_to_their_replacements() {
        let rewritten = |action: ServiceAction| format!("{:?}", action.into_command());

        assert_eq!(
            rewritten(ServiceAction::Run),
            format!("{:?}", NakodeCommand::Run)
        );
        assert_eq!(
            rewritten(ServiceAction::Shutdown),
            format!("{:?}", NakodeCommand::Stop)
        );
        assert_eq!(
            rewritten(ServiceAction::Status { json: true }),
            format!("{:?}", NakodeCommand::Status { json: true })
        );
        assert_eq!(
            rewritten(ServiceAction::Restart),
            format!("{:?}", NakodeCommand::Restart)
        );
        assert_eq!(
            rewritten(ServiceAction::Endpoint),
            format!("{:?}", NakodeCommand::Endpoint)
        );
        assert_eq!(
            rewritten(ServiceAction::Discord {
                action: DiscordAction::Enable
            }),
            format!(
                "{:?}",
                NakodeCommand::Transport {
                    action: TransportCommand::Discord {
                        action: DiscordAction::Enable
                    }
                }
            )
        );
    }

    #[test]
    fn top_level_help_presents_the_service_before_its_client() {
        use clap::CommandFactory;

        let help = Config::command().render_long_help().to_string();

        assert!(help.contains("agent service"));
        assert!(help.contains("--tui"));
        for command in ["run", "start", "stop", "restart", "status", "logs"] {
            assert!(
                help.contains(command),
                "top-level help must document {command}"
            );
        }
        assert!(
            !help.contains("nakode endpoint\n"),
            "the connector endpoint stays hidden"
        );
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
        let workspace = tempfile::tempdir().expect("workspace");
        assert!(Config::try_parse_from(["nakode", "--model", "model-only"]).is_ok());
        let invalid = parsed_in(["nakode", "--model", "model-only"], workspace.path()).validated();
        assert!(matches!(invalid, Err(super::ConfigError::InvalidModel(_))));
        assert!(
            parsed_in(
                ["nakode", "--model", "openai-codex/gpt-5"],
                workspace.path()
            )
            .validated()
            .is_ok()
        );
    }

    #[test]
    fn default_agent_directory_is_global_to_nakode_home() {
        let workspace = tempfile::tempdir().expect("workspace");
        let config = parsed_in(["nakode"], workspace.path())
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
        let config = parsed_in(["nakode", "--agents", "shared-agents"], workspace.path())
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
            parsed_in(["nakode"], workspace)
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
        let config = parsed_in(
            [
                "nakode",
                "--agents",
                agents.path().to_str().expect("UTF-8 agents"),
            ],
            workspace.path(),
        )
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
                parent_run_id: None,
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
            "--chat-channel-id",
            "123",
            "--agent-channel-id",
            "456",
            "--primary-user-id",
            "789",
        ])
        .expect("Discord setup command");
        assert!(matches!(
            setup.command,
            Some(NakodeCommand::Service {
                action: ServiceAction::Discord {
                    action: DiscordAction::Setup {
                        chat_channel_id: Some(chat_channel_id),
                        agent_channel_id: Some(agent_channel_id),
                        primary_user_id: Some(primary_user_id),
                    }
                }
            }) if chat_channel_id == "123" && agent_channel_id == "456" && primary_user_id == "789"
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
        let config = parsed_in(["nakode", "diagnostics"], &noncanonical)
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
    fn purge_unsafe_is_visible_in_help_and_accepts_no_bypass_flags() {
        use clap::CommandFactory;

        let config =
            Config::try_parse_from(["nakode", "purge-unsafe"]).expect("purge command parses");
        assert!(matches!(config.command, Some(NakodeCommand::PurgeUnsafe)));
        assert!(Config::try_parse_from(["nakode", "purge-unsafe", "--force"]).is_err());

        let help = Config::command().render_long_help().to_string();
        assert!(help.contains("purge-unsafe"));
        assert!(help.contains("Remove every Nakode session"));
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
