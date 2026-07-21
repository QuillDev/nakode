use serde::{Deserialize, Serialize};

/// Optional backend used by Nakode's provider-neutral `browser` tool.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WebBackend {
    #[default]
    Disabled,
    AgentBrowser,
    Firecrawl,
}

impl WebBackend {
    pub const ALL: [Self; 3] = [Self::Disabled, Self::AgentBrowser, Self::Firecrawl];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::AgentBrowser => "agent-browser",
            Self::Firecrawl => "Firecrawl",
        }
    }
}

/// Persisted, user-selected configuration for the optional browser tool.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WebConfig {
    pub backend: WebBackend,
    #[serde(default)]
    pub firecrawl_api_key: String,
}

impl std::fmt::Debug for WebConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebConfig")
            .field("backend", &self.backend)
            .field(
                "firecrawl_api_key",
                &if self.firecrawl_api_key.is_empty() {
                    "not set"
                } else {
                    "configured"
                },
            )
            .finish()
    }
}

impl WebConfig {
    #[must_use]
    pub fn is_available(&self) -> bool {
        match self.backend {
            WebBackend::Disabled => false,
            WebBackend::AgentBrowser => true,
            WebBackend::Firecrawl => !self.firecrawl_api_key.trim().is_empty(),
        }
    }
}
