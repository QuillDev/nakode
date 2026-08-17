use serde::{Deserialize, Serialize};

use crate::CredentialInput;

/// Public Discord bridge configuration supplied by an owner-facing client.
///
/// The credential is write-only. It is deliberately absent from [`DiscordIntegrationView`]
/// and its `Debug` representation is redacted by [`CredentialInput`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscordIntegrationInput {
    pub chat_channel_id: String,
    pub agent_channel_id: String,
    pub primary_user_id: String,
    pub bot_token: Option<CredentialInput>,
}

/// Runtime state of the Discord transport attached to one workspace service.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscordRuntimeState {
    Disabled,
    Stopped,
    Running,
    Failed,
}

/// Redacted installation configuration plus the current workspace service's transport state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscordIntegrationView {
    pub enabled: bool,
    pub configuration_complete: bool,
    pub token_configured: bool,
    pub chat_channel_id: Option<String>,
    pub agent_channel_id: Option<String>,
    pub primary_user_id: Option<String>,
    pub runtime_state: DiscordRuntimeState,
    /// Sanitized operator-facing summary. It never contains a credential, payload, or raw network
    /// metadata.
    pub runtime_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_debug_redacts_the_write_only_token() {
        let input = DiscordIntegrationInput {
            chat_channel_id: "111".to_owned(),
            agent_channel_id: "222".to_owned(),
            primary_user_id: "333".to_owned(),
            bot_token: Some(CredentialInput("never-print-this-token".to_owned())),
        };

        let debug = format!("{input:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("never-print-this-token"));
    }
}
