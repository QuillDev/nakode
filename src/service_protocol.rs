//! Transport-neutral messages exchanged between Nakode clients and the service.
//!
//! Commands, queries, snapshots, and subscription events in this module are
//! semantic service messages. Transport framing and client presentation types
//! remain separate concerns.

use serde::{Deserialize, Serialize};

pub const SERVICE_PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRequest {
    pub version: u16,
    pub request_id: String,
    pub command: ClientCommand,
}

impl ClientRequest {
    #[must_use]
    pub fn new(command: ClientCommand) -> Self {
        Self {
            version: SERVICE_PROTOCOL_VERSION,
            request_id: uuid::Uuid::now_v7().to_string(),
            command,
        }
    }

    /// Validates that the request uses the protocol version supported here.
    ///
    /// # Errors
    /// Returns [`ProtocolError::VersionMismatch`] for an incompatible version.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientCommand {
    InvokeAgent(AgentInvocation),
    Domain(DomainCommand),
}

/// Transport-neutral mutations accepted by the canonical service engine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum DomainCommand {
    SubmitPrompt {
        text: String,
    },
    EnqueuePrompt {
        text: String,
    },
    Steer {
        text: String,
    },
    Interrupt,
    ResolveApproval {
        decision: ApprovalChoice,
    },
    ResolveQuestion {
        answers: Vec<String>,
    },
    SelectModel {
        provider: String,
        model: String,
    },
    ResumeSession {
        session_id: String,
    },
    NewSession,
    SetProviderEnabled {
        provider: String,
        enabled: bool,
    },
    AuthenticateProvider {
        provider: String,
    },
    SetProviderCredential {
        provider: String,
        kind: String,
        secret: String,
    },
    ClearProviderCredential {
        provider: String,
    },
    SaveAgent {
        agent: AgentDefinition,
    },
    DeleteAgent {
        slug: String,
    },
    UpdateSettings {
        patch: SettingsPatch,
    },
    QuitSession,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalChoice {
    Once,
    Always,
    Reject,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentDefinition {
    pub slug: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub provider: Option<String>,
    pub model: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "setting", content = "value", rename_all = "snake_case")]
pub enum SettingsPatch {
    DefaultModel {
        provider: String,
        model: String,
    },
    ModelOptions {
        provider: String,
        model: String,
        options: serde_json::Value,
    },
    Web(serde_json::Value),
    Vision(serde_json::Value),
    TerminalImages(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentInvocation {
    pub agent: String,
    pub session_id: String,
    pub task: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceResponse {
    pub version: u16,
    pub request_id: String,
    pub result: CommandResult,
}

impl ServiceResponse {
    #[must_use]
    pub fn new(request_id: impl Into<String>, result: CommandResult) -> Self {
        Self {
            version: SERVICE_PROTOCOL_VERSION,
            request_id: request_id.into(),
            result,
        }
    }

    /// Validates the response version and correlation identifier.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] when the version is incompatible or the
    /// response does not belong to `request`.
    pub fn validate_for(&self, request: &ClientRequest) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        if self.request_id != request.request_id {
            return Err(ProtocolError::RequestIdMismatch {
                expected: request.request_id.clone(),
                actual: self.request_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandResult {
    Agent(AgentResponse),
    Accepted,
    Rejected { message: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentResponse {
    pub success: bool,
    pub result: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolError {
    #[error(
        "unsupported Nakode service protocol version {actual}; this client requires version {expected}"
    )]
    VersionMismatch { expected: u16, actual: u16 },
    #[error("Nakode service response id mismatch: expected {expected}, received {actual}")]
    RequestIdMismatch { expected: String, actual: String },
}

fn validate_version(actual: u16) -> Result<(), ProtocolError> {
    if actual == SERVICE_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::VersionMismatch {
            expected: SERVICE_PROTOCOL_VERSION,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentInvocation, AgentResponse, ClientCommand, ClientRequest, CommandResult, ProtocolError,
        SERVICE_PROTOCOL_VERSION, ServiceResponse,
    };

    fn request() -> ClientRequest {
        ClientRequest::new(ClientCommand::InvokeAgent(AgentInvocation {
            agent: "explorer".to_owned(),
            session_id: "session-7".to_owned(),
            task: "Inspect the protocol".to_owned(),
        }))
    }

    #[test]
    fn request_round_trips_as_versioned_json() {
        let request = request();
        let encoded = serde_json::to_string(&request).expect("serialize request");
        let decoded: ClientRequest = serde_json::from_str(&encoded).expect("deserialize request");

        assert_eq!(decoded, request);
        assert_eq!(decoded.version, SERVICE_PROTOCOL_VERSION);
        assert!(!decoded.request_id.is_empty());
        decoded.validate().expect("supported protocol version");
    }

    #[test]
    fn response_is_correlated_with_its_request() {
        let request = request();
        let response = ServiceResponse::new(
            request.request_id.clone(),
            CommandResult::Agent(AgentResponse {
                success: true,
                result: "Complete".to_owned(),
            }),
        );

        response.validate_for(&request).expect("matching response");
    }

    #[test]
    fn mismatched_response_id_is_rejected() {
        let request = request();
        let response = ServiceResponse::new(
            "another-request",
            CommandResult::Rejected {
                message: "No session".to_owned(),
            },
        );

        assert!(matches!(
            response.validate_for(&request),
            Err(ProtocolError::RequestIdMismatch { .. })
        ));
    }

    #[test]
    fn incompatible_protocol_version_is_rejected() {
        let mut request = request();
        request.version += 1;

        assert!(matches!(
            request.validate(),
            Err(ProtocolError::VersionMismatch { .. })
        ));
    }
}
