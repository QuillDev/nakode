use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    ArtifactView, BootstrapView, ClientId, Command, IdempotencyKey, Query, RequestId, RunId,
    RunView, ServerEpoch, ServiceError, SessionId, SessionSummary, SessionView, SubscriptionId,
    ViewEvent, WorkspaceId,
};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionRange {
    pub minimum: u16,
    pub maximum: u16,
}

impl VersionRange {
    #[must_use]
    pub const fn current() -> Self {
        Self {
            minimum: PROTOCOL_VERSION,
            maximum: PROTOCOL_VERSION,
        }
    }

    #[must_use]
    pub const fn supports(self, version: u16) -> bool {
        self.minimum <= version && version <= self.maximum
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientDescriptor {
    pub name: String,
    pub version: String,
    pub frontend: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceCapability {
    Subscriptions,
    EventReplay,
    MultipleClients,
    ArtifactTransfer,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceCapabilities {
    #[serde(default)]
    pub supported: BTreeSet<ServiceCapability>,
}

impl ServiceCapabilities {
    #[must_use]
    pub fn supports(&self, capability: ServiceCapability) -> bool {
        self.supported.contains(&capability)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor {
    pub server_epoch: ServerEpoch,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Snapshot<Value> {
    pub cursor: Cursor,
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum SubscriptionScope {
    Workspace { workspace_id: WorkspaceId },
    Session { session_id: SessionId },
    Run { run_id: RunId },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    Hello {
        supported: VersionRange,
        client_id: ClientId,
        client: ClientDescriptor,
    },
    Command {
        request_id: RequestId,
        idempotency_key: IdempotencyKey,
        expected_revision: Option<u64>,
        command: Command,
    },
    Query {
        request_id: RequestId,
        query: Query,
    },
    Subscribe {
        request_id: RequestId,
        scope: SubscriptionScope,
    },
    ResumeSubscription {
        request_id: RequestId,
        scope: SubscriptionScope,
        after: Cursor,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
    Ping {
        nonce: u64,
    },
    Detach,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandAccepted {
    pub resource_id: Option<String>,
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum QueryResult {
    Bootstrap(Box<BootstrapView>),
    Sessions(Vec<SessionSummary>),
    Session(Box<SessionView>),
    Transcript(crate::TranscriptPage),
    Run(Box<RunView>),
    Artifact(ArtifactView),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope", content = "value", rename_all = "snake_case")]
pub enum SubscriptionView {
    Workspace(Box<BootstrapView>),
    Session(Box<SessionView>),
    Run(Box<RunView>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    Welcome {
        version: u16,
        server_epoch: ServerEpoch,
        server_version: String,
        capabilities: ServiceCapabilities,
    },
    CommandResult {
        request_id: RequestId,
        result: Result<CommandAccepted, ServiceError>,
    },
    QueryResult {
        request_id: RequestId,
        result: Result<Snapshot<QueryResult>, ServiceError>,
    },
    Subscribed {
        request_id: RequestId,
        subscription_id: SubscriptionId,
        snapshot: Snapshot<SubscriptionView>,
    },
    SubscriptionResumed {
        request_id: RequestId,
        subscription_id: SubscriptionId,
        from: Cursor,
        through: Cursor,
    },
    Event {
        subscription_id: SubscriptionId,
        cursor: Cursor,
        event: ViewEvent,
    },
    ResyncRequired {
        request_id: RequestId,
        oldest_available: Cursor,
        current: Cursor,
    },
    SubscriptionLagged {
        subscription_id: SubscriptionId,
        oldest_available: Cursor,
        current: Cursor,
    },
    Pong {
        nonce: u64,
    },
    Fatal {
        error: ServiceError,
    },
}

#[cfg(test)]
mod tests {
    use crate::{ClientDescriptor, ClientFrame, ClientId, VersionRange};

    #[test]
    fn handshake_round_trips_without_implementation_types() {
        let frame = ClientFrame::Hello {
            supported: VersionRange::current(),
            client_id: ClientId::from("client-1"),
            client: ClientDescriptor {
                name: "test frontend".to_owned(),
                version: "1.0".to_owned(),
                frontend: "plain-text".to_owned(),
            },
        };
        let encoded = serde_json::to_string(&frame).expect("serialize");
        let decoded: ClientFrame = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, frame);
    }
}
