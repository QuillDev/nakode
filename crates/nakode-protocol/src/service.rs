use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    ArtifactView, BootstrapView, RunId, RunTextWindow, RunView, ServerEpoch, SessionId,
    SessionSummary, SessionView, TranscriptBodyWindow, WorkspaceId,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceCapability {
    Subscriptions,
    MultipleClients,
    ArtifactTransfer,
    ExternalTools,
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
    TranscriptBody(TranscriptBodyWindow),
    Run(Box<RunView>),
    Runs(crate::RunPage),
    RunText(RunTextWindow),
    Artifact(ArtifactView),
    Diagnostics(Box<crate::DiagnosticsReport>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope", content = "value", rename_all = "snake_case")]
pub enum SubscriptionView {
    Workspace(Box<BootstrapView>),
    Session(Box<SessionView>),
    Run(Box<RunView>),
}
