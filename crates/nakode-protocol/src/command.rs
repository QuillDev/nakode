use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    AgentSessionId, ArtifactId, EntryId, InteractionId, ModelId, PromptId, ProviderId, RunId,
    SessionId, TurnId, WorkspaceId,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PromptInput {
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<PromptAttachment>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptAttachment {
    Artifact { artifact_id: ArtifactId },
    LocalFile { label: String, path: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentDefinitionInput {
    pub slug: String,
    pub description: String,
    pub system_prompt: String,
    pub first_message: String,
    pub model: Option<ModelId>,
    #[serde(default)]
    pub fallback_models: Vec<ModelId>,
    #[serde(default)]
    pub fast_mode: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelOptions {
    pub reasoning_effort: Option<String>,
    pub fast_mode: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum ModelTarget {
    ProviderDefault { provider_id: ProviderId },
    AgentSession { agent_session_id: AgentSessionId },
    Vision,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CredentialInput(pub String);

impl fmt::Debug for CredentialInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialInput([REDACTED])")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InteractionResolution {
    ApproveOnce,
    ApproveForSession,
    Decline,
    Answer { option_ids: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "setting", content = "value", rename_all = "snake_case")]
pub enum SettingsPatch {
    Web {
        backend: String,
        credential: Option<CredentialInput>,
    },
    Memory {
        backend: String,
        executable: Option<String>,
        global_bank: Option<String>,
        data_directory: Option<String>,
    },
    Vision {
        model_id: Option<ModelId>,
    },
    TerminalImages {
        mode: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    CreateSession {
        workspace_id: WorkspaceId,
        title: Option<String>,
    },
    SubmitPrompt {
        session_id: SessionId,
        prompt: PromptInput,
    },
    EnqueuePrompt {
        session_id: SessionId,
        prompt: PromptInput,
    },
    RemoveQueuedPrompt {
        session_id: SessionId,
        prompt_id: PromptId,
    },
    SteerTurn {
        turn_id: TurnId,
        text: String,
    },
    CancelTurn {
        turn_id: TurnId,
    },
    CompactContext {
        agent_session_id: AgentSessionId,
    },
    SelectModel {
        target: ModelTarget,
        model_id: ModelId,
        options: ModelOptions,
    },
    ResolveInteraction {
        interaction_id: InteractionId,
        resolution: InteractionResolution,
    },
    Delegate {
        session_id: SessionId,
        agent_slug: String,
        task: String,
    },
    CancelRun {
        run_id: RunId,
    },
    RunShell {
        session_id: SessionId,
        command: String,
    },
    SetProviderEnabled {
        provider_id: ProviderId,
        enabled: bool,
    },
    BeginProviderAuthentication {
        provider_id: ProviderId,
    },
    SetProviderCredential {
        provider_id: ProviderId,
        kind: String,
        credential: CredentialInput,
    },
    ClearProviderCredential {
        provider_id: ProviderId,
    },
    SaveAgent {
        workspace_id: WorkspaceId,
        definition: AgentDefinitionInput,
        previous_slug: Option<String>,
    },
    DeleteAgent {
        workspace_id: WorkspaceId,
        slug: String,
    },
    UpdateSettings {
        patch: SettingsPatch,
    },
    ReloadWorkspace {
        workspace_id: WorkspaceId,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Query {
    Bootstrap {
        workspace: String,
        session_id: Option<SessionId>,
    },
    ListSessions {
        workspace_id: WorkspaceId,
        limit: u32,
    },
    GetSession {
        session_id: SessionId,
    },
    GetTranscriptPage {
        session_id: SessionId,
        before: Option<EntryId>,
        limit: u32,
    },
    GetRun {
        run_id: RunId,
    },
    GetArtifact {
        artifact_id: ArtifactId,
    },
}
