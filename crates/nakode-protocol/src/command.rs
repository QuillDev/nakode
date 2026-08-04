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
    Artifact {
        artifact_id: ArtifactId,
        label: String,
    },
    LocalFile {
        label: String,
        path: String,
    },
    InlineImage {
        label: String,
        media_type: String,
        #[serde(with = "crate::base64_bytes")]
        data: Vec<u8>,
    },
}

/// Identifies the canonical transcript that owns an entry.
///
/// Transcript entries are only unique within their owning logical session or
/// orchestration run, so history queries always carry this scope explicitly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TranscriptOwner {
    Session { session_id: SessionId },
    Run { run_id: RunId },
}

/// Selects one canonical text field from an orchestration run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTextField {
    Objective,
    LatestActivity,
    Outcome,
    Result,
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
pub struct ExternalToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema_json: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum ModelTarget {
    ProviderDefault { provider_id: ProviderId },
    Session { session_id: SessionId },
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
    OpenSession {
        session_id: SessionId,
    },
    /// Sends a prompt using server-owned queue-versus-start policy.
    SendPrompt {
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
    CancelSessionWork {
        session_id: SessionId,
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
    ConfigureSessionTools {
        session_id: SessionId,
        tools: Vec<ExternalToolDefinition>,
        replace_builtin_tools: bool,
    },
    SubmitExternalToolResult {
        session_id: SessionId,
        call_id: String,
        output: String,
        failed: bool,
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
    CheckAgentBrowser {
        workspace_id: WorkspaceId,
    },
    ReloadWorkspace {
        workspace_id: WorkspaceId,
        session_id: SessionId,
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
    GetRunTranscriptPage {
        run_id: RunId,
        before: Option<EntryId>,
        limit: u32,
    },
    /// Returns a bounded UTF-8 window ending strictly at `before_byte`.
    ///
    /// Omitting `before_byte` returns the newest body window. When
    /// `has_earlier` is true, pass the returned `start_byte` as `before_byte`
    /// to continue backward without overlap.
    GetTranscriptBodyWindow {
        owner: TranscriptOwner,
        entry_id: EntryId,
        before_byte: Option<u64>,
        limit_bytes: u32,
    },
    GetRun {
        run_id: RunId,
    },
    /// Lists a bounded chronological page ending strictly before `before`.
    ///
    /// Omitting `before` returns the newest page. When `has_earlier` is true,
    /// pass the first returned run ID as `before` to continue backward.
    ListRuns {
        session_id: SessionId,
        before: Option<RunId>,
        limit: u32,
    },
    /// Returns a bounded UTF-8 window from one canonical run text field.
    ///
    /// Omitting `before_byte` returns the newest window. When `has_earlier` is
    /// true, pass the returned `start_byte` to continue backward.
    GetRunTextWindow {
        run_id: RunId,
        field: RunTextField,
        before_byte: Option<u64>,
        limit_bytes: u32,
    },
    GetArtifact {
        artifact_id: ArtifactId,
    },
    /// Returns privacy-preserving runtime telemetry aggregated by the server.
    GetDiagnostics {
        days: u16,
        session_limit: u32,
        provider_id: Option<ProviderId>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        Command, ModelOptions, ModelTarget, PromptAttachment, Query, RunTextField, TranscriptOwner,
    };
    use crate::{ArtifactId, EntryId, ModelId, RunId, SessionId};

    #[test]
    fn logical_session_policy_commands_have_stable_wire_shapes() {
        let session_id = SessionId::from("session-1");
        let selection = Command::SelectModel {
            target: ModelTarget::Session {
                session_id: session_id.clone(),
            },
            model_id: ModelId::from("openai-codex/model-a"),
            options: ModelOptions::default(),
        };
        assert_eq!(
            serde_json::to_value(selection).expect("serialize model selection"),
            json!({
                "type": "select_model",
                "target": {
                    "target": "session",
                    "session_id": "session-1"
                },
                "model_id": "openai-codex/model-a",
                "options": {
                    "reasoning_effort": null,
                    "fast_mode": false
                }
            }),
        );
        assert_eq!(
            serde_json::to_value(Command::CancelSessionWork { session_id })
                .expect("serialize session cancellation"),
            json!({
                "type": "cancel_session_work",
                "session_id": "session-1"
            }),
        );
    }

    #[test]
    fn inline_images_use_bounded_base64_json() {
        let attachment = PromptAttachment::InlineImage {
            label: "clipboard.png".to_owned(),
            media_type: "image/png".to_owned(),
            data: vec![0, 1, 2, 255],
        };
        let encoded = serde_json::to_value(&attachment).expect("serialize inline image");
        assert_eq!(encoded["data"], "AAEC/w==");
        assert_eq!(
            serde_json::from_value::<PromptAttachment>(encoded).expect("decode inline image"),
            attachment
        );
    }

    #[test]
    fn server_artifact_references_preserve_the_attachment_label() {
        let attachment = PromptAttachment::Artifact {
            artifact_id: ArtifactId::from("artifact-1"),
            label: "architecture.png".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(&attachment).expect("serialize artifact reference"),
            json!({
                "type": "artifact",
                "artifact_id": "artifact-1",
                "label": "architecture.png",
            })
        );
    }

    #[test]
    fn run_pagination_uses_an_exclusive_resource_cursor() {
        assert_eq!(
            serde_json::to_value(Query::ListRuns {
                session_id: SessionId::from("session-1"),
                before: Some(RunId::from("run-64")),
                limit: 32,
            })
            .expect("serialize run page query"),
            json!({
                "type": "list_runs",
                "session_id": "session-1",
                "before": "run-64",
                "limit": 32,
            })
        );
    }

    #[test]
    fn run_text_windows_have_a_frontend_neutral_wire_shape() {
        assert_eq!(
            serde_json::to_value(Query::GetRunTextWindow {
                run_id: RunId::from("run-1"),
                field: RunTextField::Result,
                before_byte: Some(131_072),
                limit_bytes: 65_536,
            })
            .expect("serialize run text window"),
            json!({
                "type": "get_run_text_window",
                "run_id": "run-1",
                "field": "result",
                "before_byte": 131_072,
                "limit_bytes": 65_536,
            })
        );
    }

    #[test]
    fn transcript_history_queries_have_frontend_neutral_wire_shapes() {
        assert_eq!(
            serde_json::to_value(Query::GetRunTranscriptPage {
                run_id: RunId::from("run-1"),
                before: Some(EntryId::from("entry-9")),
                limit: 32,
            })
            .expect("serialize run transcript page"),
            json!({
                "type": "get_run_transcript_page",
                "run_id": "run-1",
                "before": "entry-9",
                "limit": 32,
            })
        );
        assert_eq!(
            serde_json::to_value(Query::GetTranscriptBodyWindow {
                owner: TranscriptOwner::Session {
                    session_id: SessionId::from("session-1"),
                },
                entry_id: EntryId::from("entry-1"),
                before_byte: Some(128),
                limit_bytes: 64,
            })
            .expect("serialize transcript body window"),
            json!({
                "type": "get_transcript_body_window",
                "owner": {
                    "type": "session",
                    "session_id": "session-1",
                },
                "entry_id": "entry-1",
                "before_byte": 128,
                "limit_bytes": 64,
            })
        );
    }
}
