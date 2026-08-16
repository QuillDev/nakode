use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    AgentSessionId, ArtifactId, BridgeLifecycle, EntryId, InteractionId, McpGrantPolicy,
    McpServerInput, McpSessionGrant, ModelId, OrchestratorKind, PromptId, ProviderId, RunId,
    SessionId, TurnId, WorkspaceId,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionBridgeIntent {
    pub kind: OrchestratorKind,
    pub lifecycle: BridgeLifecycle,
    pub display_title: String,
}

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
// Public command input mirrors independently meaningful protobuf fields; collapsing these booleans
// into one state enum would change the stable wire contract.
#[allow(clippy::struct_excessive_bools)]
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
    /// The level to run at, or `None` for the model's own default. Refused without `model`.
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub ownership: String,
    #[serde(default = "true_by_default")]
    pub enabled: bool,
    #[serde(default)]
    pub allowed_capabilities: Vec<String>,
    #[serde(default)]
    pub denied_capabilities: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
    #[serde(default)]
    pub tool_profile: String,
    #[serde(default)]
    pub task_shape: String,
    #[serde(default)]
    pub output_contract: String,
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
    #[serde(default)]
    pub poll_interval_ms: Option<u32>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub max_concurrency: u32,
    #[serde(default)]
    pub fallback_policy: String,
    #[serde(default)]
    pub can_delegate: bool,
    #[serde(default)]
    pub max_delegation_depth: u32,
    #[serde(default = "true_by_default")]
    pub require_parent_attribution: bool,
}

fn true_by_default() -> bool {
    true
}

impl Default for AgentDefinitionInput {
    fn default() -> Self {
        Self {
            slug: String::new(),
            description: String::new(),
            system_prompt: String::new(),
            first_message: String::new(),
            model: None,
            fallback_models: Vec::new(),
            fast_mode: false,
            reasoning_effort: None,
            ownership: "owner_defined".to_owned(),
            enabled: true,
            allowed_capabilities: Vec::new(),
            denied_capabilities: Vec::new(),
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
            tool_profile: "custom".to_owned(),
            task_shape: String::new(),
            output_contract: String::new(),
            timeout_seconds: None,
            poll_interval_ms: None,
            max_turns: None,
            max_concurrency: 4,
            fallback_policy: "configured_only".to_owned(),
            can_delegate: false,
            max_delegation_depth: 0,
            require_parent_attribution: true,
        }
    }
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
pub struct SessionToolConfiguration {
    pub tools: Vec<ExternalToolDefinition>,
    pub replace_builtin_tools: bool,
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
pub struct QuestionResponse {
    pub question_id: String,
    #[serde(default)]
    pub option_ids: Vec<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InteractionResolution {
    ApproveOnce,
    ApproveForSession,
    Decline,
    /// Legacy single-question label answer.
    Answer {
        option_ids: Vec<String>,
    },
    /// Structured, atomic response to every item in a question interaction.
    AnswerQuestions {
        answers: Vec<QuestionResponse>,
    },
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
// Keep commands as value-shaped protocol messages. Boxing only this variant would alter the public
// Rust command API to optimize an infrequent control-plane enum.
#[allow(clippy::large_enum_variant)]
pub enum Command {
    CreateSession {
        workspace_id: WorkspaceId,
        title: Option<String>,
        /// A provider-qualified initial selection. `None` inherits the workspace/provider default.
        model_id: Option<ModelId>,
        options: ModelOptions,
        /// Client-owned tools installed before this session can accept a prompt.
        tools: Option<SessionToolConfiguration>,
        /// Optional client-owned context merged into provider system instructions for this session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initial_instructions: Option<String>,
        /// Optional external-thread projection intent owned by the creating frontend.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bridge: Option<SessionBridgeIntent>,
        /// Explicit, deny-by-default Nakode MCP server grants for this session.
        #[serde(default)]
        mcp_grant: Option<McpSessionGrant>,
    },
    OpenSession {
        session_id: SessionId,
        /// Tools for closed-session restoration, or the identical table for idempotent reattachment.
        tools: Option<SessionToolConfiguration>,
        /// Explicit MCP grant used only when restoring a closed session.
        #[serde(default)]
        mcp_grant: Option<McpSessionGrant>,
    },
    /// Sets one session's desired external-thread lifecycle. Idempotent when already in that state.
    SetSessionBridgeLifecycle {
        session_id: SessionId,
        lifecycle: BridgeLifecycle,
    },
    /// Sets every persisted bridge in a workspace to one desired lifecycle.
    SetWorkspaceBridgeLifecycle {
        workspace_id: WorkspaceId,
        lifecycle: BridgeLifecycle,
    },
    /// Claims a stable external thread for one unbound bridge.
    BindSessionBridgeThread {
        session_id: SessionId,
        transport: String,
        external_parent_id: String,
        external_thread_id: String,
    },
    /// Clears a missing/deleted external thread without changing bridge intent.
    ClearSessionBridgeThread {
        session_id: SessionId,
        transport: String,
        external_thread_id: String,
    },
    /// Atomically establishes the durable checkpoint for one final answer before network delivery.
    PrepareBridgeDelivery {
        session_id: SessionId,
        turn_id: TurnId,
        body_sha256: String,
        part_count: u64,
    },
    /// Marks one prepared delivery part as accepted by the external transport.
    CompleteBridgeDeliveryPart {
        session_id: SessionId,
        turn_id: TurnId,
        part_index: u64,
        external_message_id: String,
    },
    /// Marks a fully delivered turn final and clears its constant-size progress checkpoint.
    FinalizeBridgeDelivery {
        session_id: SessionId,
        turn_id: TurnId,
    },
    /// Records or clears the transport's one non-final/live status message.
    SetBridgeLiveMessage {
        session_id: SessionId,
        turn_id: Option<TurnId>,
        external_message_id: Option<String>,
    },
    /// Atomically verifies an open binding, rejects busy sessions instead of queueing, records the
    /// gateway event for replay suppression, and starts the next user turn.
    ContinueSessionFromBridge {
        session_id: SessionId,
        transport: String,
        external_thread_id: String,
        external_event_id: String,
        source_message_id: String,
        prompt: PromptInput,
        /// Durably consumes this authorized event as busy without ever starting a turn. Transports
        /// use this when their bounded ingress is saturated.
        consume_as_busy: bool,
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
    /// Atomically removes one queued prompt and redirects active work to it.
    ///
    /// A steering provider accepts it in the current turn. An interruption-only provider stops the
    /// current turn and starts this prompt before the remaining queue.
    SteerQueuedPrompt {
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
    /// Cancels all cancellable work owned by the logical session when this command executes.
    ///
    /// This is a priority session-policy operation rather than a turn-identity operation. Callers
    /// omit the expected revision when background progress must not invalidate the stop request.
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
        parent_run_id: Option<RunId>,
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
    ReloadProvider {
        provider_id: ProviderId,
    },
    SaveMcpServer {
        workspace_id: WorkspaceId,
        server: McpServerInput,
        grants: McpGrantPolicy,
    },
    DeleteMcpServer {
        workspace_id: WorkspaceId,
        server_id: String,
    },
    SetMcpServerEnabled {
        workspace_id: WorkspaceId,
        server_id: String,
        enabled: bool,
    },
    RefreshMcpServer {
        workspace_id: WorkspaceId,
        server_id: String,
    },
    SetMcpServerCredential {
        workspace_id: WorkspaceId,
        server_id: String,
        kind: String,
        credential: CredentialInput,
    },
    ClearMcpServerCredential {
        workspace_id: WorkspaceId,
        server_id: String,
    },
    SetMcpServerGrants {
        workspace_id: WorkspaceId,
        server_id: String,
        grants: McpGrantPolicy,
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
    /// Removes a logical session and everything persisted under it.
    ///
    /// Terminal and not undoable: the transcript, its runs and their turns all go. A session with work
    /// in flight is rejected rather than interrupted — cancelling is `CancelSessionWork`, and doing
    /// both under one verb would make a cleanup command able to stop running inference.
    DeleteSession {
        session_id: SessionId,
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
    SaveSoul {
        workspace_id: WorkspaceId,
        content: String,
        expected_digest: Option<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Query {
    Bootstrap {
        workspace: String,
        session_id: Option<SessionId>,
    },
    GetSoul {
        workspace_id: WorkspaceId,
    },
    GetMcpManagement {
        workspace_id: WorkspaceId,
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
    use crate::{ArtifactId, EntryId, ModelId, RunId, SessionId, WorkspaceId};

    #[test]
    fn logical_session_policy_commands_have_stable_wire_shapes() {
        let session_id = SessionId::from("session-1");
        let creation = Command::CreateSession {
            workspace_id: WorkspaceId::from("workspace-1"),
            title: Some("Dashboard assistant".to_owned()),
            model_id: Some(ModelId::from("anthropic/claude-opus-5")),
            options: ModelOptions {
                reasoning_effort: Some("high".to_owned()),
                fast_mode: false,
            },
            tools: None,
            initial_instructions: None,
            bridge: None,
            mcp_grant: None,
        };
        assert_eq!(
            serde_json::to_value(creation).expect("serialize configured session creation"),
            json!({
                "type": "create_session",
                "workspace_id": "workspace-1",
                "title": "Dashboard assistant",
                "model_id": "anthropic/claude-opus-5",
                "options": {
                    "reasoning_effort": "high",
                    "fast_mode": false
                },
                "tools": null
            }),
        );
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
