use serde::{Deserialize, Serialize};

use crate::{ExternalToolDefinition, WorkspaceId};

/// Stable prefix reserved for Nakode-owned MCP tools at provider boundaries.
pub const MCP_TOOL_PREFIX: &str = "mcp__";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSessionSurface {
    Chat,
    CodingAgent,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpSessionGrant {
    pub surface: Option<McpSessionSurface>,
    #[serde(default)]
    pub server_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpServerInput {
    pub id: String,
    pub display_name: String,
    pub endpoint: String,
    pub transport: String,
    pub enabled: bool,
    pub auth_kind: String,
    pub credential_required: bool,
    pub protocol_version: String,
    pub provenance_url: String,
    pub provenance_version: String,
    pub provenance_commit: String,
    pub provenance_sha256: String,
    pub license_evidence: String,
    pub timeout_ms: u32,
    pub max_response_bytes: u32,
    pub artifact_semantics: String,
    pub template_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpGrantPolicy {
    pub chat: bool,
    pub coding_agent: bool,
    #[serde(default)]
    pub archetype_slugs: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpToolView {
    pub remote_name: String,
    pub exposed_name: String,
    pub description: String,
    pub input_schema_json: String,
    pub app_only: bool,
}

impl McpToolView {
    #[must_use]
    pub fn external_definition(&self) -> ExternalToolDefinition {
        ExternalToolDefinition {
            name: self.exposed_name.clone(),
            description: self.description.clone(),
            input_schema_json: self.input_schema_json.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpServerView {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub display_name: String,
    pub endpoint: String,
    pub transport: String,
    pub enabled: bool,
    pub health: String,
    pub credential_required: bool,
    pub credential_configured: bool,
    pub credential_kind: Option<String>,
    pub protocol_version: String,
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub provenance_url: String,
    pub provenance_version: String,
    pub provenance_commit: String,
    pub provenance_sha256: String,
    pub license_evidence: String,
    pub last_error: Option<String>,
    pub last_connected_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    pub timeout_ms: u32,
    pub max_response_bytes: u32,
    pub artifact_semantics: String,
    pub template_id: Option<String>,
    pub tools: Vec<McpToolView>,
    pub grants: McpGrantPolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpTemplateView {
    pub id: String,
    pub display_name: String,
    pub description: String,
    pub endpoint: String,
    pub provenance_url: String,
    pub provenance_version: String,
    pub provenance_commit: String,
    pub provenance_sha256: String,
    pub license_evidence: String,
    pub artifact_semantics: String,
    pub credential_required: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpManagementView {
    pub workspace_id: WorkspaceId,
    pub servers: Vec<McpServerView>,
    pub templates: Vec<McpTemplateView>,
}
