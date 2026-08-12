//! Nakode-owned MCP management, streamable-HTTP discovery, invocation, and redaction.

use std::{collections::HashSet, time::Duration};

use futures_util::StreamExt;
use nakode_protocol::{
    MCP_TOOL_PREFIX, McpGrantPolicy, McpServerInput, McpServerView, McpSessionGrant,
    McpSessionSurface, McpTemplateView, McpToolView, WorkspaceId,
};
use reqwest::{Client, Response, header};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
pub const DEFAULT_TIMEOUT_MS: u32 = 20_000;
pub const DEFAULT_MAX_RESPONSE_BYTES: u32 = 1_048_576;
const ABSOLUTE_MAX_RESPONSE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_ARGUMENT_BYTES: usize = 128 * 1024;
const MAX_TOOL_COUNT: usize = 256;

pub const EXCALIDRAW_TEMPLATE_ID: &str = "excalidraw-remote-v0-3-2";
pub const EXCALIDRAW_SERVER_ID: &str = "excalidraw";
pub const EXCALIDRAW_ENDPOINT: &str = "https://mcp.excalidraw.com/mcp";
pub const EXCALIDRAW_REPOSITORY: &str = "https://github.com/excalidraw/excalidraw-mcp";
pub const EXCALIDRAW_VERSION: &str = "v0.3.2";
pub const EXCALIDRAW_COMMIT: &str = "157aa23ceb1976008aadc89eb05e3444060f09d6";
pub const EXCALIDRAW_SHA256: &str =
    "2b494012b5fee5937f9f7b86f04a76cc4a91ec843ee3339b93e4e15e415274ff";
pub const EXCALIDRAW_LICENSE_EVIDENCE: &str =
    "Repository package/manifest declare MIT; no top-level GitHub license metadata was observed.";
pub const EXCALIDRAW_ARTIFACT_SEMANTICS: &str = "create_view and checkpoints are temporary continuation handles, not Excalidraw Plus documents. The service may use in-memory storage or optional Redis with a 30-day TTL. Updating means restoring a checkpoint and applying element changes. Nakode never invokes the app-only export tool and never publishes, shares, uploads, or overwrites a drawing automatically.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerRecord {
    pub id: String,
    pub workspace: String,
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
    pub health: String,
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub last_error: Option<String>,
    pub last_connected_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    pub credential_kind: Option<String>,
    pub tools: Vec<McpToolView>,
    pub grants: McpGrantPolicy,
}

impl McpServerRecord {
    #[must_use]
    pub fn view(&self) -> McpServerView {
        McpServerView {
            id: self.id.clone(),
            workspace_id: WorkspaceId::from(self.workspace.clone()),
            display_name: self.display_name.clone(),
            endpoint: self.endpoint.clone(),
            transport: self.transport.clone(),
            enabled: self.enabled,
            health: self.health.clone(),
            credential_required: self.credential_required,
            credential_configured: self.credential_kind.is_some(),
            credential_kind: self.credential_kind.clone(),
            protocol_version: self.protocol_version.clone(),
            server_name: self.server_name.clone(),
            server_version: self.server_version.clone(),
            provenance_url: self.provenance_url.clone(),
            provenance_version: self.provenance_version.clone(),
            provenance_commit: self.provenance_commit.clone(),
            provenance_sha256: self.provenance_sha256.clone(),
            license_evidence: self.license_evidence.clone(),
            last_error: self.last_error.clone(),
            last_connected_at_ms: self.last_connected_at_ms,
            updated_at_ms: self.updated_at_ms,
            timeout_ms: self.timeout_ms,
            max_response_bytes: self.max_response_bytes,
            artifact_semantics: self.artifact_semantics.clone(),
            template_id: self.template_id.clone(),
            tools: self.tools.clone(),
            grants: self.grants.clone(),
        }
    }

    #[must_use]
    pub fn usable(&self) -> bool {
        self.enabled
            && self.health == "connected"
            && (!self.credential_required || self.credential_kind.is_some())
    }

    #[must_use]
    pub fn surface_granted(&self, surface: McpSessionSurface) -> bool {
        match surface {
            McpSessionSurface::Chat => self.grants.chat,
            McpSessionSurface::CodingAgent => self.grants.coding_agent,
        }
    }
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("invalid MCP configuration: {0}")]
    Invalid(String),
    #[error("MCP transport failed: {0}")]
    Transport(String),
    #[error("MCP response exceeded the {0} byte limit")]
    ResponseTooLarge(u32),
    #[error("MCP operation timed out")]
    Timeout,
    #[error("MCP operation was cancelled")]
    Cancelled,
    #[error("MCP protocol failed: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug)]
pub struct McpCredential {
    pub kind: String,
    pub secret: String,
}

#[derive(Clone, Debug)]
pub struct DiscoveryResult {
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub tools: Vec<McpToolView>,
}

#[derive(Clone)]
pub struct McpClient {
    http: Client,
}

impl Default for McpClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("static MCP HTTP client configuration is valid"),
        }
    }
}

impl McpClient {
    /// Discovers one MCP server and normalizes its model-visible tools.
    ///
    /// # Errors
    /// Returns a validation, transport, timeout, cancellation, size, or protocol error.
    pub async fn discover(
        &self,
        server: &McpServerRecord,
        credential: Option<&McpCredential>,
        cancellation: &CancellationToken,
    ) -> Result<DiscoveryResult, McpError> {
        validate_server(server)?;
        resolve_public_endpoint(server, cancellation).await?;
        let initialize = self
            .request(
                server,
                credential,
                1,
                "initialize",
                Some(json!({
                    "protocolVersion": server.protocol_version,
                    "capabilities": {},
                    "clientInfo": {"name":"Nakode","version":env!("CARGO_PKG_VERSION")}
                })),
                cancellation,
            )
            .await?;
        let session_id = initialize.session_id;
        self.notification(
            server,
            credential,
            "notifications/initialized",
            session_id.as_deref(),
            cancellation,
        )
        .await?;
        let tools = self
            .request_with_session(
                server,
                credential,
                2,
                "tools/list",
                Some(json!({})),
                session_id.as_deref(),
                cancellation,
            )
            .await?
            .value;
        let server_info = initialize
            .value
            .get("result")
            .and_then(|result| result.get("serverInfo"));
        Ok(DiscoveryResult {
            server_name: server_info
                .and_then(|info| info.get("name"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            server_version: server_info
                .and_then(|info| info.get("version"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            tools: normalize_tools(server, &tools)?,
        })
    }

    /// Invokes one discovered remote tool.
    ///
    /// # Errors
    /// Returns a validation, transport, timeout, cancellation, size, or protocol error.
    pub async fn invoke(
        &self,
        server: &McpServerRecord,
        remote_tool: &str,
        arguments_json: &str,
        credential: Option<&McpCredential>,
        cancellation: &CancellationToken,
    ) -> Result<String, McpError> {
        validate_server(server)?;
        resolve_public_endpoint(server, cancellation).await?;
        if arguments_json.len() > MAX_ARGUMENT_BYTES {
            return Err(McpError::Invalid(format!(
                "tool arguments exceed {MAX_ARGUMENT_BYTES} bytes"
            )));
        }
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|error| McpError::Invalid(format!("invalid tool arguments: {error}")))?;
        let initialize = self
            .request(
                server,
                credential,
                1,
                "initialize",
                Some(json!({
                    "protocolVersion": server.protocol_version,
                    "capabilities": {},
                    "clientInfo": {"name":"Nakode","version":env!("CARGO_PKG_VERSION")}
                })),
                cancellation,
            )
            .await?;
        let session_id = initialize.session_id;
        self.notification(
            server,
            credential,
            "notifications/initialized",
            session_id.as_deref(),
            cancellation,
        )
        .await?;
        let response = self
            .request_with_session(
                server,
                credential,
                2,
                "tools/call",
                Some(json!({"name": remote_tool, "arguments": arguments})),
                session_id.as_deref(),
                cancellation,
            )
            .await?
            .value;
        if let Some(error) = response.get("error") {
            return Err(McpError::Protocol(redact_error(
                error.to_string(),
                credential,
            )));
        }
        serde_json::to_string(response.get("result").unwrap_or(&response))
            .map_err(|error| McpError::Protocol(error.to_string()))
    }

    async fn notification(
        &self,
        server: &McpServerRecord,
        credential: Option<&McpCredential>,
        method: &str,
        session_id: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<(), McpError> {
        let body = json!({"jsonrpc":"2.0","method":method});
        let response = self
            .send(server, credential, &body, session_id, cancellation)
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(McpError::Transport(format!(
                "HTTP {}",
                response.status().as_u16()
            )))
        }
    }

    async fn request(
        &self,
        server: &McpServerRecord,
        credential: Option<&McpCredential>,
        id: u64,
        method: &str,
        params: Option<Value>,
        cancellation: &CancellationToken,
    ) -> Result<McpResponse, McpError> {
        self.request_with_session(server, credential, id, method, params, None, cancellation)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_with_session(
        &self,
        server: &McpServerRecord,
        credential: Option<&McpCredential>,
        id: u64,
        method: &str,
        params: Option<Value>,
        session_id: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<McpResponse, McpError> {
        let mut body = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            body["params"] = params;
        }
        let response = self
            .send(server, credential, &body, session_id, cancellation)
            .await?;
        let response_session = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "HTTP {}",
                response.status().as_u16()
            )));
        }
        let value = read_response(response, server.max_response_bytes, cancellation)
            .await
            .map_err(|error| redact_mcp_error(error, credential))?;
        if let Some(error) = value.get("error") {
            return Err(McpError::Protocol(redact_error(
                error.to_string(),
                credential,
            )));
        }
        Ok(McpResponse {
            value,
            session_id: response_session.or_else(|| session_id.map(str::to_owned)),
        })
    }

    async fn send(
        &self,
        server: &McpServerRecord,
        credential: Option<&McpCredential>,
        body: &Value,
        session_id: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<Response, McpError> {
        let resolved = resolve_public_endpoint(server, cancellation).await?;
        let mut request = if let Some(address) = resolved {
            let endpoint = reqwest::Url::parse(&server.endpoint)
                .map_err(|error| McpError::Invalid(format!("invalid endpoint: {error}")))?;
            let host = endpoint
                .host_str()
                .ok_or_else(|| McpError::Invalid("MCP endpoint host is required".to_owned()))?;
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .resolve(host, address)
                .build()
                .map_err(|error| McpError::Transport(redact_error(error.to_string(), credential)))?
                .post(&server.endpoint)
        } else {
            self.http.post(&server.endpoint)
        }
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .json(body);
        if let Some(session_id) = session_id {
            request = request.header("mcp-session-id", session_id);
        }
        if let Some(credential) = credential {
            request = match credential.kind.as_str() {
                "bearer" | "oauth" => request.bearer_auth(&credential.secret),
                "api_key" => request.header("x-api-key", &credential.secret),
                other => {
                    return Err(McpError::Invalid(format!(
                        "unsupported credential kind {other:?}"
                    )));
                }
            };
        }
        let future = request.send();
        let timeout = Duration::from_millis(u64::from(server.timeout_ms));
        tokio::select! {
            () = cancellation.cancelled() => Err(McpError::Cancelled),
            result = tokio::time::timeout(timeout, future) => match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(error)) => Err(McpError::Transport(redact_error(error.to_string(), credential))),
                Err(_) => Err(McpError::Timeout),
            }
        }
    }
}

struct McpResponse {
    value: Value,
    session_id: Option<String>,
}

async fn read_response(
    response: Response,
    configured_limit: u32,
    cancellation: &CancellationToken,
) -> Result<Value, McpError> {
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let limit = configured_limit.clamp(1, ABSOLUTE_MAX_RESPONSE_BYTES);
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => return Err(McpError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|error| McpError::Transport(error.to_string()))?;
        if bytes.len().saturating_add(chunk.len()) > limit as usize {
            return Err(McpError::ResponseTooLarge(limit));
        }
        bytes.extend_from_slice(&chunk);
    }
    if content_type.contains("text/event-stream") {
        parse_sse(&bytes)
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|error| McpError::Protocol(format!("invalid JSON response: {error}")))
    }
}

fn parse_sse(bytes: &[u8]) -> Result<Value, McpError> {
    let source = std::str::from_utf8(bytes)
        .map_err(|error| McpError::Protocol(format!("invalid SSE UTF-8: {error}")))?;
    let data = source
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() {
        return Err(McpError::Protocol(
            "SSE response had no data event".to_owned(),
        ));
    }
    serde_json::from_str(&data)
        .map_err(|error| McpError::Protocol(format!("invalid SSE JSON: {error}")))
}

fn normalize_tools(
    server: &McpServerRecord,
    response: &Value,
) -> Result<Vec<McpToolView>, McpError> {
    let tools = response
        .get("result")
        .and_then(|result| result.get("tools"))
        .and_then(Value::as_array)
        .ok_or_else(|| McpError::Protocol("tools/list returned no tools array".to_owned()))?;
    if tools.len() > MAX_TOOL_COUNT {
        return Err(McpError::Protocol(format!(
            "tools/list returned more than {MAX_TOOL_COUNT} tools"
        )));
    }
    let mut exposed = HashSet::new();
    tools
        .iter()
        .map(|tool| {
            let remote_name = tool
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| McpError::Protocol("tool has no name".to_owned()))?
                .to_owned();
            let app_only = tool
                .get("_meta")
                .and_then(|meta| meta.get("ui"))
                .and_then(|ui| ui.get("visibility"))
                .and_then(Value::as_array)
                .is_some_and(|visibility| {
                    visibility.iter().any(|value| value.as_str() == Some("app"))
                });
            let exposed_name = exposed_tool_name(&server.id, &remote_name);
            if !exposed.insert(exposed_name.clone()) {
                return Err(McpError::Protocol(format!(
                    "tool name collision after normalization: {remote_name}"
                )));
            }
            Ok(McpToolView {
                remote_name,
                exposed_name,
                description: tool
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("MCP tool")
                    .to_owned(),
                input_schema_json: serde_json::to_string(
                    tool.get("inputSchema").unwrap_or(&json!({"type":"object"})),
                )
                .map_err(|error| McpError::Protocol(error.to_string()))?,
                app_only,
            })
        })
        .filter(|result| !matches!(result, Ok(tool) if tool.app_only))
        .collect()
}

#[must_use]
pub fn exposed_tool_name(server_id: &str, remote_name: &str) -> String {
    format!(
        "{MCP_TOOL_PREFIX}{}__{}",
        slug(server_id),
        slug(remote_name)
    )
}

#[must_use]
pub fn split_exposed_tool_name(name: &str) -> Option<(&str, &str)> {
    name.strip_prefix(MCP_TOOL_PREFIX)?.split_once("__")
}

fn slug(value: &str) -> String {
    let mut result = String::new();
    let mut separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            if separator && !result.is_empty() {
                result.push('_');
            }
            result.push(character);
            separator = false;
        } else {
            separator = true;
        }
    }
    result.trim_matches('_').to_owned()
}

/// Validates one durable server definition against the supported network and transport boundary.
///
/// # Errors
/// Returns an error when the definition violates endpoint, transport, timeout, or size policy.
pub fn validate_server(server: &McpServerRecord) -> Result<(), McpError> {
    if server.id.trim().is_empty() || slug(&server.id).is_empty() {
        return Err(McpError::Invalid("server id is required".to_owned()));
    }
    if server.transport != "streamable_http" {
        return Err(McpError::Invalid(
            "only streamable_http transport is currently supported".to_owned(),
        ));
    }
    let endpoint = reqwest::Url::parse(&server.endpoint)
        .map_err(|error| McpError::Invalid(format!("invalid endpoint: {error}")))?;
    if endpoint.scheme() != "https" {
        return Err(McpError::Invalid("MCP endpoints must use HTTPS".to_owned()));
    }
    if endpoint.username() != "" || endpoint.password().is_some() {
        return Err(McpError::Invalid(
            "MCP endpoint credentials must not be embedded in URLs".to_owned(),
        ));
    }
    let Some(host) = endpoint.host_str() else {
        return Err(McpError::Invalid(
            "MCP endpoint host is required".to_owned(),
        ));
    };
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return Err(McpError::Invalid(
            "MCP endpoints must not target localhost".to_owned(),
        ));
    }
    if let Ok(address) = host.parse::<std::net::IpAddr>()
        && !public_ip(address)
    {
        return Err(McpError::Invalid(
            "MCP endpoints must use a public network address".to_owned(),
        ));
    }
    if !(1_000..=120_000).contains(&server.timeout_ms) {
        return Err(McpError::Invalid(
            "timeout_ms must be between 1000 and 120000".to_owned(),
        ));
    }
    if !(1..=ABSOLUTE_MAX_RESPONSE_BYTES).contains(&server.max_response_bytes) {
        return Err(McpError::Invalid(format!(
            "max_response_bytes must be 1-{ABSOLUTE_MAX_RESPONSE_BYTES}"
        )));
    }
    Ok(())
}

async fn resolve_public_endpoint(
    server: &McpServerRecord,
    cancellation: &CancellationToken,
) -> Result<Option<std::net::SocketAddr>, McpError> {
    let endpoint = reqwest::Url::parse(&server.endpoint)
        .map_err(|error| McpError::Invalid(format!("invalid endpoint: {error}")))?;
    let host = endpoint
        .host_str()
        .ok_or_else(|| McpError::Invalid("MCP endpoint host is required".to_owned()))?;
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(None);
    }
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| McpError::Invalid("MCP endpoint port is required".to_owned()))?;
    let lookup = tokio::net::lookup_host((host, port));
    let addresses = tokio::select! {
        () = cancellation.cancelled() => return Err(McpError::Cancelled),
        result = tokio::time::timeout(Duration::from_secs(5), lookup) => match result {
            Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
            Ok(Err(error)) => return Err(McpError::Transport(format!("endpoint DNS lookup failed: {error}"))),
            Err(_) => return Err(McpError::Timeout),
        }
    };
    if addresses.is_empty() || addresses.iter().any(|address| !public_ip(address.ip())) {
        return Err(McpError::Invalid(
            "MCP endpoint DNS resolved to a non-public network address".to_owned(),
        ));
    }
    Ok(addresses.into_iter().next())
}

fn public_ip(address: std::net::IpAddr) -> bool {
    match address {
        std::net::IpAddr::V4(address) => {
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified()
                || address.is_multicast()
                || address.octets()[0] == 0)
        }
        std::net::IpAddr::V6(address) => {
            !(address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| !public_ip(mapped.into()))
                || (address.segments()[0] & 0xfe00) == 0xfc00
                || (address.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

#[must_use]
pub fn input_record(
    workspace: &WorkspaceId,
    input: McpServerInput,
    grants: McpGrantPolicy,
) -> McpServerRecord {
    McpServerRecord {
        id: input.id,
        workspace: workspace.to_string(),
        display_name: input.display_name,
        endpoint: input.endpoint,
        transport: input.transport,
        enabled: input.enabled,
        auth_kind: input.auth_kind,
        credential_required: input.credential_required,
        protocol_version: if input.protocol_version.is_empty() {
            DEFAULT_PROTOCOL_VERSION.to_owned()
        } else {
            input.protocol_version
        },
        provenance_url: input.provenance_url,
        provenance_version: input.provenance_version,
        provenance_commit: input.provenance_commit,
        provenance_sha256: input.provenance_sha256,
        license_evidence: input.license_evidence,
        timeout_ms: if input.timeout_ms == 0 {
            DEFAULT_TIMEOUT_MS
        } else {
            input.timeout_ms
        },
        max_response_bytes: if input.max_response_bytes == 0 {
            DEFAULT_MAX_RESPONSE_BYTES
        } else {
            input.max_response_bytes
        },
        artifact_semantics: input.artifact_semantics,
        template_id: input.template_id,
        health: "saved".to_owned(),
        server_name: None,
        server_version: None,
        last_error: None,
        last_connected_at_ms: None,
        updated_at_ms: unix_time_ms(),
        credential_kind: None,
        tools: Vec::new(),
        grants,
    }
}

#[must_use]
pub fn excalidraw_template() -> McpTemplateView {
    McpTemplateView {
        id: EXCALIDRAW_TEMPLATE_ID.to_owned(),
        display_name: "Excalidraw".to_owned(),
        description: "Create and continue temporary Excalidraw checkpoint artifacts through the Excalidraw-org remote MCP service.".to_owned(),
        endpoint: EXCALIDRAW_ENDPOINT.to_owned(),
        provenance_url: EXCALIDRAW_REPOSITORY.to_owned(),
        provenance_version: EXCALIDRAW_VERSION.to_owned(),
        provenance_commit: EXCALIDRAW_COMMIT.to_owned(),
        provenance_sha256: EXCALIDRAW_SHA256.to_owned(),
        license_evidence: EXCALIDRAW_LICENSE_EVIDENCE.to_owned(),
        artifact_semantics: EXCALIDRAW_ARTIFACT_SEMANTICS.to_owned(),
        credential_required: true,
    }
}

#[must_use]
pub fn excalidraw_input() -> McpServerInput {
    McpServerInput {
        id: EXCALIDRAW_SERVER_ID.to_owned(),
        display_name: "Excalidraw".to_owned(),
        endpoint: EXCALIDRAW_ENDPOINT.to_owned(),
        transport: "streamable_http".to_owned(),
        enabled: true,
        auth_kind: "bearer".to_owned(),
        credential_required: true,
        protocol_version: DEFAULT_PROTOCOL_VERSION.to_owned(),
        provenance_url: EXCALIDRAW_REPOSITORY.to_owned(),
        provenance_version: EXCALIDRAW_VERSION.to_owned(),
        provenance_commit: EXCALIDRAW_COMMIT.to_owned(),
        provenance_sha256: EXCALIDRAW_SHA256.to_owned(),
        license_evidence: EXCALIDRAW_LICENSE_EVIDENCE.to_owned(),
        timeout_ms: DEFAULT_TIMEOUT_MS,
        max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        artifact_semantics: EXCALIDRAW_ARTIFACT_SEMANTICS.to_owned(),
        template_id: Some(EXCALIDRAW_TEMPLATE_ID.to_owned()),
    }
}

/// Resolves an explicit session grant to currently usable servers.
///
/// # Errors
/// Returns an error for an omitted surface, duplicate/unknown server, denied surface, or unusable server.
pub fn validate_grant(
    grant: &McpSessionGrant,
    servers: &[McpServerRecord],
) -> Result<Vec<McpServerRecord>, McpError> {
    let surface = grant
        .surface
        .ok_or_else(|| McpError::Invalid("MCP session surface is required".to_owned()))?;
    let mut seen = HashSet::new();
    grant
        .server_ids
        .iter()
        .map(|id| {
            if !seen.insert(id) {
                return Err(McpError::Invalid(format!(
                    "duplicate MCP server grant {id:?}"
                )));
            }
            let server = servers
                .iter()
                .find(|server| server.id == *id)
                .ok_or_else(|| McpError::Invalid(format!("unknown MCP server grant {id:?}")))?;
            if !server.surface_granted(surface) {
                return Err(McpError::Invalid(format!(
                    "MCP server {id:?} is not granted for {surface:?} sessions"
                )));
            }
            if !server.usable() {
                return Err(McpError::Invalid(format!(
                    "MCP server {id:?} is not enabled, credential-ready, connected, and discovered"
                )));
            }
            Ok(server.clone())
        })
        .collect()
}

#[must_use]
pub fn normalize_builtin_server(mut server: McpServerRecord) -> McpServerRecord {
    if server.id == EXCALIDRAW_SERVER_ID
        && server.template_id.as_deref() == Some(EXCALIDRAW_TEMPLATE_ID)
    {
        "bearer".clone_into(&mut server.auth_kind);
        server.credential_required = true;
        if server.credential_kind.is_none() {
            "credential_required".clone_into(&mut server.health);
            server.tools.clear();
        }
    }
    server
}

#[must_use]
pub fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn redact_mcp_error(error: McpError, credential: Option<&McpCredential>) -> McpError {
    match error {
        McpError::Invalid(message) => McpError::Invalid(redact_error(message, credential)),
        McpError::Transport(message) => McpError::Transport(redact_error(message, credential)),
        McpError::Protocol(message) => McpError::Protocol(redact_error(message, credential)),
        other => other,
    }
}

fn redact_error(mut message: String, credential: Option<&McpCredential>) -> String {
    if let Some(credential) = credential.filter(|credential| !credential.secret.is_empty()) {
        message = message.replace(&credential.secret, "[REDACTED]");
    }
    message
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn every_message_error_is_redacted() {
        let credential = McpCredential {
            kind: "bearer".to_owned(),
            secret: "sensitive-token".to_owned(),
        };
        for error in [
            McpError::Invalid("bad sensitive-token".to_owned()),
            McpError::Transport("failed sensitive-token".to_owned()),
            McpError::Protocol("invalid sensitive-token".to_owned()),
        ] {
            let message = redact_mcp_error(error, Some(&credential)).to_string();
            assert!(!message.contains("sensitive-token"));
            assert!(message.contains("[REDACTED]"));
        }
    }

    #[test]
    fn excalidraw_requires_a_bearer_token() {
        let input = excalidraw_input();
        assert_eq!(input.auth_kind, "bearer");
        assert!(input.credential_required);
        assert!(excalidraw_template().credential_required);
    }

    #[test]
    fn legacy_excalidraw_records_are_migrated_without_a_secret() {
        let workspace = WorkspaceId::from("workspace");
        let mut server = input_record(&workspace, excalidraw_input(), McpGrantPolicy::default());
        server.auth_kind = "none".to_owned();
        server.credential_required = false;
        server.health = "connected".to_owned();
        server.tools.push(McpToolView {
            remote_name: "create_view".to_owned(),
            exposed_name: "mcp__excalidraw__create_view".to_owned(),
            description: String::new(),
            input_schema_json: "{}".to_owned(),
            app_only: false,
        });

        let migrated = normalize_builtin_server(server.clone());
        assert_eq!(migrated.auth_kind, "bearer");
        assert!(migrated.credential_required);
        assert_eq!(migrated.health, "credential_required");
        assert!(migrated.tools.is_empty());
        server.credential_kind = Some("bearer".to_owned());
        server.health = "connected".to_owned();
        let normalized = normalize_builtin_server(server);
        assert_eq!(normalized.health, "connected");
        assert_eq!(normalized.credential_kind.as_deref(), Some("bearer"));
    }

    #[test]
    fn app_only_tools_are_filtered_and_names_are_stable() {
        let workspace = WorkspaceId::from("workspace");
        let server = input_record(&workspace, excalidraw_input(), McpGrantPolicy::default());
        let result = normalize_tools(&server, &json!({"result":{"tools":[
            {"name":"create_view","description":"Create","inputSchema":{"type":"object"}},
            {"name":"export_to_excalidraw","_meta":{"ui":{"visibility":["app"]}},"inputSchema":{"type":"object"}}
        ]}})).expect("normalize tools");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].exposed_name, "mcp__excalidraw__create_view");
    }

    #[test]
    fn grants_are_deny_by_default() {
        let workspace = WorkspaceId::from("workspace");
        let mut server = input_record(&workspace, excalidraw_input(), McpGrantPolicy::default());
        server.health = "connected".to_owned();
        let grant = McpSessionGrant {
            surface: Some(McpSessionSurface::Chat),
            server_ids: vec![server.id.clone()],
        };
        assert!(validate_grant(&grant, &[server]).is_err());
    }
}
