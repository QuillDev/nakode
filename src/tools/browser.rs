use std::{
    ffi::OsString,
    sync::{Arc, RwLock},
    time::Duration,
};

use reqwest::Client;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64,
    process::{ProcessRequest, run_process},
    required_string, truncate_output,
};
use crate::{
    runtime::ToolDefinition,
    web::{WebBackend, WebConfig},
};

pub struct BrowserTool {
    config: Arc<RwLock<WebConfig>>,
    client: Client,
}

impl BrowserTool {
    #[must_use]
    pub fn new(config: Arc<RwLock<WebConfig>>) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    fn config(&self) -> WebConfig {
        self.config
            .read()
            .map_or_else(|_| WebConfig::default(), |config| config.clone())
    }
}

impl Tool for BrowserTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "browser",
            description: "Search the web or open a web page using the optional browser backend configured in /settings. Use search for discovery and open for a known URL.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["search", "open"] },
                    "query": { "type": "string", "description": "Search query; required for search." },
                    "url": { "type": "string", "description": "HTTP(S) URL; required for open." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 10, "default": 5 }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("query")
            .or_else(|| arguments.get("url"))
            .and_then(Value::as_str)
            .unwrap_or("browse web")
            .chars()
            .take(100)
            .collect()
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::ReadOnly
    }

    fn available(&self) -> bool {
        self.config().is_available()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let config = self.config();
            let result = match config.backend {
                WebBackend::Disabled => {
                    Err("Browser add-on is disabled. Configure it in /settings.".to_owned())
                }
                WebBackend::AgentBrowser => {
                    agent_browser(context.workspace, &arguments, cancellation).await
                }
                WebBackend::Firecrawl => {
                    firecrawl(
                        &self.client,
                        &config.firecrawl_api_key,
                        &arguments,
                        cancellation,
                    )
                    .await
                }
            };
            result.unwrap_or_else(ToolResult::failure)
        })
    }
}

async fn agent_browser(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, String> {
    let action = required_string(arguments, "action")?;
    let target = match action {
        "search" => {
            let query = required_string(arguments, "query")?;
            format!("https://www.google.com/search?q={}", percent_encode(query))
        }
        "open" => validate_url(required_string(arguments, "url")?)?.to_owned(),
        _ => return Err("browser action must be search or open".to_owned()),
    };
    let open_args = vec![OsString::from("open"), OsString::from(&target)];
    let opened = run_process(
        workspace,
        ProcessRequest {
            program: "agent-browser",
            arguments: &open_args,
            input: None,
            environment: None,
            timeout: Some(Duration::from_secs(45)),
        },
        cancellation,
    )
    .await
    .map_err(|error| format!("agent-browser is not installed or could not start: {error}"))?;
    if !opened.success {
        return Ok(ToolResult::failure(opened.output));
    }
    let snapshot_args = vec![OsString::from("snapshot"), OsString::from("-c")];
    let snapshot = run_process(
        workspace,
        ProcessRequest {
            program: "agent-browser",
            arguments: &snapshot_args,
            input: None,
            environment: None,
            timeout: Some(Duration::from_secs(45)),
        },
        cancellation,
    )
    .await?;
    if snapshot.success {
        Ok(ToolResult::success(snapshot.output))
    } else {
        Ok(ToolResult::failure(snapshot.output))
    }
}

async fn firecrawl(
    client: &Client,
    api_key: &str,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, String> {
    if api_key.trim().is_empty() {
        return Err("Firecrawl API key is missing. Configure it in /settings.".to_owned());
    }
    let action = required_string(arguments, "action")?;
    let (endpoint, body) = match action {
        "search" => (
            "https://api.firecrawl.dev/v1/search",
            json!({
                "query": required_string(arguments, "query")?,
                "limit": optional_u64(arguments, "limit", 5)?.clamp(1, 10)
            }),
        ),
        "open" => (
            "https://api.firecrawl.dev/v1/scrape",
            json!({
                "url": validate_url(required_string(arguments, "url")?)?, "formats": ["markdown"]
            }),
        ),
        _ => return Err("browser action must be search or open".to_owned()),
    };
    let request = client
        .post(endpoint)
        .bearer_auth(api_key.trim())
        .json(&body);
    let response = tokio::select! {
        () = cancellation.cancelled() => return Err("browser request interrupted".to_owned()),
        response = request.send() => response.map_err(|error| format!("Firecrawl request failed: {error}"))?,
    };
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| format!("failed to read Firecrawl response: {error}"))?;
    let text = truncate_output(text.into_bytes());
    if status.is_success() {
        Ok(ToolResult::success(text))
    } else {
        Ok(ToolResult::failure(format!(
            "Firecrawl returned {status}: {text}"
        )))
    }
}

fn validate_url(url: &str) -> Result<&str, String> {
    if url.starts_with("https://") || url.starts_with("http://") {
        Ok(url)
    } else {
        Err("browser URL must start with http:// or https://".to_owned())
    }
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{percent_encode, validate_url};

    #[test]
    fn search_queries_are_url_encoded() {
        assert_eq!(percent_encode("rust web tools"), "rust%20web%20tools");
    }

    #[test]
    fn only_http_urls_are_accepted() {
        assert!(validate_url("https://example.com").is_ok());
        assert!(validate_url("file:///etc/passwd").is_err());
    }
}
