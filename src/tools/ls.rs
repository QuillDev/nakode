use std::fmt::Write;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64,
    resolve_workspace_path,
};
use crate::runtime::ToolDefinition;

const DEFAULT_LIMIT: usize = 500;
const MAX_LIMIT: usize = 10_000;

pub struct LsTool;

impl Tool for LsTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "ls",
            description: "List one directory alphabetically. Includes dotfiles and appends / to directory names.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory to list; defaults to the workspace"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LIMIT,
                        "description": "Maximum entries; defaults to 500"
                    }
                },
                "required": [],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(".")
            .to_owned()
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::ReadOnly
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return ToolResult::failure("ls interrupted");
            }
            match list_directory(context.workspace, &arguments, cancellation).await {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn list_directory(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let supplied = arguments
        .get("path")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .unwrap_or(".");
    let path = resolve_workspace_path(workspace, supplied)?;
    if !path.is_dir() {
        return Err(format!("ls path is not a directory: {}", path.display()));
    }
    let limit = usize::try_from(optional_u64(arguments, "limit", DEFAULT_LIMIT as u64)?)
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);
    let mut directory = tokio::fs::read_dir(&path)
        .await
        .map_err(|error| format!("cannot read directory {}: {error}", path.display()))?;
    let mut entries = Vec::new();
    while let Some(entry) = directory
        .next_entry()
        .await
        .map_err(|error| format!("cannot read directory {}: {error}", path.display()))?
    {
        if cancellation.is_cancelled() {
            return Err("ls interrupted".to_owned());
        }
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        let suffix = if file_type.is_dir() { "/" } else { "" };
        entries.push(format!("{}{suffix}", entry.file_name().to_string_lossy()));
    }
    entries.sort_by_cached_key(|entry| entry.to_lowercase());
    if entries.is_empty() {
        return Ok("(empty directory)".to_owned());
    }
    let limit_reached = entries.len() > limit;
    entries.truncate(limit);
    let mut output = entries.join("\n");
    if limit_reached {
        let _ = write!(
            output,
            "\n\n[{limit} entries limit reached. Increase limit to see more.]"
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn lists_dotfiles_and_marks_directories() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join(".hidden"), "").expect("hidden");
        std::fs::create_dir(workspace.path().join("src")).expect("directory");

        let output = super::list_directory(workspace.path(), &json!({}), &CancellationToken::new())
            .await
            .expect("listing");

        assert_eq!(output, ".hidden\nsrc/");
    }

    #[tokio::test]
    async fn empty_path_defaults_to_the_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file"), "").expect("fixture");

        let output = super::list_directory(
            workspace.path(),
            &json!({"path": ""}),
            &CancellationToken::new(),
        )
        .await
        .expect("listing");

        assert_eq!(output, "file");
    }
}
