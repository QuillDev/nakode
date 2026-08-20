use std::fmt::Write;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64,
    resolve_workspace_path,
    truncate::{DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, TruncatedBy, truncate_head},
};
use crate::runtime::ToolDefinition;

const MAX_FILE_READ_BYTES: u64 = 8 * 1024 * 1024;

pub struct ReadTool;

impl Tool for ReadTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "read",
            description: "Read one UTF-8 text file. Prefer read over bash commands such as cat or sed. Output is limited to 2000 lines or 50 KB. For large files, use offset and limit, then continue with a later offset until complete. Issue parallel read calls for independent files or ranges.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative path to the file"
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "First line to read, one-indexed; defaults to 1"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Maximum lines to read; the 50 KB output limit still applies"
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        let path = arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match (
            arguments.get("offset").and_then(Value::as_u64),
            arguments.get("limit").and_then(Value::as_u64),
        ) {
            (None, None) => path.to_owned(),
            (offset, limit) => format!(
                "{path}:{}+{}",
                offset.unwrap_or(1),
                limit.map_or_else(|| "all".to_owned(), |limit| limit.to_string())
            ),
        }
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
                return ToolResult::failure("read interrupted");
            }
            match read_file(context.workspace, &arguments, cancellation).await {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn read_file(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let supplied = super::required_string(arguments, "path")?;
    let path = resolve_workspace_path(workspace, supplied)?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| format!("could not read {supplied}: {error}"))?;
    if metadata.is_dir() {
        return Err(format!("{supplied} is a directory; use ls to list it"));
    }
    if metadata.len() > MAX_FILE_READ_BYTES {
        return Err(format!(
            "{supplied} is {} bytes; read is limited to {MAX_FILE_READ_BYTES} bytes",
            metadata.len()
        ));
    }
    let contents = tokio::fs::read(&path)
        .await
        .map_err(|error| format!("could not read {supplied}: {error}"))?;
    if cancellation.is_cancelled() {
        return Err("read interrupted".to_owned());
    }
    if contents.contains(&0) {
        return Err(format!(
            "{supplied} appears to be binary; use vision for supported images"
        ));
    }
    let text = String::from_utf8(contents).map_err(|_| format!("{supplied} is not valid UTF-8"))?;
    let lines = text.split('\n').collect::<Vec<_>>();
    let total_lines = lines.len();
    let offset = usize::try_from(optional_u64(arguments, "offset", 1)?)
        .unwrap_or(1)
        .max(1);
    if offset > total_lines {
        return Err(format!(
            "offset {offset} is beyond the end of {supplied} ({total_lines} lines)"
        ));
    }
    let start = offset - 1;
    let requested_limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .and_then(|limit| usize::try_from(limit).ok());
    let end = requested_limit.map_or(total_lines, |limit| {
        start.saturating_add(limit).min(total_lines)
    });
    let selected = lines[start..end].join("\n");
    let truncation = truncate_head(&selected, DEFAULT_MAX_LINES, DEFAULT_MAX_BYTES);
    if truncation.first_line_exceeds_limit {
        return Ok(format!(
            "[Line {offset} exceeds the {DEFAULT_MAX_BYTES}-byte read limit. Use bash with a byte-bounded command to inspect it.]"
        ));
    }
    let mut output = truncation.content;
    let displayed_end = offset
        .saturating_add(truncation.output_lines)
        .saturating_sub(1);
    if let Some(truncated_by) = truncation.truncated_by {
        let reason = match truncated_by {
            TruncatedBy::Lines => format!("{DEFAULT_MAX_LINES} line limit"),
            TruncatedBy::Bytes => format!("{DEFAULT_MAX_BYTES}-byte limit"),
        };
        let _ = write!(
            output,
            "\n\n[Showing lines {offset}-{displayed_end} of {total_lines} ({reason}). Use offset={} to continue.]",
            displayed_end + 1
        );
    } else if end < total_lines {
        let _ = write!(
            output,
            "\n\n[{} more lines in file. Use offset={} to continue.]",
            total_lines - end,
            end + 1
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn reads_a_typed_line_window_without_line_prefixes() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file.txt"), "one\ntwo\nthree\nfour")
            .expect("fixture");

        let output = super::read_file(
            workspace.path(),
            &json!({"path": "file.txt", "offset": 2, "limit": 2}),
            &CancellationToken::new(),
        )
        .await
        .expect("read");

        assert_eq!(
            output,
            "two\nthree\n\n[1 more lines in file. Use offset=4 to continue.]"
        );
    }

    #[tokio::test]
    async fn directories_point_to_ls() {
        let workspace = tempfile::tempdir().expect("workspace");
        let error = super::read_file(
            workspace.path(),
            &json!({"path": "."}),
            &CancellationToken::new(),
        )
        .await
        .expect_err("directory read");

        assert!(error.contains("use ls"));
    }

    #[tokio::test]
    async fn offset_past_the_end_is_explicit() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file.txt"), "one").expect("fixture");

        let error = super::read_file(
            workspace.path(),
            &json!({"path": "file.txt", "offset": 2}),
            &CancellationToken::new(),
        )
        .await
        .expect_err("invalid offset");

        assert!(error.contains("beyond the end"));
    }
}
