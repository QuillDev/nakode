use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult, required_string, resolve_workspace_path,
    write::atomic_write,
};
use crate::runtime::ToolDefinition;

pub struct EditTool;

impl Tool for EditTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "edit",
            description: "Apply one or more exact replacements to an existing UTF-8 file. Edits run in order and the file is committed atomically only when every edit succeeds.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "edits": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "old_text": {"type": "string"},
                                "new_text": {"type": "string"},
                                "all": {"type": "boolean"}
                            },
                            "required": ["old_text", "new_text"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["path", "edits"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = edit_file(context.workspace, &arguments, cancellation).await;
            match result {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn edit_file(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    if cancellation.is_cancelled() {
        return Err("edit interrupted".to_owned());
    }
    let path = resolve_workspace_path(workspace, required_string(arguments, "path")?)?;
    let edits = arguments
        .get("edits")
        .and_then(Value::as_array)
        .filter(|edits| !edits.is_empty())
        .ok_or_else(|| "edit requires a non-empty edits array".to_owned())?;
    let mut contents = tokio::fs::read_to_string(&path)
        .await
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    for (index, edit) in edits.iter().enumerate() {
        if cancellation.is_cancelled() {
            return Err("edit interrupted".to_owned());
        }
        let old_text = required_string(edit, "old_text")?;
        let new_text = edit
            .get("new_text")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("edit {} is missing string argument new_text", index + 1))?;
        let replace_all = edit.get("all").and_then(Value::as_bool).unwrap_or(false);
        let occurrences = contents.matches(old_text).count();
        if occurrences == 0 {
            return Err(format!(
                "edit {} did not find its old_text in {}",
                index + 1,
                path.display()
            ));
        }
        if occurrences > 1 && !replace_all {
            return Err(format!(
                "edit {} matched {occurrences} locations in {}; add more context or set all=true",
                index + 1,
                path.display()
            ));
        }
        contents = if replace_all {
            contents.replace(old_text, new_text)
        } else {
            contents.replacen(old_text, new_text, 1)
        };
    }
    if cancellation.is_cancelled() {
        return Err("edit interrupted".to_owned());
    }
    atomic_write(&path, contents.as_bytes(), cancellation).await?;
    Ok(format!(
        "applied {} edit(s) to {}",
        edits.len(),
        path.display()
    ))
}
