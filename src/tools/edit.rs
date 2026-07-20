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
            let hint = edit_match_hint(&contents, old_text);
            return Err(format!(
                "edit {} did not find its old_text in {}{hint}",
                index + 1,
                path.display()
            ));
        }
        if occurrences > 1 && !replace_all {
            let lines = exact_match_lines(&contents, old_text)
                .into_iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "edit {} matched {occurrences} locations in {} at lines {lines}; add more context or set all=true",
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

fn exact_match_lines(contents: &str, old_text: &str) -> Vec<usize> {
    contents
        .match_indices(old_text)
        .take(8)
        .map(|(byte_index, _)| {
            contents[..byte_index]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1
        })
        .collect()
}

fn edit_match_hint(contents: &str, old_text: &str) -> String {
    let Some(anchor) = old_text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
    else {
        return "; old_text is empty after trimming".to_owned();
    };
    let lines = contents
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(anchor))
        .take(8)
        .map(|(index, _)| index + 1)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        "; re-read the surrounding range before retrying".to_owned()
    } else {
        format!("; the first old_text line appears near lines {lines:?}; re-read those ranges")
    }
}

#[cfg(test)]
mod tests {
    use super::{edit_match_hint, exact_match_lines};

    #[test]
    fn duplicate_matches_report_their_source_lines() {
        let contents = "same\nother\nsame\n";
        assert_eq!(exact_match_lines(contents, "same"), [1, 3]);
    }

    #[test]
    fn near_match_hint_points_to_the_anchor_line() {
        let contents = "alpha\nbeta changed\ngamma\n";
        let hint = edit_match_hint(contents, "beta changed\nmissing tail");
        assert!(hint.contains("lines [2]"));
        assert!(hint.contains("re-read"));
    }
}
