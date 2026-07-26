use serde_json::{Map, Value, json};
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
            description: "Edit one UTF-8 file using exact text replacement. Every edits[].oldText must identify one unique, non-overlapping region of the original file. Combine nearby changes into one edit and use multiple entries for disjoint changes.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative path to the file"
                    },
                    "edits": {
                        "type": "array",
                        "minItems": 1,
                        "description": "Disjoint replacements, all matched against the original file",
                        "items": {
                            "type": "object",
                            "properties": {
                                "oldText": {
                                    "type": "string",
                                    "minLength": 1,
                                    "description": "Exact text for one unique replacement; keep it small but include enough context to be unique"
                                },
                                "newText": {
                                    "type": "string",
                                    "description": "Replacement text"
                                }
                            },
                            "required": ["oldText", "newText"],
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

    fn prepare_arguments(&self, mut arguments: Value) -> Value {
        let Some(object) = arguments.as_object_mut() else {
            return arguments;
        };
        if let Some(Value::String(encoded)) = object.get("edits")
            && let Ok(decoded) = serde_json::from_str::<Value>(encoded)
            && decoded.is_array()
        {
            object.insert("edits".to_owned(), decoded);
        }
        prepare_legacy_top_level_edit(object);
        if let Some(edits) = object.get_mut("edits").and_then(Value::as_array_mut) {
            for edit in edits {
                let Some(edit) = edit.as_object_mut() else {
                    continue;
                };
                rename_property(edit, "old_text", "oldText");
                rename_property(edit, "new_text", "newText");
                edit.remove("all");
            }
        }
        arguments
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            match edit_file(context.workspace, &arguments, cancellation).await {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

fn prepare_legacy_top_level_edit(object: &mut Map<String, Value>) {
    let old_text = object
        .remove("oldText")
        .or_else(|| object.remove("old_text"));
    let new_text = object
        .remove("newText")
        .or_else(|| object.remove("new_text"));
    if let (Some(old_text @ Value::String(_)), Some(new_text @ Value::String(_))) =
        (old_text, new_text)
    {
        let replacement = json!({"oldText": old_text, "newText": new_text});
        if let Some(edits) = object
            .entry("edits")
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
        {
            edits.push(replacement);
        }
    }
}

fn rename_property(object: &mut Map<String, Value>, old: &str, new: &str) {
    if !object.contains_key(new)
        && let Some(value) = object.remove(old)
    {
        object.insert(new.to_owned(), value);
    }
}

#[derive(Clone, Debug)]
struct MatchedEdit {
    index: usize,
    length: usize,
    replacement: String,
}

async fn edit_file(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    if cancellation.is_cancelled() {
        return Err("edit interrupted".to_owned());
    }
    let supplied = required_string(arguments, "path")?;
    let path = resolve_workspace_path(workspace, supplied)?;
    let edits = arguments
        .get("edits")
        .and_then(Value::as_array)
        .filter(|edits| !edits.is_empty())
        .ok_or_else(|| "edit requires a non-empty edits array".to_owned())?;
    let raw = tokio::fs::read_to_string(&path)
        .await
        .map_err(|error| format!("could not edit {supplied}: {error}"))?;
    let (bom, without_bom) = raw
        .strip_prefix('\u{feff}')
        .map_or(("", raw.as_str()), |content| ("\u{feff}", content));
    let line_ending = if without_bom.contains("\r\n") {
        "\r\n"
    } else {
        "\n"
    };
    let original = without_bom.replace("\r\n", "\n").replace('\r', "\n");
    let mut matched = Vec::with_capacity(edits.len());
    for (edit_index, edit) in edits.iter().enumerate() {
        if cancellation.is_cancelled() {
            return Err("edit interrupted".to_owned());
        }
        let old_text = required_string(edit, "oldText")?
            .replace("\r\n", "\n")
            .replace('\r', "\n");
        let new_text = edit
            .get("newText")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("edit {} is missing string argument newText", edit_index + 1))?
            .replace("\r\n", "\n")
            .replace('\r', "\n");
        let (index, length) =
            unique_match(&original, &old_text, supplied, edit_index, edits.len())?;
        matched.push(MatchedEdit {
            index,
            length,
            replacement: new_text,
        });
    }
    matched.sort_by_key(|edit| edit.index);
    for pair in matched.windows(2) {
        if pair[0].index.saturating_add(pair[0].length) > pair[1].index {
            return Err(
                "edit replacements overlap; merge nearby changes into one edits[] entry".to_owned(),
            );
        }
    }
    let mut updated = original;
    for edit in matched.iter().rev() {
        updated.replace_range(
            edit.index..edit.index.saturating_add(edit.length),
            &edit.replacement,
        );
    }
    if line_ending == "\r\n" {
        updated = updated.replace('\n', "\r\n");
    }
    let updated = format!("{bom}{updated}");
    if cancellation.is_cancelled() {
        return Err("edit interrupted".to_owned());
    }
    atomic_write(&path, updated.as_bytes(), cancellation).await?;
    Ok(format!(
        "Successfully replaced {} block(s) in {supplied}.",
        edits.len()
    ))
}

fn unique_match(
    contents: &str,
    old_text: &str,
    path: &str,
    edit_index: usize,
    total_edits: usize,
) -> Result<(usize, usize), String> {
    let exact = contents.match_indices(old_text).collect::<Vec<_>>();
    match exact.as_slice() {
        [(index, _)] => return Ok((*index, old_text.len())),
        [_, _, ..] => {
            return Err(duplicate_match_error(
                contents,
                old_text,
                path,
                edit_index,
                total_edits,
            ));
        }
        [] => {}
    }
    let fuzzy = fuzzy_line_matches(contents, old_text);
    match fuzzy.as_slice() {
        [matched] => Ok(*matched),
        [_, _, ..] => Err(format!(
            "edit {} matched multiple whitespace-normalized blocks in {path}; include more surrounding context",
            edit_index + 1
        )),
        [] => Err(format!(
            "edit {} could not find oldText in {path}; re-read the surrounding lines before retrying",
            edit_index + 1
        )),
    }
}

fn duplicate_match_error(
    contents: &str,
    old_text: &str,
    path: &str,
    edit_index: usize,
    _total_edits: usize,
) -> String {
    let lines = contents
        .match_indices(old_text)
        .take(8)
        .map(|(index, _)| {
            contents[..index]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1
        })
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "edit {} matched multiple locations in {path} at lines {lines}; keep oldText small but add enough surrounding context to make it unique",
        edit_index + 1
    )
}

fn fuzzy_line_matches(contents: &str, old_text: &str) -> Vec<(usize, usize)> {
    let old_lines = old_text.split('\n').collect::<Vec<_>>();
    if old_lines.is_empty() {
        return Vec::new();
    }
    let content_lines = line_spans(contents);
    content_lines
        .windows(old_lines.len())
        .filter(|window| {
            window
                .iter()
                .zip(&old_lines)
                .all(|((_, _, line), old)| normalize_line(line) == normalize_line(old))
        })
        .map(|window| {
            let start = window[0].0;
            let end = window.last().map_or(start, |line| line.1);
            (start, end.saturating_sub(start))
        })
        .collect()
}

fn line_spans(contents: &str) -> Vec<(usize, usize, &str)> {
    let mut offset = 0;
    contents
        .split_inclusive('\n')
        .map(|line| {
            let start = offset;
            offset += line.len();
            (start, offset, line.strip_suffix('\n').unwrap_or(line))
        })
        .collect()
}

fn normalize_line(line: &str) -> String {
    line.trim_end()
        .replace(['\u{2018}', '\u{2019}'], "'")
        .replace(['\u{201c}', '\u{201d}'], "\"")
        .replace(
            [
                '\u{2010}', '\u{2011}', '\u{2012}', '\u{2013}', '\u{2014}', '\u{2015}', '\u{2212}',
            ],
            "-",
        )
        .replace(
            [
                '\u{00a0}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}',
                '\u{2008}', '\u{2009}', '\u{200a}', '\u{202f}', '\u{205f}', '\u{3000}',
            ],
            " ",
        )
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::{EditTool, Tool};

    #[tokio::test]
    async fn disjoint_edits_match_the_original_file() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file.txt"), "alpha\nmiddle\nomega\n")
            .expect("fixture");

        super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [
                    {"oldText": "alpha", "newText": "first"},
                    {"oldText": "omega", "newText": "last"}
                ]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("edit");

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file.txt")).expect("updated"),
            "first\nmiddle\nlast\n"
        );
    }

    #[tokio::test]
    async fn line_endings_and_bom_are_preserved() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(
            workspace.path().join("file.txt"),
            "\u{feff}alpha  \r\nomega\r\n",
        )
        .expect("fixture");

        super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [{"oldText": "alpha  \n", "newText": "first\n"}]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("fuzzy edit");

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file.txt")).expect("updated"),
            "\u{feff}first\r\nomega\r\n"
        );
    }

    #[test]
    fn legacy_argument_shapes_are_prepared_before_validation() {
        let prepared = EditTool.prepare_arguments(json!({
            "path": "file.txt",
            "old_text": "before",
            "new_text": "after"
        }));

        assert_eq!(
            prepared,
            json!({
                "path": "file.txt",
                "edits": [{"oldText": "before", "newText": "after"}]
            })
        );
    }
}
