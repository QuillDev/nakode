use serde_json::{Map, Value, json};
use similar::TextDiff;
use tokio_util::sync::CancellationToken;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult, required_string, resolve_workspace_path,
    truncate_output, write::atomic_write,
};
use crate::runtime::ToolDefinition;

pub struct EditTool;

impl Tool for EditTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "edit",
            description: "Edit one UTF-8 file using exact text replacement and return a unified diff. Every edits[].oldText must identify one unique, non-overlapping region of the original file. Safe normalization handles line endings, trailing whitespace, compatible Unicode, quotes, dashes, and spaces when an exact match is unavailable. Combine nearby changes into one edit and use multiple entries for disjoint changes.",
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
    let normalized_original = normalize_for_fuzzy_match(&original);
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
        let (index, length) = unique_match(
            &original,
            &normalized_original,
            &old_text,
            supplied,
            edit_index,
            edits.len(),
        )?;
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
    let mut updated = original.clone();
    for edit in matched.iter().rev() {
        updated.replace_range(
            edit.index..edit.index.saturating_add(edit.length),
            &edit.replacement,
        );
    }
    if updated == original {
        return Err(format!(
            "edit made no changes to {supplied}; oldText and newText resolve to identical content"
        ));
    }
    let diff = unified_diff(supplied, &original, &updated);
    if line_ending == "\r\n" {
        updated = updated.replace('\n', "\r\n");
    }
    let updated = format!("{bom}{updated}");
    if cancellation.is_cancelled() {
        return Err("edit interrupted".to_owned());
    }
    atomic_write(&path, updated.as_bytes(), cancellation).await?;
    Ok(truncate_output(
        format!(
            "Successfully replaced {} block(s) in {supplied}.\n\n{diff}",
            edits.len()
        )
        .into_bytes(),
    ))
}

fn unique_match(
    contents: &str,
    normalized_contents: &NormalizedText,
    old_text: &str,
    path: &str,
    edit_index: usize,
    total_edits: usize,
) -> Result<(usize, usize), String> {
    let exact = contents.match_indices(old_text).collect::<Vec<_>>();
    if exact.len() > 1 {
        return Err(duplicate_match_error(
            contents,
            exact.iter().map(|(index, _)| *index),
            path,
            edit_index,
            "locations",
        ));
    }
    let fuzzy = fuzzy_matches(normalized_contents, old_text);
    if fuzzy.len() > 1 {
        return Err(duplicate_match_error(
            contents,
            fuzzy.iter().map(|(index, _)| *index),
            path,
            edit_index,
            "normalized locations",
        ));
    }
    if let [(index, _)] = exact.as_slice() {
        return Ok((*index, old_text.len()));
    }
    if let [matched] = fuzzy.as_slice() {
        return Ok(*matched);
    }
    let label = if total_edits == 1 {
        "edit".to_owned()
    } else {
        format!("edit {}", edit_index + 1)
    };
    Err(format!(
        "{label} could not find oldText in {path}; re-read the surrounding lines before retrying"
    ))
}

fn duplicate_match_error(
    contents: &str,
    indexes: impl Iterator<Item = usize>,
    path: &str,
    edit_index: usize,
    match_kind: &str,
) -> String {
    let lines = indexes
        .take(8)
        .map(|index| {
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
        "edit {} matched multiple {match_kind} in {path} at lines {lines}; keep oldText small but add enough surrounding context to make it unique",
        edit_index + 1
    )
}

#[derive(Debug)]
struct NormalizedText {
    text: String,
    original_boundaries: Vec<Option<usize>>,
}

fn fuzzy_matches(normalized_contents: &NormalizedText, old_text: &str) -> Vec<(usize, usize)> {
    let normalized_old_text = normalize_for_fuzzy_match(old_text).text;
    if normalized_old_text.is_empty() {
        return Vec::new();
    }
    let mut matches = normalized_contents
        .text
        .match_indices(&normalized_old_text)
        .filter_map(|(start, _)| {
            let end = start.saturating_add(normalized_old_text.len());
            let original_start = normalized_contents
                .original_boundaries
                .get(start)
                .copied()
                .flatten()?;
            let original_end = normalized_contents
                .original_boundaries
                .get(end)
                .copied()
                .flatten()?;
            Some((original_start, original_end.saturating_sub(original_start)))
        })
        .collect::<Vec<_>>();
    matches.dedup();
    matches
}

fn normalize_for_fuzzy_match(value: &str) -> NormalizedText {
    let mut normalized = NormalizedText {
        text: String::new(),
        original_boundaries: vec![Some(0)],
    };
    let mut original_offset = 0_usize;
    for segment in value.split_inclusive('\n') {
        let (line, has_newline) = segment
            .strip_suffix('\n')
            .map_or((segment, false), |line| (line, true));
        let line_start: usize = original_offset;
        let mut units: Vec<(usize, usize, String)> = line
            .grapheme_indices(true)
            .map(|(relative_start, grapheme)| {
                let start = line_start.saturating_add(relative_start);
                let end = start.saturating_add(grapheme.len());
                let text = grapheme.nfkc().map(normalize_character).collect::<String>();
                (start, end, text)
            })
            .collect::<Vec<_>>();
        while units
            .last()
            .is_some_and(|(_, _, text)| text.chars().all(char::is_whitespace))
        {
            units.pop();
        }
        for (start, end, text) in units {
            append_normalized_unit(&mut normalized, &text, start, end);
        }
        let line_end = line_start.saturating_add(line.len());
        normalized.original_boundaries[normalized.text.len()] = Some(line_end);
        if has_newline {
            append_normalized_unit(&mut normalized, "\n", line_end, line_end.saturating_add(1));
        }
        original_offset = original_offset.saturating_add(segment.len());
    }
    normalized.original_boundaries[normalized.text.len()] = Some(value.len());
    normalized
}

fn append_normalized_unit(
    normalized: &mut NormalizedText,
    unit: &str,
    original_start: usize,
    original_end: usize,
) {
    let normalized_start = normalized.text.len();
    normalized.original_boundaries[normalized_start] = Some(original_start);
    normalized.text.push_str(unit);
    normalized
        .original_boundaries
        .resize(normalized.text.len().saturating_add(1), None);
    normalized.original_boundaries[normalized.text.len()] = Some(original_end);
}

const fn normalize_character(character: char) -> char {
    match character {
        '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
        '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
        '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2015}'
        | '\u{2212}' => '-',
        '\u{00a0}' | '\u{2002}' | '\u{2003}' | '\u{2004}' | '\u{2005}' | '\u{2006}'
        | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200a}' | '\u{202f}' | '\u{205f}'
        | '\u{3000}' => ' ',
        _ => character,
    }
}

fn unified_diff(path: &str, original: &str, updated: &str) -> String {
    let diff = TextDiff::from_lines(original, updated);
    let mut unified = diff.unified_diff();
    unified.context_radius(4).header(path, path);
    unified.to_string()
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

        let output = super::edit_file(
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
        assert!(output.starts_with("Successfully replaced 2 block(s) in file.txt."));
        assert!(output.contains("--- file.txt\n+++ file.txt"));
        assert!(output.contains("-alpha\n+first"));
        assert!(output.contains("-omega\n+last"));
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
        .expect("edit");

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file.txt")).expect("updated"),
            "\u{feff}first\r\nomega\r\n"
        );
    }

    #[tokio::test]
    async fn fuzzy_edit_replaces_only_the_normalized_substring() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(
            workspace.path().join("file.txt"),
            "hello \u{201c}world\u{201d}\nnext\n",
        )
        .expect("fixture");

        super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [{"oldText": "hello \"world\"", "newText": "hello \"earth\""}]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("fuzzy edit");

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file.txt")).expect("updated"),
            "hello \"earth\"\nnext\n"
        );
    }

    #[tokio::test]
    async fn fuzzy_edit_handles_compatibility_unicode_and_trailing_whitespace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(
            workspace.path().join("file.txt"),
            "\u{ff26}\u{ff4f}\u{ff4f} and cafe\u{301}  \nnext\n",
        )
        .expect("fixture");

        super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [{"oldText": "Foo and caf\u{e9}\t", "newText": "Bar"}]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("fuzzy edit");

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file.txt")).expect("updated"),
            "Bar\nnext\n"
        );
    }

    #[tokio::test]
    async fn fuzzy_equivalent_duplicates_are_rejected() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(
            workspace.path().join("file.txt"),
            "say \"hello\"\nsay \u{201c}hello\u{201d}\n",
        )
        .expect("fixture");

        let error = super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [{"oldText": "say \"hello\"", "newText": "say \"goodbye\""}]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect_err("ambiguous edit");

        assert!(error.contains("multiple normalized locations"));
        assert!(error.contains("lines 1, 2"));
    }

    #[tokio::test]
    async fn no_op_edit_is_rejected_without_rewriting_the_file() {
        let workspace = tempfile::tempdir().expect("workspace");
        let path = workspace.path().join("file.txt");
        std::fs::write(&path, "unchanged\n").expect("fixture");

        let error = super::edit_file(
            workspace.path(),
            &json!({
                "path": "file.txt",
                "edits": [{"oldText": "unchanged", "newText": "unchanged"}]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect_err("no-op edit");

        assert!(error.contains("made no changes"));
        assert_eq!(
            std::fs::read_to_string(path).expect("unchanged file"),
            "unchanged\n"
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
