use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult, required_string, resolve_workspace_path,
    truncate_output,
};
use crate::runtime::ToolDefinition;

pub struct ReadTool;

impl Tool for ReadTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "read",
            description: "Read local files and directories through one path. Append :N, :N-M, :N+K, :N-, comma-separated ranges, or :raw to select file content. Parallelize independent reads and re-read only the ranges named by a truncation notice.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Local path with optional inline selector"}
                },
                "required": ["path"],
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
            if cancellation.is_cancelled() {
                return ToolResult::failure("read interrupted");
            }
            let result = async {
                let supplied = required_string(&arguments, "path")?;
                let (path, selector) = resolve_read_target(context.workspace, supplied).await?;
                let metadata = tokio::fs::metadata(&path)
                    .await
                    .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
                if metadata.is_dir() {
                    return list_directory(&path).await;
                }
                let contents = tokio::fs::read(&path)
                    .await
                    .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
                if contents.contains(&0) {
                    return Err(format!("{} appears to be binary", path.display()));
                }
                let text = String::from_utf8(contents)
                    .map_err(|_| format!("{} is not valid UTF-8", path.display()))?;
                Ok(render_text(&text, selector))
            }
            .await;
            match result {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

const DEFAULT_MAX_LINES: usize = 2_000;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadSelector {
    Default,
    Raw,
    Ranges(Vec<(usize, Option<usize>)>),
}

async fn resolve_read_target(
    workspace: &std::path::Path,
    supplied: &str,
) -> Result<(std::path::PathBuf, ReadSelector), String> {
    let literal = resolve_workspace_path(workspace, supplied)?;
    if tokio::fs::metadata(&literal).await.is_ok() {
        return Ok((literal, ReadSelector::Default));
    }
    let Some((path, selector)) = supplied.rsplit_once(':') else {
        return Ok((literal, ReadSelector::Default));
    };
    let selector = parse_selector(selector)?;
    Ok((resolve_workspace_path(workspace, path)?, selector))
}

fn parse_selector(input: &str) -> Result<ReadSelector, String> {
    if input.eq_ignore_ascii_case("raw") {
        return Ok(ReadSelector::Raw);
    }
    let ranges = input
        .split(',')
        .map(parse_range)
        .collect::<Result<Vec<_>, _>>()?;
    if ranges.is_empty() {
        return Err("read selector must not be empty".to_owned());
    }
    Ok(ReadSelector::Ranges(ranges))
}

fn parse_range(input: &str) -> Result<(usize, Option<usize>), String> {
    let input = input.strip_prefix(['L', 'l']).unwrap_or(input);
    if let Some((start, count)) = input.split_once('+') {
        let start = parse_line_number(start)?;
        let count = parse_line_number(count)?;
        return Ok((start, Some(start.saturating_add(count).saturating_sub(1))));
    }
    if let Some((start, end)) = input.split_once(['-', '.']) {
        let start = parse_line_number(start)?;
        if end.is_empty() || end == "." {
            return Ok((start, None));
        }
        let end = end.strip_prefix('.').unwrap_or(end);
        let end = parse_line_number(end)?;
        if end < start {
            return Err(format!("invalid read range {input}: end precedes start"));
        }
        return Ok((start, Some(end)));
    }
    Ok((parse_line_number(input)?, None))
}

fn parse_line_number(input: &str) -> Result<usize, String> {
    input
        .parse::<usize>()
        .ok()
        .filter(|line| *line > 0)
        .ok_or_else(|| format!("invalid read line selector {input}"))
}

fn render_text(text: &str, selector: ReadSelector) -> String {
    if selector == ReadSelector::Raw {
        return truncate_output(text.as_bytes().to_vec());
    }
    let lines = text.lines().collect::<Vec<_>>();
    let is_default = selector == ReadSelector::Default;
    let ranges = match selector {
        ReadSelector::Default => vec![(1, Some(DEFAULT_MAX_LINES))],
        ReadSelector::Ranges(ranges) => ranges,
        ReadSelector::Raw => unreachable!("raw reads return before range rendering"),
    };
    let mut output = Vec::new();
    for (start, end) in ranges {
        let end = end.unwrap_or(lines.len()).min(lines.len());
        for line_number in start..=end {
            if let Some(line) = lines.get(line_number.saturating_sub(1)) {
                output.push(format!("{line_number}|{line}"));
            }
        }
    }
    if is_default && lines.len() > DEFAULT_MAX_LINES {
        output.push(format!(
            "[Showing lines 1-{DEFAULT_MAX_LINES} of {}. Read the next range with path:{}-{}.]",
            lines.len(),
            DEFAULT_MAX_LINES + 1,
            (DEFAULT_MAX_LINES * 2).min(lines.len())
        ));
    }
    truncate_output(output.join("\n").into_bytes())
}

async fn list_directory(path: &std::path::Path) -> Result<String, String> {
    let mut directory = tokio::fs::read_dir(path)
        .await
        .map_err(|error| format!("failed to list {}: {error}", path.display()))?;
    let mut entries = Vec::new();
    while let Some(entry) = directory
        .next_entry()
        .await
        .map_err(|error| format!("failed to list {}: {error}", path.display()))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        let suffix = if file_type.is_dir() { "/" } else { "" };
        entries.push(format!("{}{suffix}", entry.file_name().to_string_lossy()));
    }
    entries.sort_unstable();
    Ok(truncate_output(entries.join("\n").into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{ReadSelector, parse_selector, render_text};

    #[test]
    fn selectors_use_one_based_inclusive_omp_forms() {
        assert_eq!(
            parse_selector("3").expect("open range"),
            ReadSelector::Ranges(vec![(3, None)])
        );
        assert_eq!(
            parse_selector("2-4,8+2").expect("range list"),
            ReadSelector::Ranges(vec![(2, Some(4)), (8, Some(9))])
        );
        assert_eq!(
            render_text(
                "one\ntwo\nthree\nfour",
                parse_selector("2-3").expect("selector")
            ),
            "2|two\n3|three"
        );
    }
}
