use std::{
    fmt::Write,
    path::{Path, PathBuf},
};

use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use regex::{Regex, RegexBuilder};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64,
    resolve_workspace_path,
    truncate::{DEFAULT_MAX_BYTES, TruncatedBy, truncate_head, truncate_line},
};
use crate::runtime::ToolDefinition;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;
const MAX_CONTEXT: usize = 20;

pub struct GrepTool;

impl Tool for GrepTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "grep",
            description: "Search file contents for a regex or literal string. Returns matching lines with line numbers and paths relative to the searched directory, or the file name when searching one file. Respects .gitignore and includes hidden files.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regular expression, or exact text when literal is true"
                    },
                    "path": {
                        "type": "string",
                        "description": "File or directory to search; defaults to the workspace"
                    },
                    "glob": {
                        "type": "string",
                        "description": "Optional file filter relative to the search path, such as *.rs or **/*.test.rs"
                    },
                    "ignoreCase": {
                        "type": "boolean",
                        "description": "Case-insensitive search; defaults to false"
                    },
                    "literal": {
                        "type": "boolean",
                        "description": "Treat pattern as literal text instead of regex; defaults to false"
                    },
                    "context": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": MAX_CONTEXT,
                        "description": "Lines before and after each match; defaults to 0"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LIMIT,
                        "description": "Maximum matches; defaults to 100"
                    }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("pattern")
            .and_then(Value::as_str)
            .unwrap_or_default()
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
            let workspace = context.workspace.to_path_buf();
            let cancellation = cancellation.clone();
            match tokio::task::spawn_blocking(move || search(&workspace, &arguments, &cancellation))
                .await
            {
                Ok(Ok(output)) => ToolResult::success(output),
                Ok(Err(error)) => ToolResult::failure(error),
                Err(error) => ToolResult::failure(format!("grep worker failed: {error}")),
            }
        })
    }
}

fn search(
    workspace: &Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let options = SearchOptions::parse(workspace, arguments)?;
    let results = collect_matches(workspace, &options, cancellation)?;
    Ok(render_results(&results, options.limit))
}

struct SearchOptions {
    regex: Regex,
    path: PathBuf,
    glob: Option<GlobMatcher>,
    context: usize,
    limit: usize,
}

impl SearchOptions {
    fn parse(workspace: &Path, arguments: &Value) -> Result<Self, String> {
        let regex = parse_regex(arguments)?;
        let supplied_path = arguments
            .get("path")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .unwrap_or(".");
        let path = resolve_workspace_path(workspace, supplied_path)?;
        if !path.exists() {
            return Err(format!("grep path not found: {}", path.display()));
        }
        let glob = arguments
            .get("glob")
            .and_then(Value::as_str)
            .map(Glob::new)
            .transpose()
            .map_err(|error| format!("invalid grep glob: {error}"))?
            .map(|glob| glob.compile_matcher());
        let context = usize::try_from(optional_u64(arguments, "context", 0)?)
            .unwrap_or(0)
            .min(MAX_CONTEXT);
        let limit = usize::try_from(optional_u64(arguments, "limit", DEFAULT_LIMIT as u64)?)
            .unwrap_or(DEFAULT_LIMIT)
            .clamp(1, MAX_LIMIT);
        Ok(Self {
            regex,
            path,
            glob,
            context,
            limit,
        })
    }
}

fn parse_regex(arguments: &Value) -> Result<Regex, String> {
    let pattern = super::required_string(arguments, "pattern")?;
    let expression = if arguments
        .get("literal")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        regex::escape(pattern)
    } else {
        pattern.to_owned()
    };
    RegexBuilder::new(&expression)
        .case_insensitive(
            arguments
                .get("ignoreCase")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        )
        .build()
        .map_err(|error| format!("invalid regex: {error}; set literal=true to search exact text"))
}

struct SearchResults {
    lines: Vec<String>,
    limit_reached: bool,
    lines_truncated: bool,
}

fn collect_matches(
    workspace: &Path,
    options: &SearchOptions,
    cancellation: &CancellationToken,
) -> Result<SearchResults, String> {
    let search_root = if options.path.is_file() {
        options.path.parent().unwrap_or(workspace)
    } else {
        &options.path
    };
    let mut walker = WalkBuilder::new(&options.path);
    walker
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true);
    let mut rendered = Vec::new();
    let mut match_count = 0;
    let mut limit_reached = false;
    let mut lines_truncated = false;
    'files: for entry in walker.build() {
        if cancellation.is_cancelled() {
            return Err("grep interrupted".to_owned());
        }
        let entry = entry.map_err(|error| format!("grep traversal failed: {error}"))?;
        if !entry
            .file_type()
            .is_some_and(|file_type| file_type.is_file())
        {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(search_root)
            .unwrap_or(entry.path());
        if !matches_glob(options.glob.as_ref(), relative) {
            continue;
        }
        let Ok(contents) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        let lines = contents.lines().collect::<Vec<_>>();
        for (index, line) in lines.iter().enumerate() {
            if !options.regex.is_match(line) {
                continue;
            }
            if match_count == options.limit {
                limit_reached = true;
                break 'files;
            }
            match_count += 1;
            lines_truncated |=
                append_match(&mut rendered, relative, &lines, index, options.context);
        }
    }
    Ok(SearchResults {
        lines: rendered,
        limit_reached,
        lines_truncated,
    })
}

fn append_match(
    rendered: &mut Vec<String>,
    relative: &Path,
    lines: &[&str],
    match_index: usize,
    context: usize,
) -> bool {
    let start = match_index.saturating_sub(context);
    let end = match_index
        .saturating_add(context)
        .saturating_add(1)
        .min(lines.len());
    let mut any_truncated = false;
    for (line_index, text) in lines.iter().enumerate().take(end).skip(start) {
        let (text, truncated) = truncate_line(text);
        any_truncated |= truncated;
        let separator = if line_index == match_index { ':' } else { '-' };
        rendered.push(format!(
            "{}{separator}{}{separator} {text}",
            relative.to_string_lossy().replace('\\', "/"),
            line_index + 1
        ));
    }
    any_truncated
}

fn render_results(results: &SearchResults, limit: usize) -> String {
    if results.lines.is_empty() {
        return "No matches found".to_owned();
    }
    let truncation = truncate_head(&results.lines.join("\n"), usize::MAX, DEFAULT_MAX_BYTES);
    let mut output = truncation.content;
    let mut notices = Vec::new();
    if results.limit_reached {
        notices.push(format!(
            "{limit} matches limit reached. Increase limit or refine pattern"
        ));
    }
    if let Some(TruncatedBy::Bytes) = truncation.truncated_by {
        notices.push(format!("{DEFAULT_MAX_BYTES}-byte output limit reached"));
    }
    if results.lines_truncated {
        notices.push("Some lines were truncated; use read to inspect them".to_owned());
    }
    if !notices.is_empty() {
        let _ = write!(output, "\n\n[{}]", notices.join(". "));
    }
    output
}

fn matches_glob(matcher: Option<&GlobMatcher>, relative: &Path) -> bool {
    matcher.is_none_or(|matcher| {
        matcher.is_match(relative)
            || relative
                .file_name()
                .is_some_and(|name| matcher.is_match(Path::new(name)))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn literal_search_accepts_regex_metacharacters() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(
            workspace.path().join("state.rs"),
            "enum Event {\n    StartSession {\n}\n",
        )
        .expect("fixture");

        let output = super::search(
            workspace.path(),
            &json!({"pattern": "StartSession {", "literal": true}),
            &CancellationToken::new(),
        )
        .expect("grep");

        assert_eq!(output, "state.rs:2:     StartSession {");
    }

    #[test]
    fn empty_path_defaults_to_the_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file.txt"), "needle").expect("fixture");

        let output = super::search(
            workspace.path(),
            &json!({"pattern": "needle", "path": ""}),
            &CancellationToken::new(),
        )
        .expect("grep");

        assert_eq!(output, "file.txt:1: needle");
    }

    #[test]
    fn glob_and_context_are_applied() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("file.rs"), "before\nneedle\nafter")
            .expect("Rust fixture");
        std::fs::write(workspace.path().join("file.txt"), "needle").expect("text fixture");

        let output = super::search(
            workspace.path(),
            &json!({"pattern": "needle", "glob": "*.rs", "context": 1}),
            &CancellationToken::new(),
        )
        .expect("grep");

        assert_eq!(
            output,
            "file.rs-1- before\nfile.rs:2: needle\nfile.rs-3- after"
        );
    }
}
