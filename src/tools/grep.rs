use std::io::{BufRead, BufReader};

use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult, required_string, truncate_output};
use crate::runtime::ToolDefinition;

pub struct GrepTool;

impl Tool for GrepTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "grep",
            description: "Search files with a regular expression. Always use this instead of shell grep. Scope path to a file, directory, glob, or semicolon-delimited list; use skip to page by file when a result notice requests it.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": {"type": "string"},
                    "path": {"type": "string", "description": "File, directory, glob, or semicolon-delimited list"},
                    "case": {"type": "boolean", "description": "Case-sensitive search; defaults to true"},
                    "gitignore": {"type": "boolean", "description": "Respect Git ignore rules; defaults to true"},
                    "skip": {"type": ["number", "null"], "minimum": 0, "description": "Files to skip for pagination"}
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
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let pattern = required_string(arguments, "pattern")?;
    let case_sensitive = arguments
        .get("case")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let expression = RegexBuilder::new(pattern)
        .case_insensitive(!case_sensitive)
        .build()
        .map_err(|error| format!("invalid regex: {error}"))?;
    let skip = arguments
        .get("skip")
        .and_then(Value::as_u64)
        .map_or(Ok(0), |skip| {
            usize::try_from(skip).map_err(|error| error.to_string())
        })?;
    let use_gitignore = arguments
        .get("gitignore")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let path_input = arguments.get("path").and_then(Value::as_str).unwrap_or(".");
    let targets = path_input
        .split(';')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(|path| SearchTarget::parse(workspace, path))
        .collect::<Result<Vec<_>, _>>()?;
    if targets.is_empty() {
        return Err("grep path must contain at least one search target".to_owned());
    }
    let mut file_matches = Vec::<(std::path::PathBuf, Vec<String>)>::new();
    for target in targets {
        let mut walker = WalkBuilder::new(&target.root);
        walker
            .hidden(false)
            .git_ignore(use_gitignore)
            .git_global(use_gitignore)
            .git_exclude(use_gitignore);
        'files: for entry in walker.build() {
            if cancellation.is_cancelled() {
                return Err("grep interrupted".to_owned());
            }
            let entry = entry.map_err(|error| format!("grep traversal failed: {error}"))?;
            if !entry
                .file_type()
                .is_some_and(|file_type| file_type.is_file())
                || !target.matches(entry.path())
            {
                continue;
            }
            let Ok(file) = std::fs::File::open(entry.path()) else {
                continue;
            };
            let per_file_limit = if target.root.is_file() { 200 } else { 20 };
            let mut lines = Vec::new();
            for (index, line) in BufReader::new(file).lines().enumerate() {
                let Ok(line) = line else { continue 'files };
                if expression.is_match(&line) {
                    lines.push(format!("{}:{line}", index.saturating_add(1)));
                    if lines.len() == per_file_limit {
                        break;
                    }
                }
            }
            if !lines.is_empty() {
                file_matches.push((entry.into_path(), lines));
            }
        }
    }
    file_matches.sort_by(|left, right| left.0.cmp(&right.0));
    file_matches.dedup_by(|left, right| left.0 == right.0);
    let total_files = file_matches.len();
    let selected = file_matches.into_iter().skip(skip).take(20);
    let mut output = Vec::new();
    for (path, lines) in selected {
        let relative = path.strip_prefix(workspace).unwrap_or(&path);
        output.push(format!("# {}", relative.display()));
        output.extend(lines);
    }
    if total_files > skip.saturating_add(20) {
        output.push(format!(
            "[Showing files {}-{} of {total_files}. Continue with skip={}.]",
            skip + 1,
            skip + 20,
            skip + 20
        ));
    }
    Ok(truncate_output(output.join("\n").into_bytes()))
}

struct SearchTarget {
    root: std::path::PathBuf,
    matcher: Option<GlobMatcher>,
}

impl SearchTarget {
    fn parse(workspace: &std::path::Path, input: &str) -> Result<Self, String> {
        let has_glob = input
            .chars()
            .any(|character| matches!(character, '*' | '?' | '[' | '{'));
        let resolved = super::resolve_workspace_path(workspace, input)?;
        if !has_glob {
            return Ok(Self {
                root: resolved,
                matcher: None,
            });
        }
        let root = glob_root(&resolved);
        Ok(Self {
            root,
            matcher: Some(
                Glob::new(&resolved.to_string_lossy())
                    .map_err(|error| format!("invalid grep path glob: {error}"))?
                    .compile_matcher(),
            ),
        })
    }

    fn matches(&self, path: &std::path::Path) -> bool {
        self.matcher
            .as_ref()
            .is_none_or(|matcher| matcher.is_match(path))
    }
}

fn glob_root(pattern: &std::path::Path) -> std::path::PathBuf {
    let mut root = std::path::PathBuf::new();
    for component in pattern.components() {
        let text = component.as_os_str().to_string_lossy();
        if text
            .chars()
            .any(|character| matches!(character, '*' | '?' | '[' | '{'))
        {
            break;
        }
        root.push(component.as_os_str());
    }
    root
}
