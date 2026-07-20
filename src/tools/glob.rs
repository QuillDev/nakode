use std::{fmt::Write, time::SystemTime};

use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64, truncate_output,
};
use crate::runtime::ToolDefinition;

pub struct GlobTool;

impl Tool for GlobTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "glob",
            description: "Find files and directories by fast path matching. Path may be a glob, file, directory, or semicolon-delimited list. Hidden files are included and Git ignores are respected by default; narrow path when the result limit is reached.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Glob, file, directory, or semicolon-delimited list; omitted searches the workspace"},
                    "hidden": {"type": "boolean", "description": "Include hidden files; defaults to true"},
                    "gitignore": {"type": "boolean", "description": "Respect Git ignore rules; defaults to true"},
                    "limit": {"type": "number", "minimum": 1, "maximum": 200}
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
            let path = arguments
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or(".")
                .to_owned();
            let limit = match optional_u64(&arguments, "limit", 200) {
                Ok(limit) => usize::try_from(limit.clamp(1, 200)).unwrap_or(200),
                Err(error) => return ToolResult::failure(error),
            };
            let workspace = context.workspace.to_path_buf();
            let cancellation = cancellation.clone();
            match tokio::task::spawn_blocking(move || {
                find_matches(&workspace, &path, &arguments, limit, &cancellation)
            })
            .await
            {
                Ok(Ok(output)) => ToolResult::success(output),
                Ok(Err(error)) => ToolResult::failure(error),
                Err(error) => ToolResult::failure(format!("glob worker failed: {error}")),
            }
        })
    }
}

fn find_matches(
    workspace: &std::path::Path,
    path_input: &str,
    arguments: &Value,
    limit: usize,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let include_hidden = arguments
        .get("hidden")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let use_gitignore = arguments
        .get("gitignore")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let entries = path_input
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Err("glob path must contain at least one path or pattern".to_owned());
    }
    let mut paths = Vec::<(std::path::PathBuf, SystemTime)>::new();
    for input in entries {
        let target = GlobTarget::parse(workspace, input)?;
        let mut walker = WalkBuilder::new(&target.root);
        walker
            .hidden(!include_hidden)
            .git_ignore(use_gitignore)
            .git_global(use_gitignore)
            .git_exclude(use_gitignore);
        for entry in walker.build() {
            if cancellation.is_cancelled() {
                return Err("glob interrupted".to_owned());
            }
            let entry = entry.map_err(|error| format!("glob traversal failed: {error}"))?;
            if entry.path() == target.root || !target.matches(entry.path()) {
                continue;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            if !paths.iter().any(|(path, _)| path == entry.path()) {
                paths.push((entry.into_path(), modified));
            }
        }
    }
    paths.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let limit_reached = paths.len() > limit;
    paths.truncate(limit);
    let mut rendered = paths
        .iter()
        .map(|(path, _)| {
            let relative = path.strip_prefix(workspace).unwrap_or(path);
            let suffix = if path.is_dir() { "/" } else { "" };
            format!("{}{suffix}", relative.display())
        })
        .collect::<Vec<_>>()
        .join("\n");
    if limit_reached {
        let _ = write!(
            rendered,
            "\n[Result limit reached: {limit}. Narrow path to continue.]"
        );
    }
    Ok(truncate_output(rendered.into_bytes()))
}

struct GlobTarget {
    root: std::path::PathBuf,
    matcher: Option<GlobMatcher>,
}

impl GlobTarget {
    fn parse(workspace: &std::path::Path, input: &str) -> Result<Self, String> {
        let has_glob = input
            .chars()
            .any(|character| matches!(character, '*' | '?' | '[' | '{'));
        if !has_glob {
            let root = super::resolve_workspace_path(workspace, input)?;
            if !root.exists() {
                return Err(format!("path not found: {}", root.display()));
            }
            if root.is_file() {
                let escaped = globset::escape(&root.to_string_lossy());
                return Ok(Self {
                    root: root.parent().unwrap_or(workspace).to_path_buf(),
                    matcher: Some(
                        Glob::new(&escaped)
                            .map_err(|error| format!("invalid path pattern: {error}"))?
                            .compile_matcher(),
                    ),
                });
            }
            return Ok(Self {
                root,
                matcher: None,
            });
        }
        let absolute_pattern = super::resolve_workspace_path(workspace, input)?;
        let root = glob_root(&absolute_pattern);
        if root == std::path::Path::new("/") {
            return Err("searching from the filesystem root is not allowed".to_owned());
        }
        Ok(Self {
            root,
            matcher: Some(
                Glob::new(&absolute_pattern.to_string_lossy())
                    .map_err(|error| format!("invalid glob: {error}"))?
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
