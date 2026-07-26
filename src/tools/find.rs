use std::{fmt::Write, path::Path};

use globset::Glob;
use ignore::WalkBuilder;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, optional_u64,
    resolve_workspace_path,
};
use crate::runtime::ToolDefinition;

const DEFAULT_LIMIT: usize = 1_000;
const MAX_LIMIT: usize = 10_000;

pub struct FindTool;

impl Tool for FindTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "find",
            description: "Search for files by glob pattern. Returns matching file paths relative to the search directory, respects .gitignore, and includes hidden files.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern such as *.rs, **/*.json, or src/**/*.test.rs"
                    },
                    "path": {
                        "type": "string",
                        "description": "Directory to search; defaults to the workspace"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LIMIT,
                        "description": "Maximum results; defaults to 1000"
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
            match tokio::task::spawn_blocking(move || {
                find_files(&workspace, &arguments, &cancellation)
            })
            .await
            {
                Ok(Ok(output)) => ToolResult::success(output),
                Ok(Err(error)) => ToolResult::failure(error),
                Err(error) => ToolResult::failure(format!("find worker failed: {error}")),
            }
        })
    }
}

fn find_files(
    workspace: &Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    let pattern = super::required_string(arguments, "pattern")?;
    let supplied_path = arguments
        .get("path")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .unwrap_or(".");
    let search_path = resolve_workspace_path(workspace, supplied_path)?;
    if !search_path.is_dir() {
        return Err(format!(
            "find path is not a directory: {}",
            search_path.display()
        ));
    }
    let limit = usize::try_from(optional_u64(arguments, "limit", DEFAULT_LIMIT as u64)?)
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);
    let matcher = Glob::new(pattern)
        .map_err(|error| format!("invalid find pattern: {error}"))?
        .compile_matcher();
    let match_basename = !pattern.contains('/');
    let mut paths = Vec::new();
    let mut limit_reached = false;
    let mut walker = WalkBuilder::new(&search_path);
    walker
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true);
    for entry in walker.build() {
        if cancellation.is_cancelled() {
            return Err("find interrupted".to_owned());
        }
        let entry = entry.map_err(|error| format!("find traversal failed: {error}"))?;
        if !entry
            .file_type()
            .is_some_and(|file_type| file_type.is_file())
        {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(&search_path)
            .unwrap_or(entry.path());
        let candidate = if match_basename {
            relative.file_name().map_or(relative, Path::new)
        } else {
            relative
        };
        if !matcher.is_match(candidate) {
            continue;
        }
        if paths.len() == limit {
            limit_reached = true;
            break;
        }
        paths.push(relative.to_string_lossy().replace('\\', "/"));
    }
    paths.sort_unstable();
    if paths.is_empty() {
        return Ok("No files found matching pattern".to_owned());
    }
    let mut output = paths.join("\n");
    if limit_reached {
        let _ = write!(
            output,
            "\n\n[{limit} results limit reached. Increase limit or refine pattern.]"
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn finds_files_relative_to_the_selected_directory() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("src/nested")).expect("directories");
        std::fs::write(workspace.path().join("src/lib.rs"), "").expect("lib");
        std::fs::write(workspace.path().join("src/nested/mod.rs"), "").expect("module");

        let output = super::find_files(
            workspace.path(),
            &json!({"pattern": "**/*.rs", "path": "src"}),
            &CancellationToken::new(),
        )
        .expect("find output");

        assert_eq!(output, "lib.rs\nnested/mod.rs");
    }

    #[test]
    fn empty_search_path_defaults_to_the_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("lib.rs"), "").expect("fixture");

        let output = super::find_files(
            workspace.path(),
            &json!({"pattern": "*.rs", "path": ""}),
            &CancellationToken::new(),
        )
        .expect("find output");

        assert_eq!(output, "lib.rs");
    }
}
