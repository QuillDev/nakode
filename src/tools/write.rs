use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Tool, ToolContext, ToolFuture, ToolResult, required_string, resolve_workspace_path};
use crate::runtime::ToolDefinition;

pub struct WriteTool;

impl Tool for WriteTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "write",
            description: "Atomically create or overwrite a complete UTF-8 file, creating parent directories as needed. Prefer edit for modifying an existing file when an exact replacement is practical.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative file path"},
                    "content": {"type": "string", "description": "Complete replacement contents"}
                },
                "required": ["path", "content"],
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
            let result = write_file(context.workspace, &arguments, cancellation).await;
            match result {
                Ok(output) => ToolResult::success(output),
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

async fn write_file(
    workspace: &std::path::Path,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    if cancellation.is_cancelled() {
        return Err("write interrupted".to_owned());
    }
    let path = resolve_workspace_path(workspace, required_string(arguments, "path")?)?;
    let content = required_string_allow_empty(arguments, "content")?;
    atomic_write(&path, content.as_bytes(), cancellation).await?;
    Ok(format!(
        "wrote {} bytes to {}",
        content.len(),
        path.display()
    ))
}

pub(super) async fn atomic_write(
    path: &std::path::Path,
    content: &[u8],
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "write target has no parent directory".to_owned())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    let temporary = parent.join(format!(".nakode-write-{}", Uuid::now_v7()));
    let operation = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| format!("failed to create temporary file: {error}"))?;
        file.write_all(content)
            .await
            .map_err(|error| format!("failed to write temporary file: {error}"))?;
        file.flush()
            .await
            .map_err(|error| format!("failed to flush temporary file: {error}"))?;
        if let Ok(metadata) = tokio::fs::metadata(path).await {
            tokio::fs::set_permissions(&temporary, metadata.permissions())
                .await
                .map_err(|error| format!("failed to preserve file permissions: {error}"))?;
        }
        file.sync_all()
            .await
            .map_err(|error| format!("failed to sync temporary file: {error}"))?;
        drop(file);
        if cancellation.is_cancelled() {
            return Err("write interrupted".to_owned());
        }
        tokio::fs::rename(&temporary, &path)
            .await
            .map_err(|error| format!("failed to replace {}: {error}", path.display()))?;
        sync_parent_directory(parent).await?;
        Ok(())
    }
    .await;
    if operation.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    operation
}

#[cfg(unix)]
async fn sync_parent_directory(parent: &std::path::Path) -> Result<(), String> {
    let parent = parent.to_owned();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(&parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed to sync directory {}: {error}", parent.display()))
    })
    .await
    .map_err(|error| format!("directory sync task failed: {error}"))?
}

#[cfg(not(unix))]
async fn sync_parent_directory(_parent: &std::path::Path) -> Result<(), String> {
    Ok(())
}

fn required_string_allow_empty<'a>(arguments: &'a Value, name: &str) -> Result<&'a str, String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string argument {name}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[cfg(unix)]
    #[tokio::test]
    async fn shebang_does_not_implicitly_make_new_file_executable() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = tempfile::tempdir().expect("workspace");
        super::write_file(
            workspace.path(),
            &json!({"path": "script.sh", "content": "#!/bin/sh\necho ok\n"}),
            &CancellationToken::new(),
        )
        .await
        .expect("write script");

        let mode = std::fs::metadata(workspace.path().join("script.sh"))
            .expect("script metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replacement_preserves_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = tempfile::tempdir().expect("workspace");
        let path = workspace.path().join("script.sh");
        std::fs::write(&path, "old").expect("fixture");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750))
            .expect("fixture permissions");

        super::write_file(
            workspace.path(),
            &json!({"path": "script.sh", "content": "new"}),
            &CancellationToken::new(),
        )
        .await
        .expect("replace script");

        let mode = std::fs::metadata(path)
            .expect("script metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o750);
    }
}
