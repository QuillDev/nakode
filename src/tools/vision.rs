use std::sync::{Arc, RwLock};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, required_string,
    resolve_workspace_path,
};
use crate::{
    backend::PromptImage,
    runtime::ToolDefinition,
    vision::{SharedVisionService, VisionConfig},
};

const MAX_IMAGE_BYTES: u64 = 20 * 1024 * 1024;

pub struct VisionTool {
    config: Arc<RwLock<VisionConfig>>,
    service: Option<SharedVisionService>,
}

impl VisionTool {
    #[must_use]
    pub fn new(config: Arc<RwLock<VisionConfig>>, service: Option<SharedVisionService>) -> Self {
        Self { config, service }
    }

    fn enabled(&self) -> bool {
        self.service.is_some() && self.config.read().is_ok_and(|config| config.is_enabled())
    }
}

impl Tool for VisionTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "vision",
            description: "Analyze one or more workspace images with the vision model configured in /settings. Use this for screenshots, diagrams, mockups, and other visual files that text file reading cannot inspect.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative image path, or semicolon-delimited image paths"},
                    "prompt": {"type": "string", "description": "What to inspect or answer about the images"}
                },
                "required": ["path", "prompt"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("inspect image")
            .to_owned()
    }

    fn available(&self) -> bool {
        self.enabled()
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
            let Some(service) = &self.service else {
                return ToolResult::failure(
                    "Vision add-on is unavailable. Configure a supported vision model in /settings.",
                );
            };
            let result = async {
                let prompt = required_string(&arguments, "prompt")?;
                let supplied = required_string(&arguments, "path")?;
                let mut images = Vec::new();
                for target in supplied
                    .split(';')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                {
                    let path = resolve_workspace_path(context.workspace, target)?;
                    let metadata = tokio::fs::metadata(&path).await.map_err(|error| {
                        format!("failed to inspect {}: {error}", path.display())
                    })?;
                    if !metadata.is_file() {
                        return Err(format!("{} is not a file", path.display()));
                    }
                    if metadata.len() > MAX_IMAGE_BYTES {
                        return Err(format!(
                            "{} is {} bytes; vision images are limited to {MAX_IMAGE_BYTES} bytes",
                            path.display(),
                            metadata.len()
                        ));
                    }
                    let mime_type = image_mime(&path)?;
                    let data = tokio::fs::read(&path)
                        .await
                        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
                    images.push(PromptImage {
                        mime_type: mime_type.to_owned(),
                        data,
                    });
                }
                if images.is_empty() {
                    return Err("vision path must contain at least one image".to_owned());
                }
                service.analyze(prompt, images, cancellation).await
            }
            .await;
            result.map_or_else(ToolResult::failure, ToolResult::success)
        })
    }
}

fn image_mime(path: &std::path::Path) -> Result<&'static str, String> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("png") => Ok("image/png"),
        Some("jpg" | "jpeg") => Ok("image/jpeg"),
        Some("gif") => Ok("image/gif"),
        Some("webp") => Ok("image/webp"),
        _ => Err(format!(
            "{} is not a supported image (png, jpg, jpeg, gif, or webp)",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::image_mime;

    #[test]
    fn recognizes_supported_image_extensions() {
        assert_eq!(
            image_mime(std::path::Path::new("mockup.PNG")),
            Ok("image/png")
        );
        assert!(image_mime(std::path::Path::new("notes.txt")).is_err());
    }
}
