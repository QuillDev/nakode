use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult, required_string};
use crate::{
    backend::{BackendEvent, PromptAttachment, PromptImage},
    runtime::{ReturnedImage, ToolDefinition},
};

const MAX_BYTES: u64 = 5 * 1024 * 1024;

#[cfg(test)]
mod tests;

pub struct ReturnImageTool;

impl Tool for ReturnImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "return_image",
            description: "Attach an existing PNG, JPEG, GIF or WebP image from this session's workspace to the assistant transcript. The server retains the bytes for remote clients and history. This does not generate images. Maximum 5 MiB per image and eight images per turn. Never return a client-local filesystem link instead.",
            parameters: json!({"type":"object","properties":{"path":{"type":"string","description":"Workspace-relative image file"}},"required":["path"],"additionalProperties":false}),
        }
    }

    fn available(&self) -> bool {
        cfg!(unix)
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("Image")
            .to_owned()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = async {
                let supplied = required_string(&arguments, "path")?;
                if context
                    .session
                    .returned_images
                    .values()
                    .filter(|image| image.turn_id == context.turn_id)
                    .count()
                    >= 8
                {
                    return Err("at most eight images may be returned per turn".to_owned());
                }
                let root = tokio::fs::canonicalize(context.workspace)
                    .await
                    .map_err(|e| e.to_string())?;
                let path = tokio::fs::canonicalize(root.join(supplied))
                    .await
                    .map_err(|e| e.to_string())?;
                if !path.starts_with(&root) {
                    return Err("image must be inside the session workspace".to_owned());
                }
                let file = open_image(path.clone()).await?;
                let metadata = file.metadata().await.map_err(|e| e.to_string())?;
                if !metadata.is_file() || metadata.len() > MAX_BYTES {
                    return Err("image must be a regular file of at most 5 MiB".to_owned());
                }
                let mut data = Vec::new();
                file.take(MAX_BYTES + 1)
                    .read_to_end(&mut data)
                    .await
                    .map_err(|e| e.to_string())?;
                if data.len() as u64 > MAX_BYTES {
                    return Err("image exceeds 5 MiB".to_owned());
                }
                let retained_bytes: usize = context
                    .session
                    .returned_images
                    .values()
                    .filter(|image| image.turn_id == context.turn_id)
                    .filter_map(|image| image.attachment.image.as_ref())
                    .map(|image| image.data.len())
                    .sum();
                if retained_bytes.saturating_add(data.len()) > 20 * 1024 * 1024 {
                    return Err("returned images exceed 20 MiB per turn".to_owned());
                }
                let mime_type = image_media_type(&data)
                    .ok_or("unsupported image; use PNG, JPEG, GIF or WebP")?;
                let label = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("Image")
                    .to_owned();
                let image = ReturnedImage {
                    id: format!("{}:image:{}", context.turn_id, context.call_id),
                    turn_id: context.turn_id.to_owned(),
                    provider_id: context.session.provider_id.clone(),
                    model_id: context.session.model.clone(),
                    history_index: context.session.history_position(),
                    sequence: context.session.returned_images.len(),
                    attachment: PromptAttachment {
                        label,
                        path: None,
                        image: Some(PromptImage {
                            mime_type: mime_type.to_owned(),
                            data,
                        }),
                    },
                };
                context
                    .backend_events
                    .send(BackendEvent::ImageReturned(image.clone()))
                    .await
                    .map_err(|_| "session event receiver closed")?;
                context
                    .session
                    .returned_images
                    .insert(context.call_id.to_owned(), image);
                Ok("Image attached to the assistant transcript.".to_owned())
            };
            tokio::select! {
                result = result => match result { Ok(output) => ToolResult::success(output), Err(error) => ToolResult::failure(error) },
                () = cancellation.cancelled() => ToolResult::failure("image attachment cancelled"),
            }
        })
    }
}

// Walk the canonical path with directory handles, never following replacement symlinks
// in either the leaf or its parents between authorization and opening the bytes.
#[cfg(unix)]
async fn open_image(path: std::path::PathBuf) -> Result<tokio::fs::File, String> {
    tokio::task::spawn_blocking(move || {
        use nix::{
            fcntl::{OFlag, open, openat},
            sys::stat::Mode,
        };
        let flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK;
        let mut file =
            open("/", flags | OFlag::O_DIRECTORY, Mode::empty()).map_err(|e| e.to_string())?;
        let mut parts = path.components().peekable();
        while let Some(part) = parts.next() {
            match part {
                std::path::Component::RootDir => {}
                std::path::Component::Normal(name) => {
                    let mode = if parts.peek().is_some() {
                        flags | OFlag::O_DIRECTORY
                    } else {
                        flags
                    };
                    file = openat(&file, std::path::Path::new(name), mode, Mode::empty())
                        .map_err(|e| e.to_string())?;
                }
                _ => return Err("image path must be canonical".to_owned()),
            }
        }
        Ok(tokio::fs::File::from_std(std::fs::File::from(file)))
    })
    .await
    .map_err(|e| e.to_string())?
}

#[cfg(not(unix))]
async fn open_image(_path: std::path::PathBuf) -> Result<tokio::fs::File, String> {
    Err("secure image return is unavailable on this platform".to_owned())
}

pub(crate) fn image_media_type(data: &[u8]) -> Option<&'static str> {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if data.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else {
        None
    }
}
