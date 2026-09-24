use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult};
use crate::{
    backend::{BackendEvent, PromptAttachment, PromptImage},
    runtime::{ReturnedImage, ToolDefinition},
};

const MAX_BYTES: u64 = 5 * 1024 * 1024;
/// Images one turn may return, and their bytes together.
pub(crate) const MAX_IMAGES_PER_TURN: usize = 8;
pub(crate) const MAX_BYTES_PER_TURN: usize = 20 * 1024 * 1024;

pub(crate) const RETURN_IMAGE_DESCRIPTION: &str = "Attach existing PNG, JPEG, GIF or WebP images to the assistant transcript: files in this session's workspace, or under a `.tmp-gallery` directory (such as an agent's screenshots). Give one `path`, or several at once as `paths`; if any cannot be attached, none is. The server retains the bytes for remote clients and history. This does not generate images. Maximum 5 MiB per image and eight images per turn. Never return a client-local filesystem link instead.";
pub(crate) const RETURN_IMAGE_PATH_DESCRIPTION: &str =
    "Workspace-relative image file, or an absolute path inside a `.tmp-gallery` directory";

/// The images one call names: a single `path`, or up to eight distinct `paths`, never both.
pub(crate) fn requested_image_paths(arguments: &Value) -> Result<Vec<String>, String> {
    let single = arguments.get("path").and_then(Value::as_str);
    let many = arguments.get("paths").and_then(Value::as_array);
    let paths: Vec<String> = match (single, many) {
        (Some(path), None) => vec![path.to_owned()],
        (None, Some(paths)) => paths
            .iter()
            .map(|path| {
                path.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "paths must be strings".to_owned())
            })
            .collect::<Result<_, _>>()?,
        _ => return Err("give either path or paths".to_owned()),
    };
    if paths.is_empty() || paths.iter().any(String::is_empty) {
        return Err("give at least one non-empty image path".to_owned());
    }
    if paths.len() > MAX_IMAGES_PER_TURN {
        return Err("at most eight images may be returned per turn".to_owned());
    }
    let mut seen = std::collections::HashSet::new();
    if !paths.iter().all(|path| seen.insert(path)) {
        return Err("each image may be named once".to_owned());
    }
    Ok(paths)
}

/// Loads every requested image before any is attached, so a call attaches all of them or none.
/// A failure names the path that caused it.
pub(crate) async fn load_returnable_images(
    workspace: &std::path::Path,
    paths: &[String],
) -> Result<Vec<LoadedImage>, String> {
    let mut images = Vec::with_capacity(paths.len());
    for path in paths {
        images.push(
            load_returnable_image(workspace, path)
                .await
                .map_err(|error| format!("{path}: {error}"))?,
        );
    }
    Ok(images)
}

/// The confirmation a call returns to the model.
pub(crate) fn returned_images_output(count: usize) -> String {
    if count == 1 {
        "Image attached to the assistant transcript.".to_owned()
    } else {
        format!("{count} images attached to the assistant transcript.")
    }
}

/// A validated image file, ready to attach.
pub(crate) struct LoadedImage {
    pub label: String,
    pub mime_type: String,
    pub data: Vec<u8>,
}

/// Resolves and reads an image a session may return: one inside its workspace, or, by absolute
/// path, one inside a `.tmp-gallery` directory, where agents keep their screenshots. Both are
/// judged on the canonical path, so a symlink cannot lead out of either, and the file is then
/// opened without following any replacement symlink.
pub(crate) async fn load_returnable_image(
    workspace: &std::path::Path,
    supplied: &str,
) -> Result<LoadedImage, String> {
    let root = tokio::fs::canonicalize(workspace)
        .await
        .map_err(|e| e.to_string())?;
    let requested = std::path::Path::new(supplied);
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let path = tokio::fs::canonicalize(candidate)
        .await
        .map_err(|e| e.to_string())?;
    let in_gallery = path.parent().is_some_and(|parent| {
        parent
            .ancestors()
            .any(|dir| dir.file_name() == Some(".tmp-gallery".as_ref()))
    });
    if !path.starts_with(&root) && !in_gallery {
        return Err(
            "image must be inside the session workspace or a .tmp-gallery directory".to_owned(),
        );
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
    let mime_type = image_media_type(&data)
        .ok_or("unsupported image; use PNG, JPEG, GIF or WebP")?
        .to_owned();
    let label = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Image")
        .to_owned();
    Ok(LoadedImage {
        label,
        mime_type,
        data,
    })
}

#[cfg(test)]
mod tests;

pub struct ReturnImageTool;

impl Tool for ReturnImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "return_image",
            description: RETURN_IMAGE_DESCRIPTION,
            parameters: json!({"type":"object","properties":{"path":{"type":"string","description":RETURN_IMAGE_PATH_DESCRIPTION},"paths":{"type":"array","items":{"type":"string","description":RETURN_IMAGE_PATH_DESCRIPTION},"minItems":1,"maxItems":MAX_IMAGES_PER_TURN,"description":"Several images to attach at once, in order"}},"additionalProperties":false}),
        }
    }

    fn available(&self) -> bool {
        cfg!(unix)
    }

    fn summarize(&self, arguments: &Value) -> String {
        match requested_image_paths(arguments) {
            Ok(paths) if paths.len() == 1 => paths[0].clone(),
            Ok(paths) => format!("{} images", paths.len()),
            Err(_) => "Image".to_owned(),
        }
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = async {
                let paths = requested_image_paths(&arguments)?;
                let turn_images = context
                    .session
                    .returned_images
                    .values()
                    .filter(|image| image.turn_id == context.turn_id)
                    .collect::<Vec<_>>();
                let returned: usize = turn_images.iter().map(|image| image.image_count()).sum();
                if returned + paths.len() > MAX_IMAGES_PER_TURN {
                    return Err("at most eight images may be returned per turn".to_owned());
                }
                let retained_bytes: usize =
                    turn_images.iter().map(|image| image.image_bytes()).sum();
                let loaded = load_returnable_images(context.workspace, &paths).await?;
                let added: usize = loaded.iter().map(|image| image.data.len()).sum();
                if retained_bytes.saturating_add(added) > MAX_BYTES_PER_TURN {
                    return Err("returned images exceed 20 MiB per turn".to_owned());
                }
                let count = loaded.len();
                // One call is one message: every image it names travels in the same reply.
                let mut attachments = loaded.into_iter().map(
                    |LoadedImage {
                         label,
                         mime_type,
                         data,
                     }| PromptAttachment {
                        label,
                        path: None,
                        image: Some(PromptImage { mime_type, data }),
                    },
                );
                let first = attachments.next().ok_or("no image to return")?;
                let image = ReturnedImage {
                    id: format!("{}:image:{}", context.turn_id, context.call_id),
                    turn_id: context.turn_id.to_owned(),
                    provider_id: context.session.provider_id.clone(),
                    model_id: context.session.model.clone(),
                    history_index: context.session.history_position(),
                    sequence: context.session.returned_images.len(),
                    attachment: first,
                    more_attachments: attachments.collect(),
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
                Ok(returned_images_output(count))
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
