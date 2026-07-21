use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use clipboard_rs::{Clipboard, ClipboardContext, ContentFormat, common::RustImage};
use thiserror::Error;

use crate::backend::{PromptAttachment, PromptImage};

const MAX_SELECTION_BYTES: usize = 1024 * 1024;
const MAX_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Debug)]
pub enum ClipboardPayload {
    Attachments(Vec<PromptAttachment>),
    Text(String),
}

#[derive(Debug, Error)]
pub enum ClipboardError {
    #[error("selection is too large to copy ({actual} bytes; maximum {maximum})")]
    TooLarge { actual: usize, maximum: usize },
    #[error("failed to write terminal clipboard sequence: {0}")]
    Io(#[from] io::Error),
    #[error("clipboard is unavailable: {0}")]
    Unavailable(String),
    #[error("attachment is too large ({actual} bytes; maximum {maximum})")]
    AttachmentTooLarge { actual: u64, maximum: u64 },
}

/// Reads copied files or an image from the desktop clipboard, falling back to text.
///
/// # Errors
///
/// Returns an error when the desktop clipboard cannot be opened or its selected
/// content cannot be decoded.
pub fn read_desktop() -> Result<ClipboardPayload, ClipboardError> {
    let context =
        ClipboardContext::new().map_err(|error| ClipboardError::Unavailable(error.to_string()))?;
    if context.has(ContentFormat::Files) {
        let files = context
            .get_files()
            .map_err(|error| ClipboardError::Unavailable(error.to_string()))?;
        let attachments = attachments_from_paths(files.iter().map(PathBuf::from))?;
        if !attachments.is_empty() {
            return Ok(ClipboardPayload::Attachments(attachments));
        }
    }
    if context.has(ContentFormat::Image) {
        let image = context
            .get_image()
            .map_err(|error| ClipboardError::Unavailable(error.to_string()))?;
        let png = image
            .to_png()
            .map_err(|error| ClipboardError::Unavailable(error.to_string()))?;
        let data = png.get_bytes().to_vec();
        check_attachment_size(data.len().try_into().unwrap_or(u64::MAX))?;
        return Ok(ClipboardPayload::Attachments(vec![PromptAttachment {
            label: "Image".to_owned(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".to_owned(),
                data,
            }),
        }]));
    }
    context
        .get_text()
        .map(ClipboardPayload::Text)
        .map_err(|error| ClipboardError::Unavailable(error.to_string()))
}

/// Converts a terminal paste to attachments when every pasted token names an
/// existing file. Terminal emulators use this path for drag and drop.
#[must_use]
pub fn attachments_from_terminal_paste(text: &str) -> Option<Vec<PromptAttachment>> {
    let trimmed = text.trim();
    let direct = PathBuf::from(trimmed);
    let paths = if direct.is_file() {
        vec![direct]
    } else {
        shell_words::split(trimmed)
            .ok()?
            .into_iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>()
    };
    if paths.is_empty() || !paths.iter().all(|path| path.is_file()) {
        return None;
    }
    attachments_from_paths(paths).ok()
}

fn attachments_from_paths(
    paths: impl IntoIterator<Item = PathBuf>,
) -> Result<Vec<PromptAttachment>, ClipboardError> {
    paths
        .into_iter()
        .filter(|path| path.is_file())
        .map(attachment_from_path)
        .collect()
}

fn attachment_from_path(path: PathBuf) -> Result<PromptAttachment, ClipboardError> {
    let metadata = fs::metadata(&path)?;
    check_attachment_size(metadata.len())?;
    let image = image_mime(&path).map(|mime_type| {
        fs::read(&path).map(|data| PromptImage {
            mime_type: mime_type.to_owned(),
            data,
        })
    });
    let image = image.transpose()?;
    let label = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("File")
        .to_owned();
    Ok(PromptAttachment {
        label,
        path: Some(path),
        image,
    })
}

fn check_attachment_size(actual: u64) -> Result<(), ClipboardError> {
    if actual > MAX_ATTACHMENT_BYTES {
        return Err(ClipboardError::AttachmentTooLarge {
            actual,
            maximum: MAX_ATTACHMENT_BYTES,
        });
    }
    Ok(())
}

fn image_mime(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    }
}

/// Writes `text` as an OSC 52 clipboard sequence.
///
/// # Errors
///
/// Returns an error when the payload is too large or the destination cannot be
/// written.
pub fn write_osc52(
    writer: &mut impl Write,
    text: &str,
    inside_tmux: bool,
) -> Result<usize, ClipboardError> {
    let sequence = osc52_sequence(text, inside_tmux)?;
    writer.write_all(&sequence)?;
    writer.flush()?;
    Ok(text.len())
}

fn osc52_sequence(text: &str, inside_tmux: bool) -> Result<Vec<u8>, ClipboardError> {
    if text.len() > MAX_SELECTION_BYTES {
        return Err(ClipboardError::TooLarge {
            actual: text.len(),
            maximum: MAX_SELECTION_BYTES,
        });
    }

    let payload = STANDARD.encode(text);
    let osc = format!("\u{1b}]52;c;{payload}\u{7}");
    if inside_tmux {
        let escaped = osc.replace('\u{1b}', "\u{1b}\u{1b}");
        Ok(format!("\u{1b}Ptmux;{escaped}\u{1b}\\").into_bytes())
    } else {
        Ok(osc.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        MAX_SELECTION_BYTES, attachments_from_terminal_paste, osc52_sequence, write_osc52,
    };

    #[test]
    fn emits_standard_osc52_clipboard_sequence() {
        let mut output = Vec::new();
        let copied = write_osc52(&mut output, "hello", false).expect("encode clipboard text");

        assert_eq!(copied, 5);
        assert_eq!(output, b"\x1b]52;c;aGVsbG8=\x07");
    }

    #[test]
    fn wraps_osc52_for_tmux_passthrough() {
        let sequence = osc52_sequence("hi", true).expect("encode tmux clipboard text");

        assert_eq!(sequence, b"\x1bPtmux;\x1b\x1b]52;c;aGk=\x07\x1b\\");
    }

    #[test]
    fn rejects_unbounded_clipboard_payloads() {
        let oversized = "x".repeat(MAX_SELECTION_BYTES + 1);

        assert!(osc52_sequence(&oversized, false).is_err());
    }

    #[test]
    fn recognizes_dragged_or_pasted_file_paths() {
        let directory = tempfile::tempdir().expect("temp directory");
        let path = directory.path().join("screen shot.png");
        fs::write(&path, b"png bytes").expect("fixture image");

        let attachments = attachments_from_terminal_paste(&path.to_string_lossy())
            .expect("path should become attachment");

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].label, "screen shot.png");
        assert_eq!(attachments[0].path.as_deref(), Some(path.as_path()));
        assert_eq!(
            attachments[0]
                .image
                .as_ref()
                .map(|image| image.mime_type.as_str()),
            Some("image/png")
        );
    }

    #[test]
    fn leaves_regular_pasted_text_as_text() {
        assert!(attachments_from_terminal_paste("not a local file").is_none());
    }
}
