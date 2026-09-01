use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use clipboard_rs::{Clipboard, ClipboardContext, ContentFormat, common::RustImage};
use thiserror::Error;

use crate::media::ImageData;

const MAX_SELECTION_BYTES: usize = 1024 * 1024;
const MAX_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Debug)]
pub enum ClipboardPayload {
    Files(LocalFileInput),
    Attachments(Vec<ClipboardAttachment>),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalFileInput {
    pub paths: Vec<PathBuf>,
    pub image_attachments: Vec<ClipboardAttachment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClipboardAttachment {
    pub label: String,
    pub path: Option<PathBuf>,
    pub image: Option<ImageData>,
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
        let input = local_file_input(files.iter().map(PathBuf::from), false)?;
        if !input.paths.is_empty() || !input.image_attachments.is_empty() {
            return Ok(ClipboardPayload::Files(input));
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
        return Ok(ClipboardPayload::Attachments(vec![ClipboardAttachment {
            label: "Image".to_owned(),
            path: None,
            image: Some(ImageData {
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

/// Converts terminal-pasted path text when every token names a local file.
/// Terminal events expose text only, so relative paths are resolved against the
/// TUI process while native clipboard paths are kept exactly as the host reports them.
#[must_use]
pub fn local_files_from_terminal_paste(text: &str) -> Option<LocalFileInput> {
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
    local_file_input(paths, true).ok()
}

fn local_file_input(
    paths: impl IntoIterator<Item = PathBuf>,
    resolve_relative: bool,
) -> Result<LocalFileInput, ClipboardError> {
    let mut input = LocalFileInput {
        paths: Vec::new(),
        image_attachments: Vec::new(),
    };
    for path in paths {
        if !path.is_file() {
            continue;
        }
        let path = if resolve_relative && !path.is_absolute() {
            fs::canonicalize(path)?
        } else if path.is_absolute() {
            path
        } else {
            continue;
        };
        if image_mime(&path).is_some() {
            input.image_attachments.push(attachment_from_path(path)?);
        } else {
            input.paths.push(path);
        }
    }
    Ok(input)
}

fn attachment_from_path(path: PathBuf) -> Result<ClipboardAttachment, ClipboardError> {
    let metadata = fs::metadata(&path)?;
    check_attachment_size(metadata.len())?;
    let image = image_mime(&path).map(|mime_type| {
        fs::read(&path).map(|data| ImageData {
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
    Ok(ClipboardAttachment {
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
        MAX_ATTACHMENT_BYTES, MAX_SELECTION_BYTES, check_attachment_size, local_file_input,
        local_files_from_terminal_paste, osc52_sequence, write_osc52,
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
    fn attachment_limit_matches_the_service_transport_contract() {
        assert_eq!(
            MAX_ATTACHMENT_BYTES,
            u64::try_from(nakode_protocol::MAX_ARTIFACT_BYTES).expect("artifact limit fits u64")
        );
        assert!(check_attachment_size(MAX_ATTACHMENT_BYTES).is_ok());
        assert!(check_attachment_size(MAX_ATTACHMENT_BYTES + 1).is_err());
    }

    #[test]
    fn preserves_image_file_paste_and_extracts_generic_absolute_paths_in_order() {
        let directory = tempfile::tempdir().expect("temp directory");
        let image = directory.path().join("screen shot.png");
        let first = directory.path().join("notes with spaces.txt");
        let second = directory.path().join("資料.json");
        fs::write(&image, b"png bytes").expect("fixture image");
        fs::write(&first, b"notes").expect("first generic file");
        fs::write(&second, b"data").expect("second generic file");

        let input = local_file_input([image.clone(), first.clone(), second.clone()], false)
            .expect("extract local files");

        assert_eq!(input.paths, vec![first, second]);
        assert_eq!(input.image_attachments.len(), 1);
        assert_eq!(input.image_attachments[0].label, "screen shot.png");
        assert_eq!(
            input.image_attachments[0].path.as_deref(),
            Some(image.as_path())
        );
        assert_eq!(
            input.image_attachments[0]
                .image
                .as_ref()
                .map(|image| image.mime_type.as_str()),
            Some("image/png")
        );
    }

    #[test]
    fn terminal_path_text_resolves_relative_files_but_rejects_directories_and_ordinary_text() {
        let directory = tempfile::tempdir().expect("temp directory");
        let file = directory.path().join("normal.txt");
        fs::write(&file, b"normal").expect("fixture file");

        let input = local_files_from_terminal_paste(&file.to_string_lossy())
            .expect("absolute path should resolve");
        assert_eq!(input.paths, vec![file]);
        assert!(local_files_from_terminal_paste(&directory.path().to_string_lossy()).is_none());
        assert!(local_files_from_terminal_paste("not a local file").is_none());
    }

    #[test]
    fn native_file_metadata_rejects_relative_paths_instead_of_fabricating_authority() {
        let directory = tempfile::tempdir_in(".").expect("workspace temp directory");
        let path = directory.path().join("relative.txt");
        fs::write(&path, b"relative").expect("fixture file");
        let absolute = fs::canonicalize(&path).expect("absolute fixture path");
        let relative = absolute
            .strip_prefix(std::env::current_dir().expect("current directory"))
            .expect("fixture is below current directory")
            .to_path_buf();

        let input = local_file_input([relative], false)
            .expect("relative path is an unsupported payload, not an error");
        assert!(input.paths.is_empty());
        assert!(input.image_attachments.is_empty());
    }
}
