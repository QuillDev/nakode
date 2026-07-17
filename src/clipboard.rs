use std::io::{self, Write};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use thiserror::Error;

const MAX_SELECTION_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum ClipboardError {
    #[error("selection is too large to copy ({actual} bytes; maximum {maximum})")]
    TooLarge { actual: usize, maximum: usize },
    #[error("failed to write terminal clipboard sequence: {0}")]
    Io(#[from] io::Error),
}

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
    use super::{MAX_SELECTION_BYTES, osc52_sequence, write_osc52};

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
}
