use std::collections::{HashMap, HashSet};

use ratatui::{Frame, layout::Rect};
use ratatui_image::{Image, Resize, picker::Picker, protocol::Protocol};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backend::PromptImage;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalImageMode {
    #[default]
    Auto,
    On,
    Off,
}

impl TerminalImageMode {
    pub const ALL: [Self; 3] = [Self::Auto, Self::On, Self::Off];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Auto => "Automatic",
            Self::On => "On",
            Self::Off => "Off",
        }
    }
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    digest: [u8; 32],
    width: u16,
    height: u16,
}

/// Detects terminal image support and caches protocol-encoded previews.
pub struct TerminalImageRenderer {
    picker: Picker,
    protocols: HashMap<CacheKey, Protocol>,
    failed: HashSet<CacheKey>,
    terminal_size: Option<ratatui::layout::Size>,
}

impl TerminalImageRenderer {
    /// Queries the attached terminal for its graphics protocol and cell dimensions.
    ///
    /// Returns `None` when querying fails, leaving the normal attachment labels in place.
    #[must_use]
    pub fn detect(mode: TerminalImageMode) -> Option<Self> {
        let mode = environment_override().unwrap_or(mode);
        if mode == TerminalImageMode::Off
            || (mode == TerminalImageMode::Auto && !terminal_may_support_images())
        {
            return None;
        }
        Picker::from_query_stdio().ok().map(|picker| Self {
            picker,
            protocols: HashMap::new(),
            failed: HashSet::new(),
            terminal_size: None,
        })
    }

    pub fn begin_frame(&mut self, terminal_size: ratatui::layout::Size) {
        if self.terminal_size.is_some_and(|size| size != terminal_size) {
            self.protocols.clear();
            self.failed.clear();
        }
        self.terminal_size = Some(terminal_size);
    }

    pub fn render(&mut self, frame: &mut Frame<'_>, area: Rect, image: &PromptImage) {
        if area.is_empty() {
            return;
        }
        let key = CacheKey {
            digest: Sha256::digest(&image.data).into(),
            width: area.width,
            height: area.height,
        };
        if self.failed.contains(&key) {
            return;
        }
        if !self.protocols.contains_key(&key) {
            let protocol = image::load_from_memory(&image.data)
                .map_err(|error| error.to_string())
                .and_then(|image| {
                    self.picker
                        .new_protocol(
                            image,
                            ratatui::layout::Size::new(area.width, area.height),
                            Resize::Fit(None),
                        )
                        .map_err(|error| error.to_string())
                });
            if let Ok(protocol) = protocol {
                self.protocols.insert(key.clone(), protocol);
            } else {
                self.failed.insert(key);
                return;
            }
        }
        if let Some(protocol) = self.protocols.get(&key) {
            frame.render_widget(Image::new(protocol), area);
        }
    }
}

fn environment_override() -> Option<TerminalImageMode> {
    match std::env::var("NAKODE_TERMINAL_IMAGES").as_deref() {
        Ok("0" | "false" | "off") => Some(TerminalImageMode::Off),
        Ok("1" | "true" | "on") => Some(TerminalImageMode::On),
        Ok("auto") => Some(TerminalImageMode::Auto),
        _ => None,
    }
}

fn terminal_may_support_images() -> bool {
    let has_pixel_dimensions =
        crossterm::terminal::window_size().is_ok_and(|size| size.width > 0 && size.height > 0);
    if !has_pixel_dimensions {
        return false;
    }
    if ["KITTY_WINDOW_ID", "WEZTERM_PANE", "GHOSTTY_RESOURCES_DIR"]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        return true;
    }
    let term = std::env::var("TERM")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let program = std::env::var("TERM_PROGRAM")
        .unwrap_or_default()
        .to_ascii_lowercase();
    ["kitty", "wezterm", "ghostty", "iterm", "foot", "contour"]
        .iter()
        .any(|hint| term.contains(hint) || program.contains(hint))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use image::{DynamicImage, ImageFormat};
    use ratatui::{Terminal, backend::TestBackend};
    use ratatui_image::picker::Picker;

    use super::TerminalImageRenderer;
    use crate::backend::PromptImage;

    #[test]
    fn valid_image_bytes_are_encoded_once_and_rendered() {
        let mut bytes = Vec::new();
        DynamicImage::new_rgb8(2, 2)
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .expect("encode image");
        let image = PromptImage {
            mime_type: "image/png".to_owned(),
            data: bytes,
        };
        let mut renderer = TerminalImageRenderer {
            picker: Picker::halfblocks(),
            protocols: std::collections::HashMap::new(),
            failed: std::collections::HashSet::new(),
            terminal_size: None,
        };
        let mut terminal = Terminal::new(TestBackend::new(20, 10)).expect("terminal");

        terminal
            .draw(|frame| renderer.render(frame, frame.area(), &image))
            .expect("render image");
        terminal
            .draw(|frame| renderer.render(frame, frame.area(), &image))
            .expect("render cached image");

        assert_eq!(renderer.protocols.len(), 1);
        assert!(renderer.failed.is_empty());

        renderer.begin_frame(ratatui::layout::Size::new(10, 5));
        renderer.begin_frame(ratatui::layout::Size::new(20, 10));
        assert!(renderer.protocols.is_empty());
    }
}
