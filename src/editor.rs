use unicode_width::UnicodeWidthChar;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditorState {
    lines: Vec<String>,
    row: usize,
    column: usize,
    revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditorWindow {
    pub lines: Vec<String>,
    pub cursor_x: u16,
    pub cursor_y: u16,
}

impl Default for EditorState {
    fn default() -> Self {
        Self {
            lines: vec![String::new()],
            row: 0,
            column: 0,
            revision: 0,
        }
    }
}

impl EditorState {
    #[must_use]
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    #[must_use]
    pub fn is_blank(&self) -> bool {
        self.lines.iter().all(|line| line.trim().is_empty())
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn clear(&mut self) {
        self.lines.clear();
        self.lines.push(String::new());
        self.row = 0;
        self.column = 0;
        self.mark_changed();
    }

    pub fn set_text(&mut self, text: &str) {
        self.clear();
        self.insert_str(text);
    }

    pub fn insert_char(&mut self, character: char) {
        match character {
            '\n' => self.insert_newline(),
            '\r' => {}
            '\t' => self.insert_visible_char('\t'),
            character if character.is_control() => self.insert_visible_char('\u{fffd}'),
            character => self.insert_visible_char(character),
        }
    }

    pub fn insert_str(&mut self, text: &str) {
        let mut characters = text.chars().peekable();
        while let Some(character) = characters.next() {
            if character == '\r' {
                if characters.peek() == Some(&'\n') {
                    continue;
                }
                self.insert_newline();
            } else {
                self.insert_char(character);
            }
        }
    }

    pub fn insert_newline(&mut self) {
        let byte = char_to_byte(&self.lines[self.row], self.column);
        let tail = self.lines[self.row].split_off(byte);
        self.row += 1;
        self.lines.insert(self.row, tail);
        self.column = 0;
        self.mark_changed();
    }

    pub fn backspace(&mut self) {
        if self.column > 0 {
            let line = &mut self.lines[self.row];
            let end = char_to_byte(line, self.column);
            let start = char_to_byte(line, self.column - 1);
            line.replace_range(start..end, "");
            self.column -= 1;
            self.mark_changed();
        } else if self.row > 0 {
            let current = self.lines.remove(self.row);
            self.row -= 1;
            self.column = self.lines[self.row].chars().count();
            self.lines[self.row].push_str(&current);
            self.mark_changed();
        }
    }

    pub fn delete(&mut self) {
        let line_len = self.lines[self.row].chars().count();
        if self.column < line_len {
            let line = &mut self.lines[self.row];
            let start = char_to_byte(line, self.column);
            let end = char_to_byte(line, self.column + 1);
            line.replace_range(start..end, "");
            self.mark_changed();
        } else if self.row + 1 < self.lines.len() {
            let next = self.lines.remove(self.row + 1);
            self.lines[self.row].push_str(&next);
            self.mark_changed();
        }
    }

    pub fn move_left(&mut self) {
        if self.column > 0 {
            self.column -= 1;
        } else if self.row > 0 {
            self.row -= 1;
            self.column = self.lines[self.row].chars().count();
        }
    }

    pub fn move_right(&mut self) {
        let line_len = self.lines[self.row].chars().count();
        if self.column < line_len {
            self.column += 1;
        } else if self.row + 1 < self.lines.len() {
            self.row += 1;
            self.column = 0;
        }
    }

    pub fn move_up(&mut self) {
        if self.row > 0 {
            self.row -= 1;
            self.column = self.column.min(self.lines[self.row].chars().count());
        }
    }

    pub fn move_down(&mut self) {
        if self.row + 1 < self.lines.len() {
            self.row += 1;
            self.column = self.column.min(self.lines[self.row].chars().count());
        }
    }

    pub fn move_home(&mut self) {
        self.column = 0;
    }

    pub fn move_end(&mut self) {
        self.column = self.lines[self.row].chars().count();
    }

    #[must_use]
    /// Returns the visible editor window for the supplied terminal dimensions.
    ///
    /// # Panics
    ///
    /// Panics only if the editor's internal cursor invariant is broken.
    pub fn window(&self, height: u16, width: u16) -> EditorWindow {
        let height = usize::from(height.max(1));
        let width = usize::from(width.max(1));
        let first_row = self.row.saturating_sub(height - 1);
        let cursor_display_x = display_width_prefix(&self.lines[self.row], self.column);
        let horizontal_offset = cursor_display_x.saturating_sub(width - 1);

        let lines = self
            .lines
            .iter()
            .skip(first_row)
            .take(height)
            .map(|line| clip_display(line, horizontal_offset, width))
            .collect();

        EditorWindow {
            lines,
            cursor_x: u16::try_from(
                cursor_display_x
                    .saturating_sub(horizontal_offset)
                    .min(width - 1),
            )
            .expect("cursor x is bounded by the u16 terminal width"),
            cursor_y: u16::try_from(self.row.saturating_sub(first_row))
                .expect("cursor y is bounded by the u16 terminal height"),
        }
    }

    fn insert_visible_char(&mut self, character: char) {
        let byte = char_to_byte(&self.lines[self.row], self.column);
        self.lines[self.row].insert(byte, character);
        self.column += 1;
        self.mark_changed();
    }

    fn mark_changed(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

fn char_to_byte(text: &str, character_index: usize) -> usize {
    text.char_indices()
        .nth(character_index)
        .map_or(text.len(), |(index, _)| index)
}

fn display_width_prefix(text: &str, character_count: usize) -> usize {
    text.chars()
        .take(character_count)
        .map(character_width)
        .sum()
}

fn character_width(character: char) -> usize {
    if character == '\t' {
        4
    } else {
        UnicodeWidthChar::width(character).unwrap_or(0)
    }
}

fn clip_display(text: &str, offset: usize, width: usize) -> String {
    let mut output = String::new();
    let mut column = 0;

    for character in text.chars() {
        let rendered = if character == '\t' {
            "    ".to_owned()
        } else if character.is_control() {
            "�".to_owned()
        } else {
            character.to_string()
        };
        let character_width = if character == '\t' {
            4
        } else {
            character_width(character)
        };
        let next_column = column + character_width;

        if next_column <= offset {
            column = next_column;
            continue;
        }
        if column >= offset + width {
            break;
        }
        if column < offset && next_column > offset {
            output.push(' ');
        } else if next_column <= offset + width {
            output.push_str(&rendered);
        }
        column = next_column;
    }

    output
}

#[cfg(test)]
mod tests {
    use super::EditorState;

    #[test]
    fn edits_multiline_unicode_text() {
        let mut editor = EditorState::default();
        editor.insert_str("hello\n世界");
        editor.move_left();
        editor.backspace();
        editor.insert_char('界');

        assert_eq!(editor.text(), "hello\n界界");
    }

    #[test]
    fn backspace_joins_lines() {
        let mut editor = EditorState::default();
        editor.insert_str("one\ntwo");
        editor.move_home();
        editor.backspace();

        assert_eq!(editor.text(), "onetwo");
    }

    #[test]
    fn pasted_escape_is_data_not_control_input() {
        let mut editor = EditorState::default();
        editor.insert_str("safe\u{1b}[31m");

        assert_eq!(editor.text(), "safe�[31m");
    }

    #[test]
    fn window_tracks_wide_cursor() {
        let mut editor = EditorState::default();
        editor.insert_str("ab世界");
        let window = editor.window(2, 5);

        assert_eq!(window.cursor_x, 4);
        assert_eq!(window.lines, vec!["世界"]);
    }
}
