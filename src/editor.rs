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
    pub prompt_line_starts: Vec<bool>,
    pub cursor_x: u16,
    pub cursor_y: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CursorToken {
    pub text: String,
    pub at_prompt_start: bool,
    character_count: usize,
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

    #[must_use]
    pub fn token_before_cursor(&self) -> CursorToken {
        let prefix = self.lines[self.row]
            .chars()
            .take(self.column)
            .collect::<Vec<_>>();
        let word_start = prefix
            .iter()
            .rposition(|character| character.is_whitespace())
            .map_or(0, |index| index + 1);
        let start = prefix[word_start..]
            .iter()
            .rposition(|character| *character == '/')
            .map_or(word_start, |index| word_start + index);
        CursorToken {
            text: prefix[start..].iter().collect(),
            at_prompt_start: self.row == 0 && start == 0,
            character_count: prefix.len().saturating_sub(start),
        }
    }

    pub fn replace_token_before_cursor(&mut self, replacement: &str) {
        let token = self.token_before_cursor();
        let line = &mut self.lines[self.row];
        let end = char_to_byte(line, self.column);
        let start = char_to_byte(line, self.column.saturating_sub(token.character_count));
        line.replace_range(start..end, replacement);
        self.column = self
            .column
            .saturating_sub(token.character_count)
            .saturating_add(replacement.chars().count());
        self.mark_changed();
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

    pub fn delete_word_backward(&mut self) {
        if self.column == 0 {
            self.backspace();
            return;
        }

        let end_column = self.column;
        self.move_word_left();
        let line = &mut self.lines[self.row];
        let start = char_to_byte(line, self.column);
        let end = char_to_byte(line, end_column);
        line.replace_range(start..end, "");
        self.mark_changed();
    }

    pub fn delete_to_line_start(&mut self) {
        if self.column == 0 {
            self.backspace();
            return;
        }

        let line = &mut self.lines[self.row];
        let end = char_to_byte(line, self.column);
        line.replace_range(..end, "");
        self.column = 0;
        self.mark_changed();
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

    pub fn move_word_left(&mut self) {
        if self.column == 0 {
            if self.row == 0 {
                return;
            }
            self.row -= 1;
            self.column = self.lines[self.row].chars().count();
        }

        let characters = self.lines[self.row].chars().collect::<Vec<_>>();
        while self.column > 0 && characters[self.column - 1].is_whitespace() {
            self.column -= 1;
        }
        let Some(class) = self
            .column
            .checked_sub(1)
            .map(|index| character_class(characters[index]))
        else {
            return;
        };
        while self.column > 0 && character_class(characters[self.column - 1]) == class {
            self.column -= 1;
        }
    }

    pub fn move_word_right(&mut self) {
        let line_len = self.lines[self.row].chars().count();
        if self.column == line_len {
            if self.row + 1 == self.lines.len() {
                return;
            }
            self.row += 1;
            self.column = 0;
        }

        let characters = self.lines[self.row].chars().collect::<Vec<_>>();
        while self.column < characters.len() && characters[self.column].is_whitespace() {
            self.column += 1;
        }
        let Some(character) = characters.get(self.column) else {
            return;
        };
        let class = character_class(*character);
        while self.column < characters.len() && character_class(characters[self.column]) == class {
            self.column += 1;
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

    pub fn move_document_start(&mut self) {
        self.row = 0;
        self.column = 0;
    }

    pub fn move_document_end(&mut self) {
        self.row = self.lines.len() - 1;
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
        let mut visual_lines = Vec::new();
        let mut prompt_line_starts = Vec::new();
        let mut cursor = (0, 0);

        for (row, line) in self.lines.iter().enumerate() {
            let wrapped = wrap_display_line(line, width, (row == self.row).then_some(self.column));
            let first_visual_row = visual_lines.len();
            if let Some((cursor_row, cursor_column)) = wrapped.cursor {
                cursor = (first_visual_row + cursor_row, cursor_column);
            }
            for (visual_row, line) in wrapped.lines.into_iter().enumerate() {
                visual_lines.push(line);
                prompt_line_starts.push(row == 0 && visual_row == 0);
            }
        }

        let first_visual_row = cursor.0.saturating_sub(height - 1);
        let lines = visual_lines
            .into_iter()
            .skip(first_visual_row)
            .take(height)
            .collect();
        let prompt_line_starts = prompt_line_starts
            .into_iter()
            .skip(first_visual_row)
            .take(height)
            .collect();

        EditorWindow {
            lines,
            prompt_line_starts,
            cursor_x: u16::try_from(cursor.1.min(width - 1))
                .expect("cursor x is bounded by the u16 terminal width"),
            cursor_y: u16::try_from(cursor.0.saturating_sub(first_visual_row))
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

fn character_width(character: char) -> usize {
    if character == '\t' {
        4
    } else {
        UnicodeWidthChar::width(character).unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CharacterClass {
    Word,
    Punctuation,
    Whitespace,
}

fn character_class(character: char) -> CharacterClass {
    if character.is_whitespace() {
        CharacterClass::Whitespace
    } else if character.is_alphanumeric() || character == '_' {
        CharacterClass::Word
    } else {
        CharacterClass::Punctuation
    }
}

struct WrappedDisplayLine {
    lines: Vec<String>,
    cursor: Option<(usize, usize)>,
}

fn wrap_display_line(text: &str, width: usize, cursor_column: Option<usize>) -> WrappedDisplayLine {
    let mut lines = Vec::new();
    let mut line = String::new();
    let mut display_column: usize = 0;
    let mut cursor = None;

    for (character_index, character) in text.chars().enumerate() {
        let (rendered, rendered_width) = if character == '\t' {
            ("    ".to_owned(), 4)
        } else if character.is_control() {
            ("�".to_owned(), 1)
        } else {
            (character.to_string(), character_width(character))
        };
        let (rendered, rendered_width) = if rendered_width > width {
            ("�".to_owned(), 1)
        } else {
            (rendered, rendered_width)
        };

        if display_column > 0 && display_column.saturating_add(rendered_width) > width {
            lines.push(std::mem::take(&mut line));
            display_column = 0;
        }
        if cursor_column == Some(character_index) {
            cursor = Some((lines.len(), display_column));
        }
        line.push_str(&rendered);
        display_column = display_column.saturating_add(rendered_width);
    }

    if cursor_column == Some(text.chars().count()) {
        cursor = Some((lines.len(), display_column));
    }
    lines.push(line);

    WrappedDisplayLine { lines, cursor }
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
    fn window_wraps_wide_text_and_tracks_the_cursor() {
        let mut editor = EditorState::default();
        editor.insert_str("ab世界");
        let window = editor.window(2, 5);

        assert_eq!(window.cursor_x, 2);
        assert_eq!(window.cursor_y, 1);
        assert_eq!(window.lines, vec!["ab世", "界"]);
    }

    #[test]
    fn window_wraps_long_input_instead_of_scrolling_horizontally() {
        let mut editor = EditorState::default();
        editor.insert_str("abcdefghijkl");
        let window = editor.window(2, 5);

        assert_eq!(window.lines, vec!["fghij", "kl"]);
        assert_eq!(window.cursor_x, 2);
        assert_eq!(window.cursor_y, 1);
        assert_eq!(window.prompt_line_starts, vec![false, false]);
    }

    #[test]
    fn window_preserves_explicit_newlines_while_wrapping() {
        let mut editor = EditorState::default();
        editor.insert_str("abcdef\nxy");
        let window = editor.window(3, 5);

        assert_eq!(window.lines, vec!["abcde", "f", "xy"]);
        assert_eq!(window.cursor_x, 2);
        assert_eq!(window.cursor_y, 2);
        assert_eq!(window.prompt_line_starts, vec![true, false, false]);
    }

    #[test]
    fn identifies_and_replaces_the_token_before_the_cursor() {
        let mut editor = EditorState::default();
        editor.insert_str("please(/sk later");
        for _ in 0..6 {
            editor.move_left();
        }

        let token = editor.token_before_cursor();
        assert_eq!(token.text, "/sk");
        assert!(!token.at_prompt_start);

        editor.replace_token_before_cursor("/skill:");
        assert_eq!(editor.text(), "please(/skill: later");
    }

    #[test]
    fn moves_across_words_and_punctuation() {
        let mut editor = EditorState::default();
        editor.set_text("one two.three");

        editor.move_word_left();
        editor.insert_char('|');
        assert_eq!(editor.text(), "one two.|three");

        editor.move_document_start();
        editor.move_word_right();
        editor.insert_char('|');
        assert_eq!(editor.text(), "one| two.|three");
    }

    #[test]
    fn moves_to_prompt_boundaries() {
        let mut editor = EditorState::default();
        editor.set_text("first\nsecond");
        editor.move_document_start();
        editor.insert_char('^');
        editor.move_document_end();
        editor.insert_char('$');

        assert_eq!(editor.text(), "^first\nsecond$");
    }

    #[test]
    fn deletes_by_word_and_to_the_line_start() {
        let mut editor = EditorState::default();
        editor.set_text("one two.three");
        editor.delete_word_backward();
        assert_eq!(editor.text(), "one two.");
        editor.delete_word_backward();
        assert_eq!(editor.text(), "one two");
        editor.delete_to_line_start();
        assert!(editor.is_blank());
    }
}
