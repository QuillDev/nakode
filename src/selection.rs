use ratatui::{buffer::Buffer, layout::Rect};
use unicode_width::UnicodeWidthStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScreenPoint {
    pub column: u16,
    pub row: u16,
}

impl ScreenPoint {
    #[must_use]
    pub const fn new(column: u16, row: u16) -> Self {
        Self { column, row }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextSelection {
    pub anchor: ScreenPoint,
    pub head: ScreenPoint,
}

impl TextSelection {
    #[must_use]
    pub const fn new(anchor: ScreenPoint) -> Self {
        Self {
            anchor,
            head: anchor,
        }
    }

    pub fn update(&mut self, head: ScreenPoint) {
        self.head = head;
    }

    #[must_use]
    pub const fn is_range(self) -> bool {
        self.anchor.column != self.head.column || self.anchor.row != self.head.row
    }

    #[must_use]
    pub fn contains(self, point: ScreenPoint) -> bool {
        let (start, end) = self.ordered();
        point_key(point) >= point_key(start) && point_key(point) <= point_key(end)
    }

    fn ordered(self) -> (ScreenPoint, ScreenPoint) {
        if point_key(self.anchor) <= point_key(self.head) {
            (self.anchor, self.head)
        } else {
            (self.head, self.anchor)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScreenRow {
    text: String,
    byte_offsets: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenSnapshot {
    area: Rect,
    selectable_regions: Vec<Rect>,
    rows: Vec<ScreenRow>,
}

impl ScreenSnapshot {
    #[must_use]
    /// Captures selectable text and byte boundaries from a rendered buffer.
    ///
    /// # Panics
    ///
    /// Panics only if `area` describes cells outside `buffer`.
    pub fn capture(buffer: &Buffer, area: Rect, selectable_regions: Vec<Rect>) -> Self {
        let mut rows = Vec::with_capacity(usize::from(area.height));
        for row in area.y..area.y.saturating_add(area.height) {
            let mut text = String::new();
            let mut byte_offsets = vec![0; usize::from(area.width) + 1];
            let mut continuation_cells = 0usize;

            for column_offset in 0..usize::from(area.width) {
                byte_offsets[column_offset] = text.len();
                if continuation_cells > 0 {
                    continuation_cells -= 1;
                    byte_offsets[column_offset + 1] = text.len();
                    continue;
                }

                let column = area.x.saturating_add(
                    u16::try_from(column_offset)
                        .expect("column offset is bounded by the u16 terminal width"),
                );
                let symbol = buffer[(column, row)].symbol();
                text.push_str(symbol);
                let display_width = UnicodeWidthStr::width(symbol).max(1);
                continuation_cells = display_width.saturating_sub(1);
                byte_offsets[column_offset + 1] = text.len();
            }
            rows.push(ScreenRow { text, byte_offsets });
        }

        Self {
            area,
            selectable_regions,
            rows,
        }
    }

    #[must_use]
    pub fn selected_text(&self, selection: TextSelection) -> Option<String> {
        if !selection.is_range() || self.area.width == 0 || self.area.height == 0 {
            return None;
        }

        let selection_area = self
            .selectable_regions
            .iter()
            .copied()
            .find(|area| contains(*area, selection.anchor) && contains(*area, selection.head))
            .unwrap_or(self.area);
        let anchor = Self::clamp(selection.anchor, selection_area);
        let head = Self::clamp(selection.head, selection_area);
        let selection = TextSelection { anchor, head };
        let (start, end) = selection.ordered();
        let mut lines = Vec::with_capacity(usize::from(end.row - start.row + 1));

        for row in start.row..=end.row {
            let row_data = &self.rows[usize::from(row - self.area.y)];
            let start_column = if row == start.row {
                start.column
            } else {
                selection_area.x
            };
            let end_column = if row == end.row {
                end.column
            } else {
                selection_area.x + selection_area.width - 1
            };
            let start_offset = usize::from(start_column - self.area.x);
            let end_offset = usize::from(end_column - self.area.x) + 1;
            let start_byte = row_data.byte_offsets[start_offset];
            let end_byte = row_data.byte_offsets[end_offset];
            lines.push(row_data.text[start_byte..end_byte].trim_end().to_owned());
        }

        let text = lines.join("\n");
        (!text.trim().is_empty()).then_some(text)
    }

    fn clamp(point: ScreenPoint, area: Rect) -> ScreenPoint {
        ScreenPoint {
            column: point.column.clamp(area.x, area.right() - 1),
            row: point.row.clamp(area.y, area.bottom() - 1),
        }
    }
}

fn contains(area: Rect, point: ScreenPoint) -> bool {
    point.column >= area.x
        && point.column < area.right()
        && point.row >= area.y
        && point.row < area.bottom()
}

fn point_key(point: ScreenPoint) -> (u16, u16) {
    (point.row, point.column)
}

#[cfg(test)]
mod tests {
    use ratatui::{buffer::Buffer, layout::Rect};

    use super::{ScreenPoint, ScreenSnapshot, TextSelection};

    #[test]
    fn extracts_forward_and_reverse_linear_selections() {
        let buffer = Buffer::with_lines(["hello world", "second line"]);
        let snapshot = ScreenSnapshot::capture(&buffer, buffer.area, vec![buffer.area]);
        let forward = TextSelection {
            anchor: ScreenPoint::new(1, 0),
            head: ScreenPoint::new(5, 1),
        };
        let reverse = TextSelection {
            anchor: forward.head,
            head: forward.anchor,
        };

        assert_eq!(
            snapshot.selected_text(forward).as_deref(),
            Some("ello world\nsecond")
        );
        assert_eq!(
            snapshot.selected_text(reverse),
            snapshot.selected_text(forward)
        );
    }

    #[test]
    fn ignores_clicks_and_whitespace_only_ranges() {
        let buffer = Buffer::with_lines(["text    "]);
        let snapshot = ScreenSnapshot::capture(&buffer, buffer.area, vec![buffer.area]);
        let click = TextSelection::new(ScreenPoint::new(0, 0));
        let spaces = TextSelection {
            anchor: ScreenPoint::new(4, 0),
            head: ScreenPoint::new(7, 0),
        };

        assert_eq!(snapshot.selected_text(click), None);
        assert_eq!(snapshot.selected_text(spaces), None);
    }

    #[test]
    fn selection_regions_exclude_widget_borders() {
        let buffer = Buffer::with_lines(["│first  │", "│second │"]);
        let content = Rect::new(1, 0, 7, 2);
        let snapshot = ScreenSnapshot::capture(&buffer, buffer.area, vec![content]);
        let selection = TextSelection {
            anchor: ScreenPoint::new(1, 0),
            head: ScreenPoint::new(6, 1),
        };

        assert_eq!(
            snapshot.selected_text(selection).as_deref(),
            Some("first\nsecond")
        );
    }

    #[test]
    fn wide_symbols_are_not_duplicated() {
        let buffer = Buffer::with_lines(["A界B"]);
        let snapshot = ScreenSnapshot::capture(&buffer, buffer.area, vec![buffer.area]);
        let selection = TextSelection {
            anchor: ScreenPoint::new(0, 0),
            head: ScreenPoint::new(3, 0),
        };

        assert_eq!(snapshot.selected_text(selection).as_deref(), Some("A界B"));
    }
}
