pub const DEFAULT_MAX_LINES: usize = 2_000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;
pub const GREP_MAX_LINE_CHARS: usize = 500;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruncatedBy {
    Lines,
    Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Truncation {
    pub content: String,
    pub total_lines: usize,
    pub output_lines: usize,
    pub truncated_by: Option<TruncatedBy>,
    pub first_line_exceeds_limit: bool,
}

#[must_use]
pub fn truncate_head(input: &str, max_lines: usize, max_bytes: usize) -> Truncation {
    let total_lines = input.lines().count().max(usize::from(!input.is_empty()));
    if input.len() <= max_bytes && total_lines <= max_lines {
        return Truncation {
            content: input.to_owned(),
            total_lines,
            output_lines: total_lines,
            truncated_by: None,
            first_line_exceeds_limit: false,
        };
    }

    let mut content = String::new();
    let mut output_lines = 0;
    let mut truncated_by = None;
    for line in input.split_inclusive('\n') {
        if output_lines == max_lines {
            truncated_by = Some(TruncatedBy::Lines);
            break;
        }
        if content.len().saturating_add(line.len()) > max_bytes {
            truncated_by = Some(TruncatedBy::Bytes);
            break;
        }
        content.push_str(line);
        output_lines += 1;
    }
    if truncated_by.is_none() && content.len() < input.len() {
        truncated_by = Some(TruncatedBy::Bytes);
    }
    Truncation {
        first_line_exceeds_limit: output_lines == 0 && !input.is_empty(),
        content,
        total_lines,
        output_lines,
        truncated_by,
    }
}

#[must_use]
pub fn truncate_line(line: &str) -> (String, bool) {
    let mut characters = line.chars();
    let retained = characters
        .by_ref()
        .take(GREP_MAX_LINE_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        (format!("{retained}... [truncated]"), true)
    } else {
        (retained, false)
    }
}

#[cfg(test)]
mod tests {
    use super::{TruncatedBy, truncate_head, truncate_line};

    #[test]
    fn head_truncation_retains_only_complete_lines() {
        let output = truncate_head("one\ntwo\nthree\n", 2, 100);
        assert_eq!(output.content, "one\ntwo\n");
        assert_eq!(output.output_lines, 2);
        assert_eq!(output.total_lines, 3);
        assert_eq!(output.truncated_by, Some(TruncatedBy::Lines));
    }

    #[test]
    fn oversized_first_line_is_reported_without_partial_content() {
        let output = truncate_head("oversized\nnext", 20, 4);
        assert!(output.content.is_empty());
        assert!(output.first_line_exceeds_limit);
        assert_eq!(output.truncated_by, Some(TruncatedBy::Bytes));
    }

    #[test]
    fn grep_lines_are_bounded_by_characters() {
        let (line, truncated) = truncate_line(&"x".repeat(600));
        assert!(truncated);
        assert!(line.ends_with("... [truncated]"));
    }
}
