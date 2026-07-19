use std::sync::LazyLock;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use syntect::{
    easy::HighlightLines,
    highlighting::{FontStyle, Theme, ThemeSet},
    parsing::SyntaxSet,
    util::LinesWithEndings,
};
use unicode_width::UnicodeWidthChar;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkdownTone {
    Accent,
    Muted,
    Success,
    Warning,
    Link,
    Rgb(u8, u8, u8),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkdownModifier {
    Bold,
    Italic,
    Underlined,
    CrossedOut,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MarkdownModifiers(u8);

impl MarkdownModifiers {
    const BOLD: u8 = 1 << 0;
    const ITALIC: u8 = 1 << 1;
    const UNDERLINED: u8 = 1 << 2;
    const CROSSED_OUT: u8 = 1 << 3;

    pub fn insert(&mut self, modifier: MarkdownModifier) {
        self.0 |= Self::mask(modifier);
    }

    #[must_use]
    pub const fn contains(self, modifier: MarkdownModifier) -> bool {
        self.0 & Self::mask(modifier) != 0
    }

    const fn mask(modifier: MarkdownModifier) -> u8 {
        match modifier {
            MarkdownModifier::Bold => Self::BOLD,
            MarkdownModifier::Italic => Self::ITALIC,
            MarkdownModifier::Underlined => Self::UNDERLINED,
            MarkdownModifier::CrossedOut => Self::CROSSED_OUT,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MarkdownStyle {
    pub tone: Option<MarkdownTone>,
    pub modifiers: MarkdownModifiers,
    pub code: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkdownSpan {
    pub text: String,
    pub style: MarkdownStyle,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkdownLine {
    pub text: String,
    pub spans: Vec<MarkdownSpan>,
}

#[derive(Clone, Debug)]
struct ListState {
    next: Option<u64>,
}

#[derive(Clone, Debug)]
struct ItemState {
    marker: String,
    marker_tone: MarkdownTone,
    marker_used: bool,
}

#[derive(Clone, Debug)]
struct LinkState {
    destination: String,
    label: String,
    previous_style: MarkdownStyle,
}

#[derive(Clone, Debug)]
struct CodeBlock {
    language: Option<String>,
    source: String,
}

struct MarkdownRenderer {
    width: usize,
    lines: Vec<MarkdownLine>,
    current: Vec<MarkdownSpan>,
    continuation_prefix: Vec<MarkdownSpan>,
    inline_style: MarkdownStyle,
    style_stack: Vec<MarkdownStyle>,
    lists: Vec<ListState>,
    items: Vec<ItemState>,
    quote_depth: usize,
    heading: Option<HeadingLevel>,
    links: Vec<LinkState>,
    code_block: Option<CodeBlock>,
    table_cell: usize,
    table_columns: usize,
}

static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static SYNTAX_THEME: LazyLock<Theme> = LazyLock::new(|| {
    ThemeSet::load_defaults()
        .themes
        .remove("base16-ocean.dark")
        .expect("syntect ships the base16-ocean.dark theme")
});

#[must_use]
pub fn render_markdown(source: &str, width: usize) -> Vec<MarkdownLine> {
    let options = Options::ENABLE_GFM
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_TABLES
        | Options::ENABLE_FOOTNOTES;
    let mut renderer = MarkdownRenderer::new(width.max(1));
    for event in Parser::new_ext(source, options) {
        renderer.event(event);
    }
    renderer.finish()
}

impl MarkdownRenderer {
    fn new(width: usize) -> Self {
        Self {
            width,
            lines: Vec::new(),
            current: Vec::new(),
            continuation_prefix: Vec::new(),
            inline_style: MarkdownStyle::default(),
            style_stack: Vec::new(),
            lists: Vec::new(),
            items: Vec::new(),
            quote_depth: 0,
            heading: None,
            links: Vec::new(),
            code_block: None,
            table_cell: 0,
            table_columns: 0,
        }
    }

    fn event(&mut self, event: Event<'_>) {
        if let Some(code_block) = &mut self.code_block {
            match event {
                Event::Text(text) | Event::Code(text) => code_block.source.push_str(&text),
                Event::SoftBreak | Event::HardBreak => code_block.source.push('\n'),
                Event::End(TagEnd::CodeBlock) => self.finish_code_block(),
                _ => {}
            }
            return;
        }

        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) => self.append(&text, self.inline_style),
            Event::Code(code) => {
                let mut style = self.inline_style;
                style.tone = Some(MarkdownTone::Warning);
                style.code = true;
                self.append(&code, style);
            }
            Event::InlineMath(math) => {
                let mut style = self.inline_style;
                style.tone = Some(MarkdownTone::Warning);
                style.modifiers.insert(MarkdownModifier::Italic);
                self.append(&format!("${math}$"), style);
            }
            Event::DisplayMath(math) => {
                self.finish_line();
                let mut style = self.inline_style;
                style.tone = Some(MarkdownTone::Warning);
                style.modifiers.insert(MarkdownModifier::Italic);
                self.append(&format!("  {math}"), style);
                self.finish_line();
                self.push_gap();
            }
            Event::Html(html) | Event::InlineHtml(html) => self.append(&html, self.inline_style),
            Event::FootnoteReference(label) => {
                let mut style = self.inline_style;
                style.tone = Some(MarkdownTone::Link);
                style.modifiers.insert(MarkdownModifier::Underlined);
                self.append(&format!("[{label}]"), style);
            }
            Event::SoftBreak | Event::HardBreak => self.finish_line(),
            Event::Rule => {
                self.finish_line();
                self.emit_rule();
                self.push_gap();
            }
            Event::TaskListMarker(checked) => self.set_task_marker(checked),
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph | Tag::HtmlBlock | Tag::MetadataBlock(_) | Tag::DefinitionList => {}
            Tag::Heading { level, .. } => {
                self.finish_line();
                self.heading = Some(level);
                self.push_style(|style| {
                    style.modifiers.insert(MarkdownModifier::Bold);
                    style.tone = Some(MarkdownTone::Accent);
                });
            }
            Tag::BlockQuote(_) => {
                self.finish_line();
                self.quote_depth = self.quote_depth.saturating_add(1);
            }
            Tag::CodeBlock(kind) => {
                self.finish_line();
                let language = match kind {
                    CodeBlockKind::Indented => None,
                    CodeBlockKind::Fenced(info) => normalize_language(&info),
                };
                self.code_block = Some(CodeBlock {
                    language,
                    source: String::new(),
                });
            }
            Tag::List(start) => {
                self.finish_line();
                self.lists.push(ListState { next: start });
            }
            Tag::Item => self.start_item(),
            Tag::Table(_) => {
                self.finish_line();
                self.table_columns = 0;
            }
            Tag::TableHead | Tag::TableRow => {
                self.finish_line();
                self.table_cell = 0;
            }
            Tag::TableCell => {
                if self.table_cell > 0 {
                    let style = MarkdownStyle {
                        tone: Some(MarkdownTone::Muted),
                        ..MarkdownStyle::default()
                    };
                    self.append(" │ ", style);
                }
                self.table_cell = self.table_cell.saturating_add(1);
                self.table_columns = self.table_columns.max(self.table_cell);
            }
            Tag::Emphasis => self.push_style(|style| {
                style.modifiers.insert(MarkdownModifier::Italic);
            }),
            Tag::Strong => self.push_style(|style| {
                style.modifiers.insert(MarkdownModifier::Bold);
            }),
            Tag::Strikethrough => self.push_style(|style| {
                style.modifiers.insert(MarkdownModifier::CrossedOut);
            }),
            Tag::Superscript => self.append("^", self.inline_style),
            Tag::Subscript => self.append("~", self.inline_style),
            Tag::Link { dest_url, .. } => self.start_link(dest_url.into_string(), false),
            Tag::Image { dest_url, .. } => self.start_link(dest_url.into_string(), true),
            Tag::FootnoteDefinition(label) => {
                self.finish_line();
                let mut style = self.inline_style;
                style.tone = Some(MarkdownTone::Link);
                style.modifiers.insert(MarkdownModifier::Bold);
                self.append(&format!("[{label}] "), style);
            }
            Tag::DefinitionListTitle => self.push_style(|style| {
                style.modifiers.insert(MarkdownModifier::Bold);
            }),
            Tag::DefinitionListDefinition => {
                self.finish_line();
                self.append(
                    "  — ",
                    MarkdownStyle {
                        tone: Some(MarkdownTone::Muted),
                        ..MarkdownStyle::default()
                    },
                );
            }
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => {
                self.finish_line();
                if self.items.is_empty() {
                    self.push_gap();
                }
            }
            TagEnd::Heading(_) => {
                self.finish_line();
                self.pop_style();
                self.heading = None;
                self.push_gap();
            }
            TagEnd::BlockQuote(_) => {
                self.finish_line();
                self.quote_depth = self.quote_depth.saturating_sub(1);
                self.push_gap();
            }
            TagEnd::CodeBlock => self.finish_code_block(),
            TagEnd::List(_) => {
                self.finish_line();
                self.lists.pop();
                if self.lists.is_empty() {
                    self.push_gap();
                }
            }
            TagEnd::Item => {
                self.finish_line();
                self.items.pop();
            }
            TagEnd::Table => {
                self.finish_line();
                self.push_gap();
            }
            TagEnd::TableHead => {
                self.finish_line();
                self.emit_table_rule();
            }
            TagEnd::TableRow => self.finish_line(),
            TagEnd::TableCell | TagEnd::HtmlBlock | TagEnd::MetadataBlock(_) => {}
            TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough
            | TagEnd::DefinitionListTitle => self.pop_style(),
            TagEnd::Superscript => self.append("^", self.inline_style),
            TagEnd::Subscript => self.append("~", self.inline_style),
            TagEnd::Link | TagEnd::Image => self.finish_link(),
            TagEnd::FootnoteDefinition | TagEnd::DefinitionListDefinition => {
                self.finish_line();
            }
            TagEnd::DefinitionList => self.push_gap(),
        }
    }

    fn start_item(&mut self) {
        self.finish_line();
        let marker = self
            .lists
            .last_mut()
            .and_then(|list| {
                list.next.as_mut().map(|next| {
                    let marker = format!("{next}.");
                    *next = next.saturating_add(1);
                    marker
                })
            })
            .unwrap_or_else(|| "•".to_owned());
        self.items.push(ItemState {
            marker,
            marker_tone: MarkdownTone::Accent,
            marker_used: false,
        });
    }

    fn start_link(&mut self, destination: String, image: bool) {
        let previous_style = self.inline_style;
        if image {
            self.append(
                "image: ",
                MarkdownStyle {
                    tone: Some(MarkdownTone::Muted),
                    modifiers: modifiers(MarkdownModifier::Italic),
                    ..MarkdownStyle::default()
                },
            );
        }
        self.links.push(LinkState {
            destination,
            label: String::new(),
            previous_style,
        });
        self.inline_style.tone = Some(MarkdownTone::Link);
        self.inline_style
            .modifiers
            .insert(MarkdownModifier::Underlined);
    }

    fn push_style(&mut self, update: impl FnOnce(&mut MarkdownStyle)) {
        self.style_stack.push(self.inline_style);
        update(&mut self.inline_style);
    }

    fn pop_style(&mut self) {
        if let Some(style) = self.style_stack.pop() {
            self.inline_style = style;
        }
    }

    fn append(&mut self, text: &str, style: MarkdownStyle) {
        if text.is_empty() {
            return;
        }
        self.ensure_line();
        push_span(&mut self.current, text, style);
        for link in &mut self.links {
            link.label.push_str(text);
        }
    }

    fn ensure_line(&mut self) {
        if !self.current.is_empty() {
            return;
        }
        let (prefix, continuation) = self.prefixes();
        self.current = prefix;
        self.continuation_prefix = continuation;
        if let Some(item) = self.items.last_mut() {
            item.marker_used = true;
        }
    }

    fn prefixes(&self) -> (Vec<MarkdownSpan>, Vec<MarkdownSpan>) {
        let mut prefix = Vec::new();
        let mut continuation = Vec::new();
        let muted = MarkdownStyle {
            tone: Some(MarkdownTone::Muted),
            ..MarkdownStyle::default()
        };
        push_span(&mut prefix, "  ", muted);
        push_span(&mut continuation, "  ", muted);

        for _ in 0..self.quote_depth {
            push_span(&mut prefix, "│ ", muted);
            push_span(&mut continuation, "│ ", muted);
        }

        if !self.items.is_empty() {
            let indentation = "  ".repeat(self.items.len().saturating_sub(1));
            push_span(&mut prefix, &indentation, muted);
            push_span(&mut continuation, &indentation, muted);
            let item = self.items.last().expect("items is not empty");
            let marker = if item.marker_used {
                " ".repeat(display_width(&item.marker))
            } else {
                item.marker.clone()
            };
            push_span(
                &mut prefix,
                &format!("{marker} "),
                MarkdownStyle {
                    tone: Some(item.marker_tone),
                    ..MarkdownStyle::default()
                },
            );
            push_span(
                &mut continuation,
                &" ".repeat(display_width(&item.marker).saturating_add(1)),
                muted,
            );
        }

        if let Some(heading) = self.heading {
            let marker = match heading {
                HeadingLevel::H1 => "▌ ",
                HeadingLevel::H2 => "▍ ",
                HeadingLevel::H3 | HeadingLevel::H4 | HeadingLevel::H5 | HeadingLevel::H6 => "› ",
            };
            push_span(
                &mut prefix,
                marker,
                MarkdownStyle {
                    tone: Some(MarkdownTone::Accent),
                    modifiers: modifiers(MarkdownModifier::Bold),
                    ..MarkdownStyle::default()
                },
            );
            push_span(&mut continuation, "  ", muted);
        }
        (prefix, continuation)
    }

    fn finish_line(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let current = std::mem::take(&mut self.current);
        let continuation = std::mem::take(&mut self.continuation_prefix);
        self.lines
            .extend(wrap_spans(current, &continuation, self.width));
    }

    fn push_gap(&mut self) {
        self.finish_line();
        if self.lines.last().is_some_and(|line| !line.text.is_empty()) {
            self.lines.push(MarkdownLine {
                text: String::new(),
                spans: Vec::new(),
            });
        }
    }

    fn emit_rule(&mut self) {
        let width = self.width.saturating_sub(4).clamp(1, 72);
        let spans = vec![MarkdownSpan {
            text: format!("  {}", "─".repeat(width)),
            style: MarkdownStyle {
                tone: Some(MarkdownTone::Muted),
                ..MarkdownStyle::default()
            },
        }];
        self.lines.push(line_from_spans(spans));
    }

    fn emit_table_rule(&mut self) {
        if self.table_columns == 0 {
            return;
        }
        let rule = std::iter::repeat_n("───", self.table_columns)
            .collect::<Vec<_>>()
            .join("─┼─");
        self.lines.push(line_from_spans(vec![MarkdownSpan {
            text: format!("  {rule}"),
            style: MarkdownStyle {
                tone: Some(MarkdownTone::Muted),
                ..MarkdownStyle::default()
            },
        }]));
    }

    fn set_task_marker(&mut self, checked: bool) {
        if let Some(item) = self.items.last_mut()
            && !item.marker_used
        {
            if checked { "☑" } else { "☐" }.clone_into(&mut item.marker);
            item.marker_tone = if checked {
                MarkdownTone::Success
            } else {
                MarkdownTone::Muted
            };
        }
    }

    fn finish_link(&mut self) {
        let Some(link) = self.links.pop() else {
            return;
        };
        self.inline_style = link.previous_style;
        let label = link.label.trim();
        if !link.destination.is_empty() && label != link.destination {
            let mut style = self.inline_style;
            style.tone = Some(MarkdownTone::Muted);
            style.modifiers.insert(MarkdownModifier::Underlined);
            self.append(&format!(" ({})", link.destination), style);
        }
    }

    fn finish_code_block(&mut self) {
        let Some(code_block) = self.code_block.take() else {
            return;
        };
        self.finish_line();
        let language_label = code_block.language.as_deref().unwrap_or("code");
        self.lines.push(line_from_spans(vec![MarkdownSpan {
            text: format!("  ┌─ {language_label}"),
            style: MarkdownStyle {
                tone: Some(MarkdownTone::Accent),
                modifiers: modifiers(MarkdownModifier::Bold),
                ..MarkdownStyle::default()
            },
        }]));

        let highlighted = highlight_code(&code_block.source, code_block.language.as_deref());
        if highlighted.is_empty() {
            self.lines.push(line_from_spans(vec![MarkdownSpan {
                text: "  │ ".to_owned(),
                style: MarkdownStyle {
                    tone: Some(MarkdownTone::Muted),
                    code: true,
                    ..MarkdownStyle::default()
                },
            }]));
        } else {
            for spans in highlighted {
                let prefix = vec![MarkdownSpan {
                    text: "  │ ".to_owned(),
                    style: MarkdownStyle {
                        tone: Some(MarkdownTone::Muted),
                        code: true,
                        ..MarkdownStyle::default()
                    },
                }];
                let mut line = prefix.clone();
                line.extend(spans);
                self.lines.extend(wrap_spans(line, &prefix, self.width));
            }
        }
        self.lines.push(line_from_spans(vec![MarkdownSpan {
            text: "  └─".to_owned(),
            style: MarkdownStyle {
                tone: Some(MarkdownTone::Muted),
                ..MarkdownStyle::default()
            },
        }]));
        self.push_gap();
    }

    fn finish(mut self) -> Vec<MarkdownLine> {
        if self.code_block.is_some() {
            self.finish_code_block();
        }
        self.finish_line();
        while self.lines.last().is_some_and(|line| line.text.is_empty()) {
            self.lines.pop();
        }
        self.lines
    }
}

fn normalize_language(info: &str) -> Option<String> {
    info.split(|character: char| character.is_whitespace() || character == ',')
        .find(|token| !token.is_empty())
        .map(str::to_owned)
}

fn highlight_code(source: &str, language: Option<&str>) -> Vec<Vec<MarkdownSpan>> {
    let syntax = language
        .and_then(|language| SYNTAX_SET.find_syntax_by_token(language))
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());
    let mut highlighter = HighlightLines::new(syntax, &SYNTAX_THEME);
    let mut output = Vec::new();
    for line in LinesWithEndings::from(source) {
        let line = line.strip_suffix('\n').unwrap_or(line);
        let ranges = highlighter
            .highlight_line(line, &SYNTAX_SET)
            .unwrap_or_else(|_| Vec::new());
        if ranges.is_empty() {
            output.push(vec![MarkdownSpan {
                text: sanitize_text(line),
                style: MarkdownStyle {
                    code: true,
                    ..MarkdownStyle::default()
                },
            }]);
            continue;
        }
        let spans = ranges
            .into_iter()
            .map(|(style, text)| MarkdownSpan {
                text: sanitize_text(text),
                style: MarkdownStyle {
                    tone: Some(MarkdownTone::Rgb(
                        style.foreground.r,
                        style.foreground.g,
                        style.foreground.b,
                    )),
                    modifiers: syntax_modifiers(style.font_style),
                    code: true,
                },
            })
            .collect();
        output.push(spans);
    }
    output
}

fn wrap_spans(
    spans: Vec<MarkdownSpan>,
    continuation_prefix: &[MarkdownSpan],
    width: usize,
) -> Vec<MarkdownLine> {
    let mut output = Vec::new();
    let mut current = Vec::new();
    let mut current_width = 0_usize;
    for span in spans {
        for character in span.text.chars() {
            let rendered = sanitize_character(character);
            let character_width = character_width(character);
            if current_width > 0 && current_width.saturating_add(character_width) > width {
                output.push(line_from_spans(std::mem::take(&mut current)));
                current.extend_from_slice(continuation_prefix);
                current_width = spans_width(&current);
            }
            if character == '\t' {
                push_span(&mut current, "    ", span.style);
            } else {
                push_span(&mut current, &rendered.to_string(), span.style);
            }
            current_width = current_width.saturating_add(character_width);
        }
    }
    output.push(line_from_spans(current));
    output
}

fn line_from_spans(spans: Vec<MarkdownSpan>) -> MarkdownLine {
    let text = spans.iter().map(|span| span.text.as_str()).collect();
    MarkdownLine { text, spans }
}

fn modifiers(modifier: MarkdownModifier) -> MarkdownModifiers {
    let mut modifiers = MarkdownModifiers::default();
    modifiers.insert(modifier);
    modifiers
}

fn syntax_modifiers(font_style: FontStyle) -> MarkdownModifiers {
    let mut modifiers = MarkdownModifiers::default();
    for (enabled, modifier) in [
        (font_style.contains(FontStyle::BOLD), MarkdownModifier::Bold),
        (
            font_style.contains(FontStyle::ITALIC),
            MarkdownModifier::Italic,
        ),
        (
            font_style.contains(FontStyle::UNDERLINE),
            MarkdownModifier::Underlined,
        ),
    ] {
        if enabled {
            modifiers.insert(modifier);
        }
    }
    modifiers
}

fn push_span(output: &mut Vec<MarkdownSpan>, text: &str, style: MarkdownStyle) {
    if text.is_empty() {
        return;
    }
    if let Some(last) = output.last_mut()
        && last.style == style
    {
        last.text.push_str(text);
        return;
    }
    output.push(MarkdownSpan {
        text: text.to_owned(),
        style,
    });
}

fn sanitize_text(text: &str) -> String {
    text.chars().map(sanitize_character).collect()
}

fn sanitize_character(character: char) -> char {
    if character.is_control() && character != '\t' {
        '�'
    } else {
        character
    }
}

fn character_width(character: char) -> usize {
    if character == '\t' {
        4
    } else {
        UnicodeWidthChar::width(sanitize_character(character)).unwrap_or(0)
    }
}

fn display_width(text: &str) -> usize {
    text.chars().map(character_width).sum()
}

fn spans_width(spans: &[MarkdownSpan]) -> usize {
    spans.iter().map(|span| display_width(&span.text)).sum()
}

#[cfg(test)]
mod tests {
    use super::{MarkdownModifier, MarkdownTone, render_markdown};

    #[test]
    fn renders_headings_inline_styles_lists_and_checkboxes() {
        let lines = render_markdown(
            "# Heading\n\nA **bold** and *italic* [link](https://example.com).\n\n- [x] done\n- [ ] next",
            100,
        );
        let text = lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("▌ Heading"));
        assert!(!text.contains("# Heading"));
        assert!(text.contains("☑ done"));
        assert!(text.contains("☐ next"));
        assert!(text.contains("https://example.com"));
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| {
            span.style.modifiers.contains(MarkdownModifier::Bold) && span.text == "bold"
        }));
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| {
            span.style.modifiers.contains(MarkdownModifier::Italic) && span.text == "italic"
        }));
    }

    #[test]
    fn renders_quotes_ordered_lists_tables_and_strikethrough() {
        let lines = render_markdown(
            "> quoted\n\n1. first\n2. ~~removed~~\n\n| Name | Value |\n| --- | --- |\n| one | two |",
            100,
        );
        let text = lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("│ quoted"));
        assert!(text.contains("1. first"));
        assert!(text.contains("2. removed"));
        assert!(text.contains("Name │ Value"));
        assert!(text.contains("one │ two"));
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| {
            span.text == "removed" && span.style.modifiers.contains(MarkdownModifier::CrossedOut)
        }));
    }

    #[test]
    fn syntax_highlights_fenced_rust_code_without_showing_fences() {
        let lines = render_markdown("```rust\nfn main() { println!(\"hello\"); }\n```", 100);
        let text = lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("┌─ rust"));
        assert!(text.contains("fn main()"));
        assert!(!text.contains("```"));
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| {
            matches!(span.style.tone, Some(MarkdownTone::Rgb(_, _, _)))
                && !span.text.trim().is_empty()
        }));
    }

    #[test]
    fn wraps_styled_content_to_the_available_width() {
        let lines = render_markdown("**abcdefghij**", 8);
        assert!(lines.len() > 1);
        assert!(lines.iter().all(|line| line.text.chars().count() <= 8));
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .filter(|span| span.text.chars().any(char::is_alphabetic))
                .all(|span| span.style.modifiers.contains(MarkdownModifier::Bold))
        );
    }
}
