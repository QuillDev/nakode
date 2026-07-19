use std::collections::HashMap;

use unicode_width::UnicodeWidthChar;

use crate::markdown::render_markdown;
pub use crate::markdown::{
    MarkdownModifier, MarkdownModifiers, MarkdownSpan, MarkdownStyle, MarkdownTone,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    System,
    User,
    Assistant,
    Steering,
    Reasoning,
    Tool,
    Diff,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryStatus {
    Running,
    Complete,
    Failed,
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptEntry {
    pub key: Option<String>,
    pub kind: EntryKind,
    pub title: String,
    pub body: String,
    pub status: EntryStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineTone {
    Muted,
    User,
    Assistant,
    Steering,
    Reasoning,
    Tool,
    DiffAdd,
    DiffRemove,
    DiffHeader,
    Warning,
    Error,
    Body,
    Code,
    SubagentPending,
    SubagentComplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedLine {
    pub text: String,
    pub spans: Vec<MarkdownSpan>,
    pub tone: LineTone,
    pub bold: bool,
    pub source_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleTranscript {
    pub lines: Vec<ProjectedLine>,
    pub total_lines: usize,
    pub first_line: usize,
}

#[derive(Debug)]
pub struct Transcript {
    entries: Vec<TranscriptEntry>,
    item_indices: HashMap<String, usize>,
    limit: usize,
    revision: u64,
    cache_width: usize,
    cache_revision: u64,
    cache: Vec<ProjectedLine>,
}

impl Transcript {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            item_indices: HashMap::new(),
            limit: limit.max(100),
            revision: 1,
            cache_width: 0,
            cache_revision: 0,
            cache: Vec::new(),
        }
    }

    #[must_use]
    pub fn entries(&self) -> &[TranscriptEntry] {
        &self.entries
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.item_indices.clear();
        self.changed();
    }

    pub fn push(
        &mut self,
        kind: EntryKind,
        title: impl Into<String>,
        body: impl Into<String>,
        status: EntryStatus,
    ) {
        self.entries.push(TranscriptEntry {
            key: None,
            kind,
            title: title.into(),
            body: body.into(),
            status,
        });
        self.changed();
    }

    pub fn upsert(
        &mut self,
        key: impl Into<String>,
        kind: EntryKind,
        title: impl Into<String>,
        body: impl Into<String>,
        status: EntryStatus,
    ) {
        let key = key.into();
        if let Some(index) = self.item_indices.get(&key).copied() {
            let entry = &mut self.entries[index];
            entry.kind = kind;
            entry.title = title.into();
            entry.body = body.into();
            entry.status = status;
        } else {
            let index = self.entries.len();
            self.entries.push(TranscriptEntry {
                key: Some(key.clone()),
                kind,
                title: title.into(),
                body: body.into(),
                status,
            });
            self.item_indices.insert(key, index);
        }
        self.changed();
    }

    pub fn append_delta(
        &mut self,
        key: impl Into<String>,
        kind: EntryKind,
        title: impl Into<String>,
        delta: &str,
    ) {
        let key = key.into();
        if let Some(index) = self.item_indices.get(&key).copied() {
            let entry = &mut self.entries[index];
            entry.body.push_str(delta);
            entry.status = EntryStatus::Running;
        } else {
            let index = self.entries.len();
            self.entries.push(TranscriptEntry {
                key: Some(key.clone()),
                kind,
                title: title.into(),
                body: delta.to_owned(),
                status: EntryStatus::Running,
            });
            self.item_indices.insert(key, index);
        }
        self.changed();
    }

    pub fn set_status(&mut self, key: &str, status: EntryStatus) {
        if let Some(index) = self.item_indices.get(key).copied() {
            self.entries[index].status = status;
            self.changed();
        }
    }

    pub fn remove(&mut self, key: &str) {
        let Some(index) = self.item_indices.get(key).copied() else {
            return;
        };
        self.entries.remove(index);
        self.reindex();
        self.changed();
    }

    pub fn finish_running(&mut self, status: EntryStatus) {
        let mut changed = false;
        for entry in &mut self.entries {
            if entry.status == EntryStatus::Running {
                entry.status = status;
                changed = true;
            }
        }
        if changed {
            self.changed();
        }
    }

    pub fn visible(
        &mut self,
        width: usize,
        height: usize,
        scroll_from_bottom: usize,
    ) -> VisibleTranscript {
        self.rebuild_cache(width.max(1));
        let total_lines = self.cache.len();
        let first_line = total_lines.saturating_sub(height.saturating_add(scroll_from_bottom));
        let end = (first_line + height).min(total_lines);

        VisibleTranscript {
            lines: self.cache[first_line..end].to_vec(),
            total_lines,
            first_line,
        }
    }

    pub fn max_scroll(&mut self, width: usize, height: usize) -> usize {
        self.rebuild_cache(width.max(1));
        self.cache.len().saturating_sub(height)
    }

    fn changed(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        if self.entries.len() > self.limit {
            let remove_count = self.entries.len() - self.limit;
            self.entries.drain(..remove_count);
            self.reindex();
        }
    }

    fn reindex(&mut self) {
        self.item_indices.clear();
        for (index, entry) in self.entries.iter().enumerate() {
            if let Some(key) = &entry.key {
                self.item_indices.insert(key.clone(), index);
            }
        }
    }

    fn rebuild_cache(&mut self, width: usize) {
        if self.cache_width == width && self.cache_revision == self.revision {
            return;
        }

        let mut projected = Vec::new();
        for entry in &self.entries {
            project_entry(entry, width, &mut projected);
        }
        if projected.is_empty() {
            projected.push(ProjectedLine {
                text: "Start by typing a request below.".to_owned(),
                spans: Vec::new(),
                tone: LineTone::Muted,
                bold: false,
                source_key: None,
            });
        }

        self.cache = projected;
        self.cache_width = width;
        self.cache_revision = self.revision;
    }
}

fn project_entry(entry: &TranscriptEntry, width: usize, output: &mut Vec<ProjectedLine>) {
    if entry
        .key
        .as_deref()
        .is_some_and(|key| key.starts_with("subagent:"))
    {
        let pending = entry.status == EntryStatus::Running;
        let status = if pending {
            "⠋ pending"
        } else {
            "✓ completed"
        };
        let objective_width = width.saturating_sub(14);
        let objective = entry.body.lines().next().unwrap_or_default().trim();
        output.push(ProjectedLine {
            text: format!(
                " {status:<11} {}",
                truncate_display(objective, objective_width)
            ),
            spans: Vec::new(),
            tone: if pending {
                LineTone::SubagentPending
            } else {
                LineTone::SubagentComplete
            },
            bold: false,
            source_key: entry.key.clone(),
        });
        return;
    }
    let status = match entry.status {
        EntryStatus::Running => "  · running",
        EntryStatus::Failed => "  · failed",
        EntryStatus::Interrupted => "  · interrupted",
        EntryStatus::Complete => "",
    };
    let header = format!("{}{}", entry.title, status);
    output.push(ProjectedLine {
        text: truncate_display(&header, width),
        spans: Vec::new(),
        tone: header_tone(entry.kind),
        bold: true,
        source_key: entry.key.clone(),
    });

    let body_width = width.saturating_sub(2).max(1);
    if entry.body.is_empty() && entry.status == EntryStatus::Running {
        output.push(ProjectedLine {
            text: "  …".to_owned(),
            spans: Vec::new(),
            tone: LineTone::Muted,
            bold: false,
            source_key: entry.key.clone(),
        });
    } else if entry.kind == EntryKind::Assistant {
        for line in render_markdown(&entry.body, width) {
            output.push(ProjectedLine {
                text: line.text,
                spans: line.spans,
                tone: LineTone::Body,
                bold: false,
                source_key: entry.key.clone(),
            });
        }
    } else {
        let mut in_code_block = false;
        for raw_line in entry.body.split('\n') {
            let trimmed = raw_line.trim_start();
            if trimmed.starts_with("```") {
                in_code_block = !in_code_block;
            }
            let tone = body_tone(entry.kind, trimmed, in_code_block);
            let wrapped = wrap_display(raw_line, body_width);
            for line in wrapped {
                output.push(ProjectedLine {
                    text: format!("  {line}"),
                    spans: Vec::new(),
                    tone,
                    bold: trimmed.starts_with('#'),
                    source_key: entry.key.clone(),
                });
            }
        }
    }

    output.push(ProjectedLine {
        text: String::new(),
        spans: Vec::new(),
        tone: LineTone::Body,
        bold: false,
        source_key: entry.key.clone(),
    });
}

fn header_tone(kind: EntryKind) -> LineTone {
    match kind {
        EntryKind::System => LineTone::Muted,
        EntryKind::User => LineTone::User,
        EntryKind::Assistant => LineTone::Assistant,
        EntryKind::Steering => LineTone::Steering,
        EntryKind::Reasoning => LineTone::Reasoning,
        EntryKind::Tool => LineTone::Tool,
        EntryKind::Diff => LineTone::DiffHeader,
        EntryKind::Warning => LineTone::Warning,
        EntryKind::Error => LineTone::Error,
    }
}

fn body_tone(kind: EntryKind, line: &str, in_code_block: bool) -> LineTone {
    if kind == EntryKind::Diff || line.starts_with("diff --git") || line.starts_with("@@") {
        if line.starts_with('+') && !line.starts_with("+++") {
            LineTone::DiffAdd
        } else if line.starts_with('-') && !line.starts_with("---") {
            LineTone::DiffRemove
        } else {
            LineTone::DiffHeader
        }
    } else if in_code_block || line.starts_with("```") || line.starts_with("    ") {
        LineTone::Code
    } else {
        match kind {
            EntryKind::Warning => LineTone::Warning,
            EntryKind::Error => LineTone::Error,
            _ => LineTone::Body,
        }
    }
}

fn wrap_display(text: &str, width: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }

    let mut lines = Vec::new();
    let mut current = String::new();
    let mut current_width = 0;

    for character in text.chars() {
        let rendered = sanitize_character(character);
        let character_width = display_width(character);
        if current_width > 0 && current_width + character_width > width {
            lines.push(current);
            current = String::new();
            current_width = 0;
        }
        if character == '\t' {
            current.push_str("    ");
        } else {
            current.push(rendered);
        }
        current_width += character_width;
    }
    lines.push(current);
    lines
}

fn truncate_display(text: &str, width: usize) -> String {
    wrap_display(text, width)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn sanitize_character(character: char) -> char {
    if character.is_control() && character != '\t' {
        '�'
    } else {
        character
    }
}

fn display_width(character: char) -> usize {
    if character == '\t' {
        4
    } else {
        UnicodeWidthChar::width(sanitize_character(character)).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryKind, EntryStatus, LineTone, MarkdownModifier, MarkdownTone, Transcript};

    #[test]
    fn deltas_are_keyed_and_completion_replaces_stream() {
        let mut transcript = Transcript::new(100);
        transcript.append_delta("item-1", EntryKind::Assistant, "ASSISTANT", "hel");
        transcript.append_delta("item-1", EntryKind::Assistant, "ASSISTANT", "lo");
        transcript.upsert(
            "item-1",
            EntryKind::Assistant,
            "ASSISTANT",
            "hello",
            EntryStatus::Complete,
        );

        assert_eq!(transcript.entries().len(), 1);
        assert_eq!(transcript.entries()[0].body, "hello");
        assert_eq!(transcript.entries()[0].status, EntryStatus::Complete);
    }

    #[test]
    fn visible_slice_is_anchored_from_bottom() {
        let mut transcript = Transcript::new(100);
        for index in 0..5 {
            transcript.push(
                EntryKind::System,
                format!("event {index}"),
                "body",
                EntryStatus::Complete,
            );
        }

        let bottom = transcript.visible(80, 3, 0);
        let older = transcript.visible(80, 3, 2);
        assert!(bottom.first_line > older.first_line);
        assert_eq!(bottom.lines.len(), 3);
    }

    #[test]
    fn assistant_markdown_is_projected_as_styled_content() {
        let mut transcript = Transcript::new(100);
        transcript.push(
            EntryKind::Assistant,
            "ASSISTANT",
            "## Result\n\nUse **bold** and `code`.",
            EntryStatus::Complete,
        );

        let visible = transcript.visible(80, 20, 0);
        let body = visible
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(body.contains("▍ Result"));
        assert!(!body.contains("## Result"));
        assert!(!body.contains("**bold**"));
        assert!(visible.lines.iter().flat_map(|line| &line.spans).any(
            |span| span.text == "bold" && span.style.modifiers.contains(MarkdownModifier::Bold)
        ));
        assert!(
            visible
                .lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text == "code"
                    && span.style.code
                    && span.style.tone == Some(MarkdownTone::Warning))
        );
    }

    #[test]
    fn control_sequences_are_rendered_as_data() {
        let mut transcript = Transcript::new(100);
        transcript.push(
            EntryKind::Tool,
            "tool",
            "ok\u{1b}[31m",
            EntryStatus::Complete,
        );

        let visible = transcript.visible(80, 10, 0);
        assert!(visible.lines.iter().any(|line| line.text.contains("�[31m")));
    }

    #[test]
    fn subagent_entries_project_as_one_clickable_inline_row() {
        let mut transcript = Transcript::new(100);
        transcript.upsert(
            "subagent:agent-1",
            EntryKind::System,
            "pending",
            "Map authentication",
            EntryStatus::Running,
        );

        let pending = transcript.visible(80, 10, 0);
        assert_eq!(pending.lines.len(), 1);
        assert_eq!(pending.lines[0].tone, LineTone::SubagentPending);
        assert_eq!(
            pending.lines[0].source_key.as_deref(),
            Some("subagent:agent-1")
        );
        assert!(pending.lines[0].text.contains("Map authentication"));

        transcript.upsert(
            "subagent:agent-1",
            EntryKind::System,
            "completed",
            "Map authentication",
            EntryStatus::Complete,
        );
        let completed = transcript.visible(80, 10, 0);
        assert_eq!(completed.lines.len(), 1);
        assert_eq!(completed.lines[0].tone, LineTone::SubagentComplete);
        assert!(completed.lines[0].text.contains("completed"));
    }
}
