use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use unicode_width::UnicodeWidthChar;

use crate::{backend::PromptImage, markdown::render_markdown};

const RECENT_TOOL_CALL_LIMIT: usize = 5;
pub(crate) const IMAGE_PREVIEW_ROWS: usize = 8;
pub(crate) const IMAGE_PREVIEW_MARKER: &str = "nakode:image-preview:";
pub(crate) const TOOL_HISTORY_TOGGLE_KEY: &str = "nakode:tool-history-toggle";

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
    AgentPending,
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
    stream_active: bool,
    stream_label: String,
    expanded_tools: HashSet<String>,
    images: HashMap<String, Vec<PromptImage>>,
    image_previews_enabled: bool,
    show_all_tools: bool,
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
            stream_active: false,
            stream_label: "Nakode".to_owned(),
            expanded_tools: HashSet::new(),
            images: HashMap::new(),
            image_previews_enabled: false,
            show_all_tools: false,
        }
    }

    #[must_use]
    pub fn entries(&self) -> &[TranscriptEntry] {
        &self.entries
    }

    #[must_use]
    pub fn image(&self, key: &str, index: usize) -> Option<&PromptImage> {
        self.images.get(key).and_then(|images| images.get(index))
    }

    pub fn set_image_previews_enabled(&mut self, enabled: bool) {
        if self.image_previews_enabled != enabled {
            self.image_previews_enabled = enabled;
            self.changed();
        }
    }

    pub fn set_images(&mut self, key: impl Into<String>, images: Vec<PromptImage>) {
        let key = key.into();
        if images.is_empty() {
            self.images.remove(&key);
        } else {
            self.images.insert(key, images);
        }
        self.changed();
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.item_indices.clear();
        self.expanded_tools.clear();
        self.images.clear();
        self.show_all_tools = false;
        self.stream_active = false;
        self.changed();
    }

    pub fn set_stream_active(&mut self, active: bool) {
        if self.stream_active != active {
            self.stream_active = active;
            self.changed();
        }
    }

    pub fn set_stream_label(&mut self, label: impl Into<String>) {
        let label = label.into();
        if self.stream_label != label {
            self.stream_label = label;
            self.changed();
        }
    }

    pub fn toggle_tool_output(&mut self, key: &str) -> Option<bool> {
        let is_tool = self
            .item_indices
            .get(key)
            .is_some_and(|index| self.entries[*index].kind == EntryKind::Tool);
        if !is_tool {
            return None;
        }
        let expanded = if self.expanded_tools.remove(key) {
            false
        } else {
            self.expanded_tools.insert(key.to_owned());
            true
        };
        self.changed();
        Some(expanded)
    }

    pub fn toggle_tool_history(&mut self) -> Option<bool> {
        let tool_count = self
            .entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::Tool)
            .count();
        if tool_count <= RECENT_TOOL_CALL_LIMIT {
            return None;
        }
        self.show_all_tools = !self.show_all_tools;
        self.changed();
        Some(self.show_all_tools)
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
        self.expanded_tools.remove(key);
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
        if changed || self.stream_active {
            self.stream_active = false;
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
        self.expanded_tools.retain(|key| {
            self.item_indices
                .get(key)
                .is_some_and(|index| self.entries[*index].kind == EntryKind::Tool)
        });
    }

    fn rebuild_cache(&mut self, width: usize) {
        if self.cache_width == width && self.cache_revision == self.revision {
            return;
        }

        let last_user_index = self
            .entries
            .iter()
            .rposition(|entry| entry.kind == EntryKind::User);
        let tool_count = self
            .entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::Tool)
            .count();
        let hidden_tool_count = tool_count.saturating_sub(RECENT_TOOL_CALL_LIMIT);
        let recent_tools_start = (hidden_tool_count > 0).then(|| {
            self.entries
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.kind == EntryKind::Tool)
                .nth(hidden_tool_count)
                .map(|(index, _)| index)
                .expect("a hidden tool count implies a retained tool")
        });
        let mut projected = Vec::new();
        let mut stream_header_shown = false;
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.kind == EntryKind::User {
                stream_header_shown = false;
            }
            let hidden_tool = !self.show_all_tools
                && entry.kind == EntryKind::Tool
                && recent_tools_start.is_some_and(|start| index < start);
            if hidden_tool {
                continue;
            }
            if is_agent_stream(entry) && !stream_header_shown {
                let active =
                    self.stream_active && last_user_index.is_none_or(|last_user| index > last_user);
                project_stream_header(
                    &self.stream_label,
                    active,
                    entry.key.clone(),
                    &mut projected,
                );
                stream_header_shown = true;
            }

            let line_count = projected.len();
            if recent_tools_start == Some(index) {
                project_tool_history_toggle(
                    hidden_tool_count,
                    self.show_all_tools,
                    width,
                    &mut projected,
                );
            }
            let expanded = entry
                .key
                .as_ref()
                .is_some_and(|key| self.expanded_tools.contains(key));
            project_entry(entry, width, expanded, &mut projected);
            project_entry_images(
                (self.image_previews_enabled, &self.images),
                entry,
                &mut projected,
            );
            let next = self.entries.get(index + 1);
            if projected.len() > line_count
                && next.is_some_and(|next| needs_gap_between(entry, next))
                && projected.last().is_some_and(|line| !line.text.is_empty())
            {
                projected.push(blank_line(entry.key.clone()));
            }
        }
        let waiting_after_user = self.entries.last().is_some_and(|entry| {
            entry.kind == EntryKind::User && entry.status == EntryStatus::Complete
        });
        if !stream_header_shown && (self.stream_active || waiting_after_user) {
            if projected.last().is_some_and(|line| !line.text.is_empty()) {
                projected.push(blank_line(
                    self.entries.last().and_then(|entry| entry.key.clone()),
                ));
            }
            project_stream_header(
                &self.stream_label,
                self.stream_active,
                self.entries.last().and_then(|entry| entry.key.clone()),
                &mut projected,
            );
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

fn project_entry_images(
    image_state: (bool, &HashMap<String, Vec<PromptImage>>),
    entry: &TranscriptEntry,
    output: &mut Vec<ProjectedLine>,
) {
    let (enabled, images) = image_state;
    if enabled
        && entry.kind == EntryKind::User
        && let Some(key) = entry.key.as_deref()
        && let Some(images) = images.get(key)
    {
        project_image_previews(key, images.len(), output);
    }
}

fn project_image_previews(key: &str, image_count: usize, output: &mut Vec<ProjectedLine>) {
    for index in 0..image_count {
        output.push(ProjectedLine {
            text: format!("{IMAGE_PREVIEW_MARKER}{index}"),
            spans: Vec::new(),
            tone: LineTone::Body,
            bold: false,
            source_key: Some(key.to_owned()),
        });
        for _ in 1..IMAGE_PREVIEW_ROWS {
            output.push(blank_line(Some(key.to_owned())));
        }
    }
}

fn project_tool_history_toggle(
    hidden_count: usize,
    expanded: bool,
    width: usize,
    output: &mut Vec<ProjectedLine>,
) {
    let label = if expanded {
        format!("▾ all tool calls shown · click to show latest {RECENT_TOOL_CALL_LIMIT}")
    } else {
        let noun = if hidden_count == 1 { "call" } else { "calls" };
        format!("▸ {hidden_count} earlier tool {noun} hidden · click to show all")
    };
    output.push(ProjectedLine {
        text: truncate_display(&label, width),
        spans: Vec::new(),
        tone: LineTone::Muted,
        bold: false,
        source_key: Some(TOOL_HISTORY_TOGGLE_KEY.to_owned()),
    });
}

fn project_stream_header(
    label: &str,
    active: bool,
    source_key: Option<String>,
    output: &mut Vec<ProjectedLine>,
) {
    output.push(ProjectedLine {
        text: if active {
            format!("⠋ {label}")
        } else {
            label.to_owned()
        },
        spans: Vec::new(),
        tone: if active {
            LineTone::AgentPending
        } else {
            LineTone::Assistant
        },
        bold: true,
        source_key,
    });
}

fn is_agent_stream(entry: &TranscriptEntry) -> bool {
    !is_subagent(entry)
        && matches!(
            entry.kind,
            EntryKind::Assistant
                | EntryKind::Reasoning
                | EntryKind::Tool
                | EntryKind::Diff
                | EntryKind::Warning
                | EntryKind::Error
        )
}

fn project_entry(
    entry: &TranscriptEntry,
    width: usize,
    expanded: bool,
    output: &mut Vec<ProjectedLine>,
) {
    if is_subagent(entry) {
        project_subagent(entry, width, output);
        return;
    }
    project_header(entry, width, expanded, output);
    project_body(entry, width, expanded, output);
}

fn is_subagent(entry: &TranscriptEntry) -> bool {
    entry
        .key
        .as_deref()
        .is_some_and(|key| key.starts_with("subagent:"))
}

fn project_subagent(entry: &TranscriptEntry, width: usize, output: &mut Vec<ProjectedLine>) {
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
}

fn project_header(
    entry: &TranscriptEntry,
    width: usize,
    expanded: bool,
    output: &mut Vec<ProjectedLine>,
) {
    let status = status_suffix(entry.status);
    let header = match entry.kind {
        EntryKind::User => Some((
            format!("{}{status}", user_label(entry)),
            LineTone::User,
            true,
        )),
        EntryKind::Assistant | EntryKind::Reasoning => {
            if matches!(entry.status, EntryStatus::Failed | EntryStatus::Interrupted) {
                Some((format!("  · response{status}"), LineTone::Error, false))
            } else {
                None
            }
        }
        EntryKind::Tool => Some(tool_header(entry, status, expanded)),
        _ => Some((
            format!("{}{}", entry.title, status),
            header_tone(entry.kind),
            true,
        )),
    };
    let Some((text, tone, bold)) = header else {
        return;
    };
    output.push(ProjectedLine {
        text: truncate_display(&text, width),
        spans: Vec::new(),
        tone,
        bold,
        source_key: entry.key.clone(),
    });
}

fn user_label(entry: &TranscriptEntry) -> &'static str {
    if entry
        .title
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("parent"))
    {
        "Parent"
    } else {
        "User"
    }
}

fn tool_header(entry: &TranscriptEntry, status: &str, expanded: bool) -> (String, LineTone, bool) {
    let title = entry.title.trim();
    let normalized = title.to_ascii_lowercase();
    let generic = normalized == "tool output"
        || normalized == "tool result"
        || normalized.starts_with("tool result · call_");
    let label = if generic || title.is_empty() {
        "tool".to_owned()
    } else {
        title.replace(" · ", " ")
    };
    let icon = if entry.status == EntryStatus::Running {
        "⠋"
    } else if entry.key.is_none() {
        "•"
    } else if expanded {
        "▾"
    } else {
        "▸"
    };
    (format!("{icon} {label}{status}"), LineTone::Tool, false)
}

const fn status_suffix(status: EntryStatus) -> &'static str {
    match status {
        EntryStatus::Running => " · running",
        EntryStatus::Failed => " · failed",
        EntryStatus::Interrupted => " · interrupted",
        EntryStatus::Complete => "",
    }
}

fn project_body(
    entry: &TranscriptEntry,
    width: usize,
    expanded: bool,
    output: &mut Vec<ProjectedLine>,
) {
    let body_width = width.saturating_sub(2).max(1);
    if entry.kind == EntryKind::Tool && !expanded {
        return;
    }
    if entry.body.is_empty() {
        if entry.status == EntryStatus::Running {
            output.push(ProjectedLine {
                text: "  …".to_owned(),
                spans: Vec::new(),
                tone: LineTone::Muted,
                bold: false,
                source_key: entry.key.clone(),
            });
        }
    } else if matches!(entry.kind, EntryKind::Assistant | EntryKind::Reasoning) {
        let tone = if entry.kind == EntryKind::Reasoning {
            LineTone::Reasoning
        } else {
            LineTone::Body
        };
        for line in render_markdown(&markdown_source(entry), width) {
            output.push(ProjectedLine {
                text: line.text,
                spans: line.spans,
                tone,
                bold: false,
                source_key: entry.key.clone(),
            });
        }
    } else {
        let body = if entry.kind == EntryKind::Tool {
            expanded_tool_output(entry)
        } else {
            Cow::Borrowed(entry.body.as_str())
        };
        project_plain_body(entry, &body, body_width, output);
    }
}

fn expanded_tool_output(entry: &TranscriptEntry) -> Cow<'_, str> {
    if entry.key.is_some() {
        Cow::Owned(format!(
            "{}\n… full output · click to collapse",
            entry.body.trim_end_matches('\n')
        ))
    } else {
        Cow::Borrowed(&entry.body)
    }
}

pub(crate) fn is_tool_toggle_marker(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with('▸')
        || line.starts_with('▾')
        || (line.starts_with('…') && line.ends_with("click to collapse"))
}

fn project_plain_body(
    entry: &TranscriptEntry,
    body: &str,
    body_width: usize,
    output: &mut Vec<ProjectedLine>,
) {
    let mut in_code_block = false;
    for raw_line in body.trim_end_matches('\n').split('\n') {
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

fn needs_gap_between(current: &TranscriptEntry, next: &TranscriptEntry) -> bool {
    if is_subagent(current) || is_subagent(next) {
        return false;
    }
    let is_activity = |entry: &TranscriptEntry| is_agent_stream(entry);
    !is_activity(current) || !is_activity(next)
}

fn blank_line(source_key: Option<String>) -> ProjectedLine {
    ProjectedLine {
        text: String::new(),
        spans: Vec::new(),
        tone: LineTone::Body,
        bold: false,
        source_key,
    }
}

fn markdown_source(entry: &TranscriptEntry) -> Cow<'_, str> {
    if entry.kind == EntryKind::Reasoning && entry.body.contains("****") {
        Cow::Owned(entry.body.replace("****", "**\n**"))
    } else {
        Cow::Borrowed(&entry.body)
    }
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
    if is_tool_toggle_marker(line) {
        LineTone::Muted
    } else if kind == EntryKind::Diff || line.starts_with("diff --git") || line.starts_with("@@") {
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
    use super::{
        EntryKind, EntryStatus, IMAGE_PREVIEW_MARKER, IMAGE_PREVIEW_ROWS, LineTone,
        MarkdownModifier, MarkdownTone, TOOL_HISTORY_TOGGLE_KEY, Transcript,
    };

    #[test]
    fn attached_images_reserve_stable_transcript_rows() {
        let mut transcript = Transcript::new(100);
        transcript.set_image_previews_enabled(true);
        transcript.upsert(
            "user:one",
            EntryKind::User,
            "YOU",
            "[Image]",
            EntryStatus::Complete,
        );
        transcript.set_images(
            "user:one",
            vec![crate::backend::PromptImage {
                mime_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            }],
        );

        let visible = transcript.visible(80, 30, 0);
        let marker = visible
            .lines
            .iter()
            .position(|line| line.text == format!("{IMAGE_PREVIEW_MARKER}0"))
            .expect("image marker");
        assert!(visible.lines.len() >= marker + IMAGE_PREVIEW_ROWS);
        assert_eq!(transcript.image("user:one", 0).unwrap().data, [1, 2, 3]);
    }

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
    fn agent_activity_projects_as_one_compact_stream() {
        let mut transcript = Transcript::new(100);
        transcript.push(
            EntryKind::User,
            "You",
            "Commit the changes.",
            EntryStatus::Complete,
        );
        transcript.push(
            EntryKind::Reasoning,
            "Reasoning",
            "**Planning the commit**",
            EntryStatus::Complete,
        );
        transcript.upsert(
            "tool-generic",
            EntryKind::Tool,
            "Tool result · call_opaque",
            "tree-object\n",
            EntryStatus::Complete,
        );
        transcript.upsert(
            "tool-commit",
            EntryKind::Tool,
            "bash · git commit",
            "[main abc123] commit\n",
            EntryStatus::Complete,
        );
        transcript.push(
            EntryKind::Reasoning,
            "Reasoning",
            "**Verifying status**",
            EntryStatus::Complete,
        );
        transcript.push(
            EntryKind::Assistant,
            "Assistant",
            "Committed successfully.",
            EntryStatus::Complete,
        );

        let visible = transcript.visible(100, 30, 0);
        let text = visible
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>();
        assert!(text.contains(&"User"));
        assert_eq!(text.iter().filter(|line| **line == "Nakode").count(), 1);
        assert!(!text.contains(&"Reasoning"));
        assert!(!text.contains(&"Assistant"));
        assert!(!text.iter().any(|line| line.contains("call_opaque")));
        assert!(text.contains(&"▸ bash git commit"));

        let activity_start = text
            .iter()
            .position(|line| line.contains("Planning the commit"))
            .expect("reasoning starts the activity stream");
        let activity_end = text
            .iter()
            .position(|line| line.contains("Verifying status"))
            .expect("reasoning ends the activity stream");
        assert!(
            text[activity_start..=activity_end]
                .iter()
                .all(|line| !line.is_empty())
        );
        assert_eq!(text.last(), Some(&"  Committed successfully."));
    }

    #[test]
    fn only_the_five_most_recent_tool_calls_are_shown_until_expanded() {
        let mut transcript = Transcript::new(100);
        for index in 1..=7 {
            transcript.upsert(
                format!("tool-{index}"),
                EntryKind::Tool,
                format!("bash · command {index}"),
                format!("output {index}"),
                EntryStatus::Complete,
            );
        }

        let collapsed = transcript.visible(100, 30, 0);
        let collapsed_text = collapsed
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(collapsed_text.contains("2 earlier tool calls hidden · click to show all"));
        assert!(!collapsed_text.contains("command 1"));
        assert!(!collapsed_text.contains("command 2"));
        for index in 3..=7 {
            assert!(collapsed_text.contains(&format!("command {index}")));
        }

        assert_eq!(transcript.toggle_tool_history(), Some(true));
        let expanded = transcript.visible(100, 30, 0);
        let expanded_text = expanded
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(expanded_text.contains("all tool calls shown · click to show latest 5"));
        for index in 1..=7 {
            assert!(expanded_text.contains(&format!("command {index}")));
        }
        assert!(
            expanded
                .lines
                .iter()
                .any(|line| { line.source_key.as_deref() == Some(TOOL_HISTORY_TOGGLE_KEY) })
        );

        assert_eq!(transcript.toggle_tool_history(), Some(false));
        let collapsed_again = transcript.visible(100, 30, 0);
        assert!(
            collapsed_again
                .lines
                .iter()
                .all(|line| !line.text.contains("command 1"))
        );
    }

    #[test]
    fn tool_output_is_hidden_until_expanded() {
        let mut transcript = Transcript::new(100);
        let output = std::iter::once("running 159 tests".to_owned())
            .chain((0..159).map(|index| format!("test suite::case_{index} ... ok")))
            .chain(std::iter::once(
                "test result: ok. 159 passed; 0 failed".to_owned(),
            ))
            .collect::<Vec<_>>()
            .join("\n");
        transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "bash · cargo test",
            &output,
            EntryStatus::Complete,
        );

        let collapsed = transcript.visible(100, 30, 0);
        let collapsed_text = collapsed
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(collapsed_text.contains("▸ bash cargo test"));
        assert!(!collapsed_text.contains("running 159 tests"));
        assert!(!collapsed_text.contains("test result: ok. 159 passed; 0 failed"));
        assert!(!collapsed_text.contains("case_80"));
        assert_eq!(transcript.entries()[0].body, output);

        assert_eq!(transcript.toggle_tool_output("tool-1"), Some(true));
        let expanded = transcript.visible(100, 300, 0);
        let expanded_text = expanded
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(expanded_text.contains("case_80"));
        assert!(expanded_text.contains("full output · click to collapse"));
        assert_eq!(transcript.toggle_tool_output("tool-1"), Some(false));
    }

    #[test]
    fn short_tool_output_is_also_collapsed_by_default() {
        let mut transcript = Transcript::new(100);
        transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "bash · git status",
            "clean\nready",
            EntryStatus::Complete,
        );

        let visible = transcript.visible(80, 20, 0);
        let text = visible
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("▸ bash git status"));
        assert!(!text.contains("clean"));
        assert!(!text.contains("ready"));

        assert_eq!(transcript.toggle_tool_output("tool-1"), Some(true));
        let expanded = transcript.visible(80, 20, 0);
        let expanded_text = expanded
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(expanded_text.contains("clean\n  ready"));
        assert!(expanded_text.contains("click to collapse"));
    }

    #[test]
    fn failed_tool_output_is_collapsed_but_status_remains_visible() {
        let mut transcript = Transcript::new(100);
        let output = (0..40)
            .map(|index| {
                if index == 39 {
                    "fatal: final actionable error".to_owned()
                } else {
                    format!("diagnostic line {index}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "bash · cargo test",
            output,
            EntryStatus::Failed,
        );

        let visible = transcript.visible(100, 30, 0);
        let text = visible
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("▸ bash cargo test · failed"));
        assert!(!text.contains("fatal: final actionable error"));

        assert_eq!(transcript.toggle_tool_output("tool-1"), Some(true));
        let expanded = transcript.visible(100, 60, 0);
        let expanded_text = expanded
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(expanded_text.contains("fatal: final actionable error"));
    }

    #[test]
    fn active_turn_shows_one_nakode_spinner_before_items_arrive() {
        let mut transcript = Transcript::new(100);
        transcript.push(
            EntryKind::User,
            "YOU · msg-1",
            "Investigate the issue.",
            EntryStatus::Complete,
        );
        transcript.set_stream_active(true);

        let waiting = transcript.visible(80, 10, 0);
        assert_eq!(
            waiting
                .lines
                .iter()
                .filter(|line| line.tone == LineTone::AgentPending)
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            vec!["⠋ Nakode"]
        );

        transcript.append_delta(
            "reasoning-1",
            EntryKind::Reasoning,
            "Reasoning",
            "**Checking the code**",
        );
        let streaming = transcript.visible(80, 10, 0);
        assert_eq!(
            streaming
                .lines
                .iter()
                .filter(|line| line.text.contains("Nakode"))
                .count(),
            1
        );

        transcript.set_stream_active(false);
        transcript.set_status("reasoning-1", EntryStatus::Complete);
        let complete = transcript.visible(80, 10, 0);
        assert!(
            complete
                .lines
                .iter()
                .any(|line| line.text == "Nakode" && line.tone == LineTone::Assistant)
        );
        assert!(
            !complete
                .lines
                .iter()
                .any(|line| line.tone == LineTone::AgentPending)
        );
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
    fn reasoning_markdown_is_styled_and_adjacent_summaries_are_separated() {
        let mut transcript = Transcript::new(100);
        transcript.push(
            EntryKind::Reasoning,
            "REASONING",
            "**Verifying cleanliness****Planning commit creation****Validating result**",
            EntryStatus::Complete,
        );

        let visible = transcript.visible(80, 20, 0);
        let reasoning = visible
            .lines
            .iter()
            .filter(|line| line.tone == LineTone::Reasoning && !line.spans.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(reasoning.len(), 3);
        assert!(reasoning.iter().all(|line| !line.text.contains("**")));
        assert!(reasoning.iter().all(|line| {
            line.spans
                .iter()
                .any(|span| span.style.modifiers.contains(MarkdownModifier::Bold))
        }));
    }

    #[test]
    fn control_sequences_are_rendered_as_data() {
        let mut transcript = Transcript::new(100);
        transcript.upsert(
            "tool-1",
            EntryKind::Tool,
            "tool",
            "ok\u{1b}[31m",
            EntryStatus::Complete,
        );

        assert_eq!(transcript.toggle_tool_output("tool-1"), Some(true));
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
