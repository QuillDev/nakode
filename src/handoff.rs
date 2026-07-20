use std::fmt::Write as _;

use crate::transcript::{EntryKind, TranscriptEntry};

const MAX_HANDOFF_CHARACTERS: usize = 48_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandoffRole {
    User,
    Assistant,
}

impl HandoffRole {
    const fn label(self) -> &'static str {
        match self {
            Self::User => "USER",
            Self::Assistant => "ASSISTANT",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HandoffMessage {
    role: HandoffRole,
    body: String,
}

/// Visible, provider-neutral context transferred between native agent sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffPackage {
    source_provider: String,
    source_model: Option<String>,
    source_session: Option<String>,
    target_provider: String,
    messages: Vec<HandoffMessage>,
    omitted_messages: usize,
}

impl HandoffPackage {
    #[must_use]
    pub fn from_transcript(
        source_provider: impl Into<String>,
        source_model: Option<String>,
        source_session: Option<String>,
        target_provider: impl Into<String>,
        entries: &[TranscriptEntry],
    ) -> Option<Self> {
        let mut messages = entries
            .iter()
            .filter_map(|entry| {
                let role = match entry.kind {
                    EntryKind::User | EntryKind::Steering => HandoffRole::User,
                    EntryKind::Assistant => HandoffRole::Assistant,
                    EntryKind::System
                    | EntryKind::Reasoning
                    | EntryKind::Tool
                    | EntryKind::Diff
                    | EntryKind::Warning
                    | EntryKind::Error => return None,
                };
                (!entry.body.trim().is_empty()).then(|| HandoffMessage {
                    role,
                    body: entry.body.clone(),
                })
            })
            .collect::<Vec<_>>();
        if messages.is_empty() {
            return None;
        }

        let original_count = messages.len();
        let mut characters: usize = messages.iter().map(|message| message.body.len()).sum();
        while messages.len() > 1 && characters > MAX_HANDOFF_CHARACTERS {
            characters = characters.saturating_sub(messages[0].body.len());
            messages.remove(0);
        }
        if let Some(message) = messages.first_mut()
            && message.body.len() > MAX_HANDOFF_CHARACTERS
        {
            let mut split = message.body.len() - MAX_HANDOFF_CHARACTERS;
            while !message.body.is_char_boundary(split) {
                split += 1;
            }
            message.body = format!("[earlier content omitted]\n{}", &message.body[split..]);
        }

        Some(Self {
            source_provider: source_provider.into(),
            source_model,
            source_session,
            target_provider: target_provider.into(),
            omitted_messages: original_count - messages.len(),
            messages,
        })
    }

    #[must_use]
    pub fn render_with_prompt(&self, prompt: &str) -> String {
        let mut rendered = String::new();
        rendered.push_str("# Nakode continuity handoff\n\n");
        rendered.push_str(
            "You are continuing work from another provider-native agent session. Use the prior \
             visible dialogue below as conversation context. The source provider's hidden context \
             and tool state were not transferred. Do not claim otherwise.\n\n",
        );
        let _ = writeln!(rendered, "Source provider: {}", self.source_provider);
        let _ = writeln!(rendered, "Target provider: {}", self.target_provider);
        if let Some(model) = &self.source_model {
            let _ = writeln!(rendered, "Source model: {model}");
        }
        if let Some(session) = &self.source_session {
            let _ = writeln!(rendered, "Source native session: {session}");
        }
        if self.omitted_messages > 0 {
            let _ = writeln!(
                rendered,
                "Earlier dialogue entries omitted to fit the handoff: {}",
                self.omitted_messages
            );
        }
        rendered.push_str("\n## Prior visible dialogue\n");
        for (index, message) in self.messages.iter().enumerate() {
            let number = index + 1;
            let _ = write!(
                rendered,
                "\n### {} {number}\n{}\n",
                message.role.label(),
                message.body
            );
        }
        rendered.push_str("\n## Current user message\n\n");
        rendered.push_str(prompt);
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::{HandoffPackage, MAX_HANDOFF_CHARACTERS};
    use crate::transcript::{EntryKind, EntryStatus, TranscriptEntry};

    fn entry(kind: EntryKind, body: impl Into<String>) -> TranscriptEntry {
        TranscriptEntry {
            key: None,
            kind,
            title: String::new(),
            body: body.into(),
            status: EntryStatus::Complete,
        }
    }

    #[test]
    fn renders_dialogue_and_current_prompt_without_internal_artifacts() {
        let entries = vec![
            entry(EntryKind::User, "My name is Quill."),
            entry(EntryKind::Reasoning, "private reasoning"),
            entry(EntryKind::Tool, "tool output"),
            entry(EntryKind::Assistant, "Nice to meet you."),
        ];
        let package = HandoffPackage::from_transcript(
            "openai-codex",
            Some("openai-codex/gpt-5".to_owned()),
            Some("thread-1".to_owned()),
            "devin-acp",
            &entries,
        )
        .expect("dialogue should produce a handoff");

        let rendered = package.render_with_prompt("What is my name?");

        assert!(rendered.contains("My name is Quill."));
        assert!(rendered.contains("Nice to meet you."));
        assert!(rendered.contains("What is my name?"));
        assert!(!rendered.contains("private reasoning"));
        assert!(!rendered.contains("tool output"));
    }

    #[test]
    fn retains_the_tail_of_an_oversized_dialogue() {
        let entries = vec![
            entry(EntryKind::User, "old".repeat(MAX_HANDOFF_CHARACTERS)),
            entry(EntryKind::Assistant, "most recent answer"),
        ];
        let package = HandoffPackage::from_transcript("source", None, None, "target", &entries)
            .expect("dialogue should produce a handoff");
        let rendered = package.render_with_prompt("continue");

        assert!(rendered.contains("most recent answer"));
        assert!(rendered.contains("Earlier dialogue entries omitted"));
        assert!(!rendered.contains(&"old".repeat(100)));
    }
}
