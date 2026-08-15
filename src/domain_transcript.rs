use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::media::ImageData;

/// Semantic kind of one canonical transcript entry.
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

/// Server-owned lifecycle state of one canonical transcript entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryStatus {
    Running,
    Complete,
    Failed,
    Interrupted,
}

/// Canonical transcript content persisted and projected by the Nakode server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptEntry {
    pub id: String,
    pub key: Option<String>,
    pub kind: EntryKind,
    pub title: String,
    pub body: String,
    pub status: EntryStatus,
    /// Immutable server creation boundary. Absent only for imported history without source timing.
    pub created_at_ms: Option<u64>,
    /// The provider that produced this entry, when it belongs to an inference turn.
    pub provider_id: Option<String>,
    /// The canonical provider-qualified model used by that turn.
    pub model_id: Option<String>,
    /// Provider turn identifier correlating historical entries with immutable owner-turn metadata.
    pub owner_turn_id: Option<String>,
    /// Concrete reasoning effort resolved for the owner turn that produced this entry.
    pub reasoning_effort: Option<String>,
    /// Concrete fast-mode value resolved for the owner turn that produced this entry.
    pub fast_mode: Option<bool>,
    /// Versioned, bounded provider-neutral tool audit JSON. Never interpreted as markup.
    pub tool_audit_json: Option<String>,
}

#[derive(Clone, Debug)]
struct TranscriptImageArtifact {
    label: String,
    image: ImageData,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamState {
    Idle,
    Active,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HistoryRetention {
    Complete,
    Truncated,
}

/// Canonical transcript state owned by the server domain reducer.
///
/// This type intentionally contains no markdown projection, terminal width,
/// render cache, expansion preference, or image-preview preference. Clients
/// derive those presentation concerns from protocol transcript views.
#[derive(Clone, Debug)]
pub struct DomainTranscript {
    entries: Vec<TranscriptEntry>,
    item_indices: HashMap<String, usize>,
    limit: usize,
    stream_state: StreamState,
    stream_label: String,
    images: HashMap<String, Vec<TranscriptImageArtifact>>,
    history_retention: HistoryRetention,
}

impl DomainTranscript {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            item_indices: HashMap::new(),
            limit: limit.max(100),
            stream_state: StreamState::Idle,
            stream_label: "Nakode".to_owned(),
            images: HashMap::new(),
            history_retention: HistoryRetention::Complete,
        }
    }

    #[must_use]
    pub fn entries(&self) -> &[TranscriptEntry] {
        &self.entries
    }

    #[must_use]
    pub const fn has_earlier_entries(&self) -> bool {
        matches!(self.history_retention, HistoryRetention::Truncated)
    }

    #[must_use]
    pub const fn stream_active(&self) -> bool {
        matches!(self.stream_state, StreamState::Active)
    }

    #[must_use]
    pub fn stream_label(&self) -> &str {
        &self.stream_label
    }

    #[must_use]
    pub fn image(&self, key: &str, index: usize) -> Option<&ImageData> {
        self.images
            .get(key)
            .and_then(|images| images.get(index))
            .map(|artifact| &artifact.image)
    }

    pub(crate) fn image_artifacts<'a>(
        &'a self,
        entry: &'a TranscriptEntry,
    ) -> impl Iterator<Item = (&'a str, &'a ImageData)> + 'a {
        entry
            .key
            .as_ref()
            .and_then(|key| self.images.get(key))
            .into_iter()
            .flatten()
            .map(|artifact| (artifact.label.as_str(), &artifact.image))
    }

    pub fn set_images(&mut self, key: impl Into<String>, images: Vec<ImageData>) {
        let multiple = images.len() > 1;
        self.set_labeled_images(
            key,
            images
                .into_iter()
                .enumerate()
                .map(|(index, image)| {
                    let label = if multiple {
                        format!("Image {}", index.saturating_add(1))
                    } else {
                        "Image".to_owned()
                    };
                    (label, image)
                })
                .collect(),
        );
    }

    pub(crate) fn set_labeled_images(
        &mut self,
        key: impl Into<String>,
        images: Vec<(String, ImageData)>,
    ) {
        let key = key.into();
        if images.is_empty() {
            self.images.remove(&key);
        } else {
            self.images.insert(
                key,
                images
                    .into_iter()
                    .map(|(label, image)| TranscriptImageArtifact { label, image })
                    .collect(),
            );
        }
        self.enforce_limit();
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.item_indices.clear();
        self.images.clear();
        self.stream_state = StreamState::Idle;
        self.history_retention = HistoryRetention::Complete;
    }

    pub fn set_stream_active(&mut self, active: bool) {
        self.stream_state = if active {
            StreamState::Active
        } else {
            StreamState::Idle
        };
    }

    pub fn set_stream_label(&mut self, label: impl Into<String>) {
        self.stream_label = label.into();
    }

    /// Restores one already-normalized durable entry without minting a new identity or dropping
    /// provider attribution and structured tool-audit evidence.
    pub fn restore(&mut self, entry: TranscriptEntry) {
        if let Some(key) = entry.key.as_ref()
            && let Some(index) = self.item_indices.get(key).copied()
        {
            self.entries[index] = entry;
        } else {
            let index = self.entries.len();
            if let Some(key) = entry.key.as_ref() {
                self.item_indices.insert(key.clone(), index);
            }
            self.entries.push(entry);
        }
        self.enforce_limit();
    }

    pub fn push(
        &mut self,
        kind: EntryKind,
        title: impl Into<String>,
        body: impl Into<String>,
        status: EntryStatus,
    ) {
        self.entries.push(TranscriptEntry {
            id: uuid::Uuid::now_v7().to_string(),
            key: None,
            kind,
            title: title.into(),
            body: body.into(),
            status,
            created_at_ms: Some(unix_time_ms()),
            provider_id: None,
            model_id: None,
            owner_turn_id: None,
            reasoning_effort: None,
            fast_mode: None,
            tool_audit_json: None,
        });
        self.enforce_limit();
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
                id: uuid::Uuid::now_v7().to_string(),
                key: Some(key.clone()),
                kind,
                title: title.into(),
                body: body.into(),
                status,
                created_at_ms: Some(unix_time_ms()),
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                tool_audit_json: None,
            });
            self.item_indices.insert(key, index);
        }
        self.enforce_limit();
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
                id: uuid::Uuid::now_v7().to_string(),
                key: Some(key.clone()),
                kind,
                title: title.into(),
                body: delta.to_owned(),
                status: EntryStatus::Running,
                created_at_ms: Some(unix_time_ms()),
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                tool_audit_json: None,
            });
            self.item_indices.insert(key, index);
        }
        self.enforce_limit();
    }

    pub fn set_created_at_ms(&mut self, key: &str, created_at_ms: Option<u64>) {
        if let Some(index) = self.item_indices.get(key).copied() {
            self.entries[index].created_at_ms = created_at_ms;
        }
    }

    pub fn set_origin(&mut self, key: &str, provider_id: Option<&str>, model_id: Option<&str>) {
        if let Some(index) = self.item_indices.get(key).copied() {
            let entry = &mut self.entries[index];
            if entry.provider_id.is_none() {
                entry.provider_id = provider_id.map(str::to_owned);
            }
            if entry.model_id.is_none() {
                entry.model_id = model_id.map(str::to_owned);
            }
        }
    }

    pub fn set_turn_attribution(
        &mut self,
        key: &str,
        turn_id: &str,
        reasoning_effort: Option<&str>,
        fast_mode: bool,
    ) {
        if let Some(index) = self.item_indices.get(key).copied() {
            let entry = &mut self.entries[index];
            if entry.owner_turn_id.is_none() {
                entry.owner_turn_id = Some(turn_id.to_owned());
                entry.reasoning_effort = reasoning_effort.map(str::to_owned);
                entry.fast_mode = Some(fast_mode);
            }
        }
    }

    pub fn set_tool_audit(&mut self, key: &str, audit_json: Option<String>) {
        if let Some(index) = self.item_indices.get(key).copied()
            && audit_json.is_some()
        {
            // A provider update without audit data must not erase the payload already correlated by
            // this stable item key. Completed updates replace starts because they carry `Some` too.
            self.entries[index].tool_audit_json = audit_json;
        }
    }

    pub fn set_status(&mut self, key: &str, status: EntryStatus) {
        if let Some(index) = self.item_indices.get(key).copied() {
            self.entries[index].status = status;
        }
    }

    pub fn replace_body(&mut self, key: &str, body: &str, status: EntryStatus) {
        if let Some(index) = self.item_indices.get(key).copied() {
            body.clone_into(&mut self.entries[index].body);
            self.entries[index].status = status;
        }
    }

    pub fn move_before(&mut self, key: &str, anchor: &str) {
        let Some(index) = self.item_indices.get(key).copied() else {
            return;
        };
        let Some(anchor_index) = self.item_indices.get(anchor).copied() else {
            return;
        };
        if index < anchor_index {
            return;
        }
        let entry = self.entries.remove(index);
        self.entries.insert(anchor_index, entry);
        self.reindex();
    }

    pub fn remove(&mut self, key: &str) {
        let Some(index) = self.item_indices.get(key).copied() else {
            return;
        };
        self.entries.remove(index);
        self.reindex();
    }

    pub fn finish_running(&mut self, status: EntryStatus) {
        for entry in &mut self.entries {
            if entry.status == EntryStatus::Running {
                entry.status = status;
            }
        }
        self.stream_state = StreamState::Idle;
    }

    fn enforce_limit(&mut self) {
        if self.entries.len() > self.limit {
            let remove_count = self.entries.len() - self.limit;
            self.entries.drain(..remove_count);
            self.history_retention = HistoryRetention::Truncated;
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
        self.images
            .retain(|key, _| self.item_indices.contains_key(key));
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
