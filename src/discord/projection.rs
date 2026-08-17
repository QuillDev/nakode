//! Typed user/assistant projection cursors and ephemeral historical recovery storage.

use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use nakode_sdk::v1 as api;
use serde::{Deserialize, Serialize};

use super::{
    DiscordError, TRANSPORT_NAME, atomic_write, hex_digest, io_error, prepare_private_directory,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProjectionKind {
    User,
    Assistant,
}

impl ProjectionKind {
    pub(super) fn api_value(self) -> i32 {
        match self {
            Self::User => api::BridgeProjectionKind::User as i32,
            Self::Assistant => api::BridgeProjectionKind::Assistant as i32,
        }
    }

    pub(super) fn nonce_label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ProjectionItem {
    pub(super) kind: ProjectionKind,
    pub(super) turn_id: String,
    pub(super) body: String,
    pub(super) suppressed: bool,
}

impl ProjectionItem {
    pub(super) fn cursor(&self) -> api::BridgeProjection {
        api::BridgeProjection {
            kind: self.kind.api_value(),
            turn_id: self.turn_id.clone(),
        }
    }

    pub(super) fn matches(&self, cursor: &api::BridgeProjection) -> bool {
        cursor.kind == self.kind.api_value() && cursor.turn_id == self.turn_id
    }
}

pub(super) fn projection_clears_stale_source(
    projection: &ProjectionItem,
    active_owner_turn: Option<&str>,
    active_source_message_id: Option<&str>,
) -> bool {
    projection.kind == ProjectionKind::User
        && !projection.suppressed
        && active_owner_turn == Some(projection.turn_id.as_str())
        && active_source_message_id.is_some()
}

pub(super) fn same_projection(
    left: Option<&api::BridgeProjection>,
    right: Option<&api::BridgeProjection>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.kind == right.kind && left.turn_id == right.turn_id,
        (None, Some(_)) | (Some(_), None) => false,
    }
}

pub(super) fn completed_projections(
    entries: &[api::TranscriptEntry],
    active_turn: Option<&str>,
) -> Vec<ProjectionItem> {
    let mut projections = Vec::<ProjectionItem>::new();
    for entry in entries {
        let Some(projection) = projection_from_entry(entry, active_turn) else {
            continue;
        };
        if let Some(existing) = projections.iter_mut().find(|existing| {
            existing.kind == projection.kind && existing.turn_id == projection.turn_id
        }) {
            existing.body = projection.body;
            existing.suppressed |= projection.suppressed;
        } else {
            projections.push(projection);
        }
    }
    projections
}

pub(super) fn projection_from_entry(
    entry: &api::TranscriptEntry,
    active_turn: Option<&str>,
) -> Option<ProjectionItem> {
    if entry.status != api::TranscriptEntryStatus::Complete as i32 {
        return None;
    }
    let turn_id = entry.owner_turn_id.as_deref()?;
    let (kind, suppressed) = if entry.kind == api::TranscriptEntryKind::User as i32 {
        (
            ProjectionKind::User,
            entry.source_transport.as_deref() == Some(TRANSPORT_NAME),
        )
    } else if entry.kind == api::TranscriptEntryKind::Assistant as i32 {
        if active_turn == Some(turn_id) {
            return None;
        }
        (ProjectionKind::Assistant, false)
    } else {
        return None;
    };
    Some(ProjectionItem {
        kind,
        turn_id: turn_id.to_owned(),
        body: entry.body.clone(),
        suppressed,
    })
}

#[derive(Deserialize, Serialize)]
pub(super) struct RecoveryEntry {
    pub(super) id: String,
    pub(super) kind: i32,
    pub(super) turn_id: String,
    pub(super) source_transport: Option<String>,
    pub(super) body: String,
    pub(super) body_start_byte: u64,
    pub(super) body_total_bytes: u64,
}

pub(super) struct RecoverySpool {
    directory: PathBuf,
    entries: usize,
}

impl RecoverySpool {
    pub(super) fn new(root: &Path, session_id: &str) -> Result<Self, DiscordError> {
        prepare_private_directory(root)?;
        let directory = root.join(&hex_digest(session_id.as_bytes())[..32]);
        if directory.exists() {
            std::fs::remove_dir_all(&directory).map_err(|source| io_error(&directory, source))?;
        }
        prepare_private_directory(&directory)?;
        Ok(Self {
            directory,
            entries: 0,
        })
    }

    pub(super) fn push(&mut self, entry: &api::TranscriptEntry) -> Result<(), DiscordError> {
        let turn_id = entry.owner_turn_id.as_deref().ok_or_else(|| {
            DiscordError::InvalidConfig("recovery entry has no owner turn".to_owned())
        })?;
        let marker = self.directory.join(format!(
            "projection-{}-{}",
            entry.kind,
            hex_digest(turn_id.as_bytes())
        ));
        let mut marker_file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
        {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                if entry.source_transport.as_deref() == Some(TRANSPORT_NAME) {
                    let index = std::fs::read_to_string(&marker)
                        .map_err(|source| io_error(&marker, source))?
                        .parse::<usize>()
                        .map_err(|_| {
                            DiscordError::InvalidConfig(
                                "invalid transcript recovery index".to_owned(),
                            )
                        })?;
                    let path = self.directory.join(format!("entry-{index:020}.json"));
                    let encoded = std::fs::read(&path).map_err(|source| io_error(&path, source))?;
                    let mut stored: RecoveryEntry =
                        serde_json::from_slice(&encoded).map_err(|_| {
                            DiscordError::InvalidConfig(
                                "invalid private transcript recovery metadata".to_owned(),
                            )
                        })?;
                    stored.source_transport = Some(TRANSPORT_NAME.to_owned());
                    let encoded = serde_json::to_vec(&stored).map_err(|_| {
                        DiscordError::InvalidConfig(
                            "could not spool transcript recovery metadata".to_owned(),
                        )
                    })?;
                    atomic_write(&path, &encoded)?;
                }
                return Ok(());
            }
            Err(source) => return Err(io_error(&marker, source)),
        };
        write!(marker_file, "{}", self.entries).map_err(|source| io_error(&marker, source))?;
        marker_file
            .sync_all()
            .map_err(|source| io_error(&marker, source))?;
        let stored = RecoveryEntry {
            id: entry.id.clone(),
            kind: entry.kind,
            turn_id: turn_id.to_owned(),
            source_transport: entry.source_transport.clone(),
            body: entry.body.clone(),
            body_start_byte: entry.body_start_byte,
            body_total_bytes: entry.body_total_bytes,
        };
        let encoded = serde_json::to_vec(&stored).map_err(|_| {
            DiscordError::InvalidConfig("could not spool transcript recovery metadata".to_owned())
        })?;
        let path = self
            .directory
            .join(format!("entry-{:020}.json", self.entries));
        atomic_write(&path, &encoded)?;
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    pub(super) fn oldest_first(
        &self,
    ) -> impl Iterator<Item = Result<RecoveryEntry, DiscordError>> + '_ {
        (0..self.entries).rev().map(|index| {
            let path = self.directory.join(format!("entry-{index:020}.json"));
            let encoded = std::fs::read(&path).map_err(|source| io_error(&path, source))?;
            serde_json::from_slice(&encoded).map_err(|_| {
                DiscordError::InvalidConfig(
                    "invalid private transcript recovery metadata".to_owned(),
                )
            })
        })
    }
}

impl Drop for RecoverySpool {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
