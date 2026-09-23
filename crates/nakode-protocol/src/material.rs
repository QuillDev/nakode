//! Bounded transcript-material reads. Integrations authenticate the caller; the runtime additionally
//! rechecks the exact parent relationship and source session/run on every discovery and retrieval.
use serde::{Deserialize, Serialize};

use crate::{ArtifactId, ArtifactView, EntryId, RunId, SessionId};

pub const MAX_MATERIAL_PAGE_SIZE: u32 = 64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterialSource {
    pub session_id: SessionId,
    pub run_id: Option<RunId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterialScope {
    pub parent_session_id: SessionId,
    pub source: MaterialSource,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaterialMetadata {
    pub artifact_id: ArtifactId,
    pub entry_id: EntryId,
    pub label: String,
    pub media_type: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaterialPage {
    pub scope: MaterialScope,
    pub session_title: String,
    pub run_title: Option<String>,
    pub items: Vec<MaterialMetadata>,
    /// Exclusive cursor; missing/removed cursors refuse instead of restarting the page.
    pub next_after: Option<ArtifactId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMaterial {
    pub scope: MaterialScope,
    pub session_title: String,
    pub run_title: Option<String>,
    pub artifact: ArtifactView,
}
