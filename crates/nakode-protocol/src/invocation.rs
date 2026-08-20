use serde::{Deserialize, Serialize};

/// The two local capability classes whose accepted use can be recorded.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationKind {
    Archetype,
    Skill,
}

/// One cheap aggregate projection keyed by an immutable local capability identity.
///
/// Archetypes persist a generated identity across authoritative renames. Skills use the publisher's
/// optional frontmatter `id`; legacy definitions fall back to their exact catalogue name. The
/// display label is invocation-time/current catalogue metadata and is not used for attribution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationUsage {
    pub kind: InvocationKind,
    pub identity: String,
    pub display_label: String,
    pub currently_installed: bool,
    pub invocation_count: u64,
    pub first_used_at_ms: Option<u64>,
    pub last_used_at_ms: Option<u64>,
}

/// Consent state and bounded aggregate usage returned by Nakode.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationSummary {
    pub enabled: bool,
    pub items: Vec<InvocationUsage>,
}

/// One server-computed time bucket. Counts include only successful accepted invocations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationBucket {
    pub start_at_ms: u64,
    pub archetype_count: u64,
    pub skill_count: u64,
}

/// A bounded time-series projection; raw append-only events never cross the public boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationTimeline {
    pub start_at_ms: u64,
    pub end_at_ms: u64,
    pub bucket_width_ms: u64,
    pub buckets: Vec<InvocationBucket>,
}
