use serde::{Deserialize, Serialize};

/// How much of one provider account's weekly quota Nakode could observe.
///
/// Unknown usage is never reported as zero: only `Available` and `Stale` carry a window.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageState {
    /// The provider reported the weekly window on the latest read.
    Available,
    /// The latest read failed; the window is the last one the provider reported.
    Stale,
    /// The provider exposes no weekly quota for this account or credential kind.
    Unsupported,
    /// The account cannot be read now, such as when it is signed out or its sign-in expired.
    Unavailable,
    /// The provider could not be read and no earlier window is known.
    Failed,
}

/// The provider's own weekly quota window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WeeklyUsageWindow {
    /// Whole percent of the weekly quota already used. Values above 100 mean overage.
    pub used_percent: u32,
    pub resets_at_ms: Option<u64>,
}

/// Weekly usage for one configured provider account.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderAccountUsage {
    pub provider_id: String,
    pub account_id: String,
    pub account_label: String,
    pub state: ProviderUsageState,
    pub weekly: Option<WeeklyUsageWindow>,
    /// When the provider reported `weekly`.
    pub observed_at_ms: Option<u64>,
    /// A short, credential-free explanation for every state except `Available`.
    pub safe_reason: Option<String>,
}

/// Weekly usage for every enabled provider account on this server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderUsageReport {
    pub accounts: Vec<ProviderAccountUsage>,
}
