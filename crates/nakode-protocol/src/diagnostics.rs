use serde::{Deserialize, Serialize};

use crate::{ProviderId, SessionId};

/// Privacy-preserving aggregate runtime usage counters.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticsUsageTotals {
    pub inference_rounds: u64,
    pub compaction_rounds: u64,
    pub failed_rounds: u64,
    pub retry_count: u64,
    pub estimated_input_tokens: u64,
    pub reported_input_tokens: u64,
    pub reported_cached_input_tokens: u64,
    pub reported_cache_write_tokens: u64,
    pub reported_output_tokens: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub inference_duration_ms: u64,
    pub requested_tool_calls: u64,
    pub executed_tool_calls: u64,
    pub failed_tool_calls: u64,
    pub full_tool_output_bytes: u64,
    pub model_tool_output_bytes: u64,
    pub tool_duration_ms: u64,
}

impl DiagnosticsUsageTotals {
    #[must_use]
    pub fn reported_uncached_input_tokens(&self) -> u64 {
        self.reported_input_tokens
            .saturating_sub(self.reported_cached_input_tokens)
    }

    #[must_use]
    pub fn cache_rate_percent(&self) -> Option<f64> {
        (self.reported_input_tokens > 0).then(|| {
            u64_to_f64(self.reported_cached_input_tokens) * 100.0
                / u64_to_f64(self.reported_input_tokens)
        })
    }

    pub fn add(&mut self, other: &Self) {
        self.inference_rounds += other.inference_rounds;
        self.compaction_rounds += other.compaction_rounds;
        self.failed_rounds += other.failed_rounds;
        self.retry_count += other.retry_count;
        self.estimated_input_tokens += other.estimated_input_tokens;
        self.reported_input_tokens += other.reported_input_tokens;
        self.reported_cached_input_tokens += other.reported_cached_input_tokens;
        self.reported_cache_write_tokens += other.reported_cache_write_tokens;
        self.reported_output_tokens += other.reported_output_tokens;
        self.request_bytes += other.request_bytes;
        self.response_bytes += other.response_bytes;
        self.inference_duration_ms += other.inference_duration_ms;
        self.requested_tool_calls += other.requested_tool_calls;
        self.executed_tool_calls += other.executed_tool_calls;
        self.failed_tool_calls += other.failed_tool_calls;
        self.full_tool_output_bytes += other.full_tool_output_bytes;
        self.model_tool_output_bytes += other.model_tool_output_bytes;
        self.tool_duration_ms += other.tool_duration_ms;
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticsDailyUsage {
    pub date_utc: String,
    #[serde(rename = "provider")]
    pub provider_id: ProviderId,
    #[serde(flatten)]
    pub totals: DiagnosticsUsageTotals,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticsToolUsage {
    #[serde(rename = "provider")]
    pub provider_id: ProviderId,
    pub tool: String,
    pub calls: u64,
    pub failures: u64,
    pub full_output_bytes: u64,
    pub model_output_bytes: u64,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticsSessionUsage {
    pub session_id: SessionId,
    #[serde(rename = "provider")]
    pub provider_id: ProviderId,
    pub model: String,
    pub latest_activity_ms: u64,
    #[serde(flatten)]
    pub totals: DiagnosticsUsageTotals,
}

/// Semantic diagnostics data returned by the native server.
///
/// This report intentionally excludes prompt text, reasoning, tool arguments,
/// tool output, session titles, credentials, and provider authentication data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticsReport {
    pub generated_at_ms: u64,
    pub period_days: u16,
    pub provider_filter: Option<ProviderId>,
    pub sessions_scanned: u64,
    pub sessions_with_activity: u64,
    pub totals: DiagnosticsUsageTotals,
    pub daily: Vec<DiagnosticsDailyUsage>,
    pub tools: Vec<DiagnosticsToolUsage>,
    pub sessions: Vec<DiagnosticsSessionUsage>,
    pub notes: Vec<String>,
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("upper half fits u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits u32");
    f64::from(high).mul_add(4_294_967_296.0, f64::from(low))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DiagnosticsDailyUsage, DiagnosticsUsageTotals};
    use crate::ProviderId;

    #[test]
    fn daily_usage_preserves_the_existing_diagnostics_json_shape() {
        let usage = DiagnosticsDailyUsage {
            date_utc: "2026-07-30".to_owned(),
            provider_id: ProviderId::from("openai-codex"),
            totals: DiagnosticsUsageTotals {
                reported_input_tokens: 42,
                ..DiagnosticsUsageTotals::default()
            },
        };

        assert_eq!(
            serde_json::to_value(usage).expect("serialize diagnostics"),
            json!({
                "date_utc": "2026-07-30",
                "provider": "openai-codex",
                "inference_rounds": 0,
                "compaction_rounds": 0,
                "failed_rounds": 0,
                "retry_count": 0,
                "estimated_input_tokens": 0,
                "reported_input_tokens": 42,
                "reported_cached_input_tokens": 0,
                "reported_cache_write_tokens": 0,
                "reported_output_tokens": 0,
                "request_bytes": 0,
                "response_bytes": 0,
                "inference_duration_ms": 0,
                "requested_tool_calls": 0,
                "executed_tool_calls": 0,
                "failed_tool_calls": 0,
                "full_tool_output_bytes": 0,
                "model_tool_output_bytes": 0,
                "tool_duration_ms": 0,
            })
        );
    }
}
