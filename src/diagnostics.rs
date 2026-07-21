use std::{
    collections::BTreeMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::Connection;
use serde::Serialize;
use thiserror::Error;

use crate::{
    runtime::{InferenceKind, RuntimeSession},
    session::{SessionError, SqliteSessionRepository},
};

#[derive(Clone, Debug)]
pub struct DiagnosticsOptions {
    pub days: u16,
    pub session_limit: usize,
    pub provider: Option<String>,
    pub json: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UsageTotals {
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

impl UsageTotals {
    #[must_use]
    pub fn reported_uncached_input_tokens(&self) -> u64 {
        self.reported_input_tokens
            .saturating_sub(self.reported_cached_input_tokens)
    }

    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn cache_rate_percent(&self) -> Option<f64> {
        (self.reported_input_tokens > 0).then(|| {
            self.reported_cached_input_tokens as f64 * 100.0 / self.reported_input_tokens as f64
        })
    }

    fn add(&mut self, other: &Self) {
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

#[derive(Clone, Debug, Serialize)]
pub struct DailyUsage {
    pub date_utc: String,
    pub provider: String,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolUsage {
    pub provider: String,
    pub tool: String,
    pub calls: u64,
    pub failures: u64,
    pub full_output_bytes: u64,
    pub model_output_bytes: u64,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionUsage {
    pub session_id: String,
    pub provider: String,
    pub model: String,
    pub latest_activity_ms: u64,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiagnosticsReport {
    pub generated_at_ms: u64,
    pub period_days: u16,
    pub provider_filter: Option<String>,
    pub sessions_scanned: usize,
    pub sessions_with_activity: usize,
    pub totals: UsageTotals,
    pub daily: Vec<DailyUsage>,
    pub tools: Vec<ToolUsage>,
    pub sessions: Vec<SessionUsage>,
    pub notes: Vec<&'static str>,
}

#[derive(Debug, Error)]
pub enum DiagnosticsError {
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error("diagnostics database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("invalid persisted runtime session {session_id}: {source}")]
    InvalidSession {
        session_id: String,
        source: serde_json::Error,
    },
    #[error("could not serialize diagnostics: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Builds and renders a privacy-preserving report from persisted runtime telemetry.
/// Prompt text, reasoning, tool arguments, tool output, and session titles are never emitted.
///
/// # Errors
/// Returns an error when the session database cannot be opened or contains malformed telemetry.
pub fn run(options: &DiagnosticsOptions) -> Result<String, DiagnosticsError> {
    let repository = SqliteSessionRepository::open_default()?;
    let report = collect(repository.database_path(), options, unix_time_ms())?;
    if options.json {
        serde_json::to_string_pretty(&report).map_err(Into::into)
    } else {
        Ok(render_text(&report))
    }
}

#[allow(clippy::too_many_lines)]
fn collect(
    database: &Path,
    options: &DiagnosticsOptions,
    now_ms: u64,
) -> Result<DiagnosticsReport, DiagnosticsError> {
    let connection = Connection::open(database)?;
    let mut statement = connection.prepare(
        "SELECT provider, session_id, session_json FROM native_runtime_sessions ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let since_ms = now_ms.saturating_sub(u64::from(options.days) * 86_400_000);
    let mut sessions_scanned = 0_usize;
    let mut daily = BTreeMap::<(i64, String), UsageTotals>::new();
    let mut tools = BTreeMap::<(String, String), ToolUsage>::new();
    let mut sessions = Vec::<SessionUsage>::new();

    for row in rows {
        let (provider, session_id, raw) = row?;
        if options
            .provider
            .as_ref()
            .is_some_and(|filter| filter != &provider)
        {
            continue;
        }
        sessions_scanned += 1;
        let session = serde_json::from_str::<RuntimeSession>(&raw).map_err(|source| {
            DiagnosticsError::InvalidSession {
                session_id: session_id.clone(),
                source,
            }
        })?;
        let mut totals = UsageTotals::default();
        let mut latest_activity_ms = 0_u64;

        for metric in &session.telemetry.inference {
            if metric.started_at_ms < since_ms {
                continue;
            }
            latest_activity_ms = latest_activity_ms.max(metric.started_at_ms);
            let metric_totals = inference_totals(metric);
            totals.add(&metric_totals);
            daily
                .entry((day_number(metric.started_at_ms), provider.clone()))
                .or_default()
                .add(&metric_totals);
        }
        for metric in &session.telemetry.tools {
            if metric.started_at_ms < since_ms {
                continue;
            }
            latest_activity_ms = latest_activity_ms.max(metric.started_at_ms);
            let metric_totals = tool_totals(metric);
            totals.add(&metric_totals);
            daily
                .entry((day_number(metric.started_at_ms), provider.clone()))
                .or_default()
                .add(&metric_totals);
            let tool = tools
                .entry((provider.clone(), metric.name.clone()))
                .or_insert_with(|| ToolUsage {
                    provider: provider.clone(),
                    tool: metric.name.clone(),
                    calls: 0,
                    failures: 0,
                    full_output_bytes: 0,
                    model_output_bytes: 0,
                    duration_ms: 0,
                });
            tool.calls += 1;
            tool.failures += u64::from(metric.failed);
            tool.full_output_bytes += usize_to_u64(metric.output_bytes);
            tool.model_output_bytes += usize_to_u64(metric.model_output_bytes);
            tool.duration_ms += metric.duration_ms;
        }
        if latest_activity_ms > 0 {
            sessions.push(SessionUsage {
                session_id,
                provider,
                model: session.model,
                latest_activity_ms,
                totals,
            });
        }
    }

    sessions.sort_by(|left, right| {
        right
            .totals
            .reported_input_tokens
            .cmp(&left.totals.reported_input_tokens)
            .then_with(|| right.latest_activity_ms.cmp(&left.latest_activity_ms))
    });
    let sessions_with_activity = sessions.len();
    sessions.truncate(options.session_limit.min(500));

    let mut tool_values = tools.into_values().collect::<Vec<_>>();
    tool_values.sort_by(|left, right| {
        right
            .model_output_bytes
            .cmp(&left.model_output_bytes)
            .then_with(|| right.calls.cmp(&left.calls))
    });
    let daily_values = daily
        .into_iter()
        .map(|((day, provider), totals)| DailyUsage {
            date_utc: format_utc_day(day),
            provider,
            totals,
        })
        .collect::<Vec<_>>();
    let mut totals = UsageTotals::default();
    for day in &daily_values {
        totals.add(&day.totals);
    }

    Ok(DiagnosticsReport {
        generated_at_ms: now_ms,
        period_days: options.days,
        provider_filter: options.provider.clone(),
        sessions_scanned,
        sessions_with_activity,
        totals,
        daily: daily_values,
        tools: tool_values,
        sessions,
        notes: vec![
            "No prompt text, reasoning, tool arguments, tool output, session titles, or credentials are included.",
            "Reported token and cache fields are provider telemetry; zero means the provider did not report that field.",
            "Cached tokens may still count toward subscription or provider usage limits even when API pricing discounts them.",
        ],
    })
}

fn inference_totals(metric: &crate::runtime::InferenceMetric) -> UsageTotals {
    UsageTotals {
        inference_rounds: u64::from(metric.kind == InferenceKind::Turn),
        compaction_rounds: u64::from(metric.kind == InferenceKind::Compaction),
        failed_rounds: u64::from(metric.error.is_some()),
        retry_count: usize_to_u64(metric.retry_count),
        estimated_input_tokens: usize_to_u64(metric.estimated_input_tokens),
        reported_input_tokens: metric.usage.input_tokens.unwrap_or_default(),
        reported_cached_input_tokens: metric.usage.cached_input_tokens.unwrap_or_default(),
        reported_cache_write_tokens: metric.usage.cache_write_tokens.unwrap_or_default(),
        reported_output_tokens: metric.usage.output_tokens.unwrap_or_default(),
        request_bytes: usize_to_u64(metric.input_bytes),
        response_bytes: usize_to_u64(metric.output_bytes),
        inference_duration_ms: metric.duration_ms,
        requested_tool_calls: usize_to_u64(metric.tool_call_count),
        ..UsageTotals::default()
    }
}

fn tool_totals(metric: &crate::runtime::ToolMetric) -> UsageTotals {
    UsageTotals {
        executed_tool_calls: 1,
        failed_tool_calls: u64::from(metric.failed),
        full_tool_output_bytes: usize_to_u64(metric.output_bytes),
        model_tool_output_bytes: usize_to_u64(metric.model_output_bytes),
        tool_duration_ms: metric.duration_ms,
        ..UsageTotals::default()
    }
}

#[allow(clippy::format_push_string)]
fn render_text(report: &DiagnosticsReport) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Nakode diagnostics · last {} days · {} active / {} scanned sessions\n",
        report.period_days, report.sessions_with_activity, report.sessions_scanned
    ));
    if let Some(provider) = &report.provider_filter {
        output.push_str(&format!("Provider: {provider}\n"));
    }
    output.push('\n');
    append_totals(&mut output, &report.totals);

    output.push_str("\nDaily usage (UTC)\n");
    output.push_str(
        "date        provider          rounds       input      cached    uncached      output\n",
    );
    for day in &report.daily {
        output.push_str(&format!(
            "{:<10}  {:<16}  {:>6}  {:>10}  {:>10}  {:>10}  {:>10}\n",
            day.date_utc,
            day.provider,
            day.totals.inference_rounds,
            compact_number(day.totals.reported_input_tokens),
            compact_number(day.totals.reported_cached_input_tokens),
            compact_number(day.totals.reported_uncached_input_tokens()),
            compact_number(day.totals.reported_output_tokens),
        ));
    }

    output.push_str("\nTools by model-facing output\n");
    output.push_str(
        "provider          tool             calls   failed   model out    full out   duration\n",
    );
    for tool in &report.tools {
        output.push_str(&format!(
            "{:<16}  {:<14}  {:>6}  {:>7}  {:>10}  {:>10}  {:>9}\n",
            tool.provider,
            tool.tool,
            tool.calls,
            tool.failures,
            format_bytes(tool.model_output_bytes),
            format_bytes(tool.full_output_bytes),
            format_duration(tool.duration_ms),
        ));
    }

    output.push_str("\nHighest-input sessions\n");
    output.push_str("session       provider          model                 rounds       input      cached    uncached   tools\n");
    for session in &report.sessions {
        output.push_str(&format!(
            "{:<12}  {:<16}  {:<20}  {:>6}  {:>10}  {:>10}  {:>10}  {:>6}\n",
            short_id(&session.session_id),
            session.provider,
            truncate(&session.model, 20),
            session.totals.inference_rounds,
            compact_number(session.totals.reported_input_tokens),
            compact_number(session.totals.reported_cached_input_tokens),
            compact_number(session.totals.reported_uncached_input_tokens()),
            session.totals.executed_tool_calls,
        ));
    }
    output.push_str("\nPrivacy: prompts, reasoning, arguments, outputs, titles, and credentials are excluded.\n");
    output
        .push_str("Caution: cached tokens can still count toward provider subscription limits.\n");
    output
}

#[allow(clippy::format_push_string)]
fn append_totals(output: &mut String, totals: &UsageTotals) {
    let cache_rate = totals
        .cache_rate_percent()
        .map_or_else(|| "not reported".to_owned(), |rate| format!("{rate:.2}%"));
    output.push_str(&format!(
        "Inference rounds: {} ({} compactions, {} failed, {} retries)\n\
Reported tokens: {} input · {} cached · {} uncached · {} output · {cache_rate} cache rate\n\
Tool calls: {} executed · {} failed · {} model-facing output · {} full output\n\
Runtime: {} inference · {} tools\n",
        totals.inference_rounds,
        totals.compaction_rounds,
        totals.failed_rounds,
        totals.retry_count,
        compact_number(totals.reported_input_tokens),
        compact_number(totals.reported_cached_input_tokens),
        compact_number(totals.reported_uncached_input_tokens()),
        compact_number(totals.reported_output_tokens),
        totals.executed_tool_calls,
        totals.failed_tool_calls,
        format_bytes(totals.model_tool_output_bytes),
        format_bytes(totals.full_tool_output_bytes),
        format_duration(totals.inference_duration_ms),
        format_duration(totals.tool_duration_ms),
    ));
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn day_number(timestamp_ms: u64) -> i64 {
    i64::try_from(timestamp_ms / 86_400_000).unwrap_or(i64::MAX)
}

fn format_utc_day(day: i64) -> String {
    let z = day + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[allow(clippy::cast_precision_loss)]
fn compact_number(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.2}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.2}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

#[allow(clippy::cast_precision_loss)]
fn format_bytes(value: u64) -> String {
    if value >= 1_073_741_824 {
        format!("{:.2} GiB", value as f64 / 1_073_741_824.0)
    } else if value >= 1_048_576 {
        format!("{:.2} MiB", value as f64 / 1_048_576.0)
    } else if value >= 1_024 {
        format!("{:.1} KiB", value as f64 / 1_024.0)
    } else {
        format!("{value} B")
    }
}

#[allow(clippy::cast_precision_loss)]
fn format_duration(value_ms: u64) -> String {
    let seconds = value_ms / 1_000;
    if seconds >= 3_600 {
        format!("{:.1} h", seconds as f64 / 3_600.0)
    } else if seconds >= 60 {
        format!("{:.1} m", seconds as f64 / 60.0)
    } else {
        format!("{:.1} s", value_ms as f64 / 1_000.0)
    }
}

fn short_id(value: &str) -> &str {
    value.get(..12).unwrap_or(value)
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!(
            "{}…",
            prefix
                .chars()
                .take(max_chars.saturating_sub(1))
                .collect::<String>()
        )
    } else {
        prefix
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use super::*;
    use crate::runtime::{InferenceMetric, InferenceUsage, RuntimeTelemetry, ToolMetric};

    #[test]
    #[allow(clippy::too_many_lines)]
    fn aggregates_daily_session_and_tool_usage_without_content() {
        let directory = tempfile::tempdir().expect("temp directory");
        let database = directory.path().join("sessions.sqlite3");
        let connection = Connection::open(&database).expect("database");
        connection
            .execute_batch(
                "CREATE TABLE native_runtime_sessions (
                   provider TEXT NOT NULL,
                   session_id TEXT NOT NULL,
                   session_json TEXT NOT NULL,
                   updated_at INTEGER NOT NULL,
                   PRIMARY KEY(provider, session_id)
                 );",
            )
            .expect("schema");
        let mut session =
            RuntimeSession::new("gpt-test".to_owned(), "private instructions".to_owned());
        session.id = "session-private-id".to_owned();
        session.telemetry = RuntimeTelemetry {
            inference: vec![InferenceMetric {
                kind: InferenceKind::Turn,
                turn_id: "turn-private-id".to_owned(),
                round: 0,
                started_at_ms: 86_400_000,
                duration_ms: 2_000,
                estimated_input_tokens: 1_100,
                input_bytes: 4_400,
                output_bytes: 800,
                tool_call_count: 1,
                retry_count: 1,
                usage: InferenceUsage {
                    input_tokens: Some(1_000),
                    output_tokens: Some(200),
                    cached_input_tokens: Some(750),
                    cache_write_tokens: Some(50),
                },
                response_id: Some("response-private-id".to_owned()),
                error: None,
            }],
            tools: vec![ToolMetric {
                turn_id: "turn-private-id".to_owned(),
                call_id: "call-private-id".to_owned(),
                name: "read".to_owned(),
                started_at_ms: 86_400_100,
                duration_ms: 500,
                output_bytes: 20_000,
                model_output_bytes: 16_384,
                failed: false,
            }],
        };
        let raw = serde_json::to_string(&session).expect("session JSON");
        connection
            .execute(
                "INSERT INTO native_runtime_sessions VALUES (?1, ?2, ?3, ?4)",
                params!["openai-codex", session.id, raw, 86_400_i64],
            )
            .expect("insert");
        drop(connection);

        let report = collect(
            &database,
            &DiagnosticsOptions {
                days: 2,
                session_limit: 10,
                provider: None,
                json: false,
            },
            2 * 86_400_000,
        )
        .expect("report");

        assert_eq!(report.totals.reported_input_tokens, 1_000);
        assert_eq!(report.totals.reported_cached_input_tokens, 750);
        assert_eq!(report.totals.reported_uncached_input_tokens(), 250);
        assert_eq!(report.totals.executed_tool_calls, 1);
        assert_eq!(report.tools[0].model_output_bytes, 16_384);
        assert_eq!(report.daily[0].date_utc, "1970-01-02");
        let rendered = render_text(&report);
        assert!(!rendered.contains("private instructions"));
        assert!(!rendered.contains("turn-private-id"));
        assert!(!rendered.contains("response-private-id"));
        assert!(!rendered.contains("call-private-id"));

        let provider_filtered = collect(
            &database,
            &DiagnosticsOptions {
                days: 2,
                session_limit: 10,
                provider: Some("devin-acp".to_owned()),
                json: false,
            },
            2 * 86_400_000,
        )
        .expect("provider-filtered report");
        assert_eq!(provider_filtered.sessions_scanned, 0);
        assert_eq!(provider_filtered.totals.reported_input_tokens, 0);

        let age_filtered = collect(
            &database,
            &DiagnosticsOptions {
                days: 1,
                session_limit: 10,
                provider: None,
                json: false,
            },
            3 * 86_400_000,
        )
        .expect("age-filtered report");
        assert_eq!(age_filtered.sessions_with_activity, 0);
        assert_eq!(age_filtered.totals.reported_input_tokens, 0);
    }

    #[test]
    fn utc_day_format_handles_epoch_and_modern_dates() {
        assert_eq!(format_utc_day(0), "1970-01-01");
        assert_eq!(format_utc_day(19_723), "2024-01-01");
    }

    #[test]
    fn usage_helpers_report_uncached_tokens_and_cache_rate() {
        assert_eq!(day_number(172_799_999), 1);
        let totals = UsageTotals {
            reported_input_tokens: 100,
            reported_cached_input_tokens: 80,
            ..UsageTotals::default()
        };
        assert_eq!(totals.reported_uncached_input_tokens(), 20);
        assert_eq!(totals.cache_rate_percent(), Some(80.0));
    }
}
