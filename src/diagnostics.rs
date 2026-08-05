use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use nakode_protocol::{
    DiagnosticsDailyUsage, DiagnosticsReport, DiagnosticsSessionUsage, DiagnosticsToolUsage,
    DiagnosticsUsageTotals, ProviderId, SessionId,
};
use rusqlite::Connection;
use thiserror::Error;

use crate::{
    api_projection,
    config::Config,
    native_client,
    runtime::{InferenceKind, RuntimeSession},
};

type UsageTotals = DiagnosticsUsageTotals;
type DailyUsage = DiagnosticsDailyUsage;
type ToolUsage = DiagnosticsToolUsage;
type SessionUsage = DiagnosticsSessionUsage;

#[derive(Clone, Debug)]
pub struct DiagnosticsOptions {
    pub days: u16,
    pub session_limit: usize,
    pub provider: Option<String>,
    pub json: bool,
}

#[derive(Debug, Error)]
pub enum DiagnosticsError {
    #[error("failed to start the native Nakode client: {0}")]
    NativeClientStart(String),
    #[error(transparent)]
    Sdk(#[from] nakode_sdk::SdkError),
    #[error("Nakode diagnostics protocol error: {0}")]
    Protocol(String),
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

/// Queries and renders privacy-preserving runtime telemetry through the native
/// Nakode server. Prompt text, reasoning, tool arguments, tool output, and
/// session titles are never emitted.
///
/// # Errors
/// Returns an error when the native server, protocol query, or report
/// serialization fails.
pub async fn run(
    config: &Config,
    options: &DiagnosticsOptions,
) -> Result<String, DiagnosticsError> {
    let client = native_client::connect(config)
        .await
        .map_err(|error| DiagnosticsError::NativeClientStart(error.to_string()))?;
    let report = client
        .get_diagnostics(nakode_sdk::v1::GetDiagnosticsRequest {
            days: u32::from(options.days),
            session_limit: u32::try_from(options.session_limit).unwrap_or(u32::MAX),
            provider_id: options.provider.clone(),
        })
        .await?;
    let report = api_projection::diagnostics(report);
    if options.json {
        serde_json::to_string_pretty(&report).map_err(Into::into)
    } else {
        Ok(render_text(&report))
    }
}

pub(crate) fn collect(
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
        if let Some(usage) = aggregate_session(
            provider, session_id, session, since_ms, &mut daily, &mut tools,
        ) {
            sessions.push(usage);
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
            provider_id: ProviderId::from(provider),
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
        provider_filter: options.provider.clone().map(ProviderId::from),
        sessions_scanned: u64::try_from(sessions_scanned).unwrap_or(u64::MAX),
        sessions_with_activity: u64::try_from(sessions_with_activity).unwrap_or(u64::MAX),
        totals,
        daily: daily_values,
        tools: tool_values,
        sessions,
        notes: vec![
            "No prompt text, reasoning, tool arguments, tool output, session titles, or credentials are included.".to_owned(),
            "Reported token and cache fields are provider telemetry; zero means the provider did not report that field.".to_owned(),
            "Cached tokens may still count toward subscription or provider usage limits even when API pricing discounts them.".to_owned(),
        ],
    })
}

fn aggregate_session(
    provider: String,
    session_id: String,
    session: RuntimeSession,
    since_ms: u64,
    daily: &mut BTreeMap<(i64, String), UsageTotals>,
    tools: &mut BTreeMap<(String, String), ToolUsage>,
) -> Option<SessionUsage> {
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
        add_tool_usage(tools, &provider, metric);
    }
    (latest_activity_ms > 0).then_some(SessionUsage {
        session_id: SessionId::from(session_id),
        provider_id: ProviderId::from(provider),
        model: session.model,
        latest_activity_ms,
        totals,
    })
}

fn add_tool_usage(
    tools: &mut BTreeMap<(String, String), ToolUsage>,
    provider: &str,
    metric: &crate::runtime::ToolMetric,
) {
    let tool = tools
        .entry((provider.to_owned(), metric.name.clone()))
        .or_insert_with(|| ToolUsage {
            provider_id: ProviderId::from(provider),
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

fn render_text(report: &DiagnosticsReport) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "Nakode diagnostics · last {} days · {} active / {} scanned sessions",
        report.period_days, report.sessions_with_activity, report.sessions_scanned
    )
    .expect("writing to a String cannot fail");
    if let Some(provider) = &report.provider_filter {
        writeln!(output, "Provider: {provider}").expect("writing to a String cannot fail");
    }
    output.push('\n');
    append_totals(&mut output, &report.totals);

    output.push_str("\nDaily usage (UTC)\n");
    output.push_str(
        "date        provider          rounds       input      cached    uncached      output\n",
    );
    for day in &report.daily {
        writeln!(
            output,
            "{:<10}  {:<16}  {:>6}  {:>10}  {:>10}  {:>10}  {:>10}",
            day.date_utc,
            day.provider_id,
            day.totals.inference_rounds,
            compact_number(day.totals.reported_input_tokens),
            compact_number(day.totals.reported_cached_input_tokens),
            compact_number(day.totals.reported_uncached_input_tokens()),
            compact_number(day.totals.reported_output_tokens),
        )
        .expect("writing to a String cannot fail");
    }

    output.push_str("\nTools by model-facing output\n");
    output.push_str(
        "provider          tool             calls   failed   model out    full out   duration\n",
    );
    for tool in &report.tools {
        writeln!(
            output,
            "{:<16}  {:<14}  {:>6}  {:>7}  {:>10}  {:>10}  {:>9}",
            tool.provider_id,
            tool.tool,
            tool.calls,
            tool.failures,
            format_bytes(tool.model_output_bytes),
            format_bytes(tool.full_output_bytes),
            format_duration(tool.duration_ms),
        )
        .expect("writing to a String cannot fail");
    }

    output.push_str("\nHighest-input sessions\n");
    output.push_str("session       provider          model                 rounds       input      cached    uncached   tools\n");
    for session in &report.sessions {
        writeln!(
            output,
            "{:<12}  {:<16}  {:<20}  {:>6}  {:>10}  {:>10}  {:>10}  {:>6}",
            short_id(session.session_id.as_str()),
            session.provider_id,
            truncate(&session.model, 20),
            session.totals.inference_rounds,
            compact_number(session.totals.reported_input_tokens),
            compact_number(session.totals.reported_cached_input_tokens),
            compact_number(session.totals.reported_uncached_input_tokens()),
            session.totals.executed_tool_calls,
        )
        .expect("writing to a String cannot fail");
    }
    output.push_str("\nPrivacy: prompts, reasoning, arguments, outputs, titles, and credentials are excluded.\n");
    output
        .push_str("Caution: cached tokens can still count toward provider subscription limits.\n");
    output
}

fn append_totals(output: &mut String, totals: &UsageTotals) {
    let cache_rate = totals
        .cache_rate_percent()
        .map_or_else(|| "not reported".to_owned(), |rate| format!("{rate:.2}%"));
    write!(
        output,
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
    )
    .expect("writing to a String cannot fail");
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) fn day_number(timestamp_ms: u64) -> i64 {
    i64::try_from(timestamp_ms / 86_400_000).unwrap_or(i64::MAX)
}

pub(crate) fn format_utc_day(day: i64) -> String {
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

fn compact_number(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{}B", format_decimal(value, 1_000_000_000, 2))
    } else if value >= 1_000_000 {
        format!("{}M", format_decimal(value, 1_000_000, 2))
    } else if value >= 1_000 {
        format!("{}K", format_decimal(value, 1_000, 1))
    } else {
        value.to_string()
    }
}

fn format_bytes(value: u64) -> String {
    if value >= 1_073_741_824 {
        format!("{} GiB", format_decimal(value, 1_073_741_824, 2))
    } else if value >= 1_048_576 {
        format!("{} MiB", format_decimal(value, 1_048_576, 2))
    } else if value >= 1_024 {
        format!("{} KiB", format_decimal(value, 1_024, 1))
    } else {
        format!("{value} B")
    }
}

fn format_duration(value_ms: u64) -> String {
    let seconds = value_ms / 1_000;
    if seconds >= 3_600 {
        format!("{} h", format_decimal(seconds, 3_600, 1))
    } else if seconds >= 60 {
        format!("{} m", format_decimal(seconds, 60, 1))
    } else {
        format!("{} s", format_decimal(value_ms, 1_000, 1))
    }
}

fn format_decimal(value: u64, divisor: u64, precision: u32) -> String {
    let scale = 10_u128.pow(precision);
    let scaled = (u128::from(value) * scale + u128::from(divisor) / 2) / u128::from(divisor);
    let whole = scaled / scale;
    let fraction = scaled % scale;
    let width = usize::try_from(precision).expect("format precision fits usize");
    format!("{whole}.{fraction:0width$}")
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
    fn aggregates_daily_session_and_tool_usage_without_content() {
        let directory = tempfile::tempdir().expect("temp directory");
        let database = directory.path().join("sessions.sqlite3");
        write_session_fixture(&database);

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

    fn write_session_fixture(database: &Path) {
        let connection = Connection::open(database).expect("database");
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
