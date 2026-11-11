//! Weekly provider quota reads.
//!
//! Adapters own each provider's endpoint, credential shape and response format. This module owns
//! the provider-neutral states, a short-lived cache and the stale fallback. Reads use the stored
//! access token as-is: refresh tokens rotate, so only the provider supervisors refresh sign-ins.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nakode_protocol::{
    ProviderAccountUsage, ProviderUsageReport, ProviderUsageState, WeeklyUsageWindow,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::backend::{CLAUDE_PROVIDER, CODEX_PROVIDER};

/// How long one account's read is reused before the provider is asked again.
const FRESH_FOR: Duration = Duration::from_secs(5 * 60);
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// Why one account's weekly usage could not be read. Messages never contain credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UsageReadError {
    Unsupported(String),
    Unavailable(String),
    Failed(String),
}

/// One enabled provider account to read.
pub(crate) struct UsageTarget {
    pub(crate) provider_id: String,
    pub(crate) account_id: String,
    pub(crate) account_label: String,
    pub(crate) credential: Option<Value>,
}

struct CachedUsage {
    read_at: Instant,
    credential_digest: Option<[u8; 32]>,
    usage: ProviderAccountUsage,
}

/// Shared by every usage query so concurrent readers wait for one provider read.
#[derive(Clone, Default)]
pub(crate) struct ProviderUsageReader {
    client: reqwest::Client,
    cache: Arc<tokio::sync::Mutex<HashMap<(String, String), CachedUsage>>>,
}

impl ProviderUsageReader {
    pub(crate) async fn read(&self, targets: Vec<UsageTarget>) -> ProviderUsageReport {
        let mut cache = self.cache.lock().await;
        let mut accounts = Vec::with_capacity(targets.len());
        for target in targets {
            let key = (target.provider_id.clone(), target.account_id.clone());
            let credential_digest = target.credential.as_ref().map(credential_digest);
            let now_ms = unix_time_ms();
            let expired = credential_expired(target.credential.as_ref(), now_ms);
            let cached = cache.get(&key).filter(|cached| {
                cached.credential_digest == credential_digest
                    && cached.read_at.elapsed() < FRESH_FOR
                    && !expired
            });
            if let Some(cached) = cached {
                accounts.push(cached.usage.clone());
                continue;
            }
            let result = read_weekly_usage(&self.client, &target, now_ms).await;
            // Only the same sign-in's last window may stand in for a failed read.
            let previous = cache
                .get(&key)
                .filter(|cached| cached.credential_digest == credential_digest)
                .map(|cached| &cached.usage);
            let usage = settle(target, result, previous, now_ms);
            cache.insert(
                key,
                CachedUsage {
                    read_at: Instant::now(),
                    credential_digest,
                    usage: usage.clone(),
                },
            );
            accounts.push(usage);
        }
        ProviderUsageReport { accounts }
    }
}

/// A cached window never outlives the access token it was read with.
fn credential_expired(credential: Option<&Value>, now_ms: u64) -> bool {
    credential
        .and_then(|credential| credential.get("expires_at_ms"))
        .and_then(Value::as_u64)
        .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}

async fn read_weekly_usage(
    client: &reqwest::Client,
    target: &UsageTarget,
    now_ms: u64,
) -> Result<WeeklyUsageWindow, UsageReadError> {
    let supported = [CLAUDE_PROVIDER, CODEX_PROVIDER].contains(&target.provider_id.as_str());
    if !supported {
        return Err(UsageReadError::Unsupported(
            "This provider does not report weekly usage".to_owned(),
        ));
    }
    let Some(credential) = &target.credential else {
        return Err(UsageReadError::Unavailable("Signed out".to_owned()));
    };
    if target.provider_id == CLAUDE_PROVIDER {
        crate::claude::read_weekly_usage(client, credential, now_ms).await
    } else {
        crate::codex::read_weekly_usage(client, credential, now_ms).await
    }
}

/// Turns one read into its public state. A failed read keeps the previous window as stale;
/// signed-out, expired and unsupported accounts never show an old window.
fn settle(
    target: UsageTarget,
    result: Result<WeeklyUsageWindow, UsageReadError>,
    previous: Option<&ProviderAccountUsage>,
    now_ms: u64,
) -> ProviderAccountUsage {
    let mut usage = ProviderAccountUsage {
        provider_id: target.provider_id,
        account_id: target.account_id,
        account_label: target.account_label,
        state: ProviderUsageState::Available,
        weekly: None,
        observed_at_ms: None,
        safe_reason: None,
    };
    match result {
        Ok(weekly) => {
            usage.weekly = Some(weekly);
            usage.observed_at_ms = Some(now_ms);
        }
        Err(UsageReadError::Unsupported(reason)) => {
            usage.state = ProviderUsageState::Unsupported;
            usage.safe_reason = Some(reason);
        }
        Err(UsageReadError::Unavailable(reason)) => {
            usage.state = ProviderUsageState::Unavailable;
            usage.safe_reason = Some(reason);
        }
        Err(UsageReadError::Failed(reason)) => {
            let last_known = previous.filter(|previous| previous.weekly.is_some());
            match last_known {
                Some(previous) => {
                    usage.state = ProviderUsageState::Stale;
                    usage.weekly.clone_from(&previous.weekly);
                    usage.observed_at_ms = previous.observed_at_ms;
                }
                None => usage.state = ProviderUsageState::Failed,
            }
            usage.safe_reason = Some(reason);
        }
    }
    usage
}

/// Maps the HTTP status and reads a bounded JSON body. Provider bodies are never echoed.
pub(crate) async fn read_usage_response(
    mut response: reqwest::Response,
) -> Result<Value, UsageReadError> {
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(UsageReadError::Unavailable(
            "Sign-in was rejected".to_owned(),
        ));
    }
    if !status.is_success() {
        return Err(UsageReadError::Failed(format!(
            "Provider returned HTTP {}",
            status.as_u16()
        )));
    }
    let unreadable = || UsageReadError::Failed("Provider usage response was unreadable".to_owned());
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unreadable())? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(unreadable());
        }
    }
    serde_json::from_slice(&body).map_err(|_| unreadable())
}

pub(crate) fn unreachable_provider(error: &reqwest::Error) -> UsageReadError {
    if error.is_timeout() {
        UsageReadError::Failed("Provider usage request timed out".to_owned())
    } else {
        UsageReadError::Failed("Could not reach the provider".to_owned())
    }
}

/// A JSON number as a whole, non-negative percent.
pub(crate) fn whole_percent(value: &Value) -> Option<u32> {
    if let Some(whole) = value.as_u64() {
        return u32::try_from(whole).ok();
    }
    let fractional = value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)?;
    format!("{fractional:.0}").parse().ok()
}

/// Parses an RFC 3339 timestamp such as `2026-11-17T08:00:00.123+00:00` into Unix milliseconds.
pub(crate) fn rfc3339_to_unix_ms(value: &str) -> Option<u64> {
    let (date, rest) = value.split_once('T')?;
    let mut date_parts = date.splitn(3, '-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;

    let offset_start = rest.find(['Z', 'z', '+', '-'])?;
    let (time, offset) = rest.split_at(offset_start);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock_parts = clock.splitn(3, ':');
    let hour: i64 = clock_parts.next()?.parse().ok()?;
    let minute: i64 = clock_parts.next()?.parse().ok()?;
    let second: i64 = clock_parts.next()?.parse().ok()?;
    let millis: i64 = format!("{fraction:0<3}").get(..3)?.parse().ok()?;

    let offset_seconds = match offset {
        "Z" | "z" => 0,
        _ => {
            let sign = if offset.starts_with('-') { -1 } else { 1 };
            let (hours, minutes) = offset.get(1..)?.split_once(':')?;
            sign * (hours.parse::<i64>().ok()? * 3_600 + minutes.parse::<i64>().ok()? * 60)
        }
    };
    let valid = (1..=12).contains(&month)
        && (1..=31).contains(&day)
        && (0..24).contains(&hour)
        && (0..60).contains(&minute)
        && (0..=60).contains(&second);
    if !valid {
        return None;
    }
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second
        - offset_seconds;
    u64::try_from(seconds * 1_000 + millis).ok()
}

/// Days since 1970-01-01 in the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn credential_digest(credential: &Value) -> [u8; 32] {
    Sha256::digest(credential.to_string().as_bytes()).into()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn target(provider_id: &str, credential: Option<Value>) -> UsageTarget {
        UsageTarget {
            provider_id: provider_id.to_owned(),
            account_id: "account-1".to_owned(),
            account_label: "Work".to_owned(),
            credential,
        }
    }

    fn window(used_percent: u32) -> WeeklyUsageWindow {
        WeeklyUsageWindow {
            used_percent,
            resets_at_ms: Some(2_000),
        }
    }

    #[test]
    fn failed_read_keeps_the_last_window_as_stale() {
        let available = settle(target(CODEX_PROVIDER, None), Ok(window(42)), None, 1_000);
        assert_eq!(available.state, ProviderUsageState::Available);
        assert_eq!(available.observed_at_ms, Some(1_000));

        let failure = Err(UsageReadError::Failed("timed out".to_owned()));
        let stale = settle(
            target(CODEX_PROVIDER, None),
            failure,
            Some(&available),
            9_000,
        );
        assert_eq!(stale.state, ProviderUsageState::Stale);
        assert_eq!(stale.weekly, Some(window(42)));
        assert_eq!(stale.observed_at_ms, Some(1_000));
        assert_eq!(stale.safe_reason.as_deref(), Some("timed out"));
    }

    #[test]
    fn unknown_usage_never_carries_a_window() {
        let available = settle(target(CODEX_PROVIDER, None), Ok(window(42)), None, 1_000);
        for (error, state) in [
            (
                UsageReadError::Failed("down".to_owned()),
                ProviderUsageState::Failed,
            ),
            (
                UsageReadError::Unavailable("Signed out".to_owned()),
                ProviderUsageState::Unavailable,
            ),
            (
                UsageReadError::Unsupported("none".to_owned()),
                ProviderUsageState::Unsupported,
            ),
        ] {
            let previous = (state != ProviderUsageState::Failed).then_some(&available);
            let usage = settle(target(CODEX_PROVIDER, None), Err(error), previous, 2_000);
            assert_eq!(usage.state, state);
            assert_eq!(usage.weekly, None);
            assert_eq!(usage.observed_at_ms, None);
            assert!(usage.safe_reason.is_some());
        }
    }

    #[tokio::test]
    async fn cached_usage_does_not_outlive_its_token() {
        let credential = json!({"access_token": "secret", "expires_at_ms": 1});
        let reader = ProviderUsageReader::default();
        reader.cache.lock().await.insert(
            (CLAUDE_PROVIDER.to_owned(), "account-1".to_owned()),
            CachedUsage {
                read_at: Instant::now(),
                credential_digest: Some(credential_digest(&credential)),
                usage: settle(target(CLAUDE_PROVIDER, None), Ok(window(42)), None, 0),
            },
        );
        let report = reader
            .read(vec![target(CLAUDE_PROVIDER, Some(credential))])
            .await;
        assert_eq!(report.accounts[0].state, ProviderUsageState::Unavailable);
        assert_eq!(report.accounts[0].weekly, None);
    }

    #[tokio::test]
    async fn unsupported_and_signed_out_accounts_are_not_probed() {
        let reader = ProviderUsageReader::default();
        let report = reader
            .read(vec![
                target("cursor-agent", Some(json!({"token": "secret"}))),
                target(CLAUDE_PROVIDER, None),
            ])
            .await;
        assert_eq!(report.accounts[0].state, ProviderUsageState::Unsupported);
        assert_eq!(report.accounts[1].state, ProviderUsageState::Unavailable);
        assert_eq!(
            report.accounts[1].safe_reason.as_deref(),
            Some("Signed out")
        );
    }

    #[test]
    fn percents_are_whole_and_never_negative() {
        assert_eq!(whole_percent(&json!(37)), Some(37));
        assert_eq!(whole_percent(&json!(37.6)), Some(38));
        assert_eq!(whole_percent(&json!(104.0)), Some(104));
        assert_eq!(whole_percent(&json!(-1.0)), None);
        assert_eq!(whole_percent(&json!("37")), None);
    }

    #[test]
    fn rfc3339_timestamps_convert_to_unix_milliseconds() {
        assert_eq!(rfc3339_to_unix_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            rfc3339_to_unix_ms("2026-11-17T08:00:00Z"),
            Some(1_794_902_400_000)
        );
        assert_eq!(
            rfc3339_to_unix_ms("2026-11-17T08:00:00.123456+00:00"),
            Some(1_794_902_400_123)
        );
        assert_eq!(
            rfc3339_to_unix_ms("2026-11-17T09:30:00+01:30"),
            Some(1_794_902_400_000)
        );
        assert_eq!(rfc3339_to_unix_ms("2026-13-17T08:00:00Z"), None);
        assert_eq!(rfc3339_to_unix_ms("not a date"), None);
    }
}
