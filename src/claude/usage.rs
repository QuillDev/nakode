//! Claude subscription weekly quota, read from the endpoint Claude Code's `/usage` uses.

use nakode_protocol::WeeklyUsageWindow;
use serde::Deserialize;
use serde_json::Value;

use crate::provider_usage::{
    REQUEST_TIMEOUT, UsageReadError, read_usage_response, rfc3339_to_unix_ms, unreachable_provider,
    whole_percent,
};

const USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const OAUTH_BETA: &str = "oauth-2025-04-20";

/// The stored sign-in fields a usage read needs; refresh material is deliberately ignored.
#[derive(Deserialize)]
struct UsageCredential {
    access_token: String,
    expires_at_ms: u64,
}

pub(crate) async fn read_weekly_usage(
    client: &reqwest::Client,
    credential: &Value,
    now_ms: u64,
) -> Result<WeeklyUsageWindow, UsageReadError> {
    let credential = UsageCredential::deserialize(credential)
        .map_err(|_| UsageReadError::Unavailable("Sign-in is not a Claude sign-in".to_owned()))?;
    if credential.expires_at_ms <= now_ms {
        return Err(UsageReadError::Unavailable("Sign-in expired".to_owned()));
    }
    let response = client
        .get(USAGE_URL)
        .bearer_auth(&credential.access_token)
        .header("anthropic-beta", OAUTH_BETA)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|error| unreachable_provider(&error))?;
    let payload = read_usage_response(response).await?;
    weekly_window(&payload)
}

/// `seven_day` is the account-wide weekly window; per-model weekly windows are not included.
/// `utilization` is already a whole percent from 0 to 100, above 100 once over.
fn weekly_window(payload: &Value) -> Result<WeeklyUsageWindow, UsageReadError> {
    let weekly = payload
        .get("seven_day")
        .filter(|window| !window.is_null())
        .ok_or_else(|| UsageReadError::Unsupported("No weekly limit reported".to_owned()))?;
    let used_percent = weekly
        .get("utilization")
        .and_then(whole_percent)
        .ok_or_else(|| UsageReadError::Failed("Weekly usage was unreadable".to_owned()))?;
    let resets_at_ms = weekly
        .get("resets_at")
        .and_then(Value::as_str)
        .and_then(rfc3339_to_unix_ms);
    Ok(WeeklyUsageWindow {
        used_percent,
        resets_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn seven_day_window_is_the_weekly_usage() {
        let payload = json!({
            "five_hour": {"utilization": 91.0, "resets_at": "2026-11-10T18:00:00+00:00"},
            "seven_day": {"utilization": 34.0, "resets_at": "2026-11-17T08:00:00.000000+00:00"},
            "seven_day_opus": {"utilization": 60.0, "resets_at": "2026-11-17T08:00:00+00:00"}
        });
        let window = weekly_window(&payload).expect("weekly window");
        assert_eq!(window.used_percent, 34);
        assert_eq!(window.resets_at_ms, Some(1_794_902_400_000));
    }

    #[test]
    fn absent_weekly_window_is_unsupported_and_unreadable_usage_fails() {
        for payload in [json!({"five_hour": null, "seven_day": null}), json!({})] {
            assert!(matches!(
                weekly_window(&payload),
                Err(UsageReadError::Unsupported(_))
            ));
        }
        let unreadable = json!({"seven_day": {"utilization": null, "resets_at": null}});
        assert!(matches!(
            weekly_window(&unreadable),
            Err(UsageReadError::Failed(_))
        ));
    }

    #[test]
    fn unknown_reset_time_stays_unknown() {
        let payload = json!({"seven_day": {"utilization": 0, "resets_at": null}});
        let window = weekly_window(&payload).expect("weekly window");
        assert_eq!(window.used_percent, 0);
        assert_eq!(window.resets_at_ms, None);
    }
}
