//! `ChatGPT` weekly quota for a Codex sign-in, read from the same endpoint Codex's `/status` uses.

use nakode_protocol::WeeklyUsageWindow;
use serde::Deserialize;
use serde_json::Value;

use crate::provider_usage::{
    REQUEST_TIMEOUT, UsageReadError, read_usage_response, unreachable_provider, whole_percent,
};

const USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const WEEK_SECONDS: u64 = 7 * 24 * 60 * 60;

/// The stored sign-in fields a usage read needs; refresh material is deliberately ignored.
#[derive(Deserialize)]
struct UsageCredential {
    access_token: String,
    account_id: String,
    expires_at_ms: u64,
}

pub(crate) async fn read_weekly_usage(
    client: &reqwest::Client,
    credential: &Value,
    now_ms: u64,
) -> Result<WeeklyUsageWindow, UsageReadError> {
    let credential = UsageCredential::deserialize(credential)
        .map_err(|_| UsageReadError::Unavailable("Sign-in is not a ChatGPT sign-in".to_owned()))?;
    if credential.expires_at_ms <= now_ms {
        return Err(UsageReadError::Unavailable("Sign-in expired".to_owned()));
    }
    let response = client
        .get(USAGE_URL)
        .bearer_auth(&credential.access_token)
        .header("chatgpt-account-id", &credential.account_id)
        .header("originator", "nakode")
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|error| unreachable_provider(&error))?;
    let payload = read_usage_response(response).await?;
    weekly_window(&payload, now_ms)
}

/// Selects the window that lasts one week. Codex reports it as either the primary or the
/// secondary window depending on the plan, so position alone is not trusted.
fn weekly_window(payload: &Value, now_ms: u64) -> Result<WeeklyUsageWindow, UsageReadError> {
    let rate_limit = payload.get("rate_limit");
    let weekly = ["primary_window", "secondary_window"]
        .into_iter()
        .filter_map(|name| rate_limit?.get(name))
        .find(|window| {
            window.get("limit_window_seconds").and_then(Value::as_u64) == Some(WEEK_SECONDS)
        })
        .ok_or_else(|| UsageReadError::Unsupported("No weekly limit reported".to_owned()))?;
    let used_percent = weekly
        .get("used_percent")
        .and_then(whole_percent)
        .ok_or_else(|| UsageReadError::Failed("Weekly usage was unreadable".to_owned()))?;
    let resets_at_ms = weekly
        .get("reset_at")
        .and_then(Value::as_u64)
        .map(|seconds| seconds.saturating_mul(1_000))
        .or_else(|| {
            let after = weekly.get("reset_after_seconds").and_then(Value::as_u64)?;
            Some(now_ms.saturating_add(after.saturating_mul(1_000)))
        });
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
    fn weekly_window_is_chosen_by_length_not_position() {
        let payload = json!({
            "plan_type": "plus",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 81,
                    "limit_window_seconds": 18_000,
                    "reset_after_seconds": 600,
                    "reset_at": 1_700_000_600
                },
                "secondary_window": {
                    "used_percent": 23,
                    "limit_window_seconds": 604_800,
                    "reset_after_seconds": 86_400,
                    "reset_at": 1_700_086_400
                }
            }
        });
        let window = weekly_window(&payload, 1_700_000_000_000).expect("weekly window");
        assert_eq!(window.used_percent, 23);
        assert_eq!(window.resets_at_ms, Some(1_700_086_400_000));

        let primary_only = json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 7.4,
                    "limit_window_seconds": 604_800,
                    "reset_after_seconds": 3_600
                },
                "secondary_window": null
            }
        });
        let window = weekly_window(&primary_only, 1_000).expect("weekly window");
        assert_eq!(window.used_percent, 7);
        assert_eq!(window.resets_at_ms, Some(3_601_000));
    }

    #[test]
    fn missing_weekly_window_is_unsupported_not_zero() {
        let session_only = json!({
            "rate_limit": {
                "primary_window": {"used_percent": 0, "limit_window_seconds": 18_000}
            }
        });
        for payload in [session_only, json!({"rate_limit": null}), json!({})] {
            assert!(matches!(
                weekly_window(&payload, 0),
                Err(UsageReadError::Unsupported(_))
            ));
        }
    }

    #[tokio::test]
    async fn expired_sign_in_is_not_sent_to_the_provider() {
        let credential = json!({
            "access_token": "header.e30.signature",
            "refresh_token": "refresh",
            "account_id": "account-1",
            "expires_at_ms": 1_000
        });
        let result = read_weekly_usage(&reqwest::Client::new(), &credential, 1_000).await;
        assert_eq!(
            result,
            Err(UsageReadError::Unavailable("Sign-in expired".to_owned()))
        );
    }
}
