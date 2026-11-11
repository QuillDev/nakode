# Provider weekly usage

`GetProviderUsage` returns the weekly quota of every enabled account of every enabled provider.

| Provider | Source | Weekly window |
| --- | --- | --- |
| `claude-agent` | `GET https://api.anthropic.com/api/oauth/usage` | `seven_day`; `utilization` is a whole percent |
| `openai-codex` | `GET https://chatgpt.com/backend-api/wham/usage` | the `rate_limit` window whose `limit_window_seconds` is 604800 |

Other providers report `UNSUPPORTED` without a request.

## States

Only `AVAILABLE` and `STALE` carry a window. Unknown usage is never zero.

- `AVAILABLE` — read on the latest request.
- `STALE` — the latest read failed; the window and `observed_at_ms` are the last successful read.
- `UNSUPPORTED` — no weekly limit for this provider, plan or account.
- `UNAVAILABLE` — signed out, expired or rejected sign-in.
- `FAILED` — the read failed and no earlier window is known.

`safe_reason` is a short fixed phrase; provider bodies and credentials are never returned or logged.

## Limits

- Reads use the stored access token and never refresh it: refresh tokens rotate, so only provider
  supervisors refresh. An idle account whose token expired reports `UNAVAILABLE` until the provider
  is used again.
- Each account's result is reused for five minutes and on an unchanged sign-in; concurrent queries
  share one read. Model discovery and sessions never wait on usage.

`src/provider_usage.rs`, `src/claude/usage.rs`, `src/codex/usage.rs`.
