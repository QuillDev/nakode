# Codex client compatibility update

## Upstream evidence

On 2026-10-15, GitHub's latest stable release API for `openai/codex`
reported `rust-v0.162.1` (published 2026-10-09).

- Release: https://github.com/openai/codex/releases/tag/rust-v0.162.1
- Catalogue: https://github.com/openai/codex/blob/rust-v0.162.1/codex-rs/models-manager/models.json

That catalogue includes `gpt-6.1-sol`. This is an upstream identifier, not
proof that any particular account has access.

## Change

Nakode's in-process Codex adapter now advertises client version `0.162.1`
instead of `0.155.1` on discovery and inference requests. Nakode does not
execute or bundle the Codex CLI for this adapter; installing a newer CLI
therefore does not update this version. Discovery remains authoritative:
`GET /codex/models?client_version=...`, with `/models` as the existing
404/405 fallback. No model is added to a hardcoded catalogue.

The native transport regression verifies that a discovered `gpt-6.1-sol`
can be sent unchanged to inference, with advertised reasoning efforts
preserved. Its mock catalogue is transport coverage, not account availability
evidence. Authentication, session persistence, defaults and effort policy
are unchanged.

## Deployment and availability

An outdated advertised client version is established; whether it caused the
reported absence of 6.1 is not established without authenticated discovery.
Account availability was not queried during this change. Rollout or access
restrictions can still exclude an upstream model after upgrading.

FStack bundles Nakode through matching runtime/SDK Git pins, its lockfile and
`enrollment-runtime/release.json`. Updating standalone Codex or Nakode on PATH
does not replace that engine. FStack's managed runtime pin update must consume
the published Nakode revision, then its managed application/machine update
must activate the new executable when safe. No running service or session was
restarted as part of this source change.
