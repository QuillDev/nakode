# Codex client compatibility update

## Upstream evidence

On 2026-11-17, the owner's installed Codex CLI reported `codex-cli 0.169.0`
and exposed Sol 6.2 and Astra 6.2. That is evidence about what client version
the Codex backend serves those models to, not proof that any particular
account has access through Nakode.

The previous update (2026-10-15) moved from `0.155.1` to `0.162.1`, based on
the then-latest stable release `rust-v0.162.1`, whose catalogue included
`gpt-6.1-sol`.

## Change

Nakode's in-process Codex adapter now advertises client version `0.169.0`
instead of `0.162.1` on discovery and inference requests. Nakode does not
execute or bundle the Codex CLI for this adapter; installing a newer CLI
therefore does not update this version. Discovery remains authoritative:
`GET /codex/models?client_version=...`, with `/models` as the existing
404/405 fallback. No model is added to a hardcoded catalogue and model
filtering is unchanged.

The native transport regression verifies that a discovered model can be sent
unchanged to inference with the advertised client version on both requests,
and that advertised reasoning efforts are preserved. Its mock catalogue is
transport coverage, not account availability evidence. Authentication,
session persistence, defaults and effort policy are unchanged.

## Deployment and availability

Account availability was not queried during this change. Rollout or access
restrictions can still exclude an upstream model after upgrading.

FStack bundles Nakode through matching runtime/SDK Git pins, its lockfile and
`enrollment-runtime/release.json`. Updating standalone Codex or Nakode on PATH
does not replace that engine. FStack's managed runtime pin update must consume
the published Nakode revision, then its managed application/machine update
must activate the new executable when safe, and the Codex provider's model
catalogue must be rediscovered. No running service or session was restarted as
part of this source change.
