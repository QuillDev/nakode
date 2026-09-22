# Codex compatibility baseline

Nakode's production `openai-codex` provider is an in-process adapter. It does not
execute a Codex CLI, install `@openai/codex`, or link a Codex Cargo dependency.
Updating a separately installed `codex` executable does not update Nakode.

`src/codex/native.rs` advertises **0.155.1**, updated from **0.153.2**, in both
the model-discovery query and the discovery/inference version headers. This is
a protocol compatibility baseline, not a bundled CLI version or a guarantee
that every upstream feature is implemented.

## Upstream verification

The stable release was verified on 2026-09-22 against:

- [GitHub stable release](https://github.com/openai/codex/releases/tag/rust-v0.155.1)
- [npm package metadata](https://registry.npmjs.org/@openai%2fcodex)
- [Release model metadata](https://github.com/openai/codex/blob/rust-v0.155.1/codex-rs/models-manager/models.json)
- [App-server effort option schema](https://github.com/openai/codex/blob/rust-v0.155.1/codex-rs/app-server-protocol/schema/typescript/v2/ReasoningEffortOption.ts)

The released GPT-6 identifier is `gpt-6-astra`; Nakode qualifies it as
`openai-codex/gpt-6-astra`. The release metadata advertises Astra efforts
`low`, `medium`, `high`, `xhigh`, `max`, and `ultra`. These are verification
facts, not a Nakode model or effort allowlist. Account-scoped discovery remains
authoritative, including additional model IDs and future effort values.

## Discovery contract

Native discovery reads each model's `supported_reasoning_levels[].effort`.
The optional process compatibility adapter reads
`supportedReasoningEfforts[].reasoningEffort`. Effort order and exact values
are preserved, duplicate values are removed, and absent/empty metadata does
not invent supported options. The former global six-effort list is retained
only as a synthetic shared-test fixture.

An older live Nakode advertising `none/low/medium/high/xhigh/max` for every
Codex model is not evidence that the provider supports those efforts for every
model. In particular, the upstream `default_reasoning_summary: "none"` is not
a reasoning effort.

Discovery does not change saved model defaults or session selections. Selected
models and efforts continue through the existing public API and native request
encoder without a GPT-6 alias or fallback. Provider access still depends on the
configured account, server-side rollout, and saved provider model filters.

## FStack rollout

FStack embeds Nakode in `fstack agent-runtime`; it must consume a published,
validated Nakode revision containing this change. Its `Cargo.toml`
(`nakode-runtime` and `nakode-sdk`), `Cargo.lock`, and
`enrollment-runtime/release.json` must all name the same public revision.
`node enrollment-runtime/verify-runtime.mjs` verifies their agreement.

At implementation time those pins still name
`61b8c47399742d4006c2326d9301b3e1eb877ab8`, which advertises 0.153.2. They cannot
ship this uncommitted adapter change. Publish the validated Nakode commit with
owner approval, update all three FStack pin locations, and validate the bundled
build before release. Do not substitute an unpublished hash or checkout-relative
path dependency for the public release pin.

After an approved build/install, explicit activation/restart of the owning
Nakode/FStack runtime is required; replacing source or upgrading the user's CLI
does not change a running service. Refresh model discovery on that execution
machine afterward. No deployment, service restart, or publication is implied by
these source changes.
