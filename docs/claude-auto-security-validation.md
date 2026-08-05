# Claude automatic permissions and security validation

Nakode's Claude adapter uses the authoritative Claude Code/Agent SDK setting
`permissionMode: "auto"` (Claude Code 2.1.212; `@anthropic-ai/claude-agent-sdk` 0.3.220) when the
resolved Claude settings do not name a mode. The adapter calls the SDK's `resolveSettings` and
`filterEscalatingDefaultMode`, so valid managed/user/project/local `permissions.defaultMode` values
remain explicit owner overrides. This policy is Claude-specific. The Codex adapter and its existing
unrestricted/automatic approval behavior are unchanged.

In auto mode routine repository reads, edits, builds, and tests are handled by Claude's classifier and
do not enter Nakode's owner-approval channel. When Claude routes a security-sensitive proposal to
`canUseTool`, Nakode delegates a bounded review to the configured
`NAKODE_SECURITY_VALIDATOR_AGENT` archetype (default slug `security-validator`). Configure that
archetype with a Sonnet model, or the closest Sonnet tier exposed by the provider, for example:

```toml
slug = "security-validator"
description = "Independently classify one proposed security-sensitive operation. Return the requested JSON only."
model = "claude-agent/sonnet"
```

The validator receives only the tool name, proposed input, and Claude's decision reason. Its required
result is JSON with `verdict` (`allow`, `reject`, or `escalate`) and `rationale`. The delegated run id,
archetype, verdict, rationale, and whether a valid result was actually obtained are emitted into the
parent transcript; Nakode's normal delegated-run state remains the authoritative attribution.

The boundary is fail-closed:

- `allow` runs the proposed operation;
- `reject` denies it with the validator's rationale;
- `escalate`, an unavailable/misconfigured archetype, provider failure, or malformed output denies the
  operation and explicitly says validation was unavailable or inconclusive;
- validator runs carry a marker that disables validation/delegation re-entry, preventing recursive
  validator loops.

`AskUserQuestion` remains an actual question rather than a security decision. An owner can also choose
a stricter persisted permission mode; this ticket changes the absent-setting default, not Claude's
configuration surface.
