# Claude automatic permissions and security validation

Nakode's Claude sessions run with `permissionMode: "bypassPermissions"` by default, the Claude
equivalent of the Codex adapter's never-ask, full-access sessions: they run unattended, so Claude
Code's own permission prompts and auto-mode classifier do not stand between a session and its tools
(the classifier otherwise refuses dashboard tools such as ticket creation as external writes).
Archetype allow and deny lists still apply through the `PreToolUse` hook. Claude Code's
`permissions.defaultMode` is the owner's choice for interactive Claude Code and does not govern
Nakode; set `NAKODE_CLAUDE_PERMISSION_MODE` (`auto`, `acceptEdits`, `default` or `plan`) to run
Nakode's Claude sessions under a stricter mode. The rest of this page applies when that mode is
`auto`.

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

`AskUserQuestion` remains an actual question rather than a security decision. An owner chooses a stricter mode with `NAKODE_CLAUDE_PERMISSION_MODE`.
