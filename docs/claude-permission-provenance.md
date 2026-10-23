# Claude tool permission provenance

The Claude adapter retains permission evidence independently of provider tool output.
A provider cancellation sentence is not an authenticated owner decision.

| Source | Meaning |
| --- | --- |
| `policy` | Nakode archetype/tool policy rejected the call |
| `validator_allow`, `validator_reject`, `validator_error`, `validator_escalation` | Security validator outcome |
| `approval_pending`, `approval_accepted`, `approval_declined` | Actual Nakode approval lifecycle |
| `runtime_interrupt`, `runtime_stop` | Nakode runtime cancellation, not necessarily a human click |
| `sdk_abort` | SDK permission callback aborted without a Nakode runtime interrupt |
| `sdk_policy` | SDK permission-denied event; `sdkReasonType` retains its discriminator |
| `sdk_unknown` | Generic SDK cancellation/skipped-call output without an attributed decision |
| `sdk_retracted` | A model refusal fallback retracted the call's delivered result |

`decision` is `allow`, `deny`, `pending`, `interrupted`, or `unknown`. The record also
retains bounded `reason` and the selected permission `mode` when available.
Raw tool output remains unchanged. None of these fields authorizes retries or bypasses.
Runtime stop still aborts the SDK query; a late approval cannot restart it.

System guidance and PreToolUse/PostToolBatch context explain provenance to the model.
Unknown or retracted provenance is not an instruction to stop and wait: the model continues the
owner's task and checks whether the call took effect before relying on it, without replaying it.
PostToolBatch also recognises the generic text in `tool_response`, because the batch can run
before the SDK delivers the tool-result message.

Claude Code 2.1.304 uses the generic STOP text only for SDK-internal cancellation, including
in-flight tools discarded by a model refusal fallback. The SDK names those retracted results in
`model_refusal_fallback.retracted_message_uuids` and `supersedes`; the bridge re-attributes the
matching calls as `sdk_retracted` and emits a warning. `model_refusal_no_fallback` emits a warning
that the turn ended on a refusal. Neither replaces a recorded denial or runtime stop.

When a turn has `sdk_unknown` cancellations, the bridge emits one warning at turn end. It lists the
SDK lifecycle events of that turn (for example `task_notification`, `api_retry`, hook events) with
their offsets, so the next occurrence can be diagnosed from the transcript instead of provider storage.

## Background-task turns

The recurring `sdk_unknown` cancellation had one established cause. For a string prompt, the SDK
closes Claude Code's stdin at the first `result`. A background Bash or Monitor task keeps the
process alive and starts another turn when it finishes. In that turn Claude Code's control
requests fail with an `AbortError("Stream closed")`, the SDK-callback PreToolUse hook rethrows it,
and the tool call is cancelled with the generic STOP text. Nakode received no stop, so it was
`sdk_unknown`.

The bridge now always sends streaming input, sets `CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1`, and
keeps input open until Claude Code reports `session_state_changed` `idle`. Claude Code emits `idle`
only after the last background-driven turn, so follow-up turns keep their hooks however late they
start. Turns with background tasks stay open while those tasks run, as Claude Code's process already
did. A CLI that emits no session state falls back to closing at the first `result` (the old
behaviour, still attributed as `sdk_unknown`).

Remaining limits: other SDK-internal cancellations (refusal fallback, ambient watchers, unknown
CLI paths) still yield `sdk_unknown`/`sdk_retracted`. The model is told not to stop for them, but it
may, and Nakode never continues or replays the call automatically.
