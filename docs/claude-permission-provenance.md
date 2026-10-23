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

Remaining limits: the cause of a given `sdk_unknown` cancellation is still not provable from the
bridge. The model is told not to stop and wait, but it may still do so; Nakode never continues
or replays the call automatically, so an owner continuation can still be needed.
Audits carry the same evidence. Provider-history snapshots retain host permission records
by exact session and call ID because the SDK's transcript does not save callback metadata.
History refresh cannot replace known host evidence with missing/unknown SDK evidence.

Offline regression fixtures exercise production bridge functions and query registration;
Rust coverage checks retained-history reconstruction. These are not live-provider proof.
Public SDK 0.3.280 / CLI 2.1.280 uses the generic STOP sentence for cancellation and
skipped batch results. That static-source finding does not establish why an earlier live
SDK invocation cancelled. Missing metadata in historical transcripts remains unknown.
