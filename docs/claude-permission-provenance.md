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

`decision` is `allow`, `deny`, `pending`, `interrupted`, or `unknown`. The record also
retains bounded `reason` and the selected permission `mode` when available.
Raw tool output remains unchanged. None of these fields authorizes retries or bypasses.
Runtime stop still aborts the SDK query; a late approval cannot restart it.

System guidance and PreToolUse/PostToolBatch context explain provenance to the model.
Audits carry the same evidence. Provider-history snapshots retain host permission records
by exact session and call ID because the SDK's transcript does not save callback metadata.
History refresh cannot replace known host evidence with missing/unknown SDK evidence.

Offline regression fixtures exercise production bridge functions and query registration;
Rust coverage checks retained-history reconstruction. These are not live-provider proof.
Public SDK 0.3.280 / CLI 2.1.280 uses the generic STOP sentence for cancellation and
skipped batch results. That static-source finding does not establish why an earlier live
SDK invocation cancelled. Missing metadata in historical transcripts remains unknown.
