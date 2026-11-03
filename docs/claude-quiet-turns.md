# Claude partial-output / quiet-turn investigation

## Incident evidence and limits

The reported logical session is `01a18474-c949-7e21-8cfa-89d1f24b58f4`,
model `claude-agent/opus`, on the reported Mac execution host. The observations below
were supplied by the owner from authenticated parent tools; they are not a provider event trace.

| UTC, 2026-11-03 | Observation |
| --- | --- |
| 17:29:54.753 | Inbox sequence 1599's supplied received timestamp, `1793726994753`. |
| 17:35:55 | Previous visible tool completed successfully, according to the parent inspection. |
| 17:36:23.429 | Last reported transcript update: partial assistant readiness report awaiting a browser slot. Inspector preview size was 5,307 bytes. |
| 19:31:04.907 | Inventory reported working; sequence 1599 remained pending, batch null, paused false, unsettled batch null, blocked reason null. |
| 19:34:18 | Parent reports receipt of the original turn's terminal report: Redis/API/Vite had stopped at a two-hour background limit; UAT had not run. This is report receipt time, not a process-close timestamp. |
| Approximately 19:38 | Owner reported that the original agent continued on its own. |
| 20:25:32 | Parent reports receipt of UAT completion after the next turn consumed the pending slot grant. This is report receipt time, not an SDK boundary. |

The observed transcript-to-inventory gap is **1h54m41.478s**. The grant's received
timestamp precedes the final reported text update; it must not be described as an
arrival after that update. Preview size and `partial` do not establish provider
stream truncation. No historical SDK event/result, process-close, or control-request
trace was available to this investigation. A long sleep, notification wait, inference
stall, history-read delay, and projection lag remain hypotheses, not an attributed cause.
The original session, history, workspace and browser were not opened, interrupted or modified.

## Established lifecycle mechanism

- `src/claude/bridge.mjs::turnInput` supplies one prompt through streaming input.
  Claude Code can emit a result and idle while non-ambient background Bash/Monitor
  tasks remain. Those tasks may start further inference and hook-gated tool calls.
- Closing input at the first result can break those callbacks with `Stream closed`,
  producing unattributed SDK cancellation. Changes `e73bb88` and `c61613f` document
  this recurring class and its isolated fixtures. A partial assistant report is not
  a legitimate release boundary.
- `sendTurn` drains the SDK iterator and then waits for
  `src/claude/process_lifecycle.mjs`'s subprocess **close** barrier before publishing
  `turn_completed`. Iterator settlement alone cannot release the resumable provider
  session (`3fb4d93`). No change here bypasses that barrier.
- `src/state.rs::complete_turn` retires the active turn and finalizes partial rows.
  `src/server/runtime/followups.rs::dispatch_followups` requires a ready, nonbusy
  session with an empty ordinary queue. It then uses existing durable claim,
  dispatch and acceptance fences. Waiting prose does not dispatch queued input.
- Backend warnings enter the canonical transcript via `src/state.rs::handle_warning`.
  Provider-normalized `TurnProgress` separately updates the public status message;
  no frontend infers idle or completion from this text. No runtime pin update is included.
- Nakode-managed `background_tasks` process supervision and the Claude SDK's native
  background task stream are separate channels. The bridge's managed task tool is
  request/reply (`background_task_request` / `resolve_background_task` in
  `src/claude/mod.rs`); no managed-task-terminal -> SDK notification delivery path
  was found. The reported two-hour service limit alone cannot establish causation.
- FStack `src/host.rs::project_nakode_session` derives running from Nakode's
  starting/running/cancelling active turn and passes `status_message` through.
  `apps/dashboard/src/hosts/hosted-nakode.ts` maps that running flag to working
  and preserves the status message as activity detail. Its
  `src/core/chat/inspect-agent.ts::inspectEntry` separately preserves partial
  and upstream byte/window bounds. No inspected projection justifies declaring
  idle from a truncated preview or elapsed silence. This is source inspection,
  not proof that the historical client was current or free of observation lag.

## Reproduced defects and bounded fix

These are source-level defects, **not proof of the historical incident's cause**:

1. An idle event before a result closed input prematurely. Settlement now requires
   a result for the current provider turn as well as idle and no live background tasks.
2. A task arriving during the existing 30-second follow-up grace did not revoke the
   timer. A duplicate idle could also bypass grace. New task/non-idle events revoke
   settlement; duplicate snapshots neither bypass nor extend it. The grace was not
   lengthened, and it sends no prompt or retry.
3. EOF without any result was reported as success. An earlier background turn's
   result could also survive into a later unfinished continuation. Missing-result
   EOF now fails with partial output retained; a new continuation invalidates its
   predecessor's result.
4. Cancellation followed by graceful EOF could report success. The authenticated
   runtime abort now remains authoritative even without an SDK exception.
5. `sendTurn` removed its cancel controller before waiting for subprocess close.
   Stop during that wait acknowledged the request but could no longer abort the
   process. The controller now remains reachable until release succeeds or fails;
   cancellation during release publishes interrupted only after the close barrier.

## Authoritative visible progress

The adapter emits distinct progress on observed boundaries: provider processing,
SDK background-task wait (count only), background follow-up grace, missing turn
result/settlement, response-history read, stream close and subprocess exit. These
are normalized `BackendEvent::TurnProgress` events, not provider wire types in
shared state. Their `waiting` flag is adapter-derived; clients never classify prose.

`src/state.rs` retains positive waiting progress for the matching active turn
independently of transient command acknowledgements. A queued owner send cannot
replace the reason with `Queued message N`. Processing clears the retained wait;
approvals, questions, failure and cancellation retain precedence. Late progress
from an old or completed turn is ignored. `src/state/projection.rs` supplies the
reason through the existing public `SessionView.status_message` and notice path.
No new protocol field or client-owned lifecycle policy is required. Turn ownership,
partial transcript finalization and durable inbox dispatch gates are unchanged.

FStack's shared `ComposerHint` previously replaced authoritative activity detail
with literal `Agent working` for long turns. The paired UI change renders the
canonical first line, retains elapsed/Connected and reconnection precedence, and
ellipsizes long detail independently of metadata. It never converts Working to
Idle; full diagnostics stay in transcript history. Older runtimes fall back to
existing status text. A runtime update is still necessary to emit the new reasons;
no such update has been performed.

Quiet-turn diagnostics emit at most three warnings per Nakode turn, after at least
60 seconds without recognized provider progress. They contain only a bounded event
trail (12 fixed-label entries), elapsed times, last known idle/running/unknown state,
result/input flags and non-ambient task count. They distinguish provider-event,
history-collection and subprocess-release waits. No prompts, tool arguments/results,
provider task IDs, environment, credentials or raw notification bodies are included.
These timers observe only: they do not stop a process, declare idle, dispatch input,
retry a request, change pause state, or infer an owner stop. They do not retroactively
instrument an already-running installed runtime.

## Deterministic validation

`src/claude/tool_provenance.test.mjs` exercises production bridge functions against
scripted SDK input, without inference, credentials, a live runtime or browser:

- late background/running/unknown events during settlement;
- duplicate idle/task snapshots, idle-before-result, normal background follow-up;
- partial-output EOF, stale predecessor result, duplicate running event;
- graceful cancellation, result/error/throw paths;
- terminal publication held until process release, with Stop effective during that wait;
- bounded content-free quiet diagnostics that never settle input;
- a simulated two-hour SDK background wait with partial final text, visible reason,
  delayed notification, no second provider request and one terminal result;
- public wait projection surviving queued instructions, clearing on processing,
  and rejecting stale/cancelling/terminal progress without releasing busy state.

Existing Rust runtime inbox tests cover ordered late arrivals, dispatch once at the
idle boundary, manual pause versus coordinator interruption, legacy FIFO coexistence
and uncertain-dispatch non-replay. The Rust Claude test embeds and executes the Node
fixtures, so the production embedded bridge is validated too.

Local ignored evidence: `.tmp/claude-validation.log` (initial 50 passing Node tests),
`.tmp/claude-baseline.log` (six semantic failures against the pre-change bridge),
`.tmp/claude-stop-old-code.log` (additional Stop-during-release semantic failure),
`.tmp/rust-test-all-20260614-stop-rerun.log` (final required all-target/all-feature
suite: 1,307 passed, three ignored), and `.tmp/rust-clippy-20260614-stop-rerun.log`
(final Clippy gate). The final suite also runs the updated embedded Node fixtures.
The baseline copies are isolated under `.tmp/claude-baseline` and
`.tmp/claude-stop-baseline`; they do not modify Git history or any live session.
Independent static reviews found no actionable defects in the lifecycle change or
its incremental release-wait cancellation fix. CI is independent validation, not
deployed behavior.

The initial slice's all-target/all-feature suite had 1,307 passes and three ignored.
Incremental progress validation reuses that unchanged evidence plus 52 passing Node
tests, focused Rust progress/queue-boundary/embedded-bridge tests and Clippy; logs
are `.tmp/progress-final-node.log`, `.tmp/progress-final-focused.log` and
`.tmp/progress-final-clippy.log`. The FStack slice has real component-render tests
for canonical reason/fallback/reconnect/terminal/unmatched timestamp behavior and
isolated scene screenshots, never browser UAT on the original scans pane.

## Remaining correlation / live verification

A complete incident diagnosis still needs the exact turn's sanitized ordered SDK
state/background/result/EOF events, native process spawn/close times, any awaited
control/notification boundary, and canonical status snapshots. Correlate those with
inbox admission and claim times; do not infer causality from prose or long silence.
Use scoped public read-only diagnostics where available, never private credential
storage, broad account log dumps, live database edits or uncertain request replay.
If no historical trace exists, the new bounded diagnostics improve the next
observation but cannot prove this past event.

The exact scoped parent request is the original scans session's lifecycle metadata
between **17:35:55 and 19:38 UTC**: result, state/background changes, task notification,
iterator EOF/error and subprocess close, plus any public external-call/control wait
and managed-task timeout receipt. Prioritize rows around 17:36:23 and the terminal
report; retain event timestamp/type, opaque turn/task/call IDs and reason only. If
these events were not recorded, report that absence rather than reconstructing them.

On a future occurrence after an authorized update, inspect the public session's
active turn ID and status message, pending inbox metadata, and bounded warning
rows. The visible reason identifies the current boundary; warning metadata gives
the last observed result/state/task/input flags and relative event times. Healthy
watch connectivity does not mean active inference, and a quiet wait never implies
permission to replay work. A terminal event and subprocess release—not elapsed
silence—must precede dispatch. No new credential or raw-provider-log collection
is required.

The patch is not deployed. Live verification requires an owner-authorized runtime
update at a safe lifecycle boundary. No merge, auto-merge, queue, deployment, host
restart, original-session replacement or browser takeover is authorized by this work.
