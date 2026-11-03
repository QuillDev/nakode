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
| Approximately 19:38 | Owner reported that the original agent continued on its own. No exact provider completion timestamp was supplied. |

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
- Backend warnings already enter the canonical transcript and status message via
  `src/state.rs::handle_warning`. This patch uses that path, not client-side idle
  inference. No FStack change or runtime pin update is included.
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
- bounded content-free quiet diagnostics that never settle input.

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

## Remaining correlation / live verification

A complete incident diagnosis still needs the exact turn's sanitized ordered SDK
state/background/result/EOF events, native process spawn/close times, any awaited
control/notification boundary, and canonical status snapshots. Correlate those with
inbox admission and claim times; do not infer causality from prose or long silence.
Use scoped public read-only diagnostics where available, never private credential
storage, broad account log dumps, live database edits or uncertain request replay.
If no historical trace exists, the new bounded diagnostics improve the next
observation but cannot prove this past event.

The patch is not deployed. Live verification requires an owner-authorized runtime
update at a safe lifecycle boundary. No merge, auto-merge, queue, deployment, host
restart, original-session replacement or browser takeover is authorized by this work.
