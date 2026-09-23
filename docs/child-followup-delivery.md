# Native completion and durable child continuation

## Two independent paths

Native `nakode_agent` calls execute concurrently in the read-only tool batch. Previously the
`join_all` result loop withheld **all** terminal tool events until the slowest sibling finished.
`src/runtime.rs` now publishes each completed audit inside that call's future. The ordered result
loop still retains the complete outputs in model history and checkpoints them in call order before
continuing inference. No native run is converted into a parent follow-up. The model still waits for
its requested batch before its next inference; only visible terminal publication is independent.

Provider-normalized `ToolCall`s are the runtime boundary; there is no separate
`multi_tool_use.parallel` implementation in this checkout. The regression tests exercise two
native delegation response channels, releasing the second before the first and observing its
terminal event before releasing its sibling. They do not prove a provider's remote wrapper behavior.

FStack renders the authoritative tool lifecycle as a compact native delegation row. The complete
report remains in its tool audit/detail inspector and retained child history, not an unsolicited
expanded report beneath the row.

## Durable linked sessions

The existing persistence-backed `session_child_links` relationship is the sole routing authority.
It is immutable, one-level, same-runtime, and validated against bound profiles (same workspace for
two unbound legacy sessions). Native orchestration-run IDs are not logical child session links.
There is no caller-supplied destination, ambient machine selection, or cross-runtime fallback.

`followups/child_events.rs` admits completed/failed/blocker/question reports through one immediate
SQLite transaction containing both inbox message and event delivery receipt. Completed/failed turn
reports already commit with the child's terminal turn. They include that exact turn's bounded final
assistant response, explicit truncation or missing-final metadata, never an earlier turn's answer;
see [coordination messages](coordination-messages.md). Live primary-session questions are observed
on the actor tick, deduplicated by exact runtime question ID, and retained as evidence about the
original interaction; they never answer it. Each question report carries a versioned JSON body with
the runtime question ID, exact scoped interaction ID, group ID and order, plus the canonical
`InteractionQuestionView`: logical question ID, title, full text, selection mode, option IDs, labels,
descriptions and recommended flags. This reuses the public interaction projection rather than
inventing a second ask shape. Per-item receipts preserve all observed items of grouped asks, even
when they arrive on different ticks. A parent receives the actionable content in its follow-up;
inspection is not required just to discover what was asked. The observation is not proof that the
interaction is still pending: revalidate it before any authorized answer. Explicit reports use
`PublishChildReport`. Progress and cancellation reports do not wake the parent.

Runtime-owned delivery receipts mark the inbox message as `durable_child_evidence`. Batch headers
carry that origin independently of client-supplied text/sender. Report content is JSON-encoded inert
evidence, never owner authorization, consent or approval. The continuation instructs the parent to
summarize, continue only already-authorized work, and use ask only for a genuine owner decision.
No approval resolver or child-restart operation is added. Reserved event message IDs cannot be
submitted through ordinary `EnqueueFollowup`.

## Delivery and recovery guarantees

- Admission is durable and replay-deduplicated per report sequence, including reopening the store.
  The receipt survives child/report deletion so pending evidence cannot turn into an ordinary
  owner message. Ownership is rechecked at admission and batch claim.
- The existing actor/inbox scheduler waits for a loaded, ready, idle parent and an empty ordinary
  queue. It never interrupts an active parent or restores a child. A bounded FIFO batch can combine
  several reports into one attributable continuation.
- Stop pauses the inbox; new reports remain pending until explicit `SetFollowupPaused(false)`.
  Stop replay cannot clear a later resume. Closed parents retain reports without dispatch; explicit
  reopen is required. Retained parents after runtime restart require explicit `OpenSession`; the
  observer never restores providers automatically.
- Dispatch fencing, acknowledgement correlation, and consumed state reuse the ordinary inbox.
  An uncertain fenced batch is **not replayed automatically**. Consumed means delivered, not task
  success. This is not exactly-once provider execution.
- Full inboxes retain reports for later admission; a full parent does not exclude another parent's
  candidates. Question reports respect the 4096-per-child report bound. Saturation leaves the
  original question pending and publishes a held-notification diagnostic. Structured question
  bodies also have a 16-KiB serialized limit: an oversized item is explicitly held without silently
  truncating its choices or suppressing other items. This limit is a delivery boundary, not an
  answer or permission to bypass inspection. Unlinked sessions remain inert.
- Deleted/changed ownership holds previously admitted evidence rather than rerouting it. The
  current inbox has no general per-item discard/uncertainty-reconciliation API; such recovery
  remains an explicit product gap, not permission to retry uncertain work.
- A crash before a pending question is observed cannot make an in-memory provider interaction
  durable. Once observed, its report survives, but the original question's restart availability
  follows `docs/linked-child-questions.md`. Delivery never fabricates an answerable interaction.

## Observed dashboard coordination gaps

The owner reported that consent ticket session `01a0ce7c-4365-7f40-97da-211e4451507a`
displayed a real product-choice ask while Chat received no notification; `ReadAgents` returned
`needsYou: true` but an empty question and options. Inspection of that session and other launched
sessions refused current-stack attribution. The screenshot establishes the visible question, not
the underlying transport payload or parent link. No refused session was accessed by another route.

Static FStack tracing finds two distinct contracts: `server/chat-agent-read.ts` reads scalar
interaction fields, while Nakode also projects grouped `InteractionView.questions`; compatibility
and lossless grouped projection must be coordinated with `fix/chat-agent-details-freshness`.
The checked-out Nakode projection populates scalar compatibility fields, so the observed empty
payload cannot be conclusively attributed to this source without an authorized live response and
its runtime revision. FStack main now includes the separately owned inspection-attribution fix
(`#507`, incorporated through `da3d4e0`): `core/chat/inspection-stack.ts` attributes retained history
through the active profile's registered ticket/branch/worktree identity, not cwd equality or checkout
readiness. This branch makes no resolver/guard changes. Its regression tests pass after integration,
but the original live session has not been re-inspected and its recovery is not claimed.

These gaps do not justify guessing parent links, rerouting to another machine, answering the
product choice, or treating child evidence as consent. Parent binding remains owned by
`feat/general-agent-parent-linkage`; queue consumption/UI by `feat/chat-followup-inbox-parity`. Structured
runtime wakeups are independent of ReadAgents summaries, but require a trusted link and a runtime
revision containing this implementation. This change does not demonstrate delivery to the reported
live session.

## Integration dependencies

FStack's checked-out creation path does not yet pass `parent_session_id`. The separately owned
`feat/general-agent-parent-linkage` integration must bind Chat-created ticket/general sessions to
the authenticated owning Chat before first task dispatch, using the existing capability-gated
parent-aware creation contract. This patch does not infer/backfill relationships from transcript
text or creation tool results. Unlinked sessions do not notify anyone.

`feat/chat-followup-inbox-parity` owns queue consumption/batching, `SendAgentMessage` integration,
and owner-facing controls. This patch only produces/routes notices and schedules wakeups through the
existing runtime ledger/consumer; it must reuse that branch's batch contract rather than add competing
queue-draining semantics. Multiple eligible notices share the existing batch (covered by the two-item
question runtime test). The receipt-backed provenance/header changes in `followups/batches.rs` are an
explicit integration overlap, not ownership of batch consumption. Asks remain structured child
evidence and approvals remain separate unresolved interactions, never ordinary context or consent.

FStack main also includes the independently owned native-freshness fix (`#508`) and request-scoped
consent update (`#509`). Their native-only activity semantics and explicit distinction between owner
requests and child evidence are compatible with this patch. Terminal publication changes no freshness
heuristics, and compact report presentation leaves retained native inspection intact.

FStack's bundled Nakode dependency must include these runtime changes before deployment. Neither
same-runtime fixture tests nor UI scenes establish cross-machine delivery. Cross-runtime links,
automatic restoration of retained parents, complete uncertain-delivery recovery UI, and integrated
FStack parent-creation E2E remain outside the guarantees established here. No deployment is part
of this change.
