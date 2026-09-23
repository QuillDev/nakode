# Durable ordinary follow-up inbox

## Current boundary

This is an unshipped runtime/API foundation, not a completed dashboard feature.
Nakode owns the ledger and scheduler. FStack consumes the public SDK; it must not
read or rewrite these tables. No migration of an existing live prompt queue is
required or performed.

The public contract exposes `EnqueueFollowup`, `SetFollowupPaused`, and
`ListFollowups`, with the `DurableFollowupInbox` capability. SDK mutations verify
that capability before sending and never fall back to `SendPrompt`. Mutation
callers must retain their command key across transport retries. A stable message
ID additionally deduplicates the same original request under a new command key;
changed content under either identity refuses. Existing attribution and timestamps
are not replaced by retries.

## Admission and delivery

- Mutations require the exact opened logical session, like ordinary prompt input.
  The authenticated transport remains the access boundary; a client ID is audit
  attribution, not proof of human identity or an ownership grant.
- Original-request digests and immutable materialized payloads are persisted in
  immediate SQLite transactions. Command and message replay checks precede
  attachment lookup. Runtime admission validates every attachment through the
  canonical converter; volatile image artifacts are frozen as bytes.
- Metadata pages use independently stored text/labels, never decode image payloads,
  and never restore providers. They expose individual sequence, message ID, sender,
  timestamp, state and batch ID, plus pending count, pause and unresolved state.
- The actor checks every 250 ms without a sliding debounce. Ordinary dispatch remains
  disabled until explicit inbox admission or runtime-owned durable child evidence exists.
  Report observation also runs without a mounted client; store failures retain evidence for retry.
  Only loaded, ready,
  nonbusy sessions with an empty legacy queue dispatch. Candidate pages rotate
  through at most 64 identities and wrap after exhaustion.
- Each transaction claims one bounded FIFO prefix at a fixed cutoff. Later arrivals
  remain pending. The continuation includes every selected original message, ordered
  metadata, and attachment offsets/counts; attribution is not new authority.
- A durable dispatch fence precedes provider effects. Provider acceptance records
  correlation but does not consume the batch. A matching started/completed turn
  consumes it. **Consumed means delivered, not task success.**
- A recoverable un-dispatched claim is reconstructed exactly. A fenced but
  unacknowledged batch remains visibly uncertain and is never automatically replayed.
  This is not an exactly-once execution guarantee.
- Newly accepted Stop pauses the inbox; rejected revision fences, missing replay-only
  receipts and cached Stop retries cannot undo a later explicit Resume. Resume can
  clear a pre-dispatch blocked claim, but does not reset uncertain dispatch.

Ordinary-message batching is **explicit opt-in through `EnqueueFollowup` only**.
Runtime-owned linked-child reports separately enter this same ledger with persisted evidence
origin and delivery receipts; see `docs/child-followup-delivery.md`.
`SendPrompt`/`EnqueuePrompt` retain the established visible queue for both Chat
and agent sessions, including initial prompts and retries. There is no automatic
conversion and no fallback after durable admission. Answers, approvals and urgent
controls remain separate. Dashboard sends do not opt into this unfinished inbox.

The ordinary visible queue separately persists exact IDs, order, attachments,
transport provenance and handoff in `session_prompt_queues`. Queue snapshots and
owner dispatch checkpoints commit together. Retained queries expose accepted queued
work without restoring a provider; explicit restoration permits ordered drain.
Stop interrupts active work without discarding accepted ordinary follow-ups.

Queued native steering records a delivery-uncertainty fence before dispatch. If its
acknowledgement is lost or cannot be checkpointed, restart never automatically sends
that guidance again. The retained message stays visible; sending/steering refuses
with recovery guidance until the owner reviews and explicitly removes that exact ID.
Removal does not undo provider effects. This is separate from the inbox's unfinished
uncertain-batch reconciliation API below. Older executables that never persisted
queued admission must drain accepted work before an executable transition.

## Bounds

- Message IDs and command/client identities: 1–200 bytes.
- Message text: at most 64 KiB; at most eight validated attachments.
- Outstanding inbox: 256 messages / 64 MiB. Overflow explicitly refuses admission;
  previously accepted requirements remain intact.
- Batch: at most 32 messages, 128 KiB of exact composed text including escaped
  attribution headers, eight attachments and 20 MiB of inline image bytes.
- Metadata page: 1–64 items, at most 256 KiB of message text; remaining rows are
  indicated by `has_more` and paged by the last returned sequence.

## Validation and unfinished work

Store and fake-provider runtime tests cover ordered cutoff/arrivals, concurrent
producers/claimers, idle batching, receipts and original-message retries, attachment
retention, metadata-only reads, escaped-header bounds, capacity, pause/replay,
legacy queue coexistence, invalid-path admission and uncertain restart handling.
SDK loopback gRPC tests cover capability refusal and exact request identity.
These are not live-provider or cross-host delivery tests.

Still required:

1. Explicit reconciliation/resolution for uncertain dispatch; never fabricate delivery
   or blindly replay it. Retained idle-session restoration and background scheduling.
2. Full lifecycle/transport/profile authorization and compatibility testing, including
   many-session fairness and persistence-failure injection.
3. Stable end-to-end dashboard producer identities, pending/batched/consumed UI,
   explicit recovery controls and bounded paging/overflow presentation.
4. Real restart, remote transport and browser integration coverage.

The current FStack stack build intentionally uses sibling SDK/runtime paths with
owner permission. Before standalone release, both dependencies, Cargo.lock and
runtime release metadata require one coordinated published revision.
