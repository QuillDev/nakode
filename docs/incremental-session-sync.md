# Incremental session synchronization: implementation workshop

**Status:** Owner-approved direction; server-history prototype implemented; public protocol, SDK and FStack integration not implemented. **Owner:** Nakode. **Ticket:** d2b9c745-1ca0-438c-ab1a-083280c4735a. No merge or deployment approval is implied.

## 1. Abstract and scope

Long-running sessions should hydrate a bounded initial projection, then obtain complete changes since a session revision. Existing streaming entries remain mutable. Nakode owns history bookkeeping and recovery; frontends render SDK projections. Durable older history remains accessible through paging and is never deleted because it is outside the visible window.

This Markdown companion records the implementation workshop beside source. The original FStack HTML research proposal remains unchanged. The owner explicitly superseded the earlier restriction against incremental contracts; replacement watches remain the compatibility path until integration is verified.

## 2. Verified current system

- `src/server.rs::publish_session_state` builds a complete vector of semantic publications for one session update, then emits separate invalidations. `published_session` builds a **raw server projection**, not the SDK's fully hydrated history.
- `crates/nakode-protocol/src/view.rs::ViewEvent` already represents entry creation, patches, byte-offset deltas, windows, metadata, queues, interactions, todos and run changes.
- `crates/nakode-server/src/grpc.rs` uses these publications to reload replacement projections. Raw invalidation cursor progress is not proof that a complete session update was applied.
- `crates/nakode-sdk/src/lib.rs` hydration pages transcript/runs and then fetches nested bodies/text/artifacts. FStack Host requests `usize::MAX` hydration in session detail/watch.
- The existing synthetic SDK test at `crates/nakode-sdk/src/tests/hydration_cost.rs` repeats an identical snapshot over isolated real gRPC with an injected 1ms per RPC. At 16/128/512 entries it repeats 1/2/8 page RPCs plus 16/128/512 body RPCs. Observed totals approximately 60/440/1730ms. These are synthetic timings, not live incident attribution.

## 3. Decisions and options

| Option | Benefit | Limitation | Decision |
|---|---|---|---|
| Conditional replacement snapshots | Avoid unchanged responses | Still rebuilds/hydrates changed full history | Compatibility/recovery, not primary long-session solution |
| Diff fully hydrated snapshots | Smaller wire payload | Pays the same historical hydration first | Reject as primary optimization |
| Atomic semantic batches before SDK hydration | Changed-entry/control work can be independent of history size | Requires replay, projection and recovery correctness | Owner-approved direction |
| Shared streaming subscriptions | Reduces duplicate connection and request work | Does not itself reduce hydration | Later transport optimization after correctness |

## 4. Implemented native prototype

`src/server/session_sync.rs` adds an opt-in native producer on `ServerCore`. This is not an alternative frontend API. No gRPC capability advertises it; the SDK and FStack still use existing snapshots.

- `session_sync_snapshot` reads the existing bounded projection and establishes a cursor in one synchronous actor operation. It refuses a snapshot until its projection agrees with the existing publication baseline, with a retryable conflict. It never modifies that baseline to suppress legacy publications.
- `publish_session_state` records the **whole** existing publication vector before broadcasting its first invalidation. An unobserved session is not serialized or retained. Legacy event contents and replacement behavior are preserved.
- `session_sync_changes` returns all complete retained batches through the current cursor, or `ResetRequired`. It never returns a usable prefix followed by a hidden gap.
- Cursor identity is logical session ID + fresh history incarnation UUID + session revision. Recreating history, evicting a session, or restarting creates a different incarnation. Global transport event sequence is not a session cursor.
- An unchanged read has zero batches. Missing, future, inside-batch, expired or incompatible cursors reset. A non-monotonic producer update or baseline mismatch invalidates history.
- Each batch stores immutable serialized semantic events. JSON is private in-memory retention, not a public wire format. Serialization is capped before allocating an oversized retained batch.
- Current experimental budgets are **8MiB encoded bytes, 256 batches and 64 sessions globally**, with **120-second batch retention**. Overflow evicts complete least-recently-established/published session histories; oversized single batches force snapshot recovery. No subscriber blocks runtime publication.
- These are retained encoded-payload bounds, not claims about total allocator/RSS. Decoding/cloning response projections costs additional bounded work. Inactive expired batches are reclaimed on history access; global limits always bound retained payload.
- True session removal clears its history. Missing sessions retain existing lookup errors, rather than being silently recreated by reset.

The underlying raw projection/diff construction remains existing work. This prototype does **not** prove all server work is independent of total history size.

## 5. Proposed public contract and SDK rules

The language-neutral schema still needs to be added to `proto/nakode/v1/nakode.proto`, with explicit capability/version negotiation and generated mappings. The native prototype types are not a finalized public schema.

Proposed responses:

```text
Snapshot { session_id, incarnation, revision, bounded_projection }
Changes  { session_id, incarnation, through_revision,
           batches: [{ base_revision, next_revision, changes }] }
ResetRequired { reason }
```

Required SDK invariants:

1. Match endpoint identity, authorization scope, logical session, incarnation and protocol capability before applying.
2. For each batch, require `base_revision == locally_applied_revision`. Repeated completed responses are ignored, not appended twice. Unknown/out-of-order bases reset; never guess.
3. Validate and stage every change before atomically replacing the rendered SDK projection. Advance the cursor only after complete successful apply. Do not replay domain transitions.
4. Identify entries by stable ID. Each applied change revision versions that entry. Explicit `Running/Complete/Failed/Interrupted` status is authoritative even when no text changed.
5. Byte appends require exact expected offset and UTF-8 boundaries. Patches replace their explicit range/projection, not an assumed full historical body. A mismatch resets/selectively hydrates under the same cursor fence.
6. Windows define order and viewport membership. Window eviction is **not** a durable tombstone. Previously loaded older pages remain separately addressable. An evicted entry can still have subsequent authoritative updates; this must be solved before public enablement.
7. Queue IDs, owner/turn changes, approvals, interactions, run lifecycle, artifacts/audit references, current-owner anchors and omitted owner-tool projections must remain complete. Internal events need coverage auditing before being treated as a complete external projection contract.
8. Cancellation is an idempotent command, independent of observation retry. Observation recovery never replays a mutation.
9. Closing/deleting a session remains an authoritative lifecycle operation. Reset cannot swallow genuine NotFound/auth refusal or recreate a closed session.
10. New SDK + old server and old SDK + new server retain the existing snapshot path. No release pin changes until published compatible artifacts exist.

## 6. Worked timelines

**Streaming:** snapshot `(A,10)` includes entry E, Running. Batch `10→11` appends to E and changes queue state. SDK stages both, applies atomically, advances to 11. Batch `11→12` marks the **same E** Complete with no extra text. SDK advances to 12. It must not wait at E or infer completion from having received E once.

**Recovery:** SDK has `(A,10)`. The server evicts retained batches under the byte budget. A request from `(A,10)` receives ResetRequired, not partial batch 12. An authorized fresh bounded snapshot establishes `(B,12)`; late responses from A cannot overwrite B. Durable older entries remain in Nakode persistence and paging APIs.

**Publication boundary:** native state has changed but its publication baseline has not. A snapshot request returns a retryable conflict without creating history or consuming the legacy baseline. After publication, snapshot and subsequent deltas share the same baseline. This addresses the independent review finding on initial replay continuity.

## 7. Validation and measured acceptance

Implemented focused tests cover whole-batch replay including queue shape, continuation of an existing streaming ID after cursor advancement, textless terminal updates, duplicate/stale/missing baselines, cross-session/incarnation/future/inside-batch cursors, TTL/byte/count/session limits, and resnapshot incarnation changes. An authoritative core test drives real transcript mutation and publication (no provider). A long-history fixture checks small stream batches without removing the original history. These do not validate SDK application or real renderer behavior.

Final focused validation: **9 prototype tests passed**, plus the existing `large_streams_publish_bounded_deltas_instead_of_growing_snapshots` regression (**1 passed**). Log: `.tmp/final-focused-session-sync-20260912T113505.log`. `cargo clippy --locked --lib -- -D warnings`, focused Rust formatting and `git diff --check` passed. This is not the full repository test/all-features gate. Independent review identified the snapshot/publication baseline gap; the correction and regression test passed subsequent focused validation. No second independent review of that correction was performed.

Before public enablement:

- Compare incremental SDK projections to fresh canonical snapshots after every fault-injected batch: mixed text/status/control changes, UTF-8 splitting, patch after append, reorder, true deletion vs window eviction, approvals, delegated runs, tool audits and artifacts.
- Establish snapshot atomicity across commit/persistence failure, actor query scheduling, restart and epoch rollover. Test publication loss before/after first wakeup, slow consumers and queue overflow.
- Test cancellation, queued-send identity, disconnect/reconnect, session switch, genuine deletion and closed-session resume across real isolated gRPC → Host → relay → renderer, web and Electron.
- For 16/128/512/2048-entry synthetic sessions, count nested hydration RPCs and bytes for token-only, queue-only and cancel updates. Target: **zero unchanged historical body/artifact hydration RPCs** after initial hydration. Increasing unchanged history must not increase those RPC counts.
- Separately measure command acceptance → authoritative publication → watch delivery → render. Capture queue/admission/service durations and p50/p95/p99; do not subtract synthetic times from live latency.
- Native repository full fmt/test/clippy gates and independent review must pass before commit. Cross-repository integration and UX gates must pass before merge. PR453 remains draft and unmerged.

## 8. Rollout and rollback

1. Native atomic-history prototype and adversarial tests (this stage).
2. Versioned Protobuf capability, complete semantic projection coverage and SDK atomic apply. Keep disabled in FStack.
3. Opt-in isolated Host integration, selective hydration, older-page access and renderer parity.
4. Small explicitly approved canary with bytes/RPC/latency/reset-rate measurement. Stop for increased missed/duplicate updates, cancellation delays, auth/isolation violations, unexplained reset loops or loss of historical access.
5. Enable more broadly only after reviewed evidence. Roll back capability selection to unchanged replacement snapshots; retain durable data and treat old incremental cursors as invalid. Rollback does not delete history or require database rollback.

## 9. Remaining design and integration gates

- Prove internal semantic events cover every field of bounded `SessionView`, including window-omitted mutable entries and run/current-owner anchors. Existing events were invalidations, not a supported public reducer protocol.
- Specify exact entry revision representation, explicit tombstones, bounded page corrections and artifact/body version fencing in Protobuf.
- Decide response chunk budgets without advancing through incomplete batches; oversized updates currently reset. Avoid reset loops for permanently oversized raw projections.
- Define SDK ownership of retained older pages and shared subscriptions; no frontend orchestration.
- Confirm actor commit/publication/snapshot ordering under persistence failure rather than inferring durability from a revision number.
- Live latency attribution, collector configuration/health and measured production improvement remain unknown. Telemetry behavior is unchanged by this prototype.
