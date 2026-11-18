# Incremental session synchronization

**Status:** Nakode public, opt-in `nakode.v1` contract on the integration branch. Not a rollout in FStack or a published SDK release. No database change. The original workshop's native history prototype is now the replay boundary for this API.

## Following a session

Prefer `NakodeClient::follow_session(id)` for a stream of render-ready **bounded** `SessionState` values. It does not fetch historical entry bodies, artifacts or older transcript pages. Do not feed every item into `hydrate_session_with_refresh(..., usize::MAX)`: that would put the old cost back. `follow_session_versioned(id)` also returns `FollowedSession.cursor` and `entry_revisions` keyed by stable entry ID. The cursor is absent on the legacy path. Dropping the stream cancels its SDK producer; it does not cancel a turn.

For generated clients:

1. Check `GetServerInfo.capabilities` for the exact string `SessionSyncV1`. Absent: use `WatchSession`. Existing watches, including the per-stream `transcript_changes` option, do not resume from these cursors.
2. Call `NakodeService.GetSessionSyncSnapshot(GetSessionRequest { session_id })`. `SessionSyncSnapshot` supplies a bounded `SessionState` and `SessionSyncCursor { session_id, incarnation, revision }` from the same actor turn. This RPC always supplies the full bounded projection; it is not a launch-critical or parent-only watch. A publication boundary still in flight returns retryable `ABORTED`. A retained, unattached session has no live producer: the snapshot returns `FAILED_PRECONDITION` with `nakode-sync-retained: 1`. Use the ordinary read-only replacement watch; do not open a provider to observe it.
3. Call `NakodeService.FollowSession(FollowSessionRequest { session_id, after })`. It is a server stream of `SessionSyncUpdate`, one whole `SessionSyncBatch` per message. A current cursor waits for a push, not periodic empty responses. The service subscribes to publication wakes before reading history; a lagged wake triggers the same actor replay read. Never use the global event sequence as a session cursor.
4. Resume with the **last applied** cursor on transport failure, using the same endpoint, credential and logical ID. A missing, future, incompatible, expired or non-boundary cursor yields `SessionSyncResetRequired` and ends the stream. Discard the baseline and get a fresh authorized snapshot. `NOT_FOUND` and authorization errors are errors, not resets. No observation retry replays a command or reopens a deleted session.

## What one batch means

`base` must equal the locally applied cursor. `next` is in the same incarnation and strictly advances. Stage the complete result before advancing. Exact completed batches may be ignored; unknown bases require recovery. An incarnation is an opaque history identity, not a provider session ID. Never share cursors across endpoints, users, or projections.

`projection` replaces all control and page metadata: queue items and IDs, turns, approvals, external tools, runs, audit/artifact references, owner anchor and omitted-tool counts. Its `transcript.entries` contains **only changed or created entry projections**. A present `TranscriptOrder` gives the complete new window order; absent means preserve order. Reconstruct the active owner agent transcript from the assembled session page. A sent entry replaces its explicit bounded byte range and metadata, including status; it is neither a byte append nor a full historical body. This avoids guessing UTF-8 offsets and supports a textless Running → Complete update. The SDK validates entry IDs, byte spans, order, session and revision before exposing a state. Each sent entry is versioned at `next.revision`.

Window eviction is not deletion. For scrollback use `NakodeClient::previous_session_page(session_id, &page, limit)`, keeping older pages separate. Only join a page whose `prefix_through` matches the newer page's `prefix_before`; a historical edit changes that proof even when no visible entry changed. Discard or reload a stale cached page when displaying it. This is invalidation by proof, **not** a live mutation feed for every off-screen row. Older entries and demand-loaded bodies remain addressable through the regular paging and body APIs. Do not treat an entry ID alone as an immutable body or artifact version.

## Producer and costs

The server diffs bounded entry projections, not fully hydrated histories and not individual invalidations. Public followers retain only complete patches; native event observers can additionally retain semantic events. Replay history is process-local: 8 MiB encoded payload, 256 batches, 64 sessions globally, 120-second batch TTL. Limits evict complete histories/batches; oversized batches force snapshot recovery. Inactive expiry is reclaimed on access. These are not RSS bounds. Snapshot establishment cannot consume the legacy publication baseline. Restart, removal and eviction cannot silently continue an old incarnation.

The warmed transcript proof index reuses the unchanged prefix and indexes visible positions, entry IDs, owner anchors, turn tool counts and code-mode parent references. A live-end entry update recomputes its suffix and projects the bounded viewport, not every historical body. Control state and runs are still bounded-view replacements, not field-level patches. SDK staging/render-state cloning costs the visible projection size, not just the number of changed bytes.

### Measured, isolated live-end update

`cargo test --lib --all-features one_live_entry_costs -- --nocapture` drives a native core, isolated Unix gRPC server and real Rust SDK. Nine two-byte appends per length, 256-byte historical rows; medians. Publication includes core commit, projection, diff and retention. Delivery is cumulative from mutation through SDK reduction; neither includes renderer work. Bytes are encoded `SessionSyncUpdate`, not HTTP/2 framing. The fresh canonical query used to check each answer is outside the measured interval.

| Entries | Changed rows | Update bytes | Historical hydration RPCs | Publish µs | Through SDK µs |
|---:|---:|---:|---:|---:|---:|
|16|1|772|0|497|1136|
|128|1|772|0|472|1049|
|512|1|812|0|2027|4165|
|2048|1|812|0|1660|4290|
|4096|1|812|0|1767|6809|

Debug timings vary with host load. A separate run of the same path measured 774/1616 µs at 2048 and 808/1661 µs at 4096. The reproducible bounds here are the single row, payload and zero historical RPCs, not a latency SLA. Compare the earlier replacement-hydration fixture: 16/128/512 rows required 1/2/8 page RPCs plus 16/128/512 body RPCs per pass.

**Limits of the result:** the initial cold index and snapshot cost history work. An edit deep in history, reordering, truncation, or a mutation invalidating from index zero can re-prove the suffix through the end, including affected image content. A change of code-mode parent membership rewinds as far as the earliest dependent child. Some legacy producer paths still scan: e.g. raw transcript-length lookup walks backwards to the oldest visible row (including hidden rows), recoverable-prompt projection resolves a transcript key by search, and delegated-run outcome construction may search a run transcript. Changed body digest computation costs the length of that body. Queues, runs, session discovery, persistence, and frontend rendering were not held to a constant-time guarantee by this experiment. There is no claim of constant time for arbitrary historical edits or of production latency improvement.

## Validation and rollout

Native retention tests cover discontinuity, over-budget, TTL, replacement incarnations and snapshot/publication ordering. SDK tests reject partial mixed control/order changes and preserve textless terminal status. The gRPC fixture compares reduced state to fresh canonical snapshots, replays nine complete batches, detects idle actor exit, and rejects stale historical pages after off-window correction. Failed durable deletion publishes the restored/disconnected projection even without shell activity.

FStack must opt in on compatible artifacts, preserve bounded presentation and page older history on demand. No dependency pin, renderer integration, production canary or deployment is part of this Nakode change. Roll back client selection to `WatchSession`; there is no data migration to undo.
