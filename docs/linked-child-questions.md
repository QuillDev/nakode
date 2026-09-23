# Linked child questions

## Implemented boundary

The persistence-backed native runtime exposes `ListChildQuestions` and `AnswerChildQuestions`
through the public protocol, gRPC and SDK. `LinkedChildQuestions` advertises this **same-runtime**
capability. Durable `LinkChildSession` associations supply the parent/child relationship.

- Listing returns original child interaction, group, question and option identities. It creates no
  parent interaction, owner prompt or provider session. Approvals are excluded.
- Every read rechecks current profile/workspace authorization and bridge lifecycle. Closed children
  are `closed`; unloaded or disconnected children are `unavailable`, not answerable history.
- An answer names the exact parent, child and interaction. The existing child resolver validates
  the entire grouped answer before removing any question or emitting backend commands.
- Multi-select options, recommendations, descriptions and free-text answers retain the ordinary
  question semantics. Invalid, incomplete and stale answers do not partially resume a child.
- Competing parent/direct-child answers are serialized by the same runtime actor. Only the first
  valid resolution emits the original child waiter commands; another answer with a different
  command key is refused once the ask is gone.
- Command receipts retain the **original parent command**, not its internal resolution command.
  Matching in-process retries replay the receipt without validating an already-removed waiter or
  emitting another resolution. Conflicting keys refuse. Replay-only misses cannot execute.
  An explicit revision fence targets the child's revision, not the parent's.
- Canonical pending questions retire on the current turn's terminal event, matching provider
  session closure or backend disconnection. A late completion from another turn cannot erase a
  live ask. A turn already being cancelled refuses new question answers.

## Important limitations

This is not durable ask execution or the completed dashboard feature. Question waiters and ordinary
command receipts remain process-local. Restart does not reconstruct a suspended tool or authorize
replaying its answer. A retained link survives, but it does not prove that a waiter is live.
There is no exactly-once execution guarantee or provider-resumption acknowledgement in this slice.

The parent snapshot is a query, not a new parent watch stream. The tests explicitly reread both
views; they do not prove automatic UI updates. Atomic parent-aware creation, authenticated
cross-host question routing, durable resolution/recovery, parent notifications, FStack Host/SDK
integration, nonblocking parent UI, attention aggregation and rendered end-to-end tests remain
required. FStack's bundled runtime pin does not yet deliver these sibling-source APIs.

## Verification

`cargo test -p nakode --lib child_questions` exercises original identity/answer payloads through
both command routes, concurrent answers, wrong-child refusal, in-process receipt replay, key
conflicts, replay-only misses, revision fences, atomic grouped validation, ownership rechecks,
cancellation/disconnection and retained-child unavailability. The harness uses persisted temporary
sessions and fake backend command channels, not live providers, distributed hosts or browser UI.

`cargo test -p nakode --lib state::tests` covers the broader domain transitions affected by
question retirement. `cargo clippy -p nakode --tests -- -D warnings` checks production and test
code without lint exemptions.
