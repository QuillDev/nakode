# Durable session claim and transfer

`ReparentChildSession` is a public gRPC/SDK command for an authenticated pending
`ClaimAgent` or `TransferAgent` external call. The source logical session and call
are supplied by the trusted executor, not by model-selected new-parent arguments.
New transitions verify the exact pending call and its child, expected parent and
relationship revision. Both logical sessions must have the same non-null bound
profile on this runtime. Native run IDs are not durable session IDs.

`session_child_links` remains the sole parent authority. Additive revision metadata
tracks every insertion, parent change and removal, including parent deletion.
An authoritative orphan starts at revision zero; orphaning an existing child never
resets its fence. Legacy linked rows acquire revision one. Optional public revision
fields distinguish updated runtimes from older runtimes with no adoption support.

Claims require no current parent. Transfers require an explicit different expected
parent. The SQLite IMMEDIATE transaction checks both parent and revision, preserves
one-level relationships and the 32-child cap, then records an immutable audited
transition keyed by command and source call. Exact receipt replay never reapplies
an earlier transition after a subsequent transfer. Different arguments cannot reuse
the receipt. Same-profile authorization also applies to receipt reads.

## Notification boundary

The transition refuses while affected upward child evidence or downward delegated
instructions are pending, claimed or dispatching. These messages are not moved,
reopened or replayed. Pending messages can drain through the existing inbox or be
explicitly withdrawn; uncertain dispatch retains the existing recovery fence.
Consumed metadata remains history and causes no duplicate wakeup. Reports admitted
after transfer use the new canonical parent. Downward instruction authority also
uses the current link; an old parent's later peer message is not a delegated task.

The relationship/inbox checks and link update share the persistence database and
transaction, and actor authentication/publication does not yield around mutation.
Active turns, transcripts, workspaces, provider sessions and execution machines are
unchanged. Both parents and the child receive replacement projections. No provider
is restored, cancelled, restarted or duplicated by this command. Archived child or
new-parent bridges refuse adoption; reopening is a separate explicit operation.

## Verification

`src/child_reports/relationships/tests.rs` covers claims, transfer and receipt
persistence, ABA, competing transactions, profile/native/closed/cycle rejection,
upward and downward pending/inflight refusal, withdrawal, future routing and
parent-deletion fences. The runtime followup relationship test covers exact-call
authentication, running-turn/transcript preservation, replacement projections and
receipt replay after external-call settlement.

Cross-runtime relocation, native delegation adoption and automatic message migration
are unsupported. No deployment or production reassignment is required by this feature.
