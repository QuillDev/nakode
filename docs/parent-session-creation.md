# Parent-aware durable session creation

## Public contract

`CreateSessionRequest.parent_session_id` (optional protobuf field 13) creates a durable logical
child on the **same Nakode runtime**. It is not `Delegate`, does not create a native run, and does
not start inference. Send the first task through the ordinary child prompt operation after accepted
creation. A completed child turn retains its session, workspace, history and explicit lifecycle.
Native archetypes remain callable directly in either parent or child, under their existing policies.

Servers implementing this operation advertise `ParentSessionCreation`. The Rust SDK's
`create_session_request` checks it whenever a parent is supplied. Missing capability, a failed
capability read or an older unimplemented info endpoint refuses before creation; it never removes
the parent and falls back to an unrelated session. Raw gRPC clients must make the same check:
older protobuf servers can ignore an unknown optional field. Unparented helpers remain unchanged.

## Atomic boundary

`SessionCreationContext` groups initial instructions, parent and optional bridge metadata.
The SQLite creation transaction stores the child logical row, governing profile, initial tool
configuration/instructions, optional bridge and parent link together. The existing link validator
requires current same-profile ownership (or same-workspace for two unbound legacy sessions), open
lifecycles, no nesting/reparenting/self-link, and at most 32 children.

The persistence runtime snapshots core state and the command cache before parent-aware creation.
It checkpoints the **accepted child's identity**, not the default engine, before publication,
provider effects or success. A refused link/failed write rolls back the row, profile, bridge,
logical engine and cached acceptance; a later explicit retry can succeed. Bare `ServerCore`
refuses parent creation rather than pretending that an in-memory link is durable.

The relationship uses the existing additive child-link schema. Parent and child must already be
under the authority of this runtime; a foreign-host ID cannot create a remote relationship.
Provider credential accounts are not ownership identities.

## Idle drafts and retries

A child is persisted before its first task, with a pending provider-session sentinel. Reopening
that record restores its logical ID, title and creation instructions without fabricating an owner
prompt or restoring a nonexistent provider conversation. Its first actual prompt uses that same
logical ID. Recovery of a pending creation that already has an owner prompt retains the existing
prompt identity/provenance path.

The original create command, including the parent, stays in the normal canonical digest. Matching
in-process retries replay acceptance; changing parent with the same key conflicts, and replay-only
misses do not create sessions. **Creation receipts remain process-local.** After restart, the child
and relationship survive but the original acceptance receipt does not. This is not restart-safe
exactly-once creation. Do not blindly issue a fresh create after an ambiguous result: reconcile
identities first. Durable create-result reconciliation is still required for unattended recovery.

## Validation scope

`src/server/runtime/tests/child_creation.rs` exercises transactional failure injection, unchanged
publication/no provider start on refusal, retry after rollback, original-parent receipt identity,
profile/closed/missing/nested/overflow refusals, same-profile cross-workspace creation with no bridge,
legacy cross-workspace refusal, persisted idle reopen/first prompt, and native-archetype coexistence.
Native coexistence checks canonical run identities/effects without invoking inference.

`crates/nakode-sdk/src/tests/parent_creation.rs` checks capability refusal before mutation and exact
parent/profile/title delivery across the SDK and real loopback gRPC adapter. These are isolated
fixture tests, not live provider, dashboard browser or two-host E2E verification.

## Parent projections

`SessionSummary.parent_session_id` and `SessionState.parent_session_id` expose the canonical
`session_child_links` association on live and retained reads, including archived sessions. They
never infer parentage from titles, directories, timestamps or native runs. Standalone and unknown
legacy sessions project no parent. Reading these fields does not open providers or change lifecycle.

Parent observations are additive: newer readers accept an omitted field from older servers as
unavailable parent metadata, as well as explicit null and a supplied parent identity. They must not
infer a relationship for an older session or reject its otherwise valid history. A present field
with an invalid type still fails validation. This read compatibility does not relax the
`ParentSessionCreation` capability check for mutations.

`crates/nakode-protocol/tests/parent_compatibility.rs` covers older semantic projections, explicit
null, exact parent identity round trips, and malformed values. FStack's HTTP adapter uses the
camel-case `parentSessionId`; its generated SDK must accept omission in both workspace summaries
and session GET/watch/command responses. Updating Nakode alone cannot repair a deployed HTTP
client that rejects older Host responses.

## Integration still required

FStack's paired change consumes this contract through its public SDK/Host boundary; its bundled
Nakode pin must include the additive parent projections before release. Authenticated cross-host
creation/routing, independent-child participant/attention UX, durable shared-ask recovery,
actual-output reports, artifact transfer and integrated dashboard follow-up UX remain separate
unfinished work. Creation itself never wakes the parent. Later durable child events can enter the
origin-aware continuation scheduler described in `docs/child-followup-delivery.md`.
