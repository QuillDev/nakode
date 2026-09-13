# Scalar session status

## Retained-query uncertainty

Unknown session, transcript and run identities are not confirmed absent while the session inventory
is incomplete. These reads return retryable `Internal`; unknown `OpenSession` follows the same rule
without running effects. Complete inventories retain ordinary `NotFound` behavior.

Retained native-history loading distinguishes storage/deserialization errors (`Internal`, retryable)
from a missing native row (`CapabilityUnsupported`, with same-identity explicit reopen guidance).
Neither path deletes the logical session, edits its workspace or restores a provider during a read.

## Scalar inventory

`NakodeClient::list_session_statuses(limit)` calls `NakodeService.ListSessionStatuses` once.
It reads the installation service's discoverable logical sessions without constructing workspace,
transcript, interaction-question, provider-configuration, failure-detail or delegated transcript views.
It never opens/restores a provider session and does not require any attached frontend.

Each row contains only the logical ID, live revision, canonical `activity`, and three flags:
`owner_turn_running`, `has_interactions`, `has_failure`. Owner-turn presence matches the full session
projection, including starting and cancelling turns. Activity separately represents creation,
compaction, delegated and shell work; consumers must not confuse it with owner-turn presence.
These are facts, not an LED/color or frontend-priority policy.

Persisted sessions without an attached engine have revision zero, idle activity and no process-owned
turn, interaction or failure. The unpersisted initial control-plane engine is not a conversation.
No historical failure body or provider history is reconstructed by this read.

Rows are ordered by logical ID. The limit is capped at 500, including when a caller asks for more.
`complete=false` means either the service's inventory is still incomplete or the result was truncated;
consumers must not treat omission as authoritative or report an all-clear from a partial result.
The RPC uses the ordinary query lane, not the hydration or mutation/control lane. Ordinary API-key
and local-socket authority is unchanged. Older servers return gRPC `Unimplemented`; clients must
report unavailable rather than silently replacing this read with hundreds of hydrated session reads.

Tests cover scalar/full-projection parity, the initial-engine exclusion, ordering/truncation/startup
completeness, and an SDK-to-gRPC round trip requiring exactly one status query.
