# Coordination messages and completion reports

## Authority

`RelayAgentFollowup` is distinct from ordinary `EnqueueFollowup`. The runtime verifies an exact
live pending `SendAgentMessage` call on the source session, including destination, unchanged text,
and selected source-image bytes. Producer-supplied roles, headers and JSON never establish origin.
The same-owner, immutable logical parent/child link grants `delegated_instruction`, and so does an
integration-vouched owner Chat (`source_owner_chat`), which may instruct any same-owner session it
did not start; the vouch is stored with the message and honoured when a batch is claimed. Another
same-owner source is `peer_context`. Cross-owner and cross-runtime relays refuse.

A delegated instruction can assign new work beyond the initial task. It never supplies approval,
protected confirmation or an answer to a structured question. Peer messages and upward child
reports remain inert evidence. No permission policy, approval resolver or tool allowlist changes.
Source ownership/link checks also apply when claiming queued messages.

Admission persists runtime origin independently of prompt text. A source session/call can create
only one message; command/message receipts make retries safe after the original call settles.
The existing fixed FIFO cutoff, ordinary-queue priority, attachment limits, dispatch fence and
uncertain-delivery refusal remain authoritative. Closed/retained-session recovery remains explicit.

## Display and persistence

`followup_batch_display` retains ordered version-1 display metadata by immutable batch ID.
Accepted-owner-prompt identity attaches `coordination_json` to live and restored transcript rows;
Protobuf `TranscriptEntry.coordination_json` is field 22. The JSON is presentation evidence, not an
instruction channel. Model serialization separately includes runtime origin and JSON-quoted text.

Each display message includes sender title/session, source kind, timestamp, status when available,
original content and image-artifact offsets. Local files are separate `filePaths` and do not advance
image offsets. Page budgets account for the retained metadata as well as body/audit bytes. A view
too small for the whole envelope emits a bounded neutral notice, never a raw owner-attributed runtime
preamble; full-session history remains the inspection path. Clients
must not discover coordination by parsing owner prose. Old ordinary rows remain ordinary.

FStack decodes bounded metadata and renders individual authored Markdown messages, without transcript detail controls. FStack’s Coordination inbox remains available for message
identities and delivery state. Both live and restored Chat/Agent projections use the same component.

## Exact-turn completion

At terminal turn capture, only that turn's terminal assistant response is eligible. Reasoning,
system and warning rows do not supply a final. A later tool or other substantive row invalidates
preceding assistant commentary as a final. There is no fallback to an older turn.

A versioned completion body persists atomically with the terminal turn/report. It contains `turn_id`,
`final_text`, original UTF-8 `final_total_bytes` and `truncated`. The Unicode-safe prefix is bounded
to 12 KiB after JSON character escaping, inside the 16 KiB report limit. Truncation is explicit in
display; missing/empty finals are explicit and recommend inspection rather than borrowing content.
Completed and failed reports follow existing delivery policy; cancellation/progress do not wake the
parent. Older reports without captured finals retain their existing inspection fallback.

Report-sequence receipts deduplicate admission across reload, and accepted-batch identities retain
one transcript entry. A completion is child evidence, not authority to restart a child or approve
work. Transcript inspection remains available for deeper evidence and omitted final content.

## Integration and validation limits

Both FStack SDK/runtime dependencies must point to a published Nakode revision containing this API
before standalone FStack builds or release. Local sibling Cargo patches are validation-only and must
not become release lockfile changes. No deployment or installed-service replacement is part of this
work. Unit/runtime fixture tests and offscreen UI scenes do not prove live provider acceptance or
durable parent-Chat wake-up. Native `nakode_agent` completion is a separate path.
