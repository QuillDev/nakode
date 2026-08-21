# Discord orchestrator threads

Nakode can optionally project FStack Chat conversations and Ticket Agent sessions into Discord threads. Nakode remains the authority for logical sessions, readiness, turns, transcript state, thread identity, inbound deduplication, and delivery checkpoints. FStack only declares typed bridge intent and open/archive lifecycle through `nakode.v1`; it never receives the Discord credential or calls Discord.

A first credentialed diagnostic reached Discord's authenticated REST API but did not reach gateway `Ready`: the bot application's privileged Message Content intent was disabled. No successful live thread/message round trip has been validated yet.

## Discord application setup

Create a bot application in the Discord developer portal and enable:

- **Guild Messages** gateway intent;
- the privileged **Message Content** gateway intent.

The Message Content switch must be enabled under **Developer Portal → Application → Bot → Privileged Gateway Intents** and saved. Without it, Discord rejects the gateway session before `Ready`; no reconciliation, thread creation, outbound projection, or inbound control can start. Nakode treats that rejection as a terminal, redacted **Failed** state with an instruction to enable the intent and restart instead of reporting a live supervisor as Running.

Install the bot into the server containing the two parent channels. Grant the bot, in both parent channels:

- View Channel;
- Read Message History;
- Send Messages;
- Send Messages in Threads;
- Create Public Threads;
- Manage Threads (required to archive and unarchive a mapped thread);
- Add Reactions.

Use two different text-channel snowflakes: one parent for Chat Orchestrator threads and one for Agent Orchestrator threads. Copy the immutable snowflake for the one Discord user allowed to continue sessions. Names and thread titles are display-only and are never identities.

## Configure Nakode

### FStack desktop

In the desktop dashboard, open **Bridges → Discord**. Enter the Chat parent-channel, Agent parent-channel, and primary-user snowflakes, then optionally enter a replacement bot token. The token field is write-only: FStack keeps it only in component-local memory, clears it immediately after dispatch, and never preloads it. A blank token field preserves Nakode's existing credential. Save validates and persists the three public IDs; use **Enable** or **Disable** for automatic startup, **Restart transport** for the installation service, and **Refresh status** for a redacted view.

The dashboard receives only `token_configured`, public snowflake IDs, enabled/configuration-complete state, runtime state, and a generic sanitized runtime error. It cannot read or delete the stored token and never makes Discord requests. The connected Nakode service must advertise the optional `DiscordManagement` capability. After upgrading Nakode, restart the installation service so it serves the new protocol, then use **Refresh status** in Bridges. Older services show an unsupported state without affecting Chat or Agent behavior; browser-only FStack has no Bridges tab because it has no Nakode desktop port.

Configuration, credential storage, displayed runtime, and the Restart action are installation-wide. Saving while enabled restarts the singleton transport after both durable writes succeed. Mutation retries use caller idempotency keys; Nakode serializes management operations and installation writers through a private advisory lock without blocking an async runtime worker. Save changes only the three IDs and optional credential, while Enable/Disable changes only the startup flag. The process-local replay cache retains at most 128 keys with each request digest and redacted result—never credential text. A retry within that window replays the result; an evicted key or a retry after service restart executes the already-safe Save, Enable/Disable, or Restart operation again.

### CLI fallback

Run setup once for the Nakode installation:

```sh
nakode transport discord setup \
  --chat-channel-id 123456789012345678 \
  --agent-channel-id 234567890123456789 \
  --primary-user-id 345678901234567890
```

`setup` prompts for the bot token without echoing it. Omitting any ID flag prompts for that snowflake. The v2 public configuration contains only:

```toml
version = 2
enabled = true
chat_channel_id = "123456789012345678"
agent_channel_id = "234567890123456789"
primary_user_id = "345678901234567890"
```

The bot token is stored separately as `discord.token` in the private Nakode application-data root (mode `0600` on Unix). The public configuration and token are installation-level and shared by canonical workspaces; each workspace keeps its ingress/recovery state in a separate hashed subdirectory so authorities cannot consume one another's work. The token is absent from TOML, SQLite, protobuf views, status output, logs, transcript payloads, FStack state, and debug formatting. `NAKODE_DISCORD_DIR` may override the private storage root for an installation. Protect that directory as a credential store; never commit or screenshot it.

Inspect and operate the transport with:

```sh
nakode transport discord status
nakode transport discord status --json
nakode transport discord start
nakode transport discord stop
nakode transport discord restart
nakode transport discord enable
nakode transport discord disable
```

`enable` persists automatic startup and starts the live transport in the installation service when it is available. `start` is a live action and does not change the persisted enabled flag. `disable` makes the integration optional again and stops the transport without affecting dashboard or Nakode session behavior. Missing configuration, a missing token, invalid snowflakes, missing permissions, or a Discord outage cannot disable normal FStack/Nakode use.

Configuration v1 is intentionally not migrated. Rerun `setup`; historical bindings are not reconstructed.

## Session and thread lifecycle

FStack sets a typed `SessionBridgeIntent` while creating either a Chat or Agent logical session. Nakode persists one `session_bridges` row keyed by logical session ID. The row records workspace, orchestrator kind, desired lifecycle, display title/revision, transport and parent/thread snowflakes, constant-size final-delivery progress, live-message identity, source-message identity, and any accepted pending inbound prompt. The authoritative, never-expiring replay identities live in normalized `session_bridge_inbound_events` rows; a private 256-entry in-memory cache is only a fast path. Private inbound/deduplication state and prompt contents are not returned in public projections.

A bridge is optimistic: it can be open without a Discord thread. Nakode creates a thread only when reconciliation has useful work, then claims the returned thread snowflake through a typed mutation. A deterministic starter nonce includes the destination parent and recovers a thread after uncertain sends without cross-destination nonce reuse. The SQLite unique index on `(transport, external_thread_id)` prevents cross-wiring inside one authoritative workspace database, and a concurrent loser adopts the authoritative binding and archives its orphan best-effort. Titles are readable but never used to recover identity. An installation running more than one workspace service does **not** currently have an installation-wide thread-claim index; see [Current readiness](#current-readiness).

- Closing a Chat or Agent sets the bridge to `archived`; the bot archives the mapped thread.
- Reopening sets it to `open`; an already-open thread is a successful no-op.
- A deleted/missing Discord thread clears only the external binding. A later useful reconciliation attempts to create a replacement for the same logical session. Same-parent recreation currently reuses the stable starter message; the Discord contract for re-threading a starter whose prior thread was deleted is not yet covered by a credentialed or deterministic contract test, so this path remains part of the readiness gap below.
- On a fresh FStack process start, all known workspace bridges are archived before create/resume IPC is exposed. On process shutdown, Chat and every known Agent workspace enter an archive barrier before any Chat/Agent teardown begins. The whole process still has a hard shutdown deadline; an unavailable endpoint may therefore cause the process to quit without starting teardown, but teardown is never allowed to overtake an in-flight archive sweep. Individual failures are logged without credentials and cannot block/corrupt Nakode state.
- Reopening after restart reuses the durable thread snowflake when Discord still has it.

## Outbound delivery and idempotency

Only allowlisted, user-visible `User` and `Assistant` transcript entries are projected. Provider internals, reasoning, tool protocol payloads, hidden instructions, and intentional redactions are not sent. A dashboard-originated user turn has no source transport and is mirrored first with 🔄. A trusted Discord-originated user turn retains `source_transport = "discord"` in authoritative transcript provenance and advances the same typed cursor with zero Discord messages, so the bot never echoes the prompt into its source thread. The matching assistant live/final projection can begin only after that user checkpoint. Discord mentions are text-neutralized and `allowed_mentions` disables user, role, everyone, and reply pings.

Live assistant text is represented by one editable message and the yellow-circle reaction. It is not a completed turn. Completed user or assistant text is UTF-16-aware chunked at 1,900 units, with ordered continuation chunks and fenced-code close/reopen handling; content is not intentionally truncated. Before any completed projection send, Nakode durably records:

1. the projection kind (`User` or `Assistant`), provider turn ID, prior typed cursor, body SHA-256, and total part count;
2. a monotonic `completed_parts` count and only the latest accepted Discord message snowflake;
3. typed cursor advancement only after every part and its reaction have succeeded.

Part nonces are derived deterministically from destination thread, session, projection kind, turn, and index rather than retained in an ever-growing protocol record. Including the destination avoids Discord's author-wide nonce recovery returning a message from a deleted/replaced thread. Retrying the latest completed part must present the same external message snowflake; a conflicting identity fails closed. Clearing a deleted thread binding resets pending delivery progress so a replacement thread receives the projection from part zero.

Retries search for deterministic nonces before sending, edit recovered messages, and therefore survive send-before-checkpoint crashes. Removing an already-absent live reaction is an idempotent no-op; the completed reaction must succeed before finalization. Nonce history lookup is capped at 64 pages/6,400 messages and fails closed rather than risk a duplicate. Serenity applies Discord rate-limit handling; each REST operation also has a 30-second local deadline, and bridge workers reconnect with jittered backoff and retry deferred projection. A persisted typed User/Assistant cursor prevents restored transcript history from being replayed. If that cursor is older than the normal 1,024-entry replacement snapshot, the adapter pages backward through the typed transcript API into an ephemeral private disk spool while retaining only bounded history metadata in memory, locates the cursor, and then hydrates/delivers one projection at a time in chronological order. The spool is never authoritative or resumed: it is deleted after use and rebuilt from Nakode after restart. Its temporary disk use is proportional to history traversed. If the authoritative cursor cannot be found, delivery fails closed instead of replaying history.

## Discord to Nakode

Inbound control is accepted only when all of these are true:

- the author snowflake exactly equals `primary_user_id`;
- the event is not from a bot or webhook;
- the channel is the exact currently bound thread;
- the bridge is open and bound to `discord`;
- Nakode says the logical session is idle/ready.

Unauthorized users, echoes, wrong channels, unknown/deleted mappings, and archived sessions are ignored without mutating session state. Every authorized paired gateway message is first checkpointed in a private `discord-ingress.sqlite` spool (mode `0600` on Unix) before reactions or Nakode RPCs. Executable rows store text, immutable identities, attachment descriptors, and multipart grouping—not downloaded attachment bytes. Rows durably classified as overload/busy retain only identity and route metadata, never prompt text or attachment URLs. Admission uses an immediate SQLite transaction, so independent Nakode processes cannot both classify same-session events as executable. A single restartable replayer and 16 normal-work slots bound active processing; same-session later ordinary messages are durably marked forced-busy so they cannot overtake an unresolved turn. Terminal local identities become compact tombstones, and malformed payloads are fail-closed quarantined without retaining their content, so a reconnect/reopen cannot resurrect them and one corrupt row cannot block other sessions.

A busy message is durably consumed as `Busy`; it is never queued and can never become a later prompt. Accepted and busy gateway event IDs are persisted in Nakode's normalized ledger so duplicate gateway delivery and restart replay are harmless. The SDK derives the continuation mutation key from the session, transport, thread, and external-event identities, so a caller retrying the same request in a later SDK invocation uses the same server idempotency identity; an explicitly supplied mutation key is always preserved. Accepted prompts use a stable `bridge-<sha256-prefix>` provider client-turn identity and remain in a durable pending inbox until Nakode observes `TurnAccepted`, `TurnStarted`, or `TurnCompleted`. A bridge checkpoint is persisted before provider dispatch; a persistence error rolls logical and idempotency state back so the same-key retry can execute safely. Crash recovery redispatches the same client identity, providing at-least-once transport semantics. Native adapters and compatibility protocols that expose a client-message ID preserve that identity; a provider protocol that does not honor client idempotency inherently retains an ambiguous crash-after-dispatch window and cannot offer distributed exactly-once execution.

Discord's own per-message limit is handled for long inbound turns with an explicit grouping rule. Send each part as:

```text
!nakode multipart <group> <part>/<total>
<body for this part>
```

`group` is 1-32 ASCII letters, digits, `_`, or `-`. Parts may arrive out of order, but all parts must use the same total in the same paired thread. A group is complete only when every numbered part exists. One group per logical session and up to 32 groups per workspace may be active, so an incomplete turn cannot overlap a second turn or let one session exhaust every assembly slot; each incomplete part expires 30 minutes after its Discord receipt, and replay does not extend that TTL. An assembled turn is practically bounded to 256 parts and 512 KiB of UTF-8 text; exceeding either limit is rejected visibly rather than truncated or queued. Ordinary messages are never heuristically joined, so two separate messages cannot accidentally become one turn. Duplicate part message IDs are ignored. Multipart files are private and temporary; only the configured primary user can create them.

Image inputs must be Discord-hosted HTTPS attachments from `cdn.discordapp.com` or `media.discordapp.net`; redirects are limited to five and must remain on those hosts. Images are limited to 20 MiB each and 30 MiB combined. The accepted, resolved image bytes are part of Nakode's private pending inbox until provider acknowledgement, which makes accepted-turn restart replay independent of expiring CDN URLs. Before acceptance, an expired or failed attachment download is durably consumed as a failed message, receives ⚠️ plus a generic resend instruction, and never becomes a later prompt.

## Reaction vocabulary

| Reaction | Meaning |
|---|---|
| 🔄 | Accepted continuation / provider work is being started |
| 🟡 | User-visible live assistant activity; not a turn end |
| ✅ | Turn-ending answer completed and durably delivered |
| ⚠️ | Turn failed or was cancelled/interrupted |
| ❌ | Ignored as busy; wait for the active turn to finish |

Repeated event handling is idempotent. The bot removes/replaces only its own relevant reaction where practical.

## Failure behavior and observability

Gateway and SDK watches reconnect with jittered backoff, cancellation-aware waits, bounded active work, and task cleanup. Invalid bot authentication and rejected/disabled gateway intents are terminal configuration failures rather than retry loops; Bridges status exposes only a fixed, allowlisted remediation message and never raw Discord metadata. The installation service owns one gateway client and all authoritative thread mappings. A shared installation lock and Discord's `session_start_limit` serialize/throttle Identify through Ready. Inbound work uses 16 normal slots. Saturation and same-session overlap are persisted as `forced_busy` in the ingress spool before the immediate ❌ feedback; replay then calls the typed continuation boundary with `consume_as_busy`, and only a durable terminal disposition removes the row. Thus a reaction or process crash cannot turn overloaded input into later work. Pending rows remain on RPC timeout/network failure and retry the same identity. SQLite ingress operations run on Tokio's blocking pool, so a five-second SQLite lock wait/fsync cannot stall a gateway runtime worker. Archive/unarchive is tried three times. Missing threads clear mappings; permission/network/rate-limit failures are deferred and logged with shortened logical identities and sanitized SDK/Discord errors. Point Nakode bridge RPCs use three-second deadlines, Discord REST and approved-CDN attachment requests use 30-second operation deadlines (with a three-second attachment connect deadline), and long-lived gateway/watch streams remain cancellation-bound. Token material is never included.

Executable ingress payloads and downloaded pending image bytes are removed after their durable terminal/acknowledged disposition. Local compact tombstones are retained for 30 days and capped at 16,384 rows per workspace. The normalized authoritative replay ledger retains at most 4,096 identities per session plus any protected pending prompt. This is bounded operational replay protection rather than a claim that an arbitrarily old Discord event can never execute again. Operators should include the private workspace transport directory in ordinary disk monitoring.

Use `transport discord status` and Nakode logs for redacted configuration/runtime state. Status exposes only whether a valid bounded token is configured, never its value. A Discord **Failed** state stops only the optional transport; Nakode Chat, coding Agents, and dashboard session control remain available. FStack presents its remediation inline and lets the operator dismiss the notice without clearing the truthful Failed status. If Bridges reports that Discord rejected the Message Content intent, enable and save that privileged intent in the Developer Portal, then select **Restart**. A missing or rejected token requires a write-only token replacement before restart.

## Current readiness

**NO-GO for a broader credentialed rollout as of this audit.** The first bounded live diagnostic proved that the configured bot token and Discord REST/gateway discovery endpoints were valid, but the application had Message Content disabled and Discord rejected every workspace gateway before `Ready`. The diagnostic did not print or persist the token and no successful live thread/message round trip occurred. The corrective source passes the deterministic repository gates: Nakode format/check, warning-denied Clippy, all-target/all-feature build, the 675-test locked all-target/all-feature suite, and the headless Agent TUI scenario. The paired FStack follow-up passes dashboard format/typecheck, all desktop/web/shot/main builds, Discord tests (9/9), and targeted credential-free failed/dismissed scene captures; both screenshots were visually reviewed. The unchanged merged Chat lifecycle (10/10), Agent pane (42/42), and Nakode Agent integration (33/33) results remain prior evidence. The Excalidraw JSON sources and SVG XML validate, and both PNG previews were visually inspected. This is substantial offline evidence, but not a successful live integration result.

The former installation-ownership gap is resolved by the singleton service: one gateway owns the shared token, mappings, and event admission across sessions. Legacy per-workspace services that are still active during migration are preserved rather than force-killed, so operators must complete migration before treating that invariant as operationally established.

Transport-level fault coverage is also incomplete: same-parent deleted-thread starter reuse is not contract-proven; User/Assistant partial-send and reaction settlement are not exercised through a complete mocked SDK + Discord transport restart; gateway replacement/supervisor paths are only covered in focused units; management replay is process-local; and the historical recovery spool is ephemeral, synchronously written, proportional to traversed history, and has no independent disk quota. Inbound execution is durably bounded but one workspace replayer currently processes records serially, so a slow attachment/RPC can head-of-line-block unrelated sessions. These are documented readiness gaps rather than invitations to add more feature scope to this follow-up.

Credentialed rollout should wait until the singleton migration is operationally complete, then close the named transport fault gaps in deterministic adapters first. The bounded live diagnostic established valid credentialed REST access and the missing privileged-intent failure only; it did not exercise `Ready`, permissions, thread creation, projection, inbound continuation, retry, or restart behavior. Do not interpret either deterministic coverage or that failed pre-Ready diagnostic as successful live validation.

## Architecture artifacts

The PNG previews below are embedded for dashboard/GitHub viewing. Select either preview to open the editable Excalidraw source.

### Architecture and authority boundaries

[![Discord orchestrator architecture and authority boundaries](architecture/discord-orchestrator-architecture.png)](architecture/discord-orchestrator-architecture.excalidraw)

- [Editable Excalidraw source](architecture/discord-orchestrator-architecture.excalidraw)
- [Rendered PNG](architecture/discord-orchestrator-architecture.png)
- [Rendered SVG](architecture/discord-orchestrator-architecture.svg)

### Lifecycle and delivery sequence

[![Discord orchestrator lifecycle and delivery sequence](architecture/discord-orchestrator-sequence.png)](architecture/discord-orchestrator-sequence.excalidraw)

- [Editable Excalidraw source](architecture/discord-orchestrator-sequence.excalidraw)
- [Rendered PNG](architecture/discord-orchestrator-sequence.png)
- [Rendered SVG](architecture/discord-orchestrator-sequence.svg)
