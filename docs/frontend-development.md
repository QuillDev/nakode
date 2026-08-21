# Building a Nakode frontend

Every frontend is a renderer for the native Rust Nakode server. Frontends do
not load providers, open SQLite, supervise tools, reduce provider events, or
decide session and orchestration policy.

This is a hard compatibility boundary, not a convention specific to the TUI.
The server is the application and the frontend is replaceable. Shipping a
frontend in the same repository or executable grants it no private access to
server modules or behavior.

```text
TUI ───────────┐
desktop app ───┼─ generated Nakode SDK ─ gRPC ─ native Rust server
web gateway ───┤                              ├─ canonical state
automation ────┘                              ├─ providers and tools
                                               ├─ persistence
                                               └─ orchestration
```

## Contract and generation

`proto/nakode/v1/nakode.proto` is the only public contract. It generates
type-safe service clients and models for Rust, Go, TypeScript, and any other
language with Protobuf/gRPC support. `proto/buf.gen.yaml` contains the
multi-language generation configuration.

Generated RPC clients expose the complete product edge inventory, including:

- workspace discovery and replacement watches;
- create, open, list, get, and watch session;
- send, explicitly enqueue, steer, cancel, and compact work;
- interaction resolution, provider authentication, models, settings, and
  agent definitions;
- delegation, run watches, transcript paging, artifacts, and diagnostics.

The Rust `nakode-sdk` crate adds ergonomic methods, stable mutation idempotency
keys, safe retry, reconnecting watches, paging, body reconstruction, and
artifact hydration. Equivalent language packages implement the same behavioral
SDK profile over generated stubs; they do not invent new domain semantics.

Generated stubs make every RPC type-safe, but a production frontend should use
its language's high-level SDK profile. If that package does not exist yet, add
it as reusable SDK infrastructure rather than embedding retry, paging,
hydration, or session policy inside the new frontend.

## Client flow

1. Locate or start the workspace server and connect to its API endpoint.
2. Call `getWorkspace()`.
3. Call `createSession()` or `openSession()`, then `getSession()`.
4. Consume `watchWorkspace()` and `watchSession()` as authoritative
   replacement snapshots.
5. Map input to a typed SDK call and render returned or watched state.

`sendPrompt()` is the normal prompt edge. The server atomically chooses whether
to start work or queue it. `enqueuePrompt()` exists only for an explicit user
request to queue. A frontend must not infer this policy from observed state.

## Frontend-owned state

A frontend may own focus, drafts not yet sent, selection, scroll position,
viewport size, clipboard/device integration, and terminal/window lifecycle.
Everything else comes from the server: sessions, turns, queues, interactions,
todos, runs, settings, providers, models, notices, history, and artifacts.

A local projection is a disposable rendering cache, never an independent
source of truth. On a new snapshot, replace server-owned fields. Do not merge
domain events, synthesize missing state, or persist a client-owned session
model.

Watches emit complete replacements. Frontends never receive Nakode's internal
domain reducer events. The SDK resumes a watch after transport loss and
re-hydrates bounded history. Mutation retries reuse the same idempotency key.

## Queued prompt controls

`SessionState.queue` is the complete ordered queue projection. Each item carries
a stable prompt ID, full semantic text, and attachment metadata. Clients must
preserve that order and identity, including repeated prompts with identical
text; text is never an identity or deduplication key.

Queue controls cross the public boundary as one mutation, with the current
expected session revision and an idempotency key:

- `RemoveQueuedPrompt(session_id, prompt_id)` independently cancels one waiting
  follow-up.
- `SteerQueuedPrompt(session_id, prompt_id)` atomically redirects active work to
  one waiting follow-up. It is available only when the server advertises
  `QueuedPromptSteering`; clients also require either steering or interruption
  in the active agent session's provider capabilities.

The server validates the prompt identity, active turn, provider capability,
absence of another pending redirect, and the text-only boundary before it
removes anything. Attachment-bearing prompts are rejected unchanged. A
steering-capable provider receives one `BackendCommand::SteerTurn`; provider
acknowledgement records a steering transcript item, while refusal or a
turn-ending race restores the same prompt at its prior queue index.

For an interruption-only provider, the server removes the selected prompt from
the ordinary queue, interrupts the active turn, and holds that prompt as the
next continuation. Completion of the old turn starts it before every sibling,
including when completion wins the interrupt race. Interrupt failure restores
it at its original queue position. Ordinary Stop keeps queue order and starts the
first follow-up after the interrupted turn.

A frontend may optimistically overlay only operation status, keyed by prompt
ID. Authoritative IDs, text, and order always come from the latest replacement
snapshot. Pending controls suppress duplicate clicks; rejection may leave a
retryable error overlay. A snapshot that no longer contains the ID confirms
success and removes its overlay. This rule prevents loss and duplication across
reconnects without creating a client-owned queue.

For the FStack desktop client, `electron/nakode-protobuf.ts` decodes this
projection, `electron/nakode.ts` owns the typed RPC envelope and capability
check, and `electron/nakode-agent.ts` applies the status-only overlay before
publishing the normalized agent snapshot. Renderer controls cross IPC with only
`sessionId` and `promptId`; they never remove, reorder, or synthesize canonical
queue rows locally.

## Local endpoint

`nakode endpoint` ensures the installation-wide server exists and prints a machine-readable descriptor
for its `grpc+unix` endpoint. The endpoint is private to the desktop user. Stopping a renderer does not
stop the server or cancel server-owned work; stopping the server is an explicit lifecycle action.

The earlier `nakode service endpoint` spelling remains supported for installed
connectors. It prints the same descriptor on standard output and writes only its
deprecation notice to standard error, so a connector that parses standard output
is unaffected. There is no workspace or global endpoint selector.

The TUI uses this exact public SDK path and passes its current directory when creating a new session.
It is an example frontend, not a privileged application runtime.

## Discord orchestrator threads (optional)

Discord is a native Nakode transport. Nakode—not a frontend—owns the bot token, gateway client,
logical-session/thread mappings, inbound authorization and deduplication, delivery checkpoints, and
archive state. Frontends such as FStack only attach a typed `SessionBridgeIntent` while creating a
session and issue typed open/archive lifecycle commands when the corresponding dashboard object is
opened or closed. They must not read Nakode persistence or call Discord directly.

Configure the transport through the secret-safe CLI:

```bash
nakode transport discord setup \
  --chat-channel-id <CHAT_PARENT_SNOWFLAKE> \
  --agent-channel-id <AGENT_PARENT_SNOWFLAKE> \
  --primary-user-id <PRIMARY_USER_SNOWFLAKE>
nakode transport discord status
```

`setup` reads the credential privately without echo; status never prints it. The public IDs and private token are installation-level, while ingress/recovery state remains isolated by canonical workspace. Configuration is optional and an unconfigured transport does not affect normal sessions. Each bridged Chat or Agent session gets one lazily-created thread under its configured parent channel. Nakode persists the stable pairing,
reuses it after restart, archives/unarchives it from lifecycle intent, projects user-visible live and
final transcript output, and accepts a continuation only from the configured user while the logical
session is authoritatively idle.

See [`discord-orchestrator-threads.md`](discord-orchestrator-threads.md) for intents, permissions,
reaction semantics, retries, limits, failure behavior, and the current deterministic-only live-test
status.

## Adding a product capability

Implement features in this order:

1. Define the server-owned state, invariant, and mutation semantics.
2. Implement and test the operation in the native Rust server.
3. Add the typed query, mutation, or watch shape to the public Protobuf API.
4. Add the ergonomic method and shared recovery behavior to language SDKs.
5. Render the returned state and map controls to that SDK method in each
   frontend.

If a frontend would need to open persistence, call a provider, supervise a
process, interpret provider events, coordinate sessions, or reproduce a server
decision, stop and add the missing server/API/SDK edge instead.

## Frontend acceptance test

A frontend is correctly isolated when it can be replaced by an implementation
in another language using only the public schema and SDK behavior profile. The
replacement must be able to observe the same state and invoke the same
operations without importing Rust server crates, reading Nakode storage, or
depending on TUI internals.
