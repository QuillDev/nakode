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
type-safe service clients and models for Rust, Python, Go, TypeScript, and any
other language with Protobuf/gRPC support. `proto/buf.gen.yaml` contains the
multi-language generation configuration.

Generated RPC clients expose the complete product edge inventory, including:

- workspace discovery and replacement watches;
- create, open, list, get, and watch session;
- send, explicitly enqueue, steer, cancel, and compact work;
- interaction resolution, provider authentication, models, settings, and
  agent definitions;
- delegation, run watches, transcript paging, artifacts, and diagnostics.

The Rust `nakode-sdk` crate and checked-in Python conformance SDK add ergonomic
methods, stable mutation idempotency keys, safe retry, reconnecting watches,
paging, body reconstruction, and artifact hydration. Equivalent language
packages implement the same behavioral SDK profile over generated stubs; they
do not invent new domain semantics.

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

## Local endpoint

`nakode service endpoint` ensures the workspace server exists and prints a
machine-readable descriptor for its `grpc+unix` endpoint. The endpoint is
private to the desktop user. Stopping a renderer does not stop the server or
cancel server-owned work; stopping the server is an explicit lifecycle action.

The TUI uses this exact public SDK path. It is an example frontend, not a
privileged application runtime.

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
