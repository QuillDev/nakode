# Thin-client service migration checklist

This checklist tracks the migration required by `AGENTS.md`. A checkpoint is
complete only when its code, tests, and quality gates pass and the checkpoint is
committed independently. Transitional compatibility is allowed, but no
checkpoint may introduce a second long-term source of domain truth.

## 1. Protocol foundation

- [x] Declare the server/client ownership model in `AGENTS.md`.
- [x] Add transport-neutral, versioned client command envelopes.
- [x] Add request correlation and explicit protocol rejection.
- [x] Route the existing agent invocation through the protocol envelope.
- [x] Preserve the legacy workspace-socket fallback during migration.
- [x] Add protocol unit and control-boundary integration tests.

## 2. Separate client presentation state from domain state

- [x] Introduce a TUI-owned presentation state type.
- [x] Move drafts/editor cursor, focus, scroll, selection, viewport, hit regions,
      and modal navigation into presentation state.
- [x] Make rendering consume immutable server view data plus mutable local
      presentation data.
- [x] Ensure rendering cannot emit domain effects or mutate canonical state.
- [x] Update the headless TUI harness to distinguish local presentation actions
      from service commands.

## 3. Headless service engine

- [ ] Introduce a server-owned engine that owns canonical application state.
- [ ] Move backend registry, provider event reduction, persistence effects,
      tools, shell supervision, and subagent supervision behind the engine.
- [ ] Define typed service commands for every domain-changing TUI action.
- [ ] Ensure the engine can run and be tested without terminal acquisition or a
      renderer.
- [ ] Retain an in-process transport temporarily for deterministic tests.

## 4. Snapshots, revisions, and subscriptions

- [ ] Define semantic client view snapshots independent of Ratatui/Crossterm.
- [ ] Add monotonic service revisions and ordered server events.
- [ ] Add subscribe, unsubscribe, fresh-snapshot, and resume-from-revision
      operations.
- [ ] Add bounded subscriber queues so slow clients cannot block the engine.
- [ ] Add command idempotency tracking and deterministic conflict responses.

## 5. Thin TUI client

- [ ] Replace direct reducer/backend/persistence calls with a service client.
- [ ] Render only semantic snapshots/events plus local presentation state.
- [ ] Translate controls into local presentation actions or typed service
      commands.
- [ ] Handle disconnect, reconnect, resubscribe, and snapshot replacement.
- [ ] Keep terminal and image lifecycle entirely in the TUI process.

## 6. Long-running out-of-process server

- [ ] Make the service process own the headless engine and all active work.
- [ ] Add framed local transport for commands, queries, and subscriptions.
- [ ] Start or connect to one compatible user-level server safely.
- [ ] Keep work running with zero connected clients.
- [ ] Support multiple simultaneous clients and multiple logical sessions.
- [ ] Make shutdown, upgrade, stale-socket recovery, and crash recovery explicit.

## 7. Direct CLI and automation clients

- [ ] Route agent invocation and other CLI operations directly to the server.
- [ ] Remove the requirement for a matching live TUI registration.
- [ ] Expose reusable client connection and subscription APIs.
- [ ] Verify concurrent TUI and CLI clients observe consistent state.

## 8. Remove transitional ownership and harden the boundary

- [ ] Remove TUI-local provider backends, persistence, tools, shell supervision,
      subagent execution, and routing sockets.
- [ ] Remove legacy control routing after its compatibility window.
- [ ] Ensure protocol modules contain no terminal, renderer, provider-wire,
      SQLite, or process-management types.
- [ ] Convert TUI evaluation fixtures to service snapshots/events and test the
      server independently through its public protocol.
- [ ] Add end-to-end detach, reconnect, multi-client, slow-client, idempotency,
      and server-without-client tests.
- [ ] Update user and developer documentation to describe the server lifecycle
      and client API.
