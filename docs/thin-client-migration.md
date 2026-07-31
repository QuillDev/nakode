# Renderer-only frontend architecture

This document records the implemented frontend boundary. There is no legacy
frontend compatibility layer.

This boundary is permanent: the Nakode service owns all canonical state and
mutations, and every client is only an SDK consumer and renderer. Future
features must extend the server, public schema, and SDK before adding frontend
controls or presentation.

## Public contract

- [x] `proto/nakode/v1/nakode.proto` is the sole public API schema.
- [x] gRPC is the sole frontend transport.
- [x] Rust, Python, Go, and TypeScript generation is declared in
      `proto/buf.gen.yaml`.
- [x] The Python generated-client conformance test calls the native Rust
      server.
- [x] API requests and responses have explicit size limits that preserve the
      20 MiB artifact contract.

## Server ownership

- [x] The native Rust server owns workspace, session, turn, queue,
      interaction, run, provider, model, settings, artifact, and diagnostic
      state.
- [x] Provider execution, tools, persistence, orchestration, permissions,
      cancellation, and prompt queue-versus-start policy remain behind the
      semantic server request loop.
- [x] Mutations carry server-recorded idempotency keys.
- [x] Watches expose authoritative replacement snapshots; internal reducer
      events are never public frontend messages.
- [x] Slow or lagged watch consumers cause a fresh server snapshot rather than
      blocking execution.

## SDK ownership

- [x] The Rust SDK exposes distinct typed product methods rather than a generic
      command envelope.
- [x] It owns mutation keys, safe transient retry, stream reconnection,
      resubscription, history paging, body reconstruction, and artifact
      hydration.
- [x] Startup session selection/open/create policy is an SDK operation.
- [x] A generated Python package and typed high-level entrypoint demonstrate
      the same contract from a non-Rust language.

## TUI boundary

- [x] The TUI connects only through `nakode-sdk`.
- [x] Controls emit local presentation actions or calls to distinct SDK edges.
- [x] The TUI does not import the server's internal command enum, runtime,
      provider adapters, persistence, tools, or process supervision.
- [x] State watches replace the render model; the TUI does not reduce domain
      events.
- [x] Only drafts, focus, selection, scroll, viewport, clipboard, terminal
      media, and lifecycle remain local.
- [x] Headless renderer tests and PTY terminal-lifecycle tests exercise the
      resulting boundary.
- [x] A boundary regression test rejects TUI imports of server runtime,
      persistence, provider, tool, and internal command modules.

## Removed architecture

- [x] The newline-delimited JSON frontend protocol was deleted.
- [x] The reducer-based `nakode-client` crate was deleted.
- [x] The lifecycle-coupled `nakode-native-client` crate was deleted.
- [x] The second frontend socket, transport replay journal, explicit frontend
      retry UI, and TUI use of internal server commands were deleted.
