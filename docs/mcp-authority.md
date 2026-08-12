# MCP authority and extension guide

Nakode is the sole authority for Model Context Protocol servers. Clients configure and inspect MCP only through `nakode.v1`; they do not launch transports, read SQLite, hold credentials, discover tools, or settle provider calls.

## Semantic model

A **server definition** is durable configuration: stable ID, endpoint/transport, enabled state, credential requirement, bounded timeout/result size, provenance, and policy. It is not proof that a server is usable.

A **live discovery snapshot** records health, negotiated protocol/server identity, last connection/error, and normalized tools. Discovery is optional work: an absent, disabled, slow, or malicious server cannot prevent Nakode startup or alter a session that has no MCP grant.

A **tool identity** has both remote identity and collision-safe provider identity. Provider names are `mcp__<server-id>__<remote-tool>`. Nakode retains the server and remote tool names for routing and audit. Tools marked app-only by MCP metadata are never model-visible.

A **grant** is explicit session input. `CreateSessionRequest.mcp_grant` and `OpenSessionRequest.mcp_grant` name the surface (`chat` or `coding_agent`) and server IDs. Omission means no MCP access. Enabling or discovering a server never grants it. Nakode checks current enabled, connected, credential-ready, and surface-policy state when installing tools and checks current usability again at invocation.

## Lifecycle and storage

The public path is:

1. `proto/nakode/v1/nakode.proto`
2. generated `nakode-api`
3. `crates/nakode-server/src/grpc.rs`
4. `src/server.rs` domain commands/queries
5. `src/server/runtime.rs` supervision
6. `src/session.rs` / `src/credential.rs` persistence
7. `crates/nakode-sdk`

`mcp_servers` stores definitions and redacted discovery snapshots additively in Nakode's session database. `mcp_credentials` is reachable only through Nakode's credential authority and is never projected over the management API; projections contain readiness and kind only. Errors, audit arguments, and results are bounded and model-facing output is sanitized. No credential belongs in source, logs, notes, screenshots, or endpoint URLs.

Startup restores definitions and last snapshots without synchronous network discovery. Refresh is explicit and runs outside the authoritative actor. Invocation also runs outside the actor, is bounded by timeout/result limits, and returns exactly one completion through the existing provider-neutral external-tool settlement path. Calls are attributed to workspace, logical session, optional delegated run, server, and remote tool in `mcp_invocation_audit`.

Disable, delete, grant removal, or credential clearing prevents new invocation. Disable/delete/credential clearing also cancel matching in-flight work. Turn/run cancellation and service shutdown cancel pending optional work; late task completions are ignored after their correlation is removed. A transport failure is a failed tool result, not a server or session crash.

Remote endpoints require HTTPS, reject URL credentials and local/private/link-local addresses both literally and after bounded DNS resolution, pin the validated address for each request, disable redirects, and enforce bounded arguments, response bytes, tool count, and operation timeout. New transports must preserve these controls.

## Excalidraw template

The built-in template uses the Excalidraw organization server:

- endpoint: `https://mcp.excalidraw.com/mcp`
- repository: `https://github.com/excalidraw/excalidraw-mcp`
- release: `v0.3.2`
- commit: `157aa23ceb1976008aadc89eb05e3444060f09d6`
- recorded SHA-256: `2b494012b5fee5937f9f7b86f04a76cc4a91ec843ee3339b93e4e15e415274ff`
- negotiated MCP protocol: `2025-06-18`

The public endpoint did not require authentication during investigation. It is not an Excalidraw Plus account API and does not associate output with a Plus workspace.

Model-visible operations include usage guidance, view creation, and checkpoint continuation. `export_to_excalidraw` is app-only and filtered. A checkpoint ID is a temporary continuation handle, not a durable/public drawing: server storage may be in-memory or optional Redis with a 30-day TTL. Continue editing by retaining the checkpoint ID, reading it, and applying explicit element additions/deletions. Export/share is a separate app UI upload action and Nakode never invokes it automatically. Therefore no handoff should claim a durable document or public link without explicit export evidence.

The package and manifest declare MIT; the investigation observed no top-level GitHub license metadata/file. Preserve that caveat when updating provenance.

## Adding another MCP server

1. Confirm the real server, transport, authentication, tool semantics, artifact ownership/privacy, and license. Pin repository/release/commit/digest evidence.
2. Add a generic template only when useful; keep product-specific semantics in template metadata, not the core registry.
3. Extend `nakode.v1` additively if the transport or policy needs public fields; regenerate API code and update gRPC conversions and SDK.
4. Implement transport initialization, discovery, invocation, cancellation, timeout, result bounds, redirect/DNS/network policy, secret redaction, and app-only filtering in `src/mcp.rs`.
5. Persist definitions/snapshots additively; route secrets only through credential authority.
6. Preserve stable remote/provider identity and audit attribution. Never expose a tool merely because the server is enabled.
7. Add explicit surface/archetype policy and session grant UI in clients. Omitted grants must preserve old behavior.
8. Test protocol omission compatibility, denial/grant, discovery normalization, invocation, redaction, timeout/cancellation, restart, revocation, and no-MCP regression. Capture management UI states and perform a real permitted smoke test without overstating artifact durability.
