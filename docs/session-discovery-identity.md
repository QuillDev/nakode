# Session discovery identity boundary

Nakode's server starts with one internal engine that hosts workspace-scoped configuration and legacy
terminal-client state. Its UUID is not a logical session creation receipt and is not owner-visible in
`ListSessions` until provider work persists that engine in `sessions`.

An explicit `CreateSession` receipt is the canonical top-level logical `SessionId`. It is discoverable
immediately, before provider initialization or persistence. The primary provider worker has a distinct
opaque native identity exposed only through `AgentSession.native_session_id` and
`SessionSummary.owned_provider_sessions`.

The dashboard lifecycle mapping and the preserved 2026-08-05 identity/event timeline are documented
in FStack's `docs/nakode-hierarchy-contract.md`.
