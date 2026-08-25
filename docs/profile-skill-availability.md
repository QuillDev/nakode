# Profile-scoped skill availability

Nakode owns skill discovery and per-profile enablement. Clients manage it through the versioned `ListSkills` and `SetSkillEnabled` service methods (or the equivalent SDK methods) after checking the `SkillAvailability` capability.

## Identity and reconciliation

Preferences are keyed by profile ID and the discovered skill's stable ID, not its display name or path. No preference means enabled, preserving pre-upgrade behavior and making newly discovered skills available by default.

Installed skills are returned with current metadata. A saved identity that is no longer discovered remains in the manageable catalogue with its last known metadata and `available = false`. It cannot be changed until that stable identity is discovered again. Renaming metadata does not lose its preference. Invalid or duplicate discoveries never become manageable rows because normal discovery rejects them before reconciliation.

## Session lifecycle

A profile-managed logical session is durably associated with its profile. Nakode refreshes installed-skill discovery, then resolves the profile's current effective catalogue when the session is created or opened and again at each owner turn. This session-start refresh is required because clients may install or update skill packages while the long-running service remains active. Profile preference changes and explicit installed-skill refreshes update loaded sessions, so open and future sessions converge on the same service-owned authority without waiting for provider-owned context to be rewritten mid-turn.

The turn catalogue is copied into native provider runtimes before prompt construction and tool execution. `read_skill` and `read_skill_component` read only that in-process catalogue: neither rescans the filesystem and a literal `/skill:name` cannot override disabled or unavailable state. `read_skill_component` accepts only a component advertised by its parent skill; a cross-package component additionally requires its owning skill in that same catalogue. A changed catalogue is therefore visible on the next turn; a turn already executing retains the catalogue with which it started.

The durable profile association preserves profile isolation across service restart and reconnect. A client may supply the same profile when reopening, but cannot rebind an existing session to another profile. Older sessions without an association remain supported; the first profile-aware open binds them durably.

The legacy `enabled_skill_ids` session snapshot remains a compatibility projection for clients and sessions that have no profile association. It is not an authority for profile-managed sessions and cannot override current profile preferences.

## Client surfaces

The built-in TUI has no profile-management model or comparable profile settings screen: it operates one server workspace and creates sessions without an external client profile identity. Adding a TUI-only preference store would make the TUI a second authority. Profile-owning clients such as FStack should therefore use the public API/SDK. A future TUI profile selector can project these same service methods without changing ownership.
