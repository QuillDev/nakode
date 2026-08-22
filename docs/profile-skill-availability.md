# Profile-scoped skill availability

Nakode owns skill discovery and per-profile enablement. Clients manage it through the versioned `ListSkills` and `SetSkillEnabled` service methods (or the equivalent SDK methods) after checking the `SkillAvailability` capability.

## Identity and reconciliation

Preferences are keyed by profile ID and the discovered skill's stable ID, not its display name or path. No preference means enabled, preserving pre-upgrade behavior and making newly discovered skills available by default.

Installed skills are returned with current metadata. A saved identity that is no longer discovered remains in the manageable catalogue with its last known metadata and `available = false`. It cannot be changed until that stable identity is discovered again. Renaming metadata does not lose its preference. Invalid or duplicate discoveries never become manageable rows because normal discovery rejects them before reconciliation.

## Session lifecycle

Availability is snapshotted when a logical session is created. Disabled skills are removed before the initial instructions and provider prompt are published. The resolved **enabled stable IDs** are persisted with the logical session and copied into the provider runtime; `read_skill` authorizes the currently installed definition only when its immutable ID is in that snapshot. This prevents a stale display name or a newly installed same-name skill from acquiring authority.

An already-running session keeps its original catalogue. Workspace reload, provider resume/reconnect, and service restart re-filter discovery through the persisted stable-ID snapshot before rendering either the initial or current catalogue. Changes apply to subsequently created sessions. This avoids changing tool authority in the middle of a provider-owned context.

The explicit persistence semantics are:

- no profile preference rows means every discovered skill is enabled, preserving pre-upgrade behavior;
- a persisted session snapshot containing every discovered ID means all are enabled;
- an explicit empty session snapshot means deny all;
- a missing (`NULL`) session snapshot is a pre-snapshot legacy row, so the first authoritative open defaults to the then-installed catalogue and immediately persists those IDs; subsequent resumes cannot silently expand it.

The enable/disable implementation originally filtered only transient creation state. It did not persist that resolved session set, so resume and workspace reload installed the unfiltered catalogue and advertised it as the authoritative current list. At the same time, `read_skill` parsed the provider runtime's older initial instruction text. A legacy or previously disabled session could therefore advertise `ship-pr-to-green` in the current catalogue while the tool rejected it as unavailable. Persisting and propagating one stable-ID snapshot makes catalogue projection and tool authorization consume the same Nakode-owned authority instead of bypassing the guard.

## Client surfaces

The built-in TUI has no profile-management model or comparable profile settings screen: it operates one server workspace and creates sessions without an external client profile identity. Adding a TUI-only preference store would make the TUI a second authority. Profile-owning clients such as FStack should therefore use the public API/SDK. A future TUI profile selector can project these same service methods without changing ownership.
