# Profile-scoped skill availability

Nakode owns skill discovery and per-profile enablement. Clients manage it through the versioned `ListSkills` and `SetSkillEnabled` service methods (or the equivalent SDK methods) after checking the `SkillAvailability` capability.

## Identity and reconciliation

Preferences are keyed by profile ID and the discovered skill's stable ID, not its display name or path. No preference means enabled, preserving pre-upgrade behavior and making newly discovered skills available by default.

Installed skills are returned with current metadata. A saved identity that is no longer discovered remains in the manageable catalogue with its last known metadata and `available = false`. It cannot be changed until that stable identity is discovered again. Renaming metadata does not lose its preference. Invalid or duplicate discoveries never become manageable rows because normal discovery rejects them before reconciliation.

## Session lifecycle

Availability is snapshotted when a logical session is created. Disabled skills are removed before the initial instructions and provider prompt are published. Tool authorization checks the immutable skill catalogue advertised in those instructions, so an explicit `read_skill` request for a disabled, newly installed, or otherwise unadvertised skill is refused rather than substituted.

An already-running session keeps its original catalogue. Resuming that same logical session keeps its persisted original instructions and therefore the same authorization. Changes apply to subsequently created sessions. This avoids changing tool authority in the middle of a provider-owned context.

## Client surfaces

The built-in TUI has no profile-management model or comparable profile settings screen: it operates one server workspace and creates sessions without an external client profile identity. Adding a TUI-only preference store would make the TUI a second authority. Profile-owning clients such as FStack should therefore use the public API/SDK. A future TUI profile selector can project these same service methods without changing ownership.
