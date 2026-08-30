# Profile-scoped skill availability

Nakode owns skill discovery, availability, per-profile enablement, retained unavailable records, and local invocation telemetry. Clients consume that authority through the versioned skill catalogue and typed mutations; they must not inspect Nakode persistence or independently infer availability.

## Availability source of truth

A skill is **available** only when the latest successful deterministic Nakode discovery finds and validates it in one of the configured skill roots:

- machine-local `~/.agents/skills`; or
- workspace-local `<workspace>/.agents/skills`.

Workspace definitions override machine definitions by load name. Discovery treats installed `SKILL.md` frontmatter, Markdown, and referenced Markdown components as inert data. It validates directory and metadata names, stable-identity uniqueness, UTF-8 metadata/content, and safe referenced component paths. Invalid or duplicate definitions reject/fail discovery; they are not converted into manageable unavailable rows.

A previously persisted stable skill identity is **unavailable** when it is absent from the catalogue produced by a successful discovery. Provider, model, credential, runtime, MCP, and tool prerequisites are not current inputs to skill availability. A failed refresh does not authoritatively prove that a skill is absent: Nakode does not apply absence reconciliation from a failed discovery.

Every projected skill includes Nakode's concise `availability_explanation`. An unavailable retained row also includes `availability_reason`. Clients should render those authoritative fields rather than reconstructing the rules above.

## Identity, persistence, and disable reconciliation

Preferences are keyed by profile ID and the discovered skill's stable ID, not its display name or path. No preference means enabled, preserving pre-upgrade behavior and making newly discovered skills available by default.

Installed skills are returned with current metadata. A saved identity that is no longer discovered remains in the manageable catalogue with its last known inert metadata and `available = false`. On startup, explicit refresh, and catalogue listing, Nakode idempotently persists every retained unavailable identity as disabled for each profile record that still retains it. Unavailable rows are always projected with `enabled = false`, and `SetSkillEnabled(enabled = true)` rejects them. Renaming display metadata does not lose the stable preference.

This lifecycle prevents an absent definition from remaining selectable or invocable merely because an earlier profile preference enabled it. Invalid or duplicate discoveries never become manageable rows because normal discovery rejects them before reconciliation.

## Individual prune

Clients may call the typed `PruneSkill` service/SDK mutation after checking the separate `SkillPruning` capability. The request identifies one stable skill ID and the profile that owns the retained association. Successful cleanup is scoped to that exact `(profile_id, skill_id)` preference.

Nakode fails closed unless the stable identity is a retained, unavailable, removable record in the requested profile's catalogue. Unknown identities and every currently discovered/installed definition are rejected. Consequently, pruning cannot silently delete an available package, immutable or built-in definition, installed filesystem content, or another profile's retained association. For an installed row, the authoritative restriction tells the owner to remove the package first, complete a successful discovery refresh, and only then prune its retained unavailable record.

The repository deletes only the matching `skill_preferences` row. Invocation telemetry is installation-wide and has no profile ownership key, so a profile-local prune preserves `invocation_events` and `invocation_aggregates` rather than deleting shared history. After persistence succeeds, Nakode removes the identity only from that profile's in-memory preference cache and reinstalls skill authority only for sessions governed by that profile.

`PruneSkill` is idempotent only while an authoritative retained row exists: after successful removal, a repeated stale request fails closed as unknown rather than broadening its target. Clients should use explicit per-row confirmation, then request a fresh `ListSkills` replacement snapshot. They should not optimistically remove a row before Nakode reconciliation. If the mutation response or refresh is ambiguous, the client must retry catalogue loading before another action rather than presenting stale local state as authoritative.

## Session lifecycle

A profile-managed logical session is durably associated with its profile. Nakode refreshes installed-skill discovery, then resolves the profile's current effective catalogue when the session is created or opened and again at each owner turn. This session-start refresh is required because clients may install or update skill packages while the long-running service remains active. Profile preference changes, unavailable reconciliation, pruning, and explicit installed-skill refreshes update loaded sessions, so open and future sessions converge on the same service-owned authority without waiting for provider-owned context to be rewritten mid-turn.

The turn catalogue is copied into native provider runtimes before prompt construction and tool execution. `read_skill` and `read_skill_component` read only that in-process catalogue: neither rescans the filesystem and a literal `/skill:name` cannot override disabled or unavailable state. `read_skill_component` accepts only a component advertised by its parent skill; a cross-package component additionally requires its owning skill in that same catalogue. A changed catalogue is therefore visible on the next turn; a turn already executing retains the catalogue with which it started.

The durable profile association preserves profile isolation across service restart and reconnect. A client may supply the same profile when reopening, but cannot rebind an existing session to another profile. Older sessions without an association remain supported; the first profile-aware open binds them durably.

The legacy `enabled_skill_ids` session snapshot remains a compatibility projection for clients and sessions that have no profile association. It is not an authority for profile-managed sessions and cannot override current profile preferences.

## Protocol and compatibility

The protocol change is additive:

- `Skill` adds optional availability reason, prune authorization/restriction, and authoritative explanation fields;
- `PruneSkill` adds one typed mutation; and
- `SkillPruning` advertises server support independently from `SkillAvailability`.

Older clients ignore the new protobuf fields and continue to list and enable installed skills. Updated clients connected to older servers decode conservative defaults (`prunable = false`) and must hide/disable prune controls because `SkillPruning` is absent. No database migration is required: profile-scoped reconciliation and cleanup use the existing composite key on `skill_preferences`.

## Client surfaces

The built-in TUI has no profile-management model or comparable profile settings screen: it operates one server workspace and creates sessions without an external client profile identity. Adding a TUI-only preference store would make the TUI a second authority. Profile-owning clients such as FStack should therefore use the public API/SDK, hide unavailable rows by default, expose a distinct opt-in cleanup view, render Nakode's reason/restriction/help fields, and replace their catalogue from Nakode after every mutation. A future TUI profile selector can project these same service methods without changing ownership.
