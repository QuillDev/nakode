# Nako Agent architecture decisions

## Authority and direction

This file records the de-facto product direction for Nako Agent. Future work must
follow these decisions unless the user explicitly replaces them.

The detailed rationale, diagrams, target data model, and migration sequence are
in:

- `/Users/quill/projects/artifact-library/artifacts/flock-agent-orchestration-direction.html`

The current repository is an intermediate implementation. Remaining code that
conflicts with this direction is migration work, not precedent to preserve.

## Product boundary

Nako Agent is a provider-neutral agent orchestration and continuity layer. It is not
another coding-agent runtime and must not replace the execution environment of
agents such as Codex.

The governing rule is:

> Agents own execution and native context. Nako Agent owns coordination, identity,
> shared knowledge, and provenance.

### Agents own

- Native model context and provider session persistence
- Coding tools, shell execution, file mutation, and sandboxing
- Native approvals, permissions, authentication, and model configuration
- Provider-specific capabilities such as subagents, MCP, browser tools, or
  structured output

### Nako Agent owns

- Logical Nako Agent sessions spanning one or more native agent sessions
- Provider-neutral task, run, role, artifact, and handoff identities
- Delegation, review, bounded fan-out, synthesis, cancellation, and status
- Shared skills and their provider-specific materialization
- Shared memory, provenance, scopes, confidence, and supersession
- Session discovery, coordination metadata, audit history, and TUI state

Nako Agent must not maintain general-purpose `bash`, `edit`, browser, or equivalent
coding tools as part of the target architecture. Providers should expose their
native tools and security models. Nako Agent may expose narrowly scoped control-plane
operations for memory, artifacts, and orchestration through MCP, a CLI, or a
native provider interface. These are not replacements for coding tools.

## Session model

A Nako Agent session is a logical body of work, not an alias for one provider
thread. One Nako Agent session may contain many native agent sessions with different
providers and roles.

- Provider-native sessions remain authoritative for their own model context.
- Provider session IDs are opaque adapter data.
- Nako Agent stores its own logical identity and coordination metadata in SQLite.
- Nako Agent must not use private provider files or indexes as its primary database.
- Resume the same native session when its context matters.
- Move work between agents with an explicit handoff package; never claim that
  hidden model context was translated between providers.

A handoff should contain the objective, completion criteria, constraints,
summary, selected files or diffs, task state, relevant memories, artifacts,
source references, role, budget, and delegation policy.

The target relationship is:

```text
NakoSession 1 ── N AgentSession 1 ── N AgentTurn
       │                 │
       ├── tasks         ├── artifacts
       ├── runs          └── handoffs
       └── memories
```

The current `SessionRecord` mapping one Nako Agent ID to one provider session is
useful groundwork but is not the final data model.

## Backend adapters

Codex is the first adapter, not the application model. New providers must be
addable without changing shared session, task, memory, skill, or UI semantics.

All enabled providers participate in the runtime by default. Nako Agent must not
have a global backend selector: backend choice belongs to a model, agent
session, task, or orchestration run. Model identities are always canonical,
provider-qualified slugs in the form `provider-slug/model-slug`; model search,
persistence, handoffs, and user-facing selection use that same form. A single
logical Nako Agent session may use models from multiple providers, including for
delegated workers and reviewers.

Backend contracts should expose lifecycle operations plus a capability
snapshot. Capabilities may include resume, steering, interruption, forking,
native tools, MCP, native skill injection, approvals, structured output, and
subagents.

- Normalize provider wire events before they enter shared state.
- Keep provider protocol types inside adapter modules.
- Degrade UI and orchestration behavior from declared capabilities.
- Report unsupported operations explicitly instead of simulating incompatible
  semantics.
- Preserve native authentication, model selection, tools, and approval policy.
- Record the capability snapshot used by each orchestration run.
- Treat provider enablement as a persisted registry preference. A disabled
  provider is unavailable for new work; it is not replaced by a process-wide
  `--backend` mode.

Nako Agent-launched sessions run unattended by default. Each adapter must use
the provider's native strongest non-interactive permission mode rather than
emulating permissions in shared code. Codex uses `approvalPolicy: never` with
`danger-full-access`; Devin uses its `dangerous` permission mode. Unexpected
provider approval requests must be accepted inside the adapter and must not
interrupt the TUI. Apply the equivalent native mode when adding a provider.

Do not disable useful native provider capabilities merely to create a lowest
common denominator across backends. Safety policy may still restrict a
capability for a specific role or run; that restriction must be explicit and
auditable.

## Orchestration

Orchestration is explicit, bounded, and auditable. Initial primitives are:

- `delegate` — assign one bounded task to a worker
- `review` — inspect an artifact or patch, read-only by default
- `handoff` — move work to another native session with selected context
- `fan_out` — run independent investigations with bounded concurrency
- `synthesize` — combine outputs through one designated owner
- `cancel` — stop a run or native turn when supported

Each run records its initiator, role, backend, native session, inputs, skill
set, budget, permission envelope, child-run policy, outputs, artifacts, and
status.

Do not give every child unrestricted orchestration access. Workers and
reviewers must not recursively spawn agents unless an explicit nested
orchestrator role grants a bounded allowance. Keep one writer for a shared
workspace unless isolation is deliberate.

Provider-native subagents may implement a Nako Agent delegation only when the
adapter can attribute and supervise the child work, or when the run is clearly
recorded as opaque. Do not confuse a provider's subagent feature with permission
to expose Nako Agent's orchestration API recursively.

## Skills

Skills are portable behavioral packages, not provider-specific prompt strings.
A skill may contain instructions, scripts, templates, validation, role
applicability, capability requirements, and provider adaptations.

Adapters should expose a skill through the most native supported mechanism:

1. Native provider skill packaging
2. Generated instruction files such as `AGENTS.md`
3. Session-creation instructions
4. A skill attached to one delegated turn
5. An explicit handoff-prompt fallback

A skill describes when and how to use a capability. It does not replace the
service or interface that performs persistent operations.

## Memory

Memory is a Nako Agent-owned service with multiple access surfaces:

- Internal Rust API for the orchestrator
- MCP interface when a backend supports it reliably
- `nako-agent memory` CLI fallback for agents with shell access
- A portable memory skill that teaches usage policy

MCP supplies structured capability; the skill supplies behavior. Neither alone
is sufficient across all providers.

The initial memory operations should be small and semantic: `search`,
`propose`, `store`, and `supersede`. Avoid exposing database-shaped CRUD.

- Workers should normally search and propose.
- Durable writes are policy-controlled.
- Prefer project scope; global scope requires deliberate authorization.
- Store durable facts, decisions, and preferences, not routine execution logs.
- Search for duplicates before writing.
- Supersede outdated entries instead of silently contradicting them.
- Record scope, author agent and backend, source session and turn, kind,
  confidence, creation and expiration timestamps, supersession links, and
  sensitivity classification.

## Persistence

SQLite remains the Nako Agent-owned metadata store. The target model should separate
at least:

- `nako_sessions`
- `agent_sessions`
- `agent_turns`
- `orchestration_runs`
- `tasks`
- `artifacts`
- `handoffs`
- `memory_entries`
- `skill_installations`

Provider-native history remains authoritative for provider execution context.
Nako Agent persistence owns discovery, relationships, shared state, and provenance.

## Migration order

1. Define provider-neutral backend events, capabilities, logical sessions,
   agent roles, and native-session records.
2. Introduce explicit handoff packages and many-native-sessions-per-Nako-Agent-
   session persistence.
3. Build the memory service and internal API, then CLI and MCP surfaces.
4. Package memory policy as the first portable cross-provider skill.
5. Add bounded `delegate` and `review` flows before fan-out or nested
   orchestration.

## Code principles

Code must be clear, direct, self-documenting, and compliant with the project's
quality rules. Do not bypass a lint because compliance requires more design or
implementation work. Refactor the code so that its types, ownership, control
flow, and decomposition express the intent explicitly.

Do not add `#[allow(...)]`, manifest-level lint exemptions, or other lint
suppression. If a lint exposes an architectural problem, fix the architecture.

### Process boundaries

Keep terminal rendering and input, application state, orchestration, session
persistence, backend contracts, provider adapters, skills, memory, artifacts,
and PTY/process supervision as separate boundaries.

Continue preserving the proven foundations: Ratatui/Crossterm terminal
ownership, bounded channels, normalized event reduction, provider-native
authentication, queue-versus-steer semantics, SQLite metadata, and terminal and
child-process cleanup on every exit path.

### Change discipline

Remove superseded implementation paths deliberately: update fixtures,
dependencies, documentation, and tests in the same change, and preserve current
behavior until its provider-native replacement is verified.

### Rust quality gate

The crate enables Clippy's `pedantic` lint group. Treat those lints as required
code quality rules, not optional suggestions.

Do not interrupt ordinary implementation work to chase Clippy warnings after
every edit. Batch that cleanup into the final pre-commit pass. Before creating
any commit, run:

```text
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Fix every reported warning before committing. A work-in-progress tree may
temporarily have warnings, but a commit must not.
