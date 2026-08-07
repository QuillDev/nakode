# Nakode product and engineering constitution

## Authority and interpretation

This file is the authoritative product constitution for Nakode. It defines
the product goal, ownership boundaries, architectural invariants, and coding
standards. Work in this repository must follow it unless the user explicitly
replaces a decision here.

This is not an implementation inventory, release plan, or migration roadmap.
Statements are durable requirements regardless of how much of the architecture
is already implemented. Existing code that conflicts with them is migration
debt, not precedent to preserve. Preserve working behavior while replacing that
debt, and do not present an unimplemented capability as available.

## Product boundary

Nakode is a provider-neutral agent orchestration, continuity, and execution
application. Its distributed executable must be self-contained: a user must be
able to install Nakode, configure a supported provider, and run an agent
without installing Codex, Devin, OMP, Node.js, Python, or another agent harness.
Optional tools such as language evaluators may use an external runtime when it
is installed, but must report their absence locally and must not prevent Nakode
Agent or unrelated tools and providers from starting.

The governing rule is:

> Nakode owns its portable runtime and coordination plane. Providers own
> inference semantics. Optional external harnesses own only the sessions that
> explicitly select those compatibility adapters.

The frontend boundary is equally strict:

> The Nakode server owns all canonical product state and every operation that
> can mutate it. A frontend is only a renderer and input adapter over the
> public SDK. The TUI has no privileged access to application behavior.

This is a hard project boundary. New product capabilities are server
capabilities first, public API operations second, SDK methods third, and
frontend rendering last. A feature implemented only inside the TUI is not a
Nakode feature and must not be merged as one.

### In-process provider adapters own

- Provider authentication and token refresh
- Provider protocol encoding, request validation, streaming, and errors
- Model discovery and provider-specific model configuration
- Provider conversation identifiers and resumable response state
- Provider-specific inference features such as structured output, hosted tools,
  prompt caching, encrypted reasoning state, and reasoning controls

Provider protocol types and wire events must remain inside adapter modules.
Adapters normalize their output before it reaches shared runtime or application
state.

### Nakode owns

- Logical Nakode sessions spanning one or more native agent sessions
- Provider-neutral task, run, role, artifact, and handoff identities
- Delegation, review, bounded fan-out, synthesis, cancellation, and status
- The portable agent loop and local coding-tool contracts
- Local process execution, file mutation, workspace policy, and supervision
- Approval policy, permission envelopes, unattended mode, and cancellation for
  in-process sessions
- Shared skills and their provider-specific materialization
- Shared memory, provenance, scopes, confidence, and supersession
- Provider enablement and user model-default preferences
- Session discovery, coordination metadata, audit history, controls, and
  canonical client-independent application state

The portable runtime exposes clear provider-neutral tools rather than copying
the private implementation of an existing harness. Provider-hosted tools may be
used when their execution location and semantics match the task. Local
workspace work must use Nakode's supervised local tools so it never
silently executes in a provider-hosted filesystem.

External executables may be supported by optional compatibility adapters. Such
an adapter must declare its executable requirement and degrade independently
when missing. It must never prevent the application or unrelated providers from
starting.

## Server and client architecture

Nakode is a client-server application. The long-running Nakode server is the
single authority for domain state and execution; the TUI is one thin client of
that server, not the owner of the application. Web, desktop, CLI, IDE, mobile,
and automation clients must be able to use the same service contract without
reimplementing orchestration semantics.

The server is the product runtime. Frontends are replaceable projections of
that runtime. No frontend may gain special capabilities by being written in
Rust, shipped in the same executable, or maintained in this repository.

The server owns:

- Logical sessions, transcripts, turns, tasks, queues, and artifacts
- Provider connections, native agent sessions, inference streams, and retries
- Tool execution, process supervision, agents, subagents, and orchestration
- Approvals, questions, permission policy, cancellation, and unattended work
- Configuration, credentials, persistence, audit history, and canonical state
- Client subscriptions, ordered event publication, and reconnectable snapshots

The server must continue running and supervising active work with zero attached
clients. Closing, crashing, or replacing a TUI must not cancel work or transfer
ownership of a session. CLI and automation calls address the server directly;
they must not be routed through a TUI or require a TUI to be running.

Clients own only presentation and device concerns:

- Input capture and translation into protocol actions
- Rendering server-provided semantic state
- Draft text before submission, focus, scroll, selection, viewport dimensions,
  local navigation, and other per-client ephemeral presentation state
- Terminal, browser, desktop, or IDE lifecycle integration specific to that
  client

A client must not open provider connections, execute tools, mutate persistence,
or maintain an authoritative copy of domain state. It must not infer a domain
transition from a snapshot, reconcile provider events, choose queue-versus-run
policy, manage session continuity, or perform an operation because the server
API lacks an edge. Every operation that changes Nakode state is a typed SDK call
to the server. Purely presentational interactions such as typing an unsent
draft, scrolling, selecting text, or resizing remain local and must not create
server traffic merely to redraw a view.

### Service protocol

The service boundary is the explicit, versioned, language-neutral Protobuf
contract in `proto/nakode/v1/nakode.proto`, served over gRPC. It has three
concepts:

1. **Commands** request domain changes and receive accepted or rejected results.
2. **Queries and snapshots** establish a complete client view at a known
   revision.
3. **Watches** publish authoritative replacement snapshots to subscribers.

Protocol messages use stable resource IDs, monotonic revisions, and unique
idempotency keys for mutating commands. A reconnecting client must be able to
request a fresh snapshot or continue from a known revision without guessing
what changed. Slow clients must not block inference, tools, persistence, or
other subscribers. Protocol evolution must be explicitly versioned, and a
version mismatch must fail clearly rather than silently misinterpreting state.

Snapshots describe semantic product state, not terminal cells, Ratatui widgets,
or provider wire events. Clients replace their observed product projection and
render it locally; they do not replay the server's reducer. gRPC is the sole
public frontend transport. Additional network or browser transports may be
added only as adapters over the same Protobuf service and must not introduce a
second product contract or new domain semantics.

Multiple clients may observe one session concurrently. Mutating commands must
have deterministic conflict and authorization semantics. Interactive requests
such as approvals and questions remain server-owned resources with explicit
resolution status so simultaneous clients cannot resolve them inconsistently.

The public API must expose every capability needed to build a feature-complete
alternative frontend. SDKs own connection recovery, idempotent mutation retry,
watch resubscription, paging, hydration, and session open/create selection so
frontend code only calls distinct product methods and renders their state.
Generated transport stubs are the base layer; supported language SDKs add this
shared behavior without inventing client-side domain policy.

`CreateSession` applies an optional initial model, its options, and optional
client-owned tools atomically before the logical session is published.
`OpenSession` installs tools before closed-session provider restoration and
accepts only an identical table when reattaching an already-loaded session. An
omitted model keeps the server's workspace/provider default, options without a
model are invalid, and clients must not emulate initial selection or tools with
a follow-up mutation. The effective session-scoped options are projected on
`SessionView`/`SessionState`; clients must not reconstruct them from a catalogue
model's workspace defaults.

Client-owned tools use the session-scoped public external-tool contract. The
server owns pending calls and turn suspension; clients only execute a call and
submit its result.

## Session model

A Nakode session is a logical body of work, not an alias for one provider
thread. One logical session may contain many native agent sessions using
different providers, models, and roles.

- Nakode assigns and persists its own logical identities.
- Provider session and response IDs are opaque adapter data.
- In-process sessions persist the normalized history and provider state needed
  to resume through Nakode.
- Optional harness-backed sessions remain authoritative for their own hidden
  context only when that compatibility adapter is explicitly selected.
- Nakode must not use private provider files or indexes as its primary
  database.
- Resume the same native session when its context matters.
- Move work between native sessions with an explicit handoff package. Never
  claim that hidden model context was translated between providers.

A handoff contains the objective, completion criteria, constraints, summary,
selected files or diffs, task state, relevant memories, artifacts, source
references, role, budget, and delegation policy.

The required relationship is:

```text
NakodeSession 1 ── N AgentSession 1 ── N AgentTurn
       │                 │
       ├── tasks         ├── artifacts
       ├── runs          └── handoffs
       └── memories
```

Do not model a Nakode session as a one-to-one alias for a provider session,
even when adapting a transitional persistence shape that does so.

## Provider adapters and models

No provider is the application model. OpenAI and Devin use in-process adapters,
and additional providers must be addable without changing shared session, task,
memory, skill, tool, control, or UI semantics.

All enabled providers participate in the runtime. Backend choice belongs to a
model, native agent session, task, or orchestration run. Model identities are
canonical provider-qualified slugs in the form `provider-slug/model-slug`.
Search, persistence, defaults, handoffs, delegated-agent configuration, and
user-facing selection use that same form.

Provider slugs such as `openai-codex` and `devin-acp` are persisted identities.
Do not casually rename them or introduce a second spelling at a boundary. New
provider identifiers name the provider contract rather than an implementation
process.

Backend contracts expose lifecycle operations and a capability snapshot.
Capabilities may include resume, steering, interruption, model discovery,
session model configuration, native tools, MCP, native skill injection,
approvals, structured output, and subagents.

- Normalize provider wire events before shared state consumes them.
- Keep authentication and provider protocol structures inside adapter modules.
- Degrade UI and orchestration behavior from declared capabilities.
- Report unsupported operations explicitly instead of simulating incompatible
  semantics.
- Preserve useful provider-native inference features instead of forcing a
  lowest common denominator.
- Record the capability snapshot used by each orchestration run.
- Queue redirection uses native steering when compatible; otherwise interruption reserves the selected prompt until the stopped provider process has exited and released its session, then starts it exactly once.
- Treat provider enablement as a persisted registry preference. A disabled
  provider is unavailable for new work; it is not replaced by a `--backend`
  process mode.
- Treat `/models` as a durable provider-default selection and `/switch` as a
  current-session override. Session-only choices must reset to the persisted
  default when a new session begins.
- A session model or effort change made while provider work is in flight applies
  to the next turn. Persist the logical override immediately, but do not address
  native session state that an active turn or compaction has temporarily taken
  out of its adapter's idle-session map.

Nakode-launched sessions run unattended by default. In-process sessions use
the shared runtime's explicit permission and cancellation policy. Optional
harness adapters use the harness's strongest non-interactive mode. Unexpected
provider approval prompts must not block the TUI. Any safety restriction on a
capability must be explicit, role-scoped, and auditable.

## Portable runtime and tools

The agent loop, inference stream, and tool contracts are provider-neutral.
Provider adapters receive normalized inference requests and return normalized
stream events and outputs. They must not own local file or process semantics.

Local tools must:

- Resolve relative paths against the configured workspace.
- Use bounded output and cancellation-aware execution.
- Keep process supervision and terminal ownership outside provider modules.
- Return explicit failures rather than silently substituting hosted execution.
- Preserve one-writer workspace policy unless isolation is deliberate.
- Keep interactive questions in the shared question/event path.

The base tool registry intentionally excludes unrestricted recursive
orchestration tools. Delegation is a control-plane capability and must remain
bounded and attributable.

## Orchestration

Orchestration is explicit, bounded, and auditable. Its primitives are:

- `delegate` — assign one bounded task to a worker
- `review` — inspect an artifact or patch, read-only by default
- `handoff` — move work to another native session with selected context
- `fan_out` — run independent investigations with bounded concurrency
- `synthesize` — combine outputs through one designated owner
- `cancel` — stop a run or native turn when supported

Each run records its initiator, role, provider, model, native session, inputs,
skill set, budget, permission envelope, child-run policy, outputs, artifacts,
capability snapshot, and status.

Do not give every child unrestricted orchestration access. Workers and
reviewers must not recursively spawn agents unless an explicit nested
orchestrator role grants a bounded allowance. Keep one writer for a shared
workspace unless isolation is deliberate.

Provider-native subagents may implement a Nakode delegation only when the adapter
can attribute and supervise the child work, or when the run is clearly recorded
as opaque. Do not confuse a provider's subagent feature with permission to
expose Nakode's orchestration API recursively.

## Skills

Skills are portable behavioral packages, not provider-specific prompt strings.
A skill may contain instructions, scripts, templates, validation, role
applicability, capability requirements, and provider adaptations.

Adapters expose a skill through the most native supported mechanism:

1. Native provider skill packaging
2. Generated instruction files such as `AGENTS.md`
3. Session-creation instructions
4. A skill attached to one delegated turn
5. An explicit handoff-prompt fallback

A skill describes when and how to use a capability. It does not replace the
service or interface that performs persistent operations. A `/skill:` reference
resolves through the shared skill service; syntax highlighting alone does not
constitute skill support.

## Memory

Memory is a Nakode-owned service with multiple access surfaces:

- Internal Rust API for the orchestrator
- MCP interface when an adapter supports it reliably
- `nakode memory` CLI fallback for agents with shell access
- A portable memory skill that teaches usage policy

MCP supplies structured capability; the skill supplies behavior. Neither alone
is sufficient across all providers.

Memory operations are small and semantic: `search`, `propose`, `store`, and
`supersede`. Avoid database-shaped CRUD.

- Workers normally search and propose.
- Durable writes are policy-controlled.
- Prefer project scope; global scope requires deliberate authorization.
- Store durable facts, decisions, and preferences, not routine execution logs.
- Search for duplicates before writing.
- Supersede outdated entries instead of silently contradicting them.
- Record scope, author agent and provider, source session and turn, kind,
  confidence, creation and expiration timestamps, supersession links, and
  sensitivity classification.

## Persistence

SQLite is the Nakode-owned metadata and in-process runtime store. The data model
separates at least:

- `nakode_sessions`
- `agent_sessions`
- `agent_turns`
- `orchestration_runs`
- `tasks`
- `artifacts`
- `handoffs`
- `memory_entries`
- `skill_installations`

Nakode persists provider response identifiers, normalized history, and
provider state needed to resume in-process sessions. Optional harness-native
history remains authoritative only for explicitly harness-backed sessions.
Nakode persistence owns discovery, relationships, shared state, preferences, and
provenance.

Schema changes must be forward migrations that preserve existing installations.
Do not repurpose a table into a semantically different entity merely because
its columns are convenient.

## Controls and command registration

`src/controls.rs` is the single registration boundary for user controls. A
keyboard shortcut, mouse action, or slash command must be registered there
before application code can use it.

The registry owns:

- Stable action identifiers
- The contexts in which an action is active
- Key, modifier, mouse, and slash bindings
- Command-completion descriptions and placement rules
- Primary `Ctrl+?` help grouping, labels, and descriptions

Client input handling resolves registered actions and then either updates
purely local presentation state or emits a typed service command. It must not
invoke provider, persistence, tool, or orchestration code directly. Do not add
direct `KeyCode` or modifier comparisons for a new action in an input handler.
Raw text insertion and extraction of dynamic data from an already registered
pattern, such as a numbered question choice, are the narrow exceptions.

When adding or changing a control:

1. Add or reuse a `ControlAction` and register its binding under the narrowest
   valid `ControlContext` in `src/controls.rs`.
2. Route the resolved action to either local presentation behavior or a typed
   service command. Business transitions run in server-owned domain state, not
   in the client.
3. Add help metadata for controls users need to discover globally. Contextual
   overlays may compose local hints, but those hints do not define behavior and
   must agree with the registry.
4. For a slash command, add its `SlashAction` and `SlashControl`. Parsing,
   highlighting, completion, and help discovery must consume that registration
   rather than a second command list.
5. Add tests for resolution, context isolation, command parsing, rendering when
   relevant, and ambiguous bindings.

`Ctrl+?` is rendered from registry metadata. Do not restore a manually curated
duplicate list in `src/render.rs`. Registry tests must continue rejecting
ambiguous bindings within one context.

## Interface language and affordances

Prefer brief language and intuitive interaction over explanatory UI copy. The
interface should communicate through layout, focus, labels, state, and familiar
affordances rather than instructions.

- Use short labels and compact contextual key hints, not prose.
- Never use instructional sentences such as “Press X to continue.” If an action
  requires that explanation, make its affordance or focus state clearer.
- Reveal controls and information only when they become relevant. Hide
  unavailable capabilities, implementation details, and premature setup text.
- Make focus and selection visually obvious. Text entry must require deliberate
  focus; typing or pasting must not target an unfocused field.
- Do not explain routine behavior such as masking, persistence, editing, or
  cancellation unless the user must make a consequential choice.
- Prefer progressive disclosure. The default view contains only what is needed
  for the next decision.

## Process and UI boundaries

The process boundary is an architectural boundary, not merely a module
organization preference:

- The server process owns the domain reducer, provider-neutral engine, provider
  adapters, runtime, tools, persistence, credentials, process supervision, and
  all ongoing work.
- Client processes own their platform lifecycle, input mapping, ephemeral view
  state, and rendering.
- A protocol package owns shared command, query, snapshot, event, error,
  identity, revision, and capability types. It contains no Ratatui,
  Crossterm, provider-wire, SQLite, or process-management types.
- Transport adapters own framing, connection lifecycle, authentication, and
  subscriptions without acquiring domain authority.

For the terminal client specifically:

- `src/terminal.rs` — terminal acquisition, modes, and restoration
- `src/render.rs` — Ratatui layout and presentation only
- `src/controls.rs` — control discovery and input binding metadata
- TUI application code — connection lifecycle, protocol action emission,
  event/snapshot consumption, and per-client presentation state only

Server-side code owns provider-neutral transitions and effect execution.
Provider wire adapters normalize events before the engine consumes them.
Compatibility adapters stay isolated from primary native adapters. Rendering
must never perform persistence, provider, tool, or orchestration operations,
and server modules must never depend on TUI state or rendering types.

Existing `src/app.rs` and `src/state.rs` responsibilities must not be treated as
authority by filename. `src/app.rs` is client event-loop and effect wiring;
server/domain modules own canonical transitions. Presentation types may project
server snapshots but must never become a second domain model. The production
TUI must use the same public gRPC/SDK path available to an external frontend,
even when client and server ship in one executable.

Both sides must clean up what they own: clients restore terminal or platform
state on every exit path, while the server supervises and cleans up child
processes independently of client connections.

## Headless TUI evaluation

Use Nakode's deterministic TUI evaluation harness for interaction, rendering,
and UI smoke tests. It runs fully headlessly: it does not open a window, acquire
the desktop terminal, start a provider process, touch persistence, or parse ANSI
output. It drives the real control registry, client input reducer, presentation
state, and Ratatui renderer through an in-memory terminal.

Run the committed smoke scenario with:

```text
cargo run -- --workspace . tui-eval \
  --scenario tests/tui_scenarios/agent_smoke.jsonl
```

The harness reads JSON Lines actions from `--scenario` or standard input and
emits one structured JSON observation per action. Use it to:

- send real key, text, paste, mouse, and resize events;
- install service-owned snapshot changes for sessions, turns, streamed
  items, approvals, questions, todos, warnings, failures, and disconnects;
- inspect rendered lines, cursor state, optional style runs, per-client
  presentation state, recent view entries, and emitted service commands;
- assert screen content, modal state, status, draft contents, connection,
  effects, cursor visibility, and terminal dimensions.

Prefer semantic assertions for behavior and a small number of rendered-text
assertions for important affordances. Request styled snapshots only for focused
visual evaluation. A failed assertion must exit nonzero, identify its scenario
line, and retain the failed observation for diagnosis.

Keep reusable end-to-end scenarios under `tests/tui_scenarios/` and execute
them from Rust tests so they cannot silently drift. Extend client harness
fixtures at the service protocol boundary; test the headless server separately
through commands, snapshots, and event subscriptions. Provider wire-protocol
fixtures remain inside their adapter tests. The complete action, fixture,
assertion, and observation reference lives in `docs/tui-evaluation.md`.

The headless harness complements `tests/tui_terminal.rs`. Use the PTY tests only
for behavior that depends on the actual executable or terminal lifecycle, such
as Crossterm mode changes, alternate-screen ownership, control-service
coordination, and terminal restoration.

## Portability and development lifecycle

User-facing operating-system integrations use maintained cross-platform
abstractions. Do not encode a single desktop or operating system by invoking
commands such as `xdg-open`, `open`, or `start` directly. Browser and file
opening must use the shared platform-neutral opener and surface failure without
crashing the TUI.

`dev.sh` owns the lifecycle of the interactive development instance for its
resolved workspace:

- Every interactive invocation stops the existing Nakode development
  process for that workspace before starting its replacement.
- Restart logic identifies the listener precisely and refuses to terminate an
  unexpected process.
- `--clean` starts with isolated application data and agent configuration, with
  the same observable state as a fresh installation. It also follows the normal
  restart rule.
- Development runs execute as the desktop user, not through `sudo`, so browser
  integration, credentials, and created files retain the correct user context.
- Non-interactive informational and control-service subcommands do not stop the
  interactive instance merely because they share the launcher.

## Code principles

Code must be clear, direct, self-documenting, and compliant with project
quality rules. Refactor types, ownership, control flow, and decomposition so
they express intent explicitly.

Do not add `#[allow(...)]`, manifest-level lint exemptions, or another lint
suppression. If a lint exposes an architectural problem, fix the architecture.

Remove superseded implementation paths deliberately. Update fixtures,
dependencies, documentation, migrations, and tests in the same change. Preserve
user-visible behavior until its provider-native replacement is verified.

Keep changes scoped. Preserve unrelated work and any modification whose
ownership is unclear in a dirty worktree.

## Rust quality gate

The crate enables Clippy's `pedantic` lint group. Treat those lints as required
code quality rules, not optional suggestions.

Do not interrupt ordinary implementation work to chase Clippy warnings after
every edit. Batch cleanup into the final verification pass. Before creating any
commit, run:

```text
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Fix every reported warning before committing. A work-in-progress tree may
temporarily have warnings, but a commit must not.
