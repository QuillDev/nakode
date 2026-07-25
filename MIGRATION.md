# Architecture migration direction

This document defines the next three architecture migrations for Nakode. They
are intentionally ordered because each one establishes contracts used by the
next. Every migration must preserve existing installations through forward
SQLite migrations and must pass the complete Rust quality gate before it is
merged.

## Merge order

1. Unified persistence ownership
2. Logical and native agent sessions
3. Permission envelopes and approval auditing

Each item is developed on its own branch and worktree. A later branch is
rebased onto the composed integration branch only after the previous item has
passed its tests and been merged. The final pull request contains the cleanup
baseline plus all three commits in this order.

## 1. Unified persistence ownership

### Direction

`SqliteSessionRepository` becomes the single schema bootstrap boundary. Other
stores must not depend on callers knowing which repository happened to create
their tables first.

Introduce an initialized database value owned by the persistence layer. It
identifies a database whose connection policy and forward migrations have
already been applied. Session, credential, and runtime-session stores are
constructed from that value instead of independently accepting an unchecked
path and assuming their tables exist.

This is an ownership correction, not a connection-pooling project. Components
may continue to open short-lived SQLite connections where appropriate, but
schema readiness and database configuration have one owner.

### Implementation

1. Add a persistence bootstrap type that:
   - resolves and protects the database path;
   - applies SQLite connection configuration;
   - runs all schema migrations exactly once per open operation;
   - exposes a validated database path or focused store constructors.
2. Move credential and native-runtime table initialization into the bootstrap
   migration batch.
3. Change `SqliteSessionRepository`, `SqliteCredentialStore`, and
   `RuntimeSessionStore` construction to require the initialized database.
4. Update application startup and provider adapter configuration so raw
   database paths are not threaded through provider modules.
5. Update tests that currently create a session repository only as an implicit
   prerequisite for another store.

### Compatibility and failure behavior

- Existing database paths and tables remain valid.
- Migrations remain additive and idempotent.
- Startup fails with an explicit persistence error if bootstrap fails.
- No provider adapter owns schema creation or persistence policy.

### Acceptance

- Credential and runtime stores can be constructed in tests without an unused
  session repository side effect.
- Concurrent bootstrap remains serialized and deterministic.
- Existing session, credential, model, subagent, and runtime-resume tests pass.

## 2. Logical and native agent sessions

### Direction

Persist the constitutional relationship:

```text
nakode_sessions 1 ── N agent_sessions 1 ── N agent_turns
        │                    │
        ├── tasks            ├── artifacts
        ├── runs             └── handoffs
        └── memories
```

A Nakode session is the stable body of work. Provider-qualified model choice,
opaque provider session IDs, provider resume state, and normalized native
history belong to an agent session underneath it.

### Schema migration

1. Add `nakode_sessions` with the stable logical identity, workspace, title,
   and timestamps.
2. Add `agent_sessions` with:
   - its own Nakode-assigned ID;
   - `nakode_session_id` foreign key;
   - provider slug;
   - opaque provider session ID;
   - provider-local model;
   - role and lifecycle timestamps.
3. Add `agent_turns` for normalized turn identity and lifecycle metadata.
4. Backfill every legacy `sessions` row as one logical session containing one
   initial agent session. Preserve the legacy logical ID where possible so
   resume links continue to work.
5. Keep the legacy table readable during the migration boundary, then route all
   repository operations through the new records. Do not repurpose the legacy
   table into a different entity.

### Runtime and state changes

1. Make `AppState::nakode_session_id` the persisted logical identity from
   session creation onward.
2. Track the active agent-session ID separately from its opaque provider ID.
3. Provider switching creates or selects another agent session under the same
   Nakode session instead of clearing the logical identity.
4. Session discovery lists logical sessions; resume selects the appropriate
   active agent session and restores its provider state.
5. Attach orchestration runs, subagents, and handoffs to the logical session
   while retaining their initiating agent-session provenance.

### Compatibility and failure behavior

- Existing installations are migrated forward without losing resume data.
- Provider session IDs remain opaque and provider-scoped.
- A failed provider handoff cannot replace or delete the logical session.
- No hidden provider context is claimed to have moved between providers.

### Acceptance

- A migrated legacy session resumes through its original provider.
- One logical session can persist and resume two provider-native child
  sessions.
- Switching providers preserves the logical ID and creates an explicit
  handoff.
- Recent-session discovery contains one logical entry rather than one entry per
  provider child.

## 3. Permission envelopes and approval auditing

### Direction

Every in-process orchestration run and delegated agent session receives an
explicit, bounded permission envelope. Provider approval prompts and local
process/file operations are resolved against that envelope through one shared
policy path.

Unattended operation is a recorded policy, not an unconditional
`AcceptForSession`. Requests outside the envelope are declined and surfaced as
auditable failures.

### Data model

Persist with each orchestration run:

- role and initiator;
- workspace scope;
- allowed operation classes;
- process and file-mutation policy;
- approval lifetime and unattended behavior;
- child-run allowance;
- creation source and capability snapshot.

Persist approval decisions with:

- run, logical-session, agent-session, and turn provenance;
- provider request ID and approval kind;
- normalized operation summary;
- decision and policy rule used;
- timestamp and decision source.

Sensitive command or file content must follow the existing transcript and
credential redaction rules.

### Runtime and tool changes

1. Add a provider-neutral permission-envelope type at the runtime/tool contract
   boundary.
2. Carry it through `ToolContext` and the orchestration run state.
3. Require every process-capable tool, including PTY Bash and Eval kernels, to
   authorize before spawning.
4. Keep Hypa as an optional policy input or rewrite layer, never as a
   fail-open replacement for the runtime decision.
5. Route primary and delegated provider approval requests through the same
   policy evaluator.
6. Emit explicit approval request and resolution events for persistence and
   transcript projection.

### Compatibility and failure behavior

- Preserve the current unattended default only where the default envelope
  explicitly grants the operation.
- Missing or malformed policy fails closed for mutating/process operations.
- Read-only provider events and unrelated providers continue operating when a
  requested capability is denied.
- Compatibility adapters may translate decisions, but cannot expand them.

### Acceptance

- Delegated sessions no longer unconditionally accept every provider approval.
- Bash capture mode, Bash PTY mode, and Eval all traverse the same authorization
  boundary.
- Allowed, denied, and approval-required operations have deterministic tests.
- Decisions survive restart with complete run and session provenance.

## Quality and smoke-test gate

After each merge and again on the composed branch:

```text
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

The final smoke test uses an isolated application-data directory and exercises:

1. database bootstrap and provider catalog startup;
2. creation and discovery of a logical session;
3. creation of a provider-native child session;
4. restart and resume through persisted state;
5. one allowed and one denied supervised local operation;
6. clean control-service, provider, terminal, and child-process shutdown.

