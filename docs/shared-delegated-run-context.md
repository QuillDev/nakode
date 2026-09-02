# Shared delegated-run context

Nakode owns one bounded shared-context channel for each logical session's delegated run tree. The primary run and every attributed delegated descendant in that session are eligible readers; entries never cross logical-session or workspace boundaries. FStack renders the public `SessionState` projection and does not read or reproduce Nakode persistence.

Entries are concise, inert evidence rather than instructions or social chat. Accepted kinds are `finding`, `decision`, and `validation`. Each entry carries a session-local monotonic sequence, caller idempotency key, optional author run, author label, body, and timestamp. Consumers must treat bodies as untrusted plain text.

Before a delegated run starts, Nakode ranks retained entries against the task's path, symbol, subsystem, command, and decision terms. It injects at most eight matching entries and 12 KiB as a task-relevant briefing, restored to sequence order after ranking. If nothing matches, the briefing falls back to the latest three entries. A child with a non-`none` tool profile may query `search_shared_context` when the briefing is insufficient. Search is bound to the logical owner session and attributed requester run, accepts only the three context kinds, and returns 1–16 matching entries as an explicitly untrusted evidence block. This gives the model a small default context plus deliberate retrieval instead of a full retained-context prompt dump.

## Bounds and replay behavior

- Entry bodies are limited to 4 KiB.
- Idempotency keys are limited to 128 bytes and scoped to the logical session.
- Nakode retains and projects the latest 64 entries in sequence order.
- An identical retry of a retained key returns its original entry without adding or reordering context. Reusing that key with replacement content is rejected.
- Successful delegated terminal reports are deposited once as attributed `finding` entries, allowing the parent and later eligible descendants to consume the result without replaying a transcript.

The public `PublishSharedContext` RPC and SDK method provide typed publication. Session snapshots expose retained entries and the monotonic total/last sequence so clients can report omitted earlier context. FStack makes a populated snapshot available through the Agent center's **Add tab** menu; Shared Context remains absent from the center until the owner opens it, is closable, and renders ordered bodies as inert text.

## Validation evidence

Native runtime sessions retain up to 32 successful routine-validation identities as a local compatibility cache. An identity contains the normalized command, working directory, and relevant Git `HEAD`/status/diff fingerprint. When the provider session is attached to Nakode, the authoritative copy is also published as a logical-session `validation` entry keyed by that fingerprint, so a fresh parent or delegated runtime in the same run tree reuses the result after resume. Repeating the same routine validation against unchanged relevant state returns a successful, attributed skip rather than a failed tool call; a concrete `reason` permits intentional execution. Changed state produces a new identity, and failed commands are never recorded.

## Utilization observability

Each delegated run records the number and bytes of entries in its automatic briefing, whether latest-entry fallback was needed, and the count, returned-entry total, and server duration of explicit shared-context searches. These bounded counters persist with the authoritative run and are projected through the public protocol. They complement existing run starts, tool calls, token usage, and duration so clients can inspect whether transferred context is replacing enough exploration to justify its cost. FStack aggregates these typed counters in the owner-opened Shared Context tab; it does not infer them from transcripts or become telemetry authority.

## Delegation policy

Generated parent guidance treats delegation economics as a first-class routing criterion rather than minimizing child starts. That calculation includes startup overhead, reasoning latency and time-to-decision, monetary cost, and context-transfer cost. Prefer delegation when a child can inspect substantial independent or parallelizable evidence and compress it into a much smaller decision-ready result that removes meaningful parent load. Keep work with the parent when a safe handoff would return roughly the same volume and detail the child consumed, because startup and reasoning latency then add cost without reducing parent context. Ordinary exploration is not categorically parent-owned: preserve specialist offloading for history, diagnostics, bounded broad traces, mechanical execution, and one-pass independent review when the expected information-compression ratio is favorable. Shared context improves that ratio by seeding children with established facts and accepting concise reusable conclusions instead of raw exploration transcripts.

`repo-explorer` and `implementation-mapper` must not be used for the same scope, but either may remove substantial parent exploration when it can return a compact actionable result. Every delegated task packet must name the question, repository/subsystem, known paths or symbols, established facts/evidence, constraints and attempted work, the consumer decision, and a bounded output/completion condition. Lightweight formatting, lint, and focused static checks stay with the parent; test commands and suites use `test-runner`, as do broad or process-launching validation jobs.

## Archetype and model authority

Shared context does not define or persist an archetype-to-model routing matrix. Archetype model choice and reasoning effort remain owner-managed Nakode configuration. Current entries carry only Nakode-authored run attribution needed to identify their source; if model or richer archetype metadata is added later, Nakode must expose it through typed protocol and SDK state and clients such as FStack must render that state without hard-coded model assignments.
