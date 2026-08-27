# Code Mode for Nakode: investigation and prototype recommendation

- **Status:** Investigation complete; isolated opt-in PoC implemented; production adoption not approved
- **Owning system:** Nakode server/runtime
- **Ticket:** Evaluate Code Mode for Nakode tool and MCP execution
- **Date:** 2026-08-26
- **Implementation state:** Experimental protocol, SDK, native runtime, confined worker, and built-in TUI controls are implemented. Production promotion remains blocked on OS-level confinement, durable replay semantics, and benchmarks.

## 1. Decision

Prototype Code Mode as an **optional, server-owned synthesized tool over Nakode's canonical tools**. Do not adopt an upstream runtime as Nakode's execution authority and do not implement it in a provider adapter. The approved PoC exposes every canonical, client-owned, and MCP operation already authorized for its session, including mutations; each concrete invocation remains subject to the ordinary Nakode policy boundary and to whatever approval mediation that canonical path already implements. The PoC does not create a new universal approval layer.

The prototype should test the claimed efficiency gains before Nakode commits to a public protocol or durable execution model. If it succeeds, the production design should become a first-class server execution record while still presenting one ordinary provider-neutral tool to models. Every nested operation must return to Nakode for capability checks, current MCP grant checks, existing approval behavior, attribution, cancellation, audit, and transcript recording. Generated code must never receive credentials or direct filesystem, network, process, environment, or MCP access.

Why:

1. Code Mode is well matched to large catalogues, dependent reads, loops, filtering, and aggregation. It is unnecessary overhead for one or two simple calls.
2. Nakode already owns canonical tool execution, MCP grants and routing, credentials, cancellation, transcript state, and delegation attribution. Those controls are the correct host-call boundary.
3. Nakode does not yet have universal server-owned approval mediation for nested mutating tool calls, a durable Code Mode execution log, or a sandbox runtime. Those are production blockers.
4. The exact upstream implementation is OpenCode's experimental, Effect-native TypeScript package and adapter. It is excellent architectural prior art but is not a drop-in fit for Nakode's Rust, self-contained, provider-neutral runtime.

## 2. Upstream identification and ambiguity

### 2.1 Exact match: OpenCode CodeMode

The owner's description matches OpenCode's recently released experimental CodeMode implementation, not Cloudflare's earlier project:

- OpenCode merged [`feat: experimental codemode` PR #34677](https://github.com/anomalyco/opencode/pull/34677) on 2026-07-03, then split and restored the integration through [`feat(opencode): add code-mode MCP adapter` PR #35085](https://github.com/anomalyco/opencode/pull/35085) and [`feat(opencode): gate execute tool behind code mode flag` PR #35185](https://github.com/anomalyco/opencode/pull/35185).
- The first public OpenCode release containing the integrated feature is [`v1.17.14`](https://github.com/anomalyco/opencode/releases/tag/v1.17.14), published 2026-07-06. Its release note says: “Added a code mode MCP adapter for running confined orchestration scripts against connected MCP tools” and notes that `execute` is hidden unless Code Mode is enabled.
- The implementation lives in the [MIT-licensed](https://github.com/anomalyco/opencode/blob/v1.17.14/LICENSE) [`anomalyco/opencode`](https://github.com/anomalyco/opencode) repository: generic runtime [`packages/codemode`](https://github.com/anomalyco/opencode/tree/v1.17.14/packages/codemode), [package README](https://github.com/anomalyco/opencode/blob/v1.17.14/packages/codemode/README.md), [design/status document](https://github.com/anomalyco/opencode/blob/v1.17.14/packages/codemode/codemode.md), and OpenCode adapter [`packages/opencode/src/tool/code-mode.ts`](https://github.com/anomalyco/opencode/blob/v1.17.14/packages/opencode/src/tool/code-mode.ts).
- The package is named `@opencode-ai/codemode`, declares MIT, and was versioned `1.18.23` with the workspace when inspected on 2026-08-26. It is marked `private: true`: the source is open, but it is not a supported standalone npm dependency.
- Product exposure remains experimental behind `OPENCODE_EXPERIMENTAL_CODE_MODE`. It is not enabled as a stable default.

This exactly matches the described shape. OpenCode presents one model-facing `execute({ code })` tool. The model writes a small JavaScript/TypeScript-like orchestration script over connected MCP operations rather than issuing each MCP call directly.

### 2.2 OpenCode architecture and maturity

OpenCode does **not** pass arbitrary source to `eval`, Node `vm`, V8, or a subprocess sandbox. Its host-neutral package:

1. generates a token-budgeted typed catalogue and `$codemode.search` discovery tool;
2. strips supported TypeScript syntax, parses JavaScript with Acorn, and evaluates it in an owned tree-walking interpreter;
3. exposes only the explicit `tools` tree and a restricted language/standard-library subset;
4. forbids ambient filesystem, process, environment, network, modules/imports, timers, `eval`, host globals, and prototype access by construction;
5. validates Effect Schema tools and applies a plain-data boundary to all tool arguments, results, and final values;
6. supports branching, loops, error handling, eager supervised tool promises, `Promise.all`/`allSettled`/`race`, and at most eight concurrent nested calls;
7. returns normalized diagnostics and ordered admitted-call metadata while preserving host interruption.

Discovery defaults to a 2,000 estimated-token inline catalogue. Complete signatures are selected round-robin across namespaces, and `$codemode.search` returns directly callable paths, descriptions, and signatures for omitted tools.

The OpenCode adapter currently wraps **MCP tools**, not OpenCode's ordinary filesystem/shell/edit tools. When the experimental flag is enabled, direct top-level MCP tools are suppressed and replaced with `execute`; MCP resource helper tools remain direct. At execution time it:

- recomputes visible MCP tools from merged agent/session permission rules;
- excludes hard-denied tools from both the catalogue and dispatch;
- asks permission before every concrete child call;
- runs `tool.execute.before`/`after` plugin hooks around each nested MCP call;
- assigns synthetic child IDs `${outerCallId}/${sequence}`;
- streams running/completed/error child metadata on the outer tool;
- keeps structured MCP data in the interpreter while stripping images/audio/blob resources into outer attachments;
- forwards cancellation into the outer interpreter and MCP calls.

This is especially relevant to Nakode because it confirms the core recommendation: Code Mode should be one synthesized tool whose nested calls return to the host's canonical permission, hook, transport, cancellation, and observation path.

Material OpenCode limitations:

- The package explicitly does not own application authorization, generic approvals, persistence, durable pause/resume, replay, exactly-once side effects, or filesystem/process sandboxing for arbitrary JavaScript.
- The package provides `timeoutMs`, `maxToolCalls`, and `maxOutputBytes`, but OpenCode's current adapter supplies none of them. It relies on user cancellation and outer output truncation. A pathological regex executes on the host JavaScript engine and is bounded only when a timeout is configured.
- Nested calls are metadata under the single outer invocation rather than durable child transcript entities. Inputs are projected into live metadata; a Nakode implementation would need explicit redaction policy.
- The adapter captures a visible catalogue, but each child call still performs its permission ask. This is correct for authorization, though durable approval pause/resume remains outside the package.
- It remains experimental, private to the monorepo, TypeScript/Effect-specific, and under active refinement.

### 2.3 Prior art and other projects using the name

“Code Mode” is not a protocol or uniquely owned project name. Relevant alternatives must not be conflated:

| Candidate | What it is | License and maturity | Identification result |
|---|---|---|---|
| [Cloudflare Code Mode](https://blog.cloudflare.com/code-mode/) and [`@cloudflare/codemode`](https://github.com/cloudflare/agents/tree/main/packages/codemode) | Earlier named pattern and an MIT TypeScript package using isolated Workers, typed tools, progressive discovery, MCP/OpenAPI connectors, and an optional durable runtime. | MIT; initial announcement 2025-09-26; package `0.5.1` and explicitly experimental when inspected. | Important prior art cited by the ecosystem, but not the recent OpenCode release the owner meant. |
| [UTCP Code Mode](https://github.com/universal-tool-calling-protocol/code-mode) | JavaScript/TypeScript library and MCP/CLI wrappers for composing MCP, HTTP, file, and CLI tools. | MPL-2.0; first public release `v1.0.5` on 2025-11-15; active independent project. | Similar implementation, but its broad transport/runtime authority is a poor Nakode fit. |
| [`tmustier/code-mode-mcp`](https://github.com/tmustier/code-mode-mcp) | Standalone Node 22 stdio MCP server exposing one `exec` tool, progressive discovery, and a direct-vs-Code-Mode benchmark. | MIT; repository created 2026-07-15; package documentation identifies `0.4.0`. | Useful comparison, but explicitly states that `node:vm` is not a security sandbox and code has the Node process's authority. |
| [VoidMCP](https://github.com/voidmind-io/voidmcp) | Standalone Go MCP aggregator using QuickJS compiled to WebAssembly, with its own MCP registry, credentials, SQLite state, and `execute_code` tool. | MIT; repository created 2026-04-12; very early independent project. | Architecturally relevant sandbox reference, but it duplicates authority that Nakode already owns. |
| [Anthropic, “Code execution with MCP”](https://www.anthropic.com/engineering/code-execution-with-mcp) | Engineering guidance for loading tools on demand and processing intermediate results in code. | Article/pattern rather than the identified OpenCode implementation. | Strong corroboration of the pattern. |

OpenCode `v1.17.14` is therefore the exact upstream release for this investigation. Cloudflare remains foundational prior art and provides useful claims and durable-runtime comparisons, but should not have been identified as the owner's target.

## 3. Claimed benefits and tradeoffs

| Dimension | Direct model tool calls | Code Mode | Nakode assessment |
|---|---|---|---|
| Tool-schema context | Every exposed schema may occupy the model context. | One code tool plus types; progressive `search`/`describe` can defer detailed schemas. | Large potential gain for broad MCP/API catalogues; small or negative gain for Nakode's compact built-in set unless discovery is added. |
| Intermediate results | Each result returns to the model and remains in conversation history. | Code can filter, aggregate, and return a small final value. | Clear gain for large lists and fan-out reads. It can also hide useful evidence unless the server retains an audit summary. |
| Latency | Dependent operations require another model inference round trip; independent calls may already run concurrently. | One inference can express loops, branching, dependent calls, and local transforms. | Likely gain for three or more dependent calls. Network/tool latency remains, and sandbox startup/IPC adds overhead. |
| Reliability | The model chooses each next action with full semantic judgment and visible errors. | Familiar code constructs make deterministic orchestration concise, but generated programs add syntax/runtime errors, unbounded loops, bad assumptions, and partial-side-effect risk. | Benchmark exact models. Do not assume all models write correct JavaScript or understand generated declarations equally well. |
| Observability | Every call is naturally a top-level transcript event. | Intermediate values can disappear inside one opaque code call. | Must record parent execution plus every nested operation. Console output alone is not an audit trail. |
| Portability | Native function/tool calling formats differ by provider, but Nakode normalizes them. | Only requires reliable use of one function tool and JavaScript generation. | Provider-neutral in principle. In practice, Cursor currently cannot expose the necessary canonical/external tool surface, and model coding quality varies. |
| Approval | A harness can pause before each ordinary call. | One approval for the outer script is insufficient because branches and arguments are not known until runtime. | Approval must occur per nested operation at the Nakode host boundary. Pause/resume requires durable semantics. |
| Replay/resume | Nakode persists normalized history and provider state; individual calls have stable IDs. | Re-running code may repeat side effects or diverge on time/random/results. | Never replay mutating code implicitly. Reads may be retried; writes require idempotency or a durable operation log. |
| Debugging | Failures are localized to a call and model step. | Stack traces and source help, but host-call failures, sandbox failures, and partial completion must be distinguished. | Preserve bounded source, source hash, nested call sequence, timings, and terminal reason. |

Code Mode should be selected by task shape, not made the only tool-use mode. Direct calls remain better for short tasks, semantic checkpoints, rich media, user interactions, and approval-heavy mutations.

## 4. Current Nakode architecture and integration seams

### 4.1 Authority and public boundary

`AGENTS.md` makes the server the authority for tools, process supervision, approvals, credentials, persistence, cancellation, and canonical state. The public boundary is `proto/nakode/v1/nakode.proto`, generated API types, `crates/nakode-server/src/grpc.rs`, and reusable behavior in `crates/nakode-sdk/src/lib.rs`. A provider adapter or frontend-owned Code Mode would violate this boundary.

The current command model already installs tools and MCP grants atomically:

- `crates/nakode-protocol/src/command.rs::SessionToolConfiguration` carries external tools, replacement behavior, and a canonical built-in allowlist.
- `Command::CreateSession` and `Command::OpenSession` carry that configuration and an explicit `McpSessionGrant`.
- `crates/nakode-protocol/src/mcp.rs` defines deny-by-default grants and the `McpSessionSurface::{Chat, CodingAgent}` distinction.

A production Code Mode policy would belong beside these session inputs, then be projected through protobuf, gRPC, SDK, and authoritative session views. The PoC should not publish that surface before the benchmark justifies it.

### 4.2 Canonical tools and execution

The shared native runtime is the primary seam:

- `src/agent.rs` defines canonical tool/capability identities.
- `src/tools/mod.rs::ToolContext` carries workspace, runtime session, backend event sender, owner turn ID, stable call ID, question broker, delegation route, and cancellation to each tool.
- `src/runtime.rs::AgentRuntime::configure_external_tools` validates external schemas, collisions, built-in replacement/allowlist policy, turn limits, finalization reserve, and timeouts.
- `src/runtime.rs::AgentRuntime::execute_tool_calls` batches read-only built-ins but serializes exclusive or external operations.
- `execute_exclusive_tool`, `execute_read_only_tool`, and `execute_external_tool` enforce current session allowlists, validate arguments, emit start/result events, and settle external calls.

These methods are currently coupled to top-level model tool calls and mutable `RuntimeSession`. Production Code Mode needs an extracted server-owned invocation service that both ordinary tool calls and nested code calls use. Duplicating this logic inside a sandbox bridge would create a policy bypass.

Important current gap: built-in classification is primarily `ReadOnly` versus exclusive, not a complete authorization/approval taxonomy. A Code Mode host needs explicit metadata such as side-effect class, approval requirement, secret/result sensitivity, idempotency/replay policy, and whether the operation is callable from code. Unknown tools must default to denied.

### 4.3 MCP and credentials

Nakode's MCP boundary is already strong and should remain unchanged:

- `docs/mcp-authority.md` specifies explicit grants, stable `mcp__<server>__<tool>` identities, current-usability checks, bounded invocation, credential authority, cancellation, and audit attribution.
- `src/mcp.rs` supports only streamable HTTP today. It validates HTTPS/public endpoints, rejects URL credentials and redirects, pins resolved addresses, applies time and response limits, and redacts credentials from errors.
- `src/server/runtime.rs::handle_backend_event` intercepts `ExternalToolRequested` names with `MCP_TOOL_PREFIX`, resolves them against the session grant, invokes through Nakode, and returns the result with `BackendCommand::ResolveExternalTool`.
- `session.rs` persists MCP definitions/credential metadata separately and records `mcp_invocation_audit` with workspace/session/run attribution.

Code must receive only opaque typed host methods. It must not receive MCP bearer tokens, endpoint credentials, a raw HTTP client, or direct MCP transports. `search` and `describe` must reveal only tools that are currently granted, connected, credential-ready, model-visible, and callable from the selected session.

### 4.4 Providers and capability admission

`src/backend.rs` is the provider-neutral lifecycle contract. `project_provider_tools` maps canonical identities to adapter identities and reports unsupported tools rather than silently widening access. `BackendCapabilities` advertises resume, interruption, approvals, native tools, external tools, scoped runtime policy, MCP, and close support.

The native Codex, Devin, Kimi, and GLM adapters use the shared `AgentRuntime`; a synthesized canonical `code` tool can be provider-neutral there. Claude has adapter-specific Node bridge policy and approval behavior. Cursor currently exposes only a custom delegation tool and does not forward canonical built-in or external-tool policy. Therefore:

- Code Mode must not be implemented as provider-specific Codex/Claude behavior.
- Provider admission must require exact support for the one synthesized tool and Nakode-owned nested execution.
- Cursor should report Code Mode unsupported until its adapter can project and enforce the same canonical surface.
- Provider-native approval support cannot be the security basis because native adapters currently advertise approvals as unsupported. Approval must move to the Nakode invocation layer before mutating Code Mode is allowed.

### 4.5 Transcript, persistence, cancellation, and attribution

- `src/domain_transcript.rs::TranscriptEntry` stores semantic kind/status, stable identity, provider/model, owner turn, and bounded versioned `tool_audit_json`.
- `crates/nakode-protocol/src/view.rs::TranscriptEntryView` exposes that audit as inert data.
- `src/runtime.rs::RuntimeSessionStore` persists normalized native runtime sessions in SQLite.
- `session.rs` persists logical sessions, owner turns, native history, transcripts, MCP invocation audit, and orchestration runs.
- `session.rs::SubagentObservability` persists parent run, invocation turn/call, originating owner entry, policy, limits, termination, continuation, and inherited evidence.
- `src/server/runtime.rs` owns cancellation of provider work, MCP calls, and native delegation and correlates completions before publishing state.

Code Mode should reuse the same owner turn/call attribution. Delegation called from code, if ever allowed, must preserve the outer execution ID and nested sequence in addition to the existing invocation turn/call. It is an explicit non-goal for the first PoC.

A current resume gap also matters: `BackendCommand::StartSession` carries finalization-reserve state, while `ResumeSession` does not restore the complete launch policy in native adapters. Code Mode must not add another partially reconstructed policy. Any production session policy must be persisted as one resolved, versioned policy and restored atomically.

## 5. Threat model and required controls

Treat generated JavaScript as hostile. Prompt injection can intentionally generate an exfiltration program even when the model and user are benign.

| Threat | Required control |
|---|---|
| Interpreter/runtime escape or engine memory bug | A deliberately restricted owned interpreter, as OpenCode uses, can avoid ambient authority by construction but still needs parser/interpreter security review and hard resource budgets. Any general JavaScript engine must run in a disposable confined worker; an isolate alone is not a privilege boundary. Patch and pin every parser/runtime dependency. |
| CPU loop, recursion, allocation bomb, huge serialization | Enforce source/input/output limits, JS heap and stack limits, operation-count and concurrency limits, CPU and wall deadlines, parent watchdog, and unconditional worker kill. |
| Filesystem/network/process/environment access | Provide none. Empty/sanitized environment, private empty working directory, no module loader, `require`, dynamic import, FFI, sockets, fetch, subprocess, or inherited handles. Apply platform sandboxing and resource limits outside JS. |
| Credential theft | Credentials stay in Nakode credential/MCP authorities. Host methods accept logical tool IDs and arguments; they never expose headers, tokens, endpoints containing secrets, or credential-bearing errors. |
| Capability bypass | Build the visible catalogue only from the resolved session policy. Re-check capability, built-in allowlist, MCP grant/current usability, archetype policy, and code-callability on every nested invocation. Never trust generated types as enforcement. |
| Approval bypass | Approve each concrete nested operation after arguments are known. “Approve code” is not equivalent to approving its future effects. Accept-once/session decisions remain scoped to canonical tool identity and policy. |
| Data exfiltration through an allowed write tool | Classify egress/mutation tools and require approval or policy authorization per call. Apply result/argument redaction. Do not assume “no network in JS” prevents exfiltration through `send`, `write`, browser, shell, or MCP tools. |
| Tool recursion and authority amplification | Exclude `codemode` itself. The approved PoC exposes every canonical/client/MCP tool already granted to the session, including mutations and delegation, but each invocation re-enters Nakode's canonical policy, attribution, and approval path. Operations that are not canonical model tools never enter the catalogue. Bound nesting at zero and total host calls at 64. |
| Nondeterminism and replay divergence | Remove clocks/randomness where practical; record code hash and ordered calls. Do not auto-replay interrupted executions. Require explicit idempotency/replay metadata before durable mutation support. |
| Cancellation race | One cancellation token owns worker execution and all pending host calls. On cancel, reject new calls, cancel MCP/tool work, kill the worker, ignore late completions, and persist one terminal interrupted outcome. |
| Audit suppression by filtering | Model-facing results may be filtered, but Nakode records a bounded event for every nested call, including denied/failed calls, timing, redacted arguments/result summary, and approval authority. Console logs are supplemental only. |
| Cross-session state leakage | New runtime/process per execution in the PoC. No global variables, module cache, snippets, or worker reuse across sessions. If pooling is later added, prove complete reset and tenant isolation. |
| Supply-chain/runtime drift | Pin engine/runtime sources and hashes, inventory transitive licenses, run sandbox escape and limit tests in CI, and expose the resolved runtime version in diagnostics. |

Deterministic execution is not fully possible when tools read changing external state. The required invariant is narrower: Nakode can explain exactly which ordered operations occurred and never silently repeat an operation after an ambiguous failure.

## 6. Observability and transcript semantics

A Code Mode execution is one parent tool call with ordered child operations.

Proposed identifiers:

```text
execution_id = stable server ID
owner_turn_id = existing owner turn
outer_call_id = provider-neutral call ID for `code`
child_call_id = execution_id + monotonic sequence
```

For the PoC, use existing tool start/result events and a versioned `tool_audit_json` payload on the outer entry:

```json
{
  "version": 1,
  "executionId": "...",
  "sourceSha256": "...",
  "runtime": "quickjs-worker/<pinned-version>",
  "limits": {"wallMs": 5000, "memoryBytes": 33554432, "maxCalls": 20},
  "children": [
    {
      "seq": 1,
      "callId": "...:1",
      "tool": "grep",
      "status": "completed",
      "durationMs": 12,
      "authorization": "session_builtin_allowlist",
      "arguments": "<bounded/redacted summary>",
      "result": "<bounded/redacted summary or digest>"
    }
  ],
  "status": "completed"
}
```

The transcript should render the parent as “Ran code” with expandable nested operations, not as a single opaque success. The model receives only the bounded returned value. Audit retention is independent of model-facing result filtering.

If the prototype progresses to production, add a durable `code_executions`/`code_execution_calls` model and project semantic child entries through `nakode.v1`. Do not make clients parse opaque JSON to understand approval state or lifecycle. Snapshots remain authoritative replacement views.

## 7. Persistence and resume semantics

PoC rule: executions are ephemeral but terminal metadata is recorded. If the server or worker stops mid-execution:

1. cancel pending host operations;
2. mark the execution `interrupted` with the last settled child sequence;
3. do not restart or replay it automatically;
4. let the model or user issue a fresh call.

This interruption rule avoids automatic replay, but mutations make ambiguous outcomes possible. The PoC therefore relies on each canonical tool's existing lifecycle and idempotency behavior and is not a durable workflow engine. Production promotion still requires explicit execution records and recovery semantics.

A production mutation-capable design would require:

- persisted source and source hash, resolved policy version, runtime version, limits, and ordered child log;
- operation states such as `planned`, `awaiting_approval`, `executing`, `succeeded`, `failed`, and `outcome_unknown`;
- per-tool idempotency semantics and stable idempotency keys where supported;
- no replay after `executing` without authoritative outcome reconciliation;
- replay-divergence detection for tool identity and normalized arguments;
- explicit user/model retry rather than hidden whole-program replay;
- bounded retention and redaction rules.

Cloudflare's durable runtime uses abort-and-replay and records calls/steps. That is useful prior art, but it cannot be copied blindly: arbitrary Nakode tools and remote MCP mutations do not all have revert functions or idempotency keys, and a crash may leave an unknown external outcome.

## 8. Build versus adopt

| Option | Fit | Decision |
|---|---|---|
| Adopt OpenCode `@opencode-ai/codemode` | MIT, carefully tested, and closest to the desired policy-interception shape. However, it is a private TypeScript package coupled to Effect and OpenCode's Bun/TypeScript workspace. Its OpenCode adapter also owns OpenCode-specific MCP, permission, plugin, and metadata plumbing. Running it would add a Node/Bun sidecar or a second implementation language at Nakode's authority boundary. | Do not adopt or vendor directly. Reuse its restricted-language design, discovery strategy, plain-data boundary, permission tests, and supervised-concurrency semantics. |
| Adopt `@cloudflare/codemode` | MIT and feature-rich, but TypeScript and designed around Cloudflare Worker/RPC execution abstractions, with optional Durable Object behavior. It would add a non-Rust runtime and external platform assumptions to a self-contained server. | Do not adopt as runtime. Reuse concepts and benchmark claims. |
| Adopt UTCP Code Mode | Supports many transports, but MPL-2.0 obligations must be tracked and it would introduce Node/CLI/MCP configuration, credentials, and execution authority parallel to Nakode. | Reject for integration. Useful benchmark reference only. |
| Launch `tmustier/code-mode-mcp` | MIT and simple, but requires Node 22 and explicitly grants code the Node process's authority because `node:vm` is not a security boundary. | Reject. |
| Launch VoidMCP | MIT and its QuickJS-on-Wasm sandbox is relevant, but the binary owns MCP registration, encrypted credentials, SQLite persistence, and lifecycle outside Nakode. | Reject as a service dependency. Consider its sandbox approach as prior art. |
| Build a narrow Nakode host and confined runtime | Preserves canonical authority, Rust lifecycle, attribution, policy, and provider neutrality. Costs engineering effort and security maintenance. | Recommended only as a bounded PoC, then reassess. |

### Runtime choice for the PoC

Use a pinned QuickJS engine through `rquickjs` in a **disposable child process implemented by the Nakode executable itself**, communicating over a bounded framed JSON protocol, only to test the product hypothesis cheaply. Match OpenCode's restricted language and ambient-authority surface rather than presenting arbitrary Node semantics. QuickJS is MIT licensed and provides heap, stack, and interrupt controls; `rquickjs` is permissively licensed, but the exact crate release and transitive lockfile must be reviewed before implementation. A child process limits damage from an engine/native binding failure better than an in-process context and preserves Nakode's self-contained distribution better than requiring Node or Bun.

This QuickJS design is not by itself a production sandbox. The implementation review must add OS confinement for supported platforms. If robust cross-platform worker confinement cannot be achieved, evaluate QuickJS compiled to WebAssembly under Wasmtime, which provides fuel/epoch interruption, linear-memory isolation, and explicit host imports at the cost of build and ABI complexity.

This choice deliberately differs from OpenCode's Acorn-based owned interpreter. Porting that TypeScript interpreter to Rust would dominate the PoC and confound the question of whether models, tasks, and catalogues produce enough benefit. If the benchmark passes, a production design review should compare (a) a native restricted interpreter implementing only the measured language subset and (b) QuickJS-on-Wasm/OS confinement. The former minimizes ambient APIs and matches OpenCode's semantics but creates a security-critical language implementation Nakode must maintain; the latter reuses a mature engine but requires a stronger sandbox boundary.

Authoritative runtime references:

- [QuickJS project](https://bellard.org/quickjs/) and [source](https://github.com/bellard/quickjs)
- [`rquickjs`](https://github.com/DelSkayn/rquickjs)
- [Wasmtime](https://github.com/bytecodealliance/wasmtime) and [security documentation](https://docs.wasmtime.dev/security.html)

## 9. Bounded proof of concept

### 9.1 Scope

The approved PoC is implemented behind the optional `SessionToolConfiguration.code_mode` flag. It is advertised as the `CodeMode` service capability and projected as `SessionState.code_mode` so clients can show the effective mode.

The built-in TUI exposes explicit session-scoped controls:

- `/code-mode` toggles the current session at a clean turn boundary; `/code-mode on` and `/code-mode off` request an explicit state. The server rejects changes while a turn, queued prompt, shell, compaction, interaction, or delegated run is active. The selected mode is persisted and the next provider turn receives the corresponding tool surface;
- `/resume-code` opens the session picker and restores the selected session with Code Mode, while `/resume-code <session-id>` restores one directly. Ordinary `/new` and `/resume` remain unchanged.

The TUI header and Code Mode resume picker display `CODE MODE`. SDK clients use the revision-fenced `set_session_code_mode` command to change an attached idle session, or `create_session_with_configuration`/`open_session_with_attachment` at an attachment boundary. Clients that own external tools or MCP grants must resubmit the complete attachment on restoration.

Properties:

- one synthesized model tool named `codemode`; it is the only provider-facing tool when enabled;
- shared native-runtime providers only; unsupported adapters reject the option before session startup;
- every canonical, client-owned, and MCP tool already authorized for the session remains callable inside code, including mutations;
- JavaScript function-body input and a JSON-compatible return value;
- no direct filesystem, network, environment, process, timer, module, or credential APIs;
- no nested Code Mode, durable snippets, automatic replay, or worker reuse; delegated child sessions start with Code Mode disabled while preserving ordinary delegation attribution.

Allowing granted mutations does **not** approve the source program wholesale. Each concrete nested invocation still crosses the same Nakode tool dispatch, schema validation, policy, MCP grant, attribution, cancellation, and audit boundaries as an ordinary call, and retains any approval mediation implemented by that canonical path. The PoC does not add universal server-owned approval mediation where an ordinary native path has none; that remains a production blocker. Disabled Code Mode preserves the ordinary provider surface. Client-owned tool definitions are rejected if they use the reserved `mcp__` prefix, so only Nakode-projected MCP grants can enter the MCP router.

Suggested tool schema:

```json
{
  "name": "codemode",
  "description": "Run confined JavaScript that composes the listed Nakode-authorized tools.",
  "input_schema": {
    "type": "object",
    "required": ["code"],
    "properties": {
      "code": {
        "type": "string",
        "description": "JavaScript function body; return a JSON-compatible value"
      }
    },
    "additionalProperties": false
  }
}
```

Generate TypeScript declarations for context, but execute JavaScript only. The visible declarations are derived from the resolved canonical session policy and carry stable tool names.

### 9.2 Internal protocol and interception

```text
model -> codemode({code})
  -> ServerRuntime creates execution_id under owner turn/call
  -> disposable worker validates/parses and starts source
  -> worker emits Invoke(execution_id, seq, tool, arguments)
  -> Nakode invocation service re-checks policy and validates schema
  -> existing canonical tool implementation executes with child_call_id
  -> Nakode records child outcome and returns bounded result to worker
  -> worker returns one bounded final value
  -> Nakode records parent outcome and returns it to model
```

The PoC uses flushed, newline-delimited JSON frames with a 1 MiB read bound in each direction. A production protocol should become explicitly versioned and length-prefixed. Reject malformed, oversized, unknown, or post-terminal frames. The parent independently validates the tool name and maximum call sequence and owns cancellation.

Initial limits for measurement, subject to tuning:

- source: 32 KiB;
- input/final output: 1 MiB internal, 64 KiB model-facing;
- heap: 32 MiB;
- stack: 1 MiB;
- wall time: 60 seconds, also bounded by owner-turn cancellation;
- nested operations: 64;
- concurrent host calls: 1 (serialized in source order);
- one disposable worker per execution.

### 9.3 Phases

1. **Confined worker:** execute JavaScript in a disposable QuickJS child with heap, stack, source, frame, call-count, and deadline limits; expose no ambient host imports.
2. **Canonical runtime interception:** project only `codemode` to the model and route every nested operation back through the existing canonical/native/external/MCP execution paths.
3. **Protocol and resume wiring:** carry the opt-in through protobuf, protocol, server state, provider start, and provider resume. Restored logical sessions re-submit the exact tool configuration through the existing atomic `OpenSession` boundary.
4. **Decision gate:** benchmark and security-review the PoC. Remove it or keep it explicitly experimental if the thresholds fail; production promotion requires OS confinement and durable execution semantics.

### 9.4 Outcome-based tests

Implemented integration coverage runs the real QuickJS worker through `AgentRuntime`, verifies that the model sees only `codemode`, composes a native filesystem mutation plus client and MCP callback tools, and confirms cancellation removes a pending callback before a late result can settle. Server-runtime tests exercise `handle_mcp_tool_request` for a currently granted tool and for an ungranted tool, including backend completion/denial routing. Unit coverage separately checks ordinary-session isolation, hidden/recursive/unknown denials, limits, confinement, catalogue composition, nested audit identity, protocol projection, explicit TUI create/resume controls, and the reserved MCP namespace. Production promotion still requires the broader approval-equivalence, durable-interruption, redaction, adversarial, and benchmark matrix below.

The complete promotion suite should prove:

- A script composing two reads and a filter returns the same answer as direct calls, and the transcript records both nested operations under the correct owner turn/call.
- A disallowed, unknown, hidden direct, or self-recursive tool is denied even if the program/provider guesses its name. A granted mutating tool follows the same policy and approval outcome as its ordinary direct-call equivalent.
- Revoking an MCP grant between discovery and invocation prevents the call.
- Cancelling a turn kills the worker, cancels pending host work, records `interrupted`, and produces no later transcript mutation.
- Infinite loops, recursion, heap growth, output floods, malformed frames, and too many calls terminate within limits without affecting another session.
- Code cannot read environment variables, files, network, process state, credentials, or modules except through explicitly exposed host methods.
- Calls are serialized in source order and retain stable parent-derived sequence IDs and complete audit records.
- Server restart never auto-replays an interrupted execution.
- Redacted secrets never appear in model output, transcript audit, errors, or persisted execution metadata.
- Direct tool behavior is unchanged when Code Mode is disabled.

## 10. Benchmark and success criteria

Use paired tasks with identical starting context, model/version, reasoning setting, tool catalogue, data fixtures, and answer rubric. Run enough repetitions to report median, p95, success rate, and variance rather than one demo.

| Task class | Example | Expected winner |
|---|---|---|
| One simple call | Read one known file. | Direct |
| Two independent reads | Read two known files. | Direct or tie because Nakode already batches read-only calls. |
| Dependent chain | Find files, grep matches, read selected ranges. | Code Mode |
| Fan-out/filter | Inspect 20 results and return five matching records. | Code Mode |
| Large intermediate result | Query a broad MCP list and return aggregate/count/top items. | Code Mode |
| Branching/error recovery | Try a lookup, branch on absence, make a fallback read. | Measure reliability. |
| Approval boundary | Execute an authorized mutation and a denied mutation through both paths. | Outcomes and audit must match direct policy. |
| Adversarial prompt/tool result | Tool output asks code to exfiltrate or call a denied tool. | Both must remain policy-safe. |

Measure:

- task success and answer quality;
- total input/output tokens, including tool definitions and intermediate results;
- number of model inference turns;
- end-to-end median and p95 latency;
- model/tool/sandbox error and retry rates;
- host calls and bytes returned to the model;
- transcript completeness and policy-denial correctness;
- CPU, peak memory, worker startup time, and cancellation time;
- results by provider/model family, not only aggregate.

Prototype success gate:

1. no capability, secret, cancellation, or transcript-attribution failure in the security suite;
2. at least 95% of direct-call task success on the full corpus and no material regression on simple tasks when comparing separate direct-mode and Code Mode sessions;
3. at least 30% median reduction in total tokens **or** 25% median latency reduction on dependent/fan-out/large-result classes;
4. no more than 10 percentage points higher execution/retry failure rate;
5. p95 cancellation to worker termination under one second after outstanding host calls are cancelled;
6. benefits reproduce on at least two supported model families.

If these thresholds are not met, keep direct calls and invest instead in progressive tool discovery, result truncation/shaping, parallel read batching, and provider prompt/tool-schema caching. Those alternatives capture much of the value without executing model-authored code.

## 11. Final recommendation and non-goals

**Recommendation: prototype, do not adopt for production yet.** The approved implementation is an opt-in, provider-neutral experiment around Nakode's own canonical invocation path. It exposes all operations already granted to the session rather than inventing a weaker parallel policy; each nested effect remains independently enforced and audited. Borrow OpenCode's confined-language surface, plain-data boundary, supervised execution, per-child permission interception, and nested-call metadata. Use Cloudflare's durable execution and large-catalogue claims only as additional prior art. Do not import either TypeScript runtime or delegate authority to an external MCP aggregator.

Explicit non-goals for the PoC:

- production availability;
- bypassing canonical mutation policy or treating approval of source text as approval of nested effects;
- provider-specific implementations;
- arbitrary npm/ES modules, TypeScript execution, Node/Deno APIs, or ambient shell access;
- direct network, filesystem, environment, process, credential, or MCP access; granted canonical tools may provide those effects only through Nakode;
- durable snippets, automatic replay, rollback, or mutation recovery;
- nested Code Mode; delegated child sessions intentionally use ordinary mode;
- replacing ordinary tool calling.

The promotion gate is an approved follow-up design covering universal Nakode-owned approvals, durable execution/call records, replay/idempotency, protocol/SDK projections, provider admission, cross-platform sandbox confinement, and a passing benchmark/security report.
