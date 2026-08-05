# Renderer-only frontend and SDK architecture

## Decision

The public frontend boundary is `proto/nakode/v1/nakode.proto`. The native
Rust server implements that service. Rust, TypeScript, Go, and future language
clients generate transport types and service stubs from the same schema. gRPC
is the only public frontend transport.

This contract is the hard product boundary. The native server is the agent
runtime and state authority; every TUI, desktop, web, mobile, IDE, CLI, or
automation surface is an unprivileged thin client.

Generated gRPC stubs are the bottom layer of an SDK, not the API frontend code
should use directly. Each supported language exposes a high-level
`NakodeClient` with domain methods and materialized watches:

```text
connect(endpoint)
getWorkspace(workspace)
watchWorkspace(workspaceId)
createSession(workspaceId, title?)
listSessions(workspaceId)
getSession(sessionId)
watchSession(sessionId)
openSession(sessionId)
sendPrompt(sessionId, prompt)
enqueuePrompt(sessionId, prompt)
removeQueuedPrompt(sessionId, promptId)
steerQueuedPrompt(sessionId, promptId)
steerTurn(turnId, text)
cancelSessionWork(sessionId)
resolveInteraction(interactionId, resolution)
selectModel(target, model, options)
delegate(sessionId, agent, task)
getRun(runId)
watchRun(runId)
getArtifact(artifactId)
getDiagnostics(filter)
```

The full schema also exposes provider authentication, settings, agents,
history paging, shell execution, cancellation, and workspace reload so an
alternative frontend can provide every current capability.

## Ownership

The server owns canonical workspace/session/run state, persistence, provider
and model policy, prompt queues, active work, interactions, orchestration,
history, artifacts, command idempotency, ordered revisions, and authorization.

The language SDK owns transport selection, connection recovery, mutation
idempotency keys, safe retry after uncertain delivery, stream resumption,
resubscription, history and artifact hydration, and publication of materialized
authoritative snapshots. A watch yields a complete replacement
`WorkspaceState`, `SessionState`, or `RunState`; it does not require application
code to reduce internal server events.

The Rust SDK is the complete production implementation used by the TUI. Go and
TypeScript currently generate typed service clients from the same schema;
their high-level packages must implement this profile before application code,
rather than placing that behavior in an individual frontend.

A frontend owns only input mapping, rendering, focus, drafts not yet submitted,
selection, scroll position, viewport size, clipboard/device integration, and
its own lifecycle. It must not infer or persist product state, decide whether
work queues versus steers, recover uncertain mutations, merge transcript
patches, or coordinate provider/session lifecycle.

## Transport

gRPC is the canonical service shape because Protobuf provides one versioned
schema and typed unary/server-streaming code generation across languages.
Native local clients may use a Unix-domain-socket connector. Browser clients
use a gRPC-Web or Connect-compatible adapter to the same methods and messages.
Transport adapters cannot introduce new product semantics.

## Implementation rule

The Rust TUI consumes the same public SDK as another frontend. The gRPC adapter
translates into the server's semantic command and query boundary; it does not
duplicate the runtime or persistence engine. No alternate frontend transport
or client-side domain reducer is maintained.

For every new feature, the implementation sequence is server semantics, public
schema, high-level SDK edge, then frontend rendering. Missing frontend access
is an API or SDK gap; it is never justification for client-side domain logic.
