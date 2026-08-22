# Option A deferred Nakode activation plan

Status: implemented on this branch; this document records the approved product contract, architecture, and verification plan.

## Product decision

After an update installs a new Nakode executable, the currently running service may continue on its old executable only while it owns live work. Nakode must then keep an explicit, auditable activation attempt pending and automatically replace the stale service as soon as it becomes quiescent.

The owner must not have to close idle sessions or remember to run `nakode restart`. The dashboard must make the pending activation visible until installed and running executable identities agree.

Terminology in product copy:

- the **running Nakode service is stale**;
- sessions are **active**, **queued**, or **blocking activation**;
- sessions do not “become stale”; they finish and become **quiescent**.

This is the whole-service Option A. It does not run old and new service generations concurrently. All logical sessions remain on the old service until its live work drains, followed by one bounded service replacement and client reconnection.

## Owner experience

### Successful immediate activation

When `nakode update` installs a new executable and the service is already quiescent:

1. the normal update progress says `Activating Nakode…`;
2. the existing atomic quiescent replacement runs;
3. clients reconnect to the same logical workspace and sessions;
4. no pending-activation icon is shown because installed and running identities already agree.

### Deferred activation

When live work blocks the replacement:

1. the update still completes installation;
2. the update result explicitly says `Nakode installed; activation is waiting for 3 sessions to finish` rather than `up to date`;
3. a detached Nakode activation helper remains responsible for rechecking;
4. a new amber `Nakode update pending` action appears directly above the existing dashboard Update action;
5. its badge is the current number of blocking logical sessions;
6. the helper checks immediately and then every 15 seconds (a product constant, not a wire semantic);
7. once the service is quiescent, the helper atomically fences new mutations, activates the installed executable, publishes terminal `activated`, hands status to B, and exits;
8. clients reconnect and the pending action disappears.

The pending action remains visible whenever installed and running identities differ, even if the helper crashes or cannot be reached. A helper failure changes it from amber pending state to red stalled state; it must never silently remove the warning while the stale service remains.

### Pending action

Location: the dashboard sidebar footer, directly above the existing source-update action in `apps/dashboard/src/shell/Sidebar.tsx`.

Expanded sidebar:

- icon: existing `clock` glyph;
- label: `Nakode update pending`;
- trailing opaque amber badge: blocker count, for example `3`;
- status tone: amber while waiting/checking, red when stalled.

Collapsed sidebar:

- the same fixed-size icon remains in the same footer column;
- the count badge overlays without changing the button's dimensions;
- accessible name and tooltip: `Nakode update pending — waiting for 3 sessions`;
- stalled form: `Nakode activation stalled — installed 0.9.4, running 0.9.3`.

The row does not animate continuously. While an explicit check or activation is in flight, the shared Button busy treatment replaces its label/icon with the standard spinner without shifting sibling rows.

The icon is capability-gated and absent from web/development hosts that cannot reach the installed Nakode activation control service. Capability absence must not be represented as a button that throws.

## Pending-activation modal

Clicking the pending action opens one modal. It is separate from the existing `Update both` modal because source availability/installation and installed-binary activation are different state machines.

### Header and primary status

Title: `Nakode update pending`

Subtitle: `Installed 0.9.4 · Running 0.9.3`

Primary status examples:

- `Waiting for 3 sessions to finish`
- `Checking whether Nakode can activate…`
- `Activating Nakode 0.9.4…`
- `Activation stalled`

The modal shows one compact cadence row without requiring Details:

`Checks every 15 seconds · Last checked 8 seconds ago · Next check in 7 seconds`

Time labels update locally from authoritative timestamps; the dashboard does not invent the cadence or next-check deadline.

### Blocking sessions

The main body lists every authoritative blocker returned by Nakode, ordered by session ID or service order and refreshed as one replacement:

| Session | Blocking reason | Queue |
|---|---|---|
| Ticket title or short logical ID | Active owner turn | 0 |
| Ticket title or short logical ID | Waiting for owner answer | 0 |
| Ticket title or short logical ID | Delegated work is running | 0 |
| Ticket title or short logical ID | Queued prompts must finish | 2 |

The public status may expose stable logical session ID, bounded display title, activity class, queue count, and a bounded reason. It must not expose prompt text, transcript content, provider secrets, tool arguments, or private child objectives.

When no blockers remain and activation is in progress, the blocker list is replaced by the activating state rather than showing a false empty waiting list.

### Actions

Footer while waiting:

- `Close`
- `Check now`
- `Stop sessions and activate` in destructive tone

`Check now` asks the helper to run an immediate authoritative check. It is single-flight with the periodic checker. Outcomes:

- still blocked: keep the modal open and replace blocker/cadence/history state;
- activation started: keep the modal open in non-dismissible `Activating…` state;
- activation succeeded: close the modal automatically after clients reconnect and installed/running identities agree;
- check failed: keep the modal open and show the exact actionable failure.

`Stop sessions and activate` never executes directly. When the running service advertises conditional-force support, it opens a second destructive confirmation naming the exact current blocker count and explaining:

- active turns and delegated work will be interrupted;
- queued prompts, pending interactions, and process-owned shells may be lost under today's forced-shutdown semantics;
- logical session records and already-persisted transcript history are not intentionally deleted;
- FStack/Nakode will reconnect after the replacement.

Confirmation submits an exact blocker identity/revision set. A conditional-force-capable running service rechecks and fences that set atomically. If the set changed, it refuses with the new authoritative blocker list rather than stopping unconfirmed work. If unchanged, it records an audit event, invokes the explicit forced activation path, and reconnects clients. The destructive path is owner-only and never automatic.

Today's stale service supports only unconditional `LifecycleRequest::Shutdown`; it cannot compare an expected blocker set or prevent new work from racing between a helper-side recheck and shutdown. Therefore the first upgrade from a pre-capability A must **not** pretend to provide exact revision-fenced force. For that compatibility case the modal shows `Force activation unavailable for this running Nakode version` with the latest blocker list; it does not render a button that can throw or silently widen the confirmed scope. The button becomes available after the running service advertises the conditional-force lifecycle capability. If product chooses a one-time broad `stop all work` escape hatch instead, that is a separate explicit policy decision and must say that newly arriving work is also in scope.

The normal `Close` action dismisses only the modal. It does not stop the helper or clear pending activation.

### Details disclosure

The modal uses the shared `Fold` disclosure and shows:

1. **Versions and identities**
   - installed executable path, version, SHA-256, inode/device when available;
   - running service version, PID, start time, executable identity;
   - target API version and an informational capability delta when available. Today endpoint reuse is decided by executable identity or the literal API version string (`nakode.v1`), not by capability comparison; the delta must not be used to reinterpret that existing compatibility rule.
2. **Helper**
   - helper state and PID/instance ID;
   - started time and heartbeat;
   - fixed cadence and next deadline;
   - activation-attempt ID.
3. **Check history**
   - bounded newest 50 attempts;
   - timestamp, trigger (`installed`, `scheduled`, `manual`, `forced`), duration, result;
   - blocker count and bounded reasons;
   - quiescence refusal, lifecycle error, or activation result.
4. **Diagnostics**
   - bounded newest output from the helper;
   - socket/startup errors and retry disposition;
   - no credentials, prompts, transcript bodies, or raw private persistence.

History remains visible after a transient helper error. Once activation succeeds, the terminal success record remains available through ordinary Nakode status/logging, but the sidebar pending action disappears.

## Authoritative activation state machine

Nakode owns the durable phases; FStack renders them and invokes typed operations. `reconnecting` is intentionally a client-local overlay: only each client knows when its workspace and attached-session replacement snapshots have been hydrated, so the helper must not claim that every client has reconnected.

| State | Meaning | Sidebar |
|---|---|---|
| `current` | No pending attempt; installed and running executable identities agree | Hidden |
| `installed_pending` | B is installed and differs from running A; first check has not completed | Amber clock; count unknown |
| `checking` | One scheduled/manual authoritative check is in flight | Amber busy row |
| `blocked` | Latest check refused quiescence and reports current blockers | Amber clock + blocker count |
| `activating` | Quiescence was fenced and safe replacement is in progress | Amber busy row; modal non-dismissible |
| `forcing` | Explicit confirmed forced replacement is in progress | Red busy row; modal non-dismissible |
| `activated` | B is ready and identity-verified; terminal audit snapshot before helper handoff/exit | Success state, then hidden |
| `failed` | Identity mismatch remains and activation needs action | Red warning row |
| `cancelled` | An attempt was explicitly cancelled or superseded; retained in history | Red while a mismatch remains; otherwise hidden |
| client `reconnecting` | Nakode is activated but this client has not hydrated workspace/session snapshots yet | Pending row may show `Reconnecting…`; sessions are not marked failed |

A helper crash or missed heartbeat projects as `failed`, not `cancelled`. A newer installed target may cancel/supersede the old attempt only by atomically creating the new `installed_pending` attempt; it must not leave an unowned identity mismatch.

`current` is derived from full executable build identity, not only a display version. A rebuilt binary with the same package version is still stale when its content identity differs.

The pending activation record survives dashboard relaunch, helper crash, and machine restart. Querying activation status through the newly installed executable must ensure that a pending helper is running or return `failed` with an exact reason.

## Helper process contract

### Why it is a separate helper

The installed new binary must supervise deferred activation because the stale service cannot acquire newly added lifecycle behavior. The helper is launched by the newly installed executable after the installer's immediate quiescent attempt is refused.

The helper:

- is detached from the update command and dashboard process;
- owns one installation-scoped helper lease distinct from the existing short-lived activation lease;
- performs an immediate check, then checks every 15 seconds;
- serializes scheduled and manual checks into one single-flight operation;
- acquires the existing activation lease only during an individual check/cutover, so it does not block ordinary endpoint discovery while waiting;
- exact blocker rows are built from the stale service's existing full session snapshots and final lifecycle refusal IDs; an ID that an older A cannot classify is retained as `Unclassified live work reported by running service <identity>` rather than omitted;
- uses `QuiesceShutdown` as the final atomic authority rather than trusting a prior observation;
- starts the installed executable through the existing detached service-start path;
- verifies readiness and build identity before recording success and exiting;
- retains bounded structured audit history in a forward-compatible activation journal.

A helper crash cannot leave the runtime fenced: the existing abandoned quiescence response rollback remains required. A stale helper lease is reclaimed only after process identity proves the owner is gone, following the existing activation lease safeguards.

### Activation control endpoint

The stale service cannot answer RPCs added by the new binary. To preserve Nakode's public-protocol boundary without making FStack read private files, the helper hosts a small installation-scoped gRPC activation-control endpoint on a separate Unix socket.

The newly installed `nakode` executable exposes an endpoint descriptor for this control service, analogous to `nakode endpoint`. Discovery starts/reconciles the helper when a durable pending activation exists. While pending, the helper serves the activation API on the separate socket. After successful activation, the current B service serves the same activation API and the SDK rediscovers it; the helper first publishes terminal `activated`, hands off, and exits. This gives status/history a public home in both pending and current states. FStack connects through the public SDK/wire contract; it never reads the activation journal, PID file, runtime record, or lifecycle socket.

This endpoint owns only executable activation status/control. It does not become a second session server and cannot mutate transcripts, prompts, models, providers, or persistence outside the activation journal.

Required public operations:

- `GetActivationStatus`
- `WatchActivationStatus` using authoritative replacement snapshots
- `ForceActivationRecheck`
- `ForceActivate` with exact blocker IDs/revisions and explicit confirmation metadata, capability-gated on conditional-force support in the running service

A status watch prevents every dashboard renderer from inventing polling cadence. The helper still checks quiescence every 15 seconds; clients receive a replacement whenever helper state, blockers, history, or identity changes. SDK reconnects/resubscribes if the helper restarts.

### Public status shape

The Protobuf shape includes:

- stable activation attempt ID and monotonic revision;
- installed and running version/build identities;
- helper state, instance identity, started/heartbeat timestamps;
- cadence, last-check, and next-check timestamps;
- blocker rows with logical session ID, display title, activity class, queue count, reason, and observed session revision;
- bounded audit attempts with trigger, start/end, result, blocker count, and diagnostic summary;
- current error with retryability;
- whether safe and forced activation controls are supported.

Mutations carry idempotency keys. `ForceActivate` also carries the exact observed activation attempt ID, activation revision, and blocker identity/revision set. Rejected requests are durably audited and replay their original rejection for the retained idempotency window.

## Existing primitives to reuse

The safe path reuses the primitives below. The conditional-force compare/fence request and its advertised capability were net-new lifecycle/public capability work and are now implemented separately from existing unconditional `Shutdown`.

Nakode:

- install and atomic executable replacement: `src/update.rs`, `install.sh`;
- executable/service identity and endpoint activation: `src/control_service.rs:39-135`;
- stale scan/report: `src/control_service.rs:1853-2030`;
- atomic quiescence fence: `src/control_service.rs:553-586`, `src/server/runtime.rs:493-516`;
- detached restart/start/readiness: `src/control_service.rs:1768-1825`;
- session activity/queue inventory: `src/server.rs:3272-3285` and public workspace projections;
- Protobuf service boundary: `proto/nakode/v1/nakode.proto`;
- server/SDK status precedent: `crates/nakode-server/src/grpc.rs`, `crates/nakode-sdk/src/lib.rs`.

FStack:

- existing footer update action: `apps/dashboard/src/shell/Sidebar.tsx:112-131`;
- updater provider, subscription, and cadence precedent: `apps/dashboard/src/features/local-update/local-update-context.tsx`;
- modal/details pattern: `apps/dashboard/src/features/local-update/LocalUpdateRunModal.tsx`;
- process-owned state contract precedent: `apps/dashboard/electron/local-update-contract.ts`;
- optional host capability: `apps/dashboard/src/core/host.ts`, `apps/dashboard/src/hosts/electron.ts`;
- Nakode endpoint/wire client: `apps/dashboard/electron/nakode.ts`, `apps/dashboard/electron/nakode-protobuf.ts`.

## Exact implementation touchpoints

### Nakode

- `install.sh`, `src/update.rs`: report installation separately from activation and route stale-service refresh into durable deferred activation.
- `src/activation.rs`: schema-versioned journal, helper and mutation leases, heartbeat/socket recovery, immediate/scheduled/manual checks, status/watch RPC service, conditional force fencing, startup/readiness identity verification, and helper-to-service handoff.
- `src/config.rs`, `src/main.rs`, `src/service_cli.rs`: hidden helper entry point, activation endpoint/status discovery, and CLI lifecycle plumbing.
- `src/control_service.rs`: executable identity, endpoint descriptors, stale-service classification, structured lifecycle requests, atomic quiescence/conditional-force shutdown, and bounded replacement readiness.
- `src/server.rs`, `src/server/runtime.rs`: structured blockers and the complete live-work predicate, including turns, queues, interactions, native delegations, external/MCP calls, and owned shells.
- `proto/nakode/v1/nakode.proto`, `crates/nakode-api`, and `crates/nakode-protocol`: public activation messages/service and lifecycle blocker/capability transport.
- `crates/nakode-sdk/src/lib.rs`: activation discovery/watch client plus same-logical-session reconnect and authoritative replacement hydration.
- `src/app.rs`: built-in client attachment recovery without projecting transport loss as terminal session state.
- `tests/activation_lifecycle.rs`, `tests/backend_fixture.rs`, and module tests: isolated old-A/new-B cutover, helper and no-service recovery, identity mismatch/failure recording, force fences, lease ownership, and bounded fixture gates.

### FStack

- `apps/dashboard/electron/nakode-activation-contract.ts`, `nakode-protobuf.ts`, `nakode.ts`, and `nakode-activation.ts`: typed public activation wire contract, endpoint discovery, process-owned generation-fenced watch, mutations, and graceful teardown.
- `apps/dashboard/electron/nakode-session-reconnect.ts`, `nakode-agent.ts`, `chat.ts`, and attachment/cleanup registries: same-ID reconnect, authoritative full-snapshot replacement, transient reconnecting state, and late close/archive/delete guards.
- `apps/dashboard/electron/wire.ts`, `preload.ts`, `ipc.ts`, `src/core/host.ts`, and host adapters: bounded optional activation capability; web remains incapable rather than faking support.
- `apps/dashboard/src/features/nakode-activation/`: pure presentation, process-state context, status/history modal, manual recheck, and exact-fence destructive confirmation.
- `apps/dashboard/src/shell/Sidebar.tsx`: pending row directly above Update, blocker badge, modal ownership, and automatic close only after activation is no longer pending.
- `apps/dashboard/src/design/base.css` and `features.css`: fixed compact/expanded row, badge, modal, blocker, history, and confirmation layouts using existing tokens.
- `apps/dashboard/src/dev/scenes.ts`, focused tests, committed screenshots, `AGENTS.md`, and `README.md`: activation/reconnect behavior contracts and visual gallery.

## Required visual scenes

At minimum:

1. expanded sidebar, pending with one blocker;
2. expanded sidebar, pending with multi-digit blocker count;
3. collapsed sidebar, pending badge;
4. modal waiting with mixed active/queued reasons;
5. modal checking;
6. modal activating and non-dismissible;
7. modal stalled with actionable failure;
8. Details open with identities, cadence, and bounded history;
9. destructive confirmation naming exact blocker count;
10. force refusal after blocker revisions changed;
11. pending action absent after verified success;
12. narrow modal layout with long session title and diagnostics.

Every changed sidebar scene, including unrelated tabs whose footer is visible, must be reviewed or deliberately scoped through scene fixtures so the new row does not shift, wrap, or clip existing footer actions.

## Deterministic test plan

### Nakode unit/integration

- immediate quiescent install activates and creates no pending record/helper;
- active turn and queued prompts create structured blockers and one pending helper;
- helper cadence uses a paused test clock; scheduled and manual checks are single-flight;
- `ForceActivationRecheck` updates last/next timestamps and audit history;
- quiescence refusal never fences the runtime;
- a successful quiescence response fences new mutations until socket handoff;
- helper crash mid-check rolls the fence back and is recoverable;
- duplicate helper spawn loses the helper lease without killing the owner;
- endpoint discovery racing a helper-owned scheduled check/cutover serializes through the activation lease and cannot start a second replacement;
- a stale helper lease is retained during startup grace or while its socket and matching heartbeat are healthy; otherwise compare-before-remove reclamation permits deterministic recovery even from a live-but-wedged PID;
- installed/running same version but different hashes remains pending;
- success requires replacement readiness and matching executable identity;
- helper/service startup failure becomes `failed`, is presented as stalled, and retains the pending identity mismatch;
- bounded history truncates oldest attempts deterministically;
- forced activation rejects changed activation/blocker revisions;
- confirmed forced activation records interruption intent before shutdown;
- machine-restart fixture rediscovers pending activation and restarts the helper;
- older stale service remains queryable enough for structured public session blockers, with final lifecycle quiescence authoritative.

Extend existing tests around `src/control_service.rs::stale_activation_refuses_identity_rich_live_work`, activation lease ownership, lifecycle shutdown, and `src/server/runtime.rs::quiescence_fences_new_mutations_before_shutdown`.

### Nakode SDK/TUI

- activation watch reconnects after helper crash and receives a full replacement;
- activation watch rediscovers the new B service after helper handoff;
- session/workspace watches reconnect after successful service replacement;
- stale snapshots/revisions are rejected;
- a mutation racing the fence is retried idempotently or returned without duplicate acceptance;
- a downstream execution failure after durable mutation acceptance replays the same gRPC error code/message for the same idempotency key;
- TUI draft, focus, selected session, scroll, and transcript stable IDs survive reconnect;
- no transport failure is projected as authoritative session destruction.

### FStack behavior

- pending row appears only for non-current activation state and sits before the existing update action;
- blocker badge and accessible name use authoritative count;
- helper `failed` remains visible in red even when its endpoint must reconnect;
- one Electron process watch is shared across windows;
- `Check now` is single-flight with periodic status updates;
- modal remains open when blockers remain;
- modal closes only after a current identity snapshot follows successful recheck/activation;
- forced action always requires confirmation and exact blocker revisions;
- changed blockers refuse without stopping work and refresh the modal;
- absent activation capability removes the affordance on web/unsupported hosts;
- renderer never reads Nakode files or invokes lifecycle commands directly;
- reconnect preserves Agent and Chat projections.

Extend the endpoint/workspace tests and add focused activation contract/context/modal tests beside `apps/dashboard/tests/local-update*.test.ts`.

### Isolated live regression

Use a temporary `HOME`, `NAKODE_HOME`, `NAKODE_CONTROL_DIR`, `FSTACK_HOME`, source checkout, local Git remote, installed prefix, and deterministic held provider fixture. Never permit login-shell discovery outside the temporary path.

Prove:

1. build B installs over build A;
2. A remains running while a deterministic active turn/queue blocks activation;
3. helper status reports exact identities, cadence, and blockers;
4. manual recheck remains blocked without interruption;
5. releasing the fixture lets the next check quiesce A and activate B;
6. helper reaches terminal success and exits;
7. dashboard/TUI watches reconnect with stable logical session and transcript identities;
8. pending UI disappears only after B identity is verified;
9. no socket, database, process record, or executable escapes the temporary root.

## Rollout sequence

1. **Nakode authority:** structured stale status, complete quiescence predicate, durable activation journal, helper singleton, immediate/scheduled checks, safe activation.
2. **Public control plane:** activation endpoint descriptor, Protobuf query/watch/recheck, SDK reconnect.
3. **Client continuity:** session/workspace watch reconnect and full-snapshot hydration in Nakode SDK; equivalent FStack session reconnect.
4. **FStack visibility:** process-owned activation adapter, pending sidebar action, modal, history, scenes.
5. **Conditional force path:** add the running-service compare/fence capability and expose the destructive button only when advertised; keep it impossible for automatic code to invoke. Do not claim exact force semantics against a pre-capability A.
6. **Isolated old-A/new-B regression:** gate release behavior across two actual executable identities.

This ordering ensures the visual queue is always backed by Nakode-authoritative durable state. The dashboard icon must not ship before the helper can survive restart and accurately report why activation remains blocked.

## Done criteria

- `nakode update` never reports an unqualified current state while the running build differs.
- Deferred activation is durable, automatically rechecked, and visible until resolved.
- The dashboard names cadence, last/next check, blocker count/reasons, identities, and bounded history.
- Manual recheck is auditable and closes the modal only after verified activation.
- Forced activation is explicit, confirmed, revision-fenced, and audited.
- Safe activation never interrupts live work, queues, interactions, delegations, MCP calls, or owned shells.
- Clients reconnect without transcript loss, duplication, false failure, draft loss, or scroll reset.
- Helper crash/restart cannot hide the stale condition or create competing activation owners.
- FStack consumes only Nakode's public activation protocol and never reads Nakode persistence or lifecycle sockets.
