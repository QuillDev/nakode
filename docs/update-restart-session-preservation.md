# Update and restart session preservation: investigation and Option A implementation

> **Scope note.** The command traces, root-cause matrix, and “today” wording below describe the pre-change behavior at the ticket's merged base. The current branch implements the selected whole-service Option A contract; its post-change behavior and evidence are recorded under [Implemented preservation contract](#implemented-preservation-contract).

## Pre-change executive finding

The owner-visible behavior is the combination of three different mechanisms, not session deletion:

1. `nakode update` **does update the managed source checkout and atomically replace the installed executable**, but activation is only attempted once. A stale service with live work is deliberately left on the old executable. A later `nakode endpoint` also reuses that old service when it still speaks `nakode.v1`, so there is no deferred activation after the work becomes idle.
2. `nakode restart` is not the safe activation path used by the installer. It sends unconditional `Shutdown`, terminates provider, subagent, and shell runtimes, and discards process-only turn and queue state before starting the replacement.
3. FStack reconnects its shared **workspace** watch, but not an attached **session** watch. A service restart therefore changes every attached dashboard projection to local `failed` state even when the logical session row and provider resume identity remain persisted.

Consequently, “restart destroyed every session” is partly real interruption and partly a projection/reconnect defect:

- active turns, queued prompts, questions/approvals, and shells are process-only and cannot currently survive an unconditional restart;
- active delegated runs are durably reconciled to `Interrupted` when their parent is loaded again;
- idle and already-closed logical sessions are not deleted and remain discoverable/resumable when their provider supports resume;
- attached FStack rows do not automatically reattach, so those surviving sessions appear dead until a fresh discovery/open path is used.

No live update, restart, signal, endpoint mutation, or owner-data mutation was used for this investigation or its validation. Every process-level proof uses isolated homes, control directories, sockets, databases, source checkouts, and executables.

## Exact command paths

### `nakode update`

`nakode update` and the legacy top-level `--update` flag both short-circuit to `update::run` (`src/main.rs`, `src/config.rs`, `src/update.rs:38-85`). The command:

1. requires `HOME` and the managed checkout at `$HOME/.nakode/src`;
2. requires that checkout's `install.sh`;
3. reads the literal Git `origin`, normalizes known QuillDev/Cursor Origin repository identities, and retargets a retired managed remote to `https://github.com/QuillDev/nakode.git` (`src/update.rs:87-190`);
4. runs `git pull --ff-only` in the checkout;
5. runs `sh ./install.sh` with no update-specific installer options;
6. prints `Nakode is up to date.` if the installer exits successfully.

There is no release API, version manifest, semver comparison, binary download, or package selection. Source discovery is fixed to the managed Git checkout.

The installer requires Cargo, runs `cargo build --release --locked`, verifies the resulting executable, copies it to a temporary file in the destination directory, and atomically renames it over `$HOME/.local/bin/nakode` by default (`install.sh:122-288`). A running service continues executing its old inode.

After replacement, `install.sh` invokes the new executable by absolute path as `restart-stale`. That command scans the installation singleton and legacy runtime directories (`src/service_cli.rs:69-91`, `src/control_service.rs:1853-2030`):

- a current singleton is retained;
- a stale, quiescent singleton is restarted with `QuiesceShutdown`;
- a stale singleton with an active turn, queued prompt, or pending native delegation is left running and reported;
- quiescent legacy per-workspace services are retired;
- active, partly reachable, unidentified, or unsafe legacy services are left untouched.

`QuiesceShutdown` atomically asks the runtime for live session IDs and fences new mutations before acknowledging shutdown (`src/control_service.rs:553-586`, `src/server/runtime.rs:493-516`). This is the safe behavior already used for installation activation.

The update command itself performs no configuration rewrite or data migration. On replacement startup, `prepare_runtime` opens the global SQLite repository, whose open path applies forward schema migrations and seeds the provider catalogue compiled from `config/providers.toml` (`src/server/runtime.rs:255-320`, `src/session.rs:770-795`, `src/session.rs:1127-1288`, `src/session.rs:1802-1838`).

### Why an update can remain inactive indefinitely

The stale-service refresh is a one-shot attempt during installation. If it sees live work, it preserves the old process. There is no persisted “activation pending” state and no background drain/handoff.

Later frontend endpoint discovery does not necessarily activate the new executable. `frontend_api_endpoint_report` deliberately reuses a responsive service if either its executable identity matches **or** its API version is still `nakode.v1` (`src/control_service.rs:1199-1284`). Additive features are represented as capabilities, so an old but wire-compatible service can be reused forever after the one installer attempt was deferred.

This explains the report that `nakode update` appeared to update nothing: the checkout and installed file changed, but the feature-serving process did not. The final “up to date” message does not distinguish “installed and active” from “installed, activation deferred.”

FStack compounds the ordering for bundled skills. `fstack update` preflights both repositories, runs `nakode update`, and only afterward pulls and installs FStack (`fstack-inve-nakode-u/src/update.rs:116-165`). FStack's installer owns links for bundled skills under `~/.agents/skills` (`fstack-inve-nakode-u/install.sh:6-15`). Therefore the Nakode stale-service activation attempt happens **before** the new FStack skill files are installed.

### `nakode restart`

`NakodeCommand::Restart` dispatches through `service_cli::restart` to `restart_service` (`src/main.rs`, `src/service_cli.rs:52-62`). Unlike `restart-stale`, `restart_service` uses ordinary `LifecycleRequest::Shutdown`, not `QuiesceShutdown` (`src/control_service.rs:1768-1825`). It:

1. resolves the installation-wide service paths;
2. sends unconditional `Shutdown` to the lifecycle socket;
3. waits for both lifecycle and API sockets to disappear;
4. starts the currently installed executable detached;
5. waits for readiness.

No Unix signal is sent on the normal path. Ctrl-C is handled separately for a foreground service. The lifecycle request causes the service task to call runtime shutdown, abort gRPC/lifecycle listeners, and stop transports (`src/control_service.rs:430-478`, `src/control_service.rs:530-588`). Runtime shutdown:

- fails pending native delegation callers;
- cancels pending MCP calls/discoveries;
- sends `BackendCommand::Shutdown` to provider control handles, attached provider sessions, and subagent providers;
- awaits provider tasks;
- shuts down supervised shells (`src/server/runtime.rs:415-490`, `src/server/runtime.rs:2480-2502`, `src/server/runtime.rs:2751-2753`).

The installed executable and source checkout are not changed by `restart`. Startup then performs the ordinary repository open/migration/catalogue/bootstrap path described above.

## Persistence and restoration boundaries

`SessionRecord` persists logical/provider IDs, workspace and working directory, model/options, terminal owner-turn attribution, timestamps, enabled skill IDs, and delegated provider-session identities (`src/session.rs:60-98`). It does **not** persist `DomainState`'s active turn, queue, active shells, approvals, questions, pending steer/redirect, pending provider start, pending handoff, or external tool requests (`src/state.rs:1181-1237`).

Startup loads the global recent session inventory and bridge records but does not eagerly resume every provider session (`src/server/runtime.rs:255-373`). A provider session is resumed only when `OpenSession` creates an attached engine and issues `BackendCommand::ResumeSession` with the persisted provider session ID (`src/server.rs:1733-1791`, `src/state.rs:3231-3301`). Resume therefore restores provider context, not an in-flight Nakode turn or queue.

Special cases:

- accepted bridge inbox prompts are durable and replay with their original client prompt ID (`src/server.rs:1509-1541`);
- delegated-run rows are durable; a row found in an active state when its parent loads is authoritatively rewritten to `Interrupted` with retained salvage (`src/server/runtime.rs:3443-3456`, tested at `src/server/runtime.rs:3790-3899`);
- logical deletion is a separate `DeleteSession` effect/RPC. Neither update nor restart invokes it.

## Lifecycle/state matrix

“Update, deferred” means the installed binary changed but a stale service was preserved because it owned live work. “Update, activated” means the safe quiescent replacement succeeded.

| State before command | `nakode update`, activation deferred | `nakode update`, quiescent activation | `nakode restart` today | What is authoritative afterward |
|---|---|---|---|---|
| Active owner turn | Old process and turn continue unchanged; new binary inactive | Not eligible: live work blocks quiescence | Provider/session process is shut down; in-memory active-turn state is lost | Logical session row remains. No persisted active-turn terminal outcome exists; provider resume may recover prior context, not the interrupted turn |
| Idle attached session | Unchanged on old process | Logical/provider identity persists; process is replaced | Logical/provider identity persists; process is replaced | Session is resumable if the provider advertises resume. FStack's existing attached session watch fails and does not reattach |
| One or more queued prompts | Old process and queue continue; new binary inactive | Not eligible: a non-empty queue blocks quiescence | Queue is discarded with the process | Session row remains, but queued prompt IDs/text are not persisted or replayed |
| Pending question/approval/external tool call | Normally contributes to non-idle work, so old process is preserved | Not eligible while represented as live session activity | Pending interaction/call is discarded; MCP work is cancelled | No general durable interaction recovery; durable bridge inbox is the narrow exception |
| Active delegated child/native run | Old process/run continues; pending native delegation explicitly blocks quiescence | Not eligible | Child backend is shut down; pending caller fails | Durable child row is changed to `Interrupted` when loaded. FStack renders interrupted/partial children as terminal destroyed children but retained evidence is not deleted |
| Supervised shell | Old process/shell continues; `SessionActivity::RunningShell` blocks quiescence | Not eligible while the shell is running | Shell is terminated | Shell ownership/state is process-only and is not restored |
| Idle persisted, currently closed session | Persisted row unchanged | Persisted row unchanged | Persisted row unchanged | Still discoverable and openable; no provider is resumed merely by discovery |
| Already deleted session | Remains absent | Remains absent | Remains absent | No command recreates it |
| Durable bridge inbound prompt | Remains on old process/durable store | Persisted and replayable after startup | Persisted and replayable after startup | Replayed idempotently with the original client prompt ID |

The service does not authoritatively mark all logical sessions destroyed. The real destructive boundary is process-local work. The dashboard-wide “everything failed” appearance comes from its connection projection.

## FStack observation and the apparent mass destruction

FStack obtains the installation endpoint by executing `nakode endpoint`, validates a version-1 `grpc+unix` descriptor and `nakode.v1`, and retains executable/service/activation identity (`fstack-inve-nakode-u/apps/dashboard/electron/nakode.ts:1198-1308`).

The process-shared workspace watch is ref-counted and reconnects with bounded exponential backoff after stream failure (`fstack-inve-nakode-u/apps/dashboard/electron/nakode-workspace.ts:17-176`). Session inventory reconciliation removes local identities only when a complete authoritative inventory omits them; incomplete inventory keeps recoverable rows (`fstack-inve-nakode-u/apps/dashboard/electron/agents.ts:987-1077`, `fstack-inve-nakode-u/apps/dashboard/electron/agent-workspace.ts:305-400`). Dashboard shutdown itself snapshots and detaches watchers; it does not terminate Nakode-owned conversations (`fstack-inve-nakode-u/apps/dashboard/electron/agents.ts:979-985`).

An attached `NakodeAgent`, however, opens one `NakodeClient.watch(sessionId)` stream and forwards stream termination directly to `onError`; there is no retry/reopen path (`fstack-inve-nakode-u/apps/dashboard/electron/nakode-agent.ts:418-463`, `fstack-inve-nakode-u/apps/dashboard/electron/nakode.ts:598-655`). The registry handler changes that row to local `failed` and displays `Nakode service connection failed` (`fstack-inve-nakode-u/apps/dashboard/electron/agents.ts:731-741`).

Thus after a global service restart:

- the shared workspace view reconnects;
- every attached session stream dies;
- each attached row becomes locally failed;
- no fresh client opens the surviving logical session;
- the failed row is not proof that Nakode deleted or terminalized the logical session.

This is a mismatch between Nakode authority and FStack projection, independent of the genuine loss of any active process-only work.

## How new features become active

| Update class | Installed by | Activation today | Full service restart required? |
|---|---|---|---|
| Nakode server/domain/tool code | `nakode update` builds and replaces the executable | Safe stale refresh only if quiescent; otherwise manual restart is currently the only guaranteed activation | Yes, because the old process executes the old inode |
| Protobuf server/API implementation | Nakode executable | Same as server code. Old `nakode.v1` services are intentionally considered reusable even if missing additive capabilities | Server restart yes; FStack/client code must also understand the new fields/RPCs |
| Rust SDK / FStack TypeScript wire client | Their owning repository build | Nakode SDK consumers must relaunch/rebuild; FStack's dashboard relaunches after a successful supervised `fstack update` | Client process restart/relaunch, not necessarily server restart when protocol remains compatible |
| Provider/native bridge implementation compiled into Nakode | Nakode executable | Loaded/spawned by `prepare_runtime`/provider registry | Yes for new code. Live credential/enablement changes use runtime effects and periodic provider synchronization |
| Provider catalogue in `config/providers.toml` | Nakode executable (`include_str!`) | Seeded when the repository opens on startup | Yes to expose catalogue changes from a new binary |
| Global agent archetype TOML | FStack or owner files | Re-read at delegation/invocation boundaries; `/reload` also reloads it | No (`README.md:210-236`, `src/server.rs:2068-2093`) |
| Skill files in `~/.agents/skills` or workspace | FStack or owner files | New sessions/open operations load current files; `read_skill` loads the body from disk at execution; `ReloadWorkspace`/`/reload` refreshes cached metadata for an attached session while preserving its enabled stable-ID snapshot | No. Newly installed stable IDs are intentionally not silently granted to an existing session |
| Personality/Soul/local file configuration | Owner/FStack | Public reload or a fresh session, according to snapshot semantics | Usually no; creation-time instructions intentionally remain immutable for existing provider sessions |
| Persisted provider/model/add-on settings | Nakode public commands | Applied by runtime effects and provider synchronization | No for supported live mutations; new implementation code still needs server activation |
| Startup-only paths/environment/service configuration | CLI/service environment | Read while preparing the runtime | Yes unless a typed live reload exists |

For the reported skill case, a service restart is technically broader than necessary. Nakode already exposes `ReloadWorkspace` and the TUI `/reload` action for skills, agents, and backend metadata (`proto/nakode/v1/nakode.proto:11-15,121-125`, `src/controls.rs:584-590`, `src/server/runtime.rs:3181-3211`). FStack does not currently expose that operation. Also, `fstack update` installs bundled skill links only after its nested `nakode update` activation attempt, so that attempt cannot observe the new skill files.

## Root-cause classification

| Finding | Classification |
|---|---|
| Update preserves a stale service that owns active work | Intentional safety policy, implemented with atomic quiescence |
| Update has no durable pending-activation state and still says “up to date” | Product/implementation gap; installed and active versions are conflated |
| Compatible endpoint discovery can reuse the stale old service indefinitely | Intentional continuity policy that creates an activation gap when no later safe activation trigger exists |
| Explicit restart bypasses quiescence | Intentional current CLI behavior, but unsafe as the only guaranteed activation route |
| Active turns/queues/interactions are not restart checkpoints | Current persistence limitation; provider resume alone cannot reconstruct Nakode-owned in-flight state |
| Delegated runs become `Interrupted` after restart | Intentional terminal reconciliation, not deletion |
| FStack attached session watches do not reconnect | Bounded client integration defect/missing SDK behavior; the workspace watch already demonstrates the intended reconnect shape |
| All rows appear failed after restart | FStack projection mismatch; it does not prove authoritative deletion |
| Existing session cannot silently gain a newly installed skill identity | Intentional authority snapshot. Updated content for an already-enabled stable ID can be loaded without restart |

Relevant history supports these distinctions:

- Nakode `1b7b007` introduced the installation-wide singleton/global inventory.
- Nakode `176744598` added executable identity and activation reporting.
- Nakode `42598c1f` added actor-owned quiescent stale replacement while deliberately retaining ordinary shutdown for explicit restart.
- Nakode `0f6d14b` persisted canonical working-directory resume identity.
- FStack `dc19ed574` moved discovery to global Nakode sessions; `a9766f6` corrected singleton endpoint invocation.
- FStack `d92e73b` added reconnect for shared workspace watches, but the older per-session watch path remains one-shot.
- FStack `5d020765` established `nakode update` before FStack pull/install ordering.

## Recommended preservation contract

### Selected Option A: automatic whole-service drain and activation

The selected product behavior is **not** per-session old/new generation routing. `nakode update` installs build B, lets the current build A continue owning the entire service while any live work remains, and leaves a durable helper responsible for automatically activating B once the whole service is quiescent.

The safe handoff boundary is after every active turn, accepted queue, interaction, delegated descendant, MCP/external operation, and owned shell included in the authoritative live-work predicate has settled. The selected sequence is:

1. `nakode update` atomically installs B while A continues to serve its existing endpoint.
2. Nakode immediately attempts the existing `QuiesceShutdown` path.
3. If quiescence is refused, Nakode records installed B versus running A and starts one installation-scoped detached activation helper.
4. The helper reports structured blocker IDs/reasons, checks immediately and every 15 seconds, and keeps a bounded audit history.
5. Each check uses the existing activation lease only for the check/cutover and treats `QuiesceShutdown` as the final atomic authority.
6. Once the whole service is quiescent, the helper activates B through the existing detached start/readiness path and verifies B's executable identity.
7. SDK, TUI, and FStack watches reconnect, reopen the same persisted logical sessions, and consume full authoritative replacement snapshots.

Installation and activation are separate observable states. An update is not reported as fully current while the installed and running build identities differ.

The implementation-ready product, helper, public protocol, dashboard modal, audit-state, rollout, and deterministic test specification is in [`deferred-activation-plan.md`](deferred-activation-plan.md).

### Rejected for this scope: per-session generations

Running old and new execution generations concurrently could let unrelated new sessions use B before A drains, but today's architecture cannot do that safely. It would require a stable routing authority, per-session lease epochs, a durable cross-generation holding queue, schema compatibility across A and B, provider ownership transfer, and generation-neutral watches. Two current servers sharing SQLite would instead create competing authorities over session revisions, idempotency receipts, provider handles, queues, and migrations.

Side-by-side generations therefore remain a higher-complexity future architecture, not the recommendation for this ticket. Option A deliberately waits for whole-service quiescence and then performs one replacement.

### Selected contract

1. Report installed executable identity separately from the running service and persist/recompute pending activation.
2. Keep the stale service available while accepted work settles; do not ask the owner to close idle sessions.
3. Recheck automatically with visible cadence/history and perform the existing quiescent replacement as soon as the entire service is idle.
4. Keep unconditional interruption behind an explicit, revision-fenced destructive confirmation that lists exactly what work will be interrupted.
5. Reconnect Nakode SDK/TUI and FStack session watches, reopen surviving logical sessions, and consume fresh authoritative snapshots rather than converting transport loss into local terminal state.
6. Expose `ReloadWorkspace` for skill/archetype-only changes so they do not wait for binary activation.

Option A does not make new sessions use B while A is active, but it removes the current “update does not activate, close everything, then destructive restart” trap without pretending that an in-flight provider turn is resumable.

### Implemented preservation contract

The current branch implements the selected contract across Nakode and FStack:

- `install.sh` still atomically replaces the executable first, but now reports installation separately from running-service activation. `restart-stale` either activates immediately or durably schedules activation instead of ending with a one-shot refusal.
- `src/activation.rs` owns a schema-1 journal and two distinct leases under the private control directory: `activation-helper.lock` for the singleton helper generation and short-lived `activation.lock` for observation/mutation/cutover serialization. Ownership is PID plus random instance ID with heartbeat validation, compare-before-remove reclamation, stale-socket verification, corrupt-journal quarantine, and forward-schema preservation.
- The installed B executable runs one detached `activation-helper`. It checks immediately and every 15 seconds, refreshes its heartbeat even in failed states, bounds stale-service blocker queries, and uses the existing runtime-owned `QuiesceShutdown` fence as final authority. Matching runtime JSON without a reachable matching API is never projected as `current` or `activated`.
- `nakode.v1.ActivationService` publicly exposes query, attempt-qualified replacement watch, manual recheck, and conditional force. Status includes installed/running/helper identities, capabilities, exact blockers, bounded history, failures, and audit results. While A drains, the helper serves this service on `activation.sock`; after B readiness, authority hands to B on `api.sock`, terminal helper watches close, and the helper exits.
- Force is never an unconditional fallback. The owner must confirm the exact observed attempt ID, attempt-local activation revision, and blocker ID/revision set. The running service compares and fences that set atomically, audits accepted and rejected results, and preserves idempotency replay. If execution fails after durable acceptance, the same key replays the original gRPC error shape rather than pretending the retry succeeded. A pre-capability stale service reports conditional force unavailable.
- The Rust SDK persists `SessionAttachment` inputs and reopens a `NotFound` watch against the same logical session ID after fresh endpoint discovery. Identity-changing projections fail terminally. The built-in app and FStack Agent/Chat projections use full replacement snapshots and an explicit transient `reconnecting` state.
- FStack consumes only the public endpoint descriptor and Protobuf service. Its process-owned activation controller shares one generation-fenced watch across windows. The dashboard exposes pending/checking/activating/failed/history states and an attempt-scoped destructive confirmation that fails closed on disconnect or any attempt/revision/blocker-fence change.
- FStack bridge restoration is guarded twice: the local attachment must still be current, and `SetSessionBridgeLifecycle(open)` carries the fetched owning-session revision. A concurrent archive/clear/delete therefore wins or makes the reconnect retry; it cannot be undone by a late background `open`.

Installation and activation are now observably distinct. Explicit `nakode restart` intentionally remains the unconditional administrative lifecycle command described above; safe update activation does not call it.

#### Post-change state matrix

| State before activation | Automatic update activation | Explicit conditional force | Explicit `nakode restart` | Durable authority afterward |
|---|---|---|---|---|
| Active owner turn | A and the turn continue; the helper reports a blocker and waits | Only after exact destructive confirmation; the turn may be interrupted | Interrupted unconditionally | Logical session/transcript remain; process-only in-flight turn state is not checkpointed |
| Idle attached session | When the whole service is quiescent, B starts and SDK/FStack reopen the same logical ID from a fresh endpoint | Reopens after B readiness | Reopens after replacement | Persisted provider/session identity and transcript survive |
| Queued prompt | Queue blocks activation and remains owned by A | Exact confirmed force may discard it | Discarded | Session row survives; the process queue is not durable |
| Pending question, approval, external/MCP work | Represented live work blocks safe activation | Exact confirmed force may interrupt/cancel it | Interrupted/cancelled | No general process-interaction checkpoint exists |
| Active delegated/native child | Blocks safe activation and continues on A | Exact confirmed force may interrupt it | Interrupted | Durable child evidence reconciles to `Interrupted` when reloaded |
| Supervised shell | Blocks safe activation and continues on A | Exact confirmed force may terminate it | Terminated | Shell process ownership is not durable |
| Idle persisted, currently unattached session | Unchanged; lifecycle proof reopens it on B with the same ID and transcript | Unchanged and reopenable | Unchanged and reopenable | Canonical session row remains |
| Archived/already-closed session | Remains archived and discoverable under existing policy | Remains archived | Remains archived | No reconnect path silently reopens an attachment being closed |
| Already deleted session | Remains absent | Remains absent | Remains absent | Only `DeleteSession` deletes logical state |
| Durable bridge inbound prompt | Persists and remains replayable with its idempotency identity | Persists across startup | Persists across startup | Durable bridge inbox is authoritative |
| Helper crash/stale socket | Journal remains pending; endpoint discovery reclaims only a disproven owner and starts one replacement helper | Same singleton rules | Not applicable to restart itself | Pending mismatch remains visible; it is never silently projected current |

This preserves all work on the default path because cutover cannot begin until the service-wide live-work predicate is empty and the actor-owned fence prevents a mutation race. It does not claim that arbitrary provider turns can resume after explicit interruption.

#### Known bounded limitations

- Provider turn, queue, pending-interaction, native-child, and shell execution state is not checkpointed. Explicit conditional force and unconditional `nakode restart` can still interrupt it; the non-destructive activation path waits instead.
- During the helper lease's five-second startup grace, a reused live PID plus a newly written lock can temporarily look like a starting helper before socket/heartbeat confirmation. This can delay helper reclamation until grace expires; it does not authorize cutover or make an unreachable build look current.
- A helper crash after durably accepting a mutation key but before executing it is handled at-most-once: replay of that key returns the current accepted journal snapshot rather than re-driving a potentially destructive operation. The status watch/ticker reconciles within the normal cadence. Clients must continue observing authoritative status instead of treating one mutation response as the terminal outcome.
- The deterministic lifecycle acceptance uses the fixture provider. It proves Nakode's process, persistence, identity, and transport contracts, but not resumability of arbitrary process-only state inside every real provider.

## Concrete edit and test touchpoints

### Nakode

- `install.sh`, `src/update.rs`: separate installation completion from activation and launch/reconcile deferred activation.
- `src/activation.rs`: journal schema, helper singleton, heartbeat/recovery, activation socket lease, immediate/periodic/manual checks, status/watch authority, idempotency/audit history, readiness verification, and helper-to-service handoff.
- `src/config.rs`, `src/main.rs`, `src/service_cli.rs`: hidden helper process, activation endpoint discovery, and owner CLI surfaces.
- `src/control_service.rs`, `src/server.rs`, `src/server/runtime.rs`: structured live-work inventory, reachable service/build verification, conditional force compare/fence, and final atomic quiescence authority.
- `proto/nakode/v1/nakode.proto`, `crates/nakode-api`, `crates/nakode-protocol`, and `crates/nakode-sdk`: public activation service plus attached-session recovery with stable identity enforcement.
- `src/app.rs`: built-in client reconnection through SDK attached-session recovery.
- `tests/activation_lifecycle.rs` and focused module tests: isolated A/B process lifecycle, helper recovery/handoff, a stopped-A/no-B cutover gap, same-ID active and persisted-idle reattachment, transcript survival, exact execution-error idempotency replay, force fences, journal/lease behavior, blocker query bounds, and false-current prevention.

### FStack

- `apps/dashboard/electron/nakode-protobuf.ts`, `nakode.ts`, `nakode-session-reconnect.ts`, and process-owned `nakode-activation.ts`: public activation transport, fresh endpoint discovery, attached-session identity/revision fencing, and shared reconnecting activation state.
- Agent/Chat main-process files: retained attachment configuration, same-ID full-snapshot replacement, transient reconnecting projection, terminal projection errors, and close/delete guards that prevent late bridge reopen.
- `apps/dashboard/electron/wire.ts`, `preload.ts`, IPC registration, `src/core/host.ts`, and host adapters: bounded optional activation capability.
- `apps/dashboard/src/features/nakode-activation/` and `src/shell/Sidebar.tsx`: pending action, modal, details/history, accessible status, recheck, and exact-fence destructive confirmation.
- `apps/dashboard/src/dev/scenes.ts`, dashboard tests, committed screenshots, `AGENTS.md`, and `README.md`: activation gallery and behavior contracts.

The complete product and test inventory is in [`deferred-activation-plan.md`](deferred-activation-plan.md).

## Safe deterministic regression plan

All lifecycle coverage must use a temporary home and control plane; never ambient owner state.

1. Create one temporary root containing isolated `HOME`, `NAKODE_HOME`, `NAKODE_CONTROL_DIR`, `FSTACK_HOME`, Cargo target, install prefix, source checkout, workspace, and local Git remote. Put only the temporary installed binary on the login-shell `PATH` used by FStack endpoint discovery.
2. Use Nakode's deterministic fixture provider. Add a FIFO/event-held turn fixture so “active” is controlled rather than timing-based.
3. Start build A against the isolated control/data paths. Create one completed idle session, one held active session, one queued follow-up, and one active delegated child.
4. Advance the local Git remote to build B and invoke build A's `nakode update`. Assert the installed executable is B, the service is still A, the update reports activation deferred with exact live IDs, and all work continues.
5. Assert the helper's immediate/manual checks refuse atomically, report authoritative blockers/cadence/history, and never leave the runtime fenced. Release/settle all live work and advance the paused cadence clock; assert the helper automatically activates B without logical-session deletion.
6. Keep one FStack workspace watch and one attached session watch open across automatic activation. Assert both reconnect, receive fresh replacement snapshots, and the idle session remains usable with retained history, draft, focus, and scroll state.
7. In a separate isolated case, invoke explicit forced restart during the held turn. Assert provider/shell shutdown, queue disposition, delegated-run `Interrupted` reconciliation, durable bridge replay, and the approved authoritative owner-turn interruption outcome.
8. Verify no socket, process record, database, source checkout, or executable path escapes the temporary root; stop the isolated service and remove the root in a finally/RAII cleanup path.

The existing `apps/dashboard/tests/nakode-head-e2e.live.mjs` provides the nearest cross-repository isolation pattern. Unit coverage should remain the primary gate; the isolated live test proves only process/transport boundaries, not arbitrary real-provider resumability.

## Validation evidence

Every pass/fail command below was run by the bounded test runner. No validation command discovered, restarted, signalled, or mutated the ambient Nakode installation.

### Nakode

- `cargo fmt --all -- --check` — passed.
- `cargo test --locked --all-targets --all-features` — passed: the main suite reported 733 passed and zero failed; `activation_lifecycle` reported 1 passed; `backend_fixture` reported 5 passed; every additional target passed.
- `cargo clippy --locked --all-targets --all-features -- -D warnings` — passed with no diagnostics.
- `cargo run --locked -- tui-eval --scenario tests/tui_scenarios/agent_smoke.jsonl` — passed with no diagnostics.
- `git diff --check` — passed.

The all-feature lifecycle regression copied two distinct executable identities into one private temporary installation and isolated `HOME`, `TMPDIR`, `NAKODE_HOME`, `NAKODE_CONTROL_DIR`, workspace, database, fixture, FIFO gate, sockets, process records, logs, and binaries. It proved: A remained authoritative during a held owner turn; stale refresh deferred; the helper reported the exact blocker; concurrent discovery retained one helper; a `SIGKILL`ed helper was replaced without stale-socket ownership damage; a stopped-A/no-B cutover gap recovered; B became the verified service identity; both the formerly active and persisted-idle logical session IDs reattached; transcripts survived; and the helper handed status to B and exited. The test's RAII cleanup stopped or killed only PIDs published beneath that private root.

Focused module coverage additionally proves that failed replacement verification cannot leave `activating` or `forcing` stuck, a live/corrupt runtime owner prevents unfenced no-service replacement, same-version/different-build identity remains stale, helper leases and history are bounded, force fences reject any authoritative change, and an execution error after durable idempotency acceptance replays the original gRPC code and message.

### FStack dashboard

- `./dxp check` — passed.
- `./dxp build` — passed.
- `bun test` — passed: 820 passed, 1 explicitly skipped fixture-latency test, 0 failed across 124 files.
- `git diff --check` — passed.

Focused controller and contract coverage includes helper-to-service rediscovery, same-attempt revision regression rejection, new-attempt lower-revision acceptance, stop/start races during connect and status hydration, in-flight mutation teardown, fresh-source force mutation, unknown-phase visibility, exact force-fence gating, process-owned graceful shutdown, and source-order proof that the pending activation action renders directly above Update.

All 13 committed activation screenshots were inspected: compact, narrow, expanded, multi-digit blocker badge, blocked, checking, activating, history, failed, reconnecting, current-hidden, force-unavailable, and destructive force confirmation. The required footer placement, identities, cadence, blockers/reasons, history, reconnect status, failure visibility, exact revision-bound confirmation, and current-state hiding are legible without clipping or overlap.
