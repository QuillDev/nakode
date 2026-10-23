# Managed background tasks

Nakode supervises session-owned dev servers, workers and watchers. Finite builds and installs use
`bash`. FStack and other clients use the public API/SDK, never the registry directly.

## Ownership and lifetime

- The authenticated creating logical session owns each task. Native runs are additionally scoped to
  their own launches. Tool arguments cannot choose an owner; integrations are authenticated against
  the execution machine. A Chat does not own tasks launched by its durable Ticket/General children.
- Tasks survive tool completion, turns, retained close/reopen and provider changes. Only logical
  deletion fences new launches and stops that exact session's tasks; guarded child deletion remains
  a separate operation. Failed deletion lifts the fence and preserves unexpected failure records.
- Committed deletion stops live trees concurrently (3 s grace), removes confirmed-clean operational
  records and diagnostics, and retains surviving/uncertain cleanup as actionable orphan records.
  An interrupted deletion marker is reconciled at runtime startup, without deleting application data.

## Lifecycle and acknowledgement

- Intentional stops are removed after the whole managed tree is confirmed gone, regardless of shell
  exit code or signal. Natural success (known exit code 0) is removed after whole-tree confirmation.
- Unexpected failures (`exited`, `failed_to_start`) remain actionable until explicit dismissal. An
  adopted task that disappears has unknown exit status and is retained, never guessed successful.
- `cleanup_failed`, `unknown`, and trees with live descendants are not terminal service failures.
  They remain actionable and cannot be dismissed. Running means alive, not ready; endpoint TCP
  probes report accepting/refused/unknown from the execution machine, not application readiness.
- Public `DismissBackgroundTask` and SDK `dismiss_background_task` acknowledge an exact task ID.
  The real `background_tasks` tool exposes `dismiss` for its authenticated owner/run. Neither path
  stops, restarts or resumes processes. Authorized acknowledgement needs no second confirmation.
- Dismissal serializes with stop/deletion/monitor transitions and rechecks the process table. Live,
  surviving, cleanup-failed, unknown, missing-identity or unreachable state refuses acknowledgement.
  Minimal durable ID/owner/run receipts make retries idempotent, including after restart, without
  retaining command text, logs or operational task records. Arbitrary missing IDs are not receipts.

## API and projections

`ListBackgroundTasks` returns the complete live/actionable inventory (optionally filtered by owner),
without a recent-terminal-history cap. `GetBackgroundTask` returns status, bounded log tail and
endpoint probes. `StopBackgroundTask` targets an exact task; its receipt can remain valid after the
operational record has already been removed. Clients must replace inventory snapshots, including
removals, rather than union terminal rows back into state. Normal service selectors use live states
only; attention views retain failures and uncertain cleanup. An unreachable machine is unknown,
not a successful empty inventory.

## Identity, supervision and restart

Tasks lead their own process group. Identity combines leader PID and OS start time. A reused leader
PID is never signalled; known descendants retain their own PID/start-time identities independently.
The launch gate persists identity before releasing the command. Startup reconciles existing terminal
records as well as active ones, removing stale confirmed successes/stops and retaining failures.

Process observations persist escaped descendants so later reparenting and restart do not discard
already-observed members. Stop sends SIGTERM, waits the grace period, then SIGKILL and waits 3 s;
surviving or unverified trees remain `cleanup_failed`. Process-list failures never prove absence.

## Diagnostics retention

- Active/uncertain tasks keep two rotated log generations: rotation checks every 2 s above 5 MiB,
  approximately 10 MiB per task with transient writer overshoot. Inspection returns at most 64 KiB.
- Successful removal, intentional confirmed stop, dismissal and confirmed owner-deletion cleanup
  immediately unlink task-owned `.log`/`.log.1` files. A monitor sweep retries crash-interrupted
  unlinking using durable receipts.
- Terminal actionable failures retain diagnostics for at most seven days after their terminal
  timestamp (plus the 2 s sweep interval); their actionable records do not expire. No archive UI.
- Only generated task filenames in the task log directory are eligible for removal. Persisted cwd,
  command and log-path text never authorize deleting application files, databases or volumes.
- Minimal acknowledgement receipts persist without diagnostic payloads for exact-ID idempotency.

## Platform limits

Unix only. Containment is process groups plus observed parent links: a process that completely
escapes before it can be observed (double fork or launchd/systemd hand-off) cannot be attributed.
Adopted processes have no collectable exit status. OS start-time precision is one second. Rotation
uses copy/truncate and can lose a few concurrent log lines. No scheduling, restart or resume product.
