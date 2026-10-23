# Managed background tasks

Long-running non-AI processes (dev servers, workers, watchers) that agents start through the
`background_tasks` tool and owner integrations inspect through the public API. Finite work such as
installs and builds stays with `bash`. Implementation: `src/background_tasks/`.

## Ownership and lifetime

- The Nakode server process spawns every task. Providers, the Claude bridge and tool shells are
  never the parent, so a task survives the tool call, the turn, a closed session and a provider
  restart.
- The owner is the calling session's authenticated logical owner (`ToolContext.session` for
  in-process providers, the bridge's `DelegationScope` for Claude). Tool arguments cannot name an
  owner. Without a logical owner the tool is absent (Claude) or refuses (in-process).
- A native delegated run sees and stops only the tasks it started; the owning session sees all of
  its tasks. Agents never see other owners' tasks.
- Deleting the owning logical session fences new launches and marks its tasks before the durable
  deletion. A failed deletion clears the marks and leaves the tasks running. After the deletion
  commits, the tasks are stopped concurrently in the background (3 s grace) so deletion never waits
  on process shutdown. Tasks that could not be proven stopped stay `cleanup_failed` orphan records
  (`owner_deleted_at_ms`); deletion is not blocked by them, because a process that ignores SIGKILL
  or escaped containment would otherwise make the session undeletable.
- If the runtime stops between marking and cleanup, restart reconciliation stops every marked task.
  This includes the rare case where the runtime stopped before a deletion committed, so the session
  survives without those tasks.
- Closing or archiving a session does not stop its tasks.

## Public API

Capability `BackgroundTasks`. `ListBackgroundTasks` (optional owner filter), `GetBackgroundTask`
(status, log tail, endpoint probes) and `StopBackgroundTask` (exact task id). This surface is for
the authenticated owner integration: it sees every task on this machine, including orphans, and can
stop one without its original owner. Lists return every unended task plus the 50 most recent ended
ones. Each runtime reports only its own machine; aggregating machines is the client's job.

## States

`launching`, `running`, `stopping`, `exited`, `stopped`, `failed_to_start`, `cleanup_failed`,
`unknown`. Only `exited`, `stopped` and `failed_to_start` are terminal. `running` means the process
tree is alive, not that a service is ready; endpoint probes only report whether a TCP connect on
the execution machine succeeded. `localhost` endpoints are on the execution machine.

## Identity, launch and restart

- Each task leads its own process group. Its identity is the group id plus the leader's `ps`
  start time. A live leader with another start time proves the id was reused, so nothing is
  signalled. With the leader gone, remaining group members are treated as survivors.
- Launch is gated: the leader blocks reading stdin until the runtime has persisted its pid and
  start time, and exits 125 without running the command if stdin closes first. An interrupted
  launch is therefore either recorded and adoptable or provably never ran.
- On startup the runtime reconciles every unended record: live tasks are adopted, finished ones are
  settled, interrupted stops resume with their original reason, and a failed process listing marks
  tasks `unknown` rather than forgetting them. Records are never deleted.
- Stop sends SIGTERM to the group and to descendants that left it, waits the grace period, sends
  SIGKILL, and reports `cleanup_failed` if anything is still alive 3 s later. Descendants seen during
  a stop stay tracked by pid and start time, so one orphaned by the first signal is still killed.
- A launch whose identity cannot be read from `ps` closes the gate and fails without running.
- If processes cannot be listed, running tasks become `unknown` and are re-verified later; startup
  reconciliation is retried until it succeeds.

## Logs

stdout and stderr append to `<data dir>/background-tasks/<task id>.log`. Every 2 s, and when a task
ends, a log above 5 MiB is rotated to `.log.1` by copy and truncate, so a few lines written during
rotation can be lost and a very fast writer can briefly exceed the cap. Inspection reads at most
64 KiB, spanning the archive when needed. Logs are kept after a task
ends.

## Limitations

- Unix only. Windows reports the capability as unavailable.
- Containment is the process group plus parent links. A process that double-forks, reparents to
  init, or hands itself to launchd or systemd cannot be attributed and is not stopped.
- Adopted tasks (after a runtime restart) have no exit code or signal, because only the original
  parent can collect them.
- `ps` start times have one-second granularity; a reused pid inside the same second as the original
  leader's start is not distinguishable.
- No scheduling, dependencies or automatic restart.
