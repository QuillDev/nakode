# Remote machine self-update

Nakode owns the remote update command and lifecycle. FStack stores the enrolled machine identity and
renders Nakode's replacement snapshots; it never sends a shell command, executable path, environment,
or installer arguments.

## Public boundary

`nakode.v1.RemoteUpdateService` is available only on the authenticated TLS remote listener. The
request carries one idempotency key, the expected installation `server_id`, and the build revision
shown at confirmation. The server returns typed outcomes for accepted, replayed, already-running,
unsupported, and stale-target requests.

`RemoteUpdateStatus` is durable under `$NAKODE_HOME/remote-update/`. On first startup the authority
runs the fixed `fstack update --check --json` seam to report whether the installed Nakode revision is
current and, when available, the concrete target revision. Attempt-qualified revisions order
replacement snapshots. Error text is bounded and credential-free. Detailed updater output is a
private `0600` machine-local log and does not cross the API.

## Restart handoff

A supported server launches its fixed internal helper in a transient systemd user unit. The caller
cannot alter this command. The unit is outside `fstack-executor.service`, so updating Nakode and
restarting that service does not kill the updater that owns the handoff. The helper invokes only the
installed `fstack update`, observes its typed progress markers, and writes the durable status. The
normal FStack installer and remote-executor provisioner remain responsible for preserving install
prefix, install mode, `NAKODE_HOME`, remote identity/credentials/listener configuration, and systemd
unit ownership.

Repeated requests with one key replay one attempt. A different request while an attempt is active is
coalesced onto that attempt. A server-id or build-revision mismatch fails before mutation. On service
startup, an active snapshot with no progress for one hour becomes a typed, retryable
`update_interrupted` failure so a killed helper or machine reboot cannot block future updates forever.

## Supported matrix

| Platform / install mode | State | Reason |
| --- | --- | --- |
| Linux, managed headless install, systemd user manager | Supported | Restart-safe detached handoff and existing remote-executor supervision are available. |
| Linux headed/source-only install | Unsupported | The local owner UI remains the update authority. |
| macOS headed install | Unsupported remotely | The packaged dashboard's local update flow owns app replacement and relaunch. |
| Alpine or non-systemd headless Linux | Unsupported | Current packaging does not provide a safe supervised self-replacement path. |
| Windows | Unsupported | The repositories do not currently claim a managed Windows installer/service mode. |

## Integrated supervisor lifecycle

This service uses the supervisor-owned Nakode update contract: the canonical install prefix is
explicit in the systemd unit and helper environment, activation is
suppressed during replacement, and the final FStack remote bootstrap owns the single restart. The
FStack installer additionally requires a canonical-root and checkout-revision receipt before it
skips rebuilding Nakode. This repairs legacy-updater transitions that otherwise installed a second
binary under `~/.local` and never refreshes or prints enrollment credentials during routine repair.

Packaging follow-up: add equivalent durable launchers before advertising non-systemd Linux or another
service manager, and add a published-artifact updater before installations without managed source
checkouts can report support.
