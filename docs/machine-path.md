# Machine PATH

Nakode owns machine-local PATH configuration, resolution and the runtime overlay. FStack's machine settings are an authenticated renderer/relay over `nakode.v1.MachinePathService`; no client reads Nakode files or resolves a shell command locally.

## Settings and lifetime

- `GetMachinePath` reads saved command and current machine baseline without execution.
- `SaveMachinePath` preserves command bytes. It does not resolve. Saving an empty command disables the overlay immediately for future launches.
- `SyncMachinePath` executes the saved command on that service, as its user, and applies a successful result to subsequent launches. Failure returns state with an error and keeps the previous effective PATH.
- Startup resolves once before preparing providers/tools or exposing service listeners. A failed startup resolution uses the last known-good value, or the inherited PATH when none exists.
- Configuration, revision, last-good PATH, the exact command that produced it, and its Unix-millisecond resolution timestamp are atomically persisted in `$NAKODE_HOME/machine-path.json` (default `~/.nakode`). Corrupt/unreadable settings retain the inherited environment and refuse writes rather than overwriting recovery data.
- Saved configuration and runtime state are distinct. A newly saved command does not invalidate a usable older resolved value; `resolved_command` and `source` identify that fallback honestly.
- Existing shells, evaluator kernels, MCP memory subprocesses and provider adapters retain their launch environment. Sync does not restart them.

## Precedence and launches

Lowest to highest: inherited process PATH, machine overlay, session Environment PATH, explicit tool `env.PATH`. Session Environment remains memory-only/write-only and is never included in the machine PATH response. The display is the machine baseline, not a reveal of account credentials or session overrides.

Native Bash, PTY Bash, delegated Bash (through the logical owner session), owner RunShell, central subprocess tools, new evaluator kernels, memory processes, optional harness adapters, execution probes and self-invoked workers receive the overlay. No `std::env::set_var` is used. Each launch reads the current overlay.

Unconfigured services retain their existing shell startup behavior. When a PATH override is present, agent/owner command shells use absolute `/bin/sh -c`, not login mode: login startup files must not overwrite the explicitly supplied PATH. Explicit command content can of course change its own environment.

FStack-owned interactive terminals and FStack Git/project operations are separate processes and retain their existing Environment/startup contract; this feature configures Nakode's execution service, not the operating system or all FStack Host processes.

## Shell and safety limits

The command is intentional arbitrary execution, available only at the existing owner-local socket or bearer-authenticated remote service boundary. FStack additionally authorizes the exact account-owned target machine, with no default/client-host fallback. Reads never execute commands; saves never implicitly Sync.

The resolver invokes absolute `/bin/sh -c` using the service's original inherited PATH, not the PATH being resolved. It runs in the service user's home. An absolute path to a desired shell avoids bootstrap lookup failures. `zsh -c 'echo $PATH'` is only an example; zsh need not be installed. Noninteractive and login shells load different startup files (`zsh -lc` differs from `zsh -c`), and interactive dotfiles are not necessarily read by either.

Resolution is bounded to 10 seconds across process exit **and** output collection, with 64 KiB each for stdout/stderr. Output must be UTF-8, nonempty, one line with no NUL/CR/LF; one final LF or CRLF is removed. PATH separators, spaces and empty components are preserved. Errors and oversized output never replace the last-good value. Unix process-group cleanup covers ordinary descendants and inherited output pipes on timeout/cancellation; intentionally detached processes are not a sandbox and an authorized command can have side effects.

Mutations use expected configuration revision and bounded per-service idempotency receipts (128 keys). Receipts are not a durable exactly-once arbitrary-execution guarantee across service restarts. Refresh after ambiguous failure; do not automatically repeat Sync with a new key.

## Platform validation

The implementation currently supports Unix services and uses `/bin/sh`, Unix sockets and process groups. Linux tests cover isolated persistence, fallback, timeout, output bounds and ordinary/PTY shell propagation. macOS requires platform validation; Windows command resolution is explicitly unsupported. No real service restart or owner PATH reconfiguration is part of verification.
