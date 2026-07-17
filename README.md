# Flock

Flock is evolving into a provider-neutral agent orchestration and continuity
layer. The current experimental TUI can supervise either the locally installed,
authenticated OpenAI Codex CLI or Devin CLI while the shared session, backend,
memory, skill, and orchestration boundaries are developed.

The binding product direction is recorded in [`AGENTS.md`](AGENTS.md). Codex is
the first adapter, not Flock's application model.

## Stack

- **Ratatui** — immediate-mode rendering and custom cell layout
- **Crossterm** — terminal input, raw mode, alternate screen, mouse, and
  bracketed paste
- **Tokio + bounded channels** — input/backend orchestration, streaming,
  cancellation, and redraw pacing
- **portable-pty** — isolated boundary for interactive provider/process panes
- **SQLite** — Flock-owned, cross-provider session metadata and discovery
- **Codex app-server** — native Codex adapter using newline-delimited JSON-RPC
- **Devin ACP** — native Devin CLI adapter using ACP v1 JSON-RPC over stdio

Backend contracts, provider adapters, app reducer, session store, and renderer
are separate modules. Provider wire values are normalized before they reach UI
state. Capability snapshots control resume, steering, interruption, models, and
other optional behavior.

## Architecture direction

Agents own execution, native tools, approvals, authentication, and provider
context. Flock owns logical work identity, coordination, handoffs, shared
skills, memory, artifacts, and provenance.

A logical Flock session will contain multiple provider-native agent sessions.
Cross-agent continuation will use explicit handoff packages rather than claim
that private model context can be translated between providers. Flock does not
provide coding tools; each backend retains its native tool and approval model.

## Requirements

- Rust 1.88 or newer
- At least one supported backend installed and authenticated:
  - `codex` for the default Codex backend
  - `devin` for `--backend devin`
- A real interactive terminal

This build has been exercised against `codex-cli 0.144.5`. The app-server API
is experimental, so regenerate and compare schemas after Codex upgrades:

```sh
codex app-server generate-json-schema --experimental --out /tmp/codex-schema
codex app-server generate-ts --experimental --out /tmp/codex-ts
```

## Run

```sh
# Codex (default)
cargo run --release -- --workspace /path/to/project

# Devin through `devin acp`
cargo run --release -- --backend devin --workspace /path/to/project
```

Options:

```text
--backend <BACKEND>  codex or devin (or FLOCK_BACKEND; default: codex)
--codex <PATH>       Codex executable (or FLOCK_CODEX)
--devin <PATH>       Devin executable (or FLOCK_DEVIN)
--workspace <PATH>   Working directory (or FLOCK_WORKSPACE)
--model <MODEL>      Initial model when supported (or FLOCK_MODEL)
--resume <ID>        Resume a Flock session by ID or unique prefix (or FLOCK_RESUME)
--scrollback <N>     Logical transcript entry limit (or FLOCK_SCROLLBACK)
```

Flock uses each provider's existing authentication and does not store
credentials. Devin model choices come from each ACP session's model-category
`configOptions`; Flock caches the discovered list in SQLite and applies changes
through `session/set_config_option`. ACP does not define turn steering.

## Controls

| Key | Action |
| --- | --- |
| `Enter` / `Ctrl+Enter` | Send while idle; queue while a turn is active |
| `Shift+Enter` / `Alt+Enter` / `Ctrl+J` | Insert a newline |
| `Ctrl+Q` | Explicitly queue the draft |
| `Ctrl+S` | Steer the active turn; the draft clears only after acceptance |
| `Ctrl+C` | Interrupt the active turn; press again while cancelling to exit |
| `F1` | Open or close the key reference |
| `F2` | Open the model picker; changes apply to the next turn |
| `/resume` | Open the recent-session picker for this workspace |
| `/resume ID` | Resume a saved session by Flock ID or unique prefix |
| `/new` | Unsubscribe from the current backend session and start fresh |
| `/reload` | Refresh backend metadata and model choices; updates the cache |
| `PageUp` / `PageDown` | Scroll transcript |
| `Ctrl+L` | Jump to latest output |
| `Alt+Up` / `Alt+Down` | Select a queued message |
| `Alt+Delete` | Remove the selected queued message |
| `Ctrl+D` | Exit when no turn is active |
| Mouse drag | Select rendered text and automatically copy it |
| `y` / `a` / `n` | Accept once / accept for session / decline an approval |

Mouse selections are copied through the terminal's OSC 52 clipboard protocol,
including tmux passthrough. The terminal must permit clipboard writes. Pasted
control sequences are treated as text, and streamed tool output is rendered as
sanitized data rather than executed terminal control.

## Current behavior

- Initializes the selected backend and records its declared capabilities.
- Loads and caches Codex model catalogs and Devin ACP session model options.
- Lazily creates a Devin session when F2 needs an uncached model list.
- Refreshes backend metadata and model caches with `/reload`.
- Runs sequential turns in one active provider session at a time.
- Stores provider-neutral session metadata in SQLite and supports `--resume`,
  `/resume`, `/resume ID`, and `/new`; each provider remains authoritative for
  model context and reconstructed transcript history.
- Streams assistant, reasoning, plan, command, MCP, file-change, and tool items
  by protocol item ID.
- Leaves coding tools, execution policy, and approvals to the selected backend.
- Keeps queued prompts in app-owned FIFO state.
- Keeps steering separate from queueing and handles completion races
  deterministically.
- Supports turn interruption and visible approval requests.
- Highlights mouse-drag selections and copies them automatically to the local
  terminal clipboard.
- Processes every protocol delta through a bounded channel while coalescing
  terminal redraws to roughly 30 FPS.
- Restores raw mode, alternate screen, cursor, mouse capture, colors, and
  bracketed paste on normal exit, panic, `SIGINT`, `SIGTERM`, and `SIGHUP`.

Markdown rendering is intentionally lightweight today. Headings, code blocks,
and diffs receive semantic styling. Rich Markdown, additional backend adapters,
configurable keymaps, and embedded provider/process panes are later work.

## Provider-owned tools

Flock does not advertise or execute general-purpose coding tools. The Codex
and Devin adapters start and resume native provider sessions without a
Flock-owned tool registry or a restricted lowest-common-denominator
environment. Native tools, sandboxing, permissions, and approvals remain the
backend's responsibility.

Flock may later expose narrowly scoped control-plane operations for memory,
artifacts, skills, and orchestration. Those operations are not replacements for
provider coding tools.

## Current implementation architecture

```text
Crossterm events ─┐
                  ├─> single AppState reducer ─> Ratatui projection
Backend events ───┘            │
                               └─> provider-neutral commands

codex app-server <─ JSONL stdio ─> Codex adapter
    devin acp     <─ ACP stdio ───> Devin adapter
provider processes <─ PTY I/O ───> dedicated portable-pty boundary
```

The target relationship is:

```text
Flock control plane
├── logical sessions, tasks, runs, handoffs, artifacts, memory
├── portable skills materialized through backend adapters
└── provider adapters
    ├── Codex native session, context, tools, and approvals
    ├── Devin native session, context, tools, and permissions
    └── future native agent sessions
```

Important files:

- `src/backend.rs` — provider-neutral commands, events, capabilities, and handle
- `src/codex/client.rs` — Codex child supervision and JSON-RPC correlation
- `src/codex/protocol.rs` — schema-tolerant Codex normalization
- `src/devin/client.rs` — Devin ACP child supervision and normalization
- `src/state.rs` — queue, steer, cancel, model, approval, and transcript
  transitions
- `src/transcript.rs` — logical entries, projection cache, wrapping, and
  viewport slicing
- `src/editor.rs` — app-owned multiline Unicode editor
- `src/render.rs` — Ratatui layout and overlays
- `src/terminal.rs` — terminal lifecycle and restoration
- `src/pty.rs` — interactive subprocess boundary

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

The test suite includes reducer race tests, exact protocol fixtures, a fake
JSONL app-server, Ratatui `TestBackend` rendering, native PTY lifecycle, and a
real PTY terminal-restoration smoke test.
