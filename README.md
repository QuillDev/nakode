# Nako Agent

Nako Agent is evolving into a provider-neutral agent orchestration and continuity
layer. The experimental TUI now runs its portable agent loop and coding tools
in-process, with direct authenticated transports for OpenAI Codex and Devin.

The binding product direction is recorded in [`AGENTS.md`](AGENTS.md). Codex is
the first adapter, not Nako Agent's application model.

## Stack

- **Ratatui** — immediate-mode rendering and custom cell layout
- **Crossterm** — terminal input, raw mode, alternate screen, mouse, and
  bracketed paste
- **Tokio + bounded channels** — input/backend orchestration, streaming,
  cancellation, and redraw pacing
- **portable-pty** — isolated boundary for interactive provider/process panes
- **SQLite** — Nako Agent-owned, cross-provider session metadata and discovery
- **Direct OpenAI transport** — device authentication, model discovery, and
  streamed Responses events
- **Direct Devin transport** — PKCE authentication, protobuf/Connect model
  discovery, and streamed chat events

Backend contracts, provider adapters, app reducer, session store, and renderer
are separate modules. Provider wire values are normalized before they reach UI
state. Capability snapshots control resume, steering, interruption, models, and
other optional behavior.

## Architecture direction

Nako Agent's target distribution is one self-contained executable. Its portable
runtime owns the local agent loop, tools, approvals, and coordination;
in-process provider adapters own authentication, inference transport, model
discovery, and provider response state. External harness adapters are optional
compatibility paths rather than required provider implementations.

A logical Nako Agent session will contain multiple provider-native agent sessions.
Cross-agent continuation will use explicit handoff packages rather than claim
that private model context can be translated between providers.

## Requirements

- A real interactive terminal

No provider harness or language runtime must be installed separately. The
optional `eval` tool reports a clear error when its requested language runtime
is not installed. Legacy Codex app-server and Devin ACP adapters remain
isolated compatibility code for fixture coverage; normal startup and provider
setup do not invoke them.

## Run

```sh
# Development build
./dev.sh --workspace /path/to/project

# Development build with isolated, disposable Nako Agent state
./dev.sh --clean --workspace /path/to/project

# Start every enabled provider
cargo run --release -- --workspace /path/to/project
```

Options:

```text
--workspace <PATH>   Working directory (or NAKO_AGENT_WORKSPACE)
--model <MODEL>      Initial provider/model (or NAKO_AGENT_MODEL)
--resume <ID>        Resume a Nako Agent session by ID or prefix (or NAKO_AGENT_RESUME)
--scrollback <N>     Logical transcript entry limit (or NAKO_AGENT_SCROLLBACK)
--agents <PATH>      Agent-definition directory (or NAKO_AGENT_AGENTS)
```

Every interactive `dev.sh` run gracefully stops a development instance already
serving the same workspace before starting its replacement. Adding `--clean`
gives that replacement a fresh provider registry, session database, and agent
catalog without deleting the normal development installation. The isolated
state, including its provider credential stores, is removed when the process
exits. Run the script as your desktop user, not through `sudo`, so browser
integration and user credentials remain available.

Providers are configured explicitly from `/providers`. Codex setup uses its
device-code flow and refreshes OAuth tokens directly. Devin setup uses its
browser-based PKCE flow with a localhost callback. Credentials and native
session state are stored in Nako Agent's user-private SQLite database; they are
never exported to child provider processes. Models are referenced uniformly as
`provider-slug/model-slug`, such as `openai-codex/gpt-5`; F2 searches this
unified catalog, and model selection routes new work to that provider. Both
model catalogs are discovered through their authenticated native transports
and cached in SQLite. Direct in-process turns currently support interruption;
turn steering remains explicitly unsupported.

## Controls

| Key | Action |
| --- | --- |
| `Enter` / `Ctrl+Enter` | Send while idle; queue while a turn is active |
| `Shift+Enter` / `Alt+Enter` / `Ctrl+J` | Insert a newline |
| `Ctrl+Q` | Explicitly queue the draft |
| `Ctrl+S` | Steer the active turn; the draft clears only after acceptance |
| `Ctrl+C` | Interrupt the active turn and all subagents; press again while cancelling to exit |
| `F1` | Open or close the key reference |
| `F2` | Open the model picker; changes apply to the next turn |
| `/resume` | Open the recent-session picker for this workspace |
| `/resume ID` | Resume a saved session by Nako Agent ID or unique prefix |
| `/new` | Unsubscribe from the current backend session and start fresh |
| `/models` | Choose and persist a provider's default model for future sessions |
| `/switch` | Switch the model for only the current session |
| `/providers` | Open the provider registry and enable or disable adapters |
| `/agents` | View, create, edit, or delete delegated agent archetypes |
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

After a persisted session exits, Nako Agent prints a ready-to-run command
containing the workspace and logical session ID so the session can be resumed
from any directory.

## Current behavior

- Initializes every enabled provider and records each declared capability set.
- Persists the provider registry. A clean installation enables nothing; the
  user explicitly starts each provider from `/providers`.
- Loads and caches provider-qualified model catalogs from enabled providers.
- Discovers provider model catalogs at startup; providers that require a native
  session for discovery create one through their native adapter.
- Refreshes backend metadata and model caches with `/reload`.
- Runs sequential turns in one active provider session at a time.
- Stores provider-neutral session metadata in SQLite and supports `--resume`,
  `/resume`, `/resume ID`, and `/new`; each provider remains authoritative for
  model context and reconstructed transcript history.
- Streams assistant, reasoning, plan, command, MCP, file-change, and tool items
  by protocol item ID.
- Leaves coding tools, execution policy, and approvals to the provider selected
  by each model/session.
- Keeps queued prompts in app-owned FIFO state.
- Keeps steering separate from queueing and handles completion races
  deterministically.
- Supports immediate turn interruption; provider approval requests are accepted by the adapter.
- Highlights mouse-drag selections and copies them automatically to the local
  terminal clipboard.
- Processes every protocol delta through a bounded channel while coalescing
  terminal redraws to roughly 30 FPS.
- Restores raw mode, alternate screen, cursor, mouse capture, colors, and
  bracketed paste on normal exit, panic, `SIGINT`, `SIGTERM`, and `SIGHUP`.

Assistant responses render GitHub-flavored Markdown in the transcript, including
headings, emphasis, links, block quotes, ordered and unordered lists, task
checkboxes, tables, inline code, and fenced code blocks. Fenced blocks use their
language tag for syntax highlighting. Tool output and diffs retain their
provider-neutral semantic coloring.

## Portable tools

The in-process runtime registers the same dynamic function-tool schemas with
both direct providers. Its base set is `read`, `write`, `edit`, `bash`, `glob`,
`grep`, `eval`, `ask`, and `todo`; `task` and `hub` are intentionally excluded.
Local paths follow shell conventions relative to the workspace, process tools
are bounded and cancellable, `ask` supports structured related questions in the
TUI, eval kernels retain per-language state, and phased todos persist with the
provider session. Optional compatibility adapters continue to use their
provider-owned tool and permission semantics.

## Predefined agents

Use `/agents` to manage definitions, or place TOML files in
`.nako-agent/agents/` under the workspace (select another directory with
`--agents`). Each filename may be chosen freely; the
slug is the stable agent identity:

```toml
slug = "explorer"
description = "Gathers relevant context and returns a concise, detailed report"
system_prompt = "Investigate without modifying files. Report evidence and uncertainty."
first_message = "Explore the delegated question and report the context the parent needs."
model = "openai-codex/gpt-5" # optional; otherwise use the parent's provider
fallback_models = ["devin-acp/swe-1-7-lightning"] # optional, tried in order
```

Nako Agent ships `config/default-agents.toml` as its initial preset catalog. Agent
identities, prompts, primary models, and fallback models are loaded from that
configuration rather than constructed in Rust. Once the workspace agent
directory exists it becomes authoritative, so deleting a preset remains
deleted after restart.

Nako Agent adds a `[Nako Agent System Instructions]` block to new native sessions. It
identifies the logical Nako Agent session, active provider and model, lists the
configured agents, and explains the provider-neutral invocation command:

```text
nako-agent agent explorer --session-id=<nako-session-id> --task='Map the authentication flow'
```

The command connects to the workspace control service over
`.nako-agent/control.sock` and blocks until the agent finishes. The service validates
the logical session, launches a separate provider-native child session, and
allows up to four read-only explorer children to run concurrently. Each child
has an independently bounded objective, native provider session, lifecycle,
transcript, and result channel. The preset explorer configuration uses
`devin-acp/swe-1-7-lightning` by default and falls back to
`openai-codex/gpt-5.6-luna` if Devin cannot launch or create the native child
session. A workspace agent definition may override the ordered model
candidates. This
socket protocol is deliberately independent of the TUI; the TUI currently
hosts the service, while a long-lived Nako Agent daemon can take ownership of the
same protocol as orchestration and multi-client continuity mature.

All provider sessions run unattended. Codex uses `approvalPolicy: never` with
a `danger-full-access` sandbox, while Devin launches its native ACP server with
`--permission-mode dangerous`. Unexpected permission requests are accepted
inside the provider adapter rather than interrupting the TUI. The parent chat
shows a compact inline status and truncated objective where each child is
delegated without copying its raw result into the transcript. Click an inline
child row to open its live session chat; that modal uses the same streaming
transcript projection as the main chat.
Nako Agent persists each orchestration run and its ordered child transcript in
SQLite, so reopening the logical parent session restores the same inspectable
sub-agent rows and chats.
Completion is returned to the invoking parent process as:

```text
[Subagent Result] [nako-agent-000001] [explorer]
...agent response...
```

Because invocation uses the Nako Agent CLI rather than a provider-specific dynamic
tool, any parent model with native shell access can request an agent. An agent
may target another enabled provider by setting its optional provider-qualified
`model`.

## Current implementation architecture

```text
CLI clients ─> workspace Unix socket ─┐
Crossterm events ─────────────────├─> single AppState reducer ─> Ratatui projection
provider-tagged events ──────────────┘            │
                                                   └─> provider-routed commands

enabled-provider registry
├── OpenAI Codex <─ HTTPS/SSE ───────> direct adapter
└── Devin        <─ HTTPS/Connect ──> direct adapter
                         │
                         └─> shared local agent loop and supervised tools
```

The target relationship is:

```text
Nako Agent control plane
├── logical sessions, tasks, runs, handoffs, artifacts, memory
├── portable skills materialized through backend adapters
└── provider adapters
    ├── Codex inference transport and provider context
    ├── Devin inference transport and provider context
    └── future native agent sessions
```

Important files:

- `src/backend.rs` — provider-neutral commands, events, capabilities, and handle
- `src/controls.rs` — authoritative keyboard, mouse, slash-command, and help registry
- `src/runtime.rs` — portable agent loop, tool contracts, and native session state
- `src/tools/` — portable tool registry and supervised base-tool implementations
- `src/codex/native.rs` — direct Codex OAuth, discovery, and Responses transport
- `src/codex/client.rs` — optional Codex app-server compatibility adapter
- `src/codex/protocol.rs` — schema-tolerant Codex normalization
- `src/devin/native.rs` — direct Devin protobuf/Connect transport
- `src/devin/client.rs` — optional Devin ACP compatibility adapter and OAuth flow
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
