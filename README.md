# Nako Agent

Nako Agent is evolving into a provider-neutral agent orchestration and continuity
layer. The current experimental TUI can supervise either the locally installed,
authenticated OpenAI Codex CLI or Devin CLI while the shared session, backend,
memory, skill, and orchestration boundaries are developed.

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
- **Codex app-server** — native Codex adapter using newline-delimited JSON-RPC
- **Devin ACP** — native Devin CLI adapter using ACP v1 JSON-RPC over stdio

Backend contracts, provider adapters, app reducer, session store, and renderer
are separate modules. Provider wire values are normalized before they reach UI
state. Capability snapshots control resume, steering, interruption, models, and
other optional behavior.

## Architecture direction

Agents own execution, native tools, approvals, authentication, and provider
context. Nako Agent owns logical work identity, coordination, handoffs, shared
skills, memory, artifacts, and provenance.

A logical Nako Agent session will contain multiple provider-native agent sessions.
Cross-agent continuation will use explicit handoff packages rather than claim
that private model context can be translated between providers. Nako Agent does not
provide coding tools; each backend retains its native tool and approval model.

## Requirements

- Rust 1.88 or newer
- At least one supported provider installed and authenticated:
  - `codex` for the Codex provider
  - `devin` for the Devin provider
- A real interactive terminal

This build has been exercised against `codex-cli 0.144.5`. The app-server API
is experimental, so regenerate and compare schemas after Codex upgrades:

```sh
codex app-server generate-json-schema --experimental --out /tmp/codex-schema
codex app-server generate-ts --experimental --out /tmp/codex-ts
```

## Run

```sh
# Development build
./dev.sh --workspace /path/to/project

# Start every enabled provider
cargo run --release -- --workspace /path/to/project
```

Options:

```text
--codex <PATH>       Codex executable (or NAKO_AGENT_CODEX)
--devin <PATH>       Devin executable (or NAKO_AGENT_DEVIN)
--workspace <PATH>   Working directory (or NAKO_AGENT_WORKSPACE)
--model <MODEL>      Initial provider/model (or NAKO_AGENT_MODEL)
--resume <ID>        Resume a Nako Agent session by ID or prefix (or NAKO_AGENT_RESUME)
--scrollback <N>     Logical transcript entry limit (or NAKO_AGENT_SCROLLBACK)
--agents <PATH>      Agent-definition directory (or NAKO_AGENT_AGENTS)
```

Nako Agent starts every enabled provider and uses each provider's existing
authentication without storing credentials. Models are referenced uniformly as
`provider-slug/model-slug`, such as `openai-codex/gpt-5`; F2 searches this
unified catalog, and model selection routes new work to that provider. Devin
model choices come from each ACP session's model-category `configOptions`;
Nako Agent caches the discovered list in SQLite and applies changes through
`session/set_config_option`. ACP does not define turn steering.

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
| `/providers` | Open the provider registry and enable or disable adapters |
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

- Initializes every enabled provider and records each declared capability set.
- Persists the provider registry; Codex and Devin are enabled by default and
  can be toggled from `/providers`.
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

Markdown rendering is intentionally lightweight today. Headings, code blocks,
and diffs receive semantic styling. Rich Markdown, additional backend adapters,
configurable keymaps, and embedded provider/process panes are later work.

## Provider-owned tools

Nako Agent does not advertise or execute general-purpose coding tools. The Codex
and Devin adapters start and resume native provider sessions without a
Nako Agent-owned tool registry or a restricted lowest-common-denominator
environment. Native tools, sandboxing, permissions, and approvals remain the
backend's responsibility.

Nako Agent may later expose narrowly scoped control-plane operations for memory,
artifacts, skills, and orchestration. Those operations are not replacements for
provider coding tools.

## Predefined agents

Place TOML definitions in `.nako-agent/agents/` under the workspace, or select a
different directory with `--agents`. Each filename may be chosen freely; the
slug is the stable agent identity:

```toml
slug = "explorer"
description = "Gathers relevant context and returns a concise, detailed report"
system_prompt = "Investigate without modifying files. Report evidence and uncertainty."
first_message = "Explore the delegated question and report the context the parent needs."
model = "openai-codex/gpt-5" # optional; otherwise use the parent's provider
fallback_models = ["devin-acp/swe-1-7-lightning"] # optional, tried in order
```

Nako Agent includes one provider-neutral default: the read-only `explorer` gathers
relevant context and returns a concise, detailed report to its parent agent. A
workspace `explorer` definition overrides the built-in definition. Additional
slugs extend the catalog only when the workspace defines them explicitly.

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
transcript, and result channel. The built-in explorer uses
`devin-acp/swe-1-7-lightning` by default and falls back to
`openai-codex/gpt-5.6-luna` if Devin cannot launch or create the native child
session. A workspace agent definition may override the ordered model
candidates. This
socket protocol is deliberately independent of the TUI; the TUI currently
hosts the service, while a long-lived Nako Agent daemon can take ownership of the
same protocol as orchestration and multi-client continuity mature.

All provider sessions run unattended. Codex uses `approvalPolicy: never` with
a `danger-full-access` sandbox, while Devin launches ACP with
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
├── codex app-server <─ JSONL stdio ─> Codex adapter
└──     devin acp   <─ ACP stdio ───> Devin adapter
provider processes <─ PTY I/O ───> dedicated portable-pty boundary
```

The target relationship is:

```text
Nako Agent control plane
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
