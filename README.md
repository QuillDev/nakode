# Nakode

Nakode is a provider-neutral agent server for orchestration, continuity, and
execution. It owns the workspace runtime and canonical session state while
replaceable frontends provide the interface. The included TUI is one thin
client, not the application authority.

Nakode is experimental and under active development.

Developers and coding agents can drive the real TUI controls and renderer through
the deterministic JSON Lines [TUI evaluation harness](docs/tui-evaluation.md).
Alternative interfaces can use the same native server through the
[generated API and SDK](docs/frontend-development.md);
the TUI is one renderer of server-owned semantic state.

## Architecture boundary

The background Nakode service manages every session, turn, queue, provider,
tool, artifact, setting, and orchestration run. It performs all operations and
mutations, persists canonical state, and continues active work without an
attached client.

`nakode` is that service. Run it directly:

```sh
nakode start            # background, returns once it accepts connections
nakode run              # foreground until Ctrl-C
nakode status           # version, process, endpoint, capabilities, log path
nakode status --json
nakode logs -f          # tail the captured service log
nakode restart
nakode stop
```

`start` and `stop` are safe to repeat: an already-running service reports that
it is running rather than starting a second one, and stopping a stopped service
succeeds. `restart` starts a stopped service as well as replacing a running one.

`--workspace <path>` selects the workspace service, defaulting to the current
directory exactly as before. Each canonical workspace has its own sockets, log,
and running process; no command reaches a workspace you did not name.

The interactive terminal client is one frontend over that service:

```sh
nakode --tui
```

Frontend transports are managed under `transport`:

```sh
nakode transport discord status
```

The earlier `nakode service <action>` spellings still work. Each prints a
deprecation notice on standard error naming its replacement, then does the work.

Every frontend—including the built-in TUI—only:

- obtains authoritative state through the generated SDK;
- maps user intent to distinct typed SDK methods; and
- renders that state while retaining only ephemeral presentation concerns such
  as drafts, focus, selection, scrolling, viewport, and device integration.

Frontends never open Nakode's database, connect directly to providers, execute
tools, reduce provider events, or decide session and queue policy. This is a
hard project boundary: a new capability must be implemented in the server,
exposed through `proto/nakode/v1/nakode.proto`, represented in the SDK, and only
then rendered by clients. See [Building a Nakode frontend](docs/frontend-development.md)
and the [SDK architecture](docs/sdk-architecture.md).

## What does it do?

Nakode brings agentic coding work into a single terminal experience:

- Run coding agents against a local workspace.
- Inspect and edit files, search code, run commands, and track task progress.
- Stream responses, reasoning, plans, tool activity, and diffs as work happens.
- Resume saved sessions and continue long-running work.
- Queue follow-up prompts, interrupt active work, and switch models.
- Delegate bounded investigations to independently tracked agents, including
  parallel research across providers.
- Carry work between providers with an explicit continuity handoff instead of
  claiming that private model context can be transferred.

The longer-term direction is a provider-neutral orchestration and continuity
layer where logical work can span multiple agents, models, and providers. That
includes richer delegation and review workflows and durable project memory. These
are product goals, not all current features.

## Supported providers

Nakode currently supports:

- **OpenAI Codex**
- **Claude**
- **Devin**
- **Cursor**
- **Kimi For Coding**
- **GLM Coding Plan (z.ai)**

Providers are disabled on a fresh installation. Start Nakode, open
`/providers`, and sign in to the providers you want to use. Press `F2` to browse
and select from their available models.

Nakode does not require the separate Codex, Devin, Kimi, or z.ai applications. Claude
uses the official Claude Agent SDK and the login managed by an installed Claude Code
CLI; install Claude Code and run `claude auth login` before connecting Claude in
`/providers`. Claude requires Node.js 18 or newer plus npm. Nakode stores only an
external-login marker, not Claude OAuth credentials; Claude Code continues to own its
configuration and keychain entries. Agent SDK activity is subject to Anthropic's current
subscription eligibility, usage limits, and third-party application policies. Cursor uses
its local TypeScript SDK and requires Node.js 22.13 or newer plus npm. Cursor, Kimi, and GLM
setup in `/providers` includes an API-key field and a link to the provider's API-key
dashboard. Kimi requires a [Kimi Coding Plan](https://www.kimi.com/code/)
API key; Moonshot Platform API keys are a separate product and are not
interchangeable. GLM requires a [z.ai GLM Coding Plan](https://z.ai/subscribe)
API key and uses the plan's dedicated Coding API endpoint; Team Plan members must
use their Team Plan key. No single provider defines Nakode's workflow or session
model.

## Installation

Nakode requires Git and Rust 1.88 or newer. Install it with this command:

```sh
mkdir -p "$HOME/.nakode" && \
  git clone https://github.com/QuillDev/nakode.git "$HOME/.nakode/src" && \
  "$HOME/.nakode/src/install.sh"
```

This keeps the managed source checkout in `~/.nakode/src` and installs the
`nakode` executable to `~/.local/bin`. If that directory is not already in your
`PATH`, the installer prints the line to add to your shell profile. Do not run
Nakode or the entire installer through `sudo`; provider sign-in uses your normal
desktop account.

Update the checkout, rebuild Nakode, and replace the installed executable with:

```sh
nakode update
```

`nakode update` runs `git pull --ff-only` in `~/.nakode/src` and then runs that
checkout's `install.sh`. `nakode --update` is supported as a convenience alias.

For local development in another checkout, `./install.sh --debug` reuses the
development build for much faster iteration, at the cost of a larger and less
optimized installed executable. Run `./install.sh --help` for system and
custom-prefix options.

### Reset every session

To return Nakode to a clean, first-run session state:

```sh
nakode purge-unsafe
```

The command prints a warning and then asks for confirmation with a
default-negative `[N/y]` prompt. Only an explicit `y` or `Y` proceeds; an empty
line, `n`, end-of-input, and any unrecognized answer abort without changing
anything. This is deliberately interactive and has no force or bypass flag, so
it cannot be scripted by accident.

On confirmation it first stops every discoverable workspace service through its
lifecycle socket, so each server terminates its own provider children, shell
processes, delegated runs, and frontend transports before persistence is
touched. Stale socket sets left by a dead server are removed. It then deletes
every logical session, delegated orchestration run, agent turn, and native
runtime history — including orphaned histories from dead or partially
initialized sessions that ordinary close-first deletion cannot clear.

Provider credentials, provider enablement, default-model preferences, global
add-on configuration such as web and memory settings, installed providers, and
repository contents are outside the purge boundary and survive it. The command
reports what it removed and reports failures instead of claiming success, and
running it again on an already-clean install is a no-op.

### Start Nakode

Open a project workspace:

```sh
nakode --workspace /path/to/project
```

Then use `/settings` to manage general preferences, agents, models, providers,
and optional add-ons. The settings menu is searchable. `/providers`, `/agents`,
and `/models` remain available as direct shortcuts.

### Global agents

Sub-agent archetypes are global to the user rather than tied to a project workspace. Nakode stores
them as TOML files under `$NAKODE_HOME/agents`; when `NAKODE_HOME` is unset it defaults to
`~/.nakode`, so the ordinary catalogue is `~/.nakode/agents`. Every workspace loads the same
catalogue.

A definition names the archetype (`slug`, `description`), what it is told (`system_prompt`,
`first_message`), and how it runs: `model`, `fallback_models`, `fast_mode`, and an optional
`reasoning_effort`. Effort belongs to the model that runs at it, so `reasoning_effort` is refused
without a `model` and refused when that model does not offer the level named. Omit it and the
delegated run uses the model's own default level, which is what every definition written before the
field means — nothing on disk needs editing.

Owner-defined definitions additionally carry ownership and availability, canonical capability/tool
allow and deny lists, a tool profile (`none`, `read_only`, `command_runner`, `bounded_watcher`, or
`custom`), task/output contracts, bounded lifecycle values, fallback policy, and delegation/parent
attribution policy. Shipped built-ins are visible but immutable. Nakode validates these fields against
its live provider/model catalogue, persists create/update/rename atomically, and remains the sole
runtime authority; unavailable choices are reported rather than silently replaced. Native Codex,
Devin, GLM, and Kimi sessions receive an authoritative Nakode builtin-tool allowlist, while Claude
applies the equivalent SDK allowlist and permission hook. Empty custom policy retains compatibility
with definitions written before policy fields existed.

Catalogue changes are loaded without restarting the service. After installing a Nakode binary whose
public protocol changed, restart each workspace service that a frontend uses—for the FStack dashboard,
`nakode --workspace "$FSTACK_HOME" restart`—and relaunch that frontend so both ends use the same
protobuf and capability table.

```toml
slug = "code-reviewer"
description = "Reviews changes for correctness"
model = "openai-codex/gpt-5.6-sol"
reasoning_effort = "high"   # omit for the model's own default
```

Use `--agents PATH` or `NAKODE_AGENTS=PATH` to override the catalogue. Absolute paths are used as
written; relative paths resolve under Nakode home, not under the current workspace. Nakode does not
automatically import existing workspace-local `.nakode/agents` directories; move wanted definitions
into the global directory or point `--agents` at an absolute compatibility directory.

## Personalities and Soul

Nakode can append user-specific guidance to every newly created native agent
session. By default it looks in the platform configuration directory for
`personalities.toml` and an optional `SOUL.md` (for example,
`~/.config/nakode/` on Linux). It never creates either file.

`personalities.toml` supports a global default and provider-qualified,
per-model overrides:

```toml
default = """
Be warm, direct, and explain important tradeoffs.
"""

[models]
"openai-codex/gpt-5.4" = """
Prefer terse answers and make implementation decisions confidently.
"""
"zai-coding/glm-4.7" = """
Show a short plan before changing code.
"""
```

An exact model entry replaces the default personality for that model. Models
without an entry use `default`. Empty values are ignored. Model keys must use
the canonical `provider/model` form.

`SOUL.md` describes who the agent is—identity, enduring preferences, and style
controls. When present, it is always appended independently of the selected
personality, including for delegated agents. Personality and Soul content is
materialized when a provider-native session is created. After editing either
file, use `/reload`; the new content affects newly created sessions rather than
already-running or resumed sessions.

Use `--personalities PATH` / `NAKODE_PERSONALITIES` and `--soul PATH` /
`NAKODE_SOUL` to select other files. Relative explicit paths are resolved from
the workspace. Explicit paths must exist; the default files are optional.

## Terminal image previews

Sent image attachments render inline when Nakode detects Kitty, WezTerm, Ghostty,
iTerm2, Sixel, or another protocol supported by `ratatui-image`. Configure the
default under `/settings` → **Add-ons** → **Terminal images**:

- **Automatic** uses terminal hints and a capability query.
- **On** always attempts the capability query, which is useful through tmux or SSH.
- **Off** keeps attachment labels without probing.

The `NAKODE_TERMINAL_IMAGES=auto|on|off` environment variable remains available
as a per-launch override.

## Usage diagnostics

Nakode records aggregate inference and tool telemetry inside each local native session. Inspect
recent usage without exposing prompts, reasoning, tool arguments, tool output, session titles, or
credentials:

```sh
nakode diagnostics
nakode diagnostics --days 30 --provider openai-codex --sessions 40
nakode diagnostics --days 30 --json > nakode-usage.json
```

The report includes daily provider usage, reported input/cached/uncached/output tokens, inference
rounds, compactions, retries, tool calls, failures, output sizes, runtime, and the highest-input
sessions. JSON output is intended for longitudinal analysis. Token and cache values are available
only when the provider reports them; cached tokens may still count toward provider subscription or
rate limits even when an API pricing plan discounts them.

Long-running turns remain unrestricted. Nakode emits non-blocking transcript warnings after every
25 active inference rounds, when an inference request succeeds only after provider retries, and
when the same tool fails three times and then at each additional five-failure milestone in one
turn. These warnings are informational and never interrupt the agent.

Canonical transcript entries optionally retain the stable provider ID and qualified model ID active
when their turn began. Native runtime history and delegated-run SQLite persistence restore that immutable
origin, and `TranscriptEntryView` plus protobuf fields 9 (`provider_id`) and 10 (`model_id`) expose it to
SDK clients. Legacy and provider compatibility history without trustworthy origin leaves both fields
absent; consumers must not infer them from a current selection or display/model-name parsing.

## Optional web browsing

Nakode's portable runtime can expose a `browser` tool when a browser add-on is
enabled under `/settings` → **Add-ons** → **Web browsing**. Browsing is disabled by default and
neither backend is required to run Nakode:

- **agent-browser** runs the optional open-source `agent-browser` executable on
  the local machine. Install and configure it separately, then select it in
  Nakode. If the executable is missing, only browser calls fail.
- **Firecrawl** uses Firecrawl's hosted search and scrape API. Select Firecrawl
  and enter an API key in settings. The key is stored in Nakode's protected
  local application database.

Changes apply to the portable browser tool without restarting Nakode. Provider
or tool functionality unrelated to web browsing remains available when either
add-on is absent or disabled.

## Optional memory

Nakode can expose provider-neutral `memory_search` and `memory_store` tools through
[Mnemosyne](https://github.com/mnemosyne-oss/mnemosyne). Memory is disabled by
default and writes occur only when an agent explicitly calls `memory_store`; Nakode
does not ingest transcripts automatically.

Install Mnemosyne with its stdio MCP support in an isolated Python environment:

```sh
uv tool install 'mnemosyne-memory[mcp]'
```

Then open `/settings` → **Add-ons** → **Memory**, select **Mnemosyne**, confirm
the executable, and choose the Mnemosyne bank used for global user memory.
Semantic embeddings remain optional and can be installed with
`mnemosyne-memory[mcp,embeddings]`. Nakode supervises local MCP processes and
stores memories in Mnemosyne's SQLite data directory.

Nakode manages a deterministic project bank for each workspace; project-bank names
are internal and are not user settings. Every `memory_store` call must explicitly
select `project` or `global` scope. `memory_search` searches both scopes by default,
while allowing a caller to narrow a query to one scope.

Memory tools are currently available to the portable-tool runtimes used by Codex,
Devin, Kimi, and GLM. Cursor continues to work normally but does not receive these
tools. Disabling memory, clearing a required field, or removing the executable
removes the tools on the next inference request without affecting other providers.

## Skills

Nakode discovers portable Agent Skills from these directories, with
workspace-local skills taking precedence when names overlap:

- `<workspace>/.agents/skills/<skill-name>/SKILL.md`
- `~/.agents/skills/<skill-name>/SKILL.md`

Reference a discovered skill anywhere in a prompt with `/skill:<skill-name>`.
Nakode offers discovered names in composer completion and attaches the selected
skill instructions to that turn while keeping the original prompt unchanged in
the visible transcript.

## Herdr integration

Run Nakode inside a [Herdr](https://herdr.dev/) pane to expose its lifecycle in
Herdr automatically. No Nakode or Herdr plugin is required. When `HERDR_ENV=1`
and the pane identity is available, Nakode reports itself as `idle`, `working`,
or `blocked`, includes its persisted logical session id when one exists, and
releases its status authority on exit. Missing or failed Herdr reporting never
prevents Nakode from starting or handling a turn.
