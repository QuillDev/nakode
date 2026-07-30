# Nakode

Nakode is a provider-neutral terminal application for working with coding
agents. It gives you one workspace and one consistent interface while allowing
each session or delegated task to use the provider and model that fit it best.

Nakode is experimental and under active development.

Developers and coding agents can drive the real TUI reducer and renderer through
the deterministic JSON Lines [TUI evaluation harness](docs/tui-evaluation.md).

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
- **Devin**
- **Cursor**
- **Kimi For Coding**
- **GLM Coding Plan (z.ai)**

Providers are disabled on a fresh installation. Start Nakode, open
`/providers`, and sign in to the providers you want to use. Press `F2` to browse
and select from their available models.

Nakode does not require the separate Codex, Devin, Kimi, or z.ai applications. Cursor uses
its local TypeScript SDK and requires Node.js 22.13 or newer plus npm. Cursor, Kimi,
and GLM setup in `/providers` includes an API-key field and a link to the provider's
API-key dashboard. Kimi requires a [Kimi Coding Plan](https://www.kimi.com/code/)
API key; Moonshot Platform API keys are a separate product and are not
interchangeable. GLM requires a [z.ai GLM Coding Plan](https://z.ai/subscribe)
API key and uses the plan's dedicated Coding API endpoint; Team Plan members must
use their Team Plan key. No single provider defines Nakode's workflow or session
model.

## Installation

### Homebrew on macOS

Install Nakode from the official QuillDev tap:

```sh
brew install quilldev/tap/nakode
```

Update Nakode from the command line:

```sh
nakode update
```

`nakode --update` is supported as a convenience alias. For a Homebrew
installation, Nakode delegates the upgrade to Homebrew so the package manager
remains authoritative.

### Build from source

A source installation requires Rust 1.88 or newer:

```sh
git clone https://github.com/QuillDev/nakode.git
cd nakode
./install.sh
```

This installs `nakode` to `~/.local/bin`. Run `./install.sh --help` for system
and custom-prefix options. Do not run Nakode or the entire installer through
`sudo`; provider sign-in uses your normal desktop account.

### Start Nakode

Open a project workspace:

```sh
nakode --workspace /path/to/project
```

Then use `/settings` to manage general preferences, agents, models, providers,
and optional add-ons. The settings menu is searchable. `/providers`, `/agents`,
and `/models` remain available as direct shortcuts.

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
