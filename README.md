# Nakode

Nakode is a provider-neutral terminal application for working with coding
agents. It gives you one workspace and one consistent interface while allowing
each session or delegated task to use the provider and model that fit it best.

Nakode is experimental and under active development.

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
includes richer delegation and review workflows, portable skills, and durable
project memory. These are product goals, not all current features.

## Supported providers

Nakode currently supports:

- **OpenAI Codex**
- **Devin**
- **Cursor**

Providers are disabled on a fresh installation. Start Nakode, open
`/providers`, and sign in to the providers you want to use. Press `F2` to browse
and select from their available models.

Nakode does not require the separate Codex or Devin applications. Cursor uses
its local TypeScript SDK and requires Node.js 22.13 or newer plus npm. Cursor
setup in `/providers` includes an API-key field and a link to the Cursor API-key
dashboard. No single provider defines Nakode's workflow or session model.

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

## Herdr integration

Run Nakode inside a [Herdr](https://herdr.dev/) pane to expose its lifecycle in
Herdr automatically. No Nakode or Herdr plugin is required. When `HERDR_ENV=1`
and the pane identity is available, Nakode reports itself as `idle`, `working`,
or `blocked`, includes its persisted logical session id when one exists, and
releases its status authority on exit. Missing or failed Herdr reporting never
prevents Nakode from starting or handling a turn.
