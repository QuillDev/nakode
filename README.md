# Nakode

Nakode is a provider-neutral terminal application for running, continuing, and
coordinating coding agents. It gives you one workspace and one consistent
interface while allowing each task or session to use the provider and model
that fit it best.

The project is currently experimental. OpenAI Codex and Devin are the first
supported providers, but neither defines Nakode's application model or
long-term scope.

## What Nakode offers today

- **One terminal workspace for multiple providers** — enable supported
  providers, browse their available models, and choose models through a unified
  picker.
- **Durable sessions** — resume recent work, start fresh sessions, and continue
  work from the same workspace or another terminal.
- **Cross-provider continuity** — changing providers carries an explicit summary
  of the visible work forward rather than pretending private model context can
  be transferred.
- **A consistent coding environment** — agents can inspect and edit files, run
  commands, search a workspace, evaluate snippets when a suitable runtime is
  available, ask structured questions, and track task progress.
- **Delegated agents** — define reusable agent roles and send them bounded,
  independently tracked tasks. Independent investigations can run in parallel
  and remain inspectable from the parent session.
- **Responsive streaming chat** — assistant output, reasoning, plans, tool
  activity, diffs, and Markdown render as they arrive. Tool results stay compact
  until expanded.
- **Turn control** — queue follow-up prompts, interrupt active work, and switch
  models without leaving the session.
- **Long-session continuity** — Nakode can condense older conversation context
  while retaining a continuity checkpoint and recent history.
- **Terminal-native interaction** — multiline editing, searchable pickers,
  mouse selection and copy, syntax-highlighted code, and completion
  notifications are built into the interface.

## Project direction

Nakode is growing from a multi-provider chat interface into an orchestration,
continuity, and execution layer for agentic work.

The direction is to provide:

- **Logical work sessions that span agents and providers.** A body of work
  should not be tied permanently to one model or provider conversation.
- **Explicit, auditable orchestration.** Delegation, review, parallel research,
  synthesis, cancellation, and handoff should be bounded and attributable.
- **Honest context transfer.** Work moves between agents through visible
  handoffs containing objectives, constraints, progress, and selected
  artifacts—not through claims that hidden context has been translated.
- **Portable skills and tools.** Shared workflows should behave consistently
  across providers while still allowing providers to expose useful native
  capabilities.
- **Project memory with provenance.** Durable decisions and facts should be
  reusable across sessions with clear scope, sources, confidence, and history.
- **More providers without a lowest-common-denominator experience.** Nakode
  should offer a stable shared workflow while preserving provider-specific
  strengths where their semantics are clear.
- **A self-contained distribution.** Installing a separate agent harness should
  not be required to use a supported provider.

These are product goals, not a promise that every item is available today. The
authoritative product principles and contribution constraints live in
[`AGENTS.md`](AGENTS.md).

## Requirements

- A real interactive terminal
- An account for at least one supported provider

Nakode does not require a separate Codex or Devin application, or a Node.js or
Python agent harness. Individual optional tools may use a local language runtime
when one is available and report clearly when it is not.

## Install

Install for the current user:

```sh
./install.sh
```

The default destination is `~/.local/bin/nakode`. The installer will tell you
how to add that directory to `PATH` if needed.

To install for all users, build as your normal desktop user and elevate only the
final installation step:

```sh
./install.sh --system
```

A custom prefix is also supported:

```sh
./install.sh --prefix PATH
```

Update Nakode by updating the checkout and running the same install command
again:

```sh
git pull --ff-only
./install.sh
```

Do not run the entire installer or Nakode itself through `sudo`; provider sign-in
and desktop integration use your normal user account. Run `./install.sh --help`
for all installation options.

## Get started

Open a workspace:

```sh
nakode --workspace /path/to/project
```

A fresh installation has no providers enabled. Use `/providers` inside Nakode
to enable and sign in to a provider, then use `F2` to choose a model.

For development from a checkout:

```sh
./dev.sh --workspace /path/to/project
```

Use `./dev.sh --clean --workspace /path/to/project` for an isolated, disposable
development state.

Useful launch options:

```text
--workspace <PATH>   Working directory
--model <MODEL>      Initial provider-qualified model
--resume <ID>        Resume a Nakode session by ID or unique prefix
--agents <PATH>      Use agent definitions from another directory
```

Multiple Nakode windows can be open at once, including for the same workspace.

## Everyday controls

| Control | Action |
| --- | --- |
| `Enter` / `Ctrl+Enter` | Send a prompt, or queue it while a turn is active |
| `Shift+Enter` / `Alt+Enter` / `Ctrl+J` | Insert a newline |
| `Ctrl+Q` | Queue the current draft |
| `Ctrl+C` | Interrupt active work; press again while cancelling to exit |
| `F1` | Open or close the in-app key reference |
| `F2` | Choose the model for the next turn |
| `/providers` | Enable, disable, or configure providers |
| `/models` | Choose a provider's default model |
| `/switch` | Change the model for the current session |
| `/resume` | Browse recent sessions for the workspace |
| `/new` | Start a fresh session |
| `/compress` | Condense the current session context now |
| `/agents` | View and manage delegated agent roles |
| `/reload` | Refresh providers and available models |
| `PageUp` / `PageDown` | Scroll through the transcript |
| `Ctrl+L` | Jump to the latest output |
| `Ctrl+D` | Exit when no turn is active |

Use `F1` for the complete, context-sensitive control reference.

## Delegated agents

Nakode includes an `explorer` role for bounded, read-only investigation. Use
`/agents` to view or customize agent roles for a workspace. Roles can select a
preferred model and fallback models, so delegated work may use a different
provider from the parent session.

Delegated sessions are tracked separately and can be opened from the parent
chat. This keeps parallel research visible without mixing every child transcript
into the main conversation.

## Current scope

Nakode is under active development. Today, a session runs one primary provider
turn at a time, with bounded delegated investigations available alongside it.
Some operations depend on provider capabilities; when a provider cannot resume,
interrupt, steer, or expose another optional feature, Nakode should report that
limitation rather than imitate incompatible behavior.

OpenAI Codex and Devin are currently supported. Additional providers, richer
multi-agent workflows, portable skills, and durable project memory are part of
the direction described above.