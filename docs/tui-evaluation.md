# TUI evaluation harness

`nakode tui-eval` is a deterministic, headless terminal for evaluating the real
Nakode TUI. It uses the same input reducer, control registry, presentation
state, and Ratatui renderer as the interactive client. It does not start provider
processes, touch persistence, acquire the desktop terminal, or parse ANSI
output.

The harness complements, rather than replaces, `tests/tui_terminal.rs`:

- `tui-eval` covers interaction, rendering, authoritative service projections,
  emitted SDK intents, terminal sizes, and visual styles.
- PTY tests cover executable startup, Crossterm modes, control-service
  coordination, and terminal restoration.

## Run it

Build once, then provide JSON Lines on standard input:

```sh
cargo build
printf '%s\n' \
  '{"action":"type","text":"/settings"}' \
  '{"action":"key","key":"enter"}' \
  '{"action":"assert","modal":"settings","screen_contains":["Settings","General"]}' \
  | target/debug/nakode --workspace . tui-eval
```

Run a committed scenario:

```sh
cargo run -- --workspace . tui-eval \
  --scenario tests/tui_scenarios/agent_smoke.jsonl
```

Use `--width` and `--height` to set the initial terminal size. Every non-comment
input line produces one JSON observation on standard output. A malformed action
or failed assertion exits nonzero and reports its scenario line. The observation
that failed is still emitted, so an agent can inspect the exact screen and state.

## Actions

All coordinates are zero-based terminal cells.

| Action | Important fields | Purpose |
| --- | --- | --- |
| `snapshot` | `styles` | Render without changing state. `styles: true` includes compact cell-style runs. |
| `key` | `key`, `modifiers` | Send one real Crossterm key event through the control registry. |
| `type` | `text` | Send text as individual unmodified key events. |
| `paste` | `text` | Send a bracketed-paste event. |
| `mouse` | `kind`, `column`, `row`, `modifiers` | Send left down/drag/up or scroll events. |
| `resize` | `width`, `height` | Resize the TestBackend and send the matching resize event. |
| `service` | `event` | Install a fixture representing an authoritative server-side state change. |
| `assert` | assertion fields | Check the current render, semantic state, and commands from the previous action. |

Supported key names include individual characters, `enter`, `esc`, `tab`,
`backtab`, `backspace`, `delete`, arrows, `home`, `end`, `page_up`,
`page_down`, `space`, and `f1` through `f24`. Modifiers are `shift`, `control`,
`alt`, `super`, `hyper`, and `meta`.

Mouse kinds are `down`, `drag`, `up`, `scroll_up`, and `scroll_down`.

## Service fixtures

Service fixtures update the protocol view consumed by the thin client; they do
not run a provider, mutate persistence, or execute a command. Supported fixture
event types are:

- `ready` with a capability list
- `models`
- `session_created`
- `turn_started`
- `item` and `delta`
- `approval`, `question`, and `interaction_resolved`
- `todo`
- `turn_completed`
- `context_usage`
- `warning`
- `request_failed`
- `disconnected`

This is enough to evaluate full composer-to-response flows, streaming, tool and
diff presentation, queues, approvals, questions, model/session state, errors,
and disconnect behavior. Extend this semantic fixture enum when a new shared
TUI view needs evaluation; provider protocol fixtures belong in their adapter
tests.

Example:

```json
{"action":"service","event":{"type":"ready","display_name":"Codex","capabilities":["resume","steering","interruption","approvals"]}}
{"action":"type","text":"Run the tests"}
{"action":"key","key":"enter"}
{"action":"assert","commands_include":["submit_prompt"],"draft":""}
{"action":"service","event":{"type":"session_created","provider_session_id":"provider-session-1","model":"gpt-5"}}
{"action":"assert","status":"Starting turn…"}
```

## Assertions

Assertions may combine:

- `screen_contains` and `screen_excludes`
- exact `status` or `status_contains`
- exact `modal`, `draft`, or `connection`
- `commands_include` and `commands_exclude`
- `cursor_visible`
- `screen_width` and `screen_height`

Modal names are stable semantic identifiers: `none`, `help`, `approval`,
`question`, `sessions`, `providers`, `agents`, `models`, `subagent`, `settings`,
and the `settings:*` subviews.

Command names are SDK operations such as `submit_prompt`,
`resolve_interaction`, `select_model`, and `cancel_session_work`. Command
payloads remain inside the typed harness so credentials or prompt content are
not copied into observations. Provider effects belong in adapter tests.

## Observation shape

Each JSON result contains:

- `screen.lines`: exact rendered text with only trailing spaces removed
- `screen.cursor`: terminal cursor visibility and position
- `screen.styled_lines`: optional contiguous Ratatui style runs
- `state`: connection, provider, model, session/turn activity, active modal,
  status, draft, queue length, recent transcript entries, diagnostics, and quit
  state
- `commands`: typed server commands emitted by the interaction
- `devices`: local URL/clipboard intents emitted by the interaction

Prefer semantic assertions for behavior and a few high-value screen assertions
for affordances. Use styled snapshots for targeted visual evaluation instead of
checking raw debug style strings throughout the suite.
