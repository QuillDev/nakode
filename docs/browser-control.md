# Browser control

`browser_control` is a built-in tool that gives an agent its own private browser on the execution
machine for testing web work. It is offered wherever a Chromium-family browser is installed
(`NAKODE_BROWSER_EXECUTABLE`, then `chromium`, `chromium-browser`, `google-chrome`,
`google-chrome-stable` on PATH, then the macOS application bundles) and is absent otherwise.
Native providers see it as `browser_control`; Claude sees `mcp__nakode__browser_control`. It needs
the `network` capability, like `browser`.

## What it is and is not

- One Chromium per logical owner session, with a fresh temporary profile, started by the first
  `open` or `navigate` and discarded with the session or `close`.
- Driven over `--remote-debugging-pipe`: no debugging port is opened, so no other process can attach.
- Chromium's sandbox stays on. As root it refuses to start unless the operator sets
  `NAKODE_BROWSER_NO_SANDBOX=1` where the machine is itself the isolation boundary.
- Headless, except on a Linux `DISPLAY` (the agent desktop), where the window is visible to the
  desktop tools and screen recordings.
- Only `http(s)` and `about:blank` load. Downloads are denied. No script evaluation, cookie or
  credential access is exposed.
- It is never the owner's browser or the FStack dashboard browser pane, and neither is ever a
  fallback for the other. The dashboard pane has its own narrow Chat controls in FStack.

## Actions

| Action | Effect |
| --- | --- |
| `open`, `navigate` | Start the browser if needed; `open` adds a tab, `navigate` loads in the current one and waits (`wait`: `load` default, `idle`, `none`) |
| `tabs`, `select_tab`, `close_tab` | This browser's tabs only |
| `back`, `forward`, `reload` | History, with the same wait |
| `state` | Compact page state, no image |
| `screenshot` | Viewport PNG saved to the Gallery under `browser/<session>/`; image pixels are CSS pixels |
| `wait` | Bounded (default 10 s, max 30 s) wait for load, network idle, or URL/title text |
| `diagnostics` | Returns and clears console errors/warnings, page exceptions and failed requests (≤50 entries of ≤300 chars; URLs without query or fragment) |
| `click`, `scroll` | Viewport CSS-pixel `x`/`y` plus the `observation` they were read from |
| `type`, `key` | Insert text into the focused element; press key chords such as `Enter`, `ctrl+a` |
| `close` | End the browser |

Every result is page state: tab, URL, title, loading, CSS viewport size, device scale, pinch zoom,
scroll offset and an `observation` id. Any action can append a screenshot with `screenshot: true`;
none does by default.

## Coordinates and stale targets

`click` and `scroll` are refused when the observation is unknown, of another tab, older than the
page's last navigation, or when the viewport size, device scale (display scale × browser zoom),
pinch zoom or scroll offset changed since. Points outside the viewport are refused. Input is
never retried; a cancelled or interrupted call reports that its outcome is unknown, and the agent
must observe before acting again.

## Measurements

Reproduce with `scripts/browser-bench/run.sh` (Docker; Debian Chromium, Xvfb 1280×900, openbox,
real `desktop_screenshot`/`desktop_action` and the Claude bridge path). It runs the ignored
`browser_control_benchmark` test with `NAKODE_BROWSER_BENCH_ITERATIONS` (default 30) and writes
`.tmp/bench-out/linux-results.json` plus the desktop frame it aimed from. Without a desktop the
same test measures only `browser_control`:
`cargo test --release --lib browser_control::bench -- --ignored --nocapture`.

Correctness is checked through the page itself (title changes set by the fixture's click, input,
Enter and scroll handlers). Tool calls and images are what the model spends per row.

Linux container, headed Chromium, release build, 2026-10-27, n=30 (cold rows n=10):

| Path | Operation | p50 ms | p95 ms | Correct | Calls | Images |
| --- | --- | --- | --- | --- | --- | --- |
| browser_control | cold start + first navigate | 198.1 | 206.6 | 10/10 | 1 | 0 |
| browser_control | cold browser launch only | 171.0 | 184.0 | 10/10 | 0 | 0 |
| browser_control | warm navigate + wait load | 13.8 | 16.8 | 30/30 | 1 | 0 |
| browser_control | state | 0.5 | 1.2 | 30/30 | 1 | 0 |
| browser_control | screenshot | 35.4 | 49.7 | 30/30 | 1 | 1 |
| browser_control | click | 12.6 | 14.7 | 30/30 | 1 | 0 |
| browser_control | screenshot → click → verify | 51.1 | 82.9 | 30/30 | 2 | 1 |
| browser_control | type 5 chars | 1.5 | 2.1 | 30/30 | 1 | 0 |
| browser_control | key Enter | 1.2 | 2.2 | 30/30 | 1 | 0 |
| browser_control | scroll 400 px | 31.1 | 35.6 | 30/30 | 1 | 0 |
| desktop (CUA) | desktop_screenshot | 130.4 | 137.9 | 30/30 | 1 | 1 |
| desktop (CUA) | desktop_action click | 105.5 | 108.8 | 30/30 | 1 | 0 |
| desktop (CUA) | click via Claude bridge (settle + screenshot) | 661.7 | 685.2 | 30/30 | 1 | 1 |
| desktop (CUA) | screenshot → click → verify (Claude bridge) | 808.0 | 849.4 | 30/30 | 2 | 2 |
| desktop (CUA) | type 5 chars | 58.4 | 69.5 | 30/30 | 1 | 0 |
| desktop (CUA) | key Return | 21.8 | 26.8 | 30/30 | 1 | 0 |
| desktop (CUA) | scroll 5 steps | 223.4 | 254.7 | 30/30 | 1 | 0 |

The click's own result carried the verified title in 30/30 workflows, so verification needed no
extra call. macOS (Apple silicon, headless Google Chrome, no desktop tools, so no CUA baseline):
cold start + navigate p50 706 / p95 7155 ms (the first launch after boot dominates p95), warm
navigate 9.9/37.6, state 0.6/6.7, screenshot 172/225, click 86/182, type 3.6/51.5, key 2.3/21.2,
scroll 108/165, all correct.

Every interaction row is faster than the comparable desktop action, and the observe/act/verify
workflow is about 16× faster with one fewer image, so click, scroll, type and key stay in the
tool. The desktop tools remain the path for anything outside the page: native dialogs, other
applications, and the browser's own chrome.

## Limitations

- Milliseconds are tool-path latency on the execution machine, not model or network round trips.
- The CUA baseline exists only where the machine has an X desktop (`DISPLAY`, xdotool, ffmpeg).
- A server crash leaves the temporary profile directory behind; the browser exits when its pipe closes.
- Keys cover named keys, ASCII letters and digits with alt/ctrl/meta/shift; other text goes through `type`.
- JavaScript dialogs (alert, confirm, file pickers) are not answered by the tool.
