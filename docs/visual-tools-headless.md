# Add-on configuration on headless hosts

For reusable Linux provisioning, sandbox troubleshooting, effort configuration,
and safe validation steps, see [Linux provisioning](linux-provisioning.md).
The VPS sections below are dated operational evidence, not a claim that the
running VPS already includes subsequent source changes to vision effort.

FStack exposes Nakode's server-owned add-on configuration through `fstack addons`.
A TUI or display is not required. From the FStack checkout, build with
`cargo build --bin fstack`, then use its stack-built binary:

```sh
fstack addons status
fstack addons models
fstack addons browser --provider agent-browser
fstack addons vision --model openai-codex/<eligible-model>
fstack addons memory --provider mnemosyne --executable /absolute/path/to/mnemosyne
```

Use `--socket /absolute/private/api.sock` (or `FSTACK_NAKODE_SOCKET`) for an
explicit existing service and `--workspace /absolute/server/workspace` to select
the workspace. Otherwise the CLI queries the bundled runtime's read-only status.
It does not activate/restart the service. See FStack's
`docs/nakode-visual-tools-vps.md` for credential input, all options, validation, and
configuration/enablement semantics. The initial experimental `nakode visual`
command was removed in favor of this supported FStack frontend.

## Authority and actual capabilities

The CLI uses public `GetWorkspace`, `GetServerInfo`, `CheckAgentBrowser`, and
`UpdateSettings` SDK operations already present in the pinned runtime. It does
not edit server persistence or launch a provider. Settings remains the on/off UI;
currently backend/model selection itself implies enablement. Independently saving
a dormant configuration is not supported by that API. Configuration does not
change session allowlists, tool replacement, delegated-role policy, or approvals.

- `browser`: `search` and `open` through local agent-browser or hosted Firecrawl.
  Returns page text/accessibility snapshots, **not screenshots or click actions**.
- `vision`: analyzes existing workspace PNG/JPEG/GIF/WebP images up to 20 MiB.
  Needs a supported configured model and available vision/provider service;
  current model eligibility uses the Codex provider gate. Does not capture images.
- `memory_search` / `memory_store`: optional Mnemosyne backend. Install
  `mnemosyne-memory[mcp]` as the service user and configure the executable, global
  bank, and optional data directory through the CLI. Installation/detection alone
  does not verify its backing store or MCP workflow.
- Terminal image rendering is client presentation, not browser setup.

## Browser prerequisites

Pin and review optional package versions. With administrator approval, the
system-wide installation tested on this VPS was:

```sh
sudo npm install --global --prefix /usr/local --allow-scripts=agent-browser agent-browser@0.37.1
# Needed here because the installation inherited a restrictive umask.
# Repair ONLY the public package tree, never credentials or user application data.
sudo chmod -R a+rX /usr/local/lib/node_modules/agent-browser
agent-browser --version
# Run as the actual service user; cache is per-user.
agent-browser install
```

Put `/usr/local/bin` on the **service** PATH. A login shell's PATH is not proof
that a long-running service sees it. Node/npm are optional installation
prerequisites, not Nakode's core runtime dependencies. `CheckAgentBrowser` executes
`--version` on the server; it does not prove Chromium can launch.

Ubuntu AppArmor can reject the downloaded Chrome-for-Testing sandbox. Do not use
`--no-sandbox`, disable global user-namespace restrictions, or make a helper setuid
inside a user-writable tree. Prefer a packaged browser with supported sandboxing.
The owner-approved packaged alternative installed here was:

```sh
sudo snap install chromium
export AGENT_BROWSER_EXECUTABLE_PATH=/snap/bin/chromium
```

Snap confines temporary/profile directories. An explicit private profile under
`$HOME/snap/chromium/common/` allowed navigation here but not a full snapshot
workflow. This is **not a validated service environment selection**. Do not set
it on a shared service until the full workflow passes. Library installation,
sandbox-policy changes, and service deployment require operator review.

## vps-3b787b3d: working host configuration (2026-09-09)

With the owner's approval for host-wide sudo operations, the existing downloaded
Chrome-for-Testing **153.0.8010.36** was copied as a complete tree to
`/opt/google/chrome`. The tree is root-owned, readable/executable by users, and
not writable by group/others. `chrome_sandbox` is **0755, not setuid**.
The Chrome executable SHA-256 is
`79a4ebf6da53e4ceab11844257aabc5166f17b595dc694d6382cbee8ff50565f`.
This is an inventory checksum of the already installed binary, not an independent
upstream signature verification.

### Cause and repair

The downloaded executable under `/home/ubuntu/.agent-browser/` triggered
Ubuntu's AppArmor `unprivileged_userns` restriction, including a denied
`sys_admin` capability, and exited with `No usable sandbox`.
Ubuntu 26.04 already ships and loads `/etc/apparmor.d/chrome`, which attaches to
`/opt/google/chrome/chrome` and permits `userns`. Installing the browser at that
standard, root-controlled path allows Chromium's namespace sandbox to work.
No AppArmor policy or global sysctl was changed; no `--no-sandbox` or setuid
workaround was used.

Persistent selection:

- `/etc/agent-browser/config.json` (root:root, 0644) contains
  `{"executablePath":"/opt/google/chrome/chrome"}`.
- `/home/ubuntu/.agent-browser/config.json` links to that file. This is how the
  existing Nakode service and non-login subprocesses running as `ubuntu` select
  the browser without a service restart.
- `/etc/profile.d/nakode-agent-browser.sh` supplies
  `AGENT_BROWSER_EXECUTABLE_PATH=/opt/google/chrome/chrome` to future login
  shells, only if no nonempty explicit selection exists.
- Nakode's shared persisted browser backend is `agent-browser` (CLI **0.37.1**).
  Vision remains `openai-codex/gpt-5.6-sol`, with the implementation's fixed
  `low` effort.

The browser installation is host-wide; non-login services under **other Unix
users** must use their own agent-browser config or explicitly set the executable
path. Arbitrary containers/VMs do not inherit the host installation. Explicit
project, environment, or CLI selections can override defaults. These settings
survive new sessions/reboots by construction; a reboot was not performed.
The live Nakode process remained PID 204143 throughout verification.

### Verified behavior and safe use

Actual live Nakode `browser.open("https://example.com/")` returned the Example
Domain heading and link. The same browser page was captured at an **absolute**
workspace path, decoded as a nonblank PNG, and passed to the actual Nakode
`vision` tool. Vision correctly read the heading, explanatory paragraph, and
Learn more link. Fresh temporary and consistent persistent-profile workflows
also passed navigation, heading assertions, and visually verified screenshots.
A loopback-served local app passed input filling, button clicks, JavaScript status
updates, and red/blue canvas capture; vision read the resulting UI correctly.
One initial local-app attempt timed out on screenshot/close. Its successful retry
was followed by **three fresh identical complete workflows**, all passing in
18 seconds with unique nonblank PNGs and successful cleanup. The initial timeout
is retained as an unexplained intermittent observation, not erased by the later
passes; this is bounded functional verification, not a guarantee for every site.

Use a unique `AGENT_BROWSER_SESSION` for independent CLI work. Keep launch
options consistent across every command: tests that supplied `--profile` only
to `open`, then omitted it, subsequently saw `about:blank` and blank screenshots.
Export `AGENT_BROWSER_PROFILE` for the entire workflow when a persistent profile
is needed. For a handoff from Nakode's default browser session, match its launch
environment; verification unset `AGENT_BROWSER_EXECUTABLE_PATH` and
`AGENT_BROWSER_PROFILE` so the user config remained the selection source.
Never hijack or close another task's default session. Nakode's current text
wrapper does not itself assign a unique agent-browser session to each call.

Use absolute screenshot paths: relative paths can resolve against the daemon's
original working directory, not the caller's. Verify URL, expected heading,
file existence, and actual image pixels; a successful screenshot exit code is
not proof of useful visual output. Nakode's browser tool still returns text,
not screenshots; capture is performed with the installed agent-browser CLI.

Sandbox evidence from `chrome://sandbox`: PID namespaces, network namespaces,
Seccomp-BPF, and TSYNC all **Yes**. Renderer processes reported
`NoNewPrivs: 1` and `Seccomp: 2`. Test debugging listeners bound to loopback only.
Owned browser sessions were closed; unrelated Snap browser processes and shared
services were left untouched.

Local evidence (ignored operational artifacts, not committed fixtures):

- `target/validation-logs/nakode-browser-verified.log` and
  `nakode-browser-verified.png`: successful live-tool capture, 17,893-byte PNG.
- `target/validation-logs/discriminating-browser-1788970980-232406.log` and
  `discriminating-{A,B}-232406.png`: successful consistent-option workflows.
- `target/validation-logs/browser-local-js-verified.png` and
  `browser-local-js-verified-retry.log`: successful interactive local-app capture;
  `browser-local-js-verified.log` retains the initial timeout.
- `target/validation-logs/repeat-1788971454-242360.master.log`, corresponding
  `-{1,2,3}.log`, and `-{1,2,3}.png`: three complete repeatability passes.
- `target/validation-logs/host-chrome-run-1788970422-222329.log` and
  `host-chrome-proc-1788970434-222726.log`: sandbox evidence. The earlier image
  in this run was blank and is **not** screenshot success evidence.

### Maintenance and rollback

This is a **pinned, manually maintained Chrome-for-Testing installation**, not an
APT-managed or automatically updated browser. `agent-browser install` updates
its user download cache, not `/opt/google/chrome`. An operator must review newer
browser releases, stage the complete tree, retain root ownership and non-setuid
permissions, and validate sandbox/navigation/nonblank screenshots before
replacing the selected tree during an appropriate maintenance window. Do not
leave the pinned build indefinitely without security updates.

To roll back, first coordinate and close only sessions using this installation;
remove the user config symlink if it still points to the managed config, remove
only the newly created `/etc/agent-browser/config.json` and
`/etc/profile.d/nakode-agent-browser.sh`, and remove `/opt/google/chrome` only if
it is still this installation. Existing login shells may retain the exported
variable until it is unset or the shell exits. Restore browser selection via
Nakode Settings as desired; returning to the old downloaded/Snap browser also
returns to the previously observed failures. No global policy rollback is
needed because none was modified.

### Post-reboot recovery verification (2026-09-09)

A later session (`01a0872b-1ea6-7ce0-a1e4-268ba9f36834`) rechecked the
installation after the owner's reboot/hardening. The owner explicitly chose to
retain **agent-browser**, **openai-codex/gpt-5.6-sol**, and the vision path's
fixed **low** effort. No setup changes, privileged operations, service restarts,
builds, or cloud archetype edits were needed.

The live service started at `2026-09-09T17:10:08Z`. Its read-only
`fstack addons status` reported agent-browser **0.37.1**, the selected Sol model,
and a ready callable vision service. Firecrawl had no configured credential.
The browser status field `launch_verified: false` is not an active workflow
probe; the separate functional checks below establish usability. Chrome still
reports **153.0.8010.36**; its executable/helper remain root-owned **0755**,
`ldd` reported no missing libraries, and the managed browser config and user
symlink survived. AppArmor's unprivileged-userns restriction remains **1**.
Post-reboot renderer sandbox internals were not independently inspected; the
prior detailed sandbox evidence remains historical.

Fresh evidence:

- One isolated, uniquely named CLI session navigated to `https://example.com/`,
  asserted its URL/heading, wrote a **1280 × 577**, **17,893-byte** PNG, and
  closed successfully in **18 seconds**. No profile/executable environment
  override was used for that workflow.
- The actual Nakode `vision` call read the screenshot's heading, paragraph, and
  link correctly and identified it as nonblank, not an error page.
- After `agent-browser session list` reported no active sessions, the actual
  Nakode `browser` open call returned the Example Domain heading and Learn more
  link. Its browser was then closed; session listing again reported none.
- Artifacts: `target/validation-logs/post-reboot-1788974333-16062.log` and
  `post-reboot-1788974333-16062.png`. The actual tool-call results are in the
  recovery session transcript; `recovery-handoff-post-reboot.md` records context.

The live eligible vision catalogue advertised only `openai-codex` models:
`codex-auto-review`, `gpt-5.5`, `gpt-5.6-luna`, `gpt-5.6-sol`, `gpt-5.6-terra`,
`gpt-6-astra`, and `gpt-reserve`. Eligibility does not prove every model works;
only the retained Sol selection was exercised. The settings CLI exposes model
selection, not vision effort; the inspected `CodexVisionService` fixes it to
`low`. Browser alternatives remain local agent-browser or credentialed hosted
Firecrawl; Firecrawl was neither selected nor tested.

Approximately **22 GiB** RAM remained available with low load and no swap.
Checks were sequential; no build/test suites were run. Shared settings and host
installation demonstrably survived this reboot, and the new session could use
both tools without restart. Retain the maintenance and shared-default-session
cautions above; this is bounded harmless-site verification, not a reliability
or security audit.

## Earlier VPS evidence (superseded by the host repair above)

Initial Linux x86_64 host had no DISPLAY/WAYLAND_DISPLAY, agent-browser, or browser
executable on PATH; no distro Chromium package/Snap was installed. This session's
exposed tool catalogue lacked browser and vision.

Installed with owner approval:

- agent-browser **0.37.1** system-wide under `/usr/local`;
- Chrome-for-Testing **153.0.8010.36**, downloaded for ubuntu;
- Canonical Chromium Snap **152.0.7977.64**.

Downloaded Chrome failed `No usable sandbox`: the host has
`kernel.apparmor_restrict_unprivileged_userns=1`, and the downloaded helper was
ubuntu-owned mode 0755. No sandbox restriction was disabled.

Snap's default-profile workflow timed out. An explicit-profile attempt opened
`https://example.com/`, but snapshot, screenshot, and close timed out. Owned test
processes were cleaned up. No screenshot artifact or full browser success is
claimed. Local logs are in Nakode's `target/validation-logs/visual-prereq-*`.

The FStack CLI subsequently passed focused tests and configuration roundtrips
against an isolated Nakode server. That verifies public SDK configuration, not
inference or memory/browser operation. The running shared service has not been
upgraded, restarted, or reconfigured, and no session tool policy was changed.
Remaining operational work is a responsive sandboxed browser, any needed memory
runtime setup, and an authorized browser/vision/memory session validation.
