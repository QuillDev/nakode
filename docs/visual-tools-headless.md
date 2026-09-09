# Add-on configuration on headless hosts

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

## vps-3b787b3d evidence

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
