# Provisioning optional Nakode tools on Linux

This is an operator runbook, not a requirement to install every add-on. Nakode's
core agent runtime is self-contained; Node, Chromium, Python, and hosted browser
services are optional. Provision only the tools the owner chooses. For the
recorded VPS installation and validation evidence, see
[headless add-on configuration](visual-tools-headless.md).

## 1. Establish scope and capacity

Before installing or launching anything:

- Identify the authoritative server host, architecture, Unix user, workspace,
  service executable/version, and service manager. A client's OS or login-shell
  environment is not the server's environment. `nakode status` is read-only.
- Read repository instructions, inspect existing diffs and recovery notes, and
  preserve existing work. Do not inspect unrelated worktrees or global session
  transcripts to reconstruct a single task.
- Check available RAM, disk, process limits, and the service's effective cgroup
  limits. On small/shared machines run one build job (`CARGO_BUILD_JOBS=1`) and
  serial tests (`-- --test-threads=1`); avoid simultaneous browser suites. Stop
  on pressure rather than repeatedly retrying failed launches.
- Obtain approval for package installation, sudo, shared settings changes,
  service restarts, new paid services, or externally reachable listeners. Do not
  disable host hardening to make an optional tool work.

Prefer an already installed frontend. When FStack provides the service, its
read-only SDK commands are:

```sh
fstack addons status
fstack addons models
```

Use `--socket /absolute/private/api.sock` to select an existing service explicitly
when needed. Confirm the reported workspace/server before a mutation. These
commands do not start or restart the service. Do not build or deploy a second
runtime merely to inspect settings.

## 2. Choose the browser backend

| Backend | Requirements | Suitable uses / limits |
| --- | --- | --- |
| `agent-browser` | Execution-host CLI and sandboxed Chromium | Local navigation and JavaScript; CLI supports screenshots/interactions. Consumes local CPU/RAM. |
| `firecrawl` | Owner-provided API credential and outbound HTTPS | Hosted search/page extraction; usage may cost money. Not a replacement for local UI screenshots in Nakode's wrapper. |

Nakode's `browser` tool exposes `search` and `open` and returns text. It does not
capture a screenshot. The `vision` tool analyzes existing workspace images; it
does not launch a browser. Tool exposure also depends on session allowlists and
role policy; selecting a backend does not expand those permissions.

### Local browser prerequisites

1. Select a reviewed, pinned `agent-browser` release compatible with Linux and
   the host architecture. Use the package's supported installer; npm is one
   installation route, not a core Nakode dependency. The recorded VPS used
   `agent-browser@0.37.1`. Review lifecycle-script behavior for the installed npm
   version before approving a global install.
2. Install a maintained sandbox-capable Chromium build. Verify provenance,
   executable permissions and architecture, and resolve missing libraries using
   the distribution's package manager. `ldd /path/to/trusted/chrome` can identify
   missing libraries; do not run it on an untrusted executable.
3. Run the CLI as the actual service user. `agent-browser --version` establishes
   CLI detection only. `agent-browser install` downloads a per-user browser; it
   does not update an independently installed `/opt` browser.
4. Configure browser selection using the CLI's supported user config, e.g.
   `~/.agent-browser/config.json` with
   `{"executablePath":"/absolute/path/to/chrome"}`. A host-managed root-owned
   config may be linked from that user's config. Do not overwrite an existing
   user/project selection without review. Verify precedence against the pinned
   CLI's documentation; project, environment and CLI options may override it.
5. Ensure the long-running Nakode service can execute the CLI on its PATH.
   Login profile changes alone do not update existing service environments.

On Ubuntu with AppArmor's restricted unprivileged user namespaces, a downloaded
browser under a user's home may fail with `No usable sandbox`. Prefer a packaged
browser with a supported sandbox policy. On the recorded VPS, the already loaded
`/etc/apparmor.d/chrome` profile authorized the root-controlled standard path
`/opt/google/chrome/chrome`. An owner-approved complete-tree installation there
worked with a **0755, non-setuid** helper. This is a host-specific solution, not a
portable instruction to copy an arbitrary executable to that path.

Do **not** use `--no-sandbox`, disable AppArmor/userns restrictions globally, or
make a helper setuid in a user-writable directory. Snap Chromium has additional
profile/temp-directory confinement; navigation alone does not prove its
snapshot/screenshot workflow works. Diagnose a failure before changing policy.

### Hosted browser credentials

Use the supported credential-input surface, not argv, shell history, logs, or a
tracked config file. For FStack, `fstack addons browser --provider firecrawl
--credential-stdin` accepts a key from standard input. Supply it through the
owner's secure input mechanism; never echo it into a recorded command. Do not
purchase a service or switch shared settings without approval.

For the approved local backend, selection is:

```sh
fstack addons browser --provider agent-browser
```

This enables the backend but does not install dependencies or prove it works.

## 3. Configure vision and its effort

Use the server's live eligible model catalogue, not an assumed model name or
another agent's main model. Configure the provider's credentials through the
supported settings flow. A missing `OPENAI_API_KEY` in a shell does not imply
that provider-managed authentication is absent. Status should reveal credential
presence/readiness only, never credential contents.

In a runtime built with configurable vision effort:

- Settings → Add-ons → Vision → select model opens the effort picker. Only the
  selected model's advertised efforts are offered. Vision does not expose fast
  mode and does not inherit the calling agent's effort.
- Public `SelectModel` with target `Vision` accepts `ModelOptions.reasoning_effort`.
  Alternatively `UpdateSettings` accepts `VisionSettingsPatch` with `model_id`
  and optional `reasoning_effort`. Read the current model from the workspace
  snapshot and include it when changing effort; an absent model disables vision.
- Omitted effort preserves the existing setting. Existing databases and legacy
  configuration default to **low**. Unknown, empty or unsupported effort values
  are rejected when configuring an enabled model; `none` is a literal effort
  value only if that model advertises it.
- The workspace's `settings.vision.reasoning_effort` projects the persisted
  selection. An absent field indicates an older server, not a verified default.
  Confirm the read-back value after a change; older servers may ignore new
  Protobuf fields. Deploy a compatible server/client before relying on them.

For SDK integrations, the settings patch payload is structurally:

```text
VisionSettingsPatch {
  model_id: "<provider>/<eligible-model>",
  clear_model: false,
  reasoning_effort: "<advertised-effort>"
}
```

The Rust SDK exposes it through `update_settings(UpdateSettingsRequest)` with
normal mutation/idempotency options. No database editing or client-owned
inference is needed. The installed FStack CLI verified on this VPS exposes
`fstack addons vision --model ...` **only**; do not assume it has an effort flag.
Updating that external frontend is a separate integration task. The historical
live VPS checks used the older runtime's fixed `low` implementation, not a
live deployment of the configurable-effort change.

Model selection is also enablement. These settings are service-owned and can be
shared across sessions. An accepted save persists the model/effort and updates
the live shared vision configuration for subsequent calls without restart;
an already started call retains its request. Deploying new server code still
requires an operator-coordinated restart. Do not restart active work just to
exercise a new setting.

## 4. Verify actual usability with harmless data

Separate each layer of evidence:

1. **Configured:** status shows the chosen backend/model and provider readiness.
2. **Detected:** the service executes `agent-browser --version`; trusted Chrome
   reports its version and has its libraries. Neither proves a browser launch.
3. **Working browser:** navigation to `https://example.com/`, expected URL and
   heading, then a nonblank screenshot. Use one uniquely named CLI session and
   consistent launch options throughout:

   ```sh
   export AGENT_BROWSER_SESSION="nakode-check-$(date +%s)-$$"
   # Keep any approved profile/executable selection consistent for every command.
   timeout 45s agent-browser open https://example.com/
   timeout 15s agent-browser get url
   timeout 15s agent-browser snapshot -c
   timeout 20s agent-browser screenshot /absolute/authorized/workspace/check.png
   timeout 15s agent-browser close
   ```

   Check each exit code; stop after failure and clean up only the owned session.
   Use absolute screenshot paths because a daemon may retain an earlier working
   directory. If a persistent profile is needed, keep `AGENT_BROWSER_PROFILE`
   consistent for every command. Do not equate a successful screenshot exit with
   useful pixels. Check dimensions and actual image content.
4. **Working native browser tool:** invoke its `open` on the harmless site. The
   current wrapper shares the CLI default session, so coordinate this test;
   never navigate or close another task's browser. Prefer isolated CLI sessions
   for independent checks.
5. **Working vision:** with owner-approved provider/model usage, pass the harmless
   PNG to the actual Nakode `vision` tool and ask for its visible heading/link.
   This sends the image to the selected provider and may incur normal usage.
   Do not use credentials, private pages, or sensitive screenshots as test data.

Keep bounded logs and images inside the authorized workspace. Record model and
effort, runtime version, command results, failures, cleanup, and whether the
inference was real or mocked. Do not erase intermittent failures with unlimited
retries. Detailed sandbox verification (`chrome://sandbox`, renderer seccomp)
is a separate check; do not claim it merely because navigation passed.

## 5. Persistence, maintenance and recovery

- Provider/add-on settings are persisted by Nakode. Browser caches/configs are
  Unix-user-specific; containers and other users do not automatically inherit
  them. A login script affects future login shells, not every service.
- A manually pinned browser has no automatic security-update guarantee. Assign
  an operator to review updates, stage the complete tree, preserve root ownership
  and sandbox policy, and validate before replacement in a maintenance window.
- Roll back only files/configurations created for this installation after
  checking ownership and current selection. Coordinate active browser users;
  do not remove a shared browser tree or stop unrelated processes.
- After reboot, recheck the service's identity and configuration, perform one
  bounded workflow, and leave a concise recovery handoff. Document outstanding
  approvals and distinguish historical success from current verification.

Optional evaluator runtimes and memory backends should be provisioned only when
requested. Their executable detection is not a backing-service test, and their
absence must not block unrelated Nakode tools. Never change cloud-managed agent
archetypes from an operational provisioning session; return proposals to the
owner's management surface.
