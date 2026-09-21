# Directory-scoped sessions

`InspectWorkspacePath` with `directory_scope=true` performs repository-free inspection on the authoritative Nakode execution host. It accepts absolute paths and `~` / `~/…`, resolves the runtime user's home and symlinks, verifies an accessible directory, and returns its canonical path plus observed hostname, OS and architecture. It does not run Git. `NakodeClient::inspect_directory_scope` requires `DirectoryScopedSessions` capability.

After showing that scope to the owner, create a session with `working_directory` and `SessionToolConfiguration.directory_scope` both set to the returned canonical path. Nakode rejects a mismatched root rather than widening scope. An omitted built-in allowlist installs `read`, `write`, `edit`, `ls`, `find`, `grep`, `ask`, and `todo`; a supplied allowlist may narrow this set. Scope is durable session tool configuration and appears in `SessionState.directory_scope`.

Only supported portable-runtime providers can execute this policy. External tools, MCP, shell commands, evaluators, delegation, memory, skills, network tools, and Code Mode are unavailable. Model/provider changes and tool reconfiguration retain the boundary. Open restores the saved policy, including when tools are omitted; a different explicit scope or retargeted canonical root is refused. Changing scope requires a newly approved session.

## Security boundary

This is application-level tool authorization and pathname containment, **not an OS sandbox**. Structured paths reject traversal, absolute tool paths, and symlinks escaping the workspace. File-only runtime policies additionally reject a root whose canonical spelling changed, including before provider startup. Normal Nakode persistence, credentials and trusted configuration remain machine-owned and may live outside the selected directory.

The policy does not protect against hostile concurrent host filesystem changes, check/use races, or hard-linked file aliases. Do not use it to isolate mutually hostile host processes or untrusted host administrators. Existing broad home/general and ticket sessions retain their ordinary policies.

## Compatibility

Explicit directory inspection/create/attachment calls fail closed against a server without the capability. A pre-capability binary cannot interpret the new durable field; downgrading a machine with scoped sessions is unsupported and must not be treated as preserving this policy. Clients reconnecting with known scoped attachments must retain the explicit directory field rather than silently dropping it. No OS-level downgrade or filesystem isolation is provided.

Server tests cover preview and invalid paths, atomic tool policy, unsupported providers, loaded/cold restoration and retarget rejection. Runtime tests exercise traversal, external symlinks, denied process tools and retargeting before/after runtime startup.
