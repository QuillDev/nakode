# Handoff: Adding a Personality to Nakode

Nakode reads personalities from `personalities.toml` in its user configuration directory. Nakode does not create this file automatically.

Typical locations include:

- Linux: `~/.config/nakode/personalities.toml`
- macOS: the Nakode directory under `~/Library/Application Support/`
- Windows: the Nakode application configuration directory

## Global default personality

Create `personalities.toml` with a `default` value:

```toml
default = """
Be concise, practical, and direct.
Explain important tradeoffs, but avoid unnecessary background.
"""
```

The default applies to every model that does not have a model-specific personality.

## Per-model personalities

Add personalities under `[models]`, using canonical `provider/model` identifiers:

```toml
default = """
Be warm, direct, and practical.
"""

[models]
"openai-codex/gpt-5.4" = """
Be terse and implementation-focused.
Prefer making reasonable decisions over asking unnecessary questions.
"""

"zai-coding/glm-4.7" = """
Present a short plan before making changes.
Explain architectural decisions clearly.
"""
```

An exact model entry replaces the global default for that model; the two personalities are not concatenated.

## Custom configuration location

Pass the personalities file explicitly:

```sh
nakode --personalities /path/to/personalities.toml
```

Alternatively, use the environment variable:

```sh
export NAKODE_PERSONALITIES=/path/to/personalities.toml
nakode
```

Relative explicit paths are resolved against the selected workspace. Explicitly configured files must exist.

## Applying changes

If Nakode is already running, execute:

```text
/reload
```

Updated content applies when a new provider-native session is created. Existing and resumed sessions retain the system instructions with which they were originally created.

## Optional Soul

A separate `SOUL.md` can describe the agent's identity, enduring preferences, and style controls:

```md
You are Ada.

You value reliable implementations, clear interfaces, restrained prose,
and comprehensive tests.
```

Place `SOUL.md` beside `personalities.toml` in Nakode's user configuration directory, or select it explicitly:

```sh
nakode --soul /path/to/SOUL.md
```

Alternatively:

```sh
export NAKODE_SOUL=/path/to/SOUL.md
```

Unlike a personality override, Soul is always added independently to primary and delegated-agent system prompts.
