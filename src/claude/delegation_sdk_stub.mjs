// Test-only stand-in for @anthropic-ai/claude-agent-sdk, used by the Claude delegation adapter
// test. It runs no inference. Each turn starts a trivial process through the bridge's own process
// lifecycle, attempts Claude Code's native subagent routes, then calls the Nakode `delegate` MCP
// tool the way Claude Code does (with the tool-use id in `_meta`). A turn fails if the bridge was
// handed a Nakode executable to shell out to, or if any native subagent route is left open.

const TOOL_USE_ID = "toolu_stub_delegate";
const NATIVE_AGENT_ID = "toolu_native_agent";
const NATIVE_SUBAGENT_READ_ID = "toolu_native_subagent_read";
const NATIVE_DELEGATION_TOOLS = [
  "Agent",
  "Task",
  "Workflow",
  "SendMessage",
  "TeamCreate",
  "TeamDelete",
  "RemoteTrigger",
];

export function tool(name, description, schema, handler) {
  return { name, description, schema, handler };
}

export function createSdkMcpServer(config) {
  return config;
}

export async function getSessionMessages() {
  return [];
}

// Claude Code's permission order for one tool call: PreToolUse hooks first, then canUseTool.
// `agentId` is set only for calls made inside a native subagent.
async function harnessDecision(options, toolName, input, toolUseId, agentId) {
  const hookInput = {
    hook_event_name: "PreToolUse",
    tool_name: toolName,
    tool_input: input,
    tool_use_id: toolUseId,
    ...(agentId ? { agent_id: agentId, agent_type: "general-purpose" } : {}),
  };
  for (const matcher of options.hooks?.PreToolUse ?? []) {
    for (const hook of matcher.hooks) {
      const output = await hook(hookInput, toolUseId, {
        signal: options.abortController.signal,
      });
      const decision = output?.hookSpecificOutput?.permissionDecision;
      if (decision === "deny") {
        return {
          behavior: "deny",
          message: output.hookSpecificOutput.permissionDecisionReason,
        };
      }
      if (decision === "allow") {
        return { behavior: "allow" };
      }
    }
  }
  if (options.canUseTool) {
    return options.canUseTool(toolName, input, {
      signal: options.abortController.signal,
      suggestions: [],
      toolUseID: toolUseId,
      ...(agentId ? { agentID: agentId } : {}),
    });
  }
  return { behavior: "allow" };
}

// Attempts one native call. Returns the stream messages, or a failure when the call was allowed.
async function nativeAttempt(options, toolName, input, toolUseId, agentId) {
  const decision = await harnessDecision(options, toolName, input, toolUseId, agentId);
  if (decision.behavior !== "deny") {
    return { failure: `native ${toolName} (${toolUseId}) was allowed` };
  }
  return {
    messages: [
      {
        type: "assistant",
        ...(agentId ? { parent_tool_use_id: NATIVE_AGENT_ID } : {}),
        message: { content: [{ type: "tool_use", id: toolUseId, name: toolName, input }] },
      },
      {
        type: "user",
        message: {
          content: [
            {
              type: "tool_result",
              tool_use_id: toolUseId,
              content: decision.message,
              is_error: true,
            },
          ],
        },
      },
    ],
  };
}

export function query({ options }) {
  if (process.env.NAKODE_EXECUTABLE) {
    throw new Error("the Claude bridge must not receive a Nakode executable");
  }
  // Like the real SDK, the provider process starts before the stream is consumed.
  options.spawnClaudeCodeProcess({
    command: process.execPath,
    args: ["-e", ""],
    cwd: options.cwd,
    env: options.env,
    signal: options.abortController.signal,
  });
  return (async function* turn() {
    const offered = NATIVE_DELEGATION_TOOLS.filter(
      (name) => !options.disallowedTools?.includes(name),
    );
    if (offered.length > 0) {
      yield {
        type: "result",
        subtype: "error",
        is_error: true,
        result: `native delegation tools offered: ${offered.join(", ")}`,
      };
      return;
    }

    // A configuration path (settings, plugins, a future SDK) could still offer the native tool.
    const attempts = [
      await nativeAttempt(
        options,
        "Agent",
        { subagent_type: "general-purpose", description: "Read", prompt: "Read README.md" },
        NATIVE_AGENT_ID,
        null,
      ),
      // A native subagent started some other way, such as a forked skill.
      await nativeAttempt(
        options,
        "Read",
        { file_path: "README.md" },
        NATIVE_SUBAGENT_READ_ID,
        "native-subagent-1",
      ),
    ];
    for (const attempt of attempts) {
      if (attempt.failure) {
        yield { type: "result", subtype: "error", is_error: true, result: attempt.failure };
        return;
      }
      yield* attempt.messages;
    }

    const nakode = options.mcpServers?.nakode;
    const delegate = nakode?.tools?.find((candidate) => candidate.name === "delegate");
    if (!delegate) {
      yield { type: "result", subtype: "error", is_error: true, result: "delegate tool missing" };
      return;
    }
    const input = {
      agent: "repo-explorer",
      title: "Read delegation proof marker",
      task: "Read README.md and report the marker.",
    };
    yield {
      type: "assistant",
      message: {
        content: [
          { type: "tool_use", id: TOOL_USE_ID, name: "mcp__nakode__delegate", input },
        ],
      },
    };
    const output = await delegate.handler(input, {
      _meta: { "claudecode/toolUseId": TOOL_USE_ID },
      signal: options.abortController.signal,
    });
    yield {
      type: "user",
      message: {
        content: [
          {
            type: "tool_result",
            tool_use_id: TOOL_USE_ID,
            content: output.content,
            is_error: output.isError === true,
          },
        ],
      },
    };
    yield { type: "result", subtype: "success", result: "delegated" };
  })();
}
