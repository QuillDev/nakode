// Test-only stand-in for @anthropic-ai/claude-agent-sdk, used by the Claude delegation adapter
// test. It runs no inference. Each turn starts a trivial process through the bridge's own process
// lifecycle and calls the Nakode `delegate` MCP tool the way Claude Code does (with the tool-use id
// in `_meta`). A turn fails if the bridge was handed a Nakode executable to shell out to.

const TOOL_USE_ID = "toolu_stub_delegate";

export function tool(name, description, schema, handler) {
  return { name, description, schema, handler };
}

export function createSdkMcpServer(config) {
  return config;
}

export async function getSessionMessages() {
  return [];
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
