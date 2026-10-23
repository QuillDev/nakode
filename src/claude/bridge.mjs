import {
  query,
  createSdkMcpServer,
  getSessionMessages,
  tool,
} from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod/v4";
import {
  UNRESTRICTED_BUILTIN_TOOLS,
  disallowedProviderTools,
  immediateProviderToolDecision,
  nativeDelegationDecision,
  preToolUseOutput,
  permissionContext,
  SDK_CANCELLATION_TEXT,
  TOOL_PERMISSION_GUIDANCE,
} from "./tool_policy.mjs";
import { providerProcessLifecycle } from "./process_lifecycle.mjs";
import { existsSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { delimiter, join } from "node:path";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

const sessions = new Map();
const runs = new Map();
const approvals = new Map();
const externalToolCalls = new Map();
const providerToolCalls = new Map();
const providerToolPermissions = new Map();
// SDK wire uuid of a delivered tool-result message -> the tool_call events it produced. A refusal
// fallback names retracted messages by this uuid, so their calls can be re-attributed.
const providerToolResultMessages = new Map();
const streamMessageIds = new Map();
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);

const SUBAGENT_RESULT = /^\[Subagent Result\] \[([^\]]+)\] \[([^\]]+)\]\n?([\s\S]*)$/;

/**
 * Ask the Nakode supervisor to delegate one bounded task, and wait for its terminal result.
 *
 * The request names only the agent, title and task plus the invoking Claude turn and tool call.
 * The supervisor binds this session's logical owner and parent run from the control plane and
 * routes the request through the workspace server, which owns policy, nesting limits, attribution
 * and cancellation. No process is started here and no identity is taken from this payload.
 */
function requestDelegation(session, turnId, request, signal) {
  return new Promise((resolve) => {
    const id = randomUUID();
    externalToolCalls.set(id, { resolve, sessionId: session.sessionId, turnId });
    const abort = () => {
      if (!externalToolCalls.has(id)) return;
      write({ event: "delegation_cancelled", id });
      resolveExternalToolCall(
        id,
        "Nakode delegation was cancelled with its Claude tool call",
        true,
      );
    };
    if (signal?.aborted) {
      abort();
      return;
    }
    signal?.addEventListener("abort", abort, { once: true });
    write({
      event: "delegation_request",
      id,
      turnId,
      callId: request.callId,
      agent: request.agent,
      title: request.title,
      task: request.task,
    });
  });
}

/** The text and failure flag of a resolved delegation tool result. */
function delegationOutcome(toolResult) {
  const text = toolResult.content?.find((block) => block.type === "text")?.text || "";
  return { text, failed: toolResult.isError === true };
}

// Nakode's own tools for Claude: `delegate` creates attributed sub-agents, and `return_image`
// attaches an image file from this machine to the transcript. Nakode reads and validates the file
// itself, so its bytes never pass through the provider.
function nakodeServer(session, turn, { delegate: withDelegate, returnImage, desktop }) {
  const tools = [];
  if (returnImage) tools.push(returnImageTool(session, turn));
  for (const name of desktop) tools.push(desktopTool(name, session, turn));
  if (withDelegate) tools.push(delegateTool(session, turn));
  return createSdkMcpServer({
    name: "nakode",
    version: "1.0.0",
    instructions:
      "Nakode tools: delegate bounded work to configured archetypes, attach images from this machine to the transcript, and use and record this agent's private desktop when it has one.",
    tools,
  });
}

/** The agent's private desktop: its screenshots come back as images the model sees. */
const DESKTOP_TOOLS = {
  desktop_screenshot: {
    description:
      "Capture this agent's private desktop (a virtual screen with a browser, not the owner's screen) and see it. Coordinates in the image are the ones desktop_action uses.",
    schema: {},
  },
  desktop_action: {
    description:
      "Use the mouse and keyboard on this agent's private desktop, then see the screen. Start a browser with `fstack-browser <url> &` from Bash. Take a desktop_screenshot first to find coordinates.",
    schema: {
      action: z.enum(["click", "double_click", "right_click", "move", "drag", "scroll", "type", "key"]),
      x: z.number().int().min(0).optional().describe("Screen pixel, from the left"),
      y: z.number().int().min(0).optional().describe("Screen pixel, from the top"),
      to_x: z.number().int().min(0).optional().describe("drag: where to release"),
      to_y: z.number().int().min(0).optional().describe("drag: where to release"),
      direction: z.enum(["up", "down", "left", "right"]).optional().describe("scroll direction"),
      amount: z.number().int().min(1).max(20).optional().describe("scroll steps"),
      text: z.string().optional().describe("type: the text to type"),
      keys: z.string().optional().describe("key: key names such as Return, Escape or ctrl+l"),
    },
  },
  screen_record: {
    description:
      "Record this agent's private desktop as a small MP4 for the owner. `start` begins recording (across turns, up to 5 minutes); do the work you want to show; `stop` compresses it and saves it to the Gallery with a poster frame.",
    schema: {
      action: z.enum(["start", "stop"]),
      name: z.string().optional().describe("start: a short file name, e.g. checkout-flow"),
    },
  },
};

function desktopTool(name, session, turn) {
  const { description, schema } = DESKTOP_TOOLS[name];
  return tool(name, description, schema, async (args) =>
    new Promise((resolve) => {
      const id = randomUUID();
      externalToolCalls.set(id, { resolve, sessionId: session.sessionId, turnId: turn.turnId });
      write({
        event: "desktop_request",
        id,
        turnId: turn.turnId,
        workspace: turn.workspace,
        tool: name,
        arguments: args,
      });
    }),
  );
}

function returnImageTool(session, turn) {
  return tool(
    "return_image",
    "Attach existing PNG, JPEG, GIF or WebP images to the assistant transcript so the owner sees them: files in this session's workspace, or under a `.tmp-gallery` directory (such as an agent's screenshots). Give one `path`, or several at once as `paths`; if any cannot be attached, none is. The bytes are retained for remote clients. Maximum 5 MiB per image and eight images per turn. Use this instead of pasting a file path.",
    {
      path: z
        .string()
        .min(1)
        .optional()
        .describe(
          "Workspace-relative image file, or an absolute path inside a `.tmp-gallery` directory",
        ),
      paths: z
        .array(z.string().min(1))
        .min(1)
        .max(8)
        .optional()
        .describe("Several such image files to attach at once, in order"),
    },
    async ({ path, paths }) =>
      new Promise((resolve) => {
        const id = randomUUID();
        externalToolCalls.set(id, {
          resolve,
          sessionId: session.sessionId,
          turnId: turn.turnId,
        });
        write({
          event: "image_return_request",
          id,
          turnId: turn.turnId,
          workspace: turn.workspace,
          model: session.model,
          ...(path === undefined ? {} : { path }),
          ...(paths === undefined ? {} : { paths }),
        });
      }),
  );
}

// Claude's provider identity for Nakode's canonical `nakode_agent` tool, with the same arguments.
function delegateTool(session, turn) {
  return tool(
    "delegate",
    "Delegate one bounded task to a configured Nakode agent from the current Nakode agent catalogue and wait for its attributed terminal result.",
    {
      agent: z
        .string()
        .trim()
        .min(1)
        .describe("Configured agent slug from the current Nakode catalogue."),
      title: z
        .string()
        .trim()
        .min(1)
        .max(120)
        .describe("Required concise title describing this agent's specific goal, not its role or setup instructions."),
      task: z
        .string()
        .trim()
        .min(1)
        .describe("Concrete bounded task for the delegated agent."),
    },
    async (args, extra) =>
      requestDelegation(
        session,
        turn.turnId,
        {
          // Claude Code names the invoking tool_use block; Nakode anchors the child run to it.
          callId: extra?._meta?.["claudecode/toolUseId"] || randomUUID(),
          agent: args.agent,
          title: args.title,
          task: args.task,
        },
        extra?.signal,
      ),
  );
}

function externalToolShape(definition) {
  const schema = JSON.parse(definition.input_schema_json || "{}");
  if (schema?.type !== "object" || Array.isArray(schema?.properties)) {
    throw new Error(
      `External tool ${definition.name} must have an object input schema`,
    );
  }
  const required = new Set(schema.required || []);
  return Object.fromEntries(
    Object.entries(schema.properties || {}).map(([name, property]) => {
      const value = z.fromJSONSchema(property);
      return [name, required.has(name) ? value : value.optional()];
    }),
  );
}

function resolveExternalToolCall(id, output, failed, image) {
  const pending = externalToolCalls.get(id);
  if (!pending) return;
  externalToolCalls.delete(id);
  pending.resolve({
    isError: failed === true,
    content: [
      { type: "text", text: output || "" },
      ...(image?.data ? [{ type: "image", data: image.data, mimeType: image.mimeType }] : []),
    ],
  });
}

function interruptExternalToolCalls(predicate, message) {
  for (const [id, pending] of externalToolCalls) {
    if (!predicate(pending)) continue;
    externalToolCalls.delete(id);
    pending.resolve({
      isError: true,
      content: [{ type: "text", text: message }],
    });
  }
}

function externalToolsServer(session, turnId) {
  return createSdkMcpServer({
    name: "nakode_external",
    version: "1.0.0",
    instructions:
      "Tools executed by the Nakode client that owns this logical session.",
    tools: session.externalTools.map((definition) =>
      tool(
        definition.name,
        definition.description,
        externalToolShape(definition),
        async (args) =>
          new Promise((resolve) => {
            const id = randomUUID();
            externalToolCalls.set(id, {
              resolve,
              sessionId: session.sessionId,
              turnId,
            });
            write({
              event: "external_tool_request",
              id,
              turnId,
              name: definition.name,
              argumentsJson: JSON.stringify(args),
            });
          }),
      ),
    ),
  });
}

function externalToolNames(session) {
  return session.externalTools.map(
    (definition) => `mcp__nakode_external__${definition.name}`,
  );
}

function providerToolName(name) {
  const prefix = "mcp__nakode_external__";
  return typeof name === "string" && name.startsWith(prefix)
    ? name.slice(prefix.length)
    : name || "Tool";
}

const NAKODE_MCP_TOOLS = new Set([
  "mcp__nakode__delegate",
  "mcp__nakode__return_image",
  "mcp__nakode__desktop_screenshot",
  "mcp__nakode__desktop_action",
  "mcp__nakode__screen_record",
]);

function effectiveAllowedTools(session) {
  const external = externalToolNames(session);
  if (session.replaceBuiltinTools) {
    return [
      ...external,
      ...(session.allowedTools?.filter((name) => NAKODE_MCP_TOOLS.has(name)) ?? []),
    ];
  }
  if (session.allowedTools === null) return null;
  return [...session.allowedTools, ...external].filter(
    (name) => !session.deniedTools.includes(name),
  );
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function nativeAgentHistoryId(agentId, message, index, blockIndex) {
  return message.uuid
    ? `native:${agentId}:${message.uuid}${blockIndex === 0 ? "" : `:${blockIndex}`}`
    : `native:${agentId}:${index}:${blockIndex}`;
}

function nativeAgentBlockText(block) {
  if (block.type === "text" && typeof block.text === "string") return block.text;
  if (block.type === "tool_use") {
    return JSON.stringify({ toolUseId: block.id, tool: block.name, input: block.input });
  }
  if (block.type === "tool_result") {
    return JSON.stringify({ toolUseId: block.tool_use_id, output: block.content });
  }
  return null;
}

async function nativeAgentHistory(workspace, sessionId) {
  if (!workspace || !sessionId) return [];
  const root = process.env.CLAUDE_CONFIG_DIR || join(homedir(), ".claude");
  const project = workspace.replace(/[^a-zA-Z0-9]/g, "-");
  const directory = join(root, "projects", project, sessionId, "subagents");
  let files;
  try {
    files = await readdir(directory);
  } catch {
    return [];
  }
  const history = [];
  for (const file of files
    .filter((name) => /^agent-.*\.jsonl$/.test(name))
    .sort()) {
    let source;
    try {
      source = await readFile(join(directory, file), "utf8");
    } catch {
      continue;
    }
    const fallbackAgentId = file.slice("agent-".length, -".jsonl".length);
    for (const [index, line] of source.split("\n").entries()) {
      if (!line.trim()) continue;
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        continue;
      }
      const content = message?.message?.content;
      const blocks =
        typeof content === "string"
          ? [{ type: "text", text: content }]
          : content;
      if (!Array.isArray(blocks)) continue;
      const agentId = message.agentId || fallbackAgentId;
      for (const [blockIndex, block] of blocks.entries()) {
        const text = nativeAgentBlockText(block);
        if (text === null) continue;
        history.push({
          turnId: message.promptId || `${sessionId}:native:${agentId}`,
          id: nativeAgentHistoryId(agentId, message, index, blockIndex),
          kind: "tool",
          title: `NativeAgent:${agentId}`,
          body: JSON.stringify({
            agentId,
            role: message?.message?.role || message.type,
            model: message?.message?.model || null,
            text,
            stopReason: message?.message?.stop_reason || null,
            usage: message?.message?.usage || null,
          }),
          status: "complete",
        });
      }
    }
  }
  return history;
}

async function emitNativeAgentHistory(workspace, sessionId, turnId) {
  for (const item of await nativeAgentHistory(workspace, sessionId)) {
    write({
      event: "tool_call",
      turnId,
      callId: item.id,
      name: item.title,
      status: "completed",
      result: JSON.parse(item.body),
    });
  }
}

function savedHistory(messages, sessionId, permissions = new Map()) {
  const history = [];
  const historicalTools = new Map();
  let ordinal = 0;
  for (const message of messages || []) {
    const savedContent = message?.message?.content;
    const content =
      typeof savedContent === "string"
        ? [{ type: "text", text: savedContent }]
        : savedContent;
    if (!Array.isArray(content)) continue;
    const role = message.message.role || message.type;
    for (const [blockIndex, block] of content.entries()) {
      let kind;
      let title;
      let body;
      let permission;
      if (block.type === "text") {
        kind = role === "user" ? "user" : "assistant";
        title = role === "user" ? "YOU" : "CLAUDE";
        body = block.text || "";
      } else if (block.type === "thinking" && block.thinking) {
        kind = "reasoning";
        title = "THINKING";
        body = block.thinking || "";
      } else if (block.type === "tool_use") {
        kind = "tool";
        title = providerToolName(block.name);
        historicalTools.set(block.id, { name: title, input: block.input });
        body = JSON.stringify({ input: block.input });
      } else if (block.type === "tool_result") {
        kind = "tool";
        const started = historicalTools.get(block.tool_use_id);
        title = started?.name || "Tool";
        body = JSON.stringify({ input: started?.input, output: block.content });
        permission = permissions.get(block.tool_use_id)?.permission;
        if (!permission && block.is_error && block.content === SDK_CANCELLATION_TEXT) {
          permission = {
            decision: "unknown", source: "sdk_unknown",
            reason: "Retained SDK cancellation text has no authenticated owner-stop provenance. Do not retry automatically.",
          };
        }
      } else {
        continue;
      }
      // Text and thinking blocks have no provider block id. The API message id plus the content-array
      // index is the durable counterpart of stream_event.index, so resume hydration patches the same
      // logical blocks in the same order as the live stream. The SDK wrapper UUID is deliberately not
      // used: partial stream events each receive a different wrapper UUID. Tool results retain their
      // tool-use id because they are status/body updates to the call, not new timeline rows.
      const messageId =
        message.message.id || message.uuid || `${sessionId}:message:${ordinal}`;
      history.push({
        turnId: messageId,
        id:
          block.id ||
          block.tool_use_id ||
          `claude:${messageId}:${blockIndex}`,
        kind,
        title,
        body,
        status: block.is_error ? "failed" : "complete",
        ...(permission ? {
          permission,
          denied: permission.decision === "deny",
          denialReason: permission.decision === "allow" ? undefined : permission.reason,
        } : {}),
      });
      ordinal += 1;
    }
  }
  return history;
}

async function createSession(command, resumed) {
  const sessionId = command.sessionId || randomUUID();
  const instructions = command.instructions || "";
  const policy = archetypePolicy(instructions);
  const securityValidator = validatorSession(instructions);
  const structuredAllowed = securityValidator
    ? []
    : authoritativeAllowedTools(command.allowedBuiltinTools);
  sessions.set(sessionId, {
    sessionId,
    resumed,
    started: resumed,
    instructions: command.instructions || "",
    model: command.model || "sonnet",
    options: {},
    ownerSessionId: command.ownerSessionId || null,
    securityValidator,
    validationEnabled: Boolean(command.ownerSessionId) && !securityValidator,
    delegationEnabled: policy?.canDelegate ?? true,
    allowedTools: securityValidator
      ? []
      : (structuredAllowed ?? policy?.allowedTools ?? null),
    deniedTools: policy?.deniedTools ?? [],
    maxTurns: command.maxTurns || null,
    finalizationReserveTurns: Math.max(
      0,
      Math.min(command.finalizationReserveTurns || 0, command.maxTurns || 0),
    ),
    inferenceTurns: 0,
    finalizing: false,
    timeoutSeconds: command.timeoutSeconds || null,
    externalTools: Array.isArray(command.externalTools)
      ? command.externalTools
      : [],
    replaceBuiltinTools: command.replaceBuiltinTools === true,
  });
  const history = resumed ? ((await readHistory(command.workspace, sessionId)) ?? []) : [];
  write({
    event: resumed ? "session_resumed" : "session_created",
    requestId: command.requestId,
    sessionId,
    model: command.model || "sonnet",
    history,
  });
}

/** The session's history as a resume rebuilds it; null, with a diagnostic, when unreadable. */
async function readHistory(workspace, sessionId, permissions) {
  try {
    return [
      ...savedHistory(await getSessionMessages(sessionId, { dir: workspace }), sessionId, permissions),
      ...(await nativeAgentHistory(workspace, sessionId)),
    ];
  } catch (error) {
    write({
      event: "diagnostic",
      message: `could not read Claude session history: ${errorMessage(error)}`,
    });
    return null;
  }
}

function validatorSession(instructions) {
  return instructions.includes("[Nakode Security Validator]");
}

const POLICY_TOOL_NAMES = new Map([
  ["read", ["Read"]],
  ["grep", ["Grep"]],
  ["find", ["Glob"]],
  ["bash", ["Bash"]],
  ["write", ["Write"]],
  ["edit", ["Edit"]],
  ["ask", ["AskUserQuestion"]],
]);

const CLAUDE_POLICY_TOOLS = new Set([
  "Read",
  "Grep",
  "Glob",
  "Bash",
  "Write",
  "Edit",
  "AskUserQuestion",
  "mcp__nakode__delegate",
]);

/**
 * Accept Nakode's authoritative, already-projected Claude tool boundary.
 *
 * `null` means the caller supplied no structured boundary, so legacy prompt-policy parsing remains
 * available. An explicit empty array is deny-all. Exact provider identities only are accepted;
 * unknown names remain denied. Unsupported canonical names are reported by the upstream provider
 * projection and inspection view before this already-projected boundary reaches the bridge.
 */
function authoritativeAllowedTools(configured) {
  if (!Array.isArray(configured)) return null;
  return [
    ...new Set(
      configured.filter(
        (name) => typeof name === "string" && CLAUDE_POLICY_TOOLS.has(name),
      ),
    ),
  ];
}

function archetypePolicy(instructions) {
  const start = instructions.lastIndexOf("[Nakode Archetype Policy]");
  if (start < 0) return null;
  const policy = instructions.slice(start);
  const profile = /Tool profile: (\w+)/.exec(policy)?.[1];
  const configured = /Allowed tools: ([^\n]+)/.exec(policy)?.[1] || "none";
  const denied = /Denied tools: ([^\n]+)/.exec(policy)?.[1] || "none";
  const names = configured === "none" ? [] : configured.split(",").map((name) => name.trim());
  const mapped = names.flatMap((name) => POLICY_TOOL_NAMES.get(name) ?? []);
  const deniedTools = denied === "none"
    ? []
    : denied
        .split(",")
        .flatMap((name) => POLICY_TOOL_NAMES.get(name.trim()) ?? []);
  let allowedTools;
  if (profile === "none") allowedTools = [];
  // Empty custom policy preserves legacy definitions; restrictive profiles never do.
  else if (profile === "custom" && mapped.length === 0) allowedTools = null;
  else allowedTools = mapped;
  return {
    allowedTools,
    deniedTools,
    canDelegate: policy.includes("Recursive delegation is allowed"),
  };
}

// Nakode sessions run unattended, like Codex's never-ask full-access sessions: archetype allow and
// deny lists still apply through the PreToolUse hook, but Claude Code's own permission prompts and
// auto-mode classifier do not. Claude Code's `permissions.defaultMode` is the owner's choice for
// interactive Claude Code and does not govern Nakode; NAKODE_CLAUDE_PERMISSION_MODE does.
const PERMISSION_MODES = new Set([
  "bypassPermissions",
  "auto",
  "acceptEdits",
  "default",
  "plan",
]);

function permissionMode() {
  const configured = process.env.NAKODE_CLAUDE_PERMISSION_MODE?.trim();
  return configured && PERMISSION_MODES.has(configured)
    ? configured
    : "bypassPermissions";
}

function parseValidatorResult(result) {
  if (result.status !== "completed") {
    return {
      verdict: "escalate",
      rationale: `Configured Sonnet validator unavailable: ${result.error || "delegated run failed"}`,
      validated: false,
      ...result,
    };
  }
  let parsed;
  try {
    parsed = JSON.parse(result.result);
  } catch {
    return {
      verdict: "escalate",
      rationale: "Configured Sonnet validator returned malformed output.",
      validated: false,
      ...result,
    };
  }
  if (
    !["allow", "reject", "escalate"].includes(parsed?.verdict) ||
    typeof parsed?.rationale !== "string" ||
    !parsed.rationale.trim()
  ) {
    return {
      verdict: "escalate",
      rationale: "Configured Sonnet validator omitted a clear verdict or rationale.",
      validated: false,
      ...result,
    };
  }
  return { ...result, ...parsed, validated: true };
}

// The validator is an attributed Nakode run like any delegation: the supervisor binds it to this
// session's owner and active run, so a delegated Claude child is held to its own delegation policy.
async function securityValidation(session, turnId, toolName, input, options) {
  const archetype =
    process.env.NAKODE_SECURITY_VALIDATOR_AGENT || "security-validator";
  const task = `Return JSON only as {"verdict":"allow|reject|escalate","rationale":"..."}.\nDo not use tools or delegate. Independently validate this security-sensitive proposed Claude Code operation against the owner's repository task. Allow only when intent and bounded impact are clear; reject security-control bypass, credential exposure, exfiltration, and destructive out-of-scope actions; escalate uncertainty.\n\n${JSON.stringify({ tool: toolName, input, reason: options.decisionReason || options.description || "Claude auto mode requested review." })}`;
  try {
    const { text, failed } = delegationOutcome(
      await requestDelegation(
        session,
        turnId,
        {
          callId: `security:${options.toolUseID || randomUUID()}`,
          agent: archetype,
          title: `Security validation of ${toolName}`.slice(0, 120),
          task,
        },
        options.signal,
      ),
    );
    const match = text.match(SUBAGENT_RESULT);
    return parseValidatorResult({
      runId: match?.[1] || null,
      archetype: match?.[2] || archetype,
      status: failed ? "failed" : "completed",
      result: failed ? null : (match?.[3]?.trim() ?? text),
      error: failed ? text || "Nakode delegation failed" : null,
    });
  } catch (error) {
    return {
      verdict: "escalate",
      rationale: `Configured Sonnet validator unavailable: ${errorMessage(error)}`,
      validated: false,
      runId: null,
      archetype,
    };
  }
}

function interruptedPermission(turnId, mode) {
  // A hook/control-request signal is SDK-owned: timeout and batch cancellation can abort it too.
  // Only the separately held runtime controller establishes a Nakode interruption.
  const runtimeSignal = runs.get(turnId)?.signal;
  const source = runtimeSignal?.aborted
    ? runtimeSignal.reason?.source || "runtime_interrupt"
    : "sdk_abort";
  return {
    decision: "interrupted",
    source,
    reason: source === "sdk_abort"
      ? "Claude SDK cancelled the permission request; no owner decision was received."
      : `Nakode stopped this turn (${source}); do not continue its operations.`,
    mode,
  };
}

function permissionHandler(turnId, session, mode, allowedTools) {
  return async (toolName, input, options) => {
    const callId = options.toolUseID || randomUUID();
    const record = (permission) => {
      permission = { ...permission, reason: permission.reason.slice(0, 4096) };
      providerToolPermissions.set(callId, {
        name: providerToolName(toolName), turnId, permission,
      });
      return permission;
    };
    const deny = (reason, source = "policy", permission = null) => {
      permission = record(permission || { decision: "deny", source, reason, mode });
      write({
        event: "tool_call",
        turnId,
        callId,
        name: providerToolName(toolName) || "UnknownProviderTool",
        status: "error",
        args: input,
        external: externalToolNames(session).includes(toolName),
        denied: permission.decision === "deny",
        denialReason: permission.reason,
        permission,
      });
      return {
        behavior: "deny", message: permissionContext(permission),
        toolUseID: callId, permission,
      };
    };
    const allow = (source, reason) => ({
      behavior: "allow", updatedInput: input,
      permission: record({ decision: "allow", source, reason, mode }),
    });
    if (options.signal.aborted || runs.get(turnId)?.signal.aborted) {
      const permission = interruptedPermission(turnId, mode);
      return { ...deny(permission.reason, permission.source, permission), interrupt: true };
    }
    if (session.finalizing) {
      return deny(
        "Protected finalization reserve denies new tool use; synthesize the best final or partial report from retained evidence.",
        "finalization",
      );
    }
    const immediate = immediateProviderToolDecision(
      session.deniedTools, allowedTools, mode, toolName, options.agentID,
    );
    if (immediate?.behavior === "deny") return deny(immediate.message);
    if (immediate?.behavior === "allow") return allow("policy", "Nakode tool policy allows this call.");
    if (externalToolNames(session).includes(toolName)) {
      return allow("external_tool", "The external tool's application authorization remains authoritative.");
    }
    if (toolName !== "AskUserQuestion" && mode === "auto" && !options.matchedAskRule) {
      if (!session.validationEnabled) {
        return deny("Recursive security validation was prevented in a delegated validator context.");
      }
      const result = await securityValidation(session, turnId, toolName, input, options);
      write({
        event: "tool_call", turnId, callId: `security:${callId}`,
        name: "SecurityValidation",
        status: result.verdict === "allow" ? "completed" : "error",
        result: {
          verdict: result.verdict, rationale: result.rationale,
          validated: result.validated, validator: result.archetype, runId: result.runId,
        },
      });
      if (options.signal.aborted || runs.get(turnId)?.signal.aborted) {
        const permission = interruptedPermission(turnId, mode);
        return { ...deny(permission.reason, permission.source, permission), interrupt: true };
      }
      if (result.verdict === "allow") return allow("validator", result.rationale);
      const source = !result.validated ? "validator_error"
        : result.verdict === "reject" ? "validator_reject" : "validator_escalation";
      return deny(result.rationale, source);
    }
    return new Promise((resolve) => {
      let settled = false;
      const finish = (result) => {
        if (settled) return;
        settled = true;
        options.signal.removeEventListener("abort", abort);
        approvals.delete(callId);
        if (result.behavior === "allow" && runs.get(turnId)?.signal.aborted) {
          result = { behavior: "deny", interrupt: true, permission: interruptedPermission(turnId, mode) };
        }
        if (result.behavior === "deny") {
          const permission = result.permission || {
            decision: "deny", source: "approval_declined", reason: result.message, mode,
          };
          resolve({ ...result, ...deny(permission.reason, permission.source, permission) });
        } else {
          resolve({ ...result, ...allow("approval", "The pending approval was accepted.") });
        }
      };
      const abort = () => {
        const permission = interruptedPermission(turnId, mode);
        finish({ behavior: "deny", interrupt: true, permission });
      };
      if (options.signal.aborted) {
        abort();
        return;
      }
      options.signal.addEventListener("abort", abort, { once: true });
      approvals.set(callId, { finish, suggestions: options.suggestions || [] });
      const permission = record({
        decision: "pending", source: "approval", reason: "Awaiting approval; no decision has been received.", mode,
      });
      write({
        event: "approval_request", turnId, approvalId: callId, toolName, input, permission,
        title: options.title || options.displayName || `Allow ${toolName}?`,
        description: options.description || options.decisionReason || "",
      });
    });
  };
}

// This supported SDK hook supplies model-facing provenance as well as the host audit. It never
// returns retry:true, changes a permission, or rewrites the provider's raw tool result.
function permissionBatchHook(turnId) {
  return (input) => {
    const permissions = (input.tool_calls || []).flatMap((call) => {
      const recorded = providerToolPermissions.get(call.tool_use_id);
      if (recorded && recorded.turnId !== turnId) return [];
      const attributed = recorded && recorded.permission.decision !== "allow";
      if (attributed) return [{ callId: call.tool_use_id, ...recorded.permission }];
      // The batch can run before the SDK delivers the tool-result message, so the generic
      // cancellation text is checked here too. It never becomes an owner stop.
      if (!isSdkCancellation(call.tool_response)) return [];
      return [{ callId: call.tool_use_id, ...unattributedCancellation(turnId, recorded?.permission.mode) }];
    });
    return permissions.length === 0 ? {} : {
      hookSpecificOutput: {
        hookEventName: "PostToolBatch",
        additionalContext: permissionContext(permissions),
      },
    };
  };
}

function sdkPermissionDenied(turnId, message) {
  const existing = providerToolPermissions.get(message.tool_use_id);
  if (existing?.permission.decision === "deny") return;
  providerToolPermissions.set(message.tool_use_id, {
    name: providerToolName(message.tool_name), turnId,
    permission: {
      decision: "deny", source: "sdk_policy",
      reason: (message.decision_reason || message.message || "Claude SDK denied the operation.").slice(0, 4096),
      sdkReasonType: message.decision_reason_type || null,
    },
  });
}

function preToolUseHook(turnId, session, mode, allowedTools) {
  const authorize = permissionHandler(turnId, session, mode, allowedTools);
  return async (hookInput, toolUseID, options) => {
    const result = await authorize(hookInput.tool_name, hookInput.tool_input, {
      signal: options.signal,
      suggestions: [],
      toolUseID: toolUseID || hookInput.tool_use_id,
      agentID: hookInput.agent_id,
    });
    return preToolUseOutput(result);
  };
}

// Prompting modes still decide ordinary tools through canUseTool. Settings `allow` rules can approve
// a tool before canUseTool is asked, but never before PreToolUse hooks, so native delegation is
// refused here in every mode.
function nativeDelegationGuard(turnId, session, mode, allowedTools) {
  const authorize = permissionHandler(turnId, session, mode, allowedTools);
  return async (hookInput, toolUseID, options) => {
    const native = nativeDelegationDecision(hookInput.tool_name, hookInput.agent_id);
    if (!native) {
      return {};
    }
    const result = await authorize(hookInput.tool_name, hookInput.tool_input, {
      signal: options.signal,
      suggestions: [],
      toolUseID: toolUseID || hookInput.tool_use_id,
      agentID: hookInput.agent_id,
    });
    return preToolUseOutput(result);
  };
}

function emitContent(turnId, message) {
  const content = message?.message?.content;
  if (!Array.isArray(content)) return;
  for (const block of content) {
    if (block.type === "tool_use") {
      const name = providerToolName(block.name);
      const external = block.name?.startsWith("mcp__nakode_external__") === true;
      providerToolCalls.set(block.id, { name, input: block.input, external });
      write({
        event: "tool_call",
        turnId,
        callId: block.id,
        name,
        status: "running",
        args: block.input,
        external,
      });
    }
  }
}

function emitUserToolResults(turnId, message) {
  const content = message?.message?.content;
  if (!Array.isArray(content)) return;
  for (const block of content) {
    if (block.type === "tool_result") {
      const started = providerToolCalls.get(block.tool_use_id);
      providerToolCalls.delete(block.tool_use_id);
      let recorded = providerToolPermissions.get(block.tool_use_id);
      // A delivered replacement result replaces the retraction of the earlier one.
      if (recorded?.permission.source === "sdk_retracted") {
        recorded = undefined;
        providerToolPermissions.delete(block.tool_use_id);
      }
      // This exact SDK fallback is also used for internal cancellation/skipped siblings. Matching
      // it only establishes UNKNOWN provenance, never an owner stop or a policy decision.
      if (block.is_error && isSdkCancellation(block.content) &&
          (!recorded || recorded.permission.decision === "allow")) {
        const permission = unattributedCancellation(turnId, recorded?.permission.mode);
        recorded = { name: started?.name || "Tool", turnId, permission };
        providerToolPermissions.set(block.tool_use_id, recorded);
      }
      const permission = recorded?.permission;
      const event = {
        event: "tool_call",
        turnId,
        callId: block.tool_use_id,
        name: recorded?.name || started?.name || "Tool",
        status: block.is_error ? "error" : "completed",
        result: { input: started?.input, output: block.content },
        external: started?.external === true,
        denied: permission?.decision === "deny",
        denialReason: permission && permission.decision !== "allow" ? permission.reason : undefined,
        permission,
      };
      write(event);
      if (message.uuid) {
        const events = providerToolResultMessages.get(message.uuid) || [];
        providerToolResultMessages.set(message.uuid, [...events, event]);
      }
    }
  }
}

function isSdkCancellation(output) {
  if (Array.isArray(output) && output.length === 1 && output[0]?.type === "text") {
    return output[0].text === SDK_CANCELLATION_TEXT;
  }
  return output === SDK_CANCELLATION_TEXT;
}

// Generic SDK cancellation text proves only that the SDK did not run the call to completion.
// A Nakode stop always aborts the runtime controller first, so only that establishes a stop.
function unattributedCancellation(turnId, mode) {
  if (runs.get(turnId)?.signal.aborted) return interruptedPermission(turnId, mode);
  return {
    decision: "unknown",
    source: "sdk_unknown",
    reason: "Claude SDK reported cancellation, but Nakode received no stop or denial for this call, so it is not an owner stop. Execution outcome is unconfirmed; check whether it took effect before relying on it, and do not replay it automatically.",
    mode,
  };
}

// A model refusal fallback discards the refused response, including tool results the SDK had
// already delivered as cancelled. Those calls were neither denied nor stopped by anyone.
function retractToolResults(turnId, uuids, cause) {
  let retracted = 0;
  for (const uuid of uuids || []) {
    const events = providerToolResultMessages.get(uuid) || [];
    providerToolResultMessages.delete(uuid);
    for (const event of events) {
      if (event.turnId !== turnId) continue;
      const recorded = providerToolPermissions.get(event.callId);
      const decision = recorded?.permission.decision;
      if (decision && decision !== "allow" && recorded.permission.source !== "sdk_unknown") continue;
      const permission = {
        decision: "unknown",
        source: "sdk_retracted",
        reason: `Claude SDK retracted this call's result during ${cause}. Nobody stopped or denied it; its outcome is unconfirmed, so check whether it took effect before relying on it, and do not replay it automatically.`,
        mode: recorded?.permission.mode,
      };
      providerToolPermissions.set(event.callId, { name: event.name, turnId, permission });
      write({ ...event, denied: false, denialReason: permission.reason, permission });
      retracted += 1;
    }
  }
  return retracted;
}

const QUIET_SDK_EVENTS = new Set(["thinking_tokens", "hook_progress", "task_progress"]);

// Nakode cannot see why the SDK cancels without attribution. Recording the SDK lifecycle events
// around such a cancellation (task notifications, retries, refusals, hooks) makes the next one
// diagnosable from the session transcript instead of private provider storage.
function sdkLifecycleLog() {
  const started = Date.now();
  const events = [];
  let cancelled = 0;
  const note = (label) => {
    if (!label || QUIET_SDK_EVENTS.has(label)) return;
    events.push(`${label} +${((Date.now() - started) / 1000).toFixed(1)}s`);
    if (events.length > 20) events.shift();
  };
  return {
    note,
    noteUnattributedCancellations(message) {
      const content = message?.message?.content;
      if (!Array.isArray(content)) return;
      for (const block of content) {
        const recorded = providerToolPermissions.get(block.tool_use_id);
        if (block.type !== "tool_result" || recorded?.permission.source !== "sdk_unknown") continue;
        cancelled += 1;
        note(`unattributed cancellation of ${recorded.name}`);
      }
    },
    report(turnId) {
      if (cancelled === 0) return;
      write({
        event: "warning",
        turnId,
        message: `Claude SDK cancelled ${cancelled} tool call(s) without a Nakode stop or denial; this was not an owner stop. SDK lifecycle this turn: ${events.join(", ")}.`,
      });
    },
  };
}

function sdkRefusal(turnId, message) {
  const model = message.original_model || "Claude";
  const category = message.api_refusal_category ? ` (${message.api_refusal_category})` : "";
  if (message.subtype === "model_refusal_fallback") {
    const retracted = retractToolResults(turnId, message.retracted_message_uuids, "a model refusal fallback");
    write({
      event: "warning",
      message: `${model} refused this response${category}; Claude SDK retried on ${message.fallback_model || "a fallback model"} and retracted ${retracted} tool result(s). This was not an owner stop.`,
    });
    return;
  }
  write({
    event: "warning",
    message: `${model} refused this turn${category} and no fallback model ran, so the turn ended. This was not an owner stop.`,
  });
}

function emitStreamEvent(turnId, message, session) {
  // A Claude turn contains several API messages around tool results. The SDK gives every partial
  // wrapper a fresh UUID, so content deltas must instead inherit the API message id announced by
  // message_start. Otherwise every streamed chunk becomes a separate transcript row.
  const event = message.event;
  if (event?.type === "message_start") {
    if (event.message?.role === "assistant") {
      session.inferenceTurns += 1;
      const softTurnLimit = session.maxTurns
        ? Math.max(1, session.maxTurns - session.finalizationReserveTurns)
        : null;
      if (
        !session.finalizing &&
        session.finalizationReserveTurns > 0 &&
        softTurnLimit !== null &&
        session.inferenceTurns >= softTurnLimit
      ) {
        session.finalizing = true;
        write({
          event: "warning",
          message: `Protected finalization reserve started with ${session.maxTurns - session.inferenceTurns} turn(s) remaining. New tools are disabled; produce the best final or partial report and a bounded continuation proposition now.`,
        });
      }
    }
    streamMessageIds.set(
      turnId,
      event.message?.id || message.uuid || `${turnId}:message`,
    );
    return;
  }
  if (event?.type === "message_stop") {
    streamMessageIds.delete(turnId);
    return;
  }
  if (event?.type === "content_block_start") {
    const block = event.content_block;
    if (block?.type === "tool_use") {
      const name = providerToolName(block.name);
      const external = block.name?.startsWith("mcp__nakode_external__") === true;
      providerToolCalls.set(block.id, { name, input: block.input, external });
      write({
        event: "tool_call",
        turnId,
        callId: block.id,
        name,
        status: "running",
        args: block.input,
        external,
      });
    }
    return;
  }
  if (event?.type !== "content_block_delta") return;
  if (event.delta?.type === "text_delta" && event.delta.text) {
    write({
      event: "delta",
      turnId,
      messageId: streamMessageIds.get(turnId) || `${turnId}:message`,
      blockIndex: event.index,
      kind: "assistant",
      text: event.delta.text,
    });
  } else if (event.delta?.type === "thinking_delta" && event.delta.thinking) {
    write({
      event: "delta",
      turnId,
      messageId: streamMessageIds.get(turnId) || `${turnId}:message`,
      blockIndex: event.index,
      kind: "reasoning",
      text: event.delta.thinking,
    });
  }
}

// The SDK closes Claude Code's stdin at the first result of a string prompt. A background task
// (background Bash, Monitor) can then start another turn in the same process, where every SDK
// callback, including Nakode's PreToolUse hook, fails with "Stream closed". Claude Code reports
// that as the generic user-cancellation text, so the agent stops with no owner stop.
// Streaming input therefore stays open until no background task can start another turn.
const BACKGROUND_SETTLE_GRACE_MS = 2000;

function turnInput(command) {
  let settle;
  const settled = new Promise((resolve) => {
    settle = resolve;
  });
  let liveTasks = 0;
  let idle = false;
  let grace = null;
  const clearGrace = () => {
    if (grace !== null) clearTimeout(grace);
    grace = null;
  };
  const close = () => {
    clearGrace();
    settle();
  };
  const settleWhenIdle = () => {
    if (!idle || liveTasks > 0 || grace !== null) return;
    // A finished task normally announces its follow-up turn first; the grace absorbs reordering.
    grace = setTimeout(close, BACKGROUND_SETTLE_GRACE_MS);
  };
  const prompt = (async function* () {
    yield {
      type: "user",
      session_id: command.sessionId,
      parent_tool_use_id: null,
      message: {
        role: "user",
        content: [
          ...(command.images || []),
          ...(command.prompt ? [{ type: "text", text: command.prompt }] : []),
        ],
      },
    };
    await settled;
  })();
  return {
    prompt,
    close,
    observe(message) {
      if (message.type === "result") {
        idle = true;
        if (liveTasks === 0) close();
      } else if (message.type === "system" && message.subtype === "background_tasks_changed") {
        liveTasks = (message.tasks || []).filter((task) => !task.ambient).length;
        settleWhenIdle();
      } else if (message.type === "system" && message.subtype === "task_notification") {
        idle = false;
        clearGrace();
      } else if (message.type === "stream_event" || message.type === "assistant") {
        idle = false;
        clearGrace();
      }
    },
  };
}

async function sendTurn(command) {
  const session = sessions.get(command.sessionId);
  if (!session)
    throw new Error(`Claude session ${command.sessionId} is not attached`);

  session.inferenceTurns = 0;
  session.finalizing = false;
  const abortController = new AbortController();
  runs.set(command.turnId, abortController);

  const model = command.model || session.model;
  session.model = model;
  const mode = session.securityValidator
    ? "dontAsk"
    : permissionMode();
  const processLifecycle = providerProcessLifecycle(command.oauthAccessToken);
  const allowedTools = effectiveAllowedTools(session);
  const mcpServers = {};
  const allows = (name) => allowedTools === null || allowedTools.includes(name);
  const withDelegate = Boolean(
    session.ownerSessionId &&
      session.validationEnabled &&
      session.delegationEnabled &&
      allows("mcp__nakode__delegate"),
  );
  const returnImage = allows("mcp__nakode__return_image");
  // Offered where this machine gives agents a desktop.
  const desktop = process.env.DISPLAY
    ? Object.keys(DESKTOP_TOOLS).filter((name) => allows(`mcp__nakode__${name}`))
    : [];
  if (withDelegate || returnImage || desktop.length > 0) {
    mcpServers.nakode = nakodeServer(
      session,
      { turnId: command.turnId, workspace: command.workspace },
      { delegate: withDelegate, returnImage, desktop },
    );
  }
  if (session.externalTools.length > 0) {
    mcpServers.nakode_external = externalToolsServer(session, command.turnId);
  }
  // Always a positive list: Claude Code otherwise offers every built-in, including new ones.
  const builtinTools = allowedTools === null
    ? [...UNRESTRICTED_BUILTIN_TOOLS]
    : allowedTools.filter((name) => !name.startsWith("mcp__"));
  const options = {
    cwd: command.workspace,
    ...claudeExecutable(),
    // The session's variables (GH_TOKEN, git credentials, account environment) for its tools.
    env: { ...process.env, ...(command.environment || {}) },
    model,
    systemPrompt: {
      type: "preset",
      preset: "claude_code",
      append: [session.instructions, TOOL_PERMISSION_GUIDANCE].filter(Boolean).join("\n\n"),
    },
    settingSources: ["user", "project", "local"],
    includePartialMessages: true,
    abortController,
    permissionMode: mode,
    ...(mode === "bypassPermissions" ? { allowDangerouslySkipPermissions: true } : {}),
    ...(session.maxTurns ? { maxTurns: session.maxTurns } : {}),
    tools: builtinTools,
    disallowedTools: disallowedProviderTools(session.deniedTools),
    hooks: {
      PostToolBatch: [{ hooks: [permissionBatchHook(command.turnId)] }],
      PreToolUse: [
        {
          hooks: [
            mode === "bypassPermissions"
              ? preToolUseHook(command.turnId, session, mode, allowedTools)
              : nativeDelegationGuard(command.turnId, session, mode, allowedTools),
          ],
        },
      ],
    },
    ...(mode === "bypassPermissions"
      ? {}
      : {
          canUseTool: permissionHandler(
            command.turnId,
            session,
            mode,
            allowedTools,
          ),
        }),
    spawnClaudeCodeProcess: processLifecycle.spawn,
    ...(Object.keys(mcpServers).length > 0 ? { mcpServers } : {}),
    ...(session.started
      ? { resume: session.sessionId }
      : { sessionId: session.sessionId }),
    ...(session.options.reasoningEffort
      ? { effort: session.options.reasoningEffort }
      : {}),
    ...(session.options.fastMode ? { settings: { fastMode: true } } : {}),
  };

  let completion = null;
  let turnPermissions = new Map();
  const lifecycle = sdkLifecycleLog();
  const input = turnInput(command);
  let timedOut = false;
  const timeout = session.timeoutSeconds
    ? setTimeout(() => {
        timedOut = true;
        abortController.abort({ source: "runtime_timeout" });
      }, session.timeoutSeconds * 1000)
    : null;
  try {
    const stream = query({ prompt: input.prompt, options });
    await processLifecycle.started();
    write({ event: "turn_started", turnId: command.turnId });
    for await (const message of stream) {
      input.observe(message);
      // Retract superseded results first, so this message's replacement is the final projection.
      if (Array.isArray(message.supersedes))
        retractToolResults(command.turnId, message.supersedes, "a model refusal fallback");
      if (message.type === "stream_event")
        emitStreamEvent(command.turnId, message, session);
      else if (message.type === "assistant")
        emitContent(command.turnId, message);
      else if (message.type === "user") {
        emitUserToolResults(command.turnId, message);
        lifecycle.noteUnattributedCancellations(message);
      } else if (message.type === "system")
        lifecycle.note(message.subtype);
      if (message.type === "system" && message.subtype === "permission_denied")
        sdkPermissionDenied(command.turnId, message);
      else if (
        message.type === "system" &&
        (message.subtype === "model_refusal_fallback" || message.subtype === "model_refusal_no_fallback")
      )
        sdkRefusal(command.turnId, message);
      else if (message.type === "result") {
        await emitNativeAgentHistory(
          command.workspace,
          session.sessionId,
          command.turnId,
        );
        if (message.usage)
          write({
            event: "usage",
            turnId: command.turnId,
            usage: message.usage,
          });
        const success = message.subtype === "success" && !message.is_error;
        completion = {
          status: success ? "finished" : "failed",
          error: success
            ? undefined
            : (message.errors || [message.result]).filter(Boolean).join("\n"),
          result: message.result,
          usage: message.usage,
        };
      }
    }
    if (completion === null) completion = { status: "finished" };
  } catch (error) {
    const interrupted = abortController.signal.aborted;
    completion = {
      status: timedOut ? "failed" : interrupted ? "cancelled" : "failed",
      error: timedOut
        ? `archetype runtime exceeded its configured ${session.timeoutSeconds} second timeout`
        : interrupted
          ? undefined
          : errorMessage(error),
    };
  } finally {
    input.close();
    if (timeout !== null) clearTimeout(timeout);
    interruptExternalToolCalls(
      (pending) => pending.turnId === command.turnId,
      "External tool call interrupted",
    );
    streamMessageIds.delete(command.turnId);
    turnPermissions = new Map(
      [...providerToolPermissions].filter(([, recorded]) => recorded.turnId === command.turnId),
    );
    for (const callId of turnPermissions.keys()) providerToolPermissions.delete(callId);
    lifecycle.report(command.turnId);
    for (const [uuid, events] of providerToolResultMessages) {
      if (events.some((event) => event.turnId === command.turnId)) providerToolResultMessages.delete(uuid);
    }
    runs.delete(command.turnId);
  }

  try {
    await processLifecycle.released();
  } catch (error) {
    if (!processLifecycle.didStart()) {
      write({
        event: "turn_start_failed",
        turnId: command.turnId,
        message: errorMessage(error),
      });
      return;
    }
    sessions.delete(command.sessionId);
    write({
      event: "process_release_failed",
      turnId: command.turnId,
      sessionId: command.sessionId,
      message: `${errorMessage(error)}. The queued follow-up was retained; reconnect only after the Claude Code process has exited.`,
    });
    return;
  }
  if (!processLifecycle.didStart()) {
    write({
      event: "turn_start_failed",
      turnId: command.turnId,
      message: completion?.error || "Claude Code process did not start",
    });
    return;
  }
  session.started = true;
  write({
    event: "turn_completed",
    turnId: command.turnId,
    ...completion,
  });
  // Nakode keeps this so the session reads back while it is not loaded, e.g. after a restart.
  write({
    event: "history_snapshot",
    sessionId: command.sessionId,
    history: await readHistory(command.workspace, command.sessionId, turnPermissions),
  });
}

/**
 * The Claude Code to run: one this machine names or installed, else the build the Agent SDK
 * installed for this platform (a machine without Claude Code, such as an environment VM).
 */
function claudeExecutable() {
  if (process.env.CLAUDE_CODE_EXECUTABLE) {
    return { pathToClaudeCodeExecutable: process.env.CLAUDE_CODE_EXECUTABLE };
  }
  const installed = (process.env.PATH || "")
    .split(delimiter)
    .some((directory) => directory && existsSync(join(directory, "claude")));
  return installed ? { pathToClaudeCodeExecutable: "claude" } : {};
}

async function modelCatalogue(command) {
  const processLifecycle = providerProcessLifecycle(command.oauthAccessToken);
  let releasePrompt;
  const waiting = new Promise((resolve) => {
    releasePrompt = resolve;
  });
  const prompt = (async function* idlePrompt() {
    await waiting;
  })();
  const stream = query({
    prompt,
    options: {
      cwd: command.workspace,
      ...claudeExecutable(),
      persistSession: false,
      systemPrompt: "Report the installed model catalogue.",
      allowedTools: [],
      settingSources: ["user", "project", "local"],
      spawnClaudeCodeProcess: processLifecycle.spawn,
    },
  });
  const drained = (async () => {
    try {
      for await (const _ of stream) {
        // The control channel carries the catalogue. Conversation output is discarded.
      }
    } catch {
      // The intentional interrupt below closes this disposable query.
    }
  })();
  try {
    const models = await stream.supportedModels();
    write({
      event: "models",
      requestId: command.requestId,
      models: models.map((model, index) => ({
        id: model.value,
        resolvedModel: model.resolvedModel || null,
        isDefault: index === 0,
        displayName: model.displayName || "",
        description: model.description || "",
        supportedEffortLevels: model.supportedEffortLevels || [],
      })),
    });
  } finally {
    releasePrompt();
    await stream.interrupt().catch(() => undefined);
    await drained;
    await processLifecycle.released();
  }
}

async function handle(command) {
  switch (command.method) {
    case "models":
    case "reload":
      await modelCatalogue(command);
      break;
    case "create":
      await createSession(command, false);
      break;
    case "resume":
      await createSession(command, true);
      break;
    case "send":
      await sendTurn(command);
      break;
    case "history":
      write({
        event: "history_snapshot",
        sessionId: command.sessionId,
        history: await readHistory(command.workspace, command.sessionId),
      });
      break;
    case "set_options": {
      const session = sessions.get(command.sessionId);
      if (session)
        session.options = {
          fastMode: command.fastMode === true,
          reasoningEffort: command.reasoningEffort || undefined,
        };
      break;
    }
    case "resolve_approval": {
      const pending = approvals.get(command.approvalId);
      if (pending) {
        approvals.delete(command.approvalId);
        if (command.decision === "decline") {
          pending.finish({
            behavior: "deny",
            message: "Declined by user",
            toolUseID: command.approvalId,
          });
        } else {
          pending.finish({
            behavior: "allow",
            toolUseID: command.approvalId,
            ...(command.decision === "accept_session"
              ? {
                  updatedPermissions: pending.suggestions.map((suggestion) => ({
                    ...suggestion,
                    destination: "session",
                  })),
                }
              : {}),
          });
        }
      }
      write({ event: "approval_resolved", approvalId: command.approvalId });
      break;
    }
    case "resolve_external_tool":
    case "resolve_image_return":
    case "resolve_delegation":
      resolveExternalToolCall(
        command.id,
        command.output,
        command.failed === true,
      );
      break;
    case "resolve_desktop":
      resolveExternalToolCall(command.id, command.output, command.failed === true, command.image);
      break;
    case "cancel": {
      interruptExternalToolCalls(
        (pending) => pending.turnId === command.turnId,
        "External tool call interrupted",
      );
      runs.get(command.turnId)?.abort({ source: "runtime_interrupt" });
      write({
        event: "interrupt_accepted",
        requestId: command.requestId,
        turnId: command.turnId,
      });
      break;
    }
    case "close":
      interruptExternalToolCalls(
        (pending) => pending.sessionId === command.sessionId,
        "External tool session closed",
      );
      sessions.delete(command.sessionId);
      write({
        event: "session_closed",
        requestId: command.requestId,
        sessionId: command.sessionId,
      });
      break;
    case "shutdown":
      for (const controller of runs.values()) controller.abort({ source: "runtime_shutdown" });
      interruptExternalToolCalls(
        () => true,
        "External tool bridge shut down",
      );
      process.exit(0);
      break;
    default:
      throw new Error(`unknown bridge method ${command.method}`);
  }
}

const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => {
  let command;
  try {
    command = JSON.parse(line);
  } catch (error) {
    write({
      event: "diagnostic",
      message: `invalid command JSON: ${errorMessage(error)}`,
    });
    return;
  }
  Promise.resolve(handle(command)).catch((error) => {
    write({
      event: "error",
      requestId: command.requestId,
      turnId: command.turnId,
      operation: command.operation,
      message: errorMessage(error),
    });
  });
});
