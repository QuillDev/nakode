import {
  query,
  createSdkMcpServer,
  getSessionMessages,
  filterEscalatingDefaultMode,
  resolveSettings,
  tool,
} from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod/v4";
import {
  immediateProviderToolDecision,
  preToolUseOutput,
} from "./tool_policy.mjs";
import { spawn as spawnChild } from "node:child_process";
import { providerProcessLifecycle } from "./process_lifecycle.mjs";
import { readdir, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

const sessions = new Map();
const runs = new Map();
const approvals = new Map();
const externalToolCalls = new Map();
const providerToolCalls = new Map();
const deniedProviderToolCalls = new Map();
const streamMessageIds = new Map();
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);

async function delegate(ownerSessionId, task, parentRunId = null) {
  return new Promise((resolve, reject) => {
    const executable = process.env.NAKODE_EXECUTABLE;
    const workspace = process.env.NAKODE_WORKSPACE;
    if (!executable || !workspace) {
      reject(
        new Error(
          "Nakode delegation is not configured for this Claude session",
        ),
      );
      return;
    }
    const child = spawnChild(
      executable,
      [
        "agent",
        task.archetype,
        `--session-id=${ownerSessionId}`,
        `--task=${task.task}`,
        ...(parentRunId ? [`--parent-run-id=${parentRunId}`] : []),
      ],
      { cwd: workspace, stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      const output = stdout.trim();
      const match = output.match(
        /^\[Subagent Result\] \[([^\]]+)\] \[([^\]]+)\]\n?([\s\S]*)$/,
      );
      const result = {
        runId: match?.[1] || null,
        archetype: match?.[2] || task.archetype,
        status: code === 0 ? "completed" : "failed",
        result: match?.[3]?.trim() || (code === 0 ? output : null),
        error:
          code === 0
            ? null
            : stderr.trim() ||
              (!match ? output : null) ||
              `Nakode delegation exited with status ${code}`,
      };
      resolve(result);
    });
  });
}

function nakodeServer(ownerSessionId, parentRunId) {
  return createSdkMcpServer({
    name: "nakode",
    version: "1.0.0",
    instructions:
      "Creates bounded, attributed Nakode sub-agents from the configured archetype catalogue.",
    tools: [
      tool(
        "delegate",
        "Delegate one concrete bounded task to a configured Nakode archetype and wait for its result.",
        {
          archetype: z.string().describe("Configured Nakode archetype slug."),
          task: z
            .string()
            .describe("Concrete bounded assignment and expected result."),
        },
        async (args) => {
          try {
            const result = await delegate(ownerSessionId, args, parentRunId);
            return {
              isError: result.status !== "completed",
              content: [{ type: "text", text: JSON.stringify(result) }],
            };
          } catch (error) {
            return {
              isError: true,
              content: [{ type: "text", text: errorMessage(error) }],
            };
          }
        },
      ),
    ],
  });
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

function resolveExternalToolCall(id, output, failed) {
  const pending = externalToolCalls.get(id);
  if (!pending) return;
  externalToolCalls.delete(id);
  pending.resolve({
    isError: failed === true,
    content: [{ type: "text", text: output || "" }],
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

function effectiveAllowedTools(session) {
  const external = externalToolNames(session);
  if (session.replaceBuiltinTools) {
    return [
      ...external,
      ...(session.allowedTools?.filter((name) => name === "mcp__nakode__delegate") ?? []),
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

function savedHistory(messages, sessionId) {
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
    attributedRunId: command.parentRunId || policy?.runId || null,
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
  let history = [];
  if (resumed) {
    try {
      history = [
        ...savedHistory(
          await getSessionMessages(sessionId, { dir: command.workspace }),
          sessionId,
        ),
        ...(await nativeAgentHistory(command.workspace, sessionId)),
      ];
    } catch (error) {
      write({
        event: "diagnostic",
        message: `could not read Claude session history: ${errorMessage(error)}`,
      });
    }
  }
  write({
    event: resumed ? "session_resumed" : "session_created",
    requestId: command.requestId,
    sessionId,
    model: command.model || "sonnet",
    history,
  });
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
  const runId = /\[Nakode Run Attribution\]\nRun ID: ([^\n]+)/.exec(policy)?.[1];
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
    runId: runId || null,
  };
}

async function permissionMode(workspace) {
  const resolved = await resolveSettings({
    cwd: workspace,
    settingSources: ["user", "project", "local"],
  });
  return (
    filterEscalatingDefaultMode(resolved).permissions?.defaultMode || "auto"
  );
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

async function securityValidation(ownerSessionId, toolName, input, options) {
  const archetype =
    process.env.NAKODE_SECURITY_VALIDATOR_AGENT || "security-validator";
  const task = `Return JSON only as {"verdict":"allow|reject|escalate","rationale":"..."}.\nDo not use tools or delegate. Independently validate this security-sensitive proposed Claude Code operation against the owner's repository task. Allow only when intent and bounded impact are clear; reject security-control bypass, credential exposure, exfiltration, and destructive out-of-scope actions; escalate uncertainty.\n\n${JSON.stringify({ tool: toolName, input, reason: options.decisionReason || options.description || "Claude auto mode requested review." })}`;
  try {
    return parseValidatorResult(await delegate(ownerSessionId, { archetype, task }));
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

function permissionHandler(turnId, session, mode, allowedTools) {
  return async (toolName, input, options) => {
    const deny = (message) => {
      const callId = options.toolUseID || randomUUID();
      if (options.toolUseID) {
        deniedProviderToolCalls.set(options.toolUseID, {
          name: providerToolName(toolName),
          reason: message,
          turnId,
        });
      }
      write({
        event: "tool_call",
        turnId,
        callId,
        name: providerToolName(toolName) || "UnknownProviderTool",
        status: "error",
        args: input,
        external: externalToolNames(session).includes(toolName),
        denied: true,
        denialReason: message,
      });
      return { behavior: "deny", message, toolUseID: options.toolUseID };
    };
    if (session.finalizing) {
      return deny(
        "Protected finalization reserve denies new tool use; synthesize the best final or partial report from retained evidence.",
      );
    }
    const immediate = immediateProviderToolDecision(
      session.deniedTools,
      allowedTools,
      mode,
      toolName,
    );
    if (immediate?.behavior === "deny") {
      return deny(immediate.message);
    }
    if (immediate?.behavior === "allow") {
      return { ...immediate, updatedInput: input };
    }
    if (externalToolNames(session).includes(toolName)) {
      return { behavior: "allow", updatedInput: input };
    }
    if (
      toolName !== "AskUserQuestion" &&
      mode === "auto" &&
      !options.matchedAskRule &&
      session.validationEnabled
    ) {
      const result = await securityValidation(
        session.ownerSessionId,
        toolName,
        input,
        options,
      );
      write({
        event: "tool_call",
        turnId,
        callId: `security:${options.toolUseID || randomUUID()}`,
        name: "SecurityValidation",
        status: result.verdict === "allow" ? "completed" : "error",
        result: {
          verdict: result.verdict,
          rationale: result.rationale,
          validated: result.validated,
          validator: result.archetype,
          runId: result.runId,
        },
      });
      if (result.verdict === "allow") {
        return { behavior: "allow", updatedInput: input };
      }
      return deny(
        result.verdict === "reject"
          ? `Independent security validation rejected this operation: ${result.rationale}`
          : `Security validation requires escalation; the operation was not run: ${result.rationale}`,
      );
    }
    if (
      toolName !== "AskUserQuestion" &&
      mode === "auto" &&
      !options.matchedAskRule &&
      !session.validationEnabled
    ) {
      return deny(
        "Recursive security validation was prevented in a delegated validator context.",
      );
    }
    return new Promise((resolve) => {
      const approvalId = options.toolUseID || randomUUID();
      let settled = false;
      const finish = (result) => {
        if (settled) return;
        settled = true;
        options.signal.removeEventListener("abort", abort);
        approvals.delete(approvalId);
        if (result.behavior === "deny") {
          const reason = result.message || "Tool use denied";
          if (options.toolUseID) {
            deniedProviderToolCalls.set(options.toolUseID, {
              name: providerToolName(toolName),
              reason,
              turnId,
            });
          }
          write({
            event: "tool_call",
            turnId,
            callId: approvalId,
            name: providerToolName(toolName) || "UnknownProviderTool",
            status: "error",
            args: input,
            external: externalToolNames(session).includes(toolName),
            denied: true,
            denialReason: reason,
          });
        }
        resolve(result);
      };
      const abort = () =>
        finish({
          behavior: "deny",
          message: "Interrupted by user",
          interrupt: true,
          toolUseID: approvalId,
        });
      if (options.signal.aborted) {
        abort();
        return;
      }
      options.signal.addEventListener("abort", abort, { once: true });
      approvals.set(approvalId, {
        finish,
        suggestions: options.suggestions || [],
      });
      write({
        event: "approval_request",
        turnId,
        approvalId,
        toolName,
        input,
        title: options.title || options.displayName || `Allow ${toolName}?`,
        description: options.description || options.decisionReason || "",
      });
    });
  };
}

function preToolUseHook(turnId, session, mode, allowedTools) {
  const authorize = permissionHandler(turnId, session, mode, allowedTools);
  return async (hookInput, toolUseID, options) => {
    const result = await authorize(hookInput.tool_name, hookInput.tool_input, {
      signal: options.signal,
      suggestions: [],
      toolUseID: toolUseID || hookInput.tool_use_id,
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
      const denied = deniedProviderToolCalls.get(block.tool_use_id);
      deniedProviderToolCalls.delete(block.tool_use_id);
      write({
        event: "tool_call",
        turnId,
        callId: block.tool_use_id,
        name: denied?.name || started?.name || "Tool",
        status: block.is_error ? "error" : "completed",
        result: { input: started?.input, output: block.content },
        external: started?.external === true,
        denied: denied !== undefined,
        denialReason: denied?.reason,
      });
    }
  }
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
    : await permissionMode(command.workspace);
  const processLifecycle = providerProcessLifecycle(command.oauthAccessToken);
  const allowedTools = effectiveAllowedTools(session);
  const mcpServers = {};
  if (
    session.ownerSessionId &&
    session.validationEnabled &&
    session.delegationEnabled &&
    (allowedTools === null || allowedTools.includes("mcp__nakode__delegate"))
  ) {
    mcpServers.nakode = nakodeServer(
      session.ownerSessionId,
      session.attributedRunId,
    );
  }
  if (session.externalTools.length > 0) {
    mcpServers.nakode_external = externalToolsServer(session, command.turnId);
  }
  const builtinTools = allowedTools === null
    ? null
    : allowedTools.filter((name) => !name.startsWith("mcp__"));
  const options = {
    cwd: command.workspace,
    pathToClaudeCodeExecutable: process.env.CLAUDE_CODE_EXECUTABLE || "claude",
    model,
    systemPrompt: session.instructions
      ? { type: "preset", preset: "claude_code", append: session.instructions }
      : { type: "preset", preset: "claude_code" },
    settingSources: ["user", "project", "local"],
    includePartialMessages: true,
    abortController,
    permissionMode: mode,
    ...(session.maxTurns ? { maxTurns: session.maxTurns } : {}),
    ...(builtinTools !== null ? { tools: builtinTools } : {}),
    ...(session.deniedTools.length > 0
      ? { disallowedTools: session.deniedTools }
      : {}),
    ...(mode === "bypassPermissions"
      ? {
          hooks: {
            PreToolUse: [
              {
                hooks: [preToolUseHook(command.turnId, session, mode, allowedTools)],
              },
            ],
          },
        }
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
  let timedOut = false;
  const timeout = session.timeoutSeconds
    ? setTimeout(() => {
        timedOut = true;
        abortController.abort();
      }, session.timeoutSeconds * 1000)
    : null;
  try {
    const stream = query({ prompt: command.prompt, options });
    await processLifecycle.started();
    write({ event: "turn_started", turnId: command.turnId });
    for await (const message of stream) {
      if (message.type === "stream_event")
        emitStreamEvent(command.turnId, message, session);
      else if (message.type === "assistant")
        emitContent(command.turnId, message);
      else if (message.type === "user")
        emitUserToolResults(command.turnId, message);
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
    if (timeout !== null) clearTimeout(timeout);
    interruptExternalToolCalls(
      (pending) => pending.turnId === command.turnId,
      "External tool call interrupted",
    );
    streamMessageIds.delete(command.turnId);
    for (const [callId, denied] of deniedProviderToolCalls) {
      if (denied.turnId === command.turnId) deniedProviderToolCalls.delete(callId);
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
      pathToClaudeCodeExecutable:
        process.env.CLAUDE_CODE_EXECUTABLE || "claude",
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
        isDefault: index === 0,
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
      resolveExternalToolCall(
        command.id,
        command.output,
        command.failed === true,
      );
      break;
    case "cancel": {
      interruptExternalToolCalls(
        (pending) => pending.turnId === command.turnId,
        "External tool call interrupted",
      );
      runs.get(command.turnId)?.abort();
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
      for (const controller of runs.values()) controller.abort();
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
