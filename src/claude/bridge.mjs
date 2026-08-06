import {
  query,
  createSdkMcpServer,
  getSessionMessages,
  filterEscalatingDefaultMode,
  resolveSettings,
  tool,
} from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod/v4";
import { spawn as spawnChild } from "node:child_process";
import { readdir, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

const sessions = new Map();
const runs = new Map();
const approvals = new Map();
const providerToolCalls = new Map();
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);

async function delegate(ownerSessionId, task) {
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

function nakodeServer(ownerSessionId) {
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
            const result = await delegate(ownerSessionId, args);
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

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

const FORCE_RELEASE_AFTER_MS = 2_000;
const RELEASE_FAILURE_AFTER_MS = 7_000;

/**
 * Own one Claude Code subprocess and expose its close event as the session-release barrier.
 * The SDK's iterator may settle as soon as cancellation is observed; only this child close proves
 * that another process may safely resume the same provider session.
 */
function providerProcessLifecycle() {
  let child = null;
  let release = Promise.resolve();
  let started = false;
  let resolveStarted;
  let rejectStarted;
  const processStarted = new Promise((resolve, reject) => {
    resolveStarted = resolve;
    rejectStarted = reject;
  });

  return {
    spawn(options) {
      if (child !== null)
        throw new Error("a Claude Code process is already attached to this turn");

      child = spawnChild(options.command, options.args, {
        cwd: options.cwd,
        env: options.env,
        signal: options.signal,
        stdio: ["pipe", "pipe", "pipe"],
      });
      release = new Promise((resolve, reject) => {
        let forceTimer = null;
        let failureTimer = null;
        let settled = false;
        const finish = (result, error) => {
          if (settled) return;
          settled = true;
          if (forceTimer !== null) clearTimeout(forceTimer);
          if (failureTimer !== null) clearTimeout(failureTimer);
          options.signal.removeEventListener("abort", forceRelease);
          if (error) reject(error);
          else resolve(result);
        };
        const forceRelease = () => {
          forceTimer = setTimeout(() => {
            if (child !== null && child.exitCode === null)
              child.kill("SIGKILL");
          }, FORCE_RELEASE_AFTER_MS);
          failureTimer = setTimeout(
            () =>
              finish(
                null,
                new Error(
                  "Claude Code did not exit after cancellation; the provider session may still be in use",
                ),
              ),
            RELEASE_FAILURE_AFTER_MS,
          );
        };
        child.once("spawn", () => {
          started = true;
          resolveStarted();
        });
        child.once("close", (code, signal) => finish({ code, signal }, null));
        child.once("error", (error) => {
          if (child?.pid === undefined) {
            rejectStarted(error);
            finish(null, error);
          }
        });
        if (options.signal.aborted) forceRelease();
        else options.signal.addEventListener("abort", forceRelease, {
          once: true,
        });
      });
      return child;
    },
    async started() {
      await processStarted;
    },
    didStart() {
      return started;
    },
    async released() {
      await release;
    },
  };
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
        const text =
          block.type === "text"
            ? block.text
            : block.type === "tool_use"
              ? JSON.stringify({ tool: block.name, input: block.input })
              : block.type === "tool_result"
                ? JSON.stringify({
                    toolUseId: block.tool_use_id,
                    output: block.content,
                  })
                : null;
        if (text === null) continue;
        history.push({
          turnId: message.promptId || `${sessionId}:native:${agentId}`,
          id: `native:${agentId}:${message.uuid || `${index}:${blockIndex}`}`,
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
        title = block.name || "Tool";
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
      // Text and thinking blocks have no provider block id. Claude's SDK message UUID plus the
      // content-array index is the durable counterpart of stream_event.index, so resume hydration
      // patches the same logical blocks in the same order as the live stream. Tool results retain
      // their tool-use id because they are status/body updates to the call, not new timeline rows.
      const messageId = message.uuid || `${sessionId}:message:${ordinal}`;
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
  sessions.set(sessionId, {
    sessionId,
    resumed,
    started: resumed,
    instructions: command.instructions || "",
    model: command.model || "sonnet",
    options: {},
    ownerSessionId: command.ownerSessionId || null,
    securityValidator: validatorSession(command.instructions || ""),
    validationEnabled:
      Boolean(command.ownerSessionId) && !validatorSession(command.instructions || ""),
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

function permissionHandler(turnId, session, mode) {
  return async (toolName, input, options) => {
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
      return result.verdict === "allow"
        ? { behavior: "allow", updatedInput: input }
        : {
            behavior: "deny",
            message:
              result.verdict === "reject"
                ? `Independent security validation rejected this operation: ${result.rationale}`
                : `Security validation requires escalation; the operation was not run: ${result.rationale}`,
            toolUseID: options.toolUseID,
          };
    }
    if (
      toolName !== "AskUserQuestion" &&
      mode === "auto" &&
      !options.matchedAskRule &&
      !session.validationEnabled
    ) {
      return {
        behavior: "deny",
        message:
          "Recursive security validation was prevented in a delegated validator context.",
        toolUseID: options.toolUseID,
      };
    }
    return new Promise((resolve) => {
      const approvalId = options.toolUseID || randomUUID();
      let settled = false;
      const finish = (result) => {
        if (settled) return;
        settled = true;
        options.signal.removeEventListener("abort", abort);
        approvals.delete(approvalId);
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

function emitContent(turnId, message) {
  const content = message?.message?.content;
  if (!Array.isArray(content)) return;
  for (const block of content) {
    if (block.type === "tool_use") {
      providerToolCalls.set(block.id, { name: block.name, input: block.input });
      write({
        event: "tool_call",
        turnId,
        callId: block.id,
        name: block.name,
        status: "running",
        args: block.input,
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
      write({
        event: "tool_call",
        turnId,
        callId: block.tool_use_id,
        name: started?.name || "Tool",
        status: block.is_error ? "error" : "completed",
        result: { input: started?.input, output: block.content },
      });
    }
  }
}

function emitStreamEvent(turnId, message) {
  // A Claude "turn" contains several assistant messages around tool results. `uuid` scopes the raw
  // content-block index to one of those messages; using the turn alone would append the final answer
  // into the first text/thinking row and leave that row above every intervening tool call.
  const event = message.event;
  if (event?.type === "content_block_start") {
    const block = event.content_block;
    if (block?.type === "tool_use") {
      providerToolCalls.set(block.id, { name: block.name, input: block.input });
      write({
        event: "tool_call",
        turnId,
        callId: block.id,
        name: block.name,
        status: "running",
        args: block.input,
      });
    }
    return;
  }
  if (event?.type !== "content_block_delta") return;
  if (event.delta?.type === "text_delta" && event.delta.text) {
    write({
      event: "delta",
      turnId,
      messageId: message.uuid,
      blockIndex: event.index,
      kind: "assistant",
      text: event.delta.text,
    });
  } else if (event.delta?.type === "thinking_delta" && event.delta.thinking) {
    write({
      event: "delta",
      turnId,
      messageId: message.uuid,
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

  const abortController = new AbortController();
  runs.set(command.turnId, abortController);

  const model = command.model || session.model;
  session.model = model;
  const mode = session.securityValidator
    ? "dontAsk"
    : await permissionMode(command.workspace);
  const processLifecycle = providerProcessLifecycle();
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
    ...(session.securityValidator ? { allowedTools: [] } : {}),
    canUseTool: permissionHandler(command.turnId, session, mode),
    spawnClaudeCodeProcess: processLifecycle.spawn,
    ...(session.ownerSessionId && session.validationEnabled
      ? { mcpServers: { nakode: nakodeServer(session.ownerSessionId) } }
      : {}),
    ...(session.started
      ? { resume: session.sessionId }
      : { sessionId: session.sessionId }),
    ...(session.options.reasoningEffort
      ? { effort: session.options.reasoningEffort }
      : {}),
    ...(session.options.fastMode ? { settings: { fastMode: true } } : {}),
  };

  let completion = null;
  try {
    const stream = query({ prompt: command.prompt, options });
    await processLifecycle.started();
    write({ event: "turn_started", turnId: command.turnId });
    for await (const message of stream) {
      if (message.type === "stream_event")
        emitStreamEvent(command.turnId, message);
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
      status: interrupted ? "cancelled" : "failed",
      error: interrupted ? undefined : errorMessage(error),
    };
  } finally {
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
    case "cancel": {
      runs.get(command.turnId)?.abort();
      write({
        event: "interrupt_accepted",
        requestId: command.requestId,
        turnId: command.turnId,
      });
      break;
    }
    case "close":
      sessions.delete(command.sessionId);
      write({
        event: "session_closed",
        requestId: command.requestId,
        sessionId: command.sessionId,
      });
      break;
    case "shutdown":
      for (const controller of runs.values()) controller.abort();
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
