import { query } from "@anthropic-ai/claude-agent-sdk";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

const sessions = new Map();
const runs = new Map();
const approvals = new Map();
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function createSession(command, resumed) {
  const sessionId = command.sessionId || randomUUID();
  sessions.set(sessionId, {
    sessionId,
    resumed,
    started: resumed,
    instructions: command.instructions || "",
    model: command.model || "sonnet",
    options: {},
  });
  write({
    event: resumed ? "session_resumed" : "session_created",
    requestId: command.requestId,
    sessionId,
    model: command.model || "sonnet",
  });
}

function permissionHandler(turnId) {
  return (toolName, input, options) => new Promise((resolve) => {
    const approvalId = options.toolUseID || randomUUID();
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      options.signal.removeEventListener("abort", abort);
      approvals.delete(approvalId);
      resolve(result);
    };
    const abort = () => finish({
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
    approvals.set(approvalId, { finish, suggestions: options.suggestions || [] });
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
}

function emitContent(turnId, message) {
  const content = message?.message?.content;
  if (!Array.isArray(content)) return;
  for (const block of content) {
    if (block.type === "tool_use") {
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
      write({
        event: "tool_call",
        turnId,
        callId: block.tool_use_id,
        name: "Tool",
        status: block.is_error ? "error" : "completed",
        result: block.content,
      });
    }
  }
}

function emitStreamDelta(turnId, message) {
  const event = message.event;
  if (event?.type !== "content_block_delta") return;
  if (event.delta?.type === "text_delta" && event.delta.text) {
    write({ event: "delta", turnId, kind: "assistant", text: event.delta.text });
  } else if (event.delta?.type === "thinking_delta" && event.delta.thinking) {
    write({ event: "delta", turnId, kind: "reasoning", text: event.delta.thinking });
  }
}

async function sendTurn(command) {
  const session = sessions.get(command.sessionId);
  if (!session) throw new Error(`Claude session ${command.sessionId} is not attached`);

  const abortController = new AbortController();
  runs.set(command.turnId, abortController);
  write({ event: "turn_started", turnId: command.turnId });

  const model = command.model || session.model;
  session.model = model;
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
    canUseTool: permissionHandler(command.turnId),
    ...(session.started ? { resume: session.sessionId } : { sessionId: session.sessionId }),
    ...(session.options.reasoningEffort ? { effort: session.options.reasoningEffort } : {}),
    ...(session.options.fastMode ? { settings: { fastMode: true } } : {}),
  };

  let resultSeen = false;
  try {
    const stream = query({ prompt: command.prompt, options });
    for await (const message of stream) {
      if (message.type === "stream_event") emitStreamDelta(command.turnId, message);
      else if (message.type === "assistant") emitContent(command.turnId, message);
      else if (message.type === "user") emitUserToolResults(command.turnId, message);
      else if (message.type === "result") {
        resultSeen = true;
        const success = message.subtype === "success" && !message.is_error;
        write({
          event: "turn_completed",
          turnId: command.turnId,
          status: success ? "finished" : "failed",
          error: success ? undefined : (message.errors || [message.result]).filter(Boolean).join("\n"),
          result: message.result,
          usage: message.usage,
        });
      }
    }
    session.started = true;
    if (!resultSeen) {
      write({ event: "turn_completed", turnId: command.turnId, status: "finished" });
    }
  } catch (error) {
    const interrupted = abortController.signal.aborted;
    write({
      event: "turn_completed",
      turnId: command.turnId,
      status: interrupted ? "cancelled" : "failed",
      error: interrupted ? undefined : errorMessage(error),
    });
  } finally {
    runs.delete(command.turnId);
  }
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
      pathToClaudeCodeExecutable: process.env.CLAUDE_CODE_EXECUTABLE || "claude",
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
      createSession(command, false);
      break;
    case "resume":
      createSession(command, true);
      break;
    case "send":
      await sendTurn(command);
      break;
    case "set_options": {
      const session = sessions.get(command.sessionId);
      if (session) session.options = {
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
          pending.finish({ behavior: "deny", message: "Declined by user", toolUseID: command.approvalId });
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
      write({ event: "interrupt_accepted", requestId: command.requestId, turnId: command.turnId });
      break;
    }
    case "close":
      sessions.delete(command.sessionId);
      write({ event: "session_closed", requestId: command.requestId, sessionId: command.sessionId });
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
    write({ event: "diagnostic", message: `invalid command JSON: ${errorMessage(error)}` });
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
