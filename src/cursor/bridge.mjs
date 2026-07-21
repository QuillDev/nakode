import { Agent, Cursor } from "@cursor/sdk";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

const agents = new Map();
const runs = new Map();
const instructions = new Map();
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function nakodeTools() {
  return {
    nakode_agent: {
      description: "Delegate a bounded assignment to a configured Nakode workflow agent and return its result.",
      inputSchema: {
        type: "object",
        properties: {
          agent: { type: "string", description: "Configured Nakode agent slug." },
          session_id: { type: "string", description: "Logical Nakode session id from the system instructions." },
          task: { type: "string", description: "Concrete, bounded assignment for the delegated agent." },
        },
        required: ["agent", "session_id", "task"],
        additionalProperties: false,
      },
      execute(args) {
        return new Promise((resolve, reject) => {
          const executable = process.env.NAKODE_EXECUTABLE;
          if (!executable) {
            reject(new Error("NAKODE_EXECUTABLE is not configured"));
            return;
          }
          const child = spawn(executable, [
            "agent", String(args.agent),
            `--session-id=${String(args.session_id)}`,
            `--task=${String(args.task)}`,
          ], { cwd: process.env.NAKODE_WORKSPACE, stdio: ["ignore", "pipe", "pipe"] });
          let stdout = "";
          let stderr = "";
          child.stdout.setEncoding("utf8");
          child.stderr.setEncoding("utf8");
          child.stdout.on("data", (chunk) => { stdout += chunk; });
          child.stderr.on("data", (chunk) => { stderr += chunk; });
          child.on("error", reject);
          child.on("close", (code) => {
            if (code === 0) resolve(stdout.trim());
            else reject(new Error(stderr.trim() || stdout.trim() || `Nakode agent exited with status ${code}`));
          });
        });
      },
    },
  };
}

async function createAgent(command) {
  const options = {
    apiKey: command.apiKey,
    local: { cwd: command.workspace, customTools: nakodeTools() },
  };
  if (command.model) options.model = { id: command.model };
  const agent = await Agent.create(options);
  agents.set(agent.agentId, agent);
  if (command.instructions) instructions.set(agent.agentId, command.instructions);
  write({ event: "session_created", requestId: command.requestId, sessionId: agent.agentId, model: agent.model?.id ?? command.model ?? "auto" });
}

async function resumeAgent(command) {
  const agent = await Agent.resume(command.sessionId, {
    apiKey: command.apiKey,
    local: { cwd: command.workspace, customTools: nakodeTools() },
  });
  agents.set(agent.agentId, agent);
  write({ event: "session_resumed", requestId: command.requestId, sessionId: agent.agentId, model: agent.model?.id ?? command.model ?? "auto" });
}

async function sendTurn(command) {
  const agent = agents.get(command.sessionId);
  if (!agent) throw new Error(`Cursor agent ${command.sessionId} is not attached`);
  const system = instructions.get(command.sessionId);
  instructions.delete(command.sessionId);
  const prompt = system ? `${system}\n\n${command.prompt}` : command.prompt;
  let streamedText = false;
  const options = {
    onDelta: ({ update }) => {
      if (update.type === "text-delta") {
        streamedText = true;
        write({ event: "delta", turnId: command.turnId, kind: "assistant", text: update.text });
      }
      if (update.type === "thinking-delta") write({ event: "delta", turnId: command.turnId, kind: "reasoning", text: update.text });
    },
  };
  if (command.model) options.model = { id: command.model };
  const run = await agent.send(prompt, options);
  runs.set(command.turnId, run);
  write({ event: "turn_started", turnId: command.turnId, runId: run.id });
  try {
    for await (const message of run.stream()) {
      if (message.type === "tool_call") {
        write({
          event: "tool_call", turnId: command.turnId, callId: message.call_id,
          name: message.name, status: message.status, args: message.args, result: message.result,
        });
      } else if (message.type === "task" && message.text) {
        write({ event: "plan", turnId: command.turnId, text: message.text });
      }
    }
    const result = await run.wait();
    if (!streamedText && result.result) {
      write({ event: "delta", turnId: command.turnId, kind: "assistant", text: result.result });
    }
    write({
      event: "turn_completed", turnId: command.turnId, status: result.status,
      error: result.error?.message, result: result.result, model: result.model?.id,
      usage: result.usage,
    });
  } finally {
    runs.delete(command.turnId);
  }
}

async function handle(command) {
  switch (command.method) {
    case "models": {
      const models = await Cursor.models.list({ apiKey: command.apiKey });
      write({ event: "models", requestId: command.requestId, models: models.map((model, index) => ({ id: model.id, isDefault: model.id === "auto" || (index === 0 && !models.some((item) => item.id === "auto")) })) });
      break;
    }
    case "create": await createAgent(command); break;
    case "resume": await resumeAgent(command); break;
    case "send": await sendTurn(command); break;
    case "cancel": {
      const run = runs.get(command.turnId);
      if (run) await run.cancel();
      write({ event: "interrupt_accepted", requestId: command.requestId, turnId: command.turnId });
      break;
    }
    case "close": {
      const agent = agents.get(command.sessionId);
      if (agent) await agent[Symbol.asyncDispose]();
      agents.delete(command.sessionId);
      instructions.delete(command.sessionId);
      write({ event: "session_closed", requestId: command.requestId, sessionId: command.sessionId });
      break;
    }
    case "reload": {
      const agent = command.sessionId ? agents.get(command.sessionId) : undefined;
      if (agent) await agent.reload();
      const models = await Cursor.models.list({ apiKey: command.apiKey });
      write({ event: "models", requestId: command.requestId, models: models.map((model, index) => ({ id: model.id, isDefault: model.id === "auto" || (index === 0 && !models.some((item) => item.id === "auto")) })) });
      break;
    }
    case "shutdown": {
      await Promise.allSettled([...agents.values()].map((agent) => agent[Symbol.asyncDispose]()));
      process.exit(0);
      break;
    }
    default: throw new Error(`unknown bridge method ${command.method}`);
  }
}

const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => {
  let command;
  try { command = JSON.parse(line); }
  catch (error) { write({ event: "diagnostic", message: `invalid command JSON: ${errorMessage(error)}` }); return; }
  Promise.resolve(handle(command)).catch((error) => {
    write({ event: "error", requestId: command.requestId, turnId: command.turnId, operation: command.operation, message: errorMessage(error) });
  });
});
