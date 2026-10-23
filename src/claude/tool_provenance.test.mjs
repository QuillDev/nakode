import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import test from "node:test";
import {
  immediateProviderToolDecision,
  nativeDelegationDecision,
  preToolUseOutput,
  permissionContext,
  SDK_CANCELLATION_TEXT,
} from "./tool_policy.mjs";

// Exercise production permission callbacks and result projection without inference, credentials,
// processes, or a live runtime. SDK signals are deliberately independent of the runtime signal.
function fixture(mode = "bypassPermissions") {
  const source = readFileSync(new URL("./bridge.mjs", import.meta.url), "utf8");
  const emitted = [];
  const approvals = new Map();
  const providerToolPermissions = new Map();
  const providerToolCalls = new Map();
  const providerToolResultMessages = new Map();
  const runs = new Map([["turn", new AbortController()]]);
  const sessions = new Map();
  const session = { deniedTools: [], externalTools: [], validationEnabled: true };
  const write = (value) => emitted.push(value);
  const providerToolName = (name) => name;
  const externalToolNames = () => [];
  const interruptExternalToolCalls = () => {};
  const errorMessage = (error) => error.message;
  let validation = { verdict: "allow", rationale: "Authorized read" };
  const requestDelegation = async () => validation instanceof Error
    ? Promise.reject(validation)
    : { text: JSON.stringify(validation), failed: false };
  const delegationOutcome = (value) => value;
  const SUBAGENT_RESULT = /never matches/;
  // sendTurn runs against a scripted SDK stream; inference and process supervision stay isolated.
  let sdkMessages = [];
  let sdk = null;
  const query = ({ prompt }) => (sdk ? sdk(prompt) : sdkMessages);
  const streamMessageIds = new Map();
  const providerProcessLifecycle = () => ({
    spawn() {}, async started() {}, async released() {}, didStart: () => true,
  });
  const effectiveAllowedTools = () => [];
  const permissionMode = () => "bypassPermissions";
  const disallowedProviderTools = (deniedTools) => deniedTools;
  const claudeExecutable = () => ({});
  const readHistory = async () => [];
  const emitNativeAgentHistory = async () => {};
  const DESKTOP_TOOLS = {};
  const UNRESTRICTED_BUILTIN_TOOLS = [];
  const TOOL_PERMISSION_GUIDANCE = "";
  sessions.set("session", { ...session, sessionId: "session", model: "opus", options: {} });
  const functions = source.slice(source.indexOf("function parseValidatorResult"), source.indexOf("/**\n * The Claude Code to run"));
  const handler = source.slice(source.indexOf("async function handle(command)"), source.indexOf("const input = createInterface"));
  const api = eval(`(() => { ${functions}\n${handler}; return { permissionHandler, emitUserToolResults, permissionBatchHook, sdkPermissionDenied, sdkRefusal, retractToolResults, sendTurn, handle }; })()`);
  const signal = new AbortController();
  const options = { toolUseID: "call", signal: signal.signal, suggestions: [] };
  return {
    emitted, signal, runs, approvals,
    validate(value) { validation = value; },
    authorize: (allowed = null) => api.permissionHandler("turn", session, mode, allowed)("Bash", { command: "git status --short" }, options),
    resolve: (decision) => api.handle({ method: "resolve_approval", approvalId: "call", decision }),
    cancel: () => api.handle({ method: "cancel", turnId: "turn", requestId: "authenticated-control" }),
    result(output, isError = true, uuid = undefined) {
      providerToolCalls.set("call", { name: "Bash", input: { command: "git status --short" } });
      api.emitUserToolResults("turn", { uuid, message: { content: [{ type: "tool_result", tool_use_id: "call", is_error: isError, content: output }] } });
      return emitted.at(-1);
    },
    useSdk(stream) { sdk = stream; },
    async turn(messages = [], beforeResults = null) {
      sdkMessages = (async function* () {
        for (const message of messages) {
          if (beforeResults && message.type === "user") await beforeResults();
          yield message;
        }
      })();
      await api.sendTurn({ sessionId: "session", turnId: "turn", prompt: "check CI", workspace: "/isolated" });
    },
    refusal: (message) => api.sdkRefusal("turn", { original_model: "claude-opus-5-5", ...message }),
    supersede: (uuids) => api.retractToolResults("turn", uuids, "a model refusal fallback"),
    sdkDeny: (reasonType) => api.sdkPermissionDenied("turn", {
      tool_use_id: "call", tool_name: "Bash", decision_reason_type: reasonType,
      decision_reason: "SDK denied this operation", message: "denied",
    }),
    batch: (toolResponse) => api.permissionBatchHook("turn")({ tool_calls: [{ tool_use_id: "call", tool_response: toolResponse }] }),
  };
}

test("authorized read remains allowed and successful, without fabricated denial", async () => {
  const f = fixture();
  assert.equal((await f.authorize()).behavior, "allow");
  const result = f.result("clean", false);
  assert.equal(result.denied, false);
  assert.equal(result.permission.decision, "allow");
  assert.equal(result.status, "completed");
});

test("policy denial reaches SDK/model context and final audit without owner attribution", async () => {
  const f = fixture();
  const decision = await f.authorize([]);
  assert.equal(decision.behavior, "deny");
  assert.equal(decision.permission.source, "policy");
  assert.match(preToolUseOutput(decision).hookSpecificOutput.additionalContext, /"source":"policy"/);
  const result = f.result(SDK_CANCELLATION_TEXT);
  assert.equal(result.permission.source, "policy");
  assert.equal(result.denied, true);
  assert.match(f.batch().hookSpecificOutput.additionalContext, /"source":"policy"/);
  assert.equal(result.result.output, SDK_CANCELLATION_TEXT); // retain provider evidence
});

for (const [verdict, source] of [["reject", "validator_reject"], ["escalate", "validator_escalation"]]) {
  test(`validator ${verdict} preserves model-facing and audit cause`, async () => {
    const f = fixture("auto");
    f.validate({ verdict, rationale: "Scope is not established" });
    const result = await f.authorize();
    assert.equal(result.behavior, "deny");
    assert.equal(result.permission.source, source);
    assert.match(result.message, new RegExp(source));
    assert.equal(f.result("denied").permission.source, source);
  });
}

test("validator allow permits the read; unavailable and malformed validators fail closed", async () => {
  const allowed = fixture("auto");
  assert.equal((await allowed.authorize()).behavior, "allow");
  for (const validation of [new Error("validator unavailable"), { unexpected: true }]) {
    const f = fixture("auto");
    f.validate(validation);
    const denied = await f.authorize();
    assert.equal(denied.behavior, "deny");
    assert.equal(denied.permission.source, "validator_error");
    assert.equal(f.result("denied").denied, true);
  }
});

test("pending approval is not rejection, and explicit decline retains its distinct provenance", async () => {
  const f = fixture("default");
  const pending = f.authorize();
  assert.equal(f.emitted.at(-1).permission.decision, "pending");
  await f.resolve("decline");
  const denied = await pending;
  assert.equal(denied.permission.source, "approval_declined");
  assert.equal(f.result("denied").permission.source, "approval_declined");
});

test("SDK callback cancellation does not claim an owner stop", async () => {
  const f = fixture("default");
  const pending = f.authorize();
  f.signal.abort();
  const denied = await pending;
  assert.equal(denied.behavior, "deny");
  assert.equal(denied.interrupt, true);
  assert.equal(denied.permission.source, "sdk_abort");
  assert.doesNotMatch(denied.message, /Interrupted by user/);
  assert.equal(f.approvals.size, 0);
});

test("runtime StopAgent cancellation remains a stop, distinct from SDK signal and timeout", async () => {
  for (const source of ["runtime_interrupt", "runtime_timeout"]) {
    const f = fixture("default");
    const pending = f.authorize();
    if (source === "runtime_interrupt") await f.cancel();
    else f.runs.get("turn").abort({ source });
    f.signal.abort();
    const denied = await pending;
    assert.equal(denied.interrupt, true);
    assert.equal(denied.permission.source, source);
    assert.equal(denied.permission.decision, "interrupted");
    assert.equal(f.result(SDK_CANCELLATION_TEXT).permission.source, source);
  }
});

test("SDK rule and classifier denials stay distinct from owner rejection", async () => {
  for (const reasonType of ["rule", "classifier", "mode"]) {
    const f = fixture();
    await f.authorize();
    f.sdkDeny(reasonType);
    const result = f.result("denied");
    assert.equal(result.denied, true);
    assert.equal(result.permission.source, "sdk_policy");
    assert.equal(result.permission.sdkReasonType, reasonType);
    assert.match(f.batch().hookSpecificOutput.additionalContext, /sdk_policy/);
  }
});

test("late approval cannot reauthorize a runtime-stopped turn", async () => {
  const f = fixture("default");
  const pending = f.authorize();
  await f.cancel();
  await f.resolve("accept");
  const result = await pending;
  assert.equal(result.behavior, "deny");
  assert.equal(result.interrupt, true);
  assert.equal(result.permission.source, "runtime_interrupt");
});

test("unknown SDK cancellation after allow preserves uncertainty, raw output and no retry authority", async () => {
  const f = fixture();
  await f.authorize();
  const result = f.result(SDK_CANCELLATION_TEXT);
  assert.equal(result.permission.source, "sdk_unknown");
  assert.equal(result.permission.decision, "unknown");
  assert.equal(result.denied, false);
  assert.match(permissionContext(result.permission), /not.*owner/i);
  assert.match(f.batch().hookSpecificOutput.additionalContext, /Do not retry/);
  assert.match(f.batch().hookSpecificOutput.additionalContext, /not an instruction to stop/);
  assert.equal(result.result.output, SDK_CANCELLATION_TEXT);
});

test("batch context explains SDK cancellation even before the tool result is delivered", async () => {
  const f = fixture();
  await f.authorize();
  const context = f.batch(SDK_CANCELLATION_TEXT).hookSpecificOutput.additionalContext;
  assert.match(context, /"source":"sdk_unknown"/);
  assert.doesNotMatch(context, /"decision":"deny"/);
  assert.deepEqual(f.batch("clean"), {}); // an ordinary allowed result gets no context

  const stopped = fixture();
  await stopped.authorize();
  await stopped.cancel();
  assert.match(stopped.batch(SDK_CANCELLATION_TEXT).hookSpecificOutput.additionalContext, /"source":"runtime_interrupt"/);
});

test("refusal fallback retracts a cancelled tool result without attributing a stop", async () => {
  for (const retract of [
    (f) => f.refusal({ subtype: "model_refusal_fallback", fallback_model: "claude-sonnet-5-5", api_refusal_category: "cyber", retracted_message_uuids: ["wire-1"] }),
    (f) => f.supersede(["wire-1"]),
  ]) {
    const f = fixture();
    await f.authorize();
    assert.equal(f.result(SDK_CANCELLATION_TEXT, true, "wire-1").permission.source, "sdk_unknown");
    retract(f);
    const retracted = f.emitted.find((event) => event.permission?.source === "sdk_retracted");
    assert.equal(retracted.callId, "call");
    assert.equal(retracted.status, "error");
    assert.equal(retracted.denied, false);
    assert.equal(retracted.permission.decision, "unknown");
    assert.equal(retracted.result.output, SDK_CANCELLATION_TEXT); // raw provider evidence kept
    assert.match(retracted.permission.reason, /do not replay it automatically/);
    assert.match(f.batch().hookSpecificOutput.additionalContext, /sdk_retracted/);
  }
});

test("a superseding replacement result is the final projection of its call", async () => {
  const f = fixture();
  const toolResult = (uuid, content, extra = {}) => ({
    type: "user", uuid, ...extra,
    message: { content: [{ type: "tool_result", tool_use_id: "call", is_error: content === SDK_CANCELLATION_TEXT, content }] },
  });
  await f.turn([
    { type: "assistant", message: { content: [{ type: "tool_use", id: "call", name: "Bash", input: { command: "gh pr checks" } }] } },
    toolResult("wire-1", SDK_CANCELLATION_TEXT),
    toolResult("wire-2", "all checks passed", { supersedes: ["wire-1"] }),
    { type: "result", subtype: "success", result: "done" },
  ]);
  const projections = f.emitted.filter((event) => event.event === "tool_call" && event.status !== "running");
  assert.deepEqual(
    projections.map((event) => [event.permission?.source, event.result.output]),
    [["sdk_unknown", SDK_CANCELLATION_TEXT], ["sdk_retracted", SDK_CANCELLATION_TEXT], [undefined, "all checks passed"]],
  );
});

test("originating path: unattributed SDK cancellation does not stop the turn and is diagnosable", async () => {
  const f = fixture();
  await f.turn([
    { type: "assistant", message: { content: [{ type: "tool_use", id: "call", name: "Bash", input: { command: "gh pr checks 613" } }] } },
    { type: "system", subtype: "thinking_tokens" },
    { type: "system", subtype: "task_notification", task_id: "monitor", status: "completed" },
    { type: "user", uuid: "wire-1", message: { content: [{ type: "tool_result", tool_use_id: "call", is_error: true, content: SDK_CANCELLATION_TEXT }] } },
    { type: "result", subtype: "success", result: "continued" },
  ]);
  const result = f.emitted.find((event) => event.event === "tool_call" && event.status === "error");
  assert.equal(result.permission.source, "sdk_unknown");
  assert.equal(result.denied, false);
  assert.equal(result.result.output, SDK_CANCELLATION_TEXT); // no rewrite, no replay
  assert.match(permissionContext(result.permission), /not an instruction to stop/);
  const warning = f.emitted.find((event) => event.event === "warning");
  assert.match(warning.message, /not an owner stop/);
  assert.match(warning.message, /task_notification \+[\d.]+s, unattributed cancellation of Bash/);
  assert.doesNotMatch(warning.message, /thinking_tokens/);
  assert.equal(f.emitted.at(-2).status, "finished"); // turn_completed precedes the history snapshot
  assert.equal(f.runs.size, 0);
});

test("a runtime stop during a turn is never reported as unattributed", async () => {
  const f = fixture();
  const stop = { type: "system", subtype: "status" };
  await f.turn([
    { type: "assistant", message: { content: [{ type: "tool_use", id: "call", name: "Bash", input: { command: "sleep 9" } }] } },
    stop,
    { type: "user", message: { content: [{ type: "tool_result", tool_use_id: "call", is_error: true, content: SDK_CANCELLATION_TEXT }] } },
  ], () => f.cancel());
  const result = f.emitted.find((event) => event.event === "tool_call" && event.status === "error");
  assert.equal(result.permission.source, "runtime_interrupt");
  assert.equal(f.emitted.some((event) => event.event === "warning"), false);
});

// Models Claude Code's input lifetime: once the SDK finishes the prompt iterable it closes stdin,
// and a later hook-gated tool call fails with the generic cancellation text ("Stream closed").
function backgroundTaskSdk(f, { followUpTurn = true, notify = followUpTurn } = {}) {
  const pause = () => new Promise((resolve) => setImmediate(resolve));
  return async function* (prompt) {
    let inputClosed = false;
    const iterator = prompt[Symbol.asyncIterator]();
    await iterator.next();
    const drained = iterator.next().then(() => { inputClosed = true; });
    const toolTurn = (id, command) => [
      { type: "assistant", message: { content: [{ type: "tool_use", id, name: "Bash", input: { command } }] } },
      { type: "user", message: { content: [{ type: "tool_result", tool_use_id: id, is_error: inputClosed,
        content: inputClosed ? SDK_CANCELLATION_TEXT : "ok" }] } },
    ];
    yield { type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "ci", task_type: "local_bash" }] };
    yield* toolTurn("first", "gh pr checks 202");
    yield { type: "result", subtype: "success", result: "waiting on CI" };
    await pause();
    if (notify) yield { type: "system", subtype: "task_notification", task_id: "ci", status: "completed" };
    yield { type: "system", subtype: "background_tasks_changed", tasks: [] };
    await pause();
    if (followUpTurn) {
      yield* toolTurn("after-notification", "gh pr checks 202");
      yield { type: "result", subtype: "success", result: "checks passed" };
    }
    await drained;
    f.inputClosedAtEnd = inputClosed;
  };
}

test("originating path: a background-task turn keeps its tool calls instead of SDK cancellation", async () => {
  const f = fixture();
  f.useSdk(backgroundTaskSdk(f));
  await f.turn();
  const results = f.emitted.filter((event) => event.event === "tool_call" && event.status !== "running");
  assert.deepEqual(results.map((event) => [event.callId, event.status, event.result.output]), [
    ["first", "completed", "ok"],
    ["after-notification", "completed", "ok"],
  ]);
  assert.equal(results.some((event) => event.permission?.source === "sdk_unknown"), false);
  assert.equal(f.emitted.some((event) => event.event === "warning"), false);
  assert.equal(f.inputClosedAtEnd, true); // input still closes once no task can start a turn
  assert.equal(f.emitted.at(-2).status, "finished");
});

test("input closes after a finished background task that starts no follow-up turn", async (t) => {
  for (const notify of [false, true]) {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const f = fixture();
    f.useSdk(backgroundTaskSdk(f, { followUpTurn: false, notify }));
    const turn = f.turn();
    for (let tick = 0; tick < 20 && f.inputClosedAtEnd === undefined; tick += 1) {
      await new Promise((resolve) => setImmediate(resolve));
      t.mock.timers.tick(1000);
    }
    await turn;
    t.mock.timers.reset();
    assert.equal(f.inputClosedAtEnd, true);
    assert.equal(f.emitted.at(-2).event, "turn_completed");
  }
});

test("refusal notices never relabel a runtime stop or a policy denial", async () => {
  const stopped = fixture("default");
  const pending = stopped.authorize();
  await stopped.cancel();
  stopped.signal.abort();
  await pending;
  stopped.result(SDK_CANCELLATION_TEXT, true, "wire-1");
  stopped.refusal({ subtype: "model_refusal_fallback", retracted_message_uuids: ["wire-1"] });
  assert.equal(stopped.emitted.some((event) => event.permission?.source === "sdk_retracted"), false);
  assert.match(stopped.emitted.at(-1).message, /retracted 0 tool result/);

  const denied = fixture();
  await denied.authorize([]);
  denied.result(SDK_CANCELLATION_TEXT, true, "wire-1");
  denied.supersede(["wire-1"]);
  assert.equal(denied.emitted.at(-1).permission.source, "policy");
});

test("refusal without fallback reports why the turn ended, not an owner stop", () => {
  const f = fixture();
  f.refusal({ subtype: "model_refusal_no_fallback", api_refusal_category: "cyber" });
  const warning = f.emitted.at(-1);
  assert.equal(warning.event, "warning");
  assert.match(warning.message, /refused this turn \(cyber\)/);
  assert.match(warning.message, /not an owner stop/);
});
