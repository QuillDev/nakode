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
  const functions = source.slice(source.indexOf("function parseValidatorResult"), source.indexOf("function emitStreamEvent"));
  const handler = source.slice(source.indexOf("async function handle(command)"), source.indexOf("const input = createInterface"));
  const api = eval(`(() => { ${functions}\n${handler}; return { permissionHandler, emitUserToolResults, permissionBatchHook, sdkPermissionDenied, handle }; })()`);
  const signal = new AbortController();
  const options = { toolUseID: "call", signal: signal.signal, suggestions: [] };
  return {
    emitted, signal, runs, approvals,
    validate(value) { validation = value; },
    authorize: (allowed = null) => api.permissionHandler("turn", session, mode, allowed)("Bash", { command: "git status --short" }, options),
    resolve: (decision) => api.handle({ method: "resolve_approval", approvalId: "call", decision }),
    cancel: () => api.handle({ method: "cancel", turnId: "turn", requestId: "authenticated-control" }),
    result(output, isError = true) {
      providerToolCalls.set("call", { name: "Bash", input: { command: "git status --short" } });
      api.emitUserToolResults("turn", { message: { content: [{ type: "tool_result", tool_use_id: "call", is_error: isError, content: output }] } });
      return emitted.at(-1);
    },
    sdkDeny: (reasonType) => api.sdkPermissionDenied("turn", {
      tool_use_id: "call", tool_name: "Bash", decision_reason_type: reasonType,
      decision_reason: "SDK denied this operation", message: "denied",
    }),
    batch: () => api.permissionBatchHook("turn")({ tool_calls: [{ tool_use_id: "call" }] }),
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
  assert.equal(result.result.output, SDK_CANCELLATION_TEXT);
});
