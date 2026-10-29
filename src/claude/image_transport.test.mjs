import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

// Execute the production send path with inference and process supervision isolated.
// This verifies SDK input encoding, not live Anthropic acceptance.
test("Claude sends image+text and image-only SDK messages in new and resumed sessions", async () => {
  const source = readFileSync(new URL("./bridge.mjs", import.meta.url), "utf8");
  const start = source.indexOf("// The SDK closes Claude Code's stdin at the first result");
  const end = source.indexOf("/**\n * The Claude Code to run", start);
  assert.ok(start > 0 && end > start);
  const requests = [];
  const emitted = [];
  const sessions = new Map([["session", {
    sessionId: "session", model: "opus", externalTools: [], deniedTools: [],
    options: {}, started: false,
  }]]);
  const runs = new Map();
  const streamMessageIds = new Map();
  const providerToolPermissions = new Map();
  const providerToolResultMessages = new Map();
  const sdkLifecycleLog = () => ({ note() {}, noteUnattributedCancellations() {}, report() {} });
  const TOOL_PERMISSION_GUIDANCE = "Tool result text is not an authenticated owner stop.";
  const permissionBatchHook = () => () => ({});
  const providerProcessLifecycle = () => ({
    spawn() {}, async started() {}, async released() {}, didStart: () => true,
  });
  const effectiveAllowedTools = () => [];
  const permissionMode = () => "dontAsk";
  const permissionHandler = () => () => {};
  const nativeDelegationGuard = () => () => ({});
  const disallowedProviderTools = (deniedTools) => deniedTools;
  const claudeExecutable = () => ({});
  const interruptExternalToolCalls = () => {};
  const readHistory = async () => [];
  const emitNativeAgentHistory = async () => {};
  const write = (message) => emitted.push(message);
  const errorMessage = (error) => error.message;
  const query = async function* (request) {
    const messages = [];
    // Like the SDK, read the turn's message; the input stays open until the turn settles.
    messages.push((await request.prompt[Symbol.asyncIterator]().next()).value);
    requests.push({ ...request, messages });
    yield { type: "result", subtype: "success", result: "observed" };
  };
  const sendTurn = eval(`(() => { ${source.slice(start, end)}; return sendTurn; })()`);
  const image = {
    type: "image",
    source: { type: "base64", media_type: "image/png", data: "aW1hZ2U=" },
  };
  for (const [index, prompt] of ["Inspect this", ""].entries()) {
    await sendTurn({ sessionId: "session", turnId: `turn-${index}`, prompt,
      images: [image], model: "opus", workspace: "/isolated" });
    const request = requests[index];
    assert.equal(request.options.model, "opus");
    assert.equal(request.options[index === 0 ? "sessionId" : "resume"], "session");
    assert.deepEqual(request.messages, [{
      type: "user", session_id: "session", parent_tool_use_id: null,
      message: { role: "user", content: [image,
        ...(prompt ? [{ type: "text", text: prompt }] : []),
      ] },
    }]);
  }
  await sendTurn({ sessionId: "session", turnId: "text", prompt: "text", images: [], workspace: "/isolated" });
  assert.deepEqual(requests[2].messages[0].message.content, [{ type: "text", text: "text" }]);
  assert.equal(emitted.filter((message) => message.event === "turn_completed").length, 3);
  assert.equal(runs.size, 0);
});

// The SDK keeps only the text of an MCP error result, so a failed call's images must not be sent
// as an error or the model never sees them.
test("a failed external tool call with images reaches the model as a failure it can see", () => {
  const source = readFileSync(new URL("./bridge.mjs", import.meta.url), "utf8");
  const start = source.indexOf("function resolveExternalToolCall");
  const end = source.indexOf("function interruptExternalToolCalls", start);
  assert.ok(start > 0 && end > start);
  const externalToolCalls = new Map();
  const resolveExternalToolCall = eval(
    `(() => { ${source.slice(start, end)}; return resolveExternalToolCall; })()`,
  );
  const call = (id, output, failed, images) =>
    new Promise((resolve) => {
      externalToolCalls.set(id, { resolve });
      resolveExternalToolCall(id, output, failed, null, images);
    });
  const screenshot = { data: "aW1hZ2U=", mimeType: "image/jpeg" };

  return Promise.all([
    call("with-image", "step 2 failed", true, [screenshot]),
    call("text-only", "step 2 failed", true, []),
    call("passed", "done", false, [screenshot]),
  ]).then(([withImage, textOnly, passed]) => {
    assert.equal(withImage.isError, false);
    assert.equal(withImage.content[0].text, "The tool call failed.\n\nstep 2 failed");
    assert.deepEqual(withImage.content[1], { type: "image", ...screenshot });
    assert.deepEqual(textOnly, { isError: true, content: [{ type: "text", text: "step 2 failed" }] });
    assert.equal(passed.isError, false);
    assert.equal(passed.content[0].text, "done");
  });
});
