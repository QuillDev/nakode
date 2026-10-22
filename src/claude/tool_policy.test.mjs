import assert from "node:assert/strict";
import test from "node:test";
import {
  NATIVE_DELEGATION_TOOLS,
  disallowedProviderTools,
  immediateProviderToolDecision,
  preToolUseOutput,
} from "./tool_policy.mjs";

test("bypassPermissions still denies an explicitly denied exact provider identity", () => {
  const result = immediateProviderToolDecision(
    ["Bash"],
    ["Read", "Bash"],
    "bypassPermissions",
    "Bash",
  );
  assert.deepEqual(preToolUseOutput(result), {
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "Archetype policy explicitly denies Bash.",
    },
  });
});

test("bypassPermissions denies an unknown identity outside the exact projection", () => {
  const result = immediateProviderToolDecision(
    [],
    ["Read"],
    "bypassPermissions",
    "bash",
  );
  assert.equal(preToolUseOutput(result).hookSpecificOutput.permissionDecision, "deny");
});

test("bypassPermissions allows an exact projected provider identity", () => {
  const result = immediateProviderToolDecision(
    [],
    ["Read"],
    "bypassPermissions",
    "Read",
  );
  assert.deepEqual(preToolUseOutput(result), {
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "allow",
    },
  });
});

const PERMISSION_MODES = ["bypassPermissions", "auto", "acceptEdits", "default", "plan"];

test("native delegation tools are denied under every policy and permission mode", () => {
  // null is an unrestricted archetype; the list explicitly names the native tools.
  for (const allowedTools of [null, ["Read", ...NATIVE_DELEGATION_TOOLS]]) {
    for (const mode of PERMISSION_MODES) {
      for (const toolName of NATIVE_DELEGATION_TOOLS) {
        const result = immediateProviderToolDecision([], allowedTools, mode, toolName);
        assert.equal(result?.behavior, "deny", `${toolName} in ${mode}`);
        assert.match(result.message, /disabled in Nakode/);
      }
    }
  }
});

test("every tool call from inside a native subagent is denied", () => {
  for (const mode of PERMISSION_MODES) {
    const result = immediateProviderToolDecision([], null, mode, "Read", "native-subagent-1");
    assert.equal(result?.behavior, "deny", mode);
    assert.match(result.message, /native subagents cannot use tools/);
  }
});

test("Nakode delegation stays governed by the archetype policy", () => {
  assert.deepEqual(
    immediateProviderToolDecision([], null, "bypassPermissions", "mcp__nakode__delegate"),
    { behavior: "allow" },
  );
  assert.equal(
    immediateProviderToolDecision(
      ["mcp__nakode__delegate"],
      null,
      "bypassPermissions",
      "mcp__nakode__delegate",
    ).behavior,
    "deny",
  );
});

test("native delegation tools are always disallowed alongside archetype denials", () => {
  assert.deepEqual(disallowedProviderTools([]), [...NATIVE_DELEGATION_TOOLS]);
  assert.deepEqual(disallowedProviderTools(["Bash", "Agent"]), [
    "Bash",
    ...NATIVE_DELEGATION_TOOLS,
  ]);
});
