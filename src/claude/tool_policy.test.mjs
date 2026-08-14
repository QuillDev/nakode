import assert from "node:assert/strict";
import test from "node:test";
import {
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
