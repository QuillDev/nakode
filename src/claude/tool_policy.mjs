export function immediateProviderToolDecision(
  deniedTools,
  allowedTools,
  mode,
  toolName,
) {
  if (deniedTools.includes(toolName)) {
    return {
      behavior: "deny",
      message: `Archetype policy explicitly denies ${toolName}.`,
    };
  }
  if (allowedTools !== null && !allowedTools.includes(toolName)) {
    return {
      behavior: "deny",
      message: `Archetype policy does not allow ${toolName}.`,
    };
  }
  if (mode === "bypassPermissions") {
    return { behavior: "allow" };
  }
  return null;
}

export function preToolUseOutput(result) {
  if (result?.behavior === "allow") {
    return {
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "allow",
        ...(result.updatedInput ? { updatedInput: result.updatedInput } : {}),
      },
    };
  }
  return {
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: result?.message || "Tool use denied",
    },
  };
}
