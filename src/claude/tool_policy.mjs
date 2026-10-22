// Claude Code tools that start or address agents outside Nakode: subagents (`Agent`, legacy
// `Task`), workflows, agent teams and remote sessions. Nakode cannot supervise, attribute or
// limit those agents, so they are never offered and never run. Nakode's own `delegate` tool is
// the only delegation route.
export const NATIVE_DELEGATION_TOOLS = Object.freeze([
  "Agent",
  "Task",
  "Workflow",
  "SendMessage",
  "TeamCreate",
  "TeamDelete",
  "RemoteTrigger",
]);

const NAKODE_DELEGATION_HINT =
  "Delegate through the Nakode delegate tool when it is offered; otherwise do the work directly.";

// Native delegation tools are removed from every session, whatever its archetype policy.
export function disallowedProviderTools(deniedTools) {
  return [...new Set([...deniedTools, ...NATIVE_DELEGATION_TOOLS])];
}

// Denies native delegation even when a configuration path re-offers the tool, and denies every
// tool call made from inside a native subagent (for example a forked skill), so such an agent
// cannot act without Nakode.
export function nativeDelegationDecision(toolName, subagentId) {
  if (NATIVE_DELEGATION_TOOLS.includes(toolName)) {
    return {
      behavior: "deny",
      message: `Claude Code's native ${toolName} tool is disabled in Nakode, which cannot supervise or attribute its agents. ${NAKODE_DELEGATION_HINT}`,
    };
  }
  if (subagentId) {
    return {
      behavior: "deny",
      message: `Claude Code native subagents cannot use tools in Nakode, which cannot supervise or attribute them. ${NAKODE_DELEGATION_HINT}`,
    };
  }
  return null;
}

export function immediateProviderToolDecision(
  deniedTools,
  allowedTools,
  mode,
  toolName,
  subagentId = null,
) {
  const native = nativeDelegationDecision(toolName, subagentId);
  if (native) {
    return native;
  }
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
