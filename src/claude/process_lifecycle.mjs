import { spawn as spawnChild } from "node:child_process";

const FORCE_RELEASE_AFTER_MS = 2_000;
const RELEASE_FAILURE_AFTER_MS = 7_000;

/**
 * Own one Claude Code subprocess and expose its close event as the session-release barrier.
 * The SDK's iterator may settle as soon as cancellation is observed; only this child close proves
 * that another process may safely resume the same provider session.
 */
export function providerProcessLifecycle(oauthAccessToken) {
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

      const env = { ...options.env };
      delete env.ANTHROPIC_API_KEY;
      delete env.ANTHROPIC_AUTH_TOKEN;
      delete env.CLAUDE_CODE_OAUTH_TOKEN;
      env.CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR = "3";
      child = spawnChild(options.command, options.args, {
        cwd: options.cwd,
        env,
        signal: options.signal,
        stdio: ["pipe", "pipe", "pipe", "pipe"],
      });
      child.stdio[3].end(oauthAccessToken);
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
            if (child !== null && child.exitCode === null) child.kill("SIGKILL");
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
