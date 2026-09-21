import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import { CLAUDE_SESSION_STORE_CONTINUATION_KEY } from "../Services/ClaudeSessionStore.ts";

import {
  claudeSignedOutMessage,
  makeClaudeCapabilitiesCacheKey,
  makeClaudeContinuationGroupKey,
  makeClaudeEnvironment,
  makeClaudeThreadContinuationGroupKey,
  resolveClaudeConfigDirPath,
  resolveClaudeHomePath,
  resolveClaudeTranscriptDirPath,
} from "./ClaudeHome.ts";

it.layer(NodeServices.layer)("ClaudeHome", (it) => {
  describe("Claude home resolution", () => {
    it.effect(
      "shares the native memory root while keeping account and continuation identities private",
      () =>
        Effect.gen(function* () {
          const path = yield* Path.Path;
          const baseEnv = { HOME: NodeOS.homedir(), KEEP: "unchanged" };
          const first = {
            configDirPath: "~/.claude-a",
            homePath: "",
            sharedHomePath: "~/.claude-shared",
          };
          const second = { ...first, configDirPath: "~/.claude-b" };
          const firstEnv = yield* makeClaudeEnvironment(first, baseEnv);
          const secondEnv = yield* makeClaudeEnvironment(second, baseEnv);

          expect(firstEnv.CLAUDE_CODE_REMOTE_MEMORY_DIR).toBe(
            path.join(NodeOS.homedir(), ".claude-shared"),
          );
          expect(secondEnv.CLAUDE_CODE_REMOTE_MEMORY_DIR).toBe(
            firstEnv.CLAUDE_CODE_REMOTE_MEMORY_DIR,
          );
          expect(firstEnv.CLAUDE_CONFIG_DIR).not.toBe(secondEnv.CLAUDE_CONFIG_DIR);
          expect(firstEnv.HOME).toBe(baseEnv.HOME);
          expect(firstEnv.KEEP).toBe("unchanged");
          expect(baseEnv).not.toHaveProperty("CLAUDE_CODE_REMOTE_MEMORY_DIR");
          expect(yield* makeClaudeContinuationGroupKey(first, baseEnv)).not.toBe(
            yield* makeClaudeContinuationGroupKey(second, baseEnv),
          );
        }),
    );

    it.effect("clears the memory override without changing default account lookup", () =>
      Effect.gen(function* () {
        const config = { configDirPath: "", homePath: "", sharedHomePath: "~/.claude-shared" };
        const baseEnv = { HOME: NodeOS.homedir() };
        const shared = yield* makeClaudeEnvironment(config, baseEnv);
        expect(shared.CLAUDE_CONFIG_DIR).toBeUndefined();
        expect(shared.CLAUDE_CODE_REMOTE_MEMORY_DIR).toBeDefined();
        expect(yield* makeClaudeEnvironment({ ...config, sharedHomePath: "" }, baseEnv)).toBe(
          baseEnv,
        );
      }),
    );

    it.effect("prefers the configured memory root and expands an inherited fallback", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const config = { configDirPath: "", homePath: "", sharedHomePath: "~/.claude-shared" };
        const baseEnv = { CLAUDE_CODE_REMOTE_MEMORY_DIR: "~/.claude-inherited" };
        expect((yield* makeClaudeEnvironment(config, baseEnv)).CLAUDE_CODE_REMOTE_MEMORY_DIR).toBe(
          path.join(NodeOS.homedir(), ".claude-shared"),
        );
        expect(
          (yield* makeClaudeEnvironment({ ...config, sharedHomePath: "" }, baseEnv))
            .CLAUDE_CODE_REMOTE_MEMORY_DIR,
        ).toBe(path.join(NodeOS.homedir(), ".claude-inherited"));
      }),
    );

    it.effect("treats empty, ~/.claude, and the expanded default as the same Claude home", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(path.join(NodeOS.homedir(), ".claude"));

        expect(yield* resolveClaudeHomePath({ homePath: "" })).toBe(resolved);
        expect(yield* resolveClaudeHomePath({ homePath: "~/.claude" })).toBe(resolved);
        expect(yield* resolveClaudeHomePath({ homePath: resolved })).toBe(resolved);
        expect(yield* resolveClaudeTranscriptDirPath({ configDirPath: "", homePath: "" })).toBe(
          path.join(resolved, "projects"),
        );
        expect(yield* makeClaudeEnvironment({ configDirPath: "", homePath: "" })).toBe(process.env);

        const key = `claude:home:${resolved}`;
        for (const config of [
          { configDirPath: "", homePath: "" },
          { configDirPath: "", homePath: "~/.claude" },
          { configDirPath: "", homePath: resolved },
          { configDirPath: "~/.claude", homePath: "" },
        ]) {
          expect(yield* makeClaudeContinuationGroupKey(config)).toBe(key);
        }
        expect(
          yield* makeClaudeContinuationGroupKey(
            { configDirPath: "", homePath: "" },
            { CLAUDE_CONFIG_DIR: "~/.claude" },
          ),
        ).toBe(key);
      }),
    );

    it.effect("uses legacy Claude home paths as CLAUDE_CONFIG_DIR without overriding HOME", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const homePath = "~/.claude-work";
        const resolved = path.resolve(NodeOS.homedir(), ".claude-work");

        const config = { configDirPath: "", homePath };
        expect(yield* resolveClaudeHomePath(config)).toBe(resolved);
        expect(yield* resolveClaudeTranscriptDirPath(config)).toBe(path.join(resolved, "projects"));
        expect((yield* makeClaudeEnvironment(config)).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect((yield* makeClaudeEnvironment(config)).HOME).toBe(process.env.HOME);
        expect(yield* makeClaudeContinuationGroupKey(config)).toBe(`claude:home:${resolved}`);
        expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...config })).toBe(
          `claude\0claude:home:${resolved}\0`,
        );
      }),
    );

    it.effect("uses and expands CLAUDE_CONFIG_DIR for environment and provider identity", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir(), ".claude-personal");
        const config = { configDirPath: "~/.claude-personal", homePath: "" };

        expect(yield* resolveClaudeConfigDirPath(config)).toBe(resolved);
        expect(yield* resolveClaudeTranscriptDirPath(config)).toBe(path.join(resolved, "projects"));
        expect((yield* makeClaudeEnvironment(config)).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey(config)).toBe(`claude:home:${resolved}`);
        expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...config })).toBe(
          `claude\0claude:home:${resolved}\0`,
        );
      }),
    );

    it.effect("normalizes CLAUDE_CONFIG_DIR supplied through instance environment", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir(), ".claude-work");
        const config = { configDirPath: "", homePath: "" };
        const baseEnv = { CLAUDE_CONFIG_DIR: "~/.claude-work" };

        expect(yield* resolveClaudeHomePath(config, baseEnv)).toBe(resolved);
        expect((yield* makeClaudeEnvironment(config, baseEnv)).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect(yield* resolveClaudeTranscriptDirPath(config, baseEnv)).toBe(
          path.join(resolved, "projects"),
        );
        expect(yield* makeClaudeContinuationGroupKey(config, baseEnv)).toBe(
          `claude:home:${resolved}`,
        );
        expect(yield* resolveClaudeHomePath({ homePath: "~/.claude-explicit" }, baseEnv)).toBe(
          path.resolve(NodeOS.homedir(), ".claude-explicit"),
        );
      }),
    );

    it("points the signed-out hint at the configured Claude home", () => {
      expect(claudeSignedOutMessage({ configDir: undefined, cwd: "/synthetic" })).toContain(
        "run `claude auth login`",
      );
      const configDir = "/synthetic/Claude work's $literal";
      const message = claudeSignedOutMessage({ configDir, cwd: "/synthetic/project" });
      expect(message).toContain(`CLAUDE_CONFIG_DIR set to "${configDir}"`);
      expect(message).not.toContain("CLAUDE_CONFIG_DIR=");
      expect(message).toContain("then start a new thread");
    });

    it.effect("separates capability probes by cwd", () =>
      Effect.gen(function* () {
        const config = { binaryPath: "claude", configDirPath: "", homePath: "" };
        const first = yield* makeClaudeCapabilitiesCacheKey(config, undefined, "/repo-a");
        const second = yield* makeClaudeCapabilitiesCacheKey(config, undefined, "/repo-b");
        expect(first).not.toBe(second);
      }),
    );

    it.effect(
      "shares only thread continuation identity when the cross-account gate is enabled",
      () =>
        Effect.gen(function* () {
          const work = {
            configDirPath: "~/.claude-work",
            homePath: "",
            crossAccountContinuationEnabled: true,
          };
          const personal = {
            configDirPath: "~/.claude-personal",
            homePath: "",
            crossAccountContinuationEnabled: true,
          };

          expect(yield* makeClaudeThreadContinuationGroupKey(work)).toBe(
            CLAUDE_SESSION_STORE_CONTINUATION_KEY,
          );
          expect(yield* makeClaudeThreadContinuationGroupKey(personal)).toBe(
            CLAUDE_SESSION_STORE_CONTINUATION_KEY,
          );
          expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...work })).not.toBe(
            yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...personal }),
          );
        }),
    );
  });
});
