import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

import {
  makeClaudeCapabilitiesCacheKey,
  makeClaudeContinuationGroupKey,
  makeClaudeEnvironment,
  resolveClaudeConfigDirPath,
  resolveClaudeHomePath,
} from "./ClaudeHome.ts";

it.layer(NodeServices.layer)("ClaudeHome", (it) => {
  describe("Claude home resolution", () => {
    it.effect("uses the process home when no Claude home override is configured", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir());

        expect(yield* resolveClaudeHomePath({ homePath: "" })).toBe(resolved);
        expect(yield* makeClaudeEnvironment({ configDirPath: "", homePath: "" })).toBe(process.env);
      }),
    );

    it.effect("resolves configured Claude HOME and stamps continuation/cache keys with it", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const homePath = "~/.claude-work";
        const resolved = path.resolve(NodeOS.homedir(), ".claude-work");

        const config = { configDirPath: "", homePath };
        expect(yield* resolveClaudeHomePath(config)).toBe(resolved);
        expect((yield* makeClaudeEnvironment(config)).HOME).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey(config)).toBe(`claude:home:${resolved}`);
        expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...config })).toBe(
          `claude\0claude:home:${resolved}`,
        );
      }),
    );

    it.effect("uses and expands CLAUDE_CONFIG_DIR for environment and provider identity", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir(), ".claude-personal");
        const config = { configDirPath: "~/.claude-personal", homePath: "" };

        expect(yield* resolveClaudeConfigDirPath(config)).toBe(resolved);
        expect((yield* makeClaudeEnvironment(config)).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey(config)).toBe(`claude:config:${resolved}`);
        expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", ...config })).toBe(
          `claude\0claude:config:${resolved}`,
        );
      }),
    );

    it.effect("normalizes CLAUDE_CONFIG_DIR supplied through instance environment", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir(), ".claude-work");
        const config = { configDirPath: "", homePath: "" };
        const baseEnv = { CLAUDE_CONFIG_DIR: "~/.claude-work" };

        expect((yield* makeClaudeEnvironment(config, baseEnv)).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey(config, baseEnv)).toBe(
          `claude:config:${resolved}`,
        );
      }),
    );

    it.effect("keeps continuation compatible across instances with the same Claude HOME", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const resolved = path.resolve(NodeOS.homedir());

        expect(yield* makeClaudeContinuationGroupKey({ configDirPath: "", homePath: "" })).toBe(
          `claude:home:${resolved}`,
        );
      }),
    );
  });
});
