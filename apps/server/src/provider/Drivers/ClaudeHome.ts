import * as NodeOS from "node:os";

import type { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

import { expandHomePath } from "../../pathExpansion.ts";

export const resolveClaudeHomePath = Effect.fn("resolveClaudeHomePath")(function* (
  config: Pick<ClaudeSettings, "homePath">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<string, never, Path.Path> {
  const path = yield* Path.Path;
  const homePath = config.homePath.trim();
  const inheritedHome = baseEnv?.HOME?.trim();
  return path.resolve(
    homePath.length > 0
      ? expandHomePath(homePath)
      : inheritedHome
        ? expandHomePath(inheritedHome)
        : NodeOS.homedir(),
  );
});

export const resolveClaudeConfigDirPath = Effect.fn("resolveClaudeConfigDirPath")(function* (
  config: Pick<ClaudeSettings, "configDirPath" | "homePath">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<string, never, Path.Path> {
  const path = yield* Path.Path;
  const configuredPath = config.configDirPath.trim();
  const inheritedPath = baseEnv?.CLAUDE_CONFIG_DIR?.trim();
  const configDirPath = configuredPath || inheritedPath;
  if (configDirPath) return path.resolve(expandHomePath(configDirPath));

  const homePath = yield* resolveClaudeHomePath(config, baseEnv);
  return path.join(homePath, ".claude");
});

export const makeClaudeEnvironment = Effect.fn("makeClaudeEnvironment")(function* (
  config: Pick<ClaudeSettings, "configDirPath" | "homePath">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<NodeJS.ProcessEnv, never, Path.Path> {
  const resolvedBaseEnv = baseEnv ?? process.env;
  const homePath = config.homePath.trim();
  const configDirPath = config.configDirPath.trim();
  const inheritedConfigDirPath = resolvedBaseEnv.CLAUDE_CONFIG_DIR?.trim();
  if (homePath.length === 0 && configDirPath.length === 0 && !inheritedConfigDirPath) {
    return resolvedBaseEnv;
  }

  const environment = { ...resolvedBaseEnv };
  if (homePath.length > 0) {
    environment.HOME = yield* resolveClaudeHomePath(config, resolvedBaseEnv);
  }
  if (configDirPath.length > 0 || inheritedConfigDirPath) {
    environment.CLAUDE_CONFIG_DIR = yield* resolveClaudeConfigDirPath(config, resolvedBaseEnv);
  }
  return environment;
});

export const makeClaudeContinuationGroupKey = Effect.fn("makeClaudeContinuationGroupKey")(
  function* (
    config: Pick<ClaudeSettings, "configDirPath" | "homePath">,
    baseEnv?: NodeJS.ProcessEnv,
  ): Effect.fn.Return<string, never, Path.Path> {
    const hasConfigDir = Boolean(config.configDirPath.trim() || baseEnv?.CLAUDE_CONFIG_DIR?.trim());
    if (hasConfigDir) {
      const resolvedConfigDirPath = yield* resolveClaudeConfigDirPath(config, baseEnv);
      return `claude:config:${resolvedConfigDirPath}`;
    }

    const resolvedHomePath = yield* resolveClaudeHomePath(config, baseEnv);
    return `claude:home:${resolvedHomePath}`;
  },
);

export const makeClaudeCapabilitiesCacheKey = Effect.fn("makeClaudeCapabilitiesCacheKey")(
  function* (
    config: Pick<ClaudeSettings, "binaryPath" | "configDirPath" | "homePath">,
    baseEnv?: NodeJS.ProcessEnv,
  ): Effect.fn.Return<string, never, Path.Path> {
    const continuationGroupKey = yield* makeClaudeContinuationGroupKey(config, baseEnv);
    return `${config.binaryPath}\0${continuationGroupKey}`;
  },
);
