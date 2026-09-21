import * as NodeOS from "node:os";

import type { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { CLAUDE_SESSION_STORE_CONTINUATION_KEY } from "../Services/ClaudeSessionStore.ts";

const quotePath = Schema.encodeSync(Schema.fromJsonString(Schema.String));

const resolveProcessHomePath = (baseEnv?: NodeJS.ProcessEnv): string => {
  const inheritedHome = baseEnv?.HOME?.trim();
  return inheritedHome ? expandHomePath(inheritedHome) : NodeOS.homedir();
};

/**
 * Resolve the Claude config directory the legacy `homePath` field points at:
 * the instance's `homePath`, then an inherited `CLAUDE_CONFIG_DIR`, then
 * Claude's default `~/.claude`. Empty must not fall back to bare `$HOME`;
 * that leftover from the old HOME override produced a different continuation
 * group than an explicit `~/.claude`.
 */
export const resolveClaudeHomePath = Effect.fn("resolveClaudeHomePath")(function* (
  config: Pick<ClaudeSettings, "homePath">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<string, never, Path.Path> {
  const path = yield* Path.Path;
  const homePath = config.homePath.trim();
  if (homePath.length > 0) {
    return path.resolve(expandHomePath(homePath));
  }
  const inheritedConfigDirPath = baseEnv?.CLAUDE_CONFIG_DIR?.trim();
  if (inheritedConfigDirPath) {
    return path.resolve(expandHomePath(inheritedConfigDirPath));
  }
  return path.resolve(resolveProcessHomePath(baseEnv), ".claude");
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

  // `homePath` is the legacy field used by existing profiles. Treat its
  // value as CLAUDE_CONFIG_DIR rather than overriding process HOME, which
  // would also relocate the macOS login keychain lookup.
  return yield* resolveClaudeHomePath(config, baseEnv);
});

export const resolveClaudeTranscriptDirPath = Effect.fn("resolveClaudeTranscriptDirPath")(
  function* (
    config: Pick<ClaudeSettings, "configDirPath" | "homePath">,
    baseEnv?: NodeJS.ProcessEnv,
  ): Effect.fn.Return<string, never, Path.Path> {
    const path = yield* Path.Path;
    const configDirPath = yield* resolveClaudeConfigDirPath(config, baseEnv);
    return path.join(configDirPath, "projects");
  },
);

export const makeClaudeEnvironment = Effect.fn("makeClaudeEnvironment")(function* (
  config: Pick<ClaudeSettings, "configDirPath" | "homePath"> &
    Partial<Pick<ClaudeSettings, "sharedHomePath">>,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<NodeJS.ProcessEnv, never, Path.Path> {
  const resolvedBaseEnv = baseEnv ?? process.env;
  const homePath = config.homePath.trim();
  const configDirPath = config.configDirPath.trim();
  const inheritedConfigDirPath = resolvedBaseEnv.CLAUDE_CONFIG_DIR?.trim();
  const sharedHomePath = config.sharedHomePath?.trim();
  const memoryRoot = sharedHomePath || resolvedBaseEnv.CLAUDE_CODE_REMOTE_MEMORY_DIR?.trim();
  const hasConfigDir = homePath.length > 0 || configDirPath.length > 0 || inheritedConfigDirPath;
  if (!hasConfigDir && !memoryRoot) {
    return resolvedBaseEnv;
  }

  return {
    ...resolvedBaseEnv,
    ...(hasConfigDir
      ? { CLAUDE_CONFIG_DIR: yield* resolveClaudeConfigDirPath(config, resolvedBaseEnv) }
      : {}),
    // Claude's native memory root preserves its repository/worktree mapping and
    // user-subagent memory layout without sharing transcripts or credentials.
    // This CLI-owned variable is not a public SDK option; the opt-in native
    // compatibility test exercises it against an actual Claude executable.
    ...(memoryRoot
      ? {
          CLAUDE_CODE_REMOTE_MEMORY_DIR: (yield* Path.Path).resolve(expandHomePath(memoryRoot)),
        }
      : {}),
  };
});

/**
 * Continuation identity is the config directory the CLI actually reads, so an
 * empty profile, a legacy `homePath`, an explicit `configDirPath`, and an
 * inherited `CLAUDE_CONFIG_DIR` that all land on the same directory share one
 * group and can resume each other's sessions.
 */
export const makeClaudeContinuationGroupKey = Effect.fn("makeClaudeContinuationGroupKey")(
  function* (
    config: Pick<ClaudeSettings, "configDirPath" | "homePath">,
    baseEnv?: NodeJS.ProcessEnv,
  ): Effect.fn.Return<string, never, Path.Path> {
    const resolvedConfigDirPath = yield* resolveClaudeConfigDirPath(config, baseEnv);
    return `claude:home:${resolvedConfigDirPath}`;
  },
);

export const makeClaudeThreadContinuationGroupKey = Effect.fn(
  "makeClaudeThreadContinuationGroupKey",
)(function* (
  config: Pick<ClaudeSettings, "configDirPath" | "crossAccountContinuationEnabled" | "homePath">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<string, never, Path.Path> {
  return config.crossAccountContinuationEnabled
    ? CLAUDE_SESSION_STORE_CONTINUATION_KEY
    : yield* makeClaudeContinuationGroupKey(config, baseEnv);
});

export const makeClaudeCapabilitiesCacheKey = Effect.fn("makeClaudeCapabilitiesCacheKey")(
  function* (
    config: Pick<ClaudeSettings, "binaryPath" | "configDirPath" | "homePath">,
    baseEnv?: NodeJS.ProcessEnv,
    cwd?: string,
  ): Effect.fn.Return<string, never, Path.Path> {
    const continuationGroupKey = yield* makeClaudeContinuationGroupKey(config, baseEnv);
    return `${config.binaryPath}\0${continuationGroupKey}\0${cwd ?? ""}`;
  },
);

/**
 * Describe the spawned CLI's environment separately from the login command so
 * paths remain literal on every shell, including relative inherited values.
 */
export const claudeSignedOutMessage = (input: {
  readonly configDir: string | undefined;
  readonly cwd: string;
}): string => {
  const configuration =
    input.configDir !== undefined
      ? ` from ${quotePath(input.cwd)}, with CLAUDE_CONFIG_DIR set to ${quotePath(input.configDir)}`
      : "";
  return `Claude could not authenticate. For subscription login, run \`claude auth login\` on this environment's machine${configuration}, then start a new thread. For API-key authentication, check this instance's configured credentials.`;
};
