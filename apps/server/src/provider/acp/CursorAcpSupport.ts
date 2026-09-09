import {
  type CursorSettings,
  type ProviderOptionSelection,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  CURSOR_PARAMETERIZED_MODEL_PICKER_CAPABILITIES,
  resolveCursorAcpBaseModelId,
  resolveCursorAcpConfigUpdates,
} from "../Layers/CursorProvider.ts";
import { mapEffectFailuresPreservingReasons } from "./AcpAdapterSupport.ts";
import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

type CursorAcpRuntimeCursorSettings = Pick<CursorSettings, "apiEndpoint" | "binaryPath">;

function cursorAcpPermissionArgs(runtimeMode?: RuntimeMode): ReadonlyArray<string> {
  switch (runtimeMode) {
    case "auto":
      return ["--auto-review"];
    case "full-access":
      return ["--force"];
    default:
      return [];
  }
}

export interface CursorAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly cursorSettings: CursorAcpRuntimeCursorSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
  readonly runtimeMode?: RuntimeMode;
}

export interface CursorAcpModelSelectionErrorContext {
  readonly cause: EffectAcpErrors.AcpError;
  readonly step: "set-config-option" | "set-model";
  readonly configId?: string;
}

export function buildCursorAcpSpawnInput(
  cursorSettings: CursorAcpRuntimeCursorSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  runtimeMode?: RuntimeMode,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: cursorSettings?.binaryPath || "cursor-agent",
    args: [
      ...(cursorSettings?.apiEndpoint ? (["-e", cursorSettings.apiEndpoint] as const) : []),
      ...cursorAcpPermissionArgs(runtimeMode),
      "acp",
    ],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeCursorAcpRuntime = (
  input: CursorAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildCursorAcpSpawnInput(
          input.cursorSettings,
          input.cwd,
          input.environment,
          input.runtimeMode,
        ),
        authMethodId: "cursor_login",
        clientCapabilities: CURSOR_PARAMETERIZED_MODEL_PICKER_CAPABILITIES,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });

interface CursorAcpModelSelectionRuntime {
  readonly getConfigOptions: AcpSessionRuntime.AcpSessionRuntime["Service"]["getConfigOptions"];
  readonly setConfigOption: (
    configId: string,
    value: string | boolean,
  ) => Effect.Effect<unknown, EffectAcpErrors.AcpError>;
  readonly setModel: (model: string) => Effect.Effect<unknown, EffectAcpErrors.AcpError>;
}

export function applyCursorAcpModelSelection<E>(input: {
  readonly runtime: CursorAcpModelSelectionRuntime;
  readonly model: string | null | undefined;
  readonly selections: ReadonlyArray<ProviderOptionSelection> | null | undefined;
  readonly mapError: (context: CursorAcpModelSelectionErrorContext) => E;
}): Effect.Effect<
  {
    readonly model: string;
    readonly configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>;
    readonly configUpdates: ReadonlyArray<{
      readonly configId: string;
      readonly value: string | boolean;
    }>;
  },
  E
> {
  return Effect.gen(function* () {
    const model = resolveCursorAcpBaseModelId(input.model);
    yield* input.runtime.setModel(model).pipe(
      mapEffectFailuresPreservingReasons((cause) =>
        input.mapError({
          cause,
          step: "set-model",
        }),
      ),
    );

    const configOptions = yield* input.runtime.getConfigOptions;
    const configUpdates = resolveCursorAcpConfigUpdates(configOptions, input.selections);
    for (const update of configUpdates) {
      yield* input.runtime.setConfigOption(update.configId, update.value).pipe(
        mapEffectFailuresPreservingReasons((cause) =>
          input.mapError({
            cause,
            step: "set-config-option",
            configId: update.configId,
          }),
        ),
      );
    }
    return { model, configOptions: configOptions ?? [], configUpdates };
  });
}
