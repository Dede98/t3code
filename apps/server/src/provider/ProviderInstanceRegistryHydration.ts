// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import { resolveExternalMcpServers } from "@t3tools/provider-core/server/externalMcpServers";
import { ProviderRegistrySessionLifecycle } from "./ProviderRegistrySessionLifecycle.ts";
import {
  defaultInstanceIdForDriver,
  ProviderInstanceId,
  providerInstanceConfigEnabledFlag,
  resolveProviderInstanceEnabled,
  type ProviderInstanceConfig,
  type ProviderInstanceConfigMap,
  ServerSettings,
} from "@t3tools/contracts";
import * as Equal from "effect/Equal";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { ServerSettingsService } from "../serverSettings.ts";
import { BUILT_IN_DRIVERS, type BuiltInDriversEnv } from "./builtInDrivers.ts";
import * as ProviderInstanceRegistryMutator from "./ProviderInstanceRegistryMutator.ts";
import {
  ProviderRegistryRebuildBarrier,
  type ProviderRegistryRebuildBarrierShape,
} from "./ProviderRegistryRebuildBarrier.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderOrchestrationAdapterInfrastructure from "./ProviderOrchestrationAdapterInfrastructure.ts";
import * as AcpRegistrySupport from "@t3tools/provider-acp-registry/server/AcpRegistrySupport";
import * as ProviderHostLive from "./ProviderHostLive.ts";
import type * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import type * as ServerConfig from "../config.ts";

type ProviderInstanceRegistryHydrationEnv =
  | Exclude<
      BuiltInDriversEnv,
      | ProviderOrchestrationAdapterInfrastructure.ProviderOrchestrationAdapterInfrastructure
      | AcpRegistrySupport.AcpRegistryCatalog
      | ProviderHost.ProviderHost
    >
  | ServerSettingsService
  | ProviderRegistryRebuildBarrier
  | ProviderRegistrySessionLifecycle
  // Requirements of the `ProviderHost.ProviderHost` the drivers receive.
  | BackgroundPolicy.BackgroundPolicy
  | ServerConfig.ServerConfig;

export const deriveProviderInstanceConfigMap = (
  settings: ServerSettings,
): ProviderInstanceConfigMap => {
  const merged: Record<string, ProviderInstanceConfig> = { ...settings.providerInstances };

  for (const driver of BUILT_IN_DRIVERS) {
    if (driver.metadata.hasDefaultInstance === false) continue;
    const instanceId = defaultInstanceIdForDriver(driver.driverKind);
    if (instanceId in merged) continue;
    merged[instanceId] = { driver: driver.driverKind, config: {} };
  }

  // The continuation gate is global but Claude drivers read per-instance config. Inject it into
  // every Claude config so a toggle changes the compared entry and rebuilds every instance; the
  // global gate stays the single source of truth over any persisted per-instance value.
  for (const [instanceId, entry] of Object.entries(merged)) {
    if (
      entry.driver !== "claudeAgent" ||
      entry.config === null ||
      (entry.config !== undefined && typeof entry.config !== "object") ||
      globalThis.Array.isArray(entry.config)
    ) {
      continue;
    }

    merged[instanceId] = {
      ...entry,
      config: {
        ...entry.config,
        crossAccountContinuationEnabled: settings.claudeCrossAccountContinuationEnabled,
      },
    };
  }

  for (const [id, entry] of Object.entries(merged)) {
    if (
      entry.config === undefined ||
      (entry.config !== null && typeof entry.config === "object" && !Array.isArray(entry.config))
    ) {
      merged[id] = {
        ...entry,
        config: {
          ...entry.config,
          externalMcpConfiguration: NodeCrypto.createHash("sha256")
            .update(
              JSON.stringify(resolveExternalMcpServers(settings, ProviderInstanceId.make(id))),
            )
            .digest("hex"),
        },
      };
    }
  }
  // Probe project-enabled instances even if disabled machine-wide; project threads can still route to them.
  for (const overrides of Object.values(settings.projectSettingsOverrides)) {
    for (const [rawInstanceId, projectOverride] of Object.entries(
      overrides?.providerInstances ?? {},
    )) {
      if (projectOverride.enabled !== true) continue;
      const instanceId = rawInstanceId as ProviderInstanceId;
      const existing = merged[instanceId];
      if (existing === undefined || resolveProviderInstanceEnabled(existing)) continue;
      merged[instanceId] = {
        ...existing,
        enabled: true,
        ...(providerInstanceConfigEnabledFlag(existing.config) === false
          ? { config: { ...(existing.config as Record<string, unknown>), enabled: true } }
          : {}),
      };
    }
  }

  return merged as ProviderInstanceConfigMap;
};

export interface DesiredProviderRegistrySettings {
  readonly settings: ServerSettings | undefined;
  readonly version: number;
}

export function providerInstanceIdsRequiringSettle(
  current: ProviderInstanceConfigMap,
  next: ProviderInstanceConfigMap,
): ReadonlySet<ProviderInstanceId> {
  const instanceIds = new Set<ProviderInstanceId>();
  for (const [rawInstanceId, currentEntry] of Object.entries(current)) {
    const instanceId = ProviderInstanceId.make(rawInstanceId);
    const nextEntry = next[instanceId];
    if (nextEntry === undefined || !Equal.equals(currentEntry, nextEntry)) {
      instanceIds.add(instanceId);
    }
  }
  return instanceIds;
}

export interface ProviderRegistryReconcileWorkerOptions {
  readonly desired: Ref.Ref<DesiredProviderRegistrySettings>;
  readonly initialAppliedVersion: number;
  readonly initialAppliedConfigMap: ProviderInstanceConfigMap;
  readonly lifecycle: ProviderRegistrySessionLifecycle["Service"];
  readonly mutator: Pick<
    ProviderInstanceRegistryMutator.ProviderInstanceRegistryMutator["Service"],
    "reconcile"
  >;
  readonly rebuildBarrier: Pick<ProviderRegistryRebuildBarrierShape, "withRebuild">;
  readonly pollIntervalMs?: number;
}

/**
 * Coalesces settings emissions and applies only the newest provider registry
 * snapshot once sessions owned by removed or replaced instances are safe to
 * rebuild. Additions and reorder-only changes do not wait on unrelated turns.
 */
export const runProviderRegistryReconcileWorker = Effect.fn(
  "ProviderInstanceRegistryHydration.runProviderRegistryReconcileWorker",
)(function* (options: ProviderRegistryReconcileWorkerOptions) {
  const pollIntervalMs = options.pollIntervalMs ?? 100;
  let appliedVersion = options.initialAppliedVersion;
  let appliedConfigMap = options.initialAppliedConfigMap;

  while (true) {
    const desired = yield* Ref.get(options.desired);
    if (desired.settings === undefined || desired.version === appliedVersion) {
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }

    const desiredConfigMap = deriveProviderInstanceConfigMap(desired.settings);
    const affectedInstanceIds = providerInstanceIdsRequiringSettle(
      appliedConfigMap,
      desiredConfigMap,
    );
    const settled = yield* options.lifecycle
      .canRebuild(affectedInstanceIds)
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Provider settlement check failed", cause).pipe(Effect.as(false)),
        ),
      );
    if (!settled) {
      yield* Effect.logWarning(
        "Provider registry reconcile remains deferred because sessions did not settle",
        { affectedInstanceIds: [...affectedInstanceIds] },
      );
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }

    const reconcileExit = yield* options.rebuildBarrier
      .withRebuild(
        Effect.gen(function* () {
          const latest = yield* Ref.get(options.desired);
          if (latest.settings === undefined || latest.version === appliedVersion) {
            return {
              version: latest.version,
              configMap: appliedConfigMap,
            };
          }
          const latestConfigMap = deriveProviderInstanceConfigMap(latest.settings);
          const latestAffectedInstanceIds = providerInstanceIdsRequiringSettle(
            appliedConfigMap,
            latestConfigMap,
          );
          if (!(yield* options.lifecycle.canRebuild(latestAffectedInstanceIds))) {
            return undefined;
          }
          yield* options.lifecycle.closeInstances(latestAffectedInstanceIds);
          yield* options.mutator.reconcile(latestConfigMap);
          return {
            version: latest.version,
            configMap: latestConfigMap,
          };
        }),
      )
      .pipe(Effect.exit);

    if (Exit.isFailure(reconcileExit)) {
      yield* Effect.logError("ProviderInstanceRegistry reconcile failed", reconcileExit.cause);
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }
    if (reconcileExit.value === undefined) {
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }
    appliedVersion = reconcileExit.value.version;
    appliedConfigMap = reconcileExit.value.configMap;
  }
});

/**
 * The settings watcher lives in the layer scope, so shutdown interrupts it. Emissions only
 * replace a versioned desired snapshot; a separate worker coalesces them and waits only for
 * sessions of instances the reconciliation will remove or replace.
 */
const layerSettingsWatcher = (
  initialSettings: ServerSettings | undefined,
  settingsChanges: Stream.Stream<ServerSettings>,
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const mutator = yield* ProviderInstanceRegistryMutator.ProviderInstanceRegistryMutator;
      const lifecycle = yield* ProviderRegistrySessionLifecycle;
      const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;
      const serverSettings = yield* ServerSettingsService;
      const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
        settings: initialSettings,
        version: 0,
      });

      yield* runProviderRegistryReconcileWorker({
        desired,
        initialAppliedVersion: 0,
        initialAppliedConfigMap:
          initialSettings === undefined
            ? ({} as ProviderInstanceConfigMap)
            : deriveProviderInstanceConfigMap(initialSettings),
        lifecycle,
        mutator,
        rebuildBarrier,
      }).pipe(Effect.forkScoped);

      yield* settingsChanges.pipe(
        Stream.mapEffect(() =>
          serverSettings.getSettings.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Provider settings could not be materialized", cause).pipe(
                Effect.as(undefined),
              ),
            ),
          ),
        ),
        Stream.runForEach((next) =>
          next === undefined
            ? Effect.void
            : Ref.update(desired, (current) => ({
                settings: next,
                version: current.version + 1,
              })),
        ),
        Effect.forkScoped,
      );
    }),
  );

/**
 * The registry layer's scope owns every per-instance child scope; the watcher subscribes to
 * settings before forking so no change between hydration and fork is lost. `provideMerge` also
 * exposes the mutator tag, which only this module imports.
 */
export const layer: Layer.Layer<
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  never,
  ProviderInstanceRegistryHydrationEnv
> = Layer.unwrap(
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    // Subscribe before the seed read so construction cannot miss settings saves.
    const settingsChanges = yield* serverSettings.subscribeChanges;
    const initialSettings: ServerSettings | undefined = yield* serverSettings.getSettings.pipe(
      Effect.orElseSucceed(() => undefined),
    );
    const initialConfigMap =
      initialSettings === undefined
        ? ({} as ProviderInstanceConfigMap)
        : deriveProviderInstanceConfigMap(initialSettings);

    const layerMutable = ProviderInstanceRegistry.layer({
      drivers: BUILT_IN_DRIVERS,
      configMap: initialConfigMap,
    }).pipe(
      Layer.provide(ProviderOrchestrationAdapterInfrastructure.layer),
      Layer.provide(AcpRegistrySupport.layerFromHost),
      Layer.provide(ProviderHostLive.layer),
    );

    return layerSettingsWatcher(initialSettings, settingsChanges).pipe(
      Layer.provideMerge(layerMutable),
    );
  }),
) as Layer.Layer<
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  never,
  ProviderInstanceRegistryHydrationEnv
>;
