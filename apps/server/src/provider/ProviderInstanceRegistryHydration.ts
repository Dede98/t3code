// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import { resolveExternalMcpServers } from "@t3tools/provider-core/server/externalMcpServers";
import { ProviderRegistrySessionLifecycle } from "./ProviderRegistrySessionLifecycle.ts";
/**
 * ProviderInstanceRegistryHydration — derive a `ProviderInstanceConfigMap`
 * from `ServerSettings` and keep `ProviderInstanceRegistry` in sync with it.
 *
 * `settings.providerInstances` is the source of truth. Every built-in driver
 * with a default instance also runs at `defaultInstanceIdForDriver(kind)`
 * when that slot has no entry, using the driver's default config, so a fresh
 * install shows its built-in providers without writing settings first.
 *
 * Hot-reload
 * ----------
 * On layer build we:
 *   1. Read the current `ServerSettings` once and use it to seed the
 *      registry's initial state via `ProviderInstanceRegistry.layer`.
 *   2. Fork a daemon fiber (lifetime tied to the layer's scope) that
 *      acquires `ServerSettingsService.subscribeChanges` and calls
 *      `ProviderInstanceRegistryMutator.reconcile` on every emission.
 *
 * Failures inside the watcher are logged and swallowed so a single bad
 * settings emission cannot kill the registry. Unknown drivers and invalid
 * configs already round-trip through the registry's own "unavailable"
 * shadow bucket.
 *
 * @module provider/ProviderInstanceRegistryHydration
 */
import {
  defaultInstanceIdForDriver,
  ProviderInstanceId,
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
import type { ProviderHost } from "@t3tools/provider-core/server/ProviderHost";
import type * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import type * as ServerConfig from "../config.ts";

type ProviderInstanceRegistryHydrationEnv =
  | Exclude<
      BuiltInDriversEnv,
      | ProviderOrchestrationAdapterInfrastructure.ProviderOrchestrationAdapterInfrastructure
      | AcpRegistrySupport.AcpRegistryCatalog
      | ProviderHost
    >
  | ServerSettingsService
  | ProviderRegistryRebuildBarrier
  | ProviderRegistrySessionLifecycle
  // Requirements of the `ProviderHost` the drivers receive.
  | BackgroundPolicy.BackgroundPolicy
  | ServerConfig.ServerConfig;

/**
 * Explicit `providerInstances` entries plus an implicit default instance for
 * each built-in driver whose default slot is empty. Pure so the hydration
 * rule can be tested without layers.
 */
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

  // The continuation gate is global, but Claude drivers are constructed from
  // per-instance config only. Inject the current value into every valid
  // Claude config so a global toggle changes the structurally compared entry
  // and causes the registry to rebuild every Claude instance. Any persisted
  // value in an explicit instance is intentionally ignored: the global gate
  // remains the single source of truth.
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
 * Layer that consumes `ProviderInstanceRegistryMutator` and forks a
 * settings-watcher fiber. The fiber's lifetime is tied to the enclosing
 * layer scope (process lifetime in production), so it is interrupted on
 * shutdown without leaking.
 *
 * Settings emissions only replace a versioned desired snapshot. A separate
 * worker coalesces those snapshots and waits only for sessions belonging to
 * instances that the reconciliation will remove or replace.
 */
const layerSettingsWatcher = (initialSettings: ServerSettings | undefined) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const mutator = yield* ProviderInstanceRegistryMutator.ProviderInstanceRegistryMutator;
      const lifecycle = yield* ProviderRegistrySessionLifecycle;
      const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;
      const serverSettings = yield* ServerSettingsService;
      const settingsChanges = yield* serverSettings.subscribeChanges;
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
 * Hydrate `ProviderInstanceRegistry` from `ServerSettings` and keep it in
 * sync with subsequent `streamChanges` emissions.
 *
 * The Layer's two halves:
 *   - `ProviderInstanceRegistry.layer` produces the registry +
 *     mutator from the initial config map. Its scope owns every
 *     per-instance child scope created during reconcile.
 *   - `layerSettingsWatcher` consumes the mutator, acquires its settings
 *     subscription before forking, and runs a daemon fiber in the same scope.
 *
 * Composing via `Layer.provideMerge` makes the watcher's deps available
 * from the mutable layer while still surfacing the registry as an output.
 * The mutator tag is technically also exposed; only this module imports
 * it, so the visibility leak is harmless in practice.
 */
export const layer: Layer.Layer<
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  never,
  ProviderInstanceRegistryHydrationEnv
> = Layer.unwrap(
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
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

    return layerSettingsWatcher(initialSettings).pipe(Layer.provideMerge(layerMutable));
  }),
) as Layer.Layer<
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  never,
  ProviderInstanceRegistryHydrationEnv
>;
