/**
 * ProviderInstanceRegistryHydration — derive a `ProviderInstanceConfigMap`
 * from `ServerSettings` and keep `ProviderInstanceRegistry` in sync with it.
 *
 * The server still reads two shapes:
 *
 *   1. `settings.providerInstances` — the new driver-agnostic map the
 *      registry expects. Keyed by `ProviderInstanceId`, values are
 *      `ProviderInstanceConfig` envelopes.
 *   2. `settings.providers.<kind>` — the legacy single-instance-per-driver
 *      fields (`providers.codex`, `providers.claudeAgent`, …). These are
 *      the source of truth for every deployment that hasn't been migrated
 *      yet to an explicit `providerInstances` entry.
 *
 * This module bridges (2) into (1) and wires the resulting map into a
 * mutable registry. For every built-in driver whose id is not already
 * present in `providerInstances` (keyed on
 * `defaultInstanceIdForDriver(driverKind)` — literally the driver kind as a
 * routing slug), we synthesize an envelope from the legacy field. The
 * registry decodes both flavours through the same `configSchema` and ends
 * up with one uniform `ProviderInstance` per entry.
 *
 * Explicit `providerInstances` entries always win — users can already
 * override the legacy `providers.<kind>` blob by authoring a
 * `providerInstances.codex` entry with a matching driver, and we don't
 * want the synthesized envelope to silently stomp their config.
 *
 * Hot-reload
 * ----------
 * On layer build we:
 *   1. Read the current `ServerSettings` once and use it to seed the
 *      registry's initial state via `ProviderInstanceRegistryMutableLayer`.
 *   2. Fork a daemon fiber (lifetime tied to the layer's scope) that
 *      subscribes to `ServerSettingsService.streamChanges` and calls
 *      `ProviderInstanceRegistryMutator.reconcile` on every emission.
 *
 * Failures inside the watcher are logged and swallowed so a single bad
 * settings emission cannot kill the registry. Unknown drivers and invalid
 * configs already round-trip through the registry's own "unavailable"
 * shadow bucket.
 *
 * @module provider/Layers/ProviderInstanceRegistryHydration
 */
import {
  defaultInstanceIdForDriver,
  type ProviderInstanceConfig,
  type ProviderInstanceConfigMap,
  type ProviderSession,
  ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { ServerSettingsService } from "../../serverSettings.ts";
import { BUILT_IN_DRIVERS, type BuiltInDriversEnv } from "../builtInDrivers.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import type { ProviderInstanceRegistryShape } from "../Services/ProviderInstanceRegistry.ts";
import {
  ProviderInstanceRegistryMutator,
  type ProviderInstanceRegistryMutatorShape,
} from "../Services/ProviderInstanceRegistryMutator.ts";
import {
  ProviderRegistryRebuildBarrier,
  type ProviderRegistryRebuildBarrierShape,
} from "../Services/ProviderRegistryRebuildBarrier.ts";
import { ProviderInstanceRegistryMutableLayer } from "./ProviderInstanceRegistryLive.ts";

export function isProviderSessionBusyForRegistryRebuild(
  status: ProviderSession["status"],
): boolean {
  return status === "connecting" || status === "running";
}

export interface ProviderSessionSettleWaitOptions {
  readonly pollIntervalMs?: number;
  readonly timeoutMs?: number;
}

export const areProviderSessionsSettled = Effect.fn(
  "ProviderInstanceRegistryHydration.areProviderSessionsSettled",
)(function* (registry: Pick<ProviderInstanceRegistryShape, "listInstances">, timeoutMs = 100) {
  const result = yield* Effect.gen(function* () {
    const instances = yield* registry.listInstances;
    const sessionGroups = yield* Effect.forEach(
      instances,
      (instance) => instance.adapter.listSessions().pipe(Effect.exit),
      { concurrency: "unbounded" },
    );
    return sessionGroups.every(
      (sessions) =>
        Exit.isSuccess(sessions) &&
        sessions.value.every(
          (session) =>
            !isProviderSessionBusyForRegistryRebuild(session.status) &&
            session.activeTurnId === undefined,
        ),
    );
  }).pipe(Effect.exit, Effect.timeoutOption(timeoutMs));

  return Option.isSome(result) && Exit.isSuccess(result.value) && result.value.value;
});

/**
 * Check whether every provider session is safe to tear down without allowing a
 * hung or failing adapter status read to block its caller forever.
 */
export const waitForProviderSessionsToSettle = Effect.fn(
  "ProviderInstanceRegistryHydration.waitForProviderSessionsToSettle",
)(function* (
  registry: Pick<ProviderInstanceRegistryShape, "listInstances">,
  options: ProviderSessionSettleWaitOptions = {},
) {
  const pollIntervalMs = options.pollIntervalMs ?? 100;
  const timeoutMs = options.timeoutMs ?? 1_000;
  const result = yield* Effect.gen(function* () {
    while (true) {
      const sessionGroupsExit = yield* registry.listInstances.pipe(
        Effect.flatMap((instances) =>
          Effect.forEach(
            instances,
            (instance) => instance.adapter.listSessions().pipe(Effect.exit),
            { concurrency: "unbounded" },
          ),
        ),
        Effect.exit,
      );
      if (Exit.isFailure(sessionGroupsExit)) {
        yield* Effect.sleep(pollIntervalMs);
        continue;
      }
      const sessionGroups = sessionGroupsExit.value;
      const hasBusyOrUnknownSession = sessionGroups.some(
        (sessions) =>
          Exit.isFailure(sessions) ||
          sessions.value.some(
            (session) =>
              isProviderSessionBusyForRegistryRebuild(session.status) ||
              session.activeTurnId !== undefined,
          ),
      );
      if (!hasBusyOrUnknownSession) return;
      yield* Effect.sleep(pollIntervalMs);
    }
  }).pipe(Effect.timeoutOption(timeoutMs));

  return Option.isSome(result);
});

/**
 * Synthesize a `ProviderInstanceConfigMap` from a `ServerSettings` snapshot.
 *
 * Strategy:
 *   1. Copy all explicit `settings.providerInstances` entries verbatim.
 *   2. For each built-in driver whose `defaultInstanceIdForDriver(id)` key
 *      is *not* already in the explicit map, synthesize an entry from the
 *      matching legacy `settings.providers.<kind>` blob.
 *
 * The returned map is the input the registry consumes; pure & exported
 * separately so the hydration logic can be exercised by unit tests
 * without layering.
 */
export const deriveProviderInstanceConfigMap = (
  settings: ServerSettings,
): ProviderInstanceConfigMap => {
  const merged: Record<string, ProviderInstanceConfig> = { ...settings.providerInstances };

  for (const driver of BUILT_IN_DRIVERS) {
    const instanceId = defaultInstanceIdForDriver(driver.driverKind);
    if (instanceId in merged) {
      // Explicit `providerInstances` entry for this slot — user-authored
      // config always wins over the legacy mirror.
      continue;
    }

    // Only built-in drivers have a legacy mirror; the registry's
    // `providers` struct is keyed on the same literal slug as
    // `driverKind`. Access is dynamic (the driver kind is a branded string),
    // but it's constrained to `keyof settings.providers` by the union of
    // built-in driver kinds.
    const legacyKey = driver.driverKind as keyof ServerSettings["providers"];
    const legacyConfig = settings.providers[legacyKey];
    if (legacyConfig === undefined) {
      continue;
    }

    merged[instanceId] = {
      driver: driver.driverKind,
      config: legacyConfig,
    };
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
      typeof entry.config !== "object" ||
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

  return merged as ProviderInstanceConfigMap;
};

export interface DesiredProviderRegistrySettings {
  readonly settings: ServerSettings | undefined;
  readonly version: number;
}

export interface ProviderRegistryReconcileWorkerOptions {
  readonly desired: Ref.Ref<DesiredProviderRegistrySettings>;
  readonly initialAppliedVersion: number;
  readonly registry: Pick<ProviderInstanceRegistryShape, "listInstances">;
  readonly mutator: Pick<ProviderInstanceRegistryMutatorShape, "reconcile">;
  readonly rebuildBarrier: Pick<ProviderRegistryRebuildBarrierShape, "withRebuild">;
  readonly pollIntervalMs?: number;
  readonly settleTimeoutMs?: number;
  readonly exclusiveCheckTimeoutMs?: number;
}

/**
 * Coalesces settings emissions and applies only the newest provider registry
 * snapshot once all live provider sessions are safe to rebuild.
 */
export const runProviderRegistryReconcileWorker = Effect.fn(
  "ProviderInstanceRegistryHydration.runProviderRegistryReconcileWorker",
)(function* (options: ProviderRegistryReconcileWorkerOptions) {
  const pollIntervalMs = options.pollIntervalMs ?? 100;
  const settleTimeoutMs = options.settleTimeoutMs ?? 1_000;
  const exclusiveCheckTimeoutMs = options.exclusiveCheckTimeoutMs ?? 100;
  let appliedVersion = options.initialAppliedVersion;

  while (true) {
    const desired = yield* Ref.get(options.desired);
    if (desired.settings === undefined || desired.version === appliedVersion) {
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }

    const settled = yield* waitForProviderSessionsToSettle(options.registry, {
      pollIntervalMs,
      timeoutMs: settleTimeoutMs,
    });
    if (!settled) {
      yield* Effect.logWarning(
        "Provider registry reconcile remains deferred because sessions did not settle",
      );
      yield* Effect.sleep(pollIntervalMs);
      continue;
    }

    const reconcileExit = yield* options.rebuildBarrier
      .withRebuild(
        Effect.gen(function* () {
          // New adapter operations are excluded here. Recheck so a turn that
          // started after the optimistic wait can never be interrupted.
          if (!(yield* areProviderSessionsSettled(options.registry, exclusiveCheckTimeoutMs))) {
            return undefined;
          }

          const latest = yield* Ref.get(options.desired);
          if (latest.settings === undefined || latest.version === appliedVersion) {
            return latest.version;
          }
          yield* options.mutator.reconcile(deriveProviderInstanceConfigMap(latest.settings));
          return latest.version;
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
    appliedVersion = reconcileExit.value;
  }
});

/**
 * Layer that consumes `ProviderInstanceRegistryMutator` and forks a
 * settings-watcher fiber. The fiber's lifetime is tied to the enclosing
 * layer scope (process lifetime in production), so it is interrupted on
 * shutdown without leaking.
 *
 * Settings emissions only replace a versioned desired snapshot. A separate
 * worker coalesces those snapshots and performs registry reconciliation after
 * all provider sessions settle, so unrelated settings processing never waits
 * on a live provider turn.
 */
const SettingsWatcherLive = (initialSettings: ServerSettings | undefined) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const mutator = yield* ProviderInstanceRegistryMutator;
      const registry = yield* ProviderInstanceRegistry;
      const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;
      const serverSettings = yield* ServerSettingsService;
      const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
        settings: initialSettings,
        version: 0,
      });

      yield* runProviderRegistryReconcileWorker({
        desired,
        initialAppliedVersion: 0,
        registry,
        mutator,
        rebuildBarrier,
      }).pipe(Effect.forkScoped);

      yield* serverSettings.streamChanges.pipe(
        Stream.runForEach((next) =>
          Ref.update(desired, (current) => ({
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
 *   - `ProviderInstanceRegistryMutableLayer` produces the registry +
 *     mutator from the initial config map. Its scope owns every
 *     per-instance child scope created during reconcile.
 *   - `SettingsWatcherLive` consumes the mutator and runs a daemon fiber
 *     in the same scope.
 *
 * Composing via `Layer.provideMerge` makes the watcher's deps available
 * from the mutable layer while still surfacing the registry as an output.
 * The mutator tag is technically also exposed; only this module imports
 * it, so the visibility leak is harmless in practice.
 */
export const ProviderInstanceRegistryHydrationLive: Layer.Layer<
  ProviderInstanceRegistry,
  never,
  BuiltInDriversEnv | ServerSettingsService | ProviderRegistryRebuildBarrier
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

    const mutableLayer = ProviderInstanceRegistryMutableLayer({
      drivers: BUILT_IN_DRIVERS,
      configMap: initialConfigMap,
    });

    return SettingsWatcherLive(initialSettings).pipe(Layer.provideMerge(mutableLayer));
  }),
) as Layer.Layer<
  ProviderInstanceRegistry,
  never,
  BuiltInDriversEnv | ServerSettingsService | ProviderRegistryRebuildBarrier
>;
