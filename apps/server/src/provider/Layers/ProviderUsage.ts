import * as PubSub from "effect/PubSub";
import type {
  ProviderUsageSnapshot,
  ProviderUsageStreamEvent,
  ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { ProviderRegistry } from "../Services/ProviderRegistry.ts";
import { ProviderUsage } from "../Services/ProviderUsage.ts";

/** Wire compatibility for older fork clients; the registry owns all usage state. */
export function legacyProviderUsage(providers: readonly ServerProvider[]): ProviderUsageSnapshot[] {
  return providers.flatMap((provider) => {
    const limits = provider.usageLimits;
    if (!provider.enabled || !limits || limits.unavailable || limits.windows.length === 0)
      return [];
    const highest = Math.max(...limits.windows.map((window) => window.usedPercent));
    return [
      {
        providerInstanceId: provider.instanceId,
        driver: provider.driver,
        observedAt: limits.checkedAt,
        source: "refresh" as const,
        // Quota exhaustion does not prove a turn will be rejected (for example, paid overage).
        status: highest >= 90 ? ("warning" as const) : ("allowed" as const),
        windows: limits.windows.map((window) => ({
          id: window.id,
          label: window.label,
          usedPercent: window.usedPercent,
          resetsAt: window.resetsAt ?? null,
          ...(window.windowDurationMins && window.windowDurationMins > 0
            ? { durationMinutes: window.windowDurationMins }
            : {}),
        })),
      },
    ];
  });
}

export const makeProviderUsage = Effect.fn("makeProviderUsage")(function* () {
  const registry = yield* ProviderRegistry;
  const events = yield* PubSub.unbounded<ProviderUsageStreamEvent>();
  yield* registry.streamChanges.pipe(
    Stream.runForEach((providers) =>
      PubSub.publish(events, {
        version: 1,
        type: "snapshot",
        usage: legacyProviderUsage(providers),
      }),
    ),
    Effect.forkScoped,
  );
  return ProviderUsage.of({
    getSnapshot: registry.getProviders.pipe(Effect.map(legacyProviderUsage)),
    subscribeEvents: PubSub.subscribe(events),
    inspectForAdmission: (providerInstanceId) =>
      Effect.gen(function* () {
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        const providers = yield* registry.refreshInstance(providerInstanceId);
        const provider = providers.find((provider) => provider.instanceId === providerInstanceId);
        if (provider === undefined || !provider.enabled)
          return { _tag: "SupportedUnusable" as const, observedAt };
        if (provider.usageLimits?.unavailable?.reason === "unsupported")
          return { _tag: "Unsupported" as const, observedAt };
        const snapshot = legacyProviderUsage([provider])[0];
        if (snapshot !== undefined) return { _tag: "Observed" as const, snapshot };
        return { _tag: "SupportedUnusable" as const, observedAt };
      }),
    stream: Stream.unwrap(
      Effect.gen(function* () {
        // Subscribe before reading. A queued notification rereads current state rather
        // than replaying a snapshot that may already predate the initial read.
        const updates = yield* Queue.sliding<void>(1);
        yield* Stream.runForEach(registry.streamChanges, () =>
          Queue.offer(updates, undefined),
        ).pipe(Effect.forkScoped({ startImmediately: true }));
        return Stream.concat(Stream.make(undefined), Stream.fromQueue(updates)).pipe(
          Stream.mapEffect(() => registry.getProviders),
          Stream.map(legacyProviderUsage),
          Stream.changesWith((a, b) => Equal.equals(a, b)),
          Stream.map((usage) => ({ version: 1 as const, type: "snapshot" as const, usage })),
        );
      }),
    ),
    refresh: (requestedIds) =>
      Effect.gen(function* () {
        if (requestedIds === undefined) {
          yield* registry.refresh();
        } else {
          yield* Effect.forEach([...new Set(requestedIds)], (id) => registry.refreshInstance(id), {
            concurrency: 3,
          });
        }
        const providers = yield* registry.getProviders;
        const selected =
          requestedIds === undefined
            ? providers
            : providers.filter((provider) => requestedIds.includes(provider.instanceId));
        const usage = legacyProviderUsage(selected);
        const ids = requestedIds ?? selected.map((provider) => provider.instanceId);
        const failures = [...new Set(ids)].flatMap((providerInstanceId) => {
          if (usage.some((snapshot) => snapshot.providerInstanceId === providerInstanceId))
            return [];
          const provider = selected.find(
            (candidate) => candidate.instanceId === providerInstanceId,
          );
          return [
            {
              providerInstanceId,
              message:
                provider?.usageLimits?.unavailable?.message ?? "Usage limits are unavailable.",
            },
          ];
        });
        return { refreshedAt: DateTime.formatIso(yield* DateTime.now), usage, failures };
      }),
  });
});

export const ProviderUsageLive = Layer.effect(ProviderUsage, makeProviderUsage());
