import type {
  ProviderInstanceId,
  ProviderUsageRefreshFailure,
  ProviderUsageSnapshot,
  ProviderUsageStreamEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { projectProviderUsageEvent } from "../providerUsageProjection.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderUsage from "../Services/ProviderUsage.ts";

export { projectProviderUsageEvent } from "../providerUsageProjection.ts";

const CLAUDE_ACTIVE_USAGE_REFRESH_INTERVAL = Duration.seconds(60);

interface MakeProviderUsageOptions {
  readonly activeUsageRefreshInterval?: Duration.Input;
}

export const makeProviderUsage = Effect.fn("makeProviderUsage")(function* (
  options: MakeProviderUsageOptions = {},
) {
  const providerService = yield* ProviderService.ProviderService;
  const adapterRegistry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry;
  const snapshots = yield* Ref.make(new Map<ProviderInstanceId, ProviderUsageSnapshot>());
  const events = yield* PubSub.unbounded<ProviderUsageStreamEvent>();
  const activeClaudeTurns = yield* Ref.make(new Map<ProviderInstanceId, ReadonlySet<string>>());
  const activeRefreshLoops = yield* Ref.make(new Set<ProviderInstanceId>());
  const activeUsageRefreshInterval =
    options.activeUsageRefreshInterval ?? CLAUDE_ACTIVE_USAGE_REFRESH_INTERVAL;

  const publish = (usage: ProviderUsageSnapshot) =>
    Effect.all([
      Ref.update(snapshots, (state) => new Map(state).set(usage.providerInstanceId, usage)),
      PubSub.publish(events, { version: 1 as const, type: "updated" as const, usage }),
    ]).pipe(Effect.asVoid);

  const readProviderUsage = Effect.fn("ProviderUsage.readProviderUsage")(function* (
    providerInstanceId: ProviderInstanceId,
  ) {
    const adapter = yield* adapterRegistry.getByInstance(providerInstanceId);
    if (!adapter.readUsage) return Option.none<ProviderUsageSnapshot>();
    const usage = yield* adapter.readUsage();
    yield* publish(usage);
    return Option.some(usage);
  });

  const refreshAutomatically = Effect.fn("ProviderUsage.refreshAutomatically")(
    function* (providerInstanceId: ProviderInstanceId) {
      yield* readProviderUsage(providerInstanceId);
    },
    Effect.catch((cause) =>
      Effect.logWarning("Automatic provider usage refresh failed.").pipe(
        Effect.annotateLogs({
          cause,
        }),
      ),
    ),
  );

  const hasActiveClaudeTurn = (providerInstanceId: ProviderInstanceId) =>
    Ref.get(activeClaudeTurns).pipe(
      Effect.map((turnsByInstance) => (turnsByInstance.get(providerInstanceId)?.size ?? 0) > 0),
    );

  const runActiveRefreshLoop = Effect.fn("ProviderUsage.runActiveRefreshLoop")(function* (
    providerInstanceId: ProviderInstanceId,
  ) {
    while (true) {
      yield* Effect.sleep(activeUsageRefreshInterval);
      if (!(yield* hasActiveClaudeTurn(providerInstanceId))) return;
      yield* refreshAutomatically(providerInstanceId);
    }
  });

  const startActiveRefreshLoop = Effect.fn("ProviderUsage.startActiveRefreshLoop")(function* (
    providerInstanceId: ProviderInstanceId,
  ) {
    const shouldStart = yield* Ref.modify(activeRefreshLoops, (running) => {
      if (running.has(providerInstanceId)) return [false, running] as const;
      const next = new Set(running);
      next.add(providerInstanceId);
      return [true, next] as const;
    });
    if (!shouldStart) return;

    yield* runActiveRefreshLoop(providerInstanceId).pipe(
      Effect.ensuring(
        Ref.update(activeRefreshLoops, (running) => {
          const next = new Set(running);
          next.delete(providerInstanceId);
          return next;
        }),
      ),
      Effect.forkScoped,
    );
  });

  const trackClaudeTurn = Effect.fn("ProviderUsage.trackClaudeTurn")(function* (
    event: Parameters<typeof projectProviderUsageEvent>[0],
  ) {
    const providerInstanceId = event.providerInstanceId;
    if (event.provider !== "claudeAgent" || providerInstanceId === undefined) return;

    if (event.type === "turn.started") {
      yield* Ref.update(activeClaudeTurns, (turnsByInstance) => {
        const next = new Map(turnsByInstance);
        const activeTurns = new Set(next.get(providerInstanceId) ?? []);
        activeTurns.add(event.threadId);
        next.set(providerInstanceId, activeTurns);
        return next;
      });
      yield* startActiveRefreshLoop(providerInstanceId);
      return;
    }

    if (event.type !== "turn.completed") return;
    yield* Ref.update(activeClaudeTurns, (turnsByInstance) => {
      const next = new Map(turnsByInstance);
      const activeTurns = new Set(next.get(providerInstanceId) ?? []);
      activeTurns.delete(event.threadId);
      if (activeTurns.size === 0) next.delete(providerInstanceId);
      else next.set(providerInstanceId, activeTurns);
      return next;
    });
    yield* refreshAutomatically(providerInstanceId).pipe(Effect.forkScoped);
  });

  yield* providerService.streamEvents.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        if (event.providerInstanceId === undefined) return;
        const current = yield* Ref.get(snapshots);
        const usage = projectProviderUsageEvent(event, current.get(event.providerInstanceId));
        if (usage !== null) yield* publish(usage);
        yield* trackClaudeTurn(event);
      }),
    ),
    Effect.forkScoped,
  );

  return ProviderUsage.ProviderUsage.of({
    getSnapshot: Ref.get(snapshots).pipe(Effect.map((state) => Array.from(state.values()))),
    refresh: (requestedIds) =>
      Effect.gen(function* () {
        const providerInstanceIds = requestedIds ?? (yield* adapterRegistry.listInstances());
        const results = yield* Effect.forEach(
          providerInstanceIds,
          (providerInstanceId) =>
            Effect.gen(function* () {
              const usage = yield* readProviderUsage(providerInstanceId);
              if (Option.isNone(usage)) {
                return {
                  providerInstanceId,
                  message: "Usage refresh is not supported.",
                } satisfies ProviderUsageRefreshFailure;
              }
              return usage.value;
            }).pipe(
              Effect.catch((cause) =>
                Effect.succeed({
                  providerInstanceId,
                  message: cause instanceof Error ? cause.message : "Usage refresh failed.",
                } satisfies ProviderUsageRefreshFailure),
              ),
            ),
          { concurrency: 3 },
        );
        const usage = results.filter(
          (result): result is ProviderUsageSnapshot => "windows" in result,
        );
        const failures = results.filter(
          (result): result is ProviderUsageRefreshFailure => "message" in result,
        );
        return { refreshedAt: DateTime.formatIso(yield* DateTime.now), usage, failures };
      }),
    subscribeEvents: PubSub.subscribe(events),
  });
});

export const ProviderUsageLive = Layer.effect(ProviderUsage.ProviderUsage, makeProviderUsage());
