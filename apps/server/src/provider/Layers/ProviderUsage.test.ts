import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ProviderUsageStreamEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { ProviderRegistry } from "../Services/ProviderRegistry.ts";
import { legacyProviderUsage, makeProviderUsage } from "./ProviderUsage.ts";

function provider(id: string, usedPercent: number): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-09-07T10:00:00Z",
    models: [],
    slashCommands: [],
    skills: [],
    usageLimits: {
      checkedAt: "2026-09-07T10:00:00Z",
      windows: [
        { id: "primary", kind: "session", label: "Session", usedPercent, windowDurationMins: 300 },
      ],
    },
  };
}

describe("legacy usage compatibility", () => {
  it("preserves per-instance wire values without interpreting quota as a rejection", () => {
    const usage = legacyProviderUsage([provider("personal", 20), provider("work", 100)]);
    expect(
      usage.map((snapshot) => [
        snapshot.providerInstanceId,
        snapshot.status,
        snapshot.windows[0]?.usedPercent,
      ]),
    ).toEqual([
      ["personal", "allowed", 20],
      ["work", "warning", 100],
    ]);
    expect(usage[0]?.windows[0]).toMatchObject({ durationMinutes: 300, resetsAt: null });
  });
  it("drops unsupported and removed quota instead of retaining stale snapshots", () => {
    const original = provider("work", 100);
    expect(legacyProviderUsage([{ ...original, enabled: false }])).toEqual([]);
    expect(
      legacyProviderUsage([
        {
          ...original,
          usageLimits: {
            checkedAt: original.checkedAt,
            windows: [],
            unavailable: { reason: "unsupported" },
          },
        },
      ]),
    ).toEqual([]);
    expect(legacyProviderUsage([])).toEqual([]);
  });
  it.effect(
    "refreshes the registry once per requested instance and returns its current values",
    () =>
      Effect.gen(function* () {
        const state = yield* Ref.make([provider("personal", 20), provider("work", 100)]);
        const calls = yield* Ref.make<string[]>([]);
        const usage = yield* makeProviderUsage().pipe(
          Effect.provide(
            Layer.mock(ProviderRegistry)({
              getProviders: Ref.get(state),
              streamChanges: Stream.empty,
              refreshInstance: (id) =>
                Effect.gen(function* () {
                  yield* Ref.update(calls, (previous) => [...previous, id]);
                  yield* Ref.update(state, (providers) =>
                    providers.map((p) => (p.instanceId === id ? provider(id, 0) : p)),
                  );
                  return yield* Ref.get(state);
                }),
            }),
          ),
        );
        const id = ProviderInstanceId.make("work");
        const result = yield* usage.refresh([id, id, ProviderInstanceId.make("missing")]);
        expect(yield* Ref.get(calls)).toEqual(["work", "missing"]);
        expect(result.usage).toEqual(legacyProviderUsage([provider("work", 0)]));
        expect(result.failures.map((failure) => failure.providerInstanceId)).toEqual(["missing"]);
        expect((yield* Ref.get(state))[0]?.usageLimits?.windows[0]?.usedPercent).toBe(20);
      }),
  );
  it.effect("streams initial state, replenishment and account removal from the same registry", () =>
    Effect.gen(function* () {
      const state = yield* Ref.make<readonly ServerProvider[]>([provider("work", 100)]);
      const changes = yield* PubSub.unbounded<readonly ServerProvider[]>();
      const received = yield* Queue.unbounded<ProviderUsageStreamEvent>();
      const usage = yield* makeProviderUsage().pipe(
        Effect.provide(
          Layer.mock(ProviderRegistry)({
            getProviders: Ref.get(state),
            streamChanges: Stream.fromPubSub(changes),
            refresh: () =>
              Effect.gen(function* () {
                const next = [provider("work", 0)];
                yield* Ref.set(state, next);
                yield* PubSub.publish(changes, next);
                return next;
              }),
          }),
        ),
      );
      yield* usage.stream.pipe(
        Stream.runForEach((event) => Queue.offer(received, event)),
        Effect.forkScoped,
      );
      expect(yield* Queue.take(received)).toMatchObject({
        type: "snapshot",
        usage: [{ windows: [{ usedPercent: 100 }] }],
      });
      yield* usage.refresh();
      expect(yield* Queue.take(received)).toMatchObject({
        type: "snapshot",
        usage: [{ windows: [{ usedPercent: 0 }] }],
      });
      yield* Ref.set(state, []);
      yield* PubSub.publish(changes, []);
      expect(yield* Queue.take(received)).toEqual({ version: 1, type: "snapshot", usage: [] });
    }),
  );
  it.effect("subscribes before the initial read so a concurrent update is not lost", () =>
    Effect.gen(function* () {
      const state = yield* Ref.make([provider("work", 100)]);
      const changes = yield* PubSub.unbounded<readonly ServerProvider[]>();
      const reading = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let firstRead = true;
      const usage = yield* makeProviderUsage().pipe(
        Effect.provide(
          Layer.mock(ProviderRegistry)({
            getProviders: Effect.gen(function* () {
              const snapshot = yield* Ref.get(state);
              if (firstRead) {
                firstRead = false;
                yield* Deferred.succeed(reading, undefined);
                yield* Deferred.await(release);
              }
              return snapshot;
            }),
            streamChanges: Stream.fromPubSub(changes),
          }),
        ),
      );
      const collected = yield* usage.stream.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Deferred.await(reading);
      const next = [provider("work", 0)];
      yield* Ref.set(state, next);
      yield* PubSub.publish(changes, next);
      yield* Deferred.succeed(release, undefined);
      const events = yield* Fiber.join(collected);
      expect(events).toMatchObject([
        { usage: [{ windows: [{ usedPercent: 100 }] }] },
        { usage: [{ windows: [{ usedPercent: 0 }] }] },
      ]);
    }),
  );
});
