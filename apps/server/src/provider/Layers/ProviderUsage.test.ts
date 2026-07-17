import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  projectClaudeUsageHeaders,
  projectClaudeUsageResponse,
  projectCodexUsage,
} from "../providerUsageProjection.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import { ProviderRegistryRebuildBarrierLive } from "./ProviderRegistryRebuildBarrier.ts";
import { makeProviderUsage, projectProviderUsageEvent } from "./ProviderUsage.ts";

const threadId = ThreadId.make("thread-usage");

function usageEvent(
  provider: "claudeAgent" | "codex",
  providerInstanceId: string,
  rateLimits: unknown,
): ProviderRuntimeEvent {
  return {
    eventId: EventId.make(`event-${providerInstanceId}`),
    provider: ProviderDriverKind.make(provider),
    providerInstanceId: ProviderInstanceId.make(providerInstanceId),
    threadId,
    createdAt: "2026-07-14T10:00:00.000Z",
    type: "account.rate-limits.updated",
    payload: { rateLimits },
  };
}

function turnEvent(type: "turn.started" | "turn.completed"): ProviderRuntimeEvent {
  const base = {
    eventId: EventId.make(`event-${type}`),
    provider: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: ProviderInstanceId.make("claude-work"),
    threadId,
    createdAt: "2026-07-14T10:00:00.000Z",
  };
  return type === "turn.started"
    ? { ...base, type, payload: {} }
    : { ...base, type, payload: { state: "completed" } };
}

function makeUsageTestLayer(
  runtimeEvents: PubSub.PubSub<ProviderRuntimeEvent>,
  readUsage: () => Effect.Effect<ReturnType<typeof projectClaudeUsageResponse>>,
) {
  const providerInstanceId = ProviderInstanceId.make("claude-work");
  return Layer.mergeAll(
    ProviderRegistryRebuildBarrierLive,
    Layer.mock(ProviderService.ProviderService)({
      streamEvents: Stream.fromPubSub(runtimeEvents),
    }),
    Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistry)({
      getByInstance: () =>
        Effect.succeed({
          readUsage: () =>
            readUsage().pipe(
              Effect.flatMap((usage) =>
                usage === null
                  ? Effect.die("Expected projected Claude usage")
                  : Effect.succeed(usage),
              ),
            ),
        } as never),
      listInstances: () => Effect.succeed([providerInstanceId]),
    }),
  );
}

describe("projectProviderUsageEvent", () => {
  it("merges Claude rolling windows for one provider instance", () => {
    const first = projectProviderUsageEvent(
      usageEvent("claudeAgent", "claude-work", {
        type: "rate_limit_event",
        rate_limit_info: {
          status: "allowed_warning",
          rateLimitType: "five_hour",
          utilization: 0.42,
          resetsAt: 1_752_490_800,
          overageStatus: "allowed",
          isUsingOverage: false,
        },
      }),
    );
    expect(first).not.toBeNull();

    const merged = projectProviderUsageEvent(
      usageEvent("claudeAgent", "claude-work", {
        type: "rate_limit_event",
        rate_limit_info: {
          status: "allowed",
          rateLimitType: "seven_day",
          utilization: 0.67,
          resetsAt: 1_753_004_400,
        },
      }),
      first ?? undefined,
    );

    expect(merged?.providerInstanceId).toBe(ProviderInstanceId.make("claude-work"));
    expect(merged?.windows).toEqual([
      expect.objectContaining({ id: "five_hour", label: "5 hours", usedPercent: 42 }),
      expect.objectContaining({ id: "seven_day", label: "Weekly", usedPercent: 67 }),
    ]);
    expect(merged?.overageStatus).toBe("allowed");
  });

  it("projects Codex primary and weekly windows from the nested snapshot", () => {
    const usage = projectProviderUsageEvent(
      usageEvent("codex", "codex-personal", {
        rateLimits: {
          primary: { usedPercent: 31, windowDurationMins: 300, resetsAt: 1_752_490_800 },
          secondary: {
            usedPercent: 74,
            windowDurationMins: 10_080,
            resetsAt: 1_753_004_400,
          },
          rateLimitReachedType: null,
        },
      }),
    );

    expect(usage?.providerInstanceId).toBe(ProviderInstanceId.make("codex-personal"));
    expect(usage?.status).toBe("allowed");
    expect(usage?.windows).toEqual([
      expect.objectContaining({ id: "primary", label: "5 hours", usedPercent: 31 }),
      expect.objectContaining({ id: "secondary", label: "Weekly", usedPercent: 74 }),
    ]);
  });
});

describe("forced usage projection", () => {
  it("projects Claude limit response headers", () => {
    const usage = projectClaudeUsageHeaders({
      providerInstanceId: ProviderInstanceId.make("claude-work"),
      driver: ProviderDriverKind.make("claudeAgent"),
      observedAt: "2026-07-14T10:00:00.000Z",
      headers: {
        "anthropic-ratelimit-unified-5h-utilization": "0.42",
        "anthropic-ratelimit-unified-5h-reset": "2026-07-14T12:00:00Z",
        "anthropic-ratelimit-unified-7d-utilization": "0.67",
        "anthropic-ratelimit-unified-7d-reset": "2026-07-20T12:00:00Z",
        "anthropic-ratelimit-unified-7d_oi-utilization": "0.23",
        "anthropic-ratelimit-unified-7d_oi-reset": "1753004400",
        "anthropic-ratelimit-unified-status": "allowed_warning",
      },
    });

    expect(usage).toMatchObject({ source: "refresh", status: "warning" });
    expect(usage?.windows).toEqual([
      expect.objectContaining({ id: "five_hour", usedPercent: 42 }),
      expect.objectContaining({ id: "seven_day", usedPercent: 67 }),
      expect.objectContaining({
        id: "seven_day_overage_included",
        label: "Fable 5",
        usedPercent: 23,
      }),
    ]);
  });

  it("projects a direct Codex rate limit response", () => {
    const usage = projectCodexUsage({
      providerInstanceId: ProviderInstanceId.make("codex-work"),
      driver: ProviderDriverKind.make("codex"),
      observedAt: "2026-07-14T10:00:00.000Z",
      source: "refresh",
      rateLimits: {
        primary: { usedPercent: 93, windowDurationMins: 300, resetsAt: 1_752_490_800 },
        secondary: { usedPercent: 12, windowDurationMins: 10_080 },
      },
    });

    expect(usage).toMatchObject({ source: "refresh", status: "warning" });
    expect(usage?.windows[0]).toMatchObject({ label: "5 hours", usedPercent: 93 });
  });

  it("projects Claude model-scoped limits from the usage endpoint", () => {
    const usage = projectClaudeUsageResponse({
      providerInstanceId: ProviderInstanceId.make("claude-work"),
      driver: ProviderDriverKind.make("claudeAgent"),
      observedAt: "2026-07-14T10:00:00.000Z",
      response: {
        five_hour: { utilization: 67, resets_at: "2026-07-14T12:00:00Z" },
        seven_day: { utilization: 40, resets_at: "2026-07-20T12:00:00Z" },
        limits: [
          {
            kind: "weekly_scoped",
            scope: { model: { display_name: "Fable 5" } },
            percent: 12,
            resets_at: "2026-07-20T12:00:00Z",
          },
        ],
      },
    });

    expect(usage?.windows).toEqual([
      expect.objectContaining({ id: "five_hour", usedPercent: 67 }),
      expect.objectContaining({ id: "seven_day", usedPercent: 40 }),
      expect.objectContaining({
        id: "seven_day_overage_included",
        label: "Fable 5",
        usedPercent: 12,
      }),
    ]);
  });
});

describe("automatic usage refresh", () => {
  it.effect("keeps a registry rebuild behind an in-flight usage read", () =>
    Effect.gen(function* () {
      const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      const rebuildStarted = yield* Deferred.make<void>();
      const usage = projectClaudeUsageResponse({
        providerInstanceId: ProviderInstanceId.make("claude-work"),
        driver: ProviderDriverKind.make("claudeAgent"),
        observedAt: "2026-07-14T10:01:00.000Z",
        response: { five_hour: { utilization: 50 } },
      });
      const testLayer = makeUsageTestLayer(runtimeEvents, () =>
        Deferred.succeed(readStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseRead)),
          Effect.as(usage),
        ),
      );

      yield* Effect.gen(function* () {
        const providerUsage = yield* makeProviderUsage();
        const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;
        const refresh = yield* providerUsage.refresh().pipe(Effect.forkScoped);
        yield* Deferred.await(readStarted);
        const rebuild = yield* rebuildBarrier
          .withRebuild(Deferred.succeed(rebuildStarted, undefined))
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(Option.isNone(yield* Deferred.poll(rebuildStarted))).toBe(true);

        yield* Deferred.succeed(releaseRead, undefined);
        yield* Fiber.join(refresh);
        yield* Fiber.join(rebuild);
        expect(Option.isSome(yield* Deferred.poll(rebuildStarted))).toBe(true);
      }).pipe(Effect.provide(testLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes the provider-scoped Claude usage when a turn completes", () =>
    Effect.gen(function* () {
      const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const refreshed = yield* Deferred.make<void>();
      const usage = projectClaudeUsageResponse({
        providerInstanceId: ProviderInstanceId.make("claude-work"),
        driver: ProviderDriverKind.make("claudeAgent"),
        observedAt: "2026-07-14T10:01:00.000Z",
        response: {
          five_hour: { utilization: 52 },
          seven_day: { utilization: 31 },
        },
      });
      const testLayer = makeUsageTestLayer(runtimeEvents, () =>
        Deferred.succeed(refreshed, undefined).pipe(Effect.as(usage)),
      );

      yield* Effect.gen(function* () {
        const providerUsage = yield* makeProviderUsage();
        yield* Effect.yieldNow;
        yield* PubSub.publish(runtimeEvents, turnEvent("turn.completed"));
        yield* Deferred.await(refreshed);
        yield* Effect.yieldNow;

        expect(yield* providerUsage.getSnapshot).toEqual([
          expect.objectContaining({
            providerInstanceId: ProviderInstanceId.make("claude-work"),
            windows: expect.arrayContaining([
              expect.objectContaining({ id: "five_hour", usedPercent: 52 }),
            ]),
          }),
        ]);
      }).pipe(Effect.provide(testLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes long-running Claude turns while they are active", () =>
    Effect.gen(function* () {
      const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const refreshed = yield* Deferred.make<void>();
      const usage = projectClaudeUsageResponse({
        providerInstanceId: ProviderInstanceId.make("claude-work"),
        driver: ProviderDriverKind.make("claudeAgent"),
        observedAt: "2026-07-14T10:01:00.000Z",
        response: { five_hour: { utilization: 54 } },
      });
      const testLayer = makeUsageTestLayer(runtimeEvents, () =>
        Deferred.succeed(refreshed, undefined).pipe(Effect.as(usage)),
      );

      yield* Effect.gen(function* () {
        yield* makeProviderUsage({ activeUsageRefreshInterval: "10 millis" });
        yield* Effect.yieldNow;
        yield* PubSub.publish(runtimeEvents, turnEvent("turn.started"));
        yield* Effect.yieldNow;
        yield* TestClock.adjust("10 millis");
        yield* Deferred.await(refreshed);
      }).pipe(Effect.provide(testLayer));
    }).pipe(Effect.scoped),
  );
});
