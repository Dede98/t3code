import { ProviderInstanceId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { ProviderUsage } from "../../../provider/Services/ProviderUsage.ts";
import { AgentControlInitialPlanningWakeup } from "../../initialPlanning/Services/AgentControlInitialPlanningWakeup.ts";
import { AgentControlImplementationTurnWakeup } from "../../implementationTurn/Services/AgentControlImplementationTurnWakeup.ts";
import { AgentControlVerificationTurnWakeup } from "../../verificationTurn/Services/AgentControlVerificationTurnWakeup.ts";
import {
  providerAdmissionUsageEvidence,
  type ProviderResourceAdmissionLimits,
  type ProviderResourceAdmissionRequest,
} from "../model.ts";
import { ProviderAdmissionStore } from "../Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionRuntime } from "../Services/ProviderAdmissionRuntime.ts";
import { ProviderAdmissionRuntimeLive } from "./ProviderAdmissionRuntime.ts";
import { ProviderAdmissionStoreLive } from "./ProviderAdmissionStore.ts";

const instanceId = ProviderInstanceId.make("codex-work");
const limits: ProviderResourceAdmissionLimits = {
  maxConcurrent: 2,
  interactiveReserve: 1,
  backgroundAgingMs: 30_000,
  maxInteractiveBurst: 3,
};
const usage = providerAdmissionUsageEvidence({
  providerInstanceId: instanceId,
  status: "allowed",
  observedAt: "2026-09-16T10:00:00.000Z",
  source: "refresh",
  nextRelevantAt: null,
});
const request = (
  id: string,
  workloadClass: "interactive" | "background",
  requestedAt = "2026-09-16T10:00:00.000Z",
): ProviderResourceAdmissionRequest => ({
  idempotencyKey: `turn:${id}`,
  providerInstanceId: instanceId,
  threadId: `thread-${id}`,
  accountScope: "codex-account:credential-a",
  workloadClass,
  source: "manual",
  requestedAt,
});

const admit = (
  store: ProviderAdmissionStore["Service"],
  value: ProviderResourceAdmissionRequest,
  ownerId: string,
  now = "2026-09-16T10:00:00.000Z",
) =>
  store.requestResource!({
    request: value,
    usage,
    limits,
    ownerId,
    now,
    leaseExpiresAt: "2026-09-16T10:02:00.000Z",
  });

const testLayer = ProviderAdmissionStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory));

const runtimeTestLayer = Layer.unwrap(
  Effect.gen(function* () {
    const usageEvents = yield* PubSub.unbounded<never>();
    const usageLayer = Layer.succeed(ProviderUsage, {
      stream: Stream.empty,
      inspectForAdmission: () =>
        Effect.succeed({ _tag: "Unsupported" as const, observedAt: "1970-01-01T00:00:00.000Z" }),
      getSnapshot: Effect.succeed([]),
      refresh: () =>
        Effect.succeed({ refreshedAt: "1970-01-01T00:00:00.000Z", usage: [], failures: [] }),
      subscribeEvents: PubSub.subscribe(usageEvents),
    });
    const wakeups = Layer.mergeAll(
      Layer.succeed(AgentControlInitialPlanningWakeup, {
        wake: () => Effect.void,
        stream: Stream.never,
      }),
      Layer.succeed(AgentControlImplementationTurnWakeup, {
        wake: () => Effect.void,
        stream: Stream.never,
      }),
      Layer.succeed(AgentControlVerificationTurnWakeup, {
        wake: () => Effect.void,
        stream: Stream.never,
        subscribe: Effect.succeed(Stream.never),
      }),
    );
    const store = Layer.fresh(ProviderAdmissionStoreLive).pipe(
      Layer.provideMerge(SqlitePersistenceMemory),
    );
    return Layer.fresh(ProviderAdmissionRuntimeLive).pipe(
      Layer.provideMerge(store),
      Layer.provideMerge(usageLayer),
      Layer.provide(wakeups),
    );
  }),
);

it.effect("atomically reserves interactive capacity and keeps duplicate requests idempotent", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const background = yield* admit(store, request("background", "background"), "owner-a");
    assert.equal(background.decision._tag, "Admitted");
    const queued = yield* admit(store, request("queued", "background"), "owner-a");
    assert.equal(queued.decision._tag, "Waiting");
    if (queued.decision._tag === "Waiting")
      assert.equal(queued.decision.reason, "interactive-priority");
    const interactive = yield* admit(store, request("interactive", "interactive"), "owner-a");
    assert.equal(interactive.decision._tag, "Admitted");
    const replay = yield* admit(store, request("interactive", "interactive"), "owner-a");
    assert.deepStrictEqual(replay.decision, interactive.decision);
    assert.equal(
      (yield* store.listResourceActive!).filter((row) => row.status !== "waiting").length,
      2,
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("ages background work through repeated interactive grants without preemption", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const first = yield* admit(store, request("first-background", "background"), "owner-a");
    const waitingRequest = request("aged-background", "background", "2026-09-16T09:59:00.000Z");
    assert.equal((yield* admit(store, waitingRequest, "owner-a")).decision._tag, "Admitted");
    assert.equal(first.decision._tag, "Admitted");
    const manual = yield* admit(store, request("manual", "interactive"), "owner-a");
    assert.equal(manual.decision._tag, "Waiting");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("cancellation is terminal and an entered lease is never reclaimed by time", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const one = yield* admit(store, request("one", "interactive"), "owner-a");
    const two = yield* admit(store, request("two", "interactive"), "owner-a");
    const waiting = request("waiting", "interactive");
    assert.equal((yield* admit(store, waiting, "owner-a")).decision._tag, "Waiting");
    yield* store.cancelResource!({ request: waiting, cancelledAt: "2026-09-16T10:00:10.000Z" });
    assert.equal((yield* admit(store, waiting, "owner-a")).decision._tag, "Cancelled");
    assert.equal(one.decision._tag, "Admitted");
    assert.equal(two.decision._tag, "Admitted");
    if (one.decision._tag !== "Admitted") return;
    const onePermit = one.decision.permit;
    yield* store.enterResource!({
      permit: onePermit,
      enteredAt: "2026-09-16T10:00:01.000Z",
      providerTurnId: "provider-turn-one",
    });
    yield* admit(
      store,
      request("after-expiry", "interactive"),
      "owner-b",
      "2026-09-16T10:03:00.000Z",
    );
    const active = yield* store.listResourceActive!;
    const entered = active.find((row) => row.requestId === onePermit.requestId);
    assert.equal(entered?.status, "entered");
    assert.equal(entered?.providerTurnId, "provider-turn-one");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("defers a provisional provider grant without making its identity terminal", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const initial = yield* admit(store, request("deferred", "interactive"), "owner-a");
    assert.equal(initial.decision._tag, "Admitted");
    if (initial.decision._tag !== "Admitted") return;
    yield* store.deferResource!({
      permit: initial.decision.permit,
      deferredAt: "2026-09-16T10:00:01.000Z",
    });
    const resumed = yield* admit(
      store,
      request("deferred", "interactive"),
      "owner-a",
      "2026-09-16T10:00:02.000Z",
    );
    assert.equal(resumed.decision._tag, "Admitted");
    if (resumed.decision._tag !== "Admitted") return;
    assert.isAbove(resumed.decision.permit.fenceToken, initial.decision.permit.fenceToken);
    assert.equal(
      (yield* store.releaseResource!({
        permit: initial.decision.permit,
        releasedAt: "2026-09-16T10:00:03.000Z",
      }).pipe(Effect.exit))._tag,
      "Failure",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("restart reconciliation adopts only observed active work and fences the old owner", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const admitted = yield* admit(store, request("recover", "interactive"), "old-owner");
    assert.equal(admitted.decision._tag, "Admitted");
    if (admitted.decision._tag !== "Admitted") return;
    yield* store.enterResource!({
      permit: admitted.decision.permit,
      enteredAt: "2026-09-16T10:00:01.000Z",
      providerTurnId: "provider-turn-recover",
    });
    assert.isNull(
      yield* store.reconcileResource!({
        requestId: admitted.decision.permit.requestId,
        observedActivity: "unknown",
        ownerId: "new-owner",
        observedAt: "2026-09-16T10:03:00.000Z",
        leaseExpiresAt: "2026-09-16T10:05:00.000Z",
      }),
    );
    const adopted = yield* store.reconcileResource!({
      requestId: admitted.decision.permit.requestId,
      observedActivity: "active",
      ownerId: "new-owner",
      observedAt: "2026-09-16T10:03:01.000Z",
      leaseExpiresAt: "2026-09-16T10:05:01.000Z",
    });
    assert.isNotNull(adopted);
    const staleRelease = yield* store.releaseResource!({
      permit: admitted.decision.permit,
      releasedAt: "2026-09-16T10:03:02.000Z",
    }).pipe(Effect.exit);
    assert.equal(staleRelease._tag, "Failure");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("applies changed scope limits to an existing account scope", () =>
  Effect.gen(function* () {
    const store = yield* ProviderAdmissionStore;
    const oneSlot = { ...limits, maxConcurrent: 1, interactiveReserve: 0 };
    const first = request("configured-first", "interactive");
    const second = request("configured-second", "interactive");
    assert.equal(
      (yield* store.requestResource!({
        request: first,
        usage,
        limits: oneSlot,
        ownerId: "owner-a",
        now: "2026-09-16T10:00:00.000Z",
        leaseExpiresAt: "2026-09-16T10:02:00.000Z",
      })).decision._tag,
      "Admitted",
    );
    assert.equal(
      (yield* store.requestResource!({
        request: second,
        usage,
        limits: oneSlot,
        ownerId: "owner-a",
        now: "2026-09-16T10:00:00.000Z",
        leaseExpiresAt: "2026-09-16T10:02:00.000Z",
      })).decision._tag,
      "Waiting",
    );
    yield* store.configureResourceScope!({
      accountScope: second.accountScope,
      limits: { ...oneSlot, maxConcurrent: 2 },
      updatedAt: "2026-09-16T10:00:01.000Z",
    });
    yield* store.advanceResourceScope!({
      accountScope: second.accountScope,
      ownerId: "owner-a",
      now: "2026-09-16T10:00:01.000Z",
      leaseExpiresAt: "2026-09-16T10:02:01.000Z",
    });
    assert.equal(
      (yield* store.listResourceActive!).filter((row) => row.status === "admitted").length,
      2,
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("wakes aged background work without an external capacity signal", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* ProviderAdmissionRuntime;
      const aged = request("clock-aging", "background", "1970-01-01T00:00:00.000Z");
      const agingLimits = {
        ...limits,
        maxConcurrent: 1,
        interactiveReserve: 1,
        backgroundAgingMs: 1_000,
      };
      assert.equal((yield* runtime.requestResource!(aged, agingLimits))._tag, "Waiting");
      const acquired = yield* runtime.acquireResource!(aged, agingLimits).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* TestClock.adjust("1 second");
      assert.equal((yield* Fiber.join(acquired)).requestId.startsWith("provider-resource-"), true);
    }),
  ).pipe(Effect.provide(runtimeTestLayer), Effect.provide(NodeServices.layer)),
);

it.effect("wakes a waiter when an admitted pre-entry lease expires", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* ProviderAdmissionRuntime;
      const single = { ...limits, maxConcurrent: 1, interactiveReserve: 0 };
      const first = request("clock-lease-first", "interactive", "1970-01-01T00:00:00.000Z");
      const second = request("clock-lease-second", "interactive", "1970-01-01T00:00:00.000Z");
      assert.equal((yield* runtime.requestResource!(first, single))._tag, "Admitted");
      assert.equal((yield* runtime.requestResource!(second, single))._tag, "Waiting");
      const acquired = yield* runtime.acquireResource!(second, single).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* TestClock.adjust("121 seconds");
      assert.equal((yield* Fiber.join(acquired)).requestId.startsWith("provider-resource-"), true);
    }),
  ).pipe(Effect.provide(runtimeTestLayer), Effect.provide(NodeServices.layer)),
);

it.effect("rereads changed provider limits while a request is waiting", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* ProviderAdmissionRuntime;
      const single = { ...limits, maxConcurrent: 1, interactiveReserve: 0 };
      let current = single;
      const first = request("dynamic-runtime-first", "interactive");
      const second = request("dynamic-runtime-second", "interactive");
      assert.equal((yield* runtime.requestResource!(first, single))._tag, "Admitted");
      const acquired = yield* runtime.acquireResource!(
        second,
        single,
        Effect.sync(() => current),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      current = { ...single, maxConcurrent: 2 };
      yield* runtime.resourceSettingsChanged!;
      assert.equal((yield* Fiber.join(acquired)).requestId.startsWith("provider-resource-"), true);
    }),
  ).pipe(Effect.provide(runtimeTestLayer), Effect.provide(NodeServices.layer)),
);
