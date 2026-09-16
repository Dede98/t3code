import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import { makeMemoryHostBudgetLedger } from "./HostBudgetLedger.ts";
import { make, type ResourceAdmissionShape } from "./ResourceAdmission.ts";
import { ResourcePressure } from "./ResourcePressure.ts";
import {
  defaultResourceAdmissionSettings,
  type ResourceAdmissionRequest,
  type ResourceAdmissionSettings,
  type ResourcePressureSample,
} from "./model.ts";

const healthySample = (): ResourcePressureSample => ({
  sampledAtMs: 1,
  telemetry: "available",
  cpuUtilization: 0.1,
  availableMemoryBytes: 8 * 1024 * 1024 * 1024,
  gpu: { status: "unavailable" },
});

const request = (
  requestId: string,
  input: Partial<ResourceAdmissionRequest> = {},
): ResourceAdmissionRequest => ({
  requestId,
  kind: "providerTurn",
  priority: "background",
  accountScope: "account-a",
  ownerId: `owner-${requestId}`,
  ownerFenceToken: 1,
  executionKey: `execution-${requestId}`,
  ...input,
});

const makeHarness = Effect.fn("resourceAdmission.test.makeHarness")(function* (options?: {
  readonly settings?: Partial<ResourceAdmissionSettings>;
  readonly sample?: () => ResourcePressureSample;
  readonly awaitPressureChange?: (afterSampledAtMs: number) => Effect.Effect<void>;
}) {
  const ledger = yield* makeMemoryHostBudgetLedger();
  const pressure = ResourcePressure.of({
    sample: Effect.sync(options?.sample ?? healthySample),
    awaitChange: options?.awaitPressureChange ?? (() => Effect.never),
  });
  const service = yield* make({
    ledger,
    settings: { ...defaultResourceAdmissionSettings, ...options?.settings },
  }).pipe(Effect.provideService(ResourcePressure, pressure));
  return { ledger, pressure, service };
});

const admitted = Effect.fn("resourceAdmission.test.admitted")(function* (
  service: ResourceAdmissionShape,
  input: ResourceAdmissionRequest,
) {
  const result = (yield* service.request(input)).result;
  assert.equal(result._tag, "Admitted");
  if (result._tag !== "Admitted") return yield* Effect.die("expected admission");
  return result;
});

it.effect("atomically limits concurrent manual and automatic provider turns on the host", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 2, interactiveReserve: 0 },
    });
    const results = yield* Effect.all(
      [
        service.request(request("background-1")),
        service.request(request("manual", { priority: "interactive" })),
        service.request(request("background-2")),
      ],
      { concurrency: "unbounded" },
    );
    assert.equal(results.filter((entry) => entry.result._tag === "Admitted").length, 2);
    assert.equal(results.filter((entry) => entry.result._tag === "Waiting").length, 1);
  }),
);

it.effect("does not multiply the host provider budget across account scopes", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    assert.equal((yield* service.request(request("account-a"))).result._tag, "Admitted");
    const other = yield* service.request(request("account-b", { accountScope: "account-b" }));
    assert.equal(other.result._tag, "Waiting");
    if (other.result._tag === "Waiting") assert.equal(other.result.reason, "provider-limit");
  }),
);

it.effect("prefers interactive work but grants background at least every fourth grant", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    let running = yield* admitted(service, request("running"));
    yield* service.request(request("background-waiter"));
    for (let index = 1; index <= 4; index += 1) {
      yield* service.request(
        request(`manual-${index}`, { priority: "interactive", ownerId: `manual-owner-${index}` }),
      );
    }

    for (let index = 1; index <= 3; index += 1) {
      const released = yield* service.release(running.authority);
      assert.equal(released.newlyAdmitted[0]?.requestId, `manual-${index}`);
      const next = (yield* service.request(
        request(`manual-${index}`, { priority: "interactive", ownerId: `manual-owner-${index}` }),
      )).result;
      assert.equal(next._tag, "Admitted");
      if (next._tag !== "Admitted") return yield* Effect.die("expected manual admission");
      running = next;
    }
    const released = yield* service.release(running.authority);
    assert.equal(released.newlyAdmitted[0]?.requestId, "background-waiter");
  }),
);

it.effect("ages background work into the next available grant after thirty seconds", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    const running = yield* admitted(service, request("aging-running"));
    yield* service.request(request("aged-background"));
    yield* service.request(request("fresh-manual", { priority: "interactive" }));
    yield* TestClock.adjust("30 seconds");
    const released = yield* service.release(running.authority);
    assert.equal(released.newlyAdmitted[0]?.requestId, "aged-background");
  }),
);

it.effect("wakes a reserved background acquire exactly once when its aging deadline arrives", () =>
  Effect.gen(function* () {
    const pressureWaitStarted = yield* Deferred.make<void>();
    const background = request("aging-acquire");
    const { service } = yield* makeHarness({
      settings: {
        providerMaxConcurrent: 1,
        interactiveReserve: 1,
        backgroundMaxGrantDelayMs: 30_000,
      },
      awaitPressureChange: () =>
        Deferred.succeed(pressureWaitStarted, undefined).pipe(Effect.andThen(Effect.never)),
    });

    const fiber = yield* service.acquire(background).pipe(Effect.forkChild);
    yield* Deferred.await(pressureWaitStarted);
    assert.equal(
      (yield* service.snapshot).entries.find((entry) => entry.requestId === background.requestId)
        ?.state,
      "waiting",
    );

    yield* TestClock.adjust("30 seconds");
    const admission = yield* Fiber.join(fiber);
    assert.equal(admission._tag, "Admitted");
    if (admission._tag !== "Admitted") return yield* Effect.die("expected aged admission");
    const replay = (yield* service.request(background)).result;
    assert.equal(replay._tag, "Admitted");
    if (replay._tag === "Admitted") assert.deepEqual(replay.authority, admission.authority);
    assert.equal(
      (yield* service.snapshot).entries.filter(
        (entry) => entry.requestId === background.requestId && entry.state === "admitted",
      ).length,
      1,
    );
  }),
);

it.effect("does not spin an aged reserved waiter while CPU pressure still blocks admission", () =>
  Effect.gen(function* () {
    let sample = { ...healthySample(), sampledAtMs: 2, cpuUtilization: 0.99 };
    let sampleReads = 0;
    let pressureWaits = 0;
    const firstPressureWait = yield* Deferred.make<void>();
    const secondPressureWait = yield* Deferred.make<void>();
    const pressureChanged = yield* Deferred.make<void>();
    const background = request("aged-under-cpu-pressure");
    const ledger = yield* makeMemoryHostBudgetLedger();
    const pressure = ResourcePressure.of({
      sample: Effect.sync(() => {
        sampleReads += 1;
        return sample;
      }),
      awaitChange: () =>
        Effect.suspend(() => {
          pressureWaits += 1;
          const started = pressureWaits === 1 ? firstPressureWait : secondPressureWait;
          return Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(pressureChanged)),
          );
        }),
    });
    const service = yield* make({
      ledger,
      settings: {
        ...defaultResourceAdmissionSettings,
        providerMaxConcurrent: 1,
        interactiveReserve: 1,
        backgroundMaxGrantDelayMs: 30_000,
      },
    }).pipe(Effect.provideService(ResourcePressure, pressure));

    const fiber = yield* service.acquire(background).pipe(Effect.forkChild);
    yield* Deferred.await(firstPressureWait);
    yield* TestClock.adjust("30 seconds");
    yield* Deferred.await(secondPressureWait);
    assert.equal(
      (yield* service.snapshot).entries.find((entry) => entry.requestId === background.requestId)
        ?.state,
      "waiting",
    );

    const readsWhileParked = sampleReads;
    const revisionWhileParked = (yield* ledger.read).revision;
    yield* Effect.forEach([1, 2, 3, 4, 5], () => Effect.yieldNow);
    assert.equal(sampleReads, readsWhileParked);
    assert.equal((yield* ledger.read).revision, revisionWhileParked);

    sample = { ...sample, sampledAtMs: 3, cpuUtilization: 0.7 };
    yield* Deferred.succeed(pressureChanged, undefined);
    const admission = yield* Fiber.join(fiber);
    assert.equal(admission._tag, "Admitted");
    if (admission._tag !== "Admitted") return yield* Effect.die("expected CPU admission");
    const replay = (yield* service.request(background)).result;
    if (replay._tag !== "Admitted") return yield* Effect.die("expected replayed admission");
    assert.deepEqual(replay.authority, admission.authority);
  }),
);

it.effect("keeps a single host slot reserved until background aging is due", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 1 },
    });
    const background = request("reserved-background");
    const initial = yield* service.request(background);
    assert.equal(initial.result._tag, "Waiting");
    if (initial.result._tag === "Waiting")
      assert.equal(initial.result.reason, "interactive-priority");
    yield* TestClock.adjust("30 seconds");
    assert.equal((yield* service.refresh)[0]?.requestId, background.requestId);
  }),
);

it.effect("cancels a waiting request durably and never promotes it later", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    const running = yield* admitted(service, request("running"));
    const waitingRequest = request("waiting");
    assert.equal((yield* service.request(waitingRequest)).result._tag, "Waiting");
    assert.isTrue(
      (yield* service.cancelWaiting({
        reservationId: waitingRequest.requestId,
        ownerId: waitingRequest.ownerId,
        ownerFenceToken: waitingRequest.ownerFenceToken,
      })).result,
    );
    assert.deepEqual((yield* service.release(running.authority)).newlyAdmitted, []);
    assert.equal((yield* service.request(waitingRequest)).result._tag, "Rejected");
  }),
);

it.effect("defers a provisional host grant without holding capacity or losing identity", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    const firstRequest = request("defer-first", { priority: "interactive" });
    const first = yield* admitted(service, firstRequest);
    const secondRequest = request("defer-second", { priority: "interactive" });
    assert.equal((yield* service.request(secondRequest)).result._tag, "Waiting");
    assert.equal(
      (yield* service.defer(first.authority)).newlyAdmitted[0]?.requestId,
      "defer-second",
    );
    assert.equal((yield* service.request(firstRequest)).result._tag, "Waiting");
    const second = (yield* service.request(secondRequest)).result;
    if (second._tag !== "Admitted") return yield* Effect.die("expected second admission");
    assert.equal(
      (yield* service.release(second.authority)).newlyAdmitted[0]?.requestId,
      "defer-first",
    );
    const resumed = (yield* service.request(firstRequest)).result;
    if (resumed._tag !== "Admitted") return yield* Effect.die("expected resumed admission");
    assert.isAbove(resumed.authority.reservationFenceToken, first.authority.reservationFenceToken);
    assert.isFalse((yield* service.release(first.authority)).result);
  }),
);

it.effect("keeps provider and local-check capacity separate", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 2, interactiveReserve: 0 },
    });
    yield* admitted(
      service,
      request("check-running", {
        kind: "localCheck",
        accountScope: undefined,
        priority: "interactive",
      }),
    );
    const provider = yield* service.request(request("provider-free"));
    const check = yield* service.request(
      request("check-waiting", { kind: "localCheck", accountScope: undefined }),
    );
    assert.equal(provider.result._tag, "Admitted");
    assert.equal(check.result._tag, "Waiting");
    if (check.result._tag === "Waiting") assert.equal(check.result.reason, "local-check-limit");
  }),
);

it.effect("shares a same-kind parent slot without double counting or deadlock", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness();
    const parent = yield* admitted(
      service,
      request("parent", {
        kind: "localCheck",
        accountScope: undefined,
        priority: "interactive",
      }),
    );
    const child = yield* service.request(
      request("child", {
        kind: "localCheck",
        accountScope: undefined,
        priority: "interactive",
        parent: {
          reservationId: parent.authority.reservationId,
          reservationFenceToken: parent.authority.reservationFenceToken,
        },
      }),
    );
    assert.equal(child.result._tag, "Admitted");
    if (child.result._tag === "Admitted") assert.isFalse(child.result.accounted);
    assert.equal(
      (yield* service.request(
        request("unrelated", { kind: "localCheck", accountScope: undefined }),
      )).result._tag,
      "Waiting",
    );
  }),
);

it.effect("uses stable CPU and memory hysteresis for background starts", () =>
  Effect.gen(function* () {
    let sample = healthySample();
    const { service } = yield* makeHarness({ sample: () => sample });
    sample = { ...sample, sampledAtMs: 2, cpuUtilization: 0.9 };
    const cpu = yield* service.request(request("cpu"));
    assert.equal(cpu.result._tag, "Waiting");
    if (cpu.result._tag === "Waiting") assert.equal(cpu.result.reason, "cpu-pressure");
    sample = { ...sample, sampledAtMs: 3, cpuUtilization: 0.8 };
    assert.deepEqual(yield* service.refresh, []);
    sample = { ...sample, sampledAtMs: 4, cpuUtilization: 0.7 };
    assert.equal((yield* service.refresh)[0]?.requestId, "cpu");

    const cpuDecision = (yield* service.request(request("cpu"))).result;
    if (cpuDecision._tag !== "Admitted") return yield* Effect.die("expected CPU admission");
    yield* service.release(cpuDecision.authority);
    sample = {
      ...sample,
      sampledAtMs: 5,
      availableMemoryBytes: 1024 * 1024 * 1024,
    };
    const memory = yield* service.request(request("memory"));
    assert.equal(memory.result._tag, "Waiting");
    if (memory.result._tag === "Waiting") assert.equal(memory.result.reason, "memory-pressure");
    sample = {
      ...sample,
      sampledAtMs: 6,
      availableMemoryBytes: 1.8 * 1024 * 1024 * 1024,
    };
    assert.deepEqual(yield* service.refresh, []);
    sample = {
      ...sample,
      sampledAtMs: 7,
      availableMemoryBytes: 2 * 1024 * 1024 * 1024,
    };
    assert.equal((yield* service.refresh)[0]?.requestId, "memory");
  }),
);

it.effect("reschedules a waiting acquire when CPU pressure falls below the resume threshold", () =>
  Effect.gen(function* () {
    let sample = { ...healthySample(), sampledAtMs: 2, cpuUtilization: 0.9 };
    const pressureWaitStarted = yield* Deferred.make<void>();
    const pressureChanged = yield* Deferred.make<void>();
    const pending = request("cpu-acquire");
    const { service } = yield* makeHarness({
      sample: () => sample,
      awaitPressureChange: () =>
        Deferred.succeed(pressureWaitStarted, undefined).pipe(
          Effect.andThen(Deferred.await(pressureChanged)),
        ),
    });

    const fiber = yield* service.acquire(pending).pipe(Effect.forkChild);
    yield* Deferred.await(pressureWaitStarted);
    assert.equal(
      (yield* service.snapshot).entries.find((entry) => entry.requestId === pending.requestId)
        ?.waitReason,
      "cpu-pressure",
    );

    sample = { ...sample, sampledAtMs: 3, cpuUtilization: 0.7 };
    yield* Deferred.succeed(pressureChanged, undefined);
    const admission = yield* Fiber.join(fiber);
    assert.equal(admission._tag, "Admitted");
    if (admission._tag !== "Admitted") return yield* Effect.die("expected CPU admission");
    const replay = (yield* service.request(pending)).result;
    if (replay._tag !== "Admitted") return yield* Effect.die("expected replayed admission");
    assert.deepEqual(replay.authority, admission.authority);
  }),
);

it.effect("reschedules a waiting acquire when memory pressure crosses the resume threshold", () =>
  Effect.gen(function* () {
    let sample = {
      ...healthySample(),
      sampledAtMs: 2,
      availableMemoryBytes: 1024 * 1024 * 1024,
    };
    const pressureWaitStarted = yield* Deferred.make<void>();
    const pressureChanged = yield* Deferred.make<void>();
    const pending = request("memory-acquire");
    const { service } = yield* makeHarness({
      sample: () => sample,
      awaitPressureChange: () =>
        Deferred.succeed(pressureWaitStarted, undefined).pipe(
          Effect.andThen(Deferred.await(pressureChanged)),
        ),
    });

    const fiber = yield* service.acquire(pending).pipe(Effect.forkChild);
    yield* Deferred.await(pressureWaitStarted);
    assert.equal(
      (yield* service.snapshot).entries.find((entry) => entry.requestId === pending.requestId)
        ?.waitReason,
      "memory-pressure",
    );

    sample = {
      ...sample,
      sampledAtMs: 3,
      availableMemoryBytes: 2 * 1024 * 1024 * 1024,
    };
    yield* Deferred.succeed(pressureChanged, undefined);
    const admission = yield* Fiber.join(fiber);
    assert.equal(admission._tag, "Admitted");
    if (admission._tag !== "Admitted") return yield* Effect.die("expected memory admission");
    const replay = (yield* service.request(pending)).result;
    if (replay._tag !== "Admitted") return yield* Effect.die("expected replayed admission");
    assert.deepEqual(replay.authority, admission.authority);
  }),
);

it.effect("fails visibly for missing telemetry and mandatory unavailable GPU", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      sample: () => ({
        ...healthySample(),
        telemetry: "unavailable",
        cpuUtilization: null,
        availableMemoryBytes: null,
      }),
    });
    const background = yield* service.request(request("no-telemetry"));
    assert.equal(background.result._tag, "Waiting");
    if (background.result._tag === "Waiting")
      assert.equal(background.result.reason, "telemetry-unavailable");
    assert.equal(
      (yield* service.request(request("interactive-no-telemetry", { priority: "interactive" })))
        .result._tag,
      "Admitted",
    );
    const gpu = yield* service.request(request("gpu", { gpuRequired: true }));
    assert.equal(gpu.result._tag, "Rejected");
    if (gpu.result._tag === "Rejected") assert.equal(gpu.result.reason, "gpu-unavailable");
  }),
);

it.effect("fences takeover so late old-owner feedback cannot release the reservation", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness();
    const originalRequest = request("takeover", { ownerId: "old-owner", ownerFenceToken: 2 });
    const original = yield* admitted(service, originalRequest);
    const takeover = yield* service.request({
      ...originalRequest,
      ownerId: "new-owner",
      ownerFenceToken: 3,
    });
    assert.equal(takeover.result._tag, "Admitted");
    assert.isFalse((yield* service.release(original.authority)).result);
    if (takeover.result._tag !== "Admitted") return yield* Effect.die("expected takeover");
    assert.isTrue((yield* service.release(takeover.result.authority)).result);
  }),
);

it.effect("requires explicit reconciliation to take over active or orphaned work", () =>
  Effect.gen(function* () {
    const settings = {
      ...defaultResourceAdmissionSettings,
      providerMaxConcurrent: 1,
      interactiveReserve: 0,
    };
    const { ledger, pressure, service } = yield* makeHarness({ settings });
    const originalRequest = request("active-takeover", {
      ownerId: "provider-coordinator:2147483647:old",
      ownerFenceToken: 2,
    });
    const original = yield* admitted(service, originalRequest);
    yield* service.observeActivity(original.authority, "active");
    const activeReplay = yield* service.request(originalRequest);
    assert.equal(activeReplay.result._tag, "Waiting");
    if (activeReplay.result._tag === "Waiting")
      assert.equal(activeReplay.result.reason, "recovery-capacity");
    const newerRequest = {
      ...originalRequest,
      ownerId: `provider-coordinator:${process.pid}:new`,
      ownerFenceToken: 3,
    };
    const activeTakeover = yield* service.request(newerRequest);
    assert.equal(activeTakeover.result._tag, "Rejected");
    if (activeTakeover.result._tag === "Rejected")
      assert.equal(activeTakeover.result.reason, "ownership-conflict");

    const restarted = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    assert.equal(
      (yield* restarted.snapshot).entries.find((entry) => entry.requestId === "active-takeover")
        ?.activity,
      "orphaned-active",
    );
    const orphanedTakeover = yield* restarted.request(newerRequest);
    assert.equal(orphanedTakeover.result._tag, "Rejected");
    assert.isNotNull(yield* restarted.adoptActive(newerRequest));
  }),
);

it.effect("does not free possible activity merely because time passes", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    yield* admitted(service, request("possible"));
    yield* service.request(request("blocked"));
    assert.deepEqual(yield* service.refresh, []);
    const snapshot = yield* service.snapshot;
    assert.equal(
      snapshot.entries.find((entry) => entry.requestId === "possible")?.state,
      "admitted",
    );
    assert.equal(snapshot.entries.find((entry) => entry.requestId === "blocked")?.state, "waiting");
  }),
);

it.effect("interrupting acquire cancels even an unbound promoted reservation", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    const running = yield* admitted(service, request("running"));
    const waitingRequest = request("interruptible");
    const fiber = yield* service.acquire(waitingRequest).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    yield* Fiber.interrupt(fiber);
    assert.deepEqual((yield* service.release(running.authority)).newlyAdmitted, []);
    assert.equal(
      (yield* service.snapshot).entries.find((entry) => entry.requestId === "interruptible")?.state,
      "canceled",
    );
  }),
);

it.effect("startup cancels dead local-check waiters before scheduling", () =>
  Effect.gen(function* () {
    const settings = {
      ...defaultResourceAdmissionSettings,
      localCheckMaxConcurrent: 1,
    };
    const { ledger, pressure, service } = yield* makeHarness({ settings });
    yield* admitted(
      service,
      request("live-running", {
        kind: "localCheck",
        accountScope: undefined,
        priority: "interactive",
        ownerId: `local-process:${process.pid}:live`,
      }),
    );
    const dead = request("dead-waiting", {
      kind: "localCheck",
      accountScope: undefined,
      ownerId: "local-process:2147483647:dead",
    });
    assert.equal((yield* service.request(dead)).result._tag, "Waiting");

    const restarted = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    assert.equal(
      (yield* restarted.snapshot).entries.find((entry) => entry.requestId === dead.requestId)
        ?.state,
      "canceled",
    );
  }),
);

it.effect("recomputes machine settings instead of permanently ratcheting an environment", () =>
  Effect.gen(function* () {
    const ledger = yield* makeMemoryHostBudgetLedger();
    const pressure = ResourcePressure.of({
      sample: Effect.sync(healthySample),
      awaitChange: () => Effect.never,
    });
    let settings = {
      ...defaultResourceAdmissionSettings,
      providerMaxConcurrent: 1,
      interactiveReserve: 0,
    };
    const service = yield* make({ ledger, readSettings: Effect.sync(() => settings) }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    const first = yield* admitted(service, request("dynamic-first"));
    assert.equal((yield* service.request(request("dynamic-second"))).result._tag, "Waiting");
    settings = { ...settings, providerMaxConcurrent: 2 };
    assert.equal((yield* service.refresh)[0]?.requestId, "dynamic-second");
    yield* service.release(first.authority);
    assert.equal((yield* service.snapshot).effectiveSettings?.providerMaxConcurrent, 2);
  }),
);

it.effect("startup cancels an unbound dead grant and never ghost-starts it", () =>
  Effect.gen(function* () {
    const settings = {
      ...defaultResourceAdmissionSettings,
      localCheckMaxConcurrent: 1,
    };
    const { ledger, pressure, service } = yield* makeHarness({ settings });
    const dead = request("dead-possible", {
      kind: "localCheck",
      accountScope: undefined,
      ownerId: "local-process:2147483647:dead",
    });
    yield* admitted(service, dead);
    const live = request("live-waiter", {
      kind: "localCheck",
      accountScope: undefined,
      ownerId: `local-process:${process.pid}:live`,
    });
    yield* service.request(live);

    const restarted = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    const snapshot = yield* restarted.snapshot;
    assert.equal(
      snapshot.entries.find((entry) => entry.requestId === dead.requestId)?.state,
      "canceled",
    );
    assert.equal(
      snapshot.entries.find((entry) => entry.requestId === live.requestId)?.state,
      "admitted",
    );
    assert.equal((yield* restarted.request(dead)).result._tag, "Rejected");
  }),
);

it.effect("startup parks dead provider grants until a newer fenced owner retries", () =>
  Effect.gen(function* () {
    const settings = {
      ...defaultResourceAdmissionSettings,
      providerMaxConcurrent: 1,
      interactiveReserve: 0,
    };
    const { ledger, pressure, service } = yield* makeHarness({ settings });
    const deadRequest = request("dead-provider", {
      priority: "interactive",
      ownerId: "provider-coordinator:2147483647:old",
      ownerFenceToken: 2,
    });
    yield* admitted(service, deadRequest);
    const restarted = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    const parked = (yield* restarted.snapshot).entries.find(
      (entry) => entry.requestId === deadRequest.requestId,
    );
    assert.equal(parked?.state, "waiting");
    assert.equal(parked?.activity, "unknown");
    const resumed = yield* restarted.request({
      ...deadRequest,
      ownerId: `provider-coordinator:${process.pid}:new`,
      ownerFenceToken: 3,
    });
    assert.equal(resumed.result._tag, "Admitted");
  }),
);

it.effect("startup keeps dead active and unknown checks fail-closed and visible", () =>
  Effect.gen(function* () {
    for (const activity of ["active", "unknown"] as const) {
      const settings = {
        ...defaultResourceAdmissionSettings,
        localCheckMaxConcurrent: 1,
      };
      const { ledger, pressure, service } = yield* makeHarness({ settings });
      const dead = yield* admitted(
        service,
        request(`dead-${activity}`, {
          kind: "localCheck",
          accountScope: undefined,
          ownerId: "local-process:2147483647:dead",
        }),
      );
      yield* service.observeActivity(dead.authority, activity);
      const waiter = request(`waiter-${activity}`, {
        kind: "localCheck",
        accountScope: undefined,
        ownerId: `local-process:${process.pid}:live`,
      });
      yield* service.request(waiter);

      const restarted = yield* make({ ledger, settings }).pipe(
        Effect.provideService(ResourcePressure, pressure),
      );
      const snapshot = yield* restarted.snapshot;
      assert.equal(
        snapshot.entries.find((entry) => entry.requestId === dead.authority.reservationId)
          ?.activity,
        activity === "active" ? "orphaned-active" : "orphaned-unknown",
      );
      assert.equal(
        snapshot.entries.find((entry) => entry.requestId === waiter.requestId)?.state,
        "waiting",
      );
    }
  }),
);

it.effect("keeps only bounded recent terminal tombstones", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness({
      settings: { providerMaxConcurrent: 1, interactiveReserve: 0 },
    });
    for (let index = 0; index < 270; index += 1) {
      const current = request(`terminal-${index}`);
      const admission = yield* admitted(service, current);
      yield* service.release(admission.authority);
    }
    const snapshot = yield* service.snapshot;
    assert.equal(
      snapshot.entries.filter((entry) => entry.state === "released" || entry.state === "canceled")
        .length,
      256,
    );
    assert.isUndefined(snapshot.entries.find((entry) => entry.requestId === "terminal-0"));
    assert.equal((yield* service.request(request("terminal-269"))).result._tag, "Rejected");
  }),
);

it.effect("restart cancels dead-process local work that never became active", () =>
  Effect.gen(function* () {
    const ledger = yield* makeMemoryHostBudgetLedger();
    const pressure = ResourcePressure.of({
      sample: Effect.sync(healthySample),
      awaitChange: () => Effect.never,
    });
    const settings = { ...defaultResourceAdmissionSettings, localCheckMaxConcurrent: 1 };
    const first = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    const dead = request("dead-local", {
      kind: "localCheck",
      accountScope: undefined,
      priority: "background",
      ownerId: "local-process:2147483647:handoff",
    });
    assert.equal((yield* first.request(dead)).result._tag, "Admitted");
    const restarted = yield* make({ ledger, settings }).pipe(
      Effect.provideService(ResourcePressure, pressure),
    );
    assert.equal(
      (yield* restarted.snapshot).entries.find((entry) => entry.requestId === dead.requestId)
        ?.state,
      "canceled",
    );
    assert.equal(
      (yield* restarted.request(
        request("replacement-local", { kind: "localCheck", accountScope: undefined }),
      )).result._tag,
      "Admitted",
    );
  }),
);
