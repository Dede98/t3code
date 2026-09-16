// @effect-diagnostics nodeBuiltinImport:off - exercises the real host filesystem coordinator.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import { makeFileHostBudgetLedger } from "./HostBudgetLedger.ts";
import { make } from "./ResourceAdmission.ts";
import { ResourcePressure } from "./ResourcePressure.ts";
import { defaultResourceAdmissionSettings, type ResourceAdmissionRequest } from "./model.ts";

const withTemporaryDirectory = <A, E, R>(
  use: (directory: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-host-budget-"))),
    use,
    (directory) => Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
  );

const makeService = (path: string, overrides?: Partial<typeof defaultResourceAdmissionSettings>) =>
  make({
    ledger: makeFileHostBudgetLedger(path),
    settings: {
      ...defaultResourceAdmissionSettings,
      providerMaxConcurrent: 1,
      interactiveReserve: 0,
      ...overrides,
    },
  }).pipe(
    Effect.provideService(
      ResourcePressure,
      ResourcePressure.of({
        sample: Effect.succeed({
          sampledAtMs: 1,
          telemetry: "available",
          cpuUtilization: 0.1,
          availableMemoryBytes: 8 * 1024 * 1024 * 1024,
          gpu: { status: "unavailable" },
        }),
        awaitChange: () => Effect.never,
      }),
    ),
  );

const request = (id: string): ResourceAdmissionRequest => ({
  requestId: id,
  kind: "providerTurn",
  priority: "background",
  accountScope: "shared-account",
  ownerId: `owner-${id}`,
  ownerFenceToken: 1,
  executionKey: `session-${id}`,
});

it.effect("serializes transactions from independent environment ledgers without lost updates", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const first = makeFileHostBudgetLedger(path);
      const second = makeFileHostBudgetLedger(path);
      yield* Effect.all(
        Array.from({ length: 20 }, (_, index) =>
          (index % 2 === 0 ? first : second).transact((state) => ({
            state: { ...state, nextSequence: state.nextSequence + 1 },
            value: undefined,
          })),
        ),
        { concurrency: "unbounded" },
      );
      const state = yield* first.read;
      assert.equal(state.nextSequence, 21);
      assert.equal(state.revision, 20);
    }),
  ),
);

it.effect("preserves active occupancy and execution identity across service restart", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const first = yield* makeService(path);
      assert.equal((yield* first.request(request("active"))).result._tag, "Admitted");

      const restarted = yield* makeService(path);
      const blocked = yield* restarted.request(request("after-restart"));
      assert.equal(blocked.result._tag, "Waiting");
      const active = (yield* restarted.snapshot).entries.find(
        (entry) => entry.requestId === "active",
      );
      assert.equal(active?.executionKey, "session-active");
      assert.equal(active?.state, "admitted");
    }),
  ),
);

it.effect("shares one configured machine budget between two local environments", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const [environmentA, environmentB] = yield* Effect.all([
        makeService(path),
        makeService(path),
      ]);
      const results = yield* Effect.all(
        [
          environmentA.request(request("environment-a")),
          environmentB.request(request("environment-b")),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((entry) => entry.result._tag === "Admitted").length, 1);
      assert.equal(results.filter((entry) => entry.result._tag === "Waiting").length, 1);
    }),
  ),
);

it.effect("wakes an acquire in another environment from a file-ledger release", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const environmentA = yield* makeService(path);
      const environmentB = yield* makeService(path);
      const first = (yield* environmentA.request(request("active"))).result;
      assert.equal(first._tag, "Admitted");
      if (first._tag !== "Admitted") return yield* Effect.die("expected admission");
      const waiter = yield* environmentB.acquire(request("waiter")).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isUndefined(waiter.pollUnsafe());
      yield* environmentA.release(first.authority);
      const admitted = yield* Fiber.join(waiter);
      assert.equal(admitted._tag, "Admitted");
    }),
  ),
);

it.effect("composes conflicting environment settings into one conservative host limit", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const strict = yield* makeService(path, { providerMaxConcurrent: 1 });
      const loose = yield* makeService(path, { providerMaxConcurrent: 4 });
      const results = yield* Effect.all(
        [
          loose.request(request("loose-a")),
          loose.request(request("loose-b")),
          strict.request(request("strict-a")),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((entry) => entry.result._tag === "Admitted").length, 1);
      assert.equal((yield* loose.snapshot).effectiveSettings?.providerMaxConcurrent, 1);
    }),
  ),
);
