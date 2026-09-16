// @effect-diagnostics nodeBuiltinImport:off - exercises the real host filesystem coordinator.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
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

it.effect("releases the machine mutex when its native owner disappears mid-transaction", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const owner = new NodeSqlite.DatabaseSync(`${path}.mutex.sqlite`);
      owner.exec("PRAGMA busy_timeout = 0; BEGIN IMMEDIATE");
      const transaction = yield* makeFileHostBudgetLedger(path)
        .transact((state) => ({
          state: { ...state, nextSequence: state.nextSequence + 1 },
          value: undefined,
        }))
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isUndefined(transaction.pollUnsafe());

      // Closing a native handle with an open transaction models process loss:
      // SQLite rolls it back and releases the OS lock without PID-file repair.
      owner.close();
      assert.equal((yield* Fiber.join(transaction)).revision, 1);
    }),
  ),
);

it.effect("releases the machine mutex after the owning process crashes", () =>
  withTemporaryDirectory((directory) =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        NodeChildProcess.spawn(
          process.execPath,
          [
            "--no-warnings",
            "-e",
            [
              'const { DatabaseSync } = require("node:sqlite")',
              "const database = new DatabaseSync(process.argv[1])",
              'database.exec("PRAGMA busy_timeout = 0; BEGIN IMMEDIATE")',
              "globalThis.__database = database",
              'process.stdout.write("locked\\n")',
              "setInterval(() => undefined, 60_000)",
            ].join(";"),
            NodePath.join(directory, "host-budget.json.mutex.sqlite"),
          ],
          { stdio: ["ignore", "pipe", "pipe"] },
        ),
      ),
      (owner) =>
        Effect.gen(function* () {
          yield* Effect.promise(
            () =>
              new Promise<void>((resolve, reject) => {
                const onData = (chunk: Buffer) => {
                  if (!chunk.toString().includes("locked")) return;
                  owner.stdout.off("data", onData);
                  owner.off("exit", onExit);
                  resolve();
                };
                const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
                  owner.stdout.off("data", onData);
                  reject(
                    new Error(`SQLite lock owner exited before readiness (${code ?? signal}).`),
                  );
                };
                owner.stdout.on("data", onData);
                owner.once("exit", onExit);
              }),
          );
          const path = NodePath.join(directory, "host-budget.json");
          const transaction = yield* makeFileHostBudgetLedger(path)
            .transact((state) => ({
              state: { ...state, nextSequence: state.nextSequence + 1 },
              value: undefined,
            }))
            .pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          assert.isUndefined(transaction.pollUnsafe());

          const exited = new Promise<void>((resolve) => owner.once("exit", () => resolve()));
          assert.isTrue(owner.kill("SIGKILL"));
          yield* Effect.promise(() => exited);
          assert.equal((yield* Fiber.join(transaction)).revision, 1);
        }),
      (owner) =>
        Effect.sync(() => {
          if (owner.exitCode === null && owner.signalCode === null) owner.kill("SIGKILL");
        }),
    ),
  ),
);

it.effect("serializes two contenders queued at the lock-release boundary", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const owner = new NodeSqlite.DatabaseSync(`${path}.mutex.sqlite`);
      owner.exec("PRAGMA busy_timeout = 0; BEGIN IMMEDIATE");
      const ledgers = [makeFileHostBudgetLedger(path), makeFileHostBudgetLedger(path)];
      const contenders = yield* Effect.forEach(ledgers, (ledger) =>
        ledger
          .transact((state) => ({
            state: { ...state, nextSequence: state.nextSequence + 1 },
            value: undefined,
          }))
          .pipe(Effect.forkChild),
      );
      yield* Effect.yieldNow;
      assert.isTrue(contenders.every((fiber) => fiber.pollUnsafe() === undefined));

      owner.exec("ROLLBACK");
      owner.close();
      const results = yield* Effect.forEach(contenders, Fiber.join);
      assert.deepEqual(results.map((result) => result.revision).sort(), [1, 2]);
      assert.equal((yield* ledgers[0]!.read).nextSequence, 3);
    }),
  ),
);

it.effect("does not run an interrupted mutex waiter after capacity is released", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const owner = new NodeSqlite.DatabaseSync(`${path}.mutex.sqlite`);
      owner.exec("PRAGMA busy_timeout = 0; BEGIN IMMEDIATE");
      const waiter = yield* makeFileHostBudgetLedger(path)
        .transact((state) => ({
          state: { ...state, nextSequence: state.nextSequence + 1 },
          value: undefined,
        }))
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(waiter);

      owner.exec("ROLLBACK");
      owner.close();
      const state = yield* makeFileHostBudgetLedger(path).read;
      assert.equal(state.nextSequence, 1);
      assert.equal(state.revision, 0);
    }),
  ),
);

it.effect("keeps a repeated waiting request revision and ledger file unchanged", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const path = NodePath.join(directory, "host-budget.json");
      const service = yield* makeService(path);
      assert.equal((yield* service.request(request("active"))).result._tag, "Admitted");
      const firstWaiting = yield* service.request(request("waiting"));
      assert.equal(firstWaiting.result._tag, "Waiting");
      const before = yield* Effect.promise(() => NodeFSP.readFile(path, "utf8"));

      const retried = yield* service.request(request("waiting"));
      const after = yield* Effect.promise(() => NodeFSP.readFile(path, "utf8"));

      assert.equal(retried.result._tag, "Waiting");
      assert.equal(retried.ledgerRevision, firstWaiting.ledgerRevision);
      assert.equal(after, before);
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
