// @effect-diagnostics nodeBuiltinImport:off - integration owns and cleans up an actual process group.
import * as NodeChildProcess from "node:child_process";
import { assert, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import { makeMemoryHostBudgetLedger } from "./HostBudgetLedger.ts";
import { make } from "./ResourceAdmission.ts";
import { ResourcePressure } from "./ResourcePressure.ts";
import { defaultResourceAdmissionSettings, type ResourceAdmissionRequest } from "./model.ts";

const localCheck = (requestId: string): ResourceAdmissionRequest => ({
  requestId,
  kind: "localCheck",
  priority: "background",
  ownerId: `owner-${requestId}`,
  ownerFenceToken: 1,
  executionKey: `check-process-${requestId}`,
});

const spawnOwnedProcessTree = Effect.fn("resourceAdmission.test.spawnOwnedProcessTree")(
  function* () {
    const platform = yield* HostProcessPlatform;
    const script = `
      const { spawn } = require("node:child_process");
      const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], {
        stdio: ["pipe", "ignore", "ignore"]
      });
      process.stdout.write(JSON.stringify({ childPid: child.pid }) + "\\n");
      process.stdin.resume();
    `;
    const child = NodeChildProcess.spawn(process.execPath, ["-e", script], {
      detached: platform !== "win32",
      stdio: ["pipe", "pipe", "ignore"],
    });
    const rootPid = child.pid;
    if (rootPid === undefined) return yield* Effect.die("spawn returned no pid");
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    let cleaned = false;
    const cleanup = Effect.fn("resourceAdmission.test.cleanupOwnedProcessTree")(function* () {
      if (cleaned) return;
      cleaned = true;
      if (child.exitCode === null && child.signalCode === null) {
        if (platform === "win32") child.kill("SIGTERM");
        else process.kill(-rootPid, "SIGTERM");
      }
      yield* Effect.promise(() => exited);
    });
    yield* Effect.addFinalizer(() => cleanup());
    const childPid = yield* Effect.callback<number>((resume) => {
      let buffered = "";
      const onData = (chunk: Buffer) => {
        buffered += chunk.toString("utf8");
        const newline = buffered.indexOf("\n");
        if (newline < 0) return;
        child.stdout.off("data", onData);
        const value = JSON.parse(buffered.slice(0, newline)) as { readonly childPid: number };
        resume(Effect.succeed(value.childPid));
      };
      child.stdout.on("data", onData);
      return Effect.sync(() => child.stdout.off("data", onData));
    });
    return { rootPid, childPid, cleanup };
  },
);

it.effect(
  "holds a local slot for a real process tree, wakes once after cleanup, and cancels a waiter",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ledger = yield* makeMemoryHostBudgetLedger();
        const service = yield* make({
          ledger,
          settings: { ...defaultResourceAdmissionSettings, localCheckMaxConcurrent: 1 },
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
        const first = yield* service.acquire(localCheck("first"));
        assert.equal(first._tag, "Admitted");
        if (first._tag !== "Admitted") return yield* Effect.die("expected first admission");
        yield* service.observeActivity(first.authority, "active");

        const processTree = yield* spawnOwnedProcessTree();
        assert.isAbove(processTree.rootPid, 0);
        assert.isAbove(processTree.childPid, 0);

        const secondFiber = yield* service.acquire(localCheck("second")).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        assert.isUndefined(secondFiber.pollUnsafe());

        yield* processTree.cleanup();
        const released = yield* service.release(first.authority);
        assert.deepEqual(
          released.newlyAdmitted.map((grant) => grant.requestId),
          ["second"],
        );
        const second = yield* Fiber.join(secondFiber);
        assert.equal(second._tag, "Admitted");
        if (second._tag !== "Admitted") return yield* Effect.die("expected second admission");

        const canceledFiber = yield* service.acquire(localCheck("canceled")).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(canceledFiber);
        yield* service.release(second.authority);
        const canceled = (yield* service.snapshot).entries.find(
          (entry) => entry.requestId === "canceled",
        );
        assert.equal(canceled?.state, "canceled");
      }),
    ),
);
