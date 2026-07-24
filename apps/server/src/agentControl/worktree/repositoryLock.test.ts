// @effect-diagnostics nodeBuiltinImport:off - verifies cross-process filesystem lock exclusion.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";

import { withAgentControlRepositoryLock } from "./repositoryLock.ts";

const layer = it.layer(NodeServices.layer);

const childScript = `
const fs = require("node:fs");
const lockPath = process.argv[1];
try {
  fs.mkdirSync(lockPath, { mode: 0o700 });
  process.stdout.write("acquired\\n");
  process.stdin.once("data", () => {
    fs.rmdirSync(lockPath);
    process.exit(0);
  });
  process.stdin.resume();
} catch (error) {
  if (error && error.code === "EEXIST") {
    process.stdout.write("busy\\n");
    process.exit(2);
  }
  process.stderr.write(String(error));
  process.exit(3);
}
`;

const childLine = (child: NodeChildProcess.ChildProcessWithoutNullStreams) =>
  Effect.promise(
    () =>
      new Promise<string>((resolve) => {
        child.stdout.once("data", (chunk) => resolve(String(chunk).trim()));
      }),
  );
const childExit = (child: NodeChildProcess.ChildProcessWithoutNullStreams) =>
  Effect.promise(
    () =>
      new Promise<number | null>((resolve) => {
        child.once("exit", (code) => resolve(code));
      }),
  );

layer("Agent Control repository lock", (it) => {
  it.effect("serializes independent holders and cleans up through finalizers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const commonDir = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-repository-lock-",
      });
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const first = yield* withAgentControlRepositoryLock({
        repositoryCommonDir: commonDir,
        runtimeHolderId: "runtime-one",
        effect: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      }).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const second = yield* withAgentControlRepositoryLock({
        repositoryCommonDir: commonDir,
        runtimeHolderId: "runtime-two",
        effect: Effect.succeed("entered"),
      }).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.equal(second.pollUnsafe(), undefined);
      yield* Fiber.interrupt(second);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      assert.equal(yield* fs.exists(path.join(commonDir, "t3-agent-control.lock")), false);
    }),
  );

  it.effect("never steals an existing lock with an unknown owner", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const commonDir = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-repository-lock-live-",
      });
      const lockPath = path.join(commonDir, "t3-agent-control.lock");
      yield* fs.makeDirectory(lockPath, { mode: 0o700 });
      const waiter = yield* Effect.result(
        withAgentControlRepositoryLock({
          repositoryCommonDir: commonDir,
          runtimeHolderId: "runtime-waiter",
          timeoutMs: 50,
          effect: Effect.succeed("must-not-enter"),
        }),
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("100 millis");
      const result = yield* Fiber.join(waiter);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, "busy");
      assert.equal(yield* fs.exists(lockPath), true);
    }),
  );

  it.effect("an old finalizer cannot remove a replacement owner lock", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const commonDir = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-repository-lock-replacement-",
      });
      const lockPath = path.join(commonDir, "t3-agent-control.lock");
      const enteredA = yield* Deferred.make<void>();
      const releaseA = yield* Deferred.make<void>();
      const resultA = yield* Effect.result(
        withAgentControlRepositoryLock({
          repositoryCommonDir: commonDir,
          runtimeHolderId: "runtime-a",
          effect: Deferred.succeed(enteredA, undefined).pipe(
            Effect.andThen(Deferred.await(releaseA)),
          ),
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(enteredA);

      yield* fs.remove(lockPath, { recursive: true });
      const enteredB = yield* Deferred.make<void>();
      const releaseB = yield* Deferred.make<void>();
      const resultB = yield* withAgentControlRepositoryLock({
        repositoryCommonDir: commonDir,
        runtimeHolderId: "runtime-b",
        effect: Deferred.succeed(enteredB, undefined).pipe(
          Effect.andThen(Deferred.await(releaseB)),
        ),
      }).pipe(Effect.forkChild);
      yield* Deferred.await(enteredB);

      yield* Deferred.succeed(releaseA, undefined);
      const exitedA = yield* Fiber.join(resultA);
      assert.equal(exitedA._tag, "Failure");
      if (exitedA._tag === "Failure") {
        assert.equal(exitedA.failure.reason, "ownership-lost");
      }
      assert.equal(yield* fs.exists(lockPath), true);

      const contender = yield* Effect.result(
        withAgentControlRepositoryLock({
          repositoryCommonDir: commonDir,
          runtimeHolderId: "runtime-c",
          timeoutMs: 50,
          effect: Effect.succeed("must-not-enter"),
        }),
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("100 millis");
      const blocked = yield* Fiber.join(contender);
      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") assert.equal(blocked.failure.reason, "busy");

      yield* Deferred.succeed(releaseB, undefined);
      yield* Fiber.join(resultB);
      assert.equal(yield* fs.exists(lockPath), false);
    }),
  );

  it.effect("excludes a second real child process from the same lock path", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const commonDir = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-repository-lock-children-",
      });
      const lockPath = path.join(commonDir, "t3-agent-control.lock");
      const first = NodeChildProcess.spawn(process.execPath, ["-e", childScript, lockPath]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (!first.killed) first.kill("SIGKILL");
        }),
      );
      assert.equal(yield* childLine(first), "acquired");

      const second = NodeChildProcess.spawn(process.execPath, ["-e", childScript, lockPath]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (!second.killed) second.kill("SIGKILL");
        }),
      );
      assert.equal(yield* childLine(second), "busy");
      assert.equal(yield* childExit(second), 2);
      assert.equal(yield* fs.exists(lockPath), true);

      first.stdin.write("release\n");
      assert.equal(yield* childExit(first), 0);
      assert.equal(yield* fs.exists(lockPath), false);
    }),
  );
});
