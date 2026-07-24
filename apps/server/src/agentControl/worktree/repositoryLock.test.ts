import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";

import { withAgentControlRepositoryLock } from "./repositoryLock.ts";

const layer = it.layer(NodeServices.layer);

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
});
