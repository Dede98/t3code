import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import { ProviderThreadOperationLock } from "../Services/ProviderThreadOperationLock.ts";
import { ProviderThreadOperationLockLive } from "./ProviderThreadOperationLock.ts";

it.layer(ProviderThreadOperationLockLive)("ProviderThreadOperationLock", (it) => {
  it.effect("serializes operations for the same thread", () =>
    Effect.gen(function* () {
      const lock = yield* ProviderThreadOperationLock;
      const threadId = ThreadId.make("thread-lock-serial");
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();

      const first = yield* lock
        .withLock(
          threadId,
          Deferred.succeed(firstStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFirst)),
          ),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstStarted);

      const second = yield* lock
        .withLock(threadId, Deferred.succeed(secondStarted, undefined))
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* Deferred.poll(secondStarted)));

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      assert.isTrue(Option.isSome(yield* Deferred.poll(secondStarted)));
    }),
  );

  it.effect("allows different threads to proceed independently", () =>
    Effect.gen(function* () {
      const lock = yield* ProviderThreadOperationLock;
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();

      const first = yield* lock
        .withLock(
          ThreadId.make("thread-lock-a"),
          Deferred.succeed(firstStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFirst)),
          ),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstStarted);

      yield* lock.withLock(
        ThreadId.make("thread-lock-b"),
        Deferred.succeed(secondStarted, undefined),
      );
      assert.isTrue(Option.isSome(yield* Deferred.poll(secondStarted)));

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
    }),
  );
});
