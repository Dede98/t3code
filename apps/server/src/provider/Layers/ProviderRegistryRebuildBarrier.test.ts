import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import { ProviderRegistryRebuildBarrierLive } from "./ProviderRegistryRebuildBarrier.ts";

it.layer(ProviderRegistryRebuildBarrierLive)("ProviderRegistryRebuildBarrier", (it) => {
  it.effect("allows concurrent operations and makes rebuilds exclusive", () =>
    Effect.gen(function* () {
      const barrier = yield* ProviderRegistryRebuildBarrier;
      const releaseOperations = yield* Deferred.make<void>();
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const rebuildStarted = yield* Deferred.make<void>();

      const first = yield* barrier
        .withOperation(
          Deferred.succeed(firstStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseOperations)),
          ),
        )
        .pipe(Effect.forkScoped);
      const second = yield* barrier
        .withOperation(
          Deferred.succeed(secondStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseOperations)),
          ),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstStarted);
      yield* Deferred.await(secondStarted);

      const rebuild = yield* barrier
        .withRebuild(Deferred.succeed(rebuildStarted, undefined))
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* Deferred.poll(rebuildStarted)));

      yield* Deferred.succeed(releaseOperations, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      yield* Fiber.join(rebuild);
      assert.isTrue(Option.isSome(yield* Deferred.poll(rebuildStarted)));
    }),
  );

  it.effect("blocks new operations while a rebuild is active", () =>
    Effect.gen(function* () {
      const barrier = yield* ProviderRegistryRebuildBarrier;
      const rebuildStarted = yield* Deferred.make<void>();
      const releaseRebuild = yield* Deferred.make<void>();
      const operationStarted = yield* Deferred.make<void>();

      const rebuild = yield* barrier
        .withRebuild(
          Deferred.succeed(rebuildStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseRebuild)),
          ),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(rebuildStarted);

      const operation = yield* barrier
        .withOperation(Deferred.succeed(operationStarted, undefined))
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* Deferred.poll(operationStarted)));

      yield* Deferred.succeed(releaseRebuild, undefined);
      yield* Fiber.join(rebuild);
      yield* Fiber.join(operation);
      assert.isTrue(Option.isSome(yield* Deferred.poll(operationStarted)));
    }),
  );
});
