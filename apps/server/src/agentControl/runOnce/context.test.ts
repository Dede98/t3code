import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";

import {
  makeAgentControlRunOnceKeyedFence,
  withAgentControlRunOnceProjectFence,
} from "./context.ts";

it.effect("serializes one project while an independent project keeps progressing", () =>
  Effect.gen(function* () {
    const projectA = ProjectId.make("run-once-fence-project-a");
    const projectB = ProjectId.make("run-once-fence-project-b");
    const firstEntered = yield* Deferred.make<void>();
    const releaseFirst = yield* Deferred.make<void>();
    const sameProjectEntered = yield* Deferred.make<void>();
    const otherProjectEntered = yield* Deferred.make<void>();

    const first = yield* withAgentControlRunOnceProjectFence(
      projectA,
      Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Deferred.await(releaseFirst))),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(firstEntered);
    const sameProject = yield* withAgentControlRunOnceProjectFence(
      projectA,
      Deferred.succeed(sameProjectEntered, undefined),
    ).pipe(Effect.forkChild);
    const otherProject = yield* withAgentControlRunOnceProjectFence(
      projectB,
      Deferred.succeed(otherProjectEntered, undefined),
    ).pipe(Effect.forkChild);

    yield* Deferred.await(otherProjectEntered);
    assert.isFalse(yield* Deferred.isDone(sameProjectEntered));
    yield* Fiber.join(otherProject);
    yield* Deferred.succeed(releaseFirst, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(sameProject);
    assert.isTrue(yield* Deferred.isDone(sameProjectEntered));
  }),
);

it.effect("evicts one-shot keys after success and failure", () =>
  Effect.gen(function* () {
    const fence = makeAgentControlRunOnceKeyedFence<string>();
    yield* Effect.forEach(
      Array.from({ length: 1_000 }, (_, index) => `one-shot-${index}`),
      (key) => fence.withPermit(key, Effect.void),
      { concurrency: "unbounded", discard: true },
    );
    assert.equal(yield* fence.activeKeyCount, 0);

    const failure = new Error("expected keyed-fence failure");
    const failed = yield* Effect.exit(fence.withPermit("failed", Effect.fail(failure)));
    assert.deepStrictEqual(failed, Exit.fail(failure));
    assert.equal(yield* fence.activeKeyCount, 0);
  }),
);

it.effect("retains a key through owner and waiter interruption, then evicts it", () =>
  Effect.gen(function* () {
    const fence = makeAgentControlRunOnceKeyedFence<string>();
    const ownerEntered = yield* Deferred.make<void>();
    const releaseOwner = yield* Deferred.make<void>();
    const waiterEntered = yield* Deferred.make<void>();

    const owner = yield* fence
      .withPermit(
        "shared",
        Deferred.succeed(ownerEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseOwner)),
        ),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(ownerEntered);
    const waiter = yield* fence
      .withPermit("shared", Deferred.succeed(waiterEntered, undefined))
      .pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    assert.equal(yield* fence.activeKeyCount, 1);
    assert.isFalse(yield* Deferred.isDone(waiterEntered));

    yield* Fiber.interrupt(waiter);
    assert.equal(yield* fence.activeKeyCount, 1);
    yield* Deferred.succeed(releaseOwner, undefined);
    yield* Fiber.join(owner);
    assert.equal(yield* fence.activeKeyCount, 0);
    assert.isFalse(yield* Deferred.isDone(waiterEntered));
  }),
);
