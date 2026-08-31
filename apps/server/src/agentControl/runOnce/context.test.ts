import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import { withAgentControlRunOnceProjectFence } from "./context.ts";

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
