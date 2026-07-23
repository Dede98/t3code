import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import { makeAgentControlTaskReconcileLocks } from "./reconcileLocks.ts";

it.effect("does not retain locks for projects that fail availability preflight", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    for (let index = 0; index < 1_000; index += 1) {
      for (const reason of ["project-missing", "project-deleted"] as const) {
        const result = yield* Effect.result(
          locks.withLock(ProjectId.make(`${reason}-${index}`), Effect.fail(reason), Effect.void),
        );
        assert.equal(result._tag, "Failure");
      }
    }
    assert.equal(yield* locks.size, 0);
  }),
);

it.effect("rechecks availability in the pass after successful lock preflight", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    const projectId = ProjectId.make("availability-switch");
    let available = true;
    const result = yield* Effect.result(
      locks.withLock(
        projectId,
        Effect.sync(() => {
          assert.equal(available, true);
          available = false;
        }),
        Effect.suspend(() => (available ? Effect.succeed("ran") : Effect.fail("project-deleted"))),
      ),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure, "project-deleted");
    assert.equal(yield* locks.size, 1);
  }),
);

it.effect("releases a project permit when its holder is interrupted", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    const projectId = ProjectId.make("interrupt-releases-permit");
    const holderEntered = yield* Deferred.make<void>();
    const releaseHolder = yield* Deferred.make<void>();
    const waiterEntered = yield* Deferred.make<void>();

    const holder = yield* locks
      .withLock(
        projectId,
        Effect.void,
        Deferred.succeed(holderEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseHolder)),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(holderEntered);

    const waiter = yield* locks
      .withLock(
        projectId,
        Effect.void,
        Deferred.succeed(waiterEntered, undefined).pipe(Effect.as("waiter-completed")),
      )
      .pipe(Effect.forkScoped);
    yield* Effect.yieldNow;
    assert.isTrue(Option.isNone(yield* Deferred.poll(waiterEntered)));

    yield* Fiber.interrupt(holder);
    assert.equal(yield* Fiber.join(waiter), "waiter-completed");
    assert.isTrue(Option.isSome(yield* Deferred.poll(waiterEntered)));
    assert.equal(yield* locks.withLock(projectId, Effect.void, Effect.succeed("reused")), "reused");
    assert.equal(yield* locks.size, 1);
  }),
);

it.effect("allows different projects to enter their critical sections concurrently", () =>
  Effect.gen(function* () {
    const locks = yield* makeAgentControlTaskReconcileLocks<ProjectId>();
    const projectAEntered = yield* Deferred.make<void>();
    const releaseProjectA = yield* Deferred.make<void>();
    const projectBEntered = yield* Deferred.make<void>();

    const projectA = yield* locks
      .withLock(
        ProjectId.make("concurrent-project-a"),
        Effect.void,
        Deferred.succeed(projectAEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseProjectA)),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(projectAEntered);

    yield* locks.withLock(
      ProjectId.make("concurrent-project-b"),
      Effect.void,
      Deferred.succeed(projectBEntered, undefined),
    );
    assert.isTrue(Option.isSome(yield* Deferred.poll(projectBEntered)));

    yield* Deferred.succeed(releaseProjectA, undefined);
    yield* Fiber.join(projectA);
    assert.equal(yield* locks.size, 2);
  }),
);
