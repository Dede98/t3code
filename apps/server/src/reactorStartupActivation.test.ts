import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import { makeReactorStartupAttempt } from "./reactorStartupActivation.ts";

const makeOwnedChildScope = Effect.fn("makeOwnedChildScope")(function* (
  ownerScope: Scope.Closeable,
  finalize: Effect.Effect<void>,
) {
  const childScope = yield* Scope.make("sequential");
  yield* Scope.addFinalizerExit(ownerScope, (exit) => Scope.close(childScope, exit));
  yield* Scope.addFinalizer(childScope, finalize);
  return childScope;
});

it.effect("lets close win before cutover and permits a fresh complete retry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const finalized = yield* Ref.make<ReadonlyArray<string>>([]);
      const resourcesScope = yield* Scope.make("sequential");
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(finalized, (entries) => [...entries, "orchestration"]),
      );
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(finalized, (entries) => [...entries, "agent-control"]),
      );
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      const activationWaiter = yield* attempt.activation.await.pipe(Effect.forkChild);
      const barrierOpens = yield* Ref.make(0);

      yield* attempt.close(Exit.interrupt("parent-shutdown" as never));
      assert.deepStrictEqual(yield* Ref.get(finalized), ["agent-control", "orchestration"]);
      assert.equal(activationWaiter.pollUnsafe(), undefined);
      const commitExit = yield* Effect.exit(
        attempt.commit(
          Ref.update(barrierOpens, (count) => count + 1).pipe(
            Effect.andThen(attempt.activation.open),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(commitExit));
      if (Exit.isFailure(commitExit)) {
        assert.isTrue(
          commitExit.cause.reasons.some(
            (reason) =>
              Cause.isFailReason(reason) &&
              reason.error._tag === "ReactorStartupAttemptError" &&
              reason.error.reason === "attempt-closed",
          ),
        );
      }
      assert.equal(yield* Ref.get(barrierOpens), 0);
      yield* Fiber.interrupt(activationWaiter);

      const retryResources = yield* Scope.make("sequential");
      const retryFinalized = yield* Ref.make(0);
      yield* makeOwnedChildScope(
        retryResources,
        Ref.update(retryFinalized, (count) => count + 1),
      );
      const retry = yield* makeReactorStartupAttempt(retryResources);
      yield* retry.commit(
        Ref.update(barrierOpens, (count) => count + 1).pipe(Effect.andThen(retry.activation.open)),
      );
      yield* retry.activation.await;
      yield* retry.close(Exit.void);
      assert.equal(yield* Ref.get(barrierOpens), 1);
      assert.equal(yield* Ref.get(retryFinalized), 1);
    }),
  ),
);

it.effect.each(["before-barrier", "after-barrier", "after-gate"] as const)(
  "serializes parent close at the %s cutover phase",
  (phase) =>
    Effect.scoped(
      Effect.gen(function* () {
        const resourcesScope = yield* Scope.make("sequential");
        const finalized = yield* Ref.make<ReadonlyArray<string>>([]);
        yield* makeOwnedChildScope(
          resourcesScope,
          Ref.update(finalized, (entries) => [...entries, "orchestration"]),
        );
        yield* makeOwnedChildScope(
          resourcesScope,
          Ref.update(finalized, (entries) => [...entries, "agent-control"]),
        );
        const attempt = yield* makeReactorStartupAttempt(resourcesScope);
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const barrierOpens = yield* Ref.make(0);
        const activationObserved = yield* Deferred.make<void>();
        yield* attempt.activation.await.pipe(
          Effect.andThen(Deferred.succeed(activationObserved, undefined)),
          Effect.forkChild({ startImmediately: true }),
        );
        const pause = (at: typeof phase) =>
          phase === at
            ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void;
        const commitFiber = yield* attempt
          .commit(
            Effect.gen(function* () {
              yield* pause("before-barrier");
              yield* Ref.update(barrierOpens, (count) => count + 1);
              yield* pause("after-barrier");
              yield* attempt.activation.open;
              yield* pause("after-gate");
            }),
          )
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(reached);
        const closeFiber = yield* attempt
          .close(Exit.interrupt(`shutdown-${phase}` as never))
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;

        assert.equal(closeFiber.pollUnsafe(), undefined);
        assert.deepStrictEqual(yield* Ref.get(finalized), []);
        assert.equal(yield* Ref.get(barrierOpens), phase === "before-barrier" ? 0 : 1);
        assert.equal(yield* Deferred.isDone(activationObserved), phase === "after-gate");

        yield* Deferred.succeed(release, undefined);
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(commitFiber)));
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(closeFiber)));
        assert.deepStrictEqual(yield* Ref.get(finalized), ["agent-control", "orchestration"]);
        assert.equal(yield* Ref.get(barrierOpens), 1);
        assert.isTrue(yield* Deferred.isDone(activationObserved));
      }),
    ),
);

it.effect("shares one cutover across two commits and makes shutdown wait for it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const resourcesScope = yield* Scope.make("sequential");
      const finalized = yield* Ref.make(0);
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(finalized, (count) => count + 1),
      );
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cutovers = yield* Ref.make(0);
      const first = yield* attempt
        .commit(
          Ref.update(cutovers, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(entered, undefined)),
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(attempt.activation.open),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(entered);
      const second = yield* attempt
        .commit(Effect.die("a committed cutover must not run twice"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      const close = yield* attempt
        .close(Exit.interrupt("concurrent-parent-shutdown" as never))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(finalized), 0);

      yield* Deferred.succeed(release, undefined);
      for (const fiber of [first, second, close]) {
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(fiber)));
      }
      assert.equal(yield* Ref.get(cutovers), 1);
      assert.equal(yield* Ref.get(finalized), 1);
    }),
  ),
);

it.effect("does not let a blocked Agent Control finalizer overtake cutover", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const resourcesScope = yield* Scope.make("sequential");
      const orchestrationFinalized = yield* Ref.make(0);
      const agentFinalizerEntered = yield* Deferred.make<void>();
      const releaseAgentFinalizer = yield* Deferred.make<void>();
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(orchestrationFinalized, (count) => count + 1),
      );
      yield* makeOwnedChildScope(
        resourcesScope,
        Deferred.succeed(agentFinalizerEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseAgentFinalizer)),
        ),
      );
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      const cutoverEntered = yield* Deferred.make<void>();
      const releaseCutover = yield* Deferred.make<void>();
      const commit = yield* attempt
        .commit(
          Deferred.succeed(cutoverEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCutover)),
            Effect.andThen(attempt.activation.open),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cutoverEntered);
      const close = yield* attempt
        .close(Exit.void)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      assert.isFalse(yield* Deferred.isDone(agentFinalizerEntered));
      assert.equal(yield* Ref.get(orchestrationFinalized), 0);

      yield* Deferred.succeed(releaseCutover, undefined);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(commit)));
      yield* Deferred.await(agentFinalizerEntered);
      assert.equal(close.pollUnsafe(), undefined);
      assert.equal(yield* Ref.get(orchestrationFinalized), 0);
      yield* Deferred.succeed(releaseAgentFinalizer, undefined);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(close)));
      assert.equal(yield* Ref.get(orchestrationFinalized), 1);
    }),
  ),
);

it.effect("drains once before finalizers and shares completion with parallel close waiters", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const resourcesScope = yield* Scope.make("sequential");
      const finalized = yield* Ref.make(0);
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(finalized, (count) => count + 1),
      );
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      const drainEntered = yield* Deferred.make<void>();
      const releaseDrain = yield* Deferred.make<void>();
      const drainRuns = yield* Ref.make(0);
      assert.isTrue(
        yield* attempt.activation.registerShutdownDrain(
          Ref.update(drainRuns, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(drainEntered, undefined)),
            Effect.andThen(Deferred.await(releaseDrain)),
          ),
        ),
      );
      const commitEntered = yield* Deferred.make<void>();
      const releaseCommit = yield* Deferred.make<void>();
      const commit = yield* attempt
        .commit(
          Deferred.succeed(commitEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCommit)),
            Effect.andThen(attempt.activation.open),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(commitEntered);

      const closes = [
        yield* attempt
          .close(Exit.interrupt("first-parent" as never))
          .pipe(Effect.forkChild({ startImmediately: true })),
        yield* attempt
          .close(Exit.interrupt("second-parent" as never))
          .pipe(Effect.forkChild({ startImmediately: true })),
        yield* attempt
          .close(Exit.interrupt("third-parent" as never))
          .pipe(Effect.forkChild({ startImmediately: true })),
      ];
      yield* Deferred.succeed(releaseCommit, undefined);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(commit)));
      yield* Deferred.await(drainEntered);
      assert.equal(yield* Ref.get(finalized), 0);
      assert.equal(yield* Ref.get(drainRuns), 1);
      for (const close of closes) assert.isUndefined(close.pollUnsafe());

      yield* Deferred.succeed(releaseDrain, undefined);
      for (const close of closes) assert.isTrue(Exit.isSuccess(yield* Fiber.await(close)));
      assert.equal(yield* Ref.get(finalized), 1);
      assert.equal(yield* Ref.get(drainRuns), 1);
    }),
  ),
);

it.effect("combines a shutdown-drain defect with cleanup failure for every close waiter", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const resourcesScope = yield* Scope.make("sequential");
      const drainDefect = new Error("startup-shutdown-drain-defect");
      const cleanupDefect = new Error("startup-shutdown-cleanup-defect");
      yield* Scope.addFinalizer(resourcesScope, Effect.die(cleanupDefect));
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      yield* attempt.activation.registerShutdownDrain(Effect.die(drainDefect));
      yield* attempt.commit(attempt.activation.open);

      for (const close of [attempt.close(Exit.void), attempt.close(Exit.void)]) {
        const exit = yield* Effect.exit(close);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === drainDefect,
            ),
          );
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === cleanupDefect,
            ),
          );
        }
      }
    }),
  ),
);

it.effect("keeps a post-cutover defect terminal and observable after close", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const resourcesScope = yield* Scope.make("sequential");
      const finalized = yield* Ref.make(0);
      yield* makeOwnedChildScope(
        resourcesScope,
        Ref.update(finalized, (count) => count + 1),
      );
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      const defect = new Error("post-cutover-terminal-defect");
      const first = yield* Effect.exit(
        attempt.commit(attempt.activation.open.pipe(Effect.andThen(Effect.die(defect)))),
      );
      assert.isTrue(Exit.isFailure(first));
      assert.equal(yield* attempt.activation.closeDisposition, "terminal");
      yield* attempt.close(first);
      assert.equal(yield* Ref.get(finalized), 1);

      const repeated = yield* Effect.exit(attempt.commit(Effect.void));
      assert.isTrue(Exit.isFailure(repeated));
      if (Exit.isFailure(repeated)) {
        assert.isTrue(
          repeated.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === defect,
          ),
        );
      }
    }),
  ),
);
