import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import {
  AgentControlTaskIntakeReactor,
  AgentControlTaskIntakeStartupError,
} from "../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlReactor } from "../Services/AgentControlReactor.ts";
import { layer } from "./AgentControlReactor.ts";

it.effect("starts the GitHub Observe lifecycle inside the caller's scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const starts = yield* Ref.make(0);
      const finalized = yield* Ref.make(false);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(starts, (count) => count + 1).pipe(
                    Effect.andThen(Effect.addFinalizer(() => Ref.set(finalized, true))),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () => Ref.update(starts, (count) => count + 1),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );

      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const reactorScope = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(reactorScope));

      assert.equal(yield* Ref.get(starts), 2);
      assert.isFalse(yield* Ref.get(finalized));
      yield* Scope.close(reactorScope, Exit.void);
      assert.isTrue(yield* Ref.get(finalized));
    }),
  ),
);

it.effect("rolls back a partial startup and permits a clean second attempt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const githubStarts = yield* Ref.make(0);
      const githubFinalizers = yield* Ref.make(0);
      const taskStarts = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(githubStarts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(githubFinalizers, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () =>
                  Ref.getAndUpdate(taskStarts, (count) => count + 1).pipe(
                    Effect.flatMap((attempt) =>
                      attempt === 0
                        ? Effect.fail(
                            new AgentControlTaskIntakeStartupError({
                              reason: "enumeration-failed",
                            }),
                          )
                        : Effect.void,
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));

      const failedScope = yield* Scope.make("sequential");
      const failed = yield* Effect.exit(reactor.start().pipe(Scope.provide(failedScope)));
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(yield* Ref.get(githubFinalizers), 1);
      yield* Scope.close(failedScope, Exit.void);
      assert.equal(yield* Ref.get(githubFinalizers), 1);

      const retryScope = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(retryScope));
      assert.equal(yield* Ref.get(githubStarts), 2);
      assert.equal(yield* Ref.get(taskStarts), 2);
      assert.equal(yield* Ref.get(githubFinalizers), 1);
      yield* Scope.close(retryScope, Exit.void);
      assert.equal(yield* Ref.get(githubFinalizers), 2);
    }),
  ),
);

it.effect("serializes concurrent attempts without finalizing another attempt's scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstTaskEntered = yield* Deferred.make<void>();
      const releaseFirstTask = yield* Deferred.make<void>();
      const githubStarts = yield* Ref.make(0);
      const githubFinalizers = yield* Ref.make(0);
      const taskStarts = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(githubStarts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(githubFinalizers, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () =>
                  Ref.getAndUpdate(taskStarts, (count) => count + 1).pipe(
                    Effect.flatMap((attempt) =>
                      attempt === 0
                        ? Deferred.succeed(firstTaskEntered, undefined).pipe(
                            Effect.andThen(Deferred.await(releaseFirstTask)),
                            Effect.andThen(
                              Effect.fail(
                                new AgentControlTaskIntakeStartupError({
                                  reason: "enumeration-failed",
                                }),
                              ),
                            ),
                          )
                        : Effect.void,
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const firstScope = yield* Scope.make("sequential");
      const secondScope = yield* Scope.make("sequential");
      const first = yield* Effect.exit(reactor.start().pipe(Scope.provide(firstScope))).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(firstTaskEntered);
      const second = yield* reactor.start().pipe(Scope.provide(secondScope), Effect.forkChild);
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(githubStarts), 1);
      assert.equal(yield* Ref.get(taskStarts), 1);

      yield* Deferred.succeed(releaseFirstTask, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(first)));
      yield* Fiber.join(second);
      assert.equal(yield* Ref.get(githubStarts), 2);
      assert.equal(yield* Ref.get(taskStarts), 2);
      assert.equal(yield* Ref.get(githubFinalizers), 1);

      yield* Scope.close(firstScope, Exit.void);
      assert.equal(yield* Ref.get(githubFinalizers), 1);
      yield* Scope.close(secondScope, Exit.void);
      assert.equal(yield* Ref.get(githubFinalizers), 2);
    }),
  ),
);

it.effect("keeps a semaphore waiter interruptible while another startup is blocked", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstTaskEntered = yield* Deferred.make<void>();
      const releaseFirstTask = yield* Deferred.make<void>();
      const starts = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () => Ref.update(starts, (count) => count + 1),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () =>
                  Deferred.succeed(firstTaskEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirstTask)),
                    Effect.andThen(Ref.update(starts, (count) => count + 1)),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const firstScope = yield* Scope.make("sequential");
      const secondScope = yield* Scope.make("sequential");
      const first = yield* reactor.start().pipe(Scope.provide(firstScope), Effect.forkChild);
      yield* Deferred.await(firstTaskEntered);
      const second = yield* reactor.start().pipe(Scope.provide(secondScope), Effect.forkChild);
      yield* Effect.yieldNow;

      yield* Fiber.interrupt(second);
      assert.equal(yield* Ref.get(starts), 1);
      assert.equal(first.pollUnsafe(), undefined);

      yield* Deferred.succeed(releaseFirstTask, undefined);
      yield* Fiber.join(first);
      assert.equal(yield* Ref.get(starts), 2);
      yield* Scope.close(firstScope, Exit.void);
      yield* Scope.close(secondScope, Exit.void);
    }),
  ),
);

it.effect("is idempotent for one owner scope and rejects a different active owner scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const starts = yield* Ref.make(0);
      const finalizers = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(starts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(finalizers, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () =>
                  Ref.update(starts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(finalizers, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const ownerScope = yield* Scope.make("sequential");
      const otherScope = yield* Scope.make("sequential");

      yield* reactor.start().pipe(Scope.provide(ownerScope));
      yield* reactor.start().pipe(Scope.provide(ownerScope));
      assert.equal(yield* Ref.get(starts), 2);

      const other = yield* Effect.result(reactor.start().pipe(Scope.provide(otherScope)));
      assert.equal(other._tag, "Failure");
      if (other._tag === "Failure") {
        assert.equal(other.failure._tag, "AgentControlReactorStartupError");
        assert.equal(other.failure.reason, "already-started-different-scope");
      }
      assert.equal(yield* Ref.get(starts), 2);

      yield* Scope.close(otherScope, Exit.void);
      assert.equal(yield* Ref.get(finalizers), 0);
      yield* Scope.close(ownerScope, Exit.void);
      assert.equal(yield* Ref.get(finalizers), 2);
    }),
  ),
);

it.effect("holds the lifecycle permit until the old child scope has fully closed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cleanupEntered = yield* Deferred.make<void>();
      const releaseCleanup = yield* Deferred.make<void>();
      const starts = yield* Ref.make(0);
      const finalizers = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(starts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(finalizers, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () =>
                  Ref.getAndUpdate(starts, (count) => count + 1).pipe(
                    Effect.flatMap((startCount) =>
                      Effect.addFinalizer(() =>
                        Ref.update(finalizers, (count) => count + 1).pipe(
                          Effect.andThen(
                            startCount === 1
                              ? Deferred.succeed(cleanupEntered, undefined).pipe(
                                  Effect.andThen(Deferred.await(releaseCleanup)),
                                )
                              : Effect.void,
                          ),
                        ),
                      ),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const oldOwner = yield* Scope.make("sequential");
      const newOwner = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(oldOwner));

      const closing = yield* Scope.close(oldOwner, Exit.void).pipe(Effect.forkChild);
      yield* Deferred.await(cleanupEntered);
      const replacement = yield* reactor.start().pipe(Scope.provide(newOwner), Effect.forkChild);
      yield* Effect.yieldNow;
      assert.equal(replacement.pollUnsafe(), undefined);
      assert.equal(yield* Ref.get(starts), 2);

      yield* Deferred.succeed(releaseCleanup, undefined);
      yield* Fiber.join(closing);
      yield* Fiber.join(replacement);
      assert.equal(yield* Ref.get(starts), 4);
      assert.equal(yield* Ref.get(finalizers), 2);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(finalizers), 2);
      yield* Scope.close(newOwner, Exit.void);
      assert.equal(yield* Ref.get(finalizers), 4);
    }),
  ),
);

it.effect("interrupts a caller waiting behind closing without leaking the lifecycle permit", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cleanupEntered = yield* Deferred.make<void>();
      const releaseCleanup = yield* Deferred.make<void>();
      const starts = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(starts, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() =>
                        Deferred.succeed(cleanupEntered, undefined).pipe(
                          Effect.andThen(Deferred.await(releaseCleanup)),
                        ),
                      ),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () => Ref.update(starts, (count) => count + 1),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const oldOwner = yield* Scope.make("sequential");
      const interruptedOwner = yield* Scope.make("sequential");
      const replacementOwner = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(oldOwner));

      const closing = yield* Scope.close(oldOwner, Exit.void).pipe(Effect.forkChild);
      yield* Deferred.await(cleanupEntered);
      const waiting = yield* reactor
        .start()
        .pipe(Scope.provide(interruptedOwner), Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(waiting);
      assert.equal(yield* Ref.get(starts), 2);

      yield* Deferred.succeed(releaseCleanup, undefined);
      yield* Fiber.join(closing);
      yield* reactor.start().pipe(Scope.provide(replacementOwner));
      assert.equal(yield* Ref.get(starts), 4);
      yield* Scope.close(interruptedOwner, Exit.void);
      yield* Scope.close(replacementOwner, Exit.void);
    }),
  ),
);

it.effect(
  "releases the acquired lifecycle permit when startup is interrupted before ownership binds",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const firstStartEntered = yield* Deferred.make<void>();
        const starts = yield* Ref.make(0);
        const reactorLayer = layer.pipe(
          Layer.provide(
            Layer.merge(
              Layer.succeed(
                AgentControlGithubObserveReactor,
                AgentControlGithubObserveReactor.of({
                  start: () =>
                    Ref.getAndUpdate(starts, (count) => count + 1).pipe(
                      Effect.flatMap((attempt) =>
                        attempt === 0
                          ? Deferred.succeed(firstStartEntered, undefined).pipe(
                              Effect.andThen(Effect.never),
                            )
                          : Effect.void,
                      ),
                    ),
                  getStatus: () => Effect.die("unused"),
                }),
              ),
              Layer.succeed(
                AgentControlTaskIntakeReactor,
                AgentControlTaskIntakeReactor.of({
                  start: () => Ref.update(starts, (count) => count + 1),
                  getStatus: () => Effect.die("unused"),
                }),
              ),
            ),
          ),
        );
        const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
        const interruptedOwner = yield* Scope.make("sequential");
        const replacementOwner = yield* Scope.make("sequential");
        const interrupted = yield* reactor
          .start()
          .pipe(Scope.provide(interruptedOwner), Effect.forkChild);
        yield* Deferred.await(firstStartEntered);
        yield* Fiber.interrupt(interrupted);

        yield* reactor.start().pipe(Scope.provide(replacementOwner));
        assert.equal(yield* Ref.get(starts), 3);
        yield* Scope.close(interruptedOwner, Exit.void);
        yield* Scope.close(replacementOwner, Exit.void);
      }),
    ),
);
