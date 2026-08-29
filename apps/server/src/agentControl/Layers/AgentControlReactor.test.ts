import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlInitialPlanningFinalizer } from "../initialPlanning/Services/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlImplementationAdmission } from "../implementationAdmission/Services/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationStageStarter } from "../implementationTurn/Services/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationTurnCoordinator } from "../implementationTurn/Services/AgentControlImplementationTurnCoordinator.ts";
import {
  AgentControlTaskIntakeReactor,
  AgentControlTaskIntakeStartupError,
} from "../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlReactor } from "../Services/AgentControlReactor.ts";
import { AgentControlImplementationStageFinalizer } from "../implementationTurn/Services/AgentControlImplementationStageFinalizer.ts";
import { AgentControlVerificationAdmission } from "../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationStageStarter } from "../verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationTurnCoordinator } from "../verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationEvaluator } from "../verificationTurn/Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationStageFinalizer } from "../verificationTurn/Services/AgentControlVerificationStageFinalizer.ts";
import { layer as AgentControlReactorLive } from "./AgentControlReactor.ts";
import { makeReactorStartupAttempt } from "../../reactorStartupActivation.ts";

const evaluatorStubLayer = Layer.succeed(
  AgentControlVerificationEvaluator,
  AgentControlVerificationEvaluator.of({
    processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
    recover: Effect.void,
    prepare: () => Effect.void,
    drain: Effect.void,
  }),
);
const finalizerStubLayer = Layer.succeed(
  AgentControlVerificationStageFinalizer,
  AgentControlVerificationStageFinalizer.of({
    processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
    recover: Effect.void,
    prepare: () => Effect.void,
    drain: Effect.void,
  }),
);
const layer = AgentControlReactorLive.pipe(
  Layer.provide(Layer.merge(evaluatorStubLayer, finalizerStubLayer)),
);

it.effect("fails the relevant reactor composition visibly when the evaluator layer is absent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const dependenciesWithoutEvaluator = Layer.mergeAll(
        Layer.succeed(AgentControlGithubObserveReactor, {} as never),
        Layer.succeed(AgentControlTaskIntakeReactor, {} as never),
        Layer.succeed(AgentControlInitialPlanningFinalizer, {} as never),
        Layer.succeed(AgentControlImplementationAdmission, {} as never),
        Layer.succeed(AgentControlImplementationTurnCoordinator, {} as never),
        Layer.succeed(AgentControlImplementationStageStarter, {} as never),
        Layer.succeed(AgentControlImplementationStageFinalizer, {} as never),
        Layer.succeed(AgentControlVerificationAdmission, {} as never),
        Layer.succeed(AgentControlVerificationStageStarter, {} as never),
        Layer.succeed(AgentControlVerificationTurnCoordinator, {} as never),
        Layer.succeed(AgentControlVerificationStageFinalizer, {} as never),
      );
      const incomplete = AgentControlReactorLive.pipe(Layer.provide(dependenciesWithoutEvaluator));
      const missing = yield* Effect.exit(
        Layer.build(incomplete).pipe(
          Effect.provide(Context.empty() as Context.Context<AgentControlVerificationEvaluator>),
        ),
      );
      assert.isTrue(Exit.isFailure(missing));
      if (Exit.isFailure(missing)) assert.isTrue(Cause.hasDies(missing.cause));

      const complete = incomplete.pipe(Layer.provide(evaluatorStubLayer));
      const context = yield* Layer.build(complete);
      assert.isDefined(Context.get(context, AgentControlReactor));
    }),
  ),
);

it.effect("starts Verification consumers before Admission and cleans them in reverse order", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const lifecycle = yield* Ref.make<ReadonlyArray<string>>([]);
      const record = (entry: string) => Ref.update(lifecycle, (entries) => [...entries, entry]);
      const reactorLayer = AgentControlReactorLive.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () => Effect.void,
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () => Effect.void,
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlImplementationStageFinalizer,
              AgentControlImplementationStageFinalizer.of({
                processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                recover: Effect.void,
                start: () =>
                  record("implementation-start").pipe(
                    Effect.andThen(Effect.addFinalizer(() => record("implementation-cleanup"))),
                  ),
                drain: Effect.void,
                streamPublications: Stream.never,
                subscribePublications: Effect.succeed(Stream.never),
              }),
            ),
            Layer.succeed(
              AgentControlVerificationStageStarter,
              AgentControlVerificationStageStarter.of({
                processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                recover: Effect.void,
                prepare: () =>
                  record("verification-stage-starter-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-stage-starter-cleanup")),
                    ),
                  ),
                start: () =>
                  record("verification-stage-starter-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-stage-starter-cleanup")),
                    ),
                  ),
                drain: Effect.void,
              }),
            ),
            Layer.succeed(
              AgentControlVerificationTurnCoordinator,
              AgentControlVerificationTurnCoordinator.of({
                processHandoff: () => Effect.succeed({ _tag: "NotCandidate" }),
                recover: Effect.void,
                prepare: () =>
                  record("verification-coordinator-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-coordinator-cleanup")),
                    ),
                  ),
                start: () =>
                  record("verification-coordinator-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-coordinator-cleanup")),
                    ),
                  ),
                drain: Effect.void,
                streamPublications: Stream.never,
              }),
            ),
            Layer.succeed(
              AgentControlVerificationEvaluator,
              AgentControlVerificationEvaluator.of({
                processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                recover: Effect.void,
                prepare: () =>
                  record("verification-evaluator-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-evaluator-cleanup")),
                    ),
                  ),
                drain: Effect.void,
              }),
            ),
            Layer.succeed(
              AgentControlVerificationStageFinalizer,
              AgentControlVerificationStageFinalizer.of({
                processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                recover: Effect.void,
                prepare: () =>
                  record("verification-finalizer-start").pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => record("verification-finalizer-cleanup")),
                    ),
                  ),
                drain: Effect.void,
              }),
            ),
            Layer.succeed(
              AgentControlVerificationAdmission,
              AgentControlVerificationAdmission.of({
                processResultEvidence: () => Effect.succeed({ _tag: "NotCandidate" }),
                recover: Effect.void,
                start: () =>
                  record("verification-start").pipe(
                    Effect.andThen(Effect.addFinalizer(() => record("verification-cleanup"))),
                  ),
                drain: Effect.void,
                streamPublications: Stream.never,
                subscribePublications: Effect.succeed(Stream.never),
                loadAcceptedEvidence: () => Effect.succeed(Option.none()),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const reactorScope = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(reactorScope));
      assert.deepStrictEqual(yield* Ref.get(lifecycle), [
        "implementation-start",
        "verification-stage-starter-start",
        "verification-coordinator-start",
        "verification-evaluator-start",
        "verification-finalizer-start",
        "verification-start",
      ]);
      yield* Scope.close(reactorScope, Exit.void);
      assert.deepStrictEqual(yield* Ref.get(lifecycle), [
        "implementation-start",
        "verification-stage-starter-start",
        "verification-coordinator-start",
        "verification-evaluator-start",
        "verification-finalizer-start",
        "verification-start",
        "verification-cleanup",
        "verification-finalizer-cleanup",
        "verification-evaluator-cleanup",
        "verification-coordinator-cleanup",
        "verification-stage-starter-cleanup",
        "implementation-cleanup",
      ]);
    }),
  ),
);

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

it.effect("cleans a defective child close before permitting a new owner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const githubAcquires = yield* Ref.make(0);
      const githubReleases = yield* Ref.make(0);
      const taskAcquires = yield* Ref.make(0);
      const taskReleases = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(githubAcquires, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() =>
                        Ref.getAndUpdate(githubReleases, (count) => count + 1).pipe(
                          Effect.flatMap((release) =>
                            release === 0 ? Effect.die("github-close-defect") : Effect.void,
                          ),
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
                start: () =>
                  Ref.update(taskAcquires, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() => Ref.update(taskReleases, (count) => count + 1)),
                    ),
                  ),
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const ownerA = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(ownerA));

      const ownerAClose = yield* Effect.exit(Scope.close(ownerA, Exit.void));
      assert.isTrue(Exit.isFailure(ownerAClose));
      if (Exit.isFailure(ownerAClose)) assert.isTrue(Cause.hasDies(ownerAClose.cause));
      assert.equal(yield* Ref.get(githubReleases), 1);
      assert.equal(yield* Ref.get(taskReleases), 1);

      const ownerB = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(ownerB));
      assert.equal(yield* Ref.get(githubAcquires), 2);
      assert.equal(yield* Ref.get(taskAcquires), 2);

      const ownerBClose = yield* Effect.exit(Scope.close(ownerB, Exit.void));
      assert.isTrue(Exit.isSuccess(ownerBClose));
      assert.equal(yield* Ref.get(githubReleases), 2);
      assert.equal(yield* Ref.get(taskReleases), 2);
    }),
  ),
);

it.effect("combines a startup failure with a rollback defect and permits a clean retry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const githubAcquires = yield* Ref.make(0);
      const githubReleases = yield* Ref.make(0);
      const taskAcquires = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () =>
                  Ref.update(githubAcquires, (count) => count + 1).pipe(
                    Effect.andThen(
                      Effect.addFinalizer(() =>
                        Ref.getAndUpdate(githubReleases, (count) => count + 1).pipe(
                          Effect.flatMap((release) =>
                            release === 0 ? Effect.die("startup-rollback-defect") : Effect.void,
                          ),
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
                start: () =>
                  Ref.getAndUpdate(taskAcquires, (count) => count + 1).pipe(
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
      const failedOwner = yield* Scope.make("sequential");
      const failedStart = yield* Effect.exit(reactor.start().pipe(Scope.provide(failedOwner)));
      assert.isTrue(Exit.isFailure(failedStart));
      if (Exit.isFailure(failedStart)) {
        assert.isTrue(Cause.hasFails(failedStart.cause));
        assert.isTrue(Cause.hasDies(failedStart.cause));
      }
      assert.equal(yield* Ref.get(githubReleases), 1);
      yield* Scope.close(failedOwner, Exit.void);

      const retryOwner = yield* Scope.make("sequential");
      yield* reactor.start().pipe(Scope.provide(retryOwner));
      assert.equal(yield* Ref.get(githubAcquires), 2);
      assert.equal(yield* Ref.get(taskAcquires), 2);
      const retryClose = yield* Effect.exit(Scope.close(retryOwner, Exit.void));
      assert.isTrue(Exit.isSuccess(retryClose));
      assert.equal(yield* Ref.get(githubReleases), 2);
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

it.effect("does not return the Agent Control lifecycle to idle after shared cutover", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);
      const reactorLayer = layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              AgentControlGithubObserveReactor,
              AgentControlGithubObserveReactor.of({
                start: () => Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1)),
                getStatus: () => Effect.die("unused"),
              }),
            ),
            Layer.succeed(
              AgentControlTaskIntakeReactor,
              AgentControlTaskIntakeReactor.of({
                start: () => Effect.void,
                getStatus: () => Effect.die("unused"),
              }),
            ),
          ),
        ),
      );
      const reactor = yield* AgentControlReactor.pipe(Effect.provide(reactorLayer));
      const resourcesScope = yield* Scope.make("sequential");
      const attempt = yield* makeReactorStartupAttempt(resourcesScope);
      yield* reactor.start(attempt.activation).pipe(Scope.provide(resourcesScope));
      yield* attempt.commit(attempt.activation.open);
      yield* attempt.close(Exit.void);
      assert.equal(yield* Ref.get(finalized), 1);

      const retryScope = yield* Scope.make("sequential");
      const retry = yield* Effect.result(reactor.start().pipe(Scope.provide(retryScope)));
      assert.equal(retry._tag, "Failure");
      if (retry._tag === "Failure") {
        assert.equal(retry.failure._tag, "AgentControlReactorStartupError");
        assert.equal(retry.failure.reason, "lifecycle-closed");
      }
      yield* Scope.close(retryScope, Exit.void);
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
