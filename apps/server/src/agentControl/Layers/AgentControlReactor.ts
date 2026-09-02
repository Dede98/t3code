import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlInitialPlanningFinalizer } from "../initialPlanning/Services/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlImplementationAdmission } from "../implementationAdmission/Services/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationTurnCoordinator } from "../implementationTurn/Services/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlImplementationStageStarter } from "../implementationTurn/Services/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationStageFinalizer } from "../implementationTurn/Services/AgentControlImplementationStageFinalizer.ts";
import { AgentControlVerificationAdmission } from "../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationStageStarter } from "../verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationEvaluator } from "../verificationTurn/Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationStageFinalizer } from "../verificationTurn/Services/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationTurnCoordinator } from "../verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlTaskIntakeReactor } from "../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskVerificationFinalizer } from "../task/Services/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlRunOnceController } from "../runOnce/Services/AgentControlRunOnceController.ts";
import { AgentControlArmedScheduler } from "../armed/Services/AgentControlArmedScheduler.ts";
import {
  AgentControlReactor,
  AgentControlReactorStartupError,
  type AgentControlReactorShape,
} from "../Services/AgentControlReactor.ts";
import { alreadyActivated, type ReactorStartupActivation } from "../../reactorStartupActivation.ts";

const make = Effect.gen(function* () {
  const githubObserve = yield* AgentControlGithubObserveReactor;
  const taskIntake = yield* AgentControlTaskIntakeReactor;
  const taskVerificationFinalizer = yield* AgentControlTaskVerificationFinalizer;
  const runOnce = yield* AgentControlRunOnceController;
  const armed = yield* AgentControlArmedScheduler;
  const initialPlanningFinalizer = yield* AgentControlInitialPlanningFinalizer;
  const implementationAdmission = yield* AgentControlImplementationAdmission;
  const implementationTurnCoordinator = yield* AgentControlImplementationTurnCoordinator;
  const implementationStageStarter = yield* AgentControlImplementationStageStarter;
  const implementationStageFinalizer = yield* AgentControlImplementationStageFinalizer;
  const verificationAdmission = yield* AgentControlVerificationAdmission;
  const verificationStageStarter = yield* AgentControlVerificationStageStarter;
  const verificationEvaluator = yield* AgentControlVerificationEvaluator;
  const verificationStageFinalizer = yield* AgentControlVerificationStageFinalizer;
  const verificationTurnCoordinator = yield* AgentControlVerificationTurnCoordinator;
  const lifecycleSemaphore = yield* Semaphore.make(1);
  let nextAttemptId = 0;
  let lifecycleState: "idle" | "starting" | "started" | "closing" | "closed" = "idle";
  interface ActiveAttempt {
    readonly id: number;
    readonly ownerScope: Scope.Scope;
    readonly scope: Scope.Closeable;
    readonly activation: ReactorStartupActivation;
  }
  let activeAttempt: ActiveAttempt | null = null;

  const closeAttempt = (
    attemptId: number,
    ownerScope: Scope.Scope,
    exit: Exit.Exit<unknown, unknown>,
    terminal = false,
  ) =>
    lifecycleSemaphore.withPermits(1)(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (
            lifecycleState !== "started" ||
            activeAttempt?.id !== attemptId ||
            activeAttempt.ownerScope !== ownerScope
          ) {
            return;
          }
          lifecycleState = "closing";
          const closeDisposition = yield* activeAttempt.activation.closeDisposition;
          const closeExit = yield* Effect.exit(Scope.close(activeAttempt.scope, exit));
          if (activeAttempt?.id === attemptId && activeAttempt.ownerScope === ownerScope) {
            activeAttempt = null;
            lifecycleState = terminal || closeDisposition !== "retryable" ? "closed" : "idle";
          }
          if (Exit.isFailure(closeExit)) return yield* Effect.failCause(closeExit.cause);
        }),
      ),
    );

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(
    (activation = alreadyActivated) =>
      Effect.gen(function* () {
        const ownerScope = yield* Effect.scope;
        yield* Effect.acquireRelease(
          lifecycleSemaphore.withPermits(1)(
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                if (lifecycleState === "closed") {
                  return yield* new AgentControlReactorStartupError({
                    reason: "lifecycle-closed",
                  });
                }
                if (lifecycleState === "started" && activeAttempt !== null) {
                  if (activeAttempt.ownerScope === ownerScope) return null as ActiveAttempt | null;
                  return yield* new AgentControlReactorStartupError({
                    reason: "already-started-different-scope",
                  });
                }

                nextAttemptId += 1;
                const attemptId = nextAttemptId;
                const attemptScope = yield* Scope.make("sequential");
                const attempt = { id: attemptId, ownerScope, scope: attemptScope, activation };
                activeAttempt = attempt;
                lifecycleState = "starting";
                const started = yield* Effect.exit(
                  restore(
                    Effect.gen(function* () {
                      yield* githubObserve.start();
                      yield* taskIntake.start();
                      yield* initialPlanningFinalizer.start();
                      yield* implementationAdmission.start();
                      yield* implementationTurnCoordinator.start();
                      yield* implementationStageStarter.start();
                      yield* implementationStageFinalizer.start();
                      yield* verificationStageStarter.prepare(activation.await);
                      yield* verificationTurnCoordinator.prepare(activation.await);
                      yield* verificationEvaluator.prepare(activation.await);
                      yield* verificationStageFinalizer.prepare(activation.await);
                      yield* taskVerificationFinalizer.prepare(activation.await);
                      // Subscribe before the shared activation opens, then recover
                      // durable run-once state before normal command readiness.
                      yield* runOnce.prepare(activation);
                      // Armed subscribes before its catch-up and completes durable recovery
                      // before this top-level startup attempt becomes ready.
                      yield* armed.prepare(activation);
                      yield* verificationAdmission.start();
                    }).pipe(Scope.provide(attemptScope)),
                  ),
                );
                if (Exit.isFailure(started)) {
                  const closeExit = yield* Effect.exit(Scope.close(attemptScope, started));
                  if (activeAttempt?.id === attemptId) {
                    activeAttempt = null;
                    lifecycleState = "idle";
                  }
                  return yield* Effect.failCause(
                    Exit.isFailure(closeExit)
                      ? Cause.combine(started.cause, closeExit.cause)
                      : started.cause,
                  );
                }
                lifecycleState = "started";
                yield* Effect.flip(armed.awaitFailure).pipe(
                  Effect.flatMap((failure) =>
                    closeAttempt(attemptId, ownerScope, Exit.fail(failure), true),
                  ),
                  Effect.forkIn(ownerScope, { startImmediately: true }),
                );
                return attempt as ActiveAttempt | null;
              }),
            ),
          ),
          (attempt, exit) =>
            attempt === null ? Effect.void : closeAttempt(attempt.id, ownerScope, exit),
          { interruptible: true },
        );
      }),
  );

  return AgentControlReactor.of({ awaitFailure: armed.awaitFailure, start });
});

export const layer = Layer.effect(AgentControlReactor, make);
