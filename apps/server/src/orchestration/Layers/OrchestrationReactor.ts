import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import {
  OrchestrationReactor,
  OrchestrationReactorStartupError,
  type OrchestrationReactorShape,
} from "../Services/OrchestrationReactor.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import * as AgentAwarenessRelay from "../../relay/AgentAwarenessRelay.ts";
import { AgentControlInitialPlanningConsumer } from "../../agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumer.ts";
import { AgentControlImplementationTurnConsumer } from "../../agentControl/implementationTurn/Services/AgentControlImplementationTurnConsumer.ts";
import {
  AgentControlVerificationTurnConsumer,
  type AgentControlVerificationTurnConsumerActivation,
} from "../../agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";

interface ActiveAttempt {
  readonly id: number;
  readonly ownerScope: Scope.Scope;
  readonly scope: Scope.Closeable;
  readonly startCompletion: Deferred.Deferred<void, OrchestrationReactorStartupError>;
  readonly commitCompletion: Deferred.Deferred<void, OrchestrationReactorStartupError>;
  verificationActivation: AgentControlVerificationTurnConsumerActivation | undefined;
  providerBarrierOpened: boolean;
}

type LifecycleState =
  | { readonly _tag: "idle" }
  | { readonly _tag: "starting"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "prepared"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "committing"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "started"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "commit-failed"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "closing"; readonly attempt: ActiveAttempt }
  | { readonly _tag: "closed" };

const lifecycleError = (reason: OrchestrationReactorStartupError["reason"]) =>
  new OrchestrationReactorStartupError({ reason });

export const makeOrchestrationReactor = Effect.gen(function* () {
  const providerRuntimeIngestion = yield* ProviderRuntimeIngestionService;
  const providerCommandReactor = yield* ProviderCommandReactor;
  const checkpointReactor = yield* CheckpointReactor;
  const threadDeletionReactor = yield* ThreadDeletionReactor;
  const agentAwarenessRelay = yield* AgentAwarenessRelay.AgentAwarenessRelay;
  const initialPlanningConsumer = yield* AgentControlInitialPlanningConsumer;
  const implementationTurnConsumer = yield* AgentControlImplementationTurnConsumer;
  const verificationTurnConsumer = yield* AgentControlVerificationTurnConsumer;
  const lifecycleSemaphore = yield* Semaphore.make(1);
  let lifecycleState: LifecycleState = { _tag: "idle" };
  let nextAttemptId = 0;

  const hasAttempt = (
    state: LifecycleState,
  ): state is Exclude<LifecycleState, { readonly _tag: "idle" } | { readonly _tag: "closed" }> =>
    "attempt" in state;

  const closeAttempt = (
    attempt: ActiveAttempt,
    exit: Exit.Exit<unknown, unknown>,
  ): Effect.Effect<void> =>
    lifecycleSemaphore.withPermits(1)(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (!hasAttempt(lifecycleState) || lifecycleState.attempt.id !== attempt.id) return;
          lifecycleState = { _tag: "closing", attempt };
          const closeExit = yield* Effect.exit(Scope.close(attempt.scope, exit));
          lifecycleState = attempt.providerBarrierOpened ? { _tag: "closed" } : { _tag: "idle" };
          if (Exit.isFailure(closeExit)) return yield* Effect.failCause(closeExit.cause);
        }),
      ),
    );

  const runStartAttempt = (attempt: ActiveAttempt) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const startupExit = yield* Effect.exit(
          restore(
            Effect.gen(function* () {
              const [runtimeIngestionEvents, verificationEvents] = yield* Effect.all(
                [
                  providerRuntimeIngestion.subscribeProviderEvents,
                  verificationTurnConsumer.subscribeProviderEvents,
                ],
                { concurrency: "unbounded" },
              );
              yield* providerRuntimeIngestion.startProviderRuntimeEventSources;
              yield* providerRuntimeIngestion.start(runtimeIngestionEvents);
              attempt.verificationActivation =
                yield* verificationTurnConsumer.prepare(verificationEvents);
              yield* providerCommandReactor.start();
              yield* checkpointReactor.start();
              yield* threadDeletionReactor.start();
              yield* agentAwarenessRelay.start();
              yield* initialPlanningConsumer.start();
              yield* implementationTurnConsumer.start();
            }).pipe(Scope.provide(attempt.scope)),
          ),
        );

        if (Exit.isFailure(startupExit)) {
          const closeExit = yield* Effect.exit(Scope.close(attempt.scope, startupExit));
          const cause = Exit.isFailure(closeExit)
            ? Cause.combine(startupExit.cause, closeExit.cause)
            : startupExit.cause;
          yield* lifecycleSemaphore.withPermits(1)(
            Effect.gen(function* () {
              if (hasAttempt(lifecycleState) && lifecycleState.attempt.id === attempt.id) {
                lifecycleState = { _tag: "idle" };
              }
              yield* Deferred.failCause(attempt.startCompletion, cause);
            }),
          );
          return yield* Effect.failCause(cause);
        }

        yield* lifecycleSemaphore.withPermits(1)(
          Effect.gen(function* () {
            if (lifecycleState._tag === "starting" && lifecycleState.attempt.id === attempt.id) {
              lifecycleState = { _tag: "prepared", attempt };
            }
            yield* Deferred.succeed(attempt.startCompletion, undefined);
          }),
        );
        return attempt;
      }),
    );

  const start: OrchestrationReactorShape["start"] = Effect.fn("OrchestrationReactor.start")(() =>
    Effect.gen(function* () {
      const ownerScope = yield* Scope.Scope;
      yield* Effect.acquireRelease(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const decision = yield* lifecycleSemaphore.withPermits(1)(
              Effect.gen(function* () {
                if (lifecycleState._tag === "closed") {
                  return yield* lifecycleError("lifecycle-closed");
                }
                if (lifecycleState._tag === "idle") {
                  nextAttemptId += 1;
                  const attempt: ActiveAttempt = {
                    id: nextAttemptId,
                    ownerScope,
                    scope: yield* Scope.make("sequential"),
                    startCompletion: yield* Deferred.make<void, OrchestrationReactorStartupError>(),
                    commitCompletion: yield* Deferred.make<
                      void,
                      OrchestrationReactorStartupError
                    >(),
                    verificationActivation: undefined,
                    providerBarrierOpened: false,
                  };
                  lifecycleState = { _tag: "starting", attempt };
                  return { _tag: "run" as const, attempt };
                }
                const attempt = lifecycleState.attempt;
                if (attempt.ownerScope !== ownerScope) {
                  return yield* lifecycleError("already-started-different-scope");
                }
                return lifecycleState._tag === "starting"
                  ? { _tag: "wait" as const, attempt }
                  : { _tag: "ready" as const };
              }),
            );

            if (decision._tag === "ready") return null;
            if (decision._tag === "wait") {
              yield* restore(Deferred.await(decision.attempt.startCompletion));
              return null;
            }
            return yield* runStartAttempt(decision.attempt);
          }),
        ),
        (attempt, exit) => (attempt === null ? Effect.void : closeAttempt(attempt, exit)),
        { interruptible: true },
      );
    }),
  );

  const commit: OrchestrationReactorShape["commit"] = Effect.fn("OrchestrationReactor.commit")(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const ownerScope = yield* Scope.Scope;
        const decision = yield* lifecycleSemaphore.withPermits(1)(
          Effect.gen(function* () {
            if (lifecycleState._tag === "closed") {
              return yield* lifecycleError("lifecycle-closed");
            }
            if (lifecycleState._tag === "idle") {
              return yield* lifecycleError("commit-before-start");
            }
            const attempt = lifecycleState.attempt;
            if (attempt.ownerScope !== ownerScope) {
              return yield* lifecycleError("already-started-different-scope");
            }
            if (lifecycleState._tag === "starting") {
              return { _tag: "wait-start" as const, attempt };
            }
            if (lifecycleState._tag === "prepared") {
              lifecycleState = { _tag: "committing", attempt };
              return { _tag: "run" as const, attempt };
            }
            if (lifecycleState._tag === "committing") {
              return { _tag: "wait-commit" as const, attempt };
            }
            if (lifecycleState._tag === "commit-failed") {
              return { _tag: "wait-commit" as const, attempt };
            }
            return { _tag: "ready" as const };
          }),
        );

        if (decision._tag === "ready") return;
        if (decision._tag === "wait-start") {
          yield* restore(Deferred.await(decision.attempt.startCompletion));
          return yield* commit();
        }
        if (decision._tag === "wait-commit") {
          return yield* restore(Deferred.await(decision.attempt.commitCompletion));
        }

        const attempt = decision.attempt;
        const commitExit = yield* Effect.exit(
          restore(
            Effect.gen(function* () {
              if (attempt.verificationActivation === undefined) {
                return yield* Effect.die(
                  new Error("Verification recovery activation was not prepared."),
                );
              }
              yield* providerRuntimeIngestion.openProviderRuntimeEventPublishing;
              attempt.providerBarrierOpened = true;
              yield* attempt.verificationActivation.commit;
            }),
          ),
        );
        yield* lifecycleSemaphore.withPermits(1)(
          Effect.gen(function* () {
            if (lifecycleState._tag === "committing" && lifecycleState.attempt.id === attempt.id) {
              lifecycleState = Exit.isSuccess(commitExit)
                ? { _tag: "started", attempt }
                : { _tag: "commit-failed", attempt };
            }
            yield* Deferred.done(attempt.commitCompletion, commitExit);
          }),
        );
        if (Exit.isFailure(commitExit)) return yield* Effect.failCause(commitExit.cause);
      }),
    ),
  );

  return {
    start,
    commit,
  } satisfies OrchestrationReactorShape;
});

export const OrchestrationReactorLive = Layer.effect(
  OrchestrationReactor,
  makeOrchestrationReactor,
);
