import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeReactor } from "../task/Services/AgentControlTaskIntakeReactor.ts";
import {
  AgentControlReactor,
  AgentControlReactorStartupError,
  type AgentControlReactorShape,
} from "../Services/AgentControlReactor.ts";

const make = Effect.gen(function* () {
  const githubObserve = yield* AgentControlGithubObserveReactor;
  const taskIntake = yield* AgentControlTaskIntakeReactor;
  const lifecycleSemaphore = yield* Semaphore.make(1);
  let nextAttemptId = 0;
  let lifecycleState: "idle" | "starting" | "started" | "closing" = "idle";
  interface ActiveAttempt {
    readonly id: number;
    readonly ownerScope: Scope.Scope;
    readonly scope: Scope.Closeable;
  }
  let activeAttempt: ActiveAttempt | null = null;

  const closeAttempt = (
    attemptId: number,
    ownerScope: Scope.Scope,
    exit: Exit.Exit<unknown, unknown>,
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
          yield* Scope.close(activeAttempt.scope, exit).pipe(Effect.ignore);
          if (activeAttempt?.id === attemptId && activeAttempt.ownerScope === ownerScope) {
            activeAttempt = null;
            lifecycleState = "idle";
          }
        }),
      ),
    );

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(() =>
    Effect.gen(function* () {
      const ownerScope = yield* Effect.scope;
      yield* Effect.acquireRelease(
        lifecycleSemaphore.withPermits(1)(
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              if (lifecycleState === "started" && activeAttempt !== null) {
                if (activeAttempt.ownerScope === ownerScope) return null as ActiveAttempt | null;
                return yield* new AgentControlReactorStartupError({
                  reason: "already-started-different-scope",
                });
              }

              nextAttemptId += 1;
              const attemptId = nextAttemptId;
              const attemptScope = yield* Scope.make("sequential");
              const attempt = { id: attemptId, ownerScope, scope: attemptScope };
              activeAttempt = attempt;
              lifecycleState = "starting";
              const started = yield* Effect.exit(
                restore(
                  Effect.gen(function* () {
                    yield* githubObserve.start();
                    yield* taskIntake.start();
                  }).pipe(Scope.provide(attemptScope)),
                ),
              );
              if (Exit.isFailure(started)) {
                yield* Scope.close(attemptScope, started).pipe(Effect.ignore);
                if (activeAttempt?.id === attemptId) {
                  activeAttempt = null;
                  lifecycleState = "idle";
                }
                return yield* Effect.failCause(started.cause);
              }
              lifecycleState = "started";
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

  return AgentControlReactor.of({ start });
});

export const layer = Layer.effect(AgentControlReactor, make);
