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
  const startupSemaphore = yield* Semaphore.make(1);
  let nextAttemptId = 0;
  let activeAttempt: {
    readonly id: number;
    readonly ownerScope: Scope.Scope;
    readonly scope: Scope.Closeable;
  } | null = null;

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const parentScope = yield* Effect.scope;
        yield* restore(startupSemaphore.take(1));
        yield* Effect.gen(function* () {
          if (activeAttempt !== null) {
            if (activeAttempt.ownerScope === parentScope) return;
            return yield* new AgentControlReactorStartupError({
              reason: "already-started-different-scope",
            });
          }
          nextAttemptId += 1;
          const attemptId = nextAttemptId;
          const attemptScope = yield* Scope.make("sequential");
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
            return yield* Effect.failCause(started.cause);
          }
          activeAttempt = {
            id: attemptId,
            ownerScope: parentScope,
            scope: attemptScope,
          };
          yield* Scope.addFinalizerExit(parentScope, (exit) =>
            Effect.gen(function* () {
              const ownsAttempt = yield* Effect.sync(() => {
                if (activeAttempt?.id !== attemptId) return false;
                activeAttempt = null;
                return true;
              });
              if (ownsAttempt) {
                yield* Scope.close(attemptScope, exit).pipe(Effect.ignore);
              }
            }),
          );
        }).pipe(Effect.ensuring(startupSemaphore.release(1).pipe(Effect.asVoid)));
      }),
    ),
  );

  return AgentControlReactor.of({ start });
});

export const layer = Layer.effect(AgentControlReactor, make);
