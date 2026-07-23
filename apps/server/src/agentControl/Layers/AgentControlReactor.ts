import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeReactor } from "../task/Services/AgentControlTaskIntakeReactor.ts";
import {
  AgentControlReactor,
  type AgentControlReactorShape,
} from "../Services/AgentControlReactor.ts";

const make = Effect.gen(function* () {
  const githubObserve = yield* AgentControlGithubObserveReactor;
  const taskIntake = yield* AgentControlTaskIntakeReactor;
  const startupSemaphore = yield* Semaphore.make(1);
  let nextAttemptId = 0;
  let activeAttempt: { readonly id: number; readonly scope: Scope.Closeable } | null = null;

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const parentScope = yield* Effect.scope;
        yield* startupSemaphore.withPermits(1)(
          Effect.gen(function* () {
            if (activeAttempt !== null) return;
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
            activeAttempt = { id: attemptId, scope: attemptScope };
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
          }),
        );
      }),
    ),
  );

  return AgentControlReactor.of({ start });
});

export const layer = Layer.effect(AgentControlReactor, make);
