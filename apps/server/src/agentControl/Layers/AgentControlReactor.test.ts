import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeReactor } from "../task/Services/AgentControlTaskIntakeReactor.ts";
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
