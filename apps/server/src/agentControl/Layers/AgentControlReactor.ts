import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeReactor } from "../task/Services/AgentControlTaskIntakeReactor.ts";
import {
  AgentControlReactor,
  type AgentControlReactorShape,
} from "../Services/AgentControlReactor.ts";

const make = Effect.gen(function* () {
  const githubObserve = yield* AgentControlGithubObserveReactor;
  const taskIntake = yield* AgentControlTaskIntakeReactor;

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(
    function* () {
      yield* githubObserve.start();
      yield* taskIntake.start();
    },
  );

  return AgentControlReactor.of({ start });
});

export const layer = Layer.effect(AgentControlReactor, make);
