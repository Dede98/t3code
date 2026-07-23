import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { AgentControlGithubObserveReactor } from "../github/Services/AgentControlGithubObserveReactor.ts";
import {
  AgentControlReactor,
  type AgentControlReactorShape,
} from "../Services/AgentControlReactor.ts";

const make = Effect.gen(function* () {
  const githubObserve = yield* AgentControlGithubObserveReactor;

  const start: AgentControlReactorShape["start"] = Effect.fn("AgentControlReactor.start")(
    function* () {
      yield* githubObserve.start();
    },
  );

  return AgentControlReactor.of({ start });
});

export const layer = Layer.effect(AgentControlReactor, make);
