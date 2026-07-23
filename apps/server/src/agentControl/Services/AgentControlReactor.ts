import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type { AgentControlGithubObserveStartupError } from "../github/Services/AgentControlGithubObserveReactor.ts";
import type { AgentControlTaskIntakeStartupError } from "../task/Services/AgentControlTaskIntakeReactor.ts";

/** Top-level lifecycle boundary for all Agent Control reactors. */
export interface AgentControlReactorShape {
  readonly start: () => Effect.Effect<
    void,
    AgentControlGithubObserveStartupError | AgentControlTaskIntakeStartupError,
    Scope.Scope
  >;
}

export class AgentControlReactor extends Context.Service<
  AgentControlReactor,
  AgentControlReactorShape
>()("t3/agentControl/Services/AgentControlReactor") {}
