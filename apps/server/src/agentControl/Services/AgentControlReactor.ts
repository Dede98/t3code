import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type { AgentControlGithubObserveStartupError } from "../github/Services/AgentControlGithubObserveReactor.ts";
import type { AgentControlTaskIntakeStartupError } from "../task/Services/AgentControlTaskIntakeReactor.ts";

export class AgentControlReactorStartupError extends Schema.TaggedErrorClass<AgentControlReactorStartupError>()(
  "AgentControlReactorStartupError",
  {
    reason: Schema.Literal("already-started-different-scope"),
  },
) {}

/** Top-level lifecycle boundary for all Agent Control reactors. */
export interface AgentControlReactorShape {
  readonly start: () => Effect.Effect<
    void,
    | AgentControlGithubObserveStartupError
    | AgentControlTaskIntakeStartupError
    | AgentControlReactorStartupError,
    Scope.Scope
  >;
}

export class AgentControlReactor extends Context.Service<
  AgentControlReactor,
  AgentControlReactorShape
>()("t3/agentControl/Services/AgentControlReactor") {}
