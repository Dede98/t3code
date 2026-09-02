import type { ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type { ReactorStartupActivation } from "../../../reactorStartupActivation.ts";
import type { AgentControlArmedError } from "../model.ts";

export interface AgentControlArmedSchedulerShape {
  /** First fail-closed runtime failure after startup. Startup recovery failures
   * are still returned directly from `prepare`. */
  readonly awaitFailure: Effect.Effect<never, AgentControlArmedError>;
  readonly recover: Effect.Effect<void, AgentControlArmedError>;
  readonly processProject: (projectId: ProjectId) => Effect.Effect<void, AgentControlArmedError>;
  readonly prepare: (
    activation: ReactorStartupActivation,
  ) => Effect.Effect<void, AgentControlArmedError, Scope.Scope>;
}

export class AgentControlArmedScheduler extends Context.Service<
  AgentControlArmedScheduler,
  AgentControlArmedSchedulerShape
>()("t3/agentControl/armed/Services/AgentControlArmedScheduler") {}
