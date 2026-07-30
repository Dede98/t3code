import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlInitialPlanningConsumerHooksShape {
  readonly beforeClaim: (handoffId: string) => Effect.Effect<void>;
  readonly afterClaim: (handoffId: string) => Effect.Effect<void>;
}

export const AgentControlInitialPlanningConsumerHooks =
  Context.Reference<AgentControlInitialPlanningConsumerHooksShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumerHooks",
    {
      defaultValue: () => ({
        beforeClaim: () => Effect.void,
        afterClaim: () => Effect.void,
      }),
    },
  );
