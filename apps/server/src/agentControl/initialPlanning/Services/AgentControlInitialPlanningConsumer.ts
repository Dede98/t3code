import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface AgentControlInitialPlanningConsumerShape {
  /** Starts receipt-first recovery, wake-up processing, and runtime reconciliation. */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  /** Resolves after every currently queued handoff/runtime event has settled. */
  readonly drain: Effect.Effect<void>;
}

export const AgentControlInitialPlanningConsumer =
  Context.Reference<AgentControlInitialPlanningConsumerShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumer",
    {
      defaultValue: () => ({
        start: () => Effect.void,
        drain: Effect.void,
      }),
    },
  );
