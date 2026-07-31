import * as Context from "effect/Context";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";

export interface AgentControlInitialPlanningConsumerHooksShape {
  readonly beforeClaim: (handoffId: string) => Effect.Effect<void>;
  readonly afterClaim: (handoffId: string) => Effect.Effect<void>;
  readonly afterTurnDispatchBeforeAcceptanceRead?: (handoffId: string) => Effect.Effect<void>;
  readonly beforeDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly beforeRetryClassification?: (input: {
    readonly handoffId: string;
    readonly cause: Cause.Cause<unknown>;
  }) => Effect.Effect<void>;
}

export const AgentControlInitialPlanningConsumerHooks =
  Context.Reference<AgentControlInitialPlanningConsumerHooksShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumerHooks",
    {
      defaultValue: () => ({
        beforeClaim: () => Effect.void,
        afterClaim: () => Effect.void,
        afterTurnDispatchBeforeAcceptanceRead: () => Effect.void,
        beforeDeliveryCas: () => Effect.void,
        afterDeliveryCas: () => Effect.void,
        beforeRetryClassification: () => Effect.void,
      }),
    },
  );
