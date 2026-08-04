import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlImplementationTurnConsumerHooksShape {
  readonly beforeClaim: (handoffId: string) => Effect.Effect<void>;
  readonly afterClaim: (handoffId: string) => Effect.Effect<void>;
  readonly beforeDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterTurnDispatchBeforeAcceptanceRead?: (handoffId: string) => Effect.Effect<void>;
}

export const AgentControlImplementationTurnConsumerHooks =
  Context.Reference<AgentControlImplementationTurnConsumerHooksShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationTurnConsumerHooks",
    {
      defaultValue: () => ({
        beforeClaim: () => Effect.void,
        afterClaim: () => Effect.void,
      }),
    },
  );
