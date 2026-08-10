import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlVerificationTurnConsumerHooksShape {
  readonly beforeClaim: (handoffId: string) => Effect.Effect<void>;
  readonly afterClaim: (handoffId: string) => Effect.Effect<void>;
  readonly beforeDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterDeliveryCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterTurnDispatchBeforeAcceptanceRead?: (handoffId: string) => Effect.Effect<void>;
  readonly beforeProviderTerminalCas?: (handoffId: string) => Effect.Effect<void>;
  readonly afterProviderTerminalCas?: (handoffId: string) => Effect.Effect<void>;
}

export const AgentControlVerificationTurnConsumerHooks =
  Context.Reference<AgentControlVerificationTurnConsumerHooksShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumerHooks",
    {
      defaultValue: () => ({
        beforeClaim: () => Effect.void,
        afterClaim: () => Effect.void,
      }),
    },
  );
