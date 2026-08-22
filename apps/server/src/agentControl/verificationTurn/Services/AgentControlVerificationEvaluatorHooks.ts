import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlVerificationEvaluatorHooksShape {
  readonly afterSourceLoad: (handoffId: string) => Effect.Effect<void>;
  readonly afterEvidence: (handoffId: string) => Effect.Effect<void>;
  readonly afterReceipt: (handoffId: string) => Effect.Effect<void>;
  readonly afterCommit: (handoffId: string) => Effect.Effect<void>;
  readonly recoveryPageSize?: number;
}

export const AgentControlVerificationEvaluatorHooks =
  Context.Reference<AgentControlVerificationEvaluatorHooksShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationEvaluatorHooks",
    {
      defaultValue: () => ({
        afterSourceLoad: () => Effect.void,
        afterEvidence: () => Effect.void,
        afterReceipt: () => Effect.void,
        afterCommit: () => Effect.void,
      }),
    },
  );
