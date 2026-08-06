import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";

export interface AgentControlVerificationTurnConsumerShape {
  readonly processHandoff: (handoffId: string) => Effect.Effect<void, Error>;
  readonly processRuntimeEvent: (event: ProviderRuntimeEvent) => Effect.Effect<void, Error>;
  readonly recover: Effect.Effect<void, Error>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
}

export const AgentControlVerificationTurnConsumer =
  Context.Reference<AgentControlVerificationTurnConsumerShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.void,
        processRuntimeEvent: () => Effect.void,
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
      }),
    },
  );
