import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";

export interface AgentControlImplementationTurnConsumerShape {
  readonly processHandoff: (handoffId: string) => Effect.Effect<void, Error>;
  readonly processRuntimeEvent: (event: ProviderRuntimeEvent) => Effect.Effect<void, Error>;
  readonly recover: Effect.Effect<void, Error>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
}

export const AgentControlImplementationTurnConsumer =
  Context.Reference<AgentControlImplementationTurnConsumerShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationTurnConsumer",
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
