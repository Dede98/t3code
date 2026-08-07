import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";

export interface AgentControlVerificationTurnConsumerShape {
  readonly processHandoff: (handoffId: string) => Effect.Effect<void, Error>;
  readonly processRuntimeEvent: (event: ProviderRuntimeEvent) => Effect.Effect<void, Error>;
  readonly recover: Effect.Effect<void, Error>;
  readonly subscribeProviderEvents: Effect.Effect<
    PubSub.Subscription<ProviderRuntimeEvent>,
    never,
    Scope.Scope
  >;
  readonly start: (
    providerEvents?: PubSub.Subscription<ProviderRuntimeEvent>,
  ) => Effect.Effect<void, never, Scope.Scope>;
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
        subscribeProviderEvents: Effect.die("Verification provider subscription is unavailable."),
        start: () => Effect.void,
        drain: Effect.void,
      }),
    },
  );
