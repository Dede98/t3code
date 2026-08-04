import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

export interface AgentControlImplementationTurnWakeupShape {
  readonly wake: (handoffId: string) => Effect.Effect<void>;
  readonly stream: Stream.Stream<string>;
}

export const AgentControlImplementationTurnWakeup =
  Context.Reference<AgentControlImplementationTurnWakeupShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationTurnWakeup",
    {
      defaultValue: () => ({ wake: () => Effect.void, stream: Stream.never }),
    },
  );
