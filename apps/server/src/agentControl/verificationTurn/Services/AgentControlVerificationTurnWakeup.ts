import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export interface AgentControlVerificationTurnWakeupShape {
  readonly wake: (handoffId: string) => Effect.Effect<void>;
  readonly stream: Stream.Stream<string>;
  readonly subscribe: Effect.Effect<Stream.Stream<string>, never, Scope.Scope>;
}

export const AgentControlVerificationTurnWakeup =
  Context.Reference<AgentControlVerificationTurnWakeupShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnWakeup",
    {
      defaultValue: () => ({
        wake: () => Effect.void,
        stream: Stream.never,
        subscribe: Effect.succeed(Stream.never),
      }),
    },
  );
