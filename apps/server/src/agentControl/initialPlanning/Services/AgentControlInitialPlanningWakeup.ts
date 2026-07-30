import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

export interface AgentControlInitialPlanningWakeupShape {
  readonly wake: (handoffId: string) => Effect.Effect<void>;
  readonly stream: Stream.Stream<string>;
}

export const AgentControlInitialPlanningWakeup =
  Context.Reference<AgentControlInitialPlanningWakeupShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningWakeup",
    {
      defaultValue: () => ({
        wake: () => Effect.void,
        // The default is consumed only in focused coordinator layers that do
        // not start a consumer.
        stream: Stream.never,
      }),
    },
  );
