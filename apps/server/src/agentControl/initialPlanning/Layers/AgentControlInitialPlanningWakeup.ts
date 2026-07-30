import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { AgentControlInitialPlanningWakeup } from "../Services/AgentControlInitialPlanningWakeup.ts";

export const AgentControlInitialPlanningWakeupLive = Layer.effect(
  AgentControlInitialPlanningWakeup,
  Effect.gen(function* () {
    const pubSub = yield* PubSub.unbounded<string>();
    return {
      wake: (handoffId: string) => PubSub.publish(pubSub, handoffId).pipe(Effect.asVoid),
      get stream() {
        return Stream.fromPubSub(pubSub);
      },
    };
  }),
);
