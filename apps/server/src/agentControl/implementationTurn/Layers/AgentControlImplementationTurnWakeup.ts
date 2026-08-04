import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { AgentControlImplementationTurnWakeup } from "../Services/AgentControlImplementationTurnWakeup.ts";

export const AgentControlImplementationTurnWakeupLive = Layer.effect(
  AgentControlImplementationTurnWakeup,
  Effect.gen(function* () {
    const pubSub = yield* PubSub.unbounded<string>();
    return AgentControlImplementationTurnWakeup.of({
      wake: (handoffId) => PubSub.publish(pubSub, handoffId).pipe(Effect.asVoid),
      get stream() {
        return Stream.fromPubSub(pubSub);
      },
    });
  }),
);
