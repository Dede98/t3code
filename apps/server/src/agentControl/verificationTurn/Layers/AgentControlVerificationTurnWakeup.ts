import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";

export const AgentControlVerificationTurnWakeupLive = Layer.effect(
  AgentControlVerificationTurnWakeup,
  Effect.gen(function* () {
    const pubSub = yield* PubSub.unbounded<string>();
    return AgentControlVerificationTurnWakeup.of({
      wake: (handoffId) => PubSub.publish(pubSub, handoffId).pipe(Effect.asVoid),
      get stream() {
        return Stream.fromPubSub(pubSub);
      },
      subscribe: PubSub.subscribe(pubSub).pipe(Effect.map(Stream.fromSubscription)),
    });
  }),
);
