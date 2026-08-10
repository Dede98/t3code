import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import {
  AgentControlVerificationTurnWakeup,
  type AgentControlVerificationTurnWakeupShape,
  type AgentControlVerificationWakeupPublication,
} from "../Services/AgentControlVerificationTurnWakeup.ts";
import type { AgentControlVerificationStageStarterError } from "../Services/AgentControlVerificationStageStarter.ts";

export const AgentControlVerificationTurnWakeupLive = Layer.effect(
  AgentControlVerificationTurnWakeup,
  Effect.gen(function* () {
    const pubSub = yield* PubSub.unbounded<AgentControlVerificationWakeupPublication>();
    const nextDrainId = yield* Ref.make(0);
    let activeStageStarter:
      | {
          readonly terminal: Deferred.Deferred<void, AgentControlVerificationStageStarterError>;
          readonly subscription: object;
        }
      | undefined;

    const handoffStream = (stream: Stream.Stream<AgentControlVerificationWakeupPublication>) =>
      stream.pipe(
        Stream.filterMap((publication) =>
          publication._tag === "Handoff" ? Result.succeed(publication.handoffId) : Result.failVoid,
        ),
      );

    const subscribeStageStarter: NonNullable<
      AgentControlVerificationTurnWakeupShape["subscribeStageStarter"]
    > = Effect.gen(function* () {
      const ownerScope = yield* Scope.Scope;
      const subscription = yield* PubSub.subscribe(pubSub);
      const terminal = yield* Deferred.make<void, AgentControlVerificationStageStarterError>();
      if (activeStageStarter !== undefined) {
        return yield* Effect.die("Verification Stage-Starter subscription is already active.");
      }
      const identity = {};
      activeStageStarter = { terminal, subscription: identity };
      yield* Scope.addFinalizer(
        ownerScope,
        Effect.gen(function* () {
          if (activeStageStarter?.subscription !== identity) return;
          yield* Deferred.interrupt(terminal).pipe(Effect.ignore);
          activeStageStarter = undefined;
        }),
      );
      return {
        subscription,
        reportExit: (exit) => Deferred.done(terminal, exit).pipe(Effect.asVoid),
      };
    });

    const drainStageStarter: NonNullable<
      AgentControlVerificationTurnWakeupShape["drainStageStarter"]
    > = Effect.gen(function* () {
      const stageStarter = activeStageStarter;
      if (stageStarter === undefined) {
        return yield* Effect.die("Verification Stage-Starter drain has no active subscriber.");
      }
      const acknowledgement = yield* Deferred.make<
        void,
        AgentControlVerificationStageStarterError
      >();
      const token = {
        id: yield* Ref.getAndUpdate(nextDrainId, (id) => id + 1),
        acknowledgement,
      };
      const accepted = yield* PubSub.publish(pubSub, { _tag: "Drain", token });
      if (!accepted)
        return yield* Effect.die("Verification wakeup PubSub rejected a drain marker.");
      yield* Effect.raceFirst(
        Deferred.await(acknowledgement),
        Deferred.await(stageStarter.terminal).pipe(
          Effect.andThen(
            Effect.die("Verification Stage-Starter terminated before acknowledging its drain."),
          ),
        ),
      );
    });

    return AgentControlVerificationTurnWakeup.of({
      wake: (handoffId) =>
        PubSub.publish(pubSub, { _tag: "Handoff", handoffId }).pipe(
          Effect.flatMap((accepted) =>
            accepted ? Effect.void : Effect.die("Verification wakeup PubSub rejected a handoff."),
          ),
        ),
      get stream() {
        return handoffStream(Stream.fromPubSub(pubSub));
      },
      subscribe: PubSub.subscribe(pubSub).pipe(
        Effect.map(Stream.fromSubscription),
        Effect.map(handoffStream),
      ),
      subscribeStageStarter,
      drainStageStarter,
    });
  }),
);
