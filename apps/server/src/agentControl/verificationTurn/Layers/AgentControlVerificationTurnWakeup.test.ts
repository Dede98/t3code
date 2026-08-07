import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";
import { AgentControlVerificationTurnWakeupLive } from "./AgentControlVerificationTurnWakeup.ts";

it.effect("acquires a wakeup subscription before the stream is evaluated", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const wakeup = yield* AgentControlVerificationTurnWakeup;
      const subscribed = yield* wakeup.subscribe;

      yield* wakeup.wake("verification-handoff-ready");

      const received = yield* Stream.runHead(subscribed);
      assert.deepStrictEqual(received, Option.some("verification-handoff-ready"));
    }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
  ),
);

it.effect("drains the Stage-Starter prefix behind an in-flight wakeup", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const wakeup = yield* AgentControlVerificationTurnWakeup;
      const lifecycle = yield* wakeup.subscribeStageStarter!;
      const handoffEntered = yield* Deferred.make<void>();
      const releaseHandoff = yield* Deferred.make<void>();
      const processed = yield* Deferred.make<void>();
      yield* Stream.runForEach(Stream.fromSubscription(lifecycle.subscription), (publication) =>
        publication._tag === "Handoff"
          ? Deferred.succeed(handoffEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseHandoff)),
              Effect.andThen(Deferred.succeed(processed, undefined)),
              Effect.asVoid,
            )
          : Deferred.succeed(publication.token.acknowledgement, undefined).pipe(Effect.asVoid),
      ).pipe(Effect.onExit(lifecycle.reportExit), Effect.forkChild({ startImmediately: true }));

      yield* wakeup.wake("verification-stage-prefix");
      yield* Deferred.await(handoffEntered);
      const drain = yield* wakeup.drainStageStarter!.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isUndefined(drain.pollUnsafe());
      yield* Deferred.succeed(releaseHandoff, undefined);
      yield* Deferred.await(processed);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(drain)));
    }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
  ),
);

it.effect("fails a Stage-Starter drain with the subscriber's exact terminal defect", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const wakeup = yield* AgentControlVerificationTurnWakeup;
      const lifecycle = yield* wakeup.subscribeStageStarter!;
      const defect = new Error("verification-stage-starter-terminal-defect");
      const subscriber = yield* Stream.runForEach(
        Stream.fromSubscription(lifecycle.subscription),
        () => Effect.die(defect),
      ).pipe(Effect.onExit(lifecycle.reportExit), Effect.forkChild({ startImmediately: true }));

      yield* wakeup.wake("verification-stage-defect");
      assert.isTrue(Exit.isFailure(yield* Fiber.await(subscriber)));
      const drainExit = yield* Effect.exit(wakeup.drainStageStarter!);
      assert.isTrue(Exit.isFailure(drainExit));
      if (Exit.isFailure(drainExit)) {
        assert.isTrue(
          drainExit.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === defect,
          ),
        );
      }
    }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
  ),
);
