import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { AgentControlVerificationStageStarterError } from "../Services/AgentControlVerificationStageStarter.ts";
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

it.effect(
  "keeps a typed Stage-Starter terminal error as the exact Fail cause for every drain",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const wakeup = yield* AgentControlVerificationTurnWakeup;
        const lifecycle = yield* wakeup.subscribeStageStarter!;
        const failure = new AgentControlVerificationStageStarterError({
          handoffId: "verification-stage-typed-failure",
          operation: "append-stage-started",
          reason: "persistence",
        });
        const subscriber = yield* Stream.runForEach(
          Stream.fromSubscription(lifecycle.subscription),
          () => Effect.fail(failure),
        ).pipe(Effect.onExit(lifecycle.reportExit), Effect.forkChild({ startImmediately: true }));

        yield* wakeup.wake("verification-stage-typed-failure");
        assert.isTrue(Exit.isFailure(yield* Fiber.await(subscriber)));
        const drains = yield* Effect.forEach(
          [wakeup.drainStageStarter!, wakeup.drainStageStarter!, wakeup.drainStageStarter!],
          Effect.exit,
          { concurrency: "unbounded" },
        );
        const laterDrain = yield* Effect.exit(wakeup.drainStageStarter!);

        for (const drainExit of [...drains, laterDrain]) {
          assert.isTrue(Exit.isFailure(drainExit));
          if (Exit.isFailure(drainExit)) {
            assert.isTrue(
              drainExit.cause.reasons.some(
                (reason) => Cause.isFailReason(reason) && reason.error === failure,
              ),
            );
            assert.isFalse(drainExit.cause.reasons.some(Cause.isDieReason));
          }
        }
      }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
    ),
);

it.effect.each(["defect", "interrupt"] as const)(
  "preserves a typed Stage-Starter Fail combined with a terminal %s",
  (terminal) =>
    Effect.scoped(
      Effect.gen(function* () {
        const wakeup = yield* AgentControlVerificationTurnWakeup;
        const lifecycle = yield* wakeup.subscribeStageStarter!;
        const failure = new AgentControlVerificationStageStarterError({
          handoffId: `verification-stage-mixed-${terminal}`,
          operation: "append-stage-started",
          reason: "revision-conflict",
        });
        const defect = new Error("verification-stage-cleanup-defect");
        const originalCause = Cause.combine(
          Cause.fail(failure),
          terminal === "defect" ? Cause.die(defect) : Cause.interrupt(),
        );
        const subscriber = yield* Stream.runForEach(
          Stream.fromSubscription(lifecycle.subscription),
          () => Effect.failCause(originalCause),
        ).pipe(Effect.onExit(lifecycle.reportExit), Effect.forkChild({ startImmediately: true }));

        yield* wakeup.wake(`verification-stage-mixed-${terminal}`);
        assert.isTrue(Exit.isFailure(yield* Fiber.await(subscriber)));
        const drainExit = yield* Effect.exit(wakeup.drainStageStarter!);
        assert.isTrue(Exit.isFailure(drainExit));
        if (Exit.isFailure(drainExit)) {
          assert.isTrue(
            drainExit.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === failure,
            ),
          );
          if (terminal === "defect") {
            assert.isTrue(
              drainExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === defect,
              ),
            );
          } else {
            assert.isTrue(drainExit.cause.reasons.some(Cause.isInterruptReason));
          }
        }
      }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
    ),
);

it.effect("keeps a pure Stage-Starter scope interruption as Interrupt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const wakeup = yield* AgentControlVerificationTurnWakeup;
      const lifecycle = yield* wakeup.subscribeStageStarter!;
      const subscriber = yield* Stream.runForEach(
        Stream.fromSubscription(lifecycle.subscription),
        () => Effect.interrupt,
      ).pipe(Effect.onExit(lifecycle.reportExit), Effect.forkChild({ startImmediately: true }));

      yield* wakeup.wake("verification-stage-interrupt");
      assert.isTrue(Exit.isFailure(yield* Fiber.await(subscriber)));
      const drainExit = yield* Effect.exit(wakeup.drainStageStarter!);
      assert.isTrue(Exit.isFailure(drainExit));
      if (Exit.isFailure(drainExit)) {
        assert.isTrue(Cause.hasInterruptsOnly(drainExit.cause));
      }
    }).pipe(Effect.provide(Layer.fresh(AgentControlVerificationTurnWakeupLive))),
  ),
);
