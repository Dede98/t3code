import { it } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";

import { makeDrainableWorker } from "./DrainableWorker.ts";

describe("makeDrainableWorker", () => {
  it.live("waits for work enqueued during active processing before draining", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const processed: string[] = [];
        const firstStarted = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        const releaseSecond = yield* Deferred.make<void>();

        const worker = yield* makeDrainableWorker((item: string) =>
          Effect.gen(function* () {
            if (item === "first") {
              yield* Deferred.succeed(firstStarted, undefined).pipe(Effect.orDie);
              yield* Deferred.await(releaseFirst);
            }

            if (item === "second") {
              yield* Deferred.succeed(secondStarted, undefined).pipe(Effect.orDie);
              yield* Deferred.await(releaseSecond);
            }

            processed.push(item);
          }),
        );

        yield* worker.enqueue("first");
        yield* Deferred.await(firstStarted);

        const drained = yield* Deferred.make<void>();
        yield* Effect.forkChild(
          worker.drain.pipe(
            Effect.tap(() => Deferred.succeed(drained, undefined).pipe(Effect.orDie)),
          ),
        );

        yield* worker.enqueue("second");
        yield* Deferred.succeed(releaseFirst, undefined);
        yield* Deferred.await(secondStarted);

        expect(yield* Deferred.isDone(drained)).toBe(false);

        yield* Deferred.succeed(releaseSecond, undefined);
        yield* Deferred.await(drained);

        expect(processed).toEqual(["first", "second"]);
      }),
    ),
  );

  it.live("fails drain and later offers with the original terminal defect", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const defect = new Error("worker-terminal-defect");
        const firstStarted = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        const processed: string[] = [];
        const worker = yield* makeDrainableWorker(
          (item: string) =>
            Effect.gen(function* () {
              processed.push(item);
              if (item !== "first") return;
              yield* Deferred.succeed(firstStarted, undefined);
              yield* Deferred.await(releaseFirst);
              return yield* Effect.die(defect);
            }),
          { failureMode: "observable" },
        );

        yield* worker.enqueue("first");
        yield* Deferred.await(firstStarted);
        yield* worker.enqueue("already-waiting");
        const drainFiber = yield* worker.drain.pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseFirst, undefined);

        const drainExit = yield* Fiber.await(drainFiber);
        expect(Exit.isFailure(drainExit)).toBe(true);
        if (Exit.isFailure(drainExit)) {
          const reason = drainExit.cause.reasons.find(Cause.isDieReason);
          expect(reason !== undefined && Cause.isDieReason(reason)).toBe(true);
          if (reason !== undefined && Cause.isDieReason(reason)) {
            expect(reason.defect).toBe(defect);
          }
        }

        const enqueueExit = yield* Effect.exit(worker.enqueue("offered-after-failure"));
        expect(Exit.isFailure(enqueueExit)).toBe(true);
        if (Exit.isFailure(enqueueExit)) {
          const reason = enqueueExit.cause.reasons.find(Cause.isDieReason);
          expect(reason !== undefined && Cause.isDieReason(reason)).toBe(true);
          if (reason !== undefined && Cause.isDieReason(reason)) {
            expect(reason.defect).toBe(defect);
          }
        }
        expect(processed).toEqual(["first"]);
      }),
    ),
  );

  it.live("preserves interruption when its owner scope closes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const workerScope = yield* Scope.make("sequential");
        const processing = yield* Deferred.make<void>();
        const worker = yield* makeDrainableWorker(
          () => Deferred.succeed(processing, undefined).pipe(Effect.andThen(Effect.never)),
          { failureMode: "observable" },
        ).pipe(Scope.provide(workerScope));

        yield* worker.enqueue("work");
        yield* Deferred.await(processing);
        const waitingDrains = [
          yield* worker.drain.pipe(Effect.forkChild),
          yield* worker.drain.pipe(Effect.forkChild),
          yield* worker.drain.pipe(Effect.forkChild),
        ];
        yield* Scope.close(workerScope, Exit.void);

        for (const drainFiber of waitingDrains) {
          const drainExit = yield* Fiber.await(drainFiber);
          expect(Exit.isFailure(drainExit)).toBe(true);
          if (Exit.isFailure(drainExit)) {
            expect(Cause.hasInterruptsOnly(drainExit.cause)).toBe(true);
          }
        }
        const laterDrain = yield* Effect.exit(worker.drain);
        expect(Exit.isFailure(laterDrain)).toBe(true);
        if (Exit.isFailure(laterDrain)) {
          expect(Cause.hasInterruptsOnly(laterDrain.cause)).toBe(true);
        }
        const laterOffer = yield* Effect.exit(worker.enqueue("after-close"));
        expect(Exit.isFailure(laterOffer)).toBe(true);
        if (Exit.isFailure(laterOffer)) {
          expect(Cause.hasInterruptsOnly(laterOffer.cause)).toBe(true);
        }
      }),
    ),
  );

  it.live(
    "wakes every drain waiter with the fatal cause after a successful item and discards a final offer",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const defect = new Error("worker-follow-up-defect");
          const fatalStarted = yield* Deferred.make<void>();
          const releaseFatal = yield* Deferred.make<void>();
          const processed: string[] = [];
          const worker = yield* makeDrainableWorker(
            (item: string) =>
              Effect.gen(function* () {
                processed.push(item);
                if (item !== "fatal") return;
                yield* Deferred.succeed(fatalStarted, undefined);
                yield* Deferred.await(releaseFatal);
                return yield* Effect.die(defect);
              }),
            { failureMode: "observable" },
          );

          yield* worker.enqueue("successful");
          yield* worker.enqueue("fatal");
          const drainWaiters = [
            yield* worker.drain.pipe(Effect.forkChild),
            yield* worker.drain.pipe(Effect.forkChild),
            yield* worker.drain.pipe(Effect.forkChild),
          ];
          yield* Deferred.await(fatalStarted);
          yield* worker.enqueue("offered-before-termination");
          yield* Deferred.succeed(releaseFatal, undefined);

          for (const waiter of drainWaiters) {
            const exit = yield* Fiber.await(waiter);
            expect(Exit.isFailure(exit)).toBe(true);
            if (Exit.isFailure(exit)) {
              expect(
                exit.cause.reasons.some(
                  (reason) => Cause.isDieReason(reason) && reason.defect === defect,
                ),
              ).toBe(true);
            }
          }
          expect(processed).toEqual(["successful", "fatal"]);

          const finalOffer = yield* Effect.exit(worker.enqueue("offered-after-termination"));
          expect(Exit.isFailure(finalOffer)).toBe(true);
          if (Exit.isFailure(finalOffer)) {
            expect(
              finalOffer.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === defect,
              ),
            ).toBe(true);
          }
        }),
      ),
  );
});
