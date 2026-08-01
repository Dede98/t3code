import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";

import {
  ProviderThreadOperationLock,
  type ProviderThreadOperationLockShape,
} from "../Services/ProviderThreadOperationLock.ts";

export interface ProviderThreadOperationLockObserver {
  readonly onLock: (input: {
    readonly phase: "acquired" | "released";
    readonly threadId: string;
    readonly lock: object;
  }) => Effect.Effect<void, never>;
}

const makeProviderThreadOperationLockWithObserver = (
  observer?: ProviderThreadOperationLockObserver,
) =>
  Effect.gen(function* () {
    const locks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

    const getLock = (threadId: string) =>
      SynchronizedRef.modifyEffect(locks, (current) => {
        const existing = current.get(threadId);
        if (existing !== undefined) {
          return Effect.succeed([existing, current] as const);
        }
        return Semaphore.make(1).pipe(
          Effect.map((lock) => {
            const next = new Map(current);
            next.set(threadId, lock);
            return [lock, next] as const;
          }),
        );
      });

    const withLock: ProviderThreadOperationLockShape["withLock"] = (threadId, effect) =>
      Effect.flatMap(getLock(threadId), (lock) =>
        observer === undefined
          ? lock.withPermit(effect)
          : lock.withPermit(
              observer
                .onLock({ phase: "acquired", threadId, lock })
                .pipe(
                  Effect.andThen(effect),
                  Effect.ensuring(observer.onLock({ phase: "released", threadId, lock })),
                ),
            ),
      );

    return ProviderThreadOperationLock.of({ withLock });
  });

export const makeProviderThreadOperationLock = makeProviderThreadOperationLockWithObserver();

export const makeProviderThreadOperationLockLive = (
  observer?: ProviderThreadOperationLockObserver,
) =>
  Layer.effect(ProviderThreadOperationLock, makeProviderThreadOperationLockWithObserver(observer));

export const ProviderThreadOperationLockLive = makeProviderThreadOperationLockLive();
