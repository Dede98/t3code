import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TxReentrantLock from "effect/TxReentrantLock";

export interface ProviderRegistryRebuildBarrierShape {
  /** Run an adapter operation concurrently with other adapter operations. */
  readonly withOperation: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Run a registry rebuild after every adapter operation has left the barrier. */
  readonly withRebuild: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

/**
 * Process-wide reader/writer barrier between live provider adapter operations
 * and registry rebuilds that close adapter-owned child scopes.
 */
export class ProviderRegistryRebuildBarrier extends Context.Service<
  ProviderRegistryRebuildBarrier,
  ProviderRegistryRebuildBarrierShape
>()("t3/provider/ProviderRegistryRebuildBarrier") {}

export const makeProviderRegistryRebuildBarrier = Effect.gen(function* () {
  const lock = yield* Effect.tx(TxReentrantLock.make());

  return ProviderRegistryRebuildBarrier.of({
    withOperation: (effect) => TxReentrantLock.withReadLock(lock, effect),
    withRebuild: (effect) => TxReentrantLock.withWriteLock(lock, effect),
  });
});

export const layer = Layer.effect(
  ProviderRegistryRebuildBarrier,
  makeProviderRegistryRebuildBarrier,
);
