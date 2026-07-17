import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TxReentrantLock from "effect/TxReentrantLock";

import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";

export const makeProviderRegistryRebuildBarrier = Effect.gen(function* () {
  const lock = yield* Effect.tx(TxReentrantLock.make());

  return ProviderRegistryRebuildBarrier.of({
    withOperation: (effect) => TxReentrantLock.withReadLock(lock, effect),
    withRebuild: (effect) => TxReentrantLock.withWriteLock(lock, effect),
  });
});

export const ProviderRegistryRebuildBarrierLive = Layer.effect(
  ProviderRegistryRebuildBarrier,
  makeProviderRegistryRebuildBarrier,
);
