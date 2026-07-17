import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

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
>()("t3/provider/Services/ProviderRegistryRebuildBarrier") {}
