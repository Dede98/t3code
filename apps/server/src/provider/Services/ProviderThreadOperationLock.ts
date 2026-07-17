import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface ProviderThreadOperationLockShape {
  readonly withLock: <A, E, R>(
    threadId: ThreadId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

/**
 * Serializes provider lifecycle operations that must observe one coherent
 * thread binding and resume checkpoint.
 */
export class ProviderThreadOperationLock extends Context.Service<
  ProviderThreadOperationLock,
  ProviderThreadOperationLockShape
>()("t3/provider/Services/ProviderThreadOperationLock") {}
