import type { ProviderInstanceId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";

import { makeAgentControlRunOnceKeyedFence } from "../runOnce/context.ts";

// ProviderService and the automated consumers are built by separate Layers.
// This process-wide fence closes the in-process seam while SQLite remains the
// cross-process authority.
const providerEffectFences = makeAgentControlRunOnceKeyedFence<ProviderInstanceId>();

export const withProviderAdmissionEffectFence = <A, E, R>(
  providerInstanceId: ProviderInstanceId,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => providerEffectFences.withPermit(providerInstanceId, effect);
