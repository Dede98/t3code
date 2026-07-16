import type {
  ProviderInstanceId,
  ProviderUsageRefreshResult,
  ProviderUsageSnapshot,
  ProviderUsageStreamEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";

export interface ProviderUsageShape {
  readonly getSnapshot: Effect.Effect<ReadonlyArray<ProviderUsageSnapshot>>;
  readonly refresh: (
    providerInstanceIds?: ReadonlyArray<ProviderInstanceId>,
  ) => Effect.Effect<ProviderUsageRefreshResult>;
  readonly subscribeEvents: Effect.Effect<
    PubSub.Subscription<ProviderUsageStreamEvent>,
    never,
    Scope.Scope
  >;
}

export class ProviderUsage extends Context.Service<ProviderUsage, ProviderUsageShape>()(
  "t3/provider/Services/ProviderUsage",
) {}
