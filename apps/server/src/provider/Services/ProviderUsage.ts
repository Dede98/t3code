import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import type {
  ProviderInstanceId,
  ProviderUsageSnapshot,
  ProviderUsageRefreshResult,
  ProviderUsageStreamEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

export interface ProviderUsageShape {
  readonly inspectForAdmission?: (
    providerInstanceId: ProviderInstanceId,
  ) => Effect.Effect<
    | { readonly _tag: "Unsupported"; readonly observedAt: string }
    | { readonly _tag: "Observed"; readonly snapshot: ProviderUsageSnapshot }
    | { readonly _tag: "SupportedUnusable"; readonly observedAt: string }
  >;
  readonly stream: Stream.Stream<ProviderUsageStreamEvent>;
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
