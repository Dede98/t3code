import type {
  ProviderInstanceId,
  ProviderUsageRefreshResult,
  ProviderUsageStreamEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

/** Compatibility RPCs backed exclusively by the provider registry. */
export class ProviderUsage extends Context.Service<
  ProviderUsage,
  {
    readonly refresh: (
      providerInstanceIds?: ReadonlyArray<ProviderInstanceId>,
    ) => Effect.Effect<ProviderUsageRefreshResult>;
    readonly stream: Stream.Stream<ProviderUsageStreamEvent>;
  }
>()("t3/provider/Services/ProviderUsage") {}
