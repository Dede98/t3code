import type {
  ProviderThreadContinuationSyncInput,
  ProviderThreadContinuationSyncResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { ProviderThreadContinuationSyncError } from "@t3tools/contracts";

export interface ProviderThreadContinuationSyncShape {
  readonly sync: (
    input: ProviderThreadContinuationSyncInput,
  ) => Effect.Effect<ProviderThreadContinuationSyncResult, ProviderThreadContinuationSyncError>;
}

export class ProviderThreadContinuationSync extends Context.Service<
  ProviderThreadContinuationSync,
  ProviderThreadContinuationSyncShape
>()("t3/provider/Services/ProviderThreadContinuationSync") {}
