import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { ProviderAdmissionStage } from "../model.ts";
import type { ProviderAdmissionError } from "./ProviderAdmissionStore.ts";

export interface ProviderAdmissionReleaseAuthorityShape {
  readonly releaseInTransaction: (input: {
    readonly stage: ProviderAdmissionStage;
    readonly handoffId: string;
    readonly finalizedAt: string;
  }) => Effect.Effect<string | null, ProviderAdmissionError>;
  readonly signalCommitted: (providerInstanceId: string | null) => Effect.Effect<void>;
  readonly recover: Effect.Effect<void, ProviderAdmissionError>;
}

export class ProviderAdmissionReleaseAuthority extends Context.Service<
  ProviderAdmissionReleaseAuthority,
  ProviderAdmissionReleaseAuthorityShape
>()("t3/agentControl/providerAdmission/Services/ProviderAdmissionReleaseAuthority") {}
