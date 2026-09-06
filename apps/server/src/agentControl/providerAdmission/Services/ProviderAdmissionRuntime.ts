import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  ProviderAdmissionDecision,
  ProviderAdmissionRequest,
  ProviderAdmissionUsageEvidence,
} from "../model.ts";
import type { ProviderAdmissionError } from "./ProviderAdmissionStore.ts";

export interface ProviderAdmissionRuntimeShape {
  readonly request: (
    input: ProviderAdmissionRequest,
  ) => Effect.Effect<ProviderAdmissionDecision, ProviderAdmissionError>;
  readonly usageChanged: (
    providerInstanceId: string,
    evidence: ProviderAdmissionUsageEvidence,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly capacityReleased: (providerInstanceId: string) => Effect.Effect<void>;
}

export class ProviderAdmissionRuntime extends Context.Service<
  ProviderAdmissionRuntime,
  ProviderAdmissionRuntimeShape
>()("t3/agentControl/providerAdmission/Services/ProviderAdmissionRuntime") {}
