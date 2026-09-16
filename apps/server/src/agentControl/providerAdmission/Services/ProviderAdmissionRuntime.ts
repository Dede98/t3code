import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  ProviderAdmissionDecision,
  ProviderAdmissionRequest,
  ProviderAdmissionUsageEvidence,
  ProviderResourceAdmissionDecision,
  ProviderResourceAdmissionActive,
  ProviderResourceAdmissionLimits,
  ProviderResourceAdmissionPermit,
  ProviderResourceAdmissionRequest,
} from "../model.ts";
import type { ProviderAdmissionError } from "./ProviderAdmissionStore.ts";

export interface ProviderAdmissionRuntimeShape {
  /** First fail-closed failure from one of the durable admission pumps. */
  readonly awaitFailure: Effect.Effect<never, ProviderAdmissionError>;
  readonly request: (
    input: ProviderAdmissionRequest,
  ) => Effect.Effect<ProviderAdmissionDecision, ProviderAdmissionError>;
  readonly usageChanged: (
    providerInstanceId: string,
    evidence: ProviderAdmissionUsageEvidence,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly capacityReleased: (providerInstanceId: string) => Effect.Effect<void>;
  readonly resourceSettingsChanged?: Effect.Effect<void>;
  readonly requestResource?: (
    request: ProviderResourceAdmissionRequest,
    limits?: ProviderResourceAdmissionLimits,
  ) => Effect.Effect<ProviderResourceAdmissionDecision, ProviderAdmissionError>;
  readonly acquireResource?: (
    request: ProviderResourceAdmissionRequest,
    limits?: ProviderResourceAdmissionLimits,
    readLimits?: Effect.Effect<ProviderResourceAdmissionLimits>,
  ) => Effect.Effect<ProviderResourceAdmissionPermit, ProviderAdmissionError>;
  readonly enterResource?: (
    permit: ProviderResourceAdmissionPermit,
    providerTurnId?: string,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly releaseResource?: (
    permit: ProviderResourceAdmissionPermit,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly deferResource?: (
    permit: ProviderResourceAdmissionPermit,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly cancelResource?: (
    request: ProviderResourceAdmissionRequest,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly configureResourceScope?: (
    accountScope: string,
    limits: ProviderResourceAdmissionLimits,
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly listResourceActive?: Effect.Effect<
    ReadonlyArray<ProviderResourceAdmissionActive>,
    ProviderAdmissionError
  >;
  readonly reconcileResource?: (
    requestId: string,
    observedActivity: "active" | "inactive" | "unknown",
  ) => Effect.Effect<ProviderResourceAdmissionPermit | null, ProviderAdmissionError>;
}

export class ProviderAdmissionRuntime extends Context.Service<
  ProviderAdmissionRuntime,
  ProviderAdmissionRuntimeShape
>()("t3/agentControl/providerAdmission/Services/ProviderAdmissionRuntime") {}
