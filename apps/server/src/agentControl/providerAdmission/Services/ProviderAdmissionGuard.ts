import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { ProviderAdmissionPermit } from "../model.ts";
import type { ProviderAdmissionError } from "./ProviderAdmissionStore.ts";

export interface ProviderAdmissionGuardShape {
  readonly enter: (
    permit: ProviderAdmissionPermit,
    boundary: "session-start" | "turn-start",
  ) => Effect.Effect<void, ProviderAdmissionError>;
  readonly quarantineUnknown: (
    permit: ProviderAdmissionPermit,
  ) => Effect.Effect<void, ProviderAdmissionError>;
}

export class ProviderAdmissionGuard extends Context.Service<
  ProviderAdmissionGuard,
  ProviderAdmissionGuardShape
>()("t3/agentControl/providerAdmission/Services/ProviderAdmissionGuard") {}
