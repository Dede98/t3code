import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlVerificationAdmissionObservation {
  readonly implementationResultEvidenceId: string;
  readonly handoffId: string;
  readonly verificationStageRunId: string;
  readonly leaseId: string;
  readonly verificationControlledThreadReservationId: string;
}

export interface AgentControlVerificationAdmissionHooksShape {
  readonly afterFinalizerSubscriptionAcquired: () => Effect.Effect<void>;
  readonly afterStageRunSubscriptionAcquired: () => Effect.Effect<void>;
  readonly beforeStartupRecovery: () => Effect.Effect<void>;
  readonly recoveryPageSize: number;
  readonly afterAuthoritativeRead: (
    observation: AgentControlVerificationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly beforeWrites: (
    observation: AgentControlVerificationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly beforeFinalMarker: (
    observation: AgentControlVerificationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly afterNativeCommit: (
    observation: AgentControlVerificationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlVerificationAdmissionObservation,
  ) => Effect.Effect<void>;
}

const noop = () => Effect.void;

/** Production-bound Deferred seams for WAL, commit, and publication evidence. */
export const AgentControlVerificationAdmissionHooks =
  Context.Reference<AgentControlVerificationAdmissionHooksShape>(
    "t3/agentControl/verificationAdmission/Services/AgentControlVerificationAdmissionHooks",
    {
      defaultValue: () => ({
        afterFinalizerSubscriptionAcquired: noop,
        afterStageRunSubscriptionAcquired: noop,
        beforeStartupRecovery: noop,
        recoveryPageSize: 100,
        afterAuthoritativeRead: noop,
        beforeWrites: noop,
        beforeFinalMarker: noop,
        afterNativeCommit: noop,
        afterPublication: noop,
      }),
    },
  );
