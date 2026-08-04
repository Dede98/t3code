import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlImplementationAdmissionObservation {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly implementationStageRunId: string;
  readonly implementationLeaseId: string;
  readonly implementationControlledThreadReservationId: string;
}

export interface AgentControlImplementationAdmissionHooksShape {
  readonly afterAuthoritativeRead: (
    observation: AgentControlImplementationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly beforeWrites: (
    observation: AgentControlImplementationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly beforeFinalMarker: (
    observation: AgentControlImplementationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly afterNativeCommit: (
    observation: AgentControlImplementationAdmissionObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlImplementationAdmissionObservation,
  ) => Effect.Effect<void>;
}

const noop = () => Effect.void;

/** Production-bound Deferred seams for focused transaction and race evidence. */
export const AgentControlImplementationAdmissionHooks =
  Context.Reference<AgentControlImplementationAdmissionHooksShape>(
    "t3/agentControl/implementationAdmission/Services/AgentControlImplementationAdmissionHooks",
    {
      defaultValue: () => ({
        afterAuthoritativeRead: noop,
        beforeWrites: noop,
        beforeFinalMarker: noop,
        afterNativeCommit: noop,
        afterPublication: noop,
      }),
    },
  );
