import type {
  AgentControlControlledThreadReservationEvent,
  AgentControlStageRunEvent,
  AgentControlStageRunLeaseEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export class AgentControlImplementationAdmissionError extends Schema.TaggedError<AgentControlImplementationAdmissionError>()(
  "AgentControlImplementationAdmissionError",
  {
    operation: Schema.String,
    handoffId: Schema.String,
    reason: Schema.Literals([
      "persistence",
      "revision-conflict",
      "planning-evidence-corrupt",
      "planning-not-succeeded",
      "task-evidence-stale",
      "worktree-evidence-stale",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "reservation-history-corrupt",
      "identity-mismatch",
      "receipt-mismatch",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlImplementationAdmissionPublication {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly stageEvents: ReadonlyArray<AgentControlStageRunEvent>;
  readonly leaseEvents: ReadonlyArray<AgentControlStageRunLeaseEvent>;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
}

/**
 * The immutable, replay-validated admission boundary consumed by the next
 * production stage. Text persisted as JSON has already crossed the fatal
 * UTF-8 and canonical-JSON read boundary before this value is returned.
 */
export interface AgentControlImplementationAdmissionEvidence {
  readonly admissionEvidenceId: string;
  readonly admissionCommandId: string;
  readonly admissionFingerprint: string;
  readonly resultEvidenceId: string;
  readonly finalizationCommandId: string;
  readonly finalizationFingerprint: string;
  readonly markerId: string;
  readonly markerFingerprint: string;
  readonly handoffId: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly providerDeliveryId: string;
  readonly providerInstanceId: string;
  readonly providerTurnId: string;
  readonly runtimeMode: string;
  readonly modelSelectionFingerprint: string;
  readonly orchestrationStartedEventId: string;
  readonly orchestrationStartedSequence: number;
  readonly orchestrationTerminalEventId: string;
  readonly orchestrationTerminalSequence: number;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly leaseEventId: string;
  readonly leaseEventSequence: number;
  readonly leaseEventStreamVersion: number;
  readonly finalizedAt: string;
  readonly planId: string;
  readonly planEventId: string;
  readonly planEventSequence: number;
  readonly proposedPlanJson: string;
  readonly proposedPlanDigest: string;
  readonly worktreeReservationId: string;
  readonly worktreeRevision: number;
  readonly worktreeEventSequence: number;
  readonly worktreeOwnershipFingerprint: string;
  readonly worktreeVerifiedAt: string;
  readonly implementationStageRunId: string;
  readonly implementationAttemptId: string;
  readonly implementationLeaseId: string;
  readonly implementationLeaseHolderId: string;
  readonly implementationFenceToken: number;
  readonly implementationControlledThreadReservationId: string;
  readonly implementationThreadId: string;
  readonly implementationStageEventId: string;
  readonly implementationStageEventSequence: number;
  readonly implementationLeaseEventId: string;
  readonly implementationLeaseEventSequence: number;
  readonly implementationLeaseEventStreamVersion: number;
  readonly implementationReservationEventId: string;
  readonly implementationReservationEventSequence: number;
  readonly admittedAt: string;
  readonly receiptId: string;
  readonly admissionMarkerId: string;
  readonly admissionMarkerFingerprint: string;
}

export type AgentControlImplementationAdmissionResult =
  | { readonly _tag: "NotCandidate" }
  | {
      readonly _tag: "Admitted";
      readonly publication: AgentControlImplementationAdmissionPublication;
    }
  | { readonly _tag: "Replayed"; readonly resultEvidenceId: string };

export interface AgentControlImplementationAdmissionShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlImplementationAdmissionResult,
    AgentControlImplementationAdmissionError
  >;
  readonly recover: Effect.Effect<void, AgentControlImplementationAdmissionError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlImplementationAdmissionPublication>;
  readonly loadAcceptedEvidence?: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationAdmissionEvidence>,
    AgentControlImplementationAdmissionError
  >;
}

export const AgentControlImplementationAdmission =
  Context.Reference<AgentControlImplementationAdmissionShape>(
    "t3/agentControl/implementationAdmission/Services/AgentControlImplementationAdmission",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "NotCandidate" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
        loadAcceptedEvidence: () => Effect.succeed(Option.none()),
      }),
    },
  );
