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

export class AgentControlVerificationAdmissionError extends Schema.TaggedErrorClass<AgentControlVerificationAdmissionError>()(
  "AgentControlVerificationAdmissionError",
  {
    implementationResultEvidenceId: Schema.String,
    operation: Schema.String,
    reason: Schema.Literals([
      "candidate-evidence",
      "partial-replay",
      "identity-mismatch",
      "task-history-corrupt",
      "worktree-history-corrupt",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "reservation-history-corrupt",
      "orchestration-history-corrupt",
      "revision-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlVerificationAdmissionPublication {
  readonly implementationResultEvidenceId: string;
  readonly handoffId: string;
  readonly stageEvents: ReadonlyArray<AgentControlStageRunEvent>;
  readonly leaseEvents: ReadonlyArray<AgentControlStageRunLeaseEvent>;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
}

export interface AgentControlVerificationAdmissionEvidence {
  readonly admissionEvidenceId: string;
  readonly admissionCommandId: string;
  readonly admissionFingerprint: string;
  readonly implementationResultEvidenceId: string;
  readonly handoffId: string;
  readonly verificationStageRunId: string;
  readonly verificationAttemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly verificationFenceToken: number;
  readonly verificationControlledThreadReservationId: string;
  readonly verificationThreadId: string;
  readonly receiptId: string;
  readonly markerId: string;
  readonly markerFingerprint: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly repositoryDisplay: string;
  readonly sourceRevision: string;
  readonly worktreeReservationId: string;
  readonly worktreeRevision: number;
  readonly worktreeEventId: string;
  readonly worktreeEventSequence: number;
  readonly worktreeEventStreamVersion: number;
  readonly worktreeOwnershipFingerprint: string;
  readonly worktreeVerifiedAt: string;
  readonly worktreePath: string;
  readonly branch: string;
  readonly implementationStageRunId: string;
  readonly implementationAttemptId: string;
  readonly implementationFenceToken: number;
  readonly implementationControlledThreadReservationId: string;
  readonly implementationThreadId: string;
  readonly planningThreadId: string;
  readonly planId: string;
  readonly proposedPlanJson: string;
  readonly proposedPlanDigest: string;
  readonly taskSourceEventId: string;
  readonly taskSourceEventSequence: number;
  readonly taskSourceEventStreamVersion: number;
  readonly verificationStageEventId: string;
  readonly verificationStageEventSequence: number;
  readonly verificationStageEventStreamVersion: 1;
  readonly verificationLeaseEventId: string;
  readonly verificationLeaseEventSequence: number;
  readonly verificationLeaseEventStreamVersion: number;
  readonly verificationReservationEventId: string;
  readonly verificationReservationEventSequence: number;
  readonly verificationReservationEventStreamVersion: 1;
  readonly verificationAdmissionJson: string;
  readonly taskHistoryJson: string;
  readonly taskHistoryDigest: string;
  readonly worktreeHistoryJson: string;
  readonly worktreeHistoryDigest: string;
  readonly stageHistoryJson: string;
  readonly stageHistoryDigest: string;
  readonly leaseHistoryJson: string;
  readonly leaseHistoryDigest: string;
  readonly reservationHistoryJson: string;
  readonly reservationHistoryDigest: string;
  readonly orchestrationHistoryJson: string;
  readonly orchestrationHistoryDigest: string;
  readonly implementationResultJson: string;
  readonly implementationHandoffJson: string;
  readonly implementationProviderDeliveryJson: string;
  readonly admittedAt: string;
}

export type AgentControlVerificationAdmissionResult =
  | { readonly _tag: "NotCandidate" }
  | {
      readonly _tag: "Admitted";
      readonly publication: AgentControlVerificationAdmissionPublication;
    }
  | { readonly _tag: "Replayed"; readonly implementationResultEvidenceId: string };

export interface AgentControlVerificationAdmissionShape {
  readonly processResultEvidence: (
    implementationResultEvidenceId: string,
  ) => Effect.Effect<
    AgentControlVerificationAdmissionResult,
    AgentControlVerificationAdmissionError
  >;
  readonly recover: Effect.Effect<void, AgentControlVerificationAdmissionError>;
  readonly start: () => Effect.Effect<void, AgentControlVerificationAdmissionError, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlVerificationAdmissionPublication>;
  readonly subscribePublications: Effect.Effect<
    Stream.Stream<AgentControlVerificationAdmissionPublication>,
    never,
    Scope.Scope
  >;
  readonly loadAcceptedEvidence: (
    implementationResultEvidenceId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlVerificationAdmissionEvidence>,
    AgentControlVerificationAdmissionError
  >;
}

export const AgentControlVerificationAdmission =
  Context.Reference<AgentControlVerificationAdmissionShape>(
    "t3/agentControl/verificationAdmission/Services/AgentControlVerificationAdmission",
    {
      defaultValue: () => ({
        processResultEvidence: () => Effect.succeed({ _tag: "NotCandidate" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
        subscribePublications: Effect.succeed(Stream.never),
        loadAcceptedEvidence: () => Effect.succeed(Option.none()),
      }),
    },
  );
