import { loadRunOnceRepair, type RunOnceRepair } from "../../runOnce/repair.ts";
import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  IsoDateTime,
  PositiveInt,
  ProjectId,
  ThreadId,
  type AgentControlControlledThreadReservationEventDraft,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEventDraft,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseHistory,
  loadAuthoritativeLeaseState,
  loadAuthoritativeStageRunState,
} from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import {
  loadAuthoritativeControlledThreadReservation,
  loadAuthoritativeControlledThreadReservationTaskHistory,
} from "../../controlledThreadReservation/authoritative.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../controlledThreadReservation/identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../../controlledThreadReservation/projector.ts";
import { AgentControlControlledThreadReservationEngine } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationStateRepository.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import {
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  sha256Utf8,
} from "../../initialPlanning/eventEvidence.ts";
import { AgentControlInitialPlanningFinalizer } from "../../initialPlanning/Services/AgentControlInitialPlanningFinalizer.ts";
import {
  deriveImplementationAdmissionCommandId,
  deriveImplementationAdmissionEvidenceId,
  deriveImplementationAdmissionMarkerId,
  deriveImplementationAdmissionReceiptId,
  deriveImplementationLeaseReservedEventId,
  deriveImplementationReservationPreparedEventId,
  deriveImplementationStagePreparedEventId,
  fingerprintImplementationAdmission,
} from "../identity.ts";
import {
  AgentControlImplementationAdmission,
  AgentControlImplementationAdmissionError,
  type AgentControlImplementationAdmissionPublication,
  type AgentControlImplementationAdmissionResult,
  type AgentControlImplementationAdmissionShape,
} from "../Services/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationAdmissionHooks } from "../Services/AgentControlImplementationAdmissionHooks.ts";

const PlanningEvidenceRow = Schema.Struct({
  resultEvidenceId: Schema.String,
  finalizationCommandId: Schema.String,
  finalizationFingerprint: Schema.String,
  markerId: Schema.String,
  markerFingerprint: Schema.String,
  handoffId: Schema.String,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  providerDeliveryId: Schema.String,
  providerInstanceId: Schema.String,
  providerTurnId: Schema.String,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: Schema.String,
  orchestrationStartedEventId: Schema.String,
  orchestrationStartedSequence: PositiveInt,
  orchestrationTerminalEventId: Schema.String,
  orchestrationTerminalSequence: PositiveInt,
  planId: Schema.String,
  planEventId: Schema.String,
  planEventSequence: PositiveInt,
  proposedPlanBytes: Schema.Unknown,
  proposedPlanDigest: Schema.String,
  stageEventId: EventId,
  stageEventSequence: PositiveInt,
  leaseEventId: EventId,
  leaseEventSequence: PositiveInt,
  leaseEventStreamVersion: PositiveInt,
  finalizedAt: IsoDateTime,
});
type PlanningEvidence = Omit<typeof PlanningEvidenceRow.Type, "proposedPlanBytes"> & {
  readonly proposedPlanJson: string;
};
const decodePlanningEvidence = Schema.decodeUnknownEffect(PlanningEvidenceRow);

const AdmissionReplayRow = Schema.Struct({
  ...PlanningEvidenceRow.fields,
  admissionEvidenceId: Schema.String,
  admissionCommandId: CommandId,
  admissionFingerprint: Schema.String,
  worktreeReservationId: AgentControlWorktreeReservationId,
  worktreeRevision: PositiveInt,
  worktreeEventSequence: PositiveInt,
  worktreeOwnershipFingerprint: Schema.String,
  worktreeVerifiedAt: IsoDateTime,
  implementationStageRunId: AgentControlStageRunId,
  implementationAttemptId: AgentControlAttemptId,
  implementationStageEventId: EventId,
  implementationStageEventSequence: PositiveInt,
  implementationLeaseId: AgentControlStageRunLeaseId,
  implementationLeaseHolderId: AgentControlStageRunLeaseHolderId,
  implementationFenceToken: PositiveInt,
  implementationLeaseEventId: EventId,
  implementationLeaseEventSequence: PositiveInt,
  implementationLeaseEventStreamVersion: PositiveInt,
  implementationControlledThreadReservationId: AgentControlControlledThreadReservationId,
  implementationThreadId: ThreadId,
  implementationReservationEventId: EventId,
  implementationReservationEventSequence: PositiveInt,
  admittedAt: IsoDateTime,
  receiptId: Schema.String,
  receiptCommandId: CommandId,
  receiptFingerprint: Schema.String,
  receiptEvidenceId: Schema.String,
  receiptHandoffId: Schema.String,
  receiptAcceptedAt: IsoDateTime,
  admissionMarkerId: Schema.String,
  admissionMarkerFingerprint: Schema.String,
  markerCommandId: CommandId,
  markerEvidenceId: Schema.String,
  markerReceiptId: Schema.String,
  markerHandoffId: Schema.String,
  markerCommittedAt: IsoDateTime,
});
type AdmissionReplay = Omit<typeof AdmissionReplayRow.Type, "proposedPlanBytes"> & {
  readonly proposedPlanJson: string;
};
const decodeAdmissionReplay = Schema.decodeUnknownEffect(AdmissionReplayRow);

const isAdmissionError = Schema.is(AgentControlImplementationAdmissionError);
const admissionError = (
  handoffId: string,
  operation: string,
  reason: AgentControlImplementationAdmissionError["reason"],
  cause?: unknown,
) =>
  new AgentControlImplementationAdmissionError({
    handoffId,
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const admissionFingerprintParts = (row: {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly finalizationCommandId: string;
  readonly finalizationFingerprint: string;
  readonly markerId: string;
  readonly markerFingerprint: string;
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
}) => [
  row.handoffId,
  row.resultEvidenceId,
  row.finalizationCommandId,
  row.finalizationFingerprint,
  row.markerId,
  row.markerFingerprint,
  row.projectId,
  row.taskId,
  String(row.taskRevision),
  String(row.githubIntakeSequence),
  row.sourceIdentityFingerprint,
  row.controlledThreadReservationId,
  row.threadId,
  row.stageRunId,
  row.attemptId,
  row.leaseId,
  row.leaseHolderId,
  String(row.fenceToken),
  row.providerDeliveryId,
  row.providerInstanceId,
  row.providerTurnId,
  row.runtimeMode,
  row.modelSelectionFingerprint,
  row.orchestrationStartedEventId,
  String(row.orchestrationStartedSequence),
  row.orchestrationTerminalEventId,
  String(row.orchestrationTerminalSequence),
  row.stageEventId,
  String(row.stageEventSequence),
  row.leaseEventId,
  String(row.leaseEventSequence),
  String(row.leaseEventStreamVersion),
  row.finalizedAt,
  row.planId,
  row.planEventId,
  String(row.planEventSequence),
  row.proposedPlanDigest,
  row.worktreeReservationId,
  String(row.worktreeRevision),
  String(row.worktreeEventSequence),
  row.worktreeOwnershipFingerprint,
  row.worktreeVerifiedAt,
  row.implementationStageRunId,
  row.implementationAttemptId,
  row.implementationLeaseId,
  row.implementationLeaseHolderId,
  String(row.implementationFenceToken),
  row.implementationControlledThreadReservationId,
  row.implementationThreadId,
  row.implementationStageEventId,
  String(row.implementationStageEventSequence),
  row.implementationLeaseEventId,
  String(row.implementationLeaseEventSequence),
  String(row.implementationLeaseEventStreamVersion),
  row.implementationReservationEventId,
  String(row.implementationReservationEventSequence),
  row.admittedAt,
];

const decodePlan = <
  A extends { readonly proposedPlanBytes: unknown; readonly proposedPlanDigest: string },
>(
  handoffId: string,
  row: A,
) =>
  Effect.try({
    try: () => {
      const proposedPlanJson = decodeCanonicalUtf8Bytes(row.proposedPlanBytes);
      parseCanonicalJson(proposedPlanJson);
      if (sha256Utf8(proposedPlanJson) !== row.proposedPlanDigest) {
        throw new Error("planning plan digest mismatch");
      }
      const { proposedPlanBytes: _, ...rest } = row;
      return { ...rest, proposedPlanJson };
    },
    catch: (cause) => admissionError(handoffId, "decode-plan", "planning-evidence-corrupt", cause),
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const finalizer = yield* AgentControlInitialPlanningFinalizer;
  const hooks = yield* AgentControlImplementationAdmissionHooks;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const worktreeController = yield* AgentControlWorktreeController;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseProjection = yield* AgentControlStageRunLeaseProjection;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const reservationEvents = yield* AgentControlControlledThreadReservationEventStore;
  const reservationStates = yield* AgentControlControlledThreadReservationStateRepository;
  const reservationProjection = yield* AgentControlControlledThreadReservationProjection;
  const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
  const publications = yield* PubSub.unbounded<AgentControlImplementationAdmissionPublication>();

  const readPlanningEvidence = (handoffId: string) =>
    sql<Record<string, unknown>>`
      SELECT result.result_evidence_id AS "resultEvidenceId",
        result.finalization_command_id AS "finalizationCommandId",
        result.finalization_fingerprint AS "finalizationFingerprint",
        marker.marker_id AS "markerId", marker.marker_fingerprint AS "markerFingerprint",
        result.handoff_id AS "handoffId", result.project_id AS "projectId",
        result.task_id AS "taskId", result.task_revision AS "taskRevision",
        result.github_intake_sequence AS "githubIntakeSequence",
        result.source_identity_fingerprint AS "sourceIdentityFingerprint",
        result.controlled_thread_reservation_id AS "controlledThreadReservationId",
        result.thread_id AS "threadId", result.stage_run_id AS "stageRunId",
        result.attempt_id AS "attemptId", result.lease_id AS "leaseId",
        result.lease_holder_id AS "leaseHolderId", result.fence_token AS "fenceToken",
        result.provider_delivery_id AS "providerDeliveryId",
        result.provider_instance_id AS "providerInstanceId",
        result.provider_turn_id AS "providerTurnId", result.runtime_mode AS "runtimeMode",
        result.model_selection_fingerprint AS "modelSelectionFingerprint",
        result.orchestration_started_event_id AS "orchestrationStartedEventId",
        result.orchestration_started_sequence AS "orchestrationStartedSequence",
        result.orchestration_terminal_event_id AS "orchestrationTerminalEventId",
        result.orchestration_terminal_sequence AS "orchestrationTerminalSequence",
        result.plan_id AS "planId", result.plan_event_id AS "planEventId",
        result.plan_event_sequence AS "planEventSequence",
        CAST(result.proposed_plan_json AS BLOB) AS "proposedPlanBytes",
        result.proposed_plan_digest AS "proposedPlanDigest",
        result.stage_event_id AS "stageEventId",
        result.stage_event_sequence AS "stageEventSequence",
        result.lease_event_id AS "leaseEventId",
        result.lease_event_sequence AS "leaseEventSequence",
        result.lease_event_stream_version AS "leaseEventStreamVersion",
        result.finalized_at AS "finalizedAt"
      FROM agent_control_initial_planning_result_evidence result
      JOIN agent_control_initial_planning_finalization_receipts receipt
        ON receipt.result_evidence_id = result.result_evidence_id
      JOIN agent_control_initial_planning_finalization_markers marker
        ON marker.result_evidence_id = result.result_evidence_id
      WHERE result.handoff_id = ${handoffId} AND result.outcome = 'succeeded'
    `.pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "read-planning-evidence", "persistence", cause),
      ),
      Effect.flatMap((rows) => {
        if (rows.length === 0) return Effect.succeed(Option.none<PlanningEvidence>());
        if (rows.length !== 1) {
          return Effect.fail(
            admissionError(handoffId, "read-planning-evidence", "planning-evidence-corrupt"),
          );
        }
        return decodePlanningEvidence(rows[0]).pipe(
          Effect.mapError((cause) =>
            admissionError(
              handoffId,
              "decode-planning-evidence",
              "planning-evidence-corrupt",
              cause,
            ),
          ),
          Effect.flatMap((row) => decodePlan(handoffId, row)),
          Effect.map(Option.some),
        );
      }),
    );

  const readAdmissionReplay = (handoffId: string) =>
    Effect.gen(function* () {
      const counts = yield* sql<{
        readonly evidenceCount: number;
        readonly receiptCount: number;
        readonly markerCount: number;
      }>`
        SELECT
          (SELECT count(*) FROM agent_control_implementation_admission_evidence
            WHERE handoff_id = ${handoffId}) AS "evidenceCount",
          (SELECT count(*) FROM agent_control_implementation_admission_receipts
            WHERE handoff_id = ${handoffId}) AS "receiptCount",
          (SELECT count(*) FROM agent_control_implementation_admission_markers
            WHERE handoff_id = ${handoffId}) AS "markerCount"
      `.pipe(
        Effect.mapError((cause) =>
          admissionError(handoffId, "read-replay-counts", "persistence", cause),
        ),
      );
      const count = counts[0];
      if (count === undefined) {
        return yield* admissionError(handoffId, "read-replay-counts", "persistence");
      }
      if (count.evidenceCount === 0 && count.receiptCount === 0 && count.markerCount === 0) {
        return Option.none<AdmissionReplay>();
      }
      if (count.evidenceCount !== 1 || count.receiptCount !== 1 || count.markerCount !== 1) {
        return yield* admissionError(handoffId, "read-replay-counts", "receipt-mismatch");
      }
      const rows = yield* sql<Record<string, unknown>>`
        SELECT evidence.admission_evidence_id AS "admissionEvidenceId",
          evidence.admission_command_id AS "admissionCommandId",
          evidence.admission_fingerprint AS "admissionFingerprint",
          evidence.result_evidence_id AS "resultEvidenceId",
          evidence.planning_finalization_command_id AS "finalizationCommandId",
          evidence.planning_finalization_fingerprint AS "finalizationFingerprint",
          evidence.planning_marker_id AS "markerId",
          evidence.planning_marker_fingerprint AS "markerFingerprint",
          evidence.handoff_id AS "handoffId", evidence.project_id AS "projectId",
          evidence.task_id AS "taskId", evidence.task_revision AS "taskRevision",
          evidence.github_intake_sequence AS "githubIntakeSequence",
          evidence.source_identity_fingerprint AS "sourceIdentityFingerprint",
          evidence.planning_controlled_thread_reservation_id AS "controlledThreadReservationId",
          evidence.planning_thread_id AS "threadId",
          evidence.planning_stage_run_id AS "stageRunId",
          evidence.planning_attempt_id AS "attemptId",
          evidence.planning_lease_id AS "leaseId",
          evidence.planning_lease_holder_id AS "leaseHolderId",
          evidence.planning_fence_token AS "fenceToken",
          evidence.provider_delivery_id AS "providerDeliveryId",
          evidence.provider_instance_id AS "providerInstanceId",
          evidence.provider_turn_id AS "providerTurnId", evidence.runtime_mode AS "runtimeMode",
          evidence.model_selection_fingerprint AS "modelSelectionFingerprint",
          evidence.orchestration_started_event_id AS "orchestrationStartedEventId",
          evidence.orchestration_started_sequence AS "orchestrationStartedSequence",
          evidence.orchestration_terminal_event_id AS "orchestrationTerminalEventId",
          evidence.orchestration_terminal_sequence AS "orchestrationTerminalSequence",
          evidence.plan_id AS "planId", evidence.plan_event_id AS "planEventId",
          evidence.plan_event_sequence AS "planEventSequence",
          CAST(evidence.proposed_plan_json AS BLOB) AS "proposedPlanBytes",
          evidence.proposed_plan_digest AS "proposedPlanDigest",
          evidence.planning_stage_event_id AS "stageEventId",
          evidence.planning_stage_event_sequence AS "stageEventSequence",
          evidence.planning_lease_release_event_id AS "leaseEventId",
          evidence.planning_lease_release_event_sequence AS "leaseEventSequence",
          evidence.planning_lease_release_stream_version AS "leaseEventStreamVersion",
          evidence.planning_finalized_at AS "finalizedAt",
          evidence.worktree_reservation_id AS "worktreeReservationId",
          evidence.worktree_revision AS "worktreeRevision",
          evidence.worktree_event_sequence AS "worktreeEventSequence",
          evidence.worktree_ownership_fingerprint AS "worktreeOwnershipFingerprint",
          evidence.worktree_verified_at AS "worktreeVerifiedAt",
          evidence.implementation_stage_run_id AS "implementationStageRunId",
          evidence.implementation_attempt_id AS "implementationAttemptId",
          evidence.implementation_stage_event_id AS "implementationStageEventId",
          evidence.implementation_stage_event_sequence AS "implementationStageEventSequence",
          evidence.implementation_lease_id AS "implementationLeaseId",
          evidence.implementation_lease_holder_id AS "implementationLeaseHolderId",
          evidence.implementation_fence_token AS "implementationFenceToken",
          evidence.implementation_lease_event_id AS "implementationLeaseEventId",
          evidence.implementation_lease_event_sequence AS "implementationLeaseEventSequence",
          evidence.implementation_lease_event_stream_version AS
            "implementationLeaseEventStreamVersion",
          evidence.implementation_controlled_thread_reservation_id AS
            "implementationControlledThreadReservationId",
          evidence.implementation_thread_id AS "implementationThreadId",
          evidence.implementation_reservation_event_id AS "implementationReservationEventId",
          evidence.implementation_reservation_event_sequence AS
            "implementationReservationEventSequence",
          evidence.admitted_at AS "admittedAt",
          receipt.receipt_id AS "receiptId",
          receipt.admission_command_id AS "receiptCommandId",
          receipt.admission_fingerprint AS "receiptFingerprint",
          receipt.admission_evidence_id AS "receiptEvidenceId",
          receipt.handoff_id AS "receiptHandoffId", receipt.accepted_at AS "receiptAcceptedAt",
          marker.marker_id AS "admissionMarkerId",
          marker.marker_fingerprint AS "admissionMarkerFingerprint",
          marker.admission_command_id AS "markerCommandId",
          marker.admission_evidence_id AS "markerEvidenceId",
          marker.receipt_id AS "markerReceiptId", marker.handoff_id AS "markerHandoffId",
          marker.committed_at AS "markerCommittedAt"
        FROM agent_control_implementation_admission_evidence evidence
        JOIN agent_control_implementation_admission_receipts receipt
          ON receipt.admission_evidence_id = evidence.admission_evidence_id
        JOIN agent_control_implementation_admission_markers marker
          ON marker.admission_evidence_id = evidence.admission_evidence_id
        WHERE evidence.handoff_id = ${handoffId}
      `.pipe(
        Effect.mapError((cause) => admissionError(handoffId, "read-replay", "persistence", cause)),
      );
      if (rows.length !== 1) {
        return yield* admissionError(handoffId, "read-replay", "receipt-mismatch");
      }
      const decoded = yield* decodeAdmissionReplay(rows[0]).pipe(
        Effect.mapError((cause) =>
          admissionError(handoffId, "decode-replay", "receipt-mismatch", cause),
        ),
      );
      return Option.some(yield* decodePlan(handoffId, decoded));
    });

  const replayFirst = Effect.fn("AgentControlImplementationAdmission.replayFirst")(function* (
    handoffId: string,
  ) {
    const replay = yield* readAdmissionReplay(handoffId);
    if (Option.isNone(replay)) return replay;
    const row = replay.value;
    const repair = yield* loadRunOnceRepair(sql, handoffId).pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "repair-replay", "receipt-mismatch", cause),
      ),
    );
    const expectedCommandId = deriveImplementationAdmissionCommandId(
      row.handoffId,
      row.resultEvidenceId,
    );
    const expectedEvidenceId = deriveImplementationAdmissionEvidenceId(
      row.handoffId,
      row.resultEvidenceId,
    );
    const expectedReceiptId = deriveImplementationAdmissionReceiptId(
      row.handoffId,
      row.resultEvidenceId,
    );
    const expectedMarkerId = deriveImplementationAdmissionMarkerId(
      row.handoffId,
      row.resultEvidenceId,
    );
    const expectedFingerprint = fingerprintImplementationAdmission(
      "accepted",
      admissionFingerprintParts(row),
    );
    const expectedMarkerFingerprint = fingerprintImplementationAdmission("marker", [
      row.handoffId,
      row.resultEvidenceId,
      row.admissionCommandId,
      row.admissionEvidenceId,
      row.admissionFingerprint,
      row.receiptId,
      row.admittedAt,
    ]);
    if (
      row.admissionCommandId !== expectedCommandId ||
      row.admissionEvidenceId !== expectedEvidenceId ||
      row.receiptId !== expectedReceiptId ||
      row.admissionMarkerId !== expectedMarkerId ||
      row.admissionFingerprint !== expectedFingerprint ||
      row.receiptCommandId !== row.admissionCommandId ||
      row.receiptFingerprint !== row.admissionFingerprint ||
      row.receiptEvidenceId !== row.admissionEvidenceId ||
      row.receiptHandoffId !== row.handoffId ||
      row.receiptAcceptedAt !== row.admittedAt ||
      row.admissionMarkerFingerprint !== expectedMarkerFingerprint ||
      row.markerCommandId !== row.admissionCommandId ||
      row.markerEvidenceId !== row.admissionEvidenceId ||
      row.markerReceiptId !== row.receiptId ||
      row.markerHandoffId !== row.handoffId ||
      row.markerCommittedAt !== row.admittedAt ||
      row.implementationStageEventId !==
        deriveImplementationStagePreparedEventId(row.handoffId, row.resultEvidenceId) ||
      row.implementationLeaseEventId !==
        deriveImplementationLeaseReservedEventId(row.handoffId, row.resultEvidenceId) ||
      row.implementationReservationEventId !==
        deriveImplementationReservationPreparedEventId(row.handoffId, row.resultEvidenceId)
    ) {
      return yield* admissionError(handoffId, "validate-replay-identity", "receipt-mismatch");
    }
    const planningReplay = yield* finalizer
      .processHandoff(Option.isSome(repair) ? repair.value.planningHandoffId : handoffId)
      .pipe(
        Effect.mapError((cause) =>
          admissionError(handoffId, "validate-planning-replay", "planning-evidence-corrupt", cause),
        ),
      );
    if (
      planningReplay._tag !== "Replayed" ||
      planningReplay.resultEvidenceId !== row.resultEvidenceId
    ) {
      return yield* admissionError(handoffId, "validate-planning-replay", "receipt-mismatch");
    }
    const stage = yield* loadAuthoritativeStageRunState(
      row.implementationStageRunId,
      stageEvents,
      stageStates,
    ).pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "replay-stage", "stage-history-corrupt", cause),
      ),
    );
    const lease = yield* loadAuthoritativeLeaseState(
      row.implementationLeaseId,
      leaseEvents,
      leaseStates,
    ).pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "replay-lease", "lease-history-corrupt", cause),
      ),
    );
    const reservation = yield* loadAuthoritativeControlledThreadReservation(
      row.implementationControlledThreadReservationId,
      reservationEvents,
      reservationStates,
    ).pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "replay-reservation", "reservation-history-corrupt", cause),
      ),
    );
    if (Option.isNone(stage) || Option.isNone(lease) || Option.isNone(reservation)) {
      return yield* admissionError(handoffId, "replay-history", "receipt-mismatch");
    }
    const stageEvent = stage.value.events[0];
    const leaseEvent = lease.value.events[row.implementationLeaseEventStreamVersion - 1];
    const preparedStageState = stage.value.statesByVersion[0];
    const reservedLeaseState =
      lease.value.statesByVersion[row.implementationLeaseEventStreamVersion - 1];
    // The projection may already be bound. Validate the admission against its
    // prepared event after the complete stream and current projection were checked.
    const reservationPrefix = yield* reservationEvents
      .readStream(row.implementationControlledThreadReservationId, 0, 1)
      .pipe(
        Effect.mapError((cause) =>
          admissionError(
            handoffId,
            "replay-reservation-prefix",
            "reservation-history-corrupt",
            cause,
          ),
        ),
      );
    const preparedReservationEvent = reservationPrefix[0];
    if (preparedReservationEvent?.eventId !== row.implementationReservationEventId) {
      return yield* admissionError(handoffId, "replay-reservation-prefix", "receipt-mismatch");
    }
    const reservationState = yield* projectAgentControlControlledThreadReservationEvent(
      null,
      preparedReservationEvent,
    ).pipe(
      Effect.mapError((cause) =>
        admissionError(
          handoffId,
          "replay-reservation-prefix",
          "reservation-history-corrupt",
          cause,
        ),
      ),
    );
    if (
      preparedStageState === undefined ||
      preparedStageState.status !== "prepared" ||
      preparedStageState.revision !== 1 ||
      preparedStageState.stageKind !== "implementation" ||
      preparedStageState.stageOrdinal !== (Option.isSome(repair) ? 4 : 2) ||
      preparedStageState.attemptOrdinal !== 1 ||
      preparedStageState.roleId !== "implementer" ||
      preparedStageState.attemptId !== row.implementationAttemptId ||
      stageEvent?.eventId !== row.implementationStageEventId ||
      stageEvent.sequence !== row.implementationStageEventSequence ||
      reservedLeaseState === undefined ||
      reservedLeaseState.status !== "reserved" ||
      reservedLeaseState.stageRunId !== row.implementationStageRunId ||
      reservedLeaseState.attemptId !== row.implementationAttemptId ||
      reservedLeaseState.holderId !== row.implementationLeaseHolderId ||
      reservedLeaseState.fenceToken !== row.implementationFenceToken ||
      leaseEvent?.eventId !== row.implementationLeaseEventId ||
      leaseEvent.sequence !== row.implementationLeaseEventSequence ||
      reservationState.status !== "prepared" ||
      reservationState.revision !== 1 ||
      reservationState.stageRunId !== row.implementationStageRunId ||
      reservationState.attemptId !== row.implementationAttemptId ||
      reservationState.threadId !== row.implementationThreadId ||
      reservationState.leaseId !== row.implementationLeaseId ||
      reservationState.fenceToken !== row.implementationFenceToken ||
      reservationState.worktreeReservationId !== row.worktreeReservationId ||
      reservationState.sequence !== row.implementationReservationEventSequence
    ) {
      return yield* admissionError(handoffId, "validate-replay-history", "receipt-mismatch");
    }
    return replay;
  });

  const processNew = Effect.fn("AgentControlImplementationAdmission.processNew")(function* (
    planning: PlanningEvidence,
    repair: RunOnceRepair | null = null,
  ) {
    const stageOrdinal = repair === null ? 2 : 4;
    const predecessor = repair ?? planning;
    const reservationHistory = yield* loadAuthoritativeControlledThreadReservationTaskHistory(
      planning.projectId,
      planning.taskId,
      reservationEvents,
      reservationStates,
    ).pipe(
      Effect.mapError((cause) =>
        admissionError(
          planning.handoffId,
          "planning-reservation-history",
          "reservation-history-corrupt",
          cause,
        ),
      ),
    );
    const planningReservations = reservationHistory.filter(
      (state) => state.roleId === "planning" && state.stageKind === "planning",
    );
    if (
      planningReservations.length !== 1 ||
      planningReservations[0]?.controlledThreadReservationId !==
        planning.controlledThreadReservationId ||
      planningReservations[0].threadId !== planning.threadId ||
      planningReservations[0].status !== "bound" ||
      planningReservations[0].revision !== 3
    ) {
      return yield* admissionError(
        planning.handoffId,
        "planning-reservation-history",
        "planning-evidence-corrupt",
      );
    }
    const planningReservation = planningReservations[0];

    return yield* worktreeController
      .useReadyWorktree(
        {
          projectId: planning.projectId,
          reservationId: planningReservation.worktreeReservationId,
        },
        (worktree) =>
          sql.withTransaction(
            Effect.gen(function* () {
              const replay = yield* replayFirst(planning.handoffId);
              if (Option.isSome(replay)) {
                return {
                  _tag: "Replayed" as const,
                  resultEvidenceId: replay.value.resultEvidenceId,
                } satisfies AgentControlImplementationAdmissionResult;
              }
              if (worktree.ownershipFingerprint === null || worktree.verifiedAt === null) {
                return yield* admissionError(
                  planning.handoffId,
                  "worktree-revalidation",
                  "worktree-evidence-stale",
                );
              }
              const worktreeOwnershipFingerprint = worktree.ownershipFingerprint;
              const worktreeVerifiedAt = worktree.verifiedAt;
              const useTask = taskGuard.useTaskConsumableInTransaction;
              if (useTask === undefined) {
                return yield* admissionError(
                  planning.handoffId,
                  "task-transaction-boundary",
                  "persistence",
                );
              }
              return yield* useTask(planning.projectId, planning.taskId, (task) =>
                Effect.gen(function* () {
                  if (repair !== null) {
                    const activeRun = yield* sql`SELECT 1 FROM agent_control_run_once_states run
                      JOIN agent_control_project_states project ON project.project_id = run.project_id
                      WHERE run.run_id = ${repair.runId} AND run.project_id = ${planning.projectId}
                        AND run.task_id = ${planning.taskId} AND run.status = 'active'
                        AND run.last_step = 'thread-activated' AND project.mode = 'run-once'
                        AND project.paused_from_mode IS NULL`;
                    if (activeRun.length !== 1)
                      return yield* admissionError(
                        planning.handoffId,
                        "repair-run-inactive",
                        "task-evidence-stale",
                      );
                  }
                  const currentPlanning = yield* finalizer
                    .processHandoff(repair?.planningHandoffId ?? planning.handoffId)
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "planning-revalidation",
                          "planning-evidence-corrupt",
                          cause,
                        ),
                      ),
                    );
                  if (
                    currentPlanning._tag !== "Replayed" ||
                    currentPlanning.resultEvidenceId !== planning.resultEvidenceId
                  ) {
                    return yield* admissionError(
                      planning.handoffId,
                      "planning-revalidation",
                      "planning-evidence-corrupt",
                    );
                  }
                  const sourceFingerprint =
                    yield* deriveAgentControlSourceIdentityFingerprint(task);
                  if (
                    task.source.projectId !== planning.projectId ||
                    task.taskId !== planning.taskId ||
                    task.revision !== planning.taskRevision ||
                    task.githubIntakeSequence !== planning.githubIntakeSequence ||
                    sourceFingerprint !== planning.sourceIdentityFingerprint
                  ) {
                    return yield* admissionError(
                      planning.handoffId,
                      "task-revalidation",
                      "task-evidence-stale",
                    );
                  }
                  const worktreeRows = yield* sql<{ readonly count: number }>`
                    SELECT count(*) AS count
                    FROM agent_control_worktree_reservation_states current
                    WHERE current.reservation_id = ${worktree.reservationId}
                      AND current.project_id = ${planning.projectId}
                      AND current.task_id = ${planning.taskId}
                      AND current.task_revision = ${planning.taskRevision}
                      AND current.github_intake_sequence = ${planning.githubIntakeSequence}
                      AND current.source_identity_fingerprint =
                        ${planning.sourceIdentityFingerprint}
                      AND current.stage_run_id = ${planning.stageRunId}
                      AND current.attempt_id = ${planning.attemptId}
                      AND current.lease_id = ${planning.leaseId}
                      AND current.fence_token = ${planning.fenceToken}
                      AND current.status = 'ready'
                      AND current.revision = ${worktree.revision}
                      AND current.last_event_sequence = ${worktree.sequence}
                      AND current.ownership_fingerprint = ${worktreeOwnershipFingerprint}
                      AND current.verified_at = ${worktreeVerifiedAt}
                      AND current.materialization_phase = 'ownership-marked'
                      AND current.head_commit_sha = current.base_commit_sha
                  `.pipe(
                    Effect.mapError((cause) =>
                      admissionError(
                        planning.handoffId,
                        "worktree-revalidation",
                        "persistence",
                        cause,
                      ),
                    ),
                  );
                  if (worktreeRows[0]?.count !== 1) {
                    return yield* admissionError(
                      planning.handoffId,
                      "worktree-revalidation",
                      "worktree-evidence-stale",
                    );
                  }
                  const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
                    planning.projectId,
                    planning.taskId,
                    stageEvents,
                    stageStates,
                  ).pipe(
                    Effect.mapError((cause) =>
                      admissionError(
                        planning.handoffId,
                        "stage-history",
                        "stage-history-corrupt",
                        cause,
                      ),
                    ),
                  );
                  const planningStages = stageHistory.filter(
                    (state) => state.stageKind === "planning" && state.stageOrdinal === 1,
                  );
                  const implementationStages = stageHistory.filter(
                    (state) =>
                      state.stageKind === "implementation" && state.stageOrdinal === stageOrdinal,
                  );
                  if (
                    planningStages.length !== 1 ||
                    implementationStages.length !== 0 ||
                    planningStages[0]?.stageRunId !== planning.stageRunId ||
                    planningStages[0].attemptId !== planning.attemptId ||
                    planningStages[0].status !== "succeeded" ||
                    planningStages[0].revision !== 3
                  ) {
                    return yield* admissionError(
                      planning.handoffId,
                      "stage-history",
                      "stage-history-corrupt",
                    );
                  }
                  const leaseHistory = yield* loadAuthoritativeLeaseHistory(
                    leaseEvents,
                    leaseStates,
                  ).pipe(
                    Effect.mapError((cause) =>
                      admissionError(
                        planning.handoffId,
                        "lease-history",
                        "lease-history-corrupt",
                        cause,
                      ),
                    ),
                  );
                  const matchingLeases = leaseHistory.filter(
                    (state) =>
                      state.projectId === planning.projectId && state.taskId === planning.taskId,
                  );
                  const lease = matchingLeases[0];
                  const authoritativeLease = yield* loadAuthoritativeLeaseState(
                    planning.leaseId,
                    leaseEvents,
                    leaseStates,
                  ).pipe(
                    Effect.mapError((cause) =>
                      admissionError(
                        planning.handoffId,
                        "lease-stream",
                        "lease-history-corrupt",
                        cause,
                      ),
                    ),
                  );
                  if (
                    matchingLeases.length !== 1 ||
                    lease === undefined ||
                    Option.isNone(authoritativeLease) ||
                    lease.leaseId !== planning.leaseId ||
                    lease.stageRunId !== predecessor.stageRunId ||
                    lease.attemptId !== predecessor.attemptId ||
                    lease.holderId !== predecessor.leaseHolderId ||
                    lease.fenceToken !== predecessor.fenceToken ||
                    lease.status !== "released" ||
                    lease.revision !== predecessor.leaseEventStreamVersion ||
                    authoritativeLease.value.events[predecessor.leaseEventStreamVersion - 1]
                      ?.eventId !== predecessor.leaseEventId
                  ) {
                    return yield* admissionError(
                      planning.handoffId,
                      "lease-history",
                      "lease-history-corrupt",
                    );
                  }
                  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;
                  // The historical holder released this lease. The validated
                  // release and next fence authorize the current runtime's reserve.
                  const implementationStageRunId = yield* deriveAgentControlStageRunId({
                    projectId: planning.projectId,
                    taskId: planning.taskId,
                    taskRevision: planning.taskRevision,
                    githubIntakeSequence: planning.githubIntakeSequence,
                    sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
                    stageKind: "implementation",
                    stageOrdinal,
                  });
                  if (repair !== null && repair.repairStageRunId !== implementationStageRunId) {
                    return yield* admissionError(
                      planning.handoffId,
                      "repair-stage",
                      "identity-mismatch",
                    );
                  }
                  const implementationAttemptId = yield* deriveAgentControlAttemptId(
                    implementationStageRunId,
                    1,
                  );
                  const stableIdentity = {
                    projectId: planning.projectId,
                    taskId: planning.taskId,
                    taskRevision: planning.taskRevision,
                    githubIntakeSequence: planning.githubIntakeSequence,
                    sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
                    stageRunId: implementationStageRunId,
                    attemptId: implementationAttemptId,
                    roleId: AgentControlRoleId.make("implementer"),
                    stageKind: "implementation" as const,
                    stageOrdinal,
                    attemptOrdinal: 1,
                  };
                  const implementationControlledThreadReservationId =
                    yield* deriveAgentControlControlledThreadReservationId(stableIdentity);
                  const implementationThreadId =
                    yield* deriveAgentControlReservedThreadId(stableIdentity);
                  const implementationFenceToken = predecessor.fenceToken + 1;
                  const admissionCommandId = deriveImplementationAdmissionCommandId(
                    planning.handoffId,
                    planning.resultEvidenceId,
                  );
                  const admissionEvidenceId = deriveImplementationAdmissionEvidenceId(
                    planning.handoffId,
                    planning.resultEvidenceId,
                  );
                  const receiptId = deriveImplementationAdmissionReceiptId(
                    planning.handoffId,
                    planning.resultEvidenceId,
                  );
                  const markerId = deriveImplementationAdmissionMarkerId(
                    planning.handoffId,
                    planning.resultEvidenceId,
                  );
                  const admittedAt = DateTime.formatIso(yield* DateTime.now);
                  const leaseDurationMs = Date.parse(lease.expiresAt) - Date.parse(lease.renewedAt);
                  if (!Number.isSafeInteger(leaseDurationMs) || leaseDurationMs <= 0) {
                    return yield* admissionError(
                      planning.handoffId,
                      "lease-duration",
                      "lease-history-corrupt",
                    );
                  }
                  const expiresAt = DateTime.formatIso(
                    DateTime.add(DateTime.makeUnsafe(admittedAt), {
                      milliseconds: leaseDurationMs,
                    }),
                  );
                  const observation = {
                    handoffId: planning.handoffId,
                    resultEvidenceId: planning.resultEvidenceId,
                    implementationStageRunId,
                    implementationLeaseId: planning.leaseId,
                    implementationControlledThreadReservationId,
                  };
                  yield* hooks.afterAuthoritativeRead(observation);
                  yield* hooks.beforeWrites(observation);

                  const stageDraft: AgentControlStageRunEventDraft = {
                    eventId: deriveImplementationStagePreparedEventId(
                      planning.handoffId,
                      planning.resultEvidenceId,
                    ),
                    type: "agentControl.stageRun.prepared",
                    aggregateKind: "stage-run",
                    aggregateId: implementationStageRunId,
                    occurredAt: admittedAt,
                    commandId: admissionCommandId,
                    causationEventId: null,
                    correlationId: admissionCommandId,
                    authority: "controller",
                    metadata: { schemaVersion: 1 },
                    payload: {
                      projectId: planning.projectId,
                      taskId: planning.taskId,
                      stageRunId: implementationStageRunId,
                      attemptId: implementationAttemptId,
                      roleId: stableIdentity.roleId,
                      stageKind: "implementation",
                      stageOrdinal,
                      attemptOrdinal: 1,
                      status: "prepared",
                      taskRevision: planning.taskRevision,
                      githubIntakeSequence: planning.githubIntakeSequence,
                      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
                      preparedAt: admittedAt,
                    },
                  };
                  const committedStageEvents = yield* stageEvents
                    .append({
                      stageRunId: implementationStageRunId,
                      expectedStreamVersion: 0,
                      events: [stageDraft],
                    })
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "append-stage",
                          cause._tag === "AgentControlStageRunStreamVersionConflictError"
                            ? "revision-conflict"
                            : "persistence",
                          cause,
                        ),
                      ),
                    );
                  const stageEvent = committedStageEvents[0];
                  if (stageEvent === undefined) {
                    return yield* admissionError(planning.handoffId, "append-stage", "persistence");
                  }
                  yield* stageProjection
                    .projectEvent(stageEvent)
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "project-stage",
                          "stage-history-corrupt",
                          cause,
                        ),
                      ),
                    );

                  const leaseDraft: AgentControlStageRunLeaseEventDraft = {
                    eventId: deriveImplementationLeaseReservedEventId(
                      planning.handoffId,
                      planning.resultEvidenceId,
                    ),
                    type: "agentControl.stageRunLease.reserved",
                    aggregateKind: "stage-run-lease",
                    aggregateId: planning.leaseId,
                    occurredAt: admittedAt,
                    commandId: admissionCommandId,
                    causationEventId: null,
                    correlationId: admissionCommandId,
                    authority: "controller",
                    metadata: { schemaVersion: 1 },
                    payload: {
                      leaseId: planning.leaseId,
                      projectId: planning.projectId,
                      taskId: planning.taskId,
                      stageRunId: implementationStageRunId,
                      attemptId: implementationAttemptId,
                      taskRevision: planning.taskRevision,
                      githubIntakeSequence: planning.githubIntakeSequence,
                      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
                      holderId: runtimeHolderId,
                      fenceToken: implementationFenceToken,
                      acquiredAt: admittedAt,
                      renewedAt: admittedAt,
                      expiresAt,
                    },
                  };
                  const committedLeaseEvents = yield* leaseEvents
                    .append({
                      leaseId: planning.leaseId,
                      expectedStreamVersion: lease.revision,
                      events: [leaseDraft],
                    })
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "append-lease",
                          cause._tag === "AgentControlStageRunLeaseStreamVersionConflictError"
                            ? "revision-conflict"
                            : "persistence",
                          cause,
                        ),
                      ),
                    );
                  const leaseEvent = committedLeaseEvents[0];
                  if (leaseEvent === undefined) {
                    return yield* admissionError(planning.handoffId, "append-lease", "persistence");
                  }
                  yield* leaseProjection
                    .projectEvent(leaseEvent)
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "project-lease",
                          "lease-history-corrupt",
                          cause,
                        ),
                      ),
                    );

                  const reservationDraft: AgentControlControlledThreadReservationEventDraft = {
                    eventId: deriveImplementationReservationPreparedEventId(
                      planning.handoffId,
                      planning.resultEvidenceId,
                    ),
                    type: "agentControl.controlledThreadReservation.prepared",
                    aggregateKind: "controlled-thread-reservation",
                    aggregateId: implementationControlledThreadReservationId,
                    occurredAt: admittedAt,
                    commandId: admissionCommandId,
                    causationEventId: null,
                    correlationId: admissionCommandId,
                    authority: "controller",
                    metadata: { schemaVersion: 1 },
                    payload: {
                      controlledThreadReservationId: implementationControlledThreadReservationId,
                      threadId: implementationThreadId,
                      projectId: planning.projectId,
                      taskId: planning.taskId,
                      taskRevision: planning.taskRevision,
                      githubIntakeSequence: planning.githubIntakeSequence,
                      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
                      stageRunId: implementationStageRunId,
                      attemptId: implementationAttemptId,
                      roleId: stableIdentity.roleId,
                      stageKind: "implementation",
                      stageOrdinal,
                      attemptOrdinal: 1,
                      leaseId: planning.leaseId,
                      fenceToken: implementationFenceToken,
                      worktreeReservationId: worktree.reservationId,
                      status: "prepared",
                      preparedAt: admittedAt,
                    },
                  };
                  const committedReservationEvents = yield* reservationEvents
                    .appendInTransaction({
                      controlledThreadReservationId: implementationControlledThreadReservationId,
                      expectedStreamVersion: 0,
                      events: [reservationDraft],
                    })
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "append-reservation",
                          cause._tag ===
                            "AgentControlControlledThreadReservationStreamVersionConflictError"
                            ? "revision-conflict"
                            : "persistence",
                          cause,
                        ),
                      ),
                    );
                  const reservationEvent = committedReservationEvents[0];
                  if (reservationEvent === undefined) {
                    return yield* admissionError(
                      planning.handoffId,
                      "append-reservation",
                      "persistence",
                    );
                  }
                  yield* reservationProjection
                    .projectEventInTransaction(reservationEvent)
                    .pipe(
                      Effect.mapError((cause) =>
                        admissionError(
                          planning.handoffId,
                          "project-reservation",
                          "reservation-history-corrupt",
                          cause,
                        ),
                      ),
                    );

                  const fingerprintInput = {
                    ...planning,
                    worktreeReservationId: worktree.reservationId,
                    worktreeRevision: worktree.revision,
                    worktreeEventSequence: worktree.sequence,
                    worktreeOwnershipFingerprint,
                    worktreeVerifiedAt,
                    implementationStageRunId,
                    implementationAttemptId,
                    implementationLeaseId: planning.leaseId,
                    implementationLeaseHolderId: runtimeHolderId,
                    implementationFenceToken,
                    implementationControlledThreadReservationId,
                    implementationThreadId,
                    implementationStageEventId: stageEvent.eventId,
                    implementationStageEventSequence: stageEvent.sequence,
                    implementationLeaseEventId: leaseEvent.eventId,
                    implementationLeaseEventSequence: leaseEvent.sequence,
                    implementationLeaseEventStreamVersion: leaseEvent.streamVersion,
                    implementationReservationEventId: reservationEvent.eventId,
                    implementationReservationEventSequence: reservationEvent.sequence,
                    admittedAt,
                  };
                  const admissionFingerprint = fingerprintImplementationAdmission(
                    "accepted",
                    admissionFingerprintParts(fingerprintInput),
                  );
                  yield* sql`
                    INSERT INTO agent_control_implementation_admission_evidence (
                      admission_evidence_id, admission_command_id, admission_fingerprint,
                      result_evidence_id, planning_finalization_command_id,
                      planning_finalization_fingerprint, planning_marker_id,
                      planning_marker_fingerprint, handoff_id, project_id, task_id,
                      task_revision, github_intake_sequence, source_identity_fingerprint,
                      planning_stage_run_id, planning_attempt_id, planning_thread_id,
                      planning_controlled_thread_reservation_id, provider_delivery_id,
                      provider_instance_id, provider_turn_id, runtime_mode,
                      model_selection_fingerprint, orchestration_started_event_id,
                      orchestration_started_sequence, orchestration_terminal_event_id,
                      orchestration_terminal_sequence, planning_stage_event_id,
                      planning_stage_event_sequence, planning_lease_id,
                      planning_lease_holder_id, planning_fence_token,
                      planning_lease_release_event_id, planning_lease_release_event_sequence,
                      planning_lease_release_stream_version, planning_finalized_at,
                      worktree_reservation_id, worktree_revision, worktree_event_sequence,
                      worktree_ownership_fingerprint, worktree_verified_at,
                      plan_id, plan_event_id, plan_event_sequence, proposed_plan_json,
                      proposed_plan_digest, implementation_stage_run_id,
                      implementation_attempt_id, implementation_stage_event_id,
                      implementation_stage_event_sequence, implementation_lease_id,
                      implementation_lease_holder_id, implementation_fence_token,
                      implementation_lease_event_id, implementation_lease_event_sequence,
                      implementation_lease_event_stream_version,
                      implementation_controlled_thread_reservation_id,
                      implementation_thread_id, implementation_reservation_event_id,
                      implementation_reservation_event_sequence, admitted_at
                    ) VALUES (
                      ${admissionEvidenceId}, ${admissionCommandId}, ${admissionFingerprint},
                      ${planning.resultEvidenceId}, ${planning.finalizationCommandId},
                      ${planning.finalizationFingerprint}, ${planning.markerId},
                      ${planning.markerFingerprint}, ${planning.handoffId}, ${planning.projectId},
                      ${planning.taskId}, ${planning.taskRevision},
                      ${planning.githubIntakeSequence}, ${planning.sourceIdentityFingerprint},
                      ${planning.stageRunId}, ${planning.attemptId}, ${planning.threadId},
                      ${planning.controlledThreadReservationId}, ${planning.providerDeliveryId},
                      ${planning.providerInstanceId}, ${planning.providerTurnId},
                      ${planning.runtimeMode}, ${planning.modelSelectionFingerprint},
                      ${planning.orchestrationStartedEventId},
                      ${planning.orchestrationStartedSequence},
                      ${planning.orchestrationTerminalEventId},
                      ${planning.orchestrationTerminalSequence}, ${planning.stageEventId},
                      ${planning.stageEventSequence}, ${planning.leaseId},
                      ${planning.leaseHolderId}, ${planning.fenceToken},
                      ${planning.leaseEventId}, ${planning.leaseEventSequence},
                      ${planning.leaseEventStreamVersion}, ${planning.finalizedAt},
                      ${worktree.reservationId}, ${worktree.revision}, ${worktree.sequence},
                      ${worktreeOwnershipFingerprint}, ${worktreeVerifiedAt},
                      ${planning.planId}, ${planning.planEventId}, ${planning.planEventSequence},
                      ${planning.proposedPlanJson}, ${planning.proposedPlanDigest},
                      ${implementationStageRunId}, ${implementationAttemptId},
                      ${stageEvent.eventId}, ${stageEvent.sequence}, ${planning.leaseId},
                      ${runtimeHolderId}, ${implementationFenceToken}, ${leaseEvent.eventId},
                      ${leaseEvent.sequence}, ${leaseEvent.streamVersion},
                      ${implementationControlledThreadReservationId}, ${implementationThreadId},
                      ${reservationEvent.eventId}, ${reservationEvent.sequence}, ${admittedAt}
                    )
                  `;
                  yield* sql`
                    INSERT INTO agent_control_implementation_admission_receipts (
                      receipt_id, admission_command_id, admission_fingerprint,
                      admission_evidence_id, handoff_id, implementation_stage_event_id,
                      implementation_stage_event_sequence, implementation_lease_event_id,
                      implementation_lease_event_sequence,
                      implementation_reservation_event_id,
                      implementation_reservation_event_sequence, accepted_at
                    ) VALUES (
                      ${receiptId}, ${admissionCommandId}, ${admissionFingerprint},
                      ${admissionEvidenceId}, ${planning.handoffId}, ${stageEvent.eventId},
                      ${stageEvent.sequence}, ${leaseEvent.eventId}, ${leaseEvent.sequence},
                      ${reservationEvent.eventId}, ${reservationEvent.sequence}, ${admittedAt}
                    )
                  `;
                  yield* hooks.beforeFinalMarker(observation);
                  const markerFingerprint = fingerprintImplementationAdmission("marker", [
                    planning.handoffId,
                    planning.resultEvidenceId,
                    admissionCommandId,
                    admissionEvidenceId,
                    admissionFingerprint,
                    receiptId,
                    admittedAt,
                  ]);
                  yield* sql`
                    INSERT INTO agent_control_implementation_admission_markers (
                      marker_id, marker_fingerprint, admission_command_id,
                      admission_evidence_id, receipt_id, handoff_id, committed_at
                    ) VALUES (
                      ${markerId}, ${markerFingerprint}, ${admissionCommandId},
                      ${admissionEvidenceId}, ${receiptId}, ${planning.handoffId}, ${admittedAt}
                    )
                  `;
                  return {
                    _tag: "Committed" as const,
                    observation,
                    publication: {
                      handoffId: planning.handoffId,
                      resultEvidenceId: planning.resultEvidenceId,
                      stageEvents: committedStageEvents,
                      leaseEvents: committedLeaseEvents,
                      reservationEvents: committedReservationEvents,
                    } satisfies AgentControlImplementationAdmissionPublication,
                  };
                }),
              );
            }),
          ),
        {
          beforeInspection: replayFirst(planning.handoffId).pipe(
            Effect.map(
              Option.map(
                (replay) =>
                  ({
                    _tag: "Replayed" as const,
                    resultEvidenceId: replay.resultEvidenceId,
                  }) satisfies AgentControlImplementationAdmissionResult,
              ),
            ),
          ),
        },
      )
      .pipe(
        Effect.mapError((cause) =>
          isAdmissionError(cause)
            ? cause
            : admissionError(planning.handoffId, "admission-transaction", "persistence", cause),
        ),
      );
  });

  const processHandoff = Effect.fn("AgentControlImplementationAdmission.processHandoff")(function* (
    handoffId: string,
  ) {
    const accepted = yield* replayFirst(handoffId);
    if (Option.isSome(accepted)) {
      return {
        _tag: "Replayed" as const,
        resultEvidenceId: accepted.value.resultEvidenceId,
      } satisfies AgentControlImplementationAdmissionResult;
    }
    const repair = yield* loadRunOnceRepair(sql, handoffId).pipe(
      Effect.mapError((cause) =>
        admissionError(handoffId, "repair-source", "receipt-mismatch", cause),
      ),
    );
    const planningHandoffId = Option.isSome(repair) ? repair.value.planningHandoffId : handoffId;
    const planningResult = yield* finalizer
      .processHandoff(planningHandoffId)
      .pipe(
        Effect.mapError((cause) =>
          admissionError(
            handoffId,
            "planning-finalizer",
            cause.reason === "persistence"
              ? "persistence"
              : cause.reason === "revision-conflict"
                ? "revision-conflict"
                : "planning-evidence-corrupt",
            cause,
          ),
        ),
      );
    if (
      planningResult._tag === "Waiting" ||
      planningResult._tag === "Ambiguous" ||
      planningResult._tag === "Started" ||
      (planningResult._tag === "Finalized" && planningResult.publication.outcome !== "succeeded")
    ) {
      return { _tag: "NotCandidate" as const };
    }
    const planning = yield* readPlanningEvidence(planningHandoffId);
    if (Option.isNone(planning)) return { _tag: "NotCandidate" as const };
    const result = yield* processNew(
      { ...planning.value, handoffId },
      Option.getOrNull(repair),
    ).pipe(
      Effect.catchIf(
        (cause) => cause.reason === "revision-conflict" || cause.reason === "persistence",
        (cause) =>
          replayFirst(handoffId).pipe(
            Effect.flatMap((replay) =>
              Option.isSome(replay)
                ? Effect.succeed({
                    _tag: "Replayed" as const,
                    resultEvidenceId: replay.value.resultEvidenceId,
                  } satisfies AgentControlImplementationAdmissionResult)
                : Effect.fail(cause),
            ),
          ),
      ),
    );
    if (result._tag !== "Committed") return result;
    yield* hooks.afterNativeCommit(result.observation);
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        yield* stageEngine.publishCommitted(result.publication.stageEvents);
        yield* leaseEngine.publishCommitted(result.publication.leaseEvents);
        yield* reservationEngine.publishCommitted(result.publication.reservationEvents);
        yield* PubSub.publish(publications, result.publication);
      }),
    );
    yield* hooks.afterPublication(result.observation);
    return {
      _tag: "Admitted" as const,
      publication: result.publication,
    } satisfies AgentControlImplementationAdmissionResult;
  });

  const recover = Effect.gen(function* () {
    const candidates = yield* sql<{ readonly handoffId: string }>`
      SELECT handoff_id AS "handoffId" FROM agent_control_initial_planning_result_evidence
      UNION
      SELECT json_extract(payload_json, '$.handoffId') AS "handoffId"
      FROM agent_control_events
      WHERE event_type IN (
        'agentControl.stageRun.planningSucceeded',
        'agentControl.stageRun.planningFailed',
        'agentControl.stageRun.planningCancelled'
      )
      ORDER BY "handoffId" ASC
    `.pipe(
      Effect.mapError((cause) =>
        admissionError("recovery", "list-candidates", "persistence", cause),
      ),
    );
    const hasRepairs =
      yield* sql`SELECT 1 FROM sqlite_schema WHERE name = 'agent_control_run_once_repairs'`;
    const repairs =
      hasRepairs.length === 0
        ? []
        : yield* sql<{ handoffId: string }>`
      SELECT verification_handoff_id AS "handoffId" FROM agent_control_run_once_repairs`;
    yield* Effect.forEach(
      [...candidates, ...repairs],
      ({ handoffId }) =>
        processHandoff(handoffId).pipe(
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
            (cause) =>
              Effect.logError("implementation admission candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
                ...(cause.cause === undefined ? {} : { cause: cause.cause }),
              }),
          ),
        ),
      { concurrency: 1, discard: true },
    );
  }).pipe(
    Effect.mapError((cause) =>
      isAdmissionError(cause)
        ? cause
        : admissionError("recovery", "repair-candidates", "persistence", cause),
    ),
  );

  const processSafely = (handoffId: string | null) =>
    (handoffId === null ? recover : processHandoff(handoffId).pipe(Effect.asVoid)).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("implementation admission input failed", {
          handoffId,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlImplementationAdmission.start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(finalizer.streamPublications, (publication) =>
        publication.outcome === "succeeded" ? worker.enqueue(publication.handoffId) : Effect.void,
      ),
    );
    yield* Effect.forkScoped(
      Stream.runForEach(stageEngine.streamDomainEvents, (event) =>
        event.type === "agentControl.stageRun.planningSucceeded"
          ? worker.enqueue(event.payload.handoffId)
          : Effect.void,
      ),
    );
    yield* worker.enqueue(null);
  });

  return {
    processHandoff,
    loadAcceptedEvidence: replayFirst,
    recover,
    start,
    drain: worker.drain,
    streamPublications: Stream.fromPubSub(publications),
  } satisfies AgentControlImplementationAdmissionShape;
});

export const AgentControlImplementationAdmissionLive = Layer.effect(
  AgentControlImplementationAdmission,
  make,
);
