import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
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

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import { AgentControlImplementationHandoffStore } from "../../implementationTurn/Services/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationStageFinalizer } from "../../implementationTurn/Services/AgentControlImplementationStageFinalizer.ts";
import {
  deriveImplementationFinalizationCommandId,
  deriveImplementationFinalizationMarkerId,
  deriveImplementationFinalizationReceiptId,
  deriveImplementationLeaseReleaseEventId,
  deriveImplementationResultEvidenceId,
  deriveImplementationTerminalStageEventId,
  fingerprintImplementationTurn,
} from "../../implementationTurn/identity.ts";
import { loadAgentControlImplementationOrchestrationEvidence } from "../../implementationTurn/orchestrationEvidence.ts";
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
  collectControlledThreadReservationEventsForTask,
  loadAuthoritativeControlledThreadReservationTaskHistory,
} from "../../controlledThreadReservation/authoritative.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../controlledThreadReservation/identity.ts";
import { AgentControlControlledThreadReservationEngine } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationStateRepository.ts";
import { loadAuthoritativeTaskProjectHistory } from "../../task/authoritative.ts";
import { AgentControlTaskEventStore } from "../../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { loadAuthoritativeWorktreeReservation } from "../../worktree/authoritative.ts";
import { AgentControlWorktreeEventStore } from "../../worktree/Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeStateRepository } from "../../worktree/Services/AgentControlWorktreeStateRepository.ts";
import {
  deriveVerificationAdmissionCommandId,
  deriveVerificationAdmissionEvidenceId,
  deriveVerificationAdmissionMarkerId,
  deriveVerificationAdmissionReceiptId,
  deriveVerificationLeaseReservedEventId,
  deriveVerificationReservationPreparedEventId,
  deriveVerificationStagePreparedEventId,
  fingerprintVerificationAdmission,
  type VerificationAdmissionPredecessorIdentity,
} from "../identity.ts";
import {
  AgentControlVerificationAdmission,
  AgentControlVerificationAdmissionError,
  type AgentControlVerificationAdmissionEvidence,
  type AgentControlVerificationAdmissionPublication,
  type AgentControlVerificationAdmissionResult,
  type AgentControlVerificationAdmissionShape,
} from "../Services/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationAdmissionHooks } from "../Services/AgentControlVerificationAdmissionHooks.ts";

const ImplementationResultDbRow = Schema.Struct({
  resultEvidenceIdBytes: Schema.Unknown,
  receiptIdBytes: Schema.Unknown,
  markerIdBytes: Schema.Unknown,
  finalizationCommandIdBytes: Schema.Unknown,
  finalizationFingerprintBytes: Schema.Unknown,
  resultJsonBytes: Schema.Unknown,
  outcomeBytes: Schema.Unknown,
  handoffIdBytes: Schema.Unknown,
  handoffFingerprintBytes: Schema.Unknown,
  projectIdBytes: Schema.Unknown,
  taskIdBytes: Schema.Unknown,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprintBytes: Schema.Unknown,
  repositoryDisplayBytes: Schema.Unknown,
  sourceRevisionBytes: Schema.Unknown,
  worktreeReservationIdBytes: Schema.Unknown,
  worktreeEventIdBytes: Schema.Unknown,
  worktreeEventSequence: PositiveInt,
  worktreeEventStreamVersion: PositiveInt,
  worktreeOwnershipFingerprintBytes: Schema.Unknown,
  stageRunIdBytes: Schema.Unknown,
  attemptIdBytes: Schema.Unknown,
  controlledThreadReservationIdBytes: Schema.Unknown,
  threadIdBytes: Schema.Unknown,
  leaseIdBytes: Schema.Unknown,
  leaseHolderIdBytes: Schema.Unknown,
  fenceToken: PositiveInt,
  stageEventIdBytes: Schema.Unknown,
  stageEventSequence: PositiveInt,
  stageEventStreamVersion: PositiveInt,
  leaseEventIdBytes: Schema.Unknown,
  leaseEventSequence: PositiveInt,
  leaseEventStreamVersion: PositiveInt,
  providerDeliveryIdBytes: Schema.Unknown,
  providerTurnIdBytes: Schema.Unknown,
  deliveryRevision: PositiveInt,
  claimGeneration: PositiveInt,
  attemptCount: PositiveInt,
  runtimeModeBytes: Schema.Unknown,
  modelSelectionFingerprintBytes: Schema.Unknown,
  orchestrationStartedEventIdBytes: Schema.Unknown,
  orchestrationStartedSequence: PositiveInt,
  orchestrationStartedStreamVersion: PositiveInt,
  orchestrationTerminalEventIdBytes: Schema.Unknown,
  orchestrationTerminalSequence: PositiveInt,
  orchestrationTerminalStreamVersion: PositiveInt,
  orchestrationHistoryBytes: Schema.Unknown,
  orchestrationHistoryDigestBytes: Schema.Unknown,
  orchestrationHistoryEventCount: PositiveInt,
  finalizedAtBytes: Schema.Unknown,
  receiptStatusBytes: Schema.Unknown,
  receiptEvidenceIdBytes: Schema.Unknown,
  receiptMarkerIdBytes: Schema.Unknown,
  markerEvidenceIdBytes: Schema.Unknown,
  markerReceiptIdBytes: Schema.Unknown,
  markerFingerprintBytes: Schema.Unknown,
});
const decodeImplementationResultDbRow = Schema.decodeUnknownEffect(ImplementationResultDbRow);

interface ImplementationResult {
  readonly resultEvidenceId: string;
  readonly receiptId: string;
  readonly markerId: string;
  readonly finalizationCommandId: string;
  readonly finalizationFingerprint: string;
  readonly resultJson: string;
  readonly resultDocument: JsonValue;
  readonly outcome: "succeeded";
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly repositoryDisplay: string;
  readonly sourceRevision: string;
  readonly worktreeReservationId: AgentControlWorktreeReservationId;
  readonly worktreeEventId: string;
  readonly worktreeEventSequence: number;
  readonly worktreeEventStreamVersion: number;
  readonly worktreeOwnershipFingerprint: string;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly leaseId: AgentControlStageRunLeaseId;
  readonly leaseHolderId: AgentControlStageRunLeaseHolderId;
  readonly fenceToken: number;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly stageEventStreamVersion: number;
  readonly leaseEventId: string;
  readonly leaseEventSequence: number;
  readonly leaseEventStreamVersion: number;
  readonly providerDeliveryId: string;
  readonly providerTurnId: string;
  readonly deliveryRevision: number;
  readonly claimGeneration: number;
  readonly attemptCount: number;
  readonly runtimeMode: string;
  readonly modelSelectionFingerprint: string;
  readonly orchestrationStartedEventId: string;
  readonly orchestrationStartedSequence: number;
  readonly orchestrationStartedStreamVersion: number;
  readonly orchestrationTerminalEventId: string;
  readonly orchestrationTerminalSequence: number;
  readonly orchestrationTerminalStreamVersion: number;
  readonly orchestrationHistoryJson: string;
  readonly orchestrationHistory: JsonValue;
  readonly orchestrationHistoryDigest: string;
  readonly orchestrationHistoryEventCount: number;
  readonly finalizedAt: string;
}

const ReplayDbRow = Schema.Struct({
  evidenceJsonBytes: Schema.Unknown,
  admissionEvidenceIdBytes: Schema.Unknown,
  receiptIdBytes: Schema.Unknown,
  markerIdBytes: Schema.Unknown,
  admissionCommandIdBytes: Schema.Unknown,
  admissionFingerprintBytes: Schema.Unknown,
  implementationResultEvidenceIdBytes: Schema.Unknown,
  implementationFinalizationFingerprintBytes: Schema.Unknown,
  handoffIdBytes: Schema.Unknown,
  handoffFingerprintBytes: Schema.Unknown,
  projectIdBytes: Schema.Unknown,
  taskIdBytes: Schema.Unknown,
  verificationStageRunIdBytes: Schema.Unknown,
  verificationAttemptIdBytes: Schema.Unknown,
  leaseIdBytes: Schema.Unknown,
  leaseHolderIdBytes: Schema.Unknown,
  verificationFenceToken: PositiveInt,
  verificationControlledThreadReservationIdBytes: Schema.Unknown,
  verificationThreadIdBytes: Schema.Unknown,
  verificationStageEventIdBytes: Schema.Unknown,
  verificationStageEventSequence: PositiveInt,
  verificationLeaseEventIdBytes: Schema.Unknown,
  verificationLeaseEventSequence: PositiveInt,
  verificationReservationEventIdBytes: Schema.Unknown,
  verificationReservationEventSequence: PositiveInt,
  leaseDurationMs: PositiveInt,
  taskHistoryDigestBytes: Schema.Unknown,
  taskHistoryBytes: Schema.Unknown,
  taskHistoryEventCount: PositiveInt,
  worktreeHistoryDigestBytes: Schema.Unknown,
  worktreeHistoryBytes: Schema.Unknown,
  worktreeHistoryEventCount: PositiveInt,
  stageHistoryDigestBytes: Schema.Unknown,
  stageHistoryBytes: Schema.Unknown,
  stageHistoryEventCount: PositiveInt,
  leaseHistoryDigestBytes: Schema.Unknown,
  leaseHistoryBytes: Schema.Unknown,
  leaseHistoryEventCount: PositiveInt,
  reservationHistoryDigestBytes: Schema.Unknown,
  reservationHistoryBytes: Schema.Unknown,
  reservationHistoryEventCount: PositiveInt,
  orchestrationHistoryDigestBytes: Schema.Unknown,
  orchestrationHistoryBytes: Schema.Unknown,
  orchestrationHistoryEventCount: PositiveInt,
  admittedAtBytes: Schema.Unknown,
  receiptEvidenceIdBytes: Schema.Unknown,
  receiptResultEvidenceIdBytes: Schema.Unknown,
  receiptCommandIdBytes: Schema.Unknown,
  receiptFingerprintBytes: Schema.Unknown,
  receiptStatusBytes: Schema.Unknown,
  receiptAcceptedAtBytes: Schema.Unknown,
  markerEvidenceIdBytes: Schema.Unknown,
  markerResultEvidenceIdBytes: Schema.Unknown,
  markerReceiptIdBytes: Schema.Unknown,
  markerCommandIdBytes: Schema.Unknown,
  markerAdmissionFingerprintBytes: Schema.Unknown,
  markerFingerprintBytes: Schema.Unknown,
  markerCommittedAtBytes: Schema.Unknown,
});
const decodeReplayDbRow = Schema.decodeUnknownEffect(ReplayDbRow);

const isAdmissionError = Schema.is(AgentControlVerificationAdmissionError);
const candidateReasons = new Set<AgentControlVerificationAdmissionError["reason"]>([
  "candidate-evidence",
  "partial-replay",
  "identity-mismatch",
  "task-history-corrupt",
  "worktree-history-corrupt",
  "stage-history-corrupt",
  "lease-history-corrupt",
  "reservation-history-corrupt",
  "orchestration-history-corrupt",
]);

const failure = (
  implementationResultEvidenceId: string,
  operation: string,
  reason: AgentControlVerificationAdmissionError["reason"],
  cause?: unknown,
) =>
  new AgentControlVerificationAdmissionError({
    implementationResultEvidenceId,
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const decodeText = (implementationResultEvidenceId: string, operation: string, value: unknown) =>
  Effect.try({
    try: () => decodeCanonicalUtf8Bytes(value),
    catch: (cause) =>
      failure(implementationResultEvidenceId, operation, "identity-mismatch", cause),
  });

const collectGlobal = <A extends { readonly sequence: number }, E>(
  read: (after: number, limit: number) => Effect.Effect<ReadonlyArray<A>, E>,
) =>
  Effect.gen(function* () {
    const result: Array<A> = [];
    let after = 0;
    while (true) {
      const page = yield* read(after, 500);
      if (page.length === 0) return result;
      for (const event of page) {
        if (event.sequence <= after) return yield* Effect.die(new Error("non-monotonic history"));
        after = event.sequence;
        result.push(event);
      }
    }
  });

const collectStream = <A extends { readonly streamVersion: number }, E>(
  read: (after: number, limit: number) => Effect.Effect<ReadonlyArray<A>, E>,
) =>
  Effect.gen(function* () {
    const result: Array<A> = [];
    let after = 0;
    while (true) {
      const page = yield* read(after, 500);
      if (page.length === 0) return result;
      for (const event of page) {
        if (event.streamVersion <= after)
          return yield* Effect.die(new Error("non-monotonic stream"));
        after = event.streamVersion;
        result.push(event);
      }
    }
  });

const isJsonObject = (value: unknown): value is { readonly [key: string]: JsonValue } =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const hasBoundEvent = (
  history: JsonValue,
  input: {
    readonly eventId: string;
    readonly sequence: number;
    readonly streamVersion: number;
    readonly type?: string;
  },
) =>
  Array.isArray(history) &&
  history.some(
    (event) =>
      isJsonObject(event) &&
      event.eventId === input.eventId &&
      event.sequence === input.sequence &&
      event.streamVersion === input.streamVersion &&
      (input.type === undefined || event.type === input.type),
  );

const matchesBoundPrefix = (current: ReadonlyArray<unknown>, bound: JsonValue) =>
  Array.isArray(bound) &&
  current.length >= bound.length &&
  canonicalJson(current.slice(0, bound.length) as JsonValue) === canonicalJson(bound);

const isPersistenceSqlError = (cause: unknown) =>
  typeof cause === "object" &&
  cause !== null &&
  "_tag" in cause &&
  cause._tag === "AgentControlPersistenceSqlError";

const canonicalHistory = (value: ReadonlyArray<unknown>) => {
  const json = canonicalJson(value as JsonValue);
  return { json, digest: sha256Utf8(json), eventCount: value.length };
};

const admissionFingerprintParts = (input: {
  readonly implementationResultEvidenceId: string;
  readonly finalizationFingerprint: string;
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly verificationStageRunId: string;
  readonly verificationAttemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly verificationFenceToken: number;
  readonly verificationControlledThreadReservationId: string;
  readonly verificationThreadId: string;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly leaseEventId: string;
  readonly leaseEventSequence: number;
  readonly reservationEventId: string;
  readonly reservationEventSequence: number;
  readonly taskHistoryDigest: string;
  readonly worktreeHistoryDigest: string;
  readonly stageHistoryDigest: string;
  readonly leaseHistoryDigest: string;
  readonly reservationHistoryDigest: string;
  readonly orchestrationHistoryDigest: string;
  readonly admittedAt: string;
}) => [
  input.implementationResultEvidenceId,
  input.finalizationFingerprint,
  input.handoffId,
  input.handoffFingerprint,
  input.projectId,
  input.taskId,
  input.verificationStageRunId,
  input.verificationAttemptId,
  input.leaseId,
  input.leaseHolderId,
  String(input.verificationFenceToken),
  input.verificationControlledThreadReservationId,
  input.verificationThreadId,
  input.stageEventId,
  String(input.stageEventSequence),
  input.leaseEventId,
  String(input.leaseEventSequence),
  input.reservationEventId,
  String(input.reservationEventSequence),
  input.taskHistoryDigest,
  input.worktreeHistoryDigest,
  input.stageHistoryDigest,
  input.leaseHistoryDigest,
  input.reservationHistoryDigest,
  input.orchestrationHistoryDigest,
  input.admittedAt,
];

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const finalizer = yield* AgentControlImplementationStageFinalizer;
  const handoffs = yield* AgentControlImplementationHandoffStore;
  const taskEvents = yield* AgentControlTaskEventStore;
  const taskStates = yield* AgentControlTaskStateRepository;
  const worktreeEvents = yield* AgentControlWorktreeEventStore;
  const worktreeStates = yield* AgentControlWorktreeStateRepository;
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
  const hooks = yield* AgentControlVerificationAdmissionHooks;
  const publications = yield* PubSub.unbounded<AgentControlVerificationAdmissionPublication>();

  const readOutcome = Effect.fn("AgentControlVerificationAdmission.readOutcome")(function* (
    implementationResultEvidenceId: string,
  ) {
    const rows = yield* sql<{ readonly outcomeBytes: unknown }>`
      SELECT CAST(outcome AS BLOB) AS "outcomeBytes"
      FROM agent_control_implementation_result_evidence
      WHERE result_evidence_id = ${implementationResultEvidenceId}
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "read-outcome", "persistence", cause),
      ),
    );
    if (rows.length === 0) return Option.none<string>();
    if (rows.length !== 1) {
      return yield* failure(implementationResultEvidenceId, "read-outcome", "candidate-evidence");
    }
    return Option.some(
      yield* decodeText(
        implementationResultEvidenceId,
        "decode-outcome-bytes",
        rows[0]!.outcomeBytes,
      ),
    );
  });

  const readImplementationResult = Effect.fn(
    "AgentControlVerificationAdmission.readImplementationResult",
  )(function* (implementationResultEvidenceId: string) {
    const counts = yield* sql<{
      readonly evidenceCount: number;
      readonly receiptCount: number;
      readonly markerCount: number;
    }>`
      SELECT
        (SELECT count(*) FROM agent_control_implementation_result_evidence
          WHERE result_evidence_id = ${implementationResultEvidenceId}) AS "evidenceCount",
        (SELECT count(*) FROM agent_control_implementation_stage_finalization_receipts
          WHERE result_evidence_id = ${implementationResultEvidenceId}) AS "receiptCount",
        (SELECT count(*) FROM agent_control_implementation_stage_finalization_markers
          WHERE result_evidence_id = ${implementationResultEvidenceId}) AS "markerCount"
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "count-finalization-chain", "persistence", cause),
      ),
    );
    const count = counts[0];
    if (
      count === undefined ||
      count.evidenceCount !== 1 ||
      count.receiptCount !== 1 ||
      count.markerCount !== 1
    ) {
      return yield* failure(
        implementationResultEvidenceId,
        "count-finalization-chain",
        "candidate-evidence",
      );
    }
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        CAST(result.result_evidence_id AS BLOB) AS "resultEvidenceIdBytes",
        CAST(result.receipt_id AS BLOB) AS "receiptIdBytes",
        CAST(result.marker_id AS BLOB) AS "markerIdBytes",
        CAST(result.finalization_command_id AS BLOB) AS "finalizationCommandIdBytes",
        CAST(result.finalization_fingerprint AS BLOB) AS "finalizationFingerprintBytes",
        CAST(result.result_json AS BLOB) AS "resultJsonBytes",
        CAST(result.outcome AS BLOB) AS "outcomeBytes",
        CAST(result.handoff_id AS BLOB) AS "handoffIdBytes",
        CAST(result.handoff_fingerprint AS BLOB) AS "handoffFingerprintBytes",
        CAST(result.project_id AS BLOB) AS "projectIdBytes",
        CAST(result.task_id AS BLOB) AS "taskIdBytes",
        result.task_revision AS "taskRevision",
        result.github_intake_sequence AS "githubIntakeSequence",
        CAST(result.source_identity_fingerprint AS BLOB) AS "sourceIdentityFingerprintBytes",
        CAST(result.repository_display AS BLOB) AS "repositoryDisplayBytes",
        CAST(result.source_revision AS BLOB) AS "sourceRevisionBytes",
        CAST(result.worktree_reservation_id AS BLOB) AS "worktreeReservationIdBytes",
        CAST(result.worktree_event_id AS BLOB) AS "worktreeEventIdBytes",
        result.worktree_event_sequence AS "worktreeEventSequence",
        result.worktree_event_stream_version AS "worktreeEventStreamVersion",
        CAST(result.worktree_ownership_fingerprint AS BLOB)
          AS "worktreeOwnershipFingerprintBytes",
        CAST(result.stage_run_id AS BLOB) AS "stageRunIdBytes",
        CAST(result.attempt_id AS BLOB) AS "attemptIdBytes",
        CAST(result.controlled_thread_reservation_id AS BLOB)
          AS "controlledThreadReservationIdBytes",
        CAST(result.thread_id AS BLOB) AS "threadIdBytes",
        CAST(result.lease_id AS BLOB) AS "leaseIdBytes",
        CAST(result.lease_holder_id AS BLOB) AS "leaseHolderIdBytes",
        result.fence_token AS "fenceToken",
        CAST(result.stage_event_id AS BLOB) AS "stageEventIdBytes",
        result.stage_event_sequence AS "stageEventSequence",
        result.stage_event_stream_version AS "stageEventStreamVersion",
        CAST(result.lease_event_id AS BLOB) AS "leaseEventIdBytes",
        result.lease_event_sequence AS "leaseEventSequence",
        result.lease_event_stream_version AS "leaseEventStreamVersion",
        CAST(result.provider_delivery_id AS BLOB) AS "providerDeliveryIdBytes",
        CAST(result.provider_turn_id AS BLOB) AS "providerTurnIdBytes",
        result.delivery_revision AS "deliveryRevision",
        result.claim_generation AS "claimGeneration",
        result.attempt_count AS "attemptCount",
        CAST(result.runtime_mode AS BLOB) AS "runtimeModeBytes",
        CAST(result.model_selection_fingerprint AS BLOB)
          AS "modelSelectionFingerprintBytes",
        CAST(result.orchestration_started_event_id AS BLOB)
          AS "orchestrationStartedEventIdBytes",
        result.orchestration_started_sequence AS "orchestrationStartedSequence",
        result.orchestration_started_stream_version AS "orchestrationStartedStreamVersion",
        CAST(result.orchestration_terminal_event_id AS BLOB)
          AS "orchestrationTerminalEventIdBytes",
        result.orchestration_terminal_sequence AS "orchestrationTerminalSequence",
        result.orchestration_terminal_stream_version AS "orchestrationTerminalStreamVersion",
        CAST(result.orchestration_history_json AS BLOB) AS "orchestrationHistoryBytes",
        CAST(result.orchestration_history_digest AS BLOB)
          AS "orchestrationHistoryDigestBytes",
        result.orchestration_history_event_count AS "orchestrationHistoryEventCount",
        CAST(result.finalized_at AS BLOB) AS "finalizedAtBytes",
        CAST(receipt.status AS BLOB) AS "receiptStatusBytes",
        CAST(receipt.result_evidence_id AS BLOB) AS "receiptEvidenceIdBytes",
        CAST(receipt.marker_id AS BLOB) AS "receiptMarkerIdBytes",
        CAST(marker.result_evidence_id AS BLOB) AS "markerEvidenceIdBytes",
        CAST(marker.receipt_id AS BLOB) AS "markerReceiptIdBytes",
        CAST(marker.marker_fingerprint AS BLOB) AS "markerFingerprintBytes"
      FROM agent_control_implementation_result_evidence result
      JOIN agent_control_implementation_stage_finalization_receipts receipt
        ON receipt.result_evidence_id = result.result_evidence_id
      JOIN agent_control_implementation_stage_finalization_markers marker
        ON marker.result_evidence_id = result.result_evidence_id
      WHERE result.result_evidence_id = ${implementationResultEvidenceId}
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "read-finalization-chain", "persistence", cause),
      ),
    );
    if (rows.length !== 1) {
      return yield* failure(
        implementationResultEvidenceId,
        "read-finalization-chain",
        "candidate-evidence",
      );
    }
    const row = yield* decodeImplementationResultDbRow(rows[0]).pipe(
      Effect.mapError((cause) =>
        failure(
          implementationResultEvidenceId,
          "decode-finalization-storage",
          "candidate-evidence",
          cause,
        ),
      ),
    );
    const textEntries = {
      resultEvidenceId: row.resultEvidenceIdBytes,
      receiptId: row.receiptIdBytes,
      markerId: row.markerIdBytes,
      finalizationCommandId: row.finalizationCommandIdBytes,
      finalizationFingerprint: row.finalizationFingerprintBytes,
      resultJson: row.resultJsonBytes,
      outcome: row.outcomeBytes,
      handoffId: row.handoffIdBytes,
      handoffFingerprint: row.handoffFingerprintBytes,
      projectId: row.projectIdBytes,
      taskId: row.taskIdBytes,
      sourceIdentityFingerprint: row.sourceIdentityFingerprintBytes,
      repositoryDisplay: row.repositoryDisplayBytes,
      sourceRevision: row.sourceRevisionBytes,
      worktreeReservationId: row.worktreeReservationIdBytes,
      worktreeEventId: row.worktreeEventIdBytes,
      worktreeOwnershipFingerprint: row.worktreeOwnershipFingerprintBytes,
      stageRunId: row.stageRunIdBytes,
      attemptId: row.attemptIdBytes,
      controlledThreadReservationId: row.controlledThreadReservationIdBytes,
      threadId: row.threadIdBytes,
      leaseId: row.leaseIdBytes,
      leaseHolderId: row.leaseHolderIdBytes,
      stageEventId: row.stageEventIdBytes,
      leaseEventId: row.leaseEventIdBytes,
      providerDeliveryId: row.providerDeliveryIdBytes,
      providerTurnId: row.providerTurnIdBytes,
      runtimeMode: row.runtimeModeBytes,
      modelSelectionFingerprint: row.modelSelectionFingerprintBytes,
      orchestrationStartedEventId: row.orchestrationStartedEventIdBytes,
      orchestrationTerminalEventId: row.orchestrationTerminalEventIdBytes,
      orchestrationHistoryJson: row.orchestrationHistoryBytes,
      orchestrationHistoryDigest: row.orchestrationHistoryDigestBytes,
      finalizedAt: row.finalizedAtBytes,
      receiptStatus: row.receiptStatusBytes,
      receiptEvidenceId: row.receiptEvidenceIdBytes,
      receiptMarkerId: row.receiptMarkerIdBytes,
      markerEvidenceId: row.markerEvidenceIdBytes,
      markerReceiptId: row.markerReceiptIdBytes,
      markerFingerprint: row.markerFingerprintBytes,
    } as const;
    const decodedEntries = yield* Effect.forEach(
      Object.entries(textEntries),
      ([key, value]) =>
        decodeText(implementationResultEvidenceId, `decode-${key}-bytes`, value).pipe(
          Effect.map((decoded) => [key, decoded] as const),
        ),
      { concurrency: 1 },
    );
    const text = Object.fromEntries(decodedEntries) as Record<keyof typeof textEntries, string>;
    if (text.outcome !== "succeeded") {
      return yield* failure(
        implementationResultEvidenceId,
        "validate-success-outcome",
        "candidate-evidence",
      );
    }
    const resultDocument = yield* Effect.try({
      try: () => parseCanonicalJson(text.resultJson),
      catch: (cause) =>
        failure(implementationResultEvidenceId, "parse-result-json", "identity-mismatch", cause),
    });
    const orchestrationHistory = yield* Effect.try({
      try: () => parseCanonicalJson(text.orchestrationHistoryJson),
      catch: (cause) =>
        failure(
          implementationResultEvidenceId,
          "parse-orchestration-history",
          "orchestration-history-corrupt",
          cause,
        ),
    });
    const predecessor = {
      handoffId: text.handoffId,
      resultEvidenceId: text.resultEvidenceId,
      finalizationFingerprint: text.finalizationFingerprint,
    } satisfies VerificationAdmissionPredecessorIdentity;
    const finalizationCommandId = deriveImplementationFinalizationCommandId(
      text.handoffId,
      text.handoffFingerprint,
    );
    const expectedMarkerFingerprint = fingerprintImplementationTurn("finalization-marker", [
      text.handoffId,
      text.handoffFingerprint,
      String(finalizationCommandId),
      text.resultEvidenceId,
      text.finalizationFingerprint,
      text.stageEventId,
      String(row.stageEventSequence),
      text.leaseEventId,
      String(row.leaseEventSequence),
      text.finalizedAt,
    ]);
    if (
      predecessor.resultEvidenceId !== implementationResultEvidenceId ||
      text.resultEvidenceId !==
        deriveImplementationResultEvidenceId(text.handoffId, text.handoffFingerprint) ||
      text.receiptId !==
        deriveImplementationFinalizationReceiptId(text.handoffId, text.handoffFingerprint) ||
      text.markerId !==
        deriveImplementationFinalizationMarkerId(text.handoffId, text.handoffFingerprint) ||
      text.finalizationCommandId !== finalizationCommandId ||
      text.finalizationFingerprint !==
        fingerprintImplementationTurn("finalization-result", [text.resultJson]) ||
      text.stageEventId !==
        deriveImplementationTerminalStageEventId(text.handoffId, text.handoffFingerprint) ||
      text.leaseEventId !==
        deriveImplementationLeaseReleaseEventId(text.handoffId, text.handoffFingerprint) ||
      text.receiptStatus !== "accepted" ||
      text.receiptEvidenceId !== text.resultEvidenceId ||
      text.receiptMarkerId !== text.markerId ||
      text.markerEvidenceId !== text.resultEvidenceId ||
      text.markerReceiptId !== text.receiptId ||
      text.markerFingerprint !== expectedMarkerFingerprint ||
      text.orchestrationHistoryDigest !== sha256Utf8(text.orchestrationHistoryJson) ||
      !Array.isArray(orchestrationHistory) ||
      orchestrationHistory.length !== row.orchestrationHistoryEventCount
    ) {
      return yield* failure(
        implementationResultEvidenceId,
        "validate-finalization-chain",
        "identity-mismatch",
      );
    }
    return {
      resultEvidenceId: text.resultEvidenceId,
      receiptId: text.receiptId,
      markerId: text.markerId,
      finalizationCommandId: text.finalizationCommandId,
      finalizationFingerprint: text.finalizationFingerprint,
      resultJson: text.resultJson,
      resultDocument,
      outcome: "succeeded",
      handoffId: text.handoffId,
      handoffFingerprint: text.handoffFingerprint,
      projectId: ProjectId.make(text.projectId),
      taskId: AgentControlTaskId.make(text.taskId),
      taskRevision: row.taskRevision,
      githubIntakeSequence: row.githubIntakeSequence,
      sourceIdentityFingerprint: text.sourceIdentityFingerprint,
      repositoryDisplay: text.repositoryDisplay,
      sourceRevision: text.sourceRevision,
      worktreeReservationId: AgentControlWorktreeReservationId.make(text.worktreeReservationId),
      worktreeEventId: text.worktreeEventId,
      worktreeEventSequence: row.worktreeEventSequence,
      worktreeEventStreamVersion: row.worktreeEventStreamVersion,
      worktreeOwnershipFingerprint: text.worktreeOwnershipFingerprint,
      stageRunId: AgentControlStageRunId.make(text.stageRunId),
      attemptId: AgentControlAttemptId.make(text.attemptId),
      controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
        text.controlledThreadReservationId,
      ),
      threadId: ThreadId.make(text.threadId),
      leaseId: AgentControlStageRunLeaseId.make(text.leaseId),
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(text.leaseHolderId),
      fenceToken: row.fenceToken,
      stageEventId: text.stageEventId,
      stageEventSequence: row.stageEventSequence,
      stageEventStreamVersion: row.stageEventStreamVersion,
      leaseEventId: text.leaseEventId,
      leaseEventSequence: row.leaseEventSequence,
      leaseEventStreamVersion: row.leaseEventStreamVersion,
      providerDeliveryId: text.providerDeliveryId,
      providerTurnId: text.providerTurnId,
      deliveryRevision: row.deliveryRevision,
      claimGeneration: row.claimGeneration,
      attemptCount: row.attemptCount,
      runtimeMode: text.runtimeMode,
      modelSelectionFingerprint: text.modelSelectionFingerprint,
      orchestrationStartedEventId: text.orchestrationStartedEventId,
      orchestrationStartedSequence: row.orchestrationStartedSequence,
      orchestrationStartedStreamVersion: row.orchestrationStartedStreamVersion,
      orchestrationTerminalEventId: text.orchestrationTerminalEventId,
      orchestrationTerminalSequence: row.orchestrationTerminalSequence,
      orchestrationTerminalStreamVersion: row.orchestrationTerminalStreamVersion,
      orchestrationHistoryJson: text.orchestrationHistoryJson,
      orchestrationHistory,
      orchestrationHistoryDigest: text.orchestrationHistoryDigest,
      orchestrationHistoryEventCount: row.orchestrationHistoryEventCount,
      finalizedAt: text.finalizedAt,
    } satisfies ImplementationResult;
  });

  const loadImmutablePredecessorContext = Effect.fn(
    "AgentControlVerificationAdmission.loadImmutablePredecessorContext",
  )(function* (candidate: ImplementationResult) {
    const finalization = yield* finalizer
      .processHandoff(candidate.handoffId)
      .pipe(
        Effect.mapError((cause) =>
          failure(
            candidate.resultEvidenceId,
            "validate-finalizer-replay",
            cause.reason === "persistence"
              ? "persistence"
              : cause.reason === "revision-conflict"
                ? "revision-conflict"
                : "candidate-evidence",
            cause,
          ),
        ),
      );
    if (
      finalization._tag !== "Replayed" ||
      finalization.resultEvidenceId !== candidate.resultEvidenceId
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-finalizer-replay",
        "candidate-evidence",
      );
    }

    const claimOption = yield* handoffs
      .loadAcceptedByHandoffId(candidate.handoffId)
      .pipe(
        Effect.mapError((cause) =>
          failure(
            candidate.resultEvidenceId,
            "load-handoff-chain",
            cause.reason === "persistence" ? "persistence" : "candidate-evidence",
            cause,
          ),
        ),
      );
    if (Option.isNone(claimOption)) {
      return yield* failure(candidate.resultEvidenceId, "load-handoff-chain", "candidate-evidence");
    }
    const claim = claimOption.value;
    if (
      claim.evidence.handoffId !== candidate.handoffId ||
      claim.evidence.handoffFingerprint !== candidate.handoffFingerprint ||
      claim.evidence.projectId !== candidate.projectId ||
      claim.evidence.taskId !== candidate.taskId ||
      claim.evidence.taskRevision !== candidate.taskRevision ||
      claim.evidence.githubIntakeSequence !== candidate.githubIntakeSequence ||
      claim.evidence.sourceIdentityFingerprint !== candidate.sourceIdentityFingerprint ||
      claim.evidence.stageRunId !== candidate.stageRunId ||
      claim.evidence.attemptId !== candidate.attemptId ||
      claim.evidence.leaseId !== candidate.leaseId ||
      claim.evidence.leaseHolderId !== candidate.leaseHolderId ||
      claim.evidence.fenceToken !== candidate.fenceToken ||
      claim.evidence.worktreeReservationId !== candidate.worktreeReservationId ||
      claim.evidence.worktreeEventId !== candidate.worktreeEventId ||
      claim.evidence.worktreeEventSequence !== candidate.worktreeEventSequence ||
      claim.evidence.worktreeEventStreamVersion !== candidate.worktreeEventStreamVersion ||
      claim.evidence.worktreeOwnershipFingerprint !== candidate.worktreeOwnershipFingerprint ||
      claim.evidence.controlledThreadReservationId !== candidate.controlledThreadReservationId ||
      claim.evidence.threadId !== candidate.threadId ||
      claim.evidence.providerDeliveryId !== candidate.providerDeliveryId ||
      claim.evidence.providerInstanceId !== claim.delivery.providerInstanceId ||
      claim.evidence.runtimeMode !== candidate.runtimeMode ||
      claim.evidence.modelSelectionFingerprint !== candidate.modelSelectionFingerprint ||
      claim.delivery.state !== "completed" ||
      claim.delivery.providerTurnId !== candidate.providerTurnId ||
      claim.delivery.revision !== candidate.deliveryRevision ||
      claim.delivery.claimGeneration !== candidate.claimGeneration ||
      claim.delivery.attemptCount !== candidate.attemptCount ||
      claim.delivery.terminalAt !== candidate.finalizedAt
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-handoff-chain",
        "identity-mismatch",
      );
    }

    const orchestration = yield* loadAgentControlImplementationOrchestrationEvidence(sql, claim, {
      requireTerminal: true,
    }).pipe(
      Effect.mapError((cause) =>
        failure(
          candidate.resultEvidenceId,
          "load-orchestration-history",
          cause.reason === "persistence" ? "persistence" : "orchestration-history-corrupt",
          cause,
        ),
      ),
    );
    if (
      orchestration._tag !== "Ready" ||
      orchestration.evidence.outcome !== "succeeded" ||
      orchestration.evidence.terminal === null ||
      orchestration.evidence.historyDigest !== candidate.orchestrationHistoryDigest ||
      orchestration.evidence.historyJson !== candidate.orchestrationHistoryJson ||
      orchestration.evidence.history.length !== candidate.orchestrationHistoryEventCount ||
      orchestration.evidence.started.event.eventId !== candidate.orchestrationStartedEventId ||
      orchestration.evidence.started.event.sequence !== candidate.orchestrationStartedSequence ||
      orchestration.evidence.started.streamVersion !==
        candidate.orchestrationStartedStreamVersion ||
      orchestration.evidence.terminal.event.eventId !== candidate.orchestrationTerminalEventId ||
      orchestration.evidence.terminal.event.sequence !== candidate.orchestrationTerminalSequence ||
      orchestration.evidence.terminal.streamVersion !== candidate.orchestrationTerminalStreamVersion
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-orchestration-history",
        "orchestration-history-corrupt",
      );
    }

    return { claim };
  });

  const loadAuthoritativeContext = Effect.fn(
    "AgentControlVerificationAdmission.loadAuthoritativeContext",
  )(function* (candidate: ImplementationResult) {
    const { claim } = yield* loadImmutablePredecessorContext(candidate);

    const taskProjectHistory = yield* loadAuthoritativeTaskProjectHistory(
      candidate.projectId,
      taskEvents,
      taskStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "load-task-history", "task-history-corrupt", cause),
      ),
    );
    const task = taskProjectHistory.filter((state) => state.taskId === candidate.taskId);
    const allTaskEvents = yield* collectGlobal((after, limit) =>
      taskEvents.readGlobal(after, limit),
    ).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "read-task-events", "task-history-corrupt", cause),
      ),
    );
    const authoritativeTaskEvents = allTaskEvents.filter(
      (event) => event.aggregateId === candidate.taskId,
    );
    const taskSourceEvent = authoritativeTaskEvents.find(
      (event) => event.eventId === claim.evidence.taskSourceEventId,
    );
    if (
      task.length !== 1 ||
      task[0]?.revision !== candidate.taskRevision ||
      task[0].githubIntakeSequence !== candidate.githubIntakeSequence ||
      taskSourceEvent?.sequence !== claim.evidence.taskSourceEventSequence ||
      taskSourceEvent.streamVersion !== claim.evidence.taskSourceEventStreamVersion
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-task-history",
        "task-history-corrupt",
      );
    }

    const worktree = yield* loadAuthoritativeWorktreeReservation(
      candidate.worktreeReservationId,
      worktreeEvents,
      worktreeStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(
          candidate.resultEvidenceId,
          "load-worktree-history",
          "worktree-history-corrupt",
          cause,
        ),
      ),
    );
    if (Option.isNone(worktree)) {
      return yield* failure(
        candidate.resultEvidenceId,
        "load-worktree-history",
        "worktree-history-corrupt",
      );
    }
    const boundWorktreeEvent = worktree.value.events[candidate.worktreeEventStreamVersion - 1];
    if (
      worktree.value.state.reservationId !== candidate.worktreeReservationId ||
      worktree.value.state.projectId !== candidate.projectId ||
      worktree.value.state.taskId !== candidate.taskId ||
      worktree.value.state.taskRevision !== candidate.taskRevision ||
      worktree.value.state.githubIntakeSequence !== candidate.githubIntakeSequence ||
      worktree.value.state.sourceIdentityFingerprint !== candidate.sourceIdentityFingerprint ||
      worktree.value.state.ownershipFingerprint !== candidate.worktreeOwnershipFingerprint ||
      worktree.value.state.repository.nameWithOwner !== candidate.repositoryDisplay ||
      worktree.value.state.baseCommitSha !== candidate.sourceRevision ||
      boundWorktreeEvent?.eventId !== candidate.worktreeEventId ||
      boundWorktreeEvent.sequence !== candidate.worktreeEventSequence ||
      boundWorktreeEvent.streamVersion !== candidate.worktreeEventStreamVersion
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-worktree-history",
        "worktree-history-corrupt",
      );
    }

    const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
      candidate.projectId,
      candidate.taskId,
      stageEvents,
      stageStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "load-stage-history", "stage-history-corrupt", cause),
      ),
    );
    const allStageEvents = yield* collectGlobal((after, limit) =>
      stageEvents.readGlobal(after, limit),
    ).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "read-stage-events", "stage-history-corrupt", cause),
      ),
    );
    const authoritativeStageEvents = allStageEvents.filter(
      (event) =>
        event.payload.projectId === candidate.projectId &&
        event.payload.taskId === candidate.taskId,
    );
    const planningStages = stageHistory.filter(
      (state) =>
        state.stageKind === "planning" && state.roleId === "planning" && state.stageOrdinal === 1,
    );
    const implementationStages = stageHistory.filter(
      (state) =>
        state.stageKind === "implementation" &&
        state.roleId === "implementer" &&
        state.stageOrdinal === 2,
    );
    const verificationStages = stageHistory.filter((state) => state.stageKind === "verification");
    const implementationStage = yield* loadAuthoritativeStageRunState(
      candidate.stageRunId,
      stageEvents,
      stageStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(
          candidate.resultEvidenceId,
          "load-implementation-stage-stream",
          "stage-history-corrupt",
          cause,
        ),
      ),
    );
    if (
      planningStages.length !== 1 ||
      planningStages[0]?.status !== "succeeded" ||
      planningStages[0].revision !== 3 ||
      implementationStages.length !== 1 ||
      implementationStages[0]?.stageRunId !== candidate.stageRunId ||
      implementationStages[0].attemptId !== candidate.attemptId ||
      implementationStages[0].status !== "succeeded" ||
      implementationStages[0].revision !== 3 ||
      verificationStages.length !== 0 ||
      Option.isNone(implementationStage) ||
      implementationStage.value.events[2]?.eventId !== candidate.stageEventId ||
      implementationStage.value.events[2]?.sequence !== candidate.stageEventSequence ||
      implementationStage.value.events[2]?.streamVersion !== candidate.stageEventStreamVersion ||
      implementationStage.value.events[2]?.type !== "agentControl.stageRun.implementationSucceeded"
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-stage-history",
        "stage-history-corrupt",
      );
    }

    const taskLeaseHistory = (yield* loadAuthoritativeLeaseHistory(leaseEvents, leaseStates).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "load-lease-history", "lease-history-corrupt", cause),
      ),
    )).filter(
      (state) => state.projectId === candidate.projectId && state.taskId === candidate.taskId,
    );
    const lease = yield* loadAuthoritativeLeaseState(
      candidate.leaseId,
      leaseEvents,
      leaseStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(candidate.resultEvidenceId, "load-lease-stream", "lease-history-corrupt", cause),
      ),
    );
    const implementationRelease = Option.isSome(lease)
      ? lease.value.events[candidate.leaseEventStreamVersion - 1]
      : undefined;
    const implementationReleaseState = Option.isSome(lease)
      ? lease.value.statesByVersion[candidate.leaseEventStreamVersion - 1]
      : undefined;
    if (
      taskLeaseHistory.length !== 1 ||
      taskLeaseHistory[0]?.leaseId !== candidate.leaseId ||
      Option.isNone(lease) ||
      implementationRelease?.eventId !== candidate.leaseEventId ||
      implementationRelease.sequence !== candidate.leaseEventSequence ||
      implementationRelease.type !== "agentControl.stageRunLease.releasedAfterImplementation" ||
      implementationRelease.payload.resultEvidenceId !== candidate.resultEvidenceId ||
      implementationRelease.payload.stageEventId !== candidate.stageEventId ||
      implementationRelease.payload.holderId !== candidate.leaseHolderId ||
      implementationRelease.payload.fenceToken !== candidate.fenceToken ||
      implementationReleaseState?.status !== "released" ||
      implementationReleaseState.holderId !== candidate.leaseHolderId ||
      implementationReleaseState.fenceToken !== candidate.fenceToken ||
      lease.value.events.length !== candidate.leaseEventStreamVersion
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-lease-history",
        "lease-history-corrupt",
      );
    }

    const reservationHistory = yield* loadAuthoritativeControlledThreadReservationTaskHistory(
      candidate.projectId,
      candidate.taskId,
      reservationEvents,
      reservationStates,
    ).pipe(
      Effect.mapError((cause) =>
        failure(
          candidate.resultEvidenceId,
          "load-reservation-history",
          "reservation-history-corrupt",
          cause,
        ),
      ),
    );
    const authoritativeReservationEvents = yield* collectControlledThreadReservationEventsForTask(
      candidate.projectId,
      candidate.taskId,
      reservationEvents,
    ).pipe(
      Effect.mapError((cause) =>
        failure(
          candidate.resultEvidenceId,
          "read-reservation-events",
          "reservation-history-corrupt",
          cause,
        ),
      ),
    );
    const planningReservations = reservationHistory.filter(
      (state) => state.stageKind === "planning" && state.roleId === "planning",
    );
    const implementationReservations = reservationHistory.filter(
      (state) => state.stageKind === "implementation" && state.roleId === "implementer",
    );
    const verificationReservations = reservationHistory.filter(
      (state) => state.stageKind === "verification",
    );
    if (
      planningReservations.length !== 1 ||
      planningReservations[0]?.status !== "bound" ||
      planningReservations[0].revision !== 3 ||
      implementationReservations.length !== 1 ||
      implementationReservations[0]?.controlledThreadReservationId !==
        candidate.controlledThreadReservationId ||
      implementationReservations[0].threadId !== candidate.threadId ||
      implementationReservations[0].status !== "bound" ||
      implementationReservations[0].revision !== 3 ||
      implementationReservations[0].worktreeReservationId !== candidate.worktreeReservationId ||
      verificationReservations.length !== 0
    ) {
      return yield* failure(
        candidate.resultEvidenceId,
        "validate-reservation-history",
        "reservation-history-corrupt",
      );
    }

    const verificationStageRunId = yield* deriveAgentControlStageRunId({
      projectId: candidate.projectId,
      taskId: candidate.taskId,
      taskRevision: candidate.taskRevision,
      githubIntakeSequence: candidate.githubIntakeSequence,
      sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
      stageKind: "verification",
      stageOrdinal: 3,
    });
    const verificationAttemptId = yield* deriveAgentControlAttemptId(verificationStageRunId, 1);
    const stableIdentity = {
      projectId: candidate.projectId,
      taskId: candidate.taskId,
      taskRevision: candidate.taskRevision,
      githubIntakeSequence: candidate.githubIntakeSequence,
      sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
      stageRunId: verificationStageRunId,
      attemptId: verificationAttemptId,
      roleId: AgentControlRoleId.make("verifier"),
      stageKind: "verification" as const,
      stageOrdinal: 3,
      attemptOrdinal: 1,
    };
    const verificationControlledThreadReservationId =
      yield* deriveAgentControlControlledThreadReservationId(stableIdentity);
    const verificationThreadId = yield* deriveAgentControlReservedThreadId(stableIdentity);
    const verificationFenceToken = candidate.fenceToken + 1;
    const leaseDurationMs =
      DateTime.toEpochMillis(DateTime.makeUnsafe(implementationReleaseState.expiresAt)) -
      DateTime.toEpochMillis(DateTime.makeUnsafe(implementationReleaseState.renewedAt));
    if (!Number.isSafeInteger(leaseDurationMs) || leaseDurationMs <= 0) {
      return yield* failure(
        candidate.resultEvidenceId,
        "derive-lease-duration",
        "lease-history-corrupt",
      );
    }

    return {
      claim,
      taskState: task[0]!,
      worktree: worktree.value,
      stageStates: stageHistory,
      lease: lease.value,
      reservationStates: reservationHistory,
      taskEvents: authoritativeTaskEvents,
      worktreeEvents: worktree.value.events,
      stageEvents: authoritativeStageEvents,
      leaseEvents: lease.value.events,
      reservationEvents: authoritativeReservationEvents,
      orchestrationHistory: candidate.orchestrationHistory,
      verificationStageRunId,
      verificationAttemptId,
      verificationControlledThreadReservationId,
      verificationThreadId,
      verificationFenceToken,
      leaseDurationMs,
      stableIdentity,
    };
  });

  const replayFirst = Effect.fn("AgentControlVerificationAdmission.replayFirst")(function* (
    implementationResultEvidenceId: string,
  ) {
    const counts = yield* sql<{
      readonly evidenceCount: number;
      readonly receiptCount: number;
      readonly markerCount: number;
    }>`
      SELECT
        (SELECT count(*) FROM agent_control_verification_admission_evidence
          WHERE implementation_result_evidence_id = ${implementationResultEvidenceId})
          AS "evidenceCount",
        (SELECT count(*) FROM agent_control_verification_admission_receipts
          WHERE implementation_result_evidence_id = ${implementationResultEvidenceId})
          AS "receiptCount",
        (SELECT count(*) FROM agent_control_verification_admission_markers
          WHERE implementation_result_evidence_id = ${implementationResultEvidenceId})
          AS "markerCount"
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "replay-count", "persistence", cause),
      ),
    );
    const count = counts[0];
    if (count === undefined) {
      return yield* failure(implementationResultEvidenceId, "replay-count", "persistence");
    }
    if (count.evidenceCount === 0 && count.receiptCount === 0 && count.markerCount === 0) {
      return Option.none<AgentControlVerificationAdmissionEvidence>();
    }
    if (count.evidenceCount !== 1 || count.receiptCount !== 1 || count.markerCount !== 1) {
      return yield* failure(implementationResultEvidenceId, "replay-count", "partial-replay");
    }

    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        CAST(evidence.evidence_json AS BLOB) AS "evidenceJsonBytes",
        CAST(evidence.admission_evidence_id AS BLOB) AS "admissionEvidenceIdBytes",
        CAST(evidence.receipt_id AS BLOB) AS "receiptIdBytes",
        CAST(evidence.marker_id AS BLOB) AS "markerIdBytes",
        CAST(evidence.admission_command_id AS BLOB) AS "admissionCommandIdBytes",
        CAST(evidence.admission_fingerprint AS BLOB) AS "admissionFingerprintBytes",
        CAST(evidence.implementation_result_evidence_id AS BLOB)
          AS "implementationResultEvidenceIdBytes",
        CAST(evidence.implementation_finalization_fingerprint AS BLOB)
          AS "implementationFinalizationFingerprintBytes",
        CAST(evidence.handoff_id AS BLOB) AS "handoffIdBytes",
        CAST(evidence.handoff_fingerprint AS BLOB) AS "handoffFingerprintBytes",
        CAST(evidence.project_id AS BLOB) AS "projectIdBytes",
        CAST(evidence.task_id AS BLOB) AS "taskIdBytes",
        CAST(evidence.verification_stage_run_id AS BLOB) AS "verificationStageRunIdBytes",
        CAST(evidence.verification_attempt_id AS BLOB) AS "verificationAttemptIdBytes",
        CAST(evidence.lease_id AS BLOB) AS "leaseIdBytes",
        CAST(evidence.lease_holder_id AS BLOB) AS "leaseHolderIdBytes",
        evidence.verification_fence_token AS "verificationFenceToken",
        CAST(evidence.verification_controlled_thread_reservation_id AS BLOB)
          AS "verificationControlledThreadReservationIdBytes",
        CAST(evidence.verification_thread_id AS BLOB) AS "verificationThreadIdBytes",
        CAST(evidence.verification_stage_event_id AS BLOB)
          AS "verificationStageEventIdBytes",
        evidence.verification_stage_event_sequence AS "verificationStageEventSequence",
        CAST(evidence.verification_lease_event_id AS BLOB)
          AS "verificationLeaseEventIdBytes",
        evidence.verification_lease_event_sequence AS "verificationLeaseEventSequence",
        CAST(evidence.verification_reservation_event_id AS BLOB)
          AS "verificationReservationEventIdBytes",
        evidence.verification_reservation_event_sequence
          AS "verificationReservationEventSequence",
        evidence.lease_duration_ms AS "leaseDurationMs",
        CAST(evidence.task_history_digest AS BLOB) AS "taskHistoryDigestBytes",
        CAST(evidence.task_history_json AS BLOB) AS "taskHistoryBytes",
        evidence.task_history_event_count AS "taskHistoryEventCount",
        CAST(evidence.worktree_history_digest AS BLOB) AS "worktreeHistoryDigestBytes",
        CAST(evidence.worktree_history_json AS BLOB) AS "worktreeHistoryBytes",
        evidence.worktree_history_event_count AS "worktreeHistoryEventCount",
        CAST(evidence.stage_history_digest AS BLOB) AS "stageHistoryDigestBytes",
        CAST(evidence.stage_history_json AS BLOB) AS "stageHistoryBytes",
        evidence.stage_history_event_count AS "stageHistoryEventCount",
        CAST(evidence.lease_history_digest AS BLOB) AS "leaseHistoryDigestBytes",
        CAST(evidence.lease_history_json AS BLOB) AS "leaseHistoryBytes",
        evidence.lease_history_event_count AS "leaseHistoryEventCount",
        CAST(evidence.reservation_history_digest AS BLOB) AS "reservationHistoryDigestBytes",
        CAST(evidence.reservation_history_json AS BLOB) AS "reservationHistoryBytes",
        evidence.reservation_history_event_count AS "reservationHistoryEventCount",
        CAST(evidence.orchestration_history_digest AS BLOB)
          AS "orchestrationHistoryDigestBytes",
        CAST(evidence.orchestration_history_json AS BLOB) AS "orchestrationHistoryBytes",
        evidence.orchestration_history_event_count AS "orchestrationHistoryEventCount",
        CAST(evidence.admitted_at AS BLOB) AS "admittedAtBytes",
        CAST(receipt.admission_evidence_id AS BLOB) AS "receiptEvidenceIdBytes",
        CAST(receipt.implementation_result_evidence_id AS BLOB)
          AS "receiptResultEvidenceIdBytes",
        CAST(receipt.admission_command_id AS BLOB) AS "receiptCommandIdBytes",
        CAST(receipt.admission_fingerprint AS BLOB) AS "receiptFingerprintBytes",
        CAST(receipt.status AS BLOB) AS "receiptStatusBytes",
        CAST(receipt.accepted_at AS BLOB) AS "receiptAcceptedAtBytes",
        CAST(marker.admission_evidence_id AS BLOB) AS "markerEvidenceIdBytes",
        CAST(marker.implementation_result_evidence_id AS BLOB)
          AS "markerResultEvidenceIdBytes",
        CAST(marker.receipt_id AS BLOB) AS "markerReceiptIdBytes",
        CAST(marker.admission_command_id AS BLOB) AS "markerCommandIdBytes",
        CAST(marker.admission_fingerprint AS BLOB) AS "markerAdmissionFingerprintBytes",
        CAST(marker.marker_fingerprint AS BLOB) AS "markerFingerprintBytes",
        CAST(marker.committed_at AS BLOB) AS "markerCommittedAtBytes"
      FROM agent_control_verification_admission_evidence evidence
      JOIN agent_control_verification_admission_receipts receipt
        ON receipt.admission_evidence_id = evidence.admission_evidence_id
      JOIN agent_control_verification_admission_markers marker
        ON marker.admission_evidence_id = evidence.admission_evidence_id
      WHERE evidence.implementation_result_evidence_id = ${implementationResultEvidenceId}
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "replay-read", "persistence", cause),
      ),
    );
    if (rows.length !== 1) {
      return yield* failure(implementationResultEvidenceId, "replay-read", "partial-replay");
    }
    const row = yield* decodeReplayDbRow(rows[0]).pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "replay-storage", "identity-mismatch", cause),
      ),
    );
    const textEntries = {
      evidenceJson: row.evidenceJsonBytes,
      admissionEvidenceId: row.admissionEvidenceIdBytes,
      receiptId: row.receiptIdBytes,
      markerId: row.markerIdBytes,
      admissionCommandId: row.admissionCommandIdBytes,
      admissionFingerprint: row.admissionFingerprintBytes,
      resultEvidenceId: row.implementationResultEvidenceIdBytes,
      finalizationFingerprint: row.implementationFinalizationFingerprintBytes,
      handoffId: row.handoffIdBytes,
      handoffFingerprint: row.handoffFingerprintBytes,
      projectId: row.projectIdBytes,
      taskId: row.taskIdBytes,
      stageRunId: row.verificationStageRunIdBytes,
      attemptId: row.verificationAttemptIdBytes,
      leaseId: row.leaseIdBytes,
      leaseHolderId: row.leaseHolderIdBytes,
      reservationId: row.verificationControlledThreadReservationIdBytes,
      threadId: row.verificationThreadIdBytes,
      stageEventId: row.verificationStageEventIdBytes,
      leaseEventId: row.verificationLeaseEventIdBytes,
      reservationEventId: row.verificationReservationEventIdBytes,
      taskHistoryDigest: row.taskHistoryDigestBytes,
      taskHistoryJson: row.taskHistoryBytes,
      worktreeHistoryDigest: row.worktreeHistoryDigestBytes,
      worktreeHistoryJson: row.worktreeHistoryBytes,
      stageHistoryDigest: row.stageHistoryDigestBytes,
      stageHistoryJson: row.stageHistoryBytes,
      leaseHistoryDigest: row.leaseHistoryDigestBytes,
      leaseHistoryJson: row.leaseHistoryBytes,
      reservationHistoryDigest: row.reservationHistoryDigestBytes,
      reservationHistoryJson: row.reservationHistoryBytes,
      orchestrationHistoryDigest: row.orchestrationHistoryDigestBytes,
      orchestrationHistoryJson: row.orchestrationHistoryBytes,
      admittedAt: row.admittedAtBytes,
      receiptEvidenceId: row.receiptEvidenceIdBytes,
      receiptResultEvidenceId: row.receiptResultEvidenceIdBytes,
      receiptCommandId: row.receiptCommandIdBytes,
      receiptFingerprint: row.receiptFingerprintBytes,
      receiptStatus: row.receiptStatusBytes,
      receiptAcceptedAt: row.receiptAcceptedAtBytes,
      markerEvidenceId: row.markerEvidenceIdBytes,
      markerResultEvidenceId: row.markerResultEvidenceIdBytes,
      markerReceiptId: row.markerReceiptIdBytes,
      markerCommandId: row.markerCommandIdBytes,
      markerAdmissionFingerprint: row.markerAdmissionFingerprintBytes,
      markerFingerprint: row.markerFingerprintBytes,
      markerCommittedAt: row.markerCommittedAtBytes,
    } as const;
    const decodedEntries = yield* Effect.forEach(
      Object.entries(textEntries),
      ([key, value]) =>
        decodeText(implementationResultEvidenceId, `replay-decode-${key}`, value).pipe(
          Effect.map((decoded) => [key, decoded] as const),
        ),
      { concurrency: 1 },
    );
    const text = Object.fromEntries(decodedEntries) as Record<keyof typeof textEntries, string>;
    const parseReplayJson = (operation: string, source: string) =>
      Effect.try({
        try: () => parseCanonicalJson(source),
        catch: (cause) =>
          failure(implementationResultEvidenceId, operation, "identity-mismatch", cause),
      });
    const histories = yield* Effect.all({
      task: parseReplayJson("replay-task-history-json", text.taskHistoryJson),
      worktree: parseReplayJson("replay-worktree-history-json", text.worktreeHistoryJson),
      stage: parseReplayJson("replay-stage-history-json", text.stageHistoryJson),
      lease: parseReplayJson("replay-lease-history-json", text.leaseHistoryJson),
      reservation: parseReplayJson("replay-reservation-history-json", text.reservationHistoryJson),
      orchestration: parseReplayJson(
        "replay-orchestration-history-json",
        text.orchestrationHistoryJson,
      ),
    });
    const evidenceDocument = yield* Effect.try({
      try: () => parseCanonicalJson(text.evidenceJson),
      catch: (cause) =>
        failure(implementationResultEvidenceId, "replay-evidence-json", "identity-mismatch", cause),
    });
    const predecessor = {
      handoffId: text.handoffId,
      resultEvidenceId: text.resultEvidenceId,
      finalizationFingerprint: text.finalizationFingerprint,
    } satisfies VerificationAdmissionPredecessorIdentity;
    const fingerprintInput = {
      implementationResultEvidenceId: text.resultEvidenceId,
      finalizationFingerprint: text.finalizationFingerprint,
      handoffId: text.handoffId,
      handoffFingerprint: text.handoffFingerprint,
      projectId: text.projectId,
      taskId: text.taskId,
      verificationStageRunId: text.stageRunId,
      verificationAttemptId: text.attemptId,
      leaseId: text.leaseId,
      leaseHolderId: text.leaseHolderId,
      verificationFenceToken: row.verificationFenceToken,
      verificationControlledThreadReservationId: text.reservationId,
      verificationThreadId: text.threadId,
      stageEventId: text.stageEventId,
      stageEventSequence: row.verificationStageEventSequence,
      leaseEventId: text.leaseEventId,
      leaseEventSequence: row.verificationLeaseEventSequence,
      reservationEventId: text.reservationEventId,
      reservationEventSequence: row.verificationReservationEventSequence,
      taskHistoryDigest: text.taskHistoryDigest,
      worktreeHistoryDigest: text.worktreeHistoryDigest,
      stageHistoryDigest: text.stageHistoryDigest,
      leaseHistoryDigest: text.leaseHistoryDigest,
      reservationHistoryDigest: text.reservationHistoryDigest,
      orchestrationHistoryDigest: text.orchestrationHistoryDigest,
      admittedAt: text.admittedAt,
    };
    const expectedAdmissionFingerprint = fingerprintVerificationAdmission(
      "accepted",
      admissionFingerprintParts(fingerprintInput),
    );
    const expectedMarkerFingerprint = fingerprintVerificationAdmission("marker", [
      text.resultEvidenceId,
      text.admissionCommandId,
      text.admissionEvidenceId,
      expectedAdmissionFingerprint,
      text.receiptId,
      text.admittedAt,
    ]);
    const documentImplementation = isJsonObject(evidenceDocument)
      ? evidenceDocument.implementation
      : undefined;
    const documentVerification = isJsonObject(evidenceDocument)
      ? evidenceDocument.verification
      : undefined;
    const documentHistories = isJsonObject(evidenceDocument)
      ? evidenceDocument.histories
      : undefined;
    if (
      !isJsonObject(evidenceDocument) ||
      evidenceDocument.schemaVersion !== 1 ||
      evidenceDocument.admissionEvidenceId !== text.admissionEvidenceId ||
      evidenceDocument.admissionCommandId !== text.admissionCommandId ||
      evidenceDocument.admissionFingerprint !== text.admissionFingerprint ||
      !isJsonObject(documentImplementation) ||
      documentImplementation.resultEvidenceId !== text.resultEvidenceId ||
      documentImplementation.finalizationFingerprint !== text.finalizationFingerprint ||
      documentImplementation.handoffId !== text.handoffId ||
      documentImplementation.handoffFingerprint !== text.handoffFingerprint ||
      !isJsonObject(documentVerification) ||
      documentVerification.stageKind !== "verification" ||
      documentVerification.roleId !== "verifier" ||
      documentVerification.stageOrdinal !== 3 ||
      documentVerification.attemptOrdinal !== 1 ||
      documentVerification.stageRunId !== text.stageRunId ||
      documentVerification.attemptId !== text.attemptId ||
      documentVerification.leaseId !== text.leaseId ||
      documentVerification.leaseHolderId !== text.leaseHolderId ||
      documentVerification.fenceToken !== row.verificationFenceToken ||
      documentVerification.leaseDurationMs !== row.leaseDurationMs ||
      documentVerification.controlledThreadReservationId !== text.reservationId ||
      documentVerification.threadId !== text.threadId ||
      documentVerification.stageEventId !== text.stageEventId ||
      documentVerification.stageEventSequence !== row.verificationStageEventSequence ||
      documentVerification.leaseEventId !== text.leaseEventId ||
      documentVerification.leaseEventSequence !== row.verificationLeaseEventSequence ||
      documentVerification.reservationEventId !== text.reservationEventId ||
      documentVerification.reservationEventSequence !== row.verificationReservationEventSequence ||
      documentVerification.admittedAt !== text.admittedAt ||
      !isJsonObject(documentHistories) ||
      canonicalJson(documentHistories.task ?? null) !== text.taskHistoryJson ||
      canonicalJson(documentHistories.worktree ?? null) !== text.worktreeHistoryJson ||
      canonicalJson(documentHistories.stage ?? null) !== text.stageHistoryJson ||
      canonicalJson(documentHistories.lease ?? null) !== text.leaseHistoryJson ||
      canonicalJson(documentHistories.reservation ?? null) !== text.reservationHistoryJson ||
      canonicalJson(documentHistories.orchestration ?? null) !== text.orchestrationHistoryJson ||
      !Array.isArray(histories.task) ||
      !Array.isArray(histories.worktree) ||
      !Array.isArray(histories.stage) ||
      !Array.isArray(histories.lease) ||
      !Array.isArray(histories.reservation) ||
      !Array.isArray(histories.orchestration) ||
      histories.task.length !== row.taskHistoryEventCount ||
      histories.worktree.length !== row.worktreeHistoryEventCount ||
      histories.stage.length !== row.stageHistoryEventCount ||
      histories.lease.length !== row.leaseHistoryEventCount ||
      histories.reservation.length !== row.reservationHistoryEventCount ||
      histories.orchestration.length !== row.orchestrationHistoryEventCount ||
      text.resultEvidenceId !== implementationResultEvidenceId ||
      text.admissionCommandId !== deriveVerificationAdmissionCommandId(predecessor) ||
      text.admissionEvidenceId !== deriveVerificationAdmissionEvidenceId(predecessor) ||
      text.receiptId !== deriveVerificationAdmissionReceiptId(predecessor) ||
      text.markerId !== deriveVerificationAdmissionMarkerId(predecessor) ||
      text.stageEventId !== deriveVerificationStagePreparedEventId(predecessor) ||
      text.leaseEventId !== deriveVerificationLeaseReservedEventId(predecessor) ||
      text.reservationEventId !== deriveVerificationReservationPreparedEventId(predecessor) ||
      text.admissionFingerprint !== expectedAdmissionFingerprint ||
      text.receiptEvidenceId !== text.admissionEvidenceId ||
      text.receiptResultEvidenceId !== text.resultEvidenceId ||
      text.receiptCommandId !== text.admissionCommandId ||
      text.receiptFingerprint !== expectedAdmissionFingerprint ||
      text.receiptStatus !== "accepted" ||
      text.receiptAcceptedAt !== text.admittedAt ||
      text.markerEvidenceId !== text.admissionEvidenceId ||
      text.markerResultEvidenceId !== text.resultEvidenceId ||
      text.markerReceiptId !== text.receiptId ||
      text.markerCommandId !== text.admissionCommandId ||
      text.markerAdmissionFingerprint !== expectedAdmissionFingerprint ||
      text.markerFingerprint !== expectedMarkerFingerprint ||
      text.markerCommittedAt !== text.admittedAt ||
      text.taskHistoryDigest !== sha256Utf8(text.taskHistoryJson) ||
      text.worktreeHistoryDigest !== sha256Utf8(text.worktreeHistoryJson) ||
      text.stageHistoryDigest !== sha256Utf8(text.stageHistoryJson) ||
      text.leaseHistoryDigest !== sha256Utf8(text.leaseHistoryJson) ||
      text.reservationHistoryDigest !== sha256Utf8(text.reservationHistoryJson) ||
      text.orchestrationHistoryDigest !== sha256Utf8(text.orchestrationHistoryJson)
    ) {
      return yield* failure(implementationResultEvidenceId, "replay-identity", "identity-mismatch");
    }

    const candidate = yield* readImplementationResult(implementationResultEvidenceId);
    const { claim } = yield* loadImmutablePredecessorContext(candidate);
    const verificationStageRunId = yield* deriveAgentControlStageRunId({
      projectId: candidate.projectId,
      taskId: candidate.taskId,
      taskRevision: candidate.taskRevision,
      githubIntakeSequence: candidate.githubIntakeSequence,
      sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
      stageKind: "verification",
      stageOrdinal: 3,
    });
    const verificationAttemptId = yield* deriveAgentControlAttemptId(verificationStageRunId, 1);
    const verificationIdentity = {
      projectId: candidate.projectId,
      taskId: candidate.taskId,
      taskRevision: candidate.taskRevision,
      githubIntakeSequence: candidate.githubIntakeSequence,
      sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
      stageRunId: verificationStageRunId,
      attemptId: verificationAttemptId,
      roleId: AgentControlRoleId.make("verifier"),
      stageKind: "verification" as const,
      stageOrdinal: 3,
      attemptOrdinal: 1,
    };
    const verificationControlledThreadReservationId =
      yield* deriveAgentControlControlledThreadReservationId(verificationIdentity);
    const verificationThreadId = yield* deriveAgentControlReservedThreadId(verificationIdentity);
    const mapHistoryError = (
      operation: string,
      reason:
        | "task-history-corrupt"
        | "worktree-history-corrupt"
        | "stage-history-corrupt"
        | "lease-history-corrupt"
        | "reservation-history-corrupt",
    ) =>
      Effect.mapError((cause: unknown) =>
        failure(
          implementationResultEvidenceId,
          operation,
          isPersistenceSqlError(cause) ? "persistence" : reason,
          cause,
        ),
      );
    const currentTaskHistory = yield* collectStream((after, limit) =>
      taskEvents.readStream(candidate.taskId, after, limit),
    ).pipe(mapHistoryError("replay-task-history", "task-history-corrupt"));
    const currentWorktreeHistory = yield* collectStream((after, limit) =>
      worktreeEvents.readStream(candidate.worktreeReservationId, after, limit),
    ).pipe(mapHistoryError("replay-worktree-history", "worktree-history-corrupt"));
    const currentStageHistory = (yield* collectGlobal((after, limit) =>
      stageEvents.readGlobal(after, limit),
    ).pipe(mapHistoryError("replay-stage-history", "stage-history-corrupt"))).filter(
      (event) =>
        event.payload.projectId === candidate.projectId &&
        event.payload.taskId === candidate.taskId,
    );
    const currentLeaseHistory = yield* collectStream((after, limit) =>
      leaseEvents.readStream(candidate.leaseId, after, limit),
    ).pipe(mapHistoryError("replay-lease-history", "lease-history-corrupt"));
    const currentReservationHistory = yield* collectControlledThreadReservationEventsForTask(
      candidate.projectId,
      candidate.taskId,
      reservationEvents,
    ).pipe(mapHistoryError("replay-reservation-history", "reservation-history-corrupt"));
    if (
      verificationStageRunId !== text.stageRunId ||
      verificationAttemptId !== text.attemptId ||
      verificationControlledThreadReservationId !== text.reservationId ||
      verificationThreadId !== text.threadId ||
      candidate.leaseId !== text.leaseId ||
      candidate.leaseHolderId !== text.leaseHolderId ||
      candidate.fenceToken + 1 !== row.verificationFenceToken ||
      candidate.finalizationFingerprint !== text.finalizationFingerprint ||
      candidate.handoffId !== text.handoffId ||
      candidate.handoffFingerprint !== text.handoffFingerprint ||
      canonicalJson(documentImplementation.result ?? null) !== candidate.resultJson ||
      canonicalJson(documentImplementation.handoff ?? null) !==
        canonicalJson(claim.evidence as unknown as JsonValue) ||
      canonicalJson(documentImplementation.delivery ?? null) !==
        canonicalJson(claim.delivery as unknown as JsonValue) ||
      candidate.orchestrationHistoryJson !== text.orchestrationHistoryJson ||
      !matchesBoundPrefix(currentTaskHistory, histories.task) ||
      !matchesBoundPrefix(currentWorktreeHistory, histories.worktree) ||
      !matchesBoundPrefix(currentStageHistory, histories.stage) ||
      !matchesBoundPrefix(currentLeaseHistory, histories.lease) ||
      !matchesBoundPrefix(currentReservationHistory, histories.reservation) ||
      !hasBoundEvent(histories.task, {
        eventId: claim.evidence.taskSourceEventId,
        sequence: claim.evidence.taskSourceEventSequence,
        streamVersion: claim.evidence.taskSourceEventStreamVersion,
        type: "agentControl.task.created",
      }) ||
      !hasBoundEvent(histories.worktree, {
        eventId: candidate.worktreeEventId,
        sequence: candidate.worktreeEventSequence,
        streamVersion: candidate.worktreeEventStreamVersion,
      }) ||
      !hasBoundEvent(histories.stage, {
        eventId: candidate.stageEventId,
        sequence: candidate.stageEventSequence,
        streamVersion: candidate.stageEventStreamVersion,
        type: "agentControl.stageRun.implementationSucceeded",
      }) ||
      !hasBoundEvent(histories.stage, {
        eventId: text.stageEventId,
        sequence: row.verificationStageEventSequence,
        streamVersion: 1,
        type: "agentControl.stageRun.prepared",
      }) ||
      !hasBoundEvent(histories.lease, {
        eventId: candidate.leaseEventId,
        sequence: candidate.leaseEventSequence,
        streamVersion: candidate.leaseEventStreamVersion,
        type: "agentControl.stageRunLease.releasedAfterImplementation",
      }) ||
      !hasBoundEvent(histories.lease, {
        eventId: text.leaseEventId,
        sequence: row.verificationLeaseEventSequence,
        streamVersion: candidate.leaseEventStreamVersion + 1,
        type: "agentControl.stageRunLease.reserved",
      }) ||
      !hasBoundEvent(histories.reservation, {
        eventId: text.reservationEventId,
        sequence: row.verificationReservationEventSequence,
        streamVersion: 1,
        type: "agentControl.controlledThreadReservation.prepared",
      })
    ) {
      return yield* failure(
        implementationResultEvidenceId,
        "replay-bound-histories",
        "identity-mismatch",
      );
    }

    const proposedPlanRows = yield* sql<{ readonly value: unknown }>`
      SELECT CAST(materialization.proposed_plan_json AS BLOB) AS value
      FROM agent_control_implementation_handoff_intents handoff
      JOIN agent_control_implementation_materialization_evidence materialization
        ON materialization.materialization_evidence_id = handoff.materialization_evidence_id
      WHERE handoff.handoff_id = ${text.handoffId}
    `.pipe(
      Effect.mapError((cause) =>
        failure(implementationResultEvidenceId, "replay-proposed-plan", "persistence", cause),
      ),
    );
    if (proposedPlanRows.length !== 1) {
      return yield* failure(
        implementationResultEvidenceId,
        "replay-proposed-plan",
        "identity-mismatch",
      );
    }
    const proposedPlanJson = yield* decodeText(
      implementationResultEvidenceId,
      "replay-proposed-plan-decode",
      proposedPlanRows[0]!.value,
    );
    if (
      sha256Utf8(proposedPlanJson) !== claim.evidence.proposedPlanDigest ||
      canonicalJson(parseCanonicalJson(proposedPlanJson)) !== proposedPlanJson
    ) {
      return yield* failure(
        implementationResultEvidenceId,
        "replay-proposed-plan",
        "identity-mismatch",
      );
    }

    return Option.some({
      admissionEvidenceId: text.admissionEvidenceId,
      admissionCommandId: text.admissionCommandId,
      admissionFingerprint: text.admissionFingerprint,
      implementationResultEvidenceId: text.resultEvidenceId,
      handoffId: text.handoffId,
      verificationStageRunId: text.stageRunId,
      verificationAttemptId: text.attemptId,
      leaseId: text.leaseId,
      leaseHolderId: text.leaseHolderId,
      verificationFenceToken: row.verificationFenceToken,
      verificationControlledThreadReservationId: text.reservationId,
      verificationThreadId: text.threadId,
      receiptId: text.receiptId,
      markerId: text.markerId,
      markerFingerprint: text.markerFingerprint,
      projectId: candidate.projectId,
      taskId: candidate.taskId,
      taskRevision: candidate.taskRevision,
      githubIntakeSequence: candidate.githubIntakeSequence,
      sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
      repositoryDisplay: candidate.repositoryDisplay,
      sourceRevision: candidate.sourceRevision,
      worktreeReservationId: candidate.worktreeReservationId,
      worktreeRevision: claim.evidence.worktreeRevision,
      worktreeEventId: candidate.worktreeEventId,
      worktreeEventSequence: candidate.worktreeEventSequence,
      worktreeEventStreamVersion: candidate.worktreeEventStreamVersion,
      worktreeOwnershipFingerprint: candidate.worktreeOwnershipFingerprint,
      worktreeVerifiedAt: claim.evidence.worktreeVerifiedAt,
      worktreePath: claim.evidence.worktreePath,
      branch: claim.evidence.branch,
      implementationStageRunId: candidate.stageRunId,
      implementationAttemptId: candidate.attemptId,
      implementationFenceToken: candidate.fenceToken,
      implementationControlledThreadReservationId: candidate.controlledThreadReservationId,
      implementationThreadId: candidate.threadId,
      planningThreadId: claim.evidence.planningThreadId,
      planId: claim.evidence.planId,
      proposedPlanJson,
      proposedPlanDigest: claim.evidence.proposedPlanDigest,
      taskSourceEventId: claim.evidence.taskSourceEventId,
      taskSourceEventSequence: claim.evidence.taskSourceEventSequence,
      taskSourceEventStreamVersion: claim.evidence.taskSourceEventStreamVersion,
      verificationStageEventId: text.stageEventId,
      verificationStageEventSequence: row.verificationStageEventSequence,
      verificationStageEventStreamVersion: 1,
      verificationLeaseEventId: text.leaseEventId,
      verificationLeaseEventSequence: row.verificationLeaseEventSequence,
      verificationLeaseEventStreamVersion: candidate.leaseEventStreamVersion + 1,
      verificationReservationEventId: text.reservationEventId,
      verificationReservationEventSequence: row.verificationReservationEventSequence,
      verificationReservationEventStreamVersion: 1,
      verificationAdmissionJson: text.evidenceJson,
      taskHistoryJson: text.taskHistoryJson,
      taskHistoryDigest: text.taskHistoryDigest,
      worktreeHistoryJson: text.worktreeHistoryJson,
      worktreeHistoryDigest: text.worktreeHistoryDigest,
      stageHistoryJson: text.stageHistoryJson,
      stageHistoryDigest: text.stageHistoryDigest,
      leaseHistoryJson: text.leaseHistoryJson,
      leaseHistoryDigest: text.leaseHistoryDigest,
      reservationHistoryJson: text.reservationHistoryJson,
      reservationHistoryDigest: text.reservationHistoryDigest,
      orchestrationHistoryJson: text.orchestrationHistoryJson,
      orchestrationHistoryDigest: text.orchestrationHistoryDigest,
      implementationResultJson: candidate.resultJson,
      implementationHandoffJson: canonicalJson(claim.evidence as unknown as JsonValue),
      implementationProviderDeliveryJson: canonicalJson(claim.delivery as unknown as JsonValue),
      admittedAt: text.admittedAt,
    } satisfies AgentControlVerificationAdmissionEvidence);
  });

  const processNew = Effect.fn("AgentControlVerificationAdmission.processNew")(function* (
    implementationResultEvidenceId: string,
  ) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const replay = yield* replayFirst(implementationResultEvidenceId);
          if (Option.isSome(replay)) {
            return {
              _tag: "Replayed" as const,
              implementationResultEvidenceId,
            } satisfies AgentControlVerificationAdmissionResult;
          }
          const candidate = yield* readImplementationResult(implementationResultEvidenceId);
          const context = yield* loadAuthoritativeContext(candidate);
          const predecessor = {
            handoffId: candidate.handoffId,
            resultEvidenceId: candidate.resultEvidenceId,
            finalizationFingerprint: candidate.finalizationFingerprint,
          } satisfies VerificationAdmissionPredecessorIdentity;
          const admissionCommandId = deriveVerificationAdmissionCommandId(predecessor);
          const admissionEvidenceId = deriveVerificationAdmissionEvidenceId(predecessor);
          const receiptId = deriveVerificationAdmissionReceiptId(predecessor);
          const markerId = deriveVerificationAdmissionMarkerId(predecessor);
          const admittedAt = DateTime.formatIso(yield* DateTime.now);
          const expiresAt = DateTime.formatIso(
            DateTime.add(DateTime.makeUnsafe(admittedAt), {
              milliseconds: context.leaseDurationMs,
            }),
          );
          const observation = {
            implementationResultEvidenceId,
            handoffId: candidate.handoffId,
            verificationStageRunId: context.verificationStageRunId,
            leaseId: candidate.leaseId,
            verificationControlledThreadReservationId:
              context.verificationControlledThreadReservationId,
          };
          yield* hooks.afterAuthoritativeRead(observation);
          yield* hooks.beforeWrites(observation);

          const stageDraft: AgentControlStageRunEventDraft = {
            eventId: deriveVerificationStagePreparedEventId(predecessor),
            type: "agentControl.stageRun.prepared",
            aggregateKind: "stage-run",
            aggregateId: context.verificationStageRunId,
            occurredAt: IsoDateTime.make(admittedAt),
            commandId: admissionCommandId,
            causationEventId: null,
            correlationId: admissionCommandId,
            authority: "controller",
            metadata: { schemaVersion: 1 },
            payload: {
              projectId: candidate.projectId,
              taskId: candidate.taskId,
              stageRunId: context.verificationStageRunId,
              attemptId: context.verificationAttemptId,
              roleId: context.stableIdentity.roleId,
              stageKind: "verification",
              stageOrdinal: 3,
              attemptOrdinal: 1,
              status: "prepared",
              taskRevision: candidate.taskRevision,
              githubIntakeSequence: candidate.githubIntakeSequence,
              sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
              preparedAt: IsoDateTime.make(admittedAt),
            },
          };
          const committedStageEvents = yield* stageEvents
            .append({
              stageRunId: context.verificationStageRunId,
              expectedStreamVersion: 0,
              events: [stageDraft],
            })
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "append-verification-stage",
                  cause._tag === "AgentControlStageRunStreamVersionConflictError"
                    ? "revision-conflict"
                    : "persistence",
                  cause,
                ),
              ),
            );
          const stageEvent = committedStageEvents[0];
          if (stageEvent === undefined || committedStageEvents.length !== 1) {
            return yield* failure(
              implementationResultEvidenceId,
              "append-verification-stage",
              "persistence",
            );
          }
          yield* stageProjection
            .projectEvent(stageEvent)
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "project-verification-stage",
                  "stage-history-corrupt",
                  cause,
                ),
              ),
            );

          const leaseDraft: AgentControlStageRunLeaseEventDraft = {
            eventId: deriveVerificationLeaseReservedEventId(predecessor),
            type: "agentControl.stageRunLease.reserved",
            aggregateKind: "stage-run-lease",
            aggregateId: candidate.leaseId,
            occurredAt: IsoDateTime.make(admittedAt),
            commandId: admissionCommandId,
            causationEventId: null,
            correlationId: admissionCommandId,
            authority: "controller",
            metadata: { schemaVersion: 1 },
            payload: {
              leaseId: candidate.leaseId,
              projectId: candidate.projectId,
              taskId: candidate.taskId,
              stageRunId: context.verificationStageRunId,
              attemptId: context.verificationAttemptId,
              taskRevision: candidate.taskRevision,
              githubIntakeSequence: candidate.githubIntakeSequence,
              sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
              holderId: candidate.leaseHolderId,
              fenceToken: context.verificationFenceToken,
              acquiredAt: IsoDateTime.make(admittedAt),
              renewedAt: IsoDateTime.make(admittedAt),
              expiresAt: IsoDateTime.make(expiresAt),
            },
          };
          const committedLeaseEvents = yield* leaseEvents
            .append({
              leaseId: candidate.leaseId,
              expectedStreamVersion: candidate.leaseEventStreamVersion,
              events: [leaseDraft],
            })
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "append-verification-lease",
                  cause._tag === "AgentControlStageRunLeaseStreamVersionConflictError"
                    ? "revision-conflict"
                    : "persistence",
                  cause,
                ),
              ),
            );
          const leaseEvent = committedLeaseEvents[0];
          if (leaseEvent === undefined || committedLeaseEvents.length !== 1) {
            return yield* failure(
              implementationResultEvidenceId,
              "append-verification-lease",
              "persistence",
            );
          }
          yield* leaseProjection
            .projectEvent(leaseEvent)
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "project-verification-lease",
                  "lease-history-corrupt",
                  cause,
                ),
              ),
            );

          const reservationDraft: AgentControlControlledThreadReservationEventDraft = {
            eventId: deriveVerificationReservationPreparedEventId(predecessor),
            type: "agentControl.controlledThreadReservation.prepared",
            aggregateKind: "controlled-thread-reservation",
            aggregateId: context.verificationControlledThreadReservationId,
            occurredAt: IsoDateTime.make(admittedAt),
            commandId: admissionCommandId,
            causationEventId: null,
            correlationId: admissionCommandId,
            authority: "controller",
            metadata: { schemaVersion: 1 },
            payload: {
              controlledThreadReservationId: context.verificationControlledThreadReservationId,
              threadId: context.verificationThreadId,
              projectId: candidate.projectId,
              taskId: candidate.taskId,
              taskRevision: candidate.taskRevision,
              githubIntakeSequence: candidate.githubIntakeSequence,
              sourceIdentityFingerprint: candidate.sourceIdentityFingerprint,
              stageRunId: context.verificationStageRunId,
              attemptId: context.verificationAttemptId,
              roleId: context.stableIdentity.roleId,
              stageKind: "verification",
              stageOrdinal: 3,
              attemptOrdinal: 1,
              leaseId: candidate.leaseId,
              fenceToken: context.verificationFenceToken,
              worktreeReservationId: candidate.worktreeReservationId,
              status: "prepared",
              preparedAt: IsoDateTime.make(admittedAt),
            },
          };
          const committedReservationEvents = yield* reservationEvents
            .appendInTransaction({
              controlledThreadReservationId: context.verificationControlledThreadReservationId,
              expectedStreamVersion: 0,
              events: [reservationDraft],
            })
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "append-verification-reservation",
                  cause._tag === "AgentControlControlledThreadReservationStreamVersionConflictError"
                    ? "revision-conflict"
                    : "persistence",
                  cause,
                ),
              ),
            );
          const reservationEvent = committedReservationEvents[0];
          if (reservationEvent === undefined || committedReservationEvents.length !== 1) {
            return yield* failure(
              implementationResultEvidenceId,
              "append-verification-reservation",
              "persistence",
            );
          }
          yield* reservationProjection
            .projectEventInTransaction(reservationEvent)
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  implementationResultEvidenceId,
                  "project-verification-reservation",
                  "reservation-history-corrupt",
                  cause,
                ),
              ),
            );

          const taskHistory = canonicalHistory(context.taskEvents);
          const worktreeHistory = canonicalHistory(context.worktreeEvents);
          const stageHistory = canonicalHistory([...context.stageEvents, stageEvent]);
          const leaseHistory = canonicalHistory([...context.leaseEvents, leaseEvent]);
          const reservationHistory = canonicalHistory([
            ...context.reservationEvents,
            reservationEvent,
          ]);
          const fingerprintInput = {
            implementationResultEvidenceId,
            finalizationFingerprint: candidate.finalizationFingerprint,
            handoffId: candidate.handoffId,
            handoffFingerprint: candidate.handoffFingerprint,
            projectId: candidate.projectId,
            taskId: candidate.taskId,
            verificationStageRunId: context.verificationStageRunId,
            verificationAttemptId: context.verificationAttemptId,
            leaseId: candidate.leaseId,
            leaseHolderId: candidate.leaseHolderId,
            verificationFenceToken: context.verificationFenceToken,
            verificationControlledThreadReservationId:
              context.verificationControlledThreadReservationId,
            verificationThreadId: context.verificationThreadId,
            stageEventId: stageEvent.eventId,
            stageEventSequence: stageEvent.sequence,
            leaseEventId: leaseEvent.eventId,
            leaseEventSequence: leaseEvent.sequence,
            reservationEventId: reservationEvent.eventId,
            reservationEventSequence: reservationEvent.sequence,
            taskHistoryDigest: taskHistory.digest,
            worktreeHistoryDigest: worktreeHistory.digest,
            stageHistoryDigest: stageHistory.digest,
            leaseHistoryDigest: leaseHistory.digest,
            reservationHistoryDigest: reservationHistory.digest,
            orchestrationHistoryDigest: candidate.orchestrationHistoryDigest,
            admittedAt,
          };
          const admissionFingerprint = fingerprintVerificationAdmission(
            "accepted",
            admissionFingerprintParts(fingerprintInput),
          );
          const evidenceDocument = {
            schemaVersion: 1,
            admissionEvidenceId,
            admissionCommandId,
            admissionFingerprint,
            implementation: {
              resultEvidenceId: candidate.resultEvidenceId,
              result: candidate.resultDocument,
              finalizationCommandId: candidate.finalizationCommandId,
              finalizationFingerprint: candidate.finalizationFingerprint,
              finalizationReceiptId: candidate.receiptId,
              finalizationMarkerId: candidate.markerId,
              handoffId: candidate.handoffId,
              handoffFingerprint: candidate.handoffFingerprint,
              handoff: context.claim.evidence,
              delivery: context.claim.delivery,
              stageRunId: candidate.stageRunId,
              attemptId: candidate.attemptId,
              controlledThreadReservationId: candidate.controlledThreadReservationId,
              threadId: candidate.threadId,
              terminalStageEventId: candidate.stageEventId,
              leaseReleaseEventId: candidate.leaseEventId,
              leaseId: candidate.leaseId,
              leaseHolderId: candidate.leaseHolderId,
              fenceToken: candidate.fenceToken,
            },
            verification: {
              stageKind: "verification",
              roleId: "verifier",
              stageOrdinal: 3,
              attemptOrdinal: 1,
              stageRunId: context.verificationStageRunId,
              attemptId: context.verificationAttemptId,
              leaseId: candidate.leaseId,
              leaseHolderId: candidate.leaseHolderId,
              fenceToken: context.verificationFenceToken,
              leaseDurationMs: context.leaseDurationMs,
              controlledThreadReservationId: context.verificationControlledThreadReservationId,
              threadId: context.verificationThreadId,
              stageEventId: stageEvent.eventId,
              stageEventSequence: stageEvent.sequence,
              leaseEventId: leaseEvent.eventId,
              leaseEventSequence: leaseEvent.sequence,
              reservationEventId: reservationEvent.eventId,
              reservationEventSequence: reservationEvent.sequence,
              admittedAt,
            },
            histories: {
              task: parseCanonicalJson(taskHistory.json),
              worktree: parseCanonicalJson(worktreeHistory.json),
              stage: parseCanonicalJson(stageHistory.json),
              lease: parseCanonicalJson(leaseHistory.json),
              reservation: parseCanonicalJson(reservationHistory.json),
              orchestration: candidate.orchestrationHistory,
            },
            projections: {
              task: context.taskState,
              worktree: context.worktree.state,
              stage: context.stageStates,
              lease: context.lease.statesByVersion,
              reservation: context.reservationStates,
            },
          };
          const evidenceJson = canonicalJson(evidenceDocument as unknown as JsonValue);

          yield* sql`
            INSERT INTO agent_control_verification_admission_evidence (
              admission_evidence_id, receipt_id, marker_id, admission_command_id,
              admission_fingerprint, evidence_json, implementation_result_evidence_id,
              implementation_result_json, implementation_finalization_fingerprint,
              implementation_finalization_receipt_id,
              implementation_finalization_marker_id, handoff_id, handoff_fingerprint,
              project_id, task_id, task_revision, github_intake_sequence,
              source_identity_fingerprint, repository_display, source_revision,
              worktree_reservation_id, worktree_event_id, worktree_event_sequence,
              worktree_event_stream_version, worktree_ownership_fingerprint,
              implementation_stage_run_id, implementation_attempt_id,
              implementation_controlled_thread_reservation_id, implementation_thread_id,
              implementation_terminal_stage_event_id,
              implementation_terminal_stage_event_sequence,
              implementation_terminal_stage_event_stream_version, lease_id, lease_holder_id,
              implementation_fence_token, implementation_lease_release_event_id,
              implementation_lease_release_event_sequence,
              implementation_lease_release_stream_version, verification_stage_run_id,
              verification_attempt_id, verification_fence_token,
              verification_controlled_thread_reservation_id, verification_thread_id,
              verification_stage_event_id, verification_stage_event_sequence,
              verification_stage_event_stream_version, verification_lease_event_id,
              verification_lease_event_sequence, verification_lease_event_stream_version,
              verification_reservation_event_id, verification_reservation_event_sequence,
              verification_reservation_event_stream_version, lease_duration_ms,
              task_history_json, task_history_digest, task_history_event_count,
              worktree_history_json, worktree_history_digest, worktree_history_event_count,
              stage_history_json, stage_history_digest, stage_history_event_count,
              lease_history_json, lease_history_digest, lease_history_event_count,
              reservation_history_json, reservation_history_digest,
              reservation_history_event_count, orchestration_history_json,
              orchestration_history_digest, orchestration_history_event_count, admitted_at
            ) VALUES (
              ${admissionEvidenceId}, ${receiptId}, ${markerId}, ${admissionCommandId},
              ${admissionFingerprint}, ${evidenceJson}, ${candidate.resultEvidenceId},
              ${candidate.resultJson}, ${candidate.finalizationFingerprint},
              ${candidate.receiptId}, ${candidate.markerId}, ${candidate.handoffId},
              ${candidate.handoffFingerprint}, ${candidate.projectId}, ${candidate.taskId},
              ${candidate.taskRevision}, ${candidate.githubIntakeSequence},
              ${candidate.sourceIdentityFingerprint}, ${candidate.repositoryDisplay},
              ${candidate.sourceRevision}, ${candidate.worktreeReservationId},
              ${candidate.worktreeEventId}, ${candidate.worktreeEventSequence},
              ${candidate.worktreeEventStreamVersion}, ${candidate.worktreeOwnershipFingerprint},
              ${candidate.stageRunId}, ${candidate.attemptId},
              ${candidate.controlledThreadReservationId}, ${candidate.threadId},
              ${candidate.stageEventId}, ${candidate.stageEventSequence},
              ${candidate.stageEventStreamVersion}, ${candidate.leaseId},
              ${candidate.leaseHolderId}, ${candidate.fenceToken}, ${candidate.leaseEventId},
              ${candidate.leaseEventSequence}, ${candidate.leaseEventStreamVersion},
              ${context.verificationStageRunId}, ${context.verificationAttemptId},
              ${context.verificationFenceToken},
              ${context.verificationControlledThreadReservationId},
              ${context.verificationThreadId}, ${stageEvent.eventId}, ${stageEvent.sequence},
              ${stageEvent.streamVersion}, ${leaseEvent.eventId}, ${leaseEvent.sequence},
              ${leaseEvent.streamVersion}, ${reservationEvent.eventId},
              ${reservationEvent.sequence}, ${reservationEvent.streamVersion},
              ${context.leaseDurationMs}, ${taskHistory.json}, ${taskHistory.digest},
              ${taskHistory.eventCount}, ${worktreeHistory.json}, ${worktreeHistory.digest},
              ${worktreeHistory.eventCount}, ${stageHistory.json}, ${stageHistory.digest},
              ${stageHistory.eventCount}, ${leaseHistory.json}, ${leaseHistory.digest},
              ${leaseHistory.eventCount}, ${reservationHistory.json},
              ${reservationHistory.digest}, ${reservationHistory.eventCount},
              ${candidate.orchestrationHistoryJson}, ${candidate.orchestrationHistoryDigest},
              ${candidate.orchestrationHistoryEventCount}, ${admittedAt}
            )
          `;
          yield* sql`
            INSERT INTO agent_control_verification_admission_receipts (
              receipt_id, marker_id, admission_command_id, admission_fingerprint,
              admission_evidence_id, implementation_result_evidence_id,
              verification_stage_event_id, verification_stage_event_sequence,
              verification_lease_event_id, verification_lease_event_sequence,
              verification_reservation_event_id, verification_reservation_event_sequence,
              status, accepted_at
            ) VALUES (
              ${receiptId}, ${markerId}, ${admissionCommandId}, ${admissionFingerprint},
              ${admissionEvidenceId}, ${candidate.resultEvidenceId}, ${stageEvent.eventId},
              ${stageEvent.sequence}, ${leaseEvent.eventId}, ${leaseEvent.sequence},
              ${reservationEvent.eventId}, ${reservationEvent.sequence}, 'accepted', ${admittedAt}
            )
          `;
          yield* hooks.beforeFinalMarker(observation);
          const markerFingerprint = fingerprintVerificationAdmission("marker", [
            candidate.resultEvidenceId,
            admissionCommandId,
            admissionEvidenceId,
            admissionFingerprint,
            receiptId,
            admittedAt,
          ]);
          yield* sql`
            INSERT INTO agent_control_verification_admission_markers (
              marker_id, marker_fingerprint, receipt_id, admission_command_id,
              admission_fingerprint, admission_evidence_id,
              implementation_result_evidence_id, committed_at
            ) VALUES (
              ${markerId}, ${markerFingerprint}, ${receiptId}, ${admissionCommandId},
              ${admissionFingerprint}, ${admissionEvidenceId}, ${candidate.resultEvidenceId},
              ${admittedAt}
            )
          `;
          return {
            _tag: "Committed" as const,
            observation,
            publication: {
              implementationResultEvidenceId,
              handoffId: candidate.handoffId,
              stageEvents: committedStageEvents,
              leaseEvents: committedLeaseEvents,
              reservationEvents: committedReservationEvents,
            } satisfies AgentControlVerificationAdmissionPublication,
          };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isAdmissionError(cause)
            ? cause
            : failure(
                implementationResultEvidenceId,
                "admission-transaction",
                "persistence",
                cause,
              ),
        ),
      );
  });

  const processResultEvidence = Effect.fn(
    "AgentControlVerificationAdmission.processResultEvidence",
  )(function* (implementationResultEvidenceId: string) {
    const accepted = yield* replayFirst(implementationResultEvidenceId);
    if (Option.isSome(accepted)) {
      return {
        _tag: "Replayed" as const,
        implementationResultEvidenceId,
      } satisfies AgentControlVerificationAdmissionResult;
    }
    const outcome = yield* readOutcome(implementationResultEvidenceId);
    if (Option.isNone(outcome) || outcome.value === "failed" || outcome.value === "cancelled") {
      return { _tag: "NotCandidate" as const };
    }
    if (outcome.value !== "succeeded") {
      return yield* failure(
        implementationResultEvidenceId,
        "classify-outcome",
        "candidate-evidence",
      );
    }
    const result = yield* processNew(implementationResultEvidenceId).pipe(
      Effect.catchIf(
        (cause) => cause.reason === "revision-conflict" || cause.reason === "persistence",
        (cause) =>
          replayFirst(implementationResultEvidenceId).pipe(
            Effect.flatMap((replay) =>
              Option.isSome(replay)
                ? Effect.succeed({
                    _tag: "Replayed" as const,
                    implementationResultEvidenceId,
                  } satisfies AgentControlVerificationAdmissionResult)
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
    } satisfies AgentControlVerificationAdmissionResult;
  });

  const processSafely = (implementationResultEvidenceId: string) =>
    processResultEvidence(implementationResultEvidenceId).pipe(
      Effect.asVoid,
      Effect.catchIf(
        (cause) => candidateReasons.has(cause.reason),
        (cause) =>
          Effect.logError("verification admission input failed", {
            implementationResultEvidenceId,
            operation: cause.operation,
            reason: cause.reason,
            cause: cause.cause === undefined ? undefined : Cause.pretty(Cause.die(cause.cause)),
          }),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = hooks.recoveryPageSize;
    if (!Number.isSafeInteger(pageSize) || pageSize < 1) {
      return yield* Effect.die(new Error("invalid verification admission recovery page size"));
    }
    let afterResultEvidenceId: string | undefined;
    while (true) {
      const listCandidates =
        afterResultEvidenceId === undefined
          ? sql<{
              readonly resultEvidenceIdBytes: unknown;
            }>`
            SELECT CAST(result_evidence_id AS BLOB) AS "resultEvidenceIdBytes"
            FROM agent_control_implementation_result_evidence
            ORDER BY result_evidence_id ASC
            LIMIT ${pageSize}
          `
          : sql<{
              readonly resultEvidenceIdBytes: unknown;
            }>`
            SELECT CAST(result_evidence_id AS BLOB) AS "resultEvidenceIdBytes"
            FROM agent_control_implementation_result_evidence
            WHERE result_evidence_id > ${afterResultEvidenceId}
            ORDER BY result_evidence_id ASC
            LIMIT ${pageSize}
          `;
      const candidates = yield* listCandidates.pipe(
        Effect.mapError((cause) => failure("recovery", "list-candidates", "persistence", cause)),
      );
      const decoded = yield* Effect.forEach(
        candidates,
        (row) =>
          decodeText("recovery", "decode-recovery-result-evidence-id", row.resultEvidenceIdBytes),
        { concurrency: 1 },
      );
      yield* Effect.forEach(decoded, (resultEvidenceId) => processSafely(resultEvidenceId), {
        concurrency: 1,
        discard: true,
      });
      if (decoded.length < pageSize) break;
      afterResultEvidenceId = decoded.at(-1)!;
    }
  });

  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlVerificationAdmission.start")(function* () {
    const finalizerPublications = yield* finalizer.subscribePublications;
    yield* hooks.afterFinalizerSubscriptionAcquired();
    const stageRunEvents = yield* stageEngine.subscribeDomainEvents;
    yield* hooks.afterStageRunSubscriptionAcquired();
    yield* Effect.forkScoped(
      Stream.runForEach(finalizerPublications, (publication) =>
        publication.outcome === "succeeded"
          ? worker.enqueue(publication.resultEvidenceId)
          : Effect.void,
      ),
      { startImmediately: true },
    );
    yield* Effect.forkScoped(
      Stream.runForEach(stageRunEvents, (event) =>
        event.type === "agentControl.stageRun.implementationSucceeded"
          ? worker.enqueue(event.payload.resultEvidenceId)
          : Effect.void,
      ),
      { startImmediately: true },
    );
    yield* hooks.beforeStartupRecovery();
    // Both PubSub subscriptions were acquired explicitly before their readiness
    // hooks, so recovery cannot overtake subscription setup.
    yield* recover;
  });

  return AgentControlVerificationAdmission.of({
    processResultEvidence,
    loadAcceptedEvidence: replayFirst,
    recover,
    start,
    drain: worker.drain,
    streamPublications: Stream.fromPubSub(publications),
    subscribePublications: PubSub.subscribe(publications).pipe(Effect.map(Stream.fromSubscription)),
  } satisfies AgentControlVerificationAdmissionShape);
});

export const AgentControlVerificationAdmissionLive = Layer.effect(
  AgentControlVerificationAdmission,
  make,
);
