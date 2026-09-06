import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  EventId,
  ProjectId,
  AgentControlVerificationStageFinalizationDocumentStorage,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEventDraft,
  type AgentControlVerificationInvalidOutputCode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import {
  loadAuthoritativeLeaseState,
  loadAuthoritativeStageRunState,
} from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import {
  deriveVerificationEvaluationEvidenceId,
  deriveVerificationEvaluationId,
  deriveVerificationEvaluationMarkerId,
  deriveVerificationEvaluationReceiptId,
  deriveVerificationFinalizationCommandId,
  deriveVerificationFinalizationEvidenceId,
  deriveVerificationFinalizationMarkerId,
  deriveVerificationFinalizationReceiptId,
  deriveVerificationLeaseReleaseEventId,
  deriveVerificationStageStartCommandId,
  deriveVerificationStageStartEvidenceId,
  deriveVerificationStageStartEventId,
  deriveVerificationStageStartMarkerId,
  deriveVerificationStageStartReceiptId,
  deriveVerificationTerminalStageEventId,
  fingerprintVerificationTurn,
} from "../identity.ts";
import { AgentControlVerificationEvaluator } from "../Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationHandoffStore } from "../Services/AgentControlVerificationHandoffStore.ts";
import {
  AgentControlVerificationStageFinalizer,
  AgentControlVerificationStageFinalizerError,
  type AgentControlVerificationStageFinalizationPublication,
  type AgentControlVerificationStageFinalizerShape,
} from "../Services/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationStageFinalizerHooks } from "../Services/AgentControlVerificationStageFinalizerHooks.ts";
import { ProviderAdmissionReleaseAuthority } from "../../providerAdmission/Services/ProviderAdmissionReleaseAuthority.ts";
import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";
import type { AgentControlVerificationClaim } from "../model.ts";

const isFinalizerError = Schema.is(AgentControlVerificationStageFinalizerError);
const INVALID_OUTPUT_CODES = new Set<AgentControlVerificationInvalidOutputCode>([
  "missing-final-message",
  "output-too-large",
  "invalid-utf8",
  "malformed-json",
  "unsupported-schema-version",
  "schema-violation",
]);

interface StartAuthority {
  readonly startEvidenceId: string;
  readonly startReceiptId: string;
  readonly startMarkerId: string;
  readonly startCommandId: string;
  readonly startFingerprint: string;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly deliveryRevision: number;
  readonly claimGeneration: number;
  readonly attemptCount: number;
  readonly startedAt: string;
}

type AcceptedEvaluation =
  | {
      readonly evaluationAuthority: "accepted-evaluation";
      readonly evaluationId: string;
      readonly evaluationEvidenceId: string;
      readonly evaluationReceiptId: string;
      readonly evaluationMarkerId: string;
      readonly evaluationDisposition: "evaluated";
      readonly verificationVerdict: "passed" | "failed";
      readonly invalidOutputCode: null;
    }
  | {
      readonly evaluationAuthority: "accepted-evaluation";
      readonly evaluationId: string;
      readonly evaluationEvidenceId: string;
      readonly evaluationReceiptId: string;
      readonly evaluationMarkerId: string;
      readonly evaluationDisposition: "invalid-output";
      readonly verificationVerdict: null;
      readonly invalidOutputCode: AgentControlVerificationInvalidOutputCode;
    };

const noEvaluation = {
  evaluationAuthority: "not-applicable",
  evaluationId: null,
  evaluationEvidenceId: null,
  evaluationReceiptId: null,
  evaluationMarkerId: null,
  evaluationDisposition: null,
  verificationVerdict: null,
  invalidOutputCode: null,
} as const;

const decodeReplayDocument = Schema.decodeUnknownEffect(
  AgentControlVerificationStageFinalizationDocumentStorage,
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlVerificationHandoffStore;
  const evaluator = yield* AgentControlVerificationEvaluator;
  const wakeup = yield* AgentControlVerificationTurnWakeup;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseProjection = yield* AgentControlStageRunLeaseProjection;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const hooks = yield* AgentControlVerificationStageFinalizerHooks;
  const providerAdmissionRelease = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderAdmissionReleaseAuthority),
  );

  const error = (
    handoffId: string,
    operation: string,
    reason: AgentControlVerificationStageFinalizerError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlVerificationStageFinalizerError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const decodeText = (handoffId: string, operation: string, value: unknown) =>
    Effect.try({
      try: () => decodeCanonicalUtf8Bytes(value),
      catch: (cause) => error(handoffId, operation, "identity-mismatch", cause),
    });

  const loadStartAuthority = Effect.fn("AgentControlVerificationStageFinalizer.loadStartAuthority")(
    function* (handoffId: string, claim: AgentControlVerificationClaim) {
      const rows = yield* sql<Record<string, unknown>>`
      SELECT CAST(evidence.start_evidence_id AS BLOB) AS "startEvidenceIdBytes",
        CAST(receipt.start_receipt_id AS BLOB) AS "startReceiptIdBytes",
        CAST(marker.start_marker_id AS BLOB) AS "startMarkerIdBytes",
        CAST(evidence.start_command_id AS BLOB) AS "startCommandIdBytes",
        CAST(evidence.start_fingerprint AS BLOB) AS "startFingerprintBytes",
        CAST(evidence.stage_event_id AS BLOB) AS "stageEventIdBytes",
        evidence.stage_event_sequence AS "stageEventSequence",
        evidence.delivery_revision AS "deliveryRevision",
        evidence.claim_generation AS "claimGeneration",
        evidence.attempt_count AS "attemptCount",
        CAST(evidence.started_at AS BLOB) AS "startedAtBytes"
      FROM main.agent_control_verification_stage_started_evidence evidence
      JOIN main.agent_control_verification_stage_started_receipts receipt
        ON receipt.start_evidence_id = evidence.start_evidence_id
       AND receipt.provider_delivery_id = evidence.provider_delivery_id
      JOIN main.agent_control_verification_stage_started_markers marker
        ON marker.start_evidence_id = evidence.start_evidence_id
       AND marker.start_receipt_id = receipt.start_receipt_id
       AND marker.provider_delivery_id = evidence.provider_delivery_id
      WHERE evidence.handoff_id = ${handoffId}
        AND evidence.provider_delivery_id = ${claim.evidence.providerDeliveryId}
        AND typeof(evidence.start_evidence_id) = 'text'
        AND typeof(receipt.start_receipt_id) = 'text'
        AND typeof(marker.start_marker_id) = 'text'
        AND typeof(evidence.start_command_id) = 'text'
        AND typeof(evidence.start_fingerprint) = 'text'
        AND typeof(evidence.stage_event_id) = 'text'
        AND typeof(evidence.stage_event_sequence) = 'integer'
        AND typeof(evidence.delivery_revision) = 'integer'
        AND typeof(evidence.claim_generation) = 'integer'
        AND typeof(evidence.attempt_count) = 'integer'
        AND typeof(evidence.started_at) = 'text'
    `.pipe(
        Effect.mapError((cause) => error(handoffId, "load-start-authority", "persistence", cause)),
      );
      const counts = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM main.agent_control_verification_stage_started_evidence
          WHERE handoff_id = ${handoffId}) +
        (SELECT count(*) FROM main.agent_control_verification_stage_started_receipts receipt
          JOIN main.agent_control_verification_stage_started_evidence evidence
            ON evidence.start_evidence_id = receipt.start_evidence_id
          WHERE evidence.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM main.agent_control_verification_stage_started_markers marker
          JOIN main.agent_control_verification_stage_started_evidence evidence
            ON evidence.start_evidence_id = marker.start_evidence_id
          WHERE evidence.handoff_id = ${handoffId}) AS count
    `;
      if (counts[0]?.count !== 3 || rows.length !== 1) {
        return yield* error(handoffId, "validate-start-authority", "stage-history-corrupt");
      }
      const row = rows[0]!;
      const [
        startEvidenceId,
        startReceiptId,
        startMarkerId,
        startCommandId,
        startFingerprint,
        stageEventId,
        startedAt,
      ] = yield* Effect.all([
        decodeText(handoffId, "decode-start-evidence-id", row.startEvidenceIdBytes),
        decodeText(handoffId, "decode-start-receipt-id", row.startReceiptIdBytes),
        decodeText(handoffId, "decode-start-marker-id", row.startMarkerIdBytes),
        decodeText(handoffId, "decode-start-command-id", row.startCommandIdBytes),
        decodeText(handoffId, "decode-start-fingerprint", row.startFingerprintBytes),
        decodeText(handoffId, "decode-start-event-id", row.stageEventIdBytes),
        decodeText(handoffId, "decode-started-at", row.startedAtBytes),
      ]);
      if (
        typeof row.stageEventSequence !== "number" ||
        typeof row.deliveryRevision !== "number" ||
        typeof row.claimGeneration !== "number" ||
        typeof row.attemptCount !== "number" ||
        claim.delivery.providerTurnId === null
      ) {
        return yield* error(handoffId, "decode-start-numbers", "stage-history-corrupt");
      }
      const expectedCommandId = deriveVerificationStageStartCommandId(
        claim.evidence.providerDeliveryId,
        claim.delivery.providerTurnId,
      );
      const expectedFingerprint = fingerprintVerificationTurn("stage-start", [
        claim.evidence.admissionEvidenceId,
        claim.evidence.admissionReceiptId,
        claim.evidence.admissionMarkerId,
        claim.evidence.materializationEvidenceId,
        claim.evidence.materializationReceiptId,
        claim.evidence.materializationMarkerId,
        claim.evidence.handoffId,
        claim.evidence.handoffFingerprint,
        claim.evidence.providerDeliveryId,
        String(row.deliveryRevision),
        String(row.claimGeneration),
        String(row.attemptCount),
        claim.evidence.threadId,
        claim.evidence.planningThreadId,
        claim.evidence.planId,
        claim.delivery.providerTurnId,
        stageEventId,
        startedAt,
      ]);
      if (
        startCommandId !== expectedCommandId ||
        startEvidenceId !== deriveVerificationStageStartEvidenceId(expectedCommandId) ||
        startReceiptId !== deriveVerificationStageStartReceiptId(expectedCommandId) ||
        startMarkerId !== deriveVerificationStageStartMarkerId(expectedCommandId) ||
        stageEventId !== deriveVerificationStageStartEventId(expectedCommandId) ||
        startFingerprint !== expectedFingerprint ||
        startedAt !== claim.delivery.providerAcceptedAt ||
        row.deliveryRevision > claim.delivery.revision ||
        row.claimGeneration !== claim.delivery.claimGeneration ||
        row.attemptCount !== claim.delivery.attemptCount
      ) {
        return yield* error(handoffId, "compare-start-authority", "stage-history-corrupt");
      }
      return {
        startEvidenceId,
        startReceiptId,
        startMarkerId,
        startCommandId,
        startFingerprint,
        stageEventId,
        stageEventSequence: row.stageEventSequence,
        deliveryRevision: row.deliveryRevision,
        claimGeneration: row.claimGeneration,
        attemptCount: row.attemptCount,
        startedAt,
      } satisfies StartAuthority;
    },
  );

  const loadEvaluationAuthority = Effect.fn(
    "AgentControlVerificationStageFinalizer.loadEvaluationAuthority",
  )(function* (handoffId: string, claim: AgentControlVerificationClaim, start: StartAuthority) {
    const counts = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM main.agent_control_verification_evaluation_evidence
          WHERE provider_delivery_id = ${claim.evidence.providerDeliveryId}) +
        (SELECT count(*) FROM main.agent_control_verification_evaluation_receipts
          WHERE provider_delivery_id = ${claim.evidence.providerDeliveryId}) +
        (SELECT count(*) FROM main.agent_control_verification_evaluation_markers
          WHERE provider_delivery_id = ${claim.evidence.providerDeliveryId}) AS count
    `.pipe(
      Effect.mapError((cause) =>
        error(handoffId, "count-evaluation-authority", "persistence", cause),
      ),
    );
    const count = counts[0]?.count ?? 0;
    if (claim.delivery.state !== "completed") {
      if (count !== 0) {
        return yield* error(handoffId, "unexpected-evaluation-authority", "evaluation-conflict");
      }
      return noEvaluation;
    }
    if (count !== 3) {
      return yield* error(handoffId, "partial-evaluation-authority", "evaluation-conflict");
    }
    if (
      claim.delivery.providerTurnId === null ||
      claim.delivery.terminalEventId === null ||
      claim.delivery.terminalObservationDigest === null ||
      claim.evidence.resultSchemaFingerprint === null
    ) {
      return yield* error(handoffId, "completed-evaluation-identity", "authority-conflict");
    }
    const rows = yield* sql<Record<string, unknown>>`
      SELECT CAST(evidence.evaluation_id AS BLOB) AS "evaluationIdBytes",
        CAST(evidence.evidence_id AS BLOB) AS "evidenceIdBytes",
        CAST(receipt.receipt_id AS BLOB) AS "receiptIdBytes",
        CAST(marker.marker_id AS BLOB) AS "markerIdBytes",
        CAST(evidence.evaluation_fingerprint AS BLOB) AS "fingerprintBytes",
        CAST(evidence.authority_digest AS BLOB) AS "authorityDigestBytes",
        CAST(evidence.authority_json AS BLOB) AS "authorityJsonBytes",
        CAST(evidence.disposition AS BLOB) AS "dispositionBytes",
        CASE WHEN evidence.verdict IS NULL THEN NULL
          ELSE CAST(evidence.verdict AS BLOB) END AS "verdictBytes",
        CASE WHEN evidence.error_code IS NULL THEN NULL
          ELSE CAST(evidence.error_code AS BLOB) END AS "errorCodeBytes",
        CASE WHEN evidence.semantic_result_digest IS NULL THEN NULL
          ELSE CAST(evidence.semantic_result_digest AS BLOB) END AS "semanticDigestBytes",
        CAST(evidence.source_disposition AS BLOB) AS "sourceDispositionBytes",
        CASE WHEN evidence.source_message_id IS NULL THEN NULL
          ELSE CAST(evidence.source_message_id AS BLOB) END AS "sourceMessageIdBytes",
        CASE WHEN evidence.source_event_id IS NULL THEN NULL
          ELSE CAST(evidence.source_event_id AS BLOB) END AS "sourceEventIdBytes",
        evidence.source_event_sequence AS "sourceEventSequence",
        evidence.source_event_stream_version AS "sourceEventStreamVersion",
        CASE WHEN evidence.raw_output_digest IS NULL THEN NULL
          ELSE CAST(evidence.raw_output_digest AS BLOB) END AS "rawOutputDigestBytes",
        evidence.output_byte_length AS "outputByteLength",
        CAST(evidence.terminal_event_id AS BLOB) AS "terminalEventIdBytes",
        evidence.terminal_sequence AS "terminalSequence",
        evidence.terminal_stream_version AS "terminalStreamVersion",
        CAST(evidence.terminal_observation_digest AS BLOB) AS "terminalObservationDigestBytes",
        CAST(evidence.start_marker_id AS BLOB) AS "startMarkerIdBytes",
        CAST(receipt.status AS BLOB) AS "receiptStatusBytes",
        marker.marker_version AS "markerVersion",
        CAST(receipt.evaluation_fingerprint AS BLOB) AS "receiptFingerprintBytes",
        CAST(marker.evaluation_fingerprint AS BLOB) AS "markerFingerprintBytes"
      FROM main.agent_control_verification_evaluation_evidence evidence
      JOIN main.agent_control_verification_evaluation_receipts receipt
        ON receipt.evaluation_id = evidence.evaluation_id
       AND receipt.evidence_id = evidence.evidence_id
       AND receipt.receipt_id = evidence.receipt_id
      JOIN main.agent_control_verification_evaluation_markers marker
        ON marker.evaluation_id = evidence.evaluation_id
       AND marker.evidence_id = evidence.evidence_id
       AND marker.receipt_id = receipt.receipt_id
       AND marker.marker_id = evidence.marker_id
      WHERE evidence.provider_delivery_id = ${claim.evidence.providerDeliveryId}
        AND typeof(evidence.evaluation_id) = 'text'
        AND typeof(evidence.evidence_id) = 'text'
        AND typeof(receipt.receipt_id) = 'text'
        AND typeof(marker.marker_id) = 'text'
        AND typeof(evidence.authority_json) = 'text'
        AND typeof(evidence.output_byte_length) = 'integer'
        AND typeof(evidence.terminal_sequence) = 'integer'
        AND typeof(evidence.terminal_stream_version) = 'integer'
        AND typeof(marker.marker_version) = 'integer'
    `.pipe(
      Effect.mapError((cause) =>
        error(handoffId, "load-evaluation-authority", "persistence", cause),
      ),
    );
    if (rows.length !== 1) {
      return yield* error(handoffId, "join-evaluation-authority", "evaluation-conflict");
    }
    const row = rows[0]!;
    const decodeNullable = (value: unknown, operation: string) =>
      value === null ? Effect.succeed(null) : decodeText(handoffId, operation, value);
    const [
      evaluationId,
      evidenceId,
      receiptId,
      markerId,
      fingerprint,
      authorityDigest,
      authorityJson,
      disposition,
      verdict,
      errorCode,
      semanticDigest,
      sourceDisposition,
      sourceMessageId,
      sourceEventId,
      rawOutputDigest,
      terminalEventId,
      terminalObservationDigest,
      startMarkerId,
      receiptStatus,
      receiptFingerprint,
      markerFingerprint,
    ] = yield* Effect.all([
      decodeText(handoffId, "decode-evaluation-id", row.evaluationIdBytes),
      decodeText(handoffId, "decode-evaluation-evidence-id", row.evidenceIdBytes),
      decodeText(handoffId, "decode-evaluation-receipt-id", row.receiptIdBytes),
      decodeText(handoffId, "decode-evaluation-marker-id", row.markerIdBytes),
      decodeText(handoffId, "decode-evaluation-fingerprint", row.fingerprintBytes),
      decodeText(handoffId, "decode-evaluation-authority-digest", row.authorityDigestBytes),
      decodeText(handoffId, "decode-evaluation-authority-json", row.authorityJsonBytes),
      decodeText(handoffId, "decode-evaluation-disposition", row.dispositionBytes),
      decodeNullable(row.verdictBytes, "decode-evaluation-verdict"),
      decodeNullable(row.errorCodeBytes, "decode-evaluation-error-code"),
      decodeNullable(row.semanticDigestBytes, "decode-evaluation-semantic-digest"),
      decodeText(handoffId, "decode-evaluation-source-disposition", row.sourceDispositionBytes),
      decodeNullable(row.sourceMessageIdBytes, "decode-evaluation-source-message"),
      decodeNullable(row.sourceEventIdBytes, "decode-evaluation-source-event"),
      decodeNullable(row.rawOutputDigestBytes, "decode-evaluation-output-digest"),
      decodeText(handoffId, "decode-evaluation-terminal-event", row.terminalEventIdBytes),
      decodeText(
        handoffId,
        "decode-evaluation-terminal-observation",
        row.terminalObservationDigestBytes,
      ),
      decodeText(handoffId, "decode-evaluation-start-marker", row.startMarkerIdBytes),
      decodeText(handoffId, "decode-evaluation-receipt-status", row.receiptStatusBytes),
      decodeText(handoffId, "decode-evaluation-receipt-fingerprint", row.receiptFingerprintBytes),
      decodeText(handoffId, "decode-evaluation-marker-fingerprint", row.markerFingerprintBytes),
    ]);
    const expectedEvaluationId = deriveVerificationEvaluationId({
      providerDeliveryId: claim.evidence.providerDeliveryId,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: claim.delivery.providerTurnId,
      resultSchemaFingerprint: claim.evidence.resultSchemaFingerprint,
    });
    if (
      typeof row.outputByteLength !== "number" ||
      typeof row.terminalSequence !== "number" ||
      typeof row.terminalStreamVersion !== "number" ||
      (row.sourceEventSequence !== null && typeof row.sourceEventSequence !== "number") ||
      (row.sourceEventStreamVersion !== null && typeof row.sourceEventStreamVersion !== "number") ||
      typeof row.markerVersion !== "number"
    ) {
      return yield* error(handoffId, "decode-evaluation-numbers", "evaluation-conflict");
    }
    const expectedAuthorityJson = canonicalJson({
      admission: {
        evidenceId: claim.evidence.admissionEvidenceId,
        markerId: claim.evidence.admissionMarkerId,
        receiptId: claim.evidence.admissionReceiptId,
      },
      attemptId: claim.evidence.attemptId,
      controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
      disposition,
      errorCode,
      evaluationId,
      evidenceId,
      handoffFingerprint: claim.evidence.handoffFingerprint,
      handoffId: claim.evidence.handoffId,
      lease: {
        fenceToken: claim.evidence.fenceToken,
        holderId: claim.evidence.leaseHolderId,
        leaseId: claim.evidence.leaseId,
      },
      markerId,
      materialization: {
        evidenceId: claim.evidence.materializationEvidenceId,
        markerId: claim.evidence.materializationMarkerId,
        receiptId: claim.evidence.materializationReceiptId,
      },
      modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
      prompt: {
        contractFingerprint: claim.evidence.promptContractFingerprint,
        digest: claim.evidence.promptDigest,
        templateVersion: claim.evidence.templateVersion,
      },
      providerDeliveryId: claim.evidence.providerDeliveryId,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: claim.delivery.providerTurnId,
      receiptId,
      resultSchema: {
        fingerprint: claim.evidence.resultSchemaFingerprint,
        version: claim.evidence.resultSchemaVersion,
      },
      semanticResultDigest: semanticDigest,
      source: {
        byteLength: row.outputByteLength,
        disposition: sourceDisposition,
        eventId: sourceEventId,
        eventSequence: row.sourceEventSequence,
        eventStreamVersion: row.sourceEventStreamVersion,
        messageId: sourceMessageId,
        rawDigest: rawOutputDigest,
      },
      stageStartMarkerId: start.startMarkerId,
      stageRunId: claim.evidence.stageRunId,
      task: {
        githubIntakeSequence: claim.evidence.githubIntakeSequence,
        projectId: claim.evidence.projectId,
        revision: claim.evidence.taskRevision,
        sourceIdentityFingerprint: claim.evidence.sourceIdentityFingerprint,
        taskId: claim.evidence.taskId,
      },
      terminal: {
        eventId: terminalEventId,
        observationDigest: claim.delivery.terminalObservationDigest,
        runtimeEventId: claim.delivery.terminalEventId,
        sequence: row.terminalSequence,
        state: claim.delivery.terminalProviderState,
        streamVersion: row.terminalStreamVersion,
      },
      threadId: claim.evidence.threadId,
      verdict,
      worktree: {
        branch: claim.evidence.branch,
        eventId: claim.evidence.worktreeEventId,
        eventSequence: claim.evidence.worktreeEventSequence,
        eventStreamVersion: claim.evidence.worktreeEventStreamVersion,
        ownershipFingerprint: claim.evidence.worktreeOwnershipFingerprint,
        path: claim.evidence.worktreePath,
        reservationId: claim.evidence.worktreeReservationId,
        revision: claim.evidence.worktreeRevision,
        verifiedAt: claim.evidence.worktreeVerifiedAt,
      },
    } satisfies JsonValue);
    if (
      evaluationId !== expectedEvaluationId ||
      evidenceId !== deriveVerificationEvaluationEvidenceId(expectedEvaluationId) ||
      receiptId !== deriveVerificationEvaluationReceiptId(expectedEvaluationId) ||
      markerId !== deriveVerificationEvaluationMarkerId(expectedEvaluationId) ||
      startMarkerId !== start.startMarkerId ||
      authorityJson !== expectedAuthorityJson ||
      authorityDigest !== sha256Utf8(authorityJson) ||
      fingerprint !== fingerprintVerificationTurn("evaluation-fingerprint", [authorityJson]) ||
      receiptFingerprint !== fingerprint ||
      markerFingerprint !== fingerprint ||
      receiptStatus !== "accepted" ||
      row.markerVersion !== 1 ||
      terminalObservationDigest !== claim.delivery.terminalObservationDigest ||
      parseCanonicalJson(authorityJson) === null
    ) {
      return yield* error(handoffId, "compare-evaluation-authority", "evaluation-conflict");
    }
    if (disposition === "evaluated" && (verdict === "passed" || verdict === "failed")) {
      if (errorCode !== null || semanticDigest === null) {
        return yield* error(handoffId, "evaluated-authority-shape", "evaluation-conflict");
      }
      return {
        evaluationAuthority: "accepted-evaluation",
        evaluationId,
        evaluationEvidenceId: evidenceId,
        evaluationReceiptId: receiptId,
        evaluationMarkerId: markerId,
        evaluationDisposition: "evaluated",
        verificationVerdict: verdict,
        invalidOutputCode: null,
      } satisfies AcceptedEvaluation;
    }
    if (
      disposition === "invalid-output" &&
      verdict === null &&
      semanticDigest === null &&
      typeof errorCode === "string" &&
      INVALID_OUTPUT_CODES.has(errorCode as AgentControlVerificationInvalidOutputCode)
    ) {
      return {
        evaluationAuthority: "accepted-evaluation",
        evaluationId,
        evaluationEvidenceId: evidenceId,
        evaluationReceiptId: receiptId,
        evaluationMarkerId: markerId,
        evaluationDisposition: "invalid-output",
        verificationVerdict: null,
        invalidOutputCode: errorCode as AgentControlVerificationInvalidOutputCode,
      } satisfies AcceptedEvaluation;
    }
    return yield* error(handoffId, "evaluation-authority-shape", "evaluation-conflict");
  });

  const replayFirst = Effect.fn("AgentControlVerificationStageFinalizer.replayFirst")(function* (
    handoffId: string,
  ) {
    const counts = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT count(*) FROM main.agent_control_verification_finalization_evidence
            WHERE handoff_id = ${handoffId}) +
          (SELECT count(*) FROM main.agent_control_verification_finalization_receipts
            WHERE handoff_id = ${handoffId}) +
          (SELECT count(*) FROM main.agent_control_verification_finalization_markers
            WHERE handoff_id = ${handoffId}) AS count
      `.pipe(Effect.mapError((cause) => error(handoffId, "replay-count", "persistence", cause)));
    const count = counts[0]?.count ?? 0;
    if (count === 0) return Option.none<string>();
    if (count !== 3) return yield* error(handoffId, "replay-partial", "partial-replay");
    const rows = yield* sql<Record<string, unknown>>`
        SELECT CAST(evidence.finalization_evidence_id AS BLOB) AS "evidenceIdBytes",
          CAST(evidence.finalization_command_id AS BLOB) AS "evidenceCommandIdBytes",
          CAST(evidence.handoff_id AS BLOB) AS "evidenceHandoffIdBytes",
          CAST(evidence.handoff_fingerprint AS BLOB) AS "evidenceHandoffFingerprintBytes",
          CAST(evidence.stage_run_id AS BLOB) AS "stageRunIdBytes",
          CAST(evidence.lease_id AS BLOB) AS "leaseIdBytes",
          CAST(evidence.project_id AS BLOB) AS "projectIdBytes",
          CAST(evidence.task_id AS BLOB) AS "taskIdBytes",
          evidence.task_revision AS "taskRevision",
          evidence.github_intake_sequence AS "githubIntakeSequence",
          CAST(evidence.source_identity_fingerprint AS BLOB) AS "sourceIdentityFingerprintBytes",
          CAST(evidence.attempt_id AS BLOB) AS "attemptIdBytes",
          CAST(evidence.lease_holder_id AS BLOB) AS "leaseHolderIdBytes",
          evidence.fence_token AS "fenceToken",
          CAST(evidence.provider_delivery_id AS BLOB) AS "providerDeliveryIdBytes",
          CAST(evidence.provider_instance_id AS BLOB) AS "providerInstanceIdBytes",
          CAST(evidence.provider_turn_id AS BLOB) AS "providerTurnIdBytes",
          evidence.delivery_revision AS "deliveryRevision",
          CAST(evidence.terminal_at AS BLOB) AS "terminalAtBytes",
          CAST(evidence.start_evidence_id AS BLOB) AS "startEvidenceIdBytes",
          CAST(evidence.start_receipt_id AS BLOB) AS "startReceiptIdBytes",
          CAST(evidence.start_marker_id AS BLOB) AS "startMarkerIdBytes",
          CAST(evidence.finalization_json AS BLOB) AS "finalizationJsonBytes",
          CAST(evidence.finalization_fingerprint AS BLOB) AS "fingerprintBytes",
          CAST(evidence.receipt_id AS BLOB) AS "evidenceReceiptIdBytes",
          CAST(evidence.marker_id AS BLOB) AS "evidenceMarkerIdBytes",
          CAST(evidence.outcome AS BLOB) AS "evidenceOutcomeBytes",
          CAST(evidence.terminal_cause AS BLOB) AS "evidenceTerminalCauseBytes",
          CAST(evidence.delivery_terminal_state AS BLOB) AS "evidenceDeliveryStateBytes",
          CAST(evidence.terminal_runtime_event_id AS BLOB) AS "evidenceTerminalEventIdBytes",
          CAST(evidence.finalized_at AS BLOB) AS "evidenceFinalizedAtBytes",
          CAST(evidence.stage_event_id AS BLOB) AS "evidenceStageEventIdBytes",
          evidence.stage_event_sequence AS "evidenceStageEventSequence",
          evidence.stage_event_stream_version AS "evidenceStageEventStreamVersion",
          CAST(evidence.lease_event_id AS BLOB) AS "evidenceLeaseEventIdBytes",
          evidence.lease_event_sequence AS "evidenceLeaseEventSequence",
          evidence.lease_event_stream_version AS "evidenceLeaseEventStreamVersion",
          CAST(evidence.evaluation_authority AS BLOB) AS "evaluationAuthorityBytes",
          CASE WHEN evidence.evaluation_id IS NULL THEN NULL
            ELSE CAST(evidence.evaluation_id AS BLOB) END AS "evaluationIdBytes",
          CASE WHEN evidence.evaluation_evidence_id IS NULL THEN NULL
            ELSE CAST(evidence.evaluation_evidence_id AS BLOB) END AS "evaluationEvidenceIdBytes",
          CASE WHEN evidence.evaluation_receipt_id IS NULL THEN NULL
            ELSE CAST(evidence.evaluation_receipt_id AS BLOB) END AS "evaluationReceiptIdBytes",
          CASE WHEN evidence.evaluation_marker_id IS NULL THEN NULL
            ELSE CAST(evidence.evaluation_marker_id AS BLOB) END AS "evaluationMarkerIdBytes",
          CASE WHEN evidence.evaluation_disposition IS NULL THEN NULL
            ELSE CAST(evidence.evaluation_disposition AS BLOB) END AS "evaluationDispositionBytes",
          CASE WHEN evidence.verification_verdict IS NULL THEN NULL
            ELSE CAST(evidence.verification_verdict AS BLOB) END AS "verificationVerdictBytes",
          CASE WHEN evidence.invalid_output_code IS NULL THEN NULL
            ELSE CAST(evidence.invalid_output_code AS BLOB) END AS "invalidOutputCodeBytes",
          CAST(receipt.receipt_id AS BLOB) AS "receiptIdBytes",
          CAST(receipt.marker_id AS BLOB) AS "receiptMarkerIdBytes",
          CAST(receipt.finalization_evidence_id AS BLOB) AS "receiptEvidenceIdBytes",
          CAST(receipt.finalization_command_id AS BLOB) AS "receiptCommandIdBytes",
          CAST(receipt.handoff_id AS BLOB) AS "receiptHandoffIdBytes",
          CAST(receipt.finalization_fingerprint AS BLOB) AS "receiptFingerprintBytes",
          CAST(receipt.outcome AS BLOB) AS "receiptOutcomeBytes",
          CAST(receipt.terminal_cause AS BLOB) AS "receiptTerminalCauseBytes",
          CAST(receipt.stage_event_id AS BLOB) AS "receiptStageEventIdBytes",
          receipt.stage_event_sequence AS "receiptStageEventSequence",
          CAST(receipt.lease_event_id AS BLOB) AS "receiptLeaseEventIdBytes",
          receipt.lease_event_sequence AS "receiptLeaseEventSequence",
          CAST(receipt.status AS BLOB) AS "receiptStatusBytes",
          CAST(receipt.accepted_at AS BLOB) AS "receiptAcceptedAtBytes",
          CAST(marker.marker_id AS BLOB) AS "markerIdBytes",
          CAST(marker.receipt_id AS BLOB) AS "markerReceiptIdBytes",
          CAST(marker.finalization_evidence_id AS BLOB) AS "markerEvidenceIdBytes",
          CAST(marker.finalization_command_id AS BLOB) AS "markerCommandIdBytes",
          CAST(marker.handoff_id AS BLOB) AS "markerHandoffIdBytes",
          CAST(marker.finalization_fingerprint AS BLOB) AS "markerFingerprintBytes",
          CAST(marker.marker_fingerprint AS BLOB) AS "markerSealBytes",
          CAST(marker.committed_at AS BLOB) AS "markerCommittedAtBytes"
        FROM main.agent_control_verification_finalization_evidence evidence
        JOIN main.agent_control_verification_finalization_receipts receipt
          ON receipt.finalization_evidence_id = evidence.finalization_evidence_id
         AND receipt.receipt_id = evidence.receipt_id
        JOIN main.agent_control_verification_finalization_markers marker
          ON marker.finalization_evidence_id = evidence.finalization_evidence_id
         AND marker.marker_id = evidence.marker_id
         AND marker.receipt_id = receipt.receipt_id
        WHERE evidence.handoff_id = ${handoffId}
          AND typeof(evidence.finalization_evidence_id) = 'text'
          AND typeof(evidence.finalization_command_id) = 'text'
          AND typeof(evidence.handoff_id) = 'text'
          AND typeof(evidence.handoff_fingerprint) = 'text'
          AND typeof(evidence.stage_run_id) = 'text'
          AND typeof(evidence.lease_id) = 'text'
          AND typeof(evidence.project_id) = 'text'
          AND typeof(evidence.task_id) = 'text'
          AND typeof(evidence.task_revision) = 'integer'
          AND typeof(evidence.github_intake_sequence) = 'integer'
          AND typeof(evidence.source_identity_fingerprint) = 'text'
          AND typeof(evidence.attempt_id) = 'text'
          AND typeof(evidence.lease_holder_id) = 'text'
          AND typeof(evidence.fence_token) = 'integer'
          AND typeof(evidence.provider_delivery_id) = 'text'
          AND typeof(evidence.provider_instance_id) = 'text'
          AND typeof(evidence.provider_turn_id) = 'text'
          AND typeof(evidence.delivery_revision) = 'integer'
          AND typeof(evidence.terminal_at) = 'text'
          AND typeof(evidence.start_evidence_id) = 'text'
          AND typeof(evidence.start_receipt_id) = 'text'
          AND typeof(evidence.start_marker_id) = 'text'
          AND typeof(evidence.finalization_json) = 'text'
          AND typeof(evidence.finalization_fingerprint) = 'text'
          AND typeof(evidence.receipt_id) = 'text'
          AND typeof(evidence.marker_id) = 'text'
          AND typeof(evidence.outcome) = 'text'
          AND typeof(evidence.terminal_cause) = 'text'
          AND typeof(evidence.delivery_terminal_state) = 'text'
          AND typeof(evidence.terminal_runtime_event_id) = 'text'
          AND typeof(evidence.finalized_at) = 'text'
          AND typeof(evidence.stage_event_id) = 'text'
          AND typeof(evidence.stage_event_sequence) = 'integer'
          AND typeof(evidence.stage_event_stream_version) = 'integer'
          AND typeof(evidence.lease_event_id) = 'text'
          AND typeof(evidence.lease_event_sequence) = 'integer'
          AND typeof(evidence.lease_event_stream_version) = 'integer'
          AND typeof(evidence.evaluation_authority) = 'text'
          AND (evidence.evaluation_id IS NULL OR typeof(evidence.evaluation_id) = 'text')
          AND (evidence.evaluation_evidence_id IS NULL
            OR typeof(evidence.evaluation_evidence_id) = 'text')
          AND (evidence.evaluation_receipt_id IS NULL
            OR typeof(evidence.evaluation_receipt_id) = 'text')
          AND (evidence.evaluation_marker_id IS NULL
            OR typeof(evidence.evaluation_marker_id) = 'text')
          AND (evidence.evaluation_disposition IS NULL
            OR typeof(evidence.evaluation_disposition) = 'text')
          AND (evidence.verification_verdict IS NULL
            OR typeof(evidence.verification_verdict) = 'text')
          AND (evidence.invalid_output_code IS NULL
            OR typeof(evidence.invalid_output_code) = 'text')
          AND typeof(receipt.receipt_id) = 'text'
          AND typeof(receipt.marker_id) = 'text'
          AND typeof(receipt.finalization_evidence_id) = 'text'
          AND typeof(receipt.finalization_command_id) = 'text'
          AND typeof(receipt.handoff_id) = 'text'
          AND typeof(receipt.finalization_fingerprint) = 'text'
          AND typeof(receipt.outcome) = 'text'
          AND typeof(receipt.terminal_cause) = 'text'
          AND typeof(receipt.stage_event_id) = 'text'
          AND typeof(receipt.stage_event_sequence) = 'integer'
          AND typeof(receipt.lease_event_id) = 'text'
          AND typeof(receipt.lease_event_sequence) = 'integer'
          AND typeof(receipt.status) = 'text'
          AND typeof(receipt.accepted_at) = 'text'
          AND typeof(marker.marker_id) = 'text'
          AND typeof(marker.receipt_id) = 'text'
          AND typeof(marker.finalization_evidence_id) = 'text'
          AND typeof(marker.finalization_command_id) = 'text'
          AND typeof(marker.handoff_id) = 'text'
          AND typeof(marker.finalization_fingerprint) = 'text'
          AND typeof(marker.marker_fingerprint) = 'text'
          AND typeof(marker.committed_at) = 'text'
      `.pipe(Effect.mapError((cause) => error(handoffId, "replay-read", "persistence", cause)));
    if (rows.length !== 1) return yield* error(handoffId, "replay-chain", "partial-replay");
    const row = rows[0]!;
    const decodeReplayText = (operation: string, value: unknown) =>
      decodeText(handoffId, operation, value);
    const decodeReplayNullable = (operation: string, value: unknown) =>
      value === null ? Effect.succeed(null) : decodeReplayText(operation, value);
    const evidenceIdRaw = yield* decodeReplayText("replay-evidence-id", row.evidenceIdBytes);
    const evidenceCommandId = yield* decodeReplayText(
      "replay-evidence-command",
      row.evidenceCommandIdBytes,
    );
    const evidenceHandoffId = yield* decodeReplayText(
      "replay-evidence-handoff",
      row.evidenceHandoffIdBytes,
    );
    const evidenceHandoffFingerprint = yield* decodeReplayText(
      "replay-evidence-handoff-fingerprint",
      row.evidenceHandoffFingerprintBytes,
    );
    const stageRunId = yield* decodeReplayText("replay-stage-run-id", row.stageRunIdBytes);
    const leaseId = yield* decodeReplayText("replay-lease-id", row.leaseIdBytes);
    const projectId = yield* decodeReplayText("replay-project-id", row.projectIdBytes);
    const taskId = yield* decodeReplayText("replay-task-id", row.taskIdBytes);
    const sourceIdentityFingerprint = yield* decodeReplayText(
      "replay-source-identity-fingerprint",
      row.sourceIdentityFingerprintBytes,
    );
    const attemptId = yield* decodeReplayText("replay-attempt-id", row.attemptIdBytes);
    const leaseHolderId = yield* decodeReplayText("replay-lease-holder-id", row.leaseHolderIdBytes);
    const providerDeliveryId = yield* decodeReplayText(
      "replay-provider-delivery-id",
      row.providerDeliveryIdBytes,
    );
    const providerInstanceId = yield* decodeReplayText(
      "replay-provider-instance-id",
      row.providerInstanceIdBytes,
    );
    const providerTurnId = yield* decodeReplayText(
      "replay-provider-turn-id",
      row.providerTurnIdBytes,
    );
    const terminalAt = yield* decodeReplayText("replay-terminal-at", row.terminalAtBytes);
    const startEvidenceId = yield* decodeReplayText(
      "replay-start-evidence-id",
      row.startEvidenceIdBytes,
    );
    const startReceiptId = yield* decodeReplayText(
      "replay-start-receipt-id",
      row.startReceiptIdBytes,
    );
    const startMarkerId = yield* decodeReplayText("replay-start-marker-id", row.startMarkerIdBytes);
    const finalizationJson = yield* decodeReplayText("replay-json", row.finalizationJsonBytes);
    const fingerprint = yield* decodeReplayText("replay-fingerprint", row.fingerprintBytes);
    const evidenceReceiptId = yield* decodeReplayText(
      "replay-evidence-receipt",
      row.evidenceReceiptIdBytes,
    );
    const evidenceMarkerId = yield* decodeReplayText(
      "replay-evidence-marker",
      row.evidenceMarkerIdBytes,
    );
    const evidenceOutcome = yield* decodeReplayText(
      "replay-evidence-outcome",
      row.evidenceOutcomeBytes,
    );
    const evidenceTerminalCause = yield* decodeReplayText(
      "replay-evidence-terminal-cause",
      row.evidenceTerminalCauseBytes,
    );
    const evidenceDeliveryState = yield* decodeReplayText(
      "replay-evidence-delivery-state",
      row.evidenceDeliveryStateBytes,
    );
    const evidenceTerminalEventId = yield* decodeReplayText(
      "replay-evidence-terminal-event",
      row.evidenceTerminalEventIdBytes,
    );
    const evidenceFinalizedAt = yield* decodeReplayText(
      "replay-evidence-finalized-at",
      row.evidenceFinalizedAtBytes,
    );
    const evidenceStageEventId = yield* decodeReplayText(
      "replay-evidence-stage-event",
      row.evidenceStageEventIdBytes,
    );
    const evidenceLeaseEventId = yield* decodeReplayText(
      "replay-evidence-lease-event",
      row.evidenceLeaseEventIdBytes,
    );
    const evaluationAuthority = yield* decodeReplayText(
      "replay-evaluation-authority",
      row.evaluationAuthorityBytes,
    );
    const evaluationId = yield* decodeReplayNullable("replay-evaluation-id", row.evaluationIdBytes);
    const evaluationEvidenceId = yield* decodeReplayNullable(
      "replay-evaluation-evidence",
      row.evaluationEvidenceIdBytes,
    );
    const evaluationReceiptId = yield* decodeReplayNullable(
      "replay-evaluation-receipt",
      row.evaluationReceiptIdBytes,
    );
    const evaluationMarkerId = yield* decodeReplayNullable(
      "replay-evaluation-marker",
      row.evaluationMarkerIdBytes,
    );
    const evaluationDisposition = yield* decodeReplayNullable(
      "replay-evaluation-disposition",
      row.evaluationDispositionBytes,
    );
    const verificationVerdict = yield* decodeReplayNullable(
      "replay-verification-verdict",
      row.verificationVerdictBytes,
    );
    const invalidOutputCode = yield* decodeReplayNullable(
      "replay-invalid-output-code",
      row.invalidOutputCodeBytes,
    );
    const receiptId = yield* decodeReplayText("replay-receipt", row.receiptIdBytes);
    const receiptMarkerId = yield* decodeReplayText(
      "replay-receipt-marker",
      row.receiptMarkerIdBytes,
    );
    const receiptEvidenceId = yield* decodeReplayText(
      "replay-receipt-evidence",
      row.receiptEvidenceIdBytes,
    );
    const receiptCommandId = yield* decodeReplayText(
      "replay-receipt-command",
      row.receiptCommandIdBytes,
    );
    const receiptHandoffId = yield* decodeReplayText(
      "replay-receipt-handoff",
      row.receiptHandoffIdBytes,
    );
    const receiptFingerprint = yield* decodeReplayText(
      "replay-receipt-fingerprint",
      row.receiptFingerprintBytes,
    );
    const receiptOutcome = yield* decodeReplayText(
      "replay-receipt-outcome",
      row.receiptOutcomeBytes,
    );
    const receiptTerminalCause = yield* decodeReplayText(
      "replay-receipt-terminal-cause",
      row.receiptTerminalCauseBytes,
    );
    const receiptStageEventId = yield* decodeReplayText(
      "replay-receipt-stage-event",
      row.receiptStageEventIdBytes,
    );
    const receiptLeaseEventId = yield* decodeReplayText(
      "replay-receipt-lease-event",
      row.receiptLeaseEventIdBytes,
    );
    const receiptStatus = yield* decodeReplayText("replay-receipt-status", row.receiptStatusBytes);
    const receiptAcceptedAt = yield* decodeReplayText(
      "replay-receipt-accepted-at",
      row.receiptAcceptedAtBytes,
    );
    const markerId = yield* decodeReplayText("replay-marker", row.markerIdBytes);
    const markerReceiptId = yield* decodeReplayText(
      "replay-marker-receipt",
      row.markerReceiptIdBytes,
    );
    const markerEvidenceId = yield* decodeReplayText(
      "replay-marker-evidence",
      row.markerEvidenceIdBytes,
    );
    const markerCommandId = yield* decodeReplayText(
      "replay-marker-command",
      row.markerCommandIdBytes,
    );
    const markerHandoffId = yield* decodeReplayText(
      "replay-marker-handoff",
      row.markerHandoffIdBytes,
    );
    const markerFingerprint = yield* decodeReplayText(
      "replay-marker-fingerprint",
      row.markerFingerprintBytes,
    );
    const markerSeal = yield* decodeReplayText("replay-marker-seal", row.markerSealBytes);
    const markerCommittedAt = yield* decodeReplayText(
      "replay-marker-committed-at",
      row.markerCommittedAtBytes,
    );
    const parsed = yield* Effect.try({
      try: () => parseCanonicalJson(finalizationJson),
      catch: (cause) => error(handoffId, "replay-parse", "identity-mismatch", cause),
    }).pipe(
      Effect.flatMap(decodeReplayDocument),
      Effect.mapError((cause) =>
        isFinalizerError(cause)
          ? cause
          : error(handoffId, "replay-decode", "identity-mismatch", cause),
      ),
    );
    const commandId = deriveVerificationFinalizationCommandId(handoffId, parsed.handoffFingerprint);
    const evidenceId = deriveVerificationFinalizationEvidenceId(
      handoffId,
      parsed.handoffFingerprint,
    );
    const expectedReceiptId = deriveVerificationFinalizationReceiptId(
      handoffId,
      parsed.handoffFingerprint,
    );
    const expectedMarkerId = deriveVerificationFinalizationMarkerId(
      handoffId,
      parsed.handoffFingerprint,
    );
    const expectedStageEventId = deriveVerificationTerminalStageEventId(
      handoffId,
      parsed.handoffFingerprint,
    );
    const expectedLeaseEventId = deriveVerificationLeaseReleaseEventId(
      handoffId,
      parsed.handoffFingerprint,
    );
    const expectedFingerprint = fingerprintVerificationTurn("finalization-evidence", [
      finalizationJson,
    ]);
    const expectedMarkerSeal = fingerprintVerificationTurn("finalization-marker", [
      handoffId,
      parsed.handoffFingerprint,
      String(commandId),
      evidenceId,
      expectedFingerprint,
      parsed.stageEventId,
      String(parsed.stageEventSequence),
      parsed.leaseEventId,
      String(parsed.leaseEventSequence),
      parsed.finalizedAt,
    ]);
    const numbersAreValid = [
      parsed.stageEventSequence,
      parsed.leaseEventSequence,
      row.taskRevision,
      row.githubIntakeSequence,
      row.fenceToken,
      row.deliveryRevision,
      row.evidenceStageEventSequence,
      row.evidenceStageEventStreamVersion,
      row.evidenceLeaseEventSequence,
      row.evidenceLeaseEventStreamVersion,
      row.receiptStageEventSequence,
      row.receiptLeaseEventSequence,
    ].every((value) => typeof value === "number" && Number.isSafeInteger(value) && value >= 1);
    const evaluationMatchesOutcome =
      parsed.deliveryTerminalState === "completed"
        ? parsed.evaluation.evaluationAuthority === "accepted-evaluation" &&
          ((parsed.outcome === "succeeded" &&
            parsed.terminalCause === "verification-passed" &&
            parsed.evaluation.evaluationDisposition === "evaluated" &&
            parsed.evaluation.verificationVerdict === "passed" &&
            parsed.evaluation.invalidOutputCode === null) ||
            (parsed.outcome === "failed" &&
              parsed.terminalCause === "verification-failed" &&
              parsed.evaluation.evaluationDisposition === "evaluated" &&
              parsed.evaluation.verificationVerdict === "failed" &&
              parsed.evaluation.invalidOutputCode === null) ||
            (parsed.outcome === "failed" &&
              parsed.terminalCause === "verification-invalid-output" &&
              parsed.evaluation.evaluationDisposition === "invalid-output" &&
              parsed.evaluation.verificationVerdict === null &&
              parsed.evaluation.invalidOutputCode !== null))
        : parsed.evaluation.evaluationAuthority === "not-applicable" &&
          ((parsed.deliveryTerminalState === "failed" &&
            parsed.outcome === "failed" &&
            parsed.terminalCause === "provider-delivery-failed") ||
            (parsed.deliveryTerminalState === "interrupted" &&
              parsed.outcome === "cancelled" &&
              parsed.terminalCause === "provider-delivery-interrupted"));
    if (
      !numbersAreValid ||
      !evaluationMatchesOutcome ||
      parsed.handoffId !== handoffId ||
      parsed.finalizationCommandId !== commandId ||
      parsed.finalizationEvidenceId !== evidenceId ||
      parsed.stageEventId !== expectedStageEventId ||
      parsed.leaseEventId !== expectedLeaseEventId ||
      evidenceIdRaw !== evidenceId ||
      evidenceCommandId !== commandId ||
      evidenceHandoffId !== handoffId ||
      evidenceHandoffFingerprint !== parsed.handoffFingerprint ||
      fingerprint !== expectedFingerprint ||
      evidenceReceiptId !== expectedReceiptId ||
      evidenceMarkerId !== expectedMarkerId ||
      evidenceOutcome !== parsed.outcome ||
      evidenceTerminalCause !== parsed.terminalCause ||
      evidenceDeliveryState !== parsed.deliveryTerminalState ||
      evidenceTerminalEventId !== parsed.terminalRuntimeEventId ||
      terminalAt !== parsed.finalizedAt ||
      evidenceFinalizedAt !== parsed.finalizedAt ||
      evidenceStageEventId !== parsed.stageEventId ||
      row.evidenceStageEventSequence !== parsed.stageEventSequence ||
      row.evidenceStageEventStreamVersion !== parsed.stageEventStreamVersion ||
      evidenceLeaseEventId !== parsed.leaseEventId ||
      row.evidenceLeaseEventSequence !== parsed.leaseEventSequence ||
      row.evidenceLeaseEventStreamVersion !== parsed.leaseEventStreamVersion ||
      evaluationAuthority !== parsed.evaluation.evaluationAuthority ||
      evaluationId !== parsed.evaluation.evaluationId ||
      evaluationEvidenceId !== parsed.evaluation.evaluationEvidenceId ||
      evaluationReceiptId !== parsed.evaluation.evaluationReceiptId ||
      evaluationMarkerId !== parsed.evaluation.evaluationMarkerId ||
      evaluationDisposition !== parsed.evaluation.evaluationDisposition ||
      verificationVerdict !== parsed.evaluation.verificationVerdict ||
      invalidOutputCode !== parsed.evaluation.invalidOutputCode ||
      receiptId !== expectedReceiptId ||
      receiptMarkerId !== expectedMarkerId ||
      receiptEvidenceId !== evidenceId ||
      receiptCommandId !== commandId ||
      receiptHandoffId !== handoffId ||
      receiptFingerprint !== expectedFingerprint ||
      receiptOutcome !== parsed.outcome ||
      receiptTerminalCause !== parsed.terminalCause ||
      receiptStageEventId !== parsed.stageEventId ||
      row.receiptStageEventSequence !== parsed.stageEventSequence ||
      receiptLeaseEventId !== parsed.leaseEventId ||
      row.receiptLeaseEventSequence !== parsed.leaseEventSequence ||
      receiptStatus !== "accepted" ||
      receiptAcceptedAt !== parsed.finalizedAt ||
      markerId !== expectedMarkerId ||
      markerReceiptId !== expectedReceiptId ||
      markerEvidenceId !== evidenceId ||
      markerCommandId !== commandId ||
      markerHandoffId !== handoffId ||
      markerFingerprint !== expectedFingerprint ||
      markerSeal !== expectedMarkerSeal ||
      markerCommittedAt !== parsed.finalizedAt
    ) {
      return yield* error(handoffId, "replay-identity", "identity-mismatch");
    }
    const replayClaimOption = yield* store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-load-source-authority", "authority-conflict", cause),
        ),
      );
    if (Option.isNone(replayClaimOption)) {
      return yield* error(handoffId, "replay-source-authority", "identity-mismatch");
    }
    const replayClaim = replayClaimOption.value;
    const replayStart = yield* loadStartAuthority(handoffId, replayClaim);
    const sourcePayloads = [parsed.stagePayload, parsed.leasePayload] as const;
    if (
      replayClaim.delivery.providerTurnId === null ||
      replayClaim.delivery.terminalAt === null ||
      replayClaim.delivery.terminalEventId === null ||
      replayClaim.delivery.state !== parsed.deliveryTerminalState ||
      replayClaim.delivery.terminalEventId !== parsed.terminalRuntimeEventId ||
      replayClaim.delivery.terminalAt !== parsed.finalizedAt ||
      replayStart.startEvidenceId !== parsed.stagePayload.startEvidenceId ||
      replayStart.startReceiptId !== parsed.stagePayload.startReceiptId ||
      replayStart.startMarkerId !== parsed.stagePayload.startMarkerId ||
      replayStart.claimGeneration !== replayClaim.delivery.claimGeneration ||
      replayStart.attemptCount !== replayClaim.delivery.attemptCount ||
      sourcePayloads.some(
        (payload) =>
          payload.projectId !== replayClaim.evidence.projectId ||
          payload.taskId !== replayClaim.evidence.taskId ||
          payload.stageRunId !== replayClaim.evidence.stageRunId ||
          payload.attemptId !== replayClaim.evidence.attemptId ||
          payload.taskRevision !== replayClaim.evidence.taskRevision ||
          payload.githubIntakeSequence !== replayClaim.evidence.githubIntakeSequence ||
          payload.sourceIdentityFingerprint !== replayClaim.evidence.sourceIdentityFingerprint ||
          payload.admissionEvidenceId !== replayClaim.evidence.admissionEvidenceId ||
          payload.admissionReceiptId !== replayClaim.evidence.admissionReceiptId ||
          payload.admissionMarkerId !== replayClaim.evidence.admissionMarkerId ||
          payload.materializationEvidenceId !== replayClaim.evidence.materializationEvidenceId ||
          payload.materializationReceiptId !== replayClaim.evidence.materializationReceiptId ||
          payload.materializationMarkerId !== replayClaim.evidence.materializationMarkerId ||
          payload.startEvidenceId !== replayStart.startEvidenceId ||
          payload.startReceiptId !== replayStart.startReceiptId ||
          payload.startMarkerId !== replayStart.startMarkerId ||
          payload.handoffId !== replayClaim.evidence.handoffId ||
          payload.handoffFingerprint !== replayClaim.evidence.handoffFingerprint ||
          payload.controlledThreadReservationId !==
            replayClaim.evidence.controlledThreadReservationId ||
          payload.threadId !== replayClaim.evidence.threadId ||
          payload.planningThreadId !== replayClaim.evidence.planningThreadId ||
          payload.planId !== replayClaim.evidence.planId ||
          payload.proposedPlanDigest !== replayClaim.evidence.proposedPlanDigest ||
          payload.providerDeliveryId !== replayClaim.evidence.providerDeliveryId ||
          payload.deliveryRevision !== replayClaim.delivery.revision ||
          payload.providerInstanceId !== replayClaim.evidence.providerInstanceId ||
          payload.providerTurnId !== replayClaim.delivery.providerTurnId ||
          payload.runtimeMode !== replayClaim.evidence.runtimeMode ||
          payload.modelSelectionFingerprint !== replayClaim.evidence.modelSelectionFingerprint ||
          payload.leaseId !== replayClaim.evidence.leaseId ||
          payload.fenceToken !== replayClaim.evidence.fenceToken ||
          payload.deliveryTerminalState !== replayClaim.delivery.state ||
          payload.terminalRuntimeEventId !== replayClaim.delivery.terminalEventId,
      ) ||
      parsed.stagePayload.claimGeneration !== replayClaim.delivery.claimGeneration ||
      parsed.stagePayload.attemptCount !== replayClaim.delivery.attemptCount ||
      parsed.stagePayload.leaseHolderId !== replayClaim.evidence.leaseHolderId ||
      parsed.leasePayload.holderId !== replayClaim.evidence.leaseHolderId
    ) {
      return yield* error(handoffId, "replay-source-authority", "identity-mismatch");
    }
    const stage = yield* loadAuthoritativeStageRunState(
      AgentControlStageRunId.make(stageRunId),
      stageEvents,
      stageStates,
    ).pipe(
      Effect.mapError((cause) => error(handoffId, "replay-stage", "stage-history-corrupt", cause)),
    );
    const terminalStage = Option.isSome(stage) ? stage.value.events[2] : undefined;
    const expectedStageEventType =
      parsed.outcome === "succeeded"
        ? "agentControl.stageRun.verificationSucceeded"
        : parsed.outcome === "failed"
          ? "agentControl.stageRun.verificationFailed"
          : "agentControl.stageRun.verificationCancelled";
    if (
      Option.isNone(stage) ||
      stage.value.state.status !== parsed.outcome ||
      stage.value.state.revision !== 3 ||
      stage.value.state.sequence !== parsed.stageEventSequence ||
      terminalStage?.type !== expectedStageEventType ||
      terminalStage.eventId !== parsed.stageEventId ||
      terminalStage.sequence !== parsed.stageEventSequence ||
      terminalStage.streamVersion !== parsed.stageEventStreamVersion ||
      terminalStage.commandId !== commandId ||
      terminalStage.correlationId !== commandId ||
      terminalStage.causationEventId !== parsed.terminalRuntimeEventId ||
      terminalStage.payload.handoffId !== handoffId ||
      terminalStage.payload.handoffFingerprint !== parsed.handoffFingerprint ||
      terminalStage.payload.projectId !== projectId ||
      terminalStage.payload.taskId !== taskId ||
      terminalStage.payload.attemptId !== attemptId ||
      terminalStage.payload.taskRevision !== row.taskRevision ||
      terminalStage.payload.githubIntakeSequence !== row.githubIntakeSequence ||
      terminalStage.payload.sourceIdentityFingerprint !== sourceIdentityFingerprint ||
      terminalStage.payload.startEvidenceId !== startEvidenceId ||
      terminalStage.payload.startReceiptId !== startReceiptId ||
      terminalStage.payload.startMarkerId !== startMarkerId ||
      terminalStage.payload.providerDeliveryId !== providerDeliveryId ||
      terminalStage.payload.providerInstanceId !== providerInstanceId ||
      terminalStage.payload.providerTurnId !== providerTurnId ||
      terminalStage.payload.deliveryRevision !== row.deliveryRevision ||
      terminalStage.payload.leaseId !== leaseId ||
      terminalStage.payload.leaseHolderId !== leaseHolderId ||
      terminalStage.payload.fenceToken !== row.fenceToken ||
      terminalStage.payload.deliveryTerminalState !== parsed.deliveryTerminalState ||
      terminalStage.payload.terminalCause !== parsed.terminalCause ||
      terminalStage.payload.status !== parsed.outcome ||
      terminalStage.payload.terminalRuntimeEventId !== parsed.terminalRuntimeEventId ||
      terminalStage.payload.finalizationEvidenceId !== evidenceId ||
      terminalStage.payload.finalizedAt !== parsed.finalizedAt ||
      canonicalJson(terminalStage.payload.evaluation as unknown as JsonValue) !==
        canonicalJson(parsed.evaluation as unknown as JsonValue) ||
      canonicalJson(terminalStage.payload as unknown as JsonValue) !==
        canonicalJson(parsed.stagePayload as unknown as JsonValue)
    ) {
      return yield* error(handoffId, "replay-stage", "stage-history-corrupt");
    }
    const lease = yield* loadAuthoritativeLeaseState(
      AgentControlStageRunLeaseId.make(leaseId),
      leaseEvents,
      leaseStates,
    ).pipe(
      Effect.mapError((cause) => error(handoffId, "replay-lease", "lease-history-corrupt", cause)),
    );
    const leaseLast = Option.isSome(lease) ? lease.value.events.at(-1) : undefined;
    if (
      Option.isNone(lease) ||
      lease.value.state.status !== "released" ||
      lease.value.state.stageRunId !== stageRunId ||
      lease.value.state.attemptId !== attemptId ||
      lease.value.state.projectId !== projectId ||
      lease.value.state.taskId !== taskId ||
      lease.value.state.taskRevision !== row.taskRevision ||
      lease.value.state.githubIntakeSequence !== row.githubIntakeSequence ||
      lease.value.state.sourceIdentityFingerprint !== sourceIdentityFingerprint ||
      lease.value.state.holderId !== leaseHolderId ||
      lease.value.state.fenceToken !== row.fenceToken ||
      lease.value.state.revision !== leaseLast?.streamVersion ||
      lease.value.state.sequence !== parsed.leaseEventSequence ||
      leaseLast?.eventId !== parsed.leaseEventId ||
      leaseLast.sequence !== parsed.leaseEventSequence ||
      leaseLast.streamVersion !== parsed.leaseEventStreamVersion ||
      leaseLast.type !== "agentControl.stageRunLease.releasedAfterVerification" ||
      leaseLast.commandId !== commandId ||
      leaseLast.correlationId !== commandId ||
      leaseLast.causationEventId !== parsed.stageEventId ||
      leaseLast.payload.handoffId !== handoffId ||
      leaseLast.payload.handoffFingerprint !== parsed.handoffFingerprint ||
      leaseLast.payload.leaseId !== leaseId ||
      leaseLast.payload.projectId !== projectId ||
      leaseLast.payload.taskId !== taskId ||
      leaseLast.payload.stageRunId !== stageRunId ||
      leaseLast.payload.attemptId !== attemptId ||
      leaseLast.payload.taskRevision !== row.taskRevision ||
      leaseLast.payload.githubIntakeSequence !== row.githubIntakeSequence ||
      leaseLast.payload.sourceIdentityFingerprint !== sourceIdentityFingerprint ||
      leaseLast.payload.holderId !== leaseHolderId ||
      leaseLast.payload.fenceToken !== row.fenceToken ||
      leaseLast.payload.startEvidenceId !== startEvidenceId ||
      leaseLast.payload.startReceiptId !== startReceiptId ||
      leaseLast.payload.startMarkerId !== startMarkerId ||
      leaseLast.payload.providerDeliveryId !== providerDeliveryId ||
      leaseLast.payload.providerInstanceId !== providerInstanceId ||
      leaseLast.payload.providerTurnId !== providerTurnId ||
      leaseLast.payload.deliveryRevision !== row.deliveryRevision ||
      leaseLast.payload.deliveryTerminalState !== parsed.deliveryTerminalState ||
      leaseLast.payload.terminalCause !== parsed.terminalCause ||
      leaseLast.payload.stageStatus !== parsed.outcome ||
      leaseLast.payload.terminalRuntimeEventId !== parsed.terminalRuntimeEventId ||
      leaseLast.payload.finalizationEvidenceId !== evidenceId ||
      leaseLast.payload.stageEventId !== parsed.stageEventId ||
      leaseLast.payload.releasedAt !== parsed.finalizedAt ||
      canonicalJson(leaseLast.payload.evaluation as unknown as JsonValue) !==
        canonicalJson(parsed.evaluation as unknown as JsonValue) ||
      canonicalJson(leaseLast.payload as unknown as JsonValue) !==
        canonicalJson(parsed.leasePayload as unknown as JsonValue)
    ) {
      return yield* error(handoffId, "replay-lease", "lease-history-corrupt");
    }
    return Option.some(evidenceId);
  });

  const finalizeInTransaction = Effect.fn(
    "AgentControlVerificationStageFinalizer.finalizeInTransaction",
  )(function* (handoffId: string) {
    const replay = yield* replayFirst(handoffId);
    if (Option.isSome(replay)) {
      return { _tag: "Replayed", finalizationEvidenceId: replay.value } as const;
    }
    const claimOption = yield* store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) => error(handoffId, "load-handoff", "authority-conflict", cause)),
      );
    if (Option.isNone(claimOption)) {
      return yield* error(handoffId, "load-handoff", "authority-conflict");
    }
    const claim = claimOption.value;
    if (
      (claim.delivery.state !== "completed" &&
        claim.delivery.state !== "failed" &&
        claim.delivery.state !== "interrupted") ||
      claim.delivery.providerTurnId === null ||
      claim.delivery.terminalAt === null ||
      claim.delivery.terminalEventId === null
    ) {
      return { _tag: "Waiting" } as const;
    }
    const start = yield* loadStartAuthority(handoffId, claim);
    const stageRunId = AgentControlStageRunId.make(claim.evidence.stageRunId);
    const stage = yield* loadAuthoritativeStageRunState(stageRunId, stageEvents, stageStates).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "load-stage-history", "stage-history-corrupt", cause),
      ),
    );
    const startEvent = Option.isSome(stage) ? stage.value.events[1] : undefined;
    if (
      Option.isNone(stage) ||
      stage.value.state.status !== "running" ||
      stage.value.state.revision !== 2 ||
      stage.value.state.roleId !== "verifier" ||
      stage.value.state.stageKind !== "verification" ||
      stage.value.state.stageOrdinal !== 3 ||
      stage.value.state.attemptOrdinal !== 1 ||
      stage.value.state.attemptId !== claim.evidence.attemptId ||
      stage.value.state.taskRevision !== claim.evidence.taskRevision ||
      stage.value.state.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
      stage.value.state.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint ||
      stage.value.events.length !== 2 ||
      startEvent?.type !== "agentControl.stageRun.verificationStarted" ||
      startEvent.eventId !== start.stageEventId ||
      startEvent.sequence !== start.stageEventSequence ||
      startEvent.commandId !== start.startCommandId ||
      startEvent.correlationId !== start.startCommandId ||
      startEvent.causationEventId !== claim.evidence.turnRequestEventId ||
      startEvent.occurredAt !== start.startedAt ||
      startEvent.payload.admissionEvidenceId !== claim.evidence.admissionEvidenceId ||
      startEvent.payload.admissionReceiptId !== claim.evidence.admissionReceiptId ||
      startEvent.payload.admissionMarkerId !== claim.evidence.admissionMarkerId ||
      startEvent.payload.materializationEvidenceId !== claim.evidence.materializationEvidenceId ||
      startEvent.payload.materializationReceiptId !== claim.evidence.materializationReceiptId ||
      startEvent.payload.materializationMarkerId !== claim.evidence.materializationMarkerId ||
      startEvent.payload.handoffId !== handoffId ||
      startEvent.payload.handoffFingerprint !== claim.evidence.handoffFingerprint ||
      startEvent.payload.providerDeliveryId !== claim.evidence.providerDeliveryId ||
      startEvent.payload.deliveryRevision !== start.deliveryRevision ||
      startEvent.payload.claimGeneration !== start.claimGeneration ||
      startEvent.payload.attemptCount !== start.attemptCount ||
      startEvent.payload.controlledThreadReservationId !==
        claim.evidence.controlledThreadReservationId ||
      startEvent.payload.threadId !== claim.evidence.threadId ||
      startEvent.payload.planningThreadId !== claim.evidence.planningThreadId ||
      startEvent.payload.planId !== claim.evidence.planId ||
      startEvent.payload.proposedPlanDigest !== claim.evidence.proposedPlanDigest ||
      startEvent.payload.providerInstanceId !== claim.evidence.providerInstanceId ||
      startEvent.payload.providerTurnId !== claim.delivery.providerTurnId ||
      startEvent.payload.runtimeMode !== claim.evidence.runtimeMode ||
      startEvent.payload.modelSelectionFingerprint !== claim.evidence.modelSelectionFingerprint ||
      startEvent.payload.leaseId !== claim.evidence.leaseId ||
      startEvent.payload.leaseHolderId !== claim.evidence.leaseHolderId ||
      startEvent.payload.fenceToken !== claim.evidence.fenceToken ||
      startEvent.payload.startedAt !== start.startedAt
    ) {
      return yield* error(handoffId, "validate-stage-history", "stage-history-corrupt");
    }
    const leaseId = AgentControlStageRunLeaseId.make(claim.evidence.leaseId);
    const lease = yield* loadAuthoritativeLeaseState(leaseId, leaseEvents, leaseStates).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "load-lease-history", "lease-history-corrupt", cause),
      ),
    );
    if (
      Option.isNone(lease) ||
      lease.value.state.status !== "reserved" ||
      lease.value.state.stageRunId !== stageRunId ||
      lease.value.state.attemptId !== claim.evidence.attemptId ||
      lease.value.state.projectId !== claim.evidence.projectId ||
      lease.value.state.taskId !== claim.evidence.taskId ||
      lease.value.state.taskRevision !== claim.evidence.taskRevision ||
      lease.value.state.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
      lease.value.state.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint ||
      lease.value.state.holderId !== claim.evidence.leaseHolderId ||
      lease.value.state.fenceToken !== claim.evidence.fenceToken
    ) {
      return yield* error(handoffId, "validate-lease-history", "lease-history-corrupt");
    }
    const evaluation = yield* loadEvaluationAuthority(handoffId, claim, start);
    yield* hooks.afterAuthoritativeRead(handoffId);

    const mapping =
      claim.delivery.state === "completed"
        ? evaluation.evaluationAuthority !== "accepted-evaluation"
          ? null
          : evaluation.evaluationDisposition === "invalid-output"
            ? {
                outcome: "failed" as const,
                terminalCause: "verification-invalid-output" as const,
                stageType: "agentControl.stageRun.verificationFailed" as const,
              }
            : evaluation.verificationVerdict === "passed"
              ? {
                  outcome: "succeeded" as const,
                  terminalCause: "verification-passed" as const,
                  stageType: "agentControl.stageRun.verificationSucceeded" as const,
                }
              : {
                  outcome: "failed" as const,
                  terminalCause: "verification-failed" as const,
                  stageType: "agentControl.stageRun.verificationFailed" as const,
                }
        : claim.delivery.state === "failed"
          ? {
              outcome: "failed" as const,
              terminalCause: "provider-delivery-failed" as const,
              stageType: "agentControl.stageRun.verificationFailed" as const,
            }
          : {
              outcome: "cancelled" as const,
              terminalCause: "provider-delivery-interrupted" as const,
              stageType: "agentControl.stageRun.verificationCancelled" as const,
            };
    if (mapping === null) {
      return yield* error(handoffId, "map-terminal-authority", "evaluation-conflict");
    }
    const commandId = deriveVerificationFinalizationCommandId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const finalizationEvidenceId = deriveVerificationFinalizationEvidenceId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const receiptId = deriveVerificationFinalizationReceiptId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const markerId = deriveVerificationFinalizationMarkerId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const stageEventId = deriveVerificationTerminalStageEventId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const leaseEventId = deriveVerificationLeaseReleaseEventId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const finalizedAt = claim.delivery.terminalAt;
    const terminalRuntimeEventId = EventId.make(claim.delivery.terminalEventId);
    const commonPayload = {
      projectId: ProjectId.make(claim.evidence.projectId),
      taskId: AgentControlTaskId.make(claim.evidence.taskId),
      stageRunId,
      attemptId: AgentControlAttemptId.make(claim.evidence.attemptId),
      roleId: "verifier" as const,
      stageKind: "verification" as const,
      stageOrdinal: 3 as const,
      attemptOrdinal: 1 as const,
      taskRevision: claim.evidence.taskRevision,
      githubIntakeSequence: claim.evidence.githubIntakeSequence,
      sourceIdentityFingerprint: claim.evidence.sourceIdentityFingerprint,
      admissionEvidenceId: claim.evidence.admissionEvidenceId,
      admissionReceiptId: claim.evidence.admissionReceiptId,
      admissionMarkerId: claim.evidence.admissionMarkerId,
      materializationEvidenceId: claim.evidence.materializationEvidenceId,
      materializationReceiptId: claim.evidence.materializationReceiptId,
      materializationMarkerId: claim.evidence.materializationMarkerId,
      startEvidenceId: start.startEvidenceId,
      startReceiptId: start.startReceiptId,
      startMarkerId: start.startMarkerId,
      handoffId,
      handoffFingerprint: claim.evidence.handoffFingerprint,
      providerDeliveryId: claim.evidence.providerDeliveryId,
      deliveryRevision: claim.delivery.revision,
      claimGeneration: claim.delivery.claimGeneration,
      attemptCount: claim.delivery.attemptCount,
      controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
      threadId: claim.evidence.threadId,
      planningThreadId: claim.evidence.planningThreadId,
      planId: claim.evidence.planId,
      proposedPlanDigest: claim.evidence.proposedPlanDigest,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: claim.delivery.providerTurnId,
      runtimeMode: claim.evidence.runtimeMode,
      modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
      leaseId,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(claim.evidence.leaseHolderId),
      fenceToken: claim.evidence.fenceToken,
      terminalRuntimeEventId,
      finalizationEvidenceId,
      finalizedAt,
    };
    const stageDraft: AgentControlStageRunEventDraft = {
      eventId: stageEventId,
      type: mapping.stageType,
      aggregateKind: "stage-run",
      aggregateId: stageRunId,
      occurredAt: finalizedAt,
      commandId,
      causationEventId: terminalRuntimeEventId,
      correlationId: commandId,
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: {
        ...commonPayload,
        deliveryTerminalState: claim.delivery.state,
        terminalCause: mapping.terminalCause,
        status: mapping.outcome,
        evaluation,
      },
    } as AgentControlStageRunEventDraft;
    const appendedStage = yield* stageEvents
      .append({ stageRunId, expectedStreamVersion: 2, events: [stageDraft] })
      .pipe(
        Effect.mapError((cause) =>
          error(
            handoffId,
            "append-terminal-stage",
            cause._tag === "AgentControlStageRunStreamVersionConflictError"
              ? "revision-conflict"
              : "persistence",
            cause,
          ),
        ),
      );
    const stageEvent = appendedStage[0];
    if (appendedStage.length !== 1 || stageEvent === undefined) {
      return yield* error(handoffId, "append-terminal-stage", "persistence");
    }
    yield* stageProjection
      .projectEvent(stageEvent)
      .pipe(
        Effect.mapError((cause) =>
          error(handoffId, "project-terminal-stage", "persistence", cause),
        ),
      );
    yield* hooks.afterStageProjection(handoffId);

    const leaseDraft: AgentControlStageRunLeaseEventDraft = {
      eventId: leaseEventId,
      type: "agentControl.stageRunLease.releasedAfterVerification",
      aggregateKind: "stage-run-lease",
      aggregateId: leaseId,
      occurredAt: finalizedAt,
      commandId,
      causationEventId: stageEventId,
      correlationId: commandId,
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: {
        leaseId,
        projectId: commonPayload.projectId,
        taskId: commonPayload.taskId,
        stageRunId,
        attemptId: commonPayload.attemptId,
        taskRevision: commonPayload.taskRevision,
        githubIntakeSequence: commonPayload.githubIntakeSequence,
        sourceIdentityFingerprint: commonPayload.sourceIdentityFingerprint,
        holderId: commonPayload.leaseHolderId,
        fenceToken: commonPayload.fenceToken,
        admissionEvidenceId: commonPayload.admissionEvidenceId,
        admissionReceiptId: commonPayload.admissionReceiptId,
        admissionMarkerId: commonPayload.admissionMarkerId,
        materializationEvidenceId: commonPayload.materializationEvidenceId,
        materializationReceiptId: commonPayload.materializationReceiptId,
        materializationMarkerId: commonPayload.materializationMarkerId,
        startEvidenceId: commonPayload.startEvidenceId,
        startReceiptId: commonPayload.startReceiptId,
        startMarkerId: commonPayload.startMarkerId,
        handoffId,
        handoffFingerprint: commonPayload.handoffFingerprint,
        controlledThreadReservationId: commonPayload.controlledThreadReservationId,
        threadId: commonPayload.threadId,
        planningThreadId: commonPayload.planningThreadId,
        planId: commonPayload.planId,
        proposedPlanDigest: commonPayload.proposedPlanDigest,
        providerDeliveryId: commonPayload.providerDeliveryId,
        deliveryRevision: commonPayload.deliveryRevision,
        providerInstanceId: commonPayload.providerInstanceId,
        providerTurnId: commonPayload.providerTurnId,
        runtimeMode: commonPayload.runtimeMode,
        modelSelectionFingerprint: commonPayload.modelSelectionFingerprint,
        terminalRuntimeEventId,
        finalizationEvidenceId,
        stageEventId,
        deliveryTerminalState: claim.delivery.state,
        terminalCause: mapping.terminalCause,
        stageStatus: mapping.outcome,
        evaluation,
        releasedAt: finalizedAt,
      },
    } as AgentControlStageRunLeaseEventDraft;
    const appendedLease = yield* leaseEvents
      .append({ leaseId, expectedStreamVersion: lease.value.state.revision, events: [leaseDraft] })
      .pipe(
        Effect.mapError((cause) =>
          error(
            handoffId,
            "append-lease-release",
            cause._tag === "AgentControlStageRunLeaseStreamVersionConflictError"
              ? "revision-conflict"
              : "persistence",
            cause,
          ),
        ),
      );
    const leaseEvent = appendedLease[0];
    if (appendedLease.length !== 1 || leaseEvent === undefined) {
      return yield* error(handoffId, "append-lease-release", "persistence");
    }
    yield* leaseProjection
      .projectEvent(leaseEvent)
      .pipe(
        Effect.mapError((cause) => error(handoffId, "project-lease-release", "persistence", cause)),
      );
    yield* hooks.afterLeaseProjection(handoffId);

    const finalizationDocument = {
      schemaVersion: 1,
      handoffId,
      handoffFingerprint: claim.evidence.handoffFingerprint,
      finalizationCommandId: commandId,
      finalizationEvidenceId,
      outcome: mapping.outcome,
      terminalCause: mapping.terminalCause,
      deliveryTerminalState: claim.delivery.state,
      terminalRuntimeEventId,
      evaluation,
      stageEventId: stageEvent.eventId,
      stageEventSequence: stageEvent.sequence,
      stageEventStreamVersion: stageEvent.streamVersion,
      stagePayload: stageEvent.payload,
      leaseEventId: leaseEvent.eventId,
      leaseEventSequence: leaseEvent.sequence,
      leaseEventStreamVersion: leaseEvent.streamVersion,
      leasePayload: leaseEvent.payload,
      finalizedAt,
    } as const;
    const finalizationJson = canonicalJson(finalizationDocument as unknown as JsonValue);
    const finalizationFingerprint = fingerprintVerificationTurn("finalization-evidence", [
      finalizationJson,
    ]);
    const markerFingerprint = fingerprintVerificationTurn("finalization-marker", [
      handoffId,
      claim.evidence.handoffFingerprint,
      String(commandId),
      finalizationEvidenceId,
      finalizationFingerprint,
      String(stageEvent.eventId),
      String(stageEvent.sequence),
      String(leaseEvent.eventId),
      String(leaseEvent.sequence),
      finalizedAt,
    ]);
    yield* sql`
      INSERT INTO main.agent_control_verification_finalization_evidence (
        finalization_evidence_id, receipt_id, marker_id, finalization_command_id,
        finalization_fingerprint, finalization_json, handoff_id, handoff_fingerprint,
        project_id, task_id, task_revision, github_intake_sequence,
        source_identity_fingerprint, stage_run_id, attempt_id, lease_id,
        lease_holder_id, fence_token, provider_delivery_id, provider_instance_id,
        provider_turn_id, delivery_revision, delivery_terminal_state,
        terminal_runtime_event_id, terminal_at, start_evidence_id, start_receipt_id,
        start_marker_id, evaluation_authority, evaluation_id, evaluation_evidence_id,
        evaluation_receipt_id, evaluation_marker_id, evaluation_disposition,
        verification_verdict, invalid_output_code, outcome, terminal_cause,
        stage_event_id, stage_event_sequence, stage_event_stream_version,
        lease_event_id, lease_event_sequence, lease_event_stream_version, finalized_at
      ) VALUES (
        ${finalizationEvidenceId}, ${receiptId}, ${markerId}, ${commandId},
        ${finalizationFingerprint}, ${finalizationJson}, ${handoffId},
        ${claim.evidence.handoffFingerprint}, ${claim.evidence.projectId},
        ${claim.evidence.taskId}, ${claim.evidence.taskRevision},
        ${claim.evidence.githubIntakeSequence}, ${claim.evidence.sourceIdentityFingerprint},
        ${claim.evidence.stageRunId}, ${claim.evidence.attemptId}, ${claim.evidence.leaseId},
        ${claim.evidence.leaseHolderId}, ${claim.evidence.fenceToken},
        ${claim.evidence.providerDeliveryId}, ${claim.evidence.providerInstanceId},
        ${claim.delivery.providerTurnId}, ${claim.delivery.revision}, ${claim.delivery.state},
        ${claim.delivery.terminalEventId}, ${claim.delivery.terminalAt},
        ${start.startEvidenceId}, ${start.startReceiptId}, ${start.startMarkerId},
        ${evaluation.evaluationAuthority}, ${evaluation.evaluationId},
        ${evaluation.evaluationEvidenceId}, ${evaluation.evaluationReceiptId},
        ${evaluation.evaluationMarkerId}, ${evaluation.evaluationDisposition},
        ${evaluation.verificationVerdict}, ${evaluation.invalidOutputCode},
        ${mapping.outcome}, ${mapping.terminalCause}, ${stageEvent.eventId},
        ${stageEvent.sequence}, ${stageEvent.streamVersion}, ${leaseEvent.eventId},
        ${leaseEvent.sequence}, ${leaseEvent.streamVersion}, ${finalizedAt}
      )
    `.pipe(
      Effect.mapError((cause) =>
        error(handoffId, "insert-finalization-evidence", "persistence", cause),
      ),
    );
    yield* hooks.afterEvidence(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_verification_finalization_receipts (
        receipt_id, marker_id, finalization_evidence_id, finalization_command_id,
        finalization_fingerprint, handoff_id, outcome, terminal_cause,
        stage_event_id, stage_event_sequence, lease_event_id, lease_event_sequence,
        status, accepted_at
      ) VALUES (
        ${receiptId}, ${markerId}, ${finalizationEvidenceId}, ${commandId},
        ${finalizationFingerprint}, ${handoffId}, ${mapping.outcome},
        ${mapping.terminalCause}, ${stageEvent.eventId}, ${stageEvent.sequence},
        ${leaseEvent.eventId}, ${leaseEvent.sequence}, 'accepted', ${finalizedAt}
      )
    `.pipe(
      Effect.mapError((cause) =>
        error(handoffId, "insert-finalization-receipt", "persistence", cause),
      ),
    );
    yield* hooks.afterReceipt(handoffId);
    yield* hooks.beforeMarker(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_verification_finalization_markers (
        marker_id, marker_fingerprint, receipt_id, finalization_evidence_id,
        finalization_command_id, finalization_fingerprint, handoff_id, committed_at
      ) VALUES (
        ${markerId}, ${markerFingerprint}, ${receiptId}, ${finalizationEvidenceId},
        ${commandId}, ${finalizationFingerprint}, ${handoffId}, ${finalizedAt}
      )
    `.pipe(
      Effect.mapError((cause) =>
        error(handoffId, "insert-finalization-marker", "persistence", cause),
      ),
    );
    const releasedProviderInstanceId =
      providerAdmissionRelease === undefined
        ? null
        : yield* providerAdmissionRelease
            .releaseInTransaction({
              stage: "verification",
              handoffId,
              finalizedAt,
            })
            .pipe(
              Effect.mapError((cause) =>
                error(handoffId, "provider-admission-release", "persistence", cause),
              ),
            );
    return {
      _tag: "Finalized",
      releasedProviderInstanceId,
      publication: {
        handoffId,
        finalizationEvidenceId,
        outcome: mapping.outcome,
        stageEvent,
        leaseEvent,
      } satisfies AgentControlVerificationStageFinalizationPublication,
    } as const;
  });

  const processFresh = Effect.fn("AgentControlVerificationStageFinalizer.processFresh")(function* (
    handoffId: string,
  ) {
    const claimOption = yield* store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) =>
          error(handoffId, "load-before-evaluate", "authority-conflict", cause),
        ),
      );
    if (Option.isNone(claimOption)) return { _tag: "Waiting" } as const;
    if (claimOption.value.delivery.state === "completed") {
      yield* evaluator
        .processHandoff(handoffId)
        .pipe(
          Effect.mapError((cause) =>
            error(
              handoffId,
              cause.operation,
              cause.reason === "persistence" ? "persistence" : "evaluation-conflict",
              cause,
            ),
          ),
        );
    }
    yield* hooks.beforeTransaction(handoffId);
    const transactionExit = yield* Effect.exit(
      sql.withTransaction(finalizeInTransaction(handoffId)),
    );
    if (Exit.isFailure(transactionExit)) {
      const replay = yield* replayFirst(handoffId);
      if (Option.isSome(replay)) {
        return { _tag: "Replayed", finalizationEvidenceId: replay.value } as const;
      }
      return yield* Effect.failCause(transactionExit.cause);
    }
    const transaction = transactionExit.value;
    if (transaction._tag !== "Finalized") return transaction;
    yield* hooks.afterCommit(handoffId);
    yield* providerAdmissionRelease === undefined
      ? Effect.void
      : providerAdmissionRelease.signalCommitted(transaction.releasedProviderInstanceId);
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        yield* stageEngine.publishCommitted([transaction.publication.stageEvent]);
        yield* leaseEngine.publishCommitted([transaction.publication.leaseEvent]);
      }),
    );
    yield* hooks.afterPublication(handoffId);
    return {
      _tag: "Finalized",
      finalizationEvidenceId: transaction.publication.finalizationEvidenceId,
    } as const;
  });

  const processHandoff: AgentControlVerificationStageFinalizerShape["processHandoff"] = (
    handoffId,
  ) =>
    Effect.gen(function* () {
      const replay = yield* replayFirst(handoffId);
      if (Option.isSome(replay)) {
        return { _tag: "Replayed", finalizationEvidenceId: replay.value } as const;
      }
      return yield* processFresh(handoffId);
    }).pipe(
      Effect.mapError((cause) =>
        isFinalizerError(cause) ? cause : error(handoffId, "process", "persistence", cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = hooks.recoveryPageSize ?? 64;
    const listCandidates = store.listStageFinalizationCandidates;
    if (listCandidates === undefined) {
      return yield* error("recovery", "list-candidates", "persistence");
    }
    let cursor = "";
    while (true) {
      const handoffIds = yield* listCandidates(cursor, pageSize).pipe(
        Effect.mapError((cause) => error("recovery", "list-candidates", "persistence", cause)),
      );
      if (handoffIds.length === 0) break;
      yield* Effect.forEach(
        handoffIds,
        (handoffId) =>
          processHandoff(handoffId).pipe(
            Effect.catchIf(
              (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
              (cause) =>
                Effect.logError("verification finalization candidate failed", {
                  handoffId,
                  operation: cause.operation,
                  reason: cause.reason,
                }),
            ),
          ),
        { concurrency: 1, discard: true },
      );
      cursor = handoffIds.at(-1)!;
      if (handoffIds.length < pageSize) break;
    }
  });
  const processSafely = (handoffId: string | null) =>
    handoffId === null
      ? recover
      : processHandoff(handoffId).pipe(
          Effect.asVoid,
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
            (cause) =>
              Effect.logError("verification finalization candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              }),
          ),
        );
  let nextAttemptId = 0;
  let activeWorker:
    | {
        readonly attemptId: number;
        readonly drain: Effect.Effect<void, AgentControlVerificationStageFinalizerError>;
      }
    | undefined;
  let terminalDrain: Effect.Effect<void, AgentControlVerificationStageFinalizerError> = Effect.void;
  const prepare: AgentControlVerificationStageFinalizerShape["prepare"] = Effect.fn(
    "AgentControlVerificationStageFinalizer.prepare",
  )(function* (activation) {
    const ownerScope = yield* Scope.Scope;
    const worker = yield* makeDrainableWorker(processSafely, { failureMode: "observable" });
    nextAttemptId += 1;
    const attemptId = nextAttemptId;
    activeWorker = { attemptId, drain: worker.drain };
    yield* Scope.addFinalizer(
      ownerScope,
      Effect.sync(() => {
        if (activeWorker?.attemptId !== attemptId) return;
        terminalDrain = activeWorker.drain;
        activeWorker = undefined;
      }),
    );
    const handoffs = yield* wakeup.subscribe;
    yield* Effect.forkScoped(
      Stream.runForEach(handoffs, (handoffId) =>
        activation.pipe(Effect.andThen(worker.enqueue(handoffId))),
      ),
      { startImmediately: true },
    );
    yield* Effect.forkScoped(activation.pipe(Effect.andThen(worker.enqueue(null))), {
      startImmediately: true,
    });
  });

  return AgentControlVerificationStageFinalizer.of({
    processHandoff,
    recover,
    prepare,
    drain: Effect.suspend(() => activeWorker?.drain ?? terminalDrain),
  });
});

export const AgentControlVerificationStageFinalizerLive = Layer.effect(
  AgentControlVerificationStageFinalizer,
  make,
);
