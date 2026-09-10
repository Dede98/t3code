import { AgentControlRunOnceReadNotifications } from "../../runOnce/readNotifications.ts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import {
  deriveVerificationEvaluationEvidenceId,
  deriveVerificationEvaluationId,
  deriveVerificationEvaluationMarkerId,
  deriveVerificationEvaluationReceiptId,
  fingerprintVerificationTurn,
} from "../identity.ts";
import { loadVerificationResultSource } from "../orchestrationResultSource.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION } from "../prompt.ts";
import {
  AgentControlVerificationEvaluationError,
  AgentControlVerificationEvaluator,
  type AgentControlVerificationEvaluatorShape,
} from "../Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationEvaluatorHooks } from "../Services/AgentControlVerificationEvaluatorHooks.ts";
import { AgentControlVerificationHandoffStore } from "../Services/AgentControlVerificationHandoffStore.ts";
import { evaluateCheckedVerificationResult } from "../checkedResult.ts";
import { sealVerificationCheckAssessment } from "../checkEvidence.ts";
import { isLegacyVerificationEvaluation } from "../legacyEvaluation.ts";

const RECOVERY_INTERVAL = Duration.seconds(5);
const isEvaluationError = Schema.is(AgentControlVerificationEvaluationError);

const evaluationError = (
  operation: string,
  reason: AgentControlVerificationEvaluationError["reason"],
  handoffId?: string,
  cause?: unknown,
) =>
  new AgentControlVerificationEvaluationError({
    operation,
    reason,
    ...(handoffId === undefined ? {} : { handoffId }),
    ...(cause === undefined ? {} : { cause }),
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlVerificationHandoffStore;
  const hooks = yield* AgentControlVerificationEvaluatorHooks;

  const loadClaim = (handoffId: string) =>
    store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) =>
          evaluationError("load-evaluation-claim", "persistence", handoffId, cause),
        ),
      );

  const compareReplay = Effect.fn("AgentControlVerificationEvaluator.compareReplay")(
    function* (input: {
      readonly handoffId: string;
      readonly evaluationId: string;
      readonly evidenceId: string;
      readonly receiptId: string;
      readonly markerId: string;
      readonly evaluationFingerprint: string;
      readonly authorityDigest: string;
      readonly authorityJson: string;
      readonly disposition: "evaluated" | "invalid-output";
      readonly verdict: "passed" | "failed" | null;
      readonly errorCode: string | null;
      readonly semanticResultDigest: string | null;
      readonly sourceDisposition: "captured" | "missing" | "oversize";
      readonly sourceMessageId: string | null;
      readonly sourceEventId: string | null;
      readonly sourceEventSequence: number | null;
      readonly sourceEventStreamVersion: number | null;
      readonly rawOutputDigest: string | null;
      readonly outputByteLength: number;
      readonly terminalEventId: string;
      readonly terminalSequence: number;
      readonly terminalStreamVersion: number;
      readonly terminalObservationDigest: string;
    }) {
      const existingEvidence = yield* sql<Record<string, unknown>>`
      SELECT evaluation_id AS "evaluationId", evidence_id AS "evidenceId",
        revision, evaluation_fingerprint AS "evaluationFingerprint",
        authority_digest AS "authorityDigest", authority_json AS "authorityJson",
        disposition, verdict, error_code AS "errorCode",
        semantic_result_digest AS "semanticResultDigest",
        source_disposition AS "sourceDisposition", source_message_id AS "sourceMessageId",
        source_event_id AS "sourceEventId", source_event_sequence AS "sourceEventSequence",
        source_event_stream_version AS "sourceEventStreamVersion",
        raw_output_digest AS "rawOutputDigest", output_byte_length AS "outputByteLength",
        terminal_event_id AS "terminalEventId", terminal_sequence AS "terminalSequence",
        terminal_stream_version AS "terminalStreamVersion",
        terminal_observation_digest AS "terminalObservationDigest",
        receipt_id AS "receiptId", marker_id AS "markerId"
      FROM main.agent_control_verification_evaluation_evidence
      WHERE evaluation_id = ${input.evaluationId}
    `;
      const existingReceipt = yield* sql<Record<string, unknown>>`
      SELECT receipt_id AS "receiptId", evaluation_id AS "evaluationId",
        evidence_id AS "evidenceId", marker_id AS "markerId",
        evaluation_fingerprint AS "evaluationFingerprint", status,
        terminal_event_id AS "terminalEventId",
        terminal_observation_digest AS "terminalObservationDigest",
        source_message_id AS "sourceMessageId", source_event_id AS "sourceEventId",
        raw_output_digest AS "rawOutputDigest", output_byte_length AS "outputByteLength",
        source_disposition AS "sourceDisposition", disposition, verdict,
        error_code AS "errorCode"
      FROM main.agent_control_verification_evaluation_receipts
      WHERE evaluation_id = ${input.evaluationId}
    `;
      const existingMarker = yield* sql<Record<string, unknown>>`
      SELECT marker_id AS "markerId", evaluation_id AS "evaluationId",
        evidence_id AS "evidenceId", receipt_id AS "receiptId",
        evaluation_fingerprint AS "evaluationFingerprint", marker_version AS "markerVersion"
      FROM main.agent_control_verification_evaluation_markers
      WHERE evaluation_id = ${input.evaluationId}
    `;
      if (
        existingEvidence.length === 0 &&
        existingReceipt.length === 0 &&
        existingMarker.length === 0
      ) {
        return { _tag: "Absent" } as const;
      }
      if (
        existingEvidence.length === 1 &&
        existingReceipt.length === 1 &&
        existingMarker.length === 1 &&
        existingEvidence[0]?.evaluationId === input.evaluationId &&
        existingEvidence[0]?.evidenceId === input.evidenceId &&
        existingEvidence[0]?.revision === 1 &&
        existingEvidence[0]?.evaluationFingerprint === input.evaluationFingerprint &&
        existingEvidence[0]?.authorityDigest === input.authorityDigest &&
        existingEvidence[0]?.authorityJson === input.authorityJson &&
        existingEvidence[0]?.disposition === input.disposition &&
        existingEvidence[0]?.verdict === input.verdict &&
        existingEvidence[0]?.errorCode === input.errorCode &&
        existingEvidence[0]?.semanticResultDigest === input.semanticResultDigest &&
        existingEvidence[0]?.sourceDisposition === input.sourceDisposition &&
        existingEvidence[0]?.sourceMessageId === input.sourceMessageId &&
        existingEvidence[0]?.sourceEventId === input.sourceEventId &&
        existingEvidence[0]?.sourceEventSequence === input.sourceEventSequence &&
        existingEvidence[0]?.sourceEventStreamVersion === input.sourceEventStreamVersion &&
        existingEvidence[0]?.rawOutputDigest === input.rawOutputDigest &&
        existingEvidence[0]?.outputByteLength === input.outputByteLength &&
        existingEvidence[0]?.terminalEventId === input.terminalEventId &&
        existingEvidence[0]?.terminalSequence === input.terminalSequence &&
        existingEvidence[0]?.terminalStreamVersion === input.terminalStreamVersion &&
        existingEvidence[0]?.terminalObservationDigest === input.terminalObservationDigest &&
        existingEvidence[0]?.receiptId === input.receiptId &&
        existingEvidence[0]?.markerId === input.markerId &&
        existingReceipt[0]?.receiptId === input.receiptId &&
        existingReceipt[0]?.evaluationId === input.evaluationId &&
        existingReceipt[0]?.evidenceId === input.evidenceId &&
        existingReceipt[0]?.markerId === input.markerId &&
        existingReceipt[0]?.evaluationFingerprint === input.evaluationFingerprint &&
        existingReceipt[0]?.status === "accepted" &&
        existingReceipt[0]?.terminalEventId === input.terminalEventId &&
        existingReceipt[0]?.terminalObservationDigest === input.terminalObservationDigest &&
        existingReceipt[0]?.sourceMessageId === input.sourceMessageId &&
        existingReceipt[0]?.sourceEventId === input.sourceEventId &&
        existingReceipt[0]?.rawOutputDigest === input.rawOutputDigest &&
        existingReceipt[0]?.outputByteLength === input.outputByteLength &&
        existingReceipt[0]?.sourceDisposition === input.sourceDisposition &&
        existingReceipt[0]?.disposition === input.disposition &&
        existingReceipt[0]?.verdict === input.verdict &&
        existingReceipt[0]?.errorCode === input.errorCode &&
        existingMarker[0]?.markerId === input.markerId &&
        existingMarker[0]?.evaluationId === input.evaluationId &&
        existingMarker[0]?.evidenceId === input.evidenceId &&
        existingMarker[0]?.receiptId === input.receiptId &&
        existingMarker[0]?.evaluationFingerprint === input.evaluationFingerprint &&
        existingMarker[0]?.markerVersion === 1
      ) {
        return { _tag: "Replayed", evaluationId: input.evaluationId } as const;
      }
      return yield* evaluationError(
        "compare-evaluation-replay",
        "evaluation-conflict",
        input.handoffId,
      );
    },
  );

  const processUnchecked = Effect.fn("AgentControlVerificationEvaluator.processUnchecked")(
    function* (handoffId: string) {
      const claimOption = yield* loadClaim(handoffId);
      if (Option.isNone(claimOption)) return { _tag: "Waiting" } as const;
      const claim = claimOption.value;
      if (
        claim.evidence.templateVersion !== AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION ||
        claim.delivery.state !== "completed" ||
        claim.delivery.providerTurnId === null ||
        claim.evidence.promptContractFingerprint === null ||
        claim.evidence.resultSchemaVersion === null ||
        claim.evidence.resultSchemaFingerprint === null ||
        claim.delivery.terminalEventId === null ||
        claim.delivery.terminalAt === null ||
        claim.delivery.terminalObservationDigest === null
      ) {
        return { _tag: "Waiting" } as const;
      }
      const sourceResult = yield* loadVerificationResultSource(sql, claim).pipe(
        Effect.mapError((cause) =>
          evaluationError(
            cause.operation,
            cause.reason === "persistence" ? "persistence" : "history-corrupt",
            handoffId,
            cause,
          ),
        ),
      );
      if (sourceResult._tag === "Waiting") return sourceResult;
      yield* hooks.afterSourceLoad(handoffId);

      const legacy = yield* isLegacyVerificationEvaluation(sql, claim.evidence.providerDeliveryId);
      const checks = legacy
        ? null
        : yield* sealVerificationCheckAssessment(sql, claim).pipe(
            Effect.mapError((cause) =>
              evaluationError("assess-verification-checks", "persistence", handoffId, cause),
            ),
          );
      const evaluation = yield* evaluateCheckedVerificationResult(
        sourceResult.source.bytes,
        checks?.code ?? null,
        sourceResult.source.sourceDisposition === "oversize",
      );

      const evaluationId = deriveVerificationEvaluationId({
        providerDeliveryId: claim.evidence.providerDeliveryId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId: claim.delivery.providerTurnId,
        resultSchemaFingerprint: claim.evidence.resultSchemaFingerprint,
      });
      const evidenceId = deriveVerificationEvaluationEvidenceId(evaluationId);
      const receiptId = deriveVerificationEvaluationReceiptId(evaluationId);
      const markerId = deriveVerificationEvaluationMarkerId(evaluationId);
      const markerRows = yield* sql<{ readonly startMarkerId: string }>`
        SELECT marker.start_marker_id AS "startMarkerId"
        FROM main.agent_control_verification_stage_started_markers marker
        WHERE marker.provider_delivery_id = ${claim.evidence.providerDeliveryId}
      `.pipe(
        Effect.mapError((cause) =>
          evaluationError("load-evaluation-start-marker", "persistence", handoffId, cause),
        ),
      );
      if (markerRows.length !== 1) {
        return yield* evaluationError("load-evaluation-anchors", "history-corrupt", handoffId);
      }
      const startMarkerId = markerRows[0]!.startMarkerId;
      const authorityJson = canonicalJson({
        admission: {
          evidenceId: claim.evidence.admissionEvidenceId,
          markerId: claim.evidence.admissionMarkerId,
          receiptId: claim.evidence.admissionReceiptId,
        },
        attemptId: claim.evidence.attemptId,
        controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
        disposition: evaluation.disposition,
        errorCode: evaluation.errorCode,
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
        semanticResultDigest: evaluation.semanticResultDigest,
        source: {
          byteLength: sourceResult.source.outputByteLength,
          disposition: sourceResult.source.sourceDisposition,
          eventId: sourceResult.source.sourceEventId,
          eventSequence: sourceResult.source.sourceEventSequence,
          eventStreamVersion: sourceResult.source.sourceEventStreamVersion,
          messageId: sourceResult.source.finalMessageId,
          rawDigest: sourceResult.source.outputDigest,
        },
        stageStartMarkerId: startMarkerId,
        stageRunId: claim.evidence.stageRunId,
        task: {
          githubIntakeSequence: claim.evidence.githubIntakeSequence,
          projectId: claim.evidence.projectId,
          revision: claim.evidence.taskRevision,
          sourceIdentityFingerprint: claim.evidence.sourceIdentityFingerprint,
          taskId: claim.evidence.taskId,
        },
        terminal: {
          eventId: sourceResult.source.terminalEventId,
          observationDigest: claim.delivery.terminalObservationDigest,
          runtimeEventId: claim.delivery.terminalEventId,
          sequence: sourceResult.source.terminalEventSequence,
          state: claim.delivery.terminalProviderState,
          streamVersion: sourceResult.source.terminalEventStreamVersion,
        },
        threadId: claim.evidence.threadId,
        ...(checks === null ? {} : { verificationChecksDigest: checks.digest }),
        verdict: evaluation.verdict,
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
      const authorityDigest = sha256Utf8(authorityJson);
      const evaluationFingerprint = fingerprintVerificationTurn("evaluation-fingerprint", [
        authorityJson,
      ]);
      const evaluatedAt = DateTime.formatIso(yield* DateTime.now);
      const replayInput = {
        handoffId,
        evaluationId,
        evidenceId,
        receiptId,
        markerId,
        evaluationFingerprint,
        authorityDigest,
        authorityJson,
        disposition: evaluation.disposition,
        verdict: evaluation.verdict,
        errorCode: evaluation.errorCode,
        semanticResultDigest: evaluation.semanticResultDigest,
        sourceDisposition: sourceResult.source.sourceDisposition,
        sourceMessageId: sourceResult.source.finalMessageId,
        sourceEventId: sourceResult.source.sourceEventId,
        sourceEventSequence: sourceResult.source.sourceEventSequence,
        sourceEventStreamVersion: sourceResult.source.sourceEventStreamVersion,
        rawOutputDigest: sourceResult.source.outputDigest,
        outputByteLength: sourceResult.source.outputByteLength,
        terminalEventId: sourceResult.source.terminalEventId,
        terminalSequence: sourceResult.source.terminalEventSequence,
        terminalStreamVersion: sourceResult.source.terminalEventStreamVersion,
        terminalObservationDigest: claim.delivery.terminalObservationDigest,
      } as const;

      const transactionResult = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const replay = yield* compareReplay(replayInput);
            if (replay._tag === "Replayed") return replay;

            yield* sql`
            INSERT INTO main.agent_control_verification_evaluation_evidence (
              evaluation_id, evidence_id, revision, evaluation_fingerprint,
              authority_digest, authority_json, disposition, verdict, error_code,
              source_disposition, project_id, task_id, task_revision,
              github_intake_sequence, source_identity_fingerprint,
              worktree_reservation_id, worktree_revision, worktree_event_id,
              worktree_event_sequence, worktree_event_stream_version,
              worktree_ownership_fingerprint, worktree_verified_at, worktree_path, branch,
              stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
              controlled_thread_reservation_id, handoff_id, handoff_fingerprint,
              provider_delivery_id, thread_id, provider_instance_id, provider_turn_id,
              model_selection_fingerprint, prompt_template_version,
              prompt_contract_fingerprint, prompt_digest, result_schema_version,
              result_schema_fingerprint, terminal_event_id, terminal_sequence,
              terminal_stream_version, terminal_state, terminal_observation_digest,
              source_message_id, source_event_id, source_event_sequence,
              source_event_stream_version, raw_output_digest, output_byte_length,
              semantic_result_digest, start_marker_id, evaluated_at, receipt_id, marker_id
            ) VALUES (
              ${evaluationId}, ${evidenceId}, 1, ${evaluationFingerprint},
              ${authorityDigest}, ${authorityJson}, ${evaluation.disposition},
              ${evaluation.verdict}, ${evaluation.errorCode},
              ${sourceResult.source.sourceDisposition}, ${claim.evidence.projectId},
              ${claim.evidence.taskId}, ${claim.evidence.taskRevision},
              ${claim.evidence.githubIntakeSequence}, ${claim.evidence.sourceIdentityFingerprint},
              ${claim.evidence.worktreeReservationId}, ${claim.evidence.worktreeRevision},
              ${claim.evidence.worktreeEventId}, ${claim.evidence.worktreeEventSequence},
              ${claim.evidence.worktreeEventStreamVersion},
              ${claim.evidence.worktreeOwnershipFingerprint},
              ${claim.evidence.worktreeVerifiedAt}, ${claim.evidence.worktreePath},
              ${claim.evidence.branch}, ${claim.evidence.stageRunId},
              ${claim.evidence.attemptId}, ${claim.evidence.leaseId},
              ${claim.evidence.leaseHolderId}, ${claim.evidence.fenceToken},
              ${claim.evidence.controlledThreadReservationId}, ${claim.evidence.handoffId},
              ${claim.evidence.handoffFingerprint}, ${claim.evidence.providerDeliveryId},
              ${claim.evidence.threadId}, ${claim.evidence.providerInstanceId},
              ${claim.delivery.providerTurnId}, ${claim.evidence.modelSelectionFingerprint},
              ${claim.evidence.templateVersion}, ${claim.evidence.promptContractFingerprint},
              ${claim.evidence.promptDigest}, ${claim.evidence.resultSchemaVersion},
              ${claim.evidence.resultSchemaFingerprint},
              ${sourceResult.source.terminalEventId},
              ${sourceResult.source.terminalEventSequence},
              ${sourceResult.source.terminalEventStreamVersion}, 'completed',
              ${claim.delivery.terminalObservationDigest},
              ${sourceResult.source.finalMessageId}, ${sourceResult.source.sourceEventId},
              ${sourceResult.source.sourceEventSequence},
              ${sourceResult.source.sourceEventStreamVersion},
              ${sourceResult.source.outputDigest}, ${sourceResult.source.outputByteLength},
              ${evaluation.semanticResultDigest}, ${startMarkerId}, ${evaluatedAt},
              ${receiptId}, ${markerId}
            )
          `;
            yield* hooks.afterEvidence(handoffId);
            yield* sql`
            INSERT INTO main.agent_control_verification_evaluation_receipts (
              receipt_id, evaluation_id, evidence_id, marker_id, evaluation_fingerprint,
              provider_delivery_id, provider_instance_id, provider_turn_id,
              terminal_event_id, terminal_observation_digest, source_message_id,
              source_event_id, raw_output_digest, output_byte_length, source_disposition,
              disposition, verdict, error_code, status, accepted_at
            ) VALUES (
              ${receiptId}, ${evaluationId}, ${evidenceId}, ${markerId},
              ${evaluationFingerprint}, ${claim.evidence.providerDeliveryId},
              ${claim.evidence.providerInstanceId}, ${claim.delivery.providerTurnId},
              ${sourceResult.source.terminalEventId},
              ${claim.delivery.terminalObservationDigest},
              ${sourceResult.source.finalMessageId}, ${sourceResult.source.sourceEventId},
              ${sourceResult.source.outputDigest}, ${sourceResult.source.outputByteLength},
              ${sourceResult.source.sourceDisposition}, ${evaluation.disposition},
              ${evaluation.verdict}, ${evaluation.errorCode}, 'accepted', ${evaluatedAt}
            )
          `;
            yield* hooks.afterReceipt(handoffId);
            yield* sql`
            INSERT INTO main.agent_control_verification_evaluation_markers (
              marker_id, evaluation_id, evidence_id, receipt_id, evaluation_fingerprint,
              provider_delivery_id, marker_version, committed_at
            ) VALUES (
              ${markerId}, ${evaluationId}, ${evidenceId}, ${receiptId},
              ${evaluationFingerprint}, ${claim.evidence.providerDeliveryId}, 1, ${evaluatedAt}
            )
          `;
            return { _tag: "Evaluated", evaluationId } as const;
          }),
        )
        .pipe(
          Effect.catch((cause) =>
            isEvaluationError(cause)
              ? Effect.fail(cause)
              : compareReplay(replayInput).pipe(
                  Effect.flatMap((replay) =>
                    replay._tag === "Replayed"
                      ? Effect.succeed(replay)
                      : Effect.fail(
                          evaluationError("persist-evaluation", "persistence", handoffId, cause),
                        ),
                  ),
                ),
          ),
        );
      if (transactionResult._tag === "Evaluated") yield* hooks.afterCommit(handoffId);
      return transactionResult;
    },
  );

  const readNotifications = yield* AgentControlRunOnceReadNotifications;
  const processHandoff: AgentControlVerificationEvaluatorShape["processHandoff"] = (handoffId) =>
    processUnchecked(handoffId).pipe(
      Effect.tap((result) =>
        result._tag === "Evaluated" ? readNotifications.publish(handoffId) : Effect.void,
      ),
      Effect.mapError((cause) =>
        isEvaluationError(cause)
          ? cause
          : evaluationError("process-evaluation", "persistence", handoffId, cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = hooks.recoveryPageSize ?? 64;
    let cursor = 0;
    while (true) {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT rowid AS cursor,
          typeof(handoff_id) AS "handoffIdStorage",
          CAST(handoff_id AS BLOB) AS "handoffIdBytes",
          typeof(prompt_template_version) AS "promptVersionStorage",
          CASE WHEN prompt_template_version IS NULL THEN NULL
            ELSE CAST(prompt_template_version AS BLOB) END AS "promptVersionBytes"
        FROM main.agent_control_verification_handoff_intents
        WHERE rowid > ${cursor}
        ORDER BY rowid
        LIMIT ${pageSize}
      `.pipe(
        Effect.mapError((cause) =>
          evaluationError("list-evaluation-candidates", "persistence", undefined, cause),
        ),
      );
      if (rows.length === 0) break;
      yield* Effect.forEach(
        rows,
        (row) => {
          const candidate = Effect.gen(function* () {
            if (
              typeof row.cursor !== "number" ||
              !Number.isInteger(row.cursor) ||
              row.cursor <= cursor ||
              row.handoffIdStorage !== "text" ||
              (row.promptVersionStorage !== "null" && row.promptVersionStorage !== "text")
            ) {
              return yield* evaluationError(
                "decode-evaluation-candidate-routing",
                "history-corrupt",
              );
            }
            const handoffId = yield* Effect.try({
              try: () => decodeCanonicalUtf8Bytes(row.handoffIdBytes),
              catch: (cause) =>
                evaluationError(
                  "decode-evaluation-candidate-handoff",
                  "history-corrupt",
                  undefined,
                  cause,
                ),
            });
            if (row.promptVersionStorage === "null") return;
            const promptVersion = yield* Effect.try({
              try: () => decodeCanonicalUtf8Bytes(row.promptVersionBytes),
              catch: (cause) =>
                evaluationError(
                  "decode-evaluation-candidate-version",
                  "history-corrupt",
                  handoffId,
                  cause,
                ),
            });
            if (promptVersion !== AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION) {
              return yield* evaluationError(
                "decode-evaluation-candidate-version",
                "history-corrupt",
                handoffId,
              );
            }
            const claimOption = yield* loadClaim(handoffId);
            if (Option.isNone(claimOption)) return;
            const claim = claimOption.value;
            if (
              claim.evidence.templateVersion !==
                AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION ||
              claim.delivery.state !== "completed" ||
              claim.delivery.providerTurnId === null
            ) {
              return;
            }
            const deliveryBytes = new TextEncoder().encode(claim.evidence.providerDeliveryId);
            const [startedRows, markerRows] = yield* Effect.all([
              sql<Record<string, unknown>>`
                SELECT typeof(provider_delivery_id) AS "deliveryStorage",
                  typeof(start_marker_id) AS "markerStorage"
                FROM main.agent_control_verification_stage_started_markers
                WHERE CAST(provider_delivery_id AS BLOB) = ${deliveryBytes}
              `,
              sql<Record<string, unknown>>`
                SELECT typeof(provider_delivery_id) AS "deliveryStorage",
                  typeof(marker_id) AS "markerStorage"
                FROM main.agent_control_verification_evaluation_markers
                WHERE CAST(provider_delivery_id AS BLOB) = ${deliveryBytes}
              `,
            ]).pipe(
              Effect.mapError((cause) =>
                evaluationError(
                  "load-evaluation-candidate-companions",
                  "persistence",
                  handoffId,
                  cause,
                ),
              ),
            );
            if (startedRows.length === 0) return;
            if (
              startedRows.length !== 1 ||
              startedRows[0]?.deliveryStorage !== "text" ||
              startedRows[0]?.markerStorage !== "text" ||
              markerRows.length > 1 ||
              markerRows.some(
                (marker) => marker.deliveryStorage !== "text" || marker.markerStorage !== "text",
              )
            ) {
              return yield* evaluationError(
                "validate-evaluation-candidate-companions",
                "history-corrupt",
                handoffId,
              );
            }
            if (markerRows.length === 1) return;
            yield* processHandoff(handoffId);
          });
          return candidate.pipe(
            Effect.catch((cause) =>
              Effect.logError("verification evaluation candidate failed", {
                ...(cause.handoffId === undefined ? {} : { handoffId: cause.handoffId }),
                operation: cause.operation,
                reason: cause.reason,
              }),
            ),
          );
        },
        { concurrency: 1, discard: true },
      );
      const nextCursor = rows.at(-1)?.cursor;
      if (typeof nextCursor !== "number" || !Number.isInteger(nextCursor)) {
        return yield* evaluationError("decode-evaluation-candidate-cursor", "history-corrupt");
      }
      cursor = nextCursor;
      if (rows.length < pageSize) break;
    }
  });

  const processSafely = (_input: null) =>
    recover.pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason)) {
          return Effect.failCause(cause);
        }
        return Effect.failCause(cause);
      }),
    );
  let nextAttemptId = 0;
  let activeWorker:
    | {
        readonly attemptId: number;
        readonly drain: Effect.Effect<void, AgentControlVerificationEvaluationError>;
      }
    | undefined;
  let terminalDrain: Effect.Effect<void, AgentControlVerificationEvaluationError> = Effect.void;
  const prepare: AgentControlVerificationEvaluatorShape["prepare"] = Effect.fn(
    "AgentControlVerificationEvaluator.prepare",
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
    const recoveryLoop = activation.pipe(
      Effect.andThen(worker.enqueue(null)),
      Effect.andThen(
        Effect.forever(Effect.sleep(RECOVERY_INTERVAL).pipe(Effect.andThen(worker.enqueue(null)))),
      ),
    );
    yield* Effect.forkScoped(recoveryLoop, { startImmediately: true });
  });

  return AgentControlVerificationEvaluator.of({
    processHandoff,
    recover,
    prepare,
    drain: Effect.suspend(() => activeWorker?.drain ?? terminalDrain),
  });
});

export const AgentControlVerificationEvaluatorLive = Layer.effect(
  AgentControlVerificationEvaluator,
  make,
);
