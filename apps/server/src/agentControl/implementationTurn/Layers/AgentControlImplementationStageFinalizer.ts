import { loadNativeTerminalReceipt } from "../../nativeTerminalReceipt.ts";
import { makeProviderTerminalSessionCommand } from "../../../orchestration/providerTerminalSessionCommand.ts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  EventId,
  PositiveInt,
  ProjectId,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEventDraft,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
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
  deriveImplementationFinalizationCommandId,
  deriveImplementationFinalizationMarkerId,
  deriveImplementationFinalizationReceiptId,
  deriveImplementationLeaseReleaseEventId,
  deriveImplementationResultEvidenceId,
  deriveImplementationStageStartCommandId,
  deriveImplementationStageStartEvidenceId,
  deriveImplementationStageStartEventId,
  deriveImplementationStageStartMarkerId,
  deriveImplementationStageStartReceiptId,
  deriveImplementationTerminalStageEventId,
  fingerprintImplementationTurn,
} from "../identity.ts";
import {
  AgentControlImplementationOrchestrationEvidenceError,
  loadAgentControlImplementationOrchestrationEvidence,
} from "../orchestrationEvidence.ts";
import {
  AgentControlImplementationStageFinalizer,
  AgentControlImplementationStageFinalizerError,
  type AgentControlImplementationStageFinalizationPublication,
  type AgentControlImplementationStageFinalizerShape,
} from "../Services/AgentControlImplementationStageFinalizer.ts";
import { AgentControlImplementationStageFinalizerHooks } from "../Services/AgentControlImplementationStageFinalizerHooks.ts";
import { ProviderAdmissionReleaseAuthority } from "../../providerAdmission/Services/ProviderAdmissionReleaseAuthority.ts";
import { AgentControlImplementationHandoffStore } from "../Services/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationStageStarter } from "../Services/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationTurnWakeup } from "../Services/AgentControlImplementationTurnWakeup.ts";

const isFinalizerError = Schema.is(AgentControlImplementationStageFinalizerError);
const isOrchestrationError = Schema.is(AgentControlImplementationOrchestrationEvidenceError);
type TerminalDeliveryState = "completed" | "failed" | "interrupted";
const isTerminalDeliveryState = (state: string): state is TerminalDeliveryState =>
  state === "completed" || state === "failed" || state === "interrupted";

const ReplayDocument = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  resultEvidenceId: Schema.String,
  finalizationCommandId: Schema.String,
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  outcome: Schema.Literals(["succeeded", "failed", "cancelled"]),
  stageEventId: Schema.String,
  stageEventSequence: PositiveInt,
  leaseEventId: Schema.String,
  leaseEventSequence: PositiveInt,
  orchestrationHistoryDigest: Schema.String,
  orchestrationHistory: Schema.Array(Schema.Unknown),
  finalizedAt: Schema.String,
});
const decodeReplayDocument = Schema.decodeUnknownEffect(ReplayDocument);

interface StartEvidence {
  readonly startEvidenceId: string;
  readonly startReceiptId: string;
  readonly startMarkerId: string;
  readonly startCommandId: string;
  readonly startFingerprint: string;
  readonly providerDeliveryId: string;
  readonly providerTurnId: string;
  readonly deliveryRevision: number;
  readonly claimGeneration: number;
  readonly attemptCount: number;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly startedAt: string;
}

interface MaterializationContext {
  readonly repositoryDisplay: string;
  readonly sourceRevision: string;
  readonly taskTitle: string;
  readonly taskBody: string | null;
  readonly proposedPlanJson: string;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlImplementationHandoffStore;
  const starter = yield* AgentControlImplementationStageStarter;
  const wakeup = yield* AgentControlImplementationTurnWakeup;
  const orchestration = yield* OrchestrationEngineService;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseProjection = yield* AgentControlStageRunLeaseProjection;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const hooks = yield* AgentControlImplementationStageFinalizerHooks;
  const providerAdmissionRelease = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderAdmissionReleaseAuthority),
  );
  const publications =
    yield* PubSub.unbounded<AgentControlImplementationStageFinalizationPublication>();

  const failure = (
    handoffId: string,
    operation: string,
    reason: AgentControlImplementationStageFinalizerError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlImplementationStageFinalizerError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const decodeText = (handoffId: string, operation: string, value: unknown) =>
    Effect.try({
      try: () => decodeCanonicalUtf8Bytes(value),
      catch: (cause) => failure(handoffId, operation, "identity-mismatch", cause),
    });

  /** The first business read touches only the immutable accepted companion chain. */
  const replayFirst = Effect.fn("AgentControlImplementationStageFinalizer.replayFirst")(function* (
    handoffId: string,
  ) {
    const counts = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT count(*) FROM agent_control_implementation_result_evidence
            WHERE handoff_id = ${handoffId}) +
          (SELECT count(*) FROM agent_control_implementation_stage_finalization_receipts
            WHERE handoff_id = ${handoffId}) +
          (SELECT count(*) FROM agent_control_implementation_stage_finalization_markers
            WHERE handoff_id = ${handoffId}) AS count
      `.pipe(Effect.mapError((cause) => failure(handoffId, "replay-count", "persistence", cause)));
    const count = counts[0]?.count ?? 0;
    if (count === 0) return Option.none<{ readonly resultEvidenceId: string }>();
    if (count !== 3) return yield* failure(handoffId, "replay-partial", "partial-replay");

    const rows = yield* sql<Record<string, unknown>>`
        SELECT CAST(evidence.result_json AS BLOB) AS "resultJsonBytes",
          CAST(evidence.finalization_fingerprint AS BLOB) AS "fingerprintBytes",
          CAST(receipt.receipt_id AS BLOB) AS "receiptIdBytes",
          CAST(receipt.marker_id AS BLOB) AS "receiptMarkerIdBytes",
          CAST(receipt.finalization_command_id AS BLOB) AS "receiptCommandIdBytes",
          CAST(receipt.finalization_fingerprint AS BLOB) AS "receiptFingerprintBytes",
          CAST(receipt.result_evidence_id AS BLOB) AS "receiptEvidenceIdBytes",
          CAST(receipt.outcome AS BLOB) AS "receiptOutcomeBytes",
          CAST(receipt.status AS BLOB) AS "receiptStatusBytes",
          CAST(marker.marker_id AS BLOB) AS "markerIdBytes",
          CAST(marker.marker_fingerprint AS BLOB) AS "markerFingerprintBytes",
          CAST(marker.receipt_id AS BLOB) AS "markerReceiptIdBytes",
          CAST(marker.finalization_command_id AS BLOB) AS "markerCommandIdBytes",
          CAST(marker.finalization_fingerprint AS BLOB) AS "markerFinalizationFingerprintBytes",
          CAST(marker.result_evidence_id AS BLOB) AS "markerEvidenceIdBytes"
        FROM agent_control_implementation_result_evidence evidence
        JOIN agent_control_implementation_stage_finalization_receipts receipt
          ON receipt.result_evidence_id = evidence.result_evidence_id
         AND receipt.receipt_id = evidence.receipt_id
        JOIN agent_control_implementation_stage_finalization_markers marker
          ON marker.result_evidence_id = evidence.result_evidence_id
         AND marker.marker_id = evidence.marker_id
         AND marker.receipt_id = receipt.receipt_id
        WHERE evidence.handoff_id = ${handoffId}
      `.pipe(Effect.mapError((cause) => failure(handoffId, "replay-read", "persistence", cause)));
    if (rows.length !== 1) return yield* failure(handoffId, "replay-chain", "partial-replay");
    const row = rows[0]!;
    const [
      resultJson,
      fingerprint,
      receiptId,
      receiptMarkerId,
      receiptCommandId,
      receiptFingerprint,
      receiptEvidenceId,
      receiptOutcome,
      receiptStatus,
      markerId,
      markerFingerprint,
      markerReceiptId,
      markerCommandId,
      markerFinalizationFingerprint,
      markerEvidenceId,
    ] = yield* Effect.all([
      decodeText(handoffId, "replay-result-json-bytes", row.resultJsonBytes),
      decodeText(handoffId, "replay-fingerprint-bytes", row.fingerprintBytes),
      decodeText(handoffId, "replay-receipt-id-bytes", row.receiptIdBytes),
      decodeText(handoffId, "replay-receipt-marker-id-bytes", row.receiptMarkerIdBytes),
      decodeText(handoffId, "replay-receipt-command-id-bytes", row.receiptCommandIdBytes),
      decodeText(handoffId, "replay-receipt-fingerprint-bytes", row.receiptFingerprintBytes),
      decodeText(handoffId, "replay-receipt-evidence-id-bytes", row.receiptEvidenceIdBytes),
      decodeText(handoffId, "replay-receipt-outcome-bytes", row.receiptOutcomeBytes),
      decodeText(handoffId, "replay-receipt-status-bytes", row.receiptStatusBytes),
      decodeText(handoffId, "replay-marker-id-bytes", row.markerIdBytes),
      decodeText(handoffId, "replay-marker-fingerprint-bytes", row.markerFingerprintBytes),
      decodeText(handoffId, "replay-marker-receipt-id-bytes", row.markerReceiptIdBytes),
      decodeText(handoffId, "replay-marker-command-id-bytes", row.markerCommandIdBytes),
      decodeText(
        handoffId,
        "replay-marker-finalization-fingerprint-bytes",
        row.markerFinalizationFingerprintBytes,
      ),
      decodeText(handoffId, "replay-marker-evidence-id-bytes", row.markerEvidenceIdBytes),
    ]);
    const document = yield* Effect.try({
      try: () => parseCanonicalJson(resultJson),
      catch: (cause) => failure(handoffId, "replay-result-json", "identity-mismatch", cause),
    }).pipe(
      Effect.flatMap(decodeReplayDocument),
      Effect.mapError((cause) =>
        isFinalizerError(cause)
          ? cause
          : failure(handoffId, "replay-result-document", "identity-mismatch", cause),
      ),
    );
    const commandId = deriveImplementationFinalizationCommandId(
      handoffId,
      document.handoffFingerprint,
    );
    const resultEvidenceId = deriveImplementationResultEvidenceId(
      handoffId,
      document.handoffFingerprint,
    );
    const expectedReceiptId = deriveImplementationFinalizationReceiptId(
      handoffId,
      document.handoffFingerprint,
    );
    const expectedMarkerId = deriveImplementationFinalizationMarkerId(
      handoffId,
      document.handoffFingerprint,
    );
    const expectedStageEventId = deriveImplementationTerminalStageEventId(
      handoffId,
      document.handoffFingerprint,
    );
    const expectedLeaseEventId = deriveImplementationLeaseReleaseEventId(
      handoffId,
      document.handoffFingerprint,
    );
    const expectedFingerprint = fingerprintImplementationTurn("finalization-result", [resultJson]);
    const expectedHistoryDigest = sha256Utf8(
      canonicalJson(document.orchestrationHistory as JsonValue),
    );
    const expectedMarkerFingerprint = fingerprintImplementationTurn("finalization-marker", [
      handoffId,
      document.handoffFingerprint,
      String(commandId),
      resultEvidenceId,
      expectedFingerprint,
      String(document.stageEventId),
      String(document.stageEventSequence),
      String(document.leaseEventId),
      String(document.leaseEventSequence),
      document.finalizedAt,
    ]);
    if (
      document.handoffId !== handoffId ||
      document.finalizationCommandId !== commandId ||
      document.resultEvidenceId !== resultEvidenceId ||
      document.stageEventId !== expectedStageEventId ||
      document.leaseEventId !== expectedLeaseEventId ||
      document.orchestrationHistoryDigest !== expectedHistoryDigest ||
      fingerprint !== expectedFingerprint ||
      receiptId !== expectedReceiptId ||
      receiptMarkerId !== expectedMarkerId ||
      receiptCommandId !== commandId ||
      receiptFingerprint !== expectedFingerprint ||
      receiptEvidenceId !== resultEvidenceId ||
      receiptOutcome !== document.outcome ||
      receiptStatus !== "accepted" ||
      markerId !== expectedMarkerId ||
      markerFingerprint !== expectedMarkerFingerprint ||
      markerReceiptId !== expectedReceiptId ||
      markerCommandId !== commandId ||
      markerFinalizationFingerprint !== expectedFingerprint ||
      markerEvidenceId !== resultEvidenceId
    ) {
      return yield* failure(handoffId, "replay-identity", "identity-mismatch");
    }
    return Option.some({ resultEvidenceId });
  });

  const loadStartEvidence = Effect.fn("AgentControlImplementationStageFinalizer.loadStartEvidence")(
    function* (handoffId: string) {
      const rows = yield* sql<Record<string, unknown>>`
      SELECT CAST(evidence.start_evidence_id AS BLOB) AS "startEvidenceIdBytes",
        CAST(receipt.start_receipt_id AS BLOB) AS "startReceiptIdBytes",
        CAST(marker.start_marker_id AS BLOB) AS "startMarkerIdBytes",
        CAST(evidence.start_command_id AS BLOB) AS "startCommandIdBytes",
        CAST(evidence.start_fingerprint AS BLOB) AS "startFingerprintBytes",
        CAST(evidence.provider_delivery_id AS BLOB) AS "providerDeliveryIdBytes",
        CAST(evidence.provider_turn_id AS BLOB) AS "providerTurnIdBytes",
        evidence.delivery_revision AS "deliveryRevision",
        evidence.claim_generation AS "claimGeneration",
        evidence.attempt_count AS "attemptCount",
        CAST(evidence.stage_event_id AS BLOB) AS "stageEventIdBytes",
        evidence.stage_event_sequence AS "stageEventSequence",
        CAST(evidence.started_at AS BLOB) AS "startedAtBytes"
      FROM agent_control_implementation_stage_started_evidence evidence
      JOIN agent_control_implementation_stage_started_receipts receipt
        ON receipt.start_evidence_id = evidence.start_evidence_id
      JOIN agent_control_implementation_stage_started_markers marker
        ON marker.start_evidence_id = evidence.start_evidence_id
       AND marker.start_receipt_id = receipt.start_receipt_id
      WHERE evidence.handoff_id = ${handoffId}
    `.pipe(
        Effect.mapError((cause) => failure(handoffId, "read-start-chain", "persistence", cause)),
      );
      const counts = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM agent_control_implementation_stage_started_evidence
          WHERE handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_implementation_stage_started_receipts receipt
          JOIN agent_control_implementation_stage_started_evidence evidence
            ON evidence.start_evidence_id = receipt.start_evidence_id
          WHERE evidence.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_implementation_stage_started_markers marker
          JOIN agent_control_implementation_stage_started_evidence evidence
            ON evidence.start_evidence_id = marker.start_evidence_id
          WHERE evidence.handoff_id = ${handoffId}) AS count
    `.pipe(
        Effect.mapError((cause) => failure(handoffId, "count-start-chain", "persistence", cause)),
      );
      if (counts[0]?.count !== 3 || rows.length !== 1) {
        return yield* failure(handoffId, "start-chain", "candidate-evidence");
      }
      const row = rows[0]!;
      const [
        startEvidenceId,
        startReceiptId,
        startMarkerId,
        startCommandId,
        startFingerprint,
        providerDeliveryId,
        providerTurnId,
        stageEventId,
        startedAt,
      ] = yield* Effect.all([
        decodeText(handoffId, "start-evidence-id-bytes", row.startEvidenceIdBytes),
        decodeText(handoffId, "start-receipt-id-bytes", row.startReceiptIdBytes),
        decodeText(handoffId, "start-marker-id-bytes", row.startMarkerIdBytes),
        decodeText(handoffId, "start-command-id-bytes", row.startCommandIdBytes),
        decodeText(handoffId, "start-fingerprint-bytes", row.startFingerprintBytes),
        decodeText(handoffId, "start-delivery-id-bytes", row.providerDeliveryIdBytes),
        decodeText(handoffId, "start-provider-turn-id-bytes", row.providerTurnIdBytes),
        decodeText(handoffId, "start-stage-event-id-bytes", row.stageEventIdBytes),
        decodeText(handoffId, "start-time-bytes", row.startedAtBytes),
      ]);
      if (
        typeof row.deliveryRevision !== "number" ||
        typeof row.claimGeneration !== "number" ||
        typeof row.attemptCount !== "number" ||
        typeof row.stageEventSequence !== "number"
      ) {
        return yield* failure(handoffId, "start-numeric-storage", "candidate-evidence");
      }
      return {
        startEvidenceId,
        startReceiptId,
        startMarkerId,
        startCommandId,
        startFingerprint,
        providerDeliveryId,
        providerTurnId,
        deliveryRevision: row.deliveryRevision,
        claimGeneration: row.claimGeneration,
        attemptCount: row.attemptCount,
        stageEventId,
        stageEventSequence: row.stageEventSequence,
        startedAt,
      } satisfies StartEvidence;
    },
  );

  const loadMaterializationContext = Effect.fn(
    "AgentControlImplementationStageFinalizer.loadMaterializationContext",
  )(function* (handoffId: string, materializationEvidenceId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT CAST(materialization.repository_display AS BLOB) AS "repositoryDisplayBytes",
        CAST(materialization.source_revision AS BLOB) AS "sourceRevisionBytes",
        CAST(materialization.task_title AS BLOB) AS "taskTitleBytes",
        CASE WHEN materialization.task_body IS NULL THEN NULL
          ELSE CAST(materialization.task_body AS BLOB) END AS "taskBodyBytes",
        CAST(materialization.proposed_plan_json AS BLOB) AS "proposedPlanBytes"
      FROM agent_control_implementation_materialization_evidence materialization
      JOIN agent_control_implementation_handoff_intents intent
        ON intent.materialization_evidence_id = materialization.materialization_evidence_id
      WHERE intent.handoff_id = ${handoffId}
        AND materialization.materialization_evidence_id = ${materializationEvidenceId}
    `.pipe(
      Effect.mapError((cause) => failure(handoffId, "read-materialization", "persistence", cause)),
    );
    if (rows.length !== 1) {
      return yield* failure(handoffId, "materialization-context", "candidate-evidence");
    }
    const row = rows[0]!;
    const [repositoryDisplay, sourceRevision, taskTitle, taskBody, proposedPlanJson] =
      yield* Effect.all([
        decodeText(handoffId, "repository-display-bytes", row.repositoryDisplayBytes),
        decodeText(handoffId, "source-revision-bytes", row.sourceRevisionBytes),
        decodeText(handoffId, "task-title-bytes", row.taskTitleBytes),
        row.taskBodyBytes === null
          ? Effect.succeed(null)
          : decodeText(handoffId, "task-body-bytes", row.taskBodyBytes),
        decodeText(handoffId, "proposed-plan-bytes", row.proposedPlanBytes),
      ]);
    yield* Effect.try({
      try: () => parseCanonicalJson(proposedPlanJson),
      catch: (cause) => failure(handoffId, "proposed-plan-json", "identity-mismatch", cause),
    });
    return {
      repositoryDisplay,
      sourceRevision,
      taskTitle,
      taskBody,
      proposedPlanJson,
    } satisfies MaterializationContext;
  });

  const finalizeInTransaction = Effect.fn(
    "AgentControlImplementationStageFinalizer.finalizeInTransaction",
  )(function* (handoffId: string) {
    const replay = yield* replayFirst(handoffId);
    if (Option.isSome(replay))
      return { _tag: "Replayed", resultEvidenceId: replay.value.resultEvidenceId } as const;

    const claimOption = yield* store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) =>
          failure(
            handoffId,
            "load-authoritative-handoff",
            cause.reason === "candidate-evidence" ? "candidate-evidence" : "persistence",
            cause,
          ),
        ),
      );
    if (Option.isNone(claimOption)) {
      return yield* failure(handoffId, "load-authoritative-handoff", "candidate-evidence");
    }
    const claim = claimOption.value;
    if (claim.delivery.state === "ambiguous") return { _tag: "Ambiguous" } as const;
    if (!isTerminalDeliveryState(claim.delivery.state)) {
      return { _tag: "Waiting" } as const;
    }
    if (claim.delivery.providerTurnId === null || claim.delivery.terminalAt === null) {
      return yield* failure(handoffId, "terminal-delivery", "identity-mismatch");
    }
    const providerTurnId = claim.delivery.providerTurnId;
    const finalizedAt = claim.delivery.terminalAt;
    const orchestrationResult = yield* loadAgentControlImplementationOrchestrationEvidence(
      sql,
      claim,
      { requireTerminal: true },
    ).pipe(
      Effect.catchIf(
        (cause) => isOrchestrationError(cause) && cause.reason === "ambiguous-terminal",
        () => Effect.succeed({ _tag: "AmbiguousEvidence" } as const),
      ),
      Effect.mapError((cause) =>
        failure(
          handoffId,
          isOrchestrationError(cause) ? cause.operation : "orchestration-evidence",
          isOrchestrationError(cause) && cause.reason === "persistence"
            ? "persistence"
            : "orchestration-history-corrupt",
          cause,
        ),
      ),
    );
    if (orchestrationResult._tag === "Waiting") return { _tag: "Waiting" } as const;
    if (orchestrationResult._tag === "AmbiguousEvidence") return { _tag: "Ambiguous" } as const;
    const orchestrationEvidence = orchestrationResult.evidence;
    if (orchestrationEvidence.terminal === null || orchestrationEvidence.outcome === null) {
      return { _tag: "Waiting" } as const;
    }
    const materialization = yield* loadMaterializationContext(
      handoffId,
      claim.evidence.materializationEvidenceId,
    );

    const stageRunId = AgentControlStageRunId.make(claim.evidence.stageRunId);
    const stage = yield* loadAuthoritativeStageRunState(stageRunId, stageEvents, stageStates).pipe(
      Effect.mapError((cause) =>
        failure(handoffId, "load-stage-history", "stage-history-corrupt", cause),
      ),
    );
    if (
      Option.isNone(stage) ||
      stage.value.state.status !== "running" ||
      stage.value.state.revision !== 2 ||
      stage.value.state.roleId !== "implementer" ||
      stage.value.state.stageKind !== "implementation" ||
      (stage.value.state.stageOrdinal !== 2 && stage.value.state.stageOrdinal !== 4) ||
      stage.value.state.attemptOrdinal !== 1 ||
      stage.value.state.attemptId !== claim.evidence.attemptId ||
      stage.value.state.taskRevision !== claim.evidence.taskRevision ||
      stage.value.state.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
      stage.value.state.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint ||
      stage.value.events.length !== 2
    ) {
      return yield* failure(handoffId, "validate-stage-history", "stage-history-corrupt");
    }
    const start = yield* loadStartEvidence(handoffId);
    const expectedStartCommandId = deriveImplementationStageStartCommandId(
      claim.evidence.providerDeliveryId,
      providerTurnId,
    );
    const expectedStartFingerprint = fingerprintImplementationTurn("stage-start", [
      claim.evidence.admissionEvidenceId,
      claim.evidence.admissionReceiptId,
      claim.evidence.admissionMarkerId,
      claim.evidence.materializationEvidenceId,
      claim.evidence.materializationReceiptId,
      claim.evidence.materializationMarkerId,
      claim.evidence.handoffId,
      claim.evidence.handoffFingerprint,
      claim.evidence.providerDeliveryId,
      String(start.deliveryRevision),
      String(start.claimGeneration),
      String(start.attemptCount),
      claim.evidence.threadId,
      claim.evidence.planningThreadId,
      claim.evidence.planId,
      providerTurnId,
      start.stageEventId,
      start.startedAt,
    ]);
    if (
      start.startCommandId !== expectedStartCommandId ||
      start.startEvidenceId !== deriveImplementationStageStartEvidenceId(expectedStartCommandId) ||
      start.startReceiptId !== deriveImplementationStageStartReceiptId(expectedStartCommandId) ||
      start.startMarkerId !== deriveImplementationStageStartMarkerId(expectedStartCommandId) ||
      start.stageEventId !== deriveImplementationStageStartEventId(expectedStartCommandId) ||
      start.startFingerprint !== expectedStartFingerprint ||
      start.providerDeliveryId !== claim.evidence.providerDeliveryId ||
      start.providerTurnId !== providerTurnId ||
      start.deliveryRevision > claim.delivery.revision ||
      start.claimGeneration !== claim.delivery.claimGeneration ||
      start.attemptCount !== claim.delivery.attemptCount ||
      stage.value.events[1]?.eventId !== start.stageEventId ||
      stage.value.events[1]?.sequence !== start.stageEventSequence ||
      stage.value.events[1]?.type !== "agentControl.stageRun.implementationStarted" ||
      stage.value.events[1]?.occurredAt !== start.startedAt
    ) {
      return yield* failure(handoffId, "validate-start-chain", "stage-history-corrupt");
    }

    const leaseId = AgentControlStageRunLeaseId.make(claim.evidence.leaseId);
    const lease = yield* loadAuthoritativeLeaseState(leaseId, leaseEvents, leaseStates).pipe(
      Effect.mapError((cause) =>
        failure(handoffId, "load-lease-history", "lease-history-corrupt", cause),
      ),
    );
    if (
      Option.isNone(lease) ||
      lease.value.state.status !== "reserved" ||
      lease.value.state.stageRunId !== stageRunId ||
      lease.value.state.attemptId !== claim.evidence.attemptId ||
      lease.value.state.holderId !== claim.evidence.leaseHolderId ||
      lease.value.state.fenceToken !== claim.evidence.fenceToken
    ) {
      return yield* failure(handoffId, "validate-lease-history", "lease-history-corrupt");
    }
    yield* hooks.afterAuthoritativeEvidence(handoffId);
    yield* hooks.beforeAppend(handoffId);

    const commandId = deriveImplementationFinalizationCommandId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const resultEvidenceId = deriveImplementationResultEvidenceId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const receiptId = deriveImplementationFinalizationReceiptId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const markerId = deriveImplementationFinalizationMarkerId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const stageEventId = deriveImplementationTerminalStageEventId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const leaseEventId = deriveImplementationLeaseReleaseEventId(
      handoffId,
      claim.evidence.handoffFingerprint,
    );
    const outcome = orchestrationEvidence.outcome;
    const terminalEventId = EventId.make(orchestrationEvidence.terminal.event.eventId);
    const commonPayload = {
      projectId: ProjectId.make(claim.evidence.projectId),
      taskId: AgentControlTaskId.make(claim.evidence.taskId),
      stageRunId,
      attemptId: AgentControlAttemptId.make(claim.evidence.attemptId),
      roleId: "implementer" as const,
      stageKind: "implementation" as const,
      stageOrdinal: stage.value.state.stageOrdinal === 4 ? (4 as const) : (2 as const),
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
      deliveryTerminalState: claim.delivery.state,
      claimGeneration: claim.delivery.claimGeneration,
      attemptCount: claim.delivery.attemptCount,
      controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
      threadId: claim.evidence.threadId,
      planningThreadId: claim.evidence.planningThreadId,
      planId: claim.evidence.planId,
      proposedPlanDigest: claim.evidence.proposedPlanDigest,
      repositoryDisplay: materialization.repositoryDisplay,
      sourceRevision: materialization.sourceRevision,
      taskSourceEventId: EventId.make(claim.evidence.taskSourceEventId),
      taskSourceEventSequence: claim.evidence.taskSourceEventSequence,
      taskSourceEventStreamVersion: claim.evidence.taskSourceEventStreamVersion,
      worktreeReservationId: claim.evidence.worktreeReservationId,
      worktreeEventId: EventId.make(claim.evidence.worktreeEventId),
      worktreeEventSequence: claim.evidence.worktreeEventSequence,
      worktreeEventStreamVersion: claim.evidence.worktreeEventStreamVersion,
      worktreeOwnershipFingerprint: claim.evidence.worktreeOwnershipFingerprint,
      turnRequestCommandId: claim.evidence.turnRequestCommandId,
      messageId: claim.evidence.messageId,
      messageEventId: EventId.make(claim.evidence.messageEventId),
      turnRequestEventId: EventId.make(claim.evidence.turnRequestEventId),
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId,
      runtimeMode: claim.evidence.runtimeMode,
      modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
      leaseId,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(claim.evidence.leaseHolderId),
      fenceToken: claim.evidence.fenceToken,
      providerStartedEventId: EventId.make(orchestrationEvidence.started.event.eventId),
      providerStartedSequence: orchestrationEvidence.started.event.sequence,
      providerStartedStreamVersion: orchestrationEvidence.started.streamVersion,
      providerTerminalEventId: terminalEventId,
      providerTerminalSequence: orchestrationEvidence.terminal.event.sequence,
      providerTerminalStreamVersion: orchestrationEvidence.terminal.streamVersion,
      orchestrationHistoryDigest: orchestrationEvidence.historyDigest,
      orchestrationHistoryEventCount: orchestrationEvidence.history.length,
      resultEvidenceId,
      finalizedAt,
    };
    const stageDraftBase = {
      eventId: stageEventId,
      aggregateKind: "stage-run",
      aggregateId: stageRunId,
      occurredAt: finalizedAt,
      commandId,
      causationEventId: terminalEventId,
      correlationId: commandId,
      authority: "system",
      metadata: { schemaVersion: 1 },
    } as const;
    const stageDraft: AgentControlStageRunEventDraft =
      outcome === "succeeded"
        ? {
            ...stageDraftBase,
            type: "agentControl.stageRun.implementationSucceeded",
            payload: {
              ...commonPayload,
              deliveryTerminalState: "completed",
              status: "succeeded",
            },
          }
        : outcome === "failed"
          ? {
              ...stageDraftBase,
              type: "agentControl.stageRun.implementationFailed",
              payload: { ...commonPayload, deliveryTerminalState: "failed", status: "failed" },
            }
          : {
              ...stageDraftBase,
              type: "agentControl.stageRun.implementationCancelled",
              payload: {
                ...commonPayload,
                deliveryTerminalState: "interrupted",
                status: "cancelled",
              },
            };
    const appendedStage = yield* stageEvents
      .append({ stageRunId, expectedStreamVersion: 2, events: [stageDraft] })
      .pipe(
        Effect.mapError((cause) =>
          failure(
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
      return yield* failure(handoffId, "append-terminal-stage", "persistence");
    }
    yield* stageProjection
      .projectEvent(stageEvent)
      .pipe(
        Effect.mapError((cause) =>
          failure(handoffId, "project-terminal-stage", "persistence", cause),
        ),
      );

    const terminalIdentity =
      outcome === "succeeded"
        ? ({ deliveryTerminalState: "completed", stageStatus: "succeeded" } as const)
        : outcome === "failed"
          ? ({ deliveryTerminalState: "failed", stageStatus: "failed" } as const)
          : ({ deliveryTerminalState: "interrupted", stageStatus: "cancelled" } as const);

    const leaseDraft: AgentControlStageRunLeaseEventDraft = {
      eventId: leaseEventId,
      type: "agentControl.stageRunLease.releasedAfterImplementation",
      aggregateKind: "stage-run-lease",
      aggregateId: leaseId,
      occurredAt: finalizedAt,
      commandId,
      causationEventId: stageEventId,
      correlationId: commandId,
      authority: "system",
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
        providerTurnId,
        runtimeMode: commonPayload.runtimeMode,
        modelSelectionFingerprint: commonPayload.modelSelectionFingerprint,
        orchestrationHistoryDigest: commonPayload.orchestrationHistoryDigest,
        resultEvidenceId,
        stageEventId,
        ...terminalIdentity,
        releasedAt: finalizedAt,
      },
      metadata: { schemaVersion: 1 },
    };
    const appendedLease = yield* leaseEvents
      .append({ leaseId, expectedStreamVersion: lease.value.state.revision, events: [leaseDraft] })
      .pipe(
        Effect.mapError((cause) =>
          failure(
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
      return yield* failure(handoffId, "append-lease-release", "persistence");
    }
    yield* leaseProjection
      .projectEvent(leaseEvent)
      .pipe(
        Effect.mapError((cause) =>
          failure(handoffId, "project-lease-release", "persistence", cause),
        ),
      );

    const resultDocument = {
      schemaVersion: 1,
      resultEvidenceId,
      finalizationCommandId: commandId,
      handoffId,
      handoffFingerprint: claim.evidence.handoffFingerprint,
      outcome,
      admission: {
        evidenceId: claim.evidence.admissionEvidenceId,
        receiptId: claim.evidence.admissionReceiptId,
        markerId: claim.evidence.admissionMarkerId,
      },
      materialization: {
        evidenceId: claim.evidence.materializationEvidenceId,
        receiptId: claim.evidence.materializationReceiptId,
        markerId: claim.evidence.materializationMarkerId,
        repositoryDisplay: materialization.repositoryDisplay,
        sourceRevision: materialization.sourceRevision,
        taskTitle: materialization.taskTitle,
        taskBody: materialization.taskBody,
        proposedPlan: parseCanonicalJson(materialization.proposedPlanJson),
      },
      handoff: claim.evidence,
      delivery: claim.delivery,
      stageStart: start,
      orchestrationHistoryDigest: orchestrationEvidence.historyDigest,
      orchestrationHistory: parseCanonicalJson(orchestrationEvidence.historyJson),
      stageEventId: stageEvent.eventId,
      stageEventSequence: stageEvent.sequence,
      stageEventStreamVersion: stageEvent.streamVersion,
      leaseEventId: leaseEvent.eventId,
      leaseEventSequence: leaseEvent.sequence,
      leaseEventStreamVersion: leaseEvent.streamVersion,
      finalizedAt,
    };
    const resultJson = canonicalJson(resultDocument as unknown as JsonValue);
    const finalizationFingerprint = fingerprintImplementationTurn("finalization-result", [
      resultJson,
    ]);
    const markerFingerprint = fingerprintImplementationTurn("finalization-marker", [
      handoffId,
      claim.evidence.handoffFingerprint,
      String(commandId),
      resultEvidenceId,
      finalizationFingerprint,
      String(stageEvent.eventId),
      String(stageEvent.sequence),
      String(leaseEvent.eventId),
      String(leaseEvent.sequence),
      finalizedAt,
    ]);

    yield* sql`
      INSERT INTO agent_control_implementation_result_evidence (
        result_evidence_id, receipt_id, marker_id, finalization_command_id,
        finalization_fingerprint, result_json, outcome,
        admission_evidence_id, admission_receipt_id, admission_marker_id,
        materialization_evidence_id, materialization_receipt_id, materialization_marker_id,
        start_evidence_id, start_receipt_id, start_marker_id,
        handoff_id, handoff_fingerprint, project_id, task_id, task_revision,
        github_intake_sequence, source_identity_fingerprint, task_source_event_id,
        task_source_event_sequence, task_source_event_stream_version,
        repository_display, source_revision, task_title, task_body,
        stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
        worktree_reservation_id, worktree_revision, worktree_event_id,
        worktree_event_sequence, worktree_event_stream_version, worktree_ownership_fingerprint,
        controlled_thread_reservation_id, thread_id, planning_thread_id, plan_id,
        proposed_plan_digest, turn_request_command_id, message_id, message_event_id,
        turn_request_event_id, provider_delivery_id, provider_instance_id, provider_turn_id,
        runtime_mode, model_selection_fingerprint, claim_generation, attempt_count,
        delivery_terminal_state, delivery_revision, terminal_at,
        orchestration_started_event_id, orchestration_started_sequence,
        orchestration_started_stream_version, orchestration_terminal_event_id,
        orchestration_terminal_sequence, orchestration_terminal_stream_version,
        orchestration_history_json, orchestration_history_digest,
        orchestration_history_event_count, stage_event_id, stage_event_sequence,
        stage_event_stream_version, lease_event_id, lease_event_sequence,
        lease_event_stream_version, finalized_at
      ) VALUES (
        ${resultEvidenceId}, ${receiptId}, ${markerId}, ${commandId},
        ${finalizationFingerprint}, ${resultJson}, ${outcome},
        ${claim.evidence.admissionEvidenceId}, ${claim.evidence.admissionReceiptId},
        ${claim.evidence.admissionMarkerId}, ${claim.evidence.materializationEvidenceId},
        ${claim.evidence.materializationReceiptId}, ${claim.evidence.materializationMarkerId},
        ${start.startEvidenceId}, ${start.startReceiptId}, ${start.startMarkerId},
        ${handoffId}, ${claim.evidence.handoffFingerprint}, ${claim.evidence.projectId},
        ${claim.evidence.taskId}, ${claim.evidence.taskRevision},
        ${claim.evidence.githubIntakeSequence}, ${claim.evidence.sourceIdentityFingerprint},
        ${claim.evidence.taskSourceEventId}, ${claim.evidence.taskSourceEventSequence},
        ${claim.evidence.taskSourceEventStreamVersion}, ${materialization.repositoryDisplay},
        ${materialization.sourceRevision}, ${materialization.taskTitle}, ${materialization.taskBody},
        ${claim.evidence.stageRunId}, ${claim.evidence.attemptId}, ${claim.evidence.leaseId},
        ${claim.evidence.leaseHolderId}, ${claim.evidence.fenceToken},
        ${claim.evidence.worktreeReservationId}, ${claim.evidence.worktreeRevision},
        ${claim.evidence.worktreeEventId}, ${claim.evidence.worktreeEventSequence},
        ${claim.evidence.worktreeEventStreamVersion}, ${claim.evidence.worktreeOwnershipFingerprint},
        ${claim.evidence.controlledThreadReservationId}, ${claim.evidence.threadId},
        ${claim.evidence.planningThreadId}, ${claim.evidence.planId},
        ${claim.evidence.proposedPlanDigest}, ${claim.evidence.turnRequestCommandId},
        ${claim.evidence.messageId}, ${claim.evidence.messageEventId},
        ${claim.evidence.turnRequestEventId}, ${claim.evidence.providerDeliveryId},
        ${claim.evidence.providerInstanceId}, ${providerTurnId}, ${claim.evidence.runtimeMode},
        ${claim.evidence.modelSelectionFingerprint}, ${claim.delivery.claimGeneration},
        ${claim.delivery.attemptCount}, ${claim.delivery.state}, ${claim.delivery.revision},
        ${finalizedAt}, ${orchestrationEvidence.started.event.eventId},
        ${orchestrationEvidence.started.event.sequence}, ${orchestrationEvidence.started.streamVersion},
        ${orchestrationEvidence.terminal.event.eventId},
        ${orchestrationEvidence.terminal.event.sequence},
        ${orchestrationEvidence.terminal.streamVersion}, ${orchestrationEvidence.historyJson},
        ${orchestrationEvidence.historyDigest}, ${orchestrationEvidence.history.length},
        ${stageEvent.eventId}, ${stageEvent.sequence}, ${stageEvent.streamVersion},
        ${leaseEvent.eventId}, ${leaseEvent.sequence}, ${leaseEvent.streamVersion}, ${finalizedAt}
      )
    `.pipe(
      Effect.mapError((cause) =>
        failure(handoffId, "insert-result-evidence", "persistence", cause),
      ),
    );
    yield* sql`
      INSERT INTO agent_control_implementation_stage_finalization_receipts (
        receipt_id, marker_id, finalization_command_id, finalization_fingerprint,
        result_evidence_id, handoff_id, outcome, stage_event_id, stage_event_sequence,
        lease_event_id, lease_event_sequence, status, accepted_at
      ) VALUES (
        ${receiptId}, ${markerId}, ${commandId}, ${finalizationFingerprint},
        ${resultEvidenceId}, ${handoffId}, ${outcome}, ${stageEvent.eventId},
        ${stageEvent.sequence}, ${leaseEvent.eventId}, ${leaseEvent.sequence},
        'accepted', ${finalizedAt}
      )
    `.pipe(Effect.mapError((cause) => failure(handoffId, "insert-receipt", "persistence", cause)));
    yield* hooks.beforeFinalMarker(handoffId);
    yield* sql`
      INSERT INTO agent_control_implementation_stage_finalization_markers (
        marker_id, marker_fingerprint, receipt_id, finalization_command_id,
        finalization_fingerprint, result_evidence_id, handoff_id, committed_at
      ) VALUES (
        ${markerId}, ${markerFingerprint}, ${receiptId}, ${commandId},
        ${finalizationFingerprint}, ${resultEvidenceId}, ${handoffId}, ${finalizedAt}
      )
    `.pipe(Effect.mapError((cause) => failure(handoffId, "insert-marker", "persistence", cause)));

    return {
      _tag: "Finalized",
      releaseInput: { stage: "implementation" as const, handoffId, finalizedAt },
      publication: {
        handoffId,
        resultEvidenceId,
        outcome,
        stageEvent,
        leaseEvent,
      } satisfies AgentControlImplementationStageFinalizationPublication,
    } as const;
  });

  const processFresh = Effect.fn("AgentControlImplementationStageFinalizer.processFresh")(
    function* (handoffId: string) {
      const claimOption = yield* store
        .loadAcceptedByHandoffId(handoffId)
        .pipe(
          Effect.mapError((cause) =>
            failure(
              handoffId,
              "load-handoff",
              cause.reason === "candidate-evidence" ? "candidate-evidence" : "persistence",
              cause,
            ),
          ),
        );
      if (Option.isNone(claimOption))
        return yield* failure(handoffId, "load-handoff", "candidate-evidence");
      const claim = claimOption.value;
      if (claim.delivery.state === "ambiguous") return { _tag: "Ambiguous" } as const;
      if (!isTerminalDeliveryState(claim.delivery.state)) {
        return { _tag: "Waiting" } as const;
      }
      const loadTerminalHistory = (requireTerminal: boolean) =>
        loadAgentControlImplementationOrchestrationEvidence(sql, claim, { requireTerminal }).pipe(
          Effect.catchIf(
            (cause) => isOrchestrationError(cause) && cause.reason === "ambiguous-terminal",
            () => Effect.succeed({ _tag: "AmbiguousEvidence" } as const),
          ),
          Effect.mapError((cause) =>
            failure(
              handoffId,
              isOrchestrationError(cause) ? cause.operation : "orchestration-evidence",
              isOrchestrationError(cause) && cause.reason === "persistence"
                ? "persistence"
                : "orchestration-history-corrupt",
              cause,
            ),
          ),
        );
      let orchestrationResult = yield* loadTerminalHistory(false);
      if (orchestrationResult._tag === "AmbiguousEvidence") return { _tag: "Ambiguous" } as const;
      if (orchestrationResult._tag === "Waiting") {
        return yield* failure(
          handoffId,
          "native-terminal-start-history-missing",
          "orchestration-history-corrupt",
        );
      }
      if (orchestrationResult.evidence.terminal === null) {
        const receipt = yield* loadNativeTerminalReceipt("implementation", claim).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError((cause) =>
            failure(
              handoffId,
              "load-native-terminal-receipt",
              "orchestration-history-corrupt",
              cause,
            ),
          ),
        );
        if (Option.isNone(receipt))
          return yield* failure(
            handoffId,
            "native-terminal-receipt-missing",
            "orchestration-history-corrupt",
          );
        const command = yield* makeProviderTerminalSessionCommand(
          receipt.value,
          claim.evidence.runtimeMode,
        ).pipe(
          Effect.mapError((cause) =>
            failure(
              handoffId,
              "replay-native-terminal-identity",
              "orchestration-history-corrupt",
              cause,
            ),
          ),
        );
        yield* orchestration
          .dispatch(command)
          .pipe(
            Effect.mapError((cause) =>
              failure(handoffId, "replay-native-terminal-command", "persistence", cause),
            ),
          );
        orchestrationResult = yield* loadTerminalHistory(true);
        if (orchestrationResult._tag === "Waiting")
          return yield* failure(
            handoffId,
            "replay-native-terminal-history-missing",
            "orchestration-history-corrupt",
          );
        if (orchestrationResult._tag === "AmbiguousEvidence") return { _tag: "Ambiguous" } as const;
      }

      const stage = yield* loadAuthoritativeStageRunState(
        AgentControlStageRunId.make(claim.evidence.stageRunId),
        stageEvents,
        stageStates,
      ).pipe(
        Effect.mapError((cause) =>
          failure(handoffId, "load-stage-before-finalize", "stage-history-corrupt", cause),
        ),
      );
      if (Option.isNone(stage)) {
        return yield* failure(handoffId, "load-stage-before-finalize", "stage-history-corrupt");
      }
      if (stage.value.state.status === "prepared" && stage.value.state.revision === 1) {
        const started = yield* starter
          .processHandoff(handoffId)
          .pipe(
            Effect.mapError((cause) =>
              failure(
                handoffId,
                "reconstruct-stage-start",
                cause.reason === "revision-conflict" ? "revision-conflict" : "persistence",
                cause,
              ),
            ),
          );
        if (started._tag === "Waiting") return { _tag: "Waiting" } as const;
        return { _tag: "Started", stageEventSequence: started.stageEventSequence } as const;
      }
      if (stage.value.state.status !== "running" || stage.value.state.revision !== 2) {
        return yield* failure(handoffId, "stage-not-running", "stage-history-corrupt");
      }

      const transaction = yield* sql
        .withTransaction(finalizeInTransaction(handoffId))
        .pipe(
          Effect.mapError((cause) =>
            isFinalizerError(cause)
              ? cause
              : failure(handoffId, "finalization-transaction", "persistence", cause),
          ),
        );
      if (transaction._tag === "Replayed") return transaction;
      if (transaction._tag !== "Finalized") return transaction;
      yield* hooks.afterOuterCommit(handoffId);
      if (providerAdmissionRelease !== undefined) {
        const released = yield* sql
          .withTransaction(providerAdmissionRelease.releaseInTransaction(transaction.releaseInput))
          .pipe(
            Effect.mapError((cause) =>
              failure(handoffId, "provider-admission-release", "persistence", cause),
            ),
          );
        yield* providerAdmissionRelease.signalCommitted(released);
      }
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* stageEngine.publishCommitted([transaction.publication.stageEvent]);
          yield* leaseEngine.publishCommitted([transaction.publication.leaseEvent]);
          yield* PubSub.publish(publications, transaction.publication);
        }),
      );
      yield* hooks.afterPublication(handoffId);
      return {
        _tag: "Finalized",
        resultEvidenceId: transaction.publication.resultEvidenceId,
      } as const;
    },
  );

  const processHandoff: AgentControlImplementationStageFinalizerShape["processHandoff"] = (
    handoffId,
  ) =>
    Effect.gen(function* () {
      const replay = yield* replayFirst(handoffId);
      if (Option.isSome(replay)) {
        return {
          _tag: "Replayed",
          resultEvidenceId: replay.value.resultEvidenceId,
        } as const;
      }
      return yield* processFresh(handoffId);
    }).pipe(
      Effect.catch((cause) =>
        cause.reason === "revision-conflict" || cause.reason === "persistence"
          ? Effect.gen(function* () {
              const replay = yield* replayFirst(handoffId);
              if (Option.isSome(replay)) {
                return {
                  _tag: "Replayed",
                  resultEvidenceId: replay.value.resultEvidenceId,
                } as const;
              }
              return yield* cause;
            })
          : Effect.fail(cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = 100;
    let afterHandoffId: string | undefined;
    while (true) {
      const candidates = yield* store
        .listStageFinalizationCandidates({
          ...(afterHandoffId === undefined ? {} : { afterHandoffId }),
          limit: pageSize,
        })
        .pipe(
          Effect.mapError((cause) => failure("recovery", "list-candidates", "persistence", cause)),
        );
      yield* Effect.forEach(
        candidates,
        (handoffId) =>
          processHandoff(handoffId).pipe(
            Effect.catchIf(
              (cause) =>
                cause.reason === "candidate-evidence" ||
                cause.reason === "partial-replay" ||
                cause.reason === "identity-mismatch" ||
                cause.reason === "orchestration-history-corrupt" ||
                cause.reason === "stage-history-corrupt" ||
                cause.reason === "lease-history-corrupt",
              (cause) =>
                Effect.logError("implementation finalization candidate failed", {
                  handoffId,
                  operation: cause.operation,
                  reason: cause.reason,
                }),
            ),
          ),
        { concurrency: 1, discard: true },
      );
      if (candidates.length < pageSize) break;
      afterHandoffId = candidates.at(-1)!;
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
              Effect.logError("implementation finalization candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              }),
          ),
        );
  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlImplementationStageFinalizer.start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(wakeup.stream, (handoffId) => worker.enqueue(handoffId)),
    );
    if (orchestration.subscribeDomainEvents !== undefined) {
      const events = yield* orchestration.subscribeDomainEvents;
      yield* Effect.forkScoped(
        Stream.runForEach(events, (event) =>
          event.aggregateKind === "thread" ? worker.enqueue(null) : Effect.void,
        ),
      );
    }
    yield* Effect.forkScoped(
      Stream.runForEach(stageEngine.streamDomainEvents, (event) =>
        event.type === "agentControl.stageRun.implementationStarted"
          ? worker.enqueue(null)
          : Effect.void,
      ),
    );
    yield* worker.enqueue(null);
  });

  return AgentControlImplementationStageFinalizer.of({
    processHandoff,
    recover,
    start,
    drain: worker.drain,
    streamPublications: Stream.fromPubSub(publications),
    subscribePublications: PubSub.subscribe(publications).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const AgentControlImplementationStageFinalizerLive = Layer.effect(
  AgentControlImplementationStageFinalizer,
  make,
);
