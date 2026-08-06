import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  EventId,
  ProjectId,
  type AgentControlStageRunEventDraft,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { loadAuthoritativeStageRunState } from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { loadAuthoritativeLeaseState } from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { decodeCanonicalUtf8Bytes } from "../../initialPlanning/eventEvidence.ts";
import {
  deriveVerificationStageStartCommandId,
  deriveVerificationStageStartEvidenceId,
  deriveVerificationStageStartEventId,
  deriveVerificationStageStartMarkerId,
  deriveVerificationStageStartReceiptId,
  fingerprintVerificationTurn,
} from "../identity.ts";
import { AgentControlVerificationHandoffStore } from "../Services/AgentControlVerificationHandoffStore.ts";
import {
  AgentControlVerificationStageStarter,
  AgentControlVerificationStageStarterError,
  type AgentControlVerificationStageStarterShape,
  type AgentControlVerificationStageStarterResult,
} from "../Services/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationStageStarterHooks } from "../Services/AgentControlVerificationStageStarterHooks.ts";
import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";

const isStarterError = Schema.is(AgentControlVerificationStageStarterError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlVerificationHandoffStore;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const wakeup = yield* AgentControlVerificationTurnWakeup;
  const hooks = yield* AgentControlVerificationStageStarterHooks;

  const error = (
    handoffId: string,
    operation: string,
    reason: AgentControlVerificationStageStarterError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlVerificationStageStarterError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const replay = Effect.fn("AgentControlVerificationStageStarter.replay")(function* (
    handoffId: string,
  ) {
    const rawRows = yield* sql<{
      readonly stageRunIdBytes: unknown;
      readonly providerDeliveryIdBytes: unknown;
      readonly providerTurnIdBytes: unknown;
      readonly startCommandIdBytes: unknown;
      readonly startEvidenceIdBytes: unknown;
      readonly startReceiptIdBytes: unknown;
      readonly startMarkerIdBytes: unknown;
      readonly startFingerprintBytes: unknown;
      readonly stageEventIdBytes: unknown;
      readonly stageEventSequence: number;
      readonly stageEventStreamVersion: number;
      readonly deliveryRevision: number;
      readonly claimGeneration: number;
      readonly attemptCount: number;
      readonly startedAtBytes: unknown;
    }>`
      SELECT CAST(evidence.stage_run_id AS BLOB) AS "stageRunIdBytes",
        CAST(evidence.provider_delivery_id AS BLOB) AS "providerDeliveryIdBytes",
        CAST(evidence.provider_turn_id AS BLOB) AS "providerTurnIdBytes",
        CAST(evidence.start_command_id AS BLOB) AS "startCommandIdBytes",
        CAST(evidence.start_evidence_id AS BLOB) AS "startEvidenceIdBytes",
        CAST(receipt.start_receipt_id AS BLOB) AS "startReceiptIdBytes",
        CAST(marker.start_marker_id AS BLOB) AS "startMarkerIdBytes",
        CAST(evidence.start_fingerprint AS BLOB) AS "startFingerprintBytes",
        CAST(evidence.stage_event_id AS BLOB) AS "stageEventIdBytes",
        evidence.stage_event_sequence AS "stageEventSequence",
        evidence.stage_event_stream_version AS "stageEventStreamVersion",
        evidence.delivery_revision AS "deliveryRevision",
        evidence.claim_generation AS "claimGeneration",
        evidence.attempt_count AS "attemptCount",
        CAST(evidence.started_at AS BLOB) AS "startedAtBytes"
      FROM agent_control_verification_stage_started_evidence evidence
      JOIN agent_control_verification_stage_started_receipts receipt
        ON receipt.start_evidence_id = evidence.start_evidence_id
      JOIN agent_control_verification_stage_started_markers marker
        ON marker.start_evidence_id = evidence.start_evidence_id
      JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = ${handoffId}
       AND accepted.provider_delivery_id = evidence.provider_delivery_id
    `;
    const count = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM agent_control_verification_stage_started_evidence evidence
         JOIN agent_control_verification_handoff_accepted accepted
           ON accepted.provider_delivery_id = evidence.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_verification_stage_started_receipts receipt
         JOIN agent_control_verification_handoff_accepted accepted
           ON accepted.provider_delivery_id = receipt.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_verification_stage_started_markers marker
         JOIN agent_control_verification_handoff_accepted accepted
           ON accepted.provider_delivery_id = marker.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) AS count
    `;
    if ((count[0]?.count ?? 0) === 0) return Option.none<number>();
    if (count[0]?.count !== 3 || rawRows.length !== 1) {
      return yield* error(handoffId, "replay-partial", "identity-mismatch");
    }
    const raw = rawRows[0]!;
    const row = yield* Effect.try({
      try: () => ({
        stageRunId: decodeCanonicalUtf8Bytes(raw.stageRunIdBytes),
        providerDeliveryId: decodeCanonicalUtf8Bytes(raw.providerDeliveryIdBytes),
        providerTurnId: decodeCanonicalUtf8Bytes(raw.providerTurnIdBytes),
        startCommandId: decodeCanonicalUtf8Bytes(raw.startCommandIdBytes),
        startEvidenceId: decodeCanonicalUtf8Bytes(raw.startEvidenceIdBytes),
        startReceiptId: decodeCanonicalUtf8Bytes(raw.startReceiptIdBytes),
        startMarkerId: decodeCanonicalUtf8Bytes(raw.startMarkerIdBytes),
        startFingerprint: decodeCanonicalUtf8Bytes(raw.startFingerprintBytes),
        stageEventId: decodeCanonicalUtf8Bytes(raw.stageEventIdBytes),
        stageEventSequence: raw.stageEventSequence,
        stageEventStreamVersion: raw.stageEventStreamVersion,
        deliveryRevision: raw.deliveryRevision,
        claimGeneration: raw.claimGeneration,
        attemptCount: raw.attemptCount,
        startedAt: decodeCanonicalUtf8Bytes(raw.startedAtBytes),
      }),
      catch: (cause) => error(handoffId, "replay-decode", "identity-mismatch", cause),
    });
    const commandId = deriveVerificationStageStartCommandId(
      row.providerDeliveryId,
      row.providerTurnId,
    );
    if (
      row.startCommandId !== commandId ||
      row.startEvidenceId !== deriveVerificationStageStartEvidenceId(commandId) ||
      row.startReceiptId !== deriveVerificationStageStartReceiptId(commandId) ||
      row.startMarkerId !== deriveVerificationStageStartMarkerId(commandId) ||
      row.stageEventId !== deriveVerificationStageStartEventId(commandId) ||
      row.stageEventStreamVersion !== 2
    ) {
      return yield* error(handoffId, "replay-identity", "identity-mismatch");
    }
    const claimOption = yield* store
      .loadAcceptedByHandoffId(handoffId)
      .pipe(
        Effect.mapError((cause) => error(handoffId, "replay-handoff", "identity-mismatch", cause)),
      );
    if (Option.isNone(claimOption)) {
      return yield* error(handoffId, "replay-handoff", "identity-mismatch");
    }
    const claim = claimOption.value;
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
      row.providerTurnId,
      row.stageEventId,
      row.startedAt,
    ]);
    if (
      row.stageRunId !== claim.evidence.stageRunId ||
      row.providerDeliveryId !== claim.evidence.providerDeliveryId ||
      row.providerTurnId !== claim.delivery.providerTurnId ||
      row.startedAt !== claim.delivery.providerAcceptedAt ||
      row.deliveryRevision > claim.delivery.revision ||
      row.claimGeneration !== claim.delivery.claimGeneration ||
      row.attemptCount !== claim.delivery.attemptCount ||
      row.startFingerprint !== expectedFingerprint
    ) {
      return yield* error(handoffId, "replay-fingerprint", "identity-mismatch");
    }
    const stage = yield* loadAuthoritativeStageRunState(
      AgentControlStageRunId.make(row.stageRunId),
      stageEvents,
      stageStates,
    ).pipe(
      Effect.mapError((cause) => error(handoffId, "replay-stage", "stage-history-corrupt", cause)),
    );
    const prepared = Option.isSome(stage) ? stage.value.events[0] : undefined;
    const started = Option.isSome(stage) ? stage.value.events[1] : undefined;
    if (
      Option.isNone(stage) ||
      prepared?.type !== "agentControl.stageRun.prepared" ||
      prepared.streamVersion !== 1 ||
      prepared.payload.roleId !== "verifier" ||
      prepared.payload.stageKind !== "verification" ||
      prepared.payload.stageOrdinal !== 3 ||
      prepared.payload.attemptOrdinal !== 1 ||
      prepared.payload.attemptId !== claim.evidence.attemptId ||
      prepared.payload.taskRevision !== claim.evidence.taskRevision ||
      prepared.payload.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
      prepared.payload.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint ||
      started?.type !== "agentControl.stageRun.verificationStarted" ||
      started.streamVersion !== 2 ||
      started.eventId !== row.stageEventId ||
      started.sequence !== row.stageEventSequence ||
      started.payload.providerDeliveryId !== row.providerDeliveryId ||
      started.payload.providerTurnId !== row.providerTurnId
    ) {
      return yield* error(handoffId, "replay-stage", "stage-history-corrupt");
    }
    return Option.some(row.stageEventSequence);
  });

  const startInTransaction = Effect.fn("AgentControlVerificationStageStarter.startInTransaction")(
    function* (handoffId: string) {
      const claimOption = yield* store.loadAcceptedByHandoffId(handoffId);
      if (Option.isNone(claimOption)) {
        return yield* error(handoffId, "load-handoff", "identity-mismatch");
      }
      const claim = claimOption.value;
      const providerTurnId = claim.delivery.providerTurnId;
      const providerAcceptedAt = claim.delivery.providerAcceptedAt;
      if (
        providerTurnId === null ||
        providerAcceptedAt === null ||
        claim.delivery.state !== "provider-started"
      ) {
        return { _tag: "Waiting" } as const;
      }
      yield* hooks.afterProviderEvidence(handoffId);
      const stageRunId = AgentControlStageRunId.make(claim.evidence.stageRunId);
      const stage = yield* loadAuthoritativeStageRunState(
        stageRunId,
        stageEvents,
        stageStates,
      ).pipe(
        Effect.mapError((cause) => error(handoffId, "load-stage", "stage-history-corrupt", cause)),
      );
      if (
        Option.isNone(stage) ||
        stage.value.state.status !== "prepared" ||
        stage.value.state.revision !== 1 ||
        stage.value.state.roleId !== "verifier" ||
        stage.value.state.stageKind !== "verification" ||
        stage.value.state.stageOrdinal !== 3 ||
        stage.value.state.attemptOrdinal !== 1 ||
        stage.value.state.attemptId !== claim.evidence.attemptId ||
        stage.value.state.taskRevision !== claim.evidence.taskRevision ||
        stage.value.state.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
        stage.value.state.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint
      ) {
        return yield* error(handoffId, "load-stage", "stage-history-corrupt");
      }
      const lease = yield* loadAuthoritativeLeaseState(
        AgentControlStageRunLeaseId.make(claim.evidence.leaseId),
        leaseEvents,
        leaseStates,
      ).pipe(
        Effect.mapError((cause) => error(handoffId, "load-lease", "lease-history-corrupt", cause)),
      );
      if (
        Option.isNone(lease) ||
        lease.value.state.status !== "reserved" ||
        lease.value.state.stageRunId !== stageRunId ||
        lease.value.state.attemptId !== claim.evidence.attemptId ||
        lease.value.state.holderId !== claim.evidence.leaseHolderId ||
        lease.value.state.fenceToken !== claim.evidence.fenceToken
      ) {
        return yield* error(handoffId, "load-lease", "lease-history-corrupt");
      }
      const commandId = deriveVerificationStageStartCommandId(
        claim.evidence.providerDeliveryId,
        providerTurnId,
      );
      const eventId = deriveVerificationStageStartEventId(commandId);
      const startEvidenceId = deriveVerificationStageStartEvidenceId(commandId);
      const startReceiptId = deriveVerificationStageStartReceiptId(commandId);
      const startMarkerId = deriveVerificationStageStartMarkerId(commandId);
      const fingerprint = fingerprintVerificationTurn("stage-start", [
        claim.evidence.admissionEvidenceId,
        claim.evidence.admissionReceiptId,
        claim.evidence.admissionMarkerId,
        claim.evidence.materializationEvidenceId,
        claim.evidence.materializationReceiptId,
        claim.evidence.materializationMarkerId,
        claim.evidence.handoffId,
        claim.evidence.handoffFingerprint,
        claim.evidence.providerDeliveryId,
        String(claim.delivery.revision),
        String(claim.delivery.claimGeneration),
        String(claim.delivery.attemptCount),
        claim.evidence.threadId,
        claim.evidence.planningThreadId,
        claim.evidence.planId,
        providerTurnId,
        eventId,
        providerAcceptedAt,
      ]);
      const draft: AgentControlStageRunEventDraft = {
        eventId,
        type: "agentControl.stageRun.verificationStarted",
        aggregateKind: "stage-run",
        aggregateId: stageRunId,
        occurredAt: providerAcceptedAt,
        commandId,
        causationEventId: EventId.make(claim.evidence.turnRequestEventId),
        correlationId: commandId,
        authority: "system",
        metadata: { schemaVersion: 1 },
        payload: {
          projectId: ProjectId.make(claim.evidence.projectId),
          taskId: AgentControlTaskId.make(claim.evidence.taskId),
          stageRunId,
          attemptId: AgentControlAttemptId.make(claim.evidence.attemptId),
          roleId: "verifier",
          stageKind: "verification",
          stageOrdinal: 3,
          attemptOrdinal: 1,
          status: "running",
          taskRevision: claim.evidence.taskRevision,
          githubIntakeSequence: claim.evidence.githubIntakeSequence,
          sourceIdentityFingerprint: claim.evidence.sourceIdentityFingerprint,
          admissionEvidenceId: claim.evidence.admissionEvidenceId,
          admissionReceiptId: claim.evidence.admissionReceiptId,
          admissionMarkerId: claim.evidence.admissionMarkerId,
          materializationEvidenceId: claim.evidence.materializationEvidenceId,
          materializationReceiptId: claim.evidence.materializationReceiptId,
          materializationMarkerId: claim.evidence.materializationMarkerId,
          handoffId: claim.evidence.handoffId,
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
          providerTurnId,
          runtimeMode: claim.evidence.runtimeMode,
          modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
          leaseId: AgentControlStageRunLeaseId.make(claim.evidence.leaseId),
          leaseHolderId: AgentControlStageRunLeaseHolderId.make(claim.evidence.leaseHolderId),
          fenceToken: claim.evidence.fenceToken,
          startedAt: providerAcceptedAt,
        },
      };
      const committed = yield* stageEvents
        .append({
          stageRunId,
          expectedStreamVersion: 1,
          events: [draft],
        })
        .pipe(
          Effect.mapError((cause) =>
            error(
              handoffId,
              "append-stage-started",
              cause._tag === "AgentControlStageRunStreamVersionConflictError"
                ? "revision-conflict"
                : "persistence",
              cause,
            ),
          ),
        );
      const stageEvent = committed[0];
      if (committed.length !== 1 || stageEvent === undefined) {
        return yield* error(handoffId, "append-stage-started", "persistence");
      }
      yield* stageProjection
        .projectEvent(stageEvent)
        .pipe(
          Effect.mapError((cause) =>
            error(handoffId, "project-stage-started", "persistence", cause),
          ),
        );
      yield* hooks.afterStageProjection(handoffId);
      yield* sql`
      INSERT INTO agent_control_verification_stage_started_evidence (
        start_evidence_id, start_command_id, start_fingerprint,
        admission_evidence_id, admission_receipt_id, admission_marker_id,
        materialization_evidence_id, materialization_receipt_id,
        materialization_marker_id, handoff_id, handoff_fingerprint,
        provider_delivery_id, delivery_revision, claim_generation, attempt_count,
        project_id, task_id, task_revision, github_intake_sequence,
        source_identity_fingerprint, stage_run_id, attempt_id,
        controlled_thread_reservation_id, thread_id, planning_thread_id, plan_id,
        proposed_plan_digest, lease_id, lease_holder_id, fence_token,
        provider_instance_id, provider_turn_id, runtime_mode,
        model_selection_fingerprint, stage_event_id, stage_event_sequence,
        stage_event_stream_version, started_at
      ) VALUES (
        ${startEvidenceId}, ${commandId}, ${fingerprint},
        ${claim.evidence.admissionEvidenceId}, ${claim.evidence.admissionReceiptId},
        ${claim.evidence.admissionMarkerId}, ${claim.evidence.materializationEvidenceId},
        ${claim.evidence.materializationReceiptId}, ${claim.evidence.materializationMarkerId},
        ${claim.evidence.handoffId}, ${claim.evidence.handoffFingerprint},
        ${claim.evidence.providerDeliveryId}, ${claim.delivery.revision},
        ${claim.delivery.claimGeneration}, ${claim.delivery.attemptCount},
        ${claim.evidence.projectId}, ${claim.evidence.taskId}, ${claim.evidence.taskRevision},
        ${claim.evidence.githubIntakeSequence}, ${claim.evidence.sourceIdentityFingerprint},
        ${claim.evidence.stageRunId}, ${claim.evidence.attemptId},
        ${claim.evidence.controlledThreadReservationId}, ${claim.evidence.threadId},
        ${claim.evidence.planningThreadId}, ${claim.evidence.planId},
        ${claim.evidence.proposedPlanDigest}, ${claim.evidence.leaseId},
        ${claim.evidence.leaseHolderId}, ${claim.evidence.fenceToken},
        ${claim.evidence.providerInstanceId}, ${providerTurnId}, ${claim.evidence.runtimeMode},
        ${claim.evidence.modelSelectionFingerprint}, ${stageEvent.eventId},
        ${stageEvent.sequence}, ${stageEvent.streamVersion}, ${providerAcceptedAt}
      )
    `;
      yield* sql`
      INSERT INTO agent_control_verification_stage_started_receipts (
        start_receipt_id, start_evidence_id, start_command_id, start_fingerprint,
        provider_delivery_id, stage_event_id, stage_event_sequence, accepted_at
      ) VALUES (
        ${startReceiptId}, ${startEvidenceId}, ${commandId}, ${fingerprint},
        ${claim.evidence.providerDeliveryId}, ${stageEvent.eventId},
        ${stageEvent.sequence}, ${providerAcceptedAt}
      )
    `;
      yield* hooks.beforeFinalMarker(handoffId);
      yield* sql`
      INSERT INTO agent_control_verification_stage_started_markers (
        start_marker_id, start_evidence_id, start_receipt_id, start_command_id,
        start_fingerprint, provider_delivery_id, stage_event_id,
        stage_event_sequence, committed_at
      ) VALUES (
        ${startMarkerId}, ${startEvidenceId}, ${startReceiptId}, ${commandId},
        ${fingerprint}, ${claim.evidence.providerDeliveryId}, ${stageEvent.eventId},
        ${stageEvent.sequence}, ${providerAcceptedAt}
      )
    `;
      return { _tag: "Started", stageEvent } as const;
    },
  );

  const processUnchecked = Effect.fn("AgentControlVerificationStageStarter.processHandoff")(
    function* (handoffId: string) {
      const accepted = yield* replay(handoffId);
      if (Option.isSome(accepted)) {
        return {
          _tag: "Replayed",
          stageEventSequence: accepted.value,
        } satisfies AgentControlVerificationStageStarterResult;
      }
      const transaction = yield* Effect.exit(sql.withTransaction(startInTransaction(handoffId)));
      if (Exit.isFailure(transaction)) {
        const recovered = yield* replay(handoffId);
        if (Option.isSome(recovered)) {
          return {
            _tag: "Replayed",
            stageEventSequence: recovered.value,
          } satisfies AgentControlVerificationStageStarterResult;
        }
        return yield* Effect.failCause(transaction.cause);
      }
      if (transaction.value._tag === "Waiting") return transaction.value;
      yield* hooks.afterOuterCommit(handoffId);
      yield* stageEngine.publishCommitted([transaction.value.stageEvent]);
      yield* hooks.afterPublication(handoffId);
      return {
        _tag: "Started",
        stageEventSequence: transaction.value.stageEvent.sequence,
      } satisfies AgentControlVerificationStageStarterResult;
    },
  );
  const processHandoff: AgentControlVerificationStageStarterShape["processHandoff"] = (handoffId) =>
    processUnchecked(handoffId).pipe(
      Effect.mapError((cause) =>
        isStarterError(cause) ? cause : error(handoffId, "process", "persistence", cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = 64;
    let cursor = "";
    while (true) {
      const handoffIds = yield* store
        .listStageStartCandidates(cursor, pageSize)
        .pipe(
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
                Effect.logError("verification stage-start candidate failed", {
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
              Effect.logError("verification stage-start candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              }),
          ),
        );
  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlVerificationStageStarter.start")(function* () {
    const wakeupPublications = yield* wakeup.subscribe;
    yield* Effect.forkScoped(
      Stream.runForEach(wakeupPublications, (handoffId) => worker.enqueue(handoffId)),
      { startImmediately: true },
    );
    yield* worker.enqueue(null);
  });
  return AgentControlVerificationStageStarter.of({
    processHandoff,
    recover,
    start,
    drain: worker.drain,
  });
});

export const AgentControlVerificationStageStarterLive = Layer.effect(
  AgentControlVerificationStageStarter,
  make,
);
