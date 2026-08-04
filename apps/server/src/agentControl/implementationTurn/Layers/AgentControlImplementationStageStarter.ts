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
import {
  deriveImplementationStageStartCommandId,
  deriveImplementationStageStartEvidenceId,
  deriveImplementationStageStartEventId,
  deriveImplementationStageStartMarkerId,
  deriveImplementationStageStartReceiptId,
  fingerprintImplementationTurn,
} from "../identity.ts";
import { AgentControlImplementationHandoffStore } from "../Services/AgentControlImplementationHandoffStore.ts";
import {
  AgentControlImplementationStageStarter,
  AgentControlImplementationStageStarterError,
  type AgentControlImplementationStageStarterShape,
  type AgentControlImplementationStageStarterResult,
} from "../Services/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationStageStarterHooks } from "../Services/AgentControlImplementationStageStarterHooks.ts";
import { AgentControlImplementationTurnWakeup } from "../Services/AgentControlImplementationTurnWakeup.ts";

const isStarterError = Schema.is(AgentControlImplementationStageStarterError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlImplementationHandoffStore;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const wakeup = yield* AgentControlImplementationTurnWakeup;
  const hooks = yield* AgentControlImplementationStageStarterHooks;

  const error = (
    handoffId: string,
    operation: string,
    reason: AgentControlImplementationStageStarterError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlImplementationStageStarterError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const replay = Effect.fn("AgentControlImplementationStageStarter.replay")(function* (
    handoffId: string,
  ) {
    const rows = yield* sql<{
      readonly stageRunId: string;
      readonly providerDeliveryId: string;
      readonly providerTurnId: string;
      readonly startCommandId: string;
      readonly startEvidenceId: string;
      readonly startReceiptId: string;
      readonly startMarkerId: string;
      readonly startFingerprint: string;
      readonly stageEventId: string;
      readonly stageEventSequence: number;
      readonly stageEventStreamVersion: number;
      readonly deliveryRevision: number;
      readonly claimGeneration: number;
      readonly attemptCount: number;
      readonly startedAt: string;
    }>`
      SELECT evidence.stage_run_id AS "stageRunId",
        evidence.provider_delivery_id AS "providerDeliveryId",
        evidence.provider_turn_id AS "providerTurnId",
        evidence.start_command_id AS "startCommandId",
        evidence.start_evidence_id AS "startEvidenceId",
        receipt.start_receipt_id AS "startReceiptId",
        marker.start_marker_id AS "startMarkerId",
        evidence.start_fingerprint AS "startFingerprint",
        evidence.stage_event_id AS "stageEventId",
        evidence.stage_event_sequence AS "stageEventSequence",
        evidence.stage_event_stream_version AS "stageEventStreamVersion",
        evidence.delivery_revision AS "deliveryRevision",
        evidence.claim_generation AS "claimGeneration",
        evidence.attempt_count AS "attemptCount",
        evidence.started_at AS "startedAt"
      FROM agent_control_implementation_stage_started_evidence evidence
      JOIN agent_control_implementation_stage_started_receipts receipt
        ON receipt.start_evidence_id = evidence.start_evidence_id
      JOIN agent_control_implementation_stage_started_markers marker
        ON marker.start_evidence_id = evidence.start_evidence_id
      JOIN agent_control_implementation_handoff_accepted accepted
        ON accepted.handoff_id = ${handoffId}
       AND accepted.provider_delivery_id = evidence.provider_delivery_id
    `;
    const count = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM agent_control_implementation_stage_started_evidence evidence
         JOIN agent_control_implementation_handoff_accepted accepted
           ON accepted.provider_delivery_id = evidence.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_implementation_stage_started_receipts receipt
         JOIN agent_control_implementation_handoff_accepted accepted
           ON accepted.provider_delivery_id = receipt.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) +
        (SELECT count(*) FROM agent_control_implementation_stage_started_markers marker
         JOIN agent_control_implementation_handoff_accepted accepted
           ON accepted.provider_delivery_id = marker.provider_delivery_id
         WHERE accepted.handoff_id = ${handoffId}) AS count
    `;
    if ((count[0]?.count ?? 0) === 0) return Option.none<number>();
    if (count[0]?.count !== 3 || rows.length !== 1) {
      return yield* error(handoffId, "replay-partial", "identity-mismatch");
    }
    const row = rows[0]!;
    const commandId = deriveImplementationStageStartCommandId(
      row.providerDeliveryId,
      row.providerTurnId,
    );
    if (
      row.startCommandId !== commandId ||
      row.startEvidenceId !== deriveImplementationStageStartEvidenceId(commandId) ||
      row.startReceiptId !== deriveImplementationStageStartReceiptId(commandId) ||
      row.startMarkerId !== deriveImplementationStageStartMarkerId(commandId) ||
      row.stageEventId !== deriveImplementationStageStartEventId(commandId) ||
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
    const expectedFingerprint = fingerprintImplementationTurn("stage-start", [
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
    if (
      Option.isNone(stage) ||
      stage.value.state.status !== "running" ||
      stage.value.state.revision !== 2 ||
      stage.value.state.roleId !== "implementer" ||
      stage.value.state.stageKind !== "implementation" ||
      stage.value.state.stageOrdinal !== 2 ||
      stage.value.state.attemptOrdinal !== 1 ||
      stage.value.state.attemptId !== claim.evidence.attemptId ||
      stage.value.state.taskRevision !== claim.evidence.taskRevision ||
      stage.value.state.githubIntakeSequence !== claim.evidence.githubIntakeSequence ||
      stage.value.state.sourceIdentityFingerprint !== claim.evidence.sourceIdentityFingerprint ||
      stage.value.events.length !== 2 ||
      stage.value.events[1]?.eventId !== row.stageEventId ||
      stage.value.events[1]?.sequence !== row.stageEventSequence
    ) {
      return yield* error(handoffId, "replay-stage", "stage-history-corrupt");
    }
    return Option.some(row.stageEventSequence);
  });

  const startInTransaction = Effect.fn("AgentControlImplementationStageStarter.startInTransaction")(
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
        ![
          "provider-started",
          "interrupt-requested",
          "ambiguous",
          "completed",
          "failed",
          "interrupted",
        ].includes(claim.delivery.state)
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
        stage.value.state.roleId !== "implementer" ||
        stage.value.state.stageKind !== "implementation" ||
        stage.value.state.stageOrdinal !== 2 ||
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
      const commandId = deriveImplementationStageStartCommandId(
        claim.evidence.providerDeliveryId,
        providerTurnId,
      );
      const eventId = deriveImplementationStageStartEventId(commandId);
      const startEvidenceId = deriveImplementationStageStartEvidenceId(commandId);
      const startReceiptId = deriveImplementationStageStartReceiptId(commandId);
      const startMarkerId = deriveImplementationStageStartMarkerId(commandId);
      const fingerprint = fingerprintImplementationTurn("stage-start", [
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
        type: "agentControl.stageRun.implementationStarted",
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
          roleId: "implementer",
          stageKind: "implementation",
          stageOrdinal: 2,
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
      INSERT INTO agent_control_implementation_stage_started_evidence (
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
      INSERT INTO agent_control_implementation_stage_started_receipts (
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
      INSERT INTO agent_control_implementation_stage_started_markers (
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

  const processUnchecked = Effect.fn("AgentControlImplementationStageStarter.processHandoff")(
    function* (handoffId: string) {
      const accepted = yield* replay(handoffId);
      if (Option.isSome(accepted)) {
        return {
          _tag: "Replayed",
          stageEventSequence: accepted.value,
        } satisfies AgentControlImplementationStageStarterResult;
      }
      const transaction = yield* Effect.exit(sql.withTransaction(startInTransaction(handoffId)));
      if (Exit.isFailure(transaction)) {
        const recovered = yield* replay(handoffId);
        if (Option.isSome(recovered)) {
          return {
            _tag: "Replayed",
            stageEventSequence: recovered.value,
          } satisfies AgentControlImplementationStageStarterResult;
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
      } satisfies AgentControlImplementationStageStarterResult;
    },
  );
  const processHandoff: AgentControlImplementationStageStarterShape["processHandoff"] = (
    handoffId,
  ) =>
    processUnchecked(handoffId).pipe(
      Effect.mapError((cause) =>
        isStarterError(cause) ? cause : error(handoffId, "process", "persistence", cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const handoffIds = yield* store
      .listStageStartCandidates()
      .pipe(Effect.mapError((cause) => error("recovery", "list-candidates", "persistence", cause)));
    yield* Effect.forEach(
      handoffIds,
      (handoffId) =>
        processHandoff(handoffId).pipe(
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
            (cause) =>
              Effect.logError("implementation stage-start candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              }),
          ),
        ),
      { concurrency: 1, discard: true },
    );
  });
  const processSafely = (handoffId: string | null) =>
    handoffId === null
      ? recover
      : processHandoff(handoffId).pipe(
          Effect.asVoid,
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
            (cause) =>
              Effect.logError("implementation stage-start candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              }),
          ),
        );
  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlImplementationStageStarter.start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(wakeup.stream, (handoffId) => worker.enqueue(handoffId)),
    );
    yield* worker.enqueue(null);
  });
  return AgentControlImplementationStageStarter.of({
    processHandoff,
    recover,
    start,
    drain: worker.drain,
  });
});

export const AgentControlImplementationStageStarterLive = Layer.effect(
  AgentControlImplementationStageStarter,
  make,
);
