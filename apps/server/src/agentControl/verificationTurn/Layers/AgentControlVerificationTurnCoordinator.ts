import { loadRunOnceRepairForImplementationStage } from "../../runOnce/repair.ts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import {
  AgentControlControlledThreadReservationId,
  AgentControlStageRunLeaseHolderId,
  AgentControlTaskId,
  AgentControlThreadBinding,
  AgentControlThreadMaterializeCommand,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  ModelSelection,
  ProjectId,
  ThreadId,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlTaskState,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
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
} from "../../initialPlanning/eventEvidence.ts";

import { AgentControlPolicyService } from "../../AgentControlPolicyService.ts";
import {
  AgentControlVerificationAdmission,
  type AgentControlVerificationAdmissionEvidence,
} from "../../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { loadAuthoritativeStageRunState } from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { loadAuthoritativeLeaseState } from "../../stageRunLease/authoritative.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { sameAgentControlWorktreeReservationState } from "../../worktree/authoritative.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import {
  OrchestrationEngineService,
  type AgentControlThreadMaterializationTransactionResult,
} from "../../../orchestration/Services/OrchestrationEngine.ts";
import { fingerprintAgentControlThreadMaterializationCommand } from "../../../orchestration/agentControlThreadMaterializationIntent.ts";
import { loadControlledThreadMaterializationReplay } from "../../controlledThreadReservation/materializationReplay.ts";
import { loadAuthoritativeControlledThreadReservation } from "../../controlledThreadReservation/authoritative.ts";
import { decideAgentControlControlledThreadReservationCommand } from "../../controlledThreadReservation/decider.ts";
import {
  deriveAgentControlBoundTransitionCommandId,
  deriveAgentControlControlledThreadActivationCommandId,
  deriveAgentControlMaterializingTransitionCommandId,
  deriveAgentControlThreadMaterializationCommandId,
} from "../../controlledThreadReservation/identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../../controlledThreadReservation/projector.ts";
import { AgentControlControlledThreadReservationEngine } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationStateRepository.ts";
import { buildExpectedAgentControlVerificationHandoff } from "../handoffValidation.ts";
import {
  deriveVerificationHandoffId,
  deriveVerificationMaterializationEvidenceId,
  deriveVerificationMaterializationMarkerId,
  deriveVerificationMaterializationReceiptId,
  deriveVerificationProviderDeliveryId,
  fingerprintVerificationTurn,
} from "../identity.ts";
import { AgentControlVerificationHandoffStoreLive } from "./AgentControlVerificationHandoffStore.ts";
import { canonicalAgentControlVerificationPromptSource } from "../prompt.ts";
import {
  AgentControlVerificationHandoffStore,
  isAgentControlVerificationCandidateEvidenceError,
  type AgentControlVerificationStoreError,
} from "../Services/AgentControlVerificationHandoffStore.ts";
import {
  AgentControlVerificationTurnCoordinator,
  AgentControlVerificationTurnCoordinatorError,
  type AgentControlVerificationTurnCoordinatorShape,
  type AgentControlVerificationTurnMaterializationPublication,
  type AgentControlVerificationTurnMaterializationResult,
} from "../Services/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationTurnCoordinatorHooks } from "../Services/AgentControlVerificationTurnCoordinatorHooks.ts";
import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";
import {
  AgentControlVerificationHistoricalAuthorityError,
  loadAgentControlVerificationTaskAuthorityInTransaction,
  loadAgentControlVerificationWorktreeAuthorityInTransaction,
} from "../historicalAuthority.ts";

const isVerificationHistoricalAuthorityError = Schema.is(
  AgentControlVerificationHistoricalAuthorityError,
);

interface SelectedRuntime {
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: "approval-required";
  readonly policyRevision: number | null;
  readonly policyJson: string | null;
}

interface CurrentAuthority {
  readonly task: AgentControlTaskState;
  readonly taskSourceEvent: {
    readonly eventId: string;
    readonly sequence: number;
    readonly streamVersion: number;
  };
  readonly worktreeEvent: {
    readonly eventId: string;
    readonly sequence: number;
    readonly streamVersion: number;
  };
  readonly worktree: AgentControlWorktreeReservationState;
}

interface ReplayedMaterialization {
  readonly verificationHandoffId: string;
  readonly threadId: ThreadId;
  readonly orchestrationResult: AgentControlThreadMaterializationTransactionResult;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
}

type GuardedMaterialization =
  | { readonly _tag: "Replayed"; readonly value: ReplayedMaterialization }
  | { readonly _tag: "Committed"; readonly value: ReplayedMaterialization };

const decodeMaterializationCommand = Schema.decodeUnknownEffect(
  AgentControlThreadMaterializeCommand,
);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const decodeBindingJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlThreadBinding),
);
const isCoordinatorError = Schema.is(AgentControlVerificationTurnCoordinatorError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const admission = yield* AgentControlVerificationAdmission;
  const policy = yield* AgentControlPolicyService;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const worktreeController = yield* AgentControlWorktreeController;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const reservationEvents = yield* AgentControlControlledThreadReservationEventStore;
  const reservationStates = yield* AgentControlControlledThreadReservationStateRepository;
  const reservationProjection = yield* AgentControlControlledThreadReservationProjection;
  const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
  const orchestration = yield* OrchestrationEngineService;
  const handoffStore = yield* AgentControlVerificationHandoffStore;
  const wakeup = yield* AgentControlVerificationTurnWakeup;
  const hooks = yield* AgentControlVerificationTurnCoordinatorHooks;
  const publications =
    yield* PubSub.unbounded<AgentControlVerificationTurnMaterializationPublication>();

  const error = (
    handoffId: string,
    operation: string,
    reason: AgentControlVerificationTurnCoordinatorError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlVerificationTurnCoordinatorError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const observation = (evidence: AgentControlVerificationAdmissionEvidence) => ({
    handoffId: evidence.handoffId,
    controlledThreadReservationId: evidence.verificationControlledThreadReservationId,
    threadId: evidence.verificationThreadId,
  });

  const fromStoreError = (
    handoffId: string,
    operation: string,
    cause: AgentControlVerificationStoreError,
  ) =>
    error(
      handoffId,
      operation,
      isAgentControlVerificationCandidateEvidenceError(cause)
        ? "admission-corrupt"
        : cause.reason === "revision-conflict"
          ? "reservation-conflict"
          : "persistence",
      cause,
    );

  const loadPolicyBinding = (handoffId: string, projectId: ProjectId) =>
    sql<{ readonly revision: number | null; readonly policyJson: string | null }>`
      SELECT policy.revision, policy.policy_json AS "policyJson"
      FROM projection_projects project
      LEFT JOIN agent_control_project_policies policy ON policy.project_id = project.project_id
      WHERE project.project_id = ${projectId} AND project.deleted_at IS NULL
    `.pipe(
      Effect.mapError((cause) => error(handoffId, "load-policy", "persistence", cause)),
      Effect.flatMap((rows) =>
        rows.length === 1
          ? Effect.succeed(rows[0]!)
          : Effect.fail(error(handoffId, "load-policy", "source-stale")),
      ),
    );

  const resolveRuntime = Effect.fn("AgentControlVerificationTurnCoordinator.resolveRuntime")(
    function* (evidence: AgentControlVerificationAdmissionEvidence) {
      const projectId = ProjectId.make(evidence.projectId);
      const before = yield* loadPolicyBinding(evidence.handoffId, projectId);
      const preflight = yield* policy
        .preflightRuntime({ projectId })
        .pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "runtime-preflight", "runtime-policy-unavailable", cause),
          ),
        );
      const runtimeRole = preflight.roles.find((role) => role.role === "verifier");
      const staticRole = preflight.staticPreflight.roles.find((role) => role.role === "verifier");
      const selection =
        runtimeRole?.selectedCandidateIndex === null || runtimeRole === undefined
          ? undefined
          : staticRole?.validCandidates[runtimeRole.selectedCandidateIndex]?.selection;
      const selectedRuntime = runtimeRole?.candidates.find(
        (candidate) => candidate.candidateIndex === runtimeRole.selectedCandidateIndex,
      );
      if (
        !preflight.ok ||
        runtimeRole === undefined ||
        staticRole === undefined ||
        selection === undefined ||
        selectedRuntime === undefined ||
        !selectedRuntime.runtimeReady ||
        selectedRuntime.providerInstanceId !== selection.instanceId ||
        selectedRuntime.model !== selection.model
      ) {
        return yield* error(evidence.handoffId, "runtime-preflight", "runtime-policy-unavailable");
      }
      const after = yield* loadPolicyBinding(evidence.handoffId, projectId);
      if (!Equal.equals(before, after)) {
        return yield* error(evidence.handoffId, "runtime-policy-raced", "source-stale");
      }
      return {
        modelSelection: selection,
        runtimeMode: "approval-required",
        policyRevision: after.revision,
        policyJson: after.policyJson,
      } satisfies SelectedRuntime;
    },
  );

  const sameAdmissionWorktree = (
    evidence: AgentControlVerificationAdmissionEvidence,
    worktree: AgentControlWorktreeReservationState,
  ): boolean =>
    worktree.status === "ready" &&
    worktree.reservationId === evidence.worktreeReservationId &&
    worktree.projectId === evidence.projectId &&
    worktree.taskId === evidence.taskId &&
    worktree.taskRevision === evidence.taskRevision &&
    worktree.githubIntakeSequence === evidence.githubIntakeSequence &&
    worktree.sourceIdentityFingerprint === evidence.sourceIdentityFingerprint &&
    worktree.revision === evidence.worktreeRevision &&
    worktree.sequence === evidence.worktreeEventSequence &&
    worktree.ownershipFingerprint === evidence.worktreeOwnershipFingerprint &&
    worktree.verifiedAt === evidence.worktreeVerifiedAt;

  const validateCurrent = Effect.fn("AgentControlVerificationTurnCoordinator.validateCurrent")(
    function* (
      evidence: AgentControlVerificationAdmissionEvidence,
      guardedWorktree: AgentControlWorktreeReservationState,
      runtime: SelectedRuntime,
    ) {
      const projectId = ProjectId.make(evidence.projectId);
      const taskId = AgentControlTaskId.make(evidence.taskId);
      const reservationId = AgentControlControlledThreadReservationId.make(
        evidence.verificationControlledThreadReservationId,
      );
      const reservation = yield* loadAuthoritativeControlledThreadReservation(
        reservationId,
        reservationEvents,
        reservationStates,
      ).pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "load-reservation", "admission-corrupt", cause),
        ),
      );
      if (Option.isNone(reservation)) {
        return yield* error(evidence.handoffId, "load-reservation", "admission-corrupt");
      }
      const state = reservation.value;
      if (
        state.status !== "prepared" ||
        state.revision !== 1 ||
        state.threadId !== evidence.verificationThreadId ||
        state.projectId !== evidence.projectId ||
        state.taskId !== evidence.taskId ||
        state.taskRevision !== evidence.taskRevision ||
        state.githubIntakeSequence !== evidence.githubIntakeSequence ||
        state.sourceIdentityFingerprint !== evidence.sourceIdentityFingerprint ||
        state.stageRunId !== evidence.verificationStageRunId ||
        state.attemptId !== evidence.verificationAttemptId ||
        state.roleId !== "verifier" ||
        state.stageKind !== "verification" ||
        (state.stageOrdinal !== 3 && state.stageOrdinal !== 5) ||
        state.attemptOrdinal !== 1 ||
        state.leaseId !== evidence.leaseId ||
        state.fenceToken !== evidence.verificationFenceToken ||
        state.worktreeReservationId !== evidence.worktreeReservationId
      ) {
        return yield* error(evidence.handoffId, "validate-reservation", "admission-corrupt");
      }
      const history = yield* reservationEvents
        .readStream(reservationId, 0, 2)
        .pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "reservation-history", "admission-corrupt", cause),
          ),
        );
      if (
        history.length !== 1 ||
        history[0]?.eventId !== evidence.verificationReservationEventId ||
        history[0].sequence !== evidence.verificationReservationEventSequence ||
        history[0].commandId !== evidence.admissionCommandId
      ) {
        return yield* error(evidence.handoffId, "reservation-history", "admission-corrupt");
      }
      const coordinatorCommandId = yield* deriveAgentControlControlledThreadActivationCommandId(
        CommandId.make(evidence.admissionCommandId),
        reservationId,
      );
      const stage = yield* loadAuthoritativeStageRunState(
        state.stageRunId,
        stageEvents,
        stageStates,
      ).pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "stage-history", "admission-corrupt", cause),
        ),
      );
      if (
        Option.isNone(stage) ||
        stage.value.state.status !== "prepared" ||
        stage.value.state.revision !== 1 ||
        stage.value.state.stageKind !== "verification" ||
        stage.value.state.roleId !== "verifier" ||
        (stage.value.state.stageOrdinal !== 3 && stage.value.state.stageOrdinal !== 5) ||
        stage.value.state.attemptOrdinal !== 1 ||
        stage.value.events[0]?.eventId !== evidence.verificationStageEventId ||
        stage.value.events[0]?.sequence !== evidence.verificationStageEventSequence
      ) {
        return yield* error(evidence.handoffId, "stage-history", "admission-corrupt");
      }
      const leaseAuthority = Effect.gen(function* () {
        const lease = yield* loadAuthoritativeLeaseState(
          state.leaseId,
          leaseEvents,
          leaseStates,
        ).pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "lease-history", "admission-corrupt", cause),
          ),
        );
        const leaseReservation = Option.isSome(lease)
          ? lease.value.events[evidence.verificationLeaseEventStreamVersion - 1]
          : undefined;
        if (
          Option.isNone(lease) ||
          lease.value.state.status !== "reserved" ||
          lease.value.state.stageRunId !== state.stageRunId ||
          lease.value.state.attemptId !== state.attemptId ||
          lease.value.state.holderId !== evidence.leaseHolderId ||
          lease.value.state.holderId !== runtimeHolderId ||
          lease.value.state.fenceToken !== evidence.verificationFenceToken ||
          leaseReservation?.type !== "agentControl.stageRunLease.reserved" ||
          leaseReservation.eventId !== evidence.verificationLeaseEventId ||
          leaseReservation.sequence !== evidence.verificationLeaseEventSequence ||
          leaseReservation.streamVersion !== evidence.verificationLeaseEventStreamVersion ||
          leaseReservation.payload.stageRunId !== state.stageRunId ||
          leaseReservation.payload.attemptId !== state.attemptId ||
          leaseReservation.payload.holderId !== evidence.leaseHolderId ||
          leaseReservation.payload.fenceToken !== evidence.verificationFenceToken ||
          // Admission records the reserve boundary; later same-owner renewals
          // extend its lifetime without replacing that immutable evidence.
          lease.value.events
            .slice(evidence.verificationLeaseEventStreamVersion)
            .some(
              (event) =>
                event.type !== "agentControl.stageRunLease.renewed" ||
                event.payload.stageRunId !== state.stageRunId ||
                event.payload.attemptId !== state.attemptId ||
                event.payload.holderId !== evidence.leaseHolderId ||
                event.payload.fenceToken !== evidence.verificationFenceToken,
            )
        ) {
          return yield* error(evidence.handoffId, "lease-history", "admission-corrupt");
        }
        return lease.value.state;
      });
      let currentLease = yield* leaseAuthority;
      if (!sameAdmissionWorktree(evidence, guardedWorktree)) {
        return yield* error(evidence.handoffId, "guarded-worktree", "source-stale");
      }
      const currentWorktree = yield* worktreeEngine
        .loadAuthoritative(AgentControlWorktreeReservationId.make(evidence.worktreeReservationId))
        .pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "worktree-history", "admission-corrupt", cause),
          ),
        );
      if (currentWorktree === null || !sameAdmissionWorktree(evidence, currentWorktree)) {
        return yield* error(evidence.handoffId, "worktree-history", "source-stale");
      }
      const historicalWorktree = yield* loadAgentControlVerificationWorktreeAuthorityInTransaction(
        sql,
        AgentControlWorktreeReservationId.make(evidence.worktreeReservationId),
      ).pipe(
        Effect.mapError((cause) =>
          error(
            evidence.handoffId,
            "worktree-persisted-history",
            isVerificationHistoricalAuthorityError(cause) ? "admission-corrupt" : "persistence",
            cause,
          ),
        ),
      );
      if (
        !sameAgentControlWorktreeReservationState(currentWorktree, historicalWorktree.state) ||
        historicalWorktree.event.eventId.length === 0
      ) {
        return yield* error(evidence.handoffId, "worktree-persisted-history", "admission-corrupt");
      }
      const policyBinding = yield* loadPolicyBinding(evidence.handoffId, projectId);
      if (
        policyBinding.revision !== runtime.policyRevision ||
        policyBinding.policyJson !== runtime.policyJson
      ) {
        return yield* error(evidence.handoffId, "policy-raced", "source-stale");
      }
      const task = yield* taskGuard.useTaskConsumableInTransaction!(
        projectId,
        taskId,
        (candidate) =>
          Effect.gen(function* () {
            const fingerprint = yield* deriveAgentControlSourceIdentityFingerprint(candidate);
            return candidate.revision === evidence.taskRevision &&
              candidate.githubIntakeSequence === evidence.githubIntakeSequence &&
              fingerprint === evidence.sourceIdentityFingerprint
              ? candidate
              : yield* error(evidence.handoffId, "task-snapshot", "source-stale");
          }),
      ).pipe(
        Effect.mapError((cause) =>
          isCoordinatorError(cause)
            ? cause
            : error(evidence.handoffId, "task-guard", "source-stale", cause),
        ),
      );
      const historicalTask = yield* loadAgentControlVerificationTaskAuthorityInTransaction(
        sql,
        task.taskId,
        task.revision,
      ).pipe(
        Effect.mapError((cause) =>
          error(
            evidence.handoffId,
            "task-persisted-history",
            isVerificationHistoricalAuthorityError(cause) ? "admission-corrupt" : "persistence",
            cause,
          ),
        ),
      );
      if (!Equal.equals(task, historicalTask.state)) {
        return yield* error(evidence.handoffId, "task-persisted-history", "admission-corrupt");
      }
      let expiresAt = canonicalTimestampMillis(currentLease.expiresAt);
      if (expiresAt === null) {
        return yield* error(evidence.handoffId, "lease-history", "admission-corrupt");
      }
      if (expiresAt <= DateTime.toEpochMillis(yield* DateTime.now)) {
        if (leaseEngine.renewOwnedForProviderEffect === undefined) {
          return yield* error(evidence.handoffId, "lease-expired", "admission-corrupt");
        }
        // Renew only the fully validated reservation. Re-read its authority before
        // materializing so a changed owner, fence or history cannot authorize work.
        yield* leaseEngine
          .renewOwnedForProviderEffect(currentLease)
          .pipe(
            Effect.mapError((cause) =>
              error(evidence.handoffId, "lease-renewal", "admission-corrupt", cause),
            ),
          );
        currentLease = yield* leaseAuthority;
        expiresAt = canonicalTimestampMillis(currentLease.expiresAt);
        if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(yield* DateTime.now)) {
          return yield* error(evidence.handoffId, "lease-expired", "admission-corrupt");
        }
      }
      return {
        task: historicalTask.state,
        taskSourceEvent: historicalTask.event,
        worktreeEvent: historicalWorktree.event,
        worktree: historicalWorktree.state,
        coordinatorCommandId,
        stageOrdinal: state.stageOrdinal,
      } satisfies CurrentAuthority & {
        readonly coordinatorCommandId: CommandId;
        readonly stageOrdinal: number;
      };
    },
  );

  const materializationFingerprint = (parts: ReadonlyArray<string>) =>
    fingerprintVerificationTurn("materialization-fingerprint", parts);

  const replayAccepted = Effect.fn("AgentControlVerificationTurnCoordinator.replayAccepted")(
    function* (handoffId: string) {
      const rows = yield* sql<{
        readonly materializationEvidenceId: string;
        readonly materializationReceiptId: string;
        readonly materializationMarkerId: string;
        readonly materializationCommandId: string;
        readonly materializationFingerprint: string;
        readonly admissionEvidenceId: string;
        readonly admissionFingerprint: string;
        readonly controlledThreadReservationId: string;
        readonly threadId: ThreadId;
        readonly coordinatorCommandId: string;
        readonly coordinatorCommandFingerprint: string;
        readonly reservationMaterializingEventId: string;
        readonly reservationMaterializingEventSequence: number;
        readonly reservationBoundEventId: string;
        readonly reservationBoundEventSequence: number;
        readonly orchestrationResultSequence: number;
        readonly planningThreadId: ThreadId;
        readonly planId: string;
        readonly proposedPlanDigest: string;
        readonly taskSourceEventId: string;
        readonly taskSourceEventSequence: number;
        readonly taskSourceEventStreamVersion: number;
        readonly worktreeEventId: string;
        readonly worktreeEventSequence: number;
        readonly worktreeEventStreamVersion: number;
        readonly modelSelectionFingerprint: string;
        readonly implementationHandoffDigestBytes: unknown;
        readonly implementationProviderDeliveryDigestBytes: unknown;
        readonly implementationResultDigestBytes: unknown;
        readonly verificationAdmissionDigestBytes: unknown;
        readonly verificationIdentityDigestBytes: unknown;
        readonly taskHistoryBytes: unknown;
        readonly taskHistoryDigestBytes: unknown;
        readonly worktreeHistoryBytes: unknown;
        readonly worktreeHistoryDigestBytes: unknown;
        readonly stageHistoryBytes: unknown;
        readonly stageHistoryDigestBytes: unknown;
        readonly leaseHistoryBytes: unknown;
        readonly leaseHistoryDigestBytes: unknown;
        readonly reservationHistoryBytes: unknown;
        readonly reservationHistoryDigestBytes: unknown;
        readonly orchestrationHistoryBytes: unknown;
        readonly orchestrationHistoryDigestBytes: unknown;
        readonly repositoryDisplay: string;
        readonly sourceRevision: string;
        readonly taskTitle: string;
        readonly taskBody: string | null;
        readonly providerDeliveryId: string;
        readonly verificationHandoffId: string;
        readonly committedAt: string;
      }>`
        SELECT evidence.materialization_evidence_id AS "materializationEvidenceId",
          receipt.materialization_receipt_id AS "materializationReceiptId",
          marker.materialization_marker_id AS "materializationMarkerId",
          evidence.materialization_command_id AS "materializationCommandId",
          evidence.materialization_fingerprint AS "materializationFingerprint",
          evidence.admission_evidence_id AS "admissionEvidenceId",
          evidence.admission_fingerprint AS "admissionFingerprint",
          evidence.controlled_thread_reservation_id AS "controlledThreadReservationId",
          evidence.thread_id AS "threadId",
          evidence.coordinator_command_id AS "coordinatorCommandId",
          evidence.coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
          evidence.reservation_materializing_event_id AS "reservationMaterializingEventId",
          evidence.reservation_materializing_event_sequence AS
            "reservationMaterializingEventSequence",
          evidence.reservation_bound_event_id AS "reservationBoundEventId",
          evidence.reservation_bound_event_sequence AS "reservationBoundEventSequence",
          evidence.orchestration_result_sequence AS "orchestrationResultSequence",
          evidence.planning_thread_id AS "planningThreadId", evidence.plan_id AS "planId",
          evidence.proposed_plan_digest AS "proposedPlanDigest",
          evidence.task_source_event_id AS "taskSourceEventId",
          evidence.task_source_event_sequence AS "taskSourceEventSequence",
          evidence.task_source_event_stream_version AS "taskSourceEventStreamVersion",
          evidence.worktree_event_id AS "worktreeEventId",
          evidence.worktree_event_sequence AS "worktreeEventSequence",
          evidence.worktree_event_stream_version AS "worktreeEventStreamVersion",
          evidence.model_selection_fingerprint AS "modelSelectionFingerprint",
          CAST(evidence.implementation_handoff_digest AS BLOB)
            AS "implementationHandoffDigestBytes",
          CAST(evidence.implementation_provider_delivery_digest AS BLOB)
            AS "implementationProviderDeliveryDigestBytes",
          CAST(evidence.implementation_result_digest AS BLOB)
            AS "implementationResultDigestBytes",
          CAST(evidence.verification_admission_digest AS BLOB)
            AS "verificationAdmissionDigestBytes",
          CAST(evidence.verification_identity_digest AS BLOB)
            AS "verificationIdentityDigestBytes",
          CAST(evidence.task_history_json AS BLOB) AS "taskHistoryBytes",
          CAST(evidence.task_history_digest AS BLOB) AS "taskHistoryDigestBytes",
          CAST(evidence.worktree_history_json AS BLOB) AS "worktreeHistoryBytes",
          CAST(evidence.worktree_history_digest AS BLOB) AS "worktreeHistoryDigestBytes",
          CAST(evidence.stage_history_json AS BLOB) AS "stageHistoryBytes",
          CAST(evidence.stage_history_digest AS BLOB) AS "stageHistoryDigestBytes",
          CAST(evidence.lease_history_json AS BLOB) AS "leaseHistoryBytes",
          CAST(evidence.lease_history_digest AS BLOB) AS "leaseHistoryDigestBytes",
          CAST(evidence.reservation_history_json AS BLOB) AS "reservationHistoryBytes",
          CAST(evidence.reservation_history_digest AS BLOB) AS "reservationHistoryDigestBytes",
          CAST(evidence.orchestration_history_json AS BLOB) AS "orchestrationHistoryBytes",
          CAST(evidence.orchestration_history_digest AS BLOB)
            AS "orchestrationHistoryDigestBytes",
          evidence.repository_display AS "repositoryDisplay",
          evidence.source_revision AS "sourceRevision",
          evidence.task_title AS "taskTitle", evidence.task_body AS "taskBody",
          marker.provider_delivery_id AS "providerDeliveryId",
          marker.handoff_id AS "verificationHandoffId", marker.committed_at AS "committedAt"
        FROM agent_control_verification_materialization_evidence evidence
        JOIN agent_control_verification_materialization_receipts receipt
          ON receipt.materialization_evidence_id = evidence.materialization_evidence_id
        JOIN agent_control_verification_materialization_markers marker
          ON marker.materialization_evidence_id = evidence.materialization_evidence_id
        WHERE evidence.implementation_result_evidence_id = ${handoffId}
      `.pipe(Effect.mapError((cause) => error(handoffId, "replay-read", "persistence", cause)));
      const counts = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT count(*) FROM agent_control_verification_materialization_evidence
            WHERE implementation_result_evidence_id = ${handoffId}) +
          (SELECT count(*) FROM agent_control_verification_materialization_receipts receipt
            JOIN agent_control_verification_materialization_evidence evidence
              ON evidence.materialization_evidence_id = receipt.materialization_evidence_id
            WHERE evidence.implementation_result_evidence_id = ${handoffId}) +
          (SELECT count(*) FROM agent_control_verification_materialization_markers marker
            JOIN agent_control_verification_materialization_evidence evidence
              ON evidence.materialization_evidence_id = marker.materialization_evidence_id
            WHERE evidence.implementation_result_evidence_id = ${handoffId}) AS count
      `.pipe(Effect.mapError((cause) => error(handoffId, "replay-count", "persistence", cause)));
      if ((counts[0]?.count ?? 0) === 0) return Option.none<ReplayedMaterialization>();
      if (rows.length !== 1 || counts[0]?.count !== 3) {
        return yield* error(handoffId, "replay-partial", "admission-corrupt");
      }
      const rawRow = rows[0]!;
      const row = yield* Effect.try({
        try: () => {
          const decodeHistory = (jsonBytes: unknown, digestBytes: unknown) => {
            const json = decodeCanonicalUtf8Bytes(jsonBytes);
            const digest = decodeCanonicalUtf8Bytes(digestBytes);
            parseCanonicalJson(json);
            if (sha256Utf8(json) !== digest) throw new Error("history digest mismatch");
            return { json, digest };
          };
          return {
            ...rawRow,
            implementationHandoffDigest: decodeCanonicalUtf8Bytes(
              rawRow.implementationHandoffDigestBytes,
            ),
            implementationProviderDeliveryDigest: decodeCanonicalUtf8Bytes(
              rawRow.implementationProviderDeliveryDigestBytes,
            ),
            implementationResultDigest: decodeCanonicalUtf8Bytes(
              rawRow.implementationResultDigestBytes,
            ),
            verificationAdmissionDigest: decodeCanonicalUtf8Bytes(
              rawRow.verificationAdmissionDigestBytes,
            ),
            verificationIdentityDigest: decodeCanonicalUtf8Bytes(
              rawRow.verificationIdentityDigestBytes,
            ),
            taskHistory: decodeHistory(rawRow.taskHistoryBytes, rawRow.taskHistoryDigestBytes),
            worktreeHistory: decodeHistory(
              rawRow.worktreeHistoryBytes,
              rawRow.worktreeHistoryDigestBytes,
            ),
            stageHistory: decodeHistory(rawRow.stageHistoryBytes, rawRow.stageHistoryDigestBytes),
            leaseHistory: decodeHistory(rawRow.leaseHistoryBytes, rawRow.leaseHistoryDigestBytes),
            reservationHistory: decodeHistory(
              rawRow.reservationHistoryBytes,
              rawRow.reservationHistoryDigestBytes,
            ),
            orchestrationHistory: decodeHistory(
              rawRow.orchestrationHistoryBytes,
              rawRow.orchestrationHistoryDigestBytes,
            ),
          };
        },
        catch: (cause) => error(handoffId, "replay-history-decode", "admission-corrupt", cause),
      });
      const admitted = yield* admission
        .loadAcceptedEvidence(handoffId)
        .pipe(
          Effect.mapError((cause) =>
            error(handoffId, "replay-admission-prefix", "admission-corrupt", cause),
          ),
        );
      if (
        Option.isNone(admitted) ||
        admitted.value.taskHistoryJson !== row.taskHistory.json ||
        admitted.value.taskHistoryDigest !== row.taskHistory.digest ||
        admitted.value.worktreeHistoryJson !== row.worktreeHistory.json ||
        admitted.value.worktreeHistoryDigest !== row.worktreeHistory.digest ||
        admitted.value.stageHistoryJson !== row.stageHistory.json ||
        admitted.value.stageHistoryDigest !== row.stageHistory.digest ||
        admitted.value.leaseHistoryJson !== row.leaseHistory.json ||
        admitted.value.leaseHistoryDigest !== row.leaseHistory.digest ||
        admitted.value.reservationHistoryJson !== row.reservationHistory.json ||
        admitted.value.reservationHistoryDigest !== row.reservationHistory.digest ||
        admitted.value.orchestrationHistoryJson !== row.orchestrationHistory.json ||
        admitted.value.orchestrationHistoryDigest !== row.orchestrationHistory.digest
      ) {
        return yield* error(handoffId, "replay-admission-prefix", "admission-corrupt");
      }
      if (
        row.materializationReceiptId !==
          deriveVerificationMaterializationReceiptId(
            row.admissionEvidenceId,
            row.controlledThreadReservationId,
          ) ||
        row.materializationEvidenceId !==
          deriveVerificationMaterializationEvidenceId(
            row.admissionEvidenceId,
            row.controlledThreadReservationId,
          ) ||
        row.materializationMarkerId !==
          deriveVerificationMaterializationMarkerId(
            row.admissionEvidenceId,
            row.controlledThreadReservationId,
          ) ||
        row.verificationHandoffId !== deriveVerificationHandoffId(row.materializationEvidenceId) ||
        row.providerDeliveryId !== deriveVerificationProviderDeliveryId(row.verificationHandoffId)
      ) {
        return yield* error(handoffId, "replay-identity", "identity-mismatch");
      }
      const claim = yield* handoffStore
        .loadAcceptedByHandoffId(row.verificationHandoffId)
        .pipe(
          Effect.mapError((cause) => fromStoreError(handoffId, "replay-handoff-authority", cause)),
        );
      if (
        Option.isNone(claim) ||
        claim.value.evidence.materializationEvidenceId !== row.materializationEvidenceId ||
        claim.value.evidence.materializationReceiptId !== row.materializationReceiptId ||
        claim.value.evidence.materializationMarkerId !== row.materializationMarkerId ||
        claim.value.evidence.taskSourceEventId !== row.taskSourceEventId ||
        claim.value.evidence.taskSourceEventSequence !== row.taskSourceEventSequence ||
        claim.value.evidence.taskSourceEventStreamVersion !== row.taskSourceEventStreamVersion ||
        claim.value.evidence.worktreeEventId !== row.worktreeEventId ||
        claim.value.evidence.worktreeEventSequence !== row.worktreeEventSequence ||
        claim.value.evidence.worktreeEventStreamVersion !== row.worktreeEventStreamVersion ||
        claim.value.evidence.threadId !== row.threadId ||
        claim.value.evidence.planningThreadId !== row.planningThreadId ||
        claim.value.evidence.planId !== row.planId
      ) {
        return yield* error(handoffId, "replay-handoff", "admission-corrupt");
      }
      const intent = yield* sql<Record<string, unknown>>`
        SELECT command_id AS "commandId", controlled_thread_reservation_id AS
          "controlledThreadReservationId", thread_id AS "threadId", project_id AS "projectId",
          task_id AS "taskId", task_revision AS "taskRevision",
          github_intake_sequence AS "githubIntakeSequence",
          source_identity_fingerprint AS "sourceIdentityFingerprint",
          stage_run_id AS "stageRunId", attempt_id AS "attemptId", role_id AS "roleId",
          stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
          attempt_ordinal AS "attemptOrdinal", lease_id AS "leaseId",
          fence_token AS "fenceToken", worktree_reservation_id AS "worktreeReservationId",
          title, model_selection_json AS "modelSelectionJson", runtime_mode AS "runtimeMode",
          interaction_mode AS "interactionMode", branch, worktree_path AS "worktreePath",
          binding_json AS "bindingJson",
          source_proposed_plan_thread_id AS "sourceThreadId",
          source_proposed_plan_id AS "sourcePlanId", created_at AS "createdAt"
        FROM orchestration_agent_control_thread_materialization_intents
        WHERE command_id = ${row.materializationCommandId}
      `;
      if (intent.length !== 1) {
        return yield* error(handoffId, "replay-orchestration-intent", "admission-corrupt");
      }
      const stored = intent[0]!;
      const [modelSelection, binding] = yield* Effect.all([
        decodeModelSelectionJson(stored.modelSelectionJson),
        decodeBindingJson(stored.bindingJson),
      ]).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-orchestration-json", "admission-corrupt", cause),
        ),
      );
      const command = yield* decodeMaterializationCommand({
        type: "thread.agent-control.materialize",
        ...stored,
        modelSelection,
        binding,
        sourceProposedPlan: {
          threadId: stored.sourceThreadId,
          planId: stored.sourcePlanId,
        },
      }).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-orchestration-command", "admission-corrupt", cause),
        ),
      );
      const replayedCommandFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
        crypto,
        command,
      ).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-command-fingerprint", "persistence", cause),
        ),
      );
      const expectedMaterializationFingerprint = materializationFingerprint([
        row.admissionEvidenceId,
        row.admissionFingerprint,
        row.materializationCommandId,
        replayedCommandFingerprint,
        row.coordinatorCommandId,
        row.coordinatorCommandFingerprint,
        row.controlledThreadReservationId,
        row.threadId,
        row.reservationMaterializingEventId,
        String(row.reservationMaterializingEventSequence),
        row.reservationBoundEventId,
        String(row.reservationBoundEventSequence),
        String(row.orchestrationResultSequence),
        row.planningThreadId,
        row.planId,
        row.proposedPlanDigest,
        row.taskSourceEventId,
        String(row.taskSourceEventSequence),
        String(row.taskSourceEventStreamVersion),
        row.worktreeEventId,
        String(row.worktreeEventSequence),
        String(row.worktreeEventStreamVersion),
        row.modelSelectionFingerprint,
        row.implementationHandoffDigest,
        row.implementationProviderDeliveryDigest,
        row.implementationResultDigest,
        row.verificationAdmissionDigest,
        row.verificationIdentityDigest,
        row.repositoryDisplay,
        row.sourceRevision,
        row.taskTitle,
        row.taskBody ?? "",
        row.committedAt,
      ]);
      if (
        row.materializationFingerprint !== expectedMaterializationFingerprint ||
        claim.value.evidence.proposedPlanDigest !== row.proposedPlanDigest ||
        claim.value.evidence.modelSelectionFingerprint !== row.modelSelectionFingerprint
      ) {
        return yield* error(handoffId, "replay-materialization-fingerprint", "identity-mismatch");
      }
      const replay = orchestration.replayAgentControlMaterialization;
      if (replay === undefined) return yield* error(handoffId, "replay-missing", "persistence");
      const orchestrationResult = yield* replay(command).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-orchestration", "admission-corrupt", cause),
        ),
      );
      if (orchestrationResult.lastSequence !== row.orchestrationResultSequence) {
        return yield* error(handoffId, "replay-orchestration-sequence", "admission-corrupt");
      }
      const acceptedReservation = yield* loadControlledThreadMaterializationReplay(
        command,
        replayedCommandFingerprint,
        reservationEvents,
        reservationStates,
      ).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "replay-reservation", "admission-corrupt", cause),
        ),
      );
      if (
        acceptedReservation.history.length < 3 ||
        acceptedReservation.history[1]?.eventId !== row.reservationMaterializingEventId ||
        acceptedReservation.history[2]?.eventId !== row.reservationBoundEventId ||
        acceptedReservation.history[1]?.sequence !== row.reservationMaterializingEventSequence ||
        acceptedReservation.history[2]?.sequence !== row.reservationBoundEventSequence ||
        acceptedReservation.currentState.leaseHolderId !== claim.value.evidence.leaseHolderId ||
        acceptedReservation.currentState.coordinatorCommandId !== row.coordinatorCommandId ||
        acceptedReservation.currentState.coordinatorCommandFingerprint !==
          row.coordinatorCommandFingerprint ||
        acceptedReservation.currentState.orchestrationResultSequence !==
          row.orchestrationResultSequence
      ) {
        return yield* error(handoffId, "replay-reservation-events", "admission-corrupt");
      }
      return Option.some({
        verificationHandoffId: row.verificationHandoffId,
        threadId: row.threadId,
        orchestrationResult,
        reservationEvents: [acceptedReservation.history[1]!, acceptedReservation.history[2]!],
      });
    },
  );

  const commit = Effect.fn("AgentControlVerificationTurnCoordinator.commit")(function* (
    evidence: AgentControlVerificationAdmissionEvidence,
    runtime: SelectedRuntime,
    guardedWorktree: AgentControlWorktreeReservationState,
  ) {
    const current = yield* validateCurrent(evidence, guardedWorktree, runtime);
    const materializeInTransaction = orchestration.materializeAgentControlInTransaction;
    const completeInTransaction = orchestration.completeAgentControlMaterializationInTransaction;
    if (materializeInTransaction === undefined || completeInTransaction === undefined) {
      return yield* error(evidence.handoffId, "orchestration-boundary", "persistence");
    }
    const reservationId = AgentControlControlledThreadReservationId.make(
      evidence.verificationControlledThreadReservationId,
    );
    const at = DateTime.formatIso(yield* DateTime.now);
    const materializationCommandId = yield* deriveAgentControlThreadMaterializationCommandId(
      current.coordinatorCommandId,
      reservationId,
    );
    const command = yield* decodeMaterializationCommand({
      type: "thread.agent-control.materialize",
      commandId: materializationCommandId,
      controlledThreadReservationId: reservationId,
      threadId: ThreadId.make(evidence.verificationThreadId),
      projectId: ProjectId.make(evidence.projectId),
      taskId: AgentControlTaskId.make(evidence.taskId),
      taskRevision: evidence.taskRevision,
      githubIntakeSequence: evidence.githubIntakeSequence,
      sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
      stageRunId: evidence.verificationStageRunId,
      attemptId: evidence.verificationAttemptId,
      roleId: "verifier",
      stageKind: "verification",
      stageOrdinal: current.stageOrdinal === 5 ? 5 : 3,
      attemptOrdinal: 1,
      leaseId: evidence.leaseId,
      fenceToken: evidence.verificationFenceToken,
      worktreeReservationId: evidence.worktreeReservationId,
      title: current.task.sourceSnapshot.title.trim() || `Verification ${evidence.taskId}`,
      modelSelection: runtime.modelSelection,
      runtimeMode: runtime.runtimeMode,
      interactionMode: "default",
      branch: current.worktree.branchName,
      worktreePath: current.worktree.internalWorktreePath,
      binding: {
        taskId: evidence.taskId,
        stageRunId: evidence.verificationStageRunId,
        attemptId: evidence.verificationAttemptId,
        roleId: "verifier",
        controlState: "controlled",
      },
      sourceProposedPlan: {
        threadId: ThreadId.make(evidence.planningThreadId),
        planId: evidence.planId,
      },
      createdAt: at,
    }).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "build-command", "identity-mismatch", cause),
      ),
    );
    const commandFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
      crypto,
      command,
    ).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "command-fingerprint", "persistence", cause),
      ),
    );
    const coordinatorFingerprint = fingerprintVerificationTurn("coordinator", [
      evidence.admissionFingerprint,
      current.coordinatorCommandId,
      commandFingerprint,
      evidence.leaseHolderId,
      String(evidence.verificationFenceToken),
    ]);
    const materializingCommandId = yield* deriveAgentControlMaterializingTransitionCommandId(
      current.coordinatorCommandId,
      reservationId,
    );
    const boundCommandId = yield* deriveAgentControlBoundTransitionCommandId(
      current.coordinatorCommandId,
      reservationId,
    );
    const prepared = yield* loadAuthoritativeControlledThreadReservation(
      reservationId,
      reservationEvents,
      reservationStates,
    );
    if (Option.isNone(prepared) || prepared.value.status !== "prepared") {
      return yield* error(evidence.handoffId, "prepared-reservation", "reservation-conflict");
    }
    const beginCommand = {
      type: "agentControl.controlledThreadReservation.beginMaterialization",
      commandId: materializingCommandId,
      authority: "controller",
      controlledThreadReservationId: reservationId,
      threadId: command.threadId,
      projectId: command.projectId,
      taskId: command.taskId,
      taskRevision: command.taskRevision,
      githubIntakeSequence: command.githubIntakeSequence,
      sourceIdentityFingerprint: command.sourceIdentityFingerprint,
      stageRunId: command.stageRunId,
      attemptId: command.attemptId,
      roleId: command.roleId,
      stageKind: command.stageKind,
      stageOrdinal: command.stageOrdinal,
      attemptOrdinal: command.attemptOrdinal,
      leaseId: command.leaseId,
      fenceToken: command.fenceToken,
      worktreeReservationId: command.worktreeReservationId,
      expectedRevision: 1,
      coordinatorCommandId: current.coordinatorCommandId,
      coordinatorCommandFingerprint: coordinatorFingerprint,
      materializingTransitionCommandId: materializingCommandId,
      materializationCommandId,
      materializationCommandFingerprint: commandFingerprint,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(evidence.leaseHolderId),
      materializingAt: at,
    } as const;
    const beginDrafts = yield* decideAgentControlControlledThreadReservationCommand({
      state: prepared.value,
      command: beginCommand,
      eventId: EventId.make(
        yield* crypto.randomUUIDv4.pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "materializing-event-id", "persistence", cause),
          ),
        ),
      ),
      occurredAt: at,
    }).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "begin-decision", "reservation-conflict", cause),
      ),
    );
    const materializingEvents = yield* reservationEvents
      .appendInTransaction({
        controlledThreadReservationId: reservationId,
        expectedStreamVersion: 1,
        events: beginDrafts,
      })
      .pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "append-materializing", "reservation-conflict", cause),
        ),
      );
    const materializingEvent = materializingEvents[0];
    if (materializingEvents.length !== 1 || materializingEvent === undefined) {
      return yield* error(evidence.handoffId, "append-materializing", "reservation-conflict");
    }
    yield* reservationProjection
      .projectEventInTransaction(materializingEvent)
      .pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "project-materializing", "persistence", cause),
        ),
      );
    const materializingState = yield* projectAgentControlControlledThreadReservationEvent(
      prepared.value,
      materializingEvent,
    ).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "fold-materializing", "admission-corrupt", cause),
      ),
    );
    yield* hooks.afterMaterializingProjection(observation(evidence));
    const orchestrationResult = yield* materializeInTransaction(command).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "materialize-orchestration", "persistence", cause),
      ),
    );
    if (orchestrationResult.committedEvents.length !== 2) {
      return yield* error(
        evidence.handoffId,
        "materialize-orchestration-events",
        "admission-corrupt",
      );
    }
    yield* hooks.afterOrchestrationMaterialization(observation(evidence));
    const bindDrafts = yield* decideAgentControlControlledThreadReservationCommand({
      state: materializingState,
      command: {
        ...beginCommand,
        type: "agentControl.controlledThreadReservation.bindMaterialization",
        commandId: boundCommandId,
        authority: "controller",
        expectedRevision: 2,
        boundTransitionCommandId: boundCommandId,
        orchestrationResultSequence: orchestrationResult.lastSequence,
        materializedAt: at,
        boundAt: at,
      },
      eventId: EventId.make(
        yield* crypto.randomUUIDv4.pipe(
          Effect.mapError((cause) =>
            error(evidence.handoffId, "bound-event-id", "persistence", cause),
          ),
        ),
      ),
      occurredAt: at,
    }).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "bind-decision", "reservation-conflict", cause),
      ),
    );
    const boundEvents = yield* reservationEvents
      .appendInTransaction({
        controlledThreadReservationId: reservationId,
        expectedStreamVersion: 2,
        events: bindDrafts,
      })
      .pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "append-bound", "reservation-conflict", cause),
        ),
      );
    const boundEvent = boundEvents[0];
    if (boundEvents.length !== 1 || boundEvent === undefined) {
      return yield* error(evidence.handoffId, "append-bound", "reservation-conflict");
    }
    yield* reservationProjection
      .projectEventInTransaction(boundEvent)
      .pipe(
        Effect.mapError((cause) =>
          error(evidence.handoffId, "project-bound", "persistence", cause),
        ),
      );
    yield* hooks.afterBoundProjection(observation(evidence));
    yield* completeInTransaction(orchestrationResult).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "complete-orchestration", "persistence", cause),
      ),
    );

    const materializationEvidenceId = deriveVerificationMaterializationEvidenceId(
      evidence.admissionEvidenceId,
      reservationId,
    );
    const materializationReceiptId = deriveVerificationMaterializationReceiptId(
      evidence.admissionEvidenceId,
      reservationId,
    );
    const materializationMarkerId = deriveVerificationMaterializationMarkerId(
      evidence.admissionEvidenceId,
      reservationId,
    );
    const modelEvidence = canonicalProviderModelSelectionEvidence(runtime.modelSelection);
    const verificationIdentityJson = canonicalJson({
      stageRunId: evidence.verificationStageRunId,
      attemptId: evidence.verificationAttemptId,
      controlledThreadReservationId: evidence.verificationControlledThreadReservationId,
      threadId: evidence.verificationThreadId,
      leaseId: evidence.leaseId,
      leaseHolderId: evidence.leaseHolderId,
      fenceToken: evidence.verificationFenceToken,
      stageKind: "verification",
      roleId: "verifier",
      stageOrdinal: current.stageOrdinal === 5 ? 5 : 3,
      attemptOrdinal: 1,
    });
    const immutablePromptInputs = {
      implementationHandoffJson: evidence.implementationHandoffJson,
      implementationHandoffDigest: sha256Utf8(evidence.implementationHandoffJson),
      implementationProviderDeliveryJson: evidence.implementationProviderDeliveryJson,
      implementationProviderDeliveryDigest: sha256Utf8(evidence.implementationProviderDeliveryJson),
      implementationResultJson: evidence.implementationResultJson,
      implementationResultDigest: sha256Utf8(evidence.implementationResultJson),
      verificationAdmissionJson: evidence.verificationAdmissionJson,
      verificationAdmissionDigest: sha256Utf8(evidence.verificationAdmissionJson),
      verificationIdentityJson,
      verificationIdentityDigest: sha256Utf8(verificationIdentityJson),
    } as const;
    const promptSource = canonicalAgentControlVerificationPromptSource({
      repositoryDisplay: current.worktree.repository.nameWithOwner,
      sourceRevision: current.worktree.baseCommitSha,
      taskTitle: current.task.sourceSnapshot.title,
      taskBody: current.task.sourceSnapshot.body,
    });
    const materializationFingerprintValue = materializationFingerprint([
      evidence.admissionEvidenceId,
      evidence.admissionFingerprint,
      command.commandId,
      commandFingerprint,
      current.coordinatorCommandId,
      coordinatorFingerprint,
      reservationId,
      command.threadId,
      materializingEvent.eventId,
      String(materializingEvent.sequence),
      boundEvent.eventId,
      String(boundEvent.sequence),
      String(orchestrationResult.lastSequence),
      evidence.planningThreadId,
      evidence.planId,
      evidence.proposedPlanDigest,
      current.taskSourceEvent.eventId,
      String(current.taskSourceEvent.sequence),
      String(current.taskSourceEvent.streamVersion),
      current.worktreeEvent.eventId,
      String(current.worktreeEvent.sequence),
      String(current.worktreeEvent.streamVersion),
      modelEvidence.modelSelectionFingerprint,
      immutablePromptInputs.implementationHandoffDigest,
      immutablePromptInputs.implementationProviderDeliveryDigest,
      immutablePromptInputs.implementationResultDigest,
      immutablePromptInputs.verificationAdmissionDigest,
      immutablePromptInputs.verificationIdentityDigest,
      promptSource.repositoryDisplay,
      promptSource.sourceRevision,
      promptSource.taskTitle,
      promptSource.taskBody,
      at,
    ]);
    yield* sql`
      INSERT INTO agent_control_verification_materialization_evidence (
        materialization_evidence_id, materialization_command_id,
        materialization_fingerprint, admission_evidence_id, admission_receipt_id,
        admission_marker_id, admission_fingerprint, admission_marker_fingerprint,
        implementation_result_evidence_id, project_id, task_id, task_revision,
        github_intake_sequence, source_identity_fingerprint, task_source_event_id,
        task_source_event_sequence, task_source_event_stream_version, stage_run_id, attempt_id,
        lease_id, lease_holder_id, fence_token, worktree_reservation_id,
        worktree_revision, worktree_event_id, worktree_event_sequence,
        worktree_event_stream_version, worktree_ownership_fingerprint,
        worktree_verified_at, worktree_path, branch, controlled_thread_reservation_id,
        thread_id, coordinator_command_id, coordinator_command_fingerprint,
        reservation_materializing_event_id, reservation_materializing_event_sequence,
        reservation_bound_event_id, reservation_bound_event_sequence,
        orchestration_created_event_id, orchestration_created_event_sequence,
        orchestration_bound_event_id, orchestration_bound_event_sequence,
        orchestration_result_sequence, planning_thread_id, plan_id, proposed_plan_json,
        proposed_plan_digest, model_selection_json, model_selection_fingerprint,
        implementation_handoff_json, implementation_handoff_digest,
        implementation_provider_delivery_json, implementation_provider_delivery_digest,
        implementation_result_json, implementation_result_digest,
        verification_admission_json, verification_admission_digest,
        verification_identity_json, verification_identity_digest,
        task_history_json, task_history_digest, worktree_history_json, worktree_history_digest,
        stage_history_json, stage_history_digest, lease_history_json, lease_history_digest,
        reservation_history_json, reservation_history_digest,
        orchestration_history_json, orchestration_history_digest,
        provider_instance_id, runtime_mode, interaction_mode, repository_display,
        source_revision, task_title, task_body, materialized_at
      ) VALUES (
        ${materializationEvidenceId}, ${command.commandId},
        ${materializationFingerprintValue}, ${evidence.admissionEvidenceId},
        ${evidence.receiptId}, ${evidence.markerId},
        ${evidence.admissionFingerprint}, ${evidence.markerFingerprint},
        ${evidence.implementationResultEvidenceId}, ${command.projectId}, ${command.taskId},
        ${command.taskRevision}, ${command.githubIntakeSequence},
        ${command.sourceIdentityFingerprint}, ${current.taskSourceEvent.eventId},
        ${current.taskSourceEvent.sequence}, ${current.taskSourceEvent.streamVersion},
        ${command.stageRunId}, ${command.attemptId},
        ${command.leaseId}, ${evidence.leaseHolderId}, ${command.fenceToken},
        ${command.worktreeReservationId}, ${current.worktree.revision},
        ${current.worktreeEvent.eventId}, ${current.worktreeEvent.sequence},
        ${current.worktreeEvent.streamVersion}, ${current.worktree.ownershipFingerprint!},
        ${current.worktree.verifiedAt!}, ${command.worktreePath}, ${command.branch},
        ${reservationId}, ${command.threadId}, ${current.coordinatorCommandId},
        ${coordinatorFingerprint}, ${materializingEvent.eventId},
        ${materializingEvent.sequence}, ${boundEvent.eventId}, ${boundEvent.sequence},
        ${orchestrationResult.committedEvents[0]!.eventId},
        ${orchestrationResult.committedEvents[0]!.sequence},
        ${orchestrationResult.committedEvents[1]!.eventId},
        ${orchestrationResult.committedEvents[1]!.sequence},
        ${orchestrationResult.lastSequence}, ${evidence.planningThreadId}, ${evidence.planId},
        ${evidence.proposedPlanJson}, ${evidence.proposedPlanDigest},
        ${modelEvidence.modelSelectionJson}, ${modelEvidence.modelSelectionFingerprint},
        ${immutablePromptInputs.implementationHandoffJson},
        ${immutablePromptInputs.implementationHandoffDigest},
        ${immutablePromptInputs.implementationProviderDeliveryJson},
        ${immutablePromptInputs.implementationProviderDeliveryDigest},
        ${immutablePromptInputs.implementationResultJson},
        ${immutablePromptInputs.implementationResultDigest},
        ${immutablePromptInputs.verificationAdmissionJson},
        ${immutablePromptInputs.verificationAdmissionDigest},
        ${immutablePromptInputs.verificationIdentityJson},
        ${immutablePromptInputs.verificationIdentityDigest},
        ${evidence.taskHistoryJson}, ${evidence.taskHistoryDigest},
        ${evidence.worktreeHistoryJson}, ${evidence.worktreeHistoryDigest},
        ${evidence.stageHistoryJson}, ${evidence.stageHistoryDigest},
        ${evidence.leaseHistoryJson}, ${evidence.leaseHistoryDigest},
        ${evidence.reservationHistoryJson}, ${evidence.reservationHistoryDigest},
        ${evidence.orchestrationHistoryJson}, ${evidence.orchestrationHistoryDigest},
        ${runtime.modelSelection.instanceId}, ${runtime.runtimeMode}, 'default',
        ${promptSource.repositoryDisplay}, ${promptSource.sourceRevision},
        ${promptSource.taskTitle}, ${promptSource.taskBody}, ${at}
      )
    `;
    yield* sql`
      INSERT INTO agent_control_verification_materialization_receipts (
        materialization_receipt_id, materialization_evidence_id,
        materialization_command_id, materialization_fingerprint,
        controlled_thread_reservation_id, thread_id, status, accepted_at
      ) VALUES (
        ${materializationReceiptId}, ${materializationEvidenceId}, ${command.commandId},
        ${materializationFingerprintValue}, ${reservationId}, ${command.threadId},
        'accepted', ${at}
      )
    `;

    const repair = yield* loadRunOnceRepairForImplementationStage(
      sql,
      evidence.implementationStageRunId,
    ).pipe(
      Effect.mapError((cause) =>
        error(evidence.handoffId, "load-repair-report", "admission-corrupt", cause),
      ),
    );
    const handoffAuthority = {
      ...(Option.isSome(repair) ? { repairReportJson: repair.value.reportJson } : {}),
      materializationEvidenceId,
      materializationReceiptId,
      materializationMarkerId,
      admissionEvidenceId: evidence.admissionEvidenceId,
      admissionReceiptId: evidence.receiptId,
      admissionMarkerId: evidence.markerId,
      projectId: command.projectId,
      taskId: command.taskId,
      taskRevision: command.taskRevision,
      githubIntakeSequence: command.githubIntakeSequence,
      sourceIdentityFingerprint: command.sourceIdentityFingerprint,
      taskSourceEventId: current.taskSourceEvent.eventId,
      taskSourceEventSequence: current.taskSourceEvent.sequence,
      taskSourceEventStreamVersion: current.taskSourceEvent.streamVersion,
      stageRunId: command.stageRunId,
      attemptId: command.attemptId,
      leaseId: command.leaseId,
      leaseHolderId: evidence.leaseHolderId,
      fenceToken: command.fenceToken,
      worktreeReservationId: command.worktreeReservationId,
      worktreeRevision: current.worktree.revision,
      worktreeEventId: current.worktreeEvent.eventId,
      worktreeEventSequence: current.worktreeEvent.sequence,
      worktreeEventStreamVersion: current.worktreeEvent.streamVersion,
      worktreeOwnershipFingerprint: current.worktree.ownershipFingerprint!,
      worktreeVerifiedAt: current.worktree.verifiedAt!,
      worktreePath: current.worktree.internalWorktreePath,
      branch: current.worktree.branchName,
      controlledThreadReservationId: reservationId,
      threadId: command.threadId,
      planningThreadId: ThreadId.make(evidence.planningThreadId),
      planId: evidence.planId,
      proposedPlanJson: evidence.proposedPlanJson,
      proposedPlanDigest: evidence.proposedPlanDigest,
      providerInstanceId: runtime.modelSelection.instanceId,
      runtimeMode: runtime.runtimeMode,
      modelSelection: runtime.modelSelection,
      modelSelectionJson: modelEvidence.modelSelectionJson,
      modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
      ...immutablePromptInputs,
      ...promptSource,
      createdAt: at,
    } as const;
    const handoffEvidence = yield* Effect.try({
      try: () =>
        buildExpectedAgentControlVerificationHandoff(handoffAuthority, hooks.promptTemplateVersion),
      catch: (cause) =>
        error(evidence.handoffId, "verification-handoff", "admission-corrupt", cause),
    });
    const verificationHandoffId = handoffEvidence.handoffId;
    const providerDeliveryId = handoffEvidence.providerDeliveryId;
    yield* handoffStore
      .insertAcceptedInTransaction(handoffEvidence, handoffAuthority)
      .pipe(
        Effect.mapError((cause) => fromStoreError(evidence.handoffId, "insert-handoff", cause)),
      );
    yield* hooks.afterHandoffAccepted(observation(evidence));
    yield* hooks.beforeMaterializationMarker(observation(evidence));
    yield* sql`
      INSERT INTO agent_control_verification_materialization_markers (
        materialization_marker_id, materialization_evidence_id,
        materialization_receipt_id, handoff_id, provider_delivery_id,
        materialization_fingerprint, committed_at
      ) VALUES (
        ${materializationMarkerId}, ${materializationEvidenceId},
        ${materializationReceiptId}, ${verificationHandoffId}, ${providerDeliveryId},
        ${materializationFingerprintValue}, ${at}
      )
    `;
    return {
      verificationHandoffId,
      threadId: command.threadId,
      orchestrationResult,
      reservationEvents: [materializingEvent, boundEvent],
    } satisfies ReplayedMaterialization;
  });

  const finalize = Effect.fn("AgentControlVerificationTurnCoordinator.finalize")(function* (
    committed: ReplayedMaterialization,
    publish: boolean,
  ) {
    const refreshOrchestration = orchestration.refreshAgentControlMaterialization;
    const publishOrchestration = orchestration.publishAgentControlMaterialization;
    if (refreshOrchestration === undefined || publishOrchestration === undefined) {
      return yield* error(committed.verificationHandoffId, "orchestration-finalize", "persistence");
    }
    yield* reservationEngine
      .refreshCommitted(committed.reservationEvents)
      .pipe(
        Effect.mapError((cause) =>
          error(committed.verificationHandoffId, "reservation-refresh", "persistence", cause),
        ),
      );
    yield* refreshOrchestration(committed.orchestrationResult).pipe(
      Effect.mapError((cause) =>
        error(committed.verificationHandoffId, "orchestration-refresh", "persistence", cause),
      ),
    );
    if (publish) {
      yield* reservationEngine.publishCommitted(committed.reservationEvents);
      yield* publishOrchestration(committed.orchestrationResult).pipe(
        Effect.mapError((cause) =>
          error(committed.verificationHandoffId, "orchestration-publish", "persistence", cause),
        ),
      );
      yield* wakeup.wake(committed.verificationHandoffId);
    }
  });

  const processHandoffUnchecked = Effect.fn(
    "AgentControlVerificationTurnCoordinator.processHandoff",
  )(function* (handoffId: string) {
    const earlyReplay = yield* replayAccepted(handoffId);
    if (Option.isSome(earlyReplay)) {
      yield* finalize(earlyReplay.value, false).pipe(Effect.uninterruptible);
      return {
        _tag: "Replayed",
        verificationHandoffId: earlyReplay.value.verificationHandoffId,
        threadId: earlyReplay.value.threadId,
      } satisfies AgentControlVerificationTurnMaterializationResult;
    }
    const admitted = yield* admission
      .processResultEvidence(handoffId)
      .pipe(
        Effect.mapError((cause) =>
          error(handoffId, "verification-admission", "admission-corrupt", cause),
        ),
      );
    if (admitted._tag === "NotCandidate") return { _tag: "NotCandidate" } as const;
    const loadAccepted = admission.loadAcceptedEvidence;
    if (loadAccepted === undefined) {
      return yield* error(handoffId, "load-admission-boundary", "persistence");
    }
    const accepted = yield* loadAccepted(handoffId).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "load-admission-boundary", "admission-corrupt", cause),
      ),
    );
    if (Option.isNone(accepted)) {
      return yield* error(handoffId, "load-admission-boundary", "admission-corrupt");
    }
    const evidence = accepted.value;
    yield* hooks.afterAdmissionReplay(observation(evidence));
    const replayAfterAdmission = yield* replayAccepted(handoffId);
    if (Option.isSome(replayAfterAdmission)) {
      yield* finalize(replayAfterAdmission.value, false).pipe(Effect.uninterruptible);
      return {
        _tag: "Replayed",
        verificationHandoffId: replayAfterAdmission.value.verificationHandoffId,
        threadId: replayAfterAdmission.value.threadId,
      } satisfies AgentControlVerificationTurnMaterializationResult;
    }
    const runtime = yield* resolveRuntime(evidence);
    const projectId = ProjectId.make(evidence.projectId);
    const reservationId = AgentControlWorktreeReservationId.make(evidence.worktreeReservationId);
    const guarded = yield* worktreeController
      .useReadyWorktree(
        { projectId, reservationId },
        (worktree) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const transactionExit = yield* Effect.exit(
                restore(sql.withTransaction(commit(evidence, runtime, worktree))),
              );
              if (Exit.isFailure(transactionExit)) {
                const recovered = yield* Effect.exit(
                  sql.withTransaction(replayAccepted(handoffId)),
                );
                if (Exit.isSuccess(recovered) && Option.isSome(recovered.value)) {
                  yield* finalize(recovered.value.value, false);
                  return {
                    _tag: "Replayed",
                    value: recovered.value.value,
                  } satisfies GuardedMaterialization;
                }
                return yield* Effect.failCause(
                  Exit.isFailure(recovered)
                    ? Cause.combine(transactionExit.cause, recovered.cause)
                    : transactionExit.cause,
                );
              }
              const committed = transactionExit.value;
              yield* hooks.afterOuterCommit(observation(evidence));
              yield* finalize(committed, true);
              yield* PubSub.publish(publications, {
                handoffId,
                verificationHandoffId: committed.verificationHandoffId,
                threadId: committed.threadId,
                reservationEvents: committed.reservationEvents,
                orchestrationEvents: committed.orchestrationResult.committedEvents,
              });
              yield* hooks.afterPublication(observation(evidence));
              return { _tag: "Committed", value: committed } satisfies GuardedMaterialization;
            }),
          ),
        {
          beforeInspection: replayAccepted(handoffId).pipe(
            Effect.map(
              Option.map((value): GuardedMaterialization => ({ _tag: "Replayed", value })),
            ),
          ),
        },
      )
      .pipe(
        Effect.mapError((cause) =>
          isCoordinatorError(cause)
            ? cause
            : error(handoffId, "guard-worktree", "source-stale", cause),
        ),
      );
    if (guarded._tag === "Replayed") {
      yield* finalize(guarded.value, false).pipe(Effect.uninterruptible);
      return {
        _tag: "Replayed",
        verificationHandoffId: guarded.value.verificationHandoffId,
        threadId: guarded.value.threadId,
      } satisfies AgentControlVerificationTurnMaterializationResult;
    }
    return {
      _tag: "Materialized",
      publication: {
        handoffId,
        verificationHandoffId: guarded.value.verificationHandoffId,
        threadId: guarded.value.threadId,
        reservationEvents: guarded.value.reservationEvents,
        orchestrationEvents: guarded.value.orchestrationResult.committedEvents,
      },
    } satisfies AgentControlVerificationTurnMaterializationResult;
  });

  const processHandoff: AgentControlVerificationTurnCoordinatorShape["processHandoff"] = (
    handoffId,
  ) =>
    processHandoffUnchecked(handoffId).pipe(
      Effect.mapError((cause) =>
        isCoordinatorError(cause)
          ? cause
          : error(handoffId, "process-handoff", "persistence", cause),
      ),
    );

  const recover = Effect.gen(function* () {
    const pageSize = 64;
    let cursor = "";
    while (true) {
      const candidates = yield* sql<{ readonly handoffId: string }>`
        SELECT implementation_result_evidence_id AS "handoffId"
        FROM agent_control_verification_admission_markers
        WHERE implementation_result_evidence_id > ${cursor}
        ORDER BY implementation_result_evidence_id
        LIMIT ${pageSize}
      `.pipe(
        Effect.mapError((cause) => error("recovery", "list-candidates", "persistence", cause)),
      );
      if (candidates.length === 0) break;
      yield* Effect.forEach(
        candidates,
        ({ handoffId }) =>
          processHandoff(handoffId).pipe(
            Effect.catchIf(
              (cause) => cause.reason !== "persistence" && cause.reason !== "reservation-conflict",
              (cause) =>
                Effect.logError("verification materialization candidate failed", {
                  handoffId,
                  operation: cause.operation,
                  reason: cause.reason,
                  ...(cause.cause === undefined ? {} : { errorClass: "redacted-candidate-cause" }),
                }),
            ),
          ),
        { concurrency: 1, discard: true },
      );
      cursor = candidates.at(-1)!.handoffId;
      if (candidates.length < pageSize) break;
    }
  });
  const processSafely = (handoffId: string | null) =>
    handoffId === null
      ? recover
      : processHandoff(handoffId).pipe(
          Effect.asVoid,
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "reservation-conflict",
            (cause) =>
              Effect.logError("verification materialization candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
                ...(cause.cause === undefined ? {} : { errorClass: "redacted-candidate-cause" }),
              }),
          ),
        );
  let activeWorker:
    | {
        readonly drain: Effect.Effect<void, AgentControlVerificationTurnCoordinatorError>;
      }
    | undefined;
  const prepare: AgentControlVerificationTurnCoordinatorShape["prepare"] = Effect.fn(
    "AgentControlVerificationTurnCoordinator.prepare",
  )(function* (activation) {
    const worker = yield* makeDrainableWorker(processSafely, { failureMode: "observable" });
    activeWorker = { drain: worker.drain };
    const admissionPublications = yield* admission.subscribePublications;
    yield* Effect.forkScoped(
      Stream.runForEach(admissionPublications, (publication) =>
        activation.pipe(Effect.andThen(worker.enqueue(publication.implementationResultEvidenceId))),
      ),
      { startImmediately: true },
    );
    yield* Effect.forkScoped(activation.pipe(Effect.andThen(worker.enqueue(null))), {
      startImmediately: true,
    });
  });
  const start = Effect.fn("AgentControlVerificationTurnCoordinator.start")(function* () {
    yield* prepare(Effect.void);
  });

  return AgentControlVerificationTurnCoordinator.of({
    processHandoff,
    recover,
    prepare,
    start,
    drain: Effect.suspend(() => activeWorker?.drain ?? Effect.void),
    streamPublications: Stream.fromPubSub(publications),
  });
});

export const AgentControlVerificationTurnCoordinatorLive = Layer.effect(
  AgentControlVerificationTurnCoordinator,
  make,
).pipe(Layer.provideMerge(AgentControlVerificationHandoffStoreLive));
