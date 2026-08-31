import {
  AgentControlThreadBinding,
  AgentControlThreadMaterializeCommand,
  EventId,
  ModelSelection,
  type AgentControlRunOnceId,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationState,
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
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPolicyService } from "../../AgentControlPolicyService.ts";
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageEventId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestEventId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../../initialPlanning/identity.ts";
import {
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
  initialPlanningMessagePayload,
  initialPlanningTurnRequestPayload,
} from "../../initialPlanning/eventEvidence.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningHandoffStore.ts";
import {
  AGENT_CONTROL_INITIAL_PLANNING_PROMPT_TEMPLATE_VERSION,
  buildAgentControlInitialPlanningPrompt,
  deriveAgentControlRepositoryDisplay,
} from "../../initialPlanning/prompt.ts";
import { AgentControlInitialPlanningHandoffStore } from "../../initialPlanning/Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeup } from "../../initialPlanning/Services/AgentControlInitialPlanningWakeup.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseHistoryForStagePosition,
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlRunOnceExecutionContext } from "../../runOnce/context.ts";
import {
  OrchestrationEngineService,
  type AgentControlThreadMaterializationTransactionResult,
} from "../../../orchestration/Services/OrchestrationEngine.ts";
import { encodeAgentControlThreadBindingStorage } from "../../../orchestration/agentControlThreadBindingStorage.ts";
import { fingerprintAgentControlThreadMaterializationCommand } from "../../../orchestration/agentControlThreadMaterializationIntent.ts";
import {
  loadAuthoritativeControlledThreadReservation,
  loadAuthoritativeControlledThreadReservationTaskHistory,
} from "../authoritative.ts";
import { decideAgentControlControlledThreadReservationCommand } from "../decider.ts";
import {
  deriveAgentControlBoundTransitionCommandId,
  deriveAgentControlControlledThreadActivationCommandId,
  deriveAgentControlMaterializingTransitionCommandId,
  deriveAgentControlThreadMaterializationCommandId,
  sha256AgentControlIdentity,
} from "../identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../projector.ts";
import {
  deriveAgentControlControlledThreadCoordinatorFingerprint,
  deriveAgentControlControlledThreadCoordinatorRequestFingerprint,
} from "../successorEvidence.ts";
import {
  AgentControlControlledThreadMaterializationCoordinator,
  AgentControlControlledThreadMaterializationCoordinatorError,
  type AgentControlControlledThreadMaterializationCoordinatorShape,
  type AgentControlControlledThreadMaterializeInitialInput,
  type AgentControlControlledThreadMaterializeInitialResult,
} from "../Services/AgentControlControlledThreadMaterializationCoordinator.ts";
import { AgentControlControlledThreadMaterializationCoordinatorHooks } from "../Services/AgentControlControlledThreadMaterializationCoordinatorHooks.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";

interface ResolvedAuthority {
  readonly reservation: Extract<
    AgentControlControlledThreadReservationState,
    { readonly status: "prepared" }
  >;
  readonly task: AgentControlTaskState;
  readonly leaseHolderId: string;
  readonly worktree: AgentControlWorktreeReservationState;
  readonly title: string;
}

interface ProjectPolicyBinding {
  readonly revision: number | null;
  readonly policyJson: string | null;
  readonly fingerprint: string;
}

interface ResolvedRuntime {
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: "approval-required" | "full-access";
  readonly policyBinding: ProjectPolicyBinding;
  readonly runtimeObservationFingerprint: string;
}

interface ResolvedMaterialization extends ResolvedAuthority, ResolvedRuntime {}

interface CoordinatorEvidenceRow {
  readonly coordinatorCommandId: string;
  readonly intentFinalizationOwnerId: string;
  readonly requestFingerprint: string;
  readonly coordinatorCommandFingerprint: string;
  readonly policyBindingFingerprint: string;
  readonly runtimeObservationFingerprint: string;
  readonly projectId: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly roleId: string;
  readonly stageKind: string;
  readonly stageOrdinal: number;
  readonly attemptOrdinal: number;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly materializingTransitionCommandId: string;
  readonly boundTransitionCommandId: string;
  readonly materializationCommandId: string;
  readonly materializationCommandFingerprint: string;
  readonly title: string;
  readonly modelSelectionJson: string;
  readonly runtimeMode: string;
  readonly interactionMode: string;
  readonly branch: string;
  readonly worktreePath: string;
  readonly bindingJson: string;
  readonly materializingEventId: string;
  readonly materializingEventSequence: number;
  readonly boundEventId: string;
  readonly boundEventSequence: number;
  readonly orchestrationResultSequence: number;
  readonly materializingAt: string;
  readonly materializedAt: string;
  readonly boundAt: string;
  readonly acceptedAt: string;
  readonly intentAcceptedMarkerCommandId: string;
  readonly receiptRequestFingerprint: string;
  readonly receiptCoordinatorCommandFingerprint: string;
  readonly receiptControlledThreadReservationId: string;
  readonly receiptThreadId: string;
  readonly receiptMaterializationCommandId: string;
  readonly receiptMaterializationCommandFingerprint: string;
  readonly receiptOrchestrationResultSequence: number;
  readonly receiptStatus: string;
  readonly receiptAcceptedAt: string;
  readonly receiptAcceptedMarkerCommandId: string;
  readonly markerCoordinatorCommandFingerprint: string;
  readonly markerControlledThreadReservationId: string;
  readonly markerThreadId: string;
  readonly markerMaterializationCommandId: string;
  readonly markerMaterializationCommandFingerprint: string;
  readonly markerOrchestrationResultSequence: number;
  readonly markerAcceptedAt: string;
  readonly markerFinalizationOwnerId: string;
}

type CoordinatorGuardedOutcome =
  | {
      readonly _tag: "Replayed";
      readonly replayed: ReplayedAccepted;
    }
  | {
      readonly _tag: "Committed";
      readonly committed: ReplayedAccepted;
    };

interface ReplayedAccepted {
  readonly result: AgentControlControlledThreadMaterializeInitialResult;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
  readonly orchestrationResult: AgentControlThreadMaterializationTransactionResult;
  readonly finalizationOwnerId: string;
  readonly handoffId: string | null;
}

const isCoordinatorError = Schema.is(AgentControlControlledThreadMaterializationCoordinatorError);
const decodeMaterializationCommand = Schema.decodeUnknownEffect(
  AgentControlThreadMaterializeCommand,
);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const decodeBindingJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlThreadBinding),
);
const finalizationOwnerIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const encodeModelSelectionJson = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
  const reservationEvents = yield* AgentControlControlledThreadReservationEventStore;
  const reservationStates = yield* AgentControlControlledThreadReservationStateRepository;
  const reservationProjection = yield* AgentControlControlledThreadReservationProjection;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const worktreeController = yield* AgentControlWorktreeController;
  const policy = yield* AgentControlPolicyService;
  const orchestration = yield* OrchestrationEngineService;
  const hooks = yield* AgentControlControlledThreadMaterializationCoordinatorHooks;
  const initialPlanningStore = yield* AgentControlInitialPlanningHandoffStore;
  const initialPlanningWakeup = yield* AgentControlInitialPlanningWakeup;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;

  const error = (
    reason: AgentControlControlledThreadMaterializationCoordinatorError["reason"],
    input: AgentControlControlledThreadMaterializeInitialInput,
    cause?: unknown,
  ) =>
    new AgentControlControlledThreadMaterializationCoordinatorError({
      reason,
      commandId: input.commandId,
      projectId: input.projectId,
      controlledThreadReservationId: input.controlledThreadReservationId,
      ...(cause === undefined ? {} : { cause }),
    });

  const observation = (
    input: AgentControlControlledThreadMaterializeInitialInput,
    threadId: AgentControlControlledThreadReservationState["threadId"] | null,
  ) => ({
    coordinatorCommandId: input.commandId,
    controlledThreadReservationId: input.controlledThreadReservationId,
    threadId,
  });

  const loadProjectPolicyBinding = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.loadProjectPolicyBinding",
  )(function* (input: AgentControlControlledThreadMaterializeInitialInput) {
    const rows = yield* sql<{
      readonly revision: number | null;
      readonly policyJson: string | null;
    }>`
      SELECT policy.revision, policy.policy_json AS "policyJson"
      FROM projection_projects project
      LEFT JOIN agent_control_project_policies policy
        ON policy.project_id = project.project_id
      WHERE project.project_id = ${input.projectId}
        AND project.deleted_at IS NULL
    `.pipe(Effect.mapError((cause) => error("internal-persistence-error", input, cause)));
    if (rows.length !== 1) return yield* error("project-unavailable", input);
    const row = rows[0]!;
    if (
      (row.revision === null) !== (row.policyJson === null) ||
      (row.revision !== null && (!Number.isInteger(row.revision) || row.revision < 1))
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    return {
      revision: row.revision,
      policyJson: row.policyJson,
      fingerprint: sha256AgentControlIdentity([
        "agent-control-controlled-thread-policy-binding-v1",
        input.projectId,
        row.revision === null ? "none" : String(row.revision),
        row.policyJson ?? "none",
      ]),
    } satisfies ProjectPolicyBinding;
  });

  const sameProjectPolicyBinding = (
    left: ProjectPolicyBinding,
    right: ProjectPolicyBinding,
  ): boolean =>
    left.revision === right.revision &&
    left.policyJson === right.policyJson &&
    left.fingerprint === right.fingerprint;

  const sameWorktree = (
    left: AgentControlWorktreeReservationState,
    right: AgentControlWorktreeReservationState,
  ) =>
    left.reservationId === right.reservationId &&
    left.projectId === right.projectId &&
    left.taskId === right.taskId &&
    left.taskRevision === right.taskRevision &&
    left.githubIntakeSequence === right.githubIntakeSequence &&
    left.sourceIdentityFingerprint === right.sourceIdentityFingerprint &&
    left.stageRunId === right.stageRunId &&
    left.attemptId === right.attemptId &&
    left.leaseId === right.leaseId &&
    left.fenceToken === right.fenceToken &&
    left.status === "ready" &&
    right.status === "ready" &&
    left.branchName === right.branchName &&
    left.internalWorktreePath === right.internalWorktreePath &&
    left.ownershipFingerprint === right.ownershipFingerprint &&
    left.verifiedAt === right.verifiedAt;

  const resolveRuntime = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.resolveRuntime",
  )(function* (
    input: AgentControlControlledThreadMaterializeInitialInput,
    policyBinding: ProjectPolicyBinding,
  ) {
    const preflight = yield* policy
      .preflightRuntime({ projectId: input.projectId })
      .pipe(Effect.mapError(() => error("runtime-policy-unavailable", input)));
    const runtimeRole = preflight.roles.find((role) => role.role === "planner");
    const staticRole = preflight.staticPreflight.roles.find((role) => role.role === "planner");
    if (
      !preflight.ok ||
      runtimeRole === undefined ||
      staticRole === undefined ||
      runtimeRole.selectedCandidateIndex === null
    ) {
      return yield* error("runtime-policy-unavailable", input);
    }
    const modelSelection =
      staticRole.validCandidates[runtimeRole.selectedCandidateIndex]?.selection;
    const selectedRuntime = runtimeRole.candidates.find(
      (candidate) => candidate.candidateIndex === runtimeRole.selectedCandidateIndex,
    );
    if (
      modelSelection === undefined ||
      selectedRuntime === undefined ||
      !selectedRuntime.runtimeReady ||
      selectedRuntime.providerInstanceId !== modelSelection.instanceId ||
      selectedRuntime.model !== modelSelection.model
    ) {
      return yield* error("runtime-policy-unavailable", input);
    }
    const modelSelectionJson = yield* encodeModelSelectionJson(modelSelection).pipe(
      Effect.mapError(() => error("runtime-policy-unavailable", input)),
    );
    return {
      modelSelection,
      runtimeMode:
        runtimeRole.accessMode === "full-access"
          ? ("full-access" as const)
          : ("approval-required" as const),
      policyBinding,
      runtimeObservationFingerprint: sha256AgentControlIdentity([
        "agent-control-controlled-thread-runtime-observation-v1",
        policyBinding.fingerprint,
        runtimeRole.role,
        runtimeRole.accessMode,
        runtimeRole.strict ? "strict" : "non-strict",
        String(runtimeRole.selectedCandidateIndex),
        selectedRuntime.source,
        selectedRuntime.providerInstanceId,
        selectedRuntime.model,
        selectedRuntime.driverKind ?? "none",
        selectedRuntime.providerStatus ?? "none",
        selectedRuntime.authStatus ?? "none",
        selectedRuntime.checkedAt ?? "none",
        modelSelectionJson,
      ]),
    } satisfies ResolvedRuntime;
  });

  const resolveCurrent = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.resolveCurrent",
  )(function* (input: AgentControlControlledThreadMaterializeInitialInput, inTransaction: boolean) {
    const loadedReservation = yield* loadAuthoritativeControlledThreadReservation(
      input.controlledThreadReservationId,
      reservationEvents,
      reservationStates,
    ).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    if (Option.isNone(loadedReservation)) {
      return yield* error("reservation-missing", input);
    }
    const reservation = loadedReservation.value;
    const prepareHistory = yield* reservationEvents
      .readStream(input.controlledThreadReservationId, 0, 1)
      .pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    if (
      prepareHistory.length !== 1 ||
      prepareHistory[0]?.type !== "agentControl.controlledThreadReservation.prepared" ||
      (yield* deriveAgentControlControlledThreadActivationCommandId(
        prepareHistory[0].commandId,
        input.controlledThreadReservationId,
      )) !== input.commandId
    ) {
      return yield* error("command-identity-conflict", input);
    }
    if (reservation.projectId !== input.projectId) {
      return yield* error("command-identity-conflict", input);
    }
    if (reservation.status !== "prepared") {
      return yield* error("reservation-not-prepared", input);
    }
    const taskHistory = yield* loadAuthoritativeControlledThreadReservationTaskHistory(
      reservation.projectId,
      reservation.taskId,
      reservationEvents,
      reservationStates,
    ).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    if (
      taskHistory.length !== 1 ||
      taskHistory[0]?.controlledThreadReservationId !== reservation.controlledThreadReservationId
    ) {
      return yield* error("reservation-conflict", input);
    }

    const runId = yield* AgentControlRunOnceExecutionContext;
    const selectedRunOnceTask = inTransaction
      ? taskGuard.useTaskSelectedForRunOnceInTransaction
      : taskGuard.useTaskSelectedForRunOnce;
    if (runId !== null && selectedRunOnceTask === undefined) {
      return yield* error("internal-persistence-error", input);
    }
    const useTask: typeof taskGuard.useTaskConsumable =
      runId === null
        ? inTransaction && taskGuard.useTaskConsumableInTransaction !== undefined
          ? taskGuard.useTaskConsumableInTransaction
          : taskGuard.useTaskConsumable
        : (projectId, taskId, use) => selectedRunOnceTask!(runId, projectId, taskId, use);
    return yield* useTask(reservation.projectId, reservation.taskId, (task) =>
      Effect.gen(function* () {
        const sourceIdentityFingerprint = yield* deriveAgentControlSourceIdentityFingerprint(task);
        if (
          task.revision !== reservation.taskRevision ||
          task.githubIntakeSequence !== reservation.githubIntakeSequence ||
          sourceIdentityFingerprint !== reservation.sourceIdentityFingerprint
        ) {
          return yield* error("source-snapshot-stale", input);
        }
        const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
          reservation.projectId,
          reservation.taskId,
          stageEvents,
          stageStates,
        ).pipe(Effect.mapError(() => error("stage-run-unavailable", input)));
        const stageMatches = stageHistory.filter(
          (stage) =>
            stage.stageRunId === reservation.stageRunId &&
            stage.attemptId === reservation.attemptId &&
            stage.taskRevision === reservation.taskRevision &&
            stage.githubIntakeSequence === reservation.githubIntakeSequence &&
            stage.sourceIdentityFingerprint === reservation.sourceIdentityFingerprint &&
            stage.stageKind === "planning" &&
            stage.roleId === "planning" &&
            stage.stageOrdinal === 1 &&
            stage.attemptOrdinal === 1,
        );
        if (stageMatches.length !== 1 || stageMatches[0]?.status !== "prepared") {
          return yield* error("stage-run-unavailable", input);
        }
        const leases = yield* loadAuthoritativeLeaseHistoryForStagePosition(
          {
            projectId: reservation.projectId,
            taskId: reservation.taskId,
            stageRunId: reservation.stageRunId,
            attemptId: reservation.attemptId,
            taskRevision: reservation.taskRevision,
            githubIntakeSequence: reservation.githubIntakeSequence,
            sourceIdentityFingerprint: reservation.sourceIdentityFingerprint,
          },
          leaseEvents,
          leaseStates,
        ).pipe(Effect.mapError(() => error("lease-unavailable", input)));
        if (leases.length !== 1) return yield* error("lease-unavailable", input);
        const lease = leases[0]!;
        if (
          lease.status !== "reserved" ||
          lease.leaseId !== reservation.leaseId ||
          lease.fenceToken !== reservation.fenceToken
        ) {
          return yield* error("lease-unavailable", input);
        }
        if (lease.holderId !== runtimeHolderId) {
          return yield* error("lease-foreign-runtime", input);
        }
        const expiration = canonicalTimestampMillis(lease.expiresAt);
        if (expiration === null || expiration <= DateTime.toEpochMillis(yield* DateTime.now)) {
          return yield* error("lease-expired", input);
        }
        const worktree = yield* worktreeEngine
          .loadAuthoritative(reservation.worktreeReservationId)
          .pipe(Effect.mapError(() => error("worktree-unavailable", input)));
        if (
          worktree === null ||
          worktree.status !== "ready" ||
          worktree.projectId !== reservation.projectId ||
          worktree.taskId !== reservation.taskId ||
          worktree.taskRevision !== reservation.taskRevision ||
          worktree.githubIntakeSequence !== reservation.githubIntakeSequence ||
          worktree.sourceIdentityFingerprint !== reservation.sourceIdentityFingerprint ||
          worktree.stageRunId !== reservation.stageRunId ||
          worktree.attemptId !== reservation.attemptId ||
          worktree.leaseId !== reservation.leaseId ||
          worktree.fenceToken !== reservation.fenceToken ||
          worktree.verifiedAt === null ||
          worktree.ownershipFingerprint === null
        ) {
          return yield* error("worktree-unavailable", input);
        }
        const trimmedTitle = task.sourceSnapshot.title.trim();
        return {
          reservation,
          task,
          leaseHolderId: lease.holderId,
          worktree,
          title:
            trimmedTitle.length > 0 ? trimmedTitle : `Planning task ${task.source.issueNumber}`,
        } satisfies ResolvedAuthority;
      }),
    ).pipe(
      Effect.mapError((cause) =>
        isCoordinatorError(cause)
          ? cause
          : error(
              cause._tag === "AgentControlTaskConsumerGuardError"
                ? cause.reason === "mode-inactive"
                  ? "project-mode-inactive"
                  : cause.reason === "project-unavailable"
                    ? "project-unavailable"
                    : "task-unavailable"
                : "internal-persistence-error",
              input,
            ),
      ),
    );
  });

  const commandFromEvidence = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.commandFromEvidence",
  )(function* (
    row: CoordinatorEvidenceRow,
    input: AgentControlControlledThreadMaterializeInitialInput,
  ) {
    const [modelSelection, binding] = yield* Effect.all([
      decodeModelSelectionJson(row.modelSelectionJson),
      decodeBindingJson(row.bindingJson),
    ]).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    return yield* decodeMaterializationCommand({
      type: "thread.agent-control.materialize",
      commandId: row.materializationCommandId,
      controlledThreadReservationId: row.controlledThreadReservationId,
      threadId: row.threadId,
      projectId: row.projectId,
      taskId: row.taskId,
      taskRevision: row.taskRevision,
      githubIntakeSequence: row.githubIntakeSequence,
      sourceIdentityFingerprint: row.sourceIdentityFingerprint,
      stageRunId: row.stageRunId,
      attemptId: row.attemptId,
      roleId: row.roleId,
      stageKind: row.stageKind,
      stageOrdinal: row.stageOrdinal,
      attemptOrdinal: row.attemptOrdinal,
      leaseId: row.leaseId,
      fenceToken: row.fenceToken,
      worktreeReservationId: row.worktreeReservationId,
      title: row.title,
      modelSelection,
      runtimeMode: row.runtimeMode,
      interactionMode: row.interactionMode,
      branch: row.branch,
      worktreePath: row.worktreePath,
      binding,
      createdAt: row.materializedAt,
    }).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
  });

  const loadEvidence = (input: AgentControlControlledThreadMaterializeInitialInput) =>
    sql<CoordinatorEvidenceRow>`
      SELECT
        intent.coordinator_command_id AS "coordinatorCommandId",
        intent.finalization_owner_id AS "intentFinalizationOwnerId",
        intent.request_fingerprint AS "requestFingerprint",
        intent.coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        intent.policy_binding_fingerprint AS "policyBindingFingerprint",
        intent.runtime_observation_fingerprint AS "runtimeObservationFingerprint",
        intent.project_id AS "projectId",
        intent.controlled_thread_reservation_id AS "controlledThreadReservationId",
        intent.thread_id AS "threadId",
        intent.task_id AS "taskId",
        intent.task_revision AS "taskRevision",
        intent.github_intake_sequence AS "githubIntakeSequence",
        intent.source_identity_fingerprint AS "sourceIdentityFingerprint",
        intent.stage_run_id AS "stageRunId",
        intent.attempt_id AS "attemptId",
        intent.role_id AS "roleId",
        intent.stage_kind AS "stageKind",
        intent.stage_ordinal AS "stageOrdinal",
        intent.attempt_ordinal AS "attemptOrdinal",
        intent.lease_id AS "leaseId",
        intent.lease_holder_id AS "leaseHolderId",
        intent.fence_token AS "fenceToken",
        intent.worktree_reservation_id AS "worktreeReservationId",
        intent.materializing_transition_command_id AS
          "materializingTransitionCommandId",
        intent.bound_transition_command_id AS "boundTransitionCommandId",
        intent.materialization_command_id AS "materializationCommandId",
        intent.materialization_command_fingerprint AS
          "materializationCommandFingerprint",
        intent.title,
        intent.model_selection_json AS "modelSelectionJson",
        intent.runtime_mode AS "runtimeMode",
        intent.interaction_mode AS "interactionMode",
        intent.branch,
        intent.worktree_path AS "worktreePath",
        intent.binding_json AS "bindingJson",
        intent.materializing_event_id AS "materializingEventId",
        intent.materializing_event_sequence AS "materializingEventSequence",
        intent.bound_event_id AS "boundEventId",
        intent.bound_event_sequence AS "boundEventSequence",
        intent.orchestration_result_sequence AS "orchestrationResultSequence",
        intent.materializing_at AS "materializingAt",
        intent.materialized_at AS "materializedAt",
        intent.bound_at AS "boundAt",
        intent.accepted_at AS "acceptedAt",
        intent.accepted_marker_command_id AS "intentAcceptedMarkerCommandId",
        receipt.request_fingerprint AS "receiptRequestFingerprint",
        receipt.coordinator_command_fingerprint AS
          "receiptCoordinatorCommandFingerprint",
        receipt.controlled_thread_reservation_id AS
          "receiptControlledThreadReservationId",
        receipt.thread_id AS "receiptThreadId",
        receipt.materialization_command_id AS "receiptMaterializationCommandId",
        receipt.materialization_command_fingerprint AS
          "receiptMaterializationCommandFingerprint",
        receipt.orchestration_result_sequence AS
          "receiptOrchestrationResultSequence",
        receipt.status AS "receiptStatus",
        receipt.accepted_at AS "receiptAcceptedAt",
        receipt.accepted_marker_command_id AS
          "receiptAcceptedMarkerCommandId",
        accepted.coordinator_command_fingerprint AS
          "markerCoordinatorCommandFingerprint",
        accepted.controlled_thread_reservation_id AS
          "markerControlledThreadReservationId",
        accepted.thread_id AS "markerThreadId",
        accepted.materialization_command_id AS "markerMaterializationCommandId",
        accepted.materialization_command_fingerprint AS
          "markerMaterializationCommandFingerprint",
        accepted.orchestration_result_sequence AS
          "markerOrchestrationResultSequence",
        accepted.accepted_at AS "markerAcceptedAt",
        accepted.finalization_owner_id AS "markerFinalizationOwnerId"
      FROM agent_control_controlled_thread_materialization_intents intent
      JOIN agent_control_controlled_thread_materialization_receipts receipt
        ON receipt.coordinator_command_id = intent.coordinator_command_id
      JOIN agent_control_controlled_thread_materialization_accepted accepted
        ON accepted.coordinator_command_id = intent.coordinator_command_id
      WHERE intent.coordinator_command_id = ${input.commandId}
         OR intent.controlled_thread_reservation_id =
           ${input.controlledThreadReservationId}
      ORDER BY intent.coordinator_command_id
    `;

  const replayAccepted = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.replayAccepted",
  )(function* (
    input: AgentControlControlledThreadMaterializeInitialInput,
  ): Effect.fn.Return<
    Option.Option<ReplayedAccepted>,
    AgentControlControlledThreadMaterializationCoordinatorError
  > {
    const rows = yield* loadEvidence(input).pipe(
      Effect.mapError(() => error("internal-persistence-error", input)),
    );
    if (rows.length === 0) {
      const partial = yield* sql<{ readonly count: number }>`
        SELECT (
          (SELECT count(*)
           FROM agent_control_controlled_thread_materialization_intents
           WHERE coordinator_command_id = ${input.commandId}
              OR controlled_thread_reservation_id =
                ${input.controlledThreadReservationId})
          +
          (SELECT count(*)
           FROM agent_control_controlled_thread_materialization_receipts
           WHERE coordinator_command_id = ${input.commandId}
              OR controlled_thread_reservation_id =
                ${input.controlledThreadReservationId})
          +
          (SELECT count(*)
           FROM agent_control_controlled_thread_materialization_accepted
           WHERE coordinator_command_id = ${input.commandId}
              OR controlled_thread_reservation_id =
                ${input.controlledThreadReservationId})
        ) AS count
      `.pipe(Effect.mapError(() => error("internal-persistence-error", input)));
      if ((partial[0]?.count ?? 0) !== 0) {
        return yield* error("historical-evidence-corrupt", input);
      }
      return Option.none();
    }
    if (rows.length !== 1) return yield* error("command-identity-conflict", input);
    const row = rows[0]!;
    if (
      row.coordinatorCommandId !== input.commandId ||
      row.projectId !== input.projectId ||
      row.controlledThreadReservationId !== input.controlledThreadReservationId ||
      row.requestFingerprint !==
        deriveAgentControlControlledThreadCoordinatorRequestFingerprint(input)
    ) {
      return yield* error("command-identity-conflict", input);
    }
    const reservationEvidence = yield* reservationEngine
      .validateAcceptedReplayEvidence({
        controlledThreadReservationId: input.controlledThreadReservationId,
        projectId: input.projectId,
      })
      .pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    if (
      row.intentAcceptedMarkerCommandId !== input.commandId ||
      !finalizationOwnerIdPattern.test(row.intentFinalizationOwnerId) ||
      row.receiptRequestFingerprint !== row.requestFingerprint ||
      row.receiptCoordinatorCommandFingerprint !== row.coordinatorCommandFingerprint ||
      row.receiptControlledThreadReservationId !== row.controlledThreadReservationId ||
      row.receiptThreadId !== row.threadId ||
      row.receiptMaterializationCommandId !== row.materializationCommandId ||
      row.receiptMaterializationCommandFingerprint !== row.materializationCommandFingerprint ||
      row.receiptOrchestrationResultSequence !== row.orchestrationResultSequence ||
      row.receiptStatus !== "accepted" ||
      row.receiptAcceptedAt !== row.acceptedAt ||
      row.receiptAcceptedMarkerCommandId !== input.commandId ||
      row.markerCoordinatorCommandFingerprint !== row.coordinatorCommandFingerprint ||
      row.markerControlledThreadReservationId !== row.controlledThreadReservationId ||
      row.markerThreadId !== row.threadId ||
      row.markerMaterializationCommandId !== row.materializationCommandId ||
      row.markerMaterializationCommandFingerprint !== row.materializationCommandFingerprint ||
      row.markerOrchestrationResultSequence !== row.orchestrationResultSequence ||
      row.markerAcceptedAt !== row.acceptedAt ||
      row.markerFinalizationOwnerId !== row.intentFinalizationOwnerId
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    const command = yield* commandFromEvidence(row, input);
    const materializationFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
      crypto,
      command,
    ).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    const expectedCoordinatorFingerprint = deriveAgentControlControlledThreadCoordinatorFingerprint(
      input,
      materializationFingerprint,
      row.leaseHolderId,
      row.policyBindingFingerprint,
      row.runtimeObservationFingerprint,
    );
    const [materializingTransitionCommandId, materializationCommandId, boundTransitionCommandId] =
      yield* Effect.all([
        deriveAgentControlMaterializingTransitionCommandId(
          input.commandId,
          input.controlledThreadReservationId,
        ),
        deriveAgentControlThreadMaterializationCommandId(
          input.commandId,
          input.controlledThreadReservationId,
        ),
        deriveAgentControlBoundTransitionCommandId(
          input.commandId,
          input.controlledThreadReservationId,
        ),
      ]);
    if (
      row.coordinatorCommandFingerprint !== expectedCoordinatorFingerprint ||
      row.materializationCommandFingerprint !== materializationFingerprint ||
      row.materializingTransitionCommandId !== materializingTransitionCommandId ||
      row.materializationCommandId !== materializationCommandId ||
      row.boundTransitionCommandId !== boundTransitionCommandId ||
      command.commandId !== materializationCommandId
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }

    if (reservationEvidence.currentState.status !== "bound") {
      return yield* error("historical-evidence-corrupt", input);
    }
    const state = reservationEvidence.currentState;
    if (
      state.coordinatorCommandId !== input.commandId ||
      state.coordinatorCommandFingerprint !== row.coordinatorCommandFingerprint ||
      state.materializingTransitionCommandId !== row.materializingTransitionCommandId ||
      state.boundTransitionCommandId !== row.boundTransitionCommandId ||
      state.materializationCommandId !== row.materializationCommandId ||
      state.materializationCommandFingerprint !== row.materializationCommandFingerprint ||
      state.orchestrationResultSequence !== row.orchestrationResultSequence ||
      state.leaseHolderId !== row.leaseHolderId ||
      state.materializingAt !== row.materializingAt ||
      state.materializedAt !== row.materializedAt ||
      state.boundAt !== row.boundAt ||
      state.threadId !== row.threadId
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    const stream = reservationEvidence.history;
    if (
      stream.length !== 3 ||
      stream[0]?.type !== "agentControl.controlledThreadReservation.prepared" ||
      stream[0].streamVersion !== 1 ||
      stream[1]?.type !== "agentControl.controlledThreadReservation.materializing" ||
      stream[1].streamVersion !== 2 ||
      stream[1].eventId !== row.materializingEventId ||
      stream[1].sequence !== row.materializingEventSequence ||
      stream[1].commandId !== row.materializingTransitionCommandId ||
      stream[2]?.type !== "agentControl.controlledThreadReservation.bound" ||
      stream[2].streamVersion !== 3 ||
      stream[2].eventId !== row.boundEventId ||
      stream[2].sequence !== row.boundEventSequence ||
      stream[2].commandId !== row.boundTransitionCommandId
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    if (
      (yield* deriveAgentControlControlledThreadActivationCommandId(
        stream[0].commandId,
        input.controlledThreadReservationId,
      )) !== input.commandId
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    const replay = orchestration.replayAgentControlMaterialization;
    if (replay === undefined) return yield* error("internal-persistence-error", input);
    const orchestrationResult = yield* replay(command).pipe(
      Effect.mapError(() => error("historical-evidence-corrupt", input)),
    );
    if (
      orchestrationResult.lastSequence !== row.orchestrationResultSequence ||
      orchestrationResult.commandFingerprint !== row.materializationCommandFingerprint
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    const expectedHandoffId = yield* deriveAgentControlInitialPlanningHandoffId(
      input.controlledThreadReservationId,
      state.threadId,
    );
    const handoff = yield* initialPlanningStore
      .loadAcceptedByHandoffId(expectedHandoffId)
      .pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    const legacyRows = yield* sql<{ readonly count: number }>`
      SELECT count(*) AS count
      FROM agent_control_initial_planning_legacy_materializations
      WHERE coordinator_command_id = ${input.commandId}
        AND controlled_thread_reservation_id =
          ${input.controlledThreadReservationId}
        AND thread_id = ${state.threadId}
    `.pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    const legacy = legacyRows[0]?.count === 1;
    if (
      (legacy && Option.isSome(handoff)) ||
      (!legacy && Option.isNone(handoff)) ||
      (Option.isSome(handoff) &&
        (handoff.value.evidence.coordinatorCommandId !== row.coordinatorCommandId ||
          handoff.value.evidence.coordinatorCommandFingerprint !==
            row.coordinatorCommandFingerprint ||
          handoff.value.evidence.materializationCommandId !== row.materializationCommandId ||
          handoff.value.evidence.materializationCommandFingerprint !==
            row.materializationCommandFingerprint ||
          handoff.value.evidence.controlledThreadReservationId !==
            row.controlledThreadReservationId ||
          handoff.value.evidence.threadId !== row.threadId ||
          handoff.value.evidence.projectId !== row.projectId ||
          handoff.value.evidence.modelSelectionJson !== row.modelSelectionJson ||
          handoff.value.evidence.runtimeMode !== row.runtimeMode ||
          handoff.value.evidence.worktreePath !== row.worktreePath))
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    return Option.some({
      result: {
        commandId: input.commandId,
        controlledThreadReservationId: input.controlledThreadReservationId,
        threadId: state.threadId,
        orchestrationResultSequence: state.orchestrationResultSequence,
        status: "bound",
        replayed: true,
      },
      reservationEvents: [stream[1]!, stream[2]!],
      orchestrationResult,
      finalizationOwnerId: row.intentFinalizationOwnerId,
      handoffId: legacy ? null : expectedHandoffId,
    });
  });

  const makeMaterializationCommand = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.makeCommand",
  )(function* (
    input: AgentControlControlledThreadMaterializeInitialInput,
    resolved: ResolvedMaterialization,
    createdAt: string,
  ) {
    const commandId = yield* deriveAgentControlThreadMaterializationCommandId(
      input.commandId,
      input.controlledThreadReservationId,
    );
    return yield* decodeMaterializationCommand({
      type: "thread.agent-control.materialize",
      commandId,
      controlledThreadReservationId: resolved.reservation.controlledThreadReservationId,
      threadId: resolved.reservation.threadId,
      projectId: resolved.reservation.projectId,
      taskId: resolved.reservation.taskId,
      taskRevision: resolved.reservation.taskRevision,
      githubIntakeSequence: resolved.reservation.githubIntakeSequence,
      sourceIdentityFingerprint: resolved.reservation.sourceIdentityFingerprint,
      stageRunId: resolved.reservation.stageRunId,
      attemptId: resolved.reservation.attemptId,
      roleId: resolved.reservation.roleId,
      stageKind: resolved.reservation.stageKind,
      stageOrdinal: resolved.reservation.stageOrdinal,
      attemptOrdinal: resolved.reservation.attemptOrdinal,
      leaseId: resolved.reservation.leaseId,
      fenceToken: resolved.reservation.fenceToken,
      worktreeReservationId: resolved.reservation.worktreeReservationId,
      title: resolved.title,
      modelSelection: resolved.modelSelection,
      runtimeMode: resolved.runtimeMode,
      interactionMode: "plan",
      branch: resolved.worktree.branchName,
      worktreePath: resolved.worktree.internalWorktreePath,
      binding: {
        taskId: resolved.reservation.taskId,
        stageRunId: resolved.reservation.stageRunId,
        attemptId: resolved.reservation.attemptId,
        roleId: resolved.reservation.roleId,
        controlState: "controlled",
      },
      createdAt,
    }).pipe(Effect.mapError(() => error("validation", input)));
  });

  const sameAuthority = (left: ResolvedAuthority, right: ResolvedAuthority) =>
    left.reservation.controlledThreadReservationId ===
      right.reservation.controlledThreadReservationId &&
    left.reservation.sequence === right.reservation.sequence &&
    left.leaseHolderId === right.leaseHolderId &&
    sameWorktree(left.worktree, right.worktree) &&
    left.title === right.title;

  const sameResolved = (left: ResolvedMaterialization, right: ResolvedMaterialization) =>
    sameAuthority(left, right) &&
    left.runtimeMode === right.runtimeMode &&
    sameProjectPolicyBinding(left.policyBinding, right.policyBinding) &&
    left.runtimeObservationFingerprint === right.runtimeObservationFingerprint &&
    Equal.equals(left.modelSelection, right.modelSelection);

  const commitMaterialization = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.commit",
  )(function* (
    input: AgentControlControlledThreadMaterializeInitialInput,
    selected: ResolvedMaterialization,
    guardedWorktree: AgentControlWorktreeReservationState,
    finalizationOwnerId: string,
  ) {
    const currentAuthority = yield* resolveCurrent(input, true);
    const currentPolicyBinding = yield* loadProjectPolicyBinding(input);
    if (
      !sameAuthority(selected, currentAuthority) ||
      !sameProjectPolicyBinding(selected.policyBinding, currentPolicyBinding) ||
      !sameWorktree(currentAuthority.worktree, guardedWorktree)
    ) {
      return yield* error("source-snapshot-stale", input);
    }
    const current = {
      ...currentAuthority,
      modelSelection: selected.modelSelection,
      runtimeMode: selected.runtimeMode,
      policyBinding: currentPolicyBinding,
      runtimeObservationFingerprint: selected.runtimeObservationFingerprint,
    } satisfies ResolvedMaterialization;
    const transactionNow = yield* DateTime.now;
    const at = DateTime.formatIso(transactionNow);
    const command = yield* makeMaterializationCommand(input, current, at);
    const materializationFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
      crypto,
      command,
    ).pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    const resolvedCoordinatorFingerprint = deriveAgentControlControlledThreadCoordinatorFingerprint(
      input,
      materializationFingerprint,
      current.leaseHolderId,
      current.policyBinding.fingerprint,
      current.runtimeObservationFingerprint,
    );
    const [materializingTransitionCommandId, boundTransitionCommandId] = yield* Effect.all([
      deriveAgentControlMaterializingTransitionCommandId(
        input.commandId,
        input.controlledThreadReservationId,
      ),
      deriveAgentControlBoundTransitionCommandId(
        input.commandId,
        input.controlledThreadReservationId,
      ),
    ]);
    const beginCommand = {
      type: "agentControl.controlledThreadReservation.beginMaterialization",
      commandId: materializingTransitionCommandId,
      authority: "controller",
      controlledThreadReservationId: current.reservation.controlledThreadReservationId,
      threadId: current.reservation.threadId,
      projectId: current.reservation.projectId,
      taskId: current.reservation.taskId,
      taskRevision: current.reservation.taskRevision,
      githubIntakeSequence: current.reservation.githubIntakeSequence,
      sourceIdentityFingerprint: current.reservation.sourceIdentityFingerprint,
      stageRunId: current.reservation.stageRunId,
      attemptId: current.reservation.attemptId,
      roleId: current.reservation.roleId,
      stageKind: current.reservation.stageKind,
      stageOrdinal: current.reservation.stageOrdinal,
      attemptOrdinal: current.reservation.attemptOrdinal,
      leaseId: current.reservation.leaseId,
      fenceToken: current.reservation.fenceToken,
      worktreeReservationId: current.reservation.worktreeReservationId,
      expectedRevision: 1,
      coordinatorCommandId: input.commandId,
      coordinatorCommandFingerprint: resolvedCoordinatorFingerprint,
      materializingTransitionCommandId,
      materializationCommandId: command.commandId,
      materializationCommandFingerprint: materializationFingerprint,
      leaseHolderId: current.leaseHolderId,
      materializingAt: at,
    } as const;
    const beginDecision = yield* decideAgentControlControlledThreadReservationCommand({
      state: current.reservation,
      command: beginCommand,
      eventId: EventId.make(
        yield* crypto.randomUUIDv4.pipe(
          Effect.mapError(() => error("internal-persistence-error", input)),
        ),
      ),
      occurredAt: at,
    }).pipe(Effect.mapError(() => error("reservation-conflict", input)));
    if (beginDecision.length !== 1) return yield* error("reservation-conflict", input);
    const materializingEvents = yield* reservationEvents
      .appendInTransaction({
        controlledThreadReservationId: input.controlledThreadReservationId,
        expectedStreamVersion: 1,
        events: beginDecision,
      })
      .pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    const materializingEvent = materializingEvents[0];
    if (
      materializingEvents.length !== 1 ||
      materializingEvent?.type !== "agentControl.controlledThreadReservation.materializing"
    ) {
      return yield* error("internal-persistence-error", input);
    }
    yield* reservationProjection
      .projectEventInTransaction(materializingEvent)
      .pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    const materializingState = yield* projectAgentControlControlledThreadReservationEvent(
      current.reservation,
      materializingEvent,
    ).pipe(Effect.mapError(() => error("historical-evidence-corrupt", input)));
    yield* hooks.afterMaterializingProjection(observation(input, current.reservation.threadId));

    const materializeInTransaction = orchestration.materializeAgentControlInTransaction;
    const completeInTransaction = orchestration.completeAgentControlMaterializationInTransaction;
    if (materializeInTransaction === undefined || completeInTransaction === undefined) {
      return yield* error("internal-persistence-error", input);
    }
    const orchestrationResult = yield* materializeInTransaction(command).pipe(
      Effect.mapError(() => error("internal-persistence-error", input)),
    );
    if (orchestrationResult.committedEvents.length !== 2) {
      return yield* error("historical-evidence-corrupt", input);
    }
    yield* hooks.afterOrchestrationMaterialization(
      observation(input, current.reservation.threadId),
    );

    const bindCommand = {
      ...beginCommand,
      type: "agentControl.controlledThreadReservation.bindMaterialization",
      commandId: boundTransitionCommandId,
      expectedRevision: 2,
      boundTransitionCommandId,
      orchestrationResultSequence: orchestrationResult.lastSequence,
      materializedAt: at,
      boundAt: at,
    } as const;
    const bindDecision = yield* decideAgentControlControlledThreadReservationCommand({
      state: materializingState,
      command: bindCommand,
      eventId: EventId.make(
        yield* crypto.randomUUIDv4.pipe(
          Effect.mapError(() => error("internal-persistence-error", input)),
        ),
      ),
      occurredAt: at,
    }).pipe(Effect.mapError(() => error("reservation-conflict", input)));
    if (bindDecision.length !== 1) return yield* error("reservation-conflict", input);
    const boundEvents = yield* reservationEvents
      .appendInTransaction({
        controlledThreadReservationId: input.controlledThreadReservationId,
        expectedStreamVersion: 2,
        events: bindDecision,
      })
      .pipe(Effect.mapError((cause) => error("internal-persistence-error", input, cause)));
    const boundEvent = boundEvents[0];
    if (
      boundEvents.length !== 1 ||
      boundEvent?.type !== "agentControl.controlledThreadReservation.bound"
    ) {
      return yield* error("internal-persistence-error", input);
    }
    yield* reservationProjection
      .projectEventInTransaction(boundEvent)
      .pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    yield* hooks.afterBoundProjection(observation(input, current.reservation.threadId));

    const modelSelectionJson = yield* encodeModelSelectionJson(command.modelSelection).pipe(
      Effect.mapError(() => error("internal-persistence-error", input)),
    );
    const bindingJson = encodeAgentControlThreadBindingStorage(command.binding);
    yield* sql`
      INSERT INTO agent_control_controlled_thread_materialization_intents (
        coordinator_command_id, finalization_owner_id, request_fingerprint,
        coordinator_command_fingerprint, policy_binding_fingerprint,
        runtime_observation_fingerprint, project_id,
        controlled_thread_reservation_id, thread_id, task_id, task_revision,
        github_intake_sequence, source_identity_fingerprint, stage_run_id,
        attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
        lease_id, lease_holder_id, fence_token, worktree_reservation_id,
        materializing_transition_command_id, bound_transition_command_id,
        materialization_command_id, materialization_command_fingerprint,
        title, model_selection_json, runtime_mode, interaction_mode, branch,
        worktree_path, binding_json, materializing_event_id,
        materializing_event_sequence, bound_event_id, bound_event_sequence,
        orchestration_result_sequence, materializing_at, materialized_at,
        bound_at, accepted_at, accepted_marker_command_id
      ) VALUES (
        ${input.commandId}, ${finalizationOwnerId},
        ${deriveAgentControlControlledThreadCoordinatorRequestFingerprint(input)},
        ${resolvedCoordinatorFingerprint}, ${current.policyBinding.fingerprint},
        ${current.runtimeObservationFingerprint}, ${input.projectId},
        ${input.controlledThreadReservationId}, ${command.threadId},
        ${command.taskId}, ${command.taskRevision}, ${command.githubIntakeSequence},
        ${command.sourceIdentityFingerprint}, ${command.stageRunId},
        ${command.attemptId}, ${command.roleId}, ${command.stageKind},
        ${command.stageOrdinal}, ${command.attemptOrdinal}, ${command.leaseId},
        ${current.leaseHolderId}, ${command.fenceToken},
        ${command.worktreeReservationId}, ${materializingTransitionCommandId},
        ${boundTransitionCommandId}, ${command.commandId},
        ${materializationFingerprint}, ${command.title}, ${modelSelectionJson},
        ${command.runtimeMode}, ${command.interactionMode}, ${command.branch},
        ${command.worktreePath}, ${bindingJson}, ${materializingEvent.eventId},
        ${materializingEvent.sequence}, ${boundEvent.eventId}, ${boundEvent.sequence},
        ${orchestrationResult.lastSequence}, ${at}, ${at}, ${at}, ${at},
        ${input.commandId}
      )
    `.pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    yield* sql`
      INSERT INTO agent_control_controlled_thread_materialization_receipts (
        coordinator_command_id, request_fingerprint,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, materialization_command_id,
        materialization_command_fingerprint, orchestration_result_sequence,
        status, accepted_at, accepted_marker_command_id
      ) VALUES (
        ${input.commandId},
        ${deriveAgentControlControlledThreadCoordinatorRequestFingerprint(input)},
        ${resolvedCoordinatorFingerprint}, ${input.controlledThreadReservationId},
        ${command.threadId}, ${command.commandId}, ${materializationFingerprint},
        ${orchestrationResult.lastSequence}, 'accepted', ${at}, ${input.commandId}
      )
    `.pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    yield* hooks.afterCoordinatorEvidence(observation(input, command.threadId));
    yield* hooks.beforeAcceptedMarker(observation(input, command.threadId));

    // Complete the nested orchestration evidence before accepting the handoff.
    // The existing coordinator marker remains the final application statement.
    yield* completeInTransaction(orchestrationResult).pipe(
      Effect.mapError((cause) => error("internal-persistence-error", input, cause)),
    );

    const handoffId = yield* deriveAgentControlInitialPlanningHandoffId(
      input.controlledThreadReservationId,
      command.threadId,
    );
    const [turnRequestCommandId, messageId] = yield* Effect.all([
      deriveAgentControlInitialPlanningTurnRequestCommandId(handoffId),
      deriveAgentControlInitialPlanningMessageId(handoffId),
    ]);
    const [messageEventId, turnRequestEventId] = yield* Effect.all([
      deriveAgentControlInitialPlanningMessageEventId(turnRequestCommandId),
      deriveAgentControlInitialPlanningTurnRequestEventId(turnRequestCommandId),
    ]);
    const providerDeliveryId =
      yield* deriveAgentControlInitialPlanningProviderDeliveryId(handoffId);
    const promptText = buildAgentControlInitialPlanningPrompt({
      repositoryDisplay: deriveAgentControlRepositoryDisplay(current.task.sourceSnapshot.url),
      taskTitle: current.task.sourceSnapshot.title,
      taskBody: current.task.sourceSnapshot.body,
      sourceRevision: current.task.sourceUpdatedAt,
    });
    if (
      command.roleId !== "planning" ||
      command.stageKind !== "planning" ||
      command.stageOrdinal !== 1 ||
      command.attemptOrdinal !== 1 ||
      !["approval-required", "full-access"].includes(command.runtimeMode)
    ) {
      return yield* error("historical-evidence-corrupt", input);
    }
    const planningDeadlineAt = DateTime.formatIso(DateTime.add(transactionNow, { minutes: 30 }));
    const messageEventTemplateJson = canonicalInitialPlanningEventTemplate({
      streamVersion: 3,
      eventId: messageEventId,
      aggregateKind: "thread",
      aggregateId: command.threadId,
      type: "thread.message-sent",
      occurredAt: at,
      commandId: turnRequestCommandId,
      causationEventId: null,
      correlationId: turnRequestCommandId,
      actorKind: "client",
      payload: initialPlanningMessagePayload({
        threadId: command.threadId,
        messageId,
        promptText,
        createdAt: at,
      }),
      metadata: {},
    });
    const turnRequestEventTemplateJson = canonicalInitialPlanningEventTemplate({
      streamVersion: 4,
      eventId: turnRequestEventId,
      aggregateKind: "thread",
      aggregateId: command.threadId,
      type: "thread.turn-start-requested",
      occurredAt: at,
      commandId: turnRequestCommandId,
      causationEventId: messageEventId,
      correlationId: turnRequestCommandId,
      actorKind: "client",
      payload: initialPlanningTurnRequestPayload({
        threadId: command.threadId,
        messageId,
        modelSelection: command.modelSelection,
        runtimeMode: current.runtimeMode,
        createdAt: at,
      }),
      metadata: {},
    });
    const eventTemplateDigest = combinedInitialPlanningEventDigest(
      messageEventTemplateJson,
      turnRequestEventTemplateJson,
    );
    const handoffFingerprint = fingerprintAgentControlInitialPlanningHandoff({
      handoffId,
      coordinatorCommandId: input.commandId,
      coordinatorCommandFingerprint: resolvedCoordinatorFingerprint,
      materializationCommandId: command.commandId,
      materializationCommandFingerprint: materializationFingerprint,
      projectId: command.projectId,
      controlledThreadReservationId: input.controlledThreadReservationId,
      threadId: command.threadId,
      taskId: command.taskId,
      taskRevision: command.taskRevision,
      githubIntakeSequence: command.githubIntakeSequence,
      sourceIdentityFingerprint: command.sourceIdentityFingerprint,
      stageRunId: command.stageRunId,
      attemptId: command.attemptId,
      roleId: "planning",
      stageKind: "planning",
      stageOrdinal: 1,
      attemptOrdinal: 1,
      leaseId: command.leaseId,
      leaseHolderId: current.leaseHolderId,
      fenceToken: command.fenceToken,
      worktreeReservationId: command.worktreeReservationId,
      worktreePath: command.worktreePath,
      planningRole: "planner",
      providerInstanceId: command.modelSelection.instanceId,
      runtimeMode: current.runtimeMode,
      modelSelectionJson,
      templateVersion: AGENT_CONTROL_INITIAL_PLANNING_PROMPT_TEMPLATE_VERSION,
      promptText,
      turnRequestCommandId,
      messageId,
      messageEventId,
      turnRequestEventId,
      providerDeliveryId,
    });
    yield* initialPlanningStore
      .insertAcceptedInTransaction(
        {
          handoffId,
          handoffFingerprint,
          coordinatorCommandId: input.commandId,
          coordinatorCommandFingerprint: resolvedCoordinatorFingerprint,
          materializationCommandId: command.commandId,
          materializationCommandFingerprint: materializationFingerprint,
          projectId: command.projectId,
          controlledThreadReservationId: input.controlledThreadReservationId,
          threadId: command.threadId,
          taskId: command.taskId,
          taskRevision: command.taskRevision,
          githubIntakeSequence: command.githubIntakeSequence,
          sourceIdentityFingerprint: command.sourceIdentityFingerprint,
          stageRunId: command.stageRunId,
          attemptId: command.attemptId,
          roleId: "planning",
          stageKind: "planning",
          stageOrdinal: 1,
          attemptOrdinal: 1,
          leaseId: command.leaseId,
          leaseHolderId: current.leaseHolderId,
          fenceToken: command.fenceToken,
          worktreeReservationId: command.worktreeReservationId,
          worktreePath: command.worktreePath,
          providerInstanceId: command.modelSelection.instanceId,
          runtimeMode: current.runtimeMode,
          modelSelection: command.modelSelection,
          modelSelectionJson,
          planningRole: "planner",
          templateVersion: AGENT_CONTROL_INITIAL_PLANNING_PROMPT_TEMPLATE_VERSION,
          promptText,
          turnRequestCommandId,
          messageId,
          messageEventId,
          turnRequestEventId,
          messageEventTemplateJson,
          turnRequestEventTemplateJson,
          eventTemplateDigest,
          providerDeliveryId,
          createdAt: at,
          planningDeadlineAt,
        },
        {
          afterIntent: () => hooks.afterInitialPlanningIntent(observation(input, command.threadId)),
          afterReceipt: () =>
            hooks.afterInitialPlanningReceipt(observation(input, command.threadId)),
          afterAccepted: () =>
            hooks.afterInitialPlanningAccepted(observation(input, command.threadId)),
          afterDelivery: () =>
            hooks.afterInitialPlanningDelivery(observation(input, command.threadId)),
        },
      )
      .pipe(Effect.mapError((cause) => error("internal-persistence-error", input, cause)));

    // Keep this INSERT as the final application SQL statement in the outer
    // transaction. Its trigger validates both complete event families,
    // projections, intents, receipts, and the bound reservation.
    yield* sql`
      INSERT INTO agent_control_controlled_thread_materialization_accepted (
        coordinator_command_id, finalization_owner_id,
        coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ) VALUES (
        ${input.commandId}, ${finalizationOwnerId}, ${resolvedCoordinatorFingerprint},
        ${input.controlledThreadReservationId}, ${command.threadId},
        ${command.commandId}, ${materializationFingerprint},
        ${orchestrationResult.lastSequence}, ${at}
      )
    `.pipe(Effect.mapError(() => error("internal-persistence-error", input)));
    return {
      result: {
        commandId: input.commandId,
        controlledThreadReservationId: input.controlledThreadReservationId,
        threadId: command.threadId,
        orchestrationResultSequence: orchestrationResult.lastSequence,
        status: "bound",
        replayed: false,
      } satisfies AgentControlControlledThreadMaterializeInitialResult,
      reservationEvents: [
        materializingEvent,
        boundEvent,
      ] satisfies ReadonlyArray<AgentControlControlledThreadReservationEvent>,
      orchestrationResult,
      finalizationOwnerId,
      handoffId,
    };
  });

  const finalizeCommitted = Effect.fn(
    "AgentControlControlledThreadMaterializationCoordinator.finalizeCommitted",
  )(function* (
    input: AgentControlControlledThreadMaterializeInitialInput,
    committed: {
      readonly result: AgentControlControlledThreadMaterializeInitialResult;
      readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
      readonly orchestrationResult: AgentControlThreadMaterializationTransactionResult;
    },
    publish: boolean,
  ) {
    const currentObservation = observation(input, committed.result.threadId);
    const exits: Array<
      Exit.Exit<void, AgentControlControlledThreadMaterializationCoordinatorError>
    > = [];
    exits.push(yield* Effect.exit(hooks.beforeReservationFinalization(currentObservation)));
    const reservationRefresh = yield* Effect.exit(
      reservationEngine
        .refreshCommitted(committed.reservationEvents)
        .pipe(Effect.mapError((cause) => error("historical-evidence-corrupt", input, cause))),
    );
    exits.push(reservationRefresh);
    exits.push(yield* Effect.exit(hooks.afterReservationFinalization(currentObservation)));
    exits.push(yield* Effect.exit(hooks.beforeOrchestrationFinalization(currentObservation)));
    const refresh = orchestration.refreshAgentControlMaterialization;
    const refreshEffect: Effect.Effect<
      void,
      AgentControlControlledThreadMaterializationCoordinatorError
    > =
      refresh === undefined
        ? Effect.fail(error("internal-persistence-error", input))
        : refresh(committed.orchestrationResult).pipe(
            Effect.mapError((cause) => error("historical-evidence-corrupt", input, cause)),
          );
    const orchestrationRefresh = yield* Effect.exit(refreshEffect);
    exits.push(orchestrationRefresh);
    exits.push(yield* Effect.exit(hooks.afterOrchestrationFinalization(currentObservation)));

    if (publish && Exit.isSuccess(reservationRefresh) && Exit.isSuccess(orchestrationRefresh)) {
      const orchestrationPublish = orchestration.publishAgentControlMaterialization;
      const orchestrationPublicationEffect: Effect.Effect<
        void,
        AgentControlControlledThreadMaterializationCoordinatorError
      > =
        orchestrationPublish === undefined
          ? Effect.fail(error("internal-persistence-error", input))
          : orchestrationPublish(committed.orchestrationResult).pipe(
              Effect.mapError((cause) => error("internal-persistence-error", input, cause)),
            );
      const [reservationPublication, orchestrationPublication] = yield* Effect.all(
        [
          Effect.exit(reservationEngine.publishCommitted(committed.reservationEvents)),
          Effect.exit(orchestrationPublicationEffect),
        ],
        { concurrency: 1 },
      );
      exits.push(reservationPublication, orchestrationPublication);
      exits.push(yield* Effect.exit(hooks.afterPublication(currentObservation)));
    }

    return yield* Exit.asVoidAll(exits);
  });

  const materializeInitial = (input: AgentControlControlledThreadMaterializeInitialInput) =>
    Effect.gen(function* () {
      const replay = yield* replayAccepted(input);
      if (Option.isSome(replay)) {
        yield* finalizeCommitted(input, replay.value, false).pipe(Effect.uninterruptible);
        return replay.value.result;
      }

      const finalizationOwnerId = yield* crypto.randomUUIDv4.pipe(
        Effect.mapError((cause) => error("internal-persistence-error", input, cause)),
      );
      yield* hooks.afterReceiptFirst(observation(input, null));
      const policyBindingBefore = yield* loadProjectPolicyBinding(input);
      const selectedAuthority = yield* resolveCurrent(input, false);
      const selectedRuntime = yield* resolveRuntime(input, policyBindingBefore);
      const policyBindingAfter = yield* loadProjectPolicyBinding(input);
      if (!sameProjectPolicyBinding(policyBindingBefore, policyBindingAfter)) {
        return yield* error("source-snapshot-stale", input);
      }
      const selected = {
        ...selectedAuthority,
        ...selectedRuntime,
        policyBinding: policyBindingAfter,
      } satisfies ResolvedMaterialization;
      yield* hooks.afterAuthoritativeResolution(observation(input, selected.reservation.threadId));

      const guarded = yield* worktreeController
        .useReadyWorktree(
          {
            projectId: input.projectId,
            reservationId: selected.reservation.worktreeReservationId,
          },
          (guardedWorktree) =>
            Effect.gen(function* () {
              if (!sameWorktree(selected.worktree, guardedWorktree)) {
                return yield* error("worktree-unavailable", input);
              }
              const immediatelyCurrentAuthority = yield* resolveCurrent(input, false);
              const immediatelyCurrent = {
                ...immediatelyCurrentAuthority,
                modelSelection: selected.modelSelection,
                runtimeMode: selected.runtimeMode,
                policyBinding: selected.policyBinding,
                runtimeObservationFingerprint: selected.runtimeObservationFingerprint,
              } satisfies ResolvedMaterialization;
              if (
                !sameResolved(selected, immediatelyCurrent) ||
                !sameWorktree(guardedWorktree, immediatelyCurrent.worktree)
              ) {
                return yield* error("source-snapshot-stale", input);
              }
              yield* hooks.beforeTransactionAdmission(
                observation(input, selected.reservation.threadId),
              );
              return yield* Effect.uninterruptibleMask((restore) =>
                Effect.gen(function* () {
                  // Restore only the transaction. The Exit observer must stay
                  // masked so a pending post-COMMIT interrupt cannot erase the
                  // committed outcome before receipt-first recovery runs.
                  const transactionExit = yield* Effect.exit(
                    restore(
                      sql
                        .withTransaction(
                          commitMaterialization(
                            input,
                            immediatelyCurrent,
                            guardedWorktree,
                            finalizationOwnerId,
                          ),
                        )
                        .pipe(
                          Effect.catchTag("SqlError", (cause) =>
                            Effect.fail(error("internal-persistence-error", input, cause)),
                          ),
                        ),
                    ),
                  );
                  if (Exit.isFailure(transactionExit)) {
                    // This transaction is read-only and fresh. It validates the
                    // complete committed identity before any local finalization.
                    const recoveryExit = yield* Effect.exit(
                      sql
                        .withTransaction(replayAccepted(input))
                        .pipe(
                          Effect.catchTag("SqlError", (cause) =>
                            Effect.fail(error("internal-persistence-error", input, cause)),
                          ),
                        ),
                    );
                    if (Exit.isFailure(recoveryExit)) {
                      return yield* Effect.failCause(
                        Cause.combine(transactionExit.cause, recoveryExit.cause),
                      );
                    }
                    if (Option.isNone(recoveryExit.value)) {
                      return yield* Effect.failCause(transactionExit.cause);
                    }
                    const recovered = recoveryExit.value.value;
                    if (
                      recovered.handoffId !== null &&
                      recovered.finalizationOwnerId === finalizationOwnerId
                    ) {
                      yield* initialPlanningWakeup.wake(recovered.handoffId);
                    }
                    const finalizationExit = yield* Effect.exit(
                      finalizeCommitted(
                        input,
                        recovered,
                        recovered.finalizationOwnerId === finalizationOwnerId,
                      ),
                    );
                    if (Exit.isFailure(finalizationExit)) {
                      return yield* Effect.failCause(
                        Cause.combine(transactionExit.cause, finalizationExit.cause),
                      );
                    }
                    return yield* Effect.failCause(transactionExit.cause);
                  }
                  const committed = transactionExit.value;
                  if (committed.handoffId !== null) {
                    yield* initialPlanningWakeup.wake(committed.handoffId);
                  }
                  const callerExit = yield* Effect.exit(
                    restore(hooks.afterOuterCommit(observation(input, committed.result.threadId))),
                  );
                  const finalizationExit = yield* Effect.exit(
                    finalizeCommitted(input, committed, true),
                  );
                  if (Exit.isFailure(callerExit)) {
                    return yield* Effect.failCause(
                      Exit.isFailure(finalizationExit)
                        ? Cause.combine(callerExit.cause, finalizationExit.cause)
                        : callerExit.cause,
                    );
                  }
                  if (Exit.isFailure(finalizationExit)) {
                    return yield* Effect.failCause(finalizationExit.cause);
                  }
                  return {
                    _tag: "Committed",
                    committed,
                  } satisfies CoordinatorGuardedOutcome;
                }),
              );
            }),
          {
            beforeInspection: replayAccepted(input).pipe(
              Effect.map(
                Option.map(
                  (replayed): CoordinatorGuardedOutcome => ({
                    _tag: "Replayed",
                    replayed,
                  }),
                ),
              ),
            ),
          },
        )
        .pipe(
          Effect.mapError((cause) =>
            isCoordinatorError(cause)
              ? cause
              : error(
                  cause.code === "project-unavailable"
                    ? "project-unavailable"
                    : "worktree-unavailable",
                  input,
                ),
          ),
        );
      if (guarded._tag === "Replayed") {
        yield* finalizeCommitted(input, guarded.replayed, false).pipe(Effect.uninterruptible);
        return guarded.replayed.result;
      }
      return guarded.committed.result;
    }).pipe(
      Effect.mapError((cause) =>
        isCoordinatorError(cause) ? cause : error("internal-persistence-error", input),
      ),
    );

  const materializeInitialForRunOnce: NonNullable<
    AgentControlControlledThreadMaterializationCoordinatorShape["materializeInitialForRunOnce"]
  > = (runId: AgentControlRunOnceId, input) =>
    materializeInitial(input).pipe(
      Effect.provideService(AgentControlRunOnceExecutionContext, runId),
    );

  return AgentControlControlledThreadMaterializationCoordinator.of({
    materializeInitial,
    materializeInitialForRunOnce,
  });
});

export const AgentControlControlledThreadMaterializationCoordinatorLive = Layer.effect(
  AgentControlControlledThreadMaterializationCoordinator,
  make,
).pipe(Layer.provideMerge(AgentControlInitialPlanningHandoffStoreLive));
