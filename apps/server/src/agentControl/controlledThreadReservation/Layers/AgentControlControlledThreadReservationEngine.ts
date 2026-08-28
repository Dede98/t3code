import {
  AgentControlControlledThreadReservationCommand,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationRejectedCommandCode,
  AgentControlControlledThreadReservationRpcError,
  type AgentControlControlledThreadReservationCommandResult,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationPrepareCommand,
  type AgentControlControlledThreadReservationState,
  type AgentControlControlledThreadReservationView,
  type AgentControlRejectedCommandErrorCode,
  EventId,
  ProjectId as ProjectIdSchema,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { deriveRejectedAgentControlControlledThreadReservationId } from "../identity.ts";
import {
  loadAuthoritativeControlledThreadReservation,
  loadAuthoritativeControlledThreadReservationTaskHistory,
} from "../authoritative.ts";
import {
  insertControlledThreadCommandIntent,
  internalControlledThreadCommandIntent,
  loadControlledThreadCommandIntent,
  sameControlledThreadCommandIntent,
} from "../commandIntent.ts";
import { decideAgentControlControlledThreadReservationCommand } from "../decider.ts";
import { validateAgentControlControlledThreadReservationState } from "../invariant.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../projector.ts";
import {
  type PersistedControlledThreadCoordinatorFingerprintEvidence,
  validateCanonicalControlledThreadCoordinatorFingerprints,
  validateCanonicalControlledThreadSuccessors,
} from "../successorEvidence.ts";
import {
  AgentControlControlledThreadReservationEngine,
  type AgentControlControlledThreadAcceptedReplayEvidence,
  type AgentControlControlledThreadReservationDispatchOutcome,
  type AgentControlControlledThreadReservationEngineShape,
} from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";
import { AgentControlControlledThreadReservationTransactionHooks } from "../Services/AgentControlControlledThreadReservationTransactionHooks.ts";
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
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktree } from "../../worktree/Services/AgentControlWorktree.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { loadOrchestrationEventsByCommandIdPage } from "../../../orchestration/orchestrationEventRaw.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationCommand);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationId);
const isRpcError = Schema.is(AgentControlControlledThreadReservationRpcError);
const isReservationCode = Schema.is(AgentControlControlledThreadReservationRejectedCommandCode);
const internalProjectId = ProjectIdSchema.make(
  "agent-control-controlled-thread-reservation-internal",
);

interface ControlledThreadCatalogRow {
  readonly eventId: string;
  readonly aggregateKind: string;
  readonly streamVersion: number;
  readonly commandId: string;
  readonly eventType: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly projectId: string;
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
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly preparedAt: string;
  readonly coordinatorCommandId: string | null;
  readonly coordinatorCommandFingerprint: string | null;
  readonly materializingTransitionCommandId: string | null;
  readonly materializationCommandId: string | null;
  readonly materializationCommandFingerprint: string | null;
  readonly leaseHolderId: string | null;
  readonly materializingAt: string | null;
  readonly boundTransitionCommandId: string | null;
  readonly orchestrationResultSequence: number | null;
  readonly materializedAt: string | null;
  readonly boundAt: string | null;
}

interface PrepareFinalizationRow {
  readonly prepareCommandId: string;
  readonly prepareCommandFingerprint: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly controlledThreadReservationId: string;
  readonly preparedEventId: string;
  readonly preparedStreamVersion: number;
  readonly preparedEventSequence: number;
  readonly receiptCommandId: string;
  readonly receiptStatus: string;
  readonly receiptResultSequence: number;
  readonly receiptResultStreamVersion: number;
  readonly receiptEventCreated: number;
  readonly receiptAcceptedAt: string;
  readonly finalizationOwnerId: string;
  readonly status: string;
  readonly revision: number;
  readonly claimedAt: string | null;
  readonly completedAt: string | null;
  readonly intentFingerprint: string;
  readonly intentCommandType: string;
  readonly intentAuthority: string;
  readonly intentAggregateKind: string;
  readonly intentAggregateId: string;
  readonly eventCommandId: string;
  readonly eventSequence: number;
  readonly eventStreamVersion: number;
  readonly eventType: string;
  readonly eventCorrelationId: string;
  readonly eventActorAuthority: string;
  readonly receiptFingerprint: string;
  readonly receiptAggregateKind: string;
  readonly receiptAggregateId: string;
  readonly persistedReceiptStatus: string;
  readonly persistedReceiptResultSequence: number;
  readonly persistedReceiptResultStreamVersion: number;
  readonly persistedReceiptEventCreated: number;
  readonly persistedReceiptAcceptedAt: string;
}

interface PrepareFinalizationInput {
  readonly commandId: AgentControlControlledThreadReservationCommand["commandId"];
  readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
  readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
  readonly commandFingerprint: string;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly preparedEvent: AgentControlControlledThreadReservationEvent;
}

const livePrepareFinalizationOwners = new Set<string>();

const sameCatalogBinding = (
  row: ControlledThreadCatalogRow,
  state:
    | AgentControlControlledThreadReservationState
    | AgentControlControlledThreadReservationEvent["payload"],
) =>
  row.controlledThreadReservationId === state.controlledThreadReservationId &&
  row.threadId === state.threadId &&
  row.projectId === state.projectId &&
  row.taskId === state.taskId &&
  row.taskRevision === state.taskRevision &&
  row.githubIntakeSequence === state.githubIntakeSequence &&
  row.sourceIdentityFingerprint === state.sourceIdentityFingerprint &&
  row.stageRunId === state.stageRunId &&
  row.attemptId === state.attemptId &&
  row.roleId === state.roleId &&
  row.stageKind === state.stageKind &&
  row.stageOrdinal === state.stageOrdinal &&
  row.attemptOrdinal === state.attemptOrdinal &&
  row.leaseId === state.leaseId &&
  row.fenceToken === state.fenceToken &&
  row.worktreeReservationId === state.worktreeReservationId &&
  row.preparedAt === state.preparedAt;

const sameCatalogEvent = (
  row: ControlledThreadCatalogRow,
  event: AgentControlControlledThreadReservationEvent,
) =>
  row.eventId === event.eventId &&
  row.aggregateKind === event.aggregateKind &&
  row.streamVersion === event.streamVersion &&
  row.commandId === event.commandId &&
  row.eventType === event.type &&
  row.controlledThreadReservationId === event.aggregateId &&
  sameCatalogBinding(row, event.payload) &&
  (event.type === "agentControl.controlledThreadReservation.prepared"
    ? row.coordinatorCommandId === null &&
      row.coordinatorCommandFingerprint === null &&
      row.materializingTransitionCommandId === null &&
      row.materializationCommandId === null &&
      row.materializationCommandFingerprint === null &&
      row.leaseHolderId === null &&
      row.materializingAt === null &&
      row.boundTransitionCommandId === null &&
      row.orchestrationResultSequence === null &&
      row.materializedAt === null &&
      row.boundAt === null
    : row.coordinatorCommandId === event.payload.coordinatorCommandId &&
      row.coordinatorCommandFingerprint === event.payload.coordinatorCommandFingerprint &&
      row.materializingTransitionCommandId === event.payload.materializingTransitionCommandId &&
      row.materializationCommandId === event.payload.materializationCommandId &&
      row.materializationCommandFingerprint === event.payload.materializationCommandFingerprint &&
      row.leaseHolderId === event.payload.leaseHolderId &&
      row.materializingAt === event.payload.materializingAt &&
      (event.type === "agentControl.controlledThreadReservation.materializing"
        ? row.boundTransitionCommandId === null &&
          row.orchestrationResultSequence === null &&
          row.materializedAt === null &&
          row.boundAt === null
        : row.boundTransitionCommandId === event.payload.boundTransitionCommandId &&
          row.orchestrationResultSequence === event.payload.orchestrationResultSequence &&
          row.materializedAt === event.payload.materializedAt &&
          row.boundAt === event.payload.boundAt));

export const toAgentControlControlledThreadReservationView = (
  state: AgentControlControlledThreadReservationState,
): AgentControlControlledThreadReservationView => ({
  controlledThreadReservationId: state.controlledThreadReservationId,
  threadId: state.threadId,
  projectId: state.projectId,
  taskId: state.taskId,
  stageRunId: state.stageRunId,
  attemptId: state.attemptId,
  roleId: state.roleId,
  status: state.status,
  revision: state.revision,
  preparedAt: state.preparedAt,
});

const rpcError = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  input: {
    readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
    readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
    readonly controlledThreadReservationId?: AgentControlControlledThreadReservationId | undefined;
  },
  operation: AgentControlControlledThreadReservationRpcError["operation"] = "dispatch",
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation,
    projectId: input.projectId,
    taskId: input.taskId,
    controlledThreadReservationId: input.controlledThreadReservationId ?? null,
  });

const guardCode = (
  reason: AgentControlTaskConsumerGuardReason,
): AgentControlControlledThreadReservationRpcError["code"] => {
  switch (reason) {
    case "project-unavailable":
      return "project-unavailable";
    case "mode-inactive":
      return "project-mode-inactive";
    case "source-snapshot-unavailable":
      return "source-snapshot-unavailable";
    case "watermark-missing":
    case "watermark-not-completed":
    case "watermark-sequence-mismatch":
      return "source-watermark-stale";
    case "task-missing":
      return "task-missing";
    case "task-status-inactive":
      return "task-not-candidate";
    case "task-source-ineligible":
      return "task-ineligible";
    case "task-stage-inactive":
      return "task-stage-inactive";
    case "task-projection-corrupt":
      return "task-projection-corrupt";
    case "task-project-mismatch":
    case "task-sequence-mismatch":
    case "task-source-mismatch":
      return "source-snapshot-stale";
    case "internal-persistence-error":
      return "internal-persistence-error";
  }
};

const sameCommandBinding = (
  state: AgentControlControlledThreadReservationState,
  command: AgentControlControlledThreadReservationPrepareCommand,
) =>
  state.controlledThreadReservationId === command.controlledThreadReservationId &&
  state.threadId === command.threadId &&
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.roleId === command.roleId &&
  state.stageKind === command.stageKind &&
  state.stageOrdinal === command.stageOrdinal &&
  state.attemptOrdinal === command.attemptOrdinal &&
  state.leaseId === command.leaseId &&
  state.fenceToken === command.fenceToken &&
  state.worktreeReservationId === command.worktreeReservationId &&
  state.status === "prepared" &&
  state.revision === 1 &&
  command.expectedRevision === 0 &&
  command.authority === "controller";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlControlledThreadReservationEventStore;
  const projection = yield* AgentControlControlledThreadReservationProjection;
  const states = yield* AgentControlControlledThreadReservationStateRepository;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktree;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;
  const transactionHooks = yield* AgentControlControlledThreadReservationTransactionHooks;
  const prepareFinalizationOwnerId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
  livePrepareFinalizationOwners.add(prepareFinalizationOwnerId);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      livePrepareFinalizationOwners.delete(prepareFinalizationOwnerId);
    }),
  );
  const locallyPublishedPrepareEventIds = new Set<string>();

  yield* projection.bootstrap.pipe(
    Effect.mapError(() =>
      rpcError(
        "controlled-thread-reservation-corrupt",
        {
          projectId: internalProjectId,
          taskId: "controlled-thread-reservation-internal" as never,
        },
        "dispatch",
      ),
    ),
  );
  const eventPubSub =
    yield* PubSub.unbounded<
      import("@t3tools/contracts").AgentControlControlledThreadReservationEvent
    >();

  const mapHistoryError = (
    failure: { readonly _tag: string },
    input: {
      readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
      readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
      readonly controlledThreadReservationId?:
        | AgentControlControlledThreadReservationId
        | undefined;
    },
  ) =>
    rpcError(
      failure._tag === "AgentControlPersistenceSqlError"
        ? "internal-persistence-error"
        : "controlled-thread-reservation-corrupt",
      input,
    );

  const validateTaskHistory: AgentControlControlledThreadReservationEngineShape["validateTaskHistory"] =
    (projectId, taskId) =>
      Effect.gen(function* () {
        const history = yield* loadAuthoritativeControlledThreadReservationTaskHistory(
          projectId,
          taskId,
          events,
          states,
        );
        const catalog = yield* sql<ControlledThreadCatalogRow>`
          SELECT
            event_id AS "eventId", aggregate_kind AS "aggregateKind",
            stream_version AS "streamVersion", command_id AS "commandId",
            event_type AS "eventType",
            controlled_thread_reservation_id AS "controlledThreadReservationId",
            thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
            task_revision AS "taskRevision",
            github_intake_sequence AS "githubIntakeSequence",
            source_identity_fingerprint AS "sourceIdentityFingerprint",
            stage_run_id AS "stageRunId", attempt_id AS "attemptId",
            role_id AS "roleId", stage_kind AS "stageKind",
            stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
            lease_id AS "leaseId", fence_token AS "fenceToken",
            worktree_reservation_id AS "worktreeReservationId",
            prepared_at AS "preparedAt",
            coordinator_command_id AS "coordinatorCommandId",
            coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
            materializing_transition_command_id AS "materializingTransitionCommandId",
            materialization_command_id AS "materializationCommandId",
            materialization_command_fingerprint AS "materializationCommandFingerprint",
            lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
            bound_transition_command_id AS "boundTransitionCommandId",
            orchestration_result_sequence AS "orchestrationResultSequence",
            materialized_at AS "materializedAt", bound_at AS "boundAt"
          FROM agent_control_controlled_thread_stream_catalog_all
          WHERE project_id = ${projectId} AND task_id = ${taskId}
            AND stream_version = 1
          ORDER BY controlled_thread_reservation_id ASC
        `;
        if (catalog.length !== history.length) {
          return yield* rpcError("controlled-thread-reservation-corrupt", {
            projectId,
            taskId,
          });
        }
        const historyById = new Map<string, AgentControlControlledThreadReservationState>(
          history.map((state) => [state.controlledThreadReservationId, state] as const),
        );
        for (const row of catalog) {
          const state = historyById.get(row.controlledThreadReservationId);
          if (state === undefined || !sameCatalogBinding(row, state)) {
            return yield* rpcError("controlled-thread-reservation-corrupt", {
              projectId,
              taskId,
            });
          }
          historyById.delete(row.controlledThreadReservationId);
        }
        if (historyById.size !== 0) {
          return yield* rpcError("controlled-thread-reservation-corrupt", {
            projectId,
            taskId,
          });
        }
        return history;
      }).pipe(
        Effect.mapError((failure) =>
          isRpcError(failure) ? failure : mapHistoryError(failure, { projectId, taskId }),
        ),
      );

  const loadStateEvidence = (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    projectId: AgentControlControlledThreadReservationRpcError["projectId"],
    taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>,
  ) =>
    Effect.gen(function* () {
      const state = yield* loadAuthoritativeControlledThreadReservation(
        controlledThreadReservationId,
        events,
        states,
      );
      const history = yield* events.readStream(controlledThreadReservationId, 0, 4);
      const catalog = yield* sql<ControlledThreadCatalogRow>`
        SELECT
          event_id AS "eventId", aggregate_kind AS "aggregateKind",
          stream_version AS "streamVersion", command_id AS "commandId",
          event_type AS "eventType",
          controlled_thread_reservation_id AS "controlledThreadReservationId",
          thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
          task_revision AS "taskRevision",
          github_intake_sequence AS "githubIntakeSequence",
          source_identity_fingerprint AS "sourceIdentityFingerprint",
          stage_run_id AS "stageRunId", attempt_id AS "attemptId",
          role_id AS "roleId", stage_kind AS "stageKind",
          stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
          lease_id AS "leaseId", fence_token AS "fenceToken",
          worktree_reservation_id AS "worktreeReservationId",
          prepared_at AS "preparedAt",
          coordinator_command_id AS "coordinatorCommandId",
          coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
          materializing_transition_command_id AS "materializingTransitionCommandId",
          materialization_command_id AS "materializationCommandId",
          materialization_command_fingerprint AS "materializationCommandFingerprint",
          lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
          bound_transition_command_id AS "boundTransitionCommandId",
          orchestration_result_sequence AS "orchestrationResultSequence",
          materialized_at AS "materializedAt", bound_at AS "boundAt"
        FROM agent_control_controlled_thread_stream_catalog_all
        WHERE controlled_thread_reservation_id = ${controlledThreadReservationId}
        ORDER BY stream_version ASC
      `;
      if (
        (Option.isNone(state) && (history.length !== 0 || catalog.length !== 0)) ||
        (Option.isSome(state) &&
          (history.length !== state.value.revision ||
            catalog.length !== history.length ||
            history.some((event, index) => !sameCatalogEvent(catalog[index]!, event))))
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          projectId,
          taskId,
          controlledThreadReservationId,
        });
      }
      return Option.map(state, (current) => ({ current, history }));
    }).pipe(
      Effect.mapError((failure) =>
        isRpcError(failure)
          ? failure
          : mapHistoryError(failure, { projectId, taskId, controlledThreadReservationId }),
      ),
    );

  const loadState = (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    projectId: AgentControlControlledThreadReservationRpcError["projectId"],
    taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>,
  ) =>
    loadStateEvidence(controlledThreadReservationId, projectId, taskId).pipe(
      Effect.map(Option.map((evidence) => evidence.current)),
    );

  const validatePersistedBoundSuccessorEvidence = Effect.fn(
    "AgentControlControlledThreadReservationEngine.validatePersistedBoundSuccessorEvidence",
  )(function* (input: {
    readonly state: Extract<
      AgentControlControlledThreadReservationState,
      { readonly status: "bound" }
    >;
    readonly preparedEvent: Extract<
      AgentControlControlledThreadReservationEvent,
      { readonly type: "agentControl.controlledThreadReservation.prepared" }
    >;
  }) {
    const state = input.state;
    const prepared = input.preparedEvent;
    let orchestrationEventCount = 0;
    let orchestrationEventCursor = 0;
    while (true) {
      const page = yield* loadOrchestrationEventsByCommandIdPage(sql, {
        commandId: state.materializationCommandId,
        sequenceExclusive: orchestrationEventCursor,
        operationPrefix: "controlled-thread-bound-materialization-events",
      }).pipe(
        Effect.mapError(() =>
          rpcError("internal-persistence-error", {
            projectId: state.projectId,
            taskId: state.taskId,
            controlledThreadReservationId: state.controlledThreadReservationId,
          }),
        ),
      );
      orchestrationEventCount += page.rows.length;
      if (page.rows.length === 0) break;
      orchestrationEventCursor = page.nextSequenceExclusive;
    }
    if (orchestrationEventCount !== 2) return false;
    const rows = yield* sql<{ readonly valid: number }>`
      SELECT CASE WHEN
        (
          SELECT count(*)
          FROM agent_control_controlled_thread_materialization_intents
          WHERE coordinator_command_id = ${state.coordinatorCommandId}
             OR controlled_thread_reservation_id =
               ${state.controlledThreadReservationId}
        ) = 1
        AND (
          SELECT count(*)
          FROM agent_control_controlled_thread_materialization_receipts
          WHERE coordinator_command_id = ${state.coordinatorCommandId}
             OR controlled_thread_reservation_id =
               ${state.controlledThreadReservationId}
        ) = 1
        AND (
          SELECT count(*)
          FROM agent_control_controlled_thread_materialization_accepted
          WHERE coordinator_command_id = ${state.coordinatorCommandId}
             OR controlled_thread_reservation_id =
               ${state.controlledThreadReservationId}
        ) = 1
        AND EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_materialization_intents coordinator
          JOIN agent_control_controlled_thread_materialization_receipts receipt
            ON receipt.coordinator_command_id = coordinator.coordinator_command_id
          JOIN agent_control_controlled_thread_materialization_accepted accepted
            ON accepted.coordinator_command_id = coordinator.coordinator_command_id
          JOIN orchestration_agent_control_thread_materialization_intents orchestration
            ON orchestration.command_id = coordinator.materialization_command_id
          JOIN orchestration_command_receipts orchestration_receipt
            ON orchestration_receipt.command_id = orchestration.command_id
          JOIN orchestration_agent_control_thread_materialization_receipts
            orchestration_marker
            ON orchestration_marker.command_id = orchestration.command_id
          JOIN main.orchestration_events created
            ON created.event_id = orchestration.created_event_id
          JOIN main.orchestration_events binding
            ON binding.event_id = orchestration.binding_event_id
          JOIN projection_threads thread
            ON thread.thread_id = orchestration.thread_id
          WHERE coordinator.coordinator_command_id =
              ${state.coordinatorCommandId}
            AND coordinator.project_id = ${state.projectId}
            AND coordinator.controlled_thread_reservation_id =
              ${state.controlledThreadReservationId}
            AND coordinator.thread_id = ${state.threadId}
            AND coordinator.task_id = ${state.taskId}
            AND coordinator.task_revision = ${state.taskRevision}
            AND coordinator.github_intake_sequence =
              ${state.githubIntakeSequence}
            AND coordinator.source_identity_fingerprint =
              ${state.sourceIdentityFingerprint}
            AND coordinator.stage_run_id = ${state.stageRunId}
            AND coordinator.attempt_id = ${state.attemptId}
            AND coordinator.role_id = ${state.roleId}
            AND coordinator.stage_kind = ${state.stageKind}
            AND coordinator.stage_ordinal = ${state.stageOrdinal}
            AND coordinator.attempt_ordinal = ${state.attemptOrdinal}
            AND coordinator.lease_id = ${state.leaseId}
            AND coordinator.fence_token = ${state.fenceToken}
            AND coordinator.worktree_reservation_id =
              ${state.worktreeReservationId}
            AND coordinator.coordinator_command_fingerprint =
              ${state.coordinatorCommandFingerprint}
            AND coordinator.materializing_transition_command_id =
              ${state.materializingTransitionCommandId}
            AND coordinator.materialization_command_id =
              ${state.materializationCommandId}
            AND coordinator.materialization_command_fingerprint =
              ${state.materializationCommandFingerprint}
            AND coordinator.bound_transition_command_id =
              ${state.boundTransitionCommandId}
            AND coordinator.lease_holder_id = ${state.leaseHolderId}
            AND coordinator.materializing_at = ${state.materializingAt}
            AND coordinator.materialized_at = ${state.materializedAt}
            AND coordinator.bound_at = ${state.boundAt}
            AND coordinator.orchestration_result_sequence =
              ${state.orchestrationResultSequence}
            AND coordinator.materializing_event_id = (
              SELECT event_id
              FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND stream_id = ${state.controlledThreadReservationId}
                AND stream_version = 2
            )
            AND coordinator.bound_event_id = (
              SELECT event_id
              FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND stream_id = ${state.controlledThreadReservationId}
                AND stream_version = 3
            )
            AND coordinator.materializing_event_sequence = (
              SELECT sequence
              FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND stream_id = ${state.controlledThreadReservationId}
                AND stream_version = 2
            )
            AND coordinator.bound_event_sequence = ${state.sequence}
            AND coordinator.accepted_marker_command_id =
              coordinator.coordinator_command_id
            AND receipt.request_fingerprint = coordinator.request_fingerprint
            AND receipt.coordinator_command_fingerprint =
              coordinator.coordinator_command_fingerprint
            AND receipt.controlled_thread_reservation_id =
              coordinator.controlled_thread_reservation_id
            AND receipt.thread_id = coordinator.thread_id
            AND receipt.materialization_command_id =
              coordinator.materialization_command_id
            AND receipt.materialization_command_fingerprint =
              coordinator.materialization_command_fingerprint
            AND receipt.orchestration_result_sequence =
              coordinator.orchestration_result_sequence
            AND receipt.status = 'accepted'
            AND receipt.accepted_at = coordinator.accepted_at
            AND receipt.accepted_marker_command_id =
              coordinator.coordinator_command_id
            AND accepted.coordinator_command_fingerprint =
              coordinator.coordinator_command_fingerprint
            AND accepted.controlled_thread_reservation_id =
              coordinator.controlled_thread_reservation_id
            AND accepted.thread_id = coordinator.thread_id
            AND accepted.materialization_command_id =
              coordinator.materialization_command_id
            AND accepted.materialization_command_fingerprint =
              coordinator.materialization_command_fingerprint
            AND accepted.orchestration_result_sequence =
              coordinator.orchestration_result_sequence
            AND accepted.accepted_at = coordinator.accepted_at
            AND accepted.finalization_owner_id =
              coordinator.finalization_owner_id
            AND orchestration.command_type =
              'thread.agent-control.materialize'
            AND orchestration.authority = 'agent-control'
            AND orchestration.aggregate_kind = 'thread'
            AND orchestration.command_fingerprint =
              coordinator.materialization_command_fingerprint
            AND orchestration.controlled_thread_reservation_id =
              coordinator.controlled_thread_reservation_id
            AND orchestration.thread_id = coordinator.thread_id
            AND orchestration.project_id = coordinator.project_id
            AND orchestration.task_id = coordinator.task_id
            AND orchestration.task_revision = coordinator.task_revision
            AND orchestration.github_intake_sequence =
              coordinator.github_intake_sequence
            AND orchestration.source_identity_fingerprint =
              coordinator.source_identity_fingerprint
            AND orchestration.stage_run_id = coordinator.stage_run_id
            AND orchestration.attempt_id = coordinator.attempt_id
            AND orchestration.role_id = coordinator.role_id
            AND orchestration.stage_kind = coordinator.stage_kind
            AND orchestration.stage_ordinal = coordinator.stage_ordinal
            AND orchestration.attempt_ordinal = coordinator.attempt_ordinal
            AND orchestration.lease_id = coordinator.lease_id
            AND orchestration.fence_token = coordinator.fence_token
            AND orchestration.worktree_reservation_id =
              coordinator.worktree_reservation_id
            AND orchestration.title = coordinator.title
            AND json(orchestration.model_selection_json) =
              json(coordinator.model_selection_json)
            AND orchestration.runtime_mode = coordinator.runtime_mode
            AND orchestration.interaction_mode = coordinator.interaction_mode
            AND orchestration.branch = coordinator.branch
            AND orchestration.worktree_path = coordinator.worktree_path
            AND json(orchestration.binding_json) =
              json(coordinator.binding_json)
            AND orchestration.receipt_status = 'accepted'
            AND orchestration.receipt_result_sequence =
              coordinator.orchestration_result_sequence
            AND orchestration.receipt_accepted_at =
              coordinator.materialized_at
            AND orchestration.created_at = coordinator.materialized_at
            AND orchestration.accepted_receipt_command_id =
              orchestration.command_id
            AND orchestration.created_event_type = 'thread.created'
            AND orchestration.created_event_stream_version = 1
            AND orchestration.binding_event_type =
              'thread.agent-control-bound'
            AND orchestration.binding_event_stream_version = 2
            AND orchestration.binding_event_sequence =
              orchestration.created_event_sequence + 1
            AND orchestration_receipt.authority = 'agent-control'
            AND orchestration_receipt.aggregate_kind = 'thread'
            AND orchestration_receipt.aggregate_id = orchestration.thread_id
            AND orchestration_receipt.status = 'accepted'
            AND orchestration_receipt.result_sequence =
              orchestration.receipt_result_sequence
            AND orchestration_receipt.accepted_at =
              orchestration.receipt_accepted_at
            AND orchestration_marker.command_type =
              orchestration.command_type
            AND orchestration_marker.authority = orchestration.authority
            AND orchestration_marker.aggregate_kind =
              orchestration.aggregate_kind
            AND orchestration_marker.thread_id = orchestration.thread_id
            AND orchestration_marker.command_fingerprint =
              orchestration.command_fingerprint
            AND orchestration_marker.result_sequence =
              orchestration.receipt_result_sequence
            AND orchestration_marker.accepted_at =
              orchestration.receipt_accepted_at
            AND orchestration_marker.status = 'accepted'
            AND created.command_id = orchestration.command_id
            AND created.aggregate_kind = 'thread'
            AND created.stream_id = orchestration.thread_id
            AND created.stream_version = 1
            AND created.event_type = 'thread.created'
            AND created.sequence = orchestration.created_event_sequence
            AND created.occurred_at = orchestration.created_at
            AND created.correlation_id = orchestration.command_id
            AND created.causation_event_id IS NULL
            AND binding.command_id = orchestration.command_id
            AND binding.aggregate_kind = 'thread'
            AND binding.stream_id = orchestration.thread_id
            AND binding.stream_version = 2
            AND binding.event_type = 'thread.agent-control-bound'
            AND binding.sequence = orchestration.binding_event_sequence
            AND binding.occurred_at = orchestration.created_at
            AND binding.correlation_id = orchestration.command_id
            AND binding.causation_event_id IS NULL
            AND json_valid(orchestration.binding_json) = 1
            AND json_extract(orchestration.binding_json, '$.taskId') =
              ${prepared.payload.taskId}
            AND json_extract(orchestration.binding_json, '$.stageRunId') =
              ${prepared.payload.stageRunId}
            AND json_extract(orchestration.binding_json, '$.attemptId') =
              ${prepared.payload.attemptId}
            AND json_extract(orchestration.binding_json, '$.roleId') =
              ${prepared.payload.roleId}
            AND json_extract(
              orchestration.binding_json, '$.controlState'
            ) = 'controlled'
            AND json_valid(binding.payload_json) = 1
            AND json(json_extract(binding.payload_json, '$.binding')) =
              json(orchestration.binding_json)
            AND json_extract(binding.payload_json, '$.threadId') =
              orchestration.thread_id
            AND json_valid(created.payload_json) = 1
            AND json_extract(created.payload_json, '$.threadId') =
              orchestration.thread_id
            AND json_extract(created.payload_json, '$.projectId') =
              orchestration.project_id
            AND json_extract(created.payload_json, '$.title') =
              orchestration.title
            AND json(json_extract(
              created.payload_json, '$.modelSelection'
            )) = json(orchestration.model_selection_json)
            AND json_extract(created.payload_json, '$.runtimeMode') =
              orchestration.runtime_mode
            AND json_extract(created.payload_json, '$.interactionMode') =
              orchestration.interaction_mode
            AND json_extract(created.payload_json, '$.branch') =
              orchestration.branch
            AND json_extract(created.payload_json, '$.worktreePath') =
              orchestration.worktree_path
            AND thread.project_id = orchestration.project_id
            AND json(thread.agent_control_json) =
              json(orchestration.binding_json)
            AND thread.deleted_at IS NULL
            AND (
              SELECT count(*)
              FROM orchestration_agent_control_thread_materialization_intents
                candidate
              WHERE candidate.command_id = orchestration.command_id
                 OR candidate.controlled_thread_reservation_id =
                   coordinator.controlled_thread_reservation_id
            ) = 1
            AND (
              SELECT count(*)
              FROM orchestration_command_receipts candidate
              WHERE candidate.command_id = orchestration.command_id
            ) = 1
            AND (
              SELECT count(*)
              FROM projection_threads candidate
              WHERE candidate.thread_id = orchestration.thread_id
            ) = 1
        )
        THEN 1 ELSE 0 END AS valid
    `.pipe(
      Effect.mapError(() =>
        rpcError("internal-persistence-error", {
          projectId: state.projectId,
          taskId: state.taskId,
          controlledThreadReservationId: state.controlledThreadReservationId,
        }),
      ),
    );
    if (rows.length !== 1 || rows[0]?.valid !== 1) return false;
    const fingerprintEvidence = yield* sql<PersistedControlledThreadCoordinatorFingerprintEvidence>`
        SELECT
          coordinator_command_id AS "coordinatorCommandId",
          request_fingerprint AS "requestFingerprint",
          coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
          policy_binding_fingerprint AS "policyBindingFingerprint",
          runtime_observation_fingerprint AS "runtimeObservationFingerprint",
          project_id AS "projectId",
          controlled_thread_reservation_id AS
            "controlledThreadReservationId",
          thread_id AS "threadId",
          task_id AS "taskId",
          task_revision AS "taskRevision",
          github_intake_sequence AS "githubIntakeSequence",
          source_identity_fingerprint AS "sourceIdentityFingerprint",
          stage_run_id AS "stageRunId",
          attempt_id AS "attemptId",
          role_id AS "roleId",
          stage_kind AS "stageKind",
          stage_ordinal AS "stageOrdinal",
          attempt_ordinal AS "attemptOrdinal",
          lease_id AS "leaseId",
          lease_holder_id AS "leaseHolderId",
          fence_token AS "fenceToken",
          worktree_reservation_id AS "worktreeReservationId",
          materialization_command_id AS "materializationCommandId",
          materialization_command_fingerprint AS
            "materializationCommandFingerprint",
          title,
          model_selection_json AS "modelSelectionJson",
          runtime_mode AS "runtimeMode",
          interaction_mode AS "interactionMode",
          branch,
          worktree_path AS "worktreePath",
          binding_json AS "bindingJson",
          materialized_at AS "materializedAt"
        FROM agent_control_controlled_thread_materialization_intents
        WHERE coordinator_command_id = ${state.coordinatorCommandId}
          AND controlled_thread_reservation_id =
            ${state.controlledThreadReservationId}
      `.pipe(
      Effect.mapError(() =>
        rpcError("internal-persistence-error", {
          projectId: state.projectId,
          taskId: state.taskId,
          controlledThreadReservationId: state.controlledThreadReservationId,
        }),
      ),
    );
    return (
      fingerprintEvidence.length === 1 &&
      (yield* validateCanonicalControlledThreadCoordinatorFingerprints(
        crypto,
        fingerprintEvidence[0]!,
      ))
    );
  });

  const loadPrepareFinalization = Effect.fn(
    "AgentControlControlledThreadReservationEngine.loadPrepareFinalization",
  )(function* (
    input: PrepareFinalizationInput,
  ): Effect.fn.Return<PrepareFinalizationRow, AgentControlControlledThreadReservationRpcError> {
    yield* transactionHooks.beforePrepareFinalizationRead ?? Effect.void;
    const rows = yield* sql<PrepareFinalizationRow>`
      SELECT
        finalization.prepare_command_id AS "prepareCommandId",
        finalization.prepare_command_fingerprint AS "prepareCommandFingerprint",
        finalization.project_id AS "projectId",
        finalization.task_id AS "taskId",
        finalization.controlled_thread_reservation_id AS
          "controlledThreadReservationId",
        finalization.prepared_event_id AS "preparedEventId",
        finalization.prepared_stream_version AS "preparedStreamVersion",
        finalization.prepared_event_sequence AS "preparedEventSequence",
        finalization.receipt_command_id AS "receiptCommandId",
        finalization.receipt_status AS "receiptStatus",
        finalization.receipt_result_sequence AS "receiptResultSequence",
        finalization.receipt_result_stream_version AS
          "receiptResultStreamVersion",
        finalization.receipt_event_created AS "receiptEventCreated",
        finalization.receipt_accepted_at AS "receiptAcceptedAt",
        finalization.finalization_owner_id AS "finalizationOwnerId",
        finalization.status,
        finalization.revision,
        finalization.claimed_at AS "claimedAt",
        finalization.completed_at AS "completedAt",
        intent.request_fingerprint AS "intentFingerprint",
        intent.command_type AS "intentCommandType",
        intent.authority AS "intentAuthority",
        intent.aggregate_kind AS "intentAggregateKind",
        intent.aggregate_id AS "intentAggregateId",
        event.command_id AS "eventCommandId",
        event.sequence AS "eventSequence",
        event.stream_version AS "eventStreamVersion",
        event.event_type AS "eventType",
        event.correlation_id AS "eventCorrelationId",
        event.actor_authority AS "eventActorAuthority",
        receipt.command_fingerprint AS "receiptFingerprint",
        receipt.aggregate_kind AS "receiptAggregateKind",
        receipt.aggregate_id AS "receiptAggregateId",
        receipt.status AS "persistedReceiptStatus",
        receipt.result_sequence AS "persistedReceiptResultSequence",
        receipt.result_stream_version AS "persistedReceiptResultStreamVersion",
        receipt.event_created AS "persistedReceiptEventCreated",
        receipt.accepted_at AS "persistedReceiptAcceptedAt"
      FROM agent_control_controlled_thread_prepare_finalizations finalization
      JOIN agent_control_controlled_thread_prepare_acceptance_obligations obligation
        ON obligation.prepare_command_id = finalization.prepare_command_id
       AND obligation.prepare_command_fingerprint =
         finalization.prepare_command_fingerprint
       AND obligation.authority = finalization.authority
       AND obligation.aggregate_kind = finalization.aggregate_kind
       AND obligation.project_id = finalization.project_id
       AND obligation.task_id = finalization.task_id
       AND obligation.controlled_thread_reservation_id =
         finalization.controlled_thread_reservation_id
       AND obligation.receipt_command_id = finalization.receipt_command_id
       AND obligation.receipt_status = finalization.receipt_status
       AND obligation.receipt_result_sequence =
         finalization.receipt_result_sequence
       AND obligation.receipt_result_stream_version =
         finalization.receipt_result_stream_version
       AND obligation.receipt_event_created =
         finalization.receipt_event_created
       AND obligation.receipt_accepted_at = finalization.receipt_accepted_at
      JOIN agent_control_controlled_thread_prepare_accepted_evidence evidence
        ON evidence.prepare_command_id = finalization.prepare_command_id
       AND evidence.prepare_command_fingerprint =
         finalization.prepare_command_fingerprint
       AND evidence.authority = finalization.authority
       AND evidence.aggregate_kind = finalization.aggregate_kind
       AND evidence.project_id = finalization.project_id
       AND evidence.task_id = finalization.task_id
       AND evidence.controlled_thread_reservation_id =
         finalization.controlled_thread_reservation_id
       AND evidence.prepared_event_id = finalization.prepared_event_id
       AND evidence.prepared_stream_version =
         finalization.prepared_stream_version
       AND evidence.prepared_event_sequence =
         finalization.prepared_event_sequence
       AND evidence.receipt_command_id = finalization.receipt_command_id
       AND evidence.receipt_status = finalization.receipt_status
       AND evidence.receipt_result_sequence =
         finalization.receipt_result_sequence
       AND evidence.receipt_result_stream_version =
         finalization.receipt_result_stream_version
       AND evidence.receipt_event_created = finalization.receipt_event_created
       AND evidence.receipt_accepted_at = finalization.receipt_accepted_at
      JOIN agent_control_controlled_thread_prepare_final_commit_markers marker
        ON marker.prepare_command_id = finalization.prepare_command_id
       AND marker.prepare_command_fingerprint =
         finalization.prepare_command_fingerprint
       AND marker.authority = finalization.authority
       AND marker.aggregate_kind = finalization.aggregate_kind
       AND marker.project_id = finalization.project_id
       AND marker.task_id = finalization.task_id
       AND marker.controlled_thread_reservation_id =
         finalization.controlled_thread_reservation_id
       AND marker.prepared_event_id = finalization.prepared_event_id
       AND marker.prepared_stream_version =
         finalization.prepared_stream_version
       AND marker.prepared_event_sequence =
         finalization.prepared_event_sequence
       AND marker.receipt_command_id = finalization.receipt_command_id
       AND marker.receipt_status = finalization.receipt_status
       AND marker.receipt_result_sequence =
         finalization.receipt_result_sequence
       AND marker.receipt_result_stream_version =
         finalization.receipt_result_stream_version
       AND marker.receipt_event_created = finalization.receipt_event_created
       AND marker.receipt_accepted_at = finalization.receipt_accepted_at
       AND marker.finalization_owner_id =
         finalization.initial_finalization_owner_id
       AND marker.finalization_status = finalization.initial_status
       AND marker.finalization_revision = finalization.initial_revision
      JOIN agent_control_controlled_thread_command_intents intent
        ON intent.command_id = finalization.prepare_command_id
      JOIN agent_control_command_receipts receipt
        ON receipt.command_id = finalization.receipt_command_id
      JOIN agent_control_events event
        ON event.event_id = finalization.prepared_event_id
      LEFT JOIN agent_control_controlled_thread_prepare_legacy_acceptances legacy
        ON legacy.prepare_command_id = finalization.prepare_command_id
      WHERE (
          finalization.prepare_command_id = ${input.commandId}
          OR finalization.controlled_thread_reservation_id =
            ${input.controlledThreadReservationId}
        )
        AND legacy.prepare_command_id IS NULL
      ORDER BY finalization.prepare_command_id
    `.pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
    if (rows.length !== 1) {
      // Migration 050 deliberately leaves legacy accepted receipts without an
      // outbox row. They are not safe to hot-republish and therefore fail
      // closed instead of being silently upgraded into recoverable evidence.
      return yield* rpcError("controlled-thread-reservation-corrupt", input);
    }
    const row = rows[0]!;
    if (
      row.prepareCommandId !== input.commandId ||
      row.prepareCommandFingerprint !== input.commandFingerprint ||
      row.projectId !== input.projectId ||
      row.taskId !== input.taskId ||
      row.controlledThreadReservationId !== input.controlledThreadReservationId ||
      row.preparedEventId !== input.preparedEvent.eventId ||
      row.preparedStreamVersion !== 1 ||
      row.preparedEventSequence !== input.preparedEvent.sequence ||
      row.receiptCommandId !== input.commandId ||
      row.receiptStatus !== "accepted" ||
      row.receiptResultSequence !== input.preparedEvent.sequence ||
      row.receiptResultStreamVersion !== 1 ||
      row.receiptEventCreated !== 1 ||
      row.receiptAcceptedAt !== input.preparedEvent.occurredAt ||
      row.intentFingerprint !== input.commandFingerprint ||
      row.intentCommandType !== "agentControl.controlledThreadReservation.prepare" ||
      row.intentAuthority !== "controller" ||
      row.intentAggregateKind !== "controlled-thread-reservation" ||
      row.intentAggregateId !== input.controlledThreadReservationId ||
      row.eventCommandId !== input.commandId ||
      row.eventSequence !== input.preparedEvent.sequence ||
      row.eventStreamVersion !== 1 ||
      row.eventType !== "agentControl.controlledThreadReservation.prepared" ||
      row.eventCorrelationId !== input.commandId ||
      row.eventActorAuthority !== "controller" ||
      row.receiptFingerprint !== input.commandFingerprint ||
      row.receiptAggregateKind !== "controlled-thread-reservation" ||
      row.receiptAggregateId !== input.controlledThreadReservationId ||
      row.persistedReceiptStatus !== "accepted" ||
      row.persistedReceiptResultSequence !== input.preparedEvent.sequence ||
      row.persistedReceiptResultStreamVersion !== 1 ||
      row.persistedReceiptEventCreated !== 1 ||
      row.persistedReceiptAcceptedAt !== input.preparedEvent.occurredAt ||
      !Number.isInteger(row.revision) ||
      !(
        (row.status === "pending" &&
          row.revision === 0 &&
          row.claimedAt === null &&
          row.completedAt === null) ||
        (row.status === "claimed" &&
          row.revision >= 1 &&
          row.claimedAt !== null &&
          row.completedAt === null) ||
        (row.status === "completed" &&
          row.revision >= 2 &&
          row.claimedAt !== null &&
          row.completedAt !== null)
      )
    ) {
      return yield* rpcError("controlled-thread-reservation-corrupt", input);
    }
    return row;
  });

  const claimPrepareFinalization: (
    input: PrepareFinalizationInput,
    row: PrepareFinalizationRow,
  ) => Effect.Effect<PrepareFinalizationRow, AgentControlControlledThreadReservationRpcError> =
    Effect.fn("AgentControlControlledThreadReservationEngine.claimPrepareFinalization")(function* (
      input: PrepareFinalizationInput,
      row: PrepareFinalizationRow,
    ): Effect.fn.Return<PrepareFinalizationRow, AgentControlControlledThreadReservationRpcError> {
      if (
        row.status === "completed" ||
        (row.status === "claimed" && row.finalizationOwnerId === prepareFinalizationOwnerId)
      ) {
        return row;
      }
      if (
        row.finalizationOwnerId !== prepareFinalizationOwnerId &&
        row.status !== "completed" &&
        livePrepareFinalizationOwners.has(row.finalizationOwnerId)
      ) {
        for (let attempt = 0; attempt < 250; attempt += 1) {
          yield* Effect.sleep(1);
          const current = yield* loadPrepareFinalization(input);
          if (
            current.status === "completed" ||
            current.finalizationOwnerId === prepareFinalizationOwnerId
          ) {
            return current;
          }
          if (!livePrepareFinalizationOwners.has(current.finalizationOwnerId)) {
            return yield* claimPrepareFinalization(input, current);
          }
        }
        return yield* rpcError("internal-persistence-error", input);
      }
      const claimedAt = DateTime.formatIso(yield* DateTime.now);
      const updated = yield* sql
        .withTransaction(sql<PrepareFinalizationRow>`
        UPDATE agent_control_controlled_thread_prepare_finalizations
      SET finalization_owner_id = ${prepareFinalizationOwnerId},
          status = 'claimed',
          revision = revision + 1,
          claimed_at = ${claimedAt}
      WHERE prepare_command_id = ${row.prepareCommandId}
        AND prepare_command_fingerprint = ${row.prepareCommandFingerprint}
        AND project_id = ${row.projectId}
        AND task_id = ${row.taskId}
        AND controlled_thread_reservation_id = ${row.controlledThreadReservationId}
        AND prepared_event_id = ${row.preparedEventId}
        AND prepared_stream_version = ${row.preparedStreamVersion}
        AND prepared_event_sequence = ${row.preparedEventSequence}
        AND receipt_command_id = ${row.receiptCommandId}
        AND receipt_result_sequence = ${row.receiptResultSequence}
        AND receipt_result_stream_version = ${row.receiptResultStreamVersion}
        AND finalization_owner_id = ${row.finalizationOwnerId}
        AND status = ${row.status}
        AND revision = ${row.revision}
      RETURNING
        prepare_command_id AS "prepareCommandId",
        prepare_command_fingerprint AS "prepareCommandFingerprint",
        project_id AS "projectId", task_id AS "taskId",
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        prepared_event_id AS "preparedEventId",
        prepared_stream_version AS "preparedStreamVersion",
        prepared_event_sequence AS "preparedEventSequence",
        receipt_command_id AS "receiptCommandId",
        receipt_status AS "receiptStatus",
        receipt_result_sequence AS "receiptResultSequence",
        receipt_result_stream_version AS "receiptResultStreamVersion",
        receipt_event_created AS "receiptEventCreated",
        receipt_accepted_at AS "receiptAcceptedAt",
        finalization_owner_id AS "finalizationOwnerId",
        status, revision, claimed_at AS "claimedAt", completed_at AS "completedAt",
        '' AS "intentFingerprint", '' AS "intentCommandType",
        '' AS "intentAuthority", '' AS "intentAggregateKind", '' AS "intentAggregateId",
        '' AS "eventCommandId", prepared_event_sequence AS "eventSequence",
        prepared_stream_version AS "eventStreamVersion",
        '' AS "eventType", '' AS "eventCorrelationId", '' AS "eventActorAuthority",
        '' AS "receiptFingerprint", '' AS "receiptAggregateKind",
        '' AS "receiptAggregateId", receipt_status AS "persistedReceiptStatus",
        receipt_result_sequence AS "persistedReceiptResultSequence",
        receipt_result_stream_version AS "persistedReceiptResultStreamVersion",
        receipt_event_created AS "persistedReceiptEventCreated",
          receipt_accepted_at AS "persistedReceiptAcceptedAt"
      `)
        .pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
      if (updated.length !== 1) {
        return yield* loadPrepareFinalization(input).pipe(
          Effect.flatMap((current) => claimPrepareFinalization(input, current)),
        );
      }
      return yield* loadPrepareFinalization(input);
    });

  const finalizePrepare = Effect.fn(
    "AgentControlControlledThreadReservationEngine.finalizePrepare",
  )(function* (
    input: PrepareFinalizationInput,
  ): Effect.fn.Return<void, AgentControlControlledThreadReservationRpcError> {
    let row = yield* loadPrepareFinalization(input);
    if (row.status === "completed") {
      yield* transactionHooks.beforePrepareReservationRefresh ?? Effect.void;
      yield* loadStateEvidence(input.controlledThreadReservationId, input.projectId, input.taskId);
      return;
    }
    row = yield* claimPrepareFinalization(input, row);
    if (row.status === "completed") {
      yield* transactionHooks.beforePrepareReservationRefresh ?? Effect.void;
      yield* loadStateEvidence(input.controlledThreadReservationId, input.projectId, input.taskId);
      return;
    }
    if (row.status !== "claimed" || row.finalizationOwnerId !== prepareFinalizationOwnerId) {
      return yield* rpcError("internal-persistence-error", input);
    }

    const exits: Array<Exit.Exit<void, AgentControlControlledThreadReservationRpcError>> = [];
    exits.push(yield* Effect.exit(transactionHooks.beforePrepareReservationRefresh ?? Effect.void));
    const refresh = yield* Effect.exit(
      loadStateEvidence(input.controlledThreadReservationId, input.projectId, input.taskId).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(rpcError("controlled-thread-reservation-corrupt", input)),
            onSome: ({ history }) =>
              history[0]?.eventId === input.preparedEvent.eventId
                ? Effect.void
                : Effect.fail(rpcError("controlled-thread-reservation-corrupt", input)),
          }),
        ),
      ),
    );
    exits.push(refresh);
    exits.push(yield* Effect.exit(transactionHooks.beforePreparePublication ?? Effect.void));

    let publicationSucceeded = locallyPublishedPrepareEventIds.has(input.preparedEvent.eventId);
    if (Exit.isSuccess(refresh) && !publicationSucceeded) {
      const publication = yield* Effect.exit(
        PubSub.publish(eventPubSub, input.preparedEvent).pipe(Effect.asVoid),
      );
      exits.push(publication);
      if (Exit.isSuccess(publication)) {
        locallyPublishedPrepareEventIds.add(input.preparedEvent.eventId);
        publicationSucceeded = true;
      }
    }
    exits.push(
      yield* Effect.exit(transactionHooks.afterPreparePublicationBeforeCompletion ?? Effect.void),
    );
    exits.push(yield* Effect.exit(transactionHooks.beforePrepareCompletionCas ?? Effect.void));

    if (Exit.isSuccess(refresh) && publicationSucceeded) {
      const completedAt = DateTime.formatIso(yield* DateTime.now);
      const completed = yield* Effect.exit(
        sql
          .withTransaction(sql<{ readonly revision: number }>`
            UPDATE agent_control_controlled_thread_prepare_finalizations
          SET status = 'completed',
              revision = revision + 1,
              completed_at = ${completedAt}
          WHERE prepare_command_id = ${row.prepareCommandId}
            AND prepare_command_fingerprint = ${row.prepareCommandFingerprint}
            AND project_id = ${row.projectId}
            AND task_id = ${row.taskId}
            AND controlled_thread_reservation_id = ${row.controlledThreadReservationId}
            AND prepared_event_id = ${row.preparedEventId}
            AND prepared_stream_version = ${row.preparedStreamVersion}
            AND prepared_event_sequence = ${row.preparedEventSequence}
            AND receipt_command_id = ${row.receiptCommandId}
            AND receipt_result_sequence = ${row.receiptResultSequence}
            AND receipt_result_stream_version = ${row.receiptResultStreamVersion}
            AND finalization_owner_id = ${row.finalizationOwnerId}
            AND status = 'claimed'
            AND revision = ${row.revision}
            RETURNING revision
          `)
          .pipe(
            Effect.flatMap((updated) =>
              updated.length === 1
                ? Effect.void
                : Effect.fail(rpcError("controlled-thread-reservation-corrupt", input)),
            ),
            Effect.catchTag("SqlError", () =>
              Effect.fail(rpcError("internal-persistence-error", input)),
            ),
          ),
      );
      exits.push(completed);
    }
    return yield* Exit.asVoidAll(exits);
  });

  const replayState = Effect.fn("AgentControlControlledThreadReservationEngine.replayState")(
    function* (input: {
      readonly commandId: AgentControlControlledThreadReservationCommand["commandId"];
      readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
      readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
      readonly commandFingerprint: string;
      readonly command?: AgentControlControlledThreadReservationCommand;
      readonly initialReplay?: true;
    }) {
      const receipt = yield* receipts
        .getByCommandId(input.commandId)
        .pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
      if (Option.isNone(receipt)) {
        return Option.none<{
          readonly currentState: AgentControlControlledThreadReservationState;
          readonly preparedState: AgentControlControlledThreadReservationState;
          readonly result: AgentControlControlledThreadReservationCommandResult;
          readonly preparedEvent: AgentControlControlledThreadReservationEvent;
          readonly history: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
        }>();
      }
      const value = receipt.value;
      const storedIntent = yield* loadControlledThreadCommandIntent(sql, input.commandId).pipe(
        Effect.mapError((failure) =>
          rpcError(
            failure._tag === "SqlError"
              ? "internal-persistence-error"
              : "controlled-thread-reservation-corrupt",
            input,
          ),
        ),
      );
      if (
        Option.isNone(storedIntent) ||
        value.commandFingerprint !== input.commandFingerprint ||
        value.authority !== "controller" ||
        value.aggregateKind !== "controlled-thread-reservation" ||
        storedIntent.value.commandId !== input.commandId ||
        storedIntent.value.requestFingerprint !== input.commandFingerprint ||
        storedIntent.value.authority !== "controller" ||
        storedIntent.value.aggregateKind !== "controlled-thread-reservation" ||
        storedIntent.value.aggregateId !== value.aggregateId ||
        storedIntent.value.projectId !== input.projectId ||
        storedIntent.value.taskId !== input.taskId
      ) {
        return yield* rpcError("command-identity-mismatch", input);
      }
      if (
        input.initialReplay === true &&
        ((value.status === "rejected" &&
          storedIntent.value.commandType !==
            "agentControl.controlledThreadReservation.prepareInitial") ||
          (value.status === "accepted" &&
            storedIntent.value.commandType !== "agentControl.controlledThreadReservation.prepare"))
      ) {
        return yield* rpcError("command-identity-mismatch", input);
      }
      if (input.command !== undefined) {
        if (input.command.authority !== "controller") {
          return yield* rpcError("command-identity-mismatch", input);
        }
        if (
          input.command.type !== "agentControl.controlledThreadReservation.prepare" &&
          input.command.type !== "agentControl.controlledThreadReservation.transition"
        ) {
          return yield* rpcError("command-identity-mismatch", input);
        }
        const expectedIntent = yield* internalControlledThreadCommandIntent(
          crypto,
          input.command,
          input.commandFingerprint,
        ).pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
        if (!sameControlledThreadCommandIntent(storedIntent.value, expectedIntent)) {
          return yield* rpcError("command-identity-mismatch", input);
        }
      }
      if (value.status === "rejected") {
        if (
          input.initialReplay === true &&
          storedIntent.value.aggregateId !==
            (yield* deriveRejectedAgentControlControlledThreadReservationId(input))
        ) {
          return yield* rpcError("command-identity-mismatch", input);
        }
        if (
          !isReservationCode(value.errorCode) ||
          value.resultSequence !== 0 ||
          value.resultStreamVersion !== 0 ||
          value.eventCreated
        ) {
          return yield* rpcError("controlled-thread-reservation-corrupt", input);
        }
        return yield* rpcError(value.errorCode, input);
      }
      const controlledThreadReservationId = yield* decodeReservationId(value.aggregateId).pipe(
        Effect.mapError(() => rpcError("controlled-thread-reservation-corrupt", input)),
      );
      const evidence = yield* loadStateEvidence(
        controlledThreadReservationId,
        input.projectId,
        input.taskId,
      ).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                rpcError("controlled-thread-reservation-missing", {
                  ...input,
                  controlledThreadReservationId,
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      );
      const state = evidence.current;
      const preparedEvent = evidence.history[0];
      if (
        preparedEvent === undefined ||
        preparedEvent.type !== "agentControl.controlledThreadReservation.prepared"
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          ...input,
          controlledThreadReservationId,
        });
      }
      const preparedState = yield* projectAgentControlControlledThreadReservationEvent(
        null,
        preparedEvent,
      ).pipe(
        Effect.mapError(() =>
          rpcError("controlled-thread-reservation-corrupt", {
            ...input,
            controlledThreadReservationId,
          }),
        ),
      );
      const historicalCommand = {
        type: "agentControl.controlledThreadReservation.prepare" as const,
        commandId: preparedEvent.commandId,
        authority: "controller" as const,
        ...preparedEvent.payload,
        expectedRevision: 0 as const,
      };
      const historicalIntent = yield* internalControlledThreadCommandIntent(
        crypto,
        historicalCommand,
        input.commandFingerprint,
      ).pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
      if (
        !sameControlledThreadCommandIntent(storedIntent.value, historicalIntent) ||
        preparedEvent.commandId !== input.commandId ||
        preparedState.controlledThreadReservationId !== controlledThreadReservationId ||
        preparedState.projectId !== input.projectId ||
        preparedState.taskId !== input.taskId ||
        state.projectId !== input.projectId ||
        state.taskId !== input.taskId ||
        value.resultStreamVersion !== 1 ||
        value.resultSequence !== preparedState.sequence ||
        !value.eventCreated ||
        value.acceptedAt !== preparedState.preparedAt ||
        value.errorCode !== null
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          ...input,
          controlledThreadReservationId,
        });
      }
      yield* validateAgentControlControlledThreadReservationState(state).pipe(
        Effect.mapError(() =>
          rpcError("controlled-thread-reservation-corrupt", {
            ...input,
            controlledThreadReservationId,
          }),
        ),
      );
      if (
        !(yield* validateCanonicalControlledThreadSuccessors({
          prepareCommandId: input.commandId,
          state,
          history: evidence.history,
        })) ||
        (state.status === "bound" &&
          !(yield* validatePersistedBoundSuccessorEvidence({
            state,
            preparedEvent,
          })))
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          ...input,
          controlledThreadReservationId,
        });
      }
      yield* loadPrepareFinalization({
        ...input,
        controlledThreadReservationId,
        preparedEvent,
      });
      return Option.some({
        currentState: state,
        preparedState,
        preparedEvent,
        history: evidence.history,
        result: {
          reservation: toAgentControlControlledThreadReservationView(preparedState),
          resultSequence: value.resultSequence,
          eventCreated: value.eventCreated,
        },
      });
    },
  );

  const replayReceiptFirst: AgentControlControlledThreadReservationEngineShape["replayReceiptFirst"] =
    (input) =>
      sql.withTransaction(replayState({ ...input, initialReplay: true })).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed(Option.none()),
            onSome: (replayed) =>
              finalizePrepare({
                ...input,
                controlledThreadReservationId: replayed.preparedState.controlledThreadReservationId,
                preparedEvent: replayed.preparedEvent,
              }).pipe(Effect.as(Option.some(replayed.result)), Effect.uninterruptible),
          }),
        ),
        Effect.catchTag("SqlError", () =>
          Effect.fail(rpcError("internal-persistence-error", input)),
        ),
      );

  const validateAcceptedReplayEvidence: AgentControlControlledThreadReservationEngineShape["validateAcceptedReplayEvidence"] =
    (input) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const prepareHistory = yield* events
              .readStream(input.controlledThreadReservationId, 0, 1)
              .pipe(
                Effect.mapError(() =>
                  rpcError("controlled-thread-reservation-corrupt", {
                    projectId: input.projectId,
                    taskId: "controlled-thread-reservation-replay" as never,
                    controlledThreadReservationId: input.controlledThreadReservationId,
                  }),
                ),
              );
            const preparedEvent = prepareHistory[0];
            if (
              prepareHistory.length !== 1 ||
              preparedEvent?.type !== "agentControl.controlledThreadReservation.prepared" ||
              preparedEvent.payload.projectId !== input.projectId ||
              preparedEvent.payload.controlledThreadReservationId !==
                input.controlledThreadReservationId
            ) {
              return yield* rpcError("controlled-thread-reservation-corrupt", {
                projectId: input.projectId,
                taskId:
                  preparedEvent?.payload.taskId ??
                  ("controlled-thread-reservation-replay" as never),
                controlledThreadReservationId: input.controlledThreadReservationId,
              });
            }
            const receipt = yield* receipts.getByCommandId(preparedEvent.commandId).pipe(
              Effect.mapError(() =>
                rpcError("internal-persistence-error", {
                  projectId: input.projectId,
                  taskId: preparedEvent.payload.taskId,
                  controlledThreadReservationId: input.controlledThreadReservationId,
                }),
              ),
            );
            if (Option.isNone(receipt)) {
              return yield* rpcError("controlled-thread-reservation-corrupt", {
                projectId: input.projectId,
                taskId: preparedEvent.payload.taskId,
                controlledThreadReservationId: input.controlledThreadReservationId,
              });
            }
            return yield* replayState({
              commandId: preparedEvent.commandId,
              projectId: input.projectId,
              taskId: preparedEvent.payload.taskId,
              commandFingerprint: receipt.value.commandFingerprint,
              initialReplay: true,
            }).pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () =>
                    Effect.fail(
                      rpcError("controlled-thread-reservation-corrupt", {
                        projectId: input.projectId,
                        taskId: preparedEvent.payload.taskId,
                        controlledThreadReservationId: input.controlledThreadReservationId,
                      }),
                    ),
                  onSome: (
                    evidence,
                  ): Effect.Effect<AgentControlControlledThreadAcceptedReplayEvidence> =>
                    Effect.succeed(evidence),
                }),
              ),
            );
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", () =>
            Effect.fail(
              rpcError("internal-persistence-error", {
                projectId: input.projectId,
                taskId: "controlled-thread-reservation-replay" as never,
                controlledThreadReservationId: input.controlledThreadReservationId,
              }),
            ),
          ),
        );

  const ensureDbAdmission = Effect.fn(
    "AgentControlControlledThreadReservationEngine.ensureDbAdmission",
  )(function* (command: AgentControlControlledThreadReservationPrepareCommand) {
    const useTaskConsumableInTransaction = taskGuard.useTaskConsumableInTransaction;
    if (useTaskConsumableInTransaction === undefined) {
      return yield* rpcError("internal-persistence-error", command);
    }
    return yield* useTaskConsumableInTransaction(command.projectId, command.taskId, (task) =>
      Effect.gen(function* () {
        const sourceIdentityFingerprint = yield* deriveAgentControlSourceIdentityFingerprint(task);
        if (
          task.revision !== command.taskRevision ||
          task.githubIntakeSequence !== command.githubIntakeSequence ||
          sourceIdentityFingerprint !== command.sourceIdentityFingerprint
        ) {
          return yield* rpcError("source-snapshot-stale", command);
        }
        const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
          command.projectId,
          command.taskId,
          stageEvents,
          stageStates,
        ).pipe(
          Effect.mapError((failure) =>
            rpcError(
              failure._tag === "AgentControlPersistenceSqlError"
                ? "internal-persistence-error"
                : "stage-run-projection-corrupt",
              command,
            ),
          ),
        );
        const stageMatches = stageHistory.filter(
          (stage) =>
            stage.stageRunId === command.stageRunId &&
            stage.attemptId === command.attemptId &&
            stage.roleId === command.roleId &&
            stage.stageKind === command.stageKind &&
            stage.stageOrdinal === command.stageOrdinal &&
            stage.attemptOrdinal === command.attemptOrdinal &&
            stage.taskRevision === command.taskRevision &&
            stage.githubIntakeSequence === command.githubIntakeSequence &&
            stage.sourceIdentityFingerprint === command.sourceIdentityFingerprint,
        );
        if (stageMatches.length === 0) return yield* rpcError("stage-run-missing", command);
        if (stageMatches.length !== 1) {
          return yield* rpcError("stage-run-history-ambiguous", command);
        }
        if (stageMatches[0]!.status !== "prepared") {
          return yield* rpcError("stage-run-not-prepared", command);
        }

        const leaseHistory = yield* loadAuthoritativeLeaseHistoryForStagePosition(
          {
            projectId: command.projectId,
            taskId: command.taskId,
            stageRunId: command.stageRunId,
            attemptId: command.attemptId,
            taskRevision: command.taskRevision,
            githubIntakeSequence: command.githubIntakeSequence,
            sourceIdentityFingerprint: command.sourceIdentityFingerprint,
          },
          leaseEvents,
          leaseStates,
        ).pipe(
          Effect.mapError((failure) =>
            rpcError(
              failure._tag === "AgentControlPersistenceSqlError"
                ? "internal-persistence-error"
                : "lease-projection-corrupt",
              command,
            ),
          ),
        );
        if (leaseHistory.length === 0) return yield* rpcError("lease-missing", command);
        if (leaseHistory.length !== 1) {
          return yield* rpcError("lease-projection-corrupt", command);
        }
        const leaseState = leaseHistory[0]!;
        if (leaseState.leaseId !== command.leaseId) {
          return yield* rpcError("lease-projection-corrupt", command);
        }
        if (leaseState.status !== "reserved") {
          return yield* rpcError("lease-not-reserved", command);
        }
        if (leaseState.holderId !== runtimeHolderId) {
          return yield* rpcError("lease-foreign-runtime", command);
        }
        if (leaseState.fenceToken !== command.fenceToken) {
          return yield* rpcError("fence-token-mismatch", command);
        }
        if (
          leaseState.projectId !== command.projectId ||
          leaseState.taskId !== command.taskId ||
          leaseState.stageRunId !== command.stageRunId ||
          leaseState.attemptId !== command.attemptId ||
          leaseState.taskRevision !== command.taskRevision ||
          leaseState.githubIntakeSequence !== command.githubIntakeSequence ||
          leaseState.sourceIdentityFingerprint !== command.sourceIdentityFingerprint
        ) {
          return yield* rpcError("source-snapshot-stale", command);
        }
        const expiresAt = canonicalTimestampMillis(leaseState.expiresAt);
        const now = yield* DateTime.now;
        if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
          return yield* rpcError("lease-expired", command);
        }

        const listed = yield* worktrees
          .listReservations({ projectId: command.projectId })
          .pipe(
            Effect.mapError((failure) =>
              rpcError(
                failure.code === "internal-persistence-error"
                  ? "internal-persistence-error"
                  : "worktree-projection-corrupt",
                command,
              ),
            ),
          );
        if (listed.quarantinedCount !== 0) {
          return yield* rpcError("worktree-projection-corrupt", command);
        }
        const matchingViews = listed.reservations.filter(
          (worktree) =>
            worktree.reservationId === command.worktreeReservationId &&
            worktree.projectId === command.projectId &&
            worktree.taskId === command.taskId &&
            worktree.stageRunId === command.stageRunId &&
            worktree.attemptId === command.attemptId &&
            worktree.leaseId === command.leaseId &&
            worktree.fenceToken === command.fenceToken,
        );
        if (matchingViews.length === 0) return yield* rpcError("worktree-missing", command);
        if (matchingViews.length !== 1) {
          return yield* rpcError("worktree-history-ambiguous", command);
        }
        if (matchingViews[0]!.status !== "ready") {
          return yield* rpcError("worktree-not-ready", command);
        }
        const worktree = yield* worktreeEngine
          .loadAuthoritative(command.worktreeReservationId)
          .pipe(
            Effect.mapError((failure) =>
              rpcError(
                failure.code === "internal-persistence-error"
                  ? "internal-persistence-error"
                  : "worktree-projection-corrupt",
                command,
              ),
            ),
          );
        if (worktree === null) return yield* rpcError("worktree-missing", command);
        if (worktree.status !== "ready") return yield* rpcError("worktree-not-ready", command);
        if (
          worktree.projectId !== command.projectId ||
          worktree.taskId !== command.taskId ||
          worktree.taskRevision !== command.taskRevision ||
          worktree.githubIntakeSequence !== command.githubIntakeSequence ||
          worktree.sourceIdentityFingerprint !== command.sourceIdentityFingerprint ||
          worktree.stageRunId !== command.stageRunId ||
          worktree.attemptId !== command.attemptId ||
          worktree.leaseId !== command.leaseId ||
          worktree.fenceToken !== command.fenceToken
        ) {
          return yield* rpcError("source-snapshot-stale", command);
        }
      }),
    ).pipe(
      Effect.mapError((failure) =>
        failure._tag === "AgentControlTaskConsumerGuardError"
          ? rpcError(guardCode(failure.reason), command)
          : failure,
      ),
    );
  });

  const insertRejected = Effect.fn("AgentControlControlledThreadReservationEngine.insertRejected")(
    function* (
      command: Extract<
        AgentControlControlledThreadReservationCommand,
        {
          type:
            | "agentControl.controlledThreadReservation.prepare"
            | "agentControl.controlledThreadReservation.transition";
        }
      >,
      commandFingerprint: string,
      code: AgentControlControlledThreadReservationRpcError["code"],
      _state: AgentControlControlledThreadReservationState | null,
      rejectedAt: string,
    ) {
      yield* insertControlledThreadCommandIntent(
        sql,
        yield* internalControlledThreadCommandIntent(crypto, command, commandFingerprint),
      );
      yield* receipts.insert({
        commandId: command.commandId,
        commandFingerprint,
        authority: "controller",
        aggregateKind: "controlled-thread-reservation",
        aggregateId: command.controlledThreadReservationId,
        status: "rejected",
        resultSequence: 0,
        resultStreamVersion: 0,
        eventCreated: false,
        acceptedAt: rejectedAt,
        errorCode: code as AgentControlRejectedCommandErrorCode,
      });
      return {
        _tag: "Rejected" as const,
        error: rpcError(code, command),
      };
    },
  );

  const dispatchPreparedController: AgentControlControlledThreadReservationEngineShape["dispatchPreparedController"] =
    (rawCommand, commandFingerprint) =>
      Effect.gen(function* () {
        const command = yield* decodeCommand(rawCommand).pipe(
          Effect.mapError(() => rpcError("validation", rawCommand)),
        );
        if (commandFingerprint.trim().length === 0) {
          return yield* rpcError("validation", command);
        }
        yield* transactionHooks.beforeDbAdmission;
        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const transactionExit = yield* Effect.exit(
              restore(
                sql
                  .withTransaction(
                    Effect.gen(function* () {
                      const replayAttempt = yield* Effect.result(
                        replayState({
                          commandId: command.commandId,
                          projectId: command.projectId,
                          taskId: command.taskId,
                          commandFingerprint,
                          command,
                        }),
                      );
                      if (replayAttempt._tag === "Failure") {
                        if (
                          replayAttempt.failure.code === "command-identity-mismatch" ||
                          replayAttempt.failure.code === "controlled-thread-reservation-corrupt" ||
                          replayAttempt.failure.code === "internal-persistence-error"
                        ) {
                          return yield* replayAttempt.failure;
                        }
                        return {
                          _tag: "Rejected" as const,
                          error: replayAttempt.failure,
                        } satisfies AgentControlControlledThreadReservationDispatchOutcome;
                      }
                      const replay = replayAttempt.success;
                      if (Option.isSome(replay)) {
                        if (
                          command.type !== "agentControl.controlledThreadReservation.prepare" ||
                          !sameCommandBinding(replay.value.preparedState, command)
                        ) {
                          return yield* rpcError("command-identity-mismatch", command);
                        }
                        return {
                          _tag: "Accepted" as const,
                          result: replay.value.result,
                          events: [],
                        } satisfies AgentControlControlledThreadReservationDispatchOutcome;
                      }
                      if (command.authority !== "controller") {
                        return yield* rpcError("validation", command);
                      }

                      const history = yield* validateTaskHistory(command.projectId, command.taskId);
                      const current =
                        history.find(
                          (state) =>
                            state.controlledThreadReservationId ===
                            command.controlledThreadReservationId,
                        ) ?? null;
                      const occurredAt = DateTime.formatIso(yield* DateTime.now);
                      if (command.type === "agentControl.controlledThreadReservation.transition") {
                        return yield* insertRejected(
                          command,
                          commandFingerprint,
                          "state-not-available",
                          current,
                          occurredAt,
                        );
                      }
                      if (command.type !== "agentControl.controlledThreadReservation.prepare") {
                        return yield* rpcError("state-not-available", command);
                      }

                      // This is the post-Git, pre-commit authority recheck. Every failure
                      // here is receiptless so a repaired/current observation may retry.
                      yield* ensureDbAdmission(command);
                      yield* transactionHooks.afterDbAdmission;

                      const decision = yield* Effect.result(
                        decideAgentControlControlledThreadReservationCommand({
                          state: current,
                          command,
                          eventId: EventId.make(
                            yield* crypto.randomUUIDv4.pipe(
                              Effect.mapError(() =>
                                rpcError("internal-persistence-error", command),
                              ),
                            ),
                          ),
                          occurredAt,
                        }),
                      );
                      if (decision._tag === "Failure") {
                        return yield* insertRejected(
                          command,
                          commandFingerprint,
                          decision.failure.code,
                          current,
                          occurredAt,
                        );
                      }
                      const appended =
                        decision.success.length === 0
                          ? []
                          : yield* transactionHooks.beforeEventAppend.pipe(
                              Effect.andThen(
                                events.append({
                                  controlledThreadReservationId:
                                    command.controlledThreadReservationId,
                                  expectedStreamVersion: 0,
                                  events: decision.success,
                                }),
                              ),
                            );
                      let next = current;
                      for (const event of appended) {
                        yield* projection.projectEvent(event);
                        next = yield* projectAgentControlControlledThreadReservationEvent(
                          next,
                          event,
                        );
                      }
                      if (next === null) {
                        return yield* rpcError("controlled-thread-reservation-missing", command);
                      }
                      yield* insertControlledThreadCommandIntent(
                        sql,
                        yield* internalControlledThreadCommandIntent(
                          crypto,
                          command,
                          commandFingerprint,
                        ),
                      );
                      yield* receipts.insert({
                        commandId: command.commandId,
                        commandFingerprint,
                        authority: "controller",
                        aggregateKind: "controlled-thread-reservation",
                        aggregateId: command.controlledThreadReservationId,
                        status: "accepted",
                        resultSequence: next.sequence,
                        resultStreamVersion: next.revision,
                        eventCreated: appended.length > 0,
                        acceptedAt: occurredAt,
                        errorCode: null,
                      });
                      yield* transactionHooks.afterWritesBeforeCommit;
                      const preparedEvent = appended[0];
                      if (
                        appended.length !== 1 ||
                        preparedEvent?.type !== "agentControl.controlledThreadReservation.prepared"
                      ) {
                        return yield* rpcError("controlled-thread-reservation-corrupt", command);
                      }
                      // Migration 050's prepare marker is the final application SQL
                      // statement. NodeSqliteClient binds this exact marker to the
                      // native COMMIT/return hook and rejects later transaction DML.
                      yield* sql`
              INSERT INTO agent_control_controlled_thread_prepare_finalizations (
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id,
                prepared_event_id, prepared_stream_version,
                prepared_event_sequence, receipt_command_id, receipt_status,
                receipt_result_sequence, receipt_result_stream_version,
                receipt_event_created, receipt_accepted_at,
                initial_finalization_owner_id, initial_status, initial_revision,
                finalization_owner_id, status, revision
              ) VALUES (
                ${command.commandId}, ${commandFingerprint},
                'controller', 'controlled-thread-reservation',
                ${command.projectId}, ${command.taskId},
                ${command.controlledThreadReservationId},
                ${preparedEvent.eventId},
                CAST(${preparedEvent.streamVersion} AS INTEGER),
                CAST(${preparedEvent.sequence} AS INTEGER),
                ${command.commandId}, 'accepted',
                CAST(${next.sequence} AS INTEGER),
                CAST(${next.revision} AS INTEGER), 1, ${occurredAt},
                ${prepareFinalizationOwnerId}, 'pending', 0,
                ${prepareFinalizationOwnerId}, 'pending', 0
              )
            `;
                      // The commit marker is the sole Prepare boundary observed
                      // by NodeSqliteClient and must remain the final
                      // application SQL statement in this transaction.
                      yield* sql`
              INSERT INTO
                agent_control_controlled_thread_prepare_final_commit_markers (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, prepared_event_id,
                  prepared_stream_version, prepared_event_sequence,
                  receipt_command_id, receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at, finalization_owner_id,
                  finalization_status, finalization_revision
                )
              SELECT
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                prepared_stream_version, prepared_event_sequence,
                receipt_command_id, receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at, initial_finalization_owner_id,
                initial_status, initial_revision
              FROM agent_control_controlled_thread_prepare_finalizations
              WHERE prepare_command_id = ${command.commandId}
            `;
                      return {
                        _tag: "Accepted" as const,
                        events: appended,
                        result: {
                          reservation: toAgentControlControlledThreadReservationView(next),
                          resultSequence: next.sequence,
                          eventCreated: appended.length > 0,
                        },
                      } satisfies AgentControlControlledThreadReservationDispatchOutcome;
                    }),
                  )
                  .pipe(
                    Effect.catchTag("SqlError", () =>
                      Effect.fail(rpcError("internal-persistence-error", command)),
                    ),
                  ),
              ),
            );
            if (Exit.isSuccess(transactionExit)) {
              return transactionExit.value;
            }
            const recoveryExit = yield* Effect.exit(
              sql
                .withTransaction(
                  replayState({
                    commandId: command.commandId,
                    projectId: command.projectId,
                    taskId: command.taskId,
                    commandFingerprint,
                    command,
                  }),
                )
                .pipe(
                  Effect.catchTag("SqlError", () =>
                    Effect.fail(rpcError("internal-persistence-error", command)),
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
            const finalizationExit = yield* Effect.exit(
              finalizePrepare({
                commandId: command.commandId,
                projectId: command.projectId,
                taskId: command.taskId,
                commandFingerprint,
                controlledThreadReservationId:
                  recovered.preparedState.controlledThreadReservationId,
                preparedEvent: recovered.preparedEvent,
              }),
            );
            if (Exit.isFailure(finalizationExit)) {
              return yield* Effect.failCause(
                Cause.combine(transactionExit.cause, finalizationExit.cause),
              );
            }
            return yield* Effect.failCause(transactionExit.cause);
          }),
        );
      }).pipe(
        Effect.tap((outcome) => {
          if (outcome._tag !== "Accepted") return Effect.void;
          return Effect.gen(function* () {
            const committed =
              outcome.events.length === 0
                ? yield* events.readStream(
                    outcome.result.reservation.controlledThreadReservationId,
                    0,
                    1,
                  )
                : outcome.events;
            const preparedEvent = committed[0];
            if (
              committed.length !== 1 ||
              preparedEvent?.type !== "agentControl.controlledThreadReservation.prepared"
            ) {
              return yield* rpcError("controlled-thread-reservation-corrupt", rawCommand);
            }
            yield* transactionHooks.afterPrepareOuterCommit ?? Effect.void;
            yield* finalizePrepare({
              commandId: preparedEvent.commandId,
              projectId: preparedEvent.payload.projectId,
              taskId: preparedEvent.payload.taskId,
              commandFingerprint,
              controlledThreadReservationId: preparedEvent.payload.controlledThreadReservationId,
              preparedEvent,
            });
          }).pipe(Effect.uninterruptible);
        }),
        Effect.mapError((failure) => {
          if (isRpcError(failure)) return failure;
          if (
            typeof failure === "object" &&
            failure !== null &&
            "_tag" in failure &&
            (failure._tag === "AgentControlPersistenceDecodeError" ||
              failure._tag === "AgentControlControlledThreadReservationStreamVersionConflictError")
          ) {
            return rpcError("controlled-thread-reservation-corrupt", rawCommand);
          }
          return rpcError("internal-persistence-error", rawCommand);
        }),
      );

  const getAuthoritative: AgentControlControlledThreadReservationEngineShape["getAuthoritative"] = (
    controlledThreadReservationId,
  ) =>
    loadState(
      controlledThreadReservationId,
      internalProjectId,
      "controlled-thread-reservation-internal" as never,
    );

  const refreshCommitted: AgentControlControlledThreadReservationEngineShape["refreshCommitted"] = (
    committed,
  ) =>
    Effect.gen(function* () {
      const last = committed.at(-1);
      if (last === undefined) return;
      const loaded = yield* getAuthoritative(last.payload.controlledThreadReservationId);
      if (
        Option.isNone(loaded) ||
        loaded.value.revision !== last.streamVersion ||
        loaded.value.sequence !== last.sequence
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          projectId: last.payload.projectId,
          taskId: last.payload.taskId,
          controlledThreadReservationId: last.payload.controlledThreadReservationId,
        });
      }
    });

  const publishCommitted: AgentControlControlledThreadReservationEngineShape["publishCommitted"] = (
    committed,
  ) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });

  return AgentControlControlledThreadReservationEngine.of({
    dispatchPreparedController,
    replayReceiptFirst,
    validateAcceptedReplayEvidence,
    getAuthoritative,
    validateTaskHistory,
    refreshCommitted,
    publishCommitted,
    rebuild: projection.rebuild.pipe(
      Effect.mapError((failure) =>
        mapHistoryError(failure, {
          projectId: internalProjectId,
          taskId: "controlled-thread-reservation-internal" as never,
        }),
      ),
    ),
    streamDomainEvents: Stream.fromPubSub(eventPubSub),
  });
});

export const layer = Layer.effect(AgentControlControlledThreadReservationEngine, make);
