import {
  AgentControlControlledThreadReservationCommand,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationRejectedCommandCode,
  AgentControlControlledThreadReservationRpcError,
  type AgentControlControlledThreadReservationCommandResult,
  type AgentControlControlledThreadReservationPrepareCommand,
  type AgentControlControlledThreadReservationState,
  type AgentControlControlledThreadReservationView,
  type AgentControlRejectedCommandErrorCode,
  EventId,
  ProjectId as ProjectIdSchema,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
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
  AgentControlControlledThreadReservationEngine,
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

const decodeCommand = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationCommand);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationId);
const isRpcError = Schema.is(AgentControlControlledThreadReservationRpcError);
const isReservationCode = Schema.is(AgentControlControlledThreadReservationRejectedCommandCode);
const internalProjectId = ProjectIdSchema.make(
  "agent-control-controlled-thread-reservation-internal",
);

interface ControlledThreadCatalogRow {
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
}

const sameCatalogBinding = (
  row: ControlledThreadCatalogRow,
  state: AgentControlControlledThreadReservationState,
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
            prepared_at AS "preparedAt"
          FROM agent_control_controlled_thread_stream_catalog
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

  const loadState = (
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
      const catalog = yield* sql<ControlledThreadCatalogRow>`
        SELECT
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
          prepared_at AS "preparedAt"
        FROM agent_control_controlled_thread_stream_catalog
        WHERE controlled_thread_reservation_id = ${controlledThreadReservationId}
          AND stream_version = 1
      `;
      if (
        (Option.isNone(state) && catalog.length !== 0) ||
        (Option.isSome(state) &&
          (catalog.length !== 1 || !sameCatalogBinding(catalog[0]!, state.value)))
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          projectId,
          taskId,
          controlledThreadReservationId,
        });
      }
      return state;
    }).pipe(
      Effect.mapError((failure) =>
        isRpcError(failure)
          ? failure
          : mapHistoryError(failure, { projectId, taskId, controlledThreadReservationId }),
      ),
    );

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
          readonly state: AgentControlControlledThreadReservationState;
          readonly result: AgentControlControlledThreadReservationCommandResult;
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
      const state = yield* loadState(
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
      if (
        storedIntent.value.commandType !== "agentControl.controlledThreadReservation.prepare" ||
        storedIntent.value.controlledThreadReservationId !== controlledThreadReservationId ||
        storedIntent.value.threadId !== state.threadId ||
        storedIntent.value.taskRevision !== state.taskRevision ||
        storedIntent.value.githubIntakeSequence !== state.githubIntakeSequence ||
        storedIntent.value.sourceIdentityFingerprint !== state.sourceIdentityFingerprint ||
        storedIntent.value.stageRunId !== state.stageRunId ||
        storedIntent.value.attemptId !== state.attemptId ||
        storedIntent.value.roleId !== state.roleId ||
        storedIntent.value.stageKind !== state.stageKind ||
        storedIntent.value.stageOrdinal !== state.stageOrdinal ||
        storedIntent.value.attemptOrdinal !== state.attemptOrdinal ||
        storedIntent.value.leaseId !== state.leaseId ||
        storedIntent.value.fenceToken !== state.fenceToken ||
        storedIntent.value.worktreeReservationId !== state.worktreeReservationId ||
        storedIntent.value.expectedRevision !== 0 ||
        storedIntent.value.targetStatus !== null ||
        state.projectId !== input.projectId ||
        state.taskId !== input.taskId ||
        state.revision !== value.resultStreamVersion ||
        state.sequence !== value.resultSequence ||
        (value.eventCreated && value.acceptedAt !== state.preparedAt)
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
      return Option.some({
        state,
        result: {
          reservation: toAgentControlControlledThreadReservationView(state),
          resultSequence: value.resultSequence,
          eventCreated: value.eventCreated,
        },
      });
    },
  );

  const replayReceiptFirst: AgentControlControlledThreadReservationEngineShape["replayReceiptFirst"] =
    (input) =>
      sql.withTransaction(replayState({ ...input, initialReplay: true })).pipe(
        Effect.map(Option.map((replayed) => replayed.result)),
        Effect.catchTag("SqlError", () =>
          Effect.fail(rpcError("internal-persistence-error", input)),
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
        return yield* sql.withTransaction(
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
                !sameCommandBinding(replay.value.state, command)
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
                  state.controlledThreadReservationId === command.controlledThreadReservationId,
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
                    Effect.mapError(() => rpcError("internal-persistence-error", command)),
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
                        controlledThreadReservationId: command.controlledThreadReservationId,
                        expectedStreamVersion: 0,
                        events: decision.success,
                      }),
                    ),
                  );
            let next = current;
            for (const event of appended) {
              yield* projection.projectEvent(event);
              next = yield* projectAgentControlControlledThreadReservationEvent(next, event);
            }
            if (next === null) {
              return yield* rpcError("controlled-thread-reservation-missing", command);
            }
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
              status: "accepted",
              resultSequence: next.sequence,
              resultStreamVersion: next.revision,
              eventCreated: appended.length > 0,
              acceptedAt: occurredAt,
              errorCode: null,
            });
            yield* transactionHooks.afterWritesBeforeCommit;
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
        );
      }).pipe(
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

  const publishCommitted: AgentControlControlledThreadReservationEngineShape["publishCommitted"] = (
    committed,
  ) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });

  return AgentControlControlledThreadReservationEngine.of({
    dispatchPreparedController,
    replayReceiptFirst,
    getAuthoritative,
    validateTaskHistory,
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
