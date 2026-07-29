import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationState,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  IsoDateTime,
  PositiveInt,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";
import { validateAgentControlControlledThreadReservationState } from "../invariant.ts";
import {
  AgentControlControlledThreadReservationStateRepository,
  type AgentControlControlledThreadReservationStateRepositoryShape,
} from "../Services/AgentControlControlledThreadReservationStateRepository.ts";

const StateRow = Schema.Struct({
  state: Schema.fromJsonString(AgentControlControlledThreadReservationState),
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: Schema.Literal("planning"),
  stageOrdinal: Schema.Literal(1),
  attemptOrdinal: Schema.Literal(1),
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  worktreeReservationId: AgentControlWorktreeReservationId,
  status: Schema.Literals(["prepared", "materializing", "bound"]),
  revision: PositiveInt,
  sequence: PositiveInt,
  preparedAt: IsoDateTime,
  coordinatorCommandId: Schema.NullOr(CommandId),
  coordinatorCommandFingerprint: Schema.NullOr(Schema.String),
  materializingTransitionCommandId: Schema.NullOr(CommandId),
  materializationCommandId: Schema.NullOr(CommandId),
  materializationCommandFingerprint: Schema.NullOr(Schema.String),
  leaseHolderId: Schema.NullOr(AgentControlStageRunLeaseHolderId),
  materializingAt: Schema.NullOr(IsoDateTime),
  boundTransitionCommandId: Schema.NullOr(CommandId),
  orchestrationResultSequence: Schema.NullOr(PositiveInt),
  materializedAt: Schema.NullOr(IsoDateTime),
  boundAt: Schema.NullOr(IsoDateTime),
});
const decodeRow = Schema.decodeUnknownEffect(StateRow);
const decodeState = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationState);
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlControlledThreadReservationState),
);
const decodeReservationId = Schema.decodeUnknownOption(AgentControlControlledThreadReservationId);
const decodeProjectId = Schema.decodeUnknownOption(ProjectId);
const decodeTaskId = Schema.decodeUnknownOption(AgentControlTaskId);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const sameColumns = (
  state: AgentControlControlledThreadReservationState,
  columns: Omit<typeof StateRow.Type, "state">,
) =>
  state.controlledThreadReservationId === columns.controlledThreadReservationId &&
  state.threadId === columns.threadId &&
  state.projectId === columns.projectId &&
  state.taskId === columns.taskId &&
  state.taskRevision === columns.taskRevision &&
  state.githubIntakeSequence === columns.githubIntakeSequence &&
  state.sourceIdentityFingerprint === columns.sourceIdentityFingerprint &&
  state.stageRunId === columns.stageRunId &&
  state.attemptId === columns.attemptId &&
  state.roleId === columns.roleId &&
  state.stageKind === columns.stageKind &&
  state.stageOrdinal === columns.stageOrdinal &&
  state.attemptOrdinal === columns.attemptOrdinal &&
  state.leaseId === columns.leaseId &&
  state.fenceToken === columns.fenceToken &&
  state.worktreeReservationId === columns.worktreeReservationId &&
  state.status === columns.status &&
  state.revision === columns.revision &&
  state.sequence === columns.sequence &&
  state.preparedAt === columns.preparedAt &&
  (state.status === "prepared"
    ? columns.coordinatorCommandId === null &&
      columns.coordinatorCommandFingerprint === null &&
      columns.materializingTransitionCommandId === null &&
      columns.materializationCommandId === null &&
      columns.materializationCommandFingerprint === null &&
      columns.leaseHolderId === null &&
      columns.materializingAt === null &&
      columns.boundTransitionCommandId === null &&
      columns.orchestrationResultSequence === null &&
      columns.materializedAt === null &&
      columns.boundAt === null
    : state.coordinatorCommandId === columns.coordinatorCommandId &&
      state.coordinatorCommandFingerprint === columns.coordinatorCommandFingerprint &&
      state.materializingTransitionCommandId === columns.materializingTransitionCommandId &&
      state.materializationCommandId === columns.materializationCommandId &&
      state.materializationCommandFingerprint === columns.materializationCommandFingerprint &&
      state.leaseHolderId === columns.leaseHolderId &&
      state.materializingAt === columns.materializingAt &&
      (state.status === "materializing"
        ? columns.boundTransitionCommandId === null &&
          columns.orchestrationResultSequence === null &&
          columns.materializedAt === null &&
          columns.boundAt === null
        : state.boundTransitionCommandId === columns.boundTransitionCommandId &&
          state.orchestrationResultSequence === columns.orchestrationResultSequence &&
          state.materializedAt === columns.materializedAt &&
          state.boundAt === columns.boundAt));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const decodeInvariant = (row: Record<string, unknown>, operation: string) =>
    decodeRow(row).pipe(
      Effect.mapError((cause) => decodeError(operation, cause)),
      Effect.flatMap(({ state, ...columns }) =>
        sameColumns(state, columns)
          ? validateAgentControlControlledThreadReservationState(state).pipe(
              Effect.mapError((cause) => decodeError(operation, cause)),
            )
          : Effect.fail(
              decodeError(
                operation,
                new Error("controlled thread reservation projection mismatch"),
              ),
            ),
      ),
    );

  const get: AgentControlControlledThreadReservationStateRepositoryShape["get"] = (
    controlledThreadReservationId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state,
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
        task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        role_id AS "roleId", stage_kind AS "stageKind",
        stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
        lease_id AS "leaseId", fence_token AS "fenceToken",
        worktree_reservation_id AS "worktreeReservationId", status, revision,
        last_event_sequence AS sequence, prepared_at AS "preparedAt",
        coordinator_command_id AS "coordinatorCommandId",
        coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        materializing_transition_command_id AS "materializingTransitionCommandId",
        materialization_command_id AS "materializationCommandId",
        materialization_command_fingerprint AS "materializationCommandFingerprint",
        lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
        bound_transition_command_id AS "boundTransitionCommandId",
        orchestration_result_sequence AS "orchestrationResultSequence",
        materialized_at AS "materializedAt", bound_at AS "boundAt"
      FROM agent_control_controlled_thread_reservation_states
      WHERE controlled_thread_reservation_id = ${controlledThreadReservationId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlControlledThreadReservationStateRepository.get", cause),
      ),
      Effect.flatMap((rows) => {
        const row = rows[0];
        return row === undefined
          ? Effect.succeed(Option.none())
          : decodeInvariant(row, "AgentControlControlledThreadReservationStateRepository.get").pipe(
              Effect.map(Option.some),
            );
      }),
    );

  const listTask: AgentControlControlledThreadReservationStateRepositoryShape["listTask"] = (
    projectId,
    taskId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state,
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
        task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        role_id AS "roleId", stage_kind AS "stageKind",
        stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
        lease_id AS "leaseId", fence_token AS "fenceToken",
        worktree_reservation_id AS "worktreeReservationId", status, revision,
        last_event_sequence AS sequence, prepared_at AS "preparedAt",
        coordinator_command_id AS "coordinatorCommandId",
        coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        materializing_transition_command_id AS "materializingTransitionCommandId",
        materialization_command_id AS "materializationCommandId",
        materialization_command_fingerprint AS "materializationCommandFingerprint",
        lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
        bound_transition_command_id AS "boundTransitionCommandId",
        orchestration_result_sequence AS "orchestrationResultSequence",
        materialized_at AS "materializedAt", bound_at AS "boundAt"
      FROM agent_control_controlled_thread_reservation_states
      WHERE project_id = ${projectId} AND task_id = ${taskId}
      ORDER BY task_revision ASC, github_intake_sequence ASC,
        stage_ordinal ASC, attempt_ordinal ASC, controlled_thread_reservation_id ASC
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlControlledThreadReservationStateRepository.listTask", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeInvariant(row, "AgentControlControlledThreadReservationStateRepository.listTask"),
        ),
      ),
    );

  const listAll: AgentControlControlledThreadReservationStateRepositoryShape["listAll"] = sql<
    Record<string, unknown>
  >`
      SELECT
        state_json AS state,
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
        task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        role_id AS "roleId", stage_kind AS "stageKind",
        stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
        lease_id AS "leaseId", fence_token AS "fenceToken",
        worktree_reservation_id AS "worktreeReservationId", status, revision,
        last_event_sequence AS sequence, prepared_at AS "preparedAt",
        coordinator_command_id AS "coordinatorCommandId",
        coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        materializing_transition_command_id AS "materializingTransitionCommandId",
        materialization_command_id AS "materializationCommandId",
        materialization_command_fingerprint AS "materializationCommandFingerprint",
        lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
        bound_transition_command_id AS "boundTransitionCommandId",
        orchestration_result_sequence AS "orchestrationResultSequence",
        materialized_at AS "materializedAt", bound_at AS "boundAt"
      FROM agent_control_controlled_thread_reservation_states
      ORDER BY task_revision ASC, github_intake_sequence ASC,
        stage_ordinal ASC, attempt_ordinal ASC, controlled_thread_reservation_id ASC
    `.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlControlledThreadReservationStateRepository.listAll", cause),
    ),
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) =>
        decodeInvariant(row, "AgentControlControlledThreadReservationStateRepository.listAll"),
      ),
    ),
  );

  const findBySemanticPosition: AgentControlControlledThreadReservationStateRepositoryShape["findBySemanticPosition"] =
    (position) =>
      listTask(position.projectId, position.taskId).pipe(
        Effect.flatMap((states) => {
          const matches = states.filter(
            (state) =>
              state.stageRunId === position.stageRunId &&
              state.attemptId === position.attemptId &&
              state.roleId === position.roleId &&
              state.stageOrdinal === position.stageOrdinal &&
              state.attemptOrdinal === position.attemptOrdinal,
          );
          if (matches.length > 1) {
            return Effect.fail(
              decodeError(
                "AgentControlControlledThreadReservationStateRepository.findBySemanticPosition",
                new Error("ambiguous semantic reservation position"),
              ),
            );
          }
          return Effect.succeed(matches[0] === undefined ? Option.none() : Option.some(matches[0]));
        }),
      );

  const save: AgentControlControlledThreadReservationStateRepositoryShape["save"] = (
    rawState,
    expectedRevision,
  ) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlControlledThreadReservationStateRepository.save:input", cause),
        ),
        Effect.flatMap((decoded) =>
          validateAgentControlControlledThreadReservationState(decoded).pipe(
            Effect.mapError((cause) =>
              decodeError(
                "AgentControlControlledThreadReservationStateRepository.save:invariant",
                cause,
              ),
            ),
          ),
        ),
      );
      if (
        !Number.isInteger(expectedRevision) ||
        expectedRevision < 0 ||
        state.revision !== expectedRevision + 1
      ) {
        return yield* decodeError(
          "AgentControlControlledThreadReservationStateRepository.save:revision",
          new Error("reservation projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlControlledThreadReservationStateRepository.save:encode", cause),
        ),
      );
      const materializing = state.status === "prepared" ? null : state;
      const bound = state.status === "bound" ? state : null;
      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly id: unknown }>`
              INSERT INTO agent_control_controlled_thread_reservation_states (
                controlled_thread_reservation_id, thread_id, project_id, task_id,
                task_revision, github_intake_sequence, source_identity_fingerprint,
                stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
                attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
                status, revision, last_event_sequence, prepared_at,
                coordinator_command_id, coordinator_command_fingerprint,
                materializing_transition_command_id, materialization_command_id,
                materialization_command_fingerprint, lease_holder_id, materializing_at,
                bound_transition_command_id, orchestration_result_sequence,
                materialized_at, bound_at, state_json
              ) VALUES (
                ${state.controlledThreadReservationId}, ${state.threadId},
                ${state.projectId}, ${state.taskId}, ${state.taskRevision},
                ${state.githubIntakeSequence}, ${state.sourceIdentityFingerprint},
                ${state.stageRunId}, ${state.attemptId}, ${state.roleId},
                ${state.stageKind}, ${state.stageOrdinal}, ${state.attemptOrdinal},
                ${state.leaseId}, ${state.fenceToken}, ${state.worktreeReservationId},
                ${state.status}, ${state.revision}, ${state.sequence}, ${state.preparedAt},
                ${materializing?.coordinatorCommandId ?? null},
                ${materializing?.coordinatorCommandFingerprint ?? null},
                ${materializing?.materializingTransitionCommandId ?? null},
                ${materializing?.materializationCommandId ?? null},
                ${materializing?.materializationCommandFingerprint ?? null},
                ${materializing?.leaseHolderId ?? null}, ${materializing?.materializingAt ?? null},
                ${bound?.boundTransitionCommandId ?? null},
                ${bound?.orchestrationResultSequence ?? null},
                ${bound?.materializedAt ?? null}, ${bound?.boundAt ?? null}, ${stateJson}
              )
              ON CONFLICT (controlled_thread_reservation_id) DO NOTHING
              RETURNING controlled_thread_reservation_id AS id
            `
          : yield* sql<{ readonly id: unknown }>`
              UPDATE agent_control_controlled_thread_reservation_states
              SET status = ${state.status}, revision = ${state.revision},
                last_event_sequence = ${state.sequence},
                coordinator_command_id = ${materializing?.coordinatorCommandId ?? null},
                coordinator_command_fingerprint =
                  ${materializing?.coordinatorCommandFingerprint ?? null},
                materializing_transition_command_id =
                  ${materializing?.materializingTransitionCommandId ?? null},
                materialization_command_id =
                  ${materializing?.materializationCommandId ?? null},
                materialization_command_fingerprint =
                  ${materializing?.materializationCommandFingerprint ?? null},
                lease_holder_id = ${materializing?.leaseHolderId ?? null},
                materializing_at = ${materializing?.materializingAt ?? null},
                bound_transition_command_id = ${bound?.boundTransitionCommandId ?? null},
                orchestration_result_sequence = ${bound?.orchestrationResultSequence ?? null},
                materialized_at = ${bound?.materializedAt ?? null},
                bound_at = ${bound?.boundAt ?? null},
                state_json = ${stateJson}
              WHERE controlled_thread_reservation_id = ${state.controlledThreadReservationId}
                AND revision = ${expectedRevision}
              RETURNING controlled_thread_reservation_id AS id
            `;
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlControlledThreadReservationStateRepository.save:conflict",
          new Error("controlled thread reservation projection write conflict"),
        );
      }
    }).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          sqlError("AgentControlControlledThreadReservationStateRepository.save:write", cause),
        ),
      ),
    );

  const listProject: AgentControlControlledThreadReservationStateRepositoryShape["listProject"] = (
    projectId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state,
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
        task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        role_id AS "roleId", stage_kind AS "stageKind",
        stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
        lease_id AS "leaseId", fence_token AS "fenceToken",
        worktree_reservation_id AS "worktreeReservationId", status, revision,
        last_event_sequence AS sequence, prepared_at AS "preparedAt",
        coordinator_command_id AS "coordinatorCommandId",
        coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        materializing_transition_command_id AS "materializingTransitionCommandId",
        materialization_command_id AS "materializationCommandId",
        materialization_command_fingerprint AS "materializationCommandFingerprint",
        lease_holder_id AS "leaseHolderId", materializing_at AS "materializingAt",
        bound_transition_command_id AS "boundTransitionCommandId",
        orchestration_result_sequence AS "orchestrationResultSequence",
        materialized_at AS "materializedAt", bound_at AS "boundAt"
      FROM agent_control_controlled_thread_reservation_states
      WHERE project_id = ${projectId}
      ORDER BY task_revision ASC, github_intake_sequence ASC,
        stage_ordinal ASC, attempt_ordinal ASC, controlled_thread_reservation_id ASC
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlControlledThreadReservationStateRepository.listProject", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          Effect.result(
            decodeInvariant(
              row,
              "AgentControlControlledThreadReservationStateRepository.listProject",
            ),
          ).pipe(
            Effect.map((decoded) =>
              decoded._tag === "Success"
                ? ({ _tag: "Valid", state: decoded.success } as const)
                : ({
                    _tag: "Corrupt",
                    controlledThreadReservationId: Option.getOrNull(
                      decodeReservationId(row.controlledThreadReservationId),
                    ),
                    projectId: Option.getOrNull(decodeProjectId(row.projectId)),
                    taskId: Option.getOrNull(decodeTaskId(row.taskId)),
                  } as const),
            ),
          ),
        ),
      ),
    );

  const deleteAll = sql`DELETE FROM agent_control_controlled_thread_reservation_states`.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlControlledThreadReservationStateRepository.deleteAll", cause),
    ),
    Effect.asVoid,
  );

  return AgentControlControlledThreadReservationStateRepository.of({
    get,
    save,
    listTask,
    listAll,
    findBySemanticPosition,
    listProject,
    deleteAll,
  });
});

export const layer = Layer.effect(AgentControlControlledThreadReservationStateRepository, make);
