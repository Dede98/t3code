import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageKind,
  AgentControlStageRunId,
  AgentControlStageRunState,
  AgentControlStageRunStatus,
  AgentControlTaskId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
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
import { validateAgentControlStageRunState } from "../initialInvariant.ts";
import {
  AgentControlStageRunStateRepository,
  type AgentControlStageRunEnumerationEntry,
  type AgentControlStageRunStateRepositoryShape,
} from "../Services/AgentControlStageRunStateRepository.ts";

const StateRow = Schema.Struct({
  state: Schema.fromJsonString(AgentControlStageRunState),
  stageRunId: AgentControlStageRunId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: AgentControlStageKind,
  stageOrdinal: PositiveInt,
  attemptOrdinal: PositiveInt,
  status: AgentControlStageRunStatus,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
});
const decodeRow = Schema.decodeUnknownEffect(StateRow);
const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunState);
const encodeState = Schema.encodeUnknownEffect(Schema.fromJsonString(AgentControlStageRunState));
const decodeStageRunId = Schema.decodeUnknownEffect(AgentControlStageRunId);
const decodeProjectId = Schema.decodeUnknownEffect(ProjectId);
const decodeTaskId = Schema.decodeUnknownEffect(AgentControlTaskId);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const sameInitialPosition = (left: AgentControlStageRunState, right: AgentControlStageRunState) =>
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.stageKind === right.stageKind &&
  left.stageOrdinal === right.stageOrdinal;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const decodeInvariant = (row: Record<string, unknown>, operation: string) =>
    decodeRow(row).pipe(
      Effect.mapError((cause) => decodeError(operation, cause)),
      Effect.flatMap(({ state, ...columns }) => {
        if (
          state.stageRunId !== columns.stageRunId ||
          state.projectId !== columns.projectId ||
          state.taskId !== columns.taskId ||
          state.attemptId !== columns.attemptId ||
          state.roleId !== columns.roleId ||
          state.stageKind !== columns.stageKind ||
          state.stageOrdinal !== columns.stageOrdinal ||
          state.attemptOrdinal !== columns.attemptOrdinal ||
          state.status !== columns.status ||
          state.taskRevision !== columns.taskRevision ||
          state.githubIntakeSequence !== columns.githubIntakeSequence ||
          state.sourceIdentityFingerprint !== columns.sourceIdentityFingerprint ||
          state.createdAt !== columns.createdAt ||
          state.updatedAt !== columns.updatedAt ||
          state.revision !== columns.revision ||
          state.sequence !== columns.sequence
        ) {
          return Effect.fail(decodeError(operation, new Error("stage-run projection mismatch")));
        }
        return validateAgentControlStageRunState(state).pipe(
          Effect.mapError((cause) => decodeError(operation, cause)),
        );
      }),
    );

  const validateUnambiguousHistory = (
    states: ReadonlyArray<AgentControlStageRunState>,
    operation: string,
  ) =>
    Effect.gen(function* () {
      const positions: Array<AgentControlStageRunState> = [];
      for (const state of states) {
        if (positions.some((candidate) => sameInitialPosition(candidate, state))) {
          return yield* decodeError(
            `${operation}:ambiguous`,
            new Error("ambiguous initial stage-run position"),
          );
        }
        positions.push(state);
      }
      return states;
    });

  const ensureInitialPositionAvailable = (state: AgentControlStageRunState) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state, stage_run_id AS "stageRunId", project_id AS "projectId",
        task_id AS "taskId", attempt_id AS "attemptId", role_id AS "roleId",
        stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
        attempt_ordinal AS "attemptOrdinal", status, task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt", revision,
        last_event_sequence AS sequence
      FROM agent_control_stage_run_states
      WHERE project_id = ${state.projectId} AND task_id = ${state.taskId}
        AND task_revision = ${state.taskRevision}
        AND github_intake_sequence = ${state.githubIntakeSequence}
        AND stage_kind = ${state.stageKind} AND stage_ordinal = ${state.stageOrdinal}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunStateRepository.save:initial-position", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeInvariant(row, "AgentControlStageRunStateRepository.save:initial-position"),
        ),
      ),
      Effect.flatMap((states) =>
        states.length === 0
          ? Effect.void
          : Effect.fail(
              decodeError(
                "AgentControlStageRunStateRepository.save:ambiguous",
                new Error("ambiguous initial stage-run position"),
              ),
            ),
      ),
    );

  const get: AgentControlStageRunStateRepositoryShape["get"] = (stageRunId) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state, stage_run_id AS "stageRunId", project_id AS "projectId",
        task_id AS "taskId", attempt_id AS "attemptId", role_id AS "roleId",
        stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
        attempt_ordinal AS "attemptOrdinal", status, task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt", revision,
        last_event_sequence AS sequence
      FROM agent_control_stage_run_states
      WHERE stage_run_id = ${stageRunId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlStageRunStateRepository.get", cause)),
      Effect.flatMap((rows) => {
        const row = rows[0];
        return row === undefined
          ? Effect.succeed(Option.none())
          : decodeInvariant(row, "AgentControlStageRunStateRepository.get").pipe(
              Effect.map(Option.some),
            );
      }),
    );

  const save: AgentControlStageRunStateRepositoryShape["save"] = (rawState, expectedRevision) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunStateRepository.save:input", cause),
        ),
        Effect.flatMap((state) =>
          validateAgentControlStageRunState(state).pipe(
            Effect.mapError((cause) =>
              decodeError("AgentControlStageRunStateRepository.save:invariant", cause),
            ),
          ),
        ),
      );
      if (state.revision !== expectedRevision + 1) {
        return yield* decodeError(
          "AgentControlStageRunStateRepository.save:revision",
          new Error("stage-run projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* Effect.gen(function* () {
              yield* ensureInitialPositionAvailable(state);
              return yield* sql<{ readonly stageRunId: unknown }>`
                INSERT INTO agent_control_stage_run_states (
                  stage_run_id, project_id, task_id, attempt_id, role_id,
                  stage_kind, stage_ordinal, attempt_ordinal, status,
                  task_revision, github_intake_sequence, source_identity_fingerprint,
                  state_json, created_at, updated_at, revision, last_event_sequence
                ) VALUES (
                  ${state.stageRunId}, ${state.projectId}, ${state.taskId}, ${state.attemptId},
                  ${state.roleId}, ${state.stageKind}, ${state.stageOrdinal},
                  ${state.attemptOrdinal}, ${state.status}, ${state.taskRevision},
                  ${state.githubIntakeSequence}, ${state.sourceIdentityFingerprint},
                  ${stateJson}, ${state.createdAt}, ${state.updatedAt}, ${state.revision},
                  ${state.sequence}
                )
                ON CONFLICT (stage_run_id) DO NOTHING
                RETURNING stage_run_id AS "stageRunId"
              `;
            }).pipe(
              Effect.catchTag("SqlError", (cause) =>
                Effect.fail(sqlError("AgentControlStageRunStateRepository.save:write", cause)),
              ),
            )
          : yield* sql<{ readonly stageRunId: unknown }>`
              UPDATE agent_control_stage_run_states
              SET status = ${state.status}, state_json = ${stateJson},
                updated_at = ${state.updatedAt}, revision = ${state.revision},
                last_event_sequence = ${state.sequence}
              WHERE stage_run_id = ${state.stageRunId}
                AND project_id = ${state.projectId} AND task_id = ${state.taskId}
                AND attempt_id = ${state.attemptId} AND role_id = ${state.roleId}
                AND stage_kind = ${state.stageKind} AND stage_ordinal = ${state.stageOrdinal}
                AND attempt_ordinal = ${state.attemptOrdinal}
                AND task_revision = ${state.taskRevision}
                AND github_intake_sequence = ${state.githubIntakeSequence}
                AND source_identity_fingerprint = ${state.sourceIdentityFingerprint}
                AND created_at = ${state.createdAt}
                AND revision = ${expectedRevision}
                AND updated_at <= ${state.updatedAt}
                AND last_event_sequence < ${state.sequence}
              RETURNING stage_run_id AS "stageRunId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlStageRunStateRepository.save:write", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlStageRunStateRepository.save:conflict",
          new Error("stage-run projection write conflict"),
        );
      }
    });

  const listInitialForTask: AgentControlStageRunStateRepositoryShape["listInitialForTask"] = (
    projectId,
    taskId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state, stage_run_id AS "stageRunId", project_id AS "projectId",
        task_id AS "taskId", attempt_id AS "attemptId", role_id AS "roleId",
        stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
        attempt_ordinal AS "attemptOrdinal", status, task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt", revision,
        last_event_sequence AS sequence
      FROM agent_control_stage_run_states
      WHERE project_id = ${projectId} AND task_id = ${taskId}
      ORDER BY task_revision DESC, github_intake_sequence DESC,
        stage_ordinal DESC, stage_run_id ASC
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunStateRepository.listInitialForTask", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeInvariant(row, "AgentControlStageRunStateRepository.listInitialForTask"),
        ),
      ),
    );

  const findInitialForTask: AgentControlStageRunStateRepositoryShape["findInitialForTask"] = (
    projectId,
    taskId,
  ) =>
    listInitialForTask(projectId, taskId).pipe(
      Effect.flatMap((states) =>
        validateUnambiguousHistory(
          states,
          "AgentControlStageRunStateRepository.findInitialForTask",
        ),
      ),
      Effect.map((states) => (states[0] === undefined ? Option.none() : Option.some(states[0]))),
    );

  const findBySnapshot: AgentControlStageRunStateRepositoryShape["findBySnapshot"] = (identity) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state, stage_run_id AS "stageRunId", project_id AS "projectId",
        task_id AS "taskId", attempt_id AS "attemptId", role_id AS "roleId",
        stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
        attempt_ordinal AS "attemptOrdinal", status, task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt", revision,
        last_event_sequence AS sequence
      FROM agent_control_stage_run_states
      WHERE project_id = ${identity.projectId} AND task_id = ${identity.taskId}
        AND task_revision = ${identity.taskRevision}
        AND github_intake_sequence = ${identity.githubIntakeSequence}
        AND stage_kind = ${identity.stageKind} AND stage_ordinal = ${identity.stageOrdinal}
        AND source_identity_fingerprint = ${identity.sourceIdentityFingerprint}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunStateRepository.findBySnapshot", cause),
      ),
      Effect.flatMap((rows) => {
        if (rows.length === 0) return Effect.succeed(Option.none());
        return Effect.forEach(rows, (row) =>
          decodeInvariant(row, "AgentControlStageRunStateRepository.findBySnapshot"),
        ).pipe(
          Effect.flatMap((states) =>
            states.length === 1
              ? Effect.succeed(Option.some(states[0]!))
              : Effect.fail(
                  decodeError(
                    "AgentControlStageRunStateRepository.findBySnapshot:ambiguous",
                    new Error("ambiguous initial stage-run snapshot"),
                  ),
                ),
          ),
        );
      }),
    );

  const listProject: AgentControlStageRunStateRepositoryShape["listProject"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state, stage_run_id AS "stageRunId", project_id AS "projectId",
        task_id AS "taskId", attempt_id AS "attemptId", role_id AS "roleId",
        stage_kind AS "stageKind", stage_ordinal AS "stageOrdinal",
        attempt_ordinal AS "attemptOrdinal", status, task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt", revision,
        last_event_sequence AS sequence
      FROM agent_control_stage_run_states
      WHERE project_id = ${projectId}
      ORDER BY updated_at DESC, stage_run_id ASC
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunStateRepository.listProject", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            const stageRunId = Option.getOrNull(
              yield* Effect.option(decodeStageRunId(row.stageRunId)),
            );
            const rowProjectId = Option.getOrNull(
              yield* Effect.option(decodeProjectId(row.projectId)),
            );
            const taskId = Option.getOrNull(yield* Effect.option(decodeTaskId(row.taskId)));
            const decoded = yield* Effect.option(
              decodeInvariant(row, "AgentControlStageRunStateRepository.listProject"),
            );
            return Option.isSome(decoded)
              ? ({
                  _tag: "Valid",
                  state: decoded.value,
                } satisfies AgentControlStageRunEnumerationEntry)
              : ({
                  _tag: "Corrupt",
                  stageRunId,
                  projectId: rowProjectId,
                  taskId,
                } satisfies AgentControlStageRunEnumerationEntry);
          }),
        ),
      ),
    );

  const deleteAll = sql`DELETE FROM agent_control_stage_run_states`.pipe(
    Effect.mapError((cause) => sqlError("AgentControlStageRunStateRepository.deleteAll", cause)),
    Effect.asVoid,
  );

  return AgentControlStageRunStateRepository.of({
    get,
    save,
    findInitialForTask,
    listInitialForTask,
    findBySnapshot,
    listProject,
    deleteAll,
  });
});

export const layer = Layer.effect(AgentControlStageRunStateRepository, make);
